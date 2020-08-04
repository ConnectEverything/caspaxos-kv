use std::{
    collections::HashMap,
    convert::TryInto,
    future::Future,
    io,
    mem::replace,
    net::ToSocketAddrs,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_channel::Receiver;
use async_mutex::Mutex;
use futures_channel::oneshot::Receiver as OneshotReceiver;
use smol::{Task, Timer};
use uuid::Uuid;

use crate::{
    simulator::{Simulator, SimulatorRunner},
    udp_net::UdpNet,
    Envelope, Message, Request, Response,
};

#[derive(Debug)]
pub struct Net {
    pub address: SocketAddr,
    pub timeout: Option<Duration>,
    incoming: Receiver<(SocketAddr, Uuid, Request)>,
    inner: NetInner,
}

#[derive(Debug)]
pub enum NetInner {
    Simulator(Arc<Mutex<Simulator>>),
    Udp(UdpNet),
}

#[derive(Debug)]
pub(crate) struct ResponseHandle(pub(crate) OneshotReceiver<Response>);

// TODO can just do ResponseHandle(Box::pin(async { stream.next().await ... }))
impl Future for ResponseHandle {
    type Output = io::Result<Response>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        unsafe {
            if let Poll::Ready(response) =
                Pin::new_unchecked(&mut self.0).poll(cx)
            {
                let response = if let Ok(response) = response {
                    Ok(response)
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "request timed out",
                    ))
                };

                Poll::Ready(response)
            } else {
                Poll::Pending
            }
        }
    }
}

#[derive(Debug)]
struct TimeoutLimitedBroadcast {
    pending: Vec<(ResponseHandle, SocketAddr)>,
    complete: Vec<(Response, SocketAddr)>,
    timeout: Option<Timer>,
    wait_for: usize,
}

impl Future for TimeoutLimitedBroadcast {
    type Output = Vec<(Response, SocketAddr)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut to_remove = vec![];
        let mut to_complete: Vec<(Response, SocketAddr)> = vec![];
        for (idx, response) in self.pending.iter_mut().enumerate() {
            unsafe {
                if let Poll::Ready(Ok(ready)) =
                    Pin::new_unchecked(&mut response.0).poll(cx)
                {
                    to_remove.push(idx);
                    to_complete.push((ready, response.1));
                }
            }
        }

        let successes = to_complete.iter().filter(|r| r.0.is_success()).count();

        self.wait_for = self.wait_for.saturating_sub(successes);

        self.complete.append(&mut to_complete);

        while let Some(idx) = to_remove.pop() {
            self.pending.remove(idx);
        }

        if self.wait_for == 0 || self.pending.is_empty() {
            return Poll::Ready(replace(&mut self.complete, vec![]));
        }

        if let Some(ref mut timeout) = self.timeout {
            unsafe {
                if let Poll::Ready(_) = Pin::new_unchecked(timeout).poll(cx) {
                    println!("timeout");
                    return Poll::Ready(replace(&mut self.complete, vec![]));
                }
            }
        }
        Poll::Pending
    }
}

impl Net {
    /// Create a new net that listens at a particular address.
    /// Spawns a task to feed incoming messages.
    pub fn new_udp<A: ToSocketAddrs + std::fmt::Display>(
        listen_addr: A,
        timeout: Duration,
    ) -> io::Result<(Task<io::Result<()>>, Net)> {
        let (outgoing, incoming) = async_channel::unbounded();

        let mut addrs_iter = listen_addr.to_socket_addrs()?;
        // NB we only use the first address. this is buggy.
        let address = if let Some(address) = addrs_iter.next() {
            address
        } else {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("the address {} could not be resolved", listen_addr),
            ));
        };

        let udp_net = UdpNet::new(listen_addr)?;
        let udp_net_2 = udp_net.clone();

        let server = Task::spawn(udp_net_2.server_loop(outgoing));

        Ok((
            server,
            Net {
                address,
                timeout: Some(timeout),
                incoming,
                inner: NetInner::Udp(udp_net),
            },
        ))
    }

    /// Create a cluster of a certain size with a certain amount of lossiness
    pub fn simulation(
        size: usize,
        lossiness: Option<u32>,
        timeout: Option<std::time::Duration>,
    ) -> (Vec<Net>, SimulatorRunner) {
        let mut rxs = vec![];

        let mut simulator = Simulator {
            lossiness,
            in_flight: vec![],
            inboxes: HashMap::default(),
            waiting_requests: HashMap::default(),
        };

        for i in 0..size {
            let octet = i.try_into().unwrap();
            let address = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(octet, octet, octet, octet)),
                octet as u16,
            );

            let (outgoing, incoming) = async_channel::unbounded();

            simulator.inboxes.insert(address, outgoing);

            rxs.push((address, incoming));
        }

        let simulator = Arc::new(Mutex::new(simulator));

        let mut ret = vec![];

        for (address, incoming) in rxs {
            let net = Net {
                address,
                timeout,
                incoming,
                inner: NetInner::Simulator(simulator.clone()),
            };
            ret.push(net);
        }

        let runner = SimulatorRunner { simulator };

        (ret, runner)
    }

    pub(crate) async fn receive(
        &mut self,
    ) -> io::Result<(SocketAddr, Uuid, Request)> {
        crate::debug_delay().await;

        if let Ok(item) = self.incoming.recv().await {
            Ok(item)
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Net::receive failed",
            ))
        }
    }

    pub(crate) async fn respond(
        &mut self,
        to: SocketAddr,
        uuid: Uuid,
        response: Response,
    ) -> io::Result<()> {
        crate::debug_delay().await;

        match &mut self.inner {
            NetInner::Simulator(s) => {
                let mut simulator = s.lock().await;
                simulator.respond(self.address, to, uuid, response).await
            }
            NetInner::Udp(u) => {
                u.send_message(
                    to,
                    Envelope {
                        uuid,
                        message: Message::Response(response),
                    },
                )
                .await
            }
        }
    }

    /// Sends a request to several servers, returning a vector
    /// of responses that were received before a timeout.
    pub(crate) async fn request_multi(
        &mut self,
        servers: &[SocketAddr],
        request: Request,
        wait_for: usize,
    ) -> Vec<(Response, SocketAddr)> {
        let mut pending = vec![];

        let timeout = self.timeout.map(|timeout| Timer::new(timeout));

        for to in servers {
            crate::debug_delay().await;

            let uuid = Uuid::new_v4();
            match &mut self.inner {
                NetInner::Simulator(s) => {
                    let mut simulator = s.lock().await;
                    let response_handle = simulator.request(
                        self.address,
                        *to,
                        uuid,
                        request.clone(),
                    );
                    pending.push((response_handle, *to));
                }
                NetInner::Udp(u) => {
                    if let Ok(response_handle) =
                        u.request(*to, uuid, request.clone()).await
                    {
                        pending.push((response_handle, *to));
                    }
                }
            }
        }

        TimeoutLimitedBroadcast {
            pending,
            complete: vec![],
            timeout,
            wait_for,
        }
        .await
    }
}
