use std::{
    collections::HashMap,
    convert::TryInto,
    future::Future,
    io,
    mem::replace,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use async_channel::{unbounded, Receiver, Sender};
use futures::{lock::Mutex, Stream};
use smol::Timer;
use uuid::Uuid;

use crate::{
    simulator::{Simulator, SimulatorRunner},
    udp_net::UdpNet,
    Request, Response,
};

#[derive(Debug)]
pub struct Net {
    pub address: SocketAddr,
    incoming: Receiver<(SocketAddr, Uuid, Request)>,
    pending_requests: HashMap<Uuid, Sender<Response>>,
    inner: NetInner,
}

#[derive(Debug)]
pub enum NetInner {
    Simulator(Arc<Mutex<Simulator>>),
    Udp(UdpNet),
}

#[derive(Debug)]
pub(crate) struct ResponseHandle(pub(crate) Receiver<Response>);

// TODO can just do ResponseHandle(Box::pin(async { stream.next().await ... }))
impl Future for ResponseHandle {
    type Output = io::Result<Response>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        unsafe {
            if let Poll::Ready(response) =
                Pin::new_unchecked(&mut self.0).poll_next(cx)
            {
                let response = if let Some(response) = response {
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
    pending: Vec<ResponseHandle>,
    complete: Vec<Response>,
    timeout: Timer,
    wait_for: usize,
}

impl Future for TimeoutLimitedBroadcast {
    type Output = Vec<Response>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut to_remove = vec![];
        let mut to_complete = vec![];
        for (idx, response) in self.pending.iter_mut().enumerate() {
            unsafe {
                if let Poll::Ready(Ok(response)) =
                    Pin::new_unchecked(response).poll(cx)
                {
                    to_remove.push(idx);
                    to_complete.push(response);
                }
            }
        }

        self.complete.append(&mut to_complete);

        while let Some(idx) = to_remove.pop() {
            self.pending.remove(idx);
        }

        if self.complete.len() >= self.wait_for || self.pending.is_empty() {
            return Poll::Ready(replace(&mut self.complete, vec![]));
        }

        unsafe {
            if let Poll::Ready(_) =
                Pin::new_unchecked(&mut self.timeout).poll(cx)
            {
                return Poll::Ready(replace(&mut self.complete, vec![]));
            }
        }
        Poll::Pending
    }
}

impl Net {
    /// Create a cluster of a certain size with a certain amount of lossiness
    pub fn simulation(
        size: usize,
        lossiness: Option<u32>,
    ) -> (Vec<Net>, SimulatorRunner) {
        let mut rxs = vec![];

        let mut simulator = Simulator {
            lossiness,
            in_flight: vec![],
            inboxes: HashMap::default(),
            waiting_requests: HashMap::default(),
        };

        for i in 0..size {
            let address = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(0, 0, 0, i.try_into().unwrap())),
                777,
            );

            let (outgoing, incoming) = unbounded();

            simulator.inboxes.insert(address, outgoing);

            rxs.push((address, incoming));
        }

        let simulator = Arc::new(Mutex::new(simulator));

        let mut ret = vec![];

        for (address, incoming) in rxs {
            let net = Net {
                address,
                incoming,
                pending_requests: HashMap::default(),
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
        match &mut self.inner {
            NetInner::Simulator(s) => {
                let mut simulator = s.lock().await;
                simulator.respond(self.address, to, uuid, response).await
            }
            NetInner::Udp(_u) => todo!(),
        }
    }

    /// Sends a request to several servers, returning a vector
    /// of responses that were received before a timeout.
    pub(crate) async fn request_multi(
        &mut self,
        servers: &[SocketAddr],
        request: Request,
        wait_for: usize,
    ) -> Vec<Response> {
        let mut pending = vec![];

        let timeout = Timer::after(Duration::from_millis(10));

        match &mut self.inner {
            NetInner::Simulator(s) => {
                for to in servers {
                    let mut simulator = s.lock().await;
                    /*
                    println!(
                        "sending request {:?} to server {:?}",
                        request, to
                    );
                    */
                    let uuid = Uuid::new_v4();
                    let response_handle = simulator.request(
                        self.address,
                        *to,
                        uuid,
                        request.clone(),
                    );
                    pending.push(response_handle);
                }
            }
            NetInner::Udp(_u) => todo!(),
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
