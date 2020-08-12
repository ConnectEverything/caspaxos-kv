use std::{collections::HashMap, io, net::SocketAddr, sync::Arc};

use async_channel::Sender;
use async_mutex::Mutex;
use futures_channel::oneshot::{channel as oneshot, Sender as OneshotSender};
use rand::{seq::SliceRandom, thread_rng, Rng};
use smol::Task;
use uuid::Uuid;

use crate::{
    network::{Net, ResponseHandle},
    versioned_storage::VersionedStorage,
    Client, Envelope, Message, Request, Response, Server,
};

/// Run a simulation of a cluster of clients. Returns the
/// result of running each client for possible verification
/// purposes.
///
/// # Examples
///
/// ```
/// use caspaxos_kv::{simulate, Client};
/// use smol::Task;
/// fn set_client(mut client: Client) -> Task<()> {
///     Task::local(async move {
///         let responses = client.ping().await;
///         println!("majority pinger got {} responses", responses);
///
///         let set = client.set(b"k1".to_vec(), b"v1".to_vec()).await;
///         println!("set response: {:?}", set);
///     })
/// }
///
/// fn main() {
///     color_backtrace::install();
///
///     let n_servers = 5;
///     let n_clients = 15;
///
///     // drop 1 in 10 messages
///     let lossiness = Some(10);
///     //let lossiness = None;
///
///     let clients = vec![set_client as fn(Client) -> Task<_>; n_clients];
///
///     simulate(lossiness, n_servers, clients);
/// }
/// ```
pub fn simulate<T>(
    lossiness: Option<u32>,
    n_servers: usize,
    client_factories: Vec<fn(Client) -> Task<T>>,
    timeout: Option<std::time::Duration>,
) -> Vec<T> {
    let (mut nets, simulation_runner) =
        Net::simulation(n_servers + client_factories.len(), lossiness, timeout);

    let mut ret: Vec<T> = vec![];

    smol::run(async move {
        // start simulator
        let simulation_runner_task = Task::local(async move {
            simulation_runner.run().await;
        });

        let mut servers = vec![];
        let mut server_addresses = vec![];
        for _ in 0..n_servers {
            let mut server = Server {
                net: nets.pop().unwrap(),
                db: VersionedStorage {
                    db: sled::Config::new().temporary(true).open().unwrap(),
                },
                processor: None,
                promises: Default::default(),
            };
            server_addresses.push(server.net.address);
            let server_task = Task::local(async move {
                server.run().await;
            });
            servers.push(server_task);
        }

        let mut clients = vec![];
        for client_factory in client_factories {
            let client = Client {
                cache: HashMap::default(),
                known_servers: server_addresses.clone(),
                net: nets.pop().unwrap(),
                processor: None,
            };
            let client_task = client_factory(client);

            clients.push(client_task);
        }

        for client in clients {
            let res: T = client.await;
            ret.push(res);
        }

        // TODO run `cancel().await` for each server/sim runner
        drop(servers);
        drop(simulation_runner_task);

        ret
    })
}

#[derive(Debug)]
pub(crate) enum LossyDelivery {
    Loss(OneshotSender<Response>),
    Delivery(SocketAddr, SocketAddr, Envelope),
}

pub struct SimulatorRunner {
    pub(crate) simulator: Arc<Mutex<Simulator>>,
}

impl SimulatorRunner {
    /// Handle message delivery for the backing `Simulator`.
    pub async fn run(self) {
        loop {
            if !cfg!(feature = "fault_injection") {
                smol::Timer::new(std::time::Duration::from_millis(1)).await;
            }
            crate::debug_delay().await;
            let steps = {
                let simulator = self.simulator.lock().await;
                let mut rng = thread_rng();
                rng.gen_range(0, simulator.in_flight.len() + 1)
            };
            smol::Timer::new(std::time::Duration::from_millis(
                thread_rng().gen_range(0, 4),
            ))
            .await;
            for _n in 0..steps {
                let mut simulator = self.simulator.lock().await;
                simulator.step().await;
                crate::debug_delay().await;
            }
        }
    }
}

#[derive(Debug)]
pub struct Simulator {
    pub(crate) lossiness: Option<u32>,
    pub(crate) in_flight: Vec<LossyDelivery>,
    pub(crate) waiting_requests: HashMap<Uuid, OneshotSender<Response>>,
    pub(crate) inboxes:
        HashMap<SocketAddr, Sender<(SocketAddr, Uuid, Request)>>,
}

impl Simulator {
    async fn step(&mut self) {
        // chaos
        {
            let mut rng = thread_rng();
            self.in_flight.shuffle(&mut rng);
        }

        if let Some(LossyDelivery::Delivery(from, to, envelope)) =
            self.in_flight.pop()
        {
            match envelope.message {
                Message::Request(r) => {
                    log::trace!("{} -> {}: {:?}", from.port(), to.port(), r,);
                    self.inboxes[&to]
                        .send((from, envelope.uuid, r))
                        .await
                        .unwrap()
                }
                Message::Response(r) => {
                    log::trace!("{} <- {}: {:?}", to.port(), from.port(), r,);
                    let waiting_request = if let Some(waiting_request) =
                        self.waiting_requests.remove(&envelope.uuid)
                    {
                        waiting_request
                    } else {
                        // recipient no longer waiting for this message
                        return;
                    };

                    if waiting_request.send(r).is_err() {
                        // recipient already finished before message delivery
                    }
                }
            }
        } else {
            // implicit drop of the lossy Loss sender here
        }
    }

    pub(crate) async fn respond(
        &mut self,
        from: SocketAddr,
        to: SocketAddr,
        uuid: Uuid,
        response: Response,
    ) -> io::Result<()> {
        let envelope = Envelope {
            uuid,
            message: Message::Response(response),
        };

        let mut rng = thread_rng();

        if let Some(lossiness) = self.lossiness {
            if rng.gen_ratio(1, lossiness) {
                let tx: OneshotSender<_> =
                    if let Some(tx) = self.waiting_requests.remove(&uuid) {
                        tx
                    } else {
                        return Err(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "Net::respond failed",
                        ));
                    };

                self.in_flight.push(LossyDelivery::Loss(tx));
                return Ok(());
            }
        }

        self.in_flight
            .push(LossyDelivery::Delivery(from, to, envelope));

        Ok(())
    }

    pub(crate) fn request(
        &mut self,
        from: SocketAddr,
        to: SocketAddr,
        uuid: Uuid,
        request: Request,
    ) -> ResponseHandle {
        let (tx, rx) = oneshot();
        let envelope = Envelope {
            uuid,
            message: Message::Request(request),
        };

        if let Some(lossiness) = self.lossiness {
            let mut rng = thread_rng();
            if rng.gen_ratio(1, lossiness) {
                self.in_flight.push(LossyDelivery::Loss(tx));
                return ResponseHandle(rx);
            }
        }

        assert!(self.waiting_requests.insert(uuid, tx).is_none());
        self.in_flight
            .push(LossyDelivery::Delivery(from, to, envelope));
        ResponseHandle(rx)
    }
}
