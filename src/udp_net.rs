use std::{
    collections::HashMap,
    convert::TryInto,
    io,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    sync::Arc,
};

use crate::{
    network::ResponseHandle, serialization::Serialize, Envelope, Message,
    Request, Response,
};

use async_channel::Sender;
use async_mutex::Mutex;
use crc32fast::Hasher;
use futures_channel::oneshot::{channel as oneshot, Sender as OneshotSender};
use smol::Async;
use uuid::Uuid;

const MSG_MAX_SZ: usize = 64 * 1024;

/// A transport that uses UDP and bincode for sending messages
#[derive(Debug, Clone)]
pub struct UdpNet {
    socket: Arc<Async<UdpSocket>>,
    waiting_requests: Arc<Mutex<HashMap<Uuid, OneshotSender<Response>>>>,
}

impl UdpNet {
    /// Create a new `UdpNet`
    pub(crate) fn new<A: std::fmt::Display + ToSocketAddrs>(
        addr: A,
    ) -> io::Result<UdpNet> {
        match Async::<UdpSocket>::bind(&addr) {
            Ok(socket) => {
                let socket = Arc::new(socket);
                Ok(UdpNet {
                    socket,
                    waiting_requests: Arc::new(Mutex::new(HashMap::default())),
                })
            }
            Err(err) => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("the address {} could not be bound: {}", addr, err),
            )),
        }
    }

    pub(crate) async fn server_loop(
        self,
        incoming: Sender<(SocketAddr, Uuid, Request)>,
    ) -> io::Result<()> {
        loop {
            let (from, envelope) = self.next_message().await?;
            match envelope.message {
                Message::Request(req) => {
                    incoming.send((from, envelope.uuid, req)).await.unwrap();
                }
                Message::Response(res) => {
                    let receiver = {
                        let mut waiting_requests =
                            self.waiting_requests.lock().await;
                        waiting_requests.remove(&envelope.uuid).unwrap()
                    };
                    if receiver.send(res).is_err() {
                        // TODO record failure metrics
                    }
                }
            }
        }
    }

    /// Blocks until the next message is received.
    async fn next_message(&self) -> io::Result<(SocketAddr, Envelope)> {
        let mut buf: [u8; MSG_MAX_SZ] = [0; MSG_MAX_SZ];
        loop {
            unsafe {
                let (n, from) = self.socket.recv_from(&mut buf).await.unwrap();

                let crc_sz = std::mem::size_of::<u32>();
                let data_buf = &buf[..n - crc_sz];
                let crc_buf = &buf[n - crc_sz..n];

                let mut hasher = Hasher::new();
                hasher.update(&data_buf);
                let hash = hasher.finalize();

                let crc_array: [u8; 4] = crc_buf.try_into().unwrap();
                assert_eq!(u32::from_le_bytes(crc_array), hash);

                if let Ok(envelope) = Envelope::deserialize(&mut &buf[..n]) {
                    return Ok((from, envelope));
                } else {
                    eprintln!("failed to deserialize received message");
                }
            }
        }
    }

    /// Enqueues the message to be sent. Envelopeay be sent 0-N times with no ordering guarantees.
    pub(crate) async fn send_message(
        &self,
        to: SocketAddr,
        envelope: Envelope,
    ) -> io::Result<()> {
        let mut serialized: Vec<u8> = envelope.serialize();
        let mut hasher = Hasher::new();
        hasher.update(&serialized);
        let hash = hasher.finalize();
        serialized.extend_from_slice(&hash.to_le_bytes());
        assert!(serialized.len() <= MSG_MAX_SZ);

        let n = self.socket.send_to(&serialized, to).await?;
        assert_eq!(n, serialized.len());
        Ok(())
    }

    pub(crate) async fn request(
        &self,
        to: SocketAddr,
        uuid: Uuid,
        request: Request,
    ) -> io::Result<ResponseHandle> {
        let (tx, rx) = oneshot();
        let envelope = Envelope {
            uuid,
            message: Message::Request(request),
        };

        {
            let mut waiting_requests = self.waiting_requests.lock().await;

            assert!(waiting_requests.insert(uuid, tx).is_none());
        }

        self.send_message(to, envelope).await?;

        Ok(ResponseHandle(rx))
    }
}
