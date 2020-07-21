use serde::{Deserialize, Serialize};

mod network;
mod paxos;
mod simulator;
mod udp_net;
mod versioned_storage;

pub use {
    network::Net,
    paxos::{Client, Server},
    simulator::simulate,
};

use std::{io, net::ToSocketAddrs};

/// A possibly present value with an associated version number.
#[derive(
    Default,
    Debug,
    Clone,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
)]
pub struct VersionedValue {
    pub ballot: u64,
    pub value: Option<Vec<u8>>,
}

impl std::ops::Deref for VersionedValue {
    type Target = Option<Vec<u8>>;

    fn deref(&self) -> &Option<Vec<u8>> {
        &self.value
    }
}

impl std::ops::DerefMut for VersionedValue {
    fn deref_mut(&mut self) -> &mut Option<Vec<u8>> {
        &mut self.value
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Message {
    Request(Request),
    Response(Response),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Request {
    Prepare { ballot: u64, key: Vec<u8> },
    Accept { key: Vec<u8>, value: VersionedValue },
    Ping,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Response {
    Promise {
        success: bool,
        current_value: VersionedValue,
    },
    Accepted {
        success: Result<(), VersionedValue>,
    },
    Pong,
}

impl Response {
    fn to_promise(self) -> (bool, VersionedValue) {
        if let Response::Promise {
            success,
            current_value,
        } = self
        {
            (success, current_value)
        } else {
            panic!("called to_promise on {:?}", self);
        }
    }

    fn to_accepted(self) -> Result<(), VersionedValue> {
        if let Response::Accepted { success } = self {
            success
        } else {
            panic!("called to_promise on {:?}", self);
        }
    }

    fn is_pong(self) -> bool {
        if let Response::Pong = self {
            true
        } else {
            false
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Envelope {
    uuid: uuid::Uuid,
    message: Message,
}

pub fn start_udp_client<
    A: ToSocketAddrs + std::fmt::Display,
    B: ToSocketAddrs + std::fmt::Display,
>(
    listen_addr: A,
    servers: &[B],
) -> std::io::Result<Client> {
    let mut known_servers = vec![];

    for server in servers {
        let mut addrs_iter = server.to_socket_addrs()?;
        // NB we only use the first address. this is buggy.
        if let Some(addr) = addrs_iter.next() {
            known_servers.push(addr);
        } else {
            return Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                format!("the address {} could not be resolved", server),
            ));
        }
    }

    let (process_task, net) = Net::new_udp(listen_addr)?;

    let processor = smol::Task::spawn(process_task);

    Ok(Client {
        known_servers,
        net,
        cache: Default::default(),
        processor: Some(processor),
    })
}

pub fn start_udp_server<
    A: ToSocketAddrs + std::fmt::Display,
    P: AsRef<std::path::Path>,
>(
    listen_addr: A,
    storage_directory: P,
) -> std::io::Result<Server> {
    let db = match sled::open(&storage_directory) {
        Ok(db) => versioned_storage::VersionedStorage { db },
        Err(e) => {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "failed to open database at {:?}: {:?}",
                    storage_directory.as_ref(),
                    e
                ),
            ))
        }
    };

    let (process_task, net) = Net::new_udp(listen_addr)?;

    let processor = smol::Task::spawn(process_task);

    Ok(Server {
        net,
        db,
        processor: Some(processor),
    })
}
