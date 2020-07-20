use std::{
    collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration,
};

use async_channel::{unbounded, Sender};
use uuid::Uuid;

use crate::{network::ResponseHandle, Envelope, Message, Request, Response};

// TODO:
// 0. mapping from SocketAddr -> nats topic (kind of a hack, whatever)
//
// 1. nats::Subscriber that receives from our personal nats inbox
//   a. forwards Requests to self.request_inbox
//   b. responses to self.waiting_requests
//
// 2. self.send that sends messages to the nats inboxes of other nodes

#[derive(Debug)]
pub struct NatsNet {
    pub(crate) request_inbox: Sender<(SocketAddr, Uuid, Request)>,
    pub(crate) waiting_requests: HashMap<Uuid, Sender<Response>>,
}

impl NatsNet {
    pub(crate) async fn send(
        &mut self,
        to: SocketAddr,
        envelope: Envelope,
    ) -> io::Result<()> {
        todo!()
    }

    pub(crate) async fn request(
        &mut self,
        to: SocketAddr,
        uuid: Uuid,
        request: Request,
    ) -> io::Result<ResponseHandle> {
        let (tx, rx) = unbounded();
        let envelope = Envelope {
            uuid,
            message: Message::Request(request),
        };

        assert!(self.waiting_requests.insert(uuid, tx).is_none());

        self.send(to, envelope).await?;

        Ok(ResponseHandle(rx))
    }
}
