use std::{fmt::Debug, marker::PhantomData, net::SocketAddr, sync::Arc};

use bytes::Bytes;
pub use common::UdpOptions;
use fnv::FnvHashMap;
use tokio::sync::{
    mpsc::{Sender, channel, error::TrySendError},
    oneshot,
};

mod socket_task;
use socket_task::*;

mod error;
pub use error::*;

/// This handler returns the ACK bytes if the receiver acknowledges the message.
/// If the handler is dropped by the holder, the message may be dropped if not sent already.
pub type CancelHandler = oneshot::Receiver<Result<Bytes, SendError>>;

/// Internal message type for communication between the sender API and socket task.
#[derive(Debug)]
struct InnerMsg {
    payload: Bytes,
    cancel_handler: oneshot::Sender<Result<Bytes, SendError>>,
}

pub struct UdpReliableSender<Id, SendMsg> {
    address_map: FnvHashMap<Id, SocketAddr>,
    connections: FnvHashMap<Id, Sender<InnerMsg>>,
    options: Arc<UdpOptions>,
    /// Whether the socket task has been spawned yet.
    task_spawned: bool,
    _x: PhantomData<SendMsg>,
}

unsafe impl<Id, T> Send for UdpReliableSender<Id, T> where Id: Send {}
unsafe impl<Id, T> Sync for UdpReliableSender<Id, T> where Id: Sync {}

impl<Id, SendMsg> UdpReliableSender<Id, SendMsg> {
    fn new(options: UdpOptions) -> Self {
        Self {
            address_map: FnvHashMap::default(),
            connections: FnvHashMap::default(),
            options: Arc::new(options),
            task_spawned: false,
            _x: PhantomData,
        }
    }
}

impl<Id, SendMsg> UdpReliableSender<Id, SendMsg>
where
    Id: Eq + std::hash::Hash,
{
    pub fn with_peers(peers: FnvHashMap<Id, SocketAddr>) -> Self {
        Self::with_peers_and_options(peers, UdpOptions::default())
    }

    pub fn with_peers_and_options(peers: FnvHashMap<Id, SocketAddr>, options: UdpOptions) -> Self {
        let mut sender = Self::new(options);
        for (id, peer) in peers {
            sender.address_map.insert(id, peer);
        }
        sender
    }
}

impl<Id, SendMsg> UdpReliableSender<Id, SendMsg>
where
    Id: Clone,
{
    /// Returns the (Id, Address) used in this sender
    pub fn get_peers(&self) -> FnvHashMap<Id, SocketAddr> {
        self.address_map.clone()
    }
}

impl<Id, SendMsg> UdpReliableSender<Id, SendMsg>
where
    Id: Debug + Eq + std::hash::Hash + Clone,
{
    /// Ensure the shared socket task is spawned and all peer channels exist.
    fn ensure_task_spawned(&mut self) {
        if self.task_spawned {
            return;
        }

        let mut peer_receivers = FnvHashMap::default();
        for (id, addr) in &self.address_map {
            let (tx, rx) = channel(self.options.channel_capacity);
            self.connections.insert(id.clone(), tx);
            peer_receivers.insert(*addr, rx);
        }

        SocketTask::spawn(peer_receivers, (*self.options).clone());
        self.task_spawned = true;
    }

    /// Reliably send a message to a specific peer.
    /// Returns a `CancelHandler` that resolves when the receiver acknowledges the message.
    pub async fn send(
        &mut self,
        recipient: Id,
        data: Bytes,
    ) -> Result<CancelHandler, OpError<Id>> {
        log::debug!("UDP reliable sending {:?} to {:?}", data, recipient);

        if !self.address_map.contains_key(&recipient) {
            return Err(OpError::UnknownPeer(recipient));
        }

        self.ensure_task_spawned();

        let (tx, rx) = oneshot::channel();
        let inner = InnerMsg {
            payload: data,
            cancel_handler: tx,
        };

        let connection = self.connections.get(&recipient).unwrap();
        match connection.try_send(inner) {
            Ok(()) => Ok(rx),
            Err(TrySendError::Full(inner)) => {
                if let Err(e) = connection.send(inner).await {
                    log::error!("Net Send Error: {}", e);
                    return Err(OpError::SendError(recipient));
                }
                Ok(rx)
            }
            Err(TrySendError::Closed(_)) => {
                log::error!("Net Send Error: channel closed");
                Err(OpError::SendError(recipient))
            }
        }
    }

    /// Send multiple messages to the same recipient, returning all cancel handlers at once.
    /// Messages are pipelined without waiting for individual ACKs.
    pub async fn send_many(
        &mut self,
        recipient: Id,
        messages: Vec<Bytes>,
    ) -> Result<Vec<CancelHandler>, OpError<Id>> {
        if !self.address_map.contains_key(&recipient) {
            return Err(OpError::UnknownPeer(recipient));
        }

        self.ensure_task_spawned();

        let connection = self.connections.get(&recipient).unwrap();
        let mut handlers = Vec::with_capacity(messages.len());

        for data in messages {
            let (tx, rx) = oneshot::channel();
            let inner = InnerMsg {
                payload: data,
                cancel_handler: tx,
            };
            match connection.try_send(inner) {
                Ok(()) => {}
                Err(TrySendError::Full(inner)) => {
                    if let Err(e) = connection.send(inner).await {
                        log::error!("Net Send Error: {}", e);
                        return Err(OpError::SendError(recipient));
                    }
                }
                Err(TrySendError::Closed(_)) => {
                    log::error!("Net Send Error: channel closed");
                    return Err(OpError::SendError(recipient));
                }
            }
            handlers.push(rx);
        }
        Ok(handlers)
    }

    pub async fn broadcast(
        &mut self,
        recipients: &[Id],
        msg: Bytes,
    ) -> Vec<Result<CancelHandler, OpError<Id>>> {
        let mut handlers = Vec::with_capacity(recipients.len());
        for recipient in recipients {
            let handler = self.send(recipient.clone(), msg.clone()).await;
            handlers.push(handler);
        }
        handlers
    }
}
