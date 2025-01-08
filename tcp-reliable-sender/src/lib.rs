use std::{fmt::Debug, marker::PhantomData, net::SocketAddr};
use bytes::Bytes;
use fnv::FnvHashMap;
use tokio::sync::{oneshot, mpsc::{UnboundedSender, unbounded_channel}};

mod connection;
use connection::*;

mod error;
pub use error::*;

/// This handler returns the message if the sender fails to send messages after retries
/// If successful, the received ack will be sent
/// Otherwise, if the handler is dropped by the holder, the message will be dropped if not sent already
pub type CancelHandler = oneshot::Receiver<Result<Bytes, SendError>>;

/// A convenient data structure for message passing between connections and the sender
#[derive(Debug)]
struct InnerMsg
{
    payload: Bytes,
    cancel_handler: oneshot::Sender<Result<Bytes, SendError>>,
}

pub struct TcpReliableSender<Id, SendMsg>
{
    address_map: FnvHashMap<Id, SocketAddr>,
    connections: FnvHashMap<Id, UnboundedSender<InnerMsg>>,
    _x: PhantomData<SendMsg>,
}

unsafe impl<Id, T> Send for TcpReliableSender<Id, T> where Id: Send {}
unsafe impl<Id, T> Sync for TcpReliableSender<Id, T> where Id: Sync {}

impl<Id, SendMsg> TcpReliableSender<Id, SendMsg>
{
    fn new() -> Self {
        Self {
            address_map: FnvHashMap::default(),
            connections: FnvHashMap::default(),
            _x: PhantomData,
        }
    }

    fn spawn_connection(address: SocketAddr) -> UnboundedSender<InnerMsg>
    {
        log::debug!("Spawning a new connection for {}", address);
        let (tx, rx) = unbounded_channel();
        Connection::spawn(address, rx);
        tx
    }
}

impl<Id, SendMsg> TcpReliableSender<Id, SendMsg>
where Id: Eq + std::hash::Hash,
{
    pub fn with_peers(peers: FnvHashMap<Id, SocketAddr>) -> Self
    {
        let mut sender = Self::new();
        for (id, peer) in peers {
            sender.address_map
                .insert(id, peer);
        }
        sender
    }
}

impl<Id, SendMsg> TcpReliableSender<Id, SendMsg>
where Id: Clone,
{
    /// Returns the (Id, Address) used in this sender
    pub fn get_peers(&self) -> FnvHashMap<Id, SocketAddr> {
        self.address_map.clone()
    }
}

impl<Id, SendMsg> TcpReliableSender<Id, SendMsg>
where Id: Debug + Eq + std::hash::Hash + Clone,
{
    /// Reliably send a message to a specific address.
    pub async fn send(&mut self, recipient: Id, data: Bytes) -> Result<CancelHandler, OpError<Id>>
    {
        log::debug!("Async Sending {:?} to {:?}", data, recipient);

        let (tx, rx) = oneshot::channel();
        let addr = match self.address_map
            .get(&recipient) {
                Some(addr) => *addr,
                None => { return Err(OpError::UnknownPeer(recipient)); }
            };
        let connection = self.connections.entry(recipient.clone())
        .or_insert_with(|| {
            Self::spawn_connection(addr)
        });

        if let Err(e) = connection
            .send(InnerMsg { payload: data, cancel_handler: tx })
        {
            log::error!("Net Send Error: {}", e);
            return Err(OpError::SendError(recipient));
        }

        Ok(rx)
    }
}

impl<Id, SendMsg> TcpReliableSender<Id, SendMsg>
where Id: Debug + Eq + std::hash::Hash + Clone,
{
    pub async fn broadcast(&mut self, recipients: &[Id], msg: Bytes) -> Vec<Result<CancelHandler, OpError<Id>>>
    {
        let mut handlers = Vec::with_capacity(recipients.len());
        for recipient in recipients {
            let handler = self.send(recipient.clone(), msg.clone()).await;
            handlers.push(handler);
        }
        handlers
    }
}
