use std::{fmt::Debug,net::SocketAddr};
use bytes::Bytes;
use fnv::FnvHashMap;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

mod connection;
use connection::*;

#[derive(Debug)]
pub struct TcpSimpleSender<Id, SendMsg>
{
    address_map: FnvHashMap<Id, SocketAddr>,
    connections: FnvHashMap<Id, UnboundedSender<Bytes>>,
    _x: std::marker::PhantomData<SendMsg>,
}

#[derive(Debug, thiserror::Error)]
pub enum TcpSimpleSenderError {
    #[error("Error sending message to peer: {0}")]
    ConnectionSendError(#[source] tokio::sync::mpsc::error::SendError<Bytes>),

    #[error("Unknown peer")]
    UnknownPeer,
}

/// This is safe because the information about SendMsg and RecvMsg are erased and only bytes are used internally
unsafe impl<Id, SendMsg> Send for TcpSimpleSender<Id, SendMsg>  where Id: Send {}
unsafe impl<Id, SendMsg> Sync for TcpSimpleSender<Id, SendMsg>  where Id: Sync {}

impl<Id, SendMsg> TcpSimpleSender<Id, SendMsg>
    where Id: Eq + std::hash::Hash
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

    /// Returns the list of peers known to the connection
    pub fn get_peers(&self) -> &FnvHashMap<Id, SocketAddr> {
        &self.address_map
    }
}

impl<Id, SendMsg> TcpSimpleSender<Id, SendMsg>
{
    fn new() -> Self {
        Self {
            address_map: FnvHashMap::default(),
            connections: FnvHashMap::default(),
            _x: std::marker::PhantomData,
        }
    }
}

impl<Id, SendMsg> TcpSimpleSender<Id, SendMsg>
{
    fn spawn_connection(address: SocketAddr) -> UnboundedSender<Bytes>
    {
        let (tx,rx) = unbounded_channel();
        Connection::spawn(address, rx);
        tx
    }
}

impl<Id, SendMsg> TcpSimpleSender<Id, SendMsg>
where Id: Debug + Eq + std::hash::Hash + Clone,
{
    pub async fn send(&mut self, sender: Id, msg: Bytes) -> Result<(), TcpSimpleSenderError> {
        log::debug!("Async Sending {:?} to {:?}", msg, sender);
        let addr_opt = self.address_map.get(&sender);
        if addr_opt.is_none() {
            log::warn!("Unknown peer {:?}", sender);
            return Err(TcpSimpleSenderError::UnknownPeer);
        }
        let address = addr_opt.unwrap();

        let conn = self
            .connections
            .entry(sender.clone())
            // We got lucky since we have already connected to this node
            .or_insert_with(|| Self::spawn_connection(*address));

        let msg = if let Err(e) = conn.send(msg) {
            e.0
        } else {
            return Ok(());
        };

        // We have a stale connection
        // Remove it
        self.connections
            .remove(&sender);
        // Try a new connection
        let conn = Self::spawn_connection(*address);
        if let Err(e) = conn.send(msg) {
            return Err(TcpSimpleSenderError::ConnectionSendError(e));
        }
        self.connections
            .insert(sender, conn);

        // A simple sender will give up at this point
        Ok(())
    }

    pub async fn broadcast(&mut self, peers: &[Id], msg: Bytes) -> Result<(), TcpSimpleSenderError>
    {
        for peer in peers {
            self.send(peer.clone(), msg.clone()).await?;
        }
        Ok(())
    }
}
