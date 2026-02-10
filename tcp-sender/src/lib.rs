use std::{fmt::Debug, net::SocketAddr, sync::Arc};
use bytes::Bytes;
pub use common::Options;
use fnv::FnvHashMap;
use tokio::sync::mpsc::{Sender, channel, error::TrySendError};

mod connection;
use connection::*;

pub struct TcpSimpleSender<Id, SendMsg>
{
    address_map: FnvHashMap<Id, SocketAddr>,
    connections: FnvHashMap<Id, Sender<Bytes>>,
    options: Arc<Options>,
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
        Self::with_peers_and_options(peers, Options::default())
    }

    pub fn with_peers_and_options(peers: FnvHashMap<Id, SocketAddr>, options: Options) -> Self
    {
        let mut sender = Self::new(options);
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
    fn new(options: Options) -> Self {
        Self {
            address_map: FnvHashMap::default(),
            connections: FnvHashMap::default(),
            options: Arc::new(options),
            _x: std::marker::PhantomData,
        }
    }

    fn spawn_connection(address: SocketAddr, options: &Arc<Options>) -> Sender<Bytes>
    {
        let (tx, rx) = channel(options.channel_capacity);
        Connection::spawn(address, rx, (**options).clone());
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

        let options = &self.options;
        let conn = self
            .connections
            .entry(sender.clone())
            // We got lucky since we have already connected to this node
            .or_insert_with(|| Self::spawn_connection(*address, options));

        // Fast path: try_send is synchronous (no yield) when there's capacity
        match conn.try_send(msg) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(msg)) => {
                // Channel full — await backpressure
                if let Err(e) = conn.send(msg).await {
                    return Err(TcpSimpleSenderError::ConnectionSendError(e));
                }
                return Ok(());
            }
            Err(TrySendError::Closed(msg)) => {
                // Stale connection — remove and retry with a new one
                self.connections.remove(&sender);
                let conn = Self::spawn_connection(*address, &self.options);
                if let Err(e) = conn.send(msg).await {
                    return Err(TcpSimpleSenderError::ConnectionSendError(e));
                }
                self.connections.insert(sender, conn);
                Ok(())
            }
        }
    }

    pub async fn broadcast(&mut self, peers: &[Id], msg: Bytes) -> Result<(), TcpSimpleSenderError>
    {
        for peer in peers {
            self.send(peer.clone(), msg.clone()).await?;
        }
        Ok(())
    }
}
