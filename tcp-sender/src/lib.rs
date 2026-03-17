use std::{fmt::Debug, net::SocketAddr, sync::Arc};
use bytes::Bytes;
pub use common::Options;
use fnv::FnvHashMap;

#[cfg(feature = "unbounded")]
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};
#[cfg(not(feature = "unbounded"))]
use tokio::sync::mpsc::{Sender, channel, error::TrySendError};

mod connection;
use connection::*;

#[cfg(feature = "unbounded")]
type ChannelSender = UnboundedSender<Bytes>;
#[cfg(not(feature = "unbounded"))]
type ChannelSender = Sender<Bytes>;

pub struct TcpSimpleSender<Id, SendMsg>
{
    address_map: FnvHashMap<Id, SocketAddr>,
    connections: FnvHashMap<Id, ChannelSender>,
    options: Arc<Options>,
    _x: std::marker::PhantomData<SendMsg>,
}

#[derive(Debug, thiserror::Error)]
pub enum TcpSimpleSenderError {
    #[error("Error sending message to peer: {0}")]
    ConnectionSendError(String),

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

    #[cfg(feature = "unbounded")]
    fn spawn_connection(address: SocketAddr, options: &Arc<Options>) -> ChannelSender
    {
        let (tx, rx) = unbounded_channel();
        Connection::spawn(address, rx, (**options).clone());
        tx
    }

    #[cfg(not(feature = "unbounded"))]
    fn spawn_connection(address: SocketAddr, options: &Arc<Options>) -> ChannelSender
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
            .or_insert_with(|| Self::spawn_connection(*address, options));

        #[cfg(feature = "unbounded")]
        {
            if let Err(e) = conn.send(msg) {
                // Channel closed — remove stale connection and retry
                self.connections.remove(&sender);
                let conn = Self::spawn_connection(*address, &self.options);
                if let Err(e) = conn.send(e.0) {
                    return Err(TcpSimpleSenderError::ConnectionSendError(e.to_string()));
                }
                self.connections.insert(sender, conn);
            }
            Ok(())
        }
        #[cfg(not(feature = "unbounded"))]
        {
            match conn.try_send(msg) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(msg)) => {
                    if let Err(e) = conn.send(msg).await {
                        return Err(TcpSimpleSenderError::ConnectionSendError(e.to_string()));
                    }
                    return Ok(());
                }
                Err(TrySendError::Closed(msg)) => {
                    // Stale connection — remove and retry with a new one
                    self.connections.remove(&sender);
                    let conn = Self::spawn_connection(*address, &self.options);
                    if let Err(e) = conn.send(msg).await {
                        return Err(TcpSimpleSenderError::ConnectionSendError(e.to_string()));
                    }
                    self.connections.insert(sender, conn);
                    Ok(())
                }
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
