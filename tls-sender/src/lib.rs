use std::{fmt::Debug, net::SocketAddr, sync::Arc};
use bytes::Bytes;
pub use common::TlsOptions;
use fnv::FnvHashMap;
use tokio::sync::mpsc::{Sender, channel, error::TrySendError};

mod connection;
use connection::*;

pub struct TlsSimpleSender<Id, SendMsg>
{
    address_map: FnvHashMap<Id, SocketAddr>,
    connections: FnvHashMap<Id, Sender<Bytes>>,
    options: Arc<TlsOptions>,
    tls_config: Arc<rustls::ClientConfig>,
    _x: std::marker::PhantomData<SendMsg>,
}

#[derive(Debug, thiserror::Error)]
pub enum TlsSimpleSenderError {
    #[error("Error sending message to peer: {0}")]
    ConnectionSendError(#[source] tokio::sync::mpsc::error::SendError<Bytes>),

    #[error("Unknown peer")]
    UnknownPeer,

    #[error("TLS configuration error: {0}")]
    TlsConfigError(#[from] common::tls_cert::TlsCertError),
}

/// This is safe because the information about SendMsg and RecvMsg are erased and only bytes are used internally
unsafe impl<Id, SendMsg> Send for TlsSimpleSender<Id, SendMsg>  where Id: Send {}
unsafe impl<Id, SendMsg> Sync for TlsSimpleSender<Id, SendMsg>  where Id: Sync {}

impl<Id, SendMsg> TlsSimpleSender<Id, SendMsg>
    where Id: Eq + std::hash::Hash
{
    pub fn with_peers(peers: FnvHashMap<Id, SocketAddr>) -> Result<Self, TlsSimpleSenderError>
    {
        Self::with_peers_and_options(peers, TlsOptions::default())
    }

    pub fn with_peers_and_options(peers: FnvHashMap<Id, SocketAddr>, options: TlsOptions) -> Result<Self, TlsSimpleSenderError>
    {
        let mut sender = Self::new(options)?;
        for (id, peer) in peers {
            sender.address_map
                .insert(id, peer);
        }
        Ok(sender)
    }

    /// Returns the list of peers known to the connection
    pub fn get_peers(&self) -> &FnvHashMap<Id, SocketAddr> {
        &self.address_map
    }
}

impl<Id, SendMsg> TlsSimpleSender<Id, SendMsg>
{
    fn new(options: TlsOptions) -> Result<Self, TlsSimpleSenderError> {
        let tls_config = common::tls_config::build_client_config(&options)?;
        Ok(Self {
            address_map: FnvHashMap::default(),
            connections: FnvHashMap::default(),
            options: Arc::new(options),
            tls_config,
            _x: std::marker::PhantomData,
        })
    }

    fn spawn_connection(address: SocketAddr, options: &Arc<TlsOptions>, tls_config: &Arc<rustls::ClientConfig>) -> Sender<Bytes>
    {
        let (tx, rx) = channel(options.channel_capacity);
        Connection::spawn(address, rx, (**options).clone(), tls_config.clone());
        tx
    }
}

impl<Id, SendMsg> TlsSimpleSender<Id, SendMsg>
where Id: Debug + Eq + std::hash::Hash + Clone,
{
    pub async fn send(&mut self, sender: Id, msg: Bytes) -> Result<(), TlsSimpleSenderError> {
        log::debug!("Async Sending {:?} to {:?}", msg, sender);
        let addr_opt = self.address_map.get(&sender);
        if addr_opt.is_none() {
            log::warn!("Unknown peer {:?}", sender);
            return Err(TlsSimpleSenderError::UnknownPeer);
        }
        let address = addr_opt.unwrap();

        let options = &self.options;
        let tls_config = &self.tls_config;
        let conn = self
            .connections
            .entry(sender.clone())
            .or_insert_with(|| Self::spawn_connection(*address, options, tls_config));

        match conn.try_send(msg) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(msg)) => {
                if let Err(e) = conn.send(msg).await {
                    return Err(TlsSimpleSenderError::ConnectionSendError(e));
                }
                return Ok(());
            }
            Err(TrySendError::Closed(msg)) => {
                self.connections.remove(&sender);
                let conn = Self::spawn_connection(*address, &self.options, &self.tls_config);
                if let Err(e) = conn.send(msg).await {
                    return Err(TlsSimpleSenderError::ConnectionSendError(e));
                }
                self.connections.insert(sender, conn);
                Ok(())
            }
        }
    }

    pub async fn broadcast(&mut self, peers: &[Id], msg: Bytes) -> Result<(), TlsSimpleSenderError>
    {
        for peer in peers {
            self.send(peer.clone(), msg.clone()).await?;
        }
        Ok(())
    }
}
