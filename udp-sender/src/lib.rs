use std::{fmt::Debug, net::SocketAddr, sync::atomic::{AtomicU32, Ordering}};

use bytes::Bytes;
pub use common::UdpOptions;
use common::fragment::fragment_message_contiguous;
use fnv::FnvHashMap;
use tokio::sync::mpsc::{Sender, channel, error::TrySendError};

mod socket_task;
use socket_task::*;

/// Channel item: (destination, contiguous buffer, segment_size).
/// segment_size=0 means single datagram (no GSO).
type ChannelItem = (SocketAddr, Bytes, usize);

pub struct UdpSimpleSender<Id, SendMsg> {
    address_map: FnvHashMap<Id, SocketAddr>,
    /// Single aggregated channel to the shared socket task.
    tx: Option<Sender<ChannelItem>>,
    options: UdpOptions,
    msg_counter: AtomicU32,
    _x: std::marker::PhantomData<SendMsg>,
}

#[derive(Debug, thiserror::Error)]
pub enum UdpSimpleSenderError {
    #[error("Error sending datagram to socket task: {0}")]
    ChannelSendError(#[source] tokio::sync::mpsc::error::SendError<ChannelItem>),

    #[error("Unknown peer")]
    UnknownPeer,

    #[error("Message too large to fragment: {0}")]
    FragmentError(#[from] common::fragment::FragmentError),
}

/// Safety: SendMsg is a phantom type; only Bytes are used internally.
unsafe impl<Id, SendMsg> Send for UdpSimpleSender<Id, SendMsg> where Id: Send {}
unsafe impl<Id, SendMsg> Sync for UdpSimpleSender<Id, SendMsg> where Id: Sync {}

impl<Id, SendMsg> UdpSimpleSender<Id, SendMsg>
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

    /// Returns the list of peers known to the sender.
    pub fn get_peers(&self) -> &FnvHashMap<Id, SocketAddr> {
        &self.address_map
    }
}

impl<Id, SendMsg> UdpSimpleSender<Id, SendMsg> {
    fn new(options: UdpOptions) -> Self {
        Self {
            address_map: FnvHashMap::default(),
            tx: None,
            options,
            msg_counter: AtomicU32::new(0),
            _x: std::marker::PhantomData,
        }
    }

    fn ensure_socket_task(&mut self) -> &Sender<ChannelItem> {
        if self.tx.is_none() {
            let (tx, rx) = channel(self.options.channel_capacity);
            SocketTask::spawn(rx, self.options.clone());
            self.tx = Some(tx);
        }
        self.tx.as_ref().unwrap()
    }

    fn next_msg_id(&self) -> u32 {
        self.msg_counter.fetch_add(1, Ordering::Relaxed)
    }
}

impl<Id, SendMsg> UdpSimpleSender<Id, SendMsg>
where
    Id: Debug + Eq + std::hash::Hash + Clone,
{
    pub async fn send(&mut self, peer: Id, msg: Bytes) -> Result<(), UdpSimpleSenderError> {
        log::debug!("UDP sending {:?} to {:?}", msg, peer);
        let address = match self.address_map.get(&peer) {
            Some(addr) => *addr,
            None => {
                log::warn!("Unknown peer {:?}", peer);
                return Err(UdpSimpleSenderError::UnknownPeer);
            }
        };

        let msg_id = self.next_msg_id();
        let batches =
            fragment_message_contiguous(msg_id, &msg, self.options.max_datagram_payload)?;

        // One channel send per GSO batch (usually one, more for large messages)
        let tx = self.ensure_socket_task();
        for (buf, segment_size) in batches {
            Self::send_item(tx, (address, buf, segment_size)).await?;
        }
        Ok(())
    }

    pub async fn broadcast(
        &mut self,
        peers: &[Id],
        msg: Bytes,
    ) -> Result<(), UdpSimpleSenderError> {
        for peer in peers {
            self.send(peer.clone(), msg.clone()).await?;
        }
        Ok(())
    }

    async fn send_item(
        tx: &Sender<ChannelItem>,
        item: ChannelItem,
    ) -> Result<(), UdpSimpleSenderError> {
        match tx.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                tx.send(item)
                    .await
                    .map_err(UdpSimpleSenderError::ChannelSendError)
            }
            Err(TrySendError::Closed(item)) => Err(UdpSimpleSenderError::ChannelSendError(
                tokio::sync::mpsc::error::SendError(item),
            )),
        }
    }
}
