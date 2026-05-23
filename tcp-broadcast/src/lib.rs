//! TCP best-effort broadcast sender with first-class cancellation.
//!
//! Sits between `tcp-sender` (fire-and-forget, no observation, no cancel)
//! and `tcp-reliable-sender` (reliability with retries + acks).  Each
//! enqueued message returns a [`CancelHandle`] that (a) lets the caller
//! signal the per-peer worker to *skip* this message before it hits the
//! socket, and (b) carries a oneshot receiver that fires when the message
//! is handed off to TCP.
//!
//! The primary use case is BFT-style broadcast: send to all `n` peers, wait
//! until at least `n - t` have been handed off to TCP, then cancel the
//! rest.  See [`TcpBroadcastSender::broadcast_with_faults`].
//!
//! No retries on socket write error — the failing message is silently
//! dropped (its `delivered` oneshot never fires) and the worker reconnects
//! for the next message.  Use `tcp-reliable-sender` if you need retries.

use std::{
    fmt::Debug,
    marker::PhantomData,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use bytes::Bytes;
pub use common::Options;
use fnv::FnvHashMap;
use tokio::sync::oneshot;

#[cfg(feature = "unbounded")]
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
#[cfg(not(feature = "unbounded"))]
use tokio::sync::mpsc::{channel, error::TrySendError, Sender};

mod connection;
use connection::Connection;

/// One pending message in a per-peer worker queue.
///
/// Visible to the `connection` module via `pub(crate)`.
#[derive(Debug)]
pub(crate) struct Job {
    pub(crate) payload: Bytes,
    pub(crate) cancel: Arc<AtomicBool>,
    pub(crate) delivered: oneshot::Sender<()>,
}

#[cfg(feature = "unbounded")]
type ChannelSender = UnboundedSender<Job>;
#[cfg(not(feature = "unbounded"))]
type ChannelSender = Sender<Job>;

/// Handle returned by [`TcpBroadcastSender::send`].
///
/// - [`cancel`]: idempotently signals the per-peer worker to skip this
///   message if it has not already been written to TCP.  Always safe to
///   call; if the message was already sent, the flag is observed but
///   ignored.
/// - [`wait_delivered`]: awaits the oneshot that fires when the worker
///   has handed the message to TCP.  Returns `false` if the worker
///   dropped the job (channel closed, cancelled, or connection failure
///   without retry).
pub struct CancelHandle {
    pub(crate) cancel: Arc<AtomicBool>,
    pub(crate) delivered: oneshot::Receiver<()>,
}

impl CancelHandle {
    /// Signal the worker to skip this message if not already written.
    /// Idempotent.
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    /// Wait for the worker to hand the message off to TCP.  Returns
    /// `true` on delivery, `false` if the job was dropped.
    pub async fn wait_delivered(self) -> bool {
        self.delivered.await.is_ok()
    }

    /// Non-blocking check: did the worker already report delivery?
    /// Returns `false` if delivery has not happened yet OR if the job
    /// was dropped.
    pub fn is_delivered(&mut self) -> bool {
        matches!(self.delivered.try_recv(), Ok(()))
    }
}

/// TCP best-effort broadcast sender with cancellable per-message workers.
///
/// One persistent per-peer tokio task holds the TCP connection and drains
/// an mpsc of [`Job`]s.  Each enqueued job carries a cancel flag the
/// worker checks before writing the bytes — cancelled jobs are dropped
/// at the worker without touching the socket.
pub struct TcpBroadcastSender<Id, SendMsg> {
    address_map: FnvHashMap<Id, SocketAddr>,
    connections: FnvHashMap<Id, ChannelSender>,
    options: Arc<Options>,
    _x: PhantomData<SendMsg>,
}

// Same Send/Sync as the other libnet senders.  Id+PhantomData are the
// only generic state; SendMsg is phantom only.
unsafe impl<Id, T> Send for TcpBroadcastSender<Id, T> where Id: Send {}
unsafe impl<Id, T> Sync for TcpBroadcastSender<Id, T> where Id: Sync {}

impl<Id, SendMsg> TcpBroadcastSender<Id, SendMsg> {
    fn new(options: Options) -> Self {
        Self {
            address_map: FnvHashMap::default(),
            connections: FnvHashMap::default(),
            options: Arc::new(options),
            _x: PhantomData,
        }
    }

    #[cfg(feature = "unbounded")]
    fn spawn_connection(address: SocketAddr, options: &Arc<Options>) -> ChannelSender {
        let (tx, rx) = unbounded_channel();
        Connection::spawn(address, rx, (**options).clone());
        tx
    }

    #[cfg(not(feature = "unbounded"))]
    fn spawn_connection(address: SocketAddr, options: &Arc<Options>) -> ChannelSender {
        let (tx, rx) = channel(options.channel_capacity);
        Connection::spawn(address, rx, (**options).clone());
        tx
    }
}

impl<Id, SendMsg> TcpBroadcastSender<Id, SendMsg>
where
    Id: Eq + std::hash::Hash,
{
    pub fn with_peers(peers: FnvHashMap<Id, SocketAddr>) -> Self {
        Self::with_peers_and_options(peers, Options::default())
    }

    pub fn with_peers_and_options(
        peers: FnvHashMap<Id, SocketAddr>,
        options: Options,
    ) -> Self {
        let mut sender = Self::new(options);
        for (id, peer) in peers {
            sender.address_map.insert(id, peer);
        }
        sender
    }
}

impl<Id, SendMsg> TcpBroadcastSender<Id, SendMsg>
where
    Id: Clone + Debug + Eq + std::hash::Hash,
{
    /// Return a copy of the (Id, Address) peer map.
    pub fn get_peers(&self) -> FnvHashMap<Id, SocketAddr> {
        self.address_map.clone()
    }

    /// Enqueue `payload` for delivery to `peer`.  Returns a [`CancelHandle`]
    /// the caller can use to cancel the pending send or observe delivery.
    ///
    /// Returns `None` if `peer` is not in the address map, or — in bounded
    /// mode — the per-peer queue is full, or — in either mode — the
    /// per-peer channel is closed.  In bounded mode `None`-on-full is the
    /// best-effort drop semantic; callers that need to wait can retry.
    pub fn send(&mut self, peer: Id, payload: Bytes) -> Option<CancelHandle> {
        let address = *self.address_map.get(&peer)?;
        let options = &self.options;
        let conn = self
            .connections
            .entry(peer.clone())
            .or_insert_with(|| Self::spawn_connection(address, options));

        let cancel = Arc::new(AtomicBool::new(false));
        let (delivered_tx, delivered_rx) = oneshot::channel();
        let job = Job {
            payload,
            cancel: cancel.clone(),
            delivered: delivered_tx,
        };

        #[cfg(feature = "unbounded")]
        {
            if conn.send(job).is_err() {
                // Worker died — drop the stale sender and respawn for next time.
                self.connections.remove(&peer);
                return None;
            }
        }
        #[cfg(not(feature = "unbounded"))]
        {
            match conn.try_send(job) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    // Best-effort drop on full queue.
                    return None;
                }
                Err(TrySendError::Closed(_)) => {
                    self.connections.remove(&peer);
                    return None;
                }
            }
        }

        Some(CancelHandle {
            cancel,
            delivered: delivered_rx,
        })
    }

    /// Send `payload` to every peer in `peers`.  Return once at least
    /// `peers.len() - fault_threshold` peers have handed the message off
    /// to TCP.  Pending sends to slower peers are cancelled when this
    /// returns — their cancel flag is set so the worker will skip them
    /// when it pops them off the queue.
    ///
    /// Returns the number of confirmed deliveries.  Will be
    /// `>= peers.len() - fault_threshold` on success, or less if more
    /// than `fault_threshold` peers failed (unknown peer, full queue,
    /// or socket write error).
    ///
    /// CAVEAT: if more than `fault_threshold` peers' workers block
    /// forever (e.g., peer permanently unreachable AND `unbounded`
    /// feature off), this future will await indefinitely waiting for
    /// the (n - t)-th delivery that won't come.  Callers requiring a
    /// hard upper bound should wrap with `tokio::time::timeout`.
    pub async fn broadcast_with_faults(
        &mut self,
        peers: &[Id],
        payload: Bytes,
        fault_threshold: usize,
    ) -> usize {
        use futures::stream::{FuturesUnordered, StreamExt};

        // Phase 1: enqueue to every peer; collect cancel flags + delivery
        // futures.  Splitting the two halves of CancelHandle lets us await
        // deliveries while still being able to cancel after early return.
        let mut cancels: Vec<Arc<AtomicBool>> = Vec::with_capacity(peers.len());
        let mut futs: FuturesUnordered<_> = FuturesUnordered::new();
        for peer in peers {
            if let Some(h) = self.send(peer.clone(), payload.clone()) {
                cancels.push(Arc::clone(&h.cancel));
                let delivered = h.delivered;
                futs.push(async move { delivered.await.is_ok() });
            }
        }

        let needed = peers.len().saturating_sub(fault_threshold);
        let mut ok = 0usize;

        // Phase 2: await deliveries; break at quorum.
        while let Some(success) = futs.next().await {
            if success {
                ok += 1;
                if ok >= needed {
                    break;
                }
            }
        }

        // Phase 3: cancel everyone.  Idempotent for already-delivered jobs
        // (the flag is observed-but-ignored once the worker has written);
        // skips not-yet-written ones at the worker.
        for c in &cancels {
            c.store(true, Ordering::Relaxed);
        }

        ok
    }
}
