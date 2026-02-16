use std::cmp::min;
use std::collections::VecDeque;
use std::future::poll_fn;
use std::io::IoSliceMut;
use std::net::SocketAddr;
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::UdpOptions;
use common::fragment::fragment_message_contiguous;
use fnv::FnvHashMap;
use quinn_udp::{RecvMeta, Transmit, UdpSockRef, UdpSocketState, BATCH_SIZE};
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc::Receiver, oneshot};

use crate::{InnerMsg, SendError};

struct Waiter {
    delay: Duration,
    current: Duration,
    max_delay: Duration,
}

impl Waiter {
    fn new(delay: Duration, max_delay: Duration) -> Self {
        Self {
            delay,
            current: delay,
            max_delay,
        }
    }

    fn reset(&mut self) {
        self.current = self.delay;
    }

    fn backoff(&mut self) {
        self.current = min(2 * self.current, self.max_delay);
    }
}

struct PendingMsg {
    msg_id: u32,
    payload: Bytes,
    handler: oneshot::Sender<Result<Bytes, SendError>>,
    sent_at: Instant,
}

struct PeerState {
    rx: Receiver<InnerMsg>,
    pending: VecDeque<PendingMsg>,
    waiter: Waiter,
}

/// Outbound item: (destination, contiguous buffer, segment_size).
/// segment_size=0 means single datagram (no GSO).
type OutboundItem = (SocketAddr, Bytes, usize);

pub(crate) struct SocketTask {
    peers: FnvHashMap<SocketAddr, PeerState>,
    options: UdpOptions,
    msg_counter: u32,
}

impl SocketTask {
    pub(crate) fn spawn(
        peers: FnvHashMap<SocketAddr, Receiver<InnerMsg>>,
        options: UdpOptions,
    ) {
        let retry_initial = options.retry_initial_delay;
        let retry_max = options.retry_max_delay;
        let buffer_capacity = options.buffer_capacity;

        let peer_states = peers
            .into_iter()
            .map(|(addr, rx)| {
                let state = PeerState {
                    rx,
                    pending: VecDeque::with_capacity(buffer_capacity),
                    waiter: Waiter::new(retry_initial, retry_max),
                };
                (addr, state)
            })
            .collect();

        let task = Self {
            peers: peer_states,
            options,
            msg_counter: 0,
        };

        tokio::spawn(async move {
            if let Err(e) = task.run().await {
                log::error!("UDP reliable socket task error: {}", e);
            }
        });
    }

    async fn run(mut self) -> Result<(), crate::ConnectionError> {
        let io = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(crate::ConnectionError::BindError)?;

        let state = UdpSocketState::new(UdpSockRef::from(&io))
            .map_err(crate::ConnectionError::SocketStateError)?;

        // Apply socket buffer tuning
        if let Some(size) = self.options.udp_send_buffer {
            let _ = state.set_send_buffer_size(UdpSockRef::from(&io), size);
        }
        if let Some(size) = self.options.udp_recv_buffer {
            let _ = state.set_recv_buffer_size(UdpSockRef::from(&io), size);
        }

        log::debug!(
            "UDP reliable socket task bound to {}",
            io.local_addr().unwrap()
        );

        let batch_drain_cap = self.options.batch_drain_cap;
        let max_datagram_payload = self.options.max_datagram_payload;
        let peers = &mut self.peers;
        let msg_counter = &mut self.msg_counter;

        // Outbound queue with GSO support
        let mut outbound: Vec<OutboundItem> = Vec::with_capacity(batch_drain_cap);

        // Receive buffers for ACKs (ACK = msg_id:u32 + ACK_BYTE:u8 = 5 bytes)
        let ack_size = 5;
        let gro_segments = state.gro_segments();
        let recv_buf_size = ack_size * gro_segments;
        let mut recv_bufs: Vec<Vec<u8>> = (0..BATCH_SIZE)
            .map(|_| vec![0u8; recv_buf_size])
            .collect();
        let mut meta_buf = vec![RecvMeta::default(); BATCH_SIZE];

        poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Send queued outbound items via quinn-udp with GSO
                while !outbound.is_empty() {
                    let (addr, data, seg_size) = &outbound[0];
                    let transmit = Transmit {
                        destination: *addr,
                        ecn: None,
                        contents: data,
                        segment_size: if *seg_size > 0 { Some(*seg_size) } else { None },
                        src_ip: None,
                    };

                    match io.try_io(Interest::WRITABLE, || {
                        state.send(UdpSockRef::from(&io), &transmit)
                    }) {
                        Ok(()) => {
                            outbound.swap_remove(0);
                            progress = true;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            let _ = io.poll_send_ready(cx);
                            break;
                        }
                        Err(e) => {
                            log::error!("UDP reliable send error: {}", e);
                            outbound.swap_remove(0);
                            progress = true;
                        }
                    }
                }

                // Phase 2: Drain per-peer channels → fragment (contiguous) → enqueue
                let mut all_closed = true;
                for (addr, peer) in peers.iter_mut() {
                    let mut drained = 0;
                    loop {
                        if drained >= batch_drain_cap {
                            break;
                        }
                        match peer.rx.poll_recv(cx) {
                            Poll::Ready(Some(InnerMsg {
                                payload,
                                cancel_handler,
                            })) => {
                                let msg_id = *msg_counter;
                                *msg_counter = msg_counter.wrapping_add(1);

                                // Fragment into contiguous GSO batches
                                match fragment_message_contiguous(msg_id, &payload, max_datagram_payload) {
                                    Ok(batches) => {
                                        for (buf, seg_size) in batches {
                                            outbound.push((*addr, buf, seg_size));
                                        }
                                        peer.pending.push_back(PendingMsg {
                                            msg_id,
                                            payload,
                                            handler: cancel_handler,
                                            sent_at: Instant::now(),
                                        });
                                        peer.waiter.reset();
                                    }
                                    Err(e) => {
                                        log::error!("Fragment error: {}", e);
                                        let _ = cancel_handler.send(Err(SendError::Io(
                                            std::io::Error::new(
                                                std::io::ErrorKind::InvalidInput,
                                                e.to_string(),
                                            ),
                                        )));
                                    }
                                }
                                drained += 1;
                                progress = true;
                            }
                            Poll::Ready(None) => break,
                            Poll::Pending => {
                                all_closed = false;
                                break;
                            }
                        }
                    }
                    // If channel is not closed or has pending messages, not all closed
                    if !peer.pending.is_empty() {
                        all_closed = false;
                    }
                }

                if all_closed && outbound.is_empty() {
                    log::debug!("All channels closed and no pending messages, shutting down");
                    return Poll::Ready(Ok(()));
                }

                // Phase 3: Receive ACKs
                {
                    let mut io_slices: Vec<IoSliceMut<'_>> = recv_bufs
                        .iter_mut()
                        .map(|b| IoSliceMut::new(b))
                        .collect();

                    match io.try_io(Interest::READABLE, || {
                        state.recv(UdpSockRef::from(&io), &mut io_slices, &mut meta_buf)
                    }) {
                        Ok(count) => {
                            progress = true;
                            for i in 0..count {
                                let meta = &meta_buf[i];
                                let buf = &recv_bufs[i];
                                let stride = meta.stride;
                                let total_len = meta.len;

                                let mut offset = 0;
                                while offset < total_len {
                                    let end = std::cmp::min(offset + stride, total_len);
                                    let ack_data = &buf[offset..end];
                                    offset = end;

                                    if ack_data.len() >= 5 {
                                        let msg_id = u32::from_be_bytes([
                                            ack_data[0],
                                            ack_data[1],
                                            ack_data[2],
                                            ack_data[3],
                                        ]);
                                        Self::handle_ack(peers, meta.addr, msg_id);
                                    } else {
                                        log::warn!(
                                            "Received short ACK ({} bytes) from {}",
                                            ack_data.len(),
                                            meta.addr
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            let _ = io.poll_recv_ready(cx);
                        }
                        Err(e) => {
                            log::error!("UDP ACK recv error: {}", e);
                        }
                    }
                }

                // Phase 4: Check retry timers
                let now = Instant::now();
                for (addr, peer) in peers.iter_mut() {
                    let retry_delay = peer.waiter.current;
                    let mut i = 0;
                    while i < peer.pending.len() {
                        let pending = &peer.pending[i];
                        if pending.handler.is_closed() {
                            // Handler dropped, remove
                            peer.pending.remove(i);
                            continue;
                        }
                        if now.duration_since(pending.sent_at) >= retry_delay {
                            // Retry: re-fragment (contiguous) and re-enqueue
                            log::debug!(
                                "Retrying msg_id {} to {} (delay {:?})",
                                pending.msg_id,
                                addr,
                                retry_delay
                            );
                            if let Ok(batches) =
                                fragment_message_contiguous(pending.msg_id, &pending.payload, max_datagram_payload)
                            {
                                for (buf, seg_size) in batches {
                                    outbound.push((*addr, buf, seg_size));
                                }
                                // Update sent_at for next retry check
                                peer.pending[i].sent_at = now;
                                progress = true;
                            }
                            i += 1;
                        } else {
                            i += 1;
                        }
                    }
                    if progress {
                        peer.waiter.backoff();
                    }
                }
            }
            Poll::Pending
        })
        .await
    }

    fn handle_ack(peers: &mut FnvHashMap<SocketAddr, PeerState>, sender: SocketAddr, msg_id: u32) {
        if let Some(peer) = peers.get_mut(&sender) {
            // Find and remove the pending message with this msg_id
            if let Some(pos) = peer.pending.iter().position(|p| p.msg_id == msg_id) {
                let pending = peer.pending.remove(pos).unwrap();
                // Resolve the cancel handler with an ACK acknowledgement
                let ack_bytes = Bytes::from_static(common::ACK_BYTES);
                let _ = pending.handler.send(Ok(ack_bytes));
                log::debug!("ACK received for msg_id {} from {}", msg_id, sender);
                peer.waiter.reset();
            } else {
                log::warn!("Unexpected ACK for msg_id {} from {}", msg_id, sender);
            }
        } else {
            log::warn!("ACK from unknown peer {}", sender);
        }
    }
}
