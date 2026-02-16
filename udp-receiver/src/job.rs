use std::future::poll_fn;
use std::io::IoSliceMut;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use bytes::Bytes;
use common::UdpOptions;
use common::fragment::{FragmentHeader, FRAGMENT_HEADER_SIZE};
use common::reassembly::ReassemblyBuffer;
use quinn_udp::{RecvMeta, UdpSockRef, UdpSocketState, BATCH_SIZE};
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

#[cfg(feature = "ack")]
use bytes::{BufMut, BytesMut};
#[cfg(feature = "ack")]
use common::ACK_BYTES;
#[cfg(feature = "ack")]
use quinn_udp::Transmit;

use super::ConnectionError;

pub(super) struct UdpReceiverJob {
    address: SocketAddr,
    tx: UnboundedSender<(SocketAddr, Bytes)>,
    options: Arc<UdpOptions>,
}

impl UdpReceiverJob {
    pub fn spawn(address: SocketAddr, options: UdpOptions) -> UnboundedReceiver<(SocketAddr, Bytes)> {
        let (tx, rx) = unbounded_channel();
        let job = Self {
            address,
            tx,
            options: Arc::new(options),
        };
        tokio::spawn(async move {
            if let Err(e) = job.task_loop().await {
                log::error!("UdpReceiverJob error: {:?}", e);
            }
        });
        rx
    }

    async fn task_loop(&self) -> Result<(), ConnectionError> {
        let io = UdpSocket::bind(&self.address)
            .await
            .map_err(ConnectionError::BindError)?;
        log::debug!("UDP Receiver is listening on {}", self.address);

        let state = UdpSocketState::new(UdpSockRef::from(&io))
            .map_err(ConnectionError::SocketStateError)?;

        // Apply socket buffer tuning
        if let Some(size) = self.options.udp_send_buffer {
            let _ = state.set_send_buffer_size(UdpSockRef::from(&io), size);
        }
        if let Some(size) = self.options.udp_recv_buffer {
            let _ = state.set_recv_buffer_size(UdpSockRef::from(&io), size);
        }

        let gro_segments = state.gro_segments();
        let max_payload = self.options.max_datagram_payload;

        let mut reassembly = ReassemblyBuffer::new(
            self.options.reassembly_timeout,
            self.options.max_pending_reassemblies,
        );

        // Receive buffers: each buffer is large enough for GRO-coalesced datagrams
        let buf_size = max_payload * gro_segments;
        let mut recv_bufs: Vec<Vec<u8>> = (0..BATCH_SIZE).map(|_| vec![0u8; buf_size]).collect();
        let mut meta_buf = vec![RecvMeta::default(); BATCH_SIZE];

        // ACK outbound queue: (destination, ACK buffer, segment_size)
        #[cfg(feature = "ack")]
        let mut ack_outbound: Vec<(SocketAddr, Bytes, usize)> = Vec::new();

        let mut last_evict = Instant::now();
        let tx = &self.tx;

        poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1 (ACK only): Send queued ACK batches via GSO
                #[cfg(feature = "ack")]
                while !ack_outbound.is_empty() {
                    let (addr, data, seg_size) = &ack_outbound[0];
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
                            ack_outbound.swap_remove(0);
                            progress = true;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            let _ = io.poll_send_ready(cx);
                            break;
                        }
                        Err(e) => {
                            log::warn!("Failed to send ACK: {}", e);
                            ack_outbound.swap_remove(0);
                            progress = true;
                        }
                    }
                }

                // Phase 2: Receive datagrams via quinn-udp
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

                                // Handle GRO: iterate coalesced datagrams
                                let mut offset = 0;
                                while offset < total_len {
                                    let end = std::cmp::min(offset + stride, total_len);
                                    let datagram = &buf[offset..end];
                                    offset = end;

                                    Self::process_datagram(
                                        meta.addr,
                                        datagram,
                                        &mut reassembly,
                                        tx,
                                        #[cfg(feature = "ack")]
                                        &mut ack_outbound,
                                    );
                                }
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            let _ = io.poll_recv_ready(cx);
                        }
                        Err(e) => {
                            log::error!("UDP recv error: {}", e);
                        }
                    }
                }

                // Phase 3: Periodic reassembly eviction
                let now = Instant::now();
                if now.duration_since(last_evict).as_secs() >= 1 {
                    let evicted = reassembly.evict_expired();
                    if evicted > 0 {
                        log::debug!("Evicted {} expired reassembly entries", evicted);
                    }
                    last_evict = now;
                }
            }

            // Check if receiver is still alive
            if tx.is_closed() {
                return Poll::Ready(Ok(()));
            }

            Poll::Pending
        })
        .await
    }

    fn process_datagram(
        sender: SocketAddr,
        datagram: &[u8],
        reassembly: &mut ReassemblyBuffer,
        tx: &UnboundedSender<(SocketAddr, Bytes)>,
        #[cfg(feature = "ack")] ack_outbound: &mut Vec<(SocketAddr, Bytes, usize)>,
    ) {
        if datagram.len() < FRAGMENT_HEADER_SIZE {
            log::warn!(
                "Received datagram too short ({} bytes) from {}",
                datagram.len(),
                sender
            );
            return;
        }

        let header = match FragmentHeader::decode(datagram) {
            Ok(h) => h,
            Err(e) => {
                log::warn!("Invalid fragment header from {}: {}", sender, e);
                return;
            }
        };

        let payload = Bytes::copy_from_slice(&datagram[FRAGMENT_HEADER_SIZE..]);

        match reassembly.insert(sender, header, payload) {
            Ok(Some(complete_msg)) => {
                if let Err(e) = tx.send((sender, complete_msg)) {
                    log::warn!("Error forwarding message to receiver: {}", e);
                }

                // Queue ACK for complete message
                #[cfg(feature = "ack")]
                {
                    let mut ack_buf = BytesMut::with_capacity(4 + ACK_BYTES.len());
                    ack_buf.put_u32(header.msg_id);
                    ack_buf.extend_from_slice(ACK_BYTES);
                    ack_outbound.push((sender, ack_buf.freeze(), 0));
                }
            }
            Ok(None) => {
                // Fragment stored, waiting for more
            }
            Err(e) => {
                log::warn!("Reassembly error from {}: {}", sender, e);
            }
        }
    }
}
