use std::future::poll_fn;
use std::net::SocketAddr;
use std::task::Poll;

use bytes::Bytes;
use common::UdpOptions;
use quinn_udp::{Transmit, UdpSockRef, UdpSocketState};
use tokio::io::Interest;
use tokio::net::UdpSocket;
use tokio::sync::mpsc::Receiver;

/// (destination, contiguous buffer, segment_size)
type ChannelItem = (SocketAddr, Bytes, usize);

pub(crate) struct SocketTask {
    receiver: Receiver<ChannelItem>,
    options: UdpOptions,
}

#[derive(thiserror::Error, Debug)]
pub enum SocketTaskError {
    #[error("Failed to bind UDP socket: {0}")]
    BindError(#[source] std::io::Error),
}

impl SocketTask {
    pub(crate) fn spawn(rx: Receiver<ChannelItem>, options: UdpOptions) {
        let task = Self {
            receiver: rx,
            options,
        };
        tokio::spawn(async move {
            if let Err(e) = task.run().await {
                log::error!("UDP socket task error: {}", e);
            }
        });
    }

    async fn run(mut self) -> Result<(), SocketTaskError> {
        let io = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(SocketTaskError::BindError)?;

        let state = UdpSocketState::new(UdpSockRef::from(&io))
            .map_err(SocketTaskError::BindError)?;

        // Apply socket buffer tuning
        if let Some(size) = self.options.udp_send_buffer {
            let _ = state.set_send_buffer_size(UdpSockRef::from(&io), size);
        }
        if let Some(size) = self.options.udp_recv_buffer {
            let _ = state.set_recv_buffer_size(UdpSockRef::from(&io), size);
        }

        log::debug!("UDP socket task bound to {}", io.local_addr().unwrap());

        let receiver = &mut self.receiver;
        let batch_drain_cap = self.options.batch_drain_cap;

        // Outbound queue: (destination, contiguous buffer, segment_size)
        let mut outbound: Vec<ChannelItem> = Vec::with_capacity(batch_drain_cap);

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
                            log::error!("UDP send error: {}", e);
                            outbound.swap_remove(0);
                            progress = true;
                        }
                    }
                }

                // Phase 2: Drain channel into outbound queue
                let mut drained = 0;
                loop {
                    if drained >= batch_drain_cap {
                        break;
                    }
                    match receiver.poll_recv(cx) {
                        Poll::Ready(Some(item)) => {
                            outbound.push(item);
                            drained += 1;
                            progress = true;
                        }
                        Poll::Ready(None) => {
                            log::debug!("UDP sender channel closed, flushing remaining datagrams");
                            if !outbound.is_empty() {
                                progress = true;
                                break;
                            }
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => break,
                    }
                }
            }
            Poll::Pending
        })
        .await
    }
}
