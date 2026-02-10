use std::collections::VecDeque;
use std::future::poll_fn;
use std::io::IoSlice;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;

use bytes::{BufMut, Bytes, BytesMut};
use common::Options;
use futures::Stream;
use socket2::SockRef;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};


pub(crate) struct Connection {
    address: SocketAddr,
    receiver: UnboundedReceiver<Bytes>,
    options: Options,
}

#[derive(thiserror::Error, Debug)]
pub enum ConnectionError {
    #[error("Peer ({0}) connection initiation error: {1}")]
    ConnectionError(SocketAddr, #[source] tokio::io::Error),

    #[error("Connection to peer {0} closed")]
    ConnectionClosed(SocketAddr),

    #[error("Error reading messages from peer {0}")]
    ReadClosed(SocketAddr),

    #[error("Error reading messages from peer {0}: {1}")]
    ReadError(SocketAddr, #[source] tokio::io::Error),

    #[error("Error sending message to peer {0}: {1}")]
    WriteError(SocketAddr, #[source] tokio::io::Error),
}

/// Encode a message as a length-delimited frame: [4-byte BE length][payload].
/// Returns a single Bytes containing the header + payload with zero copy for the common case.
#[inline]
fn encode_frame(payload: Bytes) -> Bytes {
    let len = payload.len() as u32;
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    buf.put_u32(len);
    buf.extend_from_slice(&payload);
    buf.freeze()
}

/// Maximum number of IoSlices to pass to a single writev call.
/// Linux default UIO_MAXIOV is 1024; macOS IOV_MAX is 1024.
/// We use a conservative limit.
const MAX_IOVECS: usize = 64;

impl Connection {
    fn new(address: SocketAddr, receiver: UnboundedReceiver<Bytes>, options: Options) -> Self {
        Self { address, receiver, options }
    }

    pub(crate) fn spawn(
        address: SocketAddr,
        rx: UnboundedReceiver<Bytes>,
        options: Options,
    )
    {
        tokio::spawn(async move {
            if let Err(e) = Self::new(address, rx, options).run().await {
                log::error!("Error in connection to {}: {}", address, e);
            }
        });
    }

    async fn run(&mut self) -> Result<(), ConnectionError>
    {
        // Connect to the address
        let mut stream = TcpStream::connect(self.address).await
            .map_err(|e| {
                log::error!("Unable to connect to peer {} with error {}", self.address, e);
                ConnectionError::ConnectionError(self.address, e)
            })?;

        if self.options.tcp_nodelay {
            stream.set_nodelay(true).map_err(|e| ConnectionError::ConnectionError(self.address, e))?;
        }
        // Apply socket buffer tuning
        let sock_ref = SockRef::from(&stream);
        if let Some(size) = self.options.tcp_send_buffer {
            let _ = sock_ref.set_send_buffer_size(size);
        }
        if let Some(size) = self.options.tcp_recv_buffer {
            let _ = sock_ref.set_recv_buffer_size(size);
        }

        // Borrow-based split: zero overhead, no Arc, no Mutex
        let (rd, mut wr) = stream.split();

        let mut read_codec = LengthDelimitedCodec::new();
        read_codec.set_max_frame_length(self.options.max_frame_length);
        let mut reader = FramedRead::new(rd, read_codec);

        log::debug!("Connected to {}", self.address);

        let address = self.address;
        let receiver = &mut self.receiver;
        let backpressure_boundary = self.options.write_buffer_size;
        let batch_drain_cap = self.options.batch_drain_cap;

        // Write queue: encoded frames waiting to be written via writev
        let mut write_queue: VecDeque<Bytes> = VecDeque::with_capacity(128);
        // Byte offset into the first frame (for partial writes)
        let mut front_offset: usize = 0;
        // Total buffered bytes in write_queue
        let mut buffered: usize = 0;

        // Single poll_fn combining channel receive, vectored write, and response read
        poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Flush write queue via writev
                while !write_queue.is_empty() {
                    // Build IoSlice array from queued frames
                    let mut slices_buf: [IoSlice<'_>; MAX_IOVECS] = std::array::from_fn(|_| IoSlice::new(&[]));
                    let mut n_slices = 0;

                    for (i, frame) in write_queue.iter().enumerate() {
                        if i >= MAX_IOVECS { break; }
                        let slice = if i == 0 {
                            &frame[front_offset..]
                        } else {
                            &frame[..]
                        };
                        if slice.is_empty() { continue; }
                        slices_buf[n_slices] = IoSlice::new(slice);
                        n_slices += 1;
                    }

                    if n_slices == 0 { break; }

                    match Pin::new(&mut wr).poll_write_vectored(cx, &slices_buf[..n_slices]) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(ConnectionError::WriteError(
                                address,
                                std::io::Error::new(std::io::ErrorKind::WriteZero, "write returned 0"),
                            )));
                        }
                        Poll::Ready(Ok(n)) => {
                            progress = true;
                            buffered -= n;

                            // Advance through frames, removing fully written ones
                            let mut remaining = n;
                            while remaining > 0 {
                                let front = &write_queue[0];
                                let avail = front.len() - front_offset;
                                if remaining >= avail {
                                    remaining -= avail;
                                    write_queue.pop_front();
                                    front_offset = 0;
                                } else {
                                    front_offset += remaining;
                                    remaining = 0;
                                }
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Err(ConnectionError::WriteError(address, e)));
                        }
                        Poll::Pending => break,
                    }
                }

                // Phase 2: Flush the kernel buffer
                if write_queue.is_empty() && buffered == 0 {
                    // All data written, ensure kernel sends it
                    match Pin::new(&mut wr).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Err(ConnectionError::WriteError(address, e)));
                        }
                        Poll::Pending => {}
                    }
                }

                // Phase 3: Drain channel into write queue (respecting backpressure)
                if buffered < backpressure_boundary {
                    let mut drained = 0;
                    loop {
                        if drained >= batch_drain_cap || buffered >= backpressure_boundary {
                            break;
                        }
                        match receiver.poll_recv(cx) {
                            Poll::Ready(Some(msg)) => {
                                let frame = encode_frame(msg);
                                buffered += frame.len();
                                write_queue.push_back(frame);
                                drained += 1;
                                progress = true;
                            }
                            Poll::Ready(None) => {
                                log::debug!("Channel closed for {}, flushing remaining data", address);
                                // Channel closed — flush remaining data then terminate
                                // First drain the write queue
                                if !write_queue.is_empty() {
                                    // Let the outer loop flush the write queue, then we'll
                                    // detect channel-closed again on next iteration
                                    progress = true;
                                    break;
                                }
                                return match Pin::new(&mut wr).poll_shutdown(cx) {
                                    Poll::Ready(_) => Poll::Ready(Err(ConnectionError::ConnectionClosed(address))),
                                    Poll::Pending => Poll::Pending,
                                };
                            }
                            Poll::Pending => break,
                        }
                    }
                }

                // Phase 4: Read responses (drain peer responses)
                match Pin::new(&mut reader).poll_next(cx) {
                    Poll::Ready(Some(Ok(response))) => {
                        progress = true;
                        log::debug!("Received response from {}: {} bytes", address, response.len());
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Err(ConnectionError::ReadError(address, e)));
                    }
                    Poll::Ready(None) => {
                        return Poll::Ready(Err(ConnectionError::ReadClosed(address)));
                    }
                    Poll::Pending => {}
                }
            }
            Poll::Pending
        }).await
    }
}
