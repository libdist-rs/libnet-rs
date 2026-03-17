use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::Options;
use futures::Stream;
use socket2::SockRef;
use tokio::io::AsyncWrite;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

#[cfg(feature = "unbounded")]
type ChannelReceiver = tokio::sync::mpsc::UnboundedReceiver<Bytes>;
#[cfg(not(feature = "unbounded"))]
type ChannelReceiver = tokio::sync::mpsc::Receiver<Bytes>;


pub(crate) struct Connection {
    address: SocketAddr,
    receiver: ChannelReceiver,
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

impl Connection {
    fn new(address: SocketAddr, receiver: ChannelReceiver, options: Options) -> Self {
        Self { address, receiver, options }
    }

    pub(crate) fn spawn(
        address: SocketAddr,
        rx: ChannelReceiver,
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

        // Contiguous write buffer: frames encoded directly here, flushed via poll_write
        let mut write_buf = BytesMut::with_capacity(backpressure_boundary);

        // Single poll_fn combining channel receive, contiguous write, and response read
        poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Flush write buffer via poll_write
                while !write_buf.is_empty() {
                    match Pin::new(&mut wr).poll_write(cx, &write_buf) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(ConnectionError::WriteError(
                                address,
                                std::io::Error::new(std::io::ErrorKind::WriteZero, "write returned 0"),
                            )));
                        }
                        Poll::Ready(Ok(n)) => {
                            progress = true;
                            write_buf.advance(n);
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Err(ConnectionError::WriteError(address, e)));
                        }
                        Poll::Pending => break,
                    }
                }

                // Reclaim capacity when fully drained
                if write_buf.is_empty() {
                    write_buf.clear();
                }

                // Phase 2: Flush the kernel buffer
                if write_buf.is_empty() {
                    match Pin::new(&mut wr).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Err(ConnectionError::WriteError(address, e)));
                        }
                        Poll::Pending => {}
                    }
                }

                // Phase 3: Drain channel into write buffer (respecting backpressure)
                if write_buf.len() < backpressure_boundary {
                    let mut drained = 0;
                    loop {
                        if drained >= batch_drain_cap || write_buf.len() >= backpressure_boundary {
                            break;
                        }
                        match receiver.poll_recv(cx) {
                            Poll::Ready(Some(msg)) => {
                                write_buf.put_u32(msg.len() as u32);
                                write_buf.extend_from_slice(&msg);
                                drained += 1;
                                progress = true;
                            }
                            Poll::Ready(None) => {
                                log::debug!("Channel closed for {}, flushing remaining data", address);
                                if !write_buf.is_empty() {
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
