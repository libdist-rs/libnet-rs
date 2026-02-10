use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;

use bytes::Bytes;
use common::Options;
use futures::{Sink, Stream};
use socket2::SockRef;
use tokio::{net::TcpStream, sync::mpsc::UnboundedReceiver};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};


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
        let stream = TcpStream::connect(self.address).await
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
        let (rd, wr) = stream.into_split();

        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(self.options.max_frame_length);
        let mut reader = FramedRead::new(rd, codec);

        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(self.options.max_frame_length);
        let mut writer = FramedWrite::new(wr, codec);
        writer.set_backpressure_boundary(self.options.write_buffer_size);

        log::debug!("Connected to {}", self.address);

        let address = self.address;
        let receiver = &mut self.receiver;

        // Single poll_fn combining channel receive, write, flush, and response read
        poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Flush pending writes
                match Pin::new(&mut writer).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(ConnectionError::WriteError(address, e)));
                    }
                    Poll::Pending => {}
                }

                // Phase 2: Check write readiness + drain channel
                match Pin::new(&mut writer).poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        match receiver.poll_recv(cx) {
                            Poll::Ready(Some(msg)) => {
                                if let Err(e) = Pin::new(&mut writer).start_send(msg) {
                                    return Poll::Ready(Err(ConnectionError::WriteError(address, e)));
                                }
                                progress = true;
                            }
                            Poll::Ready(None) => {
                                log::debug!("Channel closed for {}, flushing remaining data", address);
                                // Channel closed — flush remaining data then terminate
                                return match Pin::new(&mut writer).poll_close(cx) {
                                    Poll::Ready(_) => Poll::Ready(Err(ConnectionError::ConnectionClosed(address))),
                                    Poll::Pending => Poll::Pending,
                                };
                            }
                            Poll::Pending => {}
                        }
                    }
                    Poll::Pending => {}
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(Err(ConnectionError::WriteError(address, e)));
                    }
                }

                // Phase 3: Read responses (drain peer responses)
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
