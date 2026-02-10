use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use common::Options;
use futures::Stream;
use socket2::SockRef;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

#[cfg(feature = "ack")]
use bytes::{Buf, BufMut, BytesMut};
#[cfg(feature = "ack")]
use tokio::io::AsyncWrite;
#[cfg(feature = "ack")]
use common::ACK_BYTES;

use super::ConnectionError;

pub(super) struct TcpReceiverJob {
    address: SocketAddr,
    tx_connection_to_receiver: UnboundedSender<Bytes>,
    options: Arc<Options>,
}

impl TcpReceiverJob {
    fn new(address: SocketAddr, tx_connection_to_receiver: UnboundedSender<Bytes>, options: Options) -> Self {
        Self {
            address,
            tx_connection_to_receiver,
            options: Arc::new(options),
        }
    }

    pub fn spawn(address: SocketAddr, options: Options) -> UnboundedReceiver<Bytes> {
        let (tx, rx_receiver) = unbounded_channel();
        let job = Self::new(address, tx, options);
        tokio::spawn(async move {
            if let Err(e) = job.task_loop().await {
                log::error!("TcpReceiverJob error: {:?}", e);
            }
        });
        rx_receiver
    }

    async fn task_loop(&self) -> Result<(), ConnectionError> {
        let listener = TcpListener::bind(&self.address).await.map_err(ConnectionError::BindError)?;
        log::debug!("TCP Receiver is listening on {}", self.address);
        loop {
            let result = listener.accept().await;
            if let Err(e) = result {
                log::error!("Listener error: {}", e);
                continue;
            }
            // Unwrap is okay because we checked for error
            let (sock, peer_addr) = result.unwrap();

            if self.options.tcp_nodelay {
                sock.set_nodelay(true).map_err(|e| ConnectionError::SetNoDelayError(self.address, e))?;
            }
            // Apply socket buffer tuning
            let sock_ref = SockRef::from(&sock);
            if let Some(size) = self.options.tcp_send_buffer {
                let _ = sock_ref.set_send_buffer_size(size);
            }
            if let Some(size) = self.options.tcp_recv_buffer {
                let _ = sock_ref.set_recv_buffer_size(size);
            }

            log::info!("Connected to {}", peer_addr);
            Self::spawn_runner(
                sock,
                peer_addr,
                self.tx_connection_to_receiver.clone(),
                self.options.clone(),
            );
        }
    }

    fn spawn_runner(socket: TcpStream, peer_address: SocketAddr, tx_connection: UnboundedSender<Bytes>, options: Arc<Options>)
    {
        tokio::spawn(async move {
            let mut socket = socket;

            // Borrow-based split: zero overhead, no Arc, no Mutex
            let (rd, wr) = socket.split();

            #[cfg(feature = "ack")]
            let mut wr = wr;
            #[cfg(not(feature = "ack"))]
            let _wr = wr;

            let mut read_codec = LengthDelimitedCodec::new();
            read_codec.set_max_frame_length(options.max_frame_length);
            let mut reader = FramedRead::new(rd, read_codec);

            // Contiguous write buffer for ACK frames
            #[cfg(feature = "ack")]
            let mut write_buf = BytesMut::with_capacity(512);

            let result: Result<(), std::io::Error> = poll_fn(|cx| {
                let mut progress = true;
                while progress {
                    progress = false;

                    // Phase 1: Flush ACK write buffer via poll_write
                    #[cfg(feature = "ack")]
                    while !write_buf.is_empty() {
                        match Pin::new(&mut wr).poll_write(cx, &write_buf) {
                            Poll::Ready(Ok(0)) => {
                                return Poll::Ready(Err(std::io::Error::new(
                                    std::io::ErrorKind::WriteZero,
                                    "write returned 0",
                                )));
                            }
                            Poll::Ready(Ok(n)) => {
                                progress = true;
                                write_buf.advance(n);
                            }
                            Poll::Ready(Err(e)) => {
                                return Poll::Ready(Err(e));
                            }
                            Poll::Pending => break,
                        }
                    }

                    // Reclaim capacity when fully drained
                    #[cfg(feature = "ack")]
                    if write_buf.is_empty() {
                        write_buf.clear();
                    }

                    // Phase 1b: Flush kernel buffer when write buffer is empty
                    #[cfg(feature = "ack")]
                    if write_buf.is_empty() {
                        match Pin::new(&mut wr).poll_flush(cx) {
                            Poll::Ready(Ok(())) => {}
                            Poll::Ready(Err(e)) => {
                                return Poll::Ready(Err(e));
                            }
                            Poll::Pending => {}
                        }
                    }

                    // Phase 2: Read incoming messages + encode ACK into write buffer
                    match Pin::new(&mut reader).poll_next(cx) {
                        Poll::Ready(Some(Ok(msg))) => {
                            progress = true;
                            if let Err(e) = tx_connection.send(msg.freeze()) {
                                log::warn!("Error sending message to receiver: {}", e);
                                return Poll::Ready(Ok(()));
                            }
                            #[cfg(feature = "ack")]
                            {
                                write_buf.put_u32(ACK_BYTES.len() as u32);
                                write_buf.extend_from_slice(ACK_BYTES);
                            }
                        }
                        Poll::Ready(Some(Err(e))) => {
                            log::error!("Error reading message for peer {}: {}", peer_address, e);
                            return Poll::Ready(Err(e));
                        }
                        Poll::Ready(None) => {
                            log::error!("Connection closed by peer {}", peer_address);
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => {}
                    }
                }
                Poll::Pending
            }).await;

            if let Err(e) = result {
                log::error!("Runner error for peer {}: {}", peer_address, e);
            }
        });
    }
}
