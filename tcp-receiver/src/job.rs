use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use bytes::Bytes;
use common::Options;
use futures::{Sink, Stream};
use socket2::SockRef;
use tokio::{net::{TcpListener, TcpStream}, sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

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
        let (rd, wr) = socket.into_split();

        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(options.max_frame_length);
        let mut reader = FramedRead::new(rd, codec);

        #[cfg(feature = "ack")]
        let mut writer = {
            let mut codec = LengthDelimitedCodec::new();
            codec.set_max_frame_length(options.max_frame_length);
            let mut w = FramedWrite::new(wr, codec);
            w.set_backpressure_boundary(options.write_buffer_size);
            w
        };
        #[cfg(not(feature = "ack"))]
        let _wr = wr;
        #[cfg(feature = "ack")]
        let ack_bytes = Bytes::from_static(ACK_BYTES);

        tokio::spawn(async move {
            let result: Result<(), std::io::Error> = poll_fn(|cx| {
                let mut progress = true;
                while progress {
                    progress = false;

                    // Phase 1: Flush pending ACK writes
                    #[cfg(feature = "ack")]
                    match Pin::new(&mut writer).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Err(e));
                        }
                        Poll::Pending => {}
                    }

                    // Phase 2: Read incoming messages + immediately buffer ACK
                    match Pin::new(&mut reader).poll_next(cx) {
                        Poll::Ready(Some(Ok(msg))) => {
                            progress = true;
                            if let Err(e) = tx_connection.send(msg.freeze()) {
                                log::warn!("Error sending message to receiver: {}", e);
                                return Poll::Ready(Ok(()));
                            }
                            #[cfg(feature = "ack")]
                            {
                                // Best-effort ACK — if writer not ready, it'll flush next iteration
                                if Pin::new(&mut writer).poll_ready(cx).is_ready() {
                                    if let Err(e) = Pin::new(&mut writer).start_send(ack_bytes.clone()) {
                                        log::error!("Error buffering ACK to peer {}: {}", peer_address, e);
                                        return Poll::Ready(Err(e));
                                    }
                                }
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
