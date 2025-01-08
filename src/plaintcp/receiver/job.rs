use std::net::SocketAddr;

use bytes::Bytes;
use futures::StreamExt;
use tokio::{net::{TcpListener, TcpStream}, sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

#[cfg(feature = "ack")]
use crate::Acknowledgement;

use super::ConnectionError;

pub(super) struct TcpReceiverJob {
    address: SocketAddr,
    tx_connection_to_receiver: UnboundedSender<Bytes>,
}

impl TcpReceiverJob {
    fn new(address: SocketAddr, tx_connection_to_receiver: UnboundedSender<Bytes>) -> Self {
        Self {
            address,
            tx_connection_to_receiver,
        }
    }

    pub fn spawn(address: SocketAddr) -> UnboundedReceiver<Bytes> {
        let (tx, rx_receiver) = unbounded_channel();
        let job = Self::new(address, tx);
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
                return Err(ConnectionError::AcceptError(self.address, e));
            }
            // Unwrap is okay because we checked for error
            let (sock, peer_addr) = result.unwrap();

            log::info!("Connected to {}", peer_addr);
            Self::spawn_runner(
                sock,
                peer_addr,
                self.tx_connection_to_receiver.clone(),
            );
        }
    }

    fn spawn_runner(socket: TcpStream, peer_address: SocketAddr, tx_connection: UnboundedSender<Bytes>)
    {
        let mut reader = FramedRead::new(
            socket,
            LengthDelimitedCodec::new()
        );
        tokio::spawn(async move {
            loop {
                let msg_opt = reader.next().await;
                if msg_opt.is_none() {
                    log::warn!("Connection closed by peer");
                    return;
                }
                let msg_res = msg_opt.unwrap();
                if let Err(e) = msg_res {
                    log::warn!("Error reading message for peer {}: {}", peer_address, e);
                    return;
                }
                let msg = msg_res.unwrap();
                if let Err(e) = tx_connection.send(msg.freeze()) {
                    log::warn!("Error sending message to receiver: {}", e);
                    return;
                }
            }
        });
    }
}


