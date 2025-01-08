use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use futures::{StreamExt, SinkExt};
use tokio::{net::{tcp::OwnedWriteHalf, TcpStream}, sync::mpsc::UnboundedReceiver};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};


pub(crate) struct Connection {
    address: SocketAddr,
    receiver: UnboundedReceiver<Bytes>,
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
    fn new(address: SocketAddr, receiver: UnboundedReceiver<Bytes>) -> Self {
        Self { address, receiver}
    }

    pub(crate) fn spawn(
        address: SocketAddr, 
        rx: UnboundedReceiver<Bytes>,
    ) 
    {
        tokio::spawn(async move {
            Self::new(address, rx).run().await;
        });
    }

    async fn run(&mut self) -> Result<(), ConnectionError>
    {
        // Connect to the address
        let stream_opt = TcpStream::connect(self.address).await;
        if let Err(e) = stream_opt {
            log::error!(
                "Unable to connect to peer {} with error {}", 
                self.address, 
                e,
            );
            return Err(ConnectionError::ConnectionError(self.address, e));
        }

        let stream = stream_opt.unwrap();
        let (rd, wr) = stream.into_split();
        let mut reader = FramedRead::new(rd, LengthDelimitedCodec::new());
        let mut writer = FramedWrite::new(wr, LengthDelimitedCodec::new());

        log::debug!("Connected to {}", self.address);

        // Main sender loop 
        loop {
            tokio::select! {
                // The outside world asked me to send a message
                msg_opt = self.receiver.recv() => {
                    if msg_opt.is_none() {
                        log::error!("Connection to {} closed", self.address);
                        log::error!("Shutting down connection to {}", self.address);
                        return Err(ConnectionError::ConnectionClosed(self.address));
                    }
                    let msg = msg_opt.unwrap();
                    self.handle_msg(&mut writer, msg).await?;
                },
                // The connection sent some response
                response_opt = reader.next() => {
                    if response_opt.is_none() {
                        log::error!("Error reading messages from {}", self.address);
                        return Err(ConnectionError::ReadClosed(self.address));
                    }
                    let response = response_opt.unwrap();
                    self.handle_response(response).await?;
                }
            }
        }
    }

    async fn handle_msg(&mut self, writer: &mut FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>, msg: Bytes) -> Result<(), ConnectionError> {
        // Send the message to the peer
        let result = writer.send(msg).await;
        if let Err(e) = result {
            return Err(ConnectionError::WriteError(self.address, e));
        }
        Ok(())
    }

    async fn handle_response(&mut self, response: Result<BytesMut, std::io::Error>) -> Result<(), ConnectionError> {
        if let Err(e) = response {
            return Err(ConnectionError::ReadError(self.address, e));
        }
        // We drop the response for now
        log::debug!("Received response from {}: {:?}", self.address, response);
        Ok(())
    }
}

