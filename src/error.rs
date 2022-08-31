use std::net::SocketAddr;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NetError {
    #[error("Did not receive acknowledgement from {0}")]
    NoAck(SocketAddr),
    #[error("Unexpected acknowledgement from {0}")]
    UnexpectedAck(SocketAddr),
    #[error("Failed to send a message to {0} with error {1}")]
    SendingFailed(SocketAddr, std::io::Error),
}