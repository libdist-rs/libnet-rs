use std::net::SocketAddr;

use bytes::Bytes;

#[derive(Debug, thiserror::Error)]
pub enum OpError<Id> {
    #[error("Unknown peer: {0:?}")]
    UnknownPeer(Id),

    #[error("Failed to send message to {0:?}")]
    SendError(Id),
}

#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("Failed to send message: {0:?}")]
    Send(Bytes),

    #[error("Failed to send message: {0}")]
    Io(#[source] std::io::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Failed to bind UDP socket: {0}")]
    BindError(#[source] std::io::Error),

    #[error("Socket state error: {0}")]
    SocketStateError(#[source] std::io::Error),

    #[error("Failed to send to {0}: {1}")]
    SendingFailed(SocketAddr, #[source] std::io::Error),

    #[error("All channels closed")]
    AllChannelsClosed,
}
