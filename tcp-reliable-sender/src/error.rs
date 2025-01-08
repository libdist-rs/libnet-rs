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
    #[error("Failed to send to {0}: {1}")]
    SendingFailed(SocketAddr, #[source] tokio::io::Error),

    #[error("Connection to {0} abandoned")]
    ChannelClosed(SocketAddr),

    #[error("Connection to {0} closed")]
    ConnectionClosed(SocketAddr),

    #[error("Received an unexpected Ack from {0}")]
    UnexpectedAck(SocketAddr),

    #[error("Received an unexpected Message from {0}")]
    ReadingFailed(SocketAddr, #[source] tokio::io::Error),
}
