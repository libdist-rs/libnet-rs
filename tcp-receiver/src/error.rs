use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Error binding to address: {0}")]
    BindError(#[source] std::io::Error),

    #[error("Error accepting connection from {0}: {1}")]
    AcceptError(SocketAddr, #[source] std::io::Error),

    #[error("Error setting nodelay for {0}")]
    SetNoDelayError(SocketAddr, #[source] std::io::Error),
}
