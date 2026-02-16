#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("Error binding to address: {0}")]
    BindError(#[source] std::io::Error),

    #[error("Error initializing socket state: {0}")]
    SocketStateError(#[source] std::io::Error),
}
