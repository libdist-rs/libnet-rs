mod traits;
pub use traits::*;

mod error;
pub use error::*;

mod ack;
pub use ack::*;

pub type NetResult<T> = Result<T, NetError>;

#[cfg(feature = "tcp")]
pub mod plaintcp;
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "tls")]
pub mod tls;