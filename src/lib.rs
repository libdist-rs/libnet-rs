mod traits;
pub use traits::*;

mod error;
pub use error::*;

mod codec;
pub use codec::*;

pub type NetResult<T> = anyhow::Result<T>;

#[cfg(feature = "tcp")]
pub mod plaintcp;
#[cfg(feature = "udp")]
pub mod udp;
#[cfg(feature = "tls")]
pub mod tls;