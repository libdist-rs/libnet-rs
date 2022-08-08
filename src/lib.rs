mod traits;
pub use traits::*;

mod error;
pub use error::*;

mod codec;
pub use codec::*;

pub type NetResult<T> = std::result::Result<T, NetError>;

mod plaintcp;
mod udp;
mod tls;