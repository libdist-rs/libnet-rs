pub trait Message {
    type DeserializationError;
    /// Get the object from a byte array
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError>
    where
        Self: Sized;
}

mod options;
pub use options::Options;

mod udp_options;
pub use udp_options::UdpOptions;

pub mod fragment;
pub mod reassembly;

#[cfg(feature = "ack")]
mod ack;
#[cfg(feature = "ack")]
pub use ack::*;

#[cfg(feature = "tls")]
mod tls_options;
#[cfg(feature = "tls")]
pub use tls_options::{TlsOptions, PqProtocol, CertSource};

#[cfg(feature = "tls")]
pub mod tls_cert;

#[cfg(feature = "tls")]
pub mod tls_config;
