pub trait Message {
    type DeserializationError;
    /// Get the object from a byte array
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError>
    where
        Self: Sized;
}

mod options;
pub use options::Options;

#[cfg(feature = "ack")]
mod ack;
#[cfg(feature = "ack")]
pub use ack::*;
