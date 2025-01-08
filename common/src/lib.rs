pub trait Message {
    type DeserializationError;
    /// Get the object from a byte array
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError>
    where
        Self: Sized;
}

#[cfg(feature = "ack")]
mod ack;
#[cfg(feature = "ack")]
pub use ack::*;
