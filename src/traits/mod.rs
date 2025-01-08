use std::fmt::Debug;
use serde::{Serialize, de::DeserializeOwned};

pub trait Message: 
    Debug +
    Serialize +
    DeserializeOwned + 
{
    /// How to decode from bytes
    /// Default implementation uses bincode
    fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data)
            .expect("Deserialization failed")
    }

    // How to encode self to bytes
    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self)
            .expect("Serialization failed")
    }
}

pub trait Identifier: 
    Debug +
    Clone + 
    Send +
    Sync +
    std::cmp::Eq + 
    std::cmp::PartialOrd +
    std::hash::Hash
{}

impl<T> Identifier for T 
where 
    T: Clone + 
    Debug +
    Send +
    Sync +
    std::cmp::Eq + 
    std::cmp::PartialOrd +
    std::hash::Hash
{}
