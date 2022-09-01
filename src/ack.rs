use serde::{Serialize, Deserialize};

use crate::Message;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Acknowledgement {
    Pong,
}

impl Message for Acknowledgement {
    fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data)
            .expect("Failed to deserialize acknowledgement")
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self)
            .expect("Failed to serialize acknowledgement")
    }
}
