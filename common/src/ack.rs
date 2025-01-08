use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Ack(u8);

pub const ACKNOWLEDGEMENT: Ack = Ack(0);

impl Ack {
    pub fn into_bytes(&self) -> Vec<u8> {
        self.0.to_be_bytes().to_vec()
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        Self(data[0].to_be())
    }
}
