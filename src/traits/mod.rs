use async_trait::async_trait;
use futures::Sink;
use serde::{Serialize, de::DeserializeOwned};

mod impl_msg;
pub use impl_msg::*;

pub trait Message: 
    Clone + 
    std::fmt::Debug + 
    Send + 
    Sync + 
    Serialize + 
    DeserializeOwned + 
    'static
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

#[async_trait]
/// Networking channel abstraction
pub trait NetSender<PeerId, SendMsg> 
where
    SendMsg: Message,
{
    /// This function sends message `msg` to `sender` asynchronously
    async fn send(&mut self, sender: PeerId, msg: SendMsg);

    /// This function sends `msg` to `sender` synchronously
    fn blocking_send(&mut self, sender: PeerId, msg: SendMsg);
    
    /// This function sends `msg` to all **known** nodes asynchronously.
    /// Messages sent to nodes that are currently disconnected will be sent once they are re-connected.
    async fn broadcast(&mut self, msg: SendMsg, peers: &[PeerId]);

    /// This function sends `msg` to all **known** nodes synchronously.
    /// Messages sent to nodes that are currently disconnected will be sent once they are re-connected.
    fn blocking_broadcast(&mut self, msg: SendMsg, peers: &[PeerId]);


    /// This function sends the message to a random subset of the peers
    /// Useful to synchronize, gossip, or request data from nodes
    async fn randcast(&mut self, msg: SendMsg, peers: Vec<PeerId>, subset_size: usize);

    /// This function sends the message to a random subset of the peers
    /// Useful to synchronize, gossip, or request data from nodes
    fn blocking_randcast(&mut self, msg: SendMsg, peers: Vec<PeerId>, subset_size: usize);
}

pub type Writer<SendMsg> = Box<dyn Sink<SendMsg, Error=std::io::Error> + Send + Unpin>;

#[async_trait]
pub trait Handler<SendMsg, RecvMsg>:
    Send + 
    Sync +
    Clone + 
    'static
where
    SendMsg: Message,
    RecvMsg: Message,
{
    /// The handler reacts to messages
    /// Needs to be implemented by the protocol
    async fn dispatch(&self, msg: RecvMsg, writer: &mut Writer<SendMsg>);
}

pub trait Identifier: 
    Message + 
    std::cmp::Eq + 
    std::cmp::PartialOrd +
    std::hash::Hash
{}
