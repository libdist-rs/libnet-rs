use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use futures::SinkExt;
use tokio::{sync::mpsc::{UnboundedSender, unbounded_channel}, time::sleep, net::TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{Handler, Writer, Message, plaintcp::TcpReceiver};

#[derive(Clone)]
struct TestHandler {
    deliver: UnboundedSender<String>,
}

pub type SendMsg = String;
pub type RecvMsg = String;

impl Message for String {
    fn from_bytes(data: &[u8]) -> Self {
        bincode::deserialize(data)
            .expect("Failed to deserialize a string")
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self)
            .expect("Failed to serialize string")
    }
}

impl Handler<SendMsg, RecvMsg> for TestHandler {
    fn dispatch(&self, message: RecvMsg, writer: &mut Writer<SendMsg>) 
    {
        let response = "Ack".to_string();
        // Reply with an ACK.
        let _ = writer.send(response);

        // Deliver the message to the application.
        self.deliver
            .send(message)
            .expect("Failed to deliver message");
    }
}

#[tokio::test]
async fn receive() {
    // Make the network receiver.
    let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = unbounded_channel();
    TcpReceiver::spawn(address, TestHandler { deliver: tx });
    sleep(Duration::from_millis(50)).await;

    // Send a message.
    let sent = "Hello, world!".to_string();
    let bytes = Bytes::from(bincode::serialize(&sent).unwrap());
    let stream = TcpStream::connect(address).await.unwrap();
    let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
    transport.send(bytes.clone()).await.unwrap();

    // Ensure the message gets passed to the channel.
    let message = rx.recv().await;
    assert!(message.is_some());
    let received = message.unwrap();
    assert_eq!(received, sent);
}
