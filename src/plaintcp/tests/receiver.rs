use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use futures::SinkExt;
use tokio::{sync::mpsc::{UnboundedSender, unbounded_channel}, time::sleep, net::TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::plaintcp::TcpReceiver;

pub type SendMsg = String;
pub type RecvMsg = String;

#[tokio::test]
async fn receive() {
    // Make the network receiver.
    let address = "127.0.0.1:4000".parse::<SocketAddr>().unwrap();
    let (tx, mut rx) = unbounded_channel();
    let receiver = TcpReceiver::spawn(address);
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
