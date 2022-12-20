mod receiver;
mod sender;
mod reliable_sender;

use std::net::SocketAddr;
use bytes::Bytes;
use futures::{StreamExt, SinkExt};
use tokio::{task::JoinHandle, net::TcpListener};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use crate::{Message, Acknowledgement};

pub fn listener<RecvMsg>(address: SocketAddr, expected: RecvMsg) -> JoinHandle<()> 
where
    RecvMsg: Message,
{
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let expected = RecvMsg::to_bytes(&expected);
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        let response: Bytes = Acknowledgement::Pong.to_bytes().into();
        match reader.next().await {
            Some(Ok(received)) => {
                assert_eq!(received, expected);
                writer.send(response).await.unwrap()
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}

pub type PeerId = usize;