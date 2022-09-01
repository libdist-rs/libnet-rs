use std::net::SocketAddr;
use fnv::FnvHashMap;
use futures::future::try_join_all;

use crate::{plaintcp::{tests::{listener, PeerId}, TcpSimpleSender}, NetSender};

#[tokio::test]
async fn simple_send() {
    let mut address_map = FnvHashMap::default();
    address_map.insert(
        1 as PeerId, 
        "127.0.0.1:6100".parse::<SocketAddr>().unwrap(),
    );

    // Run a TCP server for Id 1.
    let address = "127.0.0.1:6100".parse::<SocketAddr>().unwrap();
    let message = "Hello, world!".to_string();
    let handle = listener(address, message.clone());

    // Make the network sender and send the message.
    let mut sender = TcpSimpleSender::<PeerId, String, String>::with_peers(address_map.clone());
    sender.send(1 as PeerId, message).await;

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}

#[tokio::test]
async fn broadcast() {
    const N: usize = 3;
    // Create an address map
    let mut address_map = FnvHashMap::default();
    for i in 1..=N {
        address_map.insert(
            i as PeerId, 
            format!("127.0.0.1:{}", 6_000+i).parse::<SocketAddr>().unwrap(),
        );
    }
    let ids = (1..=N).collect::<Vec<_>>();

    // Run N=3 TCP servers.
    let message = "Hello, world!".to_string();
    let handles: Vec<_> = (1..=N)
        .map(|x| {
            let address = format!("127.0.0.1:{}", 6_000 + x)
                .parse::<SocketAddr>()
                .unwrap();
            listener(address, message.clone())
        })
        .collect::<Vec<_>>();

    // Make the network sender and send the message.
    let mut sender = TcpSimpleSender::<PeerId, String, String>::with_peers(address_map);
    sender.broadcast(message, ids.as_ref()).await;

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}
