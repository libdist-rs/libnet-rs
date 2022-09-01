use std::{net::SocketAddr, time::Duration};

use fnv::FnvHashMap;
use futures::future::try_join_all;
use tokio::time::sleep;

use crate::plaintcp::{tests::{listener, PeerId, receiver::{SendMsg, RecvMsg}}, TcpReliableSender};

#[tokio::test]
async fn send() {
    const N: usize = 1;
    // Create an address map
    let mut address_map = FnvHashMap::default();
    for i in 1..=N {
        address_map.insert(
            i as PeerId, 
            format!("127.0.0.1:{}", 6_200+i).parse::<SocketAddr>().unwrap(),
        );
    }

    // Run a TCP server.
    let address = format!("127.0.0.1:{}", 6_200+1).parse::<SocketAddr>().unwrap();
    let message = "Hello, world!".to_string();
    let handle = listener(address, message.clone());

    // Make the network sender and send the message.
    let mut sender = TcpReliableSender::<PeerId, SendMsg, RecvMsg>::with_peers(address_map);
    let cancel_handler = sender.send(1, message).await;

    // Ensure we get back an acknowledgement.
    assert!(cancel_handler.await.is_ok());

    // Ensure the server received the expected message (ie. it did not panic).
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
            format!("127.0.0.1:{}", 6_300+i).parse::<SocketAddr>().unwrap(),
        );
    }

    // Run N=3 TCP servers.
    let message = "Hello, world!".to_string();
    let (handles, peers): (Vec<_>, Vec<_>) = (1..=N)
        .map(|x| {
            let address = format!("127.0.0.1:{}", 6_300 + x)
                .parse::<SocketAddr>()
                .unwrap();
            (listener(address, message.to_string()), x as PeerId)
        })
        .collect::<Vec<_>>()
        .into_iter()
        .unzip();

    // Make the network sender and send the message.
    let mut sender = TcpReliableSender::<PeerId, SendMsg, RecvMsg>::with_peers(address_map);
    let cancel_handlers = sender.broadcast(&peers, message.clone()).await;

    // Ensure we get back an acknowledgement for each message.
    assert!(try_join_all(cancel_handlers).await.is_ok());

    // Ensure all servers received the broadcast.
    assert!(try_join_all(handles).await.is_ok());
}

#[tokio::test]
async fn retry() {
    const N: usize = 1;
    // Create an address map
    let mut address_map = FnvHashMap::default();
    for i in 1..=N {
        address_map.insert(
            i as PeerId, 
            format!("127.0.0.1:{}", 6_400+i).parse::<SocketAddr>().unwrap(),
        );
    }

    // Make the network sender and send the message  (no listeners are running).
    let address = format!("127.0.0.1:{}", 6_400+1).parse::<SocketAddr>().unwrap();
    let message = "Hello, world!".to_string();
    let mut sender = TcpReliableSender::<PeerId, SendMsg, RecvMsg>::with_peers(address_map);
    let cancel_handler = sender.send(1, message.clone()).await;

    // Run a TCP server.
    sleep(Duration::from_millis(50)).await;
    let handle = listener(address, message);

    // Ensure we get back an acknowledgement.
    assert!(cancel_handler.await.is_ok());

    // Ensure the server received the message (ie. it did not panic).
    assert!(handle.await.is_ok());
}
