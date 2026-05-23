//! Smoke tests for tcp-broadcast.
//!
//! Each test spins up N tokio TCP listeners on loopback, optionally kills
//! a subset, and exercises `broadcast_with_faults` against the surviving
//! address map.  All listeners drain their connection silently so the
//! sender's per-peer worker always succeeds writing.

use std::{net::SocketAddr, time::Duration};

use bytes::Bytes;
use fnv::FnvHashMap;
use tcp_broadcast::TcpBroadcastSender;
use tokio::{io::AsyncReadExt, net::TcpListener};

async fn spawn_drain_listener() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                // Drain until EOF.
                while stream.read(&mut buf).await.unwrap_or(0) > 0 {}
            });
        }
    });
    addr
}

/// Pick a port that NO listener is bound to, simulating a dead peer.
fn dead_addr() -> SocketAddr {
    // 127.0.0.1:1 is unlikely to be bound in any reasonable environment.
    "127.0.0.1:1".parse().unwrap()
}

#[tokio::test]
async fn broadcast_all_healthy_delivers_to_everyone() {
    let n = 4;
    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    for id in 0..n {
        peers.insert(id, spawn_drain_listener().await);
    }

    let mut sender = TcpBroadcastSender::<usize, ()>::with_peers(peers);
    let peer_ids: Vec<usize> = (0..n).collect();
    let payload = Bytes::from_static(b"hello world payload");

    let delivered = sender
        .broadcast_with_faults(&peer_ids, payload, /* fault_threshold = */ 0)
        .await;
    assert_eq!(
        delivered, n,
        "expected all {} peers to receive with fault_threshold=0; got {}",
        n, delivered
    );
}

#[tokio::test]
async fn broadcast_with_one_dead_peer_returns_quorum_quickly() {
    let n = 4;
    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    for id in 0..(n - 1) {
        peers.insert(id, spawn_drain_listener().await);
    }
    // Last peer is dead.
    peers.insert(n - 1, dead_addr());

    let mut sender = TcpBroadcastSender::<usize, ()>::with_peers(peers);
    let peer_ids: Vec<usize> = (0..n).collect();

    // fault_threshold=1 → need 3 deliveries; dead peer is skipped.
    let start = std::time::Instant::now();
    let delivered = tokio::time::timeout(
        Duration::from_secs(5),
        sender.broadcast_with_faults(&peer_ids, Bytes::from_static(b"x"), 1),
    )
    .await
    .expect("broadcast_with_faults hung past 5s even though n-t alive peers should reply quickly");
    let elapsed = start.elapsed();

    assert!(
        delivered >= 3,
        "expected >= 3 deliveries (n - t = 4 - 1); got {}",
        delivered
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "expected quorum within 500ms on loopback; took {:?}",
        elapsed
    );
}

#[tokio::test]
async fn cancel_handle_skips_at_worker() {
    // Single peer, but enqueue lots of messages then cancel them all
    // before they make it through the worker.  We can't directly observe
    // the worker's skip count but we can check that cancel doesn't panic
    // and the worker doesn't hang.
    let addr = spawn_drain_listener().await;
    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    peers.insert(0, addr);

    let mut sender = TcpBroadcastSender::<usize, ()>::with_peers(peers);

    let mut handles = Vec::new();
    for _ in 0..1000 {
        if let Some(h) = sender.send(0, Bytes::from_static(b"x")) {
            handles.push(h);
        }
    }
    // Cancel everything.
    for h in &handles {
        h.cancel();
    }
    // Give the worker a moment to drain.
    tokio::time::sleep(Duration::from_millis(50)).await;
    // No assertion beyond "didn't panic and didn't hang".  Cancelled jobs
    // should have been silently skipped at the worker.
}
