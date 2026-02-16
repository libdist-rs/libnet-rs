use std::future::poll_fn;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes, BytesMut};
use fnv::FnvHashMap;
use futures::{SinkExt, Stream, StreamExt};
use socket2::SockRef;
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::sync::Barrier;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use tcp_sender::{TcpSimpleSender, Options};
use udp_sender::UdpSimpleSender;
use udp_receiver::{UdpReceiver, UdpOptions};
use udp_reliable_sender::UdpReliableSender;
use tls_sender::TlsSimpleSender;
use tls_reliable_sender::TlsReliableSender;
use common::TlsOptions;

#[derive(Debug, Clone)]
struct RawMsg(#[allow(dead_code)] Bytes);

impl common::Message for RawMsg {
    type DeserializationError = std::convert::Infallible;
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        Ok(RawMsg(Bytes::copy_from_slice(bytes)))
    }
}
use tcp_reliable_sender::TcpReliableSender;

// ─── Configuration ──────────────────────────────────────────────────────────

const BASE_PORT: u16 = 20_000;

struct BenchConfig {
    name: &'static str,
    num_messages: usize,
    message_size: usize,
    num_peers: usize,
}

impl BenchConfig {
    fn payload(&self) -> Bytes {
        Bytes::from(vec![0xABu8; self.message_size])
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn port_offset() -> u16 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let val = COUNTER.fetch_add(100, Ordering::SeqCst) as u16;
    BASE_PORT + val
}

fn addr(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn print_results(name: &str, elapsed: Duration, total_messages: usize, msg_size: usize) {
    let secs = elapsed.as_secs_f64();
    let throughput_msgs = total_messages as f64 / secs;
    let throughput_bytes = (total_messages * msg_size) as f64 / secs;
    let throughput_mb = throughput_bytes / (1024.0 * 1024.0);
    let avg_latency_us = (elapsed.as_micros() as f64) / total_messages as f64;

    println!("┌─────────────────────────────────────────────────────────────");
    println!("│ {name}");
    println!("├─────────────────────────────────────────────────────────────");
    println!("│ Total messages : {total_messages}");
    println!("│ Message size   : {msg_size} bytes");
    println!("│ Elapsed        : {:.3} s", secs);
    println!("│ Throughput     : {throughput_msgs:.0} msg/s");
    println!("│ Bandwidth      : {throughput_mb:.2} MB/s");
    println!("│ Avg latency    : {avg_latency_us:.1} us/msg");
    println!("└─────────────────────────────────────────────────────────────");
    println!();
}

// ─── Echo server (receives messages, sends back ACK for reliable sender) ────

async fn echo_server(address: SocketAddr, expected: usize, counter: Arc<AtomicU64>, options: Options) {
    let listener = TcpListener::bind(address).await.unwrap();
    let ack = Bytes::from_static(&[0u8]);
    let mut remaining = expected;

    while remaining > 0 {
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        // Apply socket buffer tuning for lower latency
        let sock_ref = SockRef::from(&socket);
        if let Some(size) = options.tcp_send_buffer {
            let _ = sock_ref.set_send_buffer_size(size);
        }
        if let Some(size) = options.tcp_recv_buffer {
            let _ = sock_ref.set_recv_buffer_size(size);
        }

        // Split into independent reader/writer for concurrent R/W
        let (rd, wr) = socket.into_split();
        let mut reader = FramedRead::new(rd, LengthDelimitedCodec::new());
        let mut writer = FramedWrite::new(wr, LengthDelimitedCodec::new());
        writer.set_backpressure_boundary(options.write_buffer_size);
        let ack = ack.clone();
        let counter = counter.clone();

        while remaining > 0 {
            match reader.next().await {
                Some(Ok(_msg)) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    remaining -= 1;
                    // Use feed+flush for immediate dispatch without SinkExt::send overhead
                    if writer.feed(ack.clone()).await.is_err() {
                        break;
                    }
                    if writer.flush().await.is_err() {
                        break;
                    }
                }
                _ => break,
            }
        }
    }
}

/// Sink server: receives framed messages without sending ACKs (for simple sender).
async fn sink_server(address: SocketAddr, expected: usize, counter: Arc<AtomicU64>, options: Options) {
    let listener = TcpListener::bind(address).await.unwrap();
    let mut remaining = expected;

    while remaining > 0 {
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let sock_ref = SockRef::from(&socket);
        if let Some(size) = options.tcp_recv_buffer {
            let _ = sock_ref.set_recv_buffer_size(size);
        }
        let mut reader = FramedRead::new(socket, LengthDelimitedCodec::new());
        let counter = counter.clone();

        while remaining > 0 {
            match reader.next().await {
                Some(Ok(_msg)) => {
                    counter.fetch_add(1, Ordering::Relaxed);
                    remaining -= 1;
                }
                _ => break,
            }
        }
    }
}

// ─── Benchmark: Simple Sender throughput ────────────────────────────────────

async fn bench_simple_sender(config: &BenchConfig, options: Options) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            sink_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    // Give servers time to bind
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = TcpSimpleSender::<usize, ()>::with_peers_and_options(peers, options);

    let start = Instant::now();
    for i in 0..config.num_messages {
        let peer = i % config.num_peers;
        let _ = sender.send(peer, payload.clone()).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── Benchmark: Reliable Sender throughput ──────────────────────────────────

async fn bench_reliable_sender(config: &BenchConfig, options: Options) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            echo_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = TcpReliableSender::<usize, ()>::with_peers_and_options(peers, options);

    let start = Instant::now();
    let mut handlers = Vec::with_capacity(config.num_messages);
    for i in 0..config.num_messages {
        let peer = i % config.num_peers;
        match sender.send(peer, payload.clone()).await {
            Ok(h) => handlers.push(h),
            Err(e) => eprintln!("Send error: {e}"),
        }
    }

    // Wait for all ACKs
    for h in handlers {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── Benchmark: Reliable Sender throughput (send_many) ───────────────────────

async fn bench_reliable_sender_many(config: &BenchConfig, options: Options) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            echo_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = TcpReliableSender::<usize, ()>::with_peers_and_options(peers, options);

    let start = Instant::now();

    // Build per-peer message batches and send_many
    let mut all_handlers = Vec::with_capacity(config.num_messages);
    let msgs_per_peer = config.num_messages / config.num_peers;
    for peer in 0..config.num_peers {
        let batch: Vec<Bytes> = (0..msgs_per_peer).map(|_| payload.clone()).collect();
        match sender.send_many(peer, batch).await {
            Ok(handlers) => all_handlers.extend(handlers),
            Err(e) => eprintln!("send_many error: {e}"),
        }
    }

    // Wait for all ACKs
    for h in all_handlers {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── Benchmark: Broadcast fan-out ───────────────────────────────────────────

async fn bench_fanout(config: &BenchConfig, options: Options) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    let msgs_per_peer = config.num_messages;

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            sink_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = TcpSimpleSender::<usize, ()>::with_peers_and_options(peers, options);
    let peer_ids: Vec<usize> = (0..config.num_peers).collect();

    let start = Instant::now();
    for _ in 0..config.num_messages {
        let _ = sender.broadcast(&peer_ids, payload.clone()).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(60), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    let total_expected = config.num_messages * config.num_peers;
    println!("  (Broadcast: {received}/{total_expected} delivered)");
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── Benchmark: Many senders to one receiver ────────────────────────────────

async fn bench_many_to_one(num_senders: usize, messages_per_sender: usize, msg_size: usize, options: Options) {
    let base = port_offset();
    let receiver_addr = addr(base);
    let total = num_senders * messages_per_sender;
    let counter = Arc::new(AtomicU64::new(0));

    let c = counter.clone();
    let srv_opts = options.clone();
    let server = tokio::spawn(async move {
        sink_server(receiver_addr, total, c, srv_opts).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let barrier = Arc::new(Barrier::new(num_senders));
    let payload = Bytes::from(vec![0xCDu8; msg_size]);

    let start = Instant::now();

    let mut sender_handles = Vec::new();
    for _id in 0..num_senders {
        let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
        peers.insert(0, receiver_addr);
        let barrier = barrier.clone();
        let payload = payload.clone();

        let opts = options.clone();
        sender_handles.push(tokio::spawn(async move {
            let mut sender = TcpSimpleSender::<usize, ()>::with_peers_and_options(peers, opts);
            barrier.wait().await;
            for _ in 0..messages_per_sender {
                let _ = sender.send(0, payload.clone()).await;
            }
        }));
    }

    for h in sender_handles {
        let _ = h.await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(30), server).await;
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    let name = format!(
        "Many-to-One: {num_senders} senders x {messages_per_sender} msgs ({msg_size}B)"
    );
    print_results(&name, elapsed, received, msg_size);
}

// ─── Benchmark: Round-trip latency distribution ─────────────────────────────

async fn bench_latency(num_messages: usize, msg_size: usize, options: Options) {
    let base = port_offset();
    let server_addr = addr(base);
    let counter = Arc::new(AtomicU64::new(0));

    let c = counter.clone();
    let srv_opts = options.clone();
    let server = tokio::spawn(async move {
        echo_server(server_addr, num_messages, c, srv_opts).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    peers.insert(0, server_addr);
    let mut sender = TcpReliableSender::<usize, ()>::with_peers_and_options(peers, options);
    let payload = Bytes::from(vec![0xEFu8; msg_size]);

    let mut latencies = Vec::with_capacity(num_messages);

    for _ in 0..num_messages {
        let msg_start = Instant::now();
        match sender.send(0, payload.clone()).await {
            Ok(handler) => {
                let _ = tokio::time::timeout(Duration::from_secs(5), handler).await;
                latencies.push(msg_start.elapsed());
            }
            Err(e) => eprintln!("Send error: {e}"),
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(10), server).await;

    if latencies.is_empty() {
        println!("No latency data collected");
        return;
    }

    latencies.sort();
    let total: Duration = latencies.iter().sum();
    let avg = total / latencies.len() as u32;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min_lat = latencies[0];
    let max_lat = latencies[latencies.len() - 1];

    println!("┌─────────────────────────────────────────────────────────────");
    println!("│ Latency Distribution ({num_messages} msgs, {msg_size}B each)");
    println!("├─────────────────────────────────────────────────────────────");
    println!("│ Min    : {:>10.2} us", min_lat.as_nanos() as f64 / 1000.0);
    println!("│ Avg    : {:>10.2} us", avg.as_nanos() as f64 / 1000.0);
    println!("│ P50    : {:>10.2} us", p50.as_nanos() as f64 / 1000.0);
    println!("│ P95    : {:>10.2} us", p95.as_nanos() as f64 / 1000.0);
    println!("│ P99    : {:>10.2} us", p99.as_nanos() as f64 / 1000.0);
    println!("│ Max    : {:>10.2} us", max_lat.as_nanos() as f64 / 1000.0);
    println!("└─────────────────────────────────────────────────────────────");
    println!();
}

// ─── UDP Helpers ────────────────────────────────────────────────────────────

/// UDP sink: spawns a UdpReceiver and drains `expected` messages, counting them.
/// Uses a per-message timeout to avoid hanging forever on packet loss.
async fn udp_sink_drain(address: SocketAddr, expected: usize, counter: Arc<AtomicU64>, options: UdpOptions) {
    let receiver = UdpReceiver::<RawMsg>::spawn_with_options(address, options);
    let mut stream = Box::pin(receiver);
    let mut remaining = expected;
    while remaining > 0 {
        // Timeout per receive — if no message arrives within 2s, assume packet loss and stop
        match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
            Ok(Some(Ok(_))) => {
                counter.fetch_add(1, Ordering::Relaxed);
                remaining -= 1;
            }
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => {
                // Timeout — likely all remaining messages were lost
                break;
            }
        }
    }
}

// ─── UDP Benchmark: Simple Sender throughput ────────────────────────────────

async fn bench_udp_simple_sender(config: &BenchConfig, options: UdpOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    // All peers send to the same receiver in UDP (single socket)
    // But to mirror TCP's per-peer pattern, we use separate receiver addresses.
    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            udp_sink_drain(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = UdpSimpleSender::<usize, ()>::with_peers_and_options(peers, options);

    let start = Instant::now();
    for i in 0..config.num_messages {
        let peer = i % config.num_peers;
        let _ = sender.send(peer, payload.clone()).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── UDP Benchmark: Reliable Sender throughput ──────────────────────────────

async fn bench_udp_reliable_sender(config: &BenchConfig, options: UdpOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        // UdpReceiver with default "ack" feature sends ACKs automatically
        server_handles.push(tokio::spawn(async move {
            udp_sink_drain(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = UdpReliableSender::<usize, ()>::with_peers_and_options(peers, options);

    let start = Instant::now();
    let mut handlers = Vec::with_capacity(config.num_messages);
    for i in 0..config.num_messages {
        let peer = i % config.num_peers;
        match sender.send(peer, payload.clone()).await {
            Ok(h) => handlers.push(h),
            Err(e) => eprintln!("UDP reliable send error: {e}"),
        }
    }

    // Wait for all ACKs concurrently with a global deadline
    let ack_futs: Vec<_> = handlers
        .into_iter()
        .map(|h| tokio::time::timeout(Duration::from_secs(10), h))
        .collect();
    let _ = tokio::time::timeout(Duration::from_secs(15), futures::future::join_all(ack_futs)).await;

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── UDP Benchmark: Reliable Sender send_many ───────────────────────────────

async fn bench_udp_reliable_sender_many(config: &BenchConfig, options: UdpOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            udp_sink_drain(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = UdpReliableSender::<usize, ()>::with_peers_and_options(peers, options);

    let start = Instant::now();

    let mut all_handlers = Vec::with_capacity(config.num_messages);
    let msgs_per_peer = config.num_messages / config.num_peers;
    for peer in 0..config.num_peers {
        let batch: Vec<Bytes> = (0..msgs_per_peer).map(|_| payload.clone()).collect();
        match sender.send_many(peer, batch).await {
            Ok(handlers) => all_handlers.extend(handlers),
            Err(e) => eprintln!("UDP send_many error: {e}"),
        }
    }

    let ack_futs: Vec<_> = all_handlers
        .into_iter()
        .map(|h| tokio::time::timeout(Duration::from_secs(10), h))
        .collect();
    let _ = tokio::time::timeout(Duration::from_secs(15), futures::future::join_all(ack_futs)).await;

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(2), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── UDP Benchmark: Broadcast fan-out ───────────────────────────────────────

async fn bench_udp_fanout(config: &BenchConfig, options: UdpOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    let msgs_per_peer = config.num_messages;

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            udp_sink_drain(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut sender = UdpSimpleSender::<usize, ()>::with_peers_and_options(peers, options);
    let peer_ids: Vec<usize> = (0..config.num_peers).collect();

    let start = Instant::now();
    for _ in 0..config.num_messages {
        let _ = sender.broadcast(&peer_ids, payload.clone()).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(60), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    let total_expected = config.num_messages * config.num_peers;
    println!("  (UDP Broadcast: {received}/{total_expected} delivered)");
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── UDP Benchmark: Many senders to one receiver ────────────────────────────

async fn bench_udp_many_to_one(num_senders: usize, messages_per_sender: usize, msg_size: usize, options: UdpOptions) {
    let base = port_offset();
    let receiver_addr = addr(base);
    let total = num_senders * messages_per_sender;
    let counter = Arc::new(AtomicU64::new(0));

    let c = counter.clone();
    let srv_opts = options.clone();
    let server = tokio::spawn(async move {
        udp_sink_drain(receiver_addr, total, c, srv_opts).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let barrier = Arc::new(Barrier::new(num_senders));
    let payload = Bytes::from(vec![0xCDu8; msg_size]);

    let start = Instant::now();

    let mut sender_handles = Vec::new();
    for _id in 0..num_senders {
        let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
        peers.insert(0, receiver_addr);
        let barrier = barrier.clone();
        let payload = payload.clone();
        let opts = options.clone();

        sender_handles.push(tokio::spawn(async move {
            let mut sender = UdpSimpleSender::<usize, ()>::with_peers_and_options(peers, opts);
            barrier.wait().await;
            for _ in 0..messages_per_sender {
                let _ = sender.send(0, payload.clone()).await;
            }
        }));
    }

    for h in sender_handles {
        let _ = h.await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(30), server).await;
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    let name = format!(
        "UDP Many-to-One: {num_senders} senders x {messages_per_sender} msgs ({msg_size}B)"
    );
    print_results(&name, elapsed, received, msg_size);
}

// ─── UDP Benchmark: Round-trip latency distribution ─────────────────────────

async fn bench_udp_latency(num_messages: usize, msg_size: usize, options: UdpOptions) {
    let base = port_offset();
    let server_addr = addr(base);
    let counter = Arc::new(AtomicU64::new(0));

    let c = counter.clone();
    let srv_opts = options.clone();
    // UdpReceiver with "ack" feature acts as echo (sends ACK on receipt)
    let server = tokio::spawn(async move {
        udp_sink_drain(server_addr, num_messages, c, srv_opts).await;
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    peers.insert(0, server_addr);
    let mut sender = UdpReliableSender::<usize, ()>::with_peers_and_options(peers, options);
    let payload = Bytes::from(vec![0xEFu8; msg_size]);

    let mut latencies = Vec::with_capacity(num_messages);

    // Global deadline for the entire latency benchmark to prevent hanging
    // on fragment loss with sequential sends
    let deadline = Instant::now() + Duration::from_secs(15);

    for _ in 0..num_messages {
        if Instant::now() >= deadline {
            break;
        }
        let msg_start = Instant::now();
        match sender.send(0, payload.clone()).await {
            Ok(handler) => {
                match tokio::time::timeout(Duration::from_millis(500), handler).await {
                    Ok(Ok(_)) => latencies.push(msg_start.elapsed()),
                    _ => {} // timeout or error — skip this measurement
                }
            }
            Err(e) => eprintln!("UDP send error: {e}"),
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(2), server).await;

    if latencies.is_empty() {
        println!("No UDP latency data collected");
        return;
    }

    latencies.sort();
    let total: Duration = latencies.iter().sum();
    let avg = total / latencies.len() as u32;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min_lat = latencies[0];
    let max_lat = latencies[latencies.len() - 1];

    println!("┌─────────────────────────────────────────────────────────────");
    println!("│ UDP Latency ({}/{num_messages} msgs, {msg_size}B each)", latencies.len());
    println!("├─────────────────────────────────────────────────────────────");
    println!("│ Min    : {:>10.2} us", min_lat.as_nanos() as f64 / 1000.0);
    println!("│ Avg    : {:>10.2} us", avg.as_nanos() as f64 / 1000.0);
    println!("│ P50    : {:>10.2} us", p50.as_nanos() as f64 / 1000.0);
    println!("│ P95    : {:>10.2} us", p95.as_nanos() as f64 / 1000.0);
    println!("│ P99    : {:>10.2} us", p99.as_nanos() as f64 / 1000.0);
    println!("│ Max    : {:>10.2} us", max_lat.as_nanos() as f64 / 1000.0);
    println!("└─────────────────────────────────────────────────────────────");
    println!();
}

// ─── TLS Helpers ─────────────────────────────────────────────────────────────

/// TLS echo server: accepts TLS connections, reads framed messages, sends ACK frames back.
async fn tls_echo_server(address: SocketAddr, expected: usize, counter: Arc<AtomicU64>, options: TlsOptions) {
    let server_config = common::tls_config::build_server_config(&options).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let listener = TcpListener::bind(address).await.unwrap();
    // Pre-encode ACK frame: [len:u32][payload:1 byte]
    let ack_frame: &[u8] = &[0, 0, 0, 1, 0];
    let mut remaining = expected;

    while remaining > 0 {
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let sock_ref = SockRef::from(&socket);
        if let Some(size) = options.tcp_send_buffer {
            let _ = sock_ref.set_send_buffer_size(size);
        }
        if let Some(size) = options.tcp_recv_buffer {
            let _ = sock_ref.set_recv_buffer_size(size);
        }

        let tls_stream = match acceptor.accept(socket).await {
            Ok(s) => s,
            Err(e) => { log::error!("TLS accept error: {e}"); continue; }
        };

        let (rd, mut wr) = tokio::io::split(tls_stream);
        let mut reader = FramedRead::new(rd, LengthDelimitedCodec::new());
        let counter = counter.clone();
        let mut write_buf = BytesMut::with_capacity(8192);

        // poll_fn: batch ACK writes, only flush when buffer is empty
        let result: Result<(), std::io::Error> = poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Flush write buffer
                while !write_buf.is_empty() {
                    match Pin::new(&mut wr).poll_write(cx, &write_buf) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::WriteZero, "write returned 0",
                            )));
                        }
                        Poll::Ready(Ok(n)) => {
                            progress = true;
                            write_buf.advance(n);
                        }
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => break,
                    }
                }
                if write_buf.is_empty() { write_buf.clear(); }

                // Phase 1b: Flush kernel buffer
                if write_buf.is_empty() {
                    match Pin::new(&mut wr).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                        Poll::Pending => {}
                    }
                }

                // Exit when done and buffer flushed
                if remaining == 0 && write_buf.is_empty() {
                    return Poll::Ready(Ok(()));
                }

                // Phase 2: Read messages + batch ACK into write buffer
                if remaining > 0 {
                    match Pin::new(&mut reader).poll_next(cx) {
                        Poll::Ready(Some(Ok(_msg))) => {
                            progress = true;
                            counter.fetch_add(1, Ordering::Relaxed);
                            remaining -= 1;
                            write_buf.extend_from_slice(ack_frame);
                        }
                        Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e)),
                        Poll::Ready(None) => return Poll::Ready(Ok(())),
                        Poll::Pending => {}
                    }
                }
            }

            // Check exit condition outside the progress loop
            if remaining == 0 && write_buf.is_empty() {
                return Poll::Ready(Ok(()));
            }
            Poll::Pending
        }).await;

        if let Err(e) = result {
            log::error!("TLS echo server error: {e}");
        }
        if remaining == 0 { break; }
    }
}

/// TLS sink server: accepts TLS connections and drains messages without sending ACKs.
async fn tls_sink_server(address: SocketAddr, expected: usize, counter: Arc<AtomicU64>, options: TlsOptions) {
    let server_config = common::tls_config::build_server_config(&options).unwrap();
    let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
    let listener = TcpListener::bind(address).await.unwrap();
    let mut remaining = expected;

    while remaining > 0 {
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let sock_ref = SockRef::from(&socket);
        if let Some(size) = options.tcp_recv_buffer {
            let _ = sock_ref.set_recv_buffer_size(size);
        }

        let tls_stream = match acceptor.accept(socket).await {
            Ok(s) => s,
            Err(e) => { log::error!("TLS accept error: {e}"); continue; }
        };

        // No split needed — read-only, skip Arc+Mutex overhead
        let mut reader = FramedRead::new(tls_stream, LengthDelimitedCodec::new());
        let counter = counter.clone();

        let result: Result<(), ()> = poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;
                match Pin::new(&mut reader).poll_next(cx) {
                    Poll::Ready(Some(Ok(_msg))) => {
                        progress = true;
                        counter.fetch_add(1, Ordering::Relaxed);
                        remaining -= 1;
                        if remaining == 0 {
                            return Poll::Ready(Ok(()));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        log::error!("TLS sink read error: {e}");
                        return Poll::Ready(Err(()));
                    }
                    Poll::Ready(None) => return Poll::Ready(Ok(())),
                    Poll::Pending => {}
                }
            }
            Poll::Pending
        }).await;

        let _ = result;
        if remaining == 0 { break; }
    }
}

// ─── TLS Benchmark: Simple Sender throughput ─────────────────────────────────

async fn bench_tls_simple_sender(config: &BenchConfig, options: TlsOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            tls_sink_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sender = TlsSimpleSender::<usize, ()>::with_peers_and_options(peers, options).unwrap();

    let start = Instant::now();
    for i in 0..config.num_messages {
        let peer = i % config.num_peers;
        let _ = sender.send(peer, payload.clone()).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── TLS Benchmark: Reliable Sender throughput ───────────────────────────────

async fn bench_tls_reliable_sender(config: &BenchConfig, options: TlsOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            tls_echo_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sender = TlsReliableSender::<usize, ()>::with_peers_and_options(peers, options).unwrap();

    let start = Instant::now();
    let mut handlers = Vec::with_capacity(config.num_messages);
    for i in 0..config.num_messages {
        let peer = i % config.num_peers;
        match sender.send(peer, payload.clone()).await {
            Ok(h) => handlers.push(h),
            Err(e) => eprintln!("TLS send error: {e}"),
        }
    }

    for h in handlers {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── TLS Benchmark: Reliable Sender send_many ────────────────────────────────

async fn bench_tls_reliable_sender_many(config: &BenchConfig, options: TlsOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let msgs_per_peer = config.num_messages / config.num_peers;
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            tls_echo_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sender = TlsReliableSender::<usize, ()>::with_peers_and_options(peers, options).unwrap();

    let start = Instant::now();

    let mut all_handlers = Vec::with_capacity(config.num_messages);
    let msgs_per_peer = config.num_messages / config.num_peers;
    for peer in 0..config.num_peers {
        let batch: Vec<Bytes> = (0..msgs_per_peer).map(|_| payload.clone()).collect();
        match sender.send_many(peer, batch).await {
            Ok(handlers) => all_handlers.extend(handlers),
            Err(e) => eprintln!("TLS send_many error: {e}"),
        }
    }

    for h in all_handlers {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(30), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── TLS Benchmark: Broadcast fan-out ────────────────────────────────────────

async fn bench_tls_fanout(config: &BenchConfig, options: TlsOptions) {
    let base = port_offset();
    let payload = config.payload();
    let counter = Arc::new(AtomicU64::new(0));

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    let mut server_handles = Vec::new();

    let msgs_per_peer = config.num_messages;

    for i in 0..config.num_peers {
        let address = addr(base + i as u16);
        peers.insert(i, address);
        let c = counter.clone();
        let opts = options.clone();
        server_handles.push(tokio::spawn(async move {
            tls_sink_server(address, msgs_per_peer, c, opts).await;
        }));
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut sender = TlsSimpleSender::<usize, ()>::with_peers_and_options(peers, options).unwrap();
    let peer_ids: Vec<usize> = (0..config.num_peers).collect();

    let start = Instant::now();
    for _ in 0..config.num_messages {
        let _ = sender.broadcast(&peer_ids, payload.clone()).await;
    }

    for h in server_handles {
        let _ = tokio::time::timeout(Duration::from_secs(60), h).await;
    }
    let elapsed = start.elapsed();

    let received = counter.load(Ordering::Relaxed) as usize;
    let total_expected = config.num_messages * config.num_peers;
    println!("  (TLS Broadcast: {received}/{total_expected} delivered)");
    print_results(config.name, elapsed, received, config.message_size);
}

// ─── TLS Benchmark: Round-trip latency distribution ──────────────────────────

async fn bench_tls_latency(num_messages: usize, msg_size: usize, options: TlsOptions) {
    let base = port_offset();
    let server_addr = addr(base);
    let counter = Arc::new(AtomicU64::new(0));

    let c = counter.clone();
    let srv_opts = options.clone();
    let server = tokio::spawn(async move {
        tls_echo_server(server_addr, num_messages, c, srv_opts).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut peers: FnvHashMap<usize, SocketAddr> = FnvHashMap::default();
    peers.insert(0, server_addr);
    let mut sender = TlsReliableSender::<usize, ()>::with_peers_and_options(peers, options).unwrap();
    let payload = Bytes::from(vec![0xEFu8; msg_size]);

    let mut latencies = Vec::with_capacity(num_messages);

    for _ in 0..num_messages {
        let msg_start = Instant::now();
        match sender.send(0, payload.clone()).await {
            Ok(handler) => {
                let _ = tokio::time::timeout(Duration::from_secs(5), handler).await;
                latencies.push(msg_start.elapsed());
            }
            Err(e) => eprintln!("TLS send error: {e}"),
        }
    }

    let _ = tokio::time::timeout(Duration::from_secs(10), server).await;

    if latencies.is_empty() {
        println!("No TLS latency data collected");
        return;
    }

    latencies.sort();
    let total: Duration = latencies.iter().sum();
    let avg = total / latencies.len() as u32;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() as f64 * 0.95) as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99) as usize];
    let min_lat = latencies[0];
    let max_lat = latencies[latencies.len() - 1];

    println!("┌─────────────────────────────────────────────────────────────");
    println!("│ TLS Latency Distribution ({num_messages} msgs, {msg_size}B each)");
    println!("├─────────────────────────────────────────────────────────────");
    println!("│ Min    : {:>10.2} us", min_lat.as_nanos() as f64 / 1000.0);
    println!("│ Avg    : {:>10.2} us", avg.as_nanos() as f64 / 1000.0);
    println!("│ P50    : {:>10.2} us", p50.as_nanos() as f64 / 1000.0);
    println!("│ P95    : {:>10.2} us", p95.as_nanos() as f64 / 1000.0);
    println!("│ P99    : {:>10.2} us", p99.as_nanos() as f64 / 1000.0);
    println!("│ Max    : {:>10.2} us", max_lat.as_nanos() as f64 / 1000.0);
    println!("└─────────────────────────────────────────────────────────────");
    println!();
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    env_logger::init();

    // Use high-throughput options for optimal performance benchmarks.
    // Users can also use Options::default() or Options::low_latency() depending on workload.
    let opts = Options::high_throughput();

    println!();
    println!("================================================================");
    println!("  libnet-rs Stress Test Suite");
    println!("  Options: high_throughput (batch_drain_cap={}, write_buffer={}KB)",
        opts.batch_drain_cap, opts.write_buffer_size / 1024);
    println!("================================================================");
    println!();

    // ── Small-scale (10k) ──────────────────────────────────────────────
    bench_simple_sender(&BenchConfig {
        name: "SimpleSender: 10k x 64B, 1 peer",
        num_messages: 10_000,
        message_size: 64,
        num_peers: 1,
    }, opts.clone())
    .await;

    bench_simple_sender(&BenchConfig {
        name: "SimpleSender: 5k x 4KB, 1 peer",
        num_messages: 5_000,
        message_size: 4096,
        num_peers: 1,
    }, opts.clone())
    .await;

    bench_simple_sender(&BenchConfig {
        name: "SimpleSender: 10k x 256B, 10 peers",
        num_messages: 10_000,
        message_size: 256,
        num_peers: 10,
    }, opts.clone())
    .await;

    bench_reliable_sender(&BenchConfig {
        name: "ReliableSender: 5k x 64B, 1 peer (with ACK)",
        num_messages: 5_000,
        message_size: 64,
        num_peers: 1,
    }, opts.clone())
    .await;

    bench_reliable_sender(&BenchConfig {
        name: "ReliableSender: 5k x 256B, 5 peers (with ACK)",
        num_messages: 5_000,
        message_size: 256,
        num_peers: 5,
    }, opts.clone())
    .await;

    bench_reliable_sender_many(&BenchConfig {
        name: "ReliableSender send_many: 5k x 64B, 1 peer (with ACK)",
        num_messages: 5_000,
        message_size: 64,
        num_peers: 1,
    }, opts.clone())
    .await;

    bench_reliable_sender_many(&BenchConfig {
        name: "ReliableSender send_many: 5k x 256B, 5 peers (with ACK)",
        num_messages: 5_000,
        message_size: 256,
        num_peers: 5,
    }, opts.clone())
    .await;

    bench_fanout(&BenchConfig {
        name: "Broadcast: 1k msgs x 10 peers, 128B",
        num_messages: 1_000,
        message_size: 128,
        num_peers: 10,
    }, opts.clone())
    .await;

    bench_many_to_one(10, 1_000, 256, opts.clone()).await;
    bench_many_to_one(50, 200, 256, opts.clone()).await;

    // ── High-volume (100k+) ──────────────────────────────────────────
    println!("────────────────────────────────────────────────────────────────");
    println!("  High-Volume Benchmarks (100k+ messages)");
    println!("────────────────────────────────────────────────────────────────");
    println!();

    bench_simple_sender(&BenchConfig {
        name: "SimpleSender: 500k x 64B, 1 peer",
        num_messages: 500_000,
        message_size: 64,
        num_peers: 1,
    }, opts.clone())
    .await;

    bench_simple_sender(&BenchConfig {
        name: "SimpleSender: 500k x 256B, 10 peers",
        num_messages: 500_000,
        message_size: 256,
        num_peers: 10,
    }, opts.clone())
    .await;

    bench_reliable_sender(&BenchConfig {
        name: "ReliableSender: 100k x 64B, 1 peer (with ACK)",
        num_messages: 100_000,
        message_size: 64,
        num_peers: 1,
    }, opts.clone())
    .await;

    bench_reliable_sender(&BenchConfig {
        name: "ReliableSender: 100k x 256B, 5 peers (with ACK)",
        num_messages: 100_000,
        message_size: 256,
        num_peers: 5,
    }, opts.clone())
    .await;

    bench_reliable_sender_many(&BenchConfig {
        name: "ReliableSender send_many: 100k x 64B, 1 peer (with ACK)",
        num_messages: 100_000,
        message_size: 64,
        num_peers: 1,
    }, opts.clone())
    .await;

    bench_reliable_sender_many(&BenchConfig {
        name: "ReliableSender send_many: 100k x 256B, 5 peers (with ACK)",
        num_messages: 100_000,
        message_size: 256,
        num_peers: 5,
    }, opts.clone())
    .await;

    bench_fanout(&BenchConfig {
        name: "Broadcast: 10k msgs x 10 peers, 128B",
        num_messages: 10_000,
        message_size: 128,
        num_peers: 10,
    }, opts.clone())
    .await;

    bench_many_to_one(10, 10_000, 256, opts.clone()).await;
    bench_many_to_one(50, 2_000, 256, opts.clone()).await;

    // 8. Latency distribution -- use low-latency options for this benchmark
    let lat_opts = Options::low_latency();
    println!("────────────────────────────────────────────────────────────────");
    println!("  Latency Benchmarks (Options::low_latency)");
    println!("  batch_drain_cap={}, write_buffer={}KB, SO_SNDBUF={}KB, SO_RCVBUF={}KB",
        lat_opts.batch_drain_cap, lat_opts.write_buffer_size / 1024,
        lat_opts.tcp_send_buffer.unwrap_or(0) / 1024,
        lat_opts.tcp_recv_buffer.unwrap_or(0) / 1024);
    println!("────────────────────────────────────────────────────────────────");
    println!();
    bench_latency(2_000, 64, lat_opts.clone()).await;
    bench_latency(1_000, 4096, lat_opts).await;

    // 9. Large message stress -- use custom options with large frame limit
    let large_opts = Options {
        max_frame_length: 128 * 1024 * 1024,
        write_buffer_size: 131072,
        ..opts.clone()
    };

    bench_simple_sender(&BenchConfig {
        name: "SimpleSender: 1k x 64KB, 1 peer",
        num_messages: 1_000,
        message_size: 65_536,
        num_peers: 1,
    }, large_opts.clone())
    .await;

    bench_reliable_sender(&BenchConfig {
        name: "ReliableSender: 500 x 64KB, 1 peer (with ACK)",
        num_messages: 500,
        message_size: 65_536,
        num_peers: 1,
    }, large_opts)
    .await;

    // ══════════════════════════════════════════════════════════════════
    //  UDP Benchmarks
    // ══════════════════════════════════════════════════════════════════

    let udp_opts = UdpOptions::high_throughput();

    println!();
    println!("================================================================");
    println!("  UDP Stress Test Suite");
    println!("  Options: high_throughput (batch_drain_cap={}, max_datagram={}B)",
        udp_opts.batch_drain_cap, udp_opts.max_datagram_payload);
    println!("================================================================");
    println!();

    // ── UDP Small-scale (10k) ────────────────────────────────────────

    bench_udp_simple_sender(&BenchConfig {
        name: "UDP SimpleSender: 10k x 64B, 1 peer",
        num_messages: 10_000,
        message_size: 64,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_simple_sender(&BenchConfig {
        name: "UDP SimpleSender: 5k x 4KB, 1 peer",
        num_messages: 5_000,
        message_size: 4096,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_simple_sender(&BenchConfig {
        name: "UDP SimpleSender: 10k x 256B, 10 peers",
        num_messages: 10_000,
        message_size: 256,
        num_peers: 10,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender(&BenchConfig {
        name: "UDP ReliableSender: 5k x 64B, 1 peer (with ACK)",
        num_messages: 5_000,
        message_size: 64,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender(&BenchConfig {
        name: "UDP ReliableSender: 5k x 256B, 5 peers (with ACK)",
        num_messages: 5_000,
        message_size: 256,
        num_peers: 5,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender_many(&BenchConfig {
        name: "UDP ReliableSender send_many: 5k x 64B, 1 peer (with ACK)",
        num_messages: 5_000,
        message_size: 64,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender_many(&BenchConfig {
        name: "UDP ReliableSender send_many: 5k x 256B, 5 peers (with ACK)",
        num_messages: 5_000,
        message_size: 256,
        num_peers: 5,
    }, udp_opts.clone())
    .await;

    bench_udp_fanout(&BenchConfig {
        name: "UDP Broadcast: 1k msgs x 10 peers, 128B",
        num_messages: 1_000,
        message_size: 128,
        num_peers: 10,
    }, udp_opts.clone())
    .await;

    bench_udp_many_to_one(10, 1_000, 256, udp_opts.clone()).await;
    bench_udp_many_to_one(50, 200, 256, udp_opts.clone()).await;

    // ── UDP High-volume (100k+) ──────────────────────────────────────
    println!("────────────────────────────────────────────────────────────────");
    println!("  UDP High-Volume Benchmarks (100k+ messages)");
    println!("────────────────────────────────────────────────────────────────");
    println!();

    bench_udp_simple_sender(&BenchConfig {
        name: "UDP SimpleSender: 500k x 64B, 1 peer",
        num_messages: 500_000,
        message_size: 64,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_simple_sender(&BenchConfig {
        name: "UDP SimpleSender: 100k x 256B, 10 peers",
        num_messages: 100_000,
        message_size: 256,
        num_peers: 10,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender(&BenchConfig {
        name: "UDP ReliableSender: 100k x 64B, 1 peer (with ACK)",
        num_messages: 100_000,
        message_size: 64,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender(&BenchConfig {
        name: "UDP ReliableSender: 100k x 256B, 5 peers (with ACK)",
        num_messages: 100_000,
        message_size: 256,
        num_peers: 5,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender_many(&BenchConfig {
        name: "UDP ReliableSender send_many: 100k x 64B, 1 peer (with ACK)",
        num_messages: 100_000,
        message_size: 64,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender_many(&BenchConfig {
        name: "UDP ReliableSender send_many: 100k x 256B, 5 peers (with ACK)",
        num_messages: 100_000,
        message_size: 256,
        num_peers: 5,
    }, udp_opts.clone())
    .await;

    bench_udp_fanout(&BenchConfig {
        name: "UDP Broadcast: 10k msgs x 10 peers, 128B",
        num_messages: 10_000,
        message_size: 128,
        num_peers: 10,
    }, udp_opts.clone())
    .await;

    bench_udp_many_to_one(10, 5_000, 256, udp_opts.clone()).await;
    bench_udp_many_to_one(50, 1_000, 256, udp_opts.clone()).await;

    // ── UDP Latency ──────────────────────────────────────────────────
    let udp_lat_opts = UdpOptions::low_latency();
    println!("────────────────────────────────────────────────────────────────");
    println!("  UDP Latency Benchmarks (UdpOptions::low_latency)");
    println!("────────────────────────────────────────────────────────────────");
    println!();
    bench_udp_latency(2_000, 64, udp_lat_opts.clone()).await;
    bench_udp_latency(1_000, 4096, udp_lat_opts).await;

    // ── UDP Large message (fragmented) ───────────────────────────────
    // 64KB = ~45 fragments per message. Fire-and-forget has high fragment loss
    // so counts are kept low. Reliable sender retries handle loss.
    println!("────────────────────────────────────────────────────────────────");
    println!("  UDP Large Message Benchmarks (fragmented)");
    println!("────────────────────────────────────────────────────────────────");
    println!();

    bench_udp_simple_sender(&BenchConfig {
        name: "UDP SimpleSender: 100 x 64KB, 1 peer (fragmented)",
        num_messages: 100,
        message_size: 65_536,
        num_peers: 1,
    }, udp_opts.clone())
    .await;

    bench_udp_reliable_sender(&BenchConfig {
        name: "UDP ReliableSender: 100 x 64KB, 1 peer (fragmented + ACK)",
        num_messages: 100,
        message_size: 65_536,
        num_peers: 1,
    }, udp_opts)
    .await;

    // ══════════════════════════════════════════════════════════════════
    //  TLS Benchmarks (PQ: X25519MLKEM768)
    // ══════════════════════════════════════════════════════════════════

    let tls_opts = TlsOptions {
        danger_skip_verify: true,
        ..TlsOptions::high_throughput()
    };

    println!();
    println!("================================================================");
    println!("  TLS Stress Test Suite (PQ: {:?})", tls_opts.pq_protocol);
    println!("  Options: high_throughput (batch_drain_cap={}, write_buffer={}KB)",
        tls_opts.batch_drain_cap, tls_opts.write_buffer_size / 1024);
    println!("================================================================");
    println!();

    // ── TLS Small-scale (10k) ──────────────────────────────────────

    bench_tls_simple_sender(&BenchConfig {
        name: "TLS SimpleSender: 10k x 64B, 1 peer",
        num_messages: 10_000,
        message_size: 64,
        num_peers: 1,
    }, tls_opts.clone())
    .await;

    bench_tls_simple_sender(&BenchConfig {
        name: "TLS SimpleSender: 10k x 256B, 10 peers",
        num_messages: 10_000,
        message_size: 256,
        num_peers: 10,
    }, tls_opts.clone())
    .await;

    bench_tls_reliable_sender(&BenchConfig {
        name: "TLS ReliableSender: 5k x 64B, 1 peer (with ACK)",
        num_messages: 5_000,
        message_size: 64,
        num_peers: 1,
    }, tls_opts.clone())
    .await;

    bench_tls_reliable_sender(&BenchConfig {
        name: "TLS ReliableSender: 5k x 256B, 5 peers (with ACK)",
        num_messages: 5_000,
        message_size: 256,
        num_peers: 5,
    }, tls_opts.clone())
    .await;

    bench_tls_reliable_sender_many(&BenchConfig {
        name: "TLS ReliableSender send_many: 5k x 256B, 5 peers (with ACK)",
        num_messages: 5_000,
        message_size: 256,
        num_peers: 5,
    }, tls_opts.clone())
    .await;

    bench_tls_fanout(&BenchConfig {
        name: "TLS Broadcast: 1k msgs x 10 peers, 128B",
        num_messages: 1_000,
        message_size: 128,
        num_peers: 10,
    }, tls_opts.clone())
    .await;

    // ── TLS High-volume (100k+) ──────────────────────────────────
    println!("────────────────────────────────────────────────────────────────");
    println!("  TLS High-Volume Benchmarks (100k+ messages)");
    println!("────────────────────────────────────────────────────────────────");
    println!();

    bench_tls_simple_sender(&BenchConfig {
        name: "TLS SimpleSender: 100k x 64B, 1 peer",
        num_messages: 100_000,
        message_size: 64,
        num_peers: 1,
    }, tls_opts.clone())
    .await;

    bench_tls_reliable_sender(&BenchConfig {
        name: "TLS ReliableSender: 100k x 64B, 1 peer (with ACK)",
        num_messages: 100_000,
        message_size: 64,
        num_peers: 1,
    }, tls_opts.clone())
    .await;

    bench_tls_reliable_sender(&BenchConfig {
        name: "TLS ReliableSender: 100k x 256B, 5 peers (with ACK)",
        num_messages: 100_000,
        message_size: 256,
        num_peers: 5,
    }, tls_opts.clone())
    .await;

    bench_tls_reliable_sender_many(&BenchConfig {
        name: "TLS ReliableSender send_many: 100k x 256B, 5 peers (with ACK)",
        num_messages: 100_000,
        message_size: 256,
        num_peers: 5,
    }, tls_opts.clone())
    .await;

    // ── TLS Latency ──────────────────────────────────────────────
    let tls_lat_opts = TlsOptions {
        danger_skip_verify: true,
        ..TlsOptions::low_latency()
    };
    println!("────────────────────────────────────────────────────────────────");
    println!("  TLS Latency Benchmarks (TlsOptions::low_latency, PQ: {:?})", tls_lat_opts.pq_protocol);
    println!("────────────────────────────────────────────────────────────────");
    println!();
    bench_tls_latency(2_000, 64, tls_lat_opts.clone()).await;
    bench_tls_latency(1_000, 4096, tls_lat_opts).await;

    println!("================================================================");
    println!("  Stress Test Complete");
    println!("================================================================");
}
