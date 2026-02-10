use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use fnv::FnvHashMap;
use futures::{SinkExt, StreamExt};
use socket2::SockRef;
use tokio::net::TcpListener;
use tokio::sync::Barrier;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use tcp_sender::{TcpSimpleSender, Options};
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

    println!("================================================================");
    println!("  Stress Test Complete");
    println!("================================================================");
}
