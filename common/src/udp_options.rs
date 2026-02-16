use std::time::Duration;

/// Configuration options for libnet-rs UDP networking components.
///
/// Controls datagram sizes, fragmentation/reassembly, channel capacity, and socket options.
/// Use `UdpOptions::default()` for sane defaults or tune individual fields for your workload.
///
/// # Example
/// ```
/// use common::UdpOptions;
/// use std::time::Duration;
///
/// let opts = UdpOptions {
///     max_datagram_payload: 1200,
///     reassembly_timeout: Duration::from_secs(10),
///     ..UdpOptions::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct UdpOptions {
    /// Maximum payload per UDP datagram in bytes (including fragment header).
    /// Must account for IP (20 bytes) and UDP (8 bytes) headers within the MTU.
    /// Default: 1472 (1500 MTU - 20 IP - 8 UDP).
    pub max_datagram_payload: usize,

    /// Timeout for incomplete fragment groups before eviction.
    /// If all fragments for a message don't arrive within this duration,
    /// the partial reassembly is discarded.
    /// Default: 5 seconds.
    pub reassembly_timeout: Duration,

    /// Maximum number of concurrent in-flight reassembly groups.
    /// Limits memory usage from partially received fragmented messages.
    /// Default: 1024.
    pub max_pending_reassemblies: usize,

    /// Channel capacity for bounded sender channels.
    /// Controls how many datagrams can be queued before the sender blocks.
    /// Higher values reduce contention; lower values provide earlier backpressure.
    /// Default: 1024.
    pub channel_capacity: usize,

    /// Maximum number of datagrams to drain from channels per poll cycle.
    /// Higher values improve throughput under load but may starve the tokio runtime.
    /// Default: 1024.
    pub batch_drain_cap: usize,

    /// Pre-allocation capacity for internal VecDeque buffers (reliable sender).
    /// Default: 128.
    pub buffer_capacity: usize,

    /// Initial retry delay for the reliable sender when an ACK is not received.
    /// Uses exponential backoff from this value up to `retry_max_delay`.
    /// Default: 50ms.
    pub retry_initial_delay: Duration,

    /// Maximum retry delay for the reliable sender's exponential backoff.
    /// Default: 60s.
    pub retry_max_delay: Duration,

    /// UDP send buffer size (SO_SNDBUF) in bytes. `None` uses the OS default.
    /// Default: None (OS default).
    pub udp_send_buffer: Option<usize>,

    /// UDP receive buffer size (SO_RCVBUF) in bytes. `None` uses the OS default.
    /// Default: None (OS default).
    pub udp_recv_buffer: Option<usize>,
}

impl Default for UdpOptions {
    fn default() -> Self {
        Self {
            max_datagram_payload: 1472,
            reassembly_timeout: Duration::from_secs(5),
            max_pending_reassemblies: 1024,
            channel_capacity: 1024,
            batch_drain_cap: 1024,
            buffer_capacity: 128,
            retry_initial_delay: Duration::from_millis(50),
            retry_max_delay: Duration::from_secs(60),
            udp_send_buffer: None,
            udp_recv_buffer: None,
        }
    }
}

impl UdpOptions {
    /// Returns an optimized configuration for high-throughput workloads.
    ///
    /// Increases channel capacity, batch drain cap, and socket buffers for
    /// maximum datagrams-per-second. Uses 2MB socket buffers to absorb bursts
    /// without kernel drops (macOS default recv buffer is ~768KB).
    pub fn high_throughput() -> Self {
        Self {
            max_datagram_payload: 1472,
            reassembly_timeout: Duration::from_secs(10),
            max_pending_reassemblies: 4096,
            channel_capacity: 8192,
            batch_drain_cap: 8192,
            buffer_capacity: 1024,
            retry_initial_delay: Duration::from_millis(25),
            retry_max_delay: Duration::from_secs(30),
            udp_send_buffer: Some(2 * 1024 * 1024),
            udp_recv_buffer: Some(2 * 1024 * 1024),
        }
    }

    /// Returns a configuration optimized for low-latency workloads.
    ///
    /// Smaller batch sizes to reduce head-of-line blocking and fast retries.
    /// Uses `None` for socket buffers to keep OS defaults (typically ~768KB on
    /// macOS), avoiding shrinkage that would cause drops.
    pub fn low_latency() -> Self {
        Self {
            max_datagram_payload: 1472,
            reassembly_timeout: Duration::from_secs(2),
            max_pending_reassemblies: 512,
            channel_capacity: 512,
            batch_drain_cap: 256,
            buffer_capacity: 64,
            retry_initial_delay: Duration::from_millis(10),
            retry_max_delay: Duration::from_secs(10),
            udp_send_buffer: None,
            udp_recv_buffer: None,
        }
    }
}
