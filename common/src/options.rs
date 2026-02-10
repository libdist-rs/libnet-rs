use std::time::Duration;

/// Configuration options for libnet-rs networking components.
///
/// Controls buffer sizes, batching behavior, TCP socket options, and codec frame limits.
/// Use `Options::default()` for sane defaults or tune individual fields for your workload.
///
/// # Example
/// ```
/// use common::Options;
/// use std::time::Duration;
///
/// // High-throughput configuration
/// let opts = Options {
///     max_frame_length: 16 * 1024 * 1024, // 16 MB frames
///     write_buffer_size: 16384,            // 16 KB write buffer
///     batch_drain_cap: 4096,               // drain up to 4096 msgs per flush
///     ..Options::default()
/// };
/// ```
#[derive(Debug, Clone)]
pub struct Options {
    /// Maximum frame size in bytes for `LengthDelimitedCodec`.
    /// Frames larger than this will be rejected.
    /// Default: 8 MB (8_388_608).
    pub max_frame_length: usize,

    /// Initial write buffer capacity in bytes for the framed writer.
    /// Larger values reduce reallocations under high throughput.
    /// Default: 8192 (8 KB).
    pub write_buffer_size: usize,

    /// Maximum number of messages to drain from the channel per flush cycle.
    /// Higher values improve throughput under load but may starve the tokio runtime.
    /// Default: 1024.
    pub batch_drain_cap: usize,

    /// Pre-allocation capacity for internal VecDeque buffers (reliable sender).
    /// Default: 128.
    pub buffer_capacity: usize,

    /// Whether to set `TCP_NODELAY` on sockets (disables Nagle's algorithm).
    /// Reduces latency at the cost of more small packets.
    /// Default: true.
    pub tcp_nodelay: bool,

    /// Initial retry delay for the reliable sender when a connection fails.
    /// Uses exponential backoff from this value up to `retry_max_delay`.
    /// Default: 50ms.
    pub retry_initial_delay: Duration,

    /// Maximum retry delay for the reliable sender's exponential backoff.
    /// Default: 60s.
    pub retry_max_delay: Duration,

    /// TCP send buffer size (SO_SNDBUF) in bytes. `None` uses the OS default.
    /// Smaller values reduce buffering latency; larger values improve throughput.
    /// Default: None (OS default).
    pub tcp_send_buffer: Option<usize>,

    /// TCP receive buffer size (SO_RCVBUF) in bytes. `None` uses the OS default.
    /// Default: None (OS default).
    pub tcp_recv_buffer: Option<usize>,

    /// Channel capacity for bounded sender-to-connection channels.
    /// Controls how many messages can be queued before the sender blocks.
    /// Higher values reduce contention; lower values provide earlier backpressure.
    /// Default: 1024.
    pub channel_capacity: usize,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            max_frame_length: 8 * 1024 * 1024,
            write_buffer_size: 8192,
            batch_drain_cap: 1024,
            buffer_capacity: 128,
            tcp_nodelay: true,
            retry_initial_delay: Duration::from_millis(50),
            retry_max_delay: Duration::from_secs(60),
            tcp_send_buffer: None,
            tcp_recv_buffer: None,
            channel_capacity: 1024,
        }
    }
}

impl Options {
    /// Returns an optimized configuration for high-throughput workloads.
    ///
    /// Increases write buffers, batch drain cap, and buffer capacity
    /// for maximum messages-per-second on fast networks.
    pub fn high_throughput() -> Self {
        Self {
            max_frame_length: 16 * 1024 * 1024,
            write_buffer_size: 65536,
            batch_drain_cap: 8192,
            buffer_capacity: 1024,
            tcp_nodelay: true,
            retry_initial_delay: Duration::from_millis(25),
            retry_max_delay: Duration::from_secs(30),
            tcp_send_buffer: Some(256 * 1024),
            tcp_recv_buffer: Some(256 * 1024),
            channel_capacity: 4096,
        }
    }

    /// Returns a configuration optimized for low-latency workloads.
    ///
    /// Smaller batch sizes to reduce head-of-line blocking,
    /// aggressive TCP_NODELAY, and fast retries.
    pub fn low_latency() -> Self {
        Self {
            max_frame_length: 8 * 1024 * 1024,
            write_buffer_size: 4096,
            batch_drain_cap: 256,
            buffer_capacity: 64,
            tcp_nodelay: true,
            retry_initial_delay: Duration::from_millis(10),
            retry_max_delay: Duration::from_secs(10),
            // Small kernel buffers reduce buffering latency
            tcp_send_buffer: Some(32 * 1024),
            tcp_recv_buffer: Some(32 * 1024),
            channel_capacity: 512,
        }
    }
}
