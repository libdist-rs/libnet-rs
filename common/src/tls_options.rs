use std::path::PathBuf;
use std::time::Duration;

/// Post-quantum key exchange protocol selection for TLS 1.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PqProtocol {
    /// X25519MLKEM768: Hybrid post-quantum + classical (recommended).
    /// Combines X25519 (ECDH) with ML-KEM-768 (FIPS 203).
    X25519Mlkem768,

    /// ML-KEM-768: Pure post-quantum key exchange (FIPS 203).
    /// No classical fallback.
    Mlkem768,

    /// X25519: Classical TLS 1.3 key exchange.
    /// No post-quantum protection.
    X25519,
}

impl Default for PqProtocol {
    fn default() -> Self {
        PqProtocol::X25519Mlkem768
    }
}

/// Certificate source for TLS configuration.
#[derive(Debug, Clone)]
pub enum CertSource {
    /// Generate a self-signed certificate at runtime.
    /// The `Vec<String>` contains Subject Alternative Names (e.g., `["localhost", "127.0.0.1"]`).
    SelfSigned(Vec<String>),

    /// Load certificate chain and private key from PEM files.
    PemFiles {
        cert_chain: PathBuf,
        private_key: PathBuf,
    },
}

impl Default for CertSource {
    fn default() -> Self {
        CertSource::SelfSigned(vec!["localhost".to_string(), "127.0.0.1".to_string()])
    }
}

/// Configuration options for TLS networking components.
///
/// Combines TLS-specific settings (protocol, certificates) with TCP transport tuning.
#[derive(Debug, Clone)]
pub struct TlsOptions {
    /// Post-quantum key exchange protocol. Default: X25519Mlkem768.
    pub pq_protocol: PqProtocol,

    /// Certificate and key source. Default: SelfSigned(["localhost", "127.0.0.1"]).
    pub cert_source: CertSource,

    /// Skip server certificate verification (DANGER: testing only). Default: false.
    pub danger_skip_verify: bool,

    /// TLS server name for SNI. If None, uses "localhost" for loopback
    /// and the IP address for other addresses. Default: None.
    pub server_name: Option<String>,

    /// Maximum TLS plaintext fragment size in bytes. None = TLS maximum (16KB).
    /// Smaller values reduce latency (records decrypt sooner), larger values
    /// improve throughput (less per-record overhead). Default: None.
    pub max_fragment_size: Option<usize>,

    // ── TCP transport options ──

    /// Maximum frame size in bytes for LengthDelimitedCodec. Default: 8 MB.
    pub max_frame_length: usize,

    /// Initial write buffer capacity in bytes. Default: 8192.
    pub write_buffer_size: usize,

    /// Maximum messages to drain from channel per flush cycle. Default: 1024.
    pub batch_drain_cap: usize,

    /// Pre-allocation capacity for internal VecDeque buffers. Default: 128.
    pub buffer_capacity: usize,

    /// Whether to set TCP_NODELAY on the underlying socket. Default: true.
    pub tcp_nodelay: bool,

    /// Initial retry delay for reliable sender exponential backoff. Default: 50ms.
    pub retry_initial_delay: Duration,

    /// Maximum retry delay for reliable sender. Default: 60s.
    pub retry_max_delay: Duration,

    /// TCP send buffer size (SO_SNDBUF). None = OS default.
    pub tcp_send_buffer: Option<usize>,

    /// TCP receive buffer size (SO_RCVBUF). None = OS default.
    pub tcp_recv_buffer: Option<usize>,

    /// Channel capacity for bounded sender channels. Default: 1024.
    pub channel_capacity: usize,
}

impl Default for TlsOptions {
    fn default() -> Self {
        Self {
            pq_protocol: PqProtocol::default(),
            cert_source: CertSource::default(),
            danger_skip_verify: false,
            server_name: None,
            max_fragment_size: None,
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

impl TlsOptions {
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
            ..Self::default()
        }
    }

    pub fn low_latency() -> Self {
        Self {
            write_buffer_size: 4096,
            batch_drain_cap: 256,
            buffer_capacity: 64,
            tcp_nodelay: true,
            retry_initial_delay: Duration::from_millis(10),
            retry_max_delay: Duration::from_secs(10),
            tcp_send_buffer: Some(32 * 1024),
            tcp_recv_buffer: Some(32 * 1024),
            channel_capacity: 512,
            ..Self::default()
        }
    }
}
