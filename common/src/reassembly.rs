use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};

use crate::fragment::FragmentHeader;

#[derive(Debug, thiserror::Error)]
pub enum ReassemblyError {
    #[error("Fragment total mismatch for msg_id {msg_id} from {sender}: expected {expected}, got {got}")]
    TotalMismatch {
        sender: SocketAddr,
        msg_id: u32,
        expected: u16,
        got: u16,
    },

    #[error("Too many pending reassemblies (max {max})")]
    TooManyPending { max: usize },
}

struct PendingMessage {
    fragments: Vec<Option<Bytes>>,
    total: u16,
    received: u16,
    created_at: Instant,
}

/// Reassembly buffer for fragmented UDP messages.
///
/// Keyed by `(SocketAddr, msg_id)`. Single-fragment messages (total_fragments == 1)
/// bypass the buffer and are returned immediately.
pub struct ReassemblyBuffer {
    pending: HashMap<(SocketAddr, u32), PendingMessage>,
    timeout: Duration,
    max_pending: usize,
}

impl ReassemblyBuffer {
    pub fn new(timeout: Duration, max_pending: usize) -> Self {
        Self {
            pending: HashMap::new(),
            timeout,
            max_pending,
        }
    }

    /// Insert a fragment. Returns `Some(complete_message)` when all fragments have arrived.
    ///
    /// Single-fragment messages (`total_fragments == 1`) are returned immediately without
    /// being stored in the buffer.
    ///
    /// Duplicate fragments are silently ignored (idempotent).
    pub fn insert(
        &mut self,
        sender: SocketAddr,
        header: FragmentHeader,
        payload: Bytes,
    ) -> Result<Option<Bytes>, ReassemblyError> {
        // Fast path: single-fragment message
        if header.total_fragments == 1 {
            return Ok(Some(payload));
        }

        let key = (sender, header.msg_id);

        if let Some(pending) = self.pending.get_mut(&key) {
            // Validate total_fragments consistency
            if pending.total != header.total_fragments {
                return Err(ReassemblyError::TotalMismatch {
                    sender,
                    msg_id: header.msg_id,
                    expected: pending.total,
                    got: header.total_fragments,
                });
            }

            let idx = header.fragment_index as usize;
            if pending.fragments[idx].is_none() {
                pending.fragments[idx] = Some(payload);
                pending.received += 1;
            }
            // else: duplicate fragment, silently ignore

            if pending.received == pending.total {
                let pending = self.pending.remove(&key).unwrap();
                return Ok(Some(Self::reassemble(pending)));
            }
        } else {
            // Check capacity before inserting new entry
            if self.pending.len() >= self.max_pending {
                return Err(ReassemblyError::TooManyPending {
                    max: self.max_pending,
                });
            }

            let total = header.total_fragments as usize;
            let mut fragments = vec![None; total];
            fragments[header.fragment_index as usize] = Some(payload);

            let pending = PendingMessage {
                fragments,
                total: header.total_fragments,
                received: 1,
                created_at: Instant::now(),
            };

            // Check if this single fragment completes the message (total == 1 handled above,
            // but be defensive)
            if pending.received == pending.total {
                return Ok(Some(Self::reassemble(pending)));
            }

            self.pending.insert(key, pending);
        }

        Ok(None)
    }

    /// Evict timed-out incomplete messages. Returns the number of evicted entries.
    pub fn evict_expired(&mut self) -> usize {
        let now = Instant::now();
        let timeout = self.timeout;
        let before = self.pending.len();
        self.pending
            .retain(|_, v| now.duration_since(v.created_at) < timeout);
        before - self.pending.len()
    }

    /// Number of currently pending (incomplete) message reassemblies.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    fn reassemble(pending: PendingMessage) -> Bytes {
        // Calculate total size
        let total_size: usize = pending
            .fragments
            .iter()
            .map(|f| f.as_ref().map_or(0, |b| b.len()))
            .sum();

        let mut buf = BytesMut::with_capacity(total_size);
        for fragment in pending.fragments {
            if let Some(data) = fragment {
                buf.extend_from_slice(&data);
            }
        }
        buf.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment::{fragment_message, FragmentHeader, FRAGMENT_HEADER_SIZE};

    fn localhost() -> SocketAddr {
        "127.0.0.1:8000".parse().unwrap()
    }

    #[test]
    fn test_single_fragment_bypass() {
        let mut buf = ReassemblyBuffer::new(Duration::from_secs(5), 100);
        let header = FragmentHeader {
            msg_id: 1,
            fragment_index: 0,
            total_fragments: 1,
        };
        let result = buf.insert(localhost(), header, Bytes::from("hello")).unwrap();
        assert_eq!(result, Some(Bytes::from("hello")));
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn test_multi_fragment_reassembly() {
        let mut buf = ReassemblyBuffer::new(Duration::from_secs(5), 100);
        let sender = localhost();

        // Insert fragment 0 of 3
        let result = buf
            .insert(
                sender,
                FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 3 },
                Bytes::from("aaa"),
            )
            .unwrap();
        assert!(result.is_none());
        assert_eq!(buf.pending_count(), 1);

        // Insert fragment 2 of 3 (out of order)
        let result = buf
            .insert(
                sender,
                FragmentHeader { msg_id: 1, fragment_index: 2, total_fragments: 3 },
                Bytes::from("ccc"),
            )
            .unwrap();
        assert!(result.is_none());

        // Insert fragment 1 of 3 (completes the message)
        let result = buf
            .insert(
                sender,
                FragmentHeader { msg_id: 1, fragment_index: 1, total_fragments: 3 },
                Bytes::from("bbb"),
            )
            .unwrap();
        assert_eq!(result, Some(Bytes::from("aaabbbccc")));
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn test_duplicate_fragment_idempotent() {
        let mut buf = ReassemblyBuffer::new(Duration::from_secs(5), 100);
        let sender = localhost();

        buf.insert(
            sender,
            FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 2 },
            Bytes::from("aaa"),
        )
        .unwrap();

        // Duplicate of fragment 0
        buf.insert(
            sender,
            FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 2 },
            Bytes::from("aaa"),
        )
        .unwrap();

        // Complete with fragment 1
        let result = buf
            .insert(
                sender,
                FragmentHeader { msg_id: 1, fragment_index: 1, total_fragments: 2 },
                Bytes::from("bbb"),
            )
            .unwrap();
        assert_eq!(result, Some(Bytes::from("aaabbb")));
    }

    #[test]
    fn test_total_mismatch_error() {
        let mut buf = ReassemblyBuffer::new(Duration::from_secs(5), 100);
        let sender = localhost();

        buf.insert(
            sender,
            FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 3 },
            Bytes::from("aaa"),
        )
        .unwrap();

        let result = buf.insert(
            sender,
            FragmentHeader { msg_id: 1, fragment_index: 1, total_fragments: 5 }, // mismatch!
            Bytes::from("bbb"),
        );
        assert!(matches!(result, Err(ReassemblyError::TotalMismatch { .. })));
    }

    #[test]
    fn test_too_many_pending() {
        let mut buf = ReassemblyBuffer::new(Duration::from_secs(5), 2);
        let sender = localhost();

        // Fill to capacity
        buf.insert(
            sender,
            FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 2 },
            Bytes::from("a"),
        )
        .unwrap();
        buf.insert(
            sender,
            FragmentHeader { msg_id: 2, fragment_index: 0, total_fragments: 2 },
            Bytes::from("b"),
        )
        .unwrap();

        // Third should fail
        let result = buf.insert(
            sender,
            FragmentHeader { msg_id: 3, fragment_index: 0, total_fragments: 2 },
            Bytes::from("c"),
        );
        assert!(matches!(
            result,
            Err(ReassemblyError::TooManyPending { max: 2 })
        ));
    }

    #[test]
    fn test_evict_expired() {
        let mut buf = ReassemblyBuffer::new(Duration::from_millis(10), 100);
        let sender = localhost();

        buf.insert(
            sender,
            FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 2 },
            Bytes::from("a"),
        )
        .unwrap();

        assert_eq!(buf.pending_count(), 1);
        std::thread::sleep(Duration::from_millis(20));
        let evicted = buf.evict_expired();
        assert_eq!(evicted, 1);
        assert_eq!(buf.pending_count(), 0);
    }

    #[test]
    fn test_different_senders_same_msg_id() {
        let mut buf = ReassemblyBuffer::new(Duration::from_secs(5), 100);
        let sender1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let sender2: SocketAddr = "127.0.0.1:8002".parse().unwrap();

        // Same msg_id from different senders should be tracked independently
        buf.insert(
            sender1,
            FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 2 },
            Bytes::from("a1"),
        )
        .unwrap();
        buf.insert(
            sender2,
            FragmentHeader { msg_id: 1, fragment_index: 0, total_fragments: 2 },
            Bytes::from("a2"),
        )
        .unwrap();
        assert_eq!(buf.pending_count(), 2);

        let r1 = buf
            .insert(
                sender1,
                FragmentHeader { msg_id: 1, fragment_index: 1, total_fragments: 2 },
                Bytes::from("b1"),
            )
            .unwrap();
        assert_eq!(r1, Some(Bytes::from("a1b1")));

        let r2 = buf
            .insert(
                sender2,
                FragmentHeader { msg_id: 1, fragment_index: 1, total_fragments: 2 },
                Bytes::from("b2"),
            )
            .unwrap();
        assert_eq!(r2, Some(Bytes::from("a2b2")));
    }

    #[test]
    fn test_fragment_and_reassemble_round_trip() {
        let mut buf = ReassemblyBuffer::new(Duration::from_secs(5), 100);
        let sender = localhost();
        let data = vec![0xABu8; 3000]; // ~3KB, will need multiple fragments

        let datagrams = fragment_message(42, &data, 1472).unwrap();
        assert!(datagrams.len() > 1);

        let mut result = None;
        for dgram in &datagrams {
            let header = FragmentHeader::decode(dgram).unwrap();
            let payload = Bytes::copy_from_slice(&dgram[FRAGMENT_HEADER_SIZE..]);
            result = buf.insert(sender, header, payload).unwrap();
        }

        assert_eq!(result.unwrap(), Bytes::from(data));
    }
}
