use bytes::{BufMut, Bytes, BytesMut};

/// Size of the fragment header in bytes: msg_id(4) + fragment_index(2) + total_fragments(2).
pub const FRAGMENT_HEADER_SIZE: usize = 8;

/// Header prepended to each UDP datagram for fragmentation/reassembly.
///
/// Wire format (8 bytes, big-endian):
/// ```text
/// [msg_id: u32][fragment_index: u16][total_fragments: u16]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FragmentHeader {
    /// Message identifier. Monotonic per-sender counter (wraps at u32::MAX).
    /// Combined with the sender's SocketAddr, uniquely identifies a message.
    pub msg_id: u32,
    /// 0-based index of this fragment within the message.
    pub fragment_index: u16,
    /// Total number of fragments for this message. If 1, the message is unfragmented.
    pub total_fragments: u16,
}

#[derive(Debug, thiserror::Error)]
pub enum FragmentError {
    #[error("Buffer too short for fragment header: need {FRAGMENT_HEADER_SIZE} bytes, got {0}")]
    BufferTooShort(usize),

    #[error("total_fragments must be >= 1, got 0")]
    ZeroTotalFragments,

    #[error("fragment_index {index} >= total_fragments {total}")]
    IndexOutOfRange { index: u16, total: u16 },

    #[error("Message too large: {size} bytes would require {fragments} fragments (max {max})")]
    MessageTooLarge { size: usize, fragments: usize, max: u16 },
}

impl FragmentHeader {
    /// Encode the header into a buffer (appends 8 bytes).
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u32(self.msg_id);
        buf.put_u16(self.fragment_index);
        buf.put_u16(self.total_fragments);
    }

    /// Decode a header from the start of a byte slice.
    pub fn decode(buf: &[u8]) -> Result<Self, FragmentError> {
        if buf.len() < FRAGMENT_HEADER_SIZE {
            return Err(FragmentError::BufferTooShort(buf.len()));
        }
        let msg_id = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let fragment_index = u16::from_be_bytes([buf[4], buf[5]]);
        let total_fragments = u16::from_be_bytes([buf[6], buf[7]]);

        if total_fragments == 0 {
            return Err(FragmentError::ZeroTotalFragments);
        }
        if fragment_index >= total_fragments {
            return Err(FragmentError::IndexOutOfRange {
                index: fragment_index,
                total: total_fragments,
            });
        }

        Ok(Self {
            msg_id,
            fragment_index,
            total_fragments,
        })
    }
}

/// Split a message into datagrams, each at most `max_datagram_payload` bytes
/// (including the 8-byte fragment header).
///
/// # Panics
/// Panics if `max_datagram_payload <= FRAGMENT_HEADER_SIZE`.
pub fn fragment_message(
    msg_id: u32,
    data: &[u8],
    max_datagram_payload: usize,
) -> Result<Vec<Bytes>, FragmentError> {
    let max_payload_per_fragment = max_datagram_payload
        .checked_sub(FRAGMENT_HEADER_SIZE)
        .expect("max_datagram_payload must be > FRAGMENT_HEADER_SIZE");

    if data.is_empty() {
        // Empty message: single fragment with no payload
        let mut buf = BytesMut::with_capacity(FRAGMENT_HEADER_SIZE);
        let header = FragmentHeader {
            msg_id,
            fragment_index: 0,
            total_fragments: 1,
        };
        header.encode(&mut buf);
        return Ok(vec![buf.freeze()]);
    }

    let num_fragments = (data.len() + max_payload_per_fragment - 1) / max_payload_per_fragment;
    if num_fragments > u16::MAX as usize {
        return Err(FragmentError::MessageTooLarge {
            size: data.len(),
            fragments: num_fragments,
            max: u16::MAX,
        });
    }
    let total_fragments = num_fragments as u16;

    let mut datagrams = Vec::with_capacity(num_fragments);
    for i in 0..num_fragments {
        let start = i * max_payload_per_fragment;
        let end = std::cmp::min(start + max_payload_per_fragment, data.len());
        let payload = &data[start..end];

        let mut buf = BytesMut::with_capacity(FRAGMENT_HEADER_SIZE + payload.len());
        let header = FragmentHeader {
            msg_id,
            fragment_index: i as u16,
            total_fragments,
        };
        header.encode(&mut buf);
        buf.extend_from_slice(payload);
        datagrams.push(buf.freeze());
    }

    Ok(datagrams)
}

/// Maximum number of segments per GSO batch. macOS `sendmsg_x` supports up to 32
/// messages per syscall; quinn-udp enforces this via `debug_assert!`.
pub const MAX_GSO_SEGMENTS: usize = 32;

/// Like [`fragment_message`], but returns contiguous buffers suitable for
/// GSO (Generic Segmentation Offload) via quinn-udp's `Transmit.segment_size`.
///
/// Returns `Vec<(buffer, segment_size)>`:
/// - Each `buffer` contains up to [`MAX_GSO_SEGMENTS`] fragments concatenated
/// - `segment_size`: The size of each full fragment (header + payload). Pass as
///   `Transmit.segment_size`. 0 means single datagram (no GSO).
///
/// For messages requiring more than `MAX_GSO_SEGMENTS` fragments, multiple
/// batches are returned. Each batch (except possibly the last) contains exactly
/// `MAX_GSO_SEGMENTS` segments.
pub fn fragment_message_contiguous(
    msg_id: u32,
    data: &[u8],
    max_datagram_payload: usize,
) -> Result<Vec<(Bytes, usize)>, FragmentError> {
    let max_payload_per_fragment = max_datagram_payload
        .checked_sub(FRAGMENT_HEADER_SIZE)
        .expect("max_datagram_payload must be > FRAGMENT_HEADER_SIZE");

    if data.is_empty() {
        let mut buf = BytesMut::with_capacity(FRAGMENT_HEADER_SIZE);
        FragmentHeader { msg_id, fragment_index: 0, total_fragments: 1 }.encode(&mut buf);
        return Ok(vec![(buf.freeze(), 0)]);
    }

    let num_fragments = data.len().div_ceil(max_payload_per_fragment);
    if num_fragments > u16::MAX as usize {
        return Err(FragmentError::MessageTooLarge {
            size: data.len(),
            fragments: num_fragments,
            max: u16::MAX,
        });
    }
    let total_fragments = num_fragments as u16;

    // Single fragment: no GSO needed
    if num_fragments == 1 {
        let mut buf = BytesMut::with_capacity(FRAGMENT_HEADER_SIZE + data.len());
        FragmentHeader { msg_id, fragment_index: 0, total_fragments: 1 }.encode(&mut buf);
        buf.extend_from_slice(data);
        return Ok(vec![(buf.freeze(), 0)]);
    }

    // Multiple fragments: split into GSO batches of at most MAX_GSO_SEGMENTS
    let num_batches = num_fragments.div_ceil(MAX_GSO_SEGMENTS);
    let mut batches = Vec::with_capacity(num_batches);

    let mut frag_idx = 0usize;
    while frag_idx < num_fragments {
        let batch_end = std::cmp::min(frag_idx + MAX_GSO_SEGMENTS, num_fragments);
        let batch_count = batch_end - frag_idx;

        // Calculate buffer size for this batch
        let batch_data_start = frag_idx * max_payload_per_fragment;
        let batch_data_end = std::cmp::min(batch_end * max_payload_per_fragment, data.len());
        let batch_data_len = batch_data_end - batch_data_start;
        let buf_size = batch_count * FRAGMENT_HEADER_SIZE + batch_data_len;
        let mut buf = BytesMut::with_capacity(buf_size);

        for i in frag_idx..batch_end {
            let start = i * max_payload_per_fragment;
            let end = std::cmp::min(start + max_payload_per_fragment, data.len());
            FragmentHeader {
                msg_id,
                fragment_index: i as u16,
                total_fragments,
            }
            .encode(&mut buf);
            buf.extend_from_slice(&data[start..end]);
        }

        let seg_size = if batch_count == 1 { 0 } else { max_datagram_payload };
        batches.push((buf.freeze(), seg_size));
        frag_idx = batch_end;
    }

    Ok(batches)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_round_trip() {
        let header = FragmentHeader {
            msg_id: 42,
            fragment_index: 3,
            total_fragments: 10,
        };
        let mut buf = BytesMut::new();
        header.encode(&mut buf);
        assert_eq!(buf.len(), FRAGMENT_HEADER_SIZE);

        let decoded = FragmentHeader::decode(&buf).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn test_header_decode_too_short() {
        let buf = [0u8; 4];
        assert!(matches!(
            FragmentHeader::decode(&buf),
            Err(FragmentError::BufferTooShort(4))
        ));
    }

    #[test]
    fn test_header_decode_zero_total() {
        let mut buf = BytesMut::new();
        buf.put_u32(1);
        buf.put_u16(0);
        buf.put_u16(0); // total_fragments = 0
        assert!(matches!(
            FragmentHeader::decode(&buf),
            Err(FragmentError::ZeroTotalFragments)
        ));
    }

    #[test]
    fn test_header_decode_index_out_of_range() {
        let mut buf = BytesMut::new();
        buf.put_u32(1);
        buf.put_u16(5); // index = 5
        buf.put_u16(3); // total = 3
        assert!(matches!(
            FragmentHeader::decode(&buf),
            Err(FragmentError::IndexOutOfRange { index: 5, total: 3 })
        ));
    }

    #[test]
    fn test_fragment_small_message() {
        // Message fits in one datagram
        let data = b"hello world";
        let datagrams = fragment_message(1, data, 1472).unwrap();
        assert_eq!(datagrams.len(), 1);

        let header = FragmentHeader::decode(&datagrams[0]).unwrap();
        assert_eq!(header.msg_id, 1);
        assert_eq!(header.fragment_index, 0);
        assert_eq!(header.total_fragments, 1);
        assert_eq!(&datagrams[0][FRAGMENT_HEADER_SIZE..], b"hello world");
    }

    #[test]
    fn test_fragment_large_message() {
        // Use small max to force 3 fragments
        let data = vec![0xABu8; 30];
        let max_payload = 18; // 18 - 8 header = 10 bytes per fragment
        let datagrams = fragment_message(42, &data, max_payload).unwrap();
        assert_eq!(datagrams.len(), 3);

        for (i, dgram) in datagrams.iter().enumerate() {
            let header = FragmentHeader::decode(dgram).unwrap();
            assert_eq!(header.msg_id, 42);
            assert_eq!(header.fragment_index, i as u16);
            assert_eq!(header.total_fragments, 3);
        }

        // Verify payload reconstruction
        let mut reassembled = Vec::new();
        for dgram in &datagrams {
            reassembled.extend_from_slice(&dgram[FRAGMENT_HEADER_SIZE..]);
        }
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_fragment_empty_message() {
        let datagrams = fragment_message(0, b"", 1472).unwrap();
        assert_eq!(datagrams.len(), 1);
        let header = FragmentHeader::decode(&datagrams[0]).unwrap();
        assert_eq!(header.total_fragments, 1);
        assert_eq!(datagrams[0].len(), FRAGMENT_HEADER_SIZE);
    }

    #[test]
    fn test_fragment_exact_boundary() {
        // Message exactly fills one fragment payload
        let max_payload = 18; // 10 bytes of actual payload
        let data = vec![0xCC; 10];
        let datagrams = fragment_message(1, &data, max_payload).unwrap();
        assert_eq!(datagrams.len(), 1);
        assert_eq!(&datagrams[0][FRAGMENT_HEADER_SIZE..], &data[..]);
    }

    // ── contiguous fragmentation tests ──

    #[test]
    fn test_contiguous_single_fragment() {
        let data = b"hello world";
        let batches = fragment_message_contiguous(1, data, 1472).unwrap();
        assert_eq!(batches.len(), 1);
        let (buf, seg_size) = &batches[0];
        assert_eq!(*seg_size, 0); // no GSO for single fragment
        let header = FragmentHeader::decode(buf).unwrap();
        assert_eq!(header.total_fragments, 1);
        assert_eq!(&buf[FRAGMENT_HEADER_SIZE..], b"hello world");
    }

    #[test]
    fn test_contiguous_multi_fragment() {
        let data = vec![0xABu8; 30];
        let max_payload = 18; // 18 - 8 = 10 bytes per fragment → 3 fragments
        let batches = fragment_message_contiguous(42, &data, max_payload).unwrap();
        assert_eq!(batches.len(), 1); // 3 < MAX_GSO_SEGMENTS, single batch
        let (buf, seg_size) = &batches[0];
        assert_eq!(*seg_size, 18);

        // Verify each fragment by chunking the contiguous buffer
        let n = 3;
        let mut reassembled = Vec::new();
        let mut offset = 0;
        for i in 0..n {
            let end = if i < n - 1 {
                offset + seg_size
            } else {
                buf.len()
            };
            let chunk = &buf[offset..end];
            let header = FragmentHeader::decode(chunk).unwrap();
            assert_eq!(header.msg_id, 42);
            assert_eq!(header.fragment_index, i as u16);
            assert_eq!(header.total_fragments, 3);
            reassembled.extend_from_slice(&chunk[FRAGMENT_HEADER_SIZE..]);
            offset = end;
        }
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_contiguous_empty_message() {
        let batches = fragment_message_contiguous(0, b"", 1472).unwrap();
        assert_eq!(batches.len(), 1);
        let (buf, seg_size) = &batches[0];
        assert_eq!(*seg_size, 0);
        assert_eq!(buf.len(), FRAGMENT_HEADER_SIZE);
    }

    #[test]
    fn test_contiguous_matches_split() {
        // Verify contiguous buffer produces same wire bytes as split fragments
        let data = vec![0xFFu8; 50];
        let max_payload = 18;
        let split = fragment_message(7, &data, max_payload).unwrap();
        let batches = fragment_message_contiguous(7, &data, max_payload).unwrap();
        assert_eq!(batches.len(), 1); // 5 fragments < MAX_GSO_SEGMENTS
        let (contiguous, seg_size) = &batches[0];
        assert_eq!(split.len(), 5);
        assert_eq!(*seg_size, max_payload);

        // Concatenation of split fragments should equal contiguous buffer
        let mut concat = Vec::new();
        for dgram in &split {
            concat.extend_from_slice(dgram);
        }
        assert_eq!(&concat[..], &contiguous[..]);
    }

    #[test]
    fn test_contiguous_exceeds_batch_limit() {
        // Force more than MAX_GSO_SEGMENTS fragments
        // max_payload=18 → 10 bytes per fragment. Need >32 fragments → >320 bytes
        let data = vec![0xCCu8; 400]; // 400 / 10 = 40 fragments → 2 batches (32 + 8)
        let max_payload = 18;
        let batches = fragment_message_contiguous(99, &data, max_payload).unwrap();
        assert_eq!(batches.len(), 2);

        // First batch: 32 segments
        let (buf1, seg1) = &batches[0];
        assert_eq!(*seg1, max_payload);
        // Verify first fragment header
        let h0 = FragmentHeader::decode(buf1).unwrap();
        assert_eq!(h0.msg_id, 99);
        assert_eq!(h0.fragment_index, 0);
        assert_eq!(h0.total_fragments, 40);
        // Verify 32 segments fit: 32 * 18 = 576 bytes
        assert_eq!(buf1.len(), 32 * max_payload);

        // Second batch: 8 segments (last is shorter)
        let (buf2, seg2) = &batches[1];
        assert_eq!(*seg2, max_payload);
        let h32 = FragmentHeader::decode(buf2).unwrap();
        assert_eq!(h32.fragment_index, 32);
        assert_eq!(h32.total_fragments, 40);

        // Verify full reassembly across batches
        let split = fragment_message(99, &data, max_payload).unwrap();
        let mut concat_batches = Vec::new();
        for (buf, _) in &batches {
            concat_batches.extend_from_slice(buf);
        }
        let mut concat_split = Vec::new();
        for dgram in &split {
            concat_split.extend_from_slice(dgram);
        }
        assert_eq!(concat_batches, concat_split);
    }
}
