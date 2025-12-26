use crate::types::{Key, NodeId};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Incremental delta for replication.
/// Represents a single mutation (increment or decrement) to be replicated.
#[derive(Debug, Clone)]
pub struct Delta {
    /// Sequence number in the replication log
    pub seq: u64,
    /// The counter key
    pub key: Key,
    /// Which node made the change
    pub node_id: NodeId,
    /// Increment amount (0 if this is a decrement)
    pub delta_p: u64,
    /// Decrement amount (0 if this is an increment)
    pub delta_n: u64,
}

impl Delta {
    /// Create a new increment delta
    #[inline]
    pub fn increment(seq: u64, key: Key, node_id: NodeId, delta: u64) -> Self {
        Self {
            seq,
            key,
            node_id,
            delta_p: delta,
            delta_n: 0,
        }
    }

    /// Create a new decrement delta
    #[inline]
    pub fn decrement(seq: u64, key: Key, node_id: NodeId, delta: u64) -> Self {
        Self {
            seq,
            key,
            node_id,
            delta_p: 0,
            delta_n: delta,
        }
    }

    /// Encode delta to bytes
    /// Format: seq(8) + node_id(4) + delta_p(8) + delta_n(8) + key_len(2) + key(...)
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.seq);
        buf.put_u32_le(self.node_id.as_u32());
        buf.put_u64_le(self.delta_p);
        buf.put_u64_le(self.delta_n);
        let key_bytes = self.key.as_bytes();
        buf.put_u16_le(key_bytes.len() as u16);
        buf.put_slice(key_bytes);
    }

    /// Decode delta from bytes
    pub fn decode(buf: &mut Bytes) -> Option<Self> {
        if buf.remaining() < 30 {
            // Minimum: 8 + 4 + 8 + 8 + 2 = 30 bytes
            return None;
        }

        let seq = buf.get_u64_le();
        let node_id = NodeId::new(buf.get_u32_le());
        let delta_p = buf.get_u64_le();
        let delta_n = buf.get_u64_le();
        let key_len = buf.get_u16_le() as usize;

        if buf.remaining() < key_len {
            return None;
        }

        let key = Key::from(buf.copy_to_bytes(key_len));

        Some(Self {
            seq,
            key,
            node_id,
            delta_p,
            delta_n,
        })
    }

    /// Get the encoded size of this delta
    #[inline]
    pub fn encoded_size(&self) -> usize {
        30 + self.key.as_bytes().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_encode_decode() {
        let delta = Delta::increment(42, Key::from("test_key"), NodeId::new(1), 100);

        let mut buf = BytesMut::new();
        delta.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Delta::decode(&mut bytes).unwrap();

        assert_eq!(decoded.seq, 42);
        assert_eq!(decoded.node_id, NodeId::new(1));
        assert_eq!(decoded.delta_p, 100);
        assert_eq!(decoded.delta_n, 0);
        assert_eq!(decoded.key.as_bytes(), b"test_key");
    }

    #[test]
    fn test_delta_decrement() {
        let delta = Delta::decrement(1, Key::from("counter"), NodeId::new(2), 50);

        assert_eq!(delta.delta_p, 0);
        assert_eq!(delta.delta_n, 50);
    }
}
