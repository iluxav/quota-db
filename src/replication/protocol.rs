use crate::replication::Delta;
use crate::types::{Key, NodeId};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Message types for replication protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// Batch of deltas from sender to receiver
    DeltaBatch = 1,
    /// Acknowledgment from receiver to sender
    Ack = 2,
    /// Initial handshake with node identity
    Hello = 3,
    /// Request tokens from allocator (quota)
    QuotaRequest = 4,
    /// Grant tokens to requesting node (quota)
    QuotaGrant = 5,
    /// Deny token request (quota exhausted)
    QuotaDeny = 6,
    /// Sync quota configuration to other nodes
    QuotaSync = 7,
}

impl MessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::DeltaBatch),
            2 => Some(Self::Ack),
            3 => Some(Self::Hello),
            4 => Some(Self::QuotaRequest),
            5 => Some(Self::QuotaGrant),
            6 => Some(Self::QuotaDeny),
            7 => Some(Self::QuotaSync),
            _ => None,
        }
    }
}

/// Replication protocol message
#[derive(Debug, Clone)]
pub enum Message {
    /// Batch of deltas for a specific shard
    DeltaBatch {
        shard_id: u16,
        deltas: Vec<Delta>,
    },
    /// Acknowledgment of received deltas
    Ack {
        shard_id: u16,
        acked_seq: u64,
    },
    /// Initial handshake
    Hello {
        node_id: NodeId,
    },
    /// Request tokens from allocator node
    QuotaRequest {
        shard_id: u16,
        key: Key,
        requested: u64,
    },
    /// Grant tokens to requesting node
    QuotaGrant {
        shard_id: u16,
        key: Key,
        granted: u64,
    },
    /// Deny token request (quota exhausted)
    QuotaDeny {
        shard_id: u16,
        key: Key,
    },
    /// Sync quota configuration to other nodes
    QuotaSync {
        shard_id: u16,
        key: Key,
        limit: u64,
        window_secs: u64,
    },
}

impl Message {
    /// Encode message to bytes with length prefix
    /// Frame format: len(4) + type(1) + payload(...)
    pub fn encode(&self, buf: &mut BytesMut) {
        // Reserve space for length prefix
        let len_pos = buf.len();
        buf.put_u32_le(0); // Placeholder

        match self {
            Message::DeltaBatch { shard_id, deltas } => {
                buf.put_u8(MessageType::DeltaBatch as u8);
                buf.put_u16_le(*shard_id);
                buf.put_u16_le(deltas.len() as u16);
                for delta in deltas {
                    delta.encode(buf);
                }
            }
            Message::Ack { shard_id, acked_seq } => {
                buf.put_u8(MessageType::Ack as u8);
                buf.put_u16_le(*shard_id);
                buf.put_u64_le(*acked_seq);
            }
            Message::Hello { node_id } => {
                buf.put_u8(MessageType::Hello as u8);
                buf.put_u32_le(node_id.as_u32());
            }
            Message::QuotaRequest {
                shard_id,
                key,
                requested,
            } => {
                buf.put_u8(MessageType::QuotaRequest as u8);
                buf.put_u16_le(*shard_id);
                let key_bytes = key.as_bytes();
                buf.put_u16_le(key_bytes.len() as u16);
                buf.put_slice(key_bytes);
                buf.put_u64_le(*requested);
            }
            Message::QuotaGrant {
                shard_id,
                key,
                granted,
            } => {
                buf.put_u8(MessageType::QuotaGrant as u8);
                buf.put_u16_le(*shard_id);
                let key_bytes = key.as_bytes();
                buf.put_u16_le(key_bytes.len() as u16);
                buf.put_slice(key_bytes);
                buf.put_u64_le(*granted);
            }
            Message::QuotaDeny { shard_id, key } => {
                buf.put_u8(MessageType::QuotaDeny as u8);
                buf.put_u16_le(*shard_id);
                let key_bytes = key.as_bytes();
                buf.put_u16_le(key_bytes.len() as u16);
                buf.put_slice(key_bytes);
            }
            Message::QuotaSync {
                shard_id,
                key,
                limit,
                window_secs,
            } => {
                buf.put_u8(MessageType::QuotaSync as u8);
                buf.put_u16_le(*shard_id);
                let key_bytes = key.as_bytes();
                buf.put_u16_le(key_bytes.len() as u16);
                buf.put_slice(key_bytes);
                buf.put_u64_le(*limit);
                buf.put_u64_le(*window_secs);
            }
        }

        // Write actual length (excluding the length field itself)
        let payload_len = buf.len() - len_pos - 4;
        let len_bytes = (payload_len as u32).to_le_bytes();
        buf[len_pos..len_pos + 4].copy_from_slice(&len_bytes);
    }

    /// Try to decode a message from bytes
    /// Returns None if not enough data, Some(message, consumed) if successful
    pub fn decode(buf: &mut Bytes) -> Option<Self> {
        if buf.remaining() < 5 {
            // Need at least length(4) + type(1)
            return None;
        }

        let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        if buf.remaining() < 4 + len {
            return None;
        }

        buf.advance(4); // Skip length
        let msg_type = MessageType::from_u8(buf.get_u8())?;

        match msg_type {
            MessageType::DeltaBatch => {
                if buf.remaining() < 4 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let count = buf.get_u16_le() as usize;

                let mut deltas = Vec::with_capacity(count);
                for _ in 0..count {
                    let delta = Delta::decode(buf)?;
                    deltas.push(delta);
                }

                Some(Message::DeltaBatch { shard_id, deltas })
            }
            MessageType::Ack => {
                if buf.remaining() < 10 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let acked_seq = buf.get_u64_le();
                Some(Message::Ack { shard_id, acked_seq })
            }
            MessageType::Hello => {
                if buf.remaining() < 4 {
                    return None;
                }
                let node_id = NodeId::new(buf.get_u32_le());
                Some(Message::Hello { node_id })
            }
            MessageType::QuotaRequest => {
                if buf.remaining() < 4 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let key_len = buf.get_u16_le() as usize;
                if buf.remaining() < key_len + 8 {
                    return None;
                }
                let key = Key::new(buf.copy_to_bytes(key_len));
                let requested = buf.get_u64_le();
                Some(Message::QuotaRequest {
                    shard_id,
                    key,
                    requested,
                })
            }
            MessageType::QuotaGrant => {
                if buf.remaining() < 4 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let key_len = buf.get_u16_le() as usize;
                if buf.remaining() < key_len + 8 {
                    return None;
                }
                let key = Key::new(buf.copy_to_bytes(key_len));
                let granted = buf.get_u64_le();
                Some(Message::QuotaGrant {
                    shard_id,
                    key,
                    granted,
                })
            }
            MessageType::QuotaDeny => {
                if buf.remaining() < 4 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let key_len = buf.get_u16_le() as usize;
                if buf.remaining() < key_len {
                    return None;
                }
                let key = Key::new(buf.copy_to_bytes(key_len));
                Some(Message::QuotaDeny { shard_id, key })
            }
            MessageType::QuotaSync => {
                if buf.remaining() < 4 {
                    return None;
                }
                let shard_id = buf.get_u16_le();
                let key_len = buf.get_u16_le() as usize;
                if buf.remaining() < key_len + 16 {
                    return None;
                }
                let key = Key::new(buf.copy_to_bytes(key_len));
                let limit = buf.get_u64_le();
                let window_secs = buf.get_u64_le();
                Some(Message::QuotaSync {
                    shard_id,
                    key,
                    limit,
                    window_secs,
                })
            }
        }
    }

    /// Create a DeltaBatch message
    pub fn delta_batch(shard_id: u16, deltas: Vec<Delta>) -> Self {
        Message::DeltaBatch { shard_id, deltas }
    }

    /// Create an Ack message
    pub fn ack(shard_id: u16, acked_seq: u64) -> Self {
        Message::Ack { shard_id, acked_seq }
    }

    /// Create a Hello message
    pub fn hello(node_id: NodeId) -> Self {
        Message::Hello { node_id }
    }

    /// Create a QuotaRequest message
    pub fn quota_request(shard_id: u16, key: Key, requested: u64) -> Self {
        Message::QuotaRequest {
            shard_id,
            key,
            requested,
        }
    }

    /// Create a QuotaGrant message
    pub fn quota_grant(shard_id: u16, key: Key, granted: u64) -> Self {
        Message::QuotaGrant {
            shard_id,
            key,
            granted,
        }
    }

    /// Create a QuotaDeny message
    pub fn quota_deny(shard_id: u16, key: Key) -> Self {
        Message::QuotaDeny { shard_id, key }
    }

    /// Create a QuotaSync message
    pub fn quota_sync(shard_id: u16, key: Key, limit: u64, window_secs: u64) -> Self {
        Message::QuotaSync {
            shard_id,
            key,
            limit,
            window_secs,
        }
    }

    /// Get the number of deltas in a DeltaBatch (0 for other message types)
    pub fn delta_count(&self) -> usize {
        match self {
            Message::DeltaBatch { deltas, .. } => deltas.len(),
            _ => 0,
        }
    }
}

/// Frame decoder for reading messages from a stream
pub struct FrameDecoder {
    buffer: BytesMut,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(8192),
        }
    }

    /// Add data to the buffer
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to decode the next message
    pub fn decode(&mut self) -> Option<Message> {
        if self.buffer.len() < 5 {
            return None;
        }

        let len = u32::from_le_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]) as usize;

        if self.buffer.len() < 4 + len {
            return None;
        }

        let mut frame = self.buffer.split_to(4 + len).freeze();
        Message::decode(&mut frame)
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Key;

    #[test]
    fn test_hello_encode_decode() {
        let msg = Message::hello(NodeId::new(42));

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::Hello { node_id } => {
                assert_eq!(node_id, NodeId::new(42));
            }
            _ => panic!("Expected Hello message"),
        }
    }

    #[test]
    fn test_ack_encode_decode() {
        let msg = Message::ack(7, 12345);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::Ack { shard_id, acked_seq } => {
                assert_eq!(shard_id, 7);
                assert_eq!(acked_seq, 12345);
            }
            _ => panic!("Expected Ack message"),
        }
    }

    #[test]
    fn test_delta_batch_encode_decode() {
        let deltas = vec![
            Delta::increment(0, Key::from("key1"), NodeId::new(1), 10),
            Delta::decrement(1, Key::from("key2"), NodeId::new(1), 5),
        ];
        let msg = Message::delta_batch(3, deltas);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::DeltaBatch { shard_id, deltas } => {
                assert_eq!(shard_id, 3);
                assert_eq!(deltas.len(), 2);
                assert_eq!(deltas[0].delta_p, 10);
                assert_eq!(deltas[1].delta_n, 5);
            }
            _ => panic!("Expected DeltaBatch message"),
        }
    }

    #[test]
    fn test_frame_decoder() {
        let msg1 = Message::hello(NodeId::new(1));
        let msg2 = Message::ack(0, 100);

        let mut buf = BytesMut::new();
        msg1.encode(&mut buf);
        msg2.encode(&mut buf);

        let mut decoder = FrameDecoder::new();
        decoder.extend(&buf);

        let decoded1 = decoder.decode().unwrap();
        assert!(matches!(decoded1, Message::Hello { .. }));

        let decoded2 = decoder.decode().unwrap();
        assert!(matches!(decoded2, Message::Ack { .. }));

        assert!(decoder.decode().is_none());
    }

    #[test]
    fn test_quota_request_encode_decode() {
        let msg = Message::quota_request(5, Key::from("api:user:123"), 1000);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::QuotaRequest {
                shard_id,
                key,
                requested,
            } => {
                assert_eq!(shard_id, 5);
                assert_eq!(key.as_bytes(), b"api:user:123");
                assert_eq!(requested, 1000);
            }
            _ => panic!("Expected QuotaRequest message"),
        }
    }

    #[test]
    fn test_quota_grant_encode_decode() {
        let msg = Message::quota_grant(5, Key::from("api:user:123"), 500);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::QuotaGrant {
                shard_id,
                key,
                granted,
            } => {
                assert_eq!(shard_id, 5);
                assert_eq!(key.as_bytes(), b"api:user:123");
                assert_eq!(granted, 500);
            }
            _ => panic!("Expected QuotaGrant message"),
        }
    }

    #[test]
    fn test_quota_deny_encode_decode() {
        let msg = Message::quota_deny(5, Key::from("api:user:123"));

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::QuotaDeny { shard_id, key } => {
                assert_eq!(shard_id, 5);
                assert_eq!(key.as_bytes(), b"api:user:123");
            }
            _ => panic!("Expected QuotaDeny message"),
        }
    }

    #[test]
    fn test_quota_sync_encode_decode() {
        let msg = Message::quota_sync(5, Key::from("api:user:123"), 10000, 60);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let mut bytes = buf.freeze();
        let decoded = Message::decode(&mut bytes).unwrap();

        match decoded {
            Message::QuotaSync {
                shard_id,
                key,
                limit,
                window_secs,
            } => {
                assert_eq!(shard_id, 5);
                assert_eq!(key.as_bytes(), b"api:user:123");
                assert_eq!(limit, 10000);
                assert_eq!(window_secs, 60);
            }
            _ => panic!("Expected QuotaSync message"),
        }
    }
}
