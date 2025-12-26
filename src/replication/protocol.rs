use crate::replication::Delta;
use crate::types::NodeId;
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
}

impl MessageType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::DeltaBatch),
            2 => Some(Self::Ack),
            3 => Some(Self::Hello),
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
}
