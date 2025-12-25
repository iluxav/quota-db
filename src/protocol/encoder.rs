use bytes::{BufMut, BytesMut};

use crate::protocol::Frame;

/// RESP protocol encoder.
///
/// Encodes Frame values into the RESP wire format for sending to clients.
pub struct Encoder;

impl Encoder {
    /// Create a new encoder
    pub fn new() -> Self {
        Self
    }

    /// Encode a frame into the buffer
    pub fn encode(&self, frame: &Frame, buf: &mut BytesMut) {
        match frame {
            Frame::Simple(s) => {
                buf.put_u8(b'+');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Frame::Error(s) => {
                buf.put_u8(b'-');
                buf.put_slice(s.as_bytes());
                buf.put_slice(b"\r\n");
            }
            Frame::Integer(i) => {
                buf.put_u8(b':');
                // Use itoa for fast integer formatting
                let mut itoa_buf = itoa::Buffer::new();
                buf.put_slice(itoa_buf.format(*i).as_bytes());
                buf.put_slice(b"\r\n");
            }
            Frame::Bulk(b) => {
                buf.put_u8(b'$');
                let mut itoa_buf = itoa::Buffer::new();
                buf.put_slice(itoa_buf.format(b.len()).as_bytes());
                buf.put_slice(b"\r\n");
                buf.put_slice(b);
                buf.put_slice(b"\r\n");
            }
            Frame::Null => {
                buf.put_slice(b"$-1\r\n");
            }
            Frame::Array(arr) => {
                buf.put_u8(b'*');
                let mut itoa_buf = itoa::Buffer::new();
                buf.put_slice(itoa_buf.format(arr.len()).as_bytes());
                buf.put_slice(b"\r\n");
                for frame in arr {
                    self.encode(frame, buf);
                }
            }
        }
    }

    /// Encode a frame into a new BytesMut buffer
    pub fn encode_to_bytes(&self, frame: &Frame) -> BytesMut {
        let mut buf = BytesMut::with_capacity(64);
        self.encode(frame, &mut buf);
        buf
    }
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn test_encode_simple_string() {
        let encoder = Encoder::new();
        let frame = Frame::Simple("OK".to_string());
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let encoder = Encoder::new();
        let frame = Frame::Error("ERR unknown".to_string());
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b"-ERR unknown\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let encoder = Encoder::new();
        let frame = Frame::Integer(42);
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b":42\r\n");
    }

    #[test]
    fn test_encode_negative_integer() {
        let encoder = Encoder::new();
        let frame = Frame::Integer(-123);
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b":-123\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let encoder = Encoder::new();
        let frame = Frame::Bulk(Bytes::from_static(b"hello"));
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null() {
        let encoder = Encoder::new();
        let frame = Frame::Null;
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let encoder = Encoder::new();
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"foo")),
            Frame::Bulk(Bytes::from_static(b"bar")),
        ]);
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn test_encode_empty_array() {
        let encoder = Encoder::new();
        let frame = Frame::Array(vec![]);
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b"*0\r\n");
    }

    #[test]
    fn test_encode_pong() {
        let encoder = Encoder::new();
        let frame = Frame::pong();
        let buf = encoder.encode_to_bytes(&frame);
        assert_eq!(&buf[..], b"+PONG\r\n");
    }
}
