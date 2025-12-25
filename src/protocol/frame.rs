use bytes::Bytes;
use std::fmt;

/// RESP protocol frame representation.
///
/// Optimized for common cases:
/// - Vec for arrays (heap allocated but cache-friendly)
/// - Bytes for bulk strings enables zero-copy from network buffer
#[derive(Clone, PartialEq)]
pub enum Frame {
    /// Simple string (+OK\r\n)
    Simple(String),

    /// Error (-ERR message\r\n)
    Error(String),

    /// Integer (:123\r\n)
    Integer(i64),

    /// Bulk string ($5\r\nhello\r\n)
    Bulk(Bytes),

    /// Null bulk string ($-1\r\n)
    Null,

    /// Array (*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n)
    Array(Vec<Frame>),
}

impl Frame {
    /// Create an OK response
    #[inline]
    pub fn ok() -> Self {
        Frame::Simple("OK".to_string())
    }

    /// Create a PONG response
    #[inline]
    pub fn pong() -> Self {
        Frame::Simple("PONG".to_string())
    }

    /// Create an error response
    #[inline]
    pub fn error(msg: impl Into<String>) -> Self {
        Frame::Error(msg.into())
    }

    /// Create an integer response
    #[inline]
    pub fn integer(val: i64) -> Self {
        Frame::Integer(val)
    }

    /// Create a bulk string response
    #[inline]
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Frame::Bulk(data.into())
    }

    /// Create a null response
    #[inline]
    pub fn null() -> Self {
        Frame::Null
    }

    /// Create an array frame
    #[inline]
    pub fn array(frames: Vec<Frame>) -> Self {
        Frame::Array(frames)
    }

    /// Check if this frame is an error
    #[inline]
    pub fn is_error(&self) -> bool {
        matches!(self, Frame::Error(_))
    }

    /// Check if this frame is null
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Frame::Null)
    }

    /// Try to extract as integer
    #[inline]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            Frame::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract as bytes (from bulk string)
    #[inline]
    pub fn as_bytes(&self) -> Option<&Bytes> {
        match self {
            Frame::Bulk(b) => Some(b),
            _ => None,
        }
    }

    /// Try to extract as string (from simple or bulk)
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Frame::Simple(s) => Some(s),
            Frame::Bulk(b) => std::str::from_utf8(b).ok(),
            _ => None,
        }
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Frame::Simple(s) => write!(f, "Simple({:?})", s),
            Frame::Error(s) => write!(f, "Error({:?})", s),
            Frame::Integer(i) => write!(f, "Integer({})", i),
            Frame::Bulk(b) => match std::str::from_utf8(b) {
                Ok(s) => write!(f, "Bulk({:?})", s),
                Err(_) => write!(f, "Bulk({:?})", b),
            },
            Frame::Null => write!(f, "Null"),
            Frame::Array(arr) => write!(f, "Array({:?})", arr),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_ok() {
        let frame = Frame::ok();
        assert!(matches!(frame, Frame::Simple(s) if s == "OK"));
    }

    #[test]
    fn test_frame_pong() {
        let frame = Frame::pong();
        assert!(matches!(frame, Frame::Simple(s) if s == "PONG"));
    }

    #[test]
    fn test_frame_error() {
        let frame = Frame::error("test error");
        assert!(frame.is_error());
        assert!(matches!(frame, Frame::Error(s) if s == "test error"));
    }

    #[test]
    fn test_frame_integer() {
        let frame = Frame::integer(42);
        assert_eq!(frame.as_integer(), Some(42));
    }

    #[test]
    fn test_frame_bulk() {
        let frame = Frame::bulk(Bytes::from_static(b"hello"));
        assert_eq!(frame.as_bytes(), Some(&Bytes::from_static(b"hello")));
        assert_eq!(frame.as_str(), Some("hello"));
    }

    #[test]
    fn test_frame_null() {
        let frame = Frame::null();
        assert!(frame.is_null());
    }
}
