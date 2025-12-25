use bytes::{Buf, Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::protocol::Frame;

/// RESP protocol parser.
///
/// Implements a simple RESP2 parser for the subset we need.
pub struct Parser;

impl Parser {
    /// Create a new parser
    pub fn new() -> Self {
        Self
    }

    /// Try to parse a frame from the buffer.
    pub fn parse(&self, buf: &mut BytesMut) -> Result<Frame> {
        if buf.is_empty() {
            return Err(Error::Incomplete);
        }

        let first_byte = buf[0];
        match first_byte {
            b'+' => self.parse_simple_string(buf),
            b'-' => self.parse_error(buf),
            b':' => self.parse_integer(buf),
            b'$' => self.parse_bulk_string(buf),
            b'*' => self.parse_array(buf),
            _ => Err(Error::Protocol(format!("unknown type byte: {}", first_byte))),
        }
    }

    /// Find the position of \r\n in the buffer
    fn find_crlf(&self, buf: &[u8]) -> Option<usize> {
        for i in 0..buf.len().saturating_sub(1) {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
    }

    /// Parse a simple string: +OK\r\n
    fn parse_simple_string(&self, buf: &mut BytesMut) -> Result<Frame> {
        let crlf_pos = self.find_crlf(buf).ok_or(Error::Incomplete)?;
        let content = &buf[1..crlf_pos];
        let s = String::from_utf8(content.to_vec())
            .map_err(|e| Error::Protocol(format!("invalid utf8: {}", e)))?;
        buf.advance(crlf_pos + 2);
        Ok(Frame::Simple(s))
    }

    /// Parse an error: -ERR message\r\n
    fn parse_error(&self, buf: &mut BytesMut) -> Result<Frame> {
        let crlf_pos = self.find_crlf(buf).ok_or(Error::Incomplete)?;
        let content = &buf[1..crlf_pos];
        let s = String::from_utf8(content.to_vec())
            .map_err(|e| Error::Protocol(format!("invalid utf8: {}", e)))?;
        buf.advance(crlf_pos + 2);
        Ok(Frame::Error(s))
    }

    /// Parse an integer: :123\r\n
    fn parse_integer(&self, buf: &mut BytesMut) -> Result<Frame> {
        let crlf_pos = self.find_crlf(buf).ok_or(Error::Incomplete)?;
        let content = &buf[1..crlf_pos];
        let s = std::str::from_utf8(content)
            .map_err(|e| Error::Protocol(format!("invalid utf8: {}", e)))?;
        let n: i64 = s
            .parse()
            .map_err(|_| Error::Protocol(format!("invalid integer: {}", s)))?;
        buf.advance(crlf_pos + 2);
        Ok(Frame::Integer(n))
    }

    /// Parse a bulk string: $5\r\nhello\r\n or $-1\r\n for null
    fn parse_bulk_string(&self, buf: &mut BytesMut) -> Result<Frame> {
        let crlf_pos = self.find_crlf(buf).ok_or(Error::Incomplete)?;
        let len_str = std::str::from_utf8(&buf[1..crlf_pos])
            .map_err(|e| Error::Protocol(format!("invalid utf8: {}", e)))?;
        let len: i64 = len_str
            .parse()
            .map_err(|_| Error::Protocol(format!("invalid length: {}", len_str)))?;

        if len == -1 {
            buf.advance(crlf_pos + 2);
            return Ok(Frame::Null);
        }

        let len = len as usize;
        let total_len = crlf_pos + 2 + len + 2; // $len\r\n + content + \r\n

        if buf.len() < total_len {
            return Err(Error::Incomplete);
        }

        let content_start = crlf_pos + 2;
        let content = Bytes::copy_from_slice(&buf[content_start..content_start + len]);
        buf.advance(total_len);
        Ok(Frame::Bulk(content))
    }

    /// Parse an array: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
    fn parse_array(&self, buf: &mut BytesMut) -> Result<Frame> {
        let crlf_pos = self.find_crlf(buf).ok_or(Error::Incomplete)?;
        let len_str = std::str::from_utf8(&buf[1..crlf_pos])
            .map_err(|e| Error::Protocol(format!("invalid utf8: {}", e)))?;
        let len: i64 = len_str
            .parse()
            .map_err(|_| Error::Protocol(format!("invalid length: {}", len_str)))?;

        if len == -1 {
            buf.advance(crlf_pos + 2);
            return Ok(Frame::Null);
        }

        let len = len as usize;
        buf.advance(crlf_pos + 2);

        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            frames.push(self.parse(buf)?);
        }
        Ok(Frame::Array(frames))
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("+OK\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        assert!(matches!(frame, Frame::Simple(s) if s == "OK"));
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_error() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        assert!(matches!(frame, Frame::Error(s) if s == "ERR unknown command"));
    }

    #[test]
    fn test_parse_integer() {
        let parser = Parser::new();
        let mut buf = BytesMut::from(":1000\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        assert_eq!(frame.as_integer(), Some(1000));
    }

    #[test]
    fn test_parse_negative_integer() {
        let parser = Parser::new();
        let mut buf = BytesMut::from(":-42\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        assert_eq!(frame.as_integer(), Some(-42));
    }

    #[test]
    fn test_parse_bulk_string() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        assert_eq!(frame.as_str(), Some("hello"));
    }

    #[test]
    fn test_parse_null() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("$-1\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        assert!(frame.is_null());
    }

    #[test]
    fn test_parse_array() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("*2\r\n$4\r\nPING\r\n$4\r\ntest\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_str(), Some("PING"));
                assert_eq!(arr[1].as_str(), Some("test"));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_parse_incomplete() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("$5\r\nhel");
        let result = parser.parse(&mut buf);
        assert!(matches!(result, Err(Error::Incomplete)));
    }

    #[test]
    fn test_parse_empty() {
        let parser = Parser::new();
        let mut buf = BytesMut::new();
        let result = parser.parse(&mut buf);
        assert!(matches!(result, Err(Error::Incomplete)));
    }

    #[test]
    fn test_parse_ping_command() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("*1\r\n$4\r\nPING\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0].as_str(), Some("PING"));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn test_parse_incr_command() {
        let parser = Parser::new();
        let mut buf = BytesMut::from("*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n");
        let frame = parser.parse(&mut buf).unwrap();
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0].as_str(), Some("INCR"));
                assert_eq!(arr[1].as_str(), Some("counter"));
            }
            _ => panic!("expected array"),
        }
    }
}
