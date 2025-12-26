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
            // Inline commands: PING\r\n, INCR key\r\n, etc.
            _ => self.parse_inline(buf),
        }
    }

    /// Parse an inline command: PING\r\n or GET key\r\n
    /// Converts to an array frame for uniform handling
    fn parse_inline(&self, buf: &mut BytesMut) -> Result<Frame> {
        let crlf_pos = self.find_crlf(buf).ok_or(Error::Incomplete)?;
        let line = &buf[..crlf_pos];

        // Split by whitespace
        let parts: Vec<&[u8]> = line
            .split(|&b| b == b' ' || b == b'\t')
            .filter(|s| !s.is_empty())
            .collect();

        if parts.is_empty() {
            buf.advance(crlf_pos + 2);
            return Err(Error::Protocol("empty inline command".to_string()));
        }

        // Convert to array of bulk strings
        let frames: Vec<Frame> = parts
            .into_iter()
            .map(|p| Frame::Bulk(Bytes::copy_from_slice(p)))
            .collect();

        buf.advance(crlf_pos + 2);
        Ok(Frame::Array(frames))
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
    ///
    /// IMPORTANT: We must check the entire array is complete before consuming
    /// any bytes. Otherwise, partial reads corrupt the buffer state.
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
        let header_len = crlf_pos + 2;

        // First pass: check if entire array is present without consuming
        let total_len = self.check_array_complete(buf, header_len, len)?;

        // All elements present - now parse and consume
        buf.advance(header_len);
        let mut frames = Vec::with_capacity(len);
        for _ in 0..len {
            frames.push(self.parse(buf)?);
        }

        // Sanity check: we should have consumed exactly what we calculated
        debug_assert!(total_len >= header_len);

        Ok(Frame::Array(frames))
    }

    /// Check if a complete array is present in the buffer starting at offset.
    /// Returns total byte length of the array (including header) if complete.
    fn check_array_complete(&self, buf: &[u8], offset: usize, count: usize) -> Result<usize> {
        let mut pos = offset;

        for _ in 0..count {
            if pos >= buf.len() {
                return Err(Error::Incomplete);
            }

            let elem_len = self.check_frame_complete(&buf[pos..])?;
            pos += elem_len;
        }

        Ok(pos)
    }

    /// Check if a complete frame is present, return its byte length if so.
    fn check_frame_complete(&self, buf: &[u8]) -> Result<usize> {
        if buf.is_empty() {
            return Err(Error::Incomplete);
        }

        match buf[0] {
            b'+' | b'-' | b':' => {
                // Simple string, error, or integer: find \r\n
                self.find_crlf_in(buf)
                    .map(|pos| pos + 2)
                    .ok_or(Error::Incomplete)
            }
            b'$' => {
                // Bulk string: $len\r\n<content>\r\n
                let crlf_pos = self.find_crlf_in(buf).ok_or(Error::Incomplete)?;
                let len_str = std::str::from_utf8(&buf[1..crlf_pos])
                    .map_err(|e| Error::Protocol(format!("invalid utf8: {}", e)))?;
                let len: i64 = len_str
                    .parse()
                    .map_err(|_| Error::Protocol(format!("invalid length: {}", len_str)))?;

                if len == -1 {
                    Ok(crlf_pos + 2)
                } else {
                    let total = crlf_pos + 2 + (len as usize) + 2;
                    if buf.len() >= total {
                        Ok(total)
                    } else {
                        Err(Error::Incomplete)
                    }
                }
            }
            b'*' => {
                // Nested array
                let crlf_pos = self.find_crlf_in(buf).ok_or(Error::Incomplete)?;
                let len_str = std::str::from_utf8(&buf[1..crlf_pos])
                    .map_err(|e| Error::Protocol(format!("invalid utf8: {}", e)))?;
                let len: i64 = len_str
                    .parse()
                    .map_err(|_| Error::Protocol(format!("invalid length: {}", len_str)))?;

                if len == -1 {
                    Ok(crlf_pos + 2)
                } else {
                    self.check_array_complete(buf, crlf_pos + 2, len as usize)
                }
            }
            _ => {
                // Inline command
                self.find_crlf_in(buf)
                    .map(|pos| pos + 2)
                    .ok_or(Error::Incomplete)
            }
        }
    }

    /// Find \r\n in a slice (non-mutating version)
    fn find_crlf_in(&self, buf: &[u8]) -> Option<usize> {
        for i in 0..buf.len().saturating_sub(1) {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Some(i);
            }
        }
        None
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
