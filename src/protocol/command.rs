use bytes::Bytes;

use crate::error::{Error, Result};
use crate::protocol::Frame;
use crate::types::Key;

/// Parsed command from client.
///
/// Commands are parsed from RESP arrays and validated for correct
/// argument types and counts.
#[derive(Debug, Clone)]
pub enum Command {
    /// PING [message]
    Ping(Option<Bytes>),

    /// GET key
    Get(Key),

    /// INCR key (increment by 1)
    Incr(Key),

    /// INCRBY key delta
    IncrBy(Key, i64),

    /// DECR key (decrement by 1)
    Decr(Key),

    /// DECRBY key delta
    DecrBy(Key, i64),

    /// SET key value (for compatibility - sets counter to value)
    Set(Key, i64),

    /// QUOTASET key limit window_secs - set up a rate limit
    QuotaSet(Key, u64, u64),

    /// QUOTAGET key - get quota info (limit, window_secs, remaining)
    QuotaGet(Key),

    /// QUOTADEL key - delete quota, convert back to regular key
    QuotaDel(Key),

    /// CONFIG GET param - returns param name and empty value for compatibility
    ConfigGet(Bytes),

    /// CONFIG SET - no-op for compatibility
    ConfigSet,

    /// DBSIZE - return 0 for compatibility
    DbSize,

    /// FLUSHDB/FLUSHALL - stub for compatibility
    Flush,

    /// INFO [section] - return server statistics
    Info(Option<Bytes>),
}

impl Command {
    /// Parse a Frame into a Command
    pub fn from_frame(frame: Frame) -> Result<Self> {
        let array = match frame {
            Frame::Array(arr) => arr,
            _ => return Err(Error::Protocol("expected array".into())),
        };

        if array.is_empty() {
            return Err(Error::Protocol("empty command".into()));
        }

        // Extract command name
        let cmd_name = match &array[0] {
            Frame::Bulk(b) => b,
            _ => return Err(Error::Protocol("expected bulk string for command".into())),
        };

        // Case-insensitive command matching
        let cmd_upper: Vec<u8> = cmd_name.iter().map(|b| b.to_ascii_uppercase()).collect();

        match cmd_upper.as_slice() {
            b"PING" => {
                let msg = if array.len() > 1 {
                    Self::extract_bytes(&array, 1).ok()
                } else {
                    None
                };
                Ok(Command::Ping(msg))
            }
            b"GET" => {
                Self::ensure_args(&array, 2, "GET")?;
                let key = Self::extract_key(&array, 1)?;
                Ok(Command::Get(key))
            }
            b"INCR" => {
                Self::ensure_args(&array, 2, "INCR")?;
                let key = Self::extract_key(&array, 1)?;
                Ok(Command::Incr(key))
            }
            b"INCRBY" => {
                Self::ensure_args(&array, 3, "INCRBY")?;
                let key = Self::extract_key(&array, 1)?;
                let delta = Self::extract_i64(&array, 2)?;
                Ok(Command::IncrBy(key, delta))
            }
            b"DECR" => {
                Self::ensure_args(&array, 2, "DECR")?;
                let key = Self::extract_key(&array, 1)?;
                Ok(Command::Decr(key))
            }
            b"DECRBY" => {
                Self::ensure_args(&array, 3, "DECRBY")?;
                let key = Self::extract_key(&array, 1)?;
                let delta = Self::extract_i64(&array, 2)?;
                Ok(Command::DecrBy(key, delta))
            }
            b"SET" => {
                Self::ensure_args(&array, 3, "SET")?;
                let key = Self::extract_key(&array, 1)?;
                let value = Self::extract_i64(&array, 2)?;
                Ok(Command::Set(key, value))
            }
            b"QUOTASET" => {
                Self::ensure_args(&array, 4, "QUOTASET")?;
                let key = Self::extract_key(&array, 1)?;
                let limit = Self::extract_u64(&array, 2)?;
                let window_secs = Self::extract_u64(&array, 3)?;
                Ok(Command::QuotaSet(key, limit, window_secs))
            }
            b"QUOTAGET" => {
                Self::ensure_args(&array, 2, "QUOTAGET")?;
                let key = Self::extract_key(&array, 1)?;
                Ok(Command::QuotaGet(key))
            }
            b"QUOTADEL" => {
                Self::ensure_args(&array, 2, "QUOTADEL")?;
                let key = Self::extract_key(&array, 1)?;
                Ok(Command::QuotaDel(key))
            }
            b"CONFIG" => {
                // CONFIG GET param or CONFIG SET param value
                if array.len() >= 3 {
                    let subcommand = Self::extract_bytes(&array, 1)?;
                    let sub_upper: Vec<u8> = subcommand.iter().map(|b| b.to_ascii_uppercase()).collect();
                    match sub_upper.as_slice() {
                        b"GET" => {
                            let param = Self::extract_bytes(&array, 2)?;
                            Ok(Command::ConfigGet(param))
                        }
                        b"SET" => Ok(Command::ConfigSet),
                        _ => Ok(Command::ConfigSet), // Treat unknown subcommands as no-op
                    }
                } else {
                    Ok(Command::ConfigSet) // No-op for malformed CONFIG
                }
            }
            b"DBSIZE" => Ok(Command::DbSize),
            b"FLUSHDB" | b"FLUSHALL" => Ok(Command::Flush),
            b"INFO" => {
                let section = if array.len() > 1 {
                    Self::extract_bytes(&array, 1).ok()
                } else {
                    None
                };
                Ok(Command::Info(section))
            }
            _ => {
                let cmd_str = String::from_utf8_lossy(cmd_name);
                Err(Error::UnknownCommand(cmd_str.to_string()))
            }
        }
    }

    /// Ensure the array has exactly the expected number of arguments
    fn ensure_args(array: &[Frame], expected: usize, cmd: &str) -> Result<()> {
        if array.len() != expected {
            return Err(Error::InvalidArgument(format!(
                "wrong number of arguments for '{}' command",
                cmd
            )));
        }
        Ok(())
    }

    /// Extract a Key from the array at the given index
    fn extract_key(array: &[Frame], idx: usize) -> Result<Key> {
        match array.get(idx) {
            Some(Frame::Bulk(b)) => Ok(Key::new(b.clone())),
            _ => Err(Error::InvalidArgument("expected key".into())),
        }
    }

    /// Extract raw Bytes from the array at the given index
    fn extract_bytes(array: &[Frame], idx: usize) -> Result<Bytes> {
        match array.get(idx) {
            Some(Frame::Bulk(b)) => Ok(b.clone()),
            _ => Err(Error::InvalidArgument("expected bulk string".into())),
        }
    }

    /// Extract an i64 from the array at the given index
    fn extract_i64(array: &[Frame], idx: usize) -> Result<i64> {
        match array.get(idx) {
            Some(Frame::Bulk(b)) => std::str::from_utf8(b)
                .map_err(|_| Error::InvalidArgument("invalid utf8".into()))?
                .parse::<i64>()
                .map_err(|_| Error::InvalidArgument("value is not an integer".into())),
            Some(Frame::Integer(i)) => Ok(*i),
            _ => Err(Error::InvalidArgument("expected integer".into())),
        }
    }

    /// Extract a u64 from the array at the given index
    fn extract_u64(array: &[Frame], idx: usize) -> Result<u64> {
        match array.get(idx) {
            Some(Frame::Bulk(b)) => std::str::from_utf8(b)
                .map_err(|_| Error::InvalidArgument("invalid utf8".into()))?
                .parse::<u64>()
                .map_err(|_| Error::InvalidArgument("value is not a positive integer".into())),
            Some(Frame::Integer(i)) if *i >= 0 => Ok(*i as u64),
            Some(Frame::Integer(_)) => {
                Err(Error::InvalidArgument("value must be positive".into()))
            }
            _ => Err(Error::InvalidArgument("expected positive integer".into())),
        }
    }

    /// Get the command name for logging/debugging
    pub fn name(&self) -> &'static str {
        match self {
            Command::Ping(_) => "PING",
            Command::Get(_) => "GET",
            Command::Incr(_) => "INCR",
            Command::IncrBy(_, _) => "INCRBY",
            Command::Decr(_) => "DECR",
            Command::DecrBy(_, _) => "DECRBY",
            Command::Set(_, _) => "SET",
            Command::QuotaSet(_, _, _) => "QUOTASET",
            Command::QuotaGet(_) => "QUOTAGET",
            Command::QuotaDel(_) => "QUOTADEL",
            Command::ConfigGet(_) => "CONFIG GET",
            Command::ConfigSet => "CONFIG SET",
            Command::DbSize => "DBSIZE",
            Command::Flush => "FLUSH",
            Command::Info(_) => "INFO",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Frame {
        Frame::Bulk(Bytes::from(s.to_string()))
    }

    #[test]
    fn test_parse_ping() {
        let frame = Frame::Array(vec![bulk("PING")]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::Ping(None)));
    }

    #[test]
    fn test_parse_ping_with_message() {
        let frame = Frame::Array(vec![bulk("PING"), bulk("hello")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Ping(Some(msg)) => assert_eq!(&msg[..], b"hello"),
            _ => panic!("expected Ping with message"),
        }
    }

    #[test]
    fn test_parse_ping_lowercase() {
        let frame = Frame::Array(vec![bulk("ping")]);
        let cmd = Command::from_frame(frame).unwrap();
        assert!(matches!(cmd, Command::Ping(None)));
    }

    #[test]
    fn test_parse_get() {
        let frame = Frame::Array(vec![bulk("GET"), bulk("mykey")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Get(key) => assert_eq!(key.as_bytes(), b"mykey"),
            _ => panic!("expected Get"),
        }
    }

    #[test]
    fn test_parse_incr() {
        let frame = Frame::Array(vec![bulk("INCR"), bulk("counter")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Incr(key) => assert_eq!(key.as_bytes(), b"counter"),
            _ => panic!("expected Incr"),
        }
    }

    #[test]
    fn test_parse_incrby() {
        let frame = Frame::Array(vec![bulk("INCRBY"), bulk("counter"), bulk("10")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::IncrBy(key, delta) => {
                assert_eq!(key.as_bytes(), b"counter");
                assert_eq!(delta, 10);
            }
            _ => panic!("expected IncrBy"),
        }
    }

    #[test]
    fn test_parse_incrby_negative() {
        let frame = Frame::Array(vec![bulk("INCRBY"), bulk("counter"), bulk("-5")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::IncrBy(key, delta) => {
                assert_eq!(key.as_bytes(), b"counter");
                assert_eq!(delta, -5);
            }
            _ => panic!("expected IncrBy"),
        }
    }

    #[test]
    fn test_parse_decr() {
        let frame = Frame::Array(vec![bulk("DECR"), bulk("counter")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Decr(key) => assert_eq!(key.as_bytes(), b"counter"),
            _ => panic!("expected Decr"),
        }
    }

    #[test]
    fn test_parse_decrby() {
        let frame = Frame::Array(vec![bulk("DECRBY"), bulk("counter"), bulk("3")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::DecrBy(key, delta) => {
                assert_eq!(key.as_bytes(), b"counter");
                assert_eq!(delta, 3);
            }
            _ => panic!("expected DecrBy"),
        }
    }

    #[test]
    fn test_parse_set() {
        let frame = Frame::Array(vec![bulk("SET"), bulk("counter"), bulk("100")]);
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::Set(key, value) => {
                assert_eq!(key.as_bytes(), b"counter");
                assert_eq!(value, 100);
            }
            _ => panic!("expected Set"),
        }
    }

    #[test]
    fn test_parse_unknown_command() {
        let frame = Frame::Array(vec![bulk("UNKNOWN")]);
        let result = Command::from_frame(frame);
        assert!(matches!(result, Err(Error::UnknownCommand(_))));
    }

    #[test]
    fn test_parse_wrong_arg_count() {
        let frame = Frame::Array(vec![bulk("GET")]);
        let result = Command::from_frame(frame);
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn test_parse_invalid_integer() {
        let frame = Frame::Array(vec![bulk("INCRBY"), bulk("counter"), bulk("notanumber")]);
        let result = Command::from_frame(frame);
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn test_parse_empty_command() {
        let frame = Frame::Array(vec![]);
        let result = Command::from_frame(frame);
        assert!(matches!(result, Err(Error::Protocol(_))));
    }

    #[test]
    fn test_parse_non_array() {
        let frame = Frame::Simple("PING".to_string());
        let result = Command::from_frame(frame);
        assert!(matches!(result, Err(Error::Protocol(_))));
    }

    #[test]
    fn test_command_name() {
        assert_eq!(Command::Ping(None).name(), "PING");
        assert_eq!(Command::Get(Key::from("k")).name(), "GET");
        assert_eq!(Command::Incr(Key::from("k")).name(), "INCR");
        assert_eq!(Command::IncrBy(Key::from("k"), 1).name(), "INCRBY");
        assert_eq!(Command::Decr(Key::from("k")).name(), "DECR");
        assert_eq!(Command::DecrBy(Key::from("k"), 1).name(), "DECRBY");
        assert_eq!(Command::Set(Key::from("k"), 1).name(), "SET");
    }
}
