use std::fmt;
use std::io;

/// Unified error type for QuotaDB operations
#[derive(Debug)]
pub enum Error {
    /// I/O error from network operations
    Io(io::Error),

    /// RESP protocol parsing error
    Protocol(String),

    /// Unknown command received
    UnknownCommand(String),

    /// Connection closed by client
    ConnectionClosed,

    /// Incomplete frame (need more data)
    Incomplete,

    /// Invalid argument type or value
    InvalidArgument(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "IO error: {}", e),
            Error::Protocol(msg) => write!(f, "Protocol error: {}", msg),
            Error::UnknownCommand(cmd) => write!(f, "Unknown command: {}", cmd),
            Error::ConnectionClosed => write!(f, "Connection closed"),
            Error::Incomplete => write!(f, "Incomplete frame"),
            Error::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Error::ConnectionClosed
        } else {
            Error::Io(e)
        }
    }
}

/// Result type alias for QuotaDB operations
pub type Result<T> = std::result::Result<T, Error>;
