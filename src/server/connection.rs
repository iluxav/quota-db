use bytes::BytesMut;
use smallvec::SmallVec;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

use crate::error::{Error, Result};
use crate::pool::buffer_pool;
use crate::protocol::{Encoder, Frame, Parser};

/// Buffer size for reading from the socket
const READ_BUFFER_SIZE: usize = 8192;

/// Write buffer size
const WRITE_BUFFER_SIZE: usize = 8192;

/// Default write timeout for slow clients (5 seconds)
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Per-connection handler managing read/write frames.
pub struct Connection {
    stream: BufWriter<TcpStream>,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    parser: Parser,
    encoder: Encoder,
}

impl Connection {
    /// Create a new Connection wrapping a TcpStream.
    /// Uses pooled buffers to reduce allocation overhead.
    pub fn new(stream: TcpStream) -> Self {
        // Disable Nagle's algorithm for lower latency
        let _ = stream.set_nodelay(true);

        // Get buffers from pool (reduces per-connection allocations)
        let pool = buffer_pool();
        let read_buffer = pool.get_with_capacity(READ_BUFFER_SIZE);
        let write_buffer = pool.get_with_capacity(WRITE_BUFFER_SIZE);

        Self {
            stream: BufWriter::with_capacity(WRITE_BUFFER_SIZE, stream),
            read_buffer,
            write_buffer,
            parser: Parser::new(),
            encoder: Encoder::new(),
        }
    }

    /// Read at least one frame, then return all complete frames available.
    /// This enables automatic batching without client changes.
    ///
    /// Returns `Ok(frames)` with one or more frames.
    /// Returns `Ok(empty)` if the connection was closed cleanly.
    /// Returns `Err` on protocol or I/O errors.
    pub async fn read_frames(&mut self) -> Result<SmallVec<[Frame; 8]>> {
        let mut frames = SmallVec::new();

        // First, ensure we have at least one complete frame (blocking read)
        loop {
            // Try to parse frames from buffered data
            while !self.read_buffer.is_empty() {
                match self.parser.parse(&mut self.read_buffer) {
                    Ok(frame) => {
                        frames.push(frame);
                        // Continue parsing - there might be more frames in buffer
                    }
                    Err(Error::Incomplete) => {
                        // No more complete frames in buffer
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }

            // If we have at least one frame, return all we have
            if !frames.is_empty() {
                return Ok(frames);
            }

            // Need to read more data from socket
            let bytes_read = self.stream.get_mut().read_buf(&mut self.read_buffer).await?;

            if bytes_read == 0 {
                // Connection closed
                if self.read_buffer.is_empty() {
                    return Ok(frames); // Return empty vec to signal clean close
                } else {
                    return Err(Error::ConnectionClosed);
                }
            }
        }
    }

    /// Write a frame to the internal buffer without flushing.
    /// Call `flush()` after writing all frames in a batch.
    #[inline]
    pub fn write_frame_buffered(&mut self, frame: &Frame) {
        self.encoder.encode(frame, &mut self.write_buffer);
    }

    /// Flush all buffered writes to the socket.
    pub async fn flush(&mut self) -> io::Result<()> {
        self.flush_with_timeout(DEFAULT_WRITE_TIMEOUT).await
    }

    /// Flush all buffered writes to the socket with a timeout.
    /// Protects against slow clients that don't consume data.
    pub async fn flush_with_timeout(&mut self, timeout: Duration) -> io::Result<()> {
        if !self.write_buffer.is_empty() {
            match tokio::time::timeout(timeout, self.stream.write_all(&self.write_buffer)).await {
                Ok(result) => result?,
                Err(_) => {
                    self.write_buffer.clear();
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "write timeout: slow client",
                    ));
                }
            }
            self.write_buffer.clear();
        }
        match tokio::time::timeout(timeout, self.stream.flush()).await {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "flush timeout: slow client",
            )),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // Return buffers to pool for reuse
        let pool = buffer_pool();
        pool.put(std::mem::take(&mut self.read_buffer));
        pool.put(std::mem::take(&mut self.write_buffer));
    }
}
