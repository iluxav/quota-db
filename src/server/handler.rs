use std::sync::Arc;

use crate::engine::ShardedDb;
use crate::error::Result;
use crate::protocol::{Command, Frame};
use crate::server::Connection;

/// Command handler for a single connection.
pub struct Handler {
    connection: Connection,
    db: Arc<ShardedDb>,
}

impl Handler {
    /// Create a new Handler
    pub fn new(connection: Connection, db: Arc<ShardedDb>) -> Self {
        Self { connection, db }
    }

    /// Run the handler loop, processing commands until the connection closes.
    /// Uses automatic batching: reads all available frames, processes them,
    /// then flushes all responses in a single syscall.
    pub async fn run(&mut self) -> Result<()> {
        loop {
            // Read all available frames (at least one, more if in buffer)
            let frames = self.connection.read_frames().await?;

            if frames.is_empty() {
                // Connection closed cleanly
                return Ok(());
            }

            // Process all frames and buffer responses
            for frame in frames {
                let response = self.process_frame(frame);
                self.connection.write_frame_buffered(&response);
            }

            // Single flush for all responses
            self.connection.flush().await?;
        }
    }

    /// Process a single frame and return the response.
    fn process_frame(&self, frame: Frame) -> Frame {
        match Command::from_frame(frame) {
            Ok(cmd) => self.execute_command(cmd),
            Err(e) => Frame::error(format!("ERR {}", e)),
        }
    }

    /// Execute a command and return the response frame.
    fn execute_command(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Ping(msg) => {
                if let Some(msg) = msg {
                    Frame::Bulk(msg)
                } else {
                    Frame::pong()
                }
            }
            Command::Get(key) => match self.db.get(&key) {
                Some(value) => Frame::integer(value),
                None => Frame::Null,
            },
            Command::Incr(key) => {
                let value = self.db.increment(key, 1);
                Frame::integer(value)
            }
            Command::IncrBy(key, delta) => {
                let value = if delta >= 0 {
                    self.db.increment(key, delta as u64)
                } else {
                    self.db.decrement(key, (-delta) as u64)
                };
                Frame::integer(value)
            }
            Command::Decr(key) => {
                let value = self.db.decrement(key, 1);
                Frame::integer(value)
            }
            Command::DecrBy(key, delta) => {
                let value = if delta >= 0 {
                    self.db.decrement(key, delta as u64)
                } else {
                    self.db.increment(key, (-delta) as u64)
                };
                Frame::integer(value)
            }
            Command::Set(key, value) => {
                self.db.set(key, value);
                Frame::ok()
            }
        }
    }
}
