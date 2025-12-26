use std::sync::Arc;

use crate::engine::ShardedDb;
use crate::error::Result;
use crate::protocol::{Command, Frame};
use crate::replication::ReplicationHandle;
use crate::server::Connection;

/// Command handler for a single connection.
pub struct Handler {
    connection: Connection,
    db: Arc<ShardedDb>,
    replication: Option<ReplicationHandle>,
}

impl Handler {
    /// Create a new Handler
    pub fn new(connection: Connection, db: Arc<ShardedDb>) -> Self {
        Self {
            connection,
            db,
            replication: None,
        }
    }

    /// Create a new Handler with replication enabled
    pub fn with_replication(
        connection: Connection,
        db: Arc<ShardedDb>,
        replication: ReplicationHandle,
    ) -> Self {
        Self {
            connection,
            db,
            replication: Some(replication),
        }
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
        match Command::from_frame(frame.clone()) {
            Ok(cmd) => self.execute_command(cmd),
            Err(e) => {
                tracing::debug!("Command parse error: {} for frame: {:?}", e, frame);
                Frame::error(format!("ERR {}", e))
            }
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
                let (shard_id, value, delta) = self.db.increment(key, 1);
                self.replicate(shard_id, delta);
                Frame::integer(value)
            }
            Command::IncrBy(key, delta) => {
                let (shard_id, value, rep_delta) = if delta >= 0 {
                    self.db.increment(key, delta as u64)
                } else {
                    self.db.decrement(key, (-delta) as u64)
                };
                self.replicate(shard_id, rep_delta);
                Frame::integer(value)
            }
            Command::Decr(key) => {
                let (shard_id, value, delta) = self.db.decrement(key, 1);
                self.replicate(shard_id, delta);
                Frame::integer(value)
            }
            Command::DecrBy(key, delta) => {
                let (shard_id, value, rep_delta) = if delta >= 0 {
                    self.db.decrement(key, delta as u64)
                } else {
                    self.db.increment(key, (-delta) as u64)
                };
                self.replicate(shard_id, rep_delta);
                Frame::integer(value)
            }
            Command::Set(key, value) => {
                self.db.set(key, value);
                // TODO: SET doesn't emit a delta in current implementation
                Frame::ok()
            }
            Command::ConfigGet(param) => {
                // Return [param_name, value] for CONFIG GET
                // redis-benchmark checks these for compatibility
                let param_str = String::from_utf8_lossy(&param);
                let value = match param_str.as_ref() {
                    "save" => "",           // No persistence
                    "appendonly" => "no",   // No AOF
                    _ => "",                // Unknown params return empty
                };
                Frame::Array(vec![
                    Frame::Bulk(param.clone()),
                    Frame::Bulk(bytes::Bytes::from(value)),
                ])
            }
            Command::ConfigSet => {
                // CONFIG SET is a no-op, return OK
                Frame::ok()
            }
            Command::DbSize => {
                Frame::integer(self.db.total_entries() as i64)
            }
            Command::Flush => {
                // No-op for now (counter db doesn't support flush)
                Frame::ok()
            }
        }
    }

    /// Send a delta to the replication manager (if enabled).
    #[inline]
    fn replicate(&self, shard_id: u16, delta: crate::replication::Delta) {
        if let Some(ref handle) = self.replication {
            handle.send(shard_id, delta);
        }
    }
}
