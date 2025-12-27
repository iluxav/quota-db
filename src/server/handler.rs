use std::sync::Arc;
use std::time::Instant;

use crate::engine::{QuotaResult, ShardedDb};
use crate::error::Result;
use crate::metrics::{CommandType, METRICS};
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
            Ok(cmd) => {
                let start = Instant::now();
                let cmd_type = Self::command_type(&cmd);
                let response = self.execute_command(cmd);
                METRICS.record_command(cmd_type, start);
                response
            }
            Err(e) => {
                tracing::debug!("Command parse error: {} for frame: {:?}", e, frame);
                METRICS.inc(&METRICS.errors_parse);
                Frame::error(format!("ERR {}", e))
            }
        }
    }

    /// Map a command to its metrics type.
    #[inline]
    fn command_type(cmd: &Command) -> CommandType {
        match cmd {
            Command::Incr(_) | Command::IncrBy(_, _) => CommandType::Incr,
            Command::Decr(_) | Command::DecrBy(_, _) => CommandType::Decr,
            Command::Get(_) => CommandType::Get,
            Command::Set(_, _) => CommandType::Set,
            Command::Ping(_) => CommandType::Ping,
            Command::Info(_) => CommandType::Info,
            Command::QuotaSet(_, _, _)
            | Command::QuotaGet(_)
            | Command::QuotaDel(_) => CommandType::Quota,
            _ => CommandType::Other,
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
                // Check if this is a quota key first
                if self.db.is_quota(&key) {
                    match self.db.quota_consume(&key, 1) {
                        QuotaResult::Allowed(remaining) => Frame::integer(remaining),
                        QuotaResult::Denied => Frame::integer(-1),
                        QuotaResult::NeedTokens => Frame::integer(-1), // Should not happen after quota_consume
                        QuotaResult::NotQuota => {
                            // Race: became non-quota, fall through to regular increment
                            let (shard_id, value, delta) = self.db.increment(key, 1);
                            self.replicate(shard_id, delta);
                            Frame::integer(value)
                        }
                    }
                } else {
                    let (shard_id, value, delta) = self.db.increment(key, 1);
                    self.replicate(shard_id, delta);
                    Frame::integer(value)
                }
            }
            Command::IncrBy(key, delta) => {
                // Check if this is a quota key first
                if self.db.is_quota(&key) {
                    if delta <= 0 {
                        // DECRBY on quota doesn't make sense
                        return Frame::error("ERR DECRBY not supported on quota keys");
                    }
                    match self.db.quota_consume(&key, delta as u64) {
                        QuotaResult::Allowed(remaining) => Frame::integer(remaining),
                        QuotaResult::Denied => Frame::integer(-1),
                        QuotaResult::NeedTokens => Frame::integer(-1),
                        QuotaResult::NotQuota => {
                            // Race: became non-quota, fall through to regular increment
                            let (shard_id, value, rep_delta) = self.db.increment(key, delta as u64);
                            self.replicate(shard_id, rep_delta);
                            Frame::integer(value)
                        }
                    }
                } else {
                    let (shard_id, value, rep_delta) = if delta >= 0 {
                        self.db.increment(key, delta as u64)
                    } else {
                        self.db.decrement(key, (-delta) as u64)
                    };
                    self.replicate(shard_id, rep_delta);
                    Frame::integer(value)
                }
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
            Command::QuotaSet(key, limit, window_secs) => {
                self.db.quota_set(key, limit, window_secs);
                Frame::ok()
            }
            Command::QuotaGet(key) => match self.db.quota_get(&key) {
                Some((limit, window_secs, remaining)) => Frame::Array(vec![
                    Frame::integer(limit as i64),
                    Frame::integer(window_secs as i64),
                    Frame::integer(remaining),
                ]),
                None => Frame::Null,
            },
            Command::QuotaDel(key) => {
                if self.db.quota_del(&key) {
                    Frame::integer(1)
                } else {
                    Frame::integer(0)
                }
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
            Command::Info(section) => {
                let section_str = section
                    .as_ref()
                    .map(|b| std::str::from_utf8(b).unwrap_or(""));
                let snapshot = METRICS.snapshot();
                let info = snapshot.to_info_string(section_str);
                Frame::Bulk(bytes::Bytes::from(info))
            }
            Command::Keys(pattern) => {
                let keys = self.db.keys(&pattern);
                let frames: Vec<Frame> = keys
                    .into_iter()
                    .map(|k| Frame::Bulk(k.into_bytes()))
                    .collect();
                Frame::Array(frames)
            }
            Command::Scan { cursor, pattern, count } => {
                let (next_cursor, keys) = self.db.scan(
                    cursor,
                    pattern.as_deref(),
                    count,
                );
                let key_frames: Vec<Frame> = keys
                    .into_iter()
                    .map(|k| Frame::Bulk(k.into_bytes()))
                    .collect();
                // SCAN returns [cursor, [keys...]]
                Frame::Array(vec![
                    Frame::Bulk(bytes::Bytes::from(next_cursor.to_string())),
                    Frame::Array(key_frames),
                ])
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
