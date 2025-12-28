use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::engine::{QuotaResult, ShardedDb};
use crate::error::Result;
use crate::metrics::{CommandType, METRICS};
use crate::protocol::{Command, Frame};
use crate::replication::ReplicationHandle;
use crate::server::Connection;

/// Maximum retries for token acquisition
const QUOTA_RETRY_COUNT: usize = 3;
/// Delay between retries (milliseconds)
const QUOTA_RETRY_DELAY_MS: u64 = 2;

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
                let response = self.process_frame(frame).await;
                self.connection.write_frame_buffered(&response);
            }

            // Single flush for all responses
            self.connection.flush().await?;
        }
    }

    /// Process a single frame and return the response.
    async fn process_frame(&self, frame: Frame) -> Frame {
        match Command::from_frame(frame.clone()) {
            Ok(cmd) => {
                let start = Instant::now();
                let cmd_type = Self::command_type(&cmd);
                let response = self.execute_command(cmd).await;
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
            | Command::QuotaIncr { .. }
            | Command::QuotaGet(_)
            | Command::QuotaDel(_) => CommandType::Quota,
            _ => CommandType::Other,
        }
    }

    /// Execute a command and return the response frame.
    async fn execute_command(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Ping(msg) => {
                if let Some(msg) = msg {
                    Frame::Bulk(msg)
                } else {
                    Frame::pong()
                }
            }
            Command::Get(key) => {
                // Check for string first, then counter/quota
                if let Some(value) = self.db.get_string(&key) {
                    Frame::Bulk(value)
                } else if let Some(value) = self.db.get(&key) {
                    Frame::integer(value)
                } else {
                    Frame::Null
                }
            }
            Command::Incr(key) => {
                // INCR is always a PN-counter increment (use QUOTAINCR for rate limiting)
                let (shard_id, value, delta) = self.db.increment(key, 1);
                self.replicate(shard_id, delta);
                Frame::integer(value)
            }
            Command::IncrBy(key, delta) => {
                // INCRBY is always a PN-counter increment (use QUOTAINCR for rate limiting)
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
                // Compute shard routing
                let shard_id = (key.shard_hash() as u16) % 64;

                // Set string and get timestamp (generated once in shard)
                let timestamp = self.db.set_string_fast(shard_id, key.clone(), value.clone());

                // Replicate to other nodes if replication is enabled
                if let Some(ref handle) = self.replication {
                    handle.send_string(shard_id, key, value, timestamp);
                }

                Frame::ok()
            }
            Command::QuotaSet(key, limit, window_secs) => {
                // Compute shard_id for this key
                let shard_id = (key.shard_hash() as u16) % 64;

                // Set quota locally
                self.db.quota_set(key.clone(), limit, window_secs);

                // Replicate quota config to all nodes
                if let Some(ref handle) = self.replication {
                    handle.send_quota_sync(shard_id, key, limit, window_secs);
                }

                Frame::ok()
            }
            Command::QuotaIncr { key, limit, window_secs, amount } => {
                // Compute shard_id for this key
                let shard_id = (key.shard_hash() as u16) % 64;

                // Idempotent: ensure quota exists (only creates if not exists)
                let created = self.db.ensure_quota(key.clone(), limit, window_secs);

                // Only replicate if we created a new quota
                if created {
                    if let Some(ref handle) = self.replication {
                        handle.send_quota_sync(shard_id, key.clone(), limit, window_secs);
                    }
                    // Pre-request tokens if we're not the allocator for this shard
                    if !self.db.is_allocator(shard_id) {
                        self.request_tokens(shard_id, &key);
                    }
                }

                // Try to consume tokens with retry logic for cold start
                for retry in 0..=QUOTA_RETRY_COUNT {
                    let (shard_id, result) = self.db.quota_consume(&key, amount);
                    match result {
                        QuotaResult::Allowed(remaining) => return Frame::integer(remaining),
                        QuotaResult::Denied => return Frame::integer(-1),
                        QuotaResult::NeedTokens => {
                            // Request tokens from allocator
                            self.request_tokens(shard_id, &key);

                            if retry < QUOTA_RETRY_COUNT {
                                // Wait briefly for tokens to arrive, then retry
                                tokio::time::sleep(Duration::from_millis(QUOTA_RETRY_DELAY_MS)).await;
                            } else {
                                // Out of retries, deny this request
                                return Frame::integer(-1);
                            }
                        }
                        QuotaResult::NotQuota => {
                            return Frame::error("ERR internal error: quota not set");
                        }
                    }
                }
                // Should not reach here, but return -1 as fallback
                Frame::integer(-1)
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
            Command::ClusterInfo => {
                if let Some(ref handle) = self.replication {
                    let info = handle.cluster_info();
                    Frame::Bulk(bytes::Bytes::from(info.to_info_string()))
                } else {
                    // Not in cluster mode
                    let info = format!(
                        "# Cluster\ncluster_enabled:0\ncluster_node_id:{}\n",
                        self.db.node_id().as_u32()
                    );
                    Frame::Bulk(bytes::Bytes::from(info))
                }
            }
            Command::Select(_db) => {
                // QuotaDB only has one database, always return OK
                Frame::ok()
            }
            Command::Client => {
                // CLIENT commands - return OK for compatibility
                Frame::ok()
            }
            Command::CommandInfo => {
                // COMMAND - return empty array for compatibility
                Frame::Array(vec![])
            }
            Command::Hello => {
                // HELLO - return server info in RESP2 format for compatibility
                // go-redis expects a map-like response
                Frame::Array(vec![
                    Frame::Bulk(bytes::Bytes::from_static(b"server")),
                    Frame::Bulk(bytes::Bytes::from_static(b"quota-db")),
                    Frame::Bulk(bytes::Bytes::from_static(b"version")),
                    Frame::Bulk(bytes::Bytes::from_static(b"0.1.0")),
                    Frame::Bulk(bytes::Bytes::from_static(b"proto")),
                    Frame::integer(2), // RESP2 protocol
                    Frame::Bulk(bytes::Bytes::from_static(b"mode")),
                    Frame::Bulk(bytes::Bytes::from_static(b"standalone")),
                ])
            }
            Command::Echo(msg) => {
                Frame::Bulk(msg)
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

    /// Request tokens from the allocator node for a quota key.
    /// This triggers an async request - tokens will arrive later.
    #[inline]
    fn request_tokens(&self, shard_id: u16, key: &crate::types::Key) {
        if let Some(ref handle) = self.replication {
            let batch_size = self.db.quota_batch_size(key);
            tracing::info!("Requesting {} tokens for {:?} shard={}", batch_size, String::from_utf8_lossy(key.as_bytes()), shard_id);
            handle.request_tokens(shard_id, key.clone(), batch_size);
        } else {
            tracing::warn!("No replication handle, cannot request tokens for {:?}", String::from_utf8_lossy(key.as_bytes()));
        }
    }
}
