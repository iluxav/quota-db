//! PersistenceManager - orchestrates async WAL writers and snapshots.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, Mutex};
use tracing::{debug, info, warn};

use crate::metrics::METRICS;
use crate::persistence::{
    write_snapshot, AsyncWalWriter, PersistenceConfig, ShardSnapshot, WalEntry, WalOp,
};
use crate::types::Key;

/// Message for async WAL operations.
#[derive(Debug)]
pub struct WalMessage {
    pub shard_id: u16,
    pub entry: WalEntry,
}

/// Handle for sending WAL entries to the persistence manager.
#[derive(Clone)]
pub struct PersistenceHandle {
    tx: mpsc::Sender<WalMessage>,
    config: Arc<PersistenceConfig>,
}

impl PersistenceHandle {
    /// Log an increment operation.
    pub fn log_incr(&self, shard_id: u16, seq: u64, key: &Key, node_id: u32, delta: u64) {
        let entry = WalEntry {
            seq,
            timestamp: current_timestamp_millis(),
            op: WalOp::Incr {
                key: key.clone(),
                node_id,
                delta,
            },
        };
        self.send(shard_id, entry);
    }

    /// Log a decrement operation.
    pub fn log_decr(&self, shard_id: u16, seq: u64, key: &Key, node_id: u32, delta: u64) {
        let entry = WalEntry {
            seq,
            timestamp: current_timestamp_millis(),
            op: WalOp::Decr {
                key: key.clone(),
                node_id,
                delta,
            },
        };
        self.send(shard_id, entry);
    }

    /// Log a set operation.
    pub fn log_set(&self, shard_id: u16, seq: u64, key: &Key, value: i64) {
        let entry = WalEntry {
            seq,
            timestamp: current_timestamp_millis(),
            op: WalOp::Set {
                key: key.clone(),
                value,
            },
        };
        self.send(shard_id, entry);
    }

    /// Log a delete operation.
    pub fn log_delete(&self, shard_id: u16, seq: u64, key: &Key) {
        let entry = WalEntry {
            seq,
            timestamp: current_timestamp_millis(),
            op: WalOp::Delete { key: key.clone() },
        };
        self.send(shard_id, entry);
    }

    /// Log a quota set operation.
    pub fn log_quota_set(&self, shard_id: u16, seq: u64, key: &Key, limit: u64, window_secs: u64) {
        let entry = WalEntry {
            seq,
            timestamp: current_timestamp_millis(),
            op: WalOp::QuotaSet {
                key: key.clone(),
                limit,
                window_secs,
            },
        };
        self.send(shard_id, entry);
    }

    /// Log a quota grant operation.
    pub fn log_quota_grant(&self, shard_id: u16, seq: u64, key: &Key, node_id: u32, tokens: u64) {
        let entry = WalEntry {
            seq,
            timestamp: current_timestamp_millis(),
            op: WalOp::QuotaGrant {
                key: key.clone(),
                node_id,
                tokens,
            },
        };
        self.send(shard_id, entry);
    }

    /// Log a string set operation.
    pub fn log_set_string(&self, shard_id: u16, seq: u64, key: &Key, value: &[u8], timestamp: u64) {
        let entry = WalEntry {
            seq,
            timestamp: current_timestamp_millis(),
            op: WalOp::SetString {
                key: key.clone(),
                value: value.to_vec(),
                timestamp,
            },
        };
        self.send(shard_id, entry);
    }

    fn send(&self, shard_id: u16, entry: WalEntry) {
        let msg = WalMessage { shard_id, entry };
        // Use try_send to avoid blocking - if channel is full, track and warn
        if let Err(e) = self.tx.try_send(msg) {
            METRICS.inc(&METRICS.wal_dropped);
            warn!("WAL backpressure: dropped message for shard {}: {}", shard_id, e);
        }
    }

    /// Check if persistence is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

/// Manages per-shard async WAL writers and snapshot creation.
pub struct PersistenceManager {
    config: Arc<PersistenceConfig>,
    writers: Vec<Mutex<Option<AsyncWalWriter>>>,
    rx: mpsc::Receiver<WalMessage>,
    last_sync: Vec<Instant>,
    last_snapshot: Instant,
    num_shards: usize,
}

impl PersistenceManager {
    /// Create a new persistence manager.
    /// Writers are initialized lazily on first run.
    pub fn new(config: PersistenceConfig, num_shards: usize) -> (Self, PersistenceHandle) {
        let channel_size = config.wal_channel_size;
        let config = Arc::new(config);
        let (tx, rx) = mpsc::channel(channel_size);

        // Initialize empty writer slots (will be opened async in run())
        let writers: Vec<_> = (0..num_shards)
            .map(|_| Mutex::new(None))
            .collect();

        let last_sync: Vec<_> = (0..num_shards).map(|_| Instant::now()).collect();

        let handle = PersistenceHandle {
            tx,
            config: config.clone(),
        };

        let manager = Self {
            config,
            writers,
            rx,
            last_sync,
            last_snapshot: Instant::now(),
            num_shards,
        };

        (manager, handle)
    }

    /// Initialize async WAL writers for all shards.
    async fn init_writers(&self) {
        for i in 0..self.num_shards {
            let path = self.config.wal_path(i as u16);
            match AsyncWalWriter::open(&path, i as u16, self.config.wal_sync_ops).await {
                Ok(writer) => {
                    let mut guard = self.writers[i].lock().await;
                    *guard = Some(writer);
                }
                Err(e) => {
                    warn!("Failed to open async WAL for shard {}: {}", i, e);
                }
            }
        }
    }

    /// Run the persistence manager loop with async I/O.
    pub async fn run<F>(&mut self, mut get_shard_snapshot: F)
    where
        F: FnMut(u16) -> Option<ShardSnapshot>,
    {
        if !self.config.enabled {
            info!("Persistence disabled, manager exiting");
            return;
        }

        // Initialize writers asynchronously
        self.init_writers().await;
        info!("Persistence manager started with async I/O");

        let mut sync_interval = tokio::time::interval(self.config.wal_sync_interval);
        let mut snapshot_check = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                // Process incoming WAL messages
                Some(msg) = self.rx.recv() => {
                    self.handle_wal_message(msg).await;
                }

                // Periodic sync check
                _ = sync_interval.tick() => {
                    self.sync_all_writers().await;
                }

                // Periodic snapshot check
                _ = snapshot_check.tick() => {
                    self.check_snapshot_trigger(&mut get_shard_snapshot).await;
                }
            }
        }
    }

    async fn handle_wal_message(&mut self, msg: WalMessage) {
        let shard_id = msg.shard_id as usize;
        if shard_id >= self.writers.len() {
            warn!("Invalid shard_id {} in WAL message", shard_id);
            return;
        }

        let mut writer_guard = self.writers[shard_id].lock().await;
        if let Some(ref mut writer) = *writer_guard {
            if let Err(e) = writer.append(&msg.entry).await {
                warn!("Failed to append WAL entry for shard {}: {}", shard_id, e);
            } else {
                debug!(
                    "WAL entry appended: shard={}, seq={}",
                    shard_id, msg.entry.seq
                );

                // Check if we should sync based on ops threshold
                if writer.should_sync(self.config.wal_sync_interval) {
                    if let Err(e) = writer.sync().await {
                        METRICS.inc(&METRICS.wal_sync_failures);
                        if writer.is_degraded() {
                            warn!(
                                "WAL for shard {} entered degraded mode after {} consecutive failures: {}",
                                shard_id, writer.consecutive_sync_failures(), e
                            );
                        } else {
                            warn!("Failed to sync WAL for shard {}: {}", shard_id, e);
                        }
                    } else {
                        self.last_sync[shard_id] = Instant::now();
                    }
                }
            }
        }
    }

    async fn sync_all_writers(&mut self) {
        for shard_id in 0..self.writers.len() {
            if self.last_sync[shard_id].elapsed() >= self.config.wal_sync_interval {
                let mut writer_guard = self.writers[shard_id].lock().await;
                if let Some(ref mut writer) = *writer_guard {
                    if let Err(e) = writer.sync().await {
                        METRICS.inc(&METRICS.wal_sync_failures);
                        if writer.is_degraded() {
                            warn!(
                                "WAL for shard {} entered degraded mode after {} consecutive failures: {}",
                                shard_id, writer.consecutive_sync_failures(), e
                            );
                        } else {
                            warn!("Failed to sync WAL for shard {}: {}", shard_id, e);
                        }
                    } else {
                        self.last_sync[shard_id] = Instant::now();
                    }
                }
            }
        }
    }

    async fn check_snapshot_trigger<F>(&mut self, get_shard_snapshot: &mut F)
    where
        F: FnMut(u16) -> Option<ShardSnapshot>,
    {
        // Check time-based trigger
        let time_trigger = self.last_snapshot.elapsed() >= self.config.snapshot_interval;

        // Check size-based trigger (any shard exceeds threshold)
        let mut size_trigger = false;
        for i in 0..self.writers.len() {
            let guard = self.writers[i].lock().await;
            if let Some(ref writer) = *guard {
                if let Ok(size) = writer.file_size().await {
                    if size >= self.config.snapshot_wal_threshold {
                        size_trigger = true;
                        break;
                    }
                }
            }
        }

        if time_trigger || size_trigger {
            info!(
                "Snapshot triggered (time={}, size={})",
                time_trigger, size_trigger
            );
            self.create_snapshots(get_shard_snapshot);
            self.last_snapshot = Instant::now();
        }
    }

    fn create_snapshots<F>(&self, get_shard_snapshot: &mut F)
    where
        F: FnMut(u16) -> Option<ShardSnapshot>,
    {
        for shard_id in 0..self.writers.len() {
            if let Some(snapshot) = get_shard_snapshot(shard_id as u16) {
                let path = self.config.snapshot_path(shard_id as u16);
                match write_snapshot(&path, &snapshot) {
                    Ok(()) => {
                        info!(
                            "Snapshot created for shard {} at seq {}",
                            shard_id, snapshot.seq
                        );
                    }
                    Err(e) => {
                        warn!("Failed to create snapshot for shard {}: {}", shard_id, e);
                    }
                }
            }
        }
    }

    /// Sync all writers and shutdown.
    pub async fn shutdown(&mut self) {
        info!("Persistence manager shutting down");
        for shard_id in 0..self.writers.len() {
            let mut writer_guard = self.writers[shard_id].lock().await;
            if let Some(ref mut writer) = *writer_guard {
                if let Err(e) = writer.sync().await {
                    warn!("Failed to sync WAL for shard {} on shutdown: {}", shard_id, e);
                }
            }
        }
    }
}

fn current_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_persistence_handle_creation() {
        let config = PersistenceConfig::default();
        let (manager, handle) = PersistenceManager::new(config, 4);

        assert!(!handle.is_enabled());
        drop(manager);
    }

    #[test]
    fn test_persistence_handle_enabled() {
        let dir = std::env::temp_dir().join("quota-db-test-manager");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let (manager, handle) = PersistenceManager::new(config, 4);

        assert!(handle.is_enabled());
        drop(manager);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
