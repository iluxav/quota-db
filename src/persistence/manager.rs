//! PersistenceManager - orchestrates WAL writers and snapshots.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::metrics::METRICS;
use crate::persistence::{
    write_snapshot, PersistenceConfig, ShardSnapshot, WalEntry, WalOp, WalWriter,
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

/// Manages per-shard WAL writers and snapshot creation.
pub struct PersistenceManager {
    config: Arc<PersistenceConfig>,
    writers: Vec<RwLock<Option<WalWriter>>>,
    rx: mpsc::Receiver<WalMessage>,
    last_sync: Vec<Instant>,
    last_snapshot: Instant,
}

impl PersistenceManager {
    /// Create a new persistence manager.
    pub fn new(config: PersistenceConfig, num_shards: usize) -> (Self, PersistenceHandle) {
        let channel_size = config.wal_channel_size;
        let config = Arc::new(config);
        let (tx, rx) = mpsc::channel(channel_size);

        // Initialize per-shard writers
        let writers: Vec<_> = (0..num_shards)
            .map(|i| {
                if config.enabled {
                    let path = config.wal_path(i as u16);
                    match WalWriter::open(&path, i as u16, config.wal_sync_ops) {
                        Ok(writer) => RwLock::new(Some(writer)),
                        Err(e) => {
                            warn!("Failed to open WAL for shard {}: {}", i, e);
                            RwLock::new(None)
                        }
                    }
                } else {
                    RwLock::new(None)
                }
            })
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
        };

        (manager, handle)
    }

    /// Run the persistence manager loop.
    pub async fn run<F>(&mut self, mut get_shard_snapshot: F)
    where
        F: FnMut(u16) -> Option<ShardSnapshot>,
    {
        if !self.config.enabled {
            info!("Persistence disabled, manager exiting");
            return;
        }

        info!("Persistence manager started");

        let mut sync_interval = tokio::time::interval(self.config.wal_sync_interval);
        let mut snapshot_check = tokio::time::interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                // Process incoming WAL messages
                Some(msg) = self.rx.recv() => {
                    self.handle_wal_message(msg);
                }

                // Periodic sync check
                _ = sync_interval.tick() => {
                    self.sync_all_writers();
                }

                // Periodic snapshot check
                _ = snapshot_check.tick() => {
                    self.check_snapshot_trigger(&mut get_shard_snapshot);
                }
            }
        }
    }

    fn handle_wal_message(&mut self, msg: WalMessage) {
        let shard_id = msg.shard_id as usize;
        if shard_id >= self.writers.len() {
            warn!("Invalid shard_id {} in WAL message", shard_id);
            return;
        }

        let mut writer_guard = self.writers[shard_id].write();
        if let Some(ref mut writer) = *writer_guard {
            if let Err(e) = writer.append(&msg.entry) {
                warn!("Failed to append WAL entry for shard {}: {}", shard_id, e);
            } else {
                debug!(
                    "WAL entry appended: shard={}, seq={}",
                    shard_id, msg.entry.seq
                );

                // Check if we should sync based on ops threshold
                if writer.should_sync(self.config.wal_sync_interval) {
                    if let Err(e) = writer.sync() {
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

    fn sync_all_writers(&mut self) {
        for (shard_id, writer_lock) in self.writers.iter().enumerate() {
            let mut writer_guard = writer_lock.write();
            if let Some(ref mut writer) = *writer_guard {
                if self.last_sync[shard_id].elapsed() >= self.config.wal_sync_interval {
                    if let Err(e) = writer.sync() {
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

    fn check_snapshot_trigger<F>(&mut self, get_shard_snapshot: &mut F)
    where
        F: FnMut(u16) -> Option<ShardSnapshot>,
    {
        // Check time-based trigger
        let time_trigger = self.last_snapshot.elapsed() >= self.config.snapshot_interval;

        // Check size-based trigger (any shard exceeds threshold)
        let size_trigger = self.writers.iter().any(|w| {
            let guard = w.read();
            if let Some(ref writer) = *guard {
                writer
                    .file_size()
                    .map(|s| s >= self.config.snapshot_wal_threshold)
                    .unwrap_or(false)
            } else {
                false
            }
        });

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
    pub fn shutdown(&mut self) {
        info!("Persistence manager shutting down");
        for (shard_id, writer_lock) in self.writers.iter().enumerate() {
            let mut writer_guard = writer_lock.write();
            if let Some(ref mut writer) = *writer_guard {
                if let Err(e) = writer.sync() {
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
