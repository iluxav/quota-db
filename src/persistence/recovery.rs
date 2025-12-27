//! Recovery module for restoring shard state from persistence files.
//!
//! Provides recovery logic that:
//! 1. Loads snapshot if available
//! 2. Replays WAL entries after the snapshot sequence
//! 3. Returns the restored shard and last sequence number

use std::io;
use std::path::Path;

use tracing::{info, warn};

use crate::engine::Shard;
use crate::persistence::{read_snapshot, PersistenceConfig, WalEntry, WalOp, WalReader};
use crate::replication::Delta;
use crate::types::NodeId;

/// Recover a shard from persistence files.
///
/// This function:
/// 1. Checks if persistence data exists for the shard
/// 2. Loads the snapshot if available
/// 3. Replays WAL entries after the snapshot sequence
/// 4. Returns the recovered shard and the last sequence number
///
/// If no persistence data exists, returns a fresh shard with seq 0.
pub fn recover_shard(
    config: &PersistenceConfig,
    shard_id: u16,
    node_id: NodeId,
) -> io::Result<(Shard, u64)> {
    let shard_dir = config.shard_dir(shard_id);

    if !shard_dir.exists() {
        info!("Shard {} has no persistence data, starting fresh", shard_id);
        return Ok((Shard::new(shard_id as usize, node_id), 0));
    }

    let snapshot_path = config.snapshot_path(shard_id);
    let wal_path = config.wal_path(shard_id);

    let mut shard = Shard::new(shard_id as usize, node_id);
    let mut last_seq = 0u64;

    // Load snapshot if exists
    if snapshot_path.exists() {
        match read_snapshot(&snapshot_path) {
            Ok(snapshot) => {
                info!(
                    "Shard {} loading snapshot at seq {}",
                    shard_id, snapshot.seq
                );
                last_seq = snapshot.seq;
                shard.restore_from_snapshot(snapshot);
            }
            Err(e) => {
                warn!(
                    "Shard {} snapshot corrupted: {}, will replay full WAL",
                    shard_id, e
                );
            }
        }
    }

    // Replay WAL entries after snapshot
    if wal_path.exists() {
        match replay_wal(&wal_path, &mut shard, last_seq) {
            Ok(replayed) => {
                if replayed > 0 {
                    info!("Shard {} replayed {} WAL entries", shard_id, replayed);
                }
                last_seq += replayed as u64;
            }
            Err(e) => {
                warn!("Shard {} WAL replay error: {}", shard_id, e);
                // Continue with what we have
            }
        }
    }

    Ok((shard, last_seq))
}

/// Replay WAL entries to a shard.
///
/// Reads all entries with seq > after_seq and applies them to the shard.
fn replay_wal(path: &Path, shard: &mut Shard, after_seq: u64) -> io::Result<usize> {
    let mut reader = WalReader::open(path)?;
    let entries = reader.read_after(after_seq)?;
    let count = entries.len();

    for entry in entries {
        apply_wal_entry(shard, &entry);
    }

    Ok(count)
}

/// Apply a single WAL entry to a shard.
///
/// Maps WAL operations to shard operations:
/// - Incr/Decr: Uses apply_delta for correct node attribution
/// - Set: Uses shard.set
/// - Delete: Uses shard.delete
/// - QuotaSet: Uses shard.quota_set
/// - QuotaGrant: Uses shard.quota_add_tokens
fn apply_wal_entry(shard: &mut Shard, entry: &WalEntry) {
    match &entry.op {
        WalOp::Incr { key, node_id, delta } => {
            // Use apply_delta to correctly attribute the increment to the right node
            let replication_delta =
                Delta::increment(entry.seq, key.clone(), NodeId::new(*node_id), *delta);
            shard.apply_delta(&replication_delta);
        }
        WalOp::Decr { key, node_id, delta } => {
            // Use apply_delta to correctly attribute the decrement to the right node
            let replication_delta =
                Delta::decrement(entry.seq, key.clone(), NodeId::new(*node_id), *delta);
            shard.apply_delta(&replication_delta);
        }
        WalOp::Set { key, value } => {
            shard.set(key.clone(), *value);
        }
        WalOp::Delete { key } => {
            shard.delete(key);
        }
        WalOp::QuotaSet {
            key,
            limit,
            window_secs,
        } => {
            shard.quota_set(key.clone(), *limit, *window_secs);
        }
        WalOp::QuotaGrant { key, tokens, .. } => {
            // Add tokens to quota
            shard.quota_add_tokens(key, *tokens);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::WalWriter;
    use crate::types::Key;

    #[test]
    fn test_recover_empty_shard() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-empty");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let (shard, seq) = recover_shard(&config, 0, NodeId::new(1)).unwrap();

        assert_eq!(seq, 0);
        assert_eq!(shard.get(&Key::from("test")), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recover_from_wal() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-wal");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let wal_path = config.wal_path(0);

        // Write some WAL entries
        {
            let mut writer = WalWriter::open(&wal_path, 0, 1000).unwrap();
            writer
                .append(&WalEntry {
                    seq: 1,
                    timestamp: 0,
                    op: WalOp::Incr {
                        key: Key::from("counter"),
                        node_id: 1,
                        delta: 100,
                    },
                })
                .unwrap();
            writer
                .append(&WalEntry {
                    seq: 2,
                    timestamp: 0,
                    op: WalOp::Incr {
                        key: Key::from("counter"),
                        node_id: 1,
                        delta: 50,
                    },
                })
                .unwrap();
            writer.sync().unwrap();
        }

        // Recover
        let (shard, _) = recover_shard(&config, 0, NodeId::new(1)).unwrap();
        assert_eq!(shard.get(&Key::from("counter")), Some(150));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recover_from_wal_with_decrement() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-wal-decr");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let wal_path = config.wal_path(0);

        // Write WAL entries with inc and dec
        {
            let mut writer = WalWriter::open(&wal_path, 0, 1000).unwrap();
            writer
                .append(&WalEntry {
                    seq: 1,
                    timestamp: 0,
                    op: WalOp::Incr {
                        key: Key::from("counter"),
                        node_id: 1,
                        delta: 100,
                    },
                })
                .unwrap();
            writer
                .append(&WalEntry {
                    seq: 2,
                    timestamp: 0,
                    op: WalOp::Decr {
                        key: Key::from("counter"),
                        node_id: 1,
                        delta: 30,
                    },
                })
                .unwrap();
            writer.sync().unwrap();
        }

        // Recover
        let (shard, _) = recover_shard(&config, 0, NodeId::new(1)).unwrap();
        assert_eq!(shard.get(&Key::from("counter")), Some(70)); // 100 - 30

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recover_set_and_delete() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-set-del");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let wal_path = config.wal_path(0);

        // Write WAL entries
        {
            let mut writer = WalWriter::open(&wal_path, 0, 1000).unwrap();
            writer
                .append(&WalEntry {
                    seq: 1,
                    timestamp: 0,
                    op: WalOp::Set {
                        key: Key::from("key1"),
                        value: 42,
                    },
                })
                .unwrap();
            writer
                .append(&WalEntry {
                    seq: 2,
                    timestamp: 0,
                    op: WalOp::Set {
                        key: Key::from("key2"),
                        value: 100,
                    },
                })
                .unwrap();
            writer
                .append(&WalEntry {
                    seq: 3,
                    timestamp: 0,
                    op: WalOp::Delete {
                        key: Key::from("key2"),
                    },
                })
                .unwrap();
            writer.sync().unwrap();
        }

        // Recover
        let (shard, _) = recover_shard(&config, 0, NodeId::new(1)).unwrap();
        assert_eq!(shard.get(&Key::from("key1")), Some(42));
        assert_eq!(shard.get(&Key::from("key2")), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recover_quota_operations() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-quota");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let wal_path = config.wal_path(0);

        // Write WAL entries
        {
            let mut writer = WalWriter::open(&wal_path, 0, 1000).unwrap();
            writer
                .append(&WalEntry {
                    seq: 1,
                    timestamp: 0,
                    op: WalOp::QuotaSet {
                        key: Key::from("rate_limit"),
                        limit: 10000,
                        window_secs: 60,
                    },
                })
                .unwrap();
            writer
                .append(&WalEntry {
                    seq: 2,
                    timestamp: 0,
                    op: WalOp::QuotaGrant {
                        key: Key::from("rate_limit"),
                        node_id: 1,
                        tokens: 500,
                    },
                })
                .unwrap();
            writer.sync().unwrap();
        }

        // Recover
        let (shard, _) = recover_shard(&config, 0, NodeId::new(1)).unwrap();
        assert!(shard.is_quota(&Key::from("rate_limit")));
        let quota_info = shard.quota_get(&Key::from("rate_limit")).unwrap();
        assert_eq!(quota_info.0, 10000); // limit
        assert_eq!(quota_info.1, 60); // window_secs
        assert_eq!(quota_info.2, 500); // remaining tokens

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_recover_multiple_nodes() {
        let dir = std::env::temp_dir().join("quota-db-test-recover-multi-node");
        let _ = std::fs::remove_dir_all(&dir);

        let config = PersistenceConfig::enabled(&dir);
        let wal_path = config.wal_path(0);

        // Write WAL entries from different nodes
        {
            let mut writer = WalWriter::open(&wal_path, 0, 1000).unwrap();
            writer
                .append(&WalEntry {
                    seq: 1,
                    timestamp: 0,
                    op: WalOp::Incr {
                        key: Key::from("counter"),
                        node_id: 1,
                        delta: 100,
                    },
                })
                .unwrap();
            writer
                .append(&WalEntry {
                    seq: 2,
                    timestamp: 0,
                    op: WalOp::Incr {
                        key: Key::from("counter"),
                        node_id: 2,
                        delta: 50,
                    },
                })
                .unwrap();
            writer
                .append(&WalEntry {
                    seq: 3,
                    timestamp: 0,
                    op: WalOp::Incr {
                        key: Key::from("counter"),
                        node_id: 3,
                        delta: 25,
                    },
                })
                .unwrap();
            writer.sync().unwrap();
        }

        // Recover
        let (shard, _) = recover_shard(&config, 0, NodeId::new(1)).unwrap();
        // Total: 100 + 50 + 25 = 175
        assert_eq!(shard.get(&Key::from("counter")), Some(175));

        let _ = std::fs::remove_dir_all(&dir);
    }
}
