use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the persistence layer.
#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    /// Enable persistence (default: false for backwards compat)
    pub enabled: bool,

    /// Data directory (default: "./data")
    pub data_dir: PathBuf,

    /// WAL sync threshold: ops count (default: 1000)
    pub wal_sync_ops: usize,

    /// WAL sync threshold: interval (default: 100ms)
    pub wal_sync_interval: Duration,

    /// Snapshot interval (default: 5 min)
    pub snapshot_interval: Duration,

    /// Snapshot WAL size threshold in bytes (default: 64MB)
    pub snapshot_wal_threshold: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            data_dir: PathBuf::from("./data"),
            wal_sync_ops: 1000,
            wal_sync_interval: Duration::from_millis(100),
            snapshot_interval: Duration::from_secs(300),
            snapshot_wal_threshold: 64 * 1024 * 1024,
        }
    }
}

impl PersistenceConfig {
    /// Create a new config with persistence enabled.
    pub fn enabled(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            enabled: true,
            data_dir: data_dir.into(),
            ..Default::default()
        }
    }

    /// Get the path for a shard's data directory.
    pub fn shard_dir(&self, shard_id: u16) -> PathBuf {
        self.data_dir.join(format!("shard_{}", shard_id))
    }

    /// Get the path for a shard's WAL file.
    pub fn wal_path(&self, shard_id: u16) -> PathBuf {
        self.shard_dir(shard_id).join("wal.bin")
    }

    /// Get the path for a shard's snapshot file.
    pub fn snapshot_path(&self, shard_id: u16) -> PathBuf {
        self.shard_dir(shard_id).join("snapshot.bin")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = PersistenceConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.data_dir, PathBuf::from("./data"));
        assert_eq!(config.wal_sync_ops, 1000);
    }

    #[test]
    fn test_enabled_config() {
        let config = PersistenceConfig::enabled("/tmp/quota-db");
        assert!(config.enabled);
        assert_eq!(config.data_dir, PathBuf::from("/tmp/quota-db"));
    }

    #[test]
    fn test_shard_paths() {
        let config = PersistenceConfig::enabled("/tmp/db");
        assert_eq!(config.shard_dir(0), PathBuf::from("/tmp/db/shard_0"));
        assert_eq!(config.wal_path(0), PathBuf::from("/tmp/db/shard_0/wal.bin"));
        assert_eq!(config.snapshot_path(0), PathBuf::from("/tmp/db/shard_0/snapshot.bin"));
    }
}
