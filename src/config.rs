use crate::types::NodeId;
use clap::Parser;
use std::net::SocketAddr;

/// QuotaDB - High-performance distributed counter database
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    /// Network binding address
    #[arg(short, long, default_value = "127.0.0.1:6380")]
    pub bind: SocketAddr,

    /// Number of shards (power of 2 recommended)
    #[arg(short, long, default_value = "64")]
    pub shards: usize,

    /// This node's unique identifier for CRDT operations
    #[arg(short, long, default_value = "1")]
    pub node_id: u32,

    /// Maximum concurrent connections
    #[arg(long, default_value = "10000")]
    pub max_connections: usize,

    /// Enable TTL support
    #[arg(long, default_value = "true")]
    pub enable_ttl: bool,

    /// TTL check interval in milliseconds
    #[arg(long, default_value = "100")]
    pub ttl_tick_ms: u64,

    /// Log level (trace, debug, info, warn, error)
    #[arg(long, default_value = "info")]
    pub log_level: String,

    // === Replication settings ===

    /// Peer nodes for replication (comma-separated, e.g., "node2:6381,node3:6381")
    #[arg(long, value_delimiter = ',', default_value = "")]
    pub peers: Vec<String>,

    /// Port for replication connections (separate from client port)
    #[arg(long, default_value = "6381")]
    pub replication_port: u16,

    /// Maximum deltas per batch before flushing to peers
    #[arg(long, default_value = "100")]
    pub batch_max_size: usize,

    /// Maximum delay in milliseconds before flushing batch to peers
    #[arg(long, default_value = "10")]
    pub batch_max_delay_ms: u64,

    /// Replication channel buffer size for backpressure
    #[arg(long, default_value = "10000")]
    pub replication_channel_size: usize,

    // === Persistence settings ===

    /// Enable persistence (WAL + snapshots)
    #[arg(long, default_value = "false")]
    pub persistence: bool,

    /// Data directory for persistence files
    #[arg(long, default_value = "./data")]
    pub data_dir: String,

    /// WAL sync interval in milliseconds
    #[arg(long, default_value = "100")]
    pub wal_sync_ms: u64,

    /// WAL sync ops threshold
    #[arg(long, default_value = "1000")]
    pub wal_sync_ops: usize,

    /// Snapshot interval in seconds
    #[arg(long, default_value = "300")]
    pub snapshot_interval_secs: u64,

    /// Snapshot WAL size threshold in MB
    #[arg(long, default_value = "64")]
    pub snapshot_wal_mb: u64,

    /// WAL channel buffer size for backpressure
    #[arg(long, default_value = "10000")]
    pub wal_channel_size: usize,

    // === Connection settings ===

    /// Write timeout for slow clients in milliseconds
    #[arg(long, default_value = "5000")]
    pub write_timeout_ms: u64,
}

impl Config {
    /// Parse configuration from command line arguments
    pub fn parse_args() -> Self {
        Config::parse()
    }

    /// Get the NodeId for this instance
    pub fn node_id(&self) -> NodeId {
        NodeId::new(self.node_id)
    }

    /// Get persistence configuration.
    pub fn persistence_config(&self) -> crate::persistence::PersistenceConfig {
        use std::path::PathBuf;
        use std::time::Duration;

        crate::persistence::PersistenceConfig {
            enabled: self.persistence,
            data_dir: PathBuf::from(&self.data_dir),
            wal_sync_interval: Duration::from_millis(self.wal_sync_ms),
            wal_sync_ops: self.wal_sync_ops,
            snapshot_interval: Duration::from_secs(self.snapshot_interval_secs),
            snapshot_wal_threshold: self.snapshot_wal_mb * 1024 * 1024,
            wal_channel_size: self.wal_channel_size,
        }
    }

    /// Get the write timeout duration.
    pub fn write_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.write_timeout_ms)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:6380".parse().unwrap(),
            shards: 64,
            node_id: 1,
            max_connections: 10_000,
            enable_ttl: true,
            ttl_tick_ms: 100,
            log_level: "info".to_string(),
            peers: Vec::new(),
            replication_port: 6381,
            batch_max_size: 100,
            batch_max_delay_ms: 10,
            replication_channel_size: 10_000,
            persistence: false,
            data_dir: "./data".to_string(),
            wal_sync_ms: 100,
            wal_sync_ops: 1000,
            snapshot_interval_secs: 300,
            snapshot_wal_mb: 64,
            wal_channel_size: 10_000,
            write_timeout_ms: 5_000,
        }
    }
}
