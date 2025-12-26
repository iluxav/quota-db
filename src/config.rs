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
        }
    }
}
