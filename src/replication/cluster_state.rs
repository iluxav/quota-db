//! Shared cluster state for CLUSTER INFO command.

use crate::replication::ConnectionState;
use crate::types::NodeId;
use parking_lot::RwLock;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

/// Information about a peer node
#[derive(Debug, Clone)]
pub struct PeerInfo {
    /// Peer's address
    pub addr: SocketAddr,
    /// Peer's node ID (if known)
    pub node_id: Option<NodeId>,
    /// Connection state
    pub state: ConnectionState,
    /// Acknowledged sequence per shard (min/max/avg for summary)
    pub acked_seq_min: u64,
    pub acked_seq_max: u64,
    /// Number of pending deltas
    pub pending_count: usize,
    /// Last activity timestamp
    pub last_activity: Instant,
}

/// Snapshot of cluster state
#[derive(Debug, Clone)]
pub struct ClusterStateSnapshot {
    /// This node's ID
    pub node_id: NodeId,
    /// This node's listen port
    pub listen_port: u16,
    /// Number of shards
    pub num_shards: usize,
    /// Peer information
    pub peers: Vec<PeerInfo>,
    /// Cluster size (self + peers)
    pub cluster_size: usize,
    /// Number of connected peers
    pub connected_peers: usize,
}

impl ClusterStateSnapshot {
    /// Format as Redis-like INFO string
    pub fn to_info_string(&self) -> String {
        let mut info = String::with_capacity(1024);

        info.push_str("# Cluster\n");
        info.push_str(&format!("cluster_enabled:1\n"));
        info.push_str(&format!("cluster_node_id:{}\n", self.node_id.as_u32()));
        info.push_str(&format!("cluster_listen_port:{}\n", self.listen_port));
        info.push_str(&format!("cluster_shards:{}\n", self.num_shards));
        info.push_str(&format!("cluster_size:{}\n", self.cluster_size));
        info.push_str(&format!("cluster_known_peers:{}\n", self.peers.len()));
        info.push_str(&format!("cluster_connected_peers:{}\n", self.connected_peers));

        info.push_str("\n# Peers\n");
        for (i, peer) in self.peers.iter().enumerate() {
            let node_id_str = peer.node_id
                .map(|n| n.as_u32().to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let state_str = match peer.state {
                ConnectionState::Connected => "connected",
                ConnectionState::Connecting => "connecting",
                ConnectionState::Disconnected => "disconnected",
                ConnectionState::CircuitOpen => "circuit_open",
            };
            let age_secs = peer.last_activity.elapsed().as_secs();

            info.push_str(&format!(
                "peer{}:addr={},node_id={},state={},acked_seq={}-{},pending={},idle_secs={}\n",
                i,
                peer.addr,
                node_id_str,
                state_str,
                peer.acked_seq_min,
                peer.acked_seq_max,
                peer.pending_count,
                age_secs,
            ));
        }

        info
    }
}

/// Shared cluster state updated by ReplicationManager
pub struct ClusterState {
    inner: RwLock<ClusterStateInner>,
}

struct ClusterStateInner {
    node_id: NodeId,
    listen_port: u16,
    num_shards: usize,
    peers: Vec<PeerInfo>,
}

impl ClusterState {
    /// Create new cluster state
    pub fn new(node_id: NodeId, listen_port: u16, num_shards: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(ClusterStateInner {
                node_id,
                listen_port,
                num_shards,
                peers: Vec::new(),
            }),
        })
    }

    /// Update peer information (called by ReplicationManager)
    pub fn update_peers(&self, peers: Vec<PeerInfo>) {
        let mut inner = self.inner.write();
        inner.peers = peers;
    }

    /// Get a snapshot of cluster state
    pub fn snapshot(&self) -> ClusterStateSnapshot {
        let inner = self.inner.read();
        let connected_peers = inner.peers.iter()
            .filter(|p| matches!(p.state, ConnectionState::Connected))
            .count();

        ClusterStateSnapshot {
            node_id: inner.node_id,
            listen_port: inner.listen_port,
            num_shards: inner.num_shards,
            peers: inner.peers.clone(),
            cluster_size: inner.peers.len() + 1, // +1 for self
            connected_peers,
        }
    }
}
