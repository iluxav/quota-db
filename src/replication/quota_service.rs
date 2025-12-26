use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use rustc_hash::FxHashMap;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::engine::ShardedDb;
use crate::types::{Key, NodeId};

/// Pending token request to a remote allocator
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PendingRequest {
    pub key: Key,
    pub shard_id: u16,
    pub requested: u64,
    pub requested_at: Instant,
}

/// Quota service for multi-node token allocation
///
/// Handles:
/// - Sending token requests to allocator nodes
/// - Processing incoming token grants/denials
/// - Managing pending request state
pub struct QuotaService {
    /// Database reference
    db: Arc<ShardedDb>,
    /// Local node ID
    node_id: NodeId,
    /// Number of nodes in cluster (for allocator assignment)
    num_nodes: usize,
    /// Pending token requests (key -> request)
    pending_requests: RwLock<FxHashMap<Key, PendingRequest>>,
    /// Channel to send quota messages to replication manager
    quota_tx: Option<mpsc::Sender<QuotaMessage>>,
    /// Request timeout
    request_timeout: Duration,
}

/// Message sent to replication layer for quota operations
#[derive(Debug, Clone)]
pub enum QuotaMessage {
    /// Request tokens from allocator
    Request {
        shard_id: u16,
        key: Key,
        requested: u64,
    },
    /// Grant tokens to requesting node (response from allocator)
    Grant {
        shard_id: u16,
        key: Key,
        target_node: NodeId,
        granted: u64,
    },
    /// Deny request (quota exhausted)
    Deny {
        shard_id: u16,
        key: Key,
        target_node: NodeId,
    },
    /// Sync quota configuration to all nodes
    Sync {
        shard_id: u16,
        key: Key,
        limit: u64,
        window_secs: u64,
    },
}

impl QuotaService {
    /// Create a new quota service
    pub fn new(db: Arc<ShardedDb>, num_nodes: usize) -> Self {
        Self {
            node_id: db.node_id(),
            db,
            num_nodes,
            pending_requests: RwLock::new(FxHashMap::default()),
            quota_tx: None,
            request_timeout: Duration::from_millis(100),
        }
    }

    /// Set the channel for sending quota messages to the replication layer
    pub fn set_quota_tx(&mut self, tx: mpsc::Sender<QuotaMessage>) {
        self.quota_tx = Some(tx);
    }

    /// Get the allocator node for a given shard
    /// Uses consistent hashing: allocator = shard_id % num_nodes
    #[inline]
    pub fn allocator_for_shard(&self, shard_id: u16) -> NodeId {
        // In single-node mode, we're always the allocator
        if self.num_nodes <= 1 {
            return self.node_id;
        }
        // Simple consistent assignment: node = shard % nodes
        // Node IDs are 1-based, so add 1
        NodeId::new(((shard_id as usize) % self.num_nodes + 1) as u32)
    }

    /// Check if this node is the allocator for a given shard
    #[inline]
    pub fn is_allocator(&self, shard_id: u16) -> bool {
        self.allocator_for_shard(shard_id) == self.node_id
    }

    /// Request tokens from the allocator for a key
    /// Returns true if request was sent, false if we're the allocator or no channel
    pub async fn request_tokens(&self, shard_id: u16, key: Key, amount: u64) -> bool {
        // If we're the allocator, don't send a request
        if self.is_allocator(shard_id) {
            return false;
        }

        // Check if we already have a pending request for this key
        {
            let pending = self.pending_requests.read();
            if let Some(req) = pending.get(&key) {
                // Already have a pending request, check if it's stale
                if req.requested_at.elapsed() < self.request_timeout {
                    debug!("Already have pending request for {:?}", key);
                    return false;
                }
            }
        }

        // Record pending request
        {
            let mut pending = self.pending_requests.write();
            pending.insert(
                key.clone(),
                PendingRequest {
                    key: key.clone(),
                    shard_id,
                    requested: amount,
                    requested_at: Instant::now(),
                },
            );
        }

        // Send request to replication layer
        if let Some(ref tx) = self.quota_tx {
            let msg = QuotaMessage::Request {
                shard_id,
                key: key.clone(),
                requested: amount,
            };
            if tx.send(msg).await.is_err() {
                warn!("Failed to send token request for {:?}", key);
                return false;
            }
            debug!("Sent token request for {:?}, shard {}, amount {}", key, shard_id, amount);
            true
        } else {
            false
        }
    }

    /// Handle an incoming token grant
    pub fn handle_grant(&self, shard_id: u16, key: &Key, granted: u64) {
        // Remove from pending
        {
            let mut pending = self.pending_requests.write();
            pending.remove(key);
        }

        // Add tokens to local quota entry
        // This needs to go through the shard
        debug!(
            "Received {} tokens for {:?}, shard {}",
            granted, key, shard_id
        );

        // The actual token addition happens in the handler/db layer
        // This just clears the pending state
    }

    /// Handle an incoming token denial
    pub fn handle_deny(&self, shard_id: u16, key: &Key) {
        // Remove from pending
        {
            let mut pending = self.pending_requests.write();
            pending.remove(key);
        }

        debug!(
            "Token request denied for {:?}, shard {}",
            key, shard_id
        );
    }

    /// Process an incoming quota request (as allocator)
    /// Returns (should_grant, amount_to_grant)
    pub fn process_request_as_allocator(
        &self,
        _shard_id: u16,
        key: &Key,
        from_node: NodeId,
        requested: u64,
    ) -> (bool, u64) {
        // This node is the allocator, process the request
        debug!(
            "Processing quota request from node {} for {:?}, requested {}",
            from_node.as_u32(),
            key,
            requested
        );

        // Try to grant from the allocator state
        // Note: allocator_grant is on Shard, need to access through db
        // For now, we'll use a simplified approach
        let batch_size = self.db.quota_batch_size(key);

        // Actually grant the tokens (this modifies allocator state)
        let granted = self.db.allocator_grant(key, from_node, batch_size);

        if granted > 0 {
            (true, granted)
        } else {
            (false, 0)
        }
    }

    /// Broadcast quota configuration to all peers
    pub async fn broadcast_quota_sync(&self, shard_id: u16, key: Key, limit: u64, window_secs: u64) {
        if let Some(ref tx) = self.quota_tx {
            let msg = QuotaMessage::Sync {
                shard_id,
                key: key.clone(),
                limit,
                window_secs,
            };
            if tx.send(msg).await.is_err() {
                warn!("Failed to broadcast quota sync for {:?}", key);
            }
        }
    }

    /// Clean up stale pending requests
    pub fn cleanup_stale_requests(&self) {
        let mut pending = self.pending_requests.write();
        pending.retain(|_, req| req.requested_at.elapsed() < self.request_timeout * 10);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn create_db() -> Arc<ShardedDb> {
        let config = Config {
            shards: 4,
            node_id: 1,
            ..Default::default()
        };
        Arc::new(ShardedDb::new(&config))
    }

    #[test]
    fn test_allocator_assignment_single_node() {
        let db = create_db();
        let service = QuotaService::new(db, 1);

        // Single node is always the allocator
        assert!(service.is_allocator(0));
        assert!(service.is_allocator(1));
        assert!(service.is_allocator(2));
        assert!(service.is_allocator(3));
    }

    #[test]
    fn test_allocator_assignment_multi_node() {
        let db = create_db();
        let service = QuotaService::new(db, 3);

        // Node 1: allocator for shard 0, 3 (0 % 3 + 1 = 1, 3 % 3 + 1 = 1)
        // Node 2: allocator for shard 1 (1 % 3 + 1 = 2)
        // Node 3: allocator for shard 2 (2 % 3 + 1 = 3)
        assert!(service.is_allocator(0)); // 0 % 3 + 1 = 1, our node
        assert!(!service.is_allocator(1)); // 1 % 3 + 1 = 2, not us
        assert!(!service.is_allocator(2)); // 2 % 3 + 1 = 3, not us
        assert!(service.is_allocator(3)); // 3 % 3 + 1 = 1, our node
    }

    #[test]
    fn test_allocator_for_shard() {
        let db = create_db();
        let service = QuotaService::new(db, 3);

        assert_eq!(service.allocator_for_shard(0), NodeId::new(1));
        assert_eq!(service.allocator_for_shard(1), NodeId::new(2));
        assert_eq!(service.allocator_for_shard(2), NodeId::new(3));
        assert_eq!(service.allocator_for_shard(3), NodeId::new(1));
    }
}
