//! Quota/rate-limit entry with token bucket semantics.
//!
//! Provides guaranteed rate limits via token-based allocation:
//! - Each node gets a local token balance
//! - Requests consume tokens locally (fast path)
//! - Refill happens via allocator node (slow path)

use std::time::{SystemTime, UNIX_EPOCH};

use rustc_hash::FxHashMap;

use crate::types::NodeId;

/// Quota entry for rate limiting with token bucket semantics.
///
/// Tracks:
/// - Configuration (limit, window duration)
/// - Local token balance for fast-path decisions
/// - Window timing for automatic resets
#[derive(Debug, Clone)]
pub struct QuotaEntry {
    /// Maximum tokens per window (e.g., 10000)
    limit: u64,

    /// Window duration in seconds (e.g., 60)
    window_secs: u64,

    /// Tokens available locally for consumption
    local_tokens: i64,

    /// Unix timestamp (seconds) when current window started
    window_start: u64,
}

impl QuotaEntry {
    /// Create a new quota entry with the specified limit and window.
    pub fn new(limit: u64, window_secs: u64) -> Self {
        Self {
            limit,
            window_secs,
            local_tokens: 0, // Start with 0, must request from allocator
            window_start: current_timestamp_secs(),
        }
    }

    /// Get the configured limit (tokens per window).
    #[inline]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Get the window duration in seconds.
    #[inline]
    pub fn window_secs(&self) -> u64 {
        self.window_secs
    }

    /// Get the current local token balance.
    #[inline]
    pub fn local_tokens(&self) -> i64 {
        self.local_tokens
    }

    /// Get the window start timestamp.
    #[inline]
    pub fn window_start(&self) -> u64 {
        self.window_start
    }

    /// Check if the current window has expired.
    #[inline]
    pub fn is_window_expired(&self) -> bool {
        let now = current_timestamp_secs();
        now >= self.window_start + self.window_secs
    }

    /// Reset the window (called when window expires).
    /// Returns the old window_start for replication.
    pub fn reset_window(&mut self) -> u64 {
        let old_start = self.window_start;
        self.window_start = current_timestamp_secs();
        self.local_tokens = 0;
        old_start
    }

    /// Try to consume tokens locally.
    ///
    /// Returns:
    /// - `Some(remaining)` if tokens were consumed successfully
    /// - `None` if insufficient local tokens (need to request from allocator)
    #[inline]
    pub fn try_consume(&mut self, amount: u64) -> Option<i64> {
        let amount = amount as i64;
        if self.local_tokens >= amount {
            self.local_tokens -= amount;
            Some(self.local_tokens)
        } else {
            None
        }
    }

    /// Add tokens to local balance (called when allocator grants tokens).
    #[inline]
    pub fn add_tokens(&mut self, amount: u64) {
        self.local_tokens += amount as i64;
    }

    /// Update quota configuration.
    pub fn update_config(&mut self, limit: u64, window_secs: u64) {
        self.limit = limit;
        self.window_secs = window_secs;
    }

    /// Calculate the batch size for token requests (10% of limit).
    #[inline]
    pub fn request_batch_size(&self) -> u64 {
        // 10% of limit, minimum 1
        (self.limit / 10).max(1)
    }
}

/// Allocator state for a quota key (only on shard owner node).
///
/// Tracks how many tokens have been granted to each node in the current window.
#[derive(Debug, Clone, Default)]
pub struct AllocatorState {
    /// Tokens granted per node in current window
    grants: FxHashMap<NodeId, u64>,

    /// Total tokens granted across all nodes
    total_granted: u64,

    /// Window start timestamp (for sync with QuotaEntry)
    window_start: u64,
}

impl AllocatorState {
    /// Create a new allocator state.
    pub fn new() -> Self {
        Self {
            grants: FxHashMap::default(),
            total_granted: 0,
            window_start: current_timestamp_secs(),
        }
    }

    /// Create allocator state with a specific window start.
    pub fn with_window_start(window_start: u64) -> Self {
        Self {
            grants: FxHashMap::default(),
            total_granted: 0,
            window_start,
        }
    }

    /// Get total tokens granted.
    #[inline]
    pub fn total_granted(&self) -> u64 {
        self.total_granted
    }

    /// Get tokens granted to a specific node.
    #[inline]
    pub fn granted_to(&self, node: NodeId) -> u64 {
        self.grants.get(&node).copied().unwrap_or(0)
    }

    /// Calculate available tokens (limit - total_granted).
    #[inline]
    pub fn available(&self, limit: u64) -> u64 {
        limit.saturating_sub(self.total_granted)
    }

    /// Try to grant tokens to a node.
    ///
    /// Returns the number of tokens actually granted (may be less than requested).
    pub fn try_grant(&mut self, node: NodeId, requested: u64, limit: u64) -> u64 {
        let available = self.available(limit);
        let granted = requested.min(available);

        if granted > 0 {
            *self.grants.entry(node).or_insert(0) += granted;
            self.total_granted += granted;
        }

        granted
    }

    /// Reset for a new window.
    pub fn reset(&mut self, new_window_start: u64) {
        self.grants.clear();
        self.total_granted = 0;
        self.window_start = new_window_start;
    }

    /// Get the window start timestamp.
    #[inline]
    pub fn window_start(&self) -> u64 {
        self.window_start
    }
}

/// Get current Unix timestamp in seconds.
#[inline]
fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_entry_new() {
        let entry = QuotaEntry::new(10000, 60);
        assert_eq!(entry.limit(), 10000);
        assert_eq!(entry.window_secs(), 60);
        assert_eq!(entry.local_tokens(), 0);
    }

    #[test]
    fn test_try_consume_insufficient() {
        let mut entry = QuotaEntry::new(10000, 60);
        // No local tokens, should fail
        assert_eq!(entry.try_consume(1), None);
    }

    #[test]
    fn test_try_consume_success() {
        let mut entry = QuotaEntry::new(10000, 60);
        entry.add_tokens(100);

        assert_eq!(entry.try_consume(10), Some(90));
        assert_eq!(entry.try_consume(50), Some(40));
        assert_eq!(entry.try_consume(50), None); // Only 40 left
    }

    #[test]
    fn test_add_tokens() {
        let mut entry = QuotaEntry::new(10000, 60);
        entry.add_tokens(500);
        assert_eq!(entry.local_tokens(), 500);

        entry.add_tokens(300);
        assert_eq!(entry.local_tokens(), 800);
    }

    #[test]
    fn test_request_batch_size() {
        let entry = QuotaEntry::new(10000, 60);
        assert_eq!(entry.request_batch_size(), 1000); // 10%

        let small = QuotaEntry::new(5, 60);
        assert_eq!(small.request_batch_size(), 1); // Minimum 1
    }

    #[test]
    fn test_allocator_try_grant() {
        let mut alloc = AllocatorState::new();
        let node1 = NodeId::new(1);
        let node2 = NodeId::new(2);
        let limit = 1000;

        // Grant to node1
        assert_eq!(alloc.try_grant(node1, 300, limit), 300);
        assert_eq!(alloc.total_granted(), 300);
        assert_eq!(alloc.available(limit), 700);

        // Grant to node2
        assert_eq!(alloc.try_grant(node2, 500, limit), 500);
        assert_eq!(alloc.total_granted(), 800);
        assert_eq!(alloc.available(limit), 200);

        // Try to grant more than available
        assert_eq!(alloc.try_grant(node1, 300, limit), 200);
        assert_eq!(alloc.total_granted(), 1000);
        assert_eq!(alloc.available(limit), 0);

        // No more available
        assert_eq!(alloc.try_grant(node2, 100, limit), 0);
    }

    #[test]
    fn test_allocator_reset() {
        let mut alloc = AllocatorState::new();
        let node = NodeId::new(1);

        alloc.try_grant(node, 500, 1000);
        assert_eq!(alloc.total_granted(), 500);

        alloc.reset(12345);
        assert_eq!(alloc.total_granted(), 0);
        assert_eq!(alloc.window_start(), 12345);
        assert_eq!(alloc.granted_to(node), 0);
    }
}
