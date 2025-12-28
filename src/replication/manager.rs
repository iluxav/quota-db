use crate::engine::ShardedDb;
use crate::metrics::{ReplicationLagMetrics, METRICS};
use crate::replication::{AntiEntropyConfig, AntiEntropyTask, ClusterState, ClusterStateSnapshot, Delta, Message, PeerConnection, PeerConnectionReader, PeerConnectionWriter, PeerInfo, PeerState};
use crate::types::NodeId;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};

/// Delta notification from shard to replication manager
#[derive(Debug, Clone)]
pub struct DeltaNotification {
    pub shard_id: u16,
    pub delta: Delta,
}

/// String notification for replication (LWW semantics)
#[derive(Debug, Clone)]
pub struct StringNotification {
    pub shard_id: u16,
    pub key: crate::types::Key,
    pub value: bytes::Bytes,
    pub timestamp: u64,
}

/// Quota notification for replication
#[derive(Debug, Clone)]
pub enum QuotaNotification {
    /// Sync quota configuration to all peers
    Sync {
        shard_id: u16,
        key: crate::types::Key,
        limit: u64,
        window_secs: u64,
    },
    /// Request tokens from allocator node
    Request {
        shard_id: u16,
        key: crate::types::Key,
        requested: u64,
    },
}

/// Replication manager configuration
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// This node's ID
    pub node_id: NodeId,
    /// Port to listen for incoming replication connections
    pub listen_port: u16,
    /// Peer addresses to connect to
    pub peers: Vec<SocketAddr>,
    /// Number of shards
    pub num_shards: usize,
    /// Maximum batch size before flushing
    pub batch_max_size: usize,
    /// Maximum delay before flushing
    pub batch_max_delay: Duration,
    /// Channel capacity for delta notifications (backpressure)
    pub channel_capacity: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            node_id: NodeId::new(1),
            listen_port: 6381,
            peers: Vec::new(),
            num_shards: 64,
            batch_max_size: 100,
            batch_max_delay: Duration::from_millis(10),
            channel_capacity: DEFAULT_DELTA_CHANNEL_CAPACITY,
        }
    }
}

/// Default capacity for the delta replication channel.
/// This provides backpressure when replication can't keep up.
pub const DEFAULT_DELTA_CHANNEL_CAPACITY: usize = 10000;

/// Handle for sending deltas to the replication manager
#[derive(Clone)]
pub struct ReplicationHandle {
    tx: mpsc::Sender<DeltaNotification>,
    string_tx: mpsc::Sender<StringNotification>,
    quota_tx: mpsc::Sender<QuotaNotification>,
    cluster_state: Arc<ClusterState>,
}

impl ReplicationHandle {
    /// Send a delta to be replicated.
    /// Uses try_send to avoid blocking the hot path.
    /// If the channel is full, the delta is dropped and a metric is incremented.
    pub fn send(&self, shard_id: u16, delta: Delta) {
        if let Err(_) = self.tx.try_send(DeltaNotification { shard_id, delta }) {
            // Channel full - replication is lagging behind
            // This is tracked in metrics; anti-entropy will repair gaps
            METRICS.inc(&METRICS.replication_dropped);
        }
    }

    /// Send a string value to be replicated (LWW semantics).
    /// Uses try_send to avoid blocking the hot path.
    pub fn send_string(&self, shard_id: u16, key: crate::types::Key, value: bytes::Bytes, timestamp: u64) {
        if let Err(_) = self.string_tx.try_send(StringNotification {
            shard_id,
            key,
            value,
            timestamp,
        }) {
            // Channel full - string replication is lagging behind
            METRICS.inc(&METRICS.replication_dropped);
        }
    }

    /// Check if the channel has capacity (for testing/monitoring).
    pub fn has_capacity(&self) -> bool {
        self.tx.capacity() > 0
    }

    /// Get a snapshot of the cluster state
    pub fn cluster_info(&self) -> ClusterStateSnapshot {
        self.cluster_state.snapshot()
    }

    /// Sync quota configuration to all peers.
    /// Called when QUOTASET creates a new quota.
    pub fn send_quota_sync(&self, shard_id: u16, key: crate::types::Key, limit: u64, window_secs: u64) {
        if let Err(_) = self.quota_tx.try_send(QuotaNotification::Sync {
            shard_id,
            key,
            limit,
            window_secs,
        }) {
            METRICS.inc(&METRICS.replication_dropped);
        }
    }

    /// Request tokens from allocator node.
    /// Called when local tokens are exhausted (NeedTokens).
    pub fn request_tokens(&self, shard_id: u16, key: crate::types::Key, requested: u64) {
        if let Err(_) = self.quota_tx.try_send(QuotaNotification::Request {
            shard_id,
            key,
            requested,
        }) {
            METRICS.inc(&METRICS.replication_dropped);
        }
    }
}

/// Replication manager - handles all peer connections and delta streaming
pub struct ReplicationManager {
    config: ReplicationConfig,
    /// Channel to receive deltas from shards (bounded for backpressure)
    delta_rx: mpsc::Receiver<DeltaNotification>,
    /// Channel to receive string updates (bounded for backpressure)
    string_rx: mpsc::Receiver<StringNotification>,
    /// Channel to receive quota operations (bounded for backpressure)
    quota_rx: mpsc::Receiver<QuotaNotification>,
    /// Outbound peer states (we connect to these)
    outbound_peers: HashMap<SocketAddr, PeerState>,
    /// Active writers for sending to peers (outbound connections)
    writers: HashMap<SocketAddr, PeerConnectionWriter>,
    /// Inbound writers for sending responses back to peers
    inbound_writers: HashMap<SocketAddr, PeerConnectionWriter>,
    /// Database reference for quota operations (optional)
    db: Option<Arc<ShardedDb>>,
    /// Number of nodes in cluster for allocator assignment
    num_nodes: usize,
    /// Anti-entropy task
    anti_entropy: Option<AntiEntropyTask>,
    /// Anti-entropy configuration
    anti_entropy_config: AntiEntropyConfig,
    /// Shared cluster state for CLUSTER INFO
    cluster_state: Arc<ClusterState>,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(config: ReplicationConfig) -> (Self, ReplicationHandle) {
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let (string_tx, string_rx) = mpsc::channel(config.channel_capacity);
        let (quota_tx, quota_rx) = mpsc::channel(config.channel_capacity);

        let cluster_state = ClusterState::new(
            config.node_id,
            config.listen_port,
            config.num_shards,
        );

        let num_nodes = config.peers.len() + 1; // peers + self
        let mut outbound_peers = HashMap::new();
        for addr in &config.peers {
            outbound_peers.insert(*addr, PeerState::new(*addr, config.num_shards));
        }

        let manager = Self {
            config,
            delta_rx: rx,
            string_rx,
            quota_rx,
            outbound_peers,
            writers: HashMap::new(),
            inbound_writers: HashMap::new(),
            db: None,
            num_nodes,
            anti_entropy: None,
            anti_entropy_config: AntiEntropyConfig::default(),
            cluster_state: cluster_state.clone(),
        };

        let handle = ReplicationHandle { tx, string_tx, quota_tx, cluster_state };
        (manager, handle)
    }

    /// Set the database reference for quota operations
    pub fn set_db(&mut self, db: Arc<ShardedDb>) {
        self.db = Some(db);
    }

    /// Initialize anti-entropy with the database reference
    pub fn init_anti_entropy(&mut self) {
        if let Some(ref db) = self.db {
            self.anti_entropy = Some(AntiEntropyTask::new(
                db.clone(),
                self.anti_entropy_config.clone(),
            ));
            info!("Anti-entropy initialized");
        }
    }

    /// Check if this node is the allocator for a given shard
    #[inline]
    fn is_allocator(&self, shard_id: u16) -> bool {
        if self.num_nodes <= 1 {
            return true;
        }
        let allocator_node = ((shard_id as usize) % self.num_nodes + 1) as u32;
        allocator_node == self.config.node_id.as_u32()
    }

    /// Run the replication manager
    pub async fn run(mut self, apply_delta: Arc<dyn Fn(u16, Delta) + Send + Sync>) {
        info!(
            "Starting replication manager on port {} with {} peers",
            self.config.listen_port,
            self.config.peers.len()
        );

        // Start listener for incoming connections
        let listener = match TcpListener::bind(("0.0.0.0", self.config.listen_port)).await {
            Ok(l) => {
                info!("Replication listener bound to port {}", self.config.listen_port);
                Some(l)
            }
            Err(e) => {
                error!("Failed to bind replication listener: {}", e);
                None
            }
        };

        // Spawn connection tasks for each peer we should connect to
        let (conn_tx, mut conn_rx) = mpsc::channel::<(SocketAddr, PeerConnection)>(16);
        let (msg_tx, mut msg_rx) = mpsc::channel::<(SocketAddr, Message)>(1024);
        let (disconnect_tx, mut disconnect_rx) = mpsc::channel::<SocketAddr>(16);

        // Channel for triggering reconnections
        let (reconnect_tx, mut reconnect_rx) = mpsc::channel::<SocketAddr>(16);

        // Channel for inbound connection writers (for sending responses)
        let (inbound_tx, mut inbound_rx) = mpsc::channel::<(SocketAddr, PeerConnectionWriter)>(16);

        // Spawn outbound connector tasks
        for addr in self.config.peers.clone() {
            let conn_tx = conn_tx.clone();
            let reconnect_tx = reconnect_tx.clone();
            tokio::spawn(async move {
                Self::connector_task(addr, conn_tx, reconnect_tx).await;
            });
        }

        // Spawn listener task for incoming connections
        if let Some(listener) = listener {
            let msg_tx_clone = msg_tx.clone();
            let apply_delta_clone = apply_delta.clone();
            let node_id = self.config.node_id;
            let inbound_tx_clone = inbound_tx.clone();
            tokio::spawn(async move {
                Self::listener_task(listener, node_id, msg_tx_clone, apply_delta_clone, inbound_tx_clone).await;
            });
        }

        // Main loop
        let mut flush_interval = interval(self.config.batch_max_delay);

        // Anti-entropy interval
        let ae_interval = self.anti_entropy.as_ref()
            .map(|ae| ae.status_interval())
            .unwrap_or(Duration::from_secs(5));
        let mut anti_entropy_interval = interval(ae_interval);

        // Metrics update interval (1 second)
        let mut metrics_interval = interval(Duration::from_secs(1));

        // Circuit breaker check interval (check if any circuits can be half-opened)
        let mut circuit_check_interval = interval(Duration::from_secs(10));

        loop {
            tokio::select! {
                // Receive deltas from shards
                Some(notif) = self.delta_rx.recv() => {
                    self.handle_delta(notif);
                }

                // Receive string updates to replicate
                Some(notif) = self.string_rx.recv() => {
                    self.handle_string_notification(notif).await;
                }

                // Receive quota operations to replicate
                Some(notif) = self.quota_rx.recv() => {
                    self.handle_quota_notification(notif).await;
                }

                // New outbound connection established
                Some((addr, mut conn)) = conn_rx.recv() => {
                    info!("Outbound connection to {} established", addr);
                    if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                        peer.reset_backoff();

                        // Send hello before splitting
                        let hello = Message::hello(self.config.node_id);
                        if let Err(e) = conn.send(&hello).await {
                            error!("Failed to send hello to {}: {}", addr, e);
                            peer.increase_backoff();
                            continue;
                        }

                        // Split connection into reader and writer
                        let (reader, writer) = conn.split();

                        // Store writer for sending deltas
                        self.writers.insert(addr, writer);

                        // Register peer for anti-entropy tracking
                        if let Some(ref mut ae) = self.anti_entropy {
                            ae.register_peer(addr);
                        }

                        // Spawn reader task
                        let msg_tx = msg_tx.clone();
                        let apply_delta = apply_delta.clone();
                        let disconnect_tx = disconnect_tx.clone();
                        tokio::spawn(async move {
                            Self::split_reader_task(addr, reader, msg_tx, apply_delta).await;
                            // Signal disconnect
                            let _ = disconnect_tx.send(addr).await;
                        });
                    }
                }

                // Receive inbound connection writers
                Some((addr, writer)) = inbound_rx.recv() => {
                    info!("Storing inbound writer for {}", addr);
                    self.inbound_writers.insert(addr, writer);
                }

                // Receive messages from peers
                Some((addr, msg)) = msg_rx.recv() => {
                    let responses = self.handle_peer_message(addr, msg);
                    if !responses.is_empty() {
                        // Try inbound_writers first (for messages from inbound connections),
                        // then fall back to outbound writers
                        let writer = self.inbound_writers.get_mut(&addr)
                            .or_else(|| self.writers.get_mut(&addr));

                        if let Some(writer) = writer {
                            for resp in responses {
                                if let Err(e) = writer.send(&resp).await {
                                    error!("Failed to send response to {}: {}", addr, e);
                                    break;
                                }
                            }
                        } else {
                            warn!("No writer found for {} to send {} responses", addr, responses.len());
                        }
                    }
                }

                // Periodic flush
                _ = flush_interval.tick() => {
                    self.flush_all_peers().await;
                }

                // Anti-entropy cycle
                _ = anti_entropy_interval.tick() => {
                    if let Some(ref mut ae) = self.anti_entropy {
                        ae.increment_cycle();

                        // Build and broadcast status to all connected peers
                        let status_msg = ae.build_status_message();
                        for (addr, writer) in &mut self.writers {
                            if let Err(e) = writer.send(&status_msg).await {
                                warn!("Failed to send status to {}: {}", addr, e);
                            }
                        }

                        // If digest cycle, also send digests
                        if ae.should_check_digest() {
                            let digest_msgs = ae.build_digest_messages();
                            for (addr, writer) in &mut self.writers {
                                for msg in &digest_msgs {
                                    if let Err(e) = writer.send(msg).await {
                                        warn!("Failed to send digest to {}: {}", addr, e);
                                        break;
                                    }
                                }
                            }
                        }

                        // Check for timed-out snapshot requests and retry them
                        let timed_out = ae.check_snapshot_timeouts();
                        for (addr, shards) in timed_out {
                            if let Some(writer) = self.writers.get_mut(&addr) {
                                for shard_id in shards {
                                    info!("Retrying snapshot request for shard {} to {}", shard_id, addr);
                                    // Re-request the snapshot
                                    let msg = Message::snapshot_request(shard_id);
                                    if let Err(e) = writer.send(&msg).await {
                                        warn!("Failed to retry snapshot request to {}: {}", addr, e);
                                        break;
                                    }
                                    // Mark as pending again
                                    if let Some(peer_state) = ae.peer_state_mut(&addr) {
                                        peer_state.mark_snapshot_pending(shard_id);
                                    }
                                }
                            }
                        }
                    }
                }

                // Update metrics periodically
                _ = metrics_interval.tick() => {
                    self.update_lag_metrics();
                    self.update_cluster_state();
                }

                // Handle disconnections
                Some(addr) = disconnect_rx.recv() => {
                    info!("Peer {} disconnected, removing writer", addr);
                    self.writers.remove(&addr);

                    // Unregister peer from anti-entropy tracking
                    if let Some(ref mut ae) = self.anti_entropy {
                        ae.unregister_peer(addr);
                    }

                    // Update peer state and check circuit breaker
                    let should_reconnect = if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                        let circuit_opened = peer.increase_backoff();
                        if circuit_opened {
                            warn!("Circuit breaker opened for peer {} after {} consecutive failures",
                                  addr, peer.consecutive_failures);
                            false // Don't reconnect - circuit is open
                        } else {
                            true // Can attempt reconnection
                        }
                    } else {
                        false
                    };

                    // Only trigger reconnection if circuit is not open
                    if should_reconnect {
                        let conn_tx = conn_tx.clone();
                        let reconnect_tx = reconnect_tx.clone();
                        let backoff = self.outbound_peers.get(&addr)
                            .map(|p| p.backoff)
                            .unwrap_or(Duration::from_millis(100));
                        tokio::spawn(async move {
                            // Wait for backoff before attempting reconnection
                            tokio::time::sleep(backoff).await;
                            Self::connector_task(addr, conn_tx, reconnect_tx).await;
                        });
                    }
                }

                // Handle reconnection requests (for future use)
                Some(_addr) = reconnect_rx.recv() => {
                    // Reconnection is handled in disconnect handler
                }

                // Periodic circuit breaker check - retry peers with open circuits after cool-off
                _ = circuit_check_interval.tick() => {
                    for addr in self.config.peers.clone() {
                        if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                            // Check if circuit is open but can now attempt connection (cool-off elapsed)
                            if peer.is_circuit_open() && peer.can_attempt_connection() {
                                info!("Circuit breaker cool-off elapsed for {}, attempting reconnection", addr);
                                peer.half_open_circuit();

                                // Spawn connector task
                                let conn_tx = conn_tx.clone();
                                let reconnect_tx = reconnect_tx.clone();
                                tokio::spawn(async move {
                                    Self::connector_task(addr, conn_tx, reconnect_tx).await;
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// Handle a delta notification from a shard
    fn handle_delta(&mut self, notif: DeltaNotification) {
        for peer in self.outbound_peers.values_mut() {
            peer.queue_delta(notif.delta.clone());
        }
    }

    /// Handle a string notification - broadcast to all peers
    async fn handle_string_notification(&mut self, notif: StringNotification) {
        debug!(
            "Replicating string key {:?} to {} peers",
            String::from_utf8_lossy(notif.key.as_bytes()),
            self.writers.len()
        );

        let msg = Message::string_set(notif.shard_id, notif.key, notif.value, notif.timestamp);

        // Send to all connected peers
        for (addr, writer) in &mut self.writers {
            if let Err(e) = writer.send(&msg).await {
                warn!("Failed to send StringSet to {}: {}", addr, e);
            } else {
                debug!("Sent StringSet to {}", addr);
            }
        }
    }

    /// Handle quota notifications: sync config or request tokens
    async fn handle_quota_notification(&mut self, notif: QuotaNotification) {
        match notif {
            QuotaNotification::Sync { shard_id, key, limit, window_secs } => {
                debug!(
                    "Syncing quota key {:?} (limit={}, window={}s) to {} peers",
                    String::from_utf8_lossy(key.as_bytes()),
                    limit,
                    window_secs,
                    self.writers.len()
                );

                let msg = Message::quota_sync(shard_id, key, limit, window_secs);

                // Send to all connected peers
                for (addr, writer) in &mut self.writers {
                    if let Err(e) = writer.send(&msg).await {
                        warn!("Failed to send QuotaSync to {}: {}", addr, e);
                    } else {
                        debug!("Sent QuotaSync to {}", addr);
                    }
                }
            }
            QuotaNotification::Request { shard_id, key, requested } => {
                // Find allocator node for this shard and send request
                let allocator_node_id = ((shard_id as usize) % self.num_nodes + 1) as u32;

                info!(
                    "Token request received: {} tokens for {:?} from allocator node {}",
                    requested,
                    String::from_utf8_lossy(key.as_bytes()),
                    allocator_node_id
                );

                // If we're the allocator, handle locally
                if allocator_node_id == self.config.node_id.as_u32() {
                    if let Some(ref db) = self.db {
                        let granted = db.allocator_grant(&key, self.config.node_id, requested);
                        if granted > 0 {
                            db.quota_add_tokens(&key, granted);
                            info!("Self-granted {} tokens for {:?}", granted, key);
                        }
                    }
                    return;
                }

                // Find the peer that is the allocator
                let msg = Message::quota_request(shard_id, key.clone(), requested, self.config.node_id.as_u32());
                for (addr, writer) in &mut self.writers {
                    if let Some(peer) = self.outbound_peers.get(addr) {
                        if peer.node_id == Some(NodeId::new(allocator_node_id)) {
                            if let Err(e) = writer.send(&msg).await {
                                warn!("Failed to send QuotaRequest to {}: {}", addr, e);
                            } else {
                                info!("Sent QuotaRequest to allocator {} (node {})", addr, allocator_node_id);
                            }
                            return;
                        }
                    }
                }
                warn!("Allocator node {} not connected, cannot request tokens", allocator_node_id);
            }
        }
    }

    /// Update replication lag metrics
    fn update_lag_metrics(&self) {
        let head_seqs: Vec<u64> = if let Some(ref db) = self.db {
            (0..self.config.num_shards)
                .map(|i| db.shard_head_seq(i))
                .collect()
        } else {
            return;
        };

        let mut max_lag = 0u64;
        let mut total_lag = 0u64;
        let mut per_peer = Vec::new();

        for (addr, peer) in &self.outbound_peers {
            if peer.state == crate::replication::ConnectionState::Connected {
                let peer_max = peer.max_lag(&head_seqs);
                let peer_total = peer.total_lag(&head_seqs);
                max_lag = max_lag.max(peer_max);
                total_lag += peer_total;
                per_peer.push((addr.to_string(), peer_max));
            }
        }

        let peer_count = per_peer.len();
        METRICS.update_replication_lag(ReplicationLagMetrics {
            max_lag,
            total_lag,
            peer_count,
            per_peer,
        });
    }

    /// Update cluster state snapshot for CLUSTER INFO
    fn update_cluster_state(&self) {
        use std::time::Instant;

        let peers: Vec<PeerInfo> = self.outbound_peers.values().map(|peer| {
            let (acked_min, acked_max) = if peer.acked_seq.is_empty() {
                (0, 0)
            } else {
                let min = *peer.acked_seq.iter().min().unwrap_or(&0);
                let max = *peer.acked_seq.iter().max().unwrap_or(&0);
                (min, max)
            };

            PeerInfo {
                addr: peer.addr,
                node_id: peer.node_id,
                state: peer.state,
                acked_seq_min: acked_min,
                acked_seq_max: acked_max,
                pending_count: peer.pending.len(),
                last_activity: Instant::now(), // Approximate, we don't track exact activity time
            }
        }).collect();

        self.cluster_state.update_peers(peers);
    }

    /// Handle a message from a peer
    /// Returns response messages to send back
    fn handle_peer_message(&mut self, addr: SocketAddr, msg: Message) -> Vec<Message> {
        match msg {
            Message::Ack { shard_id, acked_seq } => {
                if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                    peer.update_acked(shard_id, acked_seq);
                    debug!("Peer {} acked shard {} seq {}", addr, shard_id, acked_seq);

                    // Calculate and record RTT
                    if let Some(rtt_us) = peer.calculate_rtt_on_ack() {
                        METRICS.record_replication_rtt(rtt_us);
                    }
                }
                vec![]
            }
            Message::Hello { node_id } => {
                if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                    peer.node_id = Some(node_id);
                    info!("Peer {} identified as node {}", addr, node_id.as_u32());
                }
                vec![]
            }
            Message::DeltaBatch { .. } => {
                // Inbound deltas are handled by the reader task directly
                vec![]
            }
            Message::QuotaRequest { shard_id, key, requested, from_node_id } => {
                // Handle token request from a remote node
                let from_node = NodeId::new(from_node_id);
                info!(
                    "Received QuotaRequest from node {} for key {:?}, shard {}, requested {}",
                    from_node_id, String::from_utf8_lossy(key.as_bytes()), shard_id, requested
                );

                // Process if we're the allocator for this shard
                if self.is_allocator(shard_id) {
                    if let Some(ref db) = self.db {
                        // Try to grant tokens
                        let batch_size = db.quota_batch_size(&key);
                        let granted = db.allocator_grant(&key, from_node, batch_size.max(requested));

                        if granted > 0 {
                            info!("Granting {} tokens for {:?} to node {}", granted, String::from_utf8_lossy(key.as_bytes()), from_node_id);
                            return vec![Message::quota_grant(shard_id, key, granted)];
                        } else {
                            info!("Denying token request for {:?} to node {}", String::from_utf8_lossy(key.as_bytes()), from_node_id);
                            return vec![Message::quota_deny(shard_id, key)];
                        }
                    }
                }
                vec![]
            }
            Message::QuotaGrant { shard_id, key, granted } => {
                // Handle token grant from allocator - add tokens to local quota entry
                info!(
                    "Received QuotaGrant from {} for key {:?}, shard {}, granted {}",
                    addr, String::from_utf8_lossy(key.as_bytes()), shard_id, granted
                );

                if let Some(ref db) = self.db {
                    db.quota_add_tokens(&key, granted);
                    info!("Added {} tokens for {:?}", granted, key);
                }
                vec![]
            }
            Message::QuotaDeny { shard_id, key } => {
                // Handle token denial from allocator
                debug!(
                    "Received QuotaDeny from {} for key {:?}, shard {}",
                    addr, key, shard_id
                );
                // Request is denied, nothing to do - client already got -1
                vec![]
            }
            Message::QuotaSync { shard_id, key, limit, window_secs } => {
                // Handle quota configuration sync from another node
                debug!(
                    "Received QuotaSync from {} for key {:?}, shard {}, limit {}, window {}s",
                    addr, key, shard_id, limit, window_secs
                );

                // Create/update local quota entry if we don't have it
                if let Some(ref db) = self.db {
                    if !db.is_quota(&key) {
                        db.quota_set(key, limit, window_secs);
                        info!("Created quota from sync: limit={}, window={}s", limit, window_secs);
                    }
                }
                vec![]
            }
            // Anti-entropy messages
            Message::Status { shard_seqs } => {
                if let Some(ref mut ae) = self.anti_entropy {
                    ae.handle_status(addr, shard_seqs)
                } else {
                    vec![]
                }
            }
            Message::DeltaRequest { shard_id, from_seq, to_seq } => {
                debug!("Received DeltaRequest for shard {} from {} to {}", shard_id, from_seq, to_seq);
                // TODO: Future - fetch from replication log and send
                vec![]
            }
            Message::DigestExchange { shard_id, head_seq, digest } => {
                if let Some(ref mut ae) = self.anti_entropy {
                    ae.handle_digest_exchange(addr, shard_id, head_seq, digest)
                        .map(|m| vec![m])
                        .unwrap_or_default()
                } else {
                    vec![]
                }
            }
            Message::SnapshotRequest { shard_id } => {
                if let Some(ref ae) = self.anti_entropy {
                    ae.handle_snapshot_request(shard_id)
                        .map(|m| vec![m])
                        .unwrap_or_default()
                } else {
                    vec![]
                }
            }
            Message::Snapshot { shard_id, head_seq, digest, entries } => {
                if let Some(ref mut ae) = self.anti_entropy {
                    ae.handle_snapshot(addr, shard_id, head_seq, digest, entries);
                }
                vec![]
            }
            Message::StringSet { shard_id: _, key, value, timestamp } => {
                // Handle string replication from another node (LWW semantics)
                debug!(
                    "Received StringSet from {} for key {:?}, timestamp {}",
                    addr, String::from_utf8_lossy(key.as_bytes()), timestamp
                );

                if let Some(ref db) = self.db {
                    // Use set_string_with_timestamp to apply LWW semantics
                    db.set_string_with_timestamp(key, value, timestamp);
                }
                vec![]
            }
        }
    }

    /// Flush pending deltas to all peers
    async fn flush_all_peers(&mut self) {
        // Collect addresses to iterate (avoid borrow issues)
        let addrs: Vec<SocketAddr> = self.outbound_peers.keys().copied().collect();

        for addr in addrs {
            // Check if we have pending deltas and an active writer
            let deltas = if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                if peer.pending.is_empty() {
                    continue;
                }
                peer.take_pending()
            } else {
                continue;
            };

            // Group deltas by shard for efficient batching
            let mut by_shard: HashMap<u16, Vec<Delta>> = HashMap::new();
            for delta in deltas {
                // Use the shard_id from the delta's key hash
                let shard_id = (delta.key.shard_hash() as u16) % (self.config.num_shards as u16);
                by_shard.entry(shard_id).or_default().push(delta);
            }

            // Send batches to the peer
            if let Some(writer) = self.writers.get_mut(&addr) {
                let mut sent = false;
                for (shard_id, shard_deltas) in by_shard {
                    let msg = Message::delta_batch(shard_id, shard_deltas);
                    if let Err(e) = writer.send(&msg).await {
                        error!("Failed to send delta batch to {}: {}", addr, e);
                        // Remove writer on error - will reconnect
                        self.writers.remove(&addr);
                        if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                            peer.state = crate::replication::ConnectionState::Disconnected;
                        }
                        break;
                    } else {
                        sent = true;
                        debug!("Sent {} deltas to {} for shard {}",
                               msg.delta_count(), addr, shard_id);
                    }
                }
                // Mark batch sent for RTT tracking
                if sent {
                    if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                        peer.mark_batch_sent();
                    }
                }
            }
        }
    }

    /// Connector task - attempts to connect to a peer with backoff
    /// Returns after successful connection (or permanent failure)
    async fn connector_task(
        addr: SocketAddr,
        conn_tx: mpsc::Sender<(SocketAddr, PeerConnection)>,
        _reconnect_tx: mpsc::Sender<SocketAddr>,
    ) {
        let mut backoff = Duration::from_millis(100);

        loop {
            debug!("Attempting to connect to peer {}", addr);

            match timeout(Duration::from_secs(5), PeerConnection::connect(addr)).await {
                Ok(Ok(conn)) => {
                    info!("Connected to peer {}", addr);

                    if conn_tx.send((addr, conn)).await.is_err() {
                        return; // Manager shut down
                    }

                    // Connection established - return and let the manager handle it
                    // If the connection closes, the manager will spawn a new connector
                    return;
                }
                Ok(Err(e)) => {
                    warn!("Failed to connect to peer {}: {}", addr, e);
                }
                Err(_) => {
                    warn!("Connection to peer {} timed out", addr);
                }
            }

            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(10));
        }
    }

    /// Listener task - accepts incoming connections
    async fn listener_task(
        listener: TcpListener,
        node_id: NodeId,
        msg_tx: mpsc::Sender<(SocketAddr, Message)>,
        apply_delta: Arc<dyn Fn(u16, Delta) + Send + Sync>,
        inbound_tx: mpsc::Sender<(SocketAddr, PeerConnectionWriter)>,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("Accepted replication connection from {}", addr);

                    match PeerConnection::from_stream(stream) {
                        Ok(mut conn) => {
                            // Send hello
                            let hello = Message::hello(node_id);
                            if let Err(e) = conn.send(&hello).await {
                                error!("Failed to send hello to {}: {}", addr, e);
                                continue;
                            }

                            // Split connection into reader and writer
                            let (reader, writer) = conn.split();

                            // Send writer to main loop for response handling
                            if let Err(e) = inbound_tx.send((addr, writer)).await {
                                error!("Failed to send inbound writer for {}: {}", addr, e);
                                continue;
                            }

                            // Spawn reader task (using split reader that works with just the reader part)
                            let msg_tx = msg_tx.clone();
                            let apply_delta = apply_delta.clone();
                            tokio::spawn(async move {
                                Self::split_reader_task(addr, reader, msg_tx, apply_delta).await;
                            });
                        }
                        Err(e) => {
                            error!("Failed to create connection from {}: {}", addr, e);
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Connection reader task - reads messages from a peer (for incoming connections)
    async fn connection_reader_task(
        addr: SocketAddr,
        mut conn: PeerConnection,
        msg_tx: mpsc::Sender<(SocketAddr, Message)>,
        apply_delta: Arc<dyn Fn(u16, Delta) + Send + Sync>,
    ) {
        loop {
            match conn.recv().await {
                Ok(msg) => {
                    match &msg {
                        Message::DeltaBatch { shard_id, deltas } => {
                            // Apply deltas directly
                            for delta in deltas {
                                apply_delta(*shard_id, delta.clone());
                            }

                            // Send ack
                            if let Some(last) = deltas.last() {
                                let ack = Message::ack(*shard_id, last.seq);
                                if let Err(e) = conn.send(&ack).await {
                                    error!("Failed to send ack to {}: {}", addr, e);
                                    break;
                                }
                            }
                        }
                        _ => {
                            // Forward other messages to manager
                            if msg_tx.send((addr, msg)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Connection to {} closed: {}", addr, e);
                    break;
                }
            }
        }
    }

    /// Split reader task - reads messages from a split connection (for outbound connections)
    async fn split_reader_task(
        addr: SocketAddr,
        mut reader: PeerConnectionReader,
        msg_tx: mpsc::Sender<(SocketAddr, Message)>,
        apply_delta: Arc<dyn Fn(u16, Delta) + Send + Sync>,
    ) {
        loop {
            match reader.recv().await {
                Ok(msg) => {
                    match &msg {
                        Message::DeltaBatch { shard_id, deltas } => {
                            // Apply deltas directly (inbound from peer)
                            debug!("Received {} deltas from {} for shard {}", deltas.len(), addr, shard_id);
                            for delta in deltas {
                                apply_delta(*shard_id, delta.clone());
                            }
                            // Note: We can't send acks here since we don't have the writer
                            // Acks will be sent by the peer when they receive our batches
                        }
                        Message::Ack { .. }
                        | Message::Hello { .. }
                        | Message::QuotaRequest { .. }
                        | Message::QuotaGrant { .. }
                        | Message::QuotaDeny { .. }
                        | Message::QuotaSync { .. }
                        | Message::Status { .. }
                        | Message::DeltaRequest { .. }
                        | Message::DigestExchange { .. }
                        | Message::SnapshotRequest { .. }
                        | Message::Snapshot { .. }
                        | Message::StringSet { .. } => {
                            // Forward to manager for processing
                            if msg_tx.send((addr, msg)).await.is_err() {
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Connection to {} closed: {}", addr, e);
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_config_default() {
        let config = ReplicationConfig::default();
        assert_eq!(config.listen_port, 6381);
        assert_eq!(config.batch_max_size, 100);
        assert_eq!(config.batch_max_delay, Duration::from_millis(10));
    }

    #[test]
    fn test_replication_handle() {
        let config = ReplicationConfig::default();
        let (_manager, handle) = ReplicationManager::new(config);

        let delta = Delta::increment(0, crate::types::Key::from("test"), NodeId::new(1), 10);
        handle.send(0, delta);
        // Message sent (channel not closed)
    }
}
