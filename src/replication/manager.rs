use crate::engine::ShardedDb;
use crate::replication::{Delta, Message, PeerConnection, PeerConnectionReader, PeerConnectionWriter, PeerState};
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
        }
    }
}

/// Handle for sending deltas to the replication manager
#[derive(Clone)]
pub struct ReplicationHandle {
    tx: mpsc::UnboundedSender<DeltaNotification>,
}

impl ReplicationHandle {
    /// Send a delta to be replicated
    pub fn send(&self, shard_id: u16, delta: Delta) {
        let _ = self.tx.send(DeltaNotification { shard_id, delta });
    }
}

/// Replication manager - handles all peer connections and delta streaming
pub struct ReplicationManager {
    config: ReplicationConfig,
    /// Channel to receive deltas from shards
    delta_rx: mpsc::UnboundedReceiver<DeltaNotification>,
    /// Outbound peer states (we connect to these)
    outbound_peers: HashMap<SocketAddr, PeerState>,
    /// Active writers for sending to peers
    writers: HashMap<SocketAddr, PeerConnectionWriter>,
    /// Database reference for quota operations (optional)
    db: Option<Arc<ShardedDb>>,
    /// Number of nodes in cluster for allocator assignment
    num_nodes: usize,
}

impl ReplicationManager {
    /// Create a new replication manager
    pub fn new(config: ReplicationConfig) -> (Self, ReplicationHandle) {
        let (tx, rx) = mpsc::unbounded_channel();

        let num_nodes = config.peers.len() + 1; // peers + self
        let mut outbound_peers = HashMap::new();
        for addr in &config.peers {
            outbound_peers.insert(*addr, PeerState::new(*addr, config.num_shards));
        }

        let manager = Self {
            config,
            delta_rx: rx,
            outbound_peers,
            writers: HashMap::new(),
            db: None,
            num_nodes,
        };

        let handle = ReplicationHandle { tx };
        (manager, handle)
    }

    /// Set the database reference for quota operations
    pub fn set_db(&mut self, db: Arc<ShardedDb>) {
        self.db = Some(db);
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
            tokio::spawn(async move {
                Self::listener_task(listener, node_id, msg_tx_clone, apply_delta_clone).await;
            });
        }

        // Main loop
        let mut flush_interval = interval(self.config.batch_max_delay);

        loop {
            tokio::select! {
                // Receive deltas from shards
                Some(notif) = self.delta_rx.recv() => {
                    self.handle_delta(notif);
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

                // Receive messages from peers
                Some((addr, msg)) = msg_rx.recv() => {
                    if let Some(response) = self.handle_peer_message(addr, msg) {
                        // Send response back to the peer
                        if let Some(writer) = self.writers.get_mut(&addr) {
                            if let Err(e) = writer.send(&response).await {
                                error!("Failed to send quota response to {}: {}", addr, e);
                            }
                        }
                    }
                }

                // Periodic flush
                _ = flush_interval.tick() => {
                    self.flush_all_peers().await;
                }

                // Handle disconnections
                Some(addr) = disconnect_rx.recv() => {
                    info!("Peer {} disconnected, removing writer", addr);
                    self.writers.remove(&addr);
                    if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                        peer.state = crate::replication::ConnectionState::Disconnected;
                        peer.increase_backoff();
                    }
                    // Trigger reconnection
                    let conn_tx = conn_tx.clone();
                    let reconnect_tx = reconnect_tx.clone();
                    tokio::spawn(async move {
                        Self::connector_task(addr, conn_tx, reconnect_tx).await;
                    });
                }

                // Handle reconnection requests (for future use)
                Some(_addr) = reconnect_rx.recv() => {
                    // Reconnection is handled in disconnect handler
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

    /// Handle a message from a peer
    /// Returns an optional response message to send back
    fn handle_peer_message(&mut self, addr: SocketAddr, msg: Message) -> Option<Message> {
        match msg {
            Message::Ack { shard_id, acked_seq } => {
                if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                    peer.update_acked(shard_id, acked_seq);
                    debug!("Peer {} acked shard {} seq {}", addr, shard_id, acked_seq);
                }
                None
            }
            Message::Hello { node_id } => {
                if let Some(peer) = self.outbound_peers.get_mut(&addr) {
                    peer.node_id = Some(node_id);
                    info!("Peer {} identified as node {}", addr, node_id.as_u32());
                }
                None
            }
            Message::DeltaBatch { .. } => {
                // Inbound deltas are handled by the reader task directly
                None
            }
            Message::QuotaRequest { shard_id, key, requested } => {
                // Handle token request from a remote node
                debug!(
                    "Received QuotaRequest from {} for key {:?}, shard {}, requested {}",
                    addr, key, shard_id, requested
                );

                // Process if we're the allocator for this shard
                if self.is_allocator(shard_id) {
                    if let Some(ref db) = self.db {
                        // Get the requesting node's ID from peer state
                        let from_node = self.outbound_peers.get(&addr)
                            .and_then(|p| p.node_id)
                            .unwrap_or_else(|| NodeId::new(0));

                        // Try to grant tokens
                        let batch_size = db.quota_batch_size(&key);
                        let granted = db.allocator_grant(&key, from_node, batch_size.max(requested));

                        if granted > 0 {
                            debug!("Granting {} tokens for {:?} to node {}", granted, key, from_node.as_u32());
                            return Some(Message::quota_grant(shard_id, key, granted));
                        } else {
                            debug!("Denying token request for {:?} to node {}", key, from_node.as_u32());
                            return Some(Message::quota_deny(shard_id, key));
                        }
                    }
                }
                None
            }
            Message::QuotaGrant { shard_id, key, granted } => {
                // Handle token grant from allocator - add tokens to local quota entry
                debug!(
                    "Received QuotaGrant from {} for key {:?}, shard {}, granted {}",
                    addr, key, shard_id, granted
                );

                if let Some(ref db) = self.db {
                    db.quota_add_tokens(&key, granted);
                    info!("Added {} tokens for {:?}", granted, key);
                }
                None
            }
            Message::QuotaDeny { shard_id, key } => {
                // Handle token denial from allocator
                debug!(
                    "Received QuotaDeny from {} for key {:?}, shard {}",
                    addr, key, shard_id
                );
                // Request is denied, nothing to do - client already got -1
                None
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
                None
            }
            // Anti-entropy messages - will be handled by the anti-entropy subsystem
            Message::Status { .. } |
            Message::DeltaRequest { .. } |
            Message::DigestExchange { .. } |
            Message::SnapshotRequest { .. } |
            Message::Snapshot { .. } => {
                // Anti-entropy messages are handled separately
                debug!("Received anti-entropy message from {}, ignoring in base handler", addr);
                None
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
                        debug!("Sent {} deltas to {} for shard {}",
                               msg.delta_count(), addr, shard_id);
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

                            // Spawn reader task
                            let msg_tx = msg_tx.clone();
                            let apply_delta = apply_delta.clone();
                            tokio::spawn(async move {
                                Self::connection_reader_task(addr, conn, msg_tx, apply_delta).await;
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
                        | Message::Snapshot { .. } => {
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
