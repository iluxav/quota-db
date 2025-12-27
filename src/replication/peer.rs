use crate::pool::buffer_pool;
use crate::replication::{Delta, FrameDecoder, Message};
use crate::types::NodeId;
use bytes::BytesMut;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// State of a peer connection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    /// Circuit breaker is open - not attempting connections
    CircuitOpen,
}

/// Maximum consecutive failures before opening circuit breaker
const MAX_CONSECUTIVE_FAILURES: u32 = 10;

/// Cool-off period when circuit breaker is open
const CIRCUIT_OPEN_DURATION: Duration = Duration::from_secs(60);

/// Peer node state for replication
pub struct PeerState {
    /// Peer's node ID (learned from Hello message)
    pub node_id: Option<NodeId>,
    /// Peer's replication address
    pub addr: SocketAddr,
    /// Acknowledged sequence per shard
    pub acked_seq: Vec<u64>,
    /// Pending deltas to send (batched)
    pub pending: Vec<Delta>,
    /// Last time we flushed the pending batch
    pub last_flush: Instant,
    /// Connection state
    pub state: ConnectionState,
    /// Reconnect backoff
    pub backoff: Duration,
    /// Timestamp when last batch was sent (for RTT calculation)
    pub last_batch_sent: Option<Instant>,
    /// Consecutive connection failures (for circuit breaker)
    pub consecutive_failures: u32,
    /// When circuit breaker opened (for cool-off timing)
    pub circuit_opened_at: Option<Instant>,
}

impl PeerState {
    /// Create a new peer state
    pub fn new(addr: SocketAddr, num_shards: usize) -> Self {
        Self {
            node_id: None,
            addr,
            acked_seq: vec![0; num_shards],
            pending: Vec::with_capacity(128),
            last_flush: Instant::now(),
            state: ConnectionState::Disconnected,
            backoff: Duration::from_millis(100),
            last_batch_sent: None,
            consecutive_failures: 0,
            circuit_opened_at: None,
        }
    }

    /// Queue a delta for sending
    #[inline]
    pub fn queue_delta(&mut self, delta: Delta) {
        self.pending.push(delta);
    }

    /// Check if we should flush based on batch size
    #[inline]
    pub fn should_flush_size(&self, max_size: usize) -> bool {
        self.pending.len() >= max_size
    }

    /// Check if we should flush based on time
    #[inline]
    pub fn should_flush_time(&self, max_delay: Duration) -> bool {
        !self.pending.is_empty() && self.last_flush.elapsed() >= max_delay
    }

    /// Take pending deltas for sending
    pub fn take_pending(&mut self) -> Vec<Delta> {
        self.last_flush = Instant::now();
        std::mem::take(&mut self.pending)
    }

    /// Update acked sequence for a shard
    pub fn update_acked(&mut self, shard_id: u16, seq: u64) {
        if (shard_id as usize) < self.acked_seq.len() {
            self.acked_seq[shard_id as usize] = seq;
        }
    }

    /// Reset backoff on successful connection
    pub fn reset_backoff(&mut self) {
        self.backoff = Duration::from_millis(100);
        self.state = ConnectionState::Connected;
        self.consecutive_failures = 0;
        self.circuit_opened_at = None;
    }

    /// Increase backoff on connection failure.
    /// Returns true if circuit breaker was opened.
    pub fn increase_backoff(&mut self) -> bool {
        self.consecutive_failures += 1;
        self.backoff = (self.backoff * 2).min(Duration::from_secs(10));

        if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            self.state = ConnectionState::CircuitOpen;
            self.circuit_opened_at = Some(Instant::now());
            true
        } else {
            self.state = ConnectionState::Disconnected;
            false
        }
    }

    /// Check if the circuit breaker allows a connection attempt.
    /// Returns true if we can attempt to connect.
    pub fn can_attempt_connection(&self) -> bool {
        match self.state {
            ConnectionState::Disconnected => true,
            ConnectionState::CircuitOpen => {
                // Check if cool-off period has elapsed
                if let Some(opened_at) = self.circuit_opened_at {
                    opened_at.elapsed() >= CIRCUIT_OPEN_DURATION
                } else {
                    true
                }
            }
            ConnectionState::Connecting | ConnectionState::Connected => false,
        }
    }

    /// Reset circuit breaker after cool-off period.
    /// Call this when attempting a new connection after circuit was open.
    pub fn half_open_circuit(&mut self) {
        if self.state == ConnectionState::CircuitOpen {
            // Move to "half-open" state - will fully close on success, reopen on failure
            self.state = ConnectionState::Connecting;
            // Keep failure count for now - will reset on success
        }
    }

    /// Check if circuit breaker is currently open
    pub fn is_circuit_open(&self) -> bool {
        self.state == ConnectionState::CircuitOpen
    }

    /// Mark that a batch was sent (for RTT calculation)
    pub fn mark_batch_sent(&mut self) {
        self.last_batch_sent = Some(Instant::now());
    }

    /// Calculate RTT if we have a pending batch and received an ACK
    /// Returns the RTT in microseconds if calculable
    pub fn calculate_rtt_on_ack(&mut self) -> Option<u64> {
        if let Some(sent_at) = self.last_batch_sent.take() {
            Some(sent_at.elapsed().as_micros() as u64)
        } else {
            None
        }
    }

    /// Calculate max lag across all shards given the local head sequences
    pub fn max_lag(&self, head_seqs: &[u64]) -> u64 {
        let mut max_lag = 0u64;
        for (shard_id, &head_seq) in head_seqs.iter().enumerate() {
            if shard_id < self.acked_seq.len() {
                let acked = self.acked_seq[shard_id];
                let lag = head_seq.saturating_sub(acked);
                max_lag = max_lag.max(lag);
            }
        }
        max_lag
    }

    /// Calculate total lag across all shards
    pub fn total_lag(&self, head_seqs: &[u64]) -> u64 {
        let mut total = 0u64;
        for (shard_id, &head_seq) in head_seqs.iter().enumerate() {
            if shard_id < self.acked_seq.len() {
                let acked = self.acked_seq[shard_id];
                total += head_seq.saturating_sub(acked);
            }
        }
        total
    }
}

/// Active connection to a peer
pub struct PeerConnection {
    stream: TcpStream,
    decoder: FrameDecoder,
    write_buf: BytesMut,
}

impl PeerConnection {
    /// Connect to a peer
    pub async fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            decoder: FrameDecoder::new(),
            write_buf: buffer_pool().get_with_capacity(8192),
        })
    }

    /// Accept an incoming connection
    pub fn from_stream(stream: TcpStream) -> std::io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            decoder: FrameDecoder::new(),
            write_buf: buffer_pool().get_with_capacity(8192),
        })
    }

    /// Send a message
    pub async fn send(&mut self, msg: &Message) -> std::io::Result<()> {
        self.write_buf.clear();
        msg.encode(&mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        Ok(())
    }

    /// Send multiple messages in a batch
    pub async fn send_batch(&mut self, msgs: &[Message]) -> std::io::Result<()> {
        self.write_buf.clear();
        for msg in msgs {
            msg.encode(&mut self.write_buf);
        }
        self.stream.write_all(&self.write_buf).await?;
        Ok(())
    }

    /// Try to receive a message (non-blocking check)
    pub async fn try_recv(&mut self) -> std::io::Result<Option<Message>> {
        // First check if we have a complete message in buffer
        if let Some(msg) = self.decoder.decode() {
            return Ok(Some(msg));
        }

        // Try to read more data
        let mut buf = [0u8; 4096];
        match self.stream.try_read(&mut buf) {
            Ok(0) => Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection closed",
            )),
            Ok(n) => {
                self.decoder.extend(&buf[..n]);
                Ok(self.decoder.decode())
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Receive a message (blocking)
    pub async fn recv(&mut self) -> std::io::Result<Message> {
        loop {
            if let Some(msg) = self.decoder.decode() {
                return Ok(msg);
            }

            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection closed",
                ));
            }
            self.decoder.extend(&buf[..n]);
        }
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Split into read and write halves for concurrent operations.
    /// Note: The write buffer is transferred to PeerConnectionWriter,
    /// which handles returning it to the pool on drop.
    pub fn split(self) -> (PeerConnectionReader, PeerConnectionWriter) {
        let (read_half, write_half) = self.stream.into_split();

        // Prevent Drop from running on self by using ManuallyDrop semantics
        // Actually, split() consumes self, so Drop won't run - the buffer
        // is moved to PeerConnectionWriter which has its own Drop impl.

        (
            PeerConnectionReader {
                stream: read_half,
                decoder: self.decoder,
            },
            PeerConnectionWriter {
                stream: write_half,
                write_buf: self.write_buf,
            },
        )
    }
}

// Note: PeerConnection does NOT implement Drop because:
// 1. If split() is called, self is consumed and buffer goes to PeerConnectionWriter
// 2. If not split, the buffer is just dropped normally (it's small overhead)
// PeerConnectionWriter has the Drop impl since it's the final owner after split.

/// Read half of a peer connection
pub struct PeerConnectionReader {
    stream: tokio::net::tcp::OwnedReadHalf,
    decoder: FrameDecoder,
}

impl PeerConnectionReader {
    /// Receive a message
    pub async fn recv(&mut self) -> std::io::Result<Message> {
        loop {
            if let Some(msg) = self.decoder.decode() {
                return Ok(msg);
            }

            let mut buf = [0u8; 4096];
            let n = self.stream.read(&mut buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "connection closed",
                ));
            }
            self.decoder.extend(&buf[..n]);
        }
    }
}

/// Write half of a peer connection
pub struct PeerConnectionWriter {
    stream: tokio::net::tcp::OwnedWriteHalf,
    write_buf: BytesMut,
}

impl PeerConnectionWriter {
    /// Send a message
    pub async fn send(&mut self, msg: &Message) -> std::io::Result<()> {
        self.write_buf.clear();
        msg.encode(&mut self.write_buf);
        self.stream.write_all(&self.write_buf).await?;
        Ok(())
    }
}

impl Drop for PeerConnectionWriter {
    fn drop(&mut self) {
        // Return buffer to pool for reuse
        buffer_pool().put(std::mem::take(&mut self.write_buf));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_state_new() {
        let addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        let state = PeerState::new(addr, 64);

        assert_eq!(state.acked_seq.len(), 64);
        assert!(state.pending.is_empty());
        assert_eq!(state.state, ConnectionState::Disconnected);
    }

    #[test]
    fn test_peer_state_queue_delta() {
        let addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        let mut state = PeerState::new(addr, 64);

        let delta = Delta::increment(0, crate::types::Key::from("key"), NodeId::new(1), 10);
        state.queue_delta(delta);

        assert_eq!(state.pending.len(), 1);
        assert!(state.should_flush_size(1));
        assert!(!state.should_flush_size(2));
    }

    #[test]
    fn test_peer_state_backoff() {
        let addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        let mut state = PeerState::new(addr, 64);

        assert_eq!(state.backoff, Duration::from_millis(100));

        state.increase_backoff();
        assert_eq!(state.backoff, Duration::from_millis(200));

        state.increase_backoff();
        assert_eq!(state.backoff, Duration::from_millis(400));

        state.reset_backoff();
        assert_eq!(state.backoff, Duration::from_millis(100));
    }

    #[test]
    fn test_circuit_breaker() {
        let addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        let mut state = PeerState::new(addr, 64);

        assert!(state.can_attempt_connection());
        assert!(!state.is_circuit_open());

        // Fail MAX_CONSECUTIVE_FAILURES - 1 times, should not open circuit
        for _ in 0..(MAX_CONSECUTIVE_FAILURES - 1) {
            let opened = state.increase_backoff();
            assert!(!opened);
            assert!(!state.is_circuit_open());
        }

        // One more failure should open the circuit
        let opened = state.increase_backoff();
        assert!(opened);
        assert!(state.is_circuit_open());
        assert!(!state.can_attempt_connection()); // Circuit is open, can't connect

        // Successful connection should reset everything
        state.reset_backoff();
        assert!(!state.is_circuit_open());
        // After reset, state is Connected, so can_attempt_connection returns false
        // (we don't want to attempt a connection when already connected)
        assert!(!state.can_attempt_connection());
        assert_eq!(state.state, ConnectionState::Connected);
        assert_eq!(state.consecutive_failures, 0);

        // After disconnection, should be able to attempt connection again
        state.state = ConnectionState::Disconnected;
        assert!(state.can_attempt_connection());
    }

    #[test]
    fn test_circuit_half_open() {
        let addr: SocketAddr = "127.0.0.1:6381".parse().unwrap();
        let mut state = PeerState::new(addr, 64);

        // Open the circuit
        for _ in 0..MAX_CONSECUTIVE_FAILURES {
            state.increase_backoff();
        }
        assert!(state.is_circuit_open());

        // Half-open the circuit (simulating retry after cool-off)
        state.half_open_circuit();
        assert_eq!(state.state, ConnectionState::Connecting);
        assert!(!state.is_circuit_open());

        // If connection fails again, circuit should reopen immediately
        let opened = state.increase_backoff();
        assert!(opened);
        assert!(state.is_circuit_open());
    }
}
