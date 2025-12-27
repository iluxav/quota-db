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
}

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
    }

    /// Increase backoff on connection failure
    pub fn increase_backoff(&mut self) {
        self.backoff = (self.backoff * 2).min(Duration::from_secs(10));
        self.state = ConnectionState::Disconnected;
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
            write_buf: BytesMut::with_capacity(8192),
        })
    }

    /// Accept an incoming connection
    pub fn from_stream(stream: TcpStream) -> std::io::Result<Self> {
        stream.set_nodelay(true)?;
        Ok(Self {
            stream,
            decoder: FrameDecoder::new(),
            write_buf: BytesMut::with_capacity(8192),
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

    /// Split into read and write halves for concurrent operations
    pub fn split(self) -> (PeerConnectionReader, PeerConnectionWriter) {
        let (read_half, write_half) = self.stream.into_split();
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
}
