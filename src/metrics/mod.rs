//! Observability and metrics collection for QuotaDB.
//!
//! Provides lock-free counters, gauges, and histograms for tracking
//! operational metrics like command counts, latencies, and replication lag.

mod histogram;

pub use histogram::LatencyHistogram;

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use parking_lot::RwLock;

/// Global metrics instance for the server.
pub static METRICS: Metrics = Metrics::new();

/// Relaxed ordering for counters (eventual visibility is fine for metrics).
const RELAXED: Ordering = Ordering::Relaxed;

/// Collection of all server metrics.
pub struct Metrics {
    // Command counters
    pub commands_total: AtomicU64,
    pub commands_incr: AtomicU64,
    pub commands_decr: AtomicU64,
    pub commands_get: AtomicU64,
    pub commands_set: AtomicU64,
    pub commands_ping: AtomicU64,
    pub commands_info: AtomicU64,
    pub commands_quota: AtomicU64,
    pub commands_other: AtomicU64,

    // Error counters
    pub errors_parse: AtomicU64,
    pub errors_unknown_cmd: AtomicU64,

    // Connection counters
    pub connections_total: AtomicU64,
    pub connections_active: AtomicU64,

    // Replication metrics
    pub replication_deltas_sent: AtomicU64,
    pub replication_deltas_received: AtomicU64,
    pub replication_bytes_sent: AtomicU64,
    pub replication_bytes_received: AtomicU64,

    // Persistence metrics
    pub wal_writes: AtomicU64,
    pub wal_bytes_written: AtomicU64,
    pub snapshots_created: AtomicU64,

    // Latency histograms (stored separately for each command type)
    pub latency_incr: LatencyHistogram,
    pub latency_decr: LatencyHistogram,
    pub latency_get: LatencyHistogram,
    pub latency_set: LatencyHistogram,

    // Replication RTT histogram (microseconds)
    pub replication_rtt: LatencyHistogram,

    // Server start time (set on first access)
    start_time: AtomicU64,

    // Replication lag per peer/shard (updated periodically)
    // Uses RwLock since it's updated infrequently but read for INFO
    replication_lag: RwLock<ReplicationLagMetrics>,
}

/// Replication lag metrics per peer
#[derive(Debug, Default, Clone)]
pub struct ReplicationLagMetrics {
    /// Max lag across all shards/peers
    pub max_lag: u64,
    /// Total lag across all shards/peers
    pub total_lag: u64,
    /// Number of peers tracked
    pub peer_count: usize,
    /// Per-peer lag info (addr -> max shard lag)
    pub per_peer: Vec<(String, u64)>,
}

impl Metrics {
    /// Create a new metrics instance with all counters at zero.
    pub const fn new() -> Self {
        Self {
            commands_total: AtomicU64::new(0),
            commands_incr: AtomicU64::new(0),
            commands_decr: AtomicU64::new(0),
            commands_get: AtomicU64::new(0),
            commands_set: AtomicU64::new(0),
            commands_ping: AtomicU64::new(0),
            commands_info: AtomicU64::new(0),
            commands_quota: AtomicU64::new(0),
            commands_other: AtomicU64::new(0),

            errors_parse: AtomicU64::new(0),
            errors_unknown_cmd: AtomicU64::new(0),

            connections_total: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),

            replication_deltas_sent: AtomicU64::new(0),
            replication_deltas_received: AtomicU64::new(0),
            replication_bytes_sent: AtomicU64::new(0),
            replication_bytes_received: AtomicU64::new(0),

            wal_writes: AtomicU64::new(0),
            wal_bytes_written: AtomicU64::new(0),
            snapshots_created: AtomicU64::new(0),

            latency_incr: LatencyHistogram::new(),
            latency_decr: LatencyHistogram::new(),
            latency_get: LatencyHistogram::new(),
            latency_set: LatencyHistogram::new(),

            replication_rtt: LatencyHistogram::new(),

            start_time: AtomicU64::new(0),

            replication_lag: RwLock::new(ReplicationLagMetrics {
                max_lag: 0,
                total_lag: 0,
                peer_count: 0,
                per_peer: Vec::new(),
            }),
        }
    }

    /// Initialize server start time. Call once at startup.
    pub fn init_start_time(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        self.start_time.store(now, RELAXED);
    }

    /// Get server uptime in seconds.
    pub fn uptime_secs(&self) -> u64 {
        let start = self.start_time.load(RELAXED);
        if start == 0 {
            return 0;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(start)
    }

    /// Increment a counter.
    #[inline]
    pub fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, RELAXED);
    }

    /// Add to a counter.
    #[inline]
    pub fn add(&self, counter: &AtomicU64, value: u64) {
        counter.fetch_add(value, RELAXED);
    }

    /// Get counter value.
    #[inline]
    pub fn get(&self, counter: &AtomicU64) -> u64 {
        counter.load(RELAXED)
    }

    /// Increment active connections.
    #[inline]
    pub fn connection_opened(&self) {
        self.connections_total.fetch_add(1, RELAXED);
        self.connections_active.fetch_add(1, RELAXED);
    }

    /// Decrement active connections.
    #[inline]
    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, RELAXED);
    }

    /// Record command execution with latency.
    #[inline]
    pub fn record_command(&self, cmd_type: CommandType, start: Instant) {
        let duration_us = start.elapsed().as_micros() as u64;

        self.commands_total.fetch_add(1, RELAXED);

        match cmd_type {
            CommandType::Incr => {
                self.commands_incr.fetch_add(1, RELAXED);
                self.latency_incr.record(duration_us);
            }
            CommandType::Decr => {
                self.commands_decr.fetch_add(1, RELAXED);
                self.latency_decr.record(duration_us);
            }
            CommandType::Get => {
                self.commands_get.fetch_add(1, RELAXED);
                self.latency_get.record(duration_us);
            }
            CommandType::Set => {
                self.commands_set.fetch_add(1, RELAXED);
                self.latency_set.record(duration_us);
            }
            CommandType::Ping => {
                self.commands_ping.fetch_add(1, RELAXED);
            }
            CommandType::Info => {
                self.commands_info.fetch_add(1, RELAXED);
            }
            CommandType::Quota => {
                self.commands_quota.fetch_add(1, RELAXED);
            }
            CommandType::Other => {
                self.commands_other.fetch_add(1, RELAXED);
            }
        }
    }

    /// Record replication RTT in microseconds.
    #[inline]
    pub fn record_replication_rtt(&self, rtt_us: u64) {
        self.replication_rtt.record(rtt_us);
    }

    /// Update replication lag metrics.
    /// Called periodically by the replication manager.
    pub fn update_replication_lag(&self, lag: ReplicationLagMetrics) {
        let mut guard = self.replication_lag.write();
        *guard = lag;
    }

    /// Get current replication lag metrics.
    pub fn get_replication_lag(&self) -> ReplicationLagMetrics {
        self.replication_lag.read().clone()
    }

    /// Get a snapshot of all metrics for reporting.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            uptime_secs: self.uptime_secs(),

            commands_total: self.commands_total.load(RELAXED),
            commands_incr: self.commands_incr.load(RELAXED),
            commands_decr: self.commands_decr.load(RELAXED),
            commands_get: self.commands_get.load(RELAXED),
            commands_set: self.commands_set.load(RELAXED),
            commands_ping: self.commands_ping.load(RELAXED),
            commands_info: self.commands_info.load(RELAXED),
            commands_quota: self.commands_quota.load(RELAXED),
            commands_other: self.commands_other.load(RELAXED),

            errors_parse: self.errors_parse.load(RELAXED),
            errors_unknown_cmd: self.errors_unknown_cmd.load(RELAXED),

            connections_total: self.connections_total.load(RELAXED),
            connections_active: self.connections_active.load(RELAXED),

            replication_deltas_sent: self.replication_deltas_sent.load(RELAXED),
            replication_deltas_received: self.replication_deltas_received.load(RELAXED),
            replication_bytes_sent: self.replication_bytes_sent.load(RELAXED),
            replication_bytes_received: self.replication_bytes_received.load(RELAXED),

            wal_writes: self.wal_writes.load(RELAXED),
            wal_bytes_written: self.wal_bytes_written.load(RELAXED),
            snapshots_created: self.snapshots_created.load(RELAXED),

            latency_incr: self.latency_incr.percentiles(),
            latency_decr: self.latency_decr.percentiles(),
            latency_get: self.latency_get.percentiles(),
            latency_set: self.latency_set.percentiles(),

            replication_rtt: self.replication_rtt.percentiles(),
            replication_lag: self.get_replication_lag(),
        }
    }
}

/// Command type for metrics tracking.
#[derive(Debug, Clone, Copy)]
pub enum CommandType {
    Incr,
    Decr,
    Get,
    Set,
    Ping,
    Info,
    Quota,
    Other,
}

/// Latency percentiles in microseconds.
#[derive(Debug, Clone, Copy, Default)]
pub struct LatencyPercentiles {
    pub count: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub max: u64,
}

/// Point-in-time snapshot of all metrics.
#[derive(Debug)]
pub struct MetricsSnapshot {
    pub uptime_secs: u64,

    pub commands_total: u64,
    pub commands_incr: u64,
    pub commands_decr: u64,
    pub commands_get: u64,
    pub commands_set: u64,
    pub commands_ping: u64,
    pub commands_info: u64,
    pub commands_quota: u64,
    pub commands_other: u64,

    pub errors_parse: u64,
    pub errors_unknown_cmd: u64,

    pub connections_total: u64,
    pub connections_active: u64,

    pub replication_deltas_sent: u64,
    pub replication_deltas_received: u64,
    pub replication_bytes_sent: u64,
    pub replication_bytes_received: u64,

    pub wal_writes: u64,
    pub wal_bytes_written: u64,
    pub snapshots_created: u64,

    pub latency_incr: LatencyPercentiles,
    pub latency_decr: LatencyPercentiles,
    pub latency_get: LatencyPercentiles,
    pub latency_set: LatencyPercentiles,

    pub replication_rtt: LatencyPercentiles,
    pub replication_lag: ReplicationLagMetrics,
}

impl MetricsSnapshot {
    /// Format as Redis INFO-style output.
    pub fn to_info_string(&self, section: Option<&str>) -> String {
        let mut out = String::with_capacity(2048);

        let include_all = section.is_none();
        let section = section.unwrap_or("");

        if include_all || section.eq_ignore_ascii_case("server") {
            out.push_str("# Server\r\n");
            out.push_str(&format!("quota_db_version:0.1.0\r\n"));
            out.push_str(&format!("uptime_in_seconds:{}\r\n", self.uptime_secs));
            out.push_str(&format!(
                "uptime_in_days:{}\r\n",
                self.uptime_secs / 86400
            ));
            out.push_str("\r\n");
        }

        if include_all || section.eq_ignore_ascii_case("clients") {
            out.push_str("# Clients\r\n");
            out.push_str(&format!(
                "connected_clients:{}\r\n",
                self.connections_active
            ));
            out.push_str(&format!(
                "total_connections_received:{}\r\n",
                self.connections_total
            ));
            out.push_str("\r\n");
        }

        if include_all || section.eq_ignore_ascii_case("stats") {
            out.push_str("# Stats\r\n");
            out.push_str(&format!(
                "total_commands_processed:{}\r\n",
                self.commands_total
            ));
            out.push_str(&format!("incr_commands:{}\r\n", self.commands_incr));
            out.push_str(&format!("decr_commands:{}\r\n", self.commands_decr));
            out.push_str(&format!("get_commands:{}\r\n", self.commands_get));
            out.push_str(&format!("set_commands:{}\r\n", self.commands_set));
            out.push_str(&format!("ping_commands:{}\r\n", self.commands_ping));
            out.push_str(&format!("quota_commands:{}\r\n", self.commands_quota));
            out.push_str(&format!("other_commands:{}\r\n", self.commands_other));
            out.push_str(&format!("parse_errors:{}\r\n", self.errors_parse));
            out.push_str(&format!(
                "unknown_command_errors:{}\r\n",
                self.errors_unknown_cmd
            ));
            out.push_str("\r\n");
        }

        if include_all || section.eq_ignore_ascii_case("latency") {
            out.push_str("# Latency (microseconds)\r\n");
            out.push_str(&format!(
                "incr_p50:{}\r\n",
                self.latency_incr.p50
            ));
            out.push_str(&format!(
                "incr_p95:{}\r\n",
                self.latency_incr.p95
            ));
            out.push_str(&format!(
                "incr_p99:{}\r\n",
                self.latency_incr.p99
            ));
            out.push_str(&format!(
                "get_p50:{}\r\n",
                self.latency_get.p50
            ));
            out.push_str(&format!(
                "get_p95:{}\r\n",
                self.latency_get.p95
            ));
            out.push_str(&format!(
                "get_p99:{}\r\n",
                self.latency_get.p99
            ));
            out.push_str("\r\n");
        }

        if include_all || section.eq_ignore_ascii_case("replication") {
            out.push_str("# Replication\r\n");
            out.push_str(&format!(
                "deltas_sent:{}\r\n",
                self.replication_deltas_sent
            ));
            out.push_str(&format!(
                "deltas_received:{}\r\n",
                self.replication_deltas_received
            ));
            out.push_str(&format!(
                "replication_bytes_sent:{}\r\n",
                self.replication_bytes_sent
            ));
            out.push_str(&format!(
                "replication_bytes_received:{}\r\n",
                self.replication_bytes_received
            ));
            out.push_str(&format!(
                "replication_lag_max:{}\r\n",
                self.replication_lag.max_lag
            ));
            out.push_str(&format!(
                "replication_lag_total:{}\r\n",
                self.replication_lag.total_lag
            ));
            out.push_str(&format!(
                "replication_peers:{}\r\n",
                self.replication_lag.peer_count
            ));
            out.push_str(&format!(
                "replication_rtt_p50:{}\r\n",
                self.replication_rtt.p50
            ));
            out.push_str(&format!(
                "replication_rtt_p99:{}\r\n",
                self.replication_rtt.p99
            ));
            out.push_str("\r\n");
        }

        if include_all || section.eq_ignore_ascii_case("persistence") {
            out.push_str("# Persistence\r\n");
            out.push_str(&format!("wal_writes:{}\r\n", self.wal_writes));
            out.push_str(&format!(
                "wal_bytes_written:{}\r\n",
                self.wal_bytes_written
            ));
            out.push_str(&format!(
                "snapshots_created:{}\r\n",
                self.snapshots_created
            ));
            out.push_str("\r\n");
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_operations() {
        let m = Metrics::new();
        assert_eq!(m.get(&m.commands_total), 0);

        m.inc(&m.commands_total);
        assert_eq!(m.get(&m.commands_total), 1);

        m.add(&m.commands_total, 10);
        assert_eq!(m.get(&m.commands_total), 11);
    }

    #[test]
    fn test_connection_tracking() {
        let m = Metrics::new();
        assert_eq!(m.get(&m.connections_active), 0);

        m.connection_opened();
        m.connection_opened();
        assert_eq!(m.get(&m.connections_active), 2);
        assert_eq!(m.get(&m.connections_total), 2);

        m.connection_closed();
        assert_eq!(m.get(&m.connections_active), 1);
        assert_eq!(m.get(&m.connections_total), 2);
    }

    #[test]
    fn test_record_command() {
        let m = Metrics::new();
        let start = Instant::now();

        m.record_command(CommandType::Incr, start);
        m.record_command(CommandType::Get, start);
        m.record_command(CommandType::Get, start);

        assert_eq!(m.get(&m.commands_total), 3);
        assert_eq!(m.get(&m.commands_incr), 1);
        assert_eq!(m.get(&m.commands_get), 2);
    }

    #[test]
    fn test_snapshot() {
        let m = Metrics::new();
        m.init_start_time();

        m.record_command(CommandType::Incr, Instant::now());
        m.connection_opened();

        let snap = m.snapshot();
        assert_eq!(snap.commands_total, 1);
        assert_eq!(snap.commands_incr, 1);
        assert_eq!(snap.connections_total, 1);
        assert_eq!(snap.connections_active, 1);
    }

    #[test]
    fn test_info_string() {
        let m = Metrics::new();
        m.init_start_time();
        m.record_command(CommandType::Ping, Instant::now());

        let snap = m.snapshot();
        let info = snap.to_info_string(None);

        assert!(info.contains("# Server"));
        assert!(info.contains("# Stats"));
        assert!(info.contains("ping_commands:1"));
    }

    #[test]
    fn test_info_section_filter() {
        let m = Metrics::new();
        m.init_start_time();
        m.connection_opened();

        let snap = m.snapshot();
        let info = snap.to_info_string(Some("clients"));

        assert!(info.contains("# Clients"));
        assert!(info.contains("connected_clients:1"));
        assert!(!info.contains("# Server"));
        assert!(!info.contains("# Stats"));
    }
}
