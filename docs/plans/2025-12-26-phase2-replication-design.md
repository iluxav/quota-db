# Phase 2: Active Replication Design

## Overview

Low-overhead delta streaming between QuotaDB nodes for eventual consistency of PN-Counter CRDTs.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Topology | Full mesh | Simple, fast convergence, O(n²) acceptable for <10 nodes |
| Transport | TCP + custom binary | Minimal overhead, internal-only traffic |
| Delta buffering | Per-shard ring buffer | Memory-bounded, O(1) append, good cache locality |
| Delta format | Incremental | Smallest wire size, preserves CRDT semantics |
| Peer discovery | Static config | Simple, works with docker-compose |
| Batching | Hybrid (size + time) | Low latency under light load, high throughput under heavy load |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Node 1                               │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐                      │
│  │ Shard 0 │  │ Shard 1 │  │ Shard N │  (each has RepLog)   │
│  └────┬────┘  └────┬────┘  └────┬────┘                      │
│       └───────────┬┴───────────┘                            │
│                   ▼                                          │
│  ┌─────────────────────────────────────┐                    │
│  │         ReplicationManager          │                    │
│  │  - Owns TCP connections to peers    │                    │
│  │  - Tracks acked_seq per peer/shard  │                    │
│  │  - Hybrid batching (size/time)      │                    │
│  └──────────────────┬──────────────────┘                    │
└─────────────────────┼───────────────────────────────────────┘
                      │ TCP (binary protocol)
        ┌─────────────┴─────────────┐
        ▼                           ▼
   ┌─────────┐                 ┌─────────┐
   │ Node 2  │                 │ Node 3  │
   └─────────┘                 └─────────┘
```

## Data Structures

### Delta

```rust
struct Delta {
    seq: u64,              // Global sequence for this shard
    key: Key,              // The counter key
    node_id: NodeId,       // Which node made the change
    delta_p: u64,          // Increment amount (0 if decrement)
    delta_n: u64,          // Decrement amount (0 if increment)
}
```

### Replication Log (per-shard)

```rust
const REPLICATION_LOG_SIZE: usize = 4096;

struct ReplicationLog {
    buffer: Box<[Option<Delta>; REPLICATION_LOG_SIZE]>,
    head_seq: u64,         // Next sequence to write
}
```

Key invariants:
- `head_seq - REPLICATION_LOG_SIZE` = oldest available seq
- If `peer.acked_seq[shard] < oldest`, peer needs full resync (Phase 4)

### Peer State

```rust
struct PeerState {
    addr: SocketAddr,
    acked_seq: Vec<u64>,   // acked_seq[shard_id] - per-shard acknowledgment
    connection: Option<TcpStream>,
    pending_batch: Vec<Delta>,
    last_flush: Instant,
}

struct ReplicationManager {
    peers: Vec<PeerState>,
    batch_max_size: usize,      // Default: 100
    batch_max_delay: Duration,  // Default: 10ms
}
```

## Wire Protocol

### Frame Format

```
┌──────────┬──────────┬─────────────────────────────────┐
│ len (4B) │ type (1B)│ payload (variable)              │
└──────────┴──────────┴─────────────────────────────────┘
```

### Message Types

```rust
enum MessageType {
    DeltaBatch = 1,    // Sender → Receiver: batch of deltas
    Ack = 2,           // Receiver → Sender: confirm receipt
    Hello = 3,         // Initial handshake with node_id
}
```

### DeltaBatch Payload

```
┌───────────┬───────────┬─────────────────────────────────┐
│ shard (2B)│ count (2B)│ deltas...                       │
└───────────┴───────────┴─────────────────────────────────┘

Each delta:
┌──────────┬───────────┬──────────┬──────────┬──────────────┬────────┐
│ seq (8B) │ node (4B) │ dP (8B)  │ dN (8B)  │ key_len (2B) │ key... │
└──────────┴───────────┴──────────┴──────────┴──────────────┴────────┘
```

### Ack Payload

```
┌───────────┬───────────┐
│ shard (2B)│ acked (8B)│   // Confirms all seqs up to this
└───────────┴───────────┘
```

Design choices:
- Little-endian byte order (native for x86/ARM)
- No compression initially (deltas are small)
- One shard per DeltaBatch for simpler ACK tracking

## Connection Management

**Connection rules:**
- Node with lower `node_id` initiates connection to higher `node_id`
- Prevents duplicate connections
- Reconnect with exponential backoff: 100ms → 200ms → ... → 10s max

**Sender flow:**
```
loop {
    select! {
        delta = delta_rx.recv() => {
            pending_batch.push(delta);
            if pending_batch.len() >= batch_max_size {
                flush_batch();
            }
        }
        _ = sleep_until(last_flush + batch_max_delay) => {
            if !pending_batch.is_empty() {
                flush_batch();
            }
        }
        ack = read_ack() => {
            acked_seq[ack.shard] = ack.seq;
        }
    }
}
```

**Receiver flow:**
```
loop {
    msg = read_message();
    match msg.type {
        DeltaBatch => {
            for delta in msg.deltas {
                shard.merge_delta(delta);
            }
            send_ack(msg.shard, msg.last_seq);
        }
        Hello => { /* record peer node_id */ }
    }
}
```

## File Structure

### New Files

```
src/replication/
├── mod.rs           # Module exports
├── delta.rs         # Delta struct, serialization
├── log.rs           # ReplicationLog (ring buffer)
├── protocol.rs      # Wire protocol encode/decode
├── manager.rs       # ReplicationManager, peer connections
└── peer.rs          # PeerState, connection handling
```

### Modified Files

```
src/config.rs        # Add: peers, replication_port, batch settings
src/engine/shard.rs  # Add: ReplicationLog, emit deltas on mutation
src/engine/entry.rs  # Add: apply_remote_p(), apply_remote_n()
src/main.rs          # Spawn ReplicationManager task
src/lib.rs           # Export replication module
```

## Config Additions

```rust
#[arg(long, value_delimiter = ',', default_value = "")]
pub peers: Vec<String>,           // e.g., "node2:6381,node3:6381"

#[arg(long, default_value = "6381")]
pub replication_port: u16,        // Separate port for replication

#[arg(long, default_value = "100")]
pub batch_max_size: usize,

#[arg(long, default_value = "10")]
pub batch_max_delay_ms: u64,
```

## Integration Points

### Shard Mutation

```rust
// In shard.rs increment()
pub fn increment(&mut self, key: Key, delta: u64) -> (i64, Option<Delta>) {
    let entry = self.data.entry(key.clone()).or_default();
    entry.increment(self.node_id, delta);
    let d = self.rep_log.append(key, self.node_id, delta, 0);
    (entry.value(), Some(d))
}
```

### Entry Remote Apply

```rust
// In entry.rs
impl PnCounterEntry {
    pub fn apply_remote_p(&mut self, node: NodeId, delta: u64) {
        let current = self.p.get(&node).copied().unwrap_or(0);
        let new_val = current + delta;
        self.p.insert(node, new_val);
        self.cached_value += delta as i64;
    }

    pub fn apply_remote_n(&mut self, node: NodeId, delta: u64) {
        let current = self.n.get(&node).copied().unwrap_or(0);
        let new_val = current + delta;
        self.n.insert(node, new_val);
        self.cached_value -= delta as i64;
    }
}
```

## Performance Considerations

- Ring buffer: O(1) append, bounded memory
- Batch sending: amortizes TCP overhead
- Separate replication port: isolates replication from client traffic
- Per-shard logs: enables parallel replication

## Future Work (Phase 4)

- Anti-entropy for gap detection when peer falls behind ring buffer
- Full state sync for new nodes joining cluster
- Gossip-based discovery for dynamic membership
