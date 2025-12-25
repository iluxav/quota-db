## QuotaDB technical specification and instructions

distributed in-memory database specifically designed for counters. It lets you enforce exact global rate limits by using token-based allocation, so each region can make fast local decisions without coordination. For general counting, it uses CRDTs to keep totals eventually consistent across regions. In other words, it’s perfect for scenarios where you need ultra-fast local increments and decrements, guaranteed limits, and efficient synchronization of counters worldwide.

## Technical recap

1. ### Two counter types

You end up with two distinct counter semantics, because one size doesn’t fit all:

- Hard-limit counters (rate limiting / quotas): use escrow / token allocation.
  Each region gets a local token balance for (key, window); requests consume tokens locally (fast path). Refill happens via an allocator (slow path). This gives correct “allow/deny” decisions without syncing every increment.

- Total counters (no hard limit, long-lived): use PN-counter CRDT (eventual totals).
  Each region maintains monotonic components per key: P_region and N_region. Replicas merge with max() per component; global value is ΣP − ΣN. This gives fast local inc/dec and eventual convergence without coordination.

2. ### Anti-entropy for gap detection (repair path)

Replication is “push deltas,” but networks drop, nodes restart, and peers can fall behind. Anti-entropy is a periodic check that:

    1. compares progress watermarks (per-shard seq, or per-site seen-vector for PN)

    2. first fills gaps by requesting missing deltas

    3. then optionally compares a cheap shard/range digest

    4. if still mismatched after catch-up → triggers range repair or snapshot resync

It’s a safety net, not the main sync path, and it avoids false alarms by comparing only when watermarks align.

3. ### Active sync with lowest overhead (hot path)

For both counter types, the main sync mechanism is delta streaming:

- updates append to a per-shard replication log (or a “dirty batch” buffer)

- peers track acked_seq per shard (or watermark vectors)

- sender transmits only “what you haven’t acked yet,” batched (size/latency tuned)

- transport can be QUIC (multi-stream) or TCP; keep parsing/alloc minimal

**Crucially: for escrow counters, you replicate token transfers/refills (rare), not every request.**

4. Sharded HashMap (scalability + sync structure)

You shard the keyspace by hash:

-shard = hash(key) % num_shards
Each shard owns:

- its own HashMap

- its own TTL wheel (if needed)

- its own replication seq/log and peer acks

- its own anti-entropy digest state

Sharding helps throughput (on multi-core) and makes replication/repair bounded (“fix shard 17” not “fix the whole DB”).

5. ### Redis-like interface, Rust + Tokio

Expose a minimal RESP-compatible interface for Redis-like clients (subset):

- INCRBY, DECRBY, GET, SCAN, KEYS

- TTL ops: EXPIRE, TTL (as needed)

- plus a combined primitive like INCRBYEX for efficiency

Implement server in Rust + Tokio, focusing on:

- zero-copy-ish RESP parsing

- batching / pipelining

- shard-local single-threaded logic (avoid locks on hot path)

- lightweight replication tasks per shard/peer

### Step-by-step implementation plan

#### Phase 0 — Skeleton & protocol

- Implement RESP subset parser + connection handling (Tokio).

- Define internal API: apply_op(shard, key, op) and reply encoding.

- Decide key representation (string vs hashed key-id) and hashing (fast, stable).

#### Phase 1 — Sharded in-memory engine

- Build num_shards shard structs, each with:

  - HashMap<KeyId, Entry>

  - optional TTL structure (timing wheel / heap)

- Route each command to shard by hash(key).

- Implement PN-counter entry layout (3 regions fixed components):
  P_us, P_eu, P_asia, N_us, N_eu, N_asia

- Implement read: GET = ΣP − ΣN.

#### Phase 2 — Active replication (low overhead)

- Add per-shard seq and a compact delta buffer/log:
  (seq, key_id, dP_local, dN_local, expiry_update?)

- Add per-peer acked_seq[shard].

- Implement replication loop:

  - batch entries after acked_seq

  - send, receive ack

  - apply on receiver shard (idempotent rules: seq ordered)

- Keep it minimal and fast; no anti-entropy yet.

#### Phase 3 — Escrow / token counters (hard limits)

- Add a second entry type for escrow counters:

  - local token balance

  - allocator logic for (key, window)

- Implement fast path: consume token locally → allow/deny.

- Implement slow path refill:

  - REQUEST_TOKENS to allocator

  - allocator grants from reserve / rebalances

- Replicate only allocator decisions / transfers (small volume).

- Add windowing/TTL semantics for escrow keys (recommended).

#### Phase 4 — Anti-entropy (gap detection + repair)

- Add periodic STATUS exchange:

  - per shard: applied_seq (and for PN case, per-site watermark if you go that route)

- Implement: if behind → request missing delta ranges.

- Add shard/range digests:

  - compute digest at watermark

  - compare after catch-up

- If mismatch persists → range repair (send keys for that range) or snapshot.

#### Phase 5 — Production hardening

- Persistence (optional): WAL per shard + periodic snapshots.

- Backpressure, batching tuning, memory pooling.

- Observability: per-shard lag, replication RTT, token refill rate, p99 latency.

- Failure handling: reconnect, resync-from-snapshot thresholds.

## IMPORTANT NOTES

**This database should perform same or better than Redis database. Always track and record performance improvment or degradation metrics/stats as we implement the new features**

- The project should be built in Rust
- Always Optimise for the best **CPU** and **Memory** performance
- Always use performance tests using docker container. **All tests should be performed onthe same docker container size (CPU2, 2GB Memory)**
- All performance tests should be conducted with comperison to Redis
- Use docker compose to run multiple replicas of the db
- Always prefer to user 3rd party battle tested library if exsists, than building from scratch our own implementation
- **Always cleanup the data andn/or temp files created as part of your tests**
- **Always kill running docker containers and processes after your tests are done**
