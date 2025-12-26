# Phase 3: Escrow/Quota Counters Design

## Overview

Add rate limiting with guaranteed limits via token-based allocation. Each node gets a local token balance; requests consume tokens locally (fast path). Refill happens via allocator (slow path).

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Rate limit model | Token bucket | Industry standard, handles bursts well |
| API | QUOTASET + existing INCR | Minimal new commands, reuses Redis semantics |
| Return value | Remaining tokens, -1 if denied | Client knows headroom |
| Token allocation | On-demand from allocator | Handles uneven traffic across nodes |
| Batch size | 10% of limit | Scales naturally with limit size |
| Failure mode | Fail closed (deny) | Correctness over availability |

## API

### New Commands

```
QUOTASET key limit window_secs    → OK
QUOTAGET key                      → [limit, window_secs, remaining]
QUOTADEL key                      → OK
```

### Modified Behavior

| Command | Regular key (PN-counter) | Quota key |
|---------|--------------------------|-----------|
| INCR | +1, return value | Consume 1, return remaining or -1 |
| INCRBY n | +n, return value | Consume n, return remaining or -1 |
| GET | Return counter value | Return remaining tokens |
| DECR/DECRBY | Decrement | Error (invalid for quotas) |

## Data Structures

### QuotaEntry

```rust
pub struct QuotaEntry {
    // Configuration
    limit: u64,              // tokens per window (e.g., 10000)
    window_secs: u64,        // window duration (e.g., 60)

    // Local state
    local_tokens: i64,       // tokens available locally
    window_start: u64,       // unix timestamp of current window
}
```

### AllocatorState (on shard owner)

```rust
pub struct AllocatorState {
    grants: HashMap<NodeId, u64>,  // tokens granted per node
    total_granted: u64,            // sum of all grants
}
// Available = limit - total_granted
```

### Entry Enum

```rust
pub enum Entry {
    Counter(PnCounterEntry),
    Quota(QuotaEntry),
}
```

## Request Flow

### INCR on Quota Key

```
1. Check local_tokens > 0?
   ├─ YES → Decrement local_tokens, return remaining
   └─ NO  → Go to step 2

2. Request tokens from allocator (shard owner)
   ├─ Allocator has tokens → Grant 10% batch, go to step 1
   ├─ Allocator exhausted → Return -1 (denied)
   └─ Allocator unreachable → Return -1 (fail closed)

3. Window expired? Reset window_start, total_granted = 0
```

### Token Request Protocol

```rust
enum QuotaMessage {
    RequestTokens { key: Key, amount: u64 },
    GrantTokens { key: Key, amount: u64 },
    DenyTokens { key: Key },
}
```

## Replication

| Event | Replicated? | Payload |
|-------|-------------|---------|
| QUOTASET | Yes | (key, limit, window_secs) |
| INCR (consume) | No | - |
| Token grant | Yes | (key, node_id, amount) |
| Window reset | Yes | (key, new_window_start) |

## Implementation Order

1. QuotaEntry struct - local token tracking, window logic
2. Entry enum - unify Counter/Quota storage
3. QUOTASET command - create quota entries
4. Local INCR path - consume tokens, return remaining/-1
5. Allocator protocol - request/grant tokens between nodes
6. Replication - sync grants across cluster
7. Tests - unit tests, integration with redis-cli

## Files to Modify

- `src/engine/quota.rs` (new) - QuotaEntry + allocator logic
- `src/engine/entry.rs` - Entry enum
- `src/engine/shard.rs` - use Entry enum
- `src/engine/db.rs` - quota_set(), modify increment()
- `src/protocol/command.rs` - new commands
- `src/server/handler.rs` - handle commands
- `src/replication/` - quota grant messages
