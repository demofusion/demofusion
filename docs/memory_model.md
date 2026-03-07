# Memory Model and Backpressure Analysis

## Overview

This document describes the memory model, backpressure mechanisms, and cleanup behavior for demofusion's streaming architecture. It focuses on the `gotv/session.rs` path used by demoflight for serving live GOTV broadcasts.

## Architecture

demofusion has two distinct data paths:

### Path 1: GOTV Session (used by demoflight)

```
GOTV HTTP → mpsc::channel(32) → Parser → distributor_channels → DataFusion → gRPC
            ^bounded             ^gate-based backpressure        ^unbounded output
```

### Path 2: Generic Streaming (not used by demoflight)

```
PacketSource → Parser → async_channel::unbounded() → Router → async_channel::unbounded()
                        ^UNBOUNDED                           ^UNBOUNDED
```

**demoflight uses Path 1** via `SpectateSession` (imported in `demoflight/src/session/state.rs`).

## Backpressure Mechanisms

| Point | Mechanism | Bounded? | Location |
|-------|-----------|----------|----------|
| GOTV HTTP → Packet channel | `mpsc::channel(32)` | **Yes** (32 packets) | `session.rs:441` |
| Packet → Parser | Pull-based | **Yes** (demand-driven) | `packet_channel_broadcast_stream.rs` |
| Parser → Entity channels | `distributor_channels` gate | **Weak** (see below) | `distributor_channels.rs` |
| Entity → DataFusion | Pull-based | **Yes** | DataFusion stream semantics |
| DataFusion → Client | `mpsc::UnboundedSender` | **No** | `session.rs:488-498` |

### Gate-Based Backpressure (distributor_channels)

The `distributor_channels` module (copied from DataFusion) implements global gate-based backpressure:

- Sends block only when **ALL** channels are non-empty
- If **ANY** channel is empty, the gate opens and all sends proceed
- Prevents deadlock but allows temporary memory spikes if entity types produce at different rates

This is a deliberate tradeoff to prevent deadlock in JOIN scenarios where multiple tables must be consumed together.

## Memory Per Session (Estimated)

| Component | Size | Bounded? | Notes |
|-----------|------|----------|-------|
| Packet buffer | 32 x ~100KB = ~3 MB | Yes | `mpsc::channel(32)` |
| EntityContainer | 16K entities x ~50 fields = 10-50 MB | Yes | Cleared on DELETE/LEAVE |
| FlattenedSerializerContainer | ~2000 serializers = 5-20 MB | Yes | Static after init |
| baseline_entities | ~500 class IDs = 1-5 MB | Yes | Static after init |
| BatchAccumulator | 8192 rows then flush = 1-2 MB | Yes | Per entity type |
| Output channel | Unbounded if client slow | **No** | See concerns below |

**Realistic estimate: 30-100 MB per session** with active consumption.

At 50 concurrent sessions: **1.5-5 GB total** (acceptable for production).

## Cleanup on Cancellation

When a client disconnects and cancellation propagates, the following sequence occurs:

### Sequence

```
1. CancellationToken cancelled
   └─ (user/demoflight calls cancel_token.cancel())

2. BroadcastClient detects cancellation
   └─ is_cancelled() check in stream_to_channel (client.rs:393)

3. BroadcastClient exits with StreamEndReason::Cancelled
   └─ packet_tx (mpsc::Sender) dropped

4. Parser sees EOF
   └─ packet_rx.recv() returns None (sender dropped)
   └─ read_cmd_header() returns UnexpectedEof
   └─ run_to_end() returns Ok(()) (EOF is normal termination)

5. visitor.flush_all() called (session.rs:521)
   └─ Flushes any rows in EntityBatchBuilder to channels

6. Visitor dropped
   └─ All DistributionSender instances dropped
   └─ Receivers wake and see None

7. forward_stream_to_channel exits
   └─ tx.send() returns Err (receiver dropped)

8. All buffered data dropped
   └─ Channel internal buffers freed by Rust drop semantics
```

### What Happens to Buffered Data

| Data Location | Behavior | Code Location |
|--------------|----------|---------------|
| Unbatched rows in `EntityBatchBuilder` | **Flushed** to channels | `session.rs:521` |
| Data in `DistributionSender` buffers | **Dropped** (not drained) | `distributor_channels.rs:295-306` |
| Data in `mpsc::UnboundedSender` buffers | **Dropped** (not drained) | Tokio internal Drop impl |

**No memory leaks.** All data is properly freed when channels drop. However, data in channel buffers that hasn't been consumed is lost (expected behavior for cancellation).

## Parser Task Lifecycle

```rust
pub struct SharedParserState {
    _parser_handle: tokio::task::JoinHandle<()>,  // Underscore = intentionally unused
}
```

The parser task lifecycle:

1. **Normal operation**: Parser runs in background, produces batches
2. **On cancellation**: `CancellationToken` triggers client exit, packet channel closes
3. **Parser sees EOF**: Returns gracefully, calls `flush_all()`, drops senders
4. **JoinHandle**: When `SharedParserState` drops, the handle drops but task has already exited

The cancellation is properly wired through:
- `session.rs:232-244` creates `CancellationToken`
- Passed to `spectate.start_with_stats(cancel_token.clone())`
- Client checks `is_cancelled()` in its streaming loop

## Known Concerns

### 1. Unbounded Output Channel

The `forward_stream_to_channel` function uses `mpsc::UnboundedSender`:

```rust
// session.rs:488-498
let (tx, rx) = mpsc::unbounded_channel();
tokio::spawn(async move {
    while let Some(result) = stream.next().await {
        if tx.send(mapped).is_err() {
            break;
        }
    }
});
```

**Risk**: If a client connects but reads slowly (or stops reading), batches accumulate until cancellation propagates.

**Mitigation**: This isn't a leak (cleaned up on cancel), but could cause temporary memory spikes. Consider:
- Bounding this channel and adding backpressure
- Adding metrics to monitor channel depth
- Ensuring fast cancellation propagation on client disconnect

### 2. Gate Weakness

The gate-based backpressure allows fast-producing entity channels to grow while slow channels keep the gate open. This is by design (prevents JOIN deadlock) but can cause uneven memory distribution.

### 3. Cancellation Propagation Delay

The time between client disconnect and cancellation affects how much data accumulates:
- demoflight must detect gRPC stream closure promptly
- `CancellationToken` must be cancelled immediately on disconnect

## Monitoring Recommendations

For production deployments at scale (50+ sessions):

1. **Per-session metrics**:
   - Packet channel depth (should stay < 32)
   - Output channel depth (unbounded, monitor for growth)
   - Session state (REGISTERED, QUERYING, LOCKED, etc.)

2. **Aggregate metrics**:
   - Total active sessions
   - Memory usage per session (estimated from component sizes)
   - Session duration distribution

3. **Alerts**:
   - Output channel depth > 1000 batches
   - Session stuck in QUERYING state > timeout
   - Memory growth without corresponding session growth

## Related Documents

- `streaming_delta_architecture.md` - Future optimization to reduce cloning
- `streaming_join_analysis.md` - Analysis of JOIN behavior with bounded channels
