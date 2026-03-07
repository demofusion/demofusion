# demofusion

SQL queries over Valve Source 2 demo files via Apache DataFusion.

demofusion enables you to query game state data from Deadlock demos using SQL. It streams entity and event data through Apache DataFusion, supporting live GOTV broadcasts with real-time query results.

## Features

- **SQL queries** over entity state (players, NPCs, projectiles) and game events (damage, kills, ability usage)
- **Single-pass parsing** for efficient multi-table queries
- **Streaming architecture** designed for live GOTV broadcast support
- **Arrow-native** output via Apache DataFusion
- **JOIN support** with gate-based backpressure to prevent deadlocks

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
demofusion = { git = "https://github.com/demofusion/demofusion.git", features = ["gotv"] }
```

## Usage

### Streaming Live GOTV Broadcasts

Use `SpectateSession` for live match data:

```rust
use demofusion::gotv::SpectateSession;
use tokio_util::sync::CancellationToken;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to a GOTV broadcast
    let mut session = SpectateSession::connect(
        "http://dist1-ord1.steamcontent.com/tv/18895867"
    ).await?;
    
    println!("Map: {}", session.map());
    println!("Available entities: {:?}", session.entity_names());
    
    // Register SQL queries - returns handles immediately
    let mut pawns = session.add_query(
        "SELECT tick, entity_index FROM CCitadelPlayerPawn"
    ).await?;
    
    // Start streaming (consumes the session)
    let cancel = CancellationToken::new();
    let result = session.start(cancel.clone()).await?;
    
    // Consume results as they arrive
    while let Some(batch_result) = pawns.next().await {
        let batch = batch_result?;
        println!("Got {} rows", batch.num_rows());
    }
    
    // Wait for parser to complete
    result.parser_handle.await??;
    
    Ok(())
}
```

### Multiple Concurrent Queries

Register multiple queries that all receive data from the same parse pass:

```rust
let mut session = SpectateSession::connect(url).await?;

// Register multiple queries
let mut pawns = session.add_query(
    "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn"
).await?;
let mut troopers = session.add_query(
    "SELECT tick, entity_index FROM CNPC_Trooper"
).await?;
let mut damage = session.add_query(
    "SELECT tick, damage, victim_player_slot FROM DamageEvent"
).await?;

let cancel = CancellationToken::new();
let result = session.start(cancel.clone()).await?;

// Process streams concurrently
loop {
    tokio::select! {
        Some(batch) = pawns.next() => {
            println!("Pawns: {} rows", batch?.num_rows());
        }
        Some(batch) = troopers.next() => {
            println!("Troopers: {} rows", batch?.num_rows());
        }
        Some(batch) = damage.next() => {
            println!("Damage events: {} rows", batch?.num_rows());
        }
        else => break,
    }
}
```

### JOIN Queries

demofusion supports JOIN queries across entity and event tables:

```rust
let mut damage_with_health = session.add_query("
    SELECT 
        d.tick,
        d.damage,
        p.m_iHealth as victim_health
    FROM DamageEvent d
    JOIN CCitadelPlayerPawn p 
        ON d.tick = p.tick 
        AND d.victim_player_slot = p.entity_index
").await?;
```

## Entity Tables

Each Source 2 entity serializer becomes a queryable table. Common entity types include:

| Table | Description |
|-------|-------------|
| `CCitadelPlayerPawn` | Player character state (health, position, abilities) |
| `CCitadelPlayerController` | Player controller data |
| `CNPC_Trooper` | Lane creeps/troopers |
| `CNPC_TrooperNeutral` | Neutral jungle creeps |
| `CCitadelProjectile` | Projectiles in flight |
| `CCitadelAbility*` | Various ability entities |

All entity tables include these base columns:

| Column | Type | Description |
|--------|------|-------------|
| `tick` | Int32 | Game tick when this row was captured |
| `entity_index` | Int32 | Unique entity identifier |
| `delta_type` | Utf8 | Change type: "CREATE", "UPDATE", "DELETE", "LEAVE" |

Additional columns are generated from the entity's serializer fields.

## Event Tables

Game events are exposed as tables. Common event types include:

| Table | Description |
|-------|-------------|
| `DamageEvent` | Damage dealt events |
| `HeroKilledEvent` | Player kill events |
| `BossKilledEvent` | Boss kill events |
| `BulletHitEvent` | Bullet hit events |
| `AbilitiesChangedEvent` | Ability state changes |
| `CurrencyChangedEvent` | Currency/souls changes |

## Architecture

demofusion uses a streaming architecture designed for efficient single-pass parsing:

```
GOTV HTTP -> BroadcastClient -> Parser -> BatchingDemoVisitor -> DataFusion -> Arrow RecordBatches
                                              |
                                              +-> BatchingEntityDispatcher (entities)
                                              +-> BatchingEventDispatcher (events)
```

Key design decisions:

1. **Single-pass parsing**: Entity updates are routed to multiple tables simultaneously
2. **Producer-side batching**: Batches are built at the parser, not buffered at consumers
3. **Gate-based backpressure**: Prevents JOIN deadlocks with global flow control
4. **Memory bounded**: Configurable batch sizes with automatic backpressure

### Distribution Channels

For JOIN queries, demofusion uses distribution channels with a shared gate. When a JOIN alternates between left and right inputs, the gate ensures the parser can always send to whichever side the JOIN is currently draining - preventing deadlock.

```
Parser -> Gate -> [Channel A] -> JOIN Left
              \-> [Channel B] -> JOIN Right

Gate blocks only when ALL channels are full, not when any single channel is full.
```

## Configuration

### Batch Size

Control the number of rows per batch (default: 128):

```rust
let session = SpectateSession::connect(url).await?
    .with_batch_size(256);
```

### Pipeline Breaker Rejection

Reject queries that would cause unbounded memory usage:

```rust
let mut session = SpectateSession::connect(url).await?
    .with_reject_pipeline_breakers(true);

// This will now fail because ORDER BY requires buffering all data
let result = session.add_query(
    "SELECT * FROM CCitadelPlayerPawn ORDER BY m_iHealth"
).await;
assert!(result.is_err());
```

### Streaming Statistics

Monitor memory usage and throughput:

```rust
let result = session.start(cancel).await?;

if let Some(stats) = &result.stats {
    let snapshot = stats.snapshot();
    println!("Batches sent: {}", snapshot.batches_sent);
    println!("Rows produced: {}", snapshot.rows_produced);
    println!("Gate blocked count: {}", snapshot.gate_blocked_count);
}
```

## Feature Flags

| Feature | Description |
|---------|-------------|
| `gotv` | Enables GOTV HTTP broadcast client for live match streaming |

## License

Apache-2.0

## Acknowledgements

This project builds on the excellent work of:

- [Apache DataFusion](https://github.com/apache/datafusion) - The SQL query engine powering demofusion's query capabilities
- [Apache Arrow](https://arrow.apache.org/) - The columnar memory format enabling efficient data processing
- [haste](https://github.com/blukai/haste) - The original Source 2 demo parser that demofusion's parsing logic is derived from
- [valveprotos](https://github.com/aspect/valveprotos-rs) - Protobuf definitions for Valve Source 2 games
