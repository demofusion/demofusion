# demofusion

SQL queries over Valve Source 2 demo files via Apache DataFusion.

demofusion enables you to query game state data from Deadlock demos using SQL. It streams entity and event data through Apache DataFusion, supporting both static demo files and live GOTV broadcasts.

## Features

- **SQL queries** over entity state (players, NPCs, projectiles) and game events (damage, kills, ability usage)
- **Single-pass streaming** for efficient multi-table queries
- **Demo file support** via `DemoSource`
- **Live GOTV broadcasts** via `GotvSource` (requires `gotv` feature)
- **Arrow-native** output via Apache DataFusion
- **JOIN support** with gate-based backpressure to prevent deadlocks

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
demofusion = { git = "https://github.com/demofusion/demofusion.git" }
```

For live GOTV broadcast support:

```toml
[dependencies]
demofusion = { git = "https://github.com/demofusion/demofusion.git", features = ["gotv"] }
```

## Quick Start

```rust
use demofusion::demo::DemoSource;
use demofusion::session::IntoStreamingSession;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load demo file
    let source = DemoSource::open("match.dem").await?;
    
    // Create session and discover available schemas
    let (mut session, schemas) = source.into_session().await?;
    println!("Found {} entity tables", schemas.len());
    
    // Register a SQL query
    let mut query = session.add_query(
        "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn"
    ).await?;
    
    // Start streaming (begins parsing in background)
    let _handle = session.start()?;
    
    // Consume results as they stream
    while let Some(batch) = query.next().await {
        let batch = batch?;
        println!("Got {} rows", batch.num_rows());
    }
    
    Ok(())
}
```

## API Overview

demofusion uses a streaming session pattern:

1. **Create a source** - `DemoSource` for files, `GotvSource` for live broadcasts
2. **Initialize the session** - Call `into_session()` to discover schemas
3. **Register queries** - Call `add_query()` for each SQL query you need
4. **Start streaming** - Call `start()` to begin parsing
5. **Consume results** - Each query handle is a `Stream<Item = Result<RecordBatch>>`

### Loading Demo Files

`DemoSource` supports loading from file paths or byte buffers:

```rust
use demofusion::demo::DemoSource;
use demofusion::session::IntoStreamingSession;

// From a file path
let source = DemoSource::open("match.dem").await?;

// From bytes (useful when demo is already in memory)
let bytes = tokio::fs::read("match.dem").await?;
let source = DemoSource::from_bytes(bytes);

// Initialize session - returns session + discovered schemas
let (mut session, schemas) = source.into_session().await?;

// Inspect available tables
for schema in &schemas {
    println!("Table: {} ({} fields)", schema.name(), schema.fields().len());
}
```

### Registering Queries

Register SQL queries before calling `start()`. Each call returns a `QueryHandle` that streams results:

```rust
// Simple entity query
let mut pawns = session.add_query(
    "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn"
).await?;

// Query with filtering
let mut low_health = session.add_query(
    "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn WHERE m_iHealth < 200"
).await?;

// Event query
let mut damage = session.add_query(
    "SELECT tick, damage, entindex_victim FROM DamageEvent"
).await?;
```

### Streaming Results

After calling `start()`, consume query results using the `Stream` interface:

```rust
use futures::StreamExt;

let _handle = session.start()?;

while let Some(result) = pawns.next().await {
    let batch: RecordBatch = result?;
    
    // Access columns by name
    let health = batch.column_by_name("m_iHealth").unwrap();
    
    // Process rows...
    println!("Batch with {} rows", batch.num_rows());
}
```

### Multiple Concurrent Queries

Register multiple queries against a single parse pass, then consume them concurrently:

```rust
let mut pawns = session.add_query(
    "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn"
).await?;
let mut troopers = session.add_query(
    "SELECT tick, entity_index FROM CNPC_Trooper"
).await?;

let _handle = session.start()?;

// Process streams concurrently with tokio::select!
loop {
    tokio::select! {
        Some(batch) = pawns.next() => {
            println!("Pawns: {} rows", batch?.num_rows());
        }
        Some(batch) = troopers.next() => {
            println!("Troopers: {} rows", batch?.num_rows());
        }
        else => break,
    }
}
```

Or use `tokio::join!` for independent processing:

```rust
let _handle = session.start()?;

tokio::join!(
    async {
        while let Some(batch) = pawns.next().await {
            // Process pawn data...
        }
    },
    async {
        while let Some(batch) = troopers.next().await {
            // Process trooper data...
        }
    }
);
```

### JOIN Queries

demofusion supports streaming JOINs across entity and event tables:

```rust
let mut damage_with_positions = session.add_query("
    SELECT 
        d.tick,
        d.damage,
        d.entindex_victim,
        p.entity_index,
        p.\"CBodyComponent__m_cellX\",
        p.\"CBodyComponent__m_cellY\"
    FROM DamageEvent d
    INNER JOIN CCitadelPlayerPawn p ON d.tick = p.tick
").await?;
```

### Live GOTV Broadcasts

Use `GotvSource` (requires `gotv` feature) for live match data:

```rust
use demofusion::gotv::GotvSource;
use demofusion::session::IntoStreamingSession;
use tokio_util::sync::CancellationToken;
use futures::StreamExt;

// Connect to a GOTV broadcast
let source = GotvSource::connect("http://dist1-ord1.steamcontent.com/tv/12345").await?;
let (session, schemas) = source.into_session().await?;

// Add cancellation support for live streams
let cancel = CancellationToken::new();
let mut session = session.with_cancel_token(cancel.clone());

let mut pawns = session.add_query(
    "SELECT tick, entity_index FROM CCitadelPlayerPawn"
).await?;

let _handle = session.start()?;

while let Some(batch) = pawns.next().await {
    println!("Got {} rows", batch?.num_rows());
}
```

## Entity Tables

Each Source 2 entity serializer becomes a queryable table. Common entity types include:

| Table | Description |
|-------|-------------|
| `CCitadelPlayerPawn` | Player character state (health, position, abilities) |
| `CCitadelPlayerController` | Player controller data |
| `CNPC_Trooper` | Lane creeps/troopers |
| `CNPC_TrooperNeutral` | Neutral jungle creeps |
| `CNPC_Boss_Tier2` | Walkers |
| `CNPC_Boss_Tier3` | Patrons |
| `CCitadelProjectile` | Projectiles in flight |

All entity tables include these base columns:

| Column | Type | Description |
|--------|------|-------------|
| `tick` | Int32 | Game tick when this row was captured |
| `entity_index` | Int32 | Unique entity identifier |
| `delta_type` | Utf8 | Change type: `create`, `update`, `delete` |

Additional columns are generated from the entity's serializer fields (e.g., `m_iHealth`, `m_iTeamNum`, `CBodyComponent__m_cellX`).

## Event Tables

Game events are exposed as tables:

| Table | Description |
|-------|-------------|
| `DamageEvent` | Damage dealt events |
| `HeroKilledEvent` | Player kill events |
| `BossKilledEvent` | Boss kill events |
| `BulletHitEvent` | Bullet hit events |
| `AbilitiesChangedEvent` | Ability state changes |
| `CurrencyChangedEvent` | Currency/souls changes |

## Examples

Run the included examples to see demofusion in action:

```bash
# Track hero positions over time
cargo run --example hero_positions -- path/to/demo.dem

# Analyze damage hotspots with JOIN queries
cargo run --release --example damage_locations -- path/to/demo.dem

# Track objective health (Walkers, Guardians, Patrons)
cargo run --example objectives -- path/to/demo.dem

# Monitor trooper spawns and deaths
cargo run --example troopers -- path/to/demo.dem

# Detect combat engagements
cargo run --example combat_detection -- path/to/demo.dem

# Calculate player statistics
cargo run --example player_stats -- path/to/demo.dem
```

## Configuration

### Batch Size

Control the number of rows per `RecordBatch`:

```rust
let (mut session, _) = source.into_session().await?;

// Larger batches for throughput (default: 1024)
let session = session.with_batch_size(2048);
```

### Pipeline Breaker Rejection

Reject queries that require unbounded memory (e.g., `ORDER BY`, `GROUP BY` without streaming):

```rust
let session = session.with_reject_pipeline_breakers(true);

// This will now fail because ORDER BY requires buffering all data
let result = session.add_query(
    "SELECT * FROM CCitadelPlayerPawn ORDER BY m_iHealth"
).await;
assert!(result.is_err());
```

## Architecture

demofusion uses a streaming architecture for efficient single-pass parsing:

```
Source (File/GOTV) -> Parser -> Visitor -> DataFusion -> Arrow RecordBatches
                                  |
                                  +-> EntityDispatcher (entities by type)
                                  +-> EventDispatcher (game events)
```

Key design decisions:

1. **Single-pass parsing**: The demo is parsed once; entity updates are routed to multiple query consumers simultaneously
2. **Producer-side batching**: RecordBatches are built at the parser, minimizing downstream buffering
3. **Gate-based backpressure**: Prevents JOIN deadlocks with global flow control across distribution channels
4. **Memory bounded**: Configurable batch sizes with automatic backpressure when consumers fall behind

## Feature Flags

| Feature | Description |
|---------|-------------|
| `gotv` | Enables `GotvSource` for live GOTV broadcast streaming |

## License

Apache-2.0

## Acknowledgements

This project builds on the excellent work of:

- [Apache DataFusion](https://github.com/apache/datafusion) - The SQL query engine powering demofusion's query capabilities
- [Apache Arrow](https://arrow.apache.org/) - The columnar memory format enabling efficient data processing
- [haste](https://github.com/blukai/haste) - The original Source 2 demo parser that demofusion's parsing logic is derived from
- [valveprotos](https://github.com/aspect/valveprotos-rs) - Protobuf definitions for Valve Source 2 games
