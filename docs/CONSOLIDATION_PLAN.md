# Session API Consolidation Plan

## Goal

Consolidate demofusion's session APIs into a single `DemoSource`/`GotvSource` → `StreamingSession` pattern, eliminating ~900 lines of duplicated code.

## Target API

```rust
// ============ Sources (feature-gated) ============

/// Demo file source - loads bytes from file or memory
#[cfg(feature = "demo")]
pub struct DemoSource {
    bytes: Bytes,
}

#[cfg(feature = "demo")]
impl DemoSource {
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self>;
    pub fn from_bytes(bytes: impl Into<Bytes>) -> Self;
}

/// GOTV broadcast source - connects to live broadcast
#[cfg(feature = "gotv")]
pub struct GotvSource {
    client: BroadcastClient,
    start_packet: Bytes,
}

#[cfg(feature = "gotv")]
impl GotvSource {
    pub async fn connect(url: &str) -> Result<Self, SessionError>;
}

// ============ Trait ============

/// Trait for sources that can produce a streaming session.
/// Schema discovery happens inside into_session().
#[async_trait]
pub trait IntoStreamingSession {
    async fn into_session(self) -> Result<
        (StreamingSession, HashMap<Arc<str>, EntitySchema>), 
        SessionError
    >;
}

// Both DemoSource and GotvSource implement IntoStreamingSession.

// ============ Session (always available) ============

/// The ONE session type for all sources
pub struct StreamingSession { /* internal */ }

impl StreamingSession {
    // No public constructors - only created via IntoStreamingSession

    // Configuration (builder pattern)
    pub fn with_batch_size(self, size: usize) -> Self;
    pub fn with_reject_pipeline_breakers(self, reject: bool) -> Self;
    pub fn with_cancel_token(self, token: CancellationToken) -> Self;

    // Schema access (schemas also returned from into_session, but accessible here too)
    pub fn schemas(&self) -> &HashMap<Arc<str>, EntitySchema>;
    pub fn entity_names(&self) -> Vec<&str>;

    // Query registration
    pub async fn add_query(&mut self, sql: &str) -> Result<QueryHandle, SessionError>;

    // Execution (consumes session)
    pub fn start(self) -> Result<SessionResult, SessionError>;
}

// ============ Query Handle ============

/// Handle to a streaming query result. Implements Stream.
pub struct QueryHandle { /* internal */ }

impl Stream for QueryHandle {
    type Item = Result<RecordBatch, SessionError>;
}

// ============ Usage ============

// Demo file
let source = DemoSource::open("match.dem").await?;
let (mut session, schemas) = source.into_session().await?;

println!("Found {} entities", schemas.len());

let mut pawns = session.add_query("SELECT * FROM CCitadelPlayerPawn").await?;
let result = session.start()?;

while let Some(batch) = pawns.next().await {
    println!("{} rows", batch?.num_rows());
}

// GOTV (with cancellation)
let source = GotvSource::connect(url).await?;
let (session, schemas) = source.into_session().await?;

let mut session = session.with_cancel_token(cancel.clone());
let mut pawns = session.add_query("SELECT * FROM CCitadelPlayerPawn").await?;
let result = session.start()?;
```

## Current State

### Session Implementations (to be consolidated)

| File | Type | Lines | Status |
|------|------|-------|--------|
| `src/session.rs` | `StreamingSession` | 809 | **KEEP** — refactor to new API |
| `src/demo/session.rs` | `DemoFileSession` | 445 | **DELETE** — replace with `DemoSource` |
| `src/gotv/session.rs` | `SpectateSession` | 634 | **DELETE** — replace with `GotvSource` |
| `src/datafusion/query_session.rs` | `StreamingQuerySession` | 961 | **INTERNAL** — make `pub(crate)` |

## Design Decisions

| Decision | Choice |
|----------|--------|
| Error type | Unified `SessionError` for everything |
| Feature flags | `demo` and `gotv` are both optional features |
| Backwards compat | None — delete old APIs, port/delete their tests |
| Schema return type | `HashMap<Arc<str>, EntitySchema>` (simple) |
| GOTV metadata | Dropped — no `map()`/`tick()` on source |
| Cancellation | `session.with_cancel_token()` — works for both sources |

## Implementation Phases

### Phase 1: Create DemoSource with IntoStreamingSession

**Files:**
- Create `src/demo/source.rs` with `DemoSource` struct
- Implement `IntoStreamingSession` for `DemoSource`
- Update `src/demo/mod.rs` to export `DemoSource`

**Changes to session.rs:**
- Add `pub(crate) fn new_from_demo(bytes: Bytes, schemas: HashMap<...>) -> Self`
- Keep existing logic, just expose internal constructor

**Verification:**
```bash
nice -n 19 cargo test --features demo
```

### Phase 2: Create GotvSource with IntoStreamingSession

**Files:**
- Create `src/gotv/source.rs` with `GotvSource` struct
- Implement `IntoStreamingSession` for `GotvSource`
- Update `src/gotv/mod.rs` to export `GotvSource`

**Changes to session.rs:**
- Add `pub(crate) fn new_from_gotv(client: BroadcastClient, start_packet: Bytes, schemas: HashMap<...>) -> Self`

**Verification:**
```bash
nice -n 19 cargo test --features gotv
```

### Phase 3: Add Integration Tests for New API

Port relevant tests from old implementations:
- Entity table queries
- Event table queries  
- Multiple concurrent queries
- JOIN queries
- UNION ALL queries
- Cancellation

**Verification:**
```bash
TEST_DEMO_PATH=/path/to/test.dem nice -n 19 cargo test --features demo --ignored
```

### Phase 4: Delete Old Session Implementations

**Delete:**
- `src/demo/session.rs` (DemoFileSession)
- `src/gotv/session.rs` (SpectateSession)

**Update:**
- `src/demo/mod.rs` — remove `session` module, keep `source`
- `src/gotv/mod.rs` — remove `session` module, keep `source` + `client`

**Verification:**
```bash
nice -n 19 cargo test --all-features
```

### Phase 5: Fix Unbounded Channel

Replace `mpsc::unbounded_channel()` result forwarding with direct `SendableRecordBatchStream` in `QueryHandle`.

**Changes to session.rs:**
```rust
pub struct QueryHandle {
    // Before: receiver: mpsc::UnboundedReceiver<Result<RecordBatch, SessionError>>
    // After:
    stream: Mutex<Option<SendableRecordBatchStream>>,
    schema: SchemaRef,
}
```

**Changes to start():**
- Fill stream slot directly instead of spawning forwarder task

### Phase 6: Make query_session.rs Internal

**Changes:**
- `pub struct StreamingQuerySession` → `pub(crate) struct StreamingQuerySession`
- `pub struct QueryStreams` → `pub(crate) struct QueryStreams`
- Update any tests that use it directly

### Phase 7: Break Up start_internal()

Refactor `start_internal()` into smaller, testable functions:
- `collect_query_requirements()` — gather slots from pending queries
- `setup_distribution_channels()` — create channels, map slots
- `spawn_parser()` — start parser task
- `bind_receivers_to_slots()` — fill slots with receivers
- `execute_query_plans()` — execute plans, return streams

### Phase 8: Update Documentation

**Update README.md:**
- Replace `DemoFileSession`/`SpectateSession` examples with new API
- Document `DemoSource`/`GotvSource` pattern
- Update feature flag documentation

**Update lib.rs exports:**
```rust
// Core (always available)
pub use session::{QueryHandle, SessionError, SessionResult, StreamingSession, IntoStreamingSession};

// Demo source (feature-gated)
#[cfg(feature = "demo")]
pub use demo::DemoSource;

// GOTV source (feature-gated)  
#[cfg(feature = "gotv")]
pub use gotv::GotvSource;
```

## File Structure After Consolidation

```
src/
├── lib.rs                 # Exports: StreamingSession, QueryHandle, SessionError, IntoStreamingSession
├── session.rs             # StreamingSession implementation (~600 lines after cleanup)
├── demo/
│   ├── mod.rs             # pub use source::DemoSource
│   └── source.rs          # DemoSource + IntoStreamingSession impl (~50 lines)
├── gotv/
│   ├── mod.rs             # pub use source::GotvSource, client::*, etc.
│   ├── source.rs          # GotvSource + IntoStreamingSession impl (~80 lines)
│   ├── client.rs          # BroadcastClient (unchanged)
│   ├── config.rs          # ClientConfig (unchanged)
│   └── error.rs           # GotvError (maybe merge into SessionError)
└── datafusion/
    ├── query_session.rs   # pub(crate) StreamingQuerySession (internal)
    └── ...
```

## Lines of Code

| Component | Before | After | Change |
|-----------|--------|-------|--------|
| `session.rs` | 809 | ~600 | -200 (cleanup) |
| `demo/session.rs` | 445 | 0 | -445 (deleted) |
| `demo/source.rs` | 0 | ~50 | +50 (new) |
| `gotv/session.rs` | 634 | 0 | -634 (deleted) |
| `gotv/source.rs` | 0 | ~80 | +80 (new) |
| **Total** | **1888** | **~730** | **~-1150 lines** |

## Priority Order

1. **Phase 1-2**: Create sources with trait (establishes new API)
2. **Phase 3**: Add integration tests (verify new API works)
3. **Phase 4**: Delete old implementations (remove duplication)
4. **Phase 5**: Fix unbounded channel (bug fix)
5. **Phase 6-7**: Internal cleanup (testability)
6. **Phase 8**: Documentation (user-facing)

## Open Questions

1. **Trait location**: Should `IntoStreamingSession` be in `session.rs` or its own `traits.rs`?

2. **Error consolidation**: Merge `GotvError` into `SessionError`, or keep separate and impl `From`?

3. **Feature flag naming**: `demo` and `gotv`, or something else?
