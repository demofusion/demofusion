# Event Schema Discovery: Shared SessionContext Architecture

## Goal
Wire up event schema discovery so event tables (like `DamageEvent`) appear alongside entity tables (like `CCitadelPlayerPawn`) in `get_tables()`, `schemas`, and `get_schema()`. This involves restructuring to use a shared `SessionContext` with providers registered upfront, aligning with DataFusion's intended architecture.

## Branch
`feature/event-schema-discovery` (from `main` at `3bf9808`)

## TDD Tests (already written)
`tests/test_api.py` — `TestEventSchemaDiscovery` class (10 tests):
- `test_get_tables_includes_event_tables`
- `test_get_tables_includes_entity_tables`
- `test_schemas_includes_event_tables`
- `test_get_schema_returns_event_schema`
- `test_event_schema_has_tick_column`
- `test_event_schema_has_no_entity_index`
- `test_event_tables_end_with_event_suffix`
- `test_raw_session_get_tables_includes_events`
- `test_raw_session_schemas_includes_events`

## Implementation Steps

### Step 1: Restructure TableProviders (`src/datafusion/table_providers.rs`)
- [ ] Remove `receiver_slot` field from `EntityTableProvider` constructor
- [ ] Add `pending_slots: Arc<Mutex<Vec<ReceiverSlot>>>` field
- [ ] `scan()` creates a new empty `ReceiverSlot`, pushes to `pending_slots`, passes to `PartitionStream`
- [ ] Add `drain_pending_slots() -> Vec<ReceiverSlot>` method
- [ ] Add `entity_type()` / `event_type()` accessors
- [ ] Same changes for `EventTableProvider`
- [ ] `PartitionStream` impls unchanged (still `.take()` in `execute()`)
- [ ] Update unit tests
- [ ] `cargo check`

### Step 2: Restructure StreamingSession (`src/session.rs`)
- [ ] Add `ctx: SessionContext` field (shared context)
- [ ] Add `entity_providers: HashMap<Arc<str>, Arc<EntityTableProvider>>` field
- [ ] Add `event_providers: HashMap<Arc<str>, Arc<EventTableProvider>>` field
- [ ] Rename `schemas` → `entity_schemas` (internal use for parser)
- [ ] Remove `Schemas` public type alias
- [ ] Update constructors to create shared context + register all providers upfront
- [ ] `cargo check`

### Step 3: Simplify `add_query()` (`src/session.rs`)
- [ ] Remove `extract_table_names()` call
- [ ] Remove per-query `SessionContext` creation
- [ ] Remove per-query `TableProvider` creation + slot management
- [ ] Use shared `self.ctx.sql(sql)` → physical planning (triggers `scan()` on referenced tables)
- [ ] Simplify `PendingQuery` to just `{ physical_plan, result_tx }`
- [ ] Remove unknown table validation (DataFusion will return an error for unknown tables)
- [ ] `cargo check`

### Step 4: Rework `start()` (`src/session.rs`)
- [ ] Change signature from `start(mut self)` to `start(&mut self)`
- [ ] Remove `CollectedSlots`, `SlotBindings`, `DispatcherChannels` structs
- [ ] Iterate `entity_providers` / `event_providers`, call `drain_pending_slots()`
- [ ] Non-empty providers → need distribution channels
- [ ] Create distribution channels, wire senders to parser, inject receivers into slots
- [ ] Execute plans using shared context's `TaskContext`
- [ ] `cargo check`

### Step 5: Simplify `IntoStreamingSession` + sources
- [ ] `src/session.rs`: Change `IntoStreamingSession` return to `Result<StreamingSession, SessionError>`
- [ ] `src/demo/source.rs`: Update `DemoSource::into_session()`
- [ ] `src/gotv/` (if applicable): Update `GotvSource::into_session()`
- [ ] `cargo check`

### Step 6: Update Python bindings (`src/python/session.rs`)
- [ ] Remove `schemas: Schemas` field from `PyStreamingSession`
- [ ] `schemas` getter / `get_tables()` / `get_schema()` delegate to session
- [ ] Handle post-start schema access (session stays alive since `start(&mut self)`)
- [ ] Update `from_session()` constructor
- [ ] `cargo check`

### Step 7: Update Python wrapper + stubs
- [ ] `demofusion/__init__.py`: Update `DemoSource.open()` / `from_bytes()` for new `into_session()` return
- [ ] `demofusion/_demofusion.pyi`: Update type stubs if needed

### Step 8: Update public exports + cleanup
- [ ] `src/lib.rs`: Remove `Schemas` from re-exports, update any changed types
- [ ] `src/datafusion/mod.rs`: Update re-exports
- [ ] Remove `new_receiver_slot` from public API if no longer needed externally
- [ ] Remove `streaming_session_context()` helper or repurpose

### Step 9: Update existing tests
- [ ] `src/session.rs` unit tests: Remove/update tests for deleted structs
- [ ] `tests/test_e2e.py`: Update table count assertion (862 → 862 + event_count)
- [ ] `tests/test_api.py`: Verify all tests pass

### Step 10: Build and test
- [ ] `cargo check` (fast compile check)
- [ ] `cargo test` (Rust unit tests)
- [ ] `maturin develop` (build Python bindings)
- [ ] `pytest tests/` (Python tests including TDD event tests)

## Architecture Diagram

```
BEFORE (per-query context):
  add_query() → extract_table_names() → new SessionContext → new TableProviders → new ReceiverSlots
  start()     → CollectedSlots → DispatcherChannels → SlotBindings::bind() → execute plans

AFTER (shared context):
  constructor → SessionContext → register ALL EntityTableProviders + EventTableProviders
  add_query() → ctx.sql() → scan() auto-creates ReceiverSlot per referenced table
  start()     → drain_pending_slots() from providers → create channels → inject receivers → execute plans
```

## Key Design Decisions
1. **One shared SessionContext** — aligns with DataFusion's intended architecture
2. **Concrete provider maps** — `HashMap<Arc<str>, Arc<EntityTableProvider>>` + same for events, no `dyn`
3. **ReceiverSlot created in scan()** — DataFusion's planner tells us which tables are referenced
4. **start(&mut self)** — session survives start() for post-start schema queries
5. **No extract_table_names() in query path** — DataFusion handles table resolution
6. **Backpressure preserved** — same distribution channel + global gate pattern

## Files Modified
- `src/datafusion/table_providers.rs` — Provider restructuring
- `src/session.rs` — Major restructuring (shared context, simplified add_query/start)
- `src/python/session.rs` — Delegate to session for schemas
- `src/demo/source.rs` — Simplified IntoStreamingSession return
- `src/lib.rs` — Updated exports
- `src/datafusion/mod.rs` — Updated re-exports
- `demofusion/__init__.py` — Updated for new into_session return
- `demofusion/_demofusion.pyi` — Updated stubs
- `tests/test_api.py` — Already has TDD tests, may need minor updates
- `tests/test_e2e.py` — Table count assertion update

## Build Notes
- Release builds OOM on `valveprotos`. Use `cargo check` for fast iteration.
- Debug builds work but are slow. Use `cargo check` first, `cargo test` when ready.
- `maturin develop` for Python binding builds.
