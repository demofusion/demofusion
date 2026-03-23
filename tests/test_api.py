"""Tests for demofusion Python API."""

import asyncio
import pytest
import pyarrow as pa
from demofusion import (
    DemoSource,
    GotvSource,
    QueryHandle,
    DemofusionError,
    DemofusionIOError,
    DemofusionSchemaError,
    DemofusionSessionError,
)

# Access raw Rust classes for low-level tests
from demofusion._demofusion import (
    DemoSource as RawDemoSource,
    StreamingSession as RawStreamingSession,
)


# ── High-level API tests ─────────────────────────────────────────────


class TestDemoSourceOpen:
    """Test DemoSource.open() high-level constructor."""

    @pytest.mark.asyncio
    async def test_open_returns_demo_source(self, demo_path):
        """DemoSource.open should return a DemoSource instance."""
        demo = await DemoSource.open(demo_path)
        assert isinstance(demo, DemoSource)

    @pytest.mark.asyncio
    async def test_open_nonexistent_file(self):
        """DemoSource.open should raise IOError for missing files."""
        with pytest.raises(DemofusionIOError):
            await DemoSource.open("/nonexistent/path/demo.dem")

    @pytest.mark.asyncio
    async def test_open_with_batch_size(self, demo_path):
        """DemoSource.open should accept batch_size kwarg."""
        demo = await DemoSource.open(demo_path, batch_size=256)
        assert demo.get_tables()

    @pytest.mark.asyncio
    async def test_open_with_reject_pipeline_breakers(self, demo_path):
        """DemoSource.open should accept reject_pipeline_breakers kwarg."""
        demo = await DemoSource.open(demo_path, reject_pipeline_breakers=True)
        assert demo.get_tables()

    @pytest.mark.asyncio
    async def test_open_with_all_config(self, demo_path):
        """DemoSource.open should accept all config kwargs together."""
        demo = await DemoSource.open(
            demo_path, batch_size=512, reject_pipeline_breakers=False
        )
        assert demo.get_tables()


class TestDemoSourceFromBytes:
    """Test DemoSource.from_bytes() high-level constructor."""

    @pytest.mark.asyncio
    async def test_from_bytes_invalid_data(self):
        """from_bytes with empty data should raise DemofusionSchemaError."""
        with pytest.raises(DemofusionSchemaError):
            await DemoSource.from_bytes(b"")

    @pytest.mark.asyncio
    async def test_from_bytes_garbage_data(self):
        """from_bytes with garbage should raise DemofusionSchemaError."""
        with pytest.raises(DemofusionSchemaError):
            await DemoSource.from_bytes(b"\x00\x01\x02\x03" * 100)


class TestDemoSourceSchemas:
    """Test schema access on DemoSource."""

    @pytest.mark.asyncio
    async def test_schemas_property(self, demo_path):
        """schemas property should return a non-empty dict."""
        demo = await DemoSource.open(demo_path)
        schemas = demo.schemas
        assert isinstance(schemas, dict)
        assert len(schemas) > 0

    @pytest.mark.asyncio
    async def test_schemas_values_are_pyarrow(self, demo_path):
        """Schema values should be pyarrow.Schema instances."""
        demo = await DemoSource.open(demo_path)
        for name, schema in demo.schemas.items():
            assert isinstance(name, str)
            assert isinstance(schema, pa.Schema), f"{name}: expected pa.Schema, got {type(schema)}"

    @pytest.mark.asyncio
    async def test_get_tables(self, demo_path):
        """get_tables() should return list of table names."""
        demo = await DemoSource.open(demo_path)
        tables = demo.get_tables()
        assert isinstance(tables, list)
        assert len(tables) > 0
        assert all(isinstance(t, str) for t in tables)

    @pytest.mark.asyncio
    async def test_get_schema(self, demo_path):
        """get_schema(name) should return PyArrow schema."""
        demo = await DemoSource.open(demo_path)
        tables = demo.get_tables()
        schema = demo.get_schema(tables[0])
        assert schema is not None
        assert isinstance(schema, pa.Schema)

    @pytest.mark.asyncio
    async def test_get_schema_nonexistent(self, demo_path):
        """get_schema() should return None for missing table."""
        demo = await DemoSource.open(demo_path)
        assert demo.get_schema("NonExistentTable12345") is None


class TestEventSchemaDiscovery:
    """Test that event tables appear in schema discovery alongside entities."""

    # Well-known event table names from the generated code
    KNOWN_EVENT_TABLES = [
        "DamageEvent",
        "HeroKilledEvent",
        "BulletHitEvent",
        "CurrencyChangedEvent",
        "GameOverEvent",
    ]

    # Well-known entity table names
    KNOWN_ENTITY_TABLES = [
        "CCitadelPlayerPawn",
        "CCitadelPlayerController",
    ]

    @pytest.mark.asyncio
    async def test_get_tables_includes_event_tables(self, demo_path):
        """get_tables() should include event table names like DamageEvent."""
        demo = await DemoSource.open(demo_path)
        tables = demo.get_tables()
        for event_table in self.KNOWN_EVENT_TABLES:
            assert event_table in tables, f"Event table '{event_table}' not found in get_tables()"

    @pytest.mark.asyncio
    async def test_get_tables_includes_entity_tables(self, demo_path):
        """get_tables() should still include entity table names."""
        demo = await DemoSource.open(demo_path)
        tables = demo.get_tables()
        for entity_table in self.KNOWN_ENTITY_TABLES:
            assert entity_table in tables, f"Entity table '{entity_table}' not found in get_tables()"

    @pytest.mark.asyncio
    async def test_schemas_includes_event_tables(self, demo_path):
        """schemas property should include event table schemas."""
        demo = await DemoSource.open(demo_path)
        schemas = demo.schemas
        for event_table in self.KNOWN_EVENT_TABLES:
            assert event_table in schemas, f"Event table '{event_table}' not found in schemas"
            assert isinstance(schemas[event_table], pa.Schema)

    @pytest.mark.asyncio
    async def test_get_schema_returns_event_schema(self, demo_path):
        """get_schema() should return a schema for event tables."""
        demo = await DemoSource.open(demo_path)
        schema = demo.get_schema("DamageEvent")
        assert schema is not None
        assert isinstance(schema, pa.Schema)

    @pytest.mark.asyncio
    async def test_event_schema_has_tick_column(self, demo_path):
        """Event schemas should have a 'tick' column."""
        demo = await DemoSource.open(demo_path)
        schema = demo.get_schema("DamageEvent")
        assert schema is not None
        field_names = [schema.field(i).name for i in range(len(schema))]
        assert "tick" in field_names, "Event schema should contain a 'tick' column"

    @pytest.mark.asyncio
    async def test_event_schema_has_no_entity_index(self, demo_path):
        """Event schemas should NOT have 'entity_index' (only entities have that)."""
        demo = await DemoSource.open(demo_path)
        schema = demo.get_schema("DamageEvent")
        assert schema is not None
        field_names = [schema.field(i).name for i in range(len(schema))]
        assert "entity_index" not in field_names

    @pytest.mark.asyncio
    async def test_event_tables_end_with_event_suffix(self, demo_path):
        """All event tables in get_tables() should end with 'Event'."""
        demo = await DemoSource.open(demo_path)
        tables = demo.get_tables()
        event_tables = [t for t in tables if t.endswith("Event")]
        assert len(event_tables) > 0, "Should have at least some event tables"

    @pytest.mark.asyncio
    async def test_raw_session_get_tables_includes_events(self, demo_path):
        """Raw StreamingSession.get_tables() should also include events."""
        raw = await RawDemoSource.open(demo_path)
        session = await raw.into_session()
        tables = session.get_tables()
        for event_table in self.KNOWN_EVENT_TABLES:
            assert event_table in tables, f"Event table '{event_table}' not in raw get_tables()"

    @pytest.mark.asyncio
    async def test_raw_session_schemas_includes_events(self, demo_path):
        """Raw StreamingSession.schemas should include event schemas."""
        raw = await RawDemoSource.open(demo_path)
        session = await raw.into_session()
        schemas = session.schemas
        for event_table in self.KNOWN_EVENT_TABLES:
            assert event_table in schemas, f"Event table '{event_table}' not in raw schemas"


class TestDemoSourceContextManager:
    """Test DemoSource async context manager."""

    @pytest.mark.asyncio
    async def test_context_manager_basic(self, demo_path):
        """DemoSource should work as async context manager."""
        async with await DemoSource.open(demo_path) as demo:
            assert demo.get_tables()

    @pytest.mark.asyncio
    async def test_context_manager_cleanup(self, demo_path):
        """After exiting context, start() should fail."""
        demo = await DemoSource.open(demo_path)
        async with demo:
            pass
        with pytest.raises(DemofusionSessionError):
            demo.start()


class TestDemoSourceConfiguration:
    """Test session configuration via DemoSource.open() kwargs."""

    @pytest.mark.asyncio
    async def test_batch_size_applied(self, demo_path):
        """batch_size should limit rows per RecordBatch."""
        demo = await DemoSource.open(demo_path, batch_size=64)
        handle = await demo.add_query(
            "SELECT tick FROM CCitadelPlayerPawn LIMIT 128"
        )
        demo.start()
        batches = []
        async for batch in handle:
            batches.append(batch)
        total_rows = sum(b.num_rows for b in batches)
        assert total_rows == 128
        assert all(b.num_rows <= 64 for b in batches)


class TestQueryRegistration:
    """Test query registration."""

    @pytest.mark.asyncio
    async def test_add_query_returns_query_handle(self, demo_path):
        """add_query should return a QueryHandle."""
        demo = await DemoSource.open(demo_path)
        handle = await demo.add_query(
            "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 5"
        )
        assert isinstance(handle, QueryHandle)

    @pytest.mark.asyncio
    async def test_add_query_nonexistent_table(self, demo_path):
        """add_query should raise for nonexistent table."""
        demo = await DemoSource.open(demo_path)
        with pytest.raises(DemofusionError):
            await demo.add_query("SELECT * FROM NonExistentTable12345")

    @pytest.mark.asyncio
    async def test_add_multiple_queries(self, demo_path):
        """Multiple queries can be registered before start."""
        demo = await DemoSource.open(demo_path)
        h1 = await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 1")
        h2 = await demo.add_query("SELECT tick FROM CCitadelPlayerController LIMIT 1")
        assert isinstance(h1, QueryHandle)
        assert isinstance(h2, QueryHandle)


class TestSessionStartup:
    """Test start() method."""

    @pytest.mark.asyncio
    async def test_start_is_synchronous(self, demo_path):
        """start() should return None (not a coroutine)."""
        demo = await DemoSource.open(demo_path)
        await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 1")
        result = demo.start()
        assert result is None

    @pytest.mark.asyncio
    async def test_start_twice_raises(self, demo_path):
        """Calling start() twice should raise DemofusionSessionError."""
        demo = await DemoSource.open(demo_path)
        await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 1")
        demo.start()
        with pytest.raises(DemofusionSessionError):
            demo.start()


class TestQueryHandleBasics:
    """Test QueryHandle API."""

    @pytest.mark.asyncio
    async def test_query_handle_schema(self, demo_path):
        """QueryHandle.schema should be a pyarrow.Schema."""
        demo = await DemoSource.open(demo_path)
        handle = await demo.add_query(
            "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 1"
        )
        assert isinstance(handle.schema, pa.Schema)
        assert len(handle.schema) == 2
        assert handle.schema.field(0).name == "tick"
        assert handle.schema.field(1).name == "entity_index"

    @pytest.mark.asyncio
    async def test_query_handle_is_async_iterable(self, demo_path):
        """QueryHandle should support async iteration."""
        demo = await DemoSource.open(demo_path)
        handle = await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 1")
        demo.start()
        batches = []
        async for batch in handle:
            batches.append(batch)
        assert len(batches) >= 1

    @pytest.mark.asyncio
    async def test_query_handle_yields_record_batches(self, demo_path):
        """Iteration should yield pyarrow.RecordBatch."""
        demo = await DemoSource.open(demo_path)
        handle = await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 5")
        demo.start()
        async for batch in handle:
            assert isinstance(batch, pa.RecordBatch)
            break


class TestAsyncIteration:
    """Test async iteration over query results."""

    @pytest.mark.asyncio
    async def test_single_query_iteration(self, demo_path):
        """Iterate a single query to completion."""
        demo = await DemoSource.open(demo_path)
        handle = await demo.add_query(
            "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10"
        )
        demo.start()
        total_rows = 0
        async for batch in handle:
            assert isinstance(batch, pa.RecordBatch)
            total_rows += batch.num_rows
        assert total_rows == 10

    @pytest.mark.asyncio
    async def test_concurrent_query_iteration(self, demo_path):
        """Iterate multiple queries concurrently with asyncio.gather."""
        demo = await DemoSource.open(demo_path)
        h1 = await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 5")
        h2 = await demo.add_query("SELECT tick FROM CCitadelPlayerController LIMIT 5")
        demo.start()

        async def collect(handle):
            rows = 0
            async for batch in handle:
                rows += batch.num_rows
            return rows

        r1, r2 = await asyncio.gather(collect(h1), collect(h2))
        assert r1 == 5
        assert r2 == 5


class TestCompleteWorkflow:
    """Test complete workflows."""

    @pytest.mark.asyncio
    async def test_full_flow(self, demo_path):
        """Complete: open -> inspect schemas -> query -> iterate."""
        async with await DemoSource.open(demo_path) as demo:
            tables = demo.get_tables()
            assert "CCitadelPlayerPawn" in tables

            handle = await demo.add_query(
                'SELECT tick, entity_index, "m_iHealth" FROM CCitadelPlayerPawn LIMIT 20'
            )
            demo.start()

            total_rows = 0
            async for batch in handle:
                assert isinstance(batch, pa.RecordBatch)
                assert batch.num_columns == 3
                total_rows += batch.num_rows
            assert total_rows == 20

    @pytest.mark.asyncio
    async def test_schema_inspection(self, demo_path):
        """Inspect schemas before registering queries."""
        demo = await DemoSource.open(demo_path)
        schemas = demo.schemas
        assert isinstance(schemas, dict)
        assert len(schemas) > 100

        for name, schema in schemas.items():
            assert isinstance(schema, pa.Schema)
            field_names = [schema.field(i).name for i in range(len(schema))]
            assert "tick" in field_names
            assert "entity_index" in field_names

    @pytest.mark.asyncio
    async def test_multiple_concurrent_queries(self, demo_path):
        """Multiple queries with asyncio.gather."""
        demo = await DemoSource.open(demo_path)
        h1 = await demo.add_query(
            "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10"
        )
        h2 = await demo.add_query(
            "SELECT tick, entity_index FROM CCitadelPlayerController LIMIT 10"
        )
        demo.start()

        async def collect(handle):
            batches = []
            async for batch in handle:
                batches.append(batch)
            return batches

        b1, b2 = await asyncio.gather(collect(h1), collect(h2))
        assert sum(b.num_rows for b in b1) == 10
        assert sum(b.num_rows for b in b2) == 10


class TestErrorHandling:
    """Test error handling and exception types."""

    def test_exception_hierarchy(self):
        """All specific exceptions should inherit from DemofusionError."""
        assert issubclass(DemofusionIOError, DemofusionError)
        assert issubclass(DemofusionSchemaError, DemofusionError)
        assert issubclass(DemofusionSessionError, DemofusionError)

    @pytest.mark.asyncio
    async def test_io_error(self):
        """File not found should raise DemofusionIOError."""
        with pytest.raises(DemofusionIOError):
            await DemoSource.open("/nonexistent/demo.dem")

    @pytest.mark.asyncio
    async def test_schema_error(self):
        """Invalid demo bytes should raise DemofusionSchemaError."""
        with pytest.raises(DemofusionSchemaError):
            await DemoSource.from_bytes(b"invalid")

    @pytest.mark.asyncio
    async def test_add_query_after_start_raises(self, demo_path):
        """Adding a query after start() should raise."""
        demo = await DemoSource.open(demo_path)
        await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 1")
        demo.start()
        with pytest.raises(DemofusionSessionError):
            await demo.add_query("SELECT tick FROM CCitadelPlayerPawn LIMIT 1")


# ── Low-level (raw) API tests ────────────────────────────────────────


class TestRawDemoSource:
    """Test the low-level Rust DemoSource directly."""

    @pytest.mark.asyncio
    async def test_raw_open_and_into_session(self, demo_path):
        """Raw open -> into_session -> StreamingSession works."""
        raw = await RawDemoSource.open(demo_path)
        session = await raw.into_session()
        assert isinstance(session, RawStreamingSession)
        assert session.get_tables()

    @pytest.mark.asyncio
    async def test_raw_from_bytes_and_into_session(self, demo_path):
        """Raw from_bytes -> into_session works with real data."""
        with open(demo_path, "rb") as f:
            data = f.read()
        raw = RawDemoSource.from_bytes(data)
        session = await raw.into_session()
        assert isinstance(session, RawStreamingSession)

    @pytest.mark.asyncio
    async def test_raw_into_session_with_config(self, demo_path):
        """Raw into_session accepts config kwargs."""
        raw = await RawDemoSource.open(demo_path)
        session = await raw.into_session(batch_size=256, reject_pipeline_breakers=True)
        assert isinstance(session, RawStreamingSession)

    @pytest.mark.asyncio
    async def test_raw_double_into_session_raises(self):
        """Calling into_session() twice on raw source should raise."""
        raw = RawDemoSource.from_bytes(b"test")
        with pytest.raises(DemofusionError):
            await raw.into_session()
        with pytest.raises(DemofusionSessionError):
            await raw.into_session()

    @pytest.mark.asyncio
    async def test_raw_full_workflow(self, demo_path):
        """Raw: open -> into_session -> add_query -> start -> iterate."""
        raw = await RawDemoSource.open(demo_path)
        session = await raw.into_session(batch_size=64)
        handle = await session.add_query(
            "SELECT tick FROM CCitadelPlayerPawn LIMIT 10"
        )
        session.start()
        total_rows = 0
        async for batch in handle:
            total_rows += batch.num_rows
        assert total_rows == 10


# ── GotvSource tests ─────────────────────────────────────────────────


class TestGotvSource:
    """Test GotvSource (requires gotv feature)."""

    def test_gotv_source_class_exists(self):
        """GotvSource class should always be importable."""
        assert GotvSource is not None  # It's the Python wrapper, always defined

    @pytest.mark.asyncio
    async def test_gotv_connect_without_feature(self):
        """GotvSource.connect should raise if gotv feature is not enabled."""
        try:
            await GotvSource.connect("http://invalid.example.com/tv/123")
            pytest.fail("Should have raised")
        except RuntimeError:
            pass  # Expected: gotv feature not enabled
        except DemofusionIOError:
            pass  # Expected: gotv feature enabled, bad URL

    @pytest.mark.asyncio
    async def test_gotv_into_session_returns_streaming_session(self):
        """GotvSource.connect should return GotvSource on real server."""
        pytest.skip("Requires real GOTV server for full test")
