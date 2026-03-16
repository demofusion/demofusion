"""End-to-end tests for demofusion Python bindings.

These tests parse real demo files end-to-end and validate:
- Full file parsing (no LIMIT)
- Data correctness (known values from the demo)
- Multiple concurrent queries with concurrent draining
- WHERE filters, computed columns, multi-table queries
- from_bytes workflow
- batch_size configuration effect
- Schema inspection against real data
- Logging bridge (Rust tracing -> Python logging)

All multi-query tests drain handles concurrently via asyncio.gather,
which is required because the single-pass parser feeds all queries
simultaneously.
"""

import asyncio
import logging
import pytest
import pyarrow as pa

from demofusion import (
    DemoSource,
    DemofusionError,
    DemofusionDataFusionError,
)


# ── Helpers ──────────────────────────────────────────────────────────


async def collect_rows(handle):
    """Drain a QueryHandle, return total row count."""
    total = 0
    async for batch in handle:
        total += batch.num_rows
    return total


async def collect_batches(handle):
    """Drain a QueryHandle, return list of RecordBatches."""
    batches = []
    async for batch in handle:
        batches.append(batch)
    return batches


async def collect_dicts(handle):
    """Drain a QueryHandle, return list of row dicts."""
    rows = []
    async for batch in handle:
        d = batch.to_pydict()
        for i in range(batch.num_rows):
            rows.append({k: v[i] for k, v in d.items()})
    return rows


# ── Full Parse Tests ─────────────────────────────────────────────────


class TestFullParse:
    """Tests that parse the entire demo file (no LIMIT)."""

    @pytest.mark.asyncio
    async def test_full_parse_player_pawn(self, demo_path):
        """Parse all CCitadelPlayerPawn rows from the full demo."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                "SELECT tick, entity_index FROM CCitadelPlayerPawn"
            )
            demo.start()
            total = await collect_rows(h)

        # Known value from exploration: 1,556,196 rows
        assert total == 1_556_196

    @pytest.mark.asyncio
    async def test_full_parse_last_tick(self, demo_path):
        """Last tick in the demo should be 130767."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                "SELECT tick FROM CCitadelPlayerPawn"
            )
            demo.start()
            last_tick = 0
            async for batch in h:
                ticks = batch.column("tick").to_pylist()
                if ticks:
                    last_tick = max(last_tick, max(t for t in ticks if t is not None))

        assert last_tick == 130767

    @pytest.mark.asyncio
    async def test_full_parse_multi_table_concurrent(self, demo_path):
        """Parse multiple tables concurrently, all drained via gather."""
        async with await DemoSource.open(demo_path) as demo:
            h_pawn = await demo.add_query(
                "SELECT tick FROM CCitadelPlayerPawn"
            )
            h_ctrl = await demo.add_query(
                "SELECT tick FROM CCitadelPlayerController"
            )
            h_team = await demo.add_query(
                "SELECT tick FROM CCitadelTeam"
            )
            demo.start()

            pawn, ctrl, team = await asyncio.gather(
                collect_rows(h_pawn),
                collect_rows(h_ctrl),
                collect_rows(h_team),
            )

        assert pawn == 1_556_196
        assert ctrl == 300_715
        assert team == 123_679


# ── Data Correctness Tests ───────────────────────────────────────────


class TestDataCorrectness:
    """Validate known values from the demo file."""

    @pytest.mark.asyncio
    async def test_tick1_player_pawn_health(self, demo_path):
        """At tick 1, specific player pawns have known health values."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                'SELECT tick, entity_index, "m_iHealth" '
                "FROM CCitadelPlayerPawn WHERE tick = 1"
            )
            demo.start()
            rows = await collect_dicts(h)

        # 24 entity updates at tick 1
        assert len(rows) == 24

        # All rows should be tick 1
        assert all(r["tick"] == 1 for r in rows)

        # Known entity 65 health at tick 1
        e65 = [r for r in rows if r["entity_index"] == 65]
        assert len(e65) >= 1
        assert e65[0]["m_iHealth"] == 890

    @pytest.mark.asyncio
    async def test_tick1_player_controller_teams(self, demo_path):
        """At tick 1, player controllers have known team assignments."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                'SELECT tick, entity_index, "m_iTeamNum", "m_steamID" '
                "FROM CCitadelPlayerController WHERE tick = 1"
            )
            demo.start()
            rows = await collect_dicts(h)

        # Entity 1 at tick 1: team 1
        e1 = [r for r in rows if r["entity_index"] == 1]
        assert len(e1) >= 1
        assert e1[0]["m_iTeamNum"] == 1

        # Entity 2: known Steam ID
        e2 = [r for r in rows if r["entity_index"] == 2]
        assert len(e2) >= 1
        assert e2[0]["m_steamID"] == 76561198062635137

    @pytest.mark.asyncio
    async def test_team_entities_at_tick1(self, demo_path):
        """At tick 1, team entities have known team numbers."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                'SELECT tick, entity_index, "m_iTeamNum" '
                "FROM CCitadelTeam WHERE tick = 1"
            )
            demo.start()
            rows = await collect_dicts(h)

        team_nums = sorted(set(r["m_iTeamNum"] for r in rows))
        # Teams 0, 1, 2, 3, 4 exist at tick 1
        assert team_nums == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    async def test_game_rules_start_time(self, demo_path):
        """GameRules at tick 1 has a known game start time."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                'SELECT tick, "m_pGameRules__m_flGameStartTime" '
                "FROM CCitadelGameRulesProxy WHERE tick = 1"
            )
            demo.start()
            rows = await collect_dicts(h)

        assert len(rows) >= 1
        assert rows[0]["m_pGameRules__m_flGameStartTime"] == pytest.approx(35.375)


# ── Query Feature Tests ──────────────────────────────────────────────


class TestQueryFeatures:
    """Test SQL features: filters, projections, computed columns."""

    @pytest.mark.asyncio
    async def test_where_filter_entity_index(self, demo_path):
        """WHERE entity_index = 65 should filter correctly."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                'SELECT tick, entity_index, "m_iHealth" '
                "FROM CCitadelPlayerPawn "
                "WHERE entity_index = 65 LIMIT 5"
            )
            demo.start()
            rows = await collect_dicts(h)

        assert len(rows) == 5
        assert all(r["entity_index"] == 65 for r in rows)

    @pytest.mark.asyncio
    async def test_computed_column(self, demo_path):
        """Computed boolean column via SQL expression."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                'SELECT tick, entity_index, "m_iHealth", '
                '"m_iHealth" > 0 AS alive '
                "FROM CCitadelPlayerPawn LIMIT 10"
            )
            demo.start()
            rows = await collect_dicts(h)

        assert len(rows) == 10
        for r in rows:
            assert r["alive"] == (r["m_iHealth"] > 0)

    @pytest.mark.asyncio
    async def test_multiple_columns_projection(self, demo_path):
        """Selecting multiple entity fields returns correct schema."""
        async with await DemoSource.open(demo_path) as demo:
            h = await demo.add_query(
                'SELECT tick, entity_index, "m_iHealth", "m_lifeState", '
                '"m_angEyeAngles__x", "m_angEyeAngles__y" '
                "FROM CCitadelPlayerPawn LIMIT 5"
            )
            demo.start()
            batches = await collect_batches(h)

        assert len(batches) >= 1
        schema = batches[0].schema
        assert schema.names == [
            "tick", "entity_index", "m_iHealth", "m_lifeState",
            "m_angEyeAngles__x", "m_angEyeAngles__y",
        ]

    @pytest.mark.asyncio
    async def test_pipeline_breaker_rejected(self, demo_path):
        """Aggregate queries (pipeline breakers) should be rejected by DataFusion
        on unbounded streaming tables."""
        demo = await DemoSource.open(demo_path)
        with pytest.raises(DemofusionDataFusionError):
            await demo.add_query("SELECT COUNT(*) FROM CCitadelPlayerPawn")


# ── from_bytes Tests ─────────────────────────────────────────────────


class TestFromBytesE2E:
    """End-to-end tests using DemoSource.from_bytes()."""

    @pytest.mark.asyncio
    async def test_from_bytes_full_workflow(self, demo_path):
        """from_bytes -> session -> query -> iterate -> correct results."""
        with open(demo_path, "rb") as f:
            data = f.read()

        assert len(data) == 480_520_436  # known file size

        async with await DemoSource.from_bytes(data) as demo:
            tables = demo.get_tables()
            assert len(tables) == 862
            assert "CCitadelPlayerPawn" in tables

            h = await demo.add_query(
                "SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10"
            )
            demo.start()
            total = await collect_rows(h)

        assert total == 10

    @pytest.mark.asyncio
    async def test_from_bytes_schema_matches_open(self, demo_path):
        """from_bytes and open should produce identical schemas."""
        with open(demo_path, "rb") as f:
            data = f.read()

        demo_open = await DemoSource.open(demo_path)
        demo_bytes = await DemoSource.from_bytes(data)

        assert set(demo_open.get_tables()) == set(demo_bytes.get_tables())

        for table in ["CCitadelPlayerPawn", "CCitadelPlayerController"]:
            s1 = demo_open.get_schema(table)
            s2 = demo_bytes.get_schema(table)
            assert s1.equals(s2), f"Schema mismatch for {table}"


# ── Batch Size Tests ─────────────────────────────────────────────────


class TestBatchSizeE2E:
    """Test batch_size configuration in end-to-end scenarios."""

    @pytest.mark.asyncio
    async def test_small_batch_size(self, demo_path):
        """With batch_size=32, each batch should have <= 32 rows."""
        async with await DemoSource.open(demo_path, batch_size=32) as demo:
            h = await demo.add_query(
                "SELECT tick FROM CCitadelPlayerPawn LIMIT 100"
            )
            demo.start()
            batches = await collect_batches(h)

        total = sum(b.num_rows for b in batches)
        assert total == 100
        assert all(b.num_rows <= 32 for b in batches)
        # With batch_size=32 and 100 rows, expect at least 4 batches
        assert len(batches) >= 4

    @pytest.mark.asyncio
    async def test_large_batch_size(self, demo_path):
        """With batch_size=10000, batches are larger."""
        async with await DemoSource.open(demo_path, batch_size=10000) as demo:
            h = await demo.add_query(
                "SELECT tick FROM CCitadelPlayerPawn LIMIT 10000"
            )
            demo.start()
            batches = await collect_batches(h)

        total = sum(b.num_rows for b in batches)
        assert total == 10000
        assert all(b.num_rows <= 10000 for b in batches)


# ── Schema Inspection Tests ──────────────────────────────────────────


class TestSchemaInspectionE2E:
    """Validate schema inspection against real demo data."""

    @pytest.mark.asyncio
    async def test_table_count(self, demo_path):
        """Demo should have 862 entity tables."""
        demo = await DemoSource.open(demo_path)
        assert len(demo.get_tables()) == 862

    @pytest.mark.asyncio
    async def test_player_pawn_schema_fields(self, demo_path):
        """CCitadelPlayerPawn schema should have known fields."""
        demo = await DemoSource.open(demo_path)
        schema = demo.get_schema("CCitadelPlayerPawn")
        assert schema is not None

        field_names = schema.names
        assert len(field_names) == 322

        # Standard fields present on all entity tables
        assert "tick" in field_names
        assert "entity_index" in field_names

        # Game-specific fields
        assert "m_iHealth" in field_names
        assert "m_lifeState" in field_names
        assert "m_angEyeAngles__x" in field_names

    @pytest.mark.asyncio
    async def test_player_pawn_field_types(self, demo_path):
        """CCitadelPlayerPawn fields should have expected Arrow types."""
        demo = await DemoSource.open(demo_path)
        schema = demo.get_schema("CCitadelPlayerPawn")

        assert schema.field("tick").type == pa.int32()
        assert schema.field("entity_index").type == pa.int32()
        assert schema.field("m_iHealth").type == pa.int64()
        assert schema.field("m_angEyeAngles__x").type == pa.float32()

    @pytest.mark.asyncio
    async def test_all_schemas_have_tick_and_entity_index(self, demo_path):
        """Every entity table schema should have tick and entity_index."""
        demo = await DemoSource.open(demo_path)
        for table_name in demo.get_tables():
            schema = demo.get_schema(table_name)
            assert "tick" in schema.names, f"{table_name} missing tick"
            assert "entity_index" in schema.names, f"{table_name} missing entity_index"


# ── Concurrent Multi-Query Tests ─────────────────────────────────────


class TestConcurrentQueries:
    """Test multiple concurrent queries with concurrent draining."""

    @pytest.mark.asyncio
    async def test_three_tables_concurrent(self, demo_path):
        """Three different tables queried and drained concurrently."""
        async with await DemoSource.open(demo_path) as demo:
            h1 = await demo.add_query(
                "SELECT tick FROM CCitadelPlayerPawn LIMIT 1000"
            )
            h2 = await demo.add_query(
                "SELECT tick FROM CCitadelPlayerController LIMIT 500"
            )
            h3 = await demo.add_query(
                "SELECT tick FROM CCitadelTeam LIMIT 200"
            )
            demo.start()

            r1, r2, r3 = await asyncio.gather(
                collect_rows(h1), collect_rows(h2), collect_rows(h3)
            )

        assert r1 == 1000
        assert r2 == 500
        assert r3 == 200

    @pytest.mark.asyncio
    async def test_same_table_different_filters(self, demo_path):
        """Two queries on the same table with different filters."""
        async with await DemoSource.open(demo_path) as demo:
            h1 = await demo.add_query(
                'SELECT tick, entity_index, "m_iHealth" '
                "FROM CCitadelPlayerPawn WHERE entity_index = 65 LIMIT 10"
            )
            h2 = await demo.add_query(
                'SELECT tick, entity_index, "m_iHealth" '
                "FROM CCitadelPlayerPawn WHERE entity_index = 72 LIMIT 10"
            )
            demo.start()

            r1, r2 = await asyncio.gather(collect_dicts(h1), collect_dicts(h2))

        assert len(r1) == 10
        assert len(r2) == 10
        assert all(r["entity_index"] == 65 for r in r1)
        assert all(r["entity_index"] == 72 for r in r2)


# ── Logging Bridge Test ──────────────────────────────────────────────

