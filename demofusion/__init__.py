"""demofusion: SQL queries over Valve Source 2 demo files via Apache DataFusion."""

from __future__ import annotations

from typing import Optional

from demofusion._demofusion import (
    DemoSource as _RawDemoSource,
    StreamingSession as _RawStreamingSession,
    QueryHandle,
    DemofusionError,
    DemofusionIOError,
    DemofusionSchemaError,
    DemofusionArrowError,
    DemofusionDataFusionError,
    DemofusionHasteError,
    DemofusionSessionError,
)

# Optional GOTV support
try:
    from demofusion._demofusion import GotvSource as _RawGotvSource
except ImportError:
    _RawGotvSource = None


class DemoSource:
    """High-level interface for querying Valve Source 2 demo files.

    Wraps the low-level RawDemoSource and StreamingSession into a single
    object. Use ``await DemoSource.open(...)`` to create.

    Example::

        async with await DemoSource.open("match.dem", batch_size=256) as demo:
            handle = await demo.add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn LIMIT 10")
            demo.start()
            async for batch in handle:
                print(batch)
    """

    def __init__(self, session: _RawStreamingSession) -> None:
        self._session = session

    @classmethod
    async def open(
        cls,
        path: str,
        *,
        batch_size: Optional[int] = None,
        reject_pipeline_breakers: Optional[bool] = None,
    ) -> DemoSource:
        """Open a demo file and prepare a session.

        Args:
            path: Path to a .dem file.
            batch_size: Number of rows per RecordBatch.
            reject_pipeline_breakers: Whether to reject queries requiring
                unbounded memory (ORDER BY, GROUP BY without LIMIT).

        Returns:
            A ready-to-query DemoSource instance.

        Raises:
            DemofusionIOError: If the file cannot be read.
            DemofusionSchemaError: If schema discovery fails.
        """
        raw = await _RawDemoSource.open(path)
        session = await raw.into_session(
            batch_size=batch_size,
            reject_pipeline_breakers=reject_pipeline_breakers,
        )
        return cls(session)

    @classmethod
    async def from_bytes(
        cls,
        data: bytes,
        *,
        batch_size: Optional[int] = None,
        reject_pipeline_breakers: Optional[bool] = None,
    ) -> DemoSource:
        """Create from raw demo bytes and prepare a session.

        Args:
            data: Raw demo file bytes.
            batch_size: Number of rows per RecordBatch.
            reject_pipeline_breakers: Whether to reject queries requiring
                unbounded memory (ORDER BY, GROUP BY without LIMIT).

        Returns:
            A ready-to-query DemoSource instance.

        Raises:
            DemofusionSchemaError: If schema discovery fails.
        """
        raw = _RawDemoSource.from_bytes(data)
        session = await raw.into_session(
            batch_size=batch_size,
            reject_pipeline_breakers=reject_pipeline_breakers,
        )
        return cls(session)

    @property
    def schemas(self):
        """Get all available table schemas as ``dict[str, pa.Schema]``."""
        return self._session.schemas

    def get_tables(self):
        """List all available table names."""
        return self._session.get_tables()

    def get_schema(self, table_name: str):
        """Get PyArrow schema for a specific table, or None."""
        return self._session.get_schema(table_name)

    async def add_query(self, sql: str) -> QueryHandle:
        """Register a SQL query. Must be called before ``start()``.

        Returns:
            A QueryHandle for async iteration over results.
        """
        return await self._session.add_query(sql)

    def start(self) -> None:
        """Begin the streaming parser.

        Synchronous — spawns a background task and returns immediately.
        Must be called after all queries are registered.
        """
        self._session.start()

    async def __aenter__(self) -> DemoSource:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        # _RawStreamingSession.__aexit__ is synchronous on the Rust side
        result = self._session.__aexit__(None, None, None)
        if result is not None:
            await result


class GotvSource:
    """High-level interface for querying live GOTV broadcasts.

    Wraps the low-level RawGotvSource and StreamingSession into a single
    object. Use ``await GotvSource.connect(...)`` to create.

    Requires the ``gotv`` feature to be enabled at build time.
    """

    def __init__(self, session: _RawStreamingSession) -> None:
        self._session = session

    @classmethod
    async def connect(
        cls,
        url: str,
        *,
        batch_size: Optional[int] = None,
        reject_pipeline_breakers: Optional[bool] = None,
    ) -> GotvSource:
        """Connect to a GOTV broadcast and prepare a session.

        Args:
            url: GOTV broadcast URL.
            batch_size: Number of rows per RecordBatch.
            reject_pipeline_breakers: Whether to reject queries requiring
                unbounded memory.

        Returns:
            A ready-to-query GotvSource instance.

        Raises:
            DemofusionIOError: If connection fails.
            DemofusionSchemaError: If schema discovery fails.
            RuntimeError: If gotv feature is not enabled.
        """
        if _RawGotvSource is None:
            raise RuntimeError("GotvSource requires the 'gotv' feature to be enabled")
        raw = await _RawGotvSource.connect(url)
        session = await raw.into_session(
            batch_size=batch_size,
            reject_pipeline_breakers=reject_pipeline_breakers,
        )
        return cls(session)

    @property
    def schemas(self):
        """Get all available table schemas as ``dict[str, pa.Schema]``."""
        return self._session.schemas

    def get_tables(self):
        """List all available table names."""
        return self._session.get_tables()

    def get_schema(self, table_name: str):
        """Get PyArrow schema for a specific table, or None."""
        return self._session.get_schema(table_name)

    async def add_query(self, sql: str) -> QueryHandle:
        """Register a SQL query. Must be called before ``start()``."""
        return await self._session.add_query(sql)

    def start(self) -> None:
        """Begin the streaming parser."""
        self._session.start()

    async def __aenter__(self) -> GotvSource:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        result = self._session.__aexit__(None, None, None)
        if result is not None:
            await result


__version__ = "0.1.0"
__all__ = [
    "DemoSource",
    "GotvSource",
    "QueryHandle",
    "DemofusionError",
    "DemofusionIOError",
    "DemofusionSchemaError",
    "DemofusionArrowError",
    "DemofusionDataFusionError",
    "DemofusionHasteError",
    "DemofusionSessionError",
]
