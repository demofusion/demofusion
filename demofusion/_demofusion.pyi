"""Python bindings for demofusion (compiled Rust module)."""

from typing import AsyncIterator, Dict, Optional
import pyarrow as pa


# Exception Types


class DemofusionError(Exception):
    """Base exception for all demofusion errors."""

    pass


class DemofusionIOError(DemofusionError):
    """I/O error (file/network)."""

    pass


class DemofusionSchemaError(DemofusionError):
    """Schema discovery or parsing error."""

    pass


class DemofusionArrowError(DemofusionError):
    """Arrow format error."""

    pass


class DemofusionDataFusionError(DemofusionError):
    """SQL query execution error."""

    pass


class DemofusionHasteError(DemofusionError):
    """Demo format parsing error."""

    pass


class DemofusionSessionError(DemofusionError):
    """Session lifecycle error."""

    pass


# Sources


class DemoSource:
    """Source for static .dem demo files."""

    @staticmethod
    async def open(path: str) -> DemoSource:
        """Load demo from file path.
        
        Args:
            path: Path to .dem file
            
        Returns:
            DemoSource instance
            
        Raises:
            DemofusionIOError: If file cannot be read
            DemofusionSchemaError: If file is invalid
        """
        ...

    @staticmethod
    def from_bytes(data: bytes) -> DemoSource:
        """Load demo from bytes.
        
        Args:
            data: Raw demo file bytes
            
        Returns:
            DemoSource instance
        """
        ...

    async def into_session(
        self,
        *,
        batch_size: Optional[int] = None,
        reject_pipeline_breakers: Optional[bool] = None,
    ) -> StreamingSession:
        """Initialize session and discover schemas.
        
        Args:
            batch_size: Number of rows per RecordBatch (default: 1024).
            reject_pipeline_breakers: Whether to reject queries requiring
                unbounded memory (ORDER BY, GROUP BY). Default: False.
            
        Returns:
            StreamingSession ready to use as async context manager
            
        Raises:
            DemofusionSchemaError: If schemas cannot be discovered
        """
        ...


class GotvSource:
    """Source for live GOTV broadcasts (requires gotv feature)."""

    @staticmethod
    async def connect(url: str) -> GotvSource:
        """Connect to GOTV broadcast.
        
        Args:
            url: GOTV broadcast URL
            
        Returns:
            GotvSource instance
            
        Raises:
            DemofusionIOError: If connection fails
        """
        ...

    async def into_session(
        self,
        *,
        batch_size: Optional[int] = None,
        reject_pipeline_breakers: Optional[bool] = None,
    ) -> StreamingSession:
        """Initialize session and discover schemas.
        
        Args:
            batch_size: Number of rows per RecordBatch (default: 1024).
            reject_pipeline_breakers: Whether to reject queries requiring
                unbounded memory (ORDER BY, GROUP BY). Default: False.
            
        Returns:
            StreamingSession ready to use as async context manager
            
        Raises:
            DemofusionSchemaError: If schemas cannot be discovered
        """
        ...


# Session and Query


class StreamingSession:
    """Manages SQL queries over demo/GOTV data.
    
    Use as async context manager:
        async with await source.into_session() as session:
            ...
    """

    @property
    def schemas(self) -> Dict[str, pa.Schema]:
        """Get all available table schemas.
        
        Returns:
            Dictionary mapping table name -> PyArrow schema
        """
        ...

    def get_tables(self) -> list[str]:
        """List all available table names.
        
        Returns:
            List of entity/event table names
        """
        ...

    def get_schema(self, table_name: str) -> Optional[pa.Schema]:
        """Get schema for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            PyArrow schema or None if table not found
        """
        ...

    async def add_query(self, sql: str) -> QueryHandle:
        """Register a SQL query.
        
        Must be called before start().
        
        Args:
            sql: SQL query string
            
        Returns:
            QueryHandle for streaming results
            
        Raises:
            DemofusionDataFusionError: If SQL is invalid
            DemofusionSessionError: If called after start()
        """
        ...

    def start(self) -> None:
        """Begin streaming parser.
        
        Applies any configuration (batch_size, reject_pipeline_breakers) that
        was passed to into_session(), then spawns a background task to parse
        the demo and distribute data. Synchronous - returns immediately.
        
        Raises:
            DemofusionSessionError: If called twice
        """
        ...

    async def __aenter__(self) -> StreamingSession:
        """Enter async context."""
        ...

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context (cleanup resources)."""
        ...


class QueryHandle:
    """Async iterator for streaming query results."""

    @property
    def schema(self) -> pa.Schema:
        """Get PyArrow schema for query results.
        
        Returns:
            PyArrow schema matching the query's SELECT columns
        """
        ...

    async def __aiter__(self) -> AsyncIterator[pa.RecordBatch]:
        """Iterate over RecordBatch objects.
        
        Yields:
            pyarrow.RecordBatch objects with query results
            
        Raises:
            DemofusionSessionError: If async iteration fails
        """
        ...

    async def __anext__(self) -> pa.RecordBatch:
        """Get next RecordBatch.
        
        Returns:
            pyarrow.RecordBatch
            
        Raises:
            StopAsyncIteration: When no more batches
            DemofusionSessionError: On error
        """
        ...
