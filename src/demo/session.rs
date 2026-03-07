//! High-level streaming session for demo files.
//!
//! `DemoFileSession` provides a simplified API for querying static demo files,
//! using the same streaming architecture as `SpectateSession` for GOTV broadcasts.

use std::collections::HashMap;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::datafusion::distributor_channels::{self, DistributionReceiver, DistributionSender};
use crate::datafusion::pipeline_analysis::analyze_pipeline;
use crate::datafusion::streaming_stats::StreamingStats;
use crate::datafusion::table_providers::EntityTableProvider;
use crate::schema::EntitySchema;
use crate::sql::extract_table_names;
use crate::visitor::{BatchingDemoVisitor, BatchingEntityDispatcher, discover_schemas_from_demo};

type BatchReceiver = DistributionReceiver<RecordBatch>;
type BatchSender = DistributionSender<RecordBatch>;
type ReceiverSlot = Arc<Mutex<Option<BatchReceiver>>>;

const DEFAULT_BATCH_SIZE: usize = 1024;

fn streaming_session_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_coalesce_batches(false);
    SessionContext::new_with_config(config)
}

/// Error type for demo file operations.
#[derive(Debug, thiserror::Error)]
pub enum DemoError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Schema discovery failed: {0}")]
    Schema(String),

    #[error("SQL error: {0}")]
    Sql(String),

    #[error("Unknown entity type: {0}")]
    UnknownEntity(String),

    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("Pipeline contains blocking operators: {0}")]
    PipelineBreaker(String),

    #[error("Parser error: {0}")]
    Parser(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// A pending query registration, created by `add_query()`.
struct PendingQuery {
    physical_plan: Arc<dyn ExecutionPlan>,
    slots: HashMap<Arc<str>, ReceiverSlot>,
    context: SessionContext,
    result_tx: mpsc::UnboundedSender<std::result::Result<RecordBatch, DemoError>>,
}

/// Handle to a streaming query result.
///
/// Created by [`DemoFileSession::add_query`], becomes live after [`DemoFileSession::start`].
/// Implements [`Stream`] to yield `RecordBatch` results.
pub struct QueryHandle {
    receiver: mpsc::UnboundedReceiver<std::result::Result<RecordBatch, DemoError>>,
    schema: SchemaRef,
}

impl QueryHandle {
    /// Get the Arrow schema for this query's results.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Stream for QueryHandle {
    type Item = std::result::Result<RecordBatch, DemoError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Result of starting a demo file session.
pub struct DemoResult {
    /// Handle to the background parser task.
    /// Await this after all query streams complete to check for parser errors.
    pub parser_handle: JoinHandle<std::result::Result<(), DemoError>>,
    /// Statistics for monitoring streaming data.
    pub stats: Option<Arc<StreamingStats>>,
}

/// High-level session for streaming SQL queries over a demo file.
///
/// See module documentation for usage examples.
pub struct DemoFileSession {
    demo_bytes: Bytes,
    schemas: HashMap<Arc<str>, EntitySchema>,
    pending_queries: Vec<PendingQuery>,
    batch_size: usize,
    reject_pipeline_breakers: bool,
}

impl DemoFileSession {
    /// Open a demo file and discover schemas.
    ///
    /// This reads the file and parses the header to discover entity schemas.
    /// The file is kept in memory for streaming.
    pub async fn open(path: impl AsRef<Path>) -> std::result::Result<Self, DemoError> {
        let bytes = tokio::fs::read(path).await?;
        Self::from_bytes(bytes).await
    }

    /// Create a session from demo file bytes already in memory.
    pub async fn from_bytes(bytes: impl Into<Bytes>) -> std::result::Result<Self, DemoError> {
        let demo_bytes: Bytes = bytes.into();

        let schema_vec = discover_schemas_from_demo(&demo_bytes)
            .await
            .map_err(|e| DemoError::Schema(e.to_string()))?;

        let schemas: HashMap<Arc<str>, EntitySchema> = schema_vec
            .into_iter()
            .map(|s| (Arc::clone(&s.serializer_name), s))
            .collect();

        Ok(Self {
            demo_bytes,
            schemas,
            pending_queries: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            reject_pipeline_breakers: false,
        })
    }

    /// Set the batch size for streaming results.
    ///
    /// Larger batches are more efficient for full-file parsing.
    /// Default is 1024 rows.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Reject queries that contain pipeline breakers.
    ///
    /// Pipeline breakers (ORDER BY, aggregations, etc.) buffer all input before
    /// producing output. For demo files this is less of a concern than live streams,
    /// but can still cause high memory usage.
    pub fn with_reject_pipeline_breakers(mut self, reject: bool) -> Self {
        self.reject_pipeline_breakers = reject;
        self
    }

    /// Get all discovered entity schemas.
    pub fn schemas(&self) -> impl Iterator<Item = &EntitySchema> {
        self.schemas.values()
    }

    /// Get schema by entity name.
    pub fn schema(&self, name: &str) -> Option<&EntitySchema> {
        self.schemas.get(name)
    }

    /// Get all available entity type names.
    pub fn entity_names(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| &**s).collect()
    }

    /// Register a SQL query and return a handle for streaming results.
    ///
    /// The query is validated immediately (SQL syntax and table existence),
    /// but data won't flow until [`start`](Self::start) is called.
    pub async fn add_query(&mut self, sql: &str) -> std::result::Result<QueryHandle, DemoError> {
        let table_names = extract_table_names(sql).map_err(|e| DemoError::Sql(e.to_string()))?;

        let entity_types: Vec<Arc<str>> = table_names
            .iter()
            .filter(|name| self.schemas.contains_key(name.as_str()))
            .map(|s| Arc::<str>::from(s.as_str()))
            .collect();

        let unknown_tables: Vec<&String> = table_names
            .iter()
            .filter(|name| !self.schemas.contains_key(name.as_str()))
            .collect();

        if !unknown_tables.is_empty() {
            return Err(DemoError::UnknownEntity(
                unknown_tables
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }

        if entity_types.is_empty() {
            return Err(DemoError::Sql(
                "Query must reference at least one entity table".to_string(),
            ));
        }

        let ctx = streaming_session_context();
        let mut slots: HashMap<Arc<str>, ReceiverSlot> = HashMap::new();

        for entity_type in &entity_types {
            let schema = &self.schemas[entity_type];
            let slot: ReceiverSlot = Arc::new(Mutex::new(None));
            slots.insert(Arc::clone(entity_type), slot.clone());

            let provider = EntityTableProvider::new(
                schema.arrow_schema.clone(),
                Arc::clone(entity_type),
                slot,
            );
            ctx.register_table(&**entity_type, Arc::new(provider))
                .map_err(|e| DemoError::DataFusion(e.to_string()))?;
        }

        let logical_plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| DemoError::Sql(e.to_string()))?;

        let output_schema: SchemaRef = Arc::new(logical_plan.schema().as_arrow().clone());

        let physical_plan = ctx
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| DemoError::DataFusion(e.to_string()))?;

        let analysis = analyze_pipeline(&physical_plan);
        if analysis.has_pipeline_breakers() && self.reject_pipeline_breakers {
            return Err(DemoError::PipelineBreaker(analysis.report()));
        }

        let (result_tx, result_rx) = mpsc::unbounded_channel();

        self.pending_queries.push(PendingQuery {
            physical_plan,
            slots,
            context: ctx,
            result_tx,
        });

        Ok(QueryHandle {
            receiver: result_rx,
            schema: output_schema,
        })
    }

    /// Start streaming data to all registered queries.
    ///
    /// This consumes the session and:
    /// 1. Creates distribution channels for each entity type
    /// 2. Spawns the parser task
    /// 3. Spawns a query executor task for each query
    ///
    /// After this call, `QueryHandle`s will start yielding data.
    pub async fn start(self) -> std::result::Result<DemoResult, DemoError> {
        if self.pending_queries.is_empty() {
            return Err(DemoError::Sql("No queries registered".to_string()));
        }

        let Self {
            demo_bytes,
            schemas,
            pending_queries,
            batch_size,
            reject_pipeline_breakers: _,
        } = self;

        let stats = StreamingStats::new();

        let mut used_entity_slots: HashMap<Arc<str>, Vec<ReceiverSlot>> = HashMap::new();
        let mut plans = Vec::new();
        let mut contexts = Vec::new();
        let mut result_senders = Vec::new();

        for pending in pending_queries {
            for (entity_type, slot) in &pending.slots {
                used_entity_slots
                    .entry(Arc::clone(entity_type))
                    .or_default()
                    .push(slot.clone());
            }

            plans.push(pending.physical_plan);
            contexts.push(pending.context);
            result_senders.push(pending.result_tx);
        }

        let total_channels: usize = used_entity_slots.values().map(|v| v.len()).sum();
        let (all_senders, all_receivers) =
            distributor_channels::channels::<RecordBatch>(total_channels);
        let mut sender_iter = all_senders.into_iter();
        let mut receiver_idx = 0;

        let mut entity_dispatcher_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>> =
            HashMap::new();
        let mut entity_slot_receiver_indices: Vec<(ReceiverSlot, usize)> = Vec::new();

        for (entity_type, slots) in &used_entity_slots {
            let schema = &schemas[entity_type.as_ref()];
            for slot in slots {
                let sender = sender_iter.next().expect("channel count mismatch");
                entity_dispatcher_senders
                    .entry(schema.serializer_hash)
                    .or_default()
                    .push((sender, schema.clone()));
                entity_slot_receiver_indices.push((slot.clone(), receiver_idx));
                receiver_idx += 1;
            }
        }

        let parser_schemas: Vec<(u64, EntitySchema)> = schemas
            .iter()
            .filter(|(name, _)| used_entity_slots.contains_key(name.as_ref()))
            .map(|(_, schema)| (schema.serializer_hash, schema.clone()))
            .collect();

        let stats_clone = Arc::clone(&stats);
        let parser_handle: JoinHandle<std::result::Result<(), DemoError>> =
            tokio::spawn(run_parser(
                demo_bytes,
                parser_schemas,
                entity_dispatcher_senders,
                batch_size,
                Some(stats_clone),
            ));

        let mut receivers_by_idx: HashMap<usize, BatchReceiver> =
            all_receivers.into_iter().enumerate().collect();

        for (slot, idx) in entity_slot_receiver_indices {
            let rx = receivers_by_idx
                .remove(&idx)
                .expect("receiver index mismatch");
            let old = slot.lock().replace(rx);
            if old.is_some() {
                return Err(DemoError::Internal("Slot already set".to_string()));
            }
        }

        for ((plan, ctx), result_tx) in plans
            .into_iter()
            .zip(contexts.into_iter())
            .zip(result_senders.into_iter())
        {
            let stream = execute_stream(plan, ctx.task_ctx())
                .map_err(|e| DemoError::DataFusion(e.to_string()))?;

            tokio::spawn(forward_stream_to_channel(stream, result_tx));
        }

        Ok(DemoResult {
            parser_handle,
            stats: Some(stats),
        })
    }
}

async fn forward_stream_to_channel(
    mut stream: datafusion::physical_plan::SendableRecordBatchStream,
    tx: mpsc::UnboundedSender<std::result::Result<RecordBatch, DemoError>>,
) {
    while let Some(result) = stream.next().await {
        let mapped = result.map_err(|e| DemoError::DataFusion(e.to_string()));
        if tx.send(mapped).is_err() {
            break;
        }
    }
}

async fn run_parser(
    demo_bytes: Bytes,
    _schemas: Vec<(u64, EntitySchema)>,
    entity_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    batch_size: usize,
    stats: Option<Arc<StreamingStats>>,
) -> std::result::Result<(), DemoError> {
    use crate::haste::core::packet_channel_demo_stream::PacketChannelDemoStream;
    use crate::haste::core::packet_source::PacketSource;
    use crate::haste::parser::AsyncStreamingParser;

    /// A packet source that yields a single packet containing all demo bytes.
    struct SinglePacketSource {
        bytes: Option<Bytes>,
    }

    impl PacketSource for SinglePacketSource {
        async fn recv(&mut self) -> Option<Bytes> {
            self.bytes.take()
        }
    }

    let packet_source = SinglePacketSource {
        bytes: Some(demo_bytes),
    };
    let demo_stream = PacketChannelDemoStream::new(packet_source);

    let entity_dispatcher =
        BatchingEntityDispatcher::new_with_stats(entity_senders, batch_size, stats);

    let visitor = BatchingDemoVisitor::new(entity_dispatcher, None, Default::default());

    let mut parser = AsyncStreamingParser::from_stream_with_visitor(demo_stream, visitor)
        .map_err(|e| DemoError::Parser(e.to_string()))?;

    parser
        .run_to_end()
        .await
        .map_err(|e| DemoError::Parser(e.to_string()))?;

    // Flush remaining data
    let mut visitor = parser.into_visitor();
    visitor
        .flush_all()
        .await
        .map_err(|e| DemoError::Parser(e.to_string()))?;

    Ok(())
}
