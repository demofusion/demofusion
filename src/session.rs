//! Unified streaming session for SQL queries over demo data.
//!
//! `StreamingSession` provides a common API for querying both static demo files
//! and live GOTV broadcasts using SQL. Queries are registered with `add_query()`,
//! which returns a `QueryHandle` that implements `Stream<Item = Result<RecordBatch>>`.
//!
//! # Usage Pattern
//!
//! All sources implement [`IntoStreamingSession`], providing a consistent workflow:
//!
//! 1. Create a source (`DemoSource` or `GotvSource`)
//! 2. Call `into_session()` to discover schemas and get a session
//! 3. Add queries with `add_query()`
//! 4. Call `start()` to begin streaming
//!
//! # Example: Demo File
//!
//! ```ignore
//! use demofusion::{DemoSource, IntoStreamingSession};
//! use futures::StreamExt;
//!
//! let source = DemoSource::open("match.dem").await?;
//! let mut session = source.into_session().await?;
//!
//! println!("Found {} entities", session.entity_names().len());
//!
//! let mut pawns = session.add_query(
//!     "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn"
//! ).await?;
//!
//! let _result = session.start()?;
//!
//! while let Some(batch) = pawns.next().await {
//!     println!("Got {} rows", batch?.num_rows());
//! }
//! ```
//!
//! # Example: GOTV Broadcast
//!
//! ```ignore
//! use demofusion::{GotvSource, IntoStreamingSession};
//! use demofusion::gotv::GotvSource;
//! use tokio_util::sync::CancellationToken;
//! use futures::StreamExt;
//!
//! let source = GotvSource::connect("http://dist1-ord1.steamcontent.com/tv/12345").await?;
//! let session = source.into_session().await?;
//!
//! let cancel = CancellationToken::new();
//! let mut session = session.with_cancel_token(cancel.clone());
//!
//! let mut pawns = session.add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn").await?;
//! let _result = session.start()?;
//!
//! while let Some(batch) = pawns.next().await {
//!     println!("Got {} rows", batch?.num_rows());
//! }
//! ```

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::{Stream, StreamExt};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
#[cfg(feature = "gotv")]
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::datafusion::distributor_channels::{self, DistributionSender};
use crate::datafusion::pipeline_analysis::analyze_pipeline;
use crate::datafusion::streaming_stats::StreamingStats;
use crate::datafusion::table_providers::{
    BatchReceiver, EntityTableProvider, EventTableProvider, ReceiverSlot,
};
use crate::events::{EventType, event_schema};
use crate::haste::core::packet_source::PacketSource;
use crate::schema::EntitySchema;

type BatchSender = DistributionSender<RecordBatch>;
type ParserResult = (JoinHandle<Result<(), SessionError>>, Arc<StreamingStats>);

pub(crate) type Schemas = HashMap<Arc<str>, EntitySchema>;

const DEFAULT_DEMO_BATCH_SIZE: usize = 1024;
#[cfg(feature = "gotv")]
const DEFAULT_LIVE_BATCH_SIZE: usize = 128;

#[async_trait]
pub trait IntoStreamingSession {
    async fn into_session(self) -> Result<StreamingSession, SessionError>;
}

fn streaming_session_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_coalesce_batches(false);
    SessionContext::new_with_config(config)
}

/// Create and register all entity + event table providers in the given context.
///
/// Entity providers come from the discovered schemas (one per entity type).
/// Event providers come from `EventType::all()` (all 60+ event types registered
/// upfront so they appear in `get_tables()` and are queryable).
///
/// Returns concrete-typed maps for later draining at `start()` time.
fn register_all_providers(
    ctx: &SessionContext,
    schemas: &Schemas,
) -> (
    HashMap<Arc<str>, Arc<EntityTableProvider>>,
    HashMap<Arc<str>, Arc<EventTableProvider>>,
) {
    let mut entity_providers = HashMap::with_capacity(schemas.len());
    for (name, schema) in schemas {
        let provider = Arc::new(EntityTableProvider::new(
            schema.arrow_schema.clone(),
            Arc::clone(name),
        ));
        ctx.register_table(&**name, Arc::clone(&provider) as _)
            .expect("entity table registration should not fail");
        entity_providers.insert(Arc::clone(name), provider);
    }

    let all_events = EventType::all();
    let mut event_providers = HashMap::with_capacity(all_events.len());
    for &event_type in all_events {
        let table_name = event_type.table_name();
        let arrow_schema = event_schema(table_name)
            .unwrap_or_else(|| panic!("EventType::{:?} has no schema", event_type));
        let provider = Arc::new(EventTableProvider::new(event_type, arrow_schema));
        ctx.register_table(table_name, Arc::clone(&provider) as _)
            .expect("event table registration should not fail");
        event_providers.insert(Arc::from(table_name), provider);
    }

    (entity_providers, event_providers)
}

#[derive(Error, Debug)]
pub enum SessionError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Schema discovery failed: {0}")]
    Schema(String),

    #[error("SQL syntax error: {0}")]
    Sql(String),

    #[error("Unknown table: {0}")]
    UnknownTable(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Parser error: {0}")]
    Parser(String),

    #[error("Session already started - cannot add more queries after first poll")]
    AlreadyStarted,

    #[error("No queries registered")]
    NoQueries,

    #[error("Pipeline breaker detected: {0}")]
    PipelineBreaker(String),

    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<crate::error::Source2DfError> for SessionError {
    fn from(e: crate::error::Source2DfError) -> Self {
        SessionError::Query(e.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for SessionError {
    fn from(e: datafusion::error::DataFusionError) -> Self {
        SessionError::DataFusion(e.to_string())
    }
}

struct PendingQuery {
    physical_plan: Arc<dyn ExecutionPlan>,
    result_tx: mpsc::UnboundedSender<Result<RecordBatch, SessionError>>,
}

pub struct SessionResult {
    pub parser_handle: JoinHandle<Result<(), SessionError>>,
    pub stats: Arc<StreamingStats>,
}

struct SessionState {
    started: AtomicBool,
}

impl SessionState {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
        }
    }

    fn mark_started(&self) -> bool {
        self.started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }
}

pub struct QueryHandle {
    receiver: mpsc::UnboundedReceiver<Result<RecordBatch, SessionError>>,
    schema: SchemaRef,
}

impl QueryHandle {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Stream for QueryHandle {
    type Item = Result<RecordBatch, SessionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

enum PacketSourceKind {
    DemoBytes(Bytes),
    #[cfg(feature = "gotv")]
    GotvClient {
        client: Box<crate::gotv::BroadcastClient>,
        start_packet: Bytes,
    },
}

fn execute_query_plans(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    ctx: &SessionContext,
    result_senders: Vec<mpsc::UnboundedSender<Result<RecordBatch, SessionError>>>,
) -> Result<(), SessionError> {
    for (plan, result_tx) in plans.into_iter().zip(result_senders.into_iter()) {
        let stream = execute_stream(plan, ctx.task_ctx())
            .map_err(|e| SessionError::DataFusion(e.to_string()))?;

        tokio::spawn(forward_stream_to_channel(stream, result_tx));
    }
    Ok(())
}

pub struct StreamingSession {
    source: Option<PacketSourceKind>,
    ctx: SessionContext,
    entity_providers: HashMap<Arc<str>, Arc<EntityTableProvider>>,
    event_providers: HashMap<Arc<str>, Arc<EventTableProvider>>,
    entity_schemas: HashMap<Arc<str>, EntitySchema>,
    pending_queries: Vec<PendingQuery>,
    batch_size: usize,
    #[cfg(feature = "gotv")]
    cancel_token: Option<CancellationToken>,
    reject_pipeline_breakers: bool,
    state: Arc<SessionState>,
}

impl StreamingSession {
    pub(crate) fn from_demo_bytes_internal(demo_bytes: Bytes, schemas: Schemas) -> Self {
        let ctx = streaming_session_context();
        let (entity_providers, event_providers) = register_all_providers(&ctx, &schemas);

        Self {
            source: Some(PacketSourceKind::DemoBytes(demo_bytes)),
            ctx,
            entity_providers,
            event_providers,
            entity_schemas: schemas,
            pending_queries: Vec::new(),
            batch_size: DEFAULT_DEMO_BATCH_SIZE,
            #[cfg(feature = "gotv")]
            cancel_token: None,
            reject_pipeline_breakers: false,
            state: Arc::new(SessionState::new()),
        }
    }

    #[cfg(feature = "gotv")]
    pub(crate) fn from_gotv_internal(
        client: crate::gotv::BroadcastClient,
        schemas: Schemas,
        start_packet: Bytes,
    ) -> Self {
        let ctx = streaming_session_context();
        let (entity_providers, event_providers) = register_all_providers(&ctx, &schemas);

        Self {
            source: Some(PacketSourceKind::GotvClient {
                client: Box::new(client),
                start_packet,
            }),
            ctx,
            entity_providers,
            event_providers,
            entity_schemas: schemas,
            pending_queries: Vec::new(),
            batch_size: DEFAULT_LIVE_BATCH_SIZE,
            cancel_token: None,
            reject_pipeline_breakers: false,
            state: Arc::new(SessionState::new()),
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[cfg(feature = "gotv")]
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = Some(token);
        self
    }

    pub fn with_reject_pipeline_breakers(mut self, reject: bool) -> Self {
        self.reject_pipeline_breakers = reject;
        self
    }

    /// Iterate over all entity schemas (excludes event schemas).
    pub fn schemas(&self) -> impl Iterator<Item = &EntitySchema> {
        self.entity_schemas.values()
    }

    /// Look up an entity schema by table name.
    pub fn schema(&self, name: &str) -> Option<&EntitySchema> {
        self.entity_schemas.get(name)
    }

    /// Return all entity table names.
    pub fn entity_names(&self) -> Vec<&str> {
        self.entity_schemas.keys().map(|s| &**s).collect()
    }

    /// Return all event table names.
    pub fn event_names(&self) -> Vec<&str> {
        self.event_providers.keys().map(|s| &**s).collect()
    }

    /// Return all table names (entity + event).
    pub fn all_table_names(&self) -> Vec<&str> {
        self.entity_schemas
            .keys()
            .chain(self.event_providers.keys())
            .map(|s| &**s)
            .collect()
    }

    /// Look up an Arrow schema for any table (entity or event).
    pub fn get_table_schema(&self, name: &str) -> Option<SchemaRef> {
        // Check entity schemas first
        if let Some(entity_schema) = self.entity_schemas.get(name) {
            return Some(entity_schema.arrow_schema.clone());
        }
        // Then check event providers
        if let Some(event_provider) = self.event_providers.get(name) {
            return Some(event_provider.arrow_schema());
        }
        None
    }

    pub async fn add_query(&mut self, sql: &str) -> Result<QueryHandle, SessionError> {
        if self.state.is_started() {
            return Err(SessionError::AlreadyStarted);
        }

        // Use the shared SessionContext — all entity and event tables are
        // already registered. DataFusion will call scan() on referenced
        // providers, which creates ReceiverSlots automatically.
        let logical_plan = self
            .ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| SessionError::Sql(e.to_string()))?;

        let output_schema: SchemaRef = Arc::new(logical_plan.schema().as_arrow().clone());

        let physical_plan = self
            .ctx
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| SessionError::DataFusion(e.to_string()))?;

        let analysis = analyze_pipeline(&physical_plan);
        if analysis.has_pipeline_breakers() {
            if self.reject_pipeline_breakers {
                return Err(SessionError::PipelineBreaker(analysis.report()));
            }
            warn!(
                query = sql,
                breakers = ?analysis.breakers.iter().map(|b| &b.operator_name).collect::<Vec<_>>(),
                "Query contains pipeline breakers - streaming will block until source closes"
            );
        }

        // TODO: Use bounded channel with backpressure
        let (result_tx, result_rx) = mpsc::unbounded_channel();

        self.pending_queries.push(PendingQuery {
            physical_plan,
            result_tx,
        });

        Ok(QueryHandle {
            receiver: result_rx,
            schema: output_schema,
        })
    }

    pub fn start(&mut self) -> Result<SessionResult, SessionError> {
        if self.pending_queries.is_empty() {
            return Err(SessionError::NoQueries);
        }

        if !self.state.mark_started() {
            return Err(SessionError::AlreadyStarted);
        }

        let (parser_handle, stats) = self.start_internal()?;

        Ok(SessionResult {
            parser_handle,
            stats,
        })
    }

    fn start_internal(&mut self) -> Result<ParserResult, SessionError> {
        let stats = StreamingStats::new();

        // Separate plans and result senders from pending queries
        let pending: Vec<PendingQuery> = self.pending_queries.drain(..).collect();
        let mut plans = Vec::with_capacity(pending.len());
        let mut result_senders = Vec::with_capacity(pending.len());
        for pq in pending {
            plans.push(pq.physical_plan);
            result_senders.push(pq.result_tx);
        }

        // Drain pending slots from providers to discover which tables were
        // referenced by queries. scan() was called during add_query() planning,
        // which pushed ReceiverSlots into each provider's pending_slots.
        let mut entity_slots: HashMap<Arc<str>, Vec<ReceiverSlot>> = HashMap::new();
        for (name, provider) in &self.entity_providers {
            let slots = provider.drain_pending_slots();
            if !slots.is_empty() {
                entity_slots.insert(Arc::clone(name), slots);
            }
        }

        let mut event_slots: HashMap<EventType, Vec<ReceiverSlot>> = HashMap::new();
        for (_name, provider) in &self.event_providers {
            let slots = provider.drain_pending_slots();
            if !slots.is_empty() {
                event_slots.insert(provider.event_type(), slots);
            }
        }

        // Count total channels needed
        let entity_channel_count: usize = entity_slots.values().map(|v| v.len()).sum();
        let event_channel_count: usize = event_slots.values().map(|v| v.len()).sum();
        let total_channels = entity_channel_count + event_channel_count;

        // Create distribution channels with shared global gate for backpressure
        let (all_senders, all_receivers) =
            distributor_channels::channels::<RecordBatch>(total_channels);
        let mut sender_iter = all_senders.into_iter();
        let mut receiver_idx = 0;

        // Wire up entity channels
        let mut entity_dispatcher_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>> =
            HashMap::new();
        let mut slot_receiver_pairs: Vec<(ReceiverSlot, usize)> = Vec::new();

        for (entity_name, slots) in &entity_slots {
            let schema = &self.entity_schemas[entity_name.as_ref()];
            for slot in slots {
                let sender = sender_iter.next().expect("channel count mismatch");
                entity_dispatcher_senders
                    .entry(schema.serializer_hash)
                    .or_default()
                    .push((sender, schema.clone()));
                slot_receiver_pairs.push((slot.clone(), receiver_idx));
                receiver_idx += 1;
            }
        }

        // Wire up event channels
        let mut event_dispatcher_senders: HashMap<u32, Vec<(BatchSender, EventType)>> =
            HashMap::new();

        for (event_type, slots) in &event_slots {
            for slot in slots {
                let sender = sender_iter.next().expect("channel count mismatch");
                event_dispatcher_senders
                    .entry(event_type.message_id())
                    .or_default()
                    .push((sender, *event_type));
                slot_receiver_pairs.push((slot.clone(), receiver_idx));
                receiver_idx += 1;
            }
        }

        // Collect parser schemas (only entity types that were referenced)
        let parser_schemas: Vec<(u64, EntitySchema)> = self
            .entity_schemas
            .iter()
            .filter(|(name, _)| entity_slots.contains_key(name.as_ref()))
            .map(|(_, schema)| (schema.serializer_hash, schema.clone()))
            .collect();

        // Inject receivers into slots
        let mut receivers_by_idx: HashMap<usize, BatchReceiver> =
            all_receivers.into_iter().enumerate().collect();
        for (slot, idx) in slot_receiver_pairs {
            let rx = receivers_by_idx
                .remove(&idx)
                .expect("receiver index mismatch");
            slot.inject(rx).map_err(|_| {
                SessionError::Internal("Slot already filled during start()".to_string())
            })?;
        }

        let batch_size = self.batch_size;
        let stats_clone = Arc::clone(&stats);

        let source = self
            .source
            .take()
            .ok_or_else(|| SessionError::Internal("Packet source already consumed".to_string()))?;

        let parser_handle = self.spawn_parser(
            source,
            parser_schemas,
            entity_dispatcher_senders,
            event_dispatcher_senders,
            batch_size,
            stats_clone,
        );

        execute_query_plans(plans, &self.ctx, result_senders)?;

        Ok((parser_handle, stats))
    }

    fn spawn_parser(
        &self,
        source: PacketSourceKind,
        parser_schemas: Vec<(u64, EntitySchema)>,
        entity_dispatcher_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
        event_dispatcher_senders: HashMap<u32, Vec<(BatchSender, EventType)>>,
        batch_size: usize,
        stats: Arc<StreamingStats>,
    ) -> JoinHandle<Result<(), SessionError>> {
        match source {
            PacketSourceKind::DemoBytes(demo_bytes) => {
                let source = ChunkedBytesSource::new(demo_bytes);
                tokio::spawn(run_parser_demo(
                    source,
                    parser_schemas,
                    entity_dispatcher_senders,
                    event_dispatcher_senders,
                    batch_size,
                    stats,
                ))
            }
            #[cfg(feature = "gotv")]
            PacketSourceKind::GotvClient {
                client,
                start_packet,
            } => {
                let (packet_tx, packet_rx) = mpsc::channel::<Bytes>(32);

                let cancel_token = self.cancel_token.clone();
                let client_with_token = if let Some(token) = cancel_token {
                    (*client).with_cancel_token(token)
                } else {
                    (*client).with_cancel_token(CancellationToken::new())
                };

                tokio::spawn(async move {
                    let _ = client_with_token.stream_to_channel(packet_tx).await;
                });

                tokio::spawn(run_parser_broadcast(
                    packet_rx,
                    start_packet,
                    parser_schemas,
                    entity_dispatcher_senders,
                    event_dispatcher_senders,
                    batch_size,
                    stats,
                ))
            }
        }
    }
}

async fn forward_stream_to_channel(
    mut stream: SendableRecordBatchStream,
    tx: mpsc::UnboundedSender<Result<RecordBatch, SessionError>>,
) {
    while let Some(result) = stream.next().await {
        let mapped = result.map_err(|e| SessionError::DataFusion(e.to_string()));
        if tx.send(mapped).is_err() {
            break;
        }
    }
}

struct ChunkedBytesSource {
    data: Bytes,
    position: usize,
    chunk_size: usize,
}

impl ChunkedBytesSource {
    fn new(data: Bytes) -> Self {
        Self {
            data,
            position: 0,
            chunk_size: 65536,
        }
    }
}

impl PacketSource for ChunkedBytesSource {
    async fn recv(&mut self) -> Option<Bytes> {
        if self.position >= self.data.len() {
            return None;
        }
        let end = std::cmp::min(self.position + self.chunk_size, self.data.len());
        let chunk = self.data.slice(self.position..end);
        self.position = end;
        Some(chunk)
    }
}

async fn run_parser_demo<P: PacketSource + 'static>(
    source: P,
    schemas: Vec<(u64, EntitySchema)>,
    entity_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    event_senders: HashMap<u32, Vec<(BatchSender, EventType)>>,
    batch_size: usize,
    stats: Arc<StreamingStats>,
) -> Result<(), SessionError> {
    use crate::haste::core::packet_channel_demo_stream::PacketChannelDemoStream;
    use crate::haste::parser::AsyncStreamingParser;
    use crate::visitor::{BatchingDemoVisitor, BatchingEntityDispatcher, BatchingEventDispatcher};

    debug!(
        target: "demofusion::parser",
        batch_size,
        entity_types = entity_senders.len(),
        event_types = event_senders.len(),
        "run_parser_demo: starting"
    );

    let demo_stream = PacketChannelDemoStream::new(source);

    let entity_dispatcher = BatchingEntityDispatcher::new_with_stats(
        entity_senders,
        batch_size,
        Some(Arc::clone(&stats)),
    );

    let event_dispatcher = if event_senders.is_empty() {
        None
    } else {
        Some(BatchingEventDispatcher::new_with_stats(
            event_senders,
            batch_size,
            Some(Arc::clone(&stats)),
        ))
    };

    let visitor = BatchingDemoVisitor::new(entity_dispatcher, event_dispatcher, &schemas);

    let mut parser = AsyncStreamingParser::from_stream_with_visitor(demo_stream, visitor)
        .map_err(|e| SessionError::Parser(e.to_string()))?;

    debug!(target: "demofusion::parser", "run_parser_demo: running parser to end");
    let _ = parser.run_to_end().await;

    debug!(target: "demofusion::parser", "run_parser_demo: flushing remaining data");
    let mut visitor = parser.into_visitor();
    let _ = visitor.flush_all().await;

    info!(target: "demofusion::parser", "run_parser_demo: complete");
    Ok(())
}

#[cfg(feature = "gotv")]
async fn run_parser_broadcast(
    packet_rx: mpsc::Receiver<Bytes>,
    start_packet: Bytes,
    schemas: Vec<(u64, EntitySchema)>,
    entity_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    event_senders: HashMap<u32, Vec<(BatchSender, EventType)>>,
    batch_size: usize,
    stats: Arc<StreamingStats>,
) -> Result<(), SessionError> {
    use crate::haste::core::packet_channel_broadcast_stream::PacketChannelBroadcastStream;
    use crate::haste::parser::AsyncStreamingParser;
    use crate::visitor::{BatchingDemoVisitor, BatchingEntityDispatcher, BatchingEventDispatcher};

    let broadcast_stream =
        PacketChannelBroadcastStream::with_initial_packet(packet_rx, start_packet);

    let entity_dispatcher = BatchingEntityDispatcher::new_with_stats(
        entity_senders,
        batch_size,
        Some(Arc::clone(&stats)),
    );

    let event_dispatcher = if event_senders.is_empty() {
        None
    } else {
        Some(BatchingEventDispatcher::new_with_stats(
            event_senders,
            batch_size,
            Some(Arc::clone(&stats)),
        ))
    };

    let visitor = BatchingDemoVisitor::new(entity_dispatcher, event_dispatcher, &schemas);

    let mut parser = AsyncStreamingParser::from_stream_with_visitor(broadcast_stream, visitor)
        .map_err(|e| SessionError::Parser(e.to_string()))?;

    let _ = parser.run_to_end().await;

    let mut visitor = parser.into_visitor();
    let _ = visitor.flush_all().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn make_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int32, false),
            Field::new("entity_index", DataType::Int32, false),
        ]))
    }

    #[tokio::test]
    async fn test_query_handle_receives_batches() {
        let (tx, rx) = mpsc::unbounded_channel();
        let schema = make_test_schema();

        let mut handle = QueryHandle {
            receiver: rx,
            schema: schema.clone(),
        };

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 200])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 2])),
            ],
        )
        .unwrap();

        tx.send(Ok(batch.clone())).unwrap();
        drop(tx);

        let received = handle.next().await;
        assert!(received.is_some());
        let received_batch = received.unwrap().unwrap();
        assert_eq!(received_batch.num_rows(), 2);

        let next = handle.next().await;
        assert!(next.is_none());
    }

    #[tokio::test]
    async fn test_query_handle_receives_errors() {
        let (tx, rx) = mpsc::unbounded_channel();
        let schema = make_test_schema();

        let mut handle = QueryHandle {
            receiver: rx,
            schema,
        };

        tx.send(Err(SessionError::DataFusion("test error".to_string())))
            .unwrap();
        drop(tx);

        let received = handle.next().await;
        assert!(received.is_some());
        assert!(matches!(
            received.unwrap(),
            Err(SessionError::DataFusion(_))
        ));
    }

    #[test]
    fn test_query_handle_schema() {
        let (_, rx) = mpsc::unbounded_channel::<Result<RecordBatch, SessionError>>();
        let schema = make_test_schema();

        let handle = QueryHandle {
            receiver: rx,
            schema: schema.clone(),
        };

        assert_eq!(handle.schema().fields().len(), 2);
        assert_eq!(handle.schema().field(0).name(), "tick");
    }

    #[test]
    fn test_register_all_providers_creates_entity_providers() {
        let ctx = streaming_session_context();
        let mut schemas = HashMap::new();
        schemas.insert(
            Arc::from("TestEntity"),
            crate::schema::EntitySchema {
                serializer_name: Arc::from("TestEntity"),
                serializer_hash: 12345,
                arrow_schema: make_test_schema(),
                field_keys: vec![],
                field_column_counts: vec![],
            },
        );

        let (entity_providers, event_providers) = register_all_providers(&ctx, &schemas);

        assert_eq!(entity_providers.len(), 1);
        assert!(entity_providers.contains_key("TestEntity"));
        assert!(&**entity_providers["TestEntity"].entity_type() == "TestEntity");

        // All event types should be registered
        assert!(!event_providers.is_empty());
        assert!(event_providers.contains_key("DamageEvent"));
    }

    #[test]
    fn test_register_all_providers_events_registered_in_context() {
        let ctx = streaming_session_context();
        let schemas = HashMap::new(); // no entity schemas

        let (_entity_providers, event_providers) = register_all_providers(&ctx, &schemas);

        // Event tables should be visible in the context's catalog
        let all_events = EventType::all();
        assert_eq!(event_providers.len(), all_events.len());

        for event_type in all_events {
            let table_name = event_type.table_name();
            assert!(
                event_providers.contains_key(table_name),
                "Missing event provider for {}",
                table_name,
            );
        }
    }

    #[test]
    fn test_receiver_slot_inject_and_take() {
        use crate::datafusion::distributor_channels;

        let (senders, receivers) = distributor_channels::channels::<RecordBatch>(2);

        let slot1 = ReceiverSlot::new();
        let slot2 = ReceiverSlot::new();

        let mut rx_iter = receivers.into_iter();
        slot1.inject(rx_iter.next().unwrap()).unwrap();
        slot2.inject(rx_iter.next().unwrap()).unwrap();

        // Verify slots were filled by taking from them
        let _rx1 = slot1.take();
        let _rx2 = slot2.take();

        drop(senders);
    }

    #[test]
    fn test_receiver_slot_inject_fails_if_already_filled() {
        use crate::datafusion::distributor_channels;

        let (_senders, receivers) = distributor_channels::channels::<RecordBatch>(2);
        let mut rx_iter = receivers.into_iter();

        let slot = ReceiverSlot::new();
        slot.inject(rx_iter.next().unwrap()).unwrap();

        let result = slot.inject(rx_iter.next().unwrap());
        assert!(result.is_err());
    }
}
