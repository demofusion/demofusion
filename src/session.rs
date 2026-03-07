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
//! let (mut session, schemas) = source.into_session().await?;
//!
//! println!("Found {} entities", schemas.len());
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
//! let (session, schemas) = source.into_session().await?;
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
use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
#[cfg(feature = "gotv")]
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::datafusion::distributor_channels::{self, DistributionReceiver, DistributionSender};
use crate::datafusion::pipeline_analysis::analyze_pipeline;
use crate::datafusion::streaming_stats::StreamingStats;
use crate::datafusion::table_providers::{
    EntityTableProvider, EventTableProvider, new_receiver_slot,
};
use crate::events::{EventType, event_schema};
use crate::haste::core::packet_source::PacketSource;
use crate::schema::EntitySchema;
use crate::sql::extract_table_names;

type BatchReceiver = DistributionReceiver<RecordBatch>;
type BatchSender = DistributionSender<RecordBatch>;
type ReceiverSlot = Arc<Mutex<Option<BatchReceiver>>>;
type ParserResult = (JoinHandle<Result<(), SessionError>>, Arc<StreamingStats>);
type DispatcherChannelParts = (
    HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    HashMap<u32, Vec<(BatchSender, EventType)>>,
    Vec<(u64, EntitySchema)>,
    SlotBindings,
);

pub type Schemas = HashMap<Arc<str>, EntitySchema>;

const DEFAULT_DEMO_BATCH_SIZE: usize = 1024;
const DEFAULT_LIVE_BATCH_SIZE: usize = 128;

#[async_trait]
pub trait IntoStreamingSession {
    async fn into_session(self) -> Result<(StreamingSession, Schemas), SessionError>;
}

// QUESTION: Why do we create a new session for every query? is that just how demofusion works?
fn streaming_session_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_coalesce_batches(false);
    SessionContext::new_with_config(config)
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
    entity_slots: HashMap<Arc<str>, ReceiverSlot>,
    event_slots: HashMap<EventType, ReceiverSlot>,
    context: SessionContext,
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
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: AtomicBool::new(false),
        })
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

struct CollectedSlots {
    entity_slots: HashMap<Arc<str>, Vec<ReceiverSlot>>,
    event_slots: HashMap<EventType, Vec<ReceiverSlot>>,
    plans: Vec<Arc<dyn ExecutionPlan>>,
    contexts: Vec<SessionContext>,
    result_senders: Vec<mpsc::UnboundedSender<Result<RecordBatch, SessionError>>>,
}

impl CollectedSlots {
    fn from_pending_queries(queries: Vec<PendingQuery>) -> Self {
        let mut entity_slots: HashMap<Arc<str>, Vec<ReceiverSlot>> = HashMap::new();
        let mut event_slots: HashMap<EventType, Vec<ReceiverSlot>> = HashMap::new();
        let mut plans = Vec::new();
        let mut contexts = Vec::new();
        let mut result_senders = Vec::new();

        for pending in queries {
            for (entity_type, slot) in pending.entity_slots {
                entity_slots.entry(entity_type).or_default().push(slot);
            }
            for (event_type, slot) in pending.event_slots {
                event_slots.entry(event_type).or_default().push(slot);
            }
            plans.push(pending.physical_plan);
            contexts.push(pending.context);
            result_senders.push(pending.result_tx);
        }

        Self {
            entity_slots,
            event_slots,
            plans,
            contexts,
            result_senders,
        }
    }

    fn total_channel_count(&self) -> usize {
        let entity_count: usize = self.entity_slots.values().map(|v| v.len()).sum();
        let event_count: usize = self.event_slots.values().map(|v| v.len()).sum();
        entity_count + event_count
    }
}

struct DispatcherChannels {
    entity_dispatcher_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    event_dispatcher_senders: HashMap<u32, Vec<(BatchSender, EventType)>>,
    parser_schemas: Vec<(u64, EntitySchema)>,
    slot_bindings: SlotBindings,
}

struct SlotBindings {
    entity_slot_indices: Vec<(ReceiverSlot, usize)>,
    event_slot_indices: Vec<(ReceiverSlot, usize)>,
    receivers: Vec<BatchReceiver>,
}

impl SlotBindings {
    fn bind(self) -> Result<(), SessionError> {
        let mut receivers_by_idx: HashMap<usize, BatchReceiver> =
            self.receivers.into_iter().enumerate().collect();

        for (slot, idx) in self.entity_slot_indices {
            let rx = receivers_by_idx
                .remove(&idx)
                .expect("receiver index mismatch");
            let old = slot.lock().replace(rx);
            if old.is_some() {
                return Err(SessionError::Internal(
                    "Entity slot already set".to_string(),
                ));
            }
        }

        for (slot, idx) in self.event_slot_indices {
            let rx = receivers_by_idx
                .remove(&idx)
                .expect("receiver index mismatch");
            let old = slot.lock().replace(rx);
            if old.is_some() {
                return Err(SessionError::Internal("Event slot already set".to_string()));
            }
        }

        Ok(())
    }
}

impl DispatcherChannels {
    fn setup(collected: &CollectedSlots, all_schemas: &Schemas) -> Self {
        let total_channels = collected.total_channel_count();
        let (all_senders, all_receivers) =
            distributor_channels::channels::<RecordBatch>(total_channels);

        let mut sender_iter = all_senders.into_iter();
        let mut receiver_idx = 0;

        let mut entity_dispatcher_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>> =
            HashMap::new();
        let mut entity_slot_indices: Vec<(ReceiverSlot, usize)> = Vec::new();

        for (entity_type, slots) in &collected.entity_slots {
            let schema = &all_schemas[entity_type.as_ref()];
            for slot in slots {
                let sender = sender_iter.next().expect("channel count mismatch");
                entity_dispatcher_senders
                    .entry(schema.serializer_hash)
                    .or_default()
                    .push((sender, schema.clone()));
                entity_slot_indices.push((slot.clone(), receiver_idx));
                receiver_idx += 1;
            }
        }

        let mut event_dispatcher_senders: HashMap<u32, Vec<(BatchSender, EventType)>> =
            HashMap::new();
        let mut event_slot_indices: Vec<(ReceiverSlot, usize)> = Vec::new();

        for (event_type, slots) in &collected.event_slots {
            for slot in slots {
                let sender = sender_iter.next().expect("channel count mismatch");
                event_dispatcher_senders
                    .entry(event_type.message_id())
                    .or_default()
                    .push((sender, *event_type));
                event_slot_indices.push((slot.clone(), receiver_idx));
                receiver_idx += 1;
            }
        }

        let parser_schemas: Vec<(u64, EntitySchema)> = all_schemas
            .iter()
            .filter(|(name, _)| collected.entity_slots.contains_key(name.as_ref()))
            .map(|(_, schema)| (schema.serializer_hash, schema.clone()))
            .collect();

        Self {
            entity_dispatcher_senders,
            event_dispatcher_senders,
            parser_schemas,
            slot_bindings: SlotBindings {
                entity_slot_indices,
                event_slot_indices,
                receivers: all_receivers,
            },
        }
    }

    fn into_parts(self) -> DispatcherChannelParts {
        (
            self.entity_dispatcher_senders,
            self.event_dispatcher_senders,
            self.parser_schemas,
            self.slot_bindings,
        )
    }
}

fn execute_query_plans(
    plans: Vec<Arc<dyn ExecutionPlan>>,
    contexts: Vec<SessionContext>,
    result_senders: Vec<mpsc::UnboundedSender<Result<RecordBatch, SessionError>>>,
) -> Result<(), SessionError> {
    for ((plan, ctx), result_tx) in plans
        .into_iter()
        .zip(contexts.into_iter())
        .zip(result_senders.into_iter())
    {
        let stream = execute_stream(plan, ctx.task_ctx())
            .map_err(|e| SessionError::DataFusion(e.to_string()))?;

        tokio::spawn(forward_stream_to_channel(stream, result_tx));
    }
    Ok(())
}

pub struct StreamingSession {
    source: Option<PacketSourceKind>,
    schemas: HashMap<Arc<str>, EntitySchema>,
    pending_queries: Vec<PendingQuery>,
    batch_size: usize,
    #[cfg(feature = "gotv")]
    cancel_token: Option<CancellationToken>,
    reject_pipeline_breakers: bool,
    state: Arc<SessionState>,
}

impl StreamingSession {
    pub(crate) fn from_demo_bytes_internal(demo_bytes: Bytes, schemas: Schemas) -> Self {
        Self {
            source: Some(PacketSourceKind::DemoBytes(demo_bytes)),
            schemas,
            pending_queries: Vec::new(),
            batch_size: DEFAULT_DEMO_BATCH_SIZE,
            #[cfg(feature = "gotv")]
            cancel_token: None,
            reject_pipeline_breakers: false,
            state: SessionState::new(),
        }
    }

    #[cfg(feature = "gotv")]
    pub(crate) fn from_gotv_internal(
        client: crate::gotv::BroadcastClient,
        schemas: Schemas,
        start_packet: Bytes,
    ) -> Self {
        Self {
            source: Some(PacketSourceKind::GotvClient {
                client: Box::new(client),
                start_packet,
            }),
            schemas,
            pending_queries: Vec::new(),
            batch_size: DEFAULT_LIVE_BATCH_SIZE,
            cancel_token: None,
            reject_pipeline_breakers: false,
            state: SessionState::new(),
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

    pub fn schemas(&self) -> impl Iterator<Item = &EntitySchema> {
        self.schemas.values()
    }

    pub fn schema(&self, name: &str) -> Option<&EntitySchema> {
        self.schemas.get(name)
    }

    pub fn entity_names(&self) -> Vec<&str> {
        self.schemas.keys().map(|s| &**s).collect()
    }

    pub async fn add_query(&mut self, sql: &str) -> Result<QueryHandle, SessionError> {
        if self.state.is_started() {
            return Err(SessionError::AlreadyStarted);
        }

        let table_names = extract_table_names(sql).map_err(|e| SessionError::Sql(e.to_string()))?;

        let entity_types: Vec<Arc<str>> = table_names
            .iter()
            .filter(|name| self.schemas.contains_key(name.as_str()))
            .map(|s| Arc::<str>::from(s.as_str()))
            .collect();

        let event_types: Vec<EventType> = table_names
            .iter()
            .filter_map(|name| {
                EventType::all()
                    .iter()
                    .find(|e| e.table_name() == name)
                    .copied()
            })
            .collect();

        let unknown_tables: Vec<&String> = table_names
            .iter()
            .filter(|name| {
                !self.schemas.contains_key(name.as_str())
                    && !EventType::all().iter().any(|e| e.table_name() == *name)
            })
            .collect();

        if !unknown_tables.is_empty() {
            return Err(SessionError::UnknownTable(
                unknown_tables
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }

        if entity_types.is_empty() && event_types.is_empty() {
            return Err(SessionError::Sql(
                "Query must reference at least one entity or event table".to_string(),
            ));
        }

        let ctx = streaming_session_context();

        let mut entity_slots: HashMap<Arc<str>, ReceiverSlot> = HashMap::new();
        for entity_type in &entity_types {
            let schema = &self.schemas[entity_type];
            let slot = new_receiver_slot();
            entity_slots.insert(Arc::clone(entity_type), slot.clone());

            let provider = EntityTableProvider::new(
                schema.arrow_schema.clone(),
                Arc::clone(entity_type),
                slot,
            );
            ctx.register_table(&**entity_type, Arc::new(provider))?;
        }

        let mut event_slots: HashMap<EventType, ReceiverSlot> = HashMap::new();
        for event_type in &event_types {
            let schema = event_schema(event_type.table_name()).ok_or_else(|| {
                SessionError::Schema(format!(
                    "No schema found for event type: {}",
                    event_type.table_name()
                ))
            })?;
            let slot = new_receiver_slot();
            event_slots.insert(*event_type, slot.clone());

            let provider = EventTableProvider::new(*event_type, schema, slot);
            ctx.register_table(event_type.table_name(), Arc::new(provider))?;
        }

        let logical_plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| SessionError::Sql(e.to_string()))?;

        let output_schema: SchemaRef = Arc::new(logical_plan.schema().as_arrow().clone());

        let physical_plan = ctx
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

        // NOTE: These should not be unbounded
        let (result_tx, result_rx) = mpsc::unbounded_channel();

        self.pending_queries.push(PendingQuery {
            physical_plan,
            entity_slots,
            event_slots,
            context: ctx,
            result_tx,
        });

        // NOTE:  May be convenient to incude the logical and physical plans here for inspection.
        Ok(QueryHandle {
            receiver: result_rx,
            schema: output_schema,
        })
    }

    pub fn start(mut self) -> Result<SessionResult, SessionError> {
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

        let collected =
            CollectedSlots::from_pending_queries(self.pending_queries.drain(..).collect());

        let channels = DispatcherChannels::setup(&collected, &self.schemas);
        let (entity_senders, event_senders, parser_schemas, slot_bindings) = channels.into_parts();

        let batch_size = self.batch_size;
        let stats_clone = Arc::clone(&stats);

        let source = self
            .source
            .take()
            .ok_or_else(|| SessionError::Internal("Packet source already consumed".to_string()))?;

        let parser_handle = self.spawn_parser(
            source,
            parser_schemas,
            entity_senders,
            event_senders,
            batch_size,
            stats_clone,
        );

        slot_bindings.bind()?;

        execute_query_plans(
            collected.plans,
            collected.contexts,
            collected.result_senders,
        )?;

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
    fn test_collected_slots_empty() {
        let collected = CollectedSlots::from_pending_queries(vec![]);

        assert!(collected.entity_slots.is_empty());
        assert!(collected.event_slots.is_empty());
        assert!(collected.plans.is_empty());
        assert!(collected.contexts.is_empty());
        assert!(collected.result_senders.is_empty());
        assert_eq!(collected.total_channel_count(), 0);
    }

    #[test]
    fn test_collected_slots_channel_count() {
        use crate::datafusion::table_providers::new_receiver_slot;

        let mut entity_slots: HashMap<Arc<str>, Vec<ReceiverSlot>> = HashMap::new();
        entity_slots.insert(
            Arc::from("EntityA"),
            vec![new_receiver_slot(), new_receiver_slot()],
        );
        entity_slots.insert(Arc::from("EntityB"), vec![new_receiver_slot()]);

        let mut event_slots: HashMap<EventType, Vec<ReceiverSlot>> = HashMap::new();
        event_slots.insert(EventType::Damage, vec![new_receiver_slot()]);

        let collected = CollectedSlots {
            entity_slots,
            event_slots,
            plans: vec![],
            contexts: vec![],
            result_senders: vec![],
        };

        assert_eq!(collected.total_channel_count(), 4);
    }

    #[test]
    fn test_slot_bindings_bind_success() {
        use crate::datafusion::distributor_channels;
        use crate::datafusion::table_providers::new_receiver_slot;

        let (senders, receivers) = distributor_channels::channels::<RecordBatch>(2);

        let slot1 = new_receiver_slot();
        let slot2 = new_receiver_slot();

        let bindings = SlotBindings {
            entity_slot_indices: vec![(slot1.clone(), 0)],
            event_slot_indices: vec![(slot2.clone(), 1)],
            receivers,
        };

        let result = bindings.bind();
        assert!(result.is_ok());

        assert!(slot1.lock().is_some());
        assert!(slot2.lock().is_some());

        drop(senders);
    }

    #[test]
    fn test_slot_bindings_bind_fails_if_slot_already_set() {
        use crate::datafusion::distributor_channels;
        use crate::datafusion::table_providers::new_receiver_slot;

        let (senders, receivers) = distributor_channels::channels::<RecordBatch>(2);

        let slot = new_receiver_slot();
        {
            let (_, pre_receivers) = distributor_channels::channels::<RecordBatch>(1);
            let pre_rx = pre_receivers.into_iter().next().unwrap();
            slot.lock().replace(pre_rx);
        }

        let bindings = SlotBindings {
            entity_slot_indices: vec![(slot, 0)],
            event_slot_indices: vec![],
            receivers,
        };

        let result = bindings.bind();
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(matches!(err, SessionError::Internal(_)));

        drop(senders);
    }
}
