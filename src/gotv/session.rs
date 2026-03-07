//! High-level streaming session for GOTV broadcasts.
//!
//! `SpectateSession` provides a simplified API for connecting to live GOTV broadcasts,
//! discovering schemas, registering SQL queries, and streaming results.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────────────┐
//! │                           SpectateSession                                 │
//! ├──────────────────────────────────────────────────────────────────────────┤
//! │  Phase 1: connect(url)                                                   │
//! │    - Sync with GOTV server                                               │
//! │    - Fetch /start packet                                                 │
//! │    - Discover entity schemas                                             │
//! │    - Return SpectateSession with schemas ready for inspection            │
//! │                                                                          │
//! │  Phase 2: add_query(sql) → QueryHandle                                   │
//! │    - Parse SQL to validate syntax                                        │
//! │    - Extract table names to verify schemas exist                         │
//! │    - Create channel, store sender, return handle with receiver           │
//! │    - Queries can be added incrementally                                  │
//! │                                                                          │
//! │  Phase 3: start(cancel_token) → consumes session                         │
//! │    - Create distribution channels with shared gate                       │
//! │    - Build DataFusion plans for each query                               │
//! │    - Spawn parser task                                                   │
//! │    - QueryHandles become live (data starts flowing)                      │
//! └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use demofusion::gotv::SpectateSession;
//! use tokio_util::sync::CancellationToken;
//! use futures::StreamExt;
//!
//! // Phase 1: Connect and discover schemas
//! let mut session = SpectateSession::connect("http://dist1-ord1.steamcontent.com/tv/18895867").await?;
//!
//! // Inspect metadata and schemas
//! println!("Map: {}", session.map());
//! for schema in session.schemas() {
//!     println!("  {}: {} fields", schema.serializer_name, schema.arrow_schema.fields().len());
//! }
//!
//! // Phase 2: Register queries - returns handles immediately
//! let mut pawns = session.add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn")?;
//! let mut troopers = session.add_query("SELECT tick, entity_index FROM CNPC_Trooper")?;
//!
//! // Phase 3: Start streaming - consumes session
//! let cancel = CancellationToken::new();
//! let _parser_handle = session.start(cancel.clone()).await?;
//!
//! // Phase 4: Consume streams (handles are now live)
//! loop {
//!     tokio::select! {
//!         Some(result) = pawns.next() => {
//!             let batch = result?;
//!             println!("Got {} pawn rows", batch.num_rows());
//!         }
//!         Some(result) = troopers.next() => {
//!             let batch = result?;
//!             println!("Got {} trooper rows", batch.num_rows());
//!         }
//!         else => break,
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream, execute_stream};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::datafusion::pipeline_analysis::analyze_pipeline;
use futures::{Stream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::client::BroadcastClient;
use super::config::ClientConfig;
use super::error::{GotvError, SqlError};
use crate::datafusion::distributor_channels::{self, DistributionReceiver, DistributionSender};
use crate::datafusion::streaming_stats::StreamingStats;
use crate::datafusion::table_providers::EntityTableProvider;
use crate::schema::EntitySchema;
use crate::sql::extract_table_names;
use crate::visitor::discover_schemas_from_broadcast;

type BatchReceiver = DistributionReceiver<RecordBatch>;
type BatchSender = DistributionSender<RecordBatch>;
type ReceiverSlot = Arc<Mutex<Option<BatchReceiver>>>;

const DEFAULT_BATCH_SIZE: usize = 128;

fn streaming_session_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_coalesce_batches(false);
    SessionContext::new_with_config(config)
}

/// A pending query registration, created by `add_query()`.
struct PendingQuery {
    physical_plan: Arc<dyn ExecutionPlan>,
    slots: HashMap<Arc<str>, ReceiverSlot>,
    context: SessionContext,
    result_tx: mpsc::UnboundedSender<Result<RecordBatch, GotvError>>,
}

/// Handle to a streaming query result.
///
/// Created by [`SpectateSession::add_query`], becomes live after [`SpectateSession::start`].
/// Implements [`Stream`] to yield `RecordBatch` results.
///
/// Before `start()` is called, the handle will block waiting for data.
/// After `start()` is called, data will begin flowing.
pub struct QueryHandle {
    receiver: mpsc::UnboundedReceiver<Result<RecordBatch, GotvError>>,
    schema: SchemaRef,
}

impl QueryHandle {
    /// Get the Arrow schema for this query's results.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Stream for QueryHandle {
    type Item = Result<RecordBatch, GotvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Result of starting a spectate session.
///
/// Contains the parser task handle and optional streaming statistics.
pub struct SpectateResult {
    /// Handle to the background parser task.
    /// Await this after all query streams complete to check for parser errors.
    pub parser_handle: JoinHandle<Result<(), GotvError>>,
    /// Statistics for monitoring in-flight streaming data.
    /// Available when stats collection is enabled.
    pub stats: Option<Arc<StreamingStats>>,
}

/// High-level session for streaming SQL queries over a GOTV broadcast.
///
/// See module documentation for usage examples.
pub struct SpectateSession {
    client: BroadcastClient,
    schemas: HashMap<Arc<str>, EntitySchema>,
    start_packet: Bytes,
    pending_queries: Vec<PendingQuery>,
    batch_size: usize,
    reject_pipeline_breakers: bool,
}

impl SpectateSession {
    /// Connect to a GOTV broadcast and discover schemas.
    ///
    /// This performs the initial handshake:
    /// 1. Sync with the GOTV server to get broadcast metadata
    /// 2. Fetch the /start packet containing schema data
    /// 3. Parse schemas from the start packet
    pub async fn connect(url: impl Into<String>) -> Result<Self, GotvError> {
        Self::connect_with_config(url, ClientConfig::default()).await
    }

    /// Connect with custom client configuration.
    pub async fn connect_with_config(
        url: impl Into<String>,
        config: ClientConfig,
    ) -> Result<Self, GotvError> {
        let mut client = BroadcastClient::with_config(url, config);

        client.sync().await?;
        let start_packet = client.fetch_start().await?;

        let schema_vec = discover_schemas_from_broadcast(&start_packet)
            .await
            .map_err(|e| GotvError::Schema(e.to_string()))?;

        let schemas: HashMap<Arc<str>, EntitySchema> = schema_vec
            .into_iter()
            .map(|s| (Arc::clone(&s.serializer_name), s))
            .collect();

        Ok(Self {
            client,
            schemas,
            start_packet,
            pending_queries: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            reject_pipeline_breakers: false,
        })
    }

    /// Set the batch size for streaming results.
    ///
    /// Smaller batches provide lower latency, larger batches are more efficient.
    /// Default is 128 rows.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Reject queries that contain pipeline breakers.
    ///
    /// Pipeline breakers (ORDER BY, aggregations, etc.) buffer all input before
    /// producing output, which defeats streaming. When enabled, `add_query()` will
    /// return `GotvError::PipelineBreaker` for such queries.
    ///
    /// Default is `false` (allow pipeline breakers with a warning).
    pub fn with_reject_pipeline_breakers(mut self, reject: bool) -> Self {
        self.reject_pipeline_breakers = reject;
        self
    }

    /// Get the sync response from the GOTV server.
    pub fn sync_response(&self) -> Option<&super::client::SyncResponse> {
        self.client.sync_response()
    }

    /// Get the map name.
    pub fn map(&self) -> &str {
        self.client
            .sync_response()
            .map(|s| s.map.as_str())
            .unwrap_or("")
    }

    /// Get the current tick.
    pub fn tick(&self) -> i64 {
        self.client.sync_response().map(|s| s.tick).unwrap_or(0)
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

    /// Get the raw start packet bytes.
    pub fn start_packet(&self) -> &Bytes {
        &self.start_packet
    }

    /// Register a SQL query and return a handle for streaming results.
    ///
    /// The query is validated immediately (SQL syntax and table existence),
    /// but data won't flow until [`start`](Self::start) is called.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The SQL syntax is invalid
    /// - The query references unknown entity types
    /// - The query contains pipeline breakers and `reject_pipeline_breakers` is enabled
    pub async fn add_query(&mut self, sql: &str) -> Result<QueryHandle, GotvError> {
        let table_names = extract_table_names(sql).map_err(|e| {
            GotvError::Sql(SqlError::Syntax {
                message: e.to_string(),
                line: 0,
                column: 0,
            })
        })?;

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
            return Err(GotvError::UnknownEntity(
                unknown_tables
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", "),
            ));
        }

        if entity_types.is_empty() {
            return Err(SqlError::InvalidTable {
                table: "(none)".to_string(),
            }
            .into());
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
                .map_err(|e| GotvError::DataFusion(e.to_string()))?;
        }

        let logical_plan = ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(SqlError::from_datafusion)?;

        let output_schema: SchemaRef = Arc::new(logical_plan.schema().as_arrow().clone());

        let physical_plan = ctx
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .map_err(|e| GotvError::DataFusion(e.to_string()))?;

        let analysis = analyze_pipeline(&physical_plan);
        if analysis.has_pipeline_breakers() {
            if self.reject_pipeline_breakers {
                return Err(GotvError::PipelineBreaker(analysis.report()));
            }
            tracing::warn!(
                query = sql,
                breakers = ?analysis.breakers.iter().map(|b| &b.operator_name).collect::<Vec<_>>(),
                "Query contains pipeline breakers - streaming will block until source closes"
            );
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
    ///
    /// Returns a handle to the parser task. You can await it to detect parser errors
    /// after the broadcast ends.
    pub async fn start(
        self,
        cancel_token: CancellationToken,
    ) -> Result<JoinHandle<Result<(), GotvError>>, GotvError> {
        let result = self.start_with_stats(cancel_token).await?;
        Ok(result.parser_handle)
    }

    /// Start streaming with statistics collection enabled.
    ///
    /// Same as [`start`](Self::start) but also returns streaming statistics
    /// for monitoring in-flight data (rows produced, rows sent, backpressure events).
    pub async fn start_with_stats(
        self,
        cancel_token: CancellationToken,
    ) -> Result<SpectateResult, GotvError> {
        if self.pending_queries.is_empty() {
            return Err(GotvError::NotStarted);
        }

        let Self {
            client,
            schemas,
            start_packet: _,
            pending_queries,
            batch_size,
            reject_pipeline_breakers: _,
        } = self;

        // Create stats tracker for monitoring
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

        let (packet_tx, packet_rx) = mpsc::channel::<Bytes>(32);

        let stats_clone = Arc::clone(&stats);
        let parser_handle: JoinHandle<Result<(), GotvError>> = tokio::spawn(run_parser(
            packet_rx,
            parser_schemas,
            entity_dispatcher_senders,
            batch_size,
            Some(stats_clone),
        ));

        let client_with_token = client.with_cancel_token(cancel_token);
        let _stream_handle: JoinHandle<Result<(), GotvError>> = tokio::spawn(async move {
            let result = client_with_token.stream_to_channel(packet_tx).await;
            match result {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }
        });

        let mut receivers_by_idx: HashMap<usize, BatchReceiver> =
            all_receivers.into_iter().enumerate().collect();

        for (slot, idx) in entity_slot_receiver_indices {
            let rx = receivers_by_idx
                .remove(&idx)
                .expect("receiver index mismatch");
            let old = slot.lock().replace(rx);
            if old.is_some() {
                return Err(GotvError::Internal("Slot already set".to_string()));
            }
        }

        for ((plan, ctx), result_tx) in plans
            .into_iter()
            .zip(contexts.into_iter())
            .zip(result_senders.into_iter())
        {
            let stream = execute_stream(plan, ctx.task_ctx())
                .map_err(|e| GotvError::DataFusion(e.to_string()))?;

            tokio::spawn(forward_stream_to_channel(stream, result_tx));
        }

        Ok(SpectateResult {
            parser_handle,
            stats: Some(stats),
        })
    }
}

async fn forward_stream_to_channel(
    mut stream: SendableRecordBatchStream,
    tx: mpsc::UnboundedSender<Result<RecordBatch, GotvError>>,
) {
    while let Some(result) = stream.next().await {
        let mapped = result.map_err(|e| GotvError::DataFusion(e.to_string()));
        if tx.send(mapped).is_err() {
            break;
        }
    }
}

async fn run_parser(
    packet_rx: mpsc::Receiver<Bytes>,
    schemas: Vec<(u64, EntitySchema)>,
    entity_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    batch_size: usize,
    stats: Option<Arc<StreamingStats>>,
) -> Result<(), GotvError> {
    use crate::haste::core::packet_channel_broadcast_stream::PacketChannelBroadcastStream;
    use crate::haste::parser::AsyncStreamingParser;
    use crate::visitor::{BatchingDemoVisitor, BatchingEntityDispatcher};

    let broadcast_stream = PacketChannelBroadcastStream::new(packet_rx);
    let entity_dispatcher =
        BatchingEntityDispatcher::new_with_stats(entity_senders, batch_size, stats);
    let visitor = BatchingDemoVisitor::new(entity_dispatcher, None, &schemas);

    let mut parser = AsyncStreamingParser::from_stream_with_visitor(broadcast_stream, visitor)
        .map_err(|e| GotvError::Parse(e.to_string()))?;

    let _ = parser.run_to_end().await;

    let mut visitor = parser.into_visitor();
    let _ = visitor.flush_all().await;

    drop(visitor);

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

        tx.send(Err(GotvError::DataFusion("test error".to_string())))
            .unwrap();
        drop(tx);

        let received = handle.next().await;
        assert!(received.is_some());
        assert!(matches!(received.unwrap(), Err(GotvError::DataFusion(_))));
    }

    #[test]
    fn test_query_handle_schema() {
        let (_, rx) = mpsc::unbounded_channel::<Result<RecordBatch, GotvError>>();
        let schema = make_test_schema();

        let handle = QueryHandle {
            receiver: rx,
            schema: schema.clone(),
        };

        assert_eq!(handle.schema().fields().len(), 2);
        assert_eq!(handle.schema().field(0).name(), "tick");
    }
}
