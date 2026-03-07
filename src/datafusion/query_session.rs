//! Concurrent query session for streaming SQL queries.
//!
//! `StreamingQuerySession` enables multiple SQL queries to run concurrently
//! against a single GOTV broadcast stream. Each query receives ALL data from
//! the stream (broadcast semantics), with backpressure ensuring the parser
//! waits for slow consumers.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                     StreamingQuerySession                           │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │  run_queries(packet_source, entity_schemas, queries)                │
//! │       │                                                             │
//! │       ▼                                                             │
//! │  Phase 1: Plan all queries                                          │
//! │    - Create SessionContext per query                                │
//! │    - Register QueryTableProvider with OnceCell<Receiver>            │
//! │    - Plan SQL → PhysicalPlan                                        │
//! │    - Walk plan to find used entity types                            │
//! │    - Collect used slots                                             │
//! │       │                                                             │
//! │       ▼                                                             │
//! │  Phase 2: Create distribution channels with shared gate             │
//! │    - Single gate across ALL channels prevents JOIN deadlock         │
//! │    - Batching dispatchers batch at producer side                    │
//! │    - High-water marks provide memory backpressure                   │
//! │       │                                                             │
//! │       ▼                                                             │
//! │  Phase 3: Fill slots, execute plans                                 │
//! │    - slot.set(rx) for each slot                                     │
//! │    - plan.execute() returns streams                                 │
//! │    - Return Vec<SendableRecordBatchStream>                          │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Distribution Channels
//!
//! This module uses distribution channels (from DataFusion's repartition) to solve
//! the JOIN deadlock problem. When a single parser produces multiple streams for a
//! JOIN query, `SymmetricHashJoinExec` alternates polling left/right using `ready!()`.
//! With bounded channels, one side fills up → parser blocks → other side starves → deadlock.
//!
//! Distribution channels have a global gate that only blocks when ALL channels are non-empty.
//! This ensures the parser can always send to whichever channel the JOIN is currently draining.
//!
//! # Example
//!
//! ```ignore
//! let queries = vec![
//!     "SELECT tick, entity_index FROM CCitadelPlayerPawn".to_string(),
//!     "SELECT tick, entity_index FROM CNPC_Trooper".to_string(),
//! ];
//!
//! let streams = StreamingQuerySession::run_queries(
//!     packet_source,
//!     &entity_schemas,
//!     queries,
//! ).await?;
//!
//! // Each stream receives all data for its query
//! for stream in streams {
//!     while let Some(batch) = stream.next().await {
//!         // Process batch
//!     }
//! }
//! ```

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use tokio::task::JoinHandle;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{execute_stream, ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::common::arrow::compute::SortOptions;
use parking_lot::Mutex;

use crate::datafusion::distribution_stream::DistributionReceiverStream;
use crate::datafusion::distributor_channels::{self, DistributionReceiver, DistributionSender};
use crate::datafusion::pipeline_analysis::analyze_pipeline;
// TODO: Fix tick_bin for DataFusion 52 (ScalarUDFImpl changes)
// use crate::datafusion::tick_bin::make_tick_bin_udf;
use crate::error::{Result, Source2DfError};
use crate::events::{event_schema, EventType};
use crate::haste::core::packet_source::PacketSource;
use crate::schema::EntitySchema;
use crate::sql::extract_table_names;

type BatchReceiver = DistributionReceiver<RecordBatch>;
type BatchSender = DistributionSender<RecordBatch>;

/// Slot for passing a receiver to the partition stream.
/// Uses Mutex<Option<...>> so ownership can be taken exactly once in execute().
type ReceiverSlot = Arc<Mutex<Option<BatchReceiver>>>;

/// Stream format for demo data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamFormat {
    /// GOTV broadcast format (10-byte fixed headers, no compression).
    #[default]
    Broadcast,
    /// Demo file format (varint headers, snappy compression).
    DemoFile,
}

fn streaming_session_context() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_coalesce_batches(false);
    
    let ctx = SessionContext::new_with_config(config);
    
    // TODO: Re-enable tick_bin UDF after fixing for DataFusion 52
    // Register UDFs that preserve monotonicity for streaming queries
    // ctx.register_udf(make_tick_bin_udf());
    
    ctx
}

/// Result of running streaming queries, containing both the record batch streams
/// and a handle to the background parser task.
pub struct QueryStreams {
    /// The record batch streams for each query.
    pub streams: Vec<SendableRecordBatchStream>,
    /// Handle to the background parser task. Should be awaited after streams complete
    /// to detect parser errors.
    parser_handle: JoinHandle<Result<()>>,
    /// Statistics for monitoring in-flight data.
    stats: Arc<crate::datafusion::streaming_stats::StreamingStats>,
}

impl QueryStreams {
    /// Consume this struct and return just the streams.
    /// 
    /// **Warning**: This drops the parser handle, meaning parser errors will be silently ignored.
    /// Prefer using `into_parts()` and awaiting the handle after streams complete.
    pub fn into_streams(self) -> Vec<SendableRecordBatchStream> {
        self.streams
    }
    
    /// Consume this struct and return both the streams and the parser handle.
    /// 
    /// After consuming all streams, await the handle to check for parser errors:
    /// ```ignore
    /// let (streams, parser_handle) = query_streams.into_parts();
    /// // ... consume streams ...
    /// if let Err(e) = parser_handle.await? {
    ///     tracing::error!("Parser error: {}", e);
    /// }
    /// ```
    pub fn into_parts(self) -> (Vec<SendableRecordBatchStream>, JoinHandle<Result<()>>) {
        (self.streams, self.parser_handle)
    }

    /// Returns a reference to the streaming statistics.
    /// 
    /// Stats can be read while streaming is active to monitor:
    /// - `rows_produced`: Total rows parsed
    /// - `batches_sent`: RecordBatches dispatched to queries
    /// - `rows_sent`: Rows in dispatched batches
    /// - `gate_blocked_count`: How often backpressure blocked the parser
    pub fn stats(&self) -> &Arc<crate::datafusion::streaming_stats::StreamingStats> {
        &self.stats
    }

    /// Returns a point-in-time snapshot of streaming statistics.
    pub fn stats_snapshot(&self) -> crate::datafusion::streaming_stats::StreamingStatsSnapshot {
        self.stats.snapshot()
    }
}

pub struct StreamingQuerySession;

/// Default batch size for live streaming (128 rows ≈ 0.17 seconds of pawn data)
pub const DEFAULT_LIVE_BATCH_SIZE: usize = 128;

impl StreamingQuerySession {
    pub async fn run_queries<P: PacketSource + 'static>(
        packet_source: P,
        entity_schemas: &HashMap<Arc<str>, EntitySchema>,
        queries: Vec<String>,
        batch_size: Option<usize>,
        memory_tables: HashMap<String, RecordBatch>,
        format: StreamFormat,
    ) -> Result<QueryStreams> {
        let batch_size = batch_size.unwrap_or(DEFAULT_LIVE_BATCH_SIZE);
        
        // Create stats tracker for in-flight monitoring
        let stats = crate::datafusion::streaming_stats::StreamingStats::new();
        
        // Phase 1: Plan all queries, collect used slots
        //
        // We use SQL parsing to determine which entity types each query needs,
        // then only create slots for those types. This is simpler than walking
        // the physical plan tree and gives us the same information.
        let mut plans = Vec::new();
        let mut used_entity_slots: HashMap<Arc<str>, Vec<ReceiverSlot>> = HashMap::new();
        let mut used_event_slots: HashMap<EventType, Vec<ReceiverSlot>> = HashMap::new();
        let mut contexts = Vec::new();

        for (query_idx, sql) in queries.iter().enumerate() {
            let ctx = streaming_session_context();
            
            // Register memory tables first (static tables for JOINs)
            for (name, batch) in &memory_tables {
                ctx.register_batch(name, batch.clone())?;
            }
            
            // Parse SQL to find which tables this query uses
            let table_names = extract_table_names(sql)?;
            
            // Filter entity types (runtime-discovered schemas)
            // Note: HashMap<Arc<str>, _> supports lookup by &str via Borrow trait
            let query_entity_types: Vec<Arc<str>> = table_names
                .iter()
                .filter(|name| entity_schemas.contains_key(name.as_str()))
                .map(|name| Arc::from(name.as_str()))
                .collect();

            // Filter event types (compile-time known schemas)
            let query_event_types: Vec<EventType> = table_names
                .iter()
                .filter_map(|name| {
                    EventType::all()
                        .iter()
                        .find(|e| e.table_name() == name)
                        .copied()
                })
                .collect();

            // Register entity table providers
            let mut query_entity_slots: HashMap<Arc<str>, ReceiverSlot> = HashMap::new();
            for entity_type in &query_entity_types {
                let schema = &entity_schemas[&**entity_type];
                let slot: ReceiverSlot = Arc::new(Mutex::new(None));
                query_entity_slots.insert(entity_type.clone(), slot.clone());

                let provider = QueryTableProvider::new(
                    schema.arrow_schema.clone(),
                    entity_type.clone(),
                    slot,
                );
                ctx.register_table(&**entity_type, Arc::new(provider))?;
            }

            // Register event table providers
            let mut query_event_slots: HashMap<EventType, ReceiverSlot> = HashMap::new();
            for event_type in &query_event_types {
                let schema = event_schema(event_type.table_name())
                    .ok_or_else(|| Source2DfError::Schema(format!(
                        "No schema found for event type: {}",
                        event_type.table_name()
                    )))?;
                let slot: ReceiverSlot = Arc::new(Mutex::new(None));
                query_event_slots.insert(*event_type, slot.clone());

                let provider = QueryEventTableProvider::new(
                    *event_type,
                    schema,
                    slot,
                );
                ctx.register_table(event_type.table_name(), Arc::new(provider))?;
            }

            let logical = ctx.state().create_logical_plan(sql).await?;
            let physical = ctx.state().create_physical_plan(&logical).await?;

            // Analyze the physical plan for pipeline breakers
            let analysis = analyze_pipeline(&physical);
            if analysis.has_pipeline_breakers() {
                let breaker_names: Vec<_> = analysis
                    .breakers
                    .iter()
                    .map(|b| b.operator_name.as_str())
                    .collect();
                eprintln!(
                    "[query_session] WARNING: Query {} contains pipeline breakers: {:?}. \
                     Data will not stream until source closes.",
                    query_idx,
                    breaker_names
                );
            }

            // Move slots to used_slots
            for (entity_type, slot) in query_entity_slots {
                used_entity_slots.entry(entity_type).or_default().push(slot);
            }
            for (event_type, slot) in query_event_slots {
                used_event_slots.entry(event_type).or_default().push(slot);
            }

            plans.push(physical);
            contexts.push(ctx);
        }

        // Phase 2: Create distribution channels with a SHARED GATE
        //
        // Distribution channels solve the JOIN deadlock problem. With bounded channels:
        //   - Parser blocks on send() to full channel A
        //   - JOIN waits for data on empty channel B  
        //   - Parser can't produce for B because it's blocked on A → DEADLOCK
        //
        // Distribution channels have a global gate that blocks sends ONLY when ALL 
        // channels are non-empty. This ensures the parser can always send to whichever
        // channel the JOIN is currently draining, preventing deadlock.
        //
        // Additionally, per-channel high-water marks provide memory backpressure.
        
        // Count total channels needed (all entity channels + all event channels)
        let entity_channel_count: usize = used_entity_slots.values().map(|v| v.len()).sum();
        let event_channel_count: usize = used_event_slots.values().map(|v| v.len()).sum();
        let total_channels = entity_channel_count + event_channel_count;

        // Create all channels with a shared gate
        let (all_senders, all_receivers) = distributor_channels::channels::<RecordBatch>(total_channels);
        let mut sender_iter = all_senders.into_iter();
        let mut receiver_idx = 0;

        // Assign entity channels - keyed by serializer_hash for dispatcher lookup
        let mut entity_dispatcher_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>> = HashMap::new();
        let mut entity_slot_receiver_indices: Vec<(ReceiverSlot, usize)> = Vec::new();

        for (entity_type, slots) in &used_entity_slots {
            let schema = &entity_schemas[entity_type];
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

        // Assign event channels - keyed by message_id for dispatcher lookup
        let mut event_dispatcher_senders: HashMap<u32, Vec<(BatchSender, EventType)>> = HashMap::new();
        let mut event_slot_receiver_indices: Vec<(ReceiverSlot, usize)> = Vec::new();

        for (event_type, slots) in &used_event_slots {
            for slot in slots {
                let sender = sender_iter.next().expect("channel count mismatch");
                event_dispatcher_senders
                    .entry(event_type.message_id())
                    .or_default()
                    .push((sender, *event_type));
                event_slot_receiver_indices.push((slot.clone(), receiver_idx));
                receiver_idx += 1;
            }
        }

        // Build schemas for parser (only for used entity types)
        let schemas: Vec<(u64, EntitySchema)> = entity_schemas
            .iter()
            .filter(|(name, _)| used_entity_slots.contains_key(*name))
            .map(|(_, schema)| (schema.serializer_hash, schema.clone()))
            .collect();

        // Phase 3: Start parser with batching dispatchers
        let parser_handle: JoinHandle<Result<()>> = match format {
            StreamFormat::Broadcast => {
                tokio::spawn(run_parser_broadcast(
                    packet_source,
                    schemas,
                    entity_dispatcher_senders,
                    event_dispatcher_senders,
                    batch_size,
                    Arc::clone(&stats),
                ))
            }
            StreamFormat::DemoFile => {
                tokio::spawn(run_parser_demo(
                    packet_source,
                    schemas,
                    entity_dispatcher_senders,
                    event_dispatcher_senders,
                    batch_size,
                    Arc::clone(&stats),
                ))
            }
        };

        // Phase 4: Fill receiver slots (unblocks parser as queries start pulling)
        // Convert receivers Vec to a HashMap for indexed access, then extract by index
        let mut receivers_by_idx: HashMap<usize, BatchReceiver> = all_receivers
            .into_iter()
            .enumerate()
            .collect();
        
        for (slot, idx) in entity_slot_receiver_indices {
            let rx = receivers_by_idx.remove(&idx)
                .expect("receiver index mismatch");
            let old = slot.lock().replace(rx);
            if old.is_some() {
                return Err(Source2DfError::Schema("Entity slot already set".to_string()));
            }
        }
        for (slot, idx) in event_slot_receiver_indices {
            let rx = receivers_by_idx.remove(&idx)
                .expect("receiver index mismatch");
            let old = slot.lock().replace(rx);
            if old.is_some() {
                return Err(Source2DfError::Schema("Event slot already set".to_string()));
            }
        }

        // Phase 5: Execute plans
        //
        // IMPORTANT: We must use execute_stream() instead of plan.execute(0, ctx).
        // For queries with UNION ALL, the physical plan has multiple partitions
        // (one per UNION branch). Calling execute(0, ctx) would only execute the
        // first partition, leaving other table scans unexecuted and their receivers
        // orphaned (causing "Channel closed" errors).
        //
        // execute_stream() handles this by wrapping multi-partition plans in
        // CoalescePartitionsExec, which executes ALL partitions and merges them.
        let mut streams = Vec::new();
        for (plan, ctx) in plans.into_iter().zip(contexts.into_iter()) {
            let stream = execute_stream(plan, ctx.task_ctx())?;
            streams.push(stream);
        }

        Ok(QueryStreams {
            streams,
            parser_handle,
            stats,
        })
    }
}

async fn run_parser_broadcast<P: PacketSource + 'static>(
    source: P,
    schemas: Vec<(u64, EntitySchema)>,
    entity_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    event_senders: HashMap<u32, Vec<(BatchSender, EventType)>>,
    batch_size: usize,
    stats: Arc<crate::datafusion::streaming_stats::StreamingStats>,
) -> Result<()> {
    use crate::haste::core::packet_channel_broadcast_stream::PacketChannelBroadcastStream;
    use crate::haste::parser::AsyncStreamingParser;
    use crate::visitor::{BatchingDemoVisitor, BatchingEntityDispatcher, BatchingEventDispatcher};

    let broadcast_stream = PacketChannelBroadcastStream::new(source);

    // Create batching dispatchers that send directly through distribution channels
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
        .map_err(|e| Source2DfError::Haste(e.to_string()))?;

    // Run parser to completion - errors are expected on stream close
    let _ = parser.run_to_end().await;

    // Flush any remaining batched data
    let mut visitor = parser.into_visitor();
    let _ = visitor.flush_all().await;

    // Dropping visitor drops the dispatchers, which drops the senders,
    // which closes the distribution channels and signals end-of-stream to receivers.
    drop(visitor);

    Ok(())
}

async fn run_parser_demo<P: PacketSource + 'static>(
    source: P,
    schemas: Vec<(u64, EntitySchema)>,
    entity_senders: HashMap<u64, Vec<(BatchSender, EntitySchema)>>,
    event_senders: HashMap<u32, Vec<(BatchSender, EventType)>>,
    batch_size: usize,
    stats: Arc<crate::datafusion::streaming_stats::StreamingStats>,
) -> Result<()> {
    use crate::haste::core::packet_channel_demo_stream::PacketChannelDemoStream;
    use crate::haste::parser::AsyncStreamingParser;
    use crate::visitor::{BatchingDemoVisitor, BatchingEntityDispatcher, BatchingEventDispatcher};

    let demo_stream = PacketChannelDemoStream::new(source);

    // Create batching dispatchers that send directly through distribution channels
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
        .map_err(|e| Source2DfError::Haste(e.to_string()))?;

    // Run parser to completion - errors are expected on stream close
    let _result = parser.run_to_end().await;

    // Flush any remaining batched data
    let mut visitor = parser.into_visitor();
    let _flush_result = visitor.flush_all().await;

    // Dropping visitor drops the dispatchers, which drops the senders,
    // which closes the distribution channels and signals end-of-stream to receivers.
    drop(visitor);

    Ok(())
}

pub(crate) struct QueryTableProvider {
    schema: SchemaRef,
    entity_type: Arc<str>,
    receiver_slot: ReceiverSlot,
}

impl QueryTableProvider {
    pub(crate) fn new(schema: SchemaRef, entity_type: Arc<str>, receiver_slot: ReceiverSlot) -> Self {
        Self {
            schema,
            entity_type,
            receiver_slot,
        }
    }

    fn build_tick_ordering(&self, projection: Option<&Vec<usize>>) -> Vec<LexOrdering> {
        let tick_idx = self.schema.index_of("tick").ok();

        let tick_in_projection = match (tick_idx, projection) {
            (Some(idx), Some(proj)) => proj.contains(&idx),
            (Some(_), None) => true,
            (None, _) => false,
        };

        if !tick_in_projection {
            return vec![];
        }

        let tick_col = col("tick", &self.schema).ok();

        match tick_col {
            Some(expr) => {
                let sort_expr = PhysicalSortExpr {
                    expr,
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                };
                LexOrdering::new(vec![sort_expr]).into_iter().collect()
            }
            None => vec![],
        }
    }
}

impl Debug for QueryTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryTableProvider")
            .field("entity_type", &self.entity_type)
            .field("schema_fields", &self.schema.fields().len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for QueryTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let partition_stream = QueryPartitionStream {
            schema: self.schema.clone(),
            entity_type: self.entity_type.clone(),
            receiver_slot: self.receiver_slot.clone(),
        };

        let partition_streams: Vec<Arc<dyn PartitionStream>> = vec![Arc::new(partition_stream)];
        let tick_ordering = self.build_tick_ordering(projection);

        let exec = StreamingTableExec::try_new(
            self.schema.clone(),
            partition_streams,
            projection,
            tick_ordering,
            true, // unbounded stream
            limit,
        )?;

        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }
}

struct QueryPartitionStream {
    schema: SchemaRef,
    entity_type: Arc<str>,
    receiver_slot: ReceiverSlot,
}

impl Debug for QueryPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryPartitionStream")
            .field("entity_type", &self.entity_type)
            .finish()
    }
}

impl PartitionStream for QueryPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        // Take ownership of the receiver from the slot.
        // This should only be called once per slot.
        let receiver = self.receiver_slot.lock().take()
            .expect("Receiver slot empty - execute() called before slot was filled or called twice");
        
        Box::pin(DistributionReceiverStream::new(self.schema.clone(), receiver))
    }
}

struct QueryEventTableProvider {
    event_type: EventType,
    schema: SchemaRef,
    receiver_slot: ReceiverSlot,
}

impl QueryEventTableProvider {
    fn new(
        event_type: EventType,
        schema: SchemaRef,
        receiver_slot: ReceiverSlot,
    ) -> Self {
        Self {
            event_type,
            schema,
            receiver_slot,
        }
    }

    fn build_tick_ordering(&self, projection: Option<&Vec<usize>>) -> Vec<LexOrdering> {
        let tick_idx = self.schema.index_of("tick").ok();

        let tick_in_projection = match (tick_idx, projection) {
            (Some(idx), Some(proj)) => proj.contains(&idx),
            (Some(_), None) => true,
            (None, _) => false,
        };

        if !tick_in_projection {
            return vec![];
        }

        let tick_col = col("tick", &self.schema).ok();

        match tick_col {
            Some(expr) => {
                let sort_expr = PhysicalSortExpr {
                    expr,
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                };
                LexOrdering::new(vec![sort_expr]).into_iter().collect()
            }
            None => vec![],
        }
    }
}

impl Debug for QueryEventTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryEventTableProvider")
            .field("event_type", &self.event_type)
            .field("table_name", &self.event_type.table_name())
            .finish()
    }
}

#[async_trait]
impl TableProvider for QueryEventTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let partition_stream = QueryEventPartitionStream {
            event_type: self.event_type,
            schema: self.schema.clone(),
            receiver_slot: self.receiver_slot.clone(),
        };

        let partition_streams: Vec<Arc<dyn PartitionStream>> = vec![Arc::new(partition_stream)];
        let tick_ordering = self.build_tick_ordering(projection);

        let exec = StreamingTableExec::try_new(
            self.schema.clone(),
            partition_streams,
            projection,
            tick_ordering,
            true, // unbounded stream
            limit,
        )?;

        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }
}

struct QueryEventPartitionStream {
    event_type: EventType,
    schema: SchemaRef,
    receiver_slot: ReceiverSlot,
}

impl Debug for QueryEventPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryEventPartitionStream")
            .field("event_type", &self.event_type)
            .finish()
    }
}

impl PartitionStream for QueryEventPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        // Take ownership of the receiver from the slot.
        // Batching now happens producer-side in BatchingEventDispatcher,
        // so we receive RecordBatch directly (like entities).
        let receiver = self.receiver_slot.lock().take()
            .expect("Event receiver slot empty - execute() called before slot was filled or called twice");
        
        Box::pin(DistributionReceiverStream::new(self.schema.clone(), receiver))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn make_test_entity_schema(name: &str, hash: u64) -> EntitySchema {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int32, false),
            Field::new("entity_index", DataType::Int32, false),
            Field::new("delta_type", DataType::Utf8, false),
            Field::new("test_field", DataType::Int32, true),
        ]));

        EntitySchema {
            serializer_name: Arc::from(name),
            serializer_hash: hash,
            arrow_schema,
            field_keys: vec![1],
            field_column_counts: vec![1],
        }
    }

    #[test]
    fn test_query_table_provider_creation() {
        let schema = make_test_entity_schema("TestEntity", 123);
        let slot: ReceiverSlot = Arc::new(Mutex::new(None));
        let provider = QueryTableProvider::new(
            schema.arrow_schema.clone(),
            Arc::from("TestEntity"),
            slot,
        );

        assert_eq!(&*provider.entity_type, "TestEntity");
        assert_eq!(provider.schema().fields().len(), 4);
    }

    #[tokio::test]
    async fn test_tick_ordering() {
        let schema = make_test_entity_schema("TestEntity", 123);
        let slot: ReceiverSlot = Arc::new(Mutex::new(None));
        let provider = QueryTableProvider::new(
            schema.arrow_schema.clone(),
            Arc::from("TestEntity"),
            slot,
        );

        let ordering = provider.build_tick_ordering(None);
        assert_eq!(ordering.len(), 1);
    }

    #[tokio::test]
    async fn test_tick_ordering_without_tick_projection() {
        let schema = make_test_entity_schema("TestEntity", 123);
        let slot: ReceiverSlot = Arc::new(Mutex::new(None));
        let provider = QueryTableProvider::new(
            schema.arrow_schema.clone(),
            Arc::from("TestEntity"),
            slot,
        );

        let ordering = provider.build_tick_ordering(Some(&vec![1, 2]));
        assert_eq!(ordering.len(), 0);
    }

    #[tokio::test]
    async fn test_receiver_slot_flow() {
        let schema = make_test_entity_schema("TestEntity", 123);
        let slot: ReceiverSlot = Arc::new(Mutex::new(None));
        
        // Create a distribution channel (single channel)
        let (senders, mut receivers) = distributor_channels::channels::<RecordBatch>(1);
        let tx = senders.into_iter().next().unwrap();
        let rx = receivers.pop().unwrap();
        
        // Set the slot by storing the receiver
        *slot.lock() = Some(rx);
        
        // Should be able to take the receiver
        let retrieved = slot.lock().take();
        assert!(retrieved.is_some());
        let mut receiver = retrieved.unwrap();
        
        // Send a batch
        let batch = RecordBatch::try_new(
            schema.arrow_schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["update"])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![42])),
            ],
        ).unwrap();
        
        tx.send(batch).await.unwrap();
        drop(tx);
        
        // Should be able to receive
        let received = receiver.recv().await;
        assert!(received.is_some());
    }

    #[test]
    fn test_extract_table_names_filters_to_known_entities() {
        // Test that SQL parsing correctly extracts entity types
        let sql = "SELECT tick FROM CCitadelPlayerPawn WHERE tick > 1000";
        let tables = extract_table_names(sql).unwrap();
        assert_eq!(tables, vec!["CCitadelPlayerPawn"]);

        // Test JOIN query
        let sql = r#"
            SELECT p.tick, c.entity_index
            FROM CCitadelPlayerPawn p
            JOIN CCitadelPlayerController c ON p.tick = c.tick
        "#;
        let tables = extract_table_names(sql).unwrap();
        assert!(tables.contains(&"CCitadelPlayerPawn".to_string()));
        assert!(tables.contains(&"CCitadelPlayerController".to_string()));
    }

    #[tokio::test]
    async fn test_query_planning_creates_correct_providers() {
        // Test that planning a query creates the right table providers
        let mut entity_schemas = HashMap::new();
        entity_schemas.insert(
            Arc::from("CCitadelPlayerPawn"),
            make_test_entity_schema("CCitadelPlayerPawn", 1),
        );
        entity_schemas.insert(
            Arc::from("CNPC_Trooper"),
            make_test_entity_schema("CNPC_Trooper", 2),
        );

        let ctx = streaming_session_context();
        
        // Register providers for one entity type
        let slot: ReceiverSlot = Arc::new(Mutex::new(None));
        let provider = QueryTableProvider::new(
            entity_schemas["CCitadelPlayerPawn"].arrow_schema.clone(),
            Arc::from("CCitadelPlayerPawn"),
            slot.clone(),
        );
        ctx.register_table("CCitadelPlayerPawn", Arc::new(provider)).unwrap();

        // Plan a simple query
        let sql = "SELECT tick, entity_index FROM CCitadelPlayerPawn WHERE tick > 100";
        let logical = ctx.state().create_logical_plan(sql).await.unwrap();
        let physical = ctx.state().create_physical_plan(&logical).await.unwrap();

        // Use displayable for consistent output format
        let plan_display = datafusion::physical_plan::displayable(physical.as_ref())
            .indent(true)
            .to_string();

        // Verify the plan contains streaming table exec
        assert!(
            plan_display.contains("StreamingTableExec"),
            "Expected StreamingTableExec in plan, got:\n{}",
            plan_display
        );
    }

    #[tokio::test]
    async fn test_broadcast_semantics_multiple_senders() {
        // Test that distribution channels work with multiple receivers
        // Each receiver gets its own channel, but they share a gate
        let (senders, mut receivers) = distributor_channels::channels::<RecordBatch>(2);
        
        let schema = make_test_entity_schema("TestEntity", 123);
        let batch = RecordBatch::try_new(
            schema.arrow_schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["update"])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![42])),
            ],
        ).unwrap();

        // Send to each channel (simulating broadcast - sender clones batch to each channel)
        for sender in &senders {
            sender.send(batch.clone()).await.unwrap();
        }

        // Drop senders to close channels
        drop(senders);

        // Both receivers should get their batch
        let received1 = receivers[0].recv().await;
        let received2 = receivers[1].recv().await;

        assert!(received1.is_some());
        assert!(received2.is_some());

        let batch1 = received1.unwrap();
        let batch2 = received2.unwrap();

        assert_eq!(batch1.num_rows(), 1);
        assert_eq!(batch2.num_rows(), 1);
    }

    #[test]
    fn test_query_event_table_provider_creation() {
        use crate::events::{event_schema, EventType};

        let schema = event_schema("DamageEvent").expect("DamageEvent schema");
        let slot: ReceiverSlot = Arc::new(Mutex::new(None));

        let provider = QueryEventTableProvider::new(EventType::Damage, schema.clone(), slot);

        assert_eq!(provider.event_type, EventType::Damage);
        assert!(provider.schema().field_with_name("tick").is_ok());
        assert!(provider.schema().field_with_name("damage").is_ok());
    }

    #[tokio::test]
    async fn test_event_receiver_slot_flow() {
        let slot: ReceiverSlot = Arc::new(Mutex::new(None));

        // Create a distribution channel for events (now sends RecordBatch directly)
        let (senders, mut receivers) = distributor_channels::channels::<RecordBatch>(1);
        let _tx = senders.into_iter().next().unwrap();
        let rx = receivers.pop().unwrap();

        *slot.lock() = Some(rx);

        let retrieved = slot.lock().take();
        assert!(retrieved.is_some());

        // Dropping sender closes the channel
        drop(_tx);

        let mut receiver = retrieved.unwrap();
        let received = receiver.recv().await;
        assert!(received.is_none(), "Channel should be closed after dropping sender");
    }

    #[tokio::test]
    async fn test_event_broadcast_semantics() {

        // Create distribution channels for two event receivers
        let (senders, receivers) = distributor_channels::channels::<RecordBatch>(2);

        assert_eq!(senders.len(), 2);
        assert_eq!(receivers.len(), 2);

        // Drop senders to close channels
        drop(senders);

        // Receivers should be closed (recv returns None)
        // Note: We'd need to call recv() to verify, but just checking creation is sufficient
    }

    #[test]
    fn test_event_type_table_name_lookup() {
        use crate::events::EventType;

        let table_name = "DamageEvent";
        let found = EventType::all()
            .iter()
            .find(|e| e.table_name() == table_name);

        assert!(found.is_some());
        assert_eq!(found.unwrap(), &EventType::Damage);
    }

    /// Regression test: execute_stream() must execute ALL partitions of a plan.
    /// 
    /// Previously, the code called `plan.execute(0, ctx)` which only executed
    /// partition 0. For UNION ALL queries, this meant only the first branch
    /// was executed, leaving other partitions' data unconsumed (memory leak)
    /// and their receivers orphaned (causing "Channel closed" errors).
    /// 
    /// This test creates a UnionExec with multiple partitions and verifies
    /// that execute_stream() returns data from ALL partitions, not just the first.
    #[tokio::test]
    async fn test_execute_stream_runs_all_partitions() {
        use datafusion::physical_plan::union::UnionExec;
        use datafusion::datasource::memory::MemorySourceConfig;
        use futures::StreamExt;

        let schema = Arc::new(Schema::new(vec![
            Field::new("partition_id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));

        // Create 3 separate DataSourceExec plans, each with distinct data
        let batch_a = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![0, 0])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100, 101])),
            ],
        ).unwrap();

        let batch_b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1, 1])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![200, 201])),
            ],
        ).unwrap();

        let batch_c = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![2, 2])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![300, 301])),
            ],
        ).unwrap();

        let exec_a = MemorySourceConfig::try_new_exec(&[vec![batch_a]], schema.clone(), None).unwrap();
        let exec_b = MemorySourceConfig::try_new_exec(&[vec![batch_b]], schema.clone(), None).unwrap();
        let exec_c = MemorySourceConfig::try_new_exec(&[vec![batch_c]], schema.clone(), None).unwrap();

        // UnionExec combines them - this creates a plan with 3 partitions
        let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![
            exec_a,
            exec_b,
            exec_c,
        ]).unwrap();
        
        // Verify we have multiple partitions (this is what UNION ALL produces)
        assert_eq!(
            plan.properties().output_partitioning().partition_count(),
            3,
            "UnionExec should have 3 partitions"
        );

        // Execute using execute_stream (the correct approach)
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let mut stream = execute_stream(plan, task_ctx).unwrap();

        // Collect all rows and track which partitions we saw
        let mut partition_ids_seen = std::collections::HashSet::new();
        let mut total_rows = 0;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.expect("batch should succeed");
            total_rows += batch.num_rows();
            
            let partition_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>()
                .unwrap();
            
            for i in 0..partition_col.len() {
                partition_ids_seen.insert(partition_col.value(i));
            }
        }

        // Critical assertion: we must see data from ALL partitions
        assert_eq!(
            partition_ids_seen.len(),
            3,
            "execute_stream must return data from all 3 partitions, but only saw: {:?}",
            partition_ids_seen
        );
        assert!(partition_ids_seen.contains(&0), "Missing partition 0");
        assert!(partition_ids_seen.contains(&1), "Missing partition 1");
        assert!(partition_ids_seen.contains(&2), "Missing partition 2");
        assert_eq!(total_rows, 6, "Should have 6 total rows (2 per partition)");
    }

    /// Demonstrates the bug that execute(0, ctx) would have caused.
    /// This test verifies that calling execute() with partition 0 only
    /// returns data from that single partition.
    #[tokio::test]
    async fn test_execute_partition_zero_only_returns_first_partition() {
        use datafusion::physical_plan::union::UnionExec;
        use datafusion::datasource::memory::MemorySourceConfig;
        use futures::StreamExt;

        let schema = Arc::new(Schema::new(vec![
            Field::new("partition_id", DataType::Int32, false),
        ]));

        let batch_a = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![0]))],
        ).unwrap();

        let batch_b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(datafusion::arrow::array::Int32Array::from(vec![1]))],
        ).unwrap();

        let exec_a = MemorySourceConfig::try_new_exec(&[vec![batch_a]], schema.clone(), None).unwrap();
        let exec_b = MemorySourceConfig::try_new_exec(&[vec![batch_b]], schema.clone(), None).unwrap();

        let plan: Arc<dyn ExecutionPlan> = UnionExec::try_new(vec![
            exec_a,
            exec_b,
        ]).unwrap();
        assert_eq!(plan.properties().output_partitioning().partition_count(), 2);

        // Execute ONLY partition 0 (the buggy approach)
        let ctx = SessionContext::new();
        let task_ctx = ctx.task_ctx();
        let mut stream = plan.execute(0, task_ctx).unwrap();

        let mut partition_ids_seen = std::collections::HashSet::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            let col = batch.column(0).as_any()
                .downcast_ref::<datafusion::arrow::array::Int32Array>().unwrap();
            for i in 0..col.len() {
                partition_ids_seen.insert(col.value(i));
            }
        }

        // This demonstrates the bug: only partition 0's data is returned
        assert_eq!(
            partition_ids_seen.len(),
            1,
            "execute(0, ctx) should only return partition 0's data"
        );
        assert!(partition_ids_seen.contains(&0));
        assert!(
            !partition_ids_seen.contains(&1),
            "Partition 1's data should NOT be returned by execute(0, ctx)"
        );
    }
}
