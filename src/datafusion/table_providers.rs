//! TableProviders for streaming entity and event tables.
//!
//! These providers receive pre-batched RecordBatches from distribution channels,
//! enabling streaming SQL queries over unbounded data sources like GOTV broadcasts
//! or demo files.

use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::arrow::compute::SortOptions;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use parking_lot::Mutex;
use tracing::debug;

use crate::datafusion::distribution_stream::DistributionReceiverStream;
use crate::datafusion::distributor_channels::DistributionReceiver;
use crate::events::EventType;

pub type BatchReceiver = DistributionReceiver<RecordBatch>;

/// Shared slot for injecting a distribution channel receiver into a PartitionStream.
///
/// Created empty by `scan()` and stored in both the provider's `pending_slots`
/// and the `PartitionStream`. At `start()` time, the session injects a receiver
/// via [`inject()`](ReceiverSlot::inject). When DataFusion executes the plan,
/// the `PartitionStream` takes ownership via [`take()`](ReceiverSlot::take).
///
/// Clone is cheap — it bumps the `Arc` reference count so both the provider
/// and the `PartitionStream` share the same underlying slot.
#[derive(Clone)]
pub struct ReceiverSlot(Arc<Mutex<Option<BatchReceiver>>>);

impl ReceiverSlot {
    /// Creates a new empty slot with no receiver.
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// Injects a receiver into the slot. Returns `Err` if the slot was already filled.
    pub fn inject(&self, receiver: BatchReceiver) -> Result<(), &'static str> {
        let mut guard = self.0.lock();
        if guard.is_some() {
            return Err("slot already filled");
        }
        *guard = Some(receiver);
        Ok(())
    }

    /// Takes the receiver out of the slot, leaving it empty.
    /// Panics if the slot is empty (execute called before inject, or called twice).
    pub fn take(&self) -> BatchReceiver {
        self.0
            .lock()
            .take()
            .expect("ReceiverSlot::take() called on empty slot — inject() not called or take() called twice")
    }
}

fn build_tick_ordering(schema: &SchemaRef, projection: Option<&Vec<usize>>) -> Vec<LexOrdering> {
    let tick_idx = schema.index_of("tick").ok();

    let tick_in_projection = match (tick_idx, projection) {
        (Some(idx), Some(proj)) => proj.contains(&idx),
        (Some(_), None) => true,
        (None, _) => false,
    };

    if !tick_in_projection {
        return vec![];
    }

    let tick_col = col("tick", schema).ok();

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

/// TableProvider for streaming entity tables (e.g., CCitadelPlayerPawn).
///
/// Receives pre-batched RecordBatches from a DistributionReceiver, enabling
/// streaming SQL queries over entities discovered from demo/GOTV data.
///
/// Each call to `scan()` creates a new empty [`ReceiverSlot`] and registers it
/// in `pending_slots`. At `start()` time, the session drains these slots to
/// determine which tables were referenced by queries and how many distribution
/// channels to create.
pub struct EntityTableProvider {
    schema: SchemaRef,
    entity_type: Arc<str>,
    pending_slots: Mutex<Vec<ReceiverSlot>>,
}

impl EntityTableProvider {
    pub fn new(schema: SchemaRef, entity_type: Arc<str>) -> Self {
        Self {
            schema,
            entity_type,
            pending_slots: Mutex::new(Vec::new()),
        }
    }

    /// Returns the entity type name (e.g., "CCitadelPlayerPawn").
    pub fn entity_type(&self) -> &Arc<str> {
        &self.entity_type
    }

    /// Drain all pending receiver slots created by `scan()` calls.
    ///
    /// Called by the session at `start()` time to discover which tables were
    /// referenced by queries and to inject distribution channel receivers.
    pub fn drain_pending_slots(&self) -> Vec<ReceiverSlot> {
        self.pending_slots.lock().drain(..).collect()
    }
}

impl Debug for EntityTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityTableProvider")
            .field("entity_type", &self.entity_type)
            .field("schema_fields", &self.schema.fields().len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for EntityTableProvider {
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
        let slot = ReceiverSlot::new();
        self.pending_slots.lock().push(slot.clone());

        let partition_stream = EntityPartitionStream {
            schema: self.schema.clone(),
            entity_type: self.entity_type.clone(),
            receiver_slot: slot,
        };

        let partition_streams: Vec<Arc<dyn PartitionStream>> = vec![Arc::new(partition_stream)];
        let tick_ordering = build_tick_ordering(&self.schema, projection);

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

struct EntityPartitionStream {
    schema: SchemaRef,
    entity_type: Arc<str>,
    receiver_slot: ReceiverSlot,
}

impl Debug for EntityPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EntityPartitionStream")
            .field("entity_type", &self.entity_type)
            .finish()
    }
}

impl PartitionStream for EntityPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let receiver = self.receiver_slot.take();

        Box::pin(DistributionReceiverStream::new(
            self.schema.clone(),
            receiver,
        ))
    }
}

/// TableProvider for streaming event tables (e.g., DamageEvent, HeroKilledEvent).
///
/// Receives pre-batched RecordBatches from a DistributionReceiver, enabling
/// streaming SQL queries over game events from demo/GOTV data.
///
/// Each call to `scan()` creates a new empty [`ReceiverSlot`] and registers it
/// in `pending_slots`. At `start()` time, the session drains these slots to
/// determine which tables were referenced by queries and how many distribution
/// channels to create.
pub struct EventTableProvider {
    event_type: EventType,
    schema: SchemaRef,
    pending_slots: Mutex<Vec<ReceiverSlot>>,
}

impl EventTableProvider {
    pub fn new(event_type: EventType, schema: SchemaRef) -> Self {
        Self {
            event_type,
            schema,
            pending_slots: Mutex::new(Vec::new()),
        }
    }

    /// Returns the event type.
    pub fn event_type(&self) -> EventType {
        self.event_type
    }

    /// Drain all pending receiver slots created by `scan()` calls.
    ///
    /// Called by the session at `start()` time to discover which tables were
    /// referenced by queries and to inject distribution channel receivers.
    pub fn drain_pending_slots(&self) -> Vec<ReceiverSlot> {
        self.pending_slots.lock().drain(..).collect()
    }
}

impl Debug for EventTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventTableProvider")
            .field("event_type", &self.event_type)
            .field("table_name", &self.event_type.table_name())
            .finish()
    }
}

#[async_trait]
impl TableProvider for EventTableProvider {
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
        let slot = ReceiverSlot::new();
        self.pending_slots.lock().push(slot.clone());

        let partition_stream = EventPartitionStream {
            event_type: self.event_type,
            schema: self.schema.clone(),
            receiver_slot: slot,
        };

        let partition_streams: Vec<Arc<dyn PartitionStream>> = vec![Arc::new(partition_stream)];
        let tick_ordering = build_tick_ordering(&self.schema, projection);

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

struct EventPartitionStream {
    event_type: EventType,
    schema: SchemaRef,
    receiver_slot: ReceiverSlot,
}

impl Debug for EventPartitionStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventPartitionStream")
            .field("event_type", &self.event_type)
            .finish()
    }
}

impl PartitionStream for EventPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        debug!(
            target: "demofusion::table",
            event_type = ?self.event_type,
            "EventPartitionStream::execute called"
        );
        let receiver = self.receiver_slot.take();

        Box::pin(DistributionReceiverStream::new(
            self.schema.clone(),
            receiver,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn make_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int32, false),
            Field::new("entity_index", DataType::Int32, false),
            Field::new("test_field", DataType::Int32, true),
        ]))
    }

    #[test]
    fn test_entity_table_provider_creation() {
        let schema = make_test_schema();
        let provider = EntityTableProvider::new(schema.clone(), Arc::from("TestEntity"));

        assert_eq!(&**provider.entity_type(), "TestEntity");
        assert_eq!(provider.schema().fields().len(), 3);
        assert!(provider.drain_pending_slots().is_empty());
    }

    #[test]
    fn test_event_table_provider_creation() {
        use crate::events::{EventType, event_schema};

        let schema = event_schema("DamageEvent").expect("DamageEvent schema");
        let provider = EventTableProvider::new(EventType::Damage, schema.clone());

        assert_eq!(provider.event_type(), EventType::Damage);
        assert!(provider.schema().field_with_name("tick").is_ok());
        assert!(provider.drain_pending_slots().is_empty());
    }

    #[tokio::test]
    async fn test_tick_ordering_included() {
        let schema = make_test_schema();
        let ordering = build_tick_ordering(&schema, None);
        assert_eq!(
            ordering.len(),
            1,
            "Should have tick ordering when tick is in schema"
        );
    }

    #[tokio::test]
    async fn test_tick_ordering_excluded_by_projection() {
        let schema = make_test_schema();
        // Project only entity_index (index 1) and test_field (index 2), excluding tick (index 0)
        let projection = vec![1, 2];
        let ordering = build_tick_ordering(&schema, Some(&projection));
        assert_eq!(
            ordering.len(),
            0,
            "Should not have tick ordering when tick not in projection"
        );
    }
}
