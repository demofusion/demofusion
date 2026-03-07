//! TableProvider for streaming event tables.
//!
//! `EventTableProvider` receives decoded events from a channel and converts them
//! to Arrow RecordBatches on-the-fly. Events are only decoded in the visitor if
//! a channel is registered for that event type.

use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr_common::sort_expr::LexOrdering;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, RecordBatchStream};
use futures::Stream;

use crate::events::{event_schema, DecodedEvent, EventType};

const DEFAULT_BATCH_SIZE: usize = 1024;

pub struct EventTableProvider {
    event_type: EventType,
    schema: SchemaRef,
    receiver: async_channel::Receiver<(i32, Arc<DecodedEvent>)>,
}

impl EventTableProvider {
    pub fn new(
        event_type: EventType,
        receiver: async_channel::Receiver<(i32, Arc<DecodedEvent>)>,
    ) -> Option<Self> {
        let schema = event_schema(event_type.table_name())?;
        Some(Self {
            event_type,
            schema,
            receiver,
        })
    }

    fn build_tick_ordering(&self, projection: Option<&Vec<usize>>) -> Vec<LexOrdering> {
        use datafusion::common::arrow::compute::SortOptions;
        use datafusion::physical_expr::expressions::col;

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

impl Debug for EventTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        let partition_stream = EventPartitionStream {
            event_type: self.event_type,
            schema: self.schema.clone(),
            receiver: self.receiver.clone(),
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

struct EventPartitionStream {
    event_type: EventType,
    schema: SchemaRef,
    receiver: async_channel::Receiver<(i32, Arc<DecodedEvent>)>,
}

impl Debug for EventPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventPartitionStream")
            .field("event_type", &self.event_type)
            .finish()
    }
}

impl PartitionStream for EventPartitionStream {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<datafusion::execution::TaskContext>) -> datafusion::execution::SendableRecordBatchStream {
        Box::pin(EventRecordBatchStream::new(
            self.event_type,
            self.schema.clone(),
            self.receiver.clone(),
        ))
    }
}

struct EventRecordBatchStream {
    _event_type: EventType,
    schema: SchemaRef,
    receiver: async_channel::Receiver<(i32, Arc<DecodedEvent>)>,
    builder: crate::events::EventBatchBuilder,
    finished: bool,
}

impl Unpin for EventRecordBatchStream {}

impl EventRecordBatchStream {
    fn new(
        event_type: EventType,
        schema: SchemaRef,
        receiver: async_channel::Receiver<(i32, Arc<DecodedEvent>)>,
    ) -> Self {
Self {
            _event_type: event_type,
            schema,
            receiver,
            builder: crate::events::EventBatchBuilder::new(event_type, DEFAULT_BATCH_SIZE),
            finished: false,
        }
    }

    fn flush_batch(&mut self) -> Option<DfResult<RecordBatch>> {
        if self.builder.has_data() {
            match self.builder.flush() {
                Ok(batch) => Some(Ok(batch)),
                Err(e) => Some(Err(datafusion::error::DataFusionError::External(Box::new(e)))),
            }
        } else {
            None
        }
    }
}

impl Stream for EventRecordBatchStream {
    type Item = DfResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            let recv_result = {
                let rx = &self.receiver;
                // Try to receive without blocking first
                match rx.try_recv() {
                    Ok(item) => Some(item),
                    Err(async_channel::TryRecvError::Empty) => {
                        // Need to poll the receiver
                        let mut pinned_rx = std::pin::pin!(rx.recv());
                        match pinned_rx.as_mut().poll(cx) {
                            Poll::Ready(Ok(item)) => Some(item),
                            Poll::Ready(Err(_)) => None, // Channel closed
                            Poll::Pending => {
                                // If we have pending data and are waiting, flush it
                                if self.builder.has_data() {
                                    return Poll::Ready(self.flush_batch());
                                }
                                return Poll::Pending;
                            }
                        }
                    }
                    Err(async_channel::TryRecvError::Closed) => None,
                }
            };

            match recv_result {
                Some((tick, event)) => {
                    self.builder.append(tick, &event);

                    if self.builder.should_flush() {
                        return Poll::Ready(self.flush_batch());
                    }
                    // Continue receiving more events
                }
                None => {
                    // Channel closed - flush any remaining data
                    self.finished = true;
                    if self.builder.has_data() {
                        return Poll::Ready(self.flush_batch());
                    }
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for EventRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_table_provider_creation() {
        let (_tx, rx) = async_channel::unbounded();
        let provider = EventTableProvider::new(EventType::Damage, rx);
        assert!(provider.is_some());

        let provider = provider.unwrap();
        assert_eq!(provider.event_type, EventType::Damage);
        assert!(provider.schema.field_with_name("tick").is_ok());
    }

    #[test]
    fn test_event_table_provider_schema() {
        let (_tx, rx) = async_channel::unbounded();
        let provider = EventTableProvider::new(EventType::HeroKilled, rx).unwrap();

        let schema = provider.schema();
        assert!(schema.field_with_name("tick").is_ok());
    }
}
