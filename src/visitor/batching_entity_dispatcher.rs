//! Batching entity dispatcher for producer-side batching with distribution channels.
//!
//! This module provides entity routing that batches updates at the producer side
//! and sends RecordBatch through DistributionSender. This works with the distribution
//! channel's gate mechanism to prevent JOIN deadlocks.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use tracing::{error, trace};

use crate::batch::EntityBatchBuilder;
use crate::datafusion::distributor_channels::DistributionSender;
use crate::datafusion::stream_fault::StreamFault;
use crate::datafusion::streaming_stats::StreamingStats;
use crate::haste::entities::{DeltaHeader, Entity};
use crate::schema::EntitySchema;

use super::arrow_visitor::ArrowVisitorError;

struct SenderWithBuilder {
    sender: DistributionSender<RecordBatch>,
    builder: EntityBatchBuilder,
    /// This channel's fault slot. Set when the batch builder fails so that the
    /// consumer sees an error rather than a stream that simply stops.
    fault: StreamFault,
}

pub struct BatchingEntityDispatcher {
    senders: HashMap<u64, Vec<SenderWithBuilder>>,
    _batch_size: usize,
    stats: Option<Arc<StreamingStats>>,
}

impl BatchingEntityDispatcher {
    pub fn new(
        senders: HashMap<u64, Vec<(DistributionSender<RecordBatch>, EntitySchema, StreamFault)>>,
        batch_size: usize,
    ) -> Self {
        Self::new_with_stats(senders, batch_size, None)
    }

    pub fn new_with_stats(
        senders: HashMap<u64, Vec<(DistributionSender<RecordBatch>, EntitySchema, StreamFault)>>,
        batch_size: usize,
        stats: Option<Arc<StreamingStats>>,
    ) -> Self {
        let senders = senders
            .into_iter()
            .map(|(hash, sender_list)| {
                let senders_with_builders = sender_list
                    .into_iter()
                    .map(|(sender, schema, fault)| SenderWithBuilder {
                        sender,
                        builder: EntityBatchBuilder::new(&schema, batch_size),
                        fault,
                    })
                    .collect();
                (hash, senders_with_builders)
            })
            .collect();

        Self {
            senders,
            _batch_size: batch_size,
            stats,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.senders.is_empty()
    }

    pub fn has_entity_type(&self, serializer_hash: u64) -> bool {
        self.senders.contains_key(&serializer_hash)
    }

    /// Returns true if there are any active senders (receivers still listening).
    pub fn has_active_senders(&self) -> bool {
        self.senders.values().any(|v| !v.is_empty())
    }

    pub async fn send(
        &mut self,
        tick: i32,
        delta_header: DeltaHeader,
        entity: &Entity,
    ) -> Result<(), ArrowVisitorError> {
        let serializer_hash = entity.serializer().serializer_name.hash;

        if let Some(sender_list) = self.senders.get_mut(&serializer_hash) {
            let entity_index = entity.index();

            // Record row production (once per entity, not per sender)
            if let Some(stats) = &self.stats {
                stats.record_rows_produced(1);
            }

            // Track which senders failed (receiver dropped) so we can remove them
            let mut failed_indices = Vec::new();

            for (idx, swb) in sender_list.iter_mut().enumerate() {
                swb.builder
                    .append_entity(tick, entity_index, delta_header, entity);

                if swb.builder.should_flush() {
                    // A builder failure belongs to THIS channel only. Failing
                    // the whole parse here used to truncate every other
                    // registered query along with it, so instead record the
                    // fault (the consumer will see an error, not a clean end)
                    // and retire just this sender.
                    let batch = match swb.builder.flush() {
                        Ok(batch) => batch,
                        Err(e) => {
                            error!(
                                target: "demofusion::dispatcher",
                                serializer_hash,
                                error = %e,
                                "entity batch builder failed; failing this stream only"
                            );
                            swb.fault
                                .set(format!("entity batch builder failed: {e}"));
                            failed_indices.push(idx);
                            continue;
                        }
                    };

                    let rows = batch.num_rows();
                    let gate_blocked = swb.sender.is_gate_blocked();

                    if let Some(stats) = &self.stats {
                        stats.record_batch_sent(rows as u64);
                        if gate_blocked {
                            stats.record_gate_blocked();
                        }
                    }

                    trace!(target: "demofusion::dispatcher", rows, gate_blocked, "sending entity batch");

                    if swb.sender.send(batch).await.is_err() {
                        // Receiver dropped - mark for removal
                        failed_indices.push(idx);
                        continue;
                    }

                    // Yield after sending a batch to allow consumers to process.
                    // This is the natural backpressure point - we've just produced data.
                    tokio::task::yield_now().await;
                }
            }

            // Remove failed senders in reverse order to preserve indices
            for idx in failed_indices.into_iter().rev() {
                sender_list.swap_remove(idx);
            }
        }

        Ok(())
    }

    pub async fn flush_all(&mut self) -> Result<(), ArrowVisitorError> {
        trace!(target: "demofusion::dispatcher", "flushing all entity batches");
        for sender_list in self.senders.values_mut() {
            let mut failed_indices = Vec::new();

            for (idx, swb) in sender_list.iter_mut().enumerate() {
                if swb.builder.has_data() {
                    let batch = match swb.builder.flush() {
                        Ok(batch) => batch,
                        Err(e) => {
                            error!(
                                target: "demofusion::dispatcher",
                                error = %e,
                                "entity batch builder failed on final flush; failing this stream only"
                            );
                            swb.fault
                                .set(format!("entity batch builder failed: {e}"));
                            failed_indices.push(idx);
                            continue;
                        }
                    };

                    let rows = batch.num_rows();
                    if let Some(stats) = &self.stats {
                        stats.record_batch_sent(rows as u64);
                    }

                    trace!(target: "demofusion::dispatcher", rows, "sending final entity batch");

                    if swb.sender.send(batch).await.is_err() {
                        failed_indices.push(idx);
                    }
                }
            }

            for idx in failed_indices.into_iter().rev() {
                sender_list.swap_remove(idx);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::distributor_channels::channels;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_simple_entity_schema(hash: u64) -> EntitySchema {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int32, false),
            Field::new("entity_index", DataType::Int32, false),
            Field::new("delta_type", DataType::Utf8, false),
            Field::new("health", DataType::Int32, true),
        ]));

        EntitySchema {
            serializer_name: Arc::from("TestEntity"),
            serializer_hash: hash,
            arrow_schema,
            field_keys: vec![100],
            field_column_counts: vec![1],
        }
    }

    #[test]
    fn test_dispatcher_creation() {
        let (senders, _receivers) = channels::<RecordBatch>(1);
        let schema = make_simple_entity_schema(12345);

        let mut sender_map = HashMap::new();
        sender_map.insert(12345u64, vec![(senders[0].clone(), schema, StreamFault::new())]);

        let dispatcher = BatchingEntityDispatcher::new(sender_map, 100);

        assert!(!dispatcher.is_empty());
        assert!(dispatcher.has_entity_type(12345));
        assert!(!dispatcher.has_entity_type(99999));
    }

    #[test]
    fn test_empty_dispatcher() {
        let dispatcher = BatchingEntityDispatcher::new(HashMap::new(), 100);
        assert!(dispatcher.is_empty());
        assert!(!dispatcher.has_entity_type(12345));
    }

    #[tokio::test]
    async fn test_sender_detects_receiver_drop() {
        // Simple test: send to a receiver, then drop receiver, then try to send again
        let (senders, mut receivers) = channels::<RecordBatch>(1);
        let schema = make_simple_entity_schema(12345);

        let sender = senders[0].clone();
        drop(senders);

        let rx = receivers.pop().unwrap();

        // Create a batch
        let batch = RecordBatch::try_new(
            schema.arrow_schema.clone(),
            vec![
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])),
                Arc::new(datafusion::arrow::array::StringArray::from(vec!["created"])),
                Arc::new(datafusion::arrow::array::Int32Array::from(vec![100])),
            ],
        )
        .unwrap();

        // Send should succeed
        assert!(sender.send(batch.clone()).await.is_ok());
        assert_eq!(rx.queue_len(), 1);

        // Drop the receiver
        drop(rx);

        // Now send should fail
        assert!(sender.send(batch).await.is_err());
    }

    #[tokio::test]
    async fn test_has_active_senders() {
        let (senders, receivers) = channels::<RecordBatch>(2);
        let schema = make_simple_entity_schema(12345);

        let mut sender_map = HashMap::new();
        sender_map.insert(
            12345u64,
            vec![
                (senders[0].clone(), schema.clone(), StreamFault::new()),
                (senders[1].clone(), schema, StreamFault::new()),
            ],
        );

        let dispatcher = BatchingEntityDispatcher::new(sender_map, 100);
        drop(senders);

        // With senders registered, has_active_senders returns true
        assert!(dispatcher.has_active_senders());

        // Keep receivers alive
        drop(receivers);
    }

    #[test]
    fn test_empty_dispatcher_has_no_active_senders() {
        let dispatcher = BatchingEntityDispatcher::new(HashMap::new(), 100);
        assert!(!dispatcher.has_active_senders());
    }

    #[tokio::test]
    async fn test_one_finished_consumer_leaves_the_others_running() {
        // A consumer that is done (LIMIT satisfied, handle dropped) retires
        // its own sender and nothing else. The dispatcher must keep reporting
        // active senders so the parse continues for everyone still listening.
        let (senders, receivers) = channels::<RecordBatch>(2);
        let schema = make_simple_entity_schema(12345);

        let mut sender_map = HashMap::new();
        sender_map.insert(
            12345u64,
            vec![
                (senders[0].clone(), schema.clone(), StreamFault::new()),
                (senders[1].clone(), schema, StreamFault::new()),
            ],
        );
        let mut dispatcher = BatchingEntityDispatcher::new(sender_map, 100);
        drop(senders);

        let mut receivers = receivers;
        let _kept = receivers.remove(1);
        drop(receivers.remove(0));

        assert!(
            dispatcher.flush_all().await.is_ok(),
            "a departed consumer must not fail the flush for the rest"
        );
        assert!(
            dispatcher.has_active_senders(),
            "the surviving consumer must keep the parse alive"
        );
    }

    #[tokio::test]
    async fn test_flush_never_fails_the_parse_over_a_departed_consumer() {
        // The counterpart on the hot path: a send to a receiver that is gone
        // reports Ok and retires that sender. Returning Err here would abort
        // the whole parse — which is exactly how one query's ending truncated
        // every other stream in the pass.
        let (senders, receivers) = channels::<RecordBatch>(1);
        let schema = make_simple_entity_schema(12345);

        let mut sender_map = HashMap::new();
        sender_map.insert(
            12345u64,
            vec![(senders[0].clone(), schema, StreamFault::new())],
        );
        let mut dispatcher = BatchingEntityDispatcher::new(sender_map, 1);
        drop(senders);
        drop(receivers);

        assert!(
            dispatcher.flush_all().await.is_ok(),
            "a departed consumer is a normal event, not a parse failure"
        );
    }
}
