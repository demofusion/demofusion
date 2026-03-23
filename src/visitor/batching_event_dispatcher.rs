//! Batching event dispatcher for producer-side batching with distribution channels.
//!
//! This module provides event routing that batches events at the producer side
//! and sends RecordBatch through DistributionSender. This works with the distribution
//! channel's gate mechanism to prevent JOIN deadlocks.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use tracing::{debug, trace};

use crate::datafusion::distributor_channels::DistributionSender;
use crate::datafusion::streaming_stats::StreamingStats;
use crate::events::{EventBatchBuilder, EventType, decode_event};

use super::arrow_visitor::ArrowVisitorError;

struct SenderWithBuilder {
    sender: DistributionSender<RecordBatch>,
    builder: EventBatchBuilder,
    #[allow(dead_code)]
    event_type: EventType,
}

pub struct BatchingEventDispatcher {
    senders: HashMap<u32, Vec<SenderWithBuilder>>,
    _batch_size: usize,
    stats: Option<Arc<StreamingStats>>,
}

impl BatchingEventDispatcher {
    pub fn new(
        senders: HashMap<u32, Vec<(DistributionSender<RecordBatch>, EventType)>>,
        batch_size: usize,
    ) -> Self {
        Self::new_with_stats(senders, batch_size, None)
    }

    pub fn new_with_stats(
        senders: HashMap<u32, Vec<(DistributionSender<RecordBatch>, EventType)>>,
        batch_size: usize,
        stats: Option<Arc<StreamingStats>>,
    ) -> Self {
        let senders = senders
            .into_iter()
            .map(|(message_id, sender_list)| {
                let senders_with_builders = sender_list
                    .into_iter()
                    .map(|(sender, event_type)| SenderWithBuilder {
                        sender,
                        builder: EventBatchBuilder::new(event_type, batch_size),
                        event_type,
                    })
                    .collect();
                (message_id, senders_with_builders)
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

    /// Returns true if there are any active senders (receivers still listening).
    pub fn has_active_senders(&self) -> bool {
        self.senders.values().any(|v| !v.is_empty())
    }

    pub async fn send(
        &mut self,
        tick: i32,
        packet_type: u32,
        data: &[u8],
    ) -> Result<(), ArrowVisitorError> {
        if let Some(sender_list) = self.senders.get_mut(&packet_type)
            && let Some(event) = decode_event(packet_type, data)
        {
            // Record row production (once per event, not per sender)
            if let Some(stats) = &self.stats {
                stats.record_rows_produced(1);
            }

            // Track which senders failed (receiver dropped) so we can remove them
            let mut failed_indices = Vec::new();

            for (idx, swb) in sender_list.iter_mut().enumerate() {
                swb.builder.append(tick, &event);

                if swb.builder.should_flush() {
                    let batch = swb
                        .builder
                        .flush()
                        .map_err(|e| ArrowVisitorError::BatchError(e.to_string()))?;

                    let rows = batch.num_rows();
                    let gate_blocked = swb.sender.is_gate_blocked();

                    if let Some(stats) = &self.stats {
                        stats.record_batch_sent(rows as u64);
                        if gate_blocked {
                            stats.record_gate_blocked();
                        }
                    }

                    trace!(target: "demofusion::dispatcher", rows, gate_blocked, "sending event batch");

                    if swb.sender.send(batch).await.is_err() {
                        // Receiver dropped - mark for removal
                        failed_indices.push(idx);
                        continue;
                    }

                    // Yield after sending a batch to allow consumers to process.
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
        let mut total_flushed = 0;
        let mut total_rows = 0;

        for (message_id, sender_list) in self.senders.iter_mut() {
            let mut failed_indices = Vec::new();

            for (idx, swb) in sender_list.iter_mut().enumerate() {
                if swb.builder.has_data() {
                    let batch = swb
                        .builder
                        .flush()
                        .map_err(|e| ArrowVisitorError::BatchError(e.to_string()))?;

                    let num_rows = batch.num_rows();
                    total_rows += num_rows;
                    total_flushed += 1;

                    debug!(
                        target: "demofusion::dispatcher",
                        message_id,
                        num_rows,
                        "flush_all: flushing partial event batch"
                    );

                    // Record final batch stats
                    if let Some(stats) = &self.stats {
                        stats.record_batch_sent(num_rows as u64);
                    }

                    if swb.sender.send(batch).await.is_err() {
                        // Receiver dropped - mark for removal
                        failed_indices.push(idx);
                    }
                }
            }

            // Remove failed senders in reverse order to preserve indices
            for idx in failed_indices.into_iter().rev() {
                sender_list.swap_remove(idx);
            }
        }

        debug!(
            target: "demofusion::dispatcher",
            total_flushed,
            total_rows,
            "flush_all: event flush complete"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion::distributor_channels::channels;
    use crate::events::EventType;

    #[test]
    fn test_dispatcher_creation() {
        let (senders, _receivers) = channels::<RecordBatch>(1);

        let mut sender_map = HashMap::new();
        sender_map.insert(
            EventType::Damage.message_id(),
            vec![(senders[0].clone(), EventType::Damage)],
        );

        let dispatcher = BatchingEventDispatcher::new(sender_map, 100);

        assert!(!dispatcher.is_empty());
    }

    #[test]
    fn test_empty_dispatcher() {
        let dispatcher = BatchingEventDispatcher::new(HashMap::new(), 100);
        assert!(dispatcher.is_empty());
    }

    #[test]
    fn test_has_active_senders_with_senders() {
        let (senders, _receivers) = channels::<RecordBatch>(2);

        let mut sender_map = HashMap::new();
        sender_map.insert(
            EventType::Damage.message_id(),
            vec![(senders[0].clone(), EventType::Damage)],
        );
        sender_map.insert(
            EventType::HeroKilled.message_id(),
            vec![(senders[1].clone(), EventType::HeroKilled)],
        );

        let dispatcher = BatchingEventDispatcher::new(sender_map, 100);
        assert!(dispatcher.has_active_senders());
    }

    #[test]
    fn test_empty_dispatcher_has_no_active_senders() {
        let dispatcher = BatchingEventDispatcher::new(HashMap::new(), 100);
        assert!(!dispatcher.has_active_senders());
    }

    #[tokio::test]
    async fn test_flush_all_survives_dropped_receiver() {
        // Create two event types, each with a sender
        let (senders, receivers) = channels::<RecordBatch>(2);

        let mut sender_map = HashMap::new();
        sender_map.insert(
            EventType::Damage.message_id(),
            vec![(senders[0].clone(), EventType::Damage)],
        );
        sender_map.insert(
            EventType::HeroKilled.message_id(),
            vec![(senders[1].clone(), EventType::HeroKilled)],
        );

        let mut dispatcher = BatchingEventDispatcher::new(sender_map, 100);
        drop(senders); // drop original sender refs

        // Drop one receiver — simulates a consumer that finished early (e.g., LIMIT reached)
        let mut receivers = receivers;
        let _kept_receiver = receivers.remove(1); // keep HeroKilled receiver
        let dropped_receiver = receivers.remove(0); // drop Damage receiver
        drop(dropped_receiver);

        // flush_all should NOT return an error even though one receiver is gone.
        // (The dispatcher has no pending data, so this is a no-op flush, but it
        // exercises the code path without needing protobuf-encoded event data.)
        let result = dispatcher.flush_all().await;
        assert!(result.is_ok(), "flush_all should succeed even with a dropped receiver");

        // The dispatcher should still report active senders (HeroKilled is alive)
        assert!(
            dispatcher.has_active_senders(),
            "should still have active senders after one receiver dropped"
        );
    }
}
