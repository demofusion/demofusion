//! Batching event dispatcher for producer-side batching with distribution channels.
//!
//! This module provides event routing that batches events at the producer side
//! and sends RecordBatch through DistributionSender. This works with the distribution
//! channel's gate mechanism to prevent JOIN deadlocks.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;

use crate::datafusion::distributor_channels::DistributionSender;
use crate::datafusion::streaming_stats::StreamingStats;
use crate::events::{decode_event, EventBatchBuilder, EventType};

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

        Self { senders, _batch_size: batch_size, stats }
    }

    pub fn is_empty(&self) -> bool {
        self.senders.is_empty()
    }

    pub async fn send(
        &mut self,
        tick: i32,
        packet_type: u32,
        data: &[u8],
    ) -> Result<(), ArrowVisitorError> {
        if let Some(sender_list) = self.senders.get_mut(&packet_type) {
            if let Some(event) = decode_event(packet_type, data) {
                // Record row production (once per event, not per sender)
                if let Some(stats) = &self.stats {
                    stats.record_rows_produced(1);
                }

                for swb in sender_list.iter_mut() {
                    swb.builder.append(tick, &event);

                    if swb.builder.should_flush() {
                        let batch = swb.builder.flush()
                            .map_err(|e| ArrowVisitorError::BatchError(e.to_string()))?;
                        
                        // Record batch stats before sending
                        if let Some(stats) = &self.stats {
                            stats.record_batch_sent(batch.num_rows() as u64);
                            if swb.sender.is_gate_blocked() {
                                stats.record_gate_blocked();
                            }
                        }

                        swb.sender
                            .send(batch)
                            .await
                            .map_err(|_| ArrowVisitorError::ChannelClosed)?;
                        
                        // Yield after sending a batch to allow consumers to process.
                        tokio::task::yield_now().await;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn flush_all(&mut self) -> Result<(), ArrowVisitorError> {
        for (_message_id, sender_list) in self.senders.iter_mut() {
            for swb in sender_list.iter_mut() {
                if swb.builder.has_data() {
                    let batch = swb.builder.flush()
                        .map_err(|e| ArrowVisitorError::BatchError(e.to_string()))?;
                    
                    // Record final batch stats
                    if let Some(stats) = &self.stats {
                        stats.record_batch_sent(batch.num_rows() as u64);
                    }

                    swb.sender
                        .send(batch)
                        .await
                        .map_err(|_| ArrowVisitorError::ChannelClosed)?;
                }
            }
        }
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
}
