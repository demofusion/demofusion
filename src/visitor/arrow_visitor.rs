use crate::haste::entities::{DeltaHeader, Entity};
use crate::haste::parser::{Context, Visitor};

use super::batching_entity_dispatcher::BatchingEntityDispatcher;
use super::batching_event_dispatcher::BatchingEventDispatcher;

#[derive(Debug)]
pub enum ArrowVisitorError {
    ChannelClosed,
    BatchError(String),
}

impl std::fmt::Display for ArrowVisitorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowVisitorError::ChannelClosed => write!(f, "Channel closed"),
            ArrowVisitorError::BatchError(msg) => write!(f, "Batch error: {}", msg),
        }
    }
}

impl std::error::Error for ArrowVisitorError {}

/// Visitor using distribution channels with producer-side batching.
///
/// Uses distribution channels with a global gate that prevents JOIN deadlocks
/// while providing memory backpressure.
///
/// Each entity/event type can have multiple receivers (for broadcast to multiple queries),
/// and the batching dispatchers handle batching at the producer side before sending through
/// the distribution channels.
pub struct BatchingDemoVisitor {
    entity_dispatcher: BatchingEntityDispatcher,
    event_dispatcher: Option<BatchingEventDispatcher>,
    tracked_hashes: std::collections::HashSet<u64>,
    last_flush_tick: i32,
}

/// Flush partial batches every N ticks to ensure sparse streams make progress.
/// 64 ticks ≈ 1 second at standard tick rate.
const FLUSH_INTERVAL_TICKS: i32 = 64;

impl BatchingDemoVisitor {
    pub fn new(
        entity_dispatcher: BatchingEntityDispatcher,
        event_dispatcher: Option<BatchingEventDispatcher>,
        schemas: &[(u64, crate::schema::EntitySchema)],
    ) -> Self {
        let tracked_hashes = schemas.iter().map(|(hash, _)| *hash).collect();
        
        Self {
            entity_dispatcher,
            event_dispatcher,
            tracked_hashes,
            last_flush_tick: 0,
        }
    }

    pub async fn flush_all(&mut self) -> std::result::Result<(), ArrowVisitorError> {
        self.entity_dispatcher.flush_all().await?;
        
        if let Some(ref mut event_dispatcher) = self.event_dispatcher {
            event_dispatcher.flush_all().await?;
        }
        
        Ok(())
    }
}

impl Visitor for BatchingDemoVisitor {
    type Error = ArrowVisitorError;

    fn should_track_entity(&self, serializer_name_hash: u64) -> bool {
        self.tracked_hashes.contains(&serializer_name_hash)
    }

    async fn on_entity(
        &mut self,
        ctx: &Context,
        delta_header: DeltaHeader,
        entity: &Entity,
    ) -> std::result::Result<(), Self::Error> {
        self.entity_dispatcher
            .send(ctx.tick(), delta_header, entity)
            .await
    }

    async fn on_packet(
        &mut self,
        ctx: &Context,
        packet_type: u32,
        data: &[u8],
    ) -> std::result::Result<(), Self::Error> {
        if let Some(ref mut event_dispatcher) = self.event_dispatcher {
            event_dispatcher.send(ctx.tick(), packet_type, data).await?;
        }
        Ok(())
    }

    async fn on_tick_end(
        &mut self,
        ctx: &Context,
    ) -> std::result::Result<(), Self::Error> {
        let tick = ctx.tick();
        
        if tick - self.last_flush_tick >= FLUSH_INTERVAL_TICKS {
            self.flush_all().await?;
            self.last_flush_tick = tick;
        }
        
        Ok(())
    }
}
