pub mod arrow_visitor;
pub mod batching_entity_dispatcher;
pub mod batching_event_dispatcher;
pub mod schema_discovery;

pub use arrow_visitor::{ArrowVisitorError, BatchingDemoVisitor};
pub use batching_entity_dispatcher::BatchingEntityDispatcher;
pub use batching_event_dispatcher::BatchingEventDispatcher;
pub use schema_discovery::{
    DiscoveredSchemas, SchemaDiscoveryError, SchemaDiscoveryVisitor,
    discover_schemas_from_broadcast, discover_schemas_from_demo,
};

/// Trait for visitors that need explicit flushing at end-of-stream.
///
/// This allows different visitor implementations to have custom flush behavior
/// while maintaining a common interface for the execution layer.
#[async_trait::async_trait]
pub trait FlushableVisitor: Send {
    async fn flush_all(&mut self) -> std::result::Result<(), ArrowVisitorError>;
}

#[async_trait::async_trait]
impl FlushableVisitor for BatchingDemoVisitor {
    async fn flush_all(&mut self) -> std::result::Result<(), ArrowVisitorError> {
        BatchingDemoVisitor::flush_all(self).await
    }
}
