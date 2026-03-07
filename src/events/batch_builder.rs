use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder,
    Int32Builder, Int64Builder, ListBuilder, RecordBatch, StringBuilder, StructBuilder,
    UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};

use super::{append_event_to_builders, event_schema, DecodedEvent, EventType};

const DEFAULT_BATCH_SIZE: usize = 1024;

pub struct EventBatchBuilder {
    _event_type: EventType,
    schema: SchemaRef,
    batch_size: usize,
    tick_builder: Int32Builder,
    field_builders: Vec<Box<dyn ArrayBuilder>>,
    row_count: usize,
}

impl EventBatchBuilder {
    pub fn new(event_type: EventType, batch_size: usize) -> Self {
        let schema = event_schema(event_type.table_name()).expect("schema not found");
        let field_builders = create_field_builders(&schema, batch_size);
        Self {
            _event_type: event_type,
            schema,
            batch_size,
            tick_builder: Int32Builder::with_capacity(batch_size),
            field_builders,
            row_count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.row_count
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn has_data(&self) -> bool {
        !self.is_empty()
    }

    pub fn should_flush(&self) -> bool {
        self.row_count >= self.batch_size
    }

    pub fn append(&mut self, tick: i32, event: &DecodedEvent) {
        self.tick_builder.append_value(tick);
        self.row_count += 1;
        append_event_to_builders(event, &mut self.field_builders);
    }

    pub fn flush(&mut self) -> Result<RecordBatch, datafusion::arrow::error::ArrowError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.field_builders.len() + 1);
        columns.push(Arc::new(self.tick_builder.finish()));
        for builder in &mut self.field_builders {
            columns.push(builder.finish());
        }
        self.row_count = 0;
        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

pub struct EventBatchBuilders {
    builders: HashMap<EventType, EventBatchBuilder>,
    _batch_size: usize,
}

impl EventBatchBuilders {
    pub fn new() -> Self {
        Self::with_batch_size(DEFAULT_BATCH_SIZE)
    }

    pub fn with_batch_size(batch_size: usize) -> Self {
        let mut builders = HashMap::new();
        for event_type in EventType::all() {
            builders.insert(*event_type, EventBatchBuilder::new(*event_type, batch_size));
        }
        Self {
            builders,
            _batch_size: batch_size,
        }
    }

    pub fn append(&mut self, tick: i32, event: &DecodedEvent) {
        let event_type = event.event_type();
        if let Some(builder) = self.builders.get_mut(&event_type) {
            builder.append(tick, event);
        }
    }

    pub fn should_flush(&self, event_type: EventType) -> bool {
        self.builders
            .get(&event_type)
            .map(|b| b.should_flush())
            .unwrap_or(false)
    }

    pub fn flush(
        &mut self,
        event_type: EventType,
    ) -> Option<Result<RecordBatch, datafusion::arrow::error::ArrowError>> {
        self.builders.get_mut(&event_type).map(|b| b.flush())
    }

    pub fn flush_all(&mut self) -> Vec<(EventType, Result<RecordBatch, datafusion::arrow::error::ArrowError>)> {
        let mut results = Vec::new();
        for event_type in EventType::all() {
            if let Some(builder) = self.builders.get_mut(event_type) {
                if builder.has_data() {
                    results.push((*event_type, builder.flush()));
                }
            }
        }
        results
    }

    pub fn has_data(&self, event_type: EventType) -> bool {
        self.builders
            .get(&event_type)
            .map(|b| b.has_data())
            .unwrap_or(false)
    }

    pub fn len(&self, event_type: EventType) -> usize {
        self.builders.get(&event_type).map(|b| b.len()).unwrap_or(0)
    }
}

impl Default for EventBatchBuilders {
    fn default() -> Self {
        Self::new()
    }
}

fn create_field_builders(schema: &Schema, capacity: usize) -> Vec<Box<dyn ArrayBuilder>> {
    schema
        .fields()
        .iter()
        .skip(1) // Skip tick field
        .map(|field| create_builder_for_type(field.data_type(), capacity))
        .collect()
}

fn create_builder_for_type(data_type: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Int32 => Box::new(Int32Builder::with_capacity(capacity)),
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::UInt32 => Box::new(UInt32Builder::with_capacity(capacity)),
        DataType::UInt64 => Box::new(UInt64Builder::with_capacity(capacity)),
        DataType::Float32 => Box::new(Float32Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Binary => Box::new(BinaryBuilder::with_capacity(capacity, capacity * 32)),
        DataType::List(inner) => match inner.data_type() {
            DataType::Int32 => Box::new(ListBuilder::new(Int32Builder::new())),
            DataType::Int64 => Box::new(ListBuilder::new(Int64Builder::new())),
            DataType::UInt32 => Box::new(ListBuilder::new(UInt32Builder::new())),
            DataType::Float32 => Box::new(ListBuilder::new(Float32Builder::new())),
            DataType::Utf8 => Box::new(ListBuilder::new(StringBuilder::new())),
            _ => Box::new(ListBuilder::new(Int32Builder::new())),
        },
        DataType::Struct(fields) => {
            let child_builders: Vec<Box<dyn ArrayBuilder>> = fields
                .iter()
                .map(|f| create_builder_for_type(f.data_type(), capacity))
                .collect();
            Box::new(StructBuilder::new(fields.clone(), child_builders))
        }
        _ => Box::new(Int32Builder::with_capacity(capacity)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_batch_builder_creation() {
        // Use BossDamaged - has only scalar fields (no nested messages or lists)
        let builder = EventBatchBuilder::new(EventType::BossDamaged, 128);
        assert!(builder.is_empty());
        assert!(!builder.has_data());
        assert!(!builder.should_flush());
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn test_event_batch_builder_append_and_flush() {
        use valveprotos::deadlock::CCitadelUserMsgBossDamaged;

        let mut builder = EventBatchBuilder::new(EventType::BossDamaged, 10);

        // Create a BossDamaged event (only scalar fields)
        let msg = CCitadelUserMsgBossDamaged {
            objective_team: Some(1),
            objective_id: Some(42),
            entity_damaged: Some(100),
        };
        let event = DecodedEvent::BossDamaged(msg);

        // Append the event
        builder.append(1000, &event);
        assert_eq!(builder.len(), 1);
        assert!(builder.has_data());
        assert!(!builder.should_flush());

        // Flush and verify
        let batch = builder.flush().expect("flush should succeed");
        assert_eq!(batch.num_rows(), 1);
        assert!(builder.is_empty());

        // Verify schema has tick column and event fields
        assert!(batch.schema().field_with_name("tick").is_ok());
        assert!(batch.schema().field_with_name("objective_team").is_ok());
        assert!(batch.schema().field_with_name("entity_damaged").is_ok());
    }

    #[test]
    fn test_event_batch_builder_should_flush_at_capacity() {
        use valveprotos::deadlock::CCitadelUserMsgRejuvStatus;

        let mut builder = EventBatchBuilder::new(EventType::RejuvStatus, 5);

        let msg = CCitadelUserMsgRejuvStatus {
            killing_team: Some(2),
            player_pawn: Some(50),
            user_team: Some(1),
            event_type: Some(3),
        };
        let event = DecodedEvent::RejuvStatus(msg);

        // Add 4 events - should not trigger flush
        for i in 0..4 {
            builder.append(i, &event);
            assert!(!builder.should_flush());
        }

        // Add 5th event - should trigger flush
        builder.append(5, &event);
        assert!(builder.should_flush());
        assert_eq!(builder.len(), 5);

        let batch = builder.flush().expect("flush");
        assert_eq!(batch.num_rows(), 5);
    }

    #[test]
    fn test_event_batch_builders_multi_type() {
        use valveprotos::deadlock::{CCitadelUserMsgBossDamaged, CCitadelUserMsgRejuvStatus};

        let mut builders = EventBatchBuilders::with_batch_size(10);

        // Add BossDamaged events
        let boss_damaged = DecodedEvent::BossDamaged(CCitadelUserMsgBossDamaged {
            objective_team: Some(1),
            objective_id: Some(10),
            entity_damaged: Some(200),
        });
        builders.append(1, &boss_damaged);
        builders.append(2, &boss_damaged);

        // Add RejuvStatus event
        let rejuv = DecodedEvent::RejuvStatus(CCitadelUserMsgRejuvStatus {
            killing_team: Some(2),
            ..Default::default()
        });
        builders.append(3, &rejuv);

        // Check counts
        assert_eq!(builders.len(EventType::BossDamaged), 2);
        assert_eq!(builders.len(EventType::RejuvStatus), 1);
        assert!(builders.has_data(EventType::BossDamaged));
        assert!(builders.has_data(EventType::RejuvStatus));

        // Flush individual type
        let boss_batch = builders.flush(EventType::BossDamaged).unwrap().unwrap();
        assert_eq!(boss_batch.num_rows(), 2);
        assert_eq!(builders.len(EventType::BossDamaged), 0);
    }

    #[test]
    fn test_event_batch_builders_flush_all() {
        use valveprotos::deadlock::{CCitadelUserMsgBossDamaged, CCitadelUserMsgRejuvStatus};

        let mut builders = EventBatchBuilders::with_batch_size(10);

        builders.append(
            1,
            &DecodedEvent::BossDamaged(CCitadelUserMsgBossDamaged::default()),
        );
        builders.append(
            2,
            &DecodedEvent::RejuvStatus(CCitadelUserMsgRejuvStatus::default()),
        );

        let results = builders.flush_all();
        assert_eq!(results.len(), 2);

        // All builders should now be empty
        for event_type in EventType::all() {
            assert!(!builders.has_data(*event_type));
        }
    }
}
