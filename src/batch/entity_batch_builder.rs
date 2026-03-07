//! Builder for accumulating entity updates into Arrow RecordBatches.
//!
//! This accumulates entity data and builds RecordBatches for DataFusion consumption.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Builder, StringBuilder};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;

use super::dynamic_builder::DynamicBuilder;
use crate::haste::entities::DeltaHeader;
use crate::haste::fieldvalue::FieldValue;
use crate::schema::EntitySchema;

const DEFAULT_BATCH_SIZE: usize = 8192;

struct FieldMapping {
    key: u64,
    column_count: usize,
    builder_start_idx: usize,
}

pub struct EntityBatchBuilder {
    schema: SchemaRef,
    field_mappings: Vec<FieldMapping>,
    tick_builder: Int32Builder,
    entity_index_builder: Int32Builder,
    delta_type_builder: StringBuilder,
    field_builders: Vec<DynamicBuilder>,
    row_count: usize,
    batch_size: usize,
}

impl EntityBatchBuilder {
    pub fn new(entity_schema: &EntitySchema, batch_size: usize) -> Self {
        let schema = entity_schema.arrow_schema.clone();

        let field_builders: Vec<DynamicBuilder> = schema
            .fields()
            .iter()
            .skip(3)
            .map(|f| DynamicBuilder::new(f.data_type(), batch_size))
            .collect();

        let field_mappings = build_field_mappings(
            &entity_schema.field_keys,
            &entity_schema.field_column_counts,
        );

        Self {
            schema,
            field_mappings,
            tick_builder: Int32Builder::with_capacity(batch_size),
            entity_index_builder: Int32Builder::with_capacity(batch_size),
            delta_type_builder: StringBuilder::with_capacity(batch_size, batch_size * 8),
            field_builders,
            row_count: 0,
            batch_size,
        }
    }

    pub fn with_default_batch_size(entity_schema: &EntitySchema) -> Self {
        Self::new(entity_schema, DEFAULT_BATCH_SIZE)
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

    pub fn append_entity(
        &mut self,
        tick: i32,
        entity_index: i32,
        delta_type: DeltaHeader,
        entity: &crate::haste::entities::Entity,
    ) {
        self.tick_builder.append_value(tick);
        self.entity_index_builder.append_value(entity_index);
        self.delta_type_builder
            .append_value(delta_header_to_str(delta_type));

        if delta_type == DeltaHeader::DELETE || delta_type == DeltaHeader::LEAVE {
            for builder in &mut self.field_builders {
                builder.append_null();
            }
        } else {
            for i in 0..self.field_mappings.len() {
                let mapping = &self.field_mappings[i];
                let key = mapping.key;
                let builder_start_idx = mapping.builder_start_idx;
                let column_count = mapping.column_count;

                if let Some(value) = entity.get_field_value(&key) {
                    Self::append_field_value_to_builders(
                        &mut self.field_builders,
                        builder_start_idx,
                        column_count,
                        value,
                    );
                } else {
                    Self::append_nulls_to_builders(
                        &mut self.field_builders,
                        builder_start_idx,
                        column_count,
                    );
                }
            }
        }

        self.row_count += 1;
    }

    fn append_field_value_to_builders(
        field_builders: &mut [DynamicBuilder],
        builder_start_idx: usize,
        column_count: usize,
        value: &FieldValue,
    ) {
        match value {
            FieldValue::Vector3(arr) | FieldValue::QAngle(arr) => {
                for i in 0..column_count {
                    if let Some(builder) = field_builders.get_mut(builder_start_idx + i) {
                        if let Some(&v) = arr.get(i) {
                            let _ = builder.append_field_value(&FieldValue::F32(v));
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            FieldValue::Vector2(arr) => {
                for i in 0..column_count {
                    if let Some(builder) = field_builders.get_mut(builder_start_idx + i) {
                        if let Some(&v) = arr.get(i) {
                            let _ = builder.append_field_value(&FieldValue::F32(v));
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            FieldValue::Vector4(arr) => {
                for i in 0..column_count {
                    if let Some(builder) = field_builders.get_mut(builder_start_idx + i) {
                        if let Some(&v) = arr.get(i) {
                            let _ = builder.append_field_value(&FieldValue::F32(v));
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            _ => {
                if let Some(builder) = field_builders.get_mut(builder_start_idx) {
                    let _ = builder.append_field_value(value);
                }
            }
        }
    }

    fn append_nulls_to_builders(
        field_builders: &mut [DynamicBuilder],
        builder_start_idx: usize,
        column_count: usize,
    ) {
        for i in 0..column_count {
            if let Some(builder) = field_builders.get_mut(builder_start_idx + i) {
                builder.append_null();
            }
        }
    }

    pub fn flush(&mut self) -> Result<RecordBatch, datafusion::arrow::error::ArrowError> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(3 + self.field_builders.len());

        arrays.push(Arc::new(self.tick_builder.finish()));
        arrays.push(Arc::new(self.entity_index_builder.finish()));
        arrays.push(Arc::new(self.delta_type_builder.finish()));

        for builder in &mut self.field_builders {
            arrays.push(builder.finish());
        }

        self.row_count = 0;

        RecordBatch::try_new(self.schema.clone(), arrays)
    }
}

fn build_field_mappings(field_keys: &[u64], field_column_counts: &[usize]) -> Vec<FieldMapping> {
    let mut mappings = Vec::with_capacity(field_keys.len());
    let mut builder_idx = 0;

    for (i, &key) in field_keys.iter().enumerate() {
        let column_count = field_column_counts.get(i).copied().unwrap_or(1);
        mappings.push(FieldMapping {
            key,
            column_count,
            builder_start_idx: builder_idx,
        });
        builder_idx += column_count;
    }

    mappings
}

fn delta_header_to_str(delta: DeltaHeader) -> &'static str {
    match delta {
        DeltaHeader::CREATE => "create",
        DeltaHeader::UPDATE => "update",
        DeltaHeader::DELETE => "delete",
        DeltaHeader::LEAVE => "leave",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn make_simple_entity_schema() -> EntitySchema {
        let arrow_schema = Arc::new(Schema::new(vec![
            Field::new("tick", DataType::Int32, false),
            Field::new("entity_index", DataType::Int32, false),
            Field::new("delta_type", DataType::Utf8, false),
            Field::new("health", DataType::Int32, true),
        ]));

        EntitySchema {
            serializer_name: Arc::from("TestEntity"),
            serializer_hash: 12345,
            arrow_schema,
            field_keys: vec![100],
            field_column_counts: vec![1],
        }
    }

    #[test]
    fn test_new_creates_empty_builder() {
        let schema = make_simple_entity_schema();
        let builder = EntityBatchBuilder::new(&schema, 100);

        assert_eq!(builder.len(), 0);
        assert!(!builder.has_data());
        assert!(!builder.should_flush());
    }

    #[test]
    fn test_should_flush_at_capacity() {
        let schema = make_simple_entity_schema();
        let builder = EntityBatchBuilder::new(&schema, 5);

        assert!(!builder.should_flush());
    }
}
