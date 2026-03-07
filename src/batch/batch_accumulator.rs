use std::sync::Arc;

use crate::haste::entities::Entity;
use crate::haste::fieldvalue::FieldValue;
use datafusion::arrow::array::{ArrayBuilder, Int32Builder, StringBuilder};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};

use super::dynamic_builder::DynamicBuilder;
use crate::error::{Result, Source2DfError};
use crate::schema::EntitySchema;

const DEFAULT_BATCH_SIZE: usize = 8192;

struct FieldMapping {
    key: u64,
    column_count: usize,
    builder_start_idx: usize,
}

pub struct BatchAccumulator {
    schema: Arc<Schema>,
    field_mappings: Vec<FieldMapping>,
    tick_builder: Option<Int32Builder>,
    entity_index_builder: Option<Int32Builder>,
    delta_type_builder: Option<StringBuilder>,
    field_builders: Vec<DynamicBuilder>,
    row_count: usize,
    batch_size: usize,
}

impl BatchAccumulator {
    pub fn new(entity_schema: &EntitySchema, batch_size: Option<usize>) -> Self {
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
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
            tick_builder: Some(Int32Builder::with_capacity(batch_size)),
            entity_index_builder: Some(Int32Builder::with_capacity(batch_size)),
            delta_type_builder: Some(StringBuilder::with_capacity(batch_size, batch_size * 8)),
            field_builders,
            row_count: 0,
            batch_size,
        }
    }

    pub fn new_with_projection(
        entity_schema: &EntitySchema,
        batch_size: Option<usize>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

        match projection {
            Some(proj) => {
                let projected_schema = entity_schema.project(&proj);
                let schema = projected_schema.arrow_schema.clone();

                let has_tick = proj.contains(&0);
                let has_entity_index = proj.contains(&1);
                let has_delta_type = proj.contains(&2);

                let field_builders: Vec<DynamicBuilder> = schema
                    .fields()
                    .iter()
                    .skip(
                        (if has_tick { 1 } else { 0 })
                            + (if has_entity_index { 1 } else { 0 })
                            + (if has_delta_type { 1 } else { 0 }),
                    )
                    .map(|f| DynamicBuilder::new(f.data_type(), batch_size))
                    .collect();

                let field_mappings = build_field_mappings(
                    &projected_schema.field_keys,
                    &projected_schema.field_column_counts,
                );

                Self {
                    schema,
                    field_mappings,
                    tick_builder: if has_tick {
                        Some(Int32Builder::with_capacity(batch_size))
                    } else {
                        None
                    },
                    entity_index_builder: if has_entity_index {
                        Some(Int32Builder::with_capacity(batch_size))
                    } else {
                        None
                    },
                    delta_type_builder: if has_delta_type {
                        Some(StringBuilder::with_capacity(batch_size, batch_size * 8))
                    } else {
                        None
                    },
                    field_builders,
                    row_count: 0,
                    batch_size,
                }
            }
            None => Self::new(entity_schema, Some(batch_size)),
        }
    }

    pub fn append_entity(
        &mut self,
        tick: i32,
        entity_index: i32,
        delta_type: &str,
        entity: &Entity,
    ) -> Result<Option<RecordBatch>> {
        if let Some(ref mut builder) = self.tick_builder {
            builder.append_value(tick);
        }
        if let Some(ref mut builder) = self.entity_index_builder {
            builder.append_value(entity_index);
        }
        if let Some(ref mut builder) = self.delta_type_builder {
            builder.append_value(delta_type);
        }

        let num_mappings = self.field_mappings.len();
        for i in 0..num_mappings {
            let key = self.field_mappings[i].key;
            let builder_start_idx = self.field_mappings[i].builder_start_idx;
            let column_count = self.field_mappings[i].column_count;

            if let Some(value) = entity.get_field_value(&key) {
                self.append_field_value(builder_start_idx, column_count, value)?;
            } else {
                self.append_nulls(builder_start_idx, column_count);
            }
        }

        self.row_count += 1;

        if self.row_count >= self.batch_size {
            return self.flush().map(Some);
        }

        Ok(None)
    }

    fn append_field_value(
        &mut self,
        builder_start_idx: usize,
        column_count: usize,
        value: &FieldValue,
    ) -> Result<()> {
        match value {
            FieldValue::Vector3(arr) | FieldValue::QAngle(arr) => {
                // Always append exactly column_count values (should be 3)
                for i in 0..column_count {
                    if let Some(builder) = self.field_builders.get_mut(builder_start_idx + i) {
                        if let Some(&v) = arr.get(i) {
                            builder.append_field_value(&FieldValue::F32(v))?;
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            FieldValue::Vector2(arr) => {
                // Always append exactly column_count values (should be 2)
                for i in 0..column_count {
                    if let Some(builder) = self.field_builders.get_mut(builder_start_idx + i) {
                        if let Some(&v) = arr.get(i) {
                            builder.append_field_value(&FieldValue::F32(v))?;
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            FieldValue::Vector4(arr) => {
                // Always append exactly column_count values (should be 4)
                for i in 0..column_count {
                    if let Some(builder) = self.field_builders.get_mut(builder_start_idx + i) {
                        if let Some(&v) = arr.get(i) {
                            builder.append_field_value(&FieldValue::F32(v))?;
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            _ => {
                if let Some(builder) = self.field_builders.get_mut(builder_start_idx) {
                    builder.append_field_value(value)?;
                }
            }
        }
        Ok(())
    }

    fn append_nulls(&mut self, builder_start_idx: usize, column_count: usize) {
        for i in 0..column_count {
            if let Some(builder) = self.field_builders.get_mut(builder_start_idx + i) {
                builder.append_null();
            }
        }
    }

    pub fn append_delete_or_leave(
        &mut self,
        tick: i32,
        entity_index: i32,
        delta_type: &str,
    ) -> Result<Option<RecordBatch>> {
        if let Some(ref mut builder) = self.tick_builder {
            builder.append_value(tick);
        }
        if let Some(ref mut builder) = self.entity_index_builder {
            builder.append_value(entity_index);
        }
        if let Some(ref mut builder) = self.delta_type_builder {
            builder.append_value(delta_type);
        }

        for builder in &mut self.field_builders {
            builder.append_null();
        }

        self.row_count += 1;

        if self.row_count >= self.batch_size {
            return self.flush().map(Some);
        }

        Ok(None)
    }

    pub fn flush(&mut self) -> Result<RecordBatch> {
        if self.row_count == 0 {
            return Err(Source2DfError::Schema("No rows to flush".to_string()));
        }

        let expected_len = self.row_count;

        // Validate base column lengths
        if let Some(ref builder) = self.tick_builder {
            if builder.len() != expected_len {
                return Err(Source2DfError::Schema(format!(
                    "tick column has {} rows, expected {}",
                    builder.len(),
                    expected_len
                )));
            }
        }
        if let Some(ref builder) = self.entity_index_builder {
            if builder.len() != expected_len {
                return Err(Source2DfError::Schema(format!(
                    "entity_index column has {} rows, expected {}",
                    builder.len(),
                    expected_len
                )));
            }
        }
        if let Some(ref builder) = self.delta_type_builder {
            if builder.len() != expected_len {
                return Err(Source2DfError::Schema(format!(
                    "delta_type column has {} rows, expected {}",
                    builder.len(),
                    expected_len
                )));
            }
        }

        // Validate field builder lengths
        for (i, builder) in self.field_builders.iter().enumerate() {
            let builder_len = builder.len();
            if builder_len != expected_len {
                let field_idx = i + 3;
                let field_name = if field_idx < self.schema.fields().len() {
                    self.schema.field(field_idx).name().as_str()
                } else {
                    "unknown"
                };
                return Err(Source2DfError::Schema(format!(
                    "column '{}' (index {}) has {} rows, expected {}",
                    field_name, field_idx, builder_len, expected_len
                )));
            }
        }

        let base_column_count = self.tick_builder.as_ref().map(|_| 1).unwrap_or(0)
            + self.entity_index_builder.as_ref().map(|_| 1).unwrap_or(0)
            + self.delta_type_builder.as_ref().map(|_| 1).unwrap_or(0);

        let mut arrays = Vec::with_capacity(base_column_count + self.field_builders.len());

        if let Some(ref mut builder) = self.tick_builder {
            arrays.push(Arc::new(builder.finish()) as _);
        }
        if let Some(ref mut builder) = self.entity_index_builder {
            arrays.push(Arc::new(builder.finish()) as _);
        }
        if let Some(ref mut builder) = self.delta_type_builder {
            arrays.push(Arc::new(builder.finish()) as _);
        }

        for builder in &mut self.field_builders {
            arrays.push(builder.finish());
        }

        let row_count = self.row_count;
        self.row_count = 0;

        // Use try_new_with_options to handle 0-column case (e.g., COUNT(*))
        // where we need to specify row_count explicitly
        RecordBatch::try_new_with_options(
            self.schema.clone(),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )
        .map_err(|e| e.into())
    }

    pub fn has_data(&self) -> bool {
        self.row_count > 0
    }

    pub fn row_count(&self) -> usize {
        self.row_count
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
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

pub fn delta_header_to_str(delta: crate::haste::entities::DeltaHeader) -> &'static str {
    match delta {
        crate::haste::entities::DeltaHeader::CREATE => "create",
        crate::haste::entities::DeltaHeader::UPDATE => "update",
        crate::haste::entities::DeltaHeader::DELETE => "delete",
        crate::haste::entities::DeltaHeader::LEAVE => "leave",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use datafusion::arrow::datatypes::{DataType, Field};

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
            field_keys: vec![100], // field key for "health"
            field_column_counts: vec![1],
        }
    }

    #[test]
    fn test_new_creates_empty_accumulator() {
        let schema = make_simple_entity_schema();
        let acc = BatchAccumulator::new(&schema, Some(100));

        assert_eq!(acc.row_count(), 0);
        assert!(!acc.has_data());
    }

    #[test]
    fn test_append_delete_increments_row_count() {
        let schema = make_simple_entity_schema();
        let mut acc = BatchAccumulator::new(&schema, Some(100));

        let result = acc.append_delete_or_leave(1000, 42, "delete").unwrap();

        assert!(result.is_none()); // Not at batch_size yet
        assert_eq!(acc.row_count(), 1);
        assert!(acc.has_data());
    }

    #[test]
    fn test_flush_produces_record_batch() {
        let schema = make_simple_entity_schema();
        let mut acc = BatchAccumulator::new(&schema, Some(100));

        acc.append_delete_or_leave(1000, 42, "delete").unwrap();
        acc.append_delete_or_leave(1001, 43, "leave").unwrap();

        let batch = acc.flush().unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4); // tick, entity_index, delta_type, health

        // Verify tick values
        let tick_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(tick_col.value(0), 1000);
        assert_eq!(tick_col.value(1), 1001);

        // Verify entity_index values
        let idx_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(idx_col.value(0), 42);
        assert_eq!(idx_col.value(1), 43);

        // Verify delta_type values
        let delta_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        assert_eq!(delta_col.value(0), "delete");
        assert_eq!(delta_col.value(1), "leave");

        // Verify health is null (delete/leave don't have field values)
        let health_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert!(health_col.is_null(0));
        assert!(health_col.is_null(1));
    }

    #[test]
    fn test_flush_resets_state() {
        let schema = make_simple_entity_schema();
        let mut acc = BatchAccumulator::new(&schema, Some(100));

        acc.append_delete_or_leave(1000, 42, "delete").unwrap();
        let _ = acc.flush().unwrap();

        assert_eq!(acc.row_count(), 0);
        assert!(!acc.has_data());
    }

    #[test]
    fn test_flush_empty_returns_error() {
        let schema = make_simple_entity_schema();
        let mut acc = BatchAccumulator::new(&schema, Some(100));

        let result = acc.flush();
        assert!(result.is_err());
    }

    #[test]
    fn test_auto_flush_at_batch_size() {
        let schema = make_simple_entity_schema();
        let mut acc = BatchAccumulator::new(&schema, Some(3)); // Small batch size

        // First two don't trigger flush
        assert!(acc
            .append_delete_or_leave(1, 1, "update")
            .unwrap()
            .is_none());
        assert!(acc
            .append_delete_or_leave(2, 2, "update")
            .unwrap()
            .is_none());

        // Third triggers auto-flush
        let result = acc.append_delete_or_leave(3, 3, "update").unwrap();
        assert!(result.is_some());

        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 3);

        // Accumulator should be reset
        assert_eq!(acc.row_count(), 0);
    }

    #[test]
    fn test_multiple_flushes() {
        let schema = make_simple_entity_schema();
        let mut acc = BatchAccumulator::new(&schema, Some(2));

        // First batch
        acc.append_delete_or_leave(1, 1, "update").unwrap();
        let batch1 = acc.append_delete_or_leave(2, 2, "update").unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 2);

        // Second batch
        acc.append_delete_or_leave(3, 3, "update").unwrap();
        let batch2 = acc.append_delete_or_leave(4, 4, "update").unwrap().unwrap();
        assert_eq!(batch2.num_rows(), 2);

        // Verify ticks are correct in each batch
        let tick1 = batch1
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(tick1.value(0), 1);
        assert_eq!(tick1.value(1), 2);

        let tick2 = batch2
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(tick2.value(0), 3);
        assert_eq!(tick2.value(1), 4);
    }

    #[test]
    fn test_schema_accessor() {
        let schema = make_simple_entity_schema();
        let acc = BatchAccumulator::new(&schema, Some(100));

        let returned_schema = acc.schema();
        assert_eq!(returned_schema.fields().len(), 4);
        assert_eq!(returned_schema.field(0).name(), "tick");
        assert_eq!(returned_schema.field(3).name(), "health");
    }
}
