use std::sync::Arc;

use crate::haste::fieldvalue::FieldValue;
use datafusion::arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeListBuilder, Float32Builder,
    Float64Builder, Int32Builder, Int64Builder, ListBuilder, NullBuilder, StringBuilder,
    UInt32Builder, UInt64Builder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};

use crate::error::Result;

macro_rules! append_fixed_null {
    ($b:expr) => {{
        let size = $b.value_length() as usize;
        for _ in 0..size {
            $b.values().append_null();
        }
        $b.append(false);
    }};
}

pub enum DynamicBuilder {
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Boolean(BooleanBuilder),
    String(StringBuilder),
    Binary(BinaryBuilder),
    Null(NullBuilder),
    ListInt32(ListBuilder<Int32Builder>),
    ListInt64(ListBuilder<Int64Builder>),
    ListUInt32(ListBuilder<UInt32Builder>),
    ListUInt64(ListBuilder<UInt64Builder>),
    ListFloat32(ListBuilder<Float32Builder>),
    ListFloat64(ListBuilder<Float64Builder>),
    ListBoolean(ListBuilder<BooleanBuilder>),
    ListString(ListBuilder<StringBuilder>),
    ListBinary(ListBuilder<BinaryBuilder>),
    ListNull(ListBuilder<NullBuilder>),
    FixedListInt32(FixedSizeListBuilder<Int32Builder>),
    FixedListInt64(FixedSizeListBuilder<Int64Builder>),
    FixedListUInt32(FixedSizeListBuilder<UInt32Builder>),
    FixedListUInt64(FixedSizeListBuilder<UInt64Builder>),
    FixedListFloat32(FixedSizeListBuilder<Float32Builder>),
    FixedListFloat64(FixedSizeListBuilder<Float64Builder>),
    FixedListBoolean(FixedSizeListBuilder<BooleanBuilder>),
    FixedListString(FixedSizeListBuilder<StringBuilder>),
    FixedListBinary(FixedSizeListBuilder<BinaryBuilder>),
    FixedListNull(FixedSizeListBuilder<NullBuilder>),
}

impl DynamicBuilder {
    pub fn new(data_type: &DataType, capacity: usize) -> Self {
        match data_type {
            DataType::Int32 => Self::Int32(Int32Builder::with_capacity(capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(capacity)),
            DataType::UInt32 => Self::UInt32(UInt32Builder::with_capacity(capacity)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Utf8 => Self::String(StringBuilder::with_capacity(capacity, 1024)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(capacity, 1024)),
            DataType::Null => Self::Null(NullBuilder::new()),
            DataType::List(field) => Self::new_list(field, capacity),
            DataType::FixedSizeList(field, size) => Self::new_fixed_list(field, *size, capacity),
            _ => Self::Binary(BinaryBuilder::with_capacity(capacity, 1024)),
        }
    }

    fn new_list(field: &Arc<Field>, capacity: usize) -> Self {
        match field.data_type() {
            DataType::Int32 => Self::ListInt32(
                ListBuilder::with_capacity(Int32Builder::with_capacity(capacity), capacity)
                    .with_field(field.clone()),
            ),
            DataType::Int64 => Self::ListInt64(
                ListBuilder::with_capacity(Int64Builder::with_capacity(capacity), capacity)
                    .with_field(field.clone()),
            ),
            DataType::UInt32 => Self::ListUInt32(
                ListBuilder::with_capacity(UInt32Builder::with_capacity(capacity), capacity)
                    .with_field(field.clone()),
            ),
            DataType::UInt64 => Self::ListUInt64(
                ListBuilder::with_capacity(UInt64Builder::with_capacity(capacity), capacity)
                    .with_field(field.clone()),
            ),
            DataType::Float32 => Self::ListFloat32(
                ListBuilder::with_capacity(Float32Builder::with_capacity(capacity), capacity)
                    .with_field(field.clone()),
            ),
            DataType::Float64 => Self::ListFloat64(
                ListBuilder::with_capacity(Float64Builder::with_capacity(capacity), capacity)
                    .with_field(field.clone()),
            ),
            DataType::Boolean => Self::ListBoolean(
                ListBuilder::with_capacity(BooleanBuilder::with_capacity(capacity), capacity)
                    .with_field(field.clone()),
            ),
            DataType::Utf8 => Self::ListString(
                ListBuilder::with_capacity(StringBuilder::with_capacity(capacity, 1024), capacity)
                    .with_field(field.clone()),
            ),
            DataType::Binary => Self::ListBinary(
                ListBuilder::with_capacity(BinaryBuilder::with_capacity(capacity, 1024), capacity)
                    .with_field(field.clone()),
            ),
            DataType::Null => Self::ListNull(
                ListBuilder::with_capacity(NullBuilder::new(), capacity).with_field(field.clone()),
            ),
            _ => Self::ListBinary(
                ListBuilder::with_capacity(BinaryBuilder::with_capacity(capacity, 1024), capacity)
                    .with_field(Arc::new(Field::new("item", DataType::Binary, true))),
            ),
        }
    }

    fn new_fixed_list(field: &Arc<Field>, size: i32, capacity: usize) -> Self {
        let values_capacity = capacity * (size as usize);

        match field.data_type() {
            DataType::Int32 => Self::FixedListInt32(
                FixedSizeListBuilder::with_capacity(
                    Int32Builder::with_capacity(values_capacity),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::Int64 => Self::FixedListInt64(
                FixedSizeListBuilder::with_capacity(
                    Int64Builder::with_capacity(values_capacity),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::UInt32 => Self::FixedListUInt32(
                FixedSizeListBuilder::with_capacity(
                    UInt32Builder::with_capacity(values_capacity),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::UInt64 => Self::FixedListUInt64(
                FixedSizeListBuilder::with_capacity(
                    UInt64Builder::with_capacity(values_capacity),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::Float32 => Self::FixedListFloat32(
                FixedSizeListBuilder::with_capacity(
                    Float32Builder::with_capacity(values_capacity),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::Float64 => Self::FixedListFloat64(
                FixedSizeListBuilder::with_capacity(
                    Float64Builder::with_capacity(values_capacity),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::Boolean => Self::FixedListBoolean(
                FixedSizeListBuilder::with_capacity(
                    BooleanBuilder::with_capacity(values_capacity),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::Utf8 => Self::FixedListString(
                FixedSizeListBuilder::with_capacity(
                    StringBuilder::with_capacity(values_capacity, 1024),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::Binary => Self::FixedListBinary(
                FixedSizeListBuilder::with_capacity(
                    BinaryBuilder::with_capacity(values_capacity, 1024),
                    size,
                    capacity,
                )
                .with_field(field.clone()),
            ),
            DataType::Null => Self::FixedListNull(
                FixedSizeListBuilder::with_capacity(NullBuilder::new(), size, capacity)
                    .with_field(field.clone()),
            ),
            _ => Self::FixedListBinary(
                FixedSizeListBuilder::with_capacity(
                    BinaryBuilder::with_capacity(values_capacity, 1024),
                    size,
                    capacity,
                )
                .with_field(Arc::new(Field::new("item", DataType::Binary, true))),
            ),
        }
    }

    pub fn append_null(&mut self) {
        match self {
            Self::Int32(b) => b.append_null(),
            Self::Int64(b) => b.append_null(),
            Self::UInt32(b) => b.append_null(),
            Self::UInt64(b) => b.append_null(),
            Self::Float32(b) => b.append_null(),
            Self::Float64(b) => b.append_null(),
            Self::Boolean(b) => b.append_null(),
            Self::String(b) => b.append_null(),
            Self::Binary(b) => b.append_null(),
            Self::Null(b) => b.append_null(),
            Self::ListInt32(b) => b.append_null(),
            Self::ListInt64(b) => b.append_null(),
            Self::ListUInt32(b) => b.append_null(),
            Self::ListUInt64(b) => b.append_null(),
            Self::ListFloat32(b) => b.append_null(),
            Self::ListFloat64(b) => b.append_null(),
            Self::ListBoolean(b) => b.append_null(),
            Self::ListString(b) => b.append_null(),
            Self::ListBinary(b) => b.append_null(),
            Self::ListNull(b) => b.append_null(),
            Self::FixedListInt32(b) => append_fixed_null!(b),
            Self::FixedListInt64(b) => append_fixed_null!(b),
            Self::FixedListUInt32(b) => append_fixed_null!(b),
            Self::FixedListUInt64(b) => append_fixed_null!(b),
            Self::FixedListFloat32(b) => append_fixed_null!(b),
            Self::FixedListFloat64(b) => append_fixed_null!(b),
            Self::FixedListBoolean(b) => append_fixed_null!(b),
            Self::FixedListString(b) => append_fixed_null!(b),
            Self::FixedListBinary(b) => append_fixed_null!(b),
            Self::FixedListNull(b) => append_fixed_null!(b),
        }
    }

    pub fn append_field_value(&mut self, value: &FieldValue) -> Result<()> {
        match self {
            Self::Int32(b) => match value {
                FieldValue::I64(v) => b.append_value(*v as i32),
                FieldValue::U64(v) => b.append_value(*v as i32),
                _ => b.append_null(),
            },
            Self::Int64(b) => match value {
                FieldValue::I64(v) => b.append_value(*v),
                FieldValue::U64(v) => b.append_value(*v as i64),
                _ => b.append_null(),
            },
            Self::UInt32(b) => match value {
                FieldValue::U64(v) => b.append_value(*v as u32),
                FieldValue::I64(v) => b.append_value(*v as u32),
                _ => b.append_null(),
            },
            Self::UInt64(b) => match value {
                FieldValue::U64(v) => b.append_value(*v),
                FieldValue::I64(v) => b.append_value(*v as u64),
                _ => b.append_null(),
            },
            Self::Float32(b) => match value {
                FieldValue::F32(v) => b.append_value(*v),
                _ => b.append_null(),
            },
            Self::Float64(b) => match value {
                FieldValue::F32(v) => b.append_value(*v as f64),
                _ => b.append_null(),
            },
            Self::Boolean(b) => match value {
                FieldValue::Bool(v) => b.append_value(*v),
                _ => b.append_null(),
            },
            Self::String(b) => match value {
                FieldValue::String(v) => match std::str::from_utf8(v) {
                    Ok(s) => b.append_value(s),
                    Err(_) => b.append_null(),
                },
                _ => b.append_null(),
            },
            Self::Binary(b) => match value {
                FieldValue::String(v) => b.append_value(v.as_ref()),
                FieldValue::U64(v) => b.append_value(&v.to_le_bytes()),
                FieldValue::I64(v) => b.append_value(&v.to_le_bytes()),
                FieldValue::F32(v) => b.append_value(&v.to_le_bytes()),
                FieldValue::Bool(v) => b.append_value(&[*v as u8]),
                _ => b.append_null(),
            },
            Self::Null(b) => b.append_null(),
            Self::ListInt32(b) => b.append_null(),
            Self::ListInt64(b) => b.append_null(),
            Self::ListUInt32(b) => b.append_null(),
            Self::ListUInt64(b) => b.append_null(),
            Self::ListFloat32(b) => b.append_null(),
            Self::ListFloat64(b) => b.append_null(),
            Self::ListBoolean(b) => b.append_null(),
            Self::ListString(b) => b.append_null(),
            Self::ListBinary(b) => b.append_null(),
            Self::ListNull(b) => b.append_null(),
            Self::FixedListInt32(b) => append_fixed_null!(b),
            Self::FixedListInt64(b) => append_fixed_null!(b),
            Self::FixedListUInt32(b) => append_fixed_null!(b),
            Self::FixedListUInt64(b) => append_fixed_null!(b),
            Self::FixedListFloat32(b) => append_fixed_null!(b),
            Self::FixedListFloat64(b) => append_fixed_null!(b),
            Self::FixedListBoolean(b) => append_fixed_null!(b),
            Self::FixedListString(b) => append_fixed_null!(b),
            Self::FixedListBinary(b) => append_fixed_null!(b),
            Self::FixedListNull(b) => append_fixed_null!(b),
        }
        Ok(())
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::UInt32(b) => Arc::new(b.finish()),
            Self::UInt64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::String(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Null(b) => Arc::new(b.finish()),
            Self::ListInt32(b) => Arc::new(b.finish()),
            Self::ListInt64(b) => Arc::new(b.finish()),
            Self::ListUInt32(b) => Arc::new(b.finish()),
            Self::ListUInt64(b) => Arc::new(b.finish()),
            Self::ListFloat32(b) => Arc::new(b.finish()),
            Self::ListFloat64(b) => Arc::new(b.finish()),
            Self::ListBoolean(b) => Arc::new(b.finish()),
            Self::ListString(b) => Arc::new(b.finish()),
            Self::ListBinary(b) => Arc::new(b.finish()),
            Self::ListNull(b) => Arc::new(b.finish()),
            Self::FixedListInt32(b) => Arc::new(b.finish()),
            Self::FixedListInt64(b) => Arc::new(b.finish()),
            Self::FixedListUInt32(b) => Arc::new(b.finish()),
            Self::FixedListUInt64(b) => Arc::new(b.finish()),
            Self::FixedListFloat32(b) => Arc::new(b.finish()),
            Self::FixedListFloat64(b) => Arc::new(b.finish()),
            Self::FixedListBoolean(b) => Arc::new(b.finish()),
            Self::FixedListString(b) => Arc::new(b.finish()),
            Self::FixedListBinary(b) => Arc::new(b.finish()),
            Self::FixedListNull(b) => Arc::new(b.finish()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Int32(b) => b.len(),
            Self::Int64(b) => b.len(),
            Self::UInt32(b) => b.len(),
            Self::UInt64(b) => b.len(),
            Self::Float32(b) => b.len(),
            Self::Float64(b) => b.len(),
            Self::Boolean(b) => b.len(),
            Self::String(b) => b.len(),
            Self::Binary(b) => b.len(),
            Self::Null(b) => b.len(),
            Self::ListInt32(b) => b.len(),
            Self::ListInt64(b) => b.len(),
            Self::ListUInt32(b) => b.len(),
            Self::ListUInt64(b) => b.len(),
            Self::ListFloat32(b) => b.len(),
            Self::ListFloat64(b) => b.len(),
            Self::ListBoolean(b) => b.len(),
            Self::ListString(b) => b.len(),
            Self::ListBinary(b) => b.len(),
            Self::ListNull(b) => b.len(),
            Self::FixedListInt32(b) => b.len(),
            Self::FixedListInt64(b) => b.len(),
            Self::FixedListUInt32(b) => b.len(),
            Self::FixedListUInt64(b) => b.len(),
            Self::FixedListFloat32(b) => b.len(),
            Self::FixedListFloat64(b) => b.len(),
            Self::FixedListBoolean(b) => b.len(),
            Self::FixedListString(b) => b.len(),
            Self::FixedListBinary(b) => b.len(),
            Self::FixedListNull(b) => b.len(),
        }
    }
}

pub fn create_builders_for_schema(schema: &Schema, capacity: usize) -> Vec<DynamicBuilder> {
    schema
        .fields()
        .iter()
        .map(|f| DynamicBuilder::new(f.data_type(), capacity))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int64_builder() {
        let mut builder = DynamicBuilder::new(&DataType::Int64, 10);
        builder.append_field_value(&FieldValue::I64(42)).unwrap();
        builder.append_null();
        builder.append_field_value(&FieldValue::I64(-100)).unwrap();

        let array = builder.finish();
        assert_eq!(array.len(), 3);
    }

    #[test]
    fn test_float32_builder() {
        let mut builder = DynamicBuilder::new(&DataType::Float32, 10);
        builder.append_field_value(&FieldValue::F32(3.14)).unwrap();
        builder.append_null();

        let array = builder.finish();
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn test_list_builder() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let mut builder = DynamicBuilder::new(&list_type, 10);
        builder.append_null();
        builder.append_null();

        let array = builder.finish();
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn test_fixed_size_list_builder() {
        let list_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4);
        let mut builder = DynamicBuilder::new(&list_type, 10);
        builder.append_null();
        builder.append_null();

        let array = builder.finish();
        assert_eq!(array.len(), 2);
    }
}
