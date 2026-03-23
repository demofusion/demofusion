//! Arrow-to-PyArrow conversion via the Arrow C Data Interface (FFI).
//!
//! Uses Arrow's FFI types (`FFI_ArrowSchema`, `FFI_ArrowArray`) to export
//! Rust Arrow data, then calls PyArrow's `_import_from_c` to create Python objects.

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use pyo3::prelude::*;

/// Convert a Rust `arrow::datatypes::Schema` to a PyArrow `Schema`.
///
/// Exports the schema through the Arrow C Data Interface and imports it
/// into PyArrow using `pa.Schema._import_from_c(ptr)`.
pub fn schema_to_pyarrow(py: Python<'_>, schema: &SchemaRef) -> PyResult<PyObject> {
    // Export the schema to FFI
    let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Arrow FFI schema export failed: {e}"))
    })?;

    let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;

    // Import into PyArrow: pa.Schema._import_from_c(ptr)
    let pa = py.import_bound("pyarrow")?;
    let schema_class = pa.getattr("Schema")?;
    let py_schema = schema_class.call_method1("_import_from_c", (schema_ptr,))?;

    Ok(py_schema.into())
}

/// Convert a Rust `arrow::record_batch::RecordBatch` to a PyArrow `RecordBatch`.
///
/// Exports both the array data and schema through the Arrow C Data Interface
/// and imports them into PyArrow using `pa.RecordBatch._import_from_c(array_ptr, schema_ptr)`.
pub fn record_batch_to_pyarrow(py: Python<'_>, batch: &RecordBatch) -> PyResult<PyObject> {
    // Convert RecordBatch to StructArray for FFI export
    let struct_array: datafusion::arrow::array::StructArray = batch.clone().into();

    // Export array data and schema to FFI
    let (ffi_array, ffi_schema) = arrow::ffi::to_ffi(&struct_array.into()).map_err(|e| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Arrow FFI array export failed: {e}"))
    })?;

    let array_ptr = &ffi_array as *const FFI_ArrowArray as usize;
    let schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;

    // Import into PyArrow: pa.RecordBatch._import_from_c(array_ptr, schema_ptr)
    let pa = py.import_bound("pyarrow")?;
    let rb_class = pa.getattr("RecordBatch")?;
    let py_batch = rb_class.call_method1("_import_from_c", (array_ptr, schema_ptr))?;

    Ok(py_batch.into())
}
