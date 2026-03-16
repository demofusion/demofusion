//! PyO3 wrapper for QueryHandle.

use std::sync::Arc;

use futures::StreamExt;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;

use crate::session::QueryHandle;
use super::arrow_convert::{record_batch_to_pyarrow, schema_to_pyarrow};
use super::exceptions::session_error_to_pyexc;

#[pyclass(name = "QueryHandle")]
pub struct PyQueryHandle {
    /// The real QueryHandle, wrapped in Arc<Mutex<Option>> because:
    /// - Arc: shared between Python ref and async futures
    /// - Mutex: interior mutability for Stream::poll_next
    /// - Option: allows moving out for pinning in async context
    inner: Arc<parking_lot::Mutex<Option<QueryHandle>>>,
    /// Cached schema from the QueryHandle (SchemaRef is Arc, cheap to clone)
    schema: datafusion::arrow::datatypes::SchemaRef,
}

impl PyQueryHandle {
    pub fn from_handle(handle: QueryHandle) -> Self {
        let schema = handle.schema().clone();
        PyQueryHandle {
            inner: Arc::new(parking_lot::Mutex::new(Some(handle))),
            schema,
        }
    }
}

#[pymethods]
impl PyQueryHandle {
    /// Get the PyArrow schema for query results.
    #[getter]
    fn schema(&self, py: Python<'_>) -> PyResult<PyObject> {
        schema_to_pyarrow(py, &self.schema)
    }

    /// Async iterator protocol for streaming RecordBatch objects.
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Get next RecordBatch from the stream (async).
    ///
    /// Returns a PyArrow RecordBatch, or raises StopAsyncIteration when done.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            // Take the handle out to get mutable access for polling
            let mut handle = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| {
                    pyo3::exceptions::PyStopAsyncIteration::new_err("")
                })?
            };

            // Poll for next batch
            let result = handle.next().await;

            match result {
                Some(Ok(batch)) => {
                    // Put handle back for next iteration
                    {
                        let mut guard = inner.lock();
                        *guard = Some(handle);
                    }
                    // Convert to PyArrow
                    Python::with_gil(|py| record_batch_to_pyarrow(py, &batch))
                }
                Some(Err(e)) => {
                    // Put handle back even on error (stream may have more items)
                    {
                        let mut guard = inner.lock();
                        *guard = Some(handle);
                    }
                    Err(session_error_to_pyexc(&e))
                }
                None => {
                    // Stream exhausted - don't put handle back
                    Err(pyo3::exceptions::PyStopAsyncIteration::new_err(""))
                }
            }
        })
    }
}
