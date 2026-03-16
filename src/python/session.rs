//! PyO3 wrapper for StreamingSession.

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

use crate::session::{SessionResult, StreamingSession, Schemas};
use super::arrow_convert::schema_to_pyarrow;
use super::exceptions::{session_error_to_pyexc, DemofusionSessionError};
use super::query_handle::PyQueryHandle;

#[pyclass(name = "StreamingSession")]
pub struct PyStreamingSession {
    /// The session itself, wrapped in Option so it can be consumed by start().
    /// While the session is alive (pre-start), add_query operates on it.
    /// After start(), the session is consumed and this becomes None.
    inner: Arc<parking_lot::Mutex<Option<StreamingSession>>>,
    /// Discovered schemas from into_session(), kept separately since they're
    /// needed after start() consumes the session.
    schemas: Schemas,
    /// The result from start(), kept alive to hold the parser JoinHandle.
    /// Dropping this would drop the parser task handle.
    _session_result: Arc<parking_lot::Mutex<Option<SessionResult>>>,
    /// Optional batch size configuration, applied in start().
    batch_size: Option<usize>,
    /// Optional reject pipeline breakers configuration, applied in start().
    reject_pipeline_breakers: Option<bool>,
}

impl PyStreamingSession {
    /// Create from a Rust StreamingSession, Schemas, and optional config values.
    pub fn from_session(
        session: StreamingSession,
        schemas: Schemas,
        batch_size: Option<usize>,
        reject_pipeline_breakers: Option<bool>,
    ) -> Self {
        PyStreamingSession {
            inner: Arc::new(parking_lot::Mutex::new(Some(session))),
            schemas,
            _session_result: Arc::new(parking_lot::Mutex::new(None)),
            batch_size,
            reject_pipeline_breakers,
        }
    }
}

#[pymethods]
impl PyStreamingSession {
    /// Get all available table schemas as a dictionary mapping table name to PyArrow Schema.
    #[getter]
    fn schemas(&self, py: Python<'_>) -> PyResult<PyObject> {
        let dict = pyo3::types::PyDict::new_bound(py);
        for (name, entity_schema) in &self.schemas {
            let py_schema = schema_to_pyarrow(py, &entity_schema.arrow_schema)?;
            dict.set_item(name.as_ref(), py_schema)?;
        }
        Ok(dict.into())
    }

    /// List all available table names.
    fn get_tables(&self) -> Vec<String> {
        self.schemas.keys().map(|s| s.to_string()).collect()
    }

    /// Get PyArrow schema for a specific table (returns None if not found).
    fn get_schema(&self, py: Python<'_>, table_name: &str) -> PyResult<Option<PyObject>> {
        match self.schemas.get(table_name) {
            Some(entity_schema) => {
                let py_schema = schema_to_pyarrow(py, &entity_schema.arrow_schema)?;
                Ok(Some(py_schema))
            }
            None => Ok(None),
        }
    }

    /// Register a SQL query (async). Returns a QueryHandle for iterating results.
    fn add_query<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            // We need &mut StreamingSession for add_query, so take it, call, put back
            let mut session = {
                let mut guard = inner.lock();
                guard.take().ok_or_else(|| {
                    DemofusionSessionError::new_err(
                        "Cannot add query after start()",
                    )
                })?
            };

            let result = session.add_query(&sql).await;

            // Put the session back regardless of success/failure
            {
                let mut guard = inner.lock();
                *guard = Some(session);
            }

            match result {
                Ok(handle) => Ok(PyQueryHandle::from_handle(handle)),
                Err(e) => Err(session_error_to_pyexc(&e)),
            }
        })
    }

    /// Begin streaming parser (synchronous). Consumes the session internally.
    ///
    /// Applies any stored configuration (batch_size, reject_pipeline_breakers)
    /// before starting. After calling start(), no more queries can be added.
    /// The parser task runs in the background and feeds data to all registered
    /// QueryHandles.
    fn start(&self) -> PyResult<()> {
        let mut inner_guard = self.inner.lock();
        let mut session = inner_guard.take().ok_or_else(|| {
            DemofusionSessionError::new_err("Session already started")
        })?;

        // Apply stored configuration
        if let Some(size) = self.batch_size {
            session = session.with_batch_size(size);
        }
        if let Some(reject) = self.reject_pipeline_breakers {
            session = session.with_reject_pipeline_breakers(reject);
        }

        // session.start() internally calls tokio::spawn(), so we need to be
        // inside the Tokio runtime context. Use pyo3-async-runtimes' runtime.
        let rt = pyo3_async_runtimes::tokio::get_runtime();
        let _guard = rt.enter();

        let session_result = session.start().map_err(|e| session_error_to_pyexc(&e))?;

        // Store the result to keep the parser task alive
        let mut result_guard = self._session_result.lock();
        *result_guard = Some(session_result);

        Ok(())
    }

    /// Enter async context manager.
    fn __aenter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    /// Exit async context manager (cleanup resources).
    fn __aexit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        // Drop the session if it hasn't been started
        self.inner.lock().take();
        // Drop the session result (and parser task handle)
        self._session_result.lock().take();
        Ok(())
    }
}
