//! PyO3 wrapper for StreamingSession.

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

use super::arrow_convert::schema_to_pyarrow;
use super::exceptions::{DemofusionSessionError, session_error_to_pyexc};
use super::query_handle::PyQueryHandle;
use crate::session::{SessionResult, StreamingSession};

#[pyclass(name = "StreamingSession")]
pub struct PyStreamingSession {
    /// The session, behind a tokio Mutex so the guard can be held across
    /// `.await` points (needed by `add_query`).
    inner: Arc<tokio::sync::Mutex<Option<StreamingSession>>>,
    /// The result from start(), kept alive to hold the parser JoinHandle.
    /// Dropping this would drop the parser task handle.
    _session_result: Arc<parking_lot::Mutex<Option<SessionResult>>>,
    /// Optional batch size configuration, applied in start().
    batch_size: Option<usize>,
    /// Optional reject pipeline breakers configuration, applied in start().
    reject_pipeline_breakers: Option<bool>,
}

impl PyStreamingSession {
    /// Create from a Rust StreamingSession and optional config values.
    pub fn from_session(
        session: StreamingSession,
        batch_size: Option<usize>,
        reject_pipeline_breakers: Option<bool>,
    ) -> Self {
        PyStreamingSession {
            inner: Arc::new(tokio::sync::Mutex::new(Some(session))),
            _session_result: Arc::new(parking_lot::Mutex::new(None)),
            batch_size,
            reject_pipeline_breakers,
        }
    }
}

#[pymethods]
impl PyStreamingSession {
    /// Get all available table schemas as a dictionary mapping table name to PyArrow Schema.
    /// Includes both entity tables (from demo schema discovery) and event tables (compile-time).
    #[getter]
    fn schemas(&self, py: Python<'_>) -> PyResult<PyObject> {
        let guard = self.inner.blocking_lock();
        let session = guard
            .as_ref()
            .ok_or_else(|| DemofusionSessionError::new_err("Session has been closed"))?;
        let dict = pyo3::types::PyDict::new_bound(py);
        // Entity schemas
        for entity_schema in session.schemas() {
            let py_schema = schema_to_pyarrow(py, &entity_schema.arrow_schema)?;
            dict.set_item(entity_schema.serializer_name.as_ref(), py_schema)?;
        }
        // Event schemas
        for name in session.event_names() {
            if let Some(arrow_schema) = session.get_table_schema(name) {
                let py_schema = schema_to_pyarrow(py, &arrow_schema)?;
                dict.set_item(name, py_schema)?;
            }
        }
        Ok(dict.into())
    }

    /// List all available table names (entity + event).
    fn get_tables(&self) -> PyResult<Vec<String>> {
        let guard = self.inner.blocking_lock();
        let session = guard
            .as_ref()
            .ok_or_else(|| DemofusionSessionError::new_err("Session has been closed"))?;
        Ok(session
            .all_table_names()
            .into_iter()
            .map(|s| s.to_string())
            .collect())
    }

    /// Get PyArrow schema for a specific table (entity or event). Returns None if not found.
    fn get_schema(&self, py: Python<'_>, table_name: &str) -> PyResult<Option<PyObject>> {
        let guard = self.inner.blocking_lock();
        let session = guard
            .as_ref()
            .ok_or_else(|| DemofusionSessionError::new_err("Session has been closed"))?;
        match session.get_table_schema(table_name) {
            Some(arrow_schema) => {
                let py_schema = schema_to_pyarrow(py, &arrow_schema)?;
                Ok(Some(py_schema))
            }
            None => Ok(None),
        }
    }

    /// Register a SQL query (async). Returns a QueryHandle for iterating results.
    fn add_query<'py>(&self, py: Python<'py>, sql: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let session = guard.as_mut().ok_or_else(|| {
                DemofusionSessionError::new_err("Cannot add query after session has been closed")
            })?;

            match session.add_query(&sql).await {
                Ok(handle) => Ok(PyQueryHandle::from_handle(handle)),
                Err(e) => Err(session_error_to_pyexc(&e)),
            }
        })
    }

    /// Begin streaming parser (synchronous).
    ///
    /// Applies any stored configuration (batch_size, reject_pipeline_breakers)
    /// before starting. After calling start(), no more queries can be added.
    /// The parser task runs in the background and feeds data to all registered
    /// QueryHandles. The session remains accessible for schema queries.
    fn start(&self) -> PyResult<()> {
        let mut guard = self.inner.blocking_lock();
        let _session = guard
            .as_mut()
            .ok_or_else(|| DemofusionSessionError::new_err("Session already started or closed"))?;

        // Apply stored configuration.
        // These are builder methods that take `self` by value, so we need to
        // temporarily take the session out and put the configured one back.
        if self.batch_size.is_some() || self.reject_pipeline_breakers.is_some() {
            let mut s = guard.take().unwrap();
            if let Some(size) = self.batch_size {
                s = s.with_batch_size(size);
            }
            if let Some(reject) = self.reject_pipeline_breakers {
                s = s.with_reject_pipeline_breakers(reject);
            }
            *guard = Some(s);
        }

        // session.start() internally calls tokio::spawn(), so we need to be
        // inside the Tokio runtime context. Use pyo3-async-runtimes' runtime.
        let rt = pyo3_async_runtimes::tokio::get_runtime();
        let _rt_guard = rt.enter();

        let session = guard.as_mut().unwrap();
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
    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __aexit__(
        &mut self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        // Drop the session
        self.inner.blocking_lock().take();
        // Drop the session result (and parser task handle)
        self._session_result.lock().take();
        Ok(())
    }
}
