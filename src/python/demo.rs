//! PyO3 wrapper for DemoSource.

use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

use crate::demo::DemoSource;
use crate::session::IntoStreamingSession;
use super::exceptions::{session_error_to_pyexc, DemofusionSessionError};
use super::session::PyStreamingSession;

#[pyclass(name = "DemoSource")]
pub struct PyDemoSource {
    inner: Arc<parking_lot::Mutex<Option<DemoSource>>>,
}

#[pymethods]
impl PyDemoSource {
    /// Load demo from file path (async).
    #[staticmethod]
    fn open(path: String, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        future_into_py(py, async move {
            match DemoSource::open(&path).await {
                Ok(source) => Ok(PyDemoSource {
                    inner: Arc::new(parking_lot::Mutex::new(Some(source))),
                }),
                Err(e) => Err(session_error_to_pyexc(&e)),
            }
        })
    }

    /// Load demo from bytes (synchronous).
    #[staticmethod]
    fn from_bytes(data: Vec<u8>) -> Self {
        PyDemoSource {
            inner: Arc::new(parking_lot::Mutex::new(Some(DemoSource::from_bytes(data)))),
        }
    }

    /// Initialize session and discover schemas (async).
    ///
    /// Optional keyword arguments:
    ///   batch_size: Number of rows per RecordBatch.
    ///   reject_pipeline_breakers: Whether to reject memory-intensive queries.
    #[pyo3(signature = (*, batch_size=None, reject_pipeline_breakers=None))]
    fn into_session<'py>(
        &self,
        py: Python<'py>,
        batch_size: Option<usize>,
        reject_pipeline_breakers: Option<bool>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let source = inner.lock().take().ok_or_else(|| {
                DemofusionSessionError::new_err(
                    "DemoSource already consumed by into_session()",
                )
            })?;
            match source.into_session().await {
                Ok(session) => {
                    Ok(PyStreamingSession::from_session(
                        session,
                        batch_size,
                        reject_pipeline_breakers,
                    ))
                }
                Err(e) => Err(session_error_to_pyexc(&e)),
            }
        })
    }
}
