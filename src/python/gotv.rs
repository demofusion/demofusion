//! PyO3 wrapper for GotvSource (requires gotv feature).

#[cfg(feature = "gotv")]
pub mod gotv_impl {
    use std::sync::Arc;

    use super::super::exceptions::{DemofusionError, DemofusionSessionError, session_error_to_pyexc};
    use super::super::session::PyStreamingSession;
    use crate::gotv::GotvSource;
    use crate::session::IntoStreamingSession;
    use pyo3::prelude::*;
    use pyo3_async_runtimes::tokio::future_into_py;

    #[pyclass(name = "GotvSource")]
    pub struct PyGotvSource {
        inner: Arc<parking_lot::Mutex<Option<GotvSource>>>,
    }

    #[pymethods]
    impl PyGotvSource {
        /// Connect to GOTV broadcast (async).
        #[staticmethod]
        fn connect(url: String, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
            future_into_py(py, async move {
                match GotvSource::connect(&url).await {
                    Ok(source) => Ok(PyGotvSource {
                        inner: Arc::new(parking_lot::Mutex::new(Some(source))),
                    }),
                    Err(e) => Err(DemofusionError::new_err(e.to_string())),
                }
            })
        }

        /// Initialize session and discover schemas (async).
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
                        "GotvSource already consumed by into_session()",
                    )
                })?;
                match source.into_session().await {
                    Ok(session) => Ok(PyStreamingSession::from_session(
                        session,
                        batch_size,
                        reject_pipeline_breakers,
                    )),
                    Err(e) => Err(session_error_to_pyexc(&e)),
                }
            })
        }
    }
}

#[cfg(not(feature = "gotv"))]
pub mod gotv_impl {
    use pyo3::prelude::*;

    #[pyclass(name = "GotvSource")]
    pub struct PyGotvSource;

    #[pymethods]
    impl PyGotvSource {
        #[staticmethod]
        fn connect(_url: String, _py: Python<'_>) -> PyResult<()> {
            Err(pyo3::exceptions::PyRuntimeError::new_err(
                "GotvSource requires 'gotv' feature to be enabled",
            ))
        }
    }
}

pub use gotv_impl::PyGotvSource;
