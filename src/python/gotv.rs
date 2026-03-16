//! PyO3 wrapper for GotvSource (requires gotv feature).

#[cfg(feature = "gotv")]
pub mod gotv_impl {
    use pyo3::prelude::*;
    use pyo3_async_runtimes::tokio::future_into_py;
    use crate::gotv::GotvSource;
    use crate::session::IntoStreamingSession;
    use super::super::exceptions::session_error_to_pyexc;
    use super::super::session::PyStreamingSession;

    #[pyclass(name = "GotvSource")]
    pub struct PyGotvSource {
        inner: GotvSource,
    }

    #[pymethods]
    impl PyGotvSource {
        /// Connect to GOTV broadcast (async).
        #[staticmethod]
        fn connect(url: String, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
            future_into_py(py, async move {
                match GotvSource::connect(&url).await {
                    Ok(source) => Ok(PyGotvSource { inner: source }),
                    Err(e) => Err(session_error_to_pyexc(&e)),
                }
            })
        }

        /// Initialize session and discover schemas (async).
        #[pyo3(signature = (*, batch_size=None, reject_pipeline_breakers=None))]
        fn into_session(
            &self,
            py: Python<'_>,
            batch_size: Option<usize>,
            reject_pipeline_breakers: Option<bool>,
        ) -> PyResult<Bound<'_, PyAny>> {
            let inner = self.inner.clone();
            future_into_py(py, async move {
                match inner.into_session().await {
                    Ok((session, schemas)) => {
                        Ok(PyStreamingSession::from_session(
                            session,
                            schemas,
                            batch_size,
                            reject_pipeline_breakers,
                        ))
                    }
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
