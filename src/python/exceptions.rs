//! PyO3 exception types for demofusion.

use pyo3::create_exception;
use pyo3::prelude::*;

create_exception!(demofusion, DemofusionError, pyo3::exceptions::PyException);
create_exception!(demofusion, DemofusionIOError, DemofusionError);
create_exception!(demofusion, DemofusionSchemaError, DemofusionError);
create_exception!(demofusion, DemofusionArrowError, DemofusionError);
create_exception!(demofusion, DemofusionDataFusionError, DemofusionError);
create_exception!(demofusion, DemofusionHasteError, DemofusionError);
create_exception!(demofusion, DemofusionSessionError, DemofusionError);

/// Convert Rust errors to Python exceptions
pub fn session_error_to_pyexc(err: &crate::session::SessionError) -> PyErr {
    use crate::session::SessionError;

    match err {
        SessionError::Io(_) => DemofusionIOError::new_err(err.to_string()),
        SessionError::Schema(_) => DemofusionSchemaError::new_err(err.to_string()),
        SessionError::Sql(_) | SessionError::Query(_) | SessionError::UnknownTable(_) => {
            DemofusionDataFusionError::new_err(err.to_string())
        }
        SessionError::DataFusion(_) => DemofusionDataFusionError::new_err(err.to_string()),
        SessionError::Parser(_) | SessionError::PipelineBreaker(_) => {
            DemofusionHasteError::new_err(err.to_string())
        }
        SessionError::AlreadyStarted | SessionError::NoQueries | SessionError::Internal(_) => {
            DemofusionSessionError::new_err(err.to_string())
        }
    }
}
