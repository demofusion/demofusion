//! PyO3 Python bindings for demofusion.

pub mod exceptions;
pub mod demo;
pub mod gotv;
pub mod session;
pub mod query_handle;
pub mod arrow_convert;

use pyo3::prelude::*;
use self::exceptions::*;
use self::demo::PyDemoSource;
use self::session::PyStreamingSession;
use self::query_handle::PyQueryHandle;

#[cfg(feature = "gotv")]
use self::gotv::PyGotvSource;

/// Python module for demofusion
#[pymodule]
#[pyo3(name = "_demofusion")]
fn _demofusion(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Bridge Rust tracing/log records to Python's logging module.
    // Tracing emits log records via the `tracing/log` feature; pyo3-log
    // forwards them to Python loggers named "demofusion.<target>".
    pyo3_log::init();

    // Register exception classes
    m.add("DemofusionError", py.get_type_bound::<DemofusionError>())?;
    m.add("DemofusionIOError", py.get_type_bound::<DemofusionIOError>())?;
    m.add("DemofusionSchemaError", py.get_type_bound::<DemofusionSchemaError>())?;
    m.add("DemofusionArrowError", py.get_type_bound::<DemofusionArrowError>())?;
    m.add("DemofusionDataFusionError", py.get_type_bound::<DemofusionDataFusionError>())?;
    m.add("DemofusionHasteError", py.get_type_bound::<DemofusionHasteError>())?;
    m.add("DemofusionSessionError", py.get_type_bound::<DemofusionSessionError>())?;

    // Register classes
    m.add_class::<PyDemoSource>()?;
    
    #[cfg(feature = "gotv")]
    m.add_class::<PyGotvSource>()?;
    
    m.add_class::<PyStreamingSession>()?;
    m.add_class::<PyQueryHandle>()?;

    Ok(())
}
