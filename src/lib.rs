// Suppress warnings from pyo3 macro codegen that hasn't caught up with Rust 2024 lints.
// These are all in generated code from #[pymethods] / create_exception!, not in our code.
#![allow(unsafe_op_in_unsafe_fn)]
#![allow(unexpected_cfgs)]

pub mod batch;
pub mod datafusion;
pub mod demo;
pub mod error;
pub mod events;
pub mod haste;
pub mod schema;
pub mod session;
pub mod sql;
pub mod visitor;

#[cfg(feature = "gotv")]
pub mod gotv;

#[cfg(feature = "python")]
pub mod python;

pub use session::{
    IntoStreamingSession, QueryHandle, SessionError, SessionResult, StreamingSession,
};
