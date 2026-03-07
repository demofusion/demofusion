#![allow(clippy::all)]
#![allow(unused_variables)]

mod batch_builder;

// Include the generated event types and functions
include!(concat!(env!("OUT_DIR"), "/events_generated.rs"));

pub use batch_builder::{EventBatchBuilder, EventBatchBuilders};
