//! Demo file parsing for static `.dem` files.
//!
//! This module provides `DemoFileSession` for querying static demo files
//! using the same streaming architecture as live GOTV broadcasts.
//!
//! # Example
//!
//! ```ignore
//! use demofusion::demo::DemoFileSession;
//! use futures::StreamExt;
//!
//! // Open a demo file
//! let mut session = DemoFileSession::open("match.dem").await?;
//! println!("Available entities: {:?}", session.entity_names());
//!
//! // Register queries
//! let mut pawns = session.add_query(
//!     "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn"
//! ).await?;
//!
//! // Start streaming (parses file in background)
//! let result = session.start().await?;
//!
//! // Consume results as they stream
//! while let Some(batch) = pawns.next().await {
//!     let batch = batch?;
//!     println!("Got {} rows", batch.num_rows());
//! }
//!
//! // Check for parser errors
//! result.parser_handle.await??;
//! ```

mod session;

pub use session::{DemoFileSession, DemoResult, QueryHandle};
