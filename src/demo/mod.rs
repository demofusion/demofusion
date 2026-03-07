//! Demo file parsing for static `.dem` files.
//!
//! This module provides [`DemoSource`] for querying static demo files
//! using SQL via the [`IntoStreamingSession`](crate::IntoStreamingSession) trait.
//!
//! # Example
//!
//! ```ignore
//! use demofusion::demo::DemoSource;
//! use demofusion::IntoStreamingSession;
//! use futures::StreamExt;
//!
//! // Open a demo file and create a session
//! let source = DemoSource::open("match.dem").await?;
//! let (mut session, schemas) = source.into_session().await?;
//!
//! println!("Available entities: {:?}", session.entity_names());
//!
//! // Register queries
//! let mut pawns = session.add_query(
//!     "SELECT tick, entity_index, m_iHealth FROM CCitadelPlayerPawn"
//! ).await?;
//!
//! // Start streaming (parses file in background)
//! let _result = session.start()?;
//!
//! // Consume results as they stream
//! while let Some(batch) = pawns.next().await {
//!     println!("Got {} rows", batch?.num_rows());
//! }
//! ```

mod session;
mod source;

pub use session::{DemoFileSession, DemoResult, QueryHandle};
pub use source::DemoSource;
