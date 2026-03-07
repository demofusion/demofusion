//! GOTV HTTP Broadcast client for streaming live match data.
//!
//! This module provides a client for Valve's GOTV broadcast protocol,
//! enabling real-time streaming of demo packets from live matches.
//!
//! # Feature Flag
//!
//! This module requires the `gotv` feature:
//!
//! ```toml
//! [dependencies]
//! demofusion = { version = "0.1", features = ["gotv"] }
//! ```
//!
//! # High-Level API: SpectateSession
//!
//! For most use cases, use [`SpectateSession`] which provides a simplified interface:
//!
//! ```ignore
//! use demofusion::gotv::SpectateSession;
//! use tokio_util::sync::CancellationToken;
//! use futures::StreamExt;
//!
//! // Connect and discover schemas
//! let mut session = SpectateSession::connect("http://dist1-ord1.steamcontent.com/tv/18895867").await?;
//! println!("Map: {}", session.map());
//!
//! // Register queries
//! let pawns = session.add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn")?;
//!
//! // Start streaming
//! let cancel = CancellationToken::new();
//! session.start(cancel.clone()).await?;
//!
//! // Consume results
//! while let Some(result) = pawns.lock().next().await {
//!     let batch = result?;
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```
//!
//! # Low-Level API: BroadcastClient
//!
//! For more control, use [`BroadcastClient`] directly:
//!
//! ```ignore
//! use demofusion::gotv::{BroadcastClient, ClientConfig};
//! use demofusion::streaming::discover_schemas_from_broadcast;
//! use tokio::sync::mpsc;
//!
//! // Create client
//! let mut client = BroadcastClient::new("http://dist1-ord1.steamcontent.com/tv/18895867");
//!
//! // Sync to get broadcast metadata
//! let sync = client.sync().await?;
//! println!("Map: {}, tick: {}", sync.map, sync.tick);
//!
//! // Fetch start packet for schema discovery
//! let start_packet = client.fetch_start().await?;
//! let schemas = discover_schemas_from_broadcast(&start_packet).await?;
//!
//! // Stream packets to channel
//! let (tx, rx) = mpsc::channel(32);
//! tokio::spawn(async move {
//!     // Use rx with parser...
//! });
//!
//! let result = client.stream_to_channel(tx).await?;
//! println!("Downloaded {} bytes", result.stats.bytes_downloaded);
//! ```

mod client;
mod config;
mod error;
mod session;

pub use client::{BroadcastClient, BroadcastStats, StreamEndReason, StreamResult, SyncResponse};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::GotvError;
pub use session::{QueryHandle, SpectateResult, SpectateSession};

// Re-export StreamingStats for convenience when using gotv feature
pub use crate::datafusion::streaming_stats::{StreamingStats, StreamingStatsSnapshot};
