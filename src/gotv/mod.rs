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
//! # High-Level API: GotvSource
//!
//! For most use cases, use [`GotvSource`] which provides a simplified interface
//! via the [`IntoStreamingSession`](crate::IntoStreamingSession) trait:
//!
//! ```ignore
//! use demofusion::gotv::GotvSource;
//! use demofusion::IntoStreamingSession;
//! use tokio_util::sync::CancellationToken;
//! use futures::StreamExt;
//!
//! // Connect and discover schemas
//! let source = GotvSource::connect("http://dist1-ord1.steamcontent.com/tv/18895867").await?;
//! let (session, schemas) = source.into_session().await?;
//!
//! // Configure cancellation
//! let cancel = CancellationToken::new();
//! let mut session = session.with_cancel_token(cancel.clone());
//!
//! // Register queries
//! let mut pawns = session.add_query("SELECT tick, entity_index FROM CCitadelPlayerPawn").await?;
//!
//! // Start streaming
//! let _result = session.start()?;
//!
//! // Consume results
//! while let Some(result) = pawns.next().await {
//!     println!("Got {} rows", result?.num_rows());
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
mod source;

pub use client::{BroadcastClient, BroadcastStats, StreamEndReason, StreamResult, SyncResponse};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{GotvError, SqlError};
pub use session::{QueryHandle, SpectateResult, SpectateSession};
pub use source::GotvSource;

// Re-export StreamingStats for convenience when using gotv feature
pub use crate::datafusion::streaming_stats::{StreamingStats, StreamingStatsSnapshot};
