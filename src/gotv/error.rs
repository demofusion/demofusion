//! Error types for GOTV broadcast client.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum GotvError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Broadcast ended (no new fragments after retries)")]
    BroadcastEnded,

    #[error("Broadcast not ready after timeout")]
    BroadcastNotReady,

    #[error("Cancelled")]
    Cancelled,

    #[error("Channel closed")]
    ChannelClosed,

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Schema discovery error: {0}")]
    Schema(String),

    #[error("SQL error: {0}")]
    Sql(String),

    #[error("Unknown entity type: {0}")]
    UnknownEntity(String),

    #[error("DataFusion error: {0}")]
    DataFusion(String),

    #[error("Query contains pipeline breakers that prevent streaming:\n{0}")]
    PipelineBreaker(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Session not started")]
    NotStarted,

    #[error("Internal error: {0}")]
    Internal(String),
}

impl GotvError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http(e) => e.is_timeout() || e.is_connect() || e.is_request(),
            Self::BroadcastNotReady => true,
            _ => false,
        }
    }
}
