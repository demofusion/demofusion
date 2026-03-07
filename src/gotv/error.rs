//! Error types for GOTV broadcast client.

use datafusion::common::{DataFusionError, SchemaError};
use datafusion::sql::sqlparser::parser::ParserError;
use thiserror::Error;

#[derive(Debug, Clone, Error)]
pub enum SqlError {
    #[error("SQL syntax error at line {line}, column {column}: {message}")]
    Syntax {
        message: String,
        line: u64,
        column: u64,
    },

    #[error("Field '{field}' not found")]
    FieldNotFound { field: String },

    #[error("Ambiguous field reference: '{field}'")]
    AmbiguousReference { field: String },

    #[error("Duplicate qualified field: {qualifier}.{name}")]
    DuplicateQualifiedField { qualifier: String, name: String },

    #[error("Duplicate unqualified field: {name}")]
    DuplicateUnqualifiedField { name: String },

    #[error("Invalid table reference: {table}")]
    InvalidTable { table: String },

    #[error("Not implemented: {feature}")]
    NotImplemented { feature: String },

    #[error("Type error: {0}")]
    Type(String),

    #[error("Internal planning error")]
    Internal,
}

impl SqlError {
    pub fn from_datafusion(err: DataFusionError) -> Self {
        match err {
            DataFusionError::SQL(parser_err, _) => Self::from_parser_error(*parser_err),

            DataFusionError::SchemaError(schema_err, _) => match *schema_err {
                SchemaError::FieldNotFound { field, .. } => SqlError::FieldNotFound {
                    field: field.flat_name(),
                },
                SchemaError::AmbiguousReference { field } => SqlError::AmbiguousReference {
                    field: field.flat_name(),
                },
                SchemaError::DuplicateQualifiedField { qualifier, name } => {
                    SqlError::DuplicateQualifiedField {
                        qualifier: qualifier.to_string(),
                        name,
                    }
                }
                SchemaError::DuplicateUnqualifiedField { name } => {
                    SqlError::DuplicateUnqualifiedField { name }
                }
            },

            DataFusionError::Plan(msg) => Self::extract_plan_error(&msg),

            DataFusionError::NotImplemented(feature) => SqlError::NotImplemented { feature },

            DataFusionError::Context(_, inner) => SqlError::from_datafusion(*inner),

            DataFusionError::ArrowError(_, _)
            | DataFusionError::Execution(_)
            | DataFusionError::ExecutionJoin(_)
            | DataFusionError::ResourcesExhausted(_) => {
                tracing::warn!(error = %err, "DataFusion error mapped to Internal");
                SqlError::Internal
            }

            _ => {
                tracing::warn!(error = %err, "Unknown DataFusion error mapped to Internal");
                SqlError::Internal
            }
        }
    }

    pub fn from_parser_error(err: ParserError) -> Self {
        match err {
            ParserError::TokenizerError(msg) => SqlError::Syntax {
                message: msg,
                line: 0,
                column: 0,
            },
            ParserError::ParserError(msg) => SqlError::Syntax {
                message: msg,
                line: 0,
                column: 0,
            },
            ParserError::RecursionLimitExceeded => SqlError::Syntax {
                message: "Query too deeply nested".to_string(),
                line: 0,
                column: 0,
            },
        }
    }

    fn extract_plan_error(msg: &str) -> Self {
        if let Some(field) = msg.strip_prefix("No field named ") {
            let field = field
                .split('.')
                .next()
                .unwrap_or(field)
                .trim_matches('"')
                .to_string();
            return SqlError::FieldNotFound { field };
        }

        if msg.contains("table") && msg.contains("not found") {
            if let Some(start) = msg.find('\'') {
                if let Some(end) = msg[start + 1..].find('\'') {
                    let table = msg[start + 1..start + 1 + end].to_string();
                    return SqlError::InvalidTable { table };
                }
            }
        }

        tracing::warn!(error_message = %msg, "Unhandled DataFusion plan error");
        SqlError::Internal
    }
}

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

    #[error("{0}")]
    Sql(#[from] SqlError),

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
