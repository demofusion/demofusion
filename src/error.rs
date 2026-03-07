use thiserror::Error;

#[derive(Error, Debug)]
pub enum Source2DfError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] datafusion::arrow::error::ArrowError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] ::datafusion::error::DataFusionError),

    #[error("Haste parse error: {0}")]
    Haste(String),

    #[error("Schema error: {0}")]
    Schema(String),
}

pub type Result<T> = std::result::Result<T, Source2DfError>;
