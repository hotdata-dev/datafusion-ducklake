//! Error types for the DuckLake DataFusion extension

use thiserror::Error;

/// Error type for DuckLake operations
#[derive(Error, Debug)]
pub enum DuckLakeError {
    /// Error from DataFusion
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    /// Error from Arrow
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// DuckDB error
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),

    /// Catalog not found
    #[error("Catalog not found: {0}")]
    CatalogNotFound(String),

    /// Schema not found
    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    /// Table not found
    #[error("Table not found: {0}")]
    TableNotFound(String),

    /// Invalid snapshot
    #[error("Invalid snapshot: {0}")]
    InvalidSnapshot(String),

    /// Invalid catalog configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Unsupported DuckLake type
    #[error("Unsupported DuckLake type: {0}")]
    UnsupportedType(String),

    /// Unsupported feature
    #[error("Unsupported feature: {0}")]
    Unsupported(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Generic error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<DuckLakeError> for datafusion::error::DataFusionError {
    fn from(err: DuckLakeError) -> Self {
        match err {
            // If it's already a DataFusion error, unwrap it
            DuckLakeError::DataFusion(e) => e,
            // For all other errors, wrap them as External
            other => datafusion::error::DataFusionError::External(Box::new(other)),
        }
    }
}
