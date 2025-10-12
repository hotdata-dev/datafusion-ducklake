//! Error types for the DuckLake DataFusion extension

use std::fmt;

/// Error type for DuckLake operations
#[derive(Debug)]
pub enum DuckLakeError {
    /// Error from DataFusion
    DataFusion(datafusion::error::DataFusionError),

    /// Error from Arrow
    Arrow(arrow::error::ArrowError),

    /// DuckDB error
    DuckDb(duckdb::Error),

    /// Catalog not found
    CatalogNotFound(String),

    /// Schema not found
    SchemaNotFound(String),

    /// Table not found
    TableNotFound(String),

    /// Invalid snapshot
    InvalidSnapshot(String),

    /// Invalid catalog configuration
    InvalidConfig(String),

    /// IO error
    Io(std::io::Error),

    /// Generic error
    Internal(String),
}

impl fmt::Display for DuckLakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DuckLakeError::DataFusion(e) => write!(f, "DataFusion error: {}", e),
            DuckLakeError::Arrow(e) => write!(f, "Arrow error: {}", e),
            DuckLakeError::DuckDb(e) => write!(f, "DuckDB error: {}", e),
            DuckLakeError::CatalogNotFound(name) => write!(f, "Catalog not found: {}", name),
            DuckLakeError::SchemaNotFound(name) => write!(f, "Schema not found: {}", name),
            DuckLakeError::TableNotFound(name) => write!(f, "Table not found: {}", name),
            DuckLakeError::InvalidSnapshot(msg) => write!(f, "Invalid snapshot: {}", msg),
            DuckLakeError::InvalidConfig(msg) => write!(f, "Invalid configuration: {}", msg),
            DuckLakeError::Io(e) => write!(f, "IO error: {}", e),
            DuckLakeError::Internal(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for DuckLakeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DuckLakeError::DataFusion(e) => Some(e),
            DuckLakeError::Arrow(e) => Some(e),
            DuckLakeError::DuckDb(e) => Some(e),
            DuckLakeError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<datafusion::error::DataFusionError> for DuckLakeError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        DuckLakeError::DataFusion(err)
    }
}

impl From<arrow::error::ArrowError> for DuckLakeError {
    fn from(err: arrow::error::ArrowError) -> Self {
        DuckLakeError::Arrow(err)
    }
}

impl From<std::io::Error> for DuckLakeError {
    fn from(err: std::io::Error) -> Self {
        DuckLakeError::Io(err)
    }
}

impl From<duckdb::Error> for DuckLakeError {
    fn from(err: duckdb::Error) -> Self {
        DuckLakeError::DuckDb(err)
    }
}