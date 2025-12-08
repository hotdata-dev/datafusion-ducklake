//! # DataFusion-DuckLake
//!
//! A DataFusion extension that adds support for DuckLake, an integrated data lake and catalog format.
//!
//! ## Overview
//!
//! DuckLake uses:
//! - **Catalog Database**: SQL database (DuckDB, SQLite, PostgreSQL, MySQL) storing metadata as SQL tables
//! - **Data Storage**: Apache Parquet files stored on disk/object storage
//!
//! This extension provides read-only access to DuckLake catalogs through DataFusion's
//! catalog and table provider interfaces.
//!
//! ## Example
//!
//! ```no_run
//! # async fn example() -> datafusion_ducklake::Result<()> {
//! use datafusion::prelude::*;
//! use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
//!
//! // Create a DataFusion session context
//! let ctx = SessionContext::new();
//!
//! // Create a DuckDB metadata provider
//! let provider = DuckdbMetadataProvider::new("path/to/catalog.ducklake")?;
//!
//! // Create catalog (default: always queries latest snapshot)
//! let catalog = DuckLakeCatalog::new(provider)?;
//!
//! // Register the catalog with DataFusion
//! ctx.register_catalog("ducklake", std::sync::Arc::new(catalog));
//!
//! // Query tables from the catalog
//! let df = ctx.sql("SELECT * FROM ducklake.main.my_table").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

pub mod catalog;
pub mod delete_filter;
pub mod error;
pub mod metadata_provider;
pub mod metadata_provider_duckdb;
pub mod path_resolver;
pub mod schema;
pub mod table;
pub mod types;

// Result type for DuckLake operations
pub type Result<T> = std::result::Result<T, DuckLakeError>;

// Re-export main types for convenience
pub use catalog::{DuckLakeCatalog, SnapshotConfig};
pub use error::DuckLakeError;
pub use metadata_provider::MetadataProvider;
pub use metadata_provider_duckdb::DuckdbMetadataProvider;
pub use schema::DuckLakeSchema;
pub use table::DuckLakeTable;
