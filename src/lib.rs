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
//! # async fn example() -> datafusion_ducklake::error::Result<()> {
//! use datafusion::prelude::*;
//! use datafusion_ducklake::DuckLakeCatalog;
//!
//! // Create a DataFusion session context
//! let ctx = SessionContext::new();
//!
//! // Register a DuckLake catalog
//! let catalog = DuckLakeCatalog::new("path/to/catalog.ducklake")?;
//! ctx.register_catalog("ducklake", std::sync::Arc::new(catalog));
//!
//! // Query tables from the catalog
//! let df = ctx.sql("SELECT * FROM ducklake.main.my_table").await?;
//! df.show().await?;
//! # Ok(())
//! # }
//! ```

pub mod catalog;
pub mod error;
pub mod schema;
pub mod table;
mod metadata_provider;
mod metadata_provider_duckdb;

// Re-export main types for convenience
pub use catalog::DuckLakeCatalog;
pub use error::{DuckLakeError, Result};
pub use schema::DuckLakeSchema;
pub use table::DuckLakeTable;
