//! DuckLake catalog provider implementation

use std::any::Any;
use std::sync::Arc;

use datafusion::catalog::{CatalogProvider, SchemaProvider};

use crate::error::Result;

/// DuckLake catalog provider
///
/// Connects to a DuckLake catalog database and provides access to schemas and tables.
#[derive(Debug)]
pub struct DuckLakeCatalog {
    /// Connection to the catalog database
    catalog_path: String,
    /// Latest snapshot ID
    snapshot_id: i64,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog
    pub fn new(catalog_path: impl Into<String>) -> Result<Self> {
        let catalog_path = catalog_path.into();

        // TODO: Connect to catalog database
        // TODO: Query ducklake_snapshot to get latest snapshot ID

        Ok(Self {
            catalog_path,
            snapshot_id: 0, // TODO: Get actual latest snapshot
        })
    }

    /// Get the latest snapshot ID
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        // TODO: Query ducklake_schema table to get schema names for current snapshot
        vec![]
    }

    fn schema(&self, _name: &str) -> Option<Arc<dyn SchemaProvider>> {
        // TODO: Query ducklake_schema to get schema_id for this name
        // TODO: Create and return DuckLakeSchema
        None
    }
}