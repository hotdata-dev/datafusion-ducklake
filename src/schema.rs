//! DuckLake schema provider implementation

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result as DataFusionResult;

use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::resolve_path;
use crate::table::DuckLakeTable;

#[cfg(feature = "write")]
use crate::metadata_writer::{ColumnDef, MetadataWriter, WriteMode};
#[cfg(feature = "write")]
use datafusion::error::DataFusionError;
#[cfg(feature = "write")]
use std::path::PathBuf;

/// DuckLake schema provider
///
/// Represents a schema within a DuckLake catalog and provides access to tables.
/// Uses dynamic metadata lookup - tables are queried on-demand from the catalog database.
/// Caches snapshot_id received from catalog.schema() call for query consistency.
#[derive(Debug)]
pub struct DuckLakeSchema {
    schema_id: i64,
    schema_name: String,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    provider: Arc<dyn MetadataProvider>,
    /// Cached snapshot_id from catalog.schema() call
    snapshot_id: i64,
    /// Schema path for resolving relative table paths
    schema_path: String,
    /// Metadata writer for write operations (when write feature is enabled)
    #[cfg(feature = "write")]
    writer: Option<Arc<dyn MetadataWriter>>,
    /// Data path for write operations (when write feature is enabled)
    #[cfg(feature = "write")]
    data_path: Option<PathBuf>,
}

impl DuckLakeSchema {
    /// Create a new DuckLake schema
    pub fn new(
        schema_id: i64,
        schema_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64, // Received from catalog
        object_store_url: Arc<ObjectStoreUrl>,
        schema_path: String,
    ) -> Self {
        Self {
            schema_id,
            schema_name: schema_name.into(),
            provider,
            snapshot_id,
            object_store_url,
            schema_path,
            #[cfg(feature = "write")]
            writer: None,
            #[cfg(feature = "write")]
            data_path: None,
        }
    }

    /// Configure this schema for write operations.
    ///
    /// This method enables write support by attaching a metadata writer and data path.
    /// Once configured, the schema can handle CREATE TABLE AS and tables can handle INSERT INTO.
    ///
    /// # Arguments
    /// * `writer` - Metadata writer for catalog operations
    /// * `data_path` - Base path for data files
    #[cfg(feature = "write")]
    pub fn with_writer(mut self, writer: Arc<dyn MetadataWriter>, data_path: PathBuf) -> Self {
        self.writer = Some(writer);
        self.data_path = Some(data_path);
        self
    }
}

#[async_trait]
impl SchemaProvider for DuckLakeSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        // Use cached snapshot_id
        self.provider
            .list_tables(self.schema_id, self.snapshot_id)
            .inspect_err(|e| {
                tracing::error!(
                    error = %e,
                    schema_id = %self.schema_id,
                    snapshot_id = %self.snapshot_id,
                    schema_name = %self.schema_name,
                    "Failed to list tables from catalog"
                )
            })
            .unwrap_or_default()
            .into_iter()
            .map(|t| t.table_name)
            .collect()
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        // Use cached snapshot_id
        match self
            .provider
            .get_table_by_name(self.schema_id, name, self.snapshot_id)
        {
            Ok(Some(meta)) => {
                // Resolve table path hierarchically using path_resolver utility
                let table_path = resolve_path(&self.schema_path, &meta.path, meta.path_is_relative);

                // Pass snapshot_id to table
                let table = DuckLakeTable::new(
                    meta.table_id,
                    meta.table_name.clone(),
                    self.provider.clone(),
                    self.snapshot_id, // Propagate snapshot_id
                    self.object_store_url.clone(),
                    table_path,
                )
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                // Configure writer if this schema is writable
                #[cfg(feature = "write")]
                let table = if let (Some(writer), Some(data_path)) =
                    (self.writer.as_ref(), self.data_path.as_ref())
                {
                    table.with_writer(
                        self.schema_name.clone(),
                        Arc::clone(writer),
                        data_path.clone(),
                    )
                } else {
                    table
                };

                Ok(Some(Arc::new(table) as Arc<dyn TableProvider>))
            },
            Ok(None) => Ok(None),
            Err(e) => Err(datafusion::error::DataFusionError::External(Box::new(e))),
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        // Use cached snapshot_id
        self.provider
            .table_exists(self.schema_id, name, self.snapshot_id)
            .unwrap_or(false)
    }

    /// Register a new table in this schema.
    ///
    /// This is called by DataFusion for CREATE TABLE AS SELECT statements.
    /// It creates the table metadata in the catalog and returns a writable table provider.
    #[cfg(feature = "write")]
    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Schema is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        let data_path = self.data_path.as_ref().ok_or_else(|| {
            DataFusionError::Internal("Data path not set for writable schema".to_string())
        })?;

        // Convert Arrow schema to ColumnDefs
        let arrow_schema = table.schema();
        let columns: Vec<ColumnDef> = arrow_schema
            .fields()
            .iter()
            .map(|field| {
                ColumnDef::from_arrow(field.name(), field.data_type(), field.is_nullable())
                    .map_err(|e| DataFusionError::External(Box::new(e)))
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        // Create table in metadata (creates snapshot, table, columns in a transaction)
        let setup = writer
            .begin_write_transaction(&self.schema_name, &name, &columns, WriteMode::Replace)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Resolve table path
        let table_path = resolve_path(&self.schema_path, &name, true);

        // Create writable DuckLakeTable
        let writable_table = DuckLakeTable::new(
            setup.table_id,
            name,
            self.provider.clone(),
            setup.snapshot_id,
            self.object_store_url.clone(),
            table_path,
        )
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .with_writer(
            self.schema_name.clone(),
            Arc::clone(writer),
            data_path.clone(),
        );

        Ok(Some(Arc::new(writable_table) as Arc<dyn TableProvider>))
    }
}
