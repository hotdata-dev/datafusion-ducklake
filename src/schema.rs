//! DuckLake schema provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result as DataFusionResult;

use crate::catalog::CachedTable;
#[cfg(feature = "write")]
use crate::metadata_provider::{DuckLakeTableColumn, TableMetadata};
#[cfg(feature = "write")]
use crate::path_resolver::resolve_path;
#[cfg(feature = "write")]
use crate::table::DuckLakeTable;

#[cfg(feature = "write")]
use crate::metadata_writer::{ColumnDef, MetadataWriter, WriteMode, validate_name};
#[cfg(feature = "write")]
use datafusion::error::DataFusionError;

/// Validate table name to prevent path traversal attacks and reject
/// empty, control-character, or overlength names.
///
/// Table names are used to construct file paths, so we must ensure they
/// don't contain path separators or parent directory references.
#[cfg(feature = "write")]
fn validate_table_name(name: &str) -> DataFusionResult<()> {
    // Shared name validation (empty, control chars, length)
    validate_name(name, "Table").map_err(|e| DataFusionError::External(Box::new(e)))?;
    if name.contains('/') || name.contains('\\') || name.contains("..") {
        return Err(DataFusionError::Plan(format!(
            "Invalid table name '{}': must not contain path separators or '..'",
            name
        )));
    }
    // Also reject names that are just dots
    if name.chars().all(|c| c == '.') {
        return Err(DataFusionError::Plan(format!(
            "Invalid table name '{}': must not be only dots",
            name
        )));
    }
    Ok(())
}

/// DuckLake schema provider
///
/// Represents a schema within a DuckLake catalog and provides access to tables.
/// Uses a snapshot-pinned table cache populated by the parent catalog.
#[derive(Debug)]
pub struct DuckLakeSchema {
    #[cfg(feature = "write")]
    schema_name: String,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    #[cfg(feature = "write")]
    object_store_url: Arc<ObjectStoreUrl>,
    /// Snapshot-pinned table cache shared with the parent catalog
    tables: Arc<RwLock<HashMap<String, CachedTable>>>,
    /// Schema path for resolving relative table paths
    #[cfg(feature = "write")]
    schema_path: String,
    /// Metadata writer for write operations (when write feature is enabled)
    #[cfg(feature = "write")]
    writer: Option<Arc<dyn MetadataWriter>>,
}

impl DuckLakeSchema {
    /// Create a new DuckLake schema
    pub(crate) fn new(
        #[cfg(feature = "write")] schema_name: impl Into<String>,
        #[cfg(not(feature = "write"))] _schema_name: impl Into<String>,
        tables: Arc<RwLock<HashMap<String, CachedTable>>>,
        #[cfg(feature = "write")] object_store_url: Arc<ObjectStoreUrl>,
        #[cfg(not(feature = "write"))] _object_store_url: Arc<ObjectStoreUrl>,
        #[cfg(feature = "write")] schema_path: String,
        #[cfg(not(feature = "write"))] _schema_path: String,
    ) -> Self {
        Self {
            #[cfg(feature = "write")]
            schema_name: schema_name.into(),
            tables,
            #[cfg(feature = "write")]
            object_store_url,
            #[cfg(feature = "write")]
            schema_path,
            #[cfg(feature = "write")]
            writer: None,
        }
    }

    /// Configure this schema for write operations.
    ///
    /// This method enables write support by attaching a metadata writer.
    /// Once configured, the schema can handle CREATE TABLE AS and tables can handle INSERT INTO.
    ///
    /// # Arguments
    /// * `writer` - Metadata writer for catalog operations
    #[cfg(feature = "write")]
    pub fn with_writer(mut self, writer: Arc<dyn MetadataWriter>) -> Self {
        self.writer = Some(writer);
        self
    }
}

#[async_trait]
impl SchemaProvider for DuckLakeSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .tables
            .read()
            .expect("Schema table cache poisoned")
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let table = self
            .tables
            .read()
            .expect("Schema table cache poisoned")
            .get(name)
            .cloned()
            .map(|cached| -> Arc<dyn TableProvider> { cached.provider });

        Ok(table)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables
            .read()
            .expect("Schema table cache poisoned")
            .contains_key(name)
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
        // Validate table name to prevent path traversal attacks
        validate_table_name(&name)?;

        let writer = self.writer.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Schema is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
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
        let table_path = resolve_path(&self.schema_path, &name, true)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let ducklake_columns: Vec<DuckLakeTableColumn> = setup
            .column_ids
            .iter()
            .zip(columns.iter())
            .map(|(column_id, column)| {
                DuckLakeTableColumn::new(
                    *column_id,
                    column.name().to_string(),
                    column.ducklake_type().to_string(),
                    column.is_nullable(),
                )
            })
            .collect();

        // Create writable DuckLakeTable
        let writable_table = Arc::new(
            DuckLakeTable::from_loaded_metadata(
                setup.table_id,
                name.clone(),
                self.object_store_url.clone(),
                table_path.clone(),
                ducklake_columns,
                Vec::new(),
                None,
                Some(setup.snapshot_id),
            )
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .with_writer(self.schema_name.clone(), Arc::clone(writer)),
        );

        self.tables
            .write()
            .expect("Schema table cache poisoned")
            .insert(
                name.clone(),
                CachedTable {
                    metadata: TableMetadata {
                        table_id: setup.table_id,
                        table_name: name.clone(),
                        path: name.clone(),
                        path_is_relative: true,
                    },
                    table_path,
                    provider: Arc::clone(&writable_table),
                },
            );

        Ok(Some(writable_table as Arc<dyn TableProvider>))
    }
}

#[cfg(all(test, feature = "write"))]
mod tests {
    use super::*;

    #[test]
    fn test_validate_table_name_valid() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("my_table").is_ok());
        assert!(validate_table_name("Table123").is_ok());
        assert!(validate_table_name("a").is_ok());
    }

    #[test]
    fn test_validate_table_name_empty() {
        let result = validate_table_name("");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_validate_table_name_path_traversal() {
        // Forward slash
        let result = validate_table_name("../etc/passwd");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("path separators"));

        // Backslash
        let result = validate_table_name("..\\windows\\system32");
        assert!(result.is_err());

        // Double dot
        let result = validate_table_name("foo..bar");
        assert!(result.is_err());

        // Just slashes
        let result = validate_table_name("foo/bar");
        assert!(result.is_err());

        assert!(validate_table_name("foo/bar").is_err());
        assert!(validate_table_name("foo\\bar").is_err());
    }

    #[test]
    fn test_validate_table_name_only_dots() {
        assert!(validate_table_name(".").is_err());
        assert!(validate_table_name("..").is_err());
        assert!(validate_table_name("...").is_err());
    }

    #[test]
    fn test_validate_table_name_control_chars() {
        assert!(validate_table_name("table\0name").is_err());
        assert!(validate_table_name("table\nname").is_err());
        assert!(validate_table_name("table\tname").is_err());
        assert!(validate_table_name("\x7Ftable").is_err());
    }
}
