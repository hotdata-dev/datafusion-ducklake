//! DuckLake table provider implementation

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use url::Url;

use crate::metadata_provider::MetadataProvider;
use crate::types::build_arrow_schema;
use crate::Result;

/// DuckLake table provider
///
/// Represents a table within a DuckLake schema and provides access to data via Parquet files.
#[derive(Debug)]
pub struct DuckLakeTable {
    #[allow(dead_code)]
    table_id: i64,
    #[allow(dead_code)]
    table_name: String,
    #[allow(dead_code)]
    provider: Arc<dyn MetadataProvider>,
    #[allow(dead_code)]
    snapshot_id: i64,
    /// Base data path for resolving relative file paths
    data_path: String,
    schema: SchemaRef,
    /// Resolved absolute paths to data files
    data_files: Vec<String>,
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
        data_path: String,
    ) -> Result<Self> {
        // Get table structure (columns)
        let columns = provider.get_table_structure(table_id)?;

        // Build Arrow schema from column definitions
        let schema = Arc::new(build_arrow_schema(&columns)?);

        // Get data files and resolve paths
        let table_files = provider.get_table_files_for_select(table_id)?;
        let data_files = table_files
            .into_iter()
            .map(|tf| {
                if tf.file.path_is_relative {
                    // Join data_path with relative path
                    // data_path should end with '/' according to DuckLake spec
                    format!("{}{}", data_path, tf.file.path)
                } else {
                    // Use absolute path as-is
                    tf.file.path
                }
            })
            .collect();

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            snapshot_id,
            data_path,
            schema,
            data_files,
        })
    }
}

#[async_trait]
impl TableProvider for DuckLakeTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Determine the object store type from data_path
        // Parse data_path to determine scheme (file://, s3://, etc.)
        let (scheme, object_store_url) = if self.data_path.starts_with("file://")
            || self.data_path.starts_with("/")
            || !self.data_path.contains("://") {
            // Local filesystem
            let url = Url::parse("file:///").map_err(|e| {
                datafusion::error::DataFusionError::External(Box::new(
                    crate::DuckLakeError::Internal(format!("Failed to parse file URL: {}", e))
                ))
            })?;
            let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
            state.runtime_env().register_object_store(&url, object_store);

            let object_store_url = ObjectStoreUrl::parse("file:///").map_err(|e| {
                datafusion::error::DataFusionError::External(Box::new(
                    crate::DuckLakeError::Internal(format!("Failed to parse ObjectStoreUrl: {}", e))
                ))
            })?;
            ("file", object_store_url)
        } else {
            // For other schemes (s3://, gs://, etc.), we'll add support in the future
            return Err(datafusion::error::DataFusionError::External(Box::new(
                crate::DuckLakeError::Unsupported(format!(
                    "Object store scheme not yet supported: {}",
                    self.data_path
                ))
            )));
        };

        // Create ParquetSource with filter pushdown enabled
        let source = Arc::new(
            ParquetSource::default()
                .with_pushdown_filters(true)
                .with_enable_page_index(true),
        );

        // Build FileScanConfig using builder pattern
        let mut builder = FileScanConfigBuilder::new(
            object_store_url,
            Arc::clone(&self.schema),
            source,
        );

        // Add projection if specified
        if let Some(proj) = projection {
            builder = builder.with_projection(Some(proj.clone()));
        }

        // Add limit if specified
        if let Some(limit_value) = limit {
            builder = builder.with_limit(Some(limit_value));
        }

        // Add data files to scan
        // For local files, convert absolute paths to relative paths for object store
        for path in &self.data_files {
            let file_path = if scheme == "file" {
                // Strip leading '/' if present for object store
                // Object store expects paths relative to the root
                path.strip_prefix('/').unwrap_or(path).to_string()
            } else {
                path.clone()
            };

            let partitioned_file = PartitionedFile::new(file_path, 0);
            builder = builder.with_file(partitioned_file);
        }

        // Build the FileScanConfig
        let file_scan_config = builder.build();

        // Create DataSourceExec with the configuration
        let exec = DataSourceExec::from_data_source(file_scan_config);

        Ok(exec as Arc<dyn ExecutionPlan>)
    }
}