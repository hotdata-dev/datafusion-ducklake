//! DuckLake table provider implementation

use std::any::Any;
use std::sync::Arc;

use crate::metadata_provider::MetadataProvider;
use crate::types::build_arrow_schema;
use crate::Result;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfigBuilder, ParquetSource};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;

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
    #[allow(dead_code)]
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

        println!("data files: {:?}", data_files);

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

    /// Resolve file paths to ObjectStoreUrl and relative paths
    ///
    /// Takes the full file paths and:
    /// 1. Normalizes S3 paths (s3:/ -> s3://)
    /// 2. Extracts the bucket to construct ObjectStoreUrl
    /// 3. Strips the bucket prefix to get relative paths for DataFusion
    fn resolve_file_paths(&self) -> DataFusionResult<(ObjectStoreUrl, Vec<String>)> {
        if self.data_files.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "No data files found for table".to_string(),
            ));
        }

        // Take the first file to determine the storage scheme and bucket
        let first_file = &self.data_files[0];

        // Normalize the path: convert "s3:/" to "s3://"
        let normalized = self.normalize_path(first_file);
        

        // Determine scheme and extract object store URL
        let object_store_url = if normalized.starts_with("s3://") {
            // Extract bucket from s3://bucket/path
            let url = url::Url::parse(&normalized).map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to parse S3 URL '{}': {}",
                    normalized, e
                ))
            })?;

            let bucket = url.host_str().ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(format!(
                    "S3 URL missing bucket: {}",
                    normalized
                ))
            })?;

            ObjectStoreUrl::parse(format!("s3://{}/", bucket)).map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to create ObjectStoreUrl: {}",
                    e
                ))
            })?
        } else if normalized.starts_with("file://") || normalized.starts_with('/') {
            ObjectStoreUrl::parse("file:///").map_err(|e| {
                datafusion::error::DataFusionError::Internal(format!(
                    "Failed to create file ObjectStoreUrl: {}",
                    e
                ))
            })?
        } else {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "Unsupported storage scheme in path: {}",
                first_file
            )));
        };

        // Extract relative paths for all files
        let relative_paths: Result<Vec<String>> = self
            .data_files
            .iter()
            .map(|path| {
                let normalized = self.normalize_path(path);
                self.extract_relative_path(&normalized)
            })
            .collect();

        let relative_paths =
            relative_paths.map_err(|e| datafusion::error::DataFusionError::Internal(e.to_string()))?;

        Ok((object_store_url, relative_paths))
    }

    /// Normalize path by converting "s3:/" to "s3://"
    fn normalize_path(&self, path: &str) -> String {
        if path.starts_with("s3:/") && !path.starts_with("s3://") {
            path.replacen("s3:/", "s3://", 1)
        } else {
            path.to_string()
        }
    }

    /// Extract relative path from a full path
    ///
    /// Examples:
    /// - "s3://bucket/path/file.parquet" -> "path/file.parquet"
    /// - "file:///abs/path/file.parquet" -> "abs/path/file.parquet"
    fn extract_relative_path(&self, full_path: &str) -> Result<String> {
        if full_path.starts_with("s3://") {
            let url = url::Url::parse(full_path).map_err(|e| {
                crate::DuckLakeError::Internal(format!("Failed to parse S3 URL '{}': {}", full_path, e))
            })?;

            // Get path without leading '/'
            let path = url.path().strip_prefix('/').unwrap_or(url.path());
            Ok(path.to_string())
        } else if full_path.starts_with("file://") {
            // Strip "file://" prefix
            Ok(full_path.strip_prefix("file://").unwrap().to_string())
        } else if full_path.starts_with('/') {
            // Already a filesystem path, strip leading '/'
            Ok(full_path.strip_prefix('/').unwrap().to_string())
        } else {
            // Assume it's already relative
            Ok(full_path.to_string())
        }
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {

        let format = ParquetFormat::new();

        // Determine the object store URL and extract relative file paths
        // For S3 paths like "s3://bucket/path/file.parquet", we need to:
        // 1. Register the object store with "s3://bucket/"
        // 2. Provide "path/file.parquet" as the relative path

        let (object_store_url, relative_paths) = self.resolve_file_paths()?;

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url,
            self.schema.clone(),
            Arc::new(ParquetSource::default())
        )
            .with_limit(limit)
            .with_file_group(FileGroup::new(
                // todo:  fix we're hardcoding file size. shouldn't do that
                relative_paths.iter().map(|f| PartitionedFile::new(f, 6329509)).collect()
            ))

            .build();

        let exec = format.create_physical_plan(state, file_scan_config).await?;

        Ok(exec)
    }
}