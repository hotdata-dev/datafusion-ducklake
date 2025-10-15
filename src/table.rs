//! DuckLake table provider implementation

use std::any::Any;
use std::sync::Arc;

use crate::Result;
use crate::metadata_provider::MetadataProvider;
use crate::types::build_arrow_schema;
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
    /// the base path to the data, e.g. s3://ducklake-data
    base_data_url: Arc<ObjectStoreUrl>,
    /// relative data path from catalog/schema to this table for resolving relative file paths
    #[allow(dead_code)]
    data_path: String,
    schema: SchemaRef,
    /// Table files with resolved absolute paths and metadata
    table_files: Vec<crate::metadata_provider::DuckLakeTableFile>,
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
        base_data_url: Arc<ObjectStoreUrl>,
        data_path: String,
    ) -> Result<Self> {
        // Get table structure (columns)
        let columns = provider.get_table_structure(table_id)?;

        // Build Arrow schema from column definitions
        let schema = Arc::new(build_arrow_schema(&columns)?);

        // Get data files and resolve paths
        let table_files = provider.get_table_files_for_select(table_id)?;
        let table_files: Vec<_> = table_files
            .into_iter()
            .map(|mut tf| {
                // Resolve data file relative paths to absolute paths
                if tf.file.path_is_relative {
                    // Join data_path with relative path
                    // data_path should end with '/' according to DuckLake spec
                    tf.file.path = format!("{}{}", data_path, tf.file.path);
                    tf.file.path_is_relative = false;
                }

                // Resolve delete file relative paths to absolute paths
                if let Some(ref mut delete_file) = tf.delete_file {
                    if delete_file.path_is_relative {
                        delete_file.path = format!("{}{}", data_path, delete_file.path);
                        delete_file.path_is_relative = false;
                    }
                }

                tf
            })
            .collect();

        println!("data files: {:?}", table_files.iter().map(|tf| &tf.file.path).collect::<Vec<_>>());

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            snapshot_id,
            base_data_url,
            data_path,
            schema,
            table_files,
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
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let format = ParquetFormat::new();
        
        let file_scan_config = FileScanConfigBuilder::new(
            self.base_data_url.as_ref().clone(),
            self.schema.clone(),
            Arc::new(ParquetSource::default()),
        )
        .with_limit(limit)
        .with_file_group(FileGroup::new(
            self.table_files
                .iter()
                .map(|tf| PartitionedFile::new(&tf.file.path, tf.file.file_size_bytes as u64))
                .collect(),
        ))
        .build();

        let exec = format.create_physical_plan(state, file_scan_config).await?;

        Ok(exec)
    }
}
