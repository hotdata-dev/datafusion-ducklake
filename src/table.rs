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
    schema: SchemaRef,
    data_files: Vec<String>,
}

impl DuckLakeTable {
    /// Create a new DuckLake table
    pub fn new(
        table_id: i64,
        table_name: impl Into<String>,
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
    ) -> Result<Self> {
        // Get table structure (columns)
        let columns = provider.get_table_structure(table_id)?;

        // Build Arrow schema from column definitions
        let schema = Arc::new(build_arrow_schema(&columns)?);

        // Get data files
        let table_files = provider.get_table_files_for_select(table_id)?;
        let data_files = table_files
            .into_iter()
            .map(|tf| tf.file.path)
            .collect();

        Ok(Self {
            table_id,
            table_name: table_name.into(),
            provider,
            snapshot_id,
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
        // Register local file system object store
        let url = Url::parse("file://").unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        state
            .runtime_env()
            .register_object_store(&url, object_store.clone());

        // Create ParquetSource with filter pushdown enabled
        let source = Arc::new(
            ParquetSource::default()
                .with_pushdown_filters(true)
                .with_enable_page_index(true),
        );

        // Create object store URL for file scan
        let object_store_url = ObjectStoreUrl::parse("file://").unwrap();

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
        for path in &self.data_files {
            let partitioned_file = PartitionedFile::new(path.clone(), 0);
            builder = builder.with_file(partitioned_file);
        }

        // Build the FileScanConfig
        let file_scan_config = builder.build();

        // Create DataSourceExec with the configuration
        // from_data_source returns Arc<DataSourceExec>, which implements ExecutionPlan
        let exec = DataSourceExec::from_data_source(file_scan_config);

        Ok(exec as Arc<dyn ExecutionPlan>)
    }
}