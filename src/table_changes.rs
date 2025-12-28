//! Table changes (CDC) functionality for DuckLake

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::Session;
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::memory::MemTable;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::physical_plan::ExecutionPlan;

use crate::metadata_provider::MetadataProvider;

#[derive(Debug)]
pub struct TableChangesTable {
    provider: Arc<dyn MetadataProvider>,
    table_id: i64,
    start_snapshot: i64,
    end_snapshot: i64,
    schema: SchemaRef,
}

impl TableChangesTable {
    pub fn new(
        provider: Arc<dyn MetadataProvider>,
        table_id: i64,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("change_type", DataType::Utf8, false),
        ]));
        Self {
            provider,
            table_id,
            start_snapshot,
            end_snapshot,
            schema,
        }
    }

    fn query_changes(&self) -> DataFusionResult<RecordBatch> {
        // Get data files added (INSERT changes)
        let data_files = self
            .provider
            .get_data_files_added_between_snapshots(
                self.table_id,
                self.start_snapshot,
                self.end_snapshot,
            )
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Get delete files added (DELETE changes)
        let delete_files = self
            .provider
            .get_delete_files_added_between_snapshots(
                self.table_id,
                self.start_snapshot,
                self.end_snapshot,
            )
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Collect all changes into a sortable structure
        struct ChangeRecord {
            snapshot_id: i64,
            change_type: &'static str,
        }

        let mut changes: Vec<ChangeRecord> =
            Vec::with_capacity(data_files.len() + delete_files.len());

        // Add INSERT changes (data files added)
        for data_file in &data_files {
            changes.push(ChangeRecord {
                snapshot_id: data_file.begin_snapshot,
                change_type: "insert",
            });
        }

        // Add DELETE changes (delete files added)
        for delete_file in &delete_files {
            changes.push(ChangeRecord {
                snapshot_id: delete_file.begin_snapshot,
                change_type: "delete",
            });
        }

        // Sort by snapshot_id for deterministic output
        changes.sort_by_key(|c| c.snapshot_id);

        // Build arrays from sorted changes
        let snapshot_ids: ArrayRef = Arc::new(Int64Array::from(
            changes.iter().map(|c| c.snapshot_id).collect::<Vec<_>>(),
        ));
        let change_types: ArrayRef = Arc::new(StringArray::from(
            changes.iter().map(|c| c.change_type).collect::<Vec<_>>(),
        ));

        RecordBatch::try_new(self.schema.clone(), vec![snapshot_ids, change_types])
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

#[async_trait::async_trait]
impl TableProvider for TableChangesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let batch = self.query_changes()?;
        let mem_table = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}
