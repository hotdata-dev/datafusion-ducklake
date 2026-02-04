//! DuckLake INSERT execution plan implementation
//!
//! This module provides `DuckLakeInsertExec`, a custom execution plan that writes
//! data from an input plan to Parquet files and registers them in the DuckLake catalog.
//!
//! ## Current Limitations
//!
//! - **Memory**: Currently collects all input batches into memory before writing.
//!   For very large inserts, this could cause OOM. Future improvement: stream batches
//!   directly to Parquet writer.
//!
//! - **Single partition**: Only partition 0 is supported. Parallel writes from
//!   multiple partitions would require coordination to avoid file conflicts.
//!
//! - **Cleanup**: If write succeeds but metadata registration fails, orphaned
//!   Parquet files may remain. The WriteSession uses temp files that are renamed
//!   on success, which mitigates but doesn't eliminate this risk.

use std::any::Any;
use std::fmt::{self, Debug};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::stream::{self, TryStreamExt};

use crate::metadata_writer::{MetadataWriter, WriteMode};
use crate::table_writer::DuckLakeTableWriter;

/// Schema for the output of insert operations (count of rows inserted)
fn make_insert_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Execution plan that writes input data to DuckLake table via Parquet files.
///
/// This execution plan:
/// 1. Streams data from the input plan
/// 2. Writes batches to Parquet files using `DuckLakeTableWriter`
/// 3. Registers the files in the DuckLake catalog metadata
/// 4. Returns a single batch with the count of rows inserted
pub struct DuckLakeInsertExec {
    /// The input execution plan providing data to insert
    input: Arc<dyn ExecutionPlan>,
    /// Metadata writer for catalog operations
    writer: Arc<dyn MetadataWriter>,
    /// Schema name in the DuckLake catalog
    schema_name: String,
    /// Table name in the DuckLake catalog
    table_name: String,
    /// Arrow schema of the data being inserted
    arrow_schema: SchemaRef,
    /// Write mode (Append or Replace)
    write_mode: WriteMode,
    /// Base data path for the catalog
    data_path: PathBuf,
    /// Cached plan properties
    cache: PlanProperties,
}

impl DuckLakeInsertExec {
    /// Create a new DuckLakeInsertExec
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        writer: Arc<dyn MetadataWriter>,
        schema_name: String,
        table_name: String,
        arrow_schema: SchemaRef,
        write_mode: WriteMode,
        data_path: PathBuf,
    ) -> Self {
        let cache = Self::compute_properties();
        Self {
            input,
            writer,
            schema_name,
            table_name,
            arrow_schema,
            write_mode,
            data_path,
            cache,
        }
    }

    fn compute_properties() -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(make_insert_count_schema()),
            Partitioning::UnknownPartitioning(1),
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        )
    }
}

impl Debug for DuckLakeInsertExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DuckLakeInsertExec")
            .field("schema_name", &self.schema_name)
            .field("table_name", &self.table_name)
            .field("write_mode", &self.write_mode)
            .field("data_path", &self.data_path)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for DuckLakeInsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DuckLakeInsertExec: schema={}, table={}, mode={:?}",
                    self.schema_name, self.table_name, self.write_mode
                )
            },
        }
    }
}

impl ExecutionPlan for DuckLakeInsertExec {
    fn name(&self) -> &str {
        "DuckLakeInsertExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "DuckLakeInsertExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.writer),
            self.schema_name.clone(),
            self.table_name.clone(),
            Arc::clone(&self.arrow_schema),
            self.write_mode,
            self.data_path.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Currently only single-partition writes are supported.
        // Multi-partition parallel writes would require coordination to:
        // 1. Write to separate files per partition
        // 2. Register all files atomically in a single transaction
        // TODO: Add parallel write support for better performance on partitioned inputs
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "DuckLakeInsertExec only supports partition 0, got {}",
                partition
            )));
        }

        // Clone necessary data for the async block
        let input = Arc::clone(&self.input);
        let writer = Arc::clone(&self.writer);
        let schema_name = self.schema_name.clone();
        let table_name = self.table_name.clone();
        let arrow_schema = Arc::clone(&self.arrow_schema);
        let write_mode = self.write_mode;
        let data_path = self.data_path.clone();
        let output_schema = make_insert_count_schema();

        // Create an async stream that performs the write
        let stream = stream::once(async move {
            // Execute input and collect batches into memory.
            // TODO: Stream batches directly to Parquet writer to reduce memory usage.
            // This requires making WriteSession async-compatible or using a separate
            // writing task with a channel.
            let input_stream = input.execute(0, context)?;
            let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

            if batches.is_empty() {
                // No data to insert, return count of 0
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok(RecordBatch::try_new(output_schema, vec![count_array])?);
            }

            // Create table writer and begin write session
            let table_writer = DuckLakeTableWriter::new(writer)
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Strip metadata from schema for comparison (Arrow metadata shouldn't affect writes)
            let schema_without_metadata =
                Schema::new(arrow_schema.fields().iter().cloned().collect::<Vec<_>>());

            let mut session = table_writer
                .begin_write(
                    &schema_name,
                    &table_name,
                    &schema_without_metadata,
                    write_mode,
                )
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Ensure write goes to the correct data path
            // Note: begin_write already uses the writer's data_path internally
            let _ = data_path; // Silence unused warning - data_path is used by writer internally

            // Write all batches
            for batch in &batches {
                session
                    .write_batch(batch)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }

            // Get row count before finishing
            let row_count = session.row_count() as u64;

            // Finish the write session (this registers the file in metadata)
            session
                .finish()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Return a batch with the count of rows inserted
            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![row_count]));
            Ok(RecordBatch::try_new(output_schema, vec![count_array])?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            make_insert_count_schema(),
            stream.map_err(|e: DataFusionError| e),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_count_schema() {
        let schema = make_insert_count_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "count");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
    }
}
