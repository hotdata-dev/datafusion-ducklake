//! Custom execution plan for renaming columns
//!
//! This module implements a DataFusion execution plan that wraps a scan
//! and renames columns from their original Parquet names to current DuckLake names.
//! This is needed when columns have been renamed in DuckLake metadata but the
//! Parquet files still have the original column names.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::Boundedness;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::Stream;

/// Custom execution plan that renames columns from Parquet file names to current DuckLake names
#[derive(Debug)]
pub struct ColumnRenameExec {
    /// The input execution plan (typically ParquetExec)
    input: Arc<dyn ExecutionPlan>,
    /// Output schema with renamed columns
    output_schema: SchemaRef,
    /// Mapping from old (Parquet) column names to new (DuckLake) column names
    name_mapping: HashMap<String, String>,
    /// Reverse mapping: new name -> old name, for looking up input columns
    reverse_mapping: Arc<HashMap<String, String>>,
    /// Cached plan properties with updated schema
    properties: PlanProperties,
}

impl ColumnRenameExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        output_schema: SchemaRef,
        name_mapping: HashMap<String, String>,
    ) -> Self {
        // PlanProperties must use output schema for DataFusion schema validation
        let eq_props = EquivalenceProperties::new(Arc::clone(&output_schema));
        let properties = PlanProperties::new(
            eq_props,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            Boundedness::Bounded,
        );

        // Pre-compute reverse mapping once (new_name -> old_name)
        let reverse_mapping: HashMap<String, String> = name_mapping
            .iter()
            .map(|(old, new)| (new.clone(), old.clone()))
            .collect();

        Self {
            input,
            output_schema,
            name_mapping,
            reverse_mapping: Arc::new(reverse_mapping),
            properties,
        }
    }
}

impl DisplayAs for ColumnRenameExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ColumnRenameExec: renames={}", self.name_mapping.len())
    }
}

impl ExecutionPlan for ColumnRenameExec {
    fn name(&self) -> &str {
        "ColumnRenameExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ColumnRenameExec expects exactly one child".into(),
            ));
        }

        // Must call new() to rebuild properties from new child's partitioning
        Ok(Arc::new(ColumnRenameExec::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.output_schema),
            self.name_mapping.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(ColumnRenameStream {
            input: input_stream,
            output_schema: Arc::clone(&self.output_schema),
            reverse_mapping: Arc::clone(&self.reverse_mapping),
        }))
    }
}

/// Stream that renames columns in output batches
struct ColumnRenameStream {
    input: SendableRecordBatchStream,
    output_schema: SchemaRef,
    /// Mapping from output column name -> input column name (for renamed columns only)
    reverse_mapping: Arc<HashMap<String, String>>,
}

impl Stream for ColumnRenameStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = if batch.num_columns() == 0 {
                    // COUNT(*) case: preserve row count with empty schema
                    use arrow::record_batch::RecordBatchOptions;
                    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                    RecordBatch::try_new_with_options(
                        Arc::clone(&self.output_schema),
                        vec![],
                        &options,
                    )
                } else {
                    // Build columns by looking up each output field in the input batch
                    let input_schema = batch.schema();
                    let columns: Result<Vec<_>, _> = self
                        .output_schema
                        .fields()
                        .iter()
                        .map(|output_field| {
                            // Check if this column was renamed (new_name -> old_name)
                            let input_name = self
                                .reverse_mapping
                                .get(output_field.name())
                                .map(|s| s.as_str())
                                .unwrap_or_else(|| output_field.name().as_str());

                            input_schema
                                .index_of(input_name)
                                .map(|idx| batch.column(idx).clone())
                        })
                        .collect();

                    match columns {
                        Ok(cols) => RecordBatch::try_new(Arc::clone(&self.output_schema), cols),
                        Err(e) => Err(e),
                    }
                };

                match result {
                    Ok(renamed_batch) => Poll::Ready(Some(Ok(renamed_batch))),
                    Err(e) => {
                        Poll::Ready(Some(Err(DataFusionError::ArrowError(Box::new(e), None))))
                    },
                }
            },
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ColumnRenameStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::EmptyRecordBatchStream;

    #[test]
    fn test_column_rename_stream_schema() {
        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "old_col",
            DataType::Int32,
            false,
        )]));

        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "new_col",
            DataType::Int32,
            false,
        )]));

        let mut reverse_mapping = HashMap::new();
        reverse_mapping.insert("new_col".to_string(), "old_col".to_string());

        let stream = ColumnRenameStream {
            input: Box::pin(EmptyRecordBatchStream::new(input_schema)),
            output_schema: Arc::clone(&output_schema),
            reverse_mapping: Arc::new(reverse_mapping),
        };

        // The stream should report the output schema
        assert_eq!(stream.schema().field(0).name(), "new_col");
    }
}
