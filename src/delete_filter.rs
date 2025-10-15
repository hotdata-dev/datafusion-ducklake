//! Custom execution plan for filtering deleted rows
//!
//! This module implements a DataFusion execution plan that wraps the Parquet scan
//! and filters out rows marked as deleted in delete files.

use std::any::Any;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::Stream;

/// Custom execution plan that filters out deleted rows
#[derive(Debug)]
pub struct DeleteFilterExec {
    /// The input execution plan (typically ParquetExec)
    input: Arc<dyn ExecutionPlan>,
    /// Path of the file being scanned
    file_path: String,
    /// Set of deleted row positions for this file
    deleted_positions: HashSet<i64>,
    /// Cached plan properties
    properties: PlanProperties,
}

impl DeleteFilterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        file_path: String,
        deleted_positions: HashSet<i64>,
    ) -> Self {
        // Clone properties from input plan
        let properties = input.properties().clone();

        Self {
            input,
            file_path,
            deleted_positions,
            properties,
        }
    }
}

impl DisplayAs for DeleteFilterExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "DeleteFilterExec: file={}, deletes={}",
                    self.file_path,
                    self.deleted_positions.len()
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DeleteFilterExec: file={}, deletes={}",
                    self.file_path,
                    self.deleted_positions.len()
                )
            }
        }
    }
}

impl ExecutionPlan for DeleteFilterExec {
    fn name(&self) -> &str {
        "DeleteFilterExec"
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
                "DeleteFilterExec expects exactly one child".into(),
            ));
        }

        Ok(Arc::new(DeleteFilterExec::new(
            children[0].clone(),
            self.file_path.clone(),
            self.deleted_positions.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        Ok(Box::pin(DeleteFilterStream {
            input: input_stream,
            deleted_positions: self.deleted_positions.clone(),
            row_offset: 0,
        }))
    }
}

/// Stream that filters deleted rows from input batches
struct DeleteFilterStream {
    input: SendableRecordBatchStream,
    deleted_positions: HashSet<i64>,
    row_offset: i64,
}

impl Stream for DeleteFilterStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.input).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Filter the batch
                match self.filter_batch(&batch) {
                    Ok(filtered_batch) => {
                        // Update row offset for next batch
                        self.row_offset += batch.num_rows() as i64;
                        Poll::Ready(Some(Ok(filtered_batch)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl DeleteFilterStream {
    fn filter_batch(&self, batch: &RecordBatch) -> DataFusionResult<RecordBatch> {
        // If no deletes for this file, return batch as-is
        if self.deleted_positions.is_empty() {
            return Ok(batch.clone());
        }

        // Build list of row indices to keep
        let num_rows = batch.num_rows();
        let mut keep_indices: Vec<usize> = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let global_pos = self.row_offset + i as i64;
            if !self.deleted_positions.contains(&global_pos) {
                keep_indices.push(i);
            }
        }

        // If all rows are kept, return original batch
        if keep_indices.len() == num_rows {
            return Ok(batch.clone());
        }

        // Use Arrow's take kernel to select rows
        use arrow::array::UInt32Array;
        use arrow::compute::take;

        let indices =
            UInt32Array::from(keep_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

        let filtered_columns: DataFusionResult<Vec<_>> = batch
            .columns()
            .iter()
            .map(|col| {
                take(col.as_ref(), &indices, None)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
            })
            .collect();

        RecordBatch::try_new(batch.schema(), filtered_columns?)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl RecordBatchStream for DeleteFilterStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
