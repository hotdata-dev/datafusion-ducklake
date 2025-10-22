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
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;

/// Custom execution plan that filters out deleted rows
#[derive(Debug)]
pub struct DeleteFilterExec {
    /// The input execution plan (typically ParquetExec)
    input: Arc<dyn ExecutionPlan>,
    /// Path of the file being scanned
    file_path: String,
    /// Set of deleted row positions for this file (shared across streams)
    deleted_positions: Arc<HashSet<i64>>,
    /// Cached plan properties
    properties: PlanProperties,
}

impl DeleteFilterExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        file_path: String,
        deleted_positions: Arc<HashSet<i64>>,
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
            },
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "DeleteFilterExec: file={}, deletes={}",
                    self.file_path,
                    self.deleted_positions.len()
                )
            },
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

        let deleted_positions = self.deleted_positions.clone();

        Ok(Arc::new(DeleteFilterExec::new(
            children[0].clone(),
            self.file_path.clone(),
            deleted_positions,
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
    deleted_positions: Arc<HashSet<i64>>,
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
                    },
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            },
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

        // Special case: if there are no columns (COUNT(*) case), create an empty batch with the filtered row count
        if batch.num_columns() == 0 {
            let mut options = RecordBatchOptions::new();
            options = options.with_row_count(Some(keep_indices.len()));
            return RecordBatch::try_new_with_options(batch.schema(), vec![], &options)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
        }

        // Use Arrow's take kernel to select rows
        use arrow::array::UInt32Array;
        use arrow::compute::take;

        let indices = UInt32Array::from(keep_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::EmptyRecordBatchStream;

    #[test]
    fn test_filter_batch_ignores_out_of_bounds_positions() {
        // Create a simple RecordBatch with 4 rows
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array) as Arc<dyn Array>])
                .unwrap();

        // Create delete positions: 1 (valid), 1000, 2000, 5000 (all out of bounds)
        // Only position 1 should actually delete a row (the row with id=2)
        let deleted_positions: HashSet<i64> = [1, 1000, 2000, 5000].into_iter().collect();

        // Create a DeleteFilterStream with row_offset=0
        let stream = DeleteFilterStream {
            input: Box::pin(EmptyRecordBatchStream::new(schema.clone())),
            deleted_positions: Arc::new(deleted_positions),
            row_offset: 0,
        };

        // Apply the filter
        let filtered_batch = stream.filter_batch(&batch).unwrap();

        // Should have 3 rows (only position 1 was deleted, positions 1000+ are out of bounds)
        assert_eq!(
            filtered_batch.num_rows(),
            3,
            "Expected 3 rows after filtering (only position 1 is valid)"
        );

        // Verify the correct rows remain (ids 1, 3, 4)
        let filtered_ids = filtered_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let ids: Vec<i32> = filtered_ids.values().to_vec();
        assert_eq!(
            ids,
            vec![1, 3, 4],
            "Expected ids [1, 3, 4] after deleting position 1 (id=2)"
        );
    }

    #[test]
    fn test_filter_batch_all_out_of_bounds_positions() {
        // Test the edge case where ALL delete positions are beyond the file
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let id_array = Int32Array::from(vec![10, 20, 30]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(id_array) as Arc<dyn Array>])
                .unwrap();

        // All positions are way beyond the 3-row file
        let deleted_positions: HashSet<i64> = [1000, 2000, 3000, 9999].into_iter().collect();

        let stream = DeleteFilterStream {
            input: Box::pin(EmptyRecordBatchStream::new(schema.clone())),
            deleted_positions: Arc::new(deleted_positions),
            row_offset: 0,
        };

        let filtered_batch = stream.filter_batch(&batch).unwrap();

        // Should have all 3 rows (no valid delete positions)
        assert_eq!(
            filtered_batch.num_rows(),
            3,
            "All rows should remain when delete positions are out of bounds"
        );

        let filtered_ids = filtered_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let ids: Vec<i32> = filtered_ids.values().to_vec();
        assert_eq!(ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_filter_batch_with_row_offset() {
        // Test that row_offset is correctly considered when checking positions
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));

        let array = Int32Array::from(vec![100, 200, 300, 400]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(array) as Arc<dyn Array>]).unwrap();

        // Delete position 11 and 1000 (way out of bounds)
        // With row_offset=10, this batch contains global positions [10, 11, 12, 13]
        // So position 11 should delete the second row (value=200)
        let deleted_positions: HashSet<i64> = [11, 1000].into_iter().collect();

        let stream = DeleteFilterStream {
            input: Box::pin(EmptyRecordBatchStream::new(schema.clone())),
            deleted_positions: Arc::new(deleted_positions),
            row_offset: 10, // This batch starts at global position 10
        };

        let filtered_batch = stream.filter_batch(&batch).unwrap();

        // Should have 3 rows (position 11 deleted, position 1000 ignored)
        assert_eq!(filtered_batch.num_rows(), 3);

        let filtered_values = filtered_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        let values: Vec<i32> = filtered_values.values().to_vec();
        assert_eq!(
            values,
            vec![100, 300, 400],
            "Position 11 (value=200) should be deleted, 1000 ignored"
        );
    }
}
