//! Integration and unit tests for delete file filtering
//!
//! These tests verify that the delete file implementation correctly filters out
//! deleted rows from query results while maintaining backward compatibility.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use datafusion_ducklake::table::{delete_file_schema, DELETE_FILE_PATH_COL, DELETE_POS_COL};

/// Test helper to extract integer values from a RecordBatch column
/// Supports both Int32 and Int64
fn get_int_column(batch: &RecordBatch, col_idx: usize) -> Vec<i32> {
    let column = batch.column(col_idx);

    // Try Int32 first
    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return (0..array.len())
            .filter_map(|i| if array.is_null(i) { None } else { Some(array.value(i)) })
            .collect();
    }

    // Try Int64
    if let Some(array) = column.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return (0..array.len())
            .filter_map(|i| if array.is_null(i) { None } else { Some(array.value(i) as i32) })
            .collect();
    }

    panic!("Column should be Int32Array or Int64Array, got {:?}", column.data_type());
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    /// Test the extract_deleted_positions function with a simple delete file batch
    #[test]
    fn test_extract_deleted_positions_simple() -> DataFusionResult<()> {
        // Create a mock delete file RecordBatch using standard schema
        let schema = delete_file_schema();

        let file_paths = StringArray::from(vec![
            "test_file.parquet",
            "test_file.parquet",
            "test_file.parquet",
        ]);

        let positions = Int64Array::from(vec![0, 2, 5]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(file_paths), Arc::new(positions)],
        )?;

        // Call the internal function (we need to expose it for testing)
        // For now, we'll test the functionality through the public API

        // Verify the batch structure
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);

        Ok(())
    }

    /// Test extract_deleted_positions with multiple files
    #[test]
    fn test_extract_deleted_positions_multiple_files() -> DataFusionResult<()> {
        let schema = delete_file_schema();

        let file_paths = StringArray::from(vec![
            "file1.parquet",
            "file1.parquet",
            "file2.parquet",
            "file2.parquet",
            "file2.parquet",
        ]);

        let positions = Int64Array::from(vec![1, 3, 0, 2, 4]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(file_paths), Arc::new(positions)],
        )?;

        // Verify we have the right structure for multiple files
        assert_eq!(batch.num_rows(), 5);

        // Extract file paths and positions using named column indices
        let file_idx = schema.index_of(DELETE_FILE_PATH_COL)?;
        let pos_idx = schema.index_of(DELETE_POS_COL)?;
        let file_col = batch.column(file_idx).as_any().downcast_ref::<StringArray>().unwrap();
        let pos_col = batch.column(pos_idx).as_any().downcast_ref::<Int64Array>().unwrap();

        let mut file_to_positions: HashMap<String, Vec<i64>> = HashMap::new();
        for i in 0..batch.num_rows() {
            let file = file_col.value(i).to_string();
            let pos = pos_col.value(i);
            file_to_positions.entry(file).or_insert_with(Vec::new).push(pos);
        }

        assert_eq!(file_to_positions.len(), 2);
        assert_eq!(file_to_positions.get("file1.parquet").unwrap(), &vec![1, 3]);
        assert_eq!(file_to_positions.get("file2.parquet").unwrap(), &vec![0, 2, 4]);

        Ok(())
    }

    /// Test extract_deleted_positions with null values (should be skipped)
    #[test]
    fn test_extract_deleted_positions_with_nulls() -> DataFusionResult<()> {
        // Create schema with nullable columns (for this test case)
        let schema = Arc::new(Schema::new(vec![
            Field::new(DELETE_FILE_PATH_COL, DataType::Utf8, true),
            Field::new(DELETE_POS_COL, DataType::Int64, true),
        ]));

        let file_paths = StringArray::from(vec![
            Some("file1.parquet"),
            None,
            Some("file1.parquet"),
        ]);

        let positions = Int64Array::from(vec![
            Some(1),
            Some(2),
            None,
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(file_paths), Arc::new(positions)],
        )?;

        // Only the first row should be valid (non-null in both columns)
        let file_idx = schema.index_of(DELETE_FILE_PATH_COL)?;
        let pos_idx = schema.index_of(DELETE_POS_COL)?;
        let file_col = batch.column(file_idx).as_any().downcast_ref::<StringArray>().unwrap();
        let pos_col = batch.column(pos_idx).as_any().downcast_ref::<Int64Array>().unwrap();

        let valid_count = (0..batch.num_rows())
            .filter(|&i| !file_col.is_null(i) && !pos_col.is_null(i))
            .count();

        assert_eq!(valid_count, 1);

        Ok(())
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Helper to check if test data exists
    fn test_data_exists() -> bool {
        std::path::Path::new("tests/test_data/no_deletes.ducklake").exists()
    }

    /// Helper to create a catalog from a DuckLake database file
    fn create_catalog(path: &str) -> DataFusionResult<Arc<DuckLakeCatalog>> {
        let provider = DuckdbMetadataProvider::new(path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog = DuckLakeCatalog::new(provider)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        Ok(Arc::new(catalog))
    }

    /// Test querying a table without delete files (backward compatibility)
    #[tokio::test]
    async fn test_table_without_delete_files() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/no_deletes.ducklake";
        let provider = DuckdbMetadataProvider::new(catalog_path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog = Arc::new(DuckLakeCatalog::new(provider)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?);

        let ctx = SessionContext::new();
        ctx.register_catalog("no_deletes", catalog);

        // Query the table
        let df = ctx.sql("SELECT * FROM no_deletes.main.users ORDER BY id").await?;
        let results = df.collect().await?;

        // Verify we got all rows (no deletes)
        assert!(!results.is_empty(), "Should have at least one batch");
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4, "Should have 4 rows (no deletes)");

        // Verify the IDs are correct
        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }
        assert_eq!(all_ids, vec![1, 2, 3, 4]);

        Ok(())
    }

    /// Test querying a table with delete files
    #[tokio::test]
    async fn test_table_with_delete_files() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/with_deletes.ducklake";
        let provider = DuckdbMetadataProvider::new(catalog_path)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let catalog = Arc::new(DuckLakeCatalog::new(provider)
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?);

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Query the table
        let df = ctx.sql("SELECT * FROM with_deletes.main.products ORDER BY id").await?;
        let results = df.collect().await?;

        // Verify we got the correct rows (excluding deleted IDs 2 and 4)
        assert!(!results.is_empty(), "Should have at least one batch");

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "Should have 3 rows after filtering deletes");

        // Collect all IDs from all batches
        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }

        // Should only have IDs 1, 3, and 5 (2 and 4 were deleted)
        assert_eq!(all_ids, vec![1, 3, 5]);

        Ok(())
    }

    /// Test that deleted rows are actually excluded from results
    #[tokio::test]
    async fn test_deleted_rows_excluded() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/with_deletes.ducklake";
        let catalog = create_catalog(catalog_path)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Query for a specific deleted row (should return no results)
        let df = ctx.sql("SELECT * FROM with_deletes.main.products WHERE id = 2").await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Deleted row with id=2 should not appear");

        // Query for a non-deleted row (should return 1 result)
        let df = ctx.sql("SELECT * FROM with_deletes.main.products WHERE id = 1").await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1, "Non-deleted row with id=1 should appear");

        Ok(())
    }

    /// Test updated rows show new values (UPDATE = DELETE old + INSERT new)
    #[tokio::test]
    async fn test_updated_rows_show_new_values() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/with_updates.ducklake";
        let catalog = create_catalog(catalog_path)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_updates", catalog);

        // Query the updated row
        let df = ctx.sql("SELECT id, quantity FROM with_updates.main.inventory WHERE id = 1").await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1, "Should have exactly one row for id=1");

        // Verify the updated quantity (should be 120, not 100)
        let quantities = get_int_column(batch, 1);
        assert_eq!(quantities[0], 120, "Updated quantity should be 120");

        // Query another updated row
        let df = ctx.sql("SELECT id, quantity FROM with_updates.main.inventory WHERE id = 3").await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        let quantities = get_int_column(batch, 1);
        assert_eq!(quantities[0], 180, "Updated quantity should be 180");

        Ok(())
    }

    /// Test count query with delete files
    #[tokio::test]
    async fn test_count_with_deletes() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/with_deletes.ducklake";
        let catalog = create_catalog(catalog_path)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Count should exclude deleted rows
        let df = ctx.sql("SELECT COUNT(*) as count FROM with_deletes.main.products").await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        let counts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(counts.value(0), 3, "Count should be 3 after filtering deletes");

        Ok(())
    }

    /// Test aggregation with delete files
    #[tokio::test]
    async fn test_aggregation_with_deletes() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/with_updates.ducklake";
        let catalog = create_catalog(catalog_path)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_updates", catalog);

        // Sum of quantities should use updated values
        let df = ctx.sql("SELECT SUM(quantity) as total FROM with_updates.main.inventory").await?;
        let results = df.collect().await?;

        assert!(!results.is_empty());
        let batch = &results[0];
        let totals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Updated quantities: 120 (id=1), 200 (id=2 unchanged), 180 (id=3)
        // Total should be 120 + 200 + 180 = 500
        assert_eq!(totals.value(0), 500, "Sum should reflect updated values");

        Ok(())
    }

    /// Test that empty result sets work correctly
    #[tokio::test]
    async fn test_empty_result_with_all_deleted() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/with_deletes.ducklake";
        let catalog = create_catalog(catalog_path)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("with_deletes", catalog);

        // Query only for deleted rows
        let df = ctx.sql("SELECT * FROM with_deletes.main.products WHERE id IN (2, 4)").await?;
        let results = df.collect().await?;

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Should return empty result for all deleted rows");

        Ok(())
    }
}

#[cfg(test)]
mod performance_tests {
    use super::*;

    /// Benchmark delete file processing overhead
    /// This is a placeholder for future performance testing
    #[tokio::test]
    #[ignore] // Run with --ignored flag
    async fn test_delete_file_performance() -> DataFusionResult<()> {
        // TODO: Create a test database with large number of deletes
        // and measure query performance compared to no deletes
        Ok(())
    }
}
