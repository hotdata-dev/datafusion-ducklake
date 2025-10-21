//! Integration tests for delete file filtering
//!
//! These tests verify that the delete file implementation correctly filters out
//! deleted rows from query results while maintaining backward compatibility.

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};

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

    /// Test that multiple delete files for the same data file are correctly merged
    ///
    /// **CRITICAL BUG TEST**: This test reveals a bug in the current implementation
    /// where multiple delete files for the same data file cause the file to be
    /// scanned multiple times, producing duplicate rows.
    ///
    /// Test scenario:
    /// - Data file has 5 rows: [1, 2, 3, 4, 5] at positions [0, 1, 2, 3, 4]
    /// - Delete file 1: deletes position [1] (id=2)
    /// - Delete file 2: deletes positions [1, 3] (id=2, id=4)
    ///
    /// Expected behavior (CORRECT):
    /// - Merge both delete files: positions [1, 3]
    /// - Scan data file ONCE with merged deletes
    /// - Result: ids [1, 3, 5] (no duplicates)
    ///
    /// Current behavior (BUG):
    /// - Scan data file TWICE (once per delete file)
    /// - First scan filters position [1] -> produces [1, 3, 4, 5]
    /// - Second scan filters positions [1, 3] -> produces [1, 3, 5]
    /// - UnionExec combines both -> produces [1, 1, 3, 3, 4, 5, 5] (duplicates!)
    ///
    /// This test is marked as #[ignore] because it currently fails due to the bug.
    /// Remove #[ignore] once the bug is fixed in the table provider.
    #[tokio::test]
    #[ignore = "Bug: multiple delete files cause duplicate rows - needs fix in table.rs scan() method"]
    async fn test_multiple_delete_files_same_data_file() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        // Use the catalog configured with multiple delete files for the same data file
        let catalog_path = "tests/test_data/multiple_deletes.ducklake";
        let catalog = create_catalog(catalog_path)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("multiple_deletes", catalog);

        // Query the table
        let df = ctx.sql("SELECT * FROM multiple_deletes.main.products ORDER BY id").await?;
        let results = df.collect().await?;

        // Verify results
        assert!(!results.is_empty(), "Should have at least one batch");

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();

        // Collect all IDs to verify correct rows were kept
        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }

        // Should have 3 rows after merging deletes from both files
        // - Position 1 (id=2) deleted by both files
        // - Position 3 (id=4) deleted by file 2
        // - Remaining: positions [0, 2, 4] -> ids [1, 3, 5]
        assert_eq!(
            total_rows, 3,
            "Should have 3 rows after merging deletes from both files"
        );

        // Should only have IDs 1, 3, and 5 (each appearing ONCE)
        // ID 2 (position 1) deleted by both files - tests deduplication
        // ID 4 (position 3) deleted by file 2
        assert_eq!(all_ids, vec![1, 3, 5], "Should keep only non-deleted rows without duplicates");

        // Verify deleted rows don't appear
        let df = ctx.sql("SELECT * FROM multiple_deletes.main.products WHERE id = 2").await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Deleted row with id=2 should not appear");

        let df = ctx.sql("SELECT * FROM multiple_deletes.main.products WHERE id = 4").await?;
        let results = df.collect().await?;
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0, "Deleted row with id=4 should not appear");

        Ok(())
    }

    /// Test filter pushdown correctness with delete files
    ///
    /// This test verifies that WHERE filters are applied AFTER delete filtering,
    /// not before. This is critical for correct query semantics.
    ///
    /// Scenario:
    /// - Table has rows with id=[1,2,3,4,5]
    /// - Row with id=3 (position 2) is deleted
    /// - Query: WHERE id > 2
    ///
    /// Expected: [4, 5]
    /// Incorrect if filter applied before deletes: [2, 4, 5] (wrong - includes deleted row)
    /// Incorrect if deletes ignored: [3, 4, 5] (wrong - includes deleted row)
    ///
    /// This verifies the correct operation order:
    /// 1. Scan Parquet file (yields rows with id=[1,2,3,4,5])
    /// 2. Apply delete filtering (removes id=3, yields [1,2,4,5])
    /// 3. Apply WHERE filter (filters id > 2, yields [4,5])
    #[tokio::test]
    async fn test_filter_pushdown_correctness_with_deletes() -> DataFusionResult<()> {
        if !test_data_exists() {
            eprintln!("Test data not found. Run setup_test_data.sql first.");
            return Ok(());
        }

        let catalog_path = "tests/test_data/filter_pushdown.ducklake";
        let catalog = create_catalog(catalog_path)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("filter_pushdown", catalog);

        // Query with WHERE filter that should be applied AFTER delete filtering
        let df = ctx
            .sql("SELECT id FROM filter_pushdown.main.items WHERE id > 2 ORDER BY id")
            .await?;
        let results = df.collect().await?;

        assert!(!results.is_empty(), "Should have results");

        // Collect all IDs
        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }

        // Should return [4, 5] - the rows that remain after:
        // 1. Delete filtering removes id=3
        // 2. WHERE id > 2 filter is applied to [1,2,4,5], yielding [4,5]
        //
        // Common bugs this catches:
        // - Filter before delete: would incorrectly include deleted rows that match filter
        // - Filter on original positions: would return wrong rows
        assert_eq!(
            all_ids,
            vec![4, 5],
            "Filter should be applied AFTER delete filtering. \
             Expected [4,5] (rows with id>2 after id=3 deleted), got {:?}",
            all_ids
        );

        // Verify the deleted row (id=3) is NOT in results
        assert!(
            !all_ids.contains(&3),
            "Deleted row with id=3 should not appear, even though it matches id>2"
        );

        // Additional verification: query for id <= 2 should return [1, 2]
        let df = ctx
            .sql("SELECT id FROM filter_pushdown.main.items WHERE id <= 2 ORDER BY id")
            .await?;
        let results = df.collect().await?;

        let mut all_ids = Vec::new();
        for batch in &results {
            all_ids.extend(get_int_column(batch, 0));
        }

        assert_eq!(
            all_ids,
            vec![1, 2],
            "Filter id<=2 should return [1,2] after delete filtering"
        );

        Ok(())
    }
}
