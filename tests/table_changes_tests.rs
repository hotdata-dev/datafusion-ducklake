#![cfg(feature = "metadata-duckdb")]
//! Integration tests for ducklake_table_changes() function
//!
//! These tests verify that the table_changes function correctly returns
//! files added between snapshots for CDC (Change Data Capture) queries.

mod common;

use std::sync::Arc;

use arrow::array::{Array, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, register_ducklake_functions};
use tempfile::TempDir;

/// Helper to get i32 values from a column
fn get_int32_column(batch: &RecordBatch, col_idx: usize) -> Vec<i32> {
    let column = batch.column(col_idx);
    let array = column
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("Expected Int32Array");

    (0..array.len())
        .filter_map(|i| {
            if array.is_null(i) {
                None
            } else {
                Some(array.value(i))
            }
        })
        .collect()
}

/// Helper to get string values from a column
fn get_string_column(batch: &RecordBatch, col_idx: usize) -> Vec<String> {
    let column = batch.column(col_idx);
    let array = column
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Expected StringArray");

    (0..array.len())
        .filter_map(|i| {
            if array.is_null(i) {
                None
            } else {
                Some(array.value(i).to_string())
            }
        })
        .collect()
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// Helper to create a context with catalog and register functions
    async fn create_context_with_functions(path: &str) -> DataFusionResult<SessionContext> {
        let provider = DuckdbMetadataProvider::new(path)?;
        let provider_arc: Arc<dyn datafusion_ducklake::MetadataProvider> =
            Arc::new(DuckdbMetadataProvider::new(path)?);

        let catalog = DuckLakeCatalog::new(provider)?;

        let ctx = SessionContext::new();
        ctx.register_catalog("ducklake", Arc::new(catalog));

        // Register the table functions including ducklake_table_changes
        register_ducklake_functions(&ctx, provider_arc);

        Ok(ctx)
    }

    /// Test that ducklake_table_changes returns empty when no changes between snapshots
    #[tokio::test]
    async fn test_table_changes_no_changes() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query changes between same snapshot (should be empty)
        let df = ctx
            .sql("SELECT * FROM ducklake_table_changes('main.events', 1, 1)")
            .await?;

        let batches: Vec<RecordBatch> = df.collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert_eq!(total_rows, 0, "No changes expected between same snapshot");

        Ok(())
    }

    /// Test that ducklake_table_changes returns insert changes with actual row data
    #[tokio::test]
    async fn test_table_changes_inserts() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query all columns including table data + CDC metadata
        let df = ctx
            .sql(
                "SELECT id, event_type, value, snapshot_id, change_type FROM ducklake_table_changes('main.events', 0, 10) ORDER BY id",
            )
            .await?;

        let batches: Vec<RecordBatch> = df.collect().await?;

        // Should have rows from both inserts (5 total rows inserted)
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5, "Should have 5 inserted rows");

        // Verify actual row data
        let mut all_ids: Vec<i32> = Vec::new();
        let mut all_change_types: Vec<String> = Vec::new();
        for batch in &batches {
            all_ids.extend(get_int32_column(batch, 0)); // id column
            all_change_types.extend(get_string_column(batch, 4)); // change_type column
        }

        // All rows should have ids 1-5
        assert_eq!(all_ids, vec![1, 2, 3, 4, 5], "Should have ids 1-5");

        // All changes should be inserts (Phase 2 INSERT-only)
        for change_type in all_change_types {
            assert_eq!(
                change_type, "insert",
                "All changes should be 'insert' in Phase 2"
            );
        }

        Ok(())
    }

    /// Test that ducklake_table_changes returns delete changes (delete files added)
    ///
    /// NOTE: DELETE changes are NOT supported in Phase 2 INSERT-only.
    /// This test verifies that no delete changes are returned (expected behavior for this phase).
    /// Future phases will add DELETE support.
    #[tokio::test]
    async fn test_table_changes_deletes() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query only delete changes - in Phase 2, there should be none
        let df = ctx
            .sql("SELECT snapshot_id, change_type FROM ducklake_table_changes('main.events', 0, 100) WHERE change_type = 'delete'")
            .await?;

        let batches: Vec<RecordBatch> = df.collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Phase 2 INSERT-only: no delete changes expected
        assert_eq!(
            total_rows, 0,
            "Phase 2 INSERT-only: no delete changes expected, got {}",
            total_rows
        );

        Ok(())
    }

    /// Test that ducklake_table_changes works with table name without schema
    #[tokio::test]
    async fn test_table_changes_default_schema() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query using just table name (should default to 'main' schema)
        let df = ctx
            .sql("SELECT * FROM ducklake_table_changes('events', 0, 10)")
            .await?;

        let batches: Vec<RecordBatch> = df.collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        assert!(total_rows > 0, "Should find changes using default schema");

        Ok(())
    }

    /// Test that ducklake_table_changes returns correct schema
    #[tokio::test]
    async fn test_table_changes_schema() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        let df = ctx
            .sql("SELECT * FROM ducklake_table_changes('main.events', 0, 10)")
            .await?;

        let schema = df.schema();

        // Verify the expected columns are present (Phase 2: table columns + CDC columns)
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        // Table columns (from events table)
        assert!(
            field_names.contains(&"id"),
            "Schema should contain 'id' table column"
        );
        assert!(
            field_names.contains(&"event_type"),
            "Schema should contain 'event_type' table column"
        );
        assert!(
            field_names.contains(&"value"),
            "Schema should contain 'value' table column"
        );

        // CDC metadata columns
        assert!(
            field_names.contains(&"snapshot_id"),
            "Schema should contain 'snapshot_id' CDC column"
        );
        assert!(
            field_names.contains(&"change_type"),
            "Schema should contain 'change_type' CDC column"
        );

        // Verify column order: table columns first, then CDC columns
        assert_eq!(field_names.len(), 5, "Should have 5 columns total");
        assert_eq!(
            field_names,
            vec!["id", "event_type", "value", "snapshot_id", "change_type"],
            "Columns should be in order: table columns, then CDC columns"
        );

        Ok(())
    }

    /// Test error handling for non-existent table
    #[tokio::test]
    async fn test_table_changes_nonexistent_table() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query a non-existent table
        let result = ctx
            .sql("SELECT * FROM ducklake_table_changes('main.nonexistent', 0, 10)")
            .await;

        assert!(result.is_err(), "Should error for non-existent table");

        Ok(())
    }

    /// Test error handling for non-existent schema
    #[tokio::test]
    async fn test_table_changes_nonexistent_schema() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query a non-existent schema
        let result = ctx
            .sql("SELECT * FROM ducklake_table_changes('nonexistent.events', 0, 10)")
            .await;

        assert!(result.is_err(), "Should error for non-existent schema");

        Ok(())
    }

    /// Test error handling for invalid snapshot range (start > end)
    #[tokio::test]
    async fn test_table_changes_invalid_snapshot_range() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query with start_snapshot > end_snapshot
        let result = ctx
            .sql("SELECT * FROM ducklake_table_changes('main.events', 10, 5)")
            .await;

        assert!(
            result.is_err(),
            "Should error when start_snapshot > end_snapshot"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("start_snapshot") && err_msg.contains("end_snapshot"),
            "Error message should mention snapshot range, got: {}",
            err_msg
        );

        Ok(())
    }
}
