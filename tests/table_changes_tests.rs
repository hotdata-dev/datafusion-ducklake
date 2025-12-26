#![cfg(feature = "metadata-duckdb")]
//! Integration tests for ducklake_table_changes() function
//!
//! These tests verify that the table_changes function correctly returns
//! files added between snapshots for CDC (Change Data Capture) queries.

mod common;

use std::sync::Arc;

use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider, register_ducklake_functions};
use tempfile::TempDir;

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

    /// Test that ducklake_table_changes returns insert changes (data files added)
    #[tokio::test]
    async fn test_table_changes_inserts() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query changes from snapshot 0 to current (should include all inserts)
        let df = ctx
            .sql("SELECT change_type, file_path FROM ducklake_table_changes('main.events', 0, 10)")
            .await?;

        let batches: Vec<RecordBatch> = df.collect().await?;

        // Should have at least one insert change
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Should have some changes");

        // Check that we have insert changes
        for batch in &batches {
            let change_types = get_string_column(batch, 0);
            for change_type in change_types {
                assert!(
                    change_type == "insert" || change_type == "delete",
                    "Change type should be 'insert' or 'delete', got '{}'",
                    change_type
                );
            }
        }

        Ok(())
    }

    /// Test that ducklake_table_changes returns delete changes (delete files added)
    #[tokio::test]
    async fn test_table_changes_deletes() -> DataFusionResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let catalog_path = temp_dir.path().join("multi_snapshot.ducklake");

        common::create_catalog_multiple_snapshots(&catalog_path)
            .map_err(common::to_datafusion_error)?;

        let ctx = create_context_with_functions(catalog_path.to_str().unwrap()).await?;

        // Query only delete changes (from last snapshot which has the delete)
        // We need to find the delete files by querying a range that includes the delete operation
        let df = ctx
            .sql("SELECT change_type, row_count FROM ducklake_table_changes('main.events', 0, 100) WHERE change_type = 'delete'")
            .await?;

        let batches: Vec<RecordBatch> = df.collect().await?;
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Should have at least one delete change (we deleted row id=2)
        assert!(
            total_rows >= 1,
            "Should have at least one delete change, got {}",
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

        // Verify the expected columns are present
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

        assert!(
            field_names.contains(&"snapshot_id"),
            "Schema should contain 'snapshot_id'"
        );
        assert!(
            field_names.contains(&"change_type"),
            "Schema should contain 'change_type'"
        );
        assert!(
            field_names.contains(&"file_path"),
            "Schema should contain 'file_path'"
        );
        assert!(
            field_names.contains(&"file_size_bytes"),
            "Schema should contain 'file_size_bytes'"
        );
        assert!(
            field_names.contains(&"row_count"),
            "Schema should contain 'row_count'"
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
