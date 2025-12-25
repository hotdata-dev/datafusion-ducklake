//! Table provider tests
//!
//! Tests for DuckLakeTable functionality.

use std::sync::Arc;

use arrow::array::Int64Array;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use tempfile::TempDir;

/// Creates a catalog with an empty table (no data files)
fn create_empty_table_catalog(catalog_path: &std::path::Path) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;

    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;

    // Create data directory (DuckLake only creates it on first INSERT)
    let data_dir = catalog_path.with_extension("ducklake.files");
    std::fs::create_dir_all(&data_dir)?;

    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;

    // Multiple columns for projection tests
    conn.execute(
        "CREATE TABLE test_catalog.tbl (a INTEGER, b VARCHAR, c DOUBLE);",
        [],
    )?;

    // No INSERT - table has no data files

    Ok(())
}

fn create_catalog(path: &str) -> DataFusionResult<Arc<DuckLakeCatalog>> {
    let provider = DuckdbMetadataProvider::new(path)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog = DuckLakeCatalog::new(provider)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    Ok(Arc::new(catalog))
}

/// Helper to setup test context
async fn setup_empty_table_context(name: &str) -> DataFusionResult<SessionContext> {
    let temp_dir =
        TempDir::new().map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join(format!("{}.ducklake", name));

    create_empty_table_catalog(&catalog_path)
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", catalog);

    // Keep temp_dir alive by leaking it (test cleanup handles it)
    std::mem::forget(temp_dir);

    Ok(ctx)
}

/// Test basic empty table scan
#[tokio::test]
async fn test_empty_table_basic_scan() -> DataFusionResult<()> {
    let ctx = setup_empty_table_context("basic").await?;

    let df = ctx.sql("SELECT * FROM ducklake.main.tbl").await?;
    let batches = df.collect().await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    Ok(())
}

/// Test empty table with projection
#[tokio::test]
async fn test_empty_table_projection() -> DataFusionResult<()> {
    let ctx = setup_empty_table_context("proj").await?;

    let df = ctx.sql("SELECT a FROM ducklake.main.tbl").await?;
    let schema = df.schema().clone();
    let batches = df.collect().await?;

    // Verify schema has only projected column
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "a");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    Ok(())
}

/// Test empty table with reordered projection
#[tokio::test]
async fn test_empty_table_reordered_projection() -> DataFusionResult<()> {
    let ctx = setup_empty_table_context("reorder").await?;

    let df = ctx.sql("SELECT c, a FROM ducklake.main.tbl").await?;
    let schema = df.schema().clone();
    let batches = df.collect().await?;

    // Verify schema has columns in correct order
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "c");
    assert_eq!(schema.field(1).name(), "a");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    Ok(())
}

/// Test empty table with filter
#[tokio::test]
async fn test_empty_table_with_filter() -> DataFusionResult<()> {
    let ctx = setup_empty_table_context("filter").await?;

    let df = ctx
        .sql("SELECT * FROM ducklake.main.tbl WHERE a > 10")
        .await?;
    let batches = df.collect().await?;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    Ok(())
}

/// Test empty table with aggregate (COUNT)
#[tokio::test]
async fn test_empty_table_aggregate() -> DataFusionResult<()> {
    let ctx = setup_empty_table_context("agg").await?;

    let df = ctx
        .sql("SELECT COUNT(*) as cnt FROM ducklake.main.tbl")
        .await?;
    let batches = df.collect().await?;

    // COUNT on empty table should return 1 row with value 0
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    let cnt = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT should return Int64")
        .value(0);
    assert_eq!(cnt, 0, "COUNT(*) on empty table should be 0");

    Ok(())
}
