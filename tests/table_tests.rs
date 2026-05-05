#![cfg(feature = "metadata-duckdb")]
//! Table provider tests
//!
//! Tests for DuckLakeTable functionality.

use std::sync::Arc;

use arrow::array::Int64Array;
use datafusion::common::stats::Precision;
use datafusion::error::Result as DataFusionResult;
use datafusion::prelude::*;
use datafusion_ducklake::metadata_provider::MetadataProvider;
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

/// Creates a catalog with a table populated with rows so DuckLake writes a real
/// data file. Used to validate that `DuckLakeTable::statistics()` agrees with
/// the catalog's per-file sizes.
fn create_populated_table_catalog(catalog_path: &std::path::Path) -> anyhow::Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;

    let data_dir = catalog_path.with_extension("ducklake.files");
    std::fs::create_dir_all(&data_dir)?;

    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;
    conn.execute("CREATE TABLE test_catalog.tbl (a INTEGER, b VARCHAR);", [])?;
    // Insert enough rows that DuckLake actually emits a data file.
    conn.execute(
        "INSERT INTO test_catalog.tbl SELECT i, repeat('x', 100) FROM range(0, 1000) t(i);",
        [],
    )?;
    Ok(())
}

/// Validates that `DuckLakeTable::statistics()` returns the same byte total as
/// directly summing `file_size_bytes - delete_file_size_bytes` from the
/// catalog's per-file metadata — i.e. the same aggregate `ducklake_table_info`
/// produces.
#[tokio::test]
async fn test_statistics_total_byte_size_matches_catalog_aggregate() -> DataFusionResult<()> {
    let temp_dir =
        TempDir::new().map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let catalog_path = temp_dir.path().join("stats.ducklake");
    create_populated_table_catalog(&catalog_path)
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

    let catalog = create_catalog(&catalog_path.to_string_lossy())?;
    let schema = datafusion::catalog::CatalogProvider::schema(catalog.as_ref(), "main")
        .expect("main schema exists");
    let table = schema
        .table("tbl")
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .expect("tbl present in main schema");

    // What our impl reports.
    let stats = table.statistics().expect("statistics() returned None");
    let our_bytes = match stats.total_byte_size {
        Precision::Exact(b) | Precision::Inexact(b) => b as i64,
        Precision::Absent => {
            panic!("total_byte_size was Absent for a populated table")
        },
    };

    // Canonical: sum file_size_bytes - delete_file_size_bytes directly from
    // the catalog at the latest snapshot. Same aggregate ducklake_table_info
    // computes; same source rows our statistics() impl reads.
    let provider = DuckdbMetadataProvider::new(catalog_path.to_string_lossy().to_string())
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let snapshot_id = provider
        .get_current_snapshot()
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let schema_meta = provider
        .get_schema_by_name("main", snapshot_id)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .expect("main schema metadata");
    let table_meta = provider
        .get_table_by_name(schema_meta.schema_id, "tbl", snapshot_id)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .expect("tbl metadata");
    let files = provider
        .get_table_files_for_select(table_meta.table_id, snapshot_id)
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
    let canonical_bytes: i64 = files
        .iter()
        .map(|f| {
            let data = f.file.file_size_bytes;
            let dels = f.delete_file.as_ref().map_or(0, |d| d.file_size_bytes);
            data - dels
        })
        .sum();

    assert!(
        our_bytes > 0,
        "expected populated table to report non-zero bytes, got {}",
        our_bytes
    );
    assert_eq!(
        our_bytes, canonical_bytes,
        "statistics().total_byte_size must equal SUM(file_size) - SUM(delete_file_size) \
         from the catalog (our_bytes={}, canonical={})",
        our_bytes, canonical_bytes
    );

    // Hold temp_dir to outlive the catalog handle.
    std::mem::forget(temp_dir);
    Ok(())
}

/// Empty tables — no data files yet — should still return Statistics with
/// total_byte_size == 0, not Absent or None.
#[tokio::test]
async fn test_statistics_zero_for_empty_table() -> DataFusionResult<()> {
    let ctx = setup_empty_table_context("stats_empty").await?;
    let cat = ctx
        .catalog("ducklake")
        .expect("ducklake catalog registered");
    let schema = cat.schema("main").expect("main schema");
    let table = schema
        .table("tbl")
        .await
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .expect("tbl present");
    let stats = table.statistics().expect("statistics() returned None");
    match stats.total_byte_size {
        Precision::Exact(0) | Precision::Inexact(0) => {},
        other => panic!("expected zero bytes for empty table, got {:?}", other),
    }
    Ok(())
}
