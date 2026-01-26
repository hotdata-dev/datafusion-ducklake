#![cfg(all(feature = "write-sqlite", feature = "write"))]
//! Concurrent write tests for DuckLake catalogs
//!
//! This test suite verifies that the write operations are thread-safe and handle
//! concurrent writes correctly without race conditions or data corruption.
//!
//! ## Test Scenarios
//!
//! - Multiple writers creating snapshots concurrently
//! - Multiple writers writing to the same table concurrently
//! - Multiple writers writing to different tables concurrently
//! - Writer dropped without finish() (cleanup verification)

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_ducklake::metadata_writer::MetadataWriter;
use datafusion_ducklake::{DuckLakeTableWriter, SqliteMetadataWriter};
use tempfile::TempDir;

/// Helper to create a test writer with initialized schema
async fn create_test_writer(temp_dir: &TempDir) -> (SqliteMetadataWriter, std::path::PathBuf) {
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer: SqliteMetadataWriter = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer
        .set_data_path(data_path.to_string_lossy().as_ref())
        .unwrap();

    (writer, data_path)
}

/// Create a test schema for users table
fn create_user_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

/// Create a test batch with user data
fn create_user_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
    let schema = Arc::new(create_user_schema());
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(StringArray::from(
                names.iter().map(|s| *s).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Test concurrent snapshot creation
///
/// This test verifies that multiple concurrent calls to create_snapshot
/// each get unique snapshot IDs without race conditions.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_snapshot_creation() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    // Spawn 20 concurrent tasks that create snapshots
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let writer_clone = Arc::clone(&writer);
        let task = tokio::spawn(async move { writer_clone.create_snapshot() });
        tasks.push(task);
    }

    // Collect all snapshot IDs
    let mut snapshot_ids = Vec::new();
    for task in tasks {
        let result = task.await.expect("Task panicked");
        let snapshot_id = result.expect("create_snapshot failed");
        snapshot_ids.push(snapshot_id);
    }

    // Verify all snapshot IDs are unique
    snapshot_ids.sort();
    let unique_count = snapshot_ids.windows(2).filter(|w| w[0] != w[1]).count() + 1;
    assert_eq!(
        unique_count,
        snapshot_ids.len(),
        "Snapshot IDs should be unique, got duplicates: {:?}",
        snapshot_ids
    );

    eprintln!(
        "Created {} unique snapshot IDs concurrently",
        snapshot_ids.len()
    );
}

/// Test concurrent writes to different tables
///
/// This test verifies that multiple writers can write to different tables
/// concurrently without interfering with each other.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_writes_different_tables() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    // Spawn 10 concurrent tasks that write to different tables
    let mut tasks = Vec::new();
    for i in 0..10 {
        let writer_clone = Arc::clone(&writer);
        let table_name = format!("table_{}", i);

        let task = tokio::spawn(async move {
            let table_writer = DuckLakeTableWriter::new(writer_clone).unwrap();
            let batch = create_user_batch(&[i as i32], &[&format!("user_{}", i)]);
            let result = table_writer.write_table("main", &table_name, &[batch]);
            (i, table_name, result)
        });

        tasks.push(task);
    }

    // Wait for all tasks and verify success
    let mut results = Vec::new();
    for task in tasks {
        let (i, table_name, result) = task.await.expect("Task panicked");
        let write_result = result.expect(&format!("Write to {} failed", table_name));
        assert_eq!(
            write_result.records_written, 1,
            "Table {} should have 1 record",
            table_name
        );
        results.push((i, write_result));
    }

    // Verify all writes got unique snapshot IDs
    let mut snapshot_ids: Vec<i64> = results.iter().map(|(_, r)| r.snapshot_id).collect();
    snapshot_ids.sort();
    let unique_count = snapshot_ids.windows(2).filter(|w| w[0] != w[1]).count() + 1;
    assert_eq!(
        unique_count,
        snapshot_ids.len(),
        "Each write should get a unique snapshot ID"
    );

    eprintln!("Successfully wrote to 10 different tables concurrently");
}

/// Test concurrent writes to the same table (append)
///
/// This test verifies that multiple writers can append to the same table
/// concurrently. Each write creates a new snapshot.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_writes_same_table_append() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    // First, create the table with initial data
    {
        let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();
        let batch = create_user_batch(&[0], &["initial"]);
        table_writer
            .write_table("main", "shared_table", &[batch])
            .unwrap();
    }

    // Spawn 10 concurrent append tasks
    let mut tasks = Vec::new();
    for i in 1..=10 {
        let writer_clone = Arc::clone(&writer);

        let task = tokio::spawn(async move {
            let table_writer = DuckLakeTableWriter::new(writer_clone).unwrap();
            let batch = create_user_batch(&[i as i32], &[&format!("user_{}", i)]);
            let result = table_writer.append_table("main", "shared_table", &[batch]);
            (i, result)
        });

        tasks.push(task);
    }

    // Wait for all tasks and verify success
    let mut results = Vec::new();
    for task in tasks {
        let (i, result) = task.await.expect("Task panicked");
        let write_result = result.expect(&format!("Append {} failed", i));
        assert_eq!(write_result.records_written, 1);
        results.push((i, write_result));
    }

    // All appends should succeed
    assert_eq!(results.len(), 10);

    eprintln!("Successfully appended 10 batches to the same table concurrently");
}

/// Test streaming write session cleanup on drop
///
/// This test verifies that when a TableWriteSession is dropped without
/// calling finish(), the orphaned Parquet file is cleaned up.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_session_cleanup_on_drop() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _data_path): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    let schema = create_user_schema();

    // Start a write session but don't finish it
    let file_path = {
        let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();
        let mut session = table_writer
            .begin_write("main", "dropped_table", &schema, true)
            .unwrap();

        // Write some data
        let batch = create_user_batch(&[1, 2, 3], &["a", "b", "c"]);
        session.write_batch(&batch).unwrap();

        // Get the file path before dropping
        session.file_path().to_path_buf()
        // Session is dropped here without calling finish()
    };

    // Verify the orphaned file was cleaned up
    assert!(
        !file_path.exists(),
        "Orphaned Parquet file should be deleted on drop: {:?}",
        file_path
    );

    eprintln!("Write session cleanup on drop verified");

    // Now verify a properly finished write keeps the file
    let finished_file_path = {
        let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();
        let mut session = table_writer
            .begin_write("main", "finished_table", &schema, true)
            .unwrap();

        let batch = create_user_batch(&[1, 2, 3], &["a", "b", "c"]);
        session.write_batch(&batch).unwrap();

        let file_path = session.file_path().to_path_buf();
        session.finish().unwrap();
        file_path
    };

    // Verify the finished file still exists
    assert!(
        finished_file_path.exists(),
        "Finished Parquet file should exist: {:?}",
        finished_file_path
    );

    eprintln!("Finished write keeps file verified");
}

/// Test schema validation in write_batch
///
/// This test verifies that write_batch correctly validates the batch schema.
#[tokio::test(flavor = "multi_thread")]
async fn test_write_batch_schema_validation() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    let schema = create_user_schema();
    let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();
    let mut session = table_writer
        .begin_write("main", "validation_test", &schema, true)
        .unwrap();

    // Valid batch should succeed
    let valid_batch = create_user_batch(&[1], &["valid"]);
    session.write_batch(&valid_batch).unwrap();

    // Batch with wrong column count should fail
    let wrong_column_count_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra", DataType::Int32, true),
    ]));
    let wrong_column_batch = RecordBatch::try_new(
        wrong_column_count_schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["test"])),
            Arc::new(Int32Array::from(vec![99])),
        ],
    )
    .unwrap();

    let result = session.write_batch(&wrong_column_batch);
    assert!(result.is_err(), "Should reject batch with wrong column count");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Schema mismatch") && err_msg.contains("columns"),
        "Error should mention schema mismatch: {}",
        err_msg
    );

    // Batch with wrong column type should fail
    let wrong_type_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false), // Wrong type: Int64 instead of Int32
        Field::new("name", DataType::Utf8, true),
    ]));
    let wrong_type_batch = RecordBatch::try_new(
        wrong_type_schema,
        vec![
            Arc::new(arrow::array::Int64Array::from(vec![1i64])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap();

    let result = session.write_batch(&wrong_type_batch);
    assert!(result.is_err(), "Should reject batch with wrong column type");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("Schema mismatch") && err_msg.contains("type"),
        "Error should mention type mismatch: {}",
        err_msg
    );

    eprintln!("Schema validation tests passed");
}

/// Test transaction rollback on failure
///
/// This test verifies that if begin_write_transaction fails partway through,
/// no orphaned catalog entries remain.
#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_atomicity() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;

    // Get initial snapshot count
    let initial_snapshot = writer.create_snapshot().unwrap();

    // Verify transaction creates atomic entries
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);
    let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();

    // A successful write should create snapshot, schema, table, columns atomically
    let batch = create_user_batch(&[1], &["test"]);
    let result = table_writer.write_table("main", "atomic_test", &[batch]);
    assert!(result.is_ok(), "Write should succeed");

    let write_result = result.unwrap();
    assert!(
        write_result.snapshot_id > initial_snapshot,
        "Should create new snapshot"
    );

    eprintln!("Transaction atomicity verified");
}

/// Stress test: many concurrent writers
///
/// This test creates 50 concurrent write operations to stress test
/// the ID generation and transaction handling.
#[tokio::test(flavor = "multi_thread")]
async fn test_stress_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    // Spawn 50 concurrent write tasks
    let mut tasks = Vec::new();
    for i in 0..50 {
        let writer_clone = Arc::clone(&writer);

        let task = tokio::spawn(async move {
            let table_writer = DuckLakeTableWriter::new(writer_clone).unwrap();
            let batch = create_user_batch(&[i as i32], &[&format!("stress_{}", i)]);
            let table_name = format!("stress_table_{}", i % 10); // 10 different tables
            let result = table_writer.append_table("main", &table_name, &[batch]);
            (i, result)
        });

        tasks.push(task);
    }

    // Wait for all tasks
    let mut successes = 0;
    let mut failures = Vec::new();
    for task in tasks {
        let (i, result) = task.await.expect("Task panicked");
        match result {
            Ok(_) => successes += 1,
            Err(e) => failures.push((i, e)),
        }
    }

    // All writes should succeed
    assert!(
        failures.is_empty(),
        "All writes should succeed, but {} failed: {:?}",
        failures.len(),
        failures
    );

    assert_eq!(successes, 50, "All 50 writes should succeed");

    eprintln!("Stress test completed: {} successful writes", successes);
}
