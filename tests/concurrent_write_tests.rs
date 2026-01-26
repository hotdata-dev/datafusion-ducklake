#![cfg(all(feature = "write-sqlite", feature = "write"))]
//! Concurrent write tests for DuckLake catalogs.

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_ducklake::metadata_writer::MetadataWriter;
use datafusion_ducklake::{DuckLakeTableWriter, SqliteMetadataWriter};
use tempfile::TempDir;

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

fn create_user_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ])
}

fn create_user_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
    let schema = Arc::new(create_user_schema());
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(ids.to_vec())), Arc::new(StringArray::from(names.to_vec()))],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_snapshot_creation() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    let mut tasks = Vec::new();
    for _ in 0..20 {
        let writer_clone = Arc::clone(&writer);
        let task = tokio::spawn(async move { writer_clone.create_snapshot() });
        tasks.push(task);
    }

    let mut snapshot_ids = Vec::new();
    for task in tasks {
        let result = task.await.expect("Task panicked");
        snapshot_ids.push(result.expect("create_snapshot failed"));
    }

    snapshot_ids.sort();
    let unique_count = snapshot_ids.windows(2).filter(|w| w[0] != w[1]).count() + 1;
    assert_eq!(
        unique_count,
        snapshot_ids.len(),
        "Snapshot IDs should be unique"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_writes_different_tables() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    let mut tasks = Vec::new();
    for i in 0..10 {
        let writer_clone = Arc::clone(&writer);
        let table_name = format!("table_{}", i);

        let task = tokio::spawn(async move {
            let table_writer = DuckLakeTableWriter::new(writer_clone).unwrap();
            let batch = create_user_batch(&[i], &[&format!("user_{}", i)]);
            let result = table_writer.write_table("main", &table_name, &[batch]);
            (i, table_name, result)
        });

        tasks.push(task);
    }

    let mut results = Vec::new();
    for task in tasks {
        let (i, table_name, result) = task.await.expect("Task panicked");
        let write_result = result.unwrap_or_else(|_| panic!("Write to {} failed", table_name));
        assert_eq!(write_result.records_written, 1);
        results.push((i, write_result));
    }

    let mut snapshot_ids: Vec<i64> = results.iter().map(|(_, r)| r.snapshot_id).collect();
    snapshot_ids.sort();
    let unique_count = snapshot_ids.windows(2).filter(|w| w[0] != w[1]).count() + 1;
    assert_eq!(
        unique_count,
        snapshot_ids.len(),
        "Each write should get unique snapshot ID"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_writes_same_table_append() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    {
        let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();
        let batch = create_user_batch(&[0], &["initial"]);
        table_writer
            .write_table("main", "shared_table", &[batch])
            .unwrap();
    }

    let mut tasks = Vec::new();
    for i in 1..=10 {
        let writer_clone = Arc::clone(&writer);
        let task = tokio::spawn(async move {
            let table_writer = DuckLakeTableWriter::new(writer_clone).unwrap();
            let batch = create_user_batch(&[i], &[&format!("user_{}", i)]);
            table_writer.append_table("main", "shared_table", &[batch])
        });
        tasks.push(task);
    }

    for task in tasks {
        let result = task.await.expect("Task panicked");
        assert_eq!(result.unwrap().records_written, 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_write_session_cleanup_on_drop() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);
    let schema = create_user_schema();

    // Dropped session should clean up orphaned file
    let file_path = {
        let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();
        let mut session = table_writer
            .begin_write("main", "dropped_table", &schema, true)
            .unwrap();
        let batch = create_user_batch(&[1, 2, 3], &["a", "b", "c"]);
        session.write_batch(&batch).unwrap();
        session.file_path().to_path_buf()
    };
    assert!(
        !file_path.exists(),
        "Orphaned file should be deleted on drop"
    );

    // Finished session should keep file
    let finished_path = {
        let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();
        let mut session = table_writer
            .begin_write("main", "finished_table", &schema, true)
            .unwrap();
        let batch = create_user_batch(&[1, 2, 3], &["a", "b", "c"]);
        session.write_batch(&batch).unwrap();
        let path = session.file_path().to_path_buf();
        session.finish().unwrap();
        path
    };
    assert!(finished_path.exists(), "Finished file should exist");
}

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

    // Valid batch succeeds
    session
        .write_batch(&create_user_batch(&[1], &["valid"]))
        .unwrap();

    // Wrong column count fails
    let wrong_cols_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("extra", DataType::Int32, true),
    ]));
    let wrong_cols_batch = RecordBatch::try_new(
        wrong_cols_schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["test"])),
            Arc::new(Int32Array::from(vec![99])),
        ],
    )
    .unwrap();
    assert!(session.write_batch(&wrong_cols_batch).is_err());

    // Wrong column type fails
    let wrong_type_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
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
    assert!(session.write_batch(&wrong_type_batch).is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn test_transaction_atomicity() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let initial_snapshot = writer.create_snapshot().unwrap();

    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);
    let table_writer = DuckLakeTableWriter::new(Arc::clone(&writer)).unwrap();

    let batch = create_user_batch(&[1], &["test"]);
    let result = table_writer
        .write_table("main", "atomic_test", &[batch])
        .unwrap();
    assert!(result.snapshot_id > initial_snapshot);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_stress_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let (writer, _): (SqliteMetadataWriter, _) = create_test_writer(&temp_dir).await;
    let writer: Arc<dyn MetadataWriter> = Arc::new(writer);

    let mut tasks = Vec::new();
    for i in 0..50 {
        let writer_clone = Arc::clone(&writer);
        let task = tokio::spawn(async move {
            let table_writer = DuckLakeTableWriter::new(writer_clone).unwrap();
            let batch = create_user_batch(&[i], &[&format!("stress_{}", i)]);
            let table_name = format!("stress_table_{}", i % 10);
            table_writer.append_table("main", &table_name, &[batch])
        });
        tasks.push(task);
    }

    let mut successes = 0;
    for task in tasks {
        if task.await.expect("Task panicked").is_ok() {
            successes += 1;
        }
    }
    assert_eq!(successes, 50);
}
