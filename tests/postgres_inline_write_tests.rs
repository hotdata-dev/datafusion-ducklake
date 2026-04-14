#![cfg(all(feature = "metadata-postgres", feature = "write-postgres"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use sqlx::PgPool;
use tempfile::TempDir;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

use datafusion_ducklake::{
    ColumnDef, DuckLakeCatalog, MetadataWriter, PostgresMetadataProvider,
    PostgresMetadataWriter, WriteMode,
};

async fn create_env(
    table_name: &str,
) -> (
    SessionContext,
    TempDir,
    PgPool,
    ContainerAsync<Postgres>,
    String,
) {
    let container = Postgres::default().start().await.unwrap();
    let host = "127.0.0.1";
    let port = container.get_host_port_ipv4(5432).await.unwrap();
    let conn_str = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();
    let data_url = format!("file://{}", data_path.display());

    let writer = PostgresMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(&data_url).unwrap();
    writer
        .begin_write_transaction(
            "main",
            table_name,
            &[
                ColumnDef::new("id", "int32", false).unwrap(),
                ColumnDef::new("name", "varchar", true).unwrap(),
            ],
            WriteMode::Replace,
        )
        .unwrap();

    let provider = PostgresMetadataProvider::new(&conn_str).await.unwrap();
    let pool = provider.pool.clone();
    let catalog = DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(writer))
        .await
        .unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));

    (ctx, temp_dir, pool, container, conn_str)
}

async fn create_read_context(conn_str: &str) -> SessionContext {
    let provider = PostgresMetadataProvider::new(conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).await.unwrap();
    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

fn register_source(ctx: &SessionContext, name: &str, rows: &[(i32, &str)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|(_, name)| *name).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

fn collect_parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                files.extend(collect_parquet_files(&path));
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }
    files
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_uses_inline_storage_below_limit() {
    let (ctx, temp_dir, pool, _container, conn_str) = create_env("inline_users").await;
    register_source(&ctx, "source_inline", &[(1, "Alice"), (2, "Bob")]);

    let df = ctx
        .sql("INSERT INTO ducklake.main.inline_users SELECT * FROM source_inline")
        .await
        .unwrap();
    let result = df.collect().await.unwrap();
    assert_eq!(
        result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap()
            .value(0),
        2
    );

    let inline_tables: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ducklake_inlined_data_tables")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(inline_tables, 1);
    assert!(collect_parquet_files(temp_dir.path()).is_empty());

    let read_ctx = create_read_context(&conn_str).await;
    let rows = read_ctx
        .sql("SELECT id, name FROM ducklake.main.inline_users ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows[0].num_rows(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_falls_back_to_parquet_above_limit() {
    let (ctx, temp_dir, pool, _container, conn_str) = create_env("parquet_users").await;
    let source_rows: Vec<(i32, String)> = (0..11).map(|i| (i, format!("name-{i}"))).collect();
    let borrowed_rows: Vec<(i32, &str)> = source_rows
        .iter()
        .map(|(id, name)| (*id, name.as_str()))
        .collect();
    register_source(&ctx, "source_parquet", &borrowed_rows);

    let df = ctx
        .sql("INSERT INTO ducklake.main.parquet_users SELECT * FROM source_parquet")
        .await
        .unwrap();
    df.collect().await.unwrap();

    let inline_tables: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM ducklake_inlined_data_tables")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(inline_tables, 0);
    assert!(
        !collect_parquet_files(temp_dir.path()).is_empty(),
        "expected parquet fallback to create a parquet file"
    );

    let read_ctx = create_read_context(&conn_str).await;
    let rows = read_ctx
        .sql("SELECT COUNT(*) FROM ducklake.main.parquet_users")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0),
        11
    );
}
