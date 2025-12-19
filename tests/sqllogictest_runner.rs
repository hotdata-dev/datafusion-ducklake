//! SQL Logic Test runner for DuckLake integration
//!
//! This module runs DuckDB test files (.test) against datafusion-ducklake.
//! It handles ATTACH/CREATE/INSERT by forwarding to DuckDB, then queries via DataFusion.

use datafusion::prelude::*;
use datafusion_ducklake::DuckdbMetadataProvider;
use datafusion_ducklake::catalog::DuckLakeCatalog;
use duckdb::Connection;
use sqllogictest::AsyncDB;
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

#[derive(Debug)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

// Macro to simplify error conversions
macro_rules! impl_from_error {
    ($($t:ty),+ $(,)?) => {
        $(
            impl From<$t> for TestError {
                fn from(e: $t) -> Self {
                    TestError(e.to_string())
                }
            }
        )+
    };
}

impl_from_error!(
    datafusion::error::DataFusionError,
    datafusion_ducklake::error::DuckLakeError,
    arrow::error::ArrowError,
    duckdb::Error,
);

// Helper functions
/// Extract a value between single quotes after a prefix in SQL
fn extract_quoted_value(sql: &str, prefix: &str) -> Option<String> {
    sql.find(prefix).and_then(|start| {
        let path_start = start + prefix.len();
        sql[path_start..]
            .find('\'')
            .map(|end| sql[path_start..path_start + end].to_string())
    })
}

/// Extract rows from RecordBatches with proper NULL handling
fn extract_rows(
    batches: &[arrow::record_batch::RecordBatch],
) -> Result<Vec<Vec<String>>, TestError> {
    let mut rows = vec![];
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let row = (0..batch.num_columns())
                .map(|col_idx| {
                    let col = batch.column(col_idx);
                    if col.is_null(row_idx) {
                        Ok("NULL".to_string())
                    } else {
                        arrow::util::display::array_value_to_string(col, row_idx)
                            .map_err(TestError::from)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            rows.push(row);
        }
    }
    Ok(rows)
}

/// DuckLake database adapter for sqllogictest
struct DuckLakeDB {
    ctx: SessionContext,
    duckdb_conn: Connection,
    #[allow(dead_code)] // Kept to ensure TempDir lives for duration of test
    temp_dir: Arc<TempDir>,
    current_catalog: Option<String>,
}

impl DuckLakeDB {
    /// Creates a new DuckLakeDB with a temporary directory for test catalogs
    async fn new() -> Result<Self, TestError> {
        let temp_dir = Arc::new(TempDir::new().map_err(|e| TestError(e.to_string()))?);

        // Create DuckDB connection
        let duckdb_conn = Connection::open_in_memory()?;

        // Install and load ducklake extension
        duckdb_conn.execute("INSTALL ducklake;", [])?;
        duckdb_conn.execute("LOAD ducklake;", [])?;

        let ctx = SessionContext::new();

        Ok(Self {
            ctx,
            duckdb_conn,
            temp_dir,
            current_catalog: None,
        })
    }

    /// Process ATTACH statement and store catalog info
    fn handle_attach(&mut self, sql: &str) -> Result<(), TestError> {
        // Create data directory (either explicit or default)
        if let Some(data_path) = extract_quoted_value(sql, "DATA_PATH '") {
            std::fs::create_dir_all(data_path).map_err(|e| TestError(e.to_string()))?;
        } else if let Some(db_path) = extract_quoted_value(sql, "'ducklake:") {
            let default_data_path = format!("{}.files", db_path);
            std::fs::create_dir_all(&default_data_path).map_err(|e| TestError(e.to_string()))?;
        }

        // Execute ATTACH in DuckDB
        self.duckdb_conn.execute(sql, [])?;

        // Extract and store catalog name
        if let Some(as_pos) = sql.to_lowercase().find(" as ") {
            let catalog_name = sql[as_pos + 4..]
                .split_whitespace()
                .next()
                .unwrap_or("ducklake")
                .trim()
                .to_string();
            self.current_catalog = Some(catalog_name);
        }

        Ok(())
    }

    /// Register or re-register DataFusion catalog from DuckDB
    fn sync_catalog(&mut self) -> Result<(), TestError> {
        if let Some(catalog_name) = &self.current_catalog {
            let mut stmt = self.duckdb_conn.prepare(
                "SELECT database_name, path FROM duckdb_databases() WHERE path IS NOT NULL",
            )?;
            let mut rows = stmt.query([])?;

            while let Some(row) = rows.next()? {
                let db_name: String = row.get(0)?;
                let db_path: String = row.get(1)?;

                if db_name == *catalog_name {
                    let metadata_provider = DuckdbMetadataProvider::new(db_path)?;
                    let catalog = Arc::new(DuckLakeCatalog::new(metadata_provider)?);
                    self.ctx.register_catalog(catalog_name, catalog);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Check if SQL is a write operation that should go to DuckDB
    fn is_write_operation(&self, sql: &str) -> bool {
        let sql_lower = sql.trim().to_lowercase();
        sql_lower.starts_with("create ")
            || sql_lower.starts_with("insert ")
            || sql_lower.starts_with("update ")
            || sql_lower.starts_with("delete ")
            || sql_lower.starts_with("drop ")
            || sql_lower.starts_with("alter ")
    }

    /// Check if SQL is a query operation that should go to DataFusion
    fn is_query_operation(&self, sql: &str) -> bool {
        let sql_lower = sql.trim().to_lowercase();
        sql_lower.starts_with("select")
            || sql_lower.starts_with("with")
            || sql_lower.starts_with("explain")
    }

    /// Rewrite table references to add 'main' schema when missing
    /// E.g., "catalog.table" -> "catalog.main.table"
    fn rewrite_table_references(&self, sql: &str, catalog_name: &str) -> String {
        // Simple heuristic - for production use a proper SQL parser
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let mut new_parts = Vec::new();

        for part in parts {
            let cleaned = part.trim_matches(',').trim_matches('(').trim_matches(')');
            if cleaned.starts_with(&format!("{}.", catalog_name)) {
                // Check if already has schema (count dots)
                let dot_count = cleaned.matches('.').count();
                if dot_count == 1 {
                    // Only catalog.table - add main schema
                    let replacement = cleaned.replace(
                        &format!("{}.", catalog_name),
                        &format!("{}.main.", catalog_name),
                    );
                    // Preserve any trailing punctuation
                    let modified = part.replace(cleaned, &replacement);
                    new_parts.push(modified);
                } else {
                    new_parts.push(part.to_string());
                }
            } else {
                new_parts.push(part.to_string());
            }
        }

        new_parts.join(" ")
    }
}

#[async_trait::async_trait]
impl AsyncDB for DuckLakeDB {
    type Error = TestError;
    type ColumnType = sqllogictest::DefaultColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        let sql_lower = sql.trim().to_lowercase();

        // Handle ATTACH
        if sql_lower.starts_with("attach ") {
            self.handle_attach(sql)?;
            return Ok(sqllogictest::DBOutput::StatementComplete(0));
        }

        // Handle DETACH
        if sql_lower.starts_with("detach ") {
            self.duckdb_conn.execute(sql, [])?;
            self.current_catalog = None;
            return Ok(sqllogictest::DBOutput::StatementComplete(0));
        }

        // Handle USE
        if sql_lower.starts_with("use ") {
            if let Some(catalog_name) = sql.split_whitespace().nth(1) {
                self.current_catalog = Some(catalog_name.to_string());
            }
            return Ok(sqllogictest::DBOutput::StatementComplete(0));
        }

        // Handle SHOW TABLES
        if sql_lower == "show tables" {
            if let Some(catalog_name) = &self.current_catalog {
                if let Some(catalog) = self.ctx.catalog(catalog_name) {
                    if let Some(schema) = catalog.schema("main") {
                        let table_names = schema.table_names();
                        let rows: Vec<Vec<String>> =
                            table_names.into_iter().map(|name| vec![name]).collect();

                        return Ok(sqllogictest::DBOutput::Rows {
                            types: vec![sqllogictest::DefaultColumnType::Text],
                            rows,
                        });
                    }
                }
            }
            return Ok(sqllogictest::DBOutput::Rows {
                types: vec![],
                rows: vec![],
            });
        }

        // Handle SHOW ALL TABLES (just return statement complete)
        if sql_lower.contains("show all tables") {
            return Ok(sqllogictest::DBOutput::StatementComplete(0));
        }

        // Handle write operations via DuckDB
        if self.is_write_operation(sql) {
            self.duckdb_conn.execute(sql, [])?;
            self.sync_catalog()?;
            return Ok(sqllogictest::DBOutput::StatementComplete(0));
        }

        // Handle query operations via DataFusion
        if self.is_query_operation(sql) {
            self.sync_catalog()?;
            let rewritten_sql = if let Some(catalog_name) = &self.current_catalog {
                self.rewrite_table_references(sql, catalog_name)
            } else {
                sql.to_string()
            };

            let df = self.ctx.sql(&rewritten_sql).await?;
            let batches = df.collect().await?;

            if batches.is_empty() {
                return Ok(sqllogictest::DBOutput::Rows {
                    types: vec![],
                    rows: vec![],
                });
            }

            // Extract schema and rows
            let schema = batches[0].schema();
            let types: Vec<sqllogictest::DefaultColumnType> = schema
                .fields()
                .iter()
                .map(|_f| sqllogictest::DefaultColumnType::Any)
                .collect();

            let rows = extract_rows(&batches)?;

            Ok(sqllogictest::DBOutput::Rows {
                types,
                rows,
            })
        } else {
            // Unknown statement type - return statement complete
            Ok(sqllogictest::DBOutput::StatementComplete(0))
        }
    }

    fn engine_name(&self) -> &str {
        "datafusion-ducklake"
    }
}

/// Directives to skip when preprocessing test files
const SKIP_DIRECTIVES: &[&str] = &["require ", "# require", "mode ", "# mode"];

/// Preprocesses a test file to remove DuckDB-specific directives and handle environment variables
fn preprocess_test_file(content: &str, temp_dir: &std::path::Path) -> String {
    use std::collections::HashMap;

    let mut env_vars: HashMap<String, String> = HashMap::new();
    let mut result_lines = Vec::new();

    // Generate a UUID for {UUID} placeholder
    let uuid = uuid::Uuid::new_v4().to_string();
    let test_dir = temp_dir.to_string_lossy().to_string();

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip DuckDB-specific directives
        if SKIP_DIRECTIVES.iter().any(|&d| trimmed.starts_with(d)) {
            continue;
        }

        // Parse test-env directives
        if trimmed.starts_with("test-env ") {
            let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
            if parts.len() == 3 {
                let var_name = parts[1];
                let var_value = parts[2]
                    .replace("{UUID}", &uuid)
                    .replace("__TEST_DIR__", &test_dir);
                env_vars.insert(var_name.to_string(), var_value);
            }
            continue; // Don't include test-env lines in output
        }

        // Substitute environment variables in the line
        let mut processed_line = line.to_string();
        for (var_name, var_value) in &env_vars {
            let placeholder = format!("${{{}}}", var_name);
            processed_line = processed_line.replace(&placeholder, var_value);
        }

        // Also replace __TEST_DIR__ directly (for cases not in env vars)
        processed_line = processed_line.replace("__TEST_DIR__", &test_dir);

        result_lines.push(processed_line);
    }

    result_lines.join("\n")
}

/// Runs a single test file
async fn run_test_file(test_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Create runner with factory closure
    let mut runner = sqllogictest::Runner::new(|| async { DuckLakeDB::new().await });

    // Find test file
    let test_file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/sqllogictests/sql")
        .join(test_name);

    if !test_file.exists() {
        return Err(format!("Test file not found: {}", test_file.display()).into());
    }

    println!("Running test: {}", test_file.display());

    // Create temporary directory for this test run
    let temp_dir = tempfile::tempdir()?;

    // Read and preprocess the test file
    let content = std::fs::read_to_string(&test_file)?;
    let preprocessed = preprocess_test_file(&content, temp_dir.path());

    // Write to a temporary file
    let temp_file = temp_dir.path().join(test_name);

    // Create parent directory if needed (for tests in subfolders)
    if let Some(parent) = temp_file.parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(&temp_file, preprocessed)?;

    // Run the preprocessed file
    runner.run_file_async(temp_file).await?;

    Ok(())
}

// ============================================================================
// Test Cases
// ============================================================================

async fn run_test_from_folder(
    folder: &str,
    test_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let test_path = format!("{}/{}", folder, test_name);
    run_test_file(&test_path).await
}

// Top-level tests

#[tokio::test]
async fn test_ducklake_basic() -> Result<(), Box<dyn std::error::Error>> {
    run_test_file("ducklake_basic.test").await
}

// Insert tests

#[tokio::test]
async fn test_insert_column_list() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("insert", "insert_column_list.test").await
}

#[tokio::test]
async fn test_insert_into_self() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("insert", "insert_into_self.test").await
}

#[tokio::test]
async fn test_insert_file_size() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("insert", "insert_file_size.test").await
}

// Delete tests

#[tokio::test]
async fn test_empty_delete() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("delete", "empty_delete.test").await
}

#[tokio::test]
async fn test_basic_delete() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("delete", "basic_delete.test").await
}

#[tokio::test]
async fn test_multi_deletes() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("delete", "multi_deletes.test").await
}

// General tests

#[tokio::test]
async fn test_ducklake_read_only() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("general", "ducklake_read_only.test").await
}

// Type tests

#[tokio::test]
async fn test_floats() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("types", "floats.test").await
}

#[tokio::test]
async fn test_timestamp() -> Result<(), Box<dyn std::error::Error>> {
    run_test_from_folder("types", "timestamp.test").await
}
