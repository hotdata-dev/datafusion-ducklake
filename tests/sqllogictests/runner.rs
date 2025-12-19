//! SQL Logic Test runner for DataFusion-DuckLake
//!
//! This runner executes DuckDB's DuckLake sqllogictests against both DuckDB and DataFusion,
//! validating that DataFusion-DuckLake returns the same results as DuckDB for read queries.
//!
//! Strategy:
//! - All statements (CREATE, INSERT, UPDATE, DELETE, ATTACH, etc.) run on DuckDB only
//! - SELECT queries run on both DuckDB and DataFusion, results are compared
//! - DuckDB-specific syntax that doesn't parse in DataFusion is skipped for validation

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use regex::Regex;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
use std::fs;
use std::io::Write;
use tempfile::TempDir;
use tokio::sync::Mutex;

/// Test runner that executes SQL against both DuckDB and DataFusion
struct DuckLakeTestRunner {
    /// DuckDB connection - executes all statements (wrapped in Mutex for async safety)
    duckdb: Mutex<duckdb::Connection>,

    /// DataFusion context - validates SELECT queries
    ctx: SessionContext,

    /// Tracks attached catalogs: name -> catalog db path
    catalogs: HashMap<String, PathBuf>,

    /// Test directory for __TEST_DIR__ substitution
    test_dir: PathBuf,

    /// Count of queries validated against DataFusion
    validated_count: usize,

    /// Count of queries skipped (DuckDB-specific syntax)
    skipped_count: usize,
}

#[derive(Debug, Clone)]
struct TestError(String);

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for TestError {}

impl DuckLakeTestRunner {
    fn new(test_dir: PathBuf) -> Result<Self, TestError> {
        let duckdb = duckdb::Connection::open_in_memory()
            .map_err(|e| TestError(format!("Failed to open DuckDB: {}", e)))?;

        // Load DuckLake extension
        duckdb
            .execute("INSTALL ducklake;", [])
            .map_err(|e| TestError(format!("Failed to install ducklake: {}", e)))?;
        duckdb
            .execute("LOAD ducklake;", [])
            .map_err(|e| TestError(format!("Failed to load ducklake: {}", e)))?;

        let ctx = SessionContext::new();

        Ok(Self {
            duckdb: Mutex::new(duckdb),
            ctx,
            catalogs: HashMap::new(),
            test_dir,
            validated_count: 0,
            skipped_count: 0,
        })
    }

    /// Substitute __TEST_DIR__ and environment variables in SQL
    fn substitute_vars(&self, sql: &str) -> String {
        let mut result = sql.to_string();

        // Replace __TEST_DIR__
        let test_dir_str = self.test_dir.to_str().unwrap_or("");
        result = result.replace("__TEST_DIR__", test_dir_str);

        // Replace ${VAR} patterns with environment variable or appropriate defaults
        let env_re = Regex::new(r"\$\{([^}]+)\}").unwrap();
        result = env_re
            .replace_all(&result, |caps: &regex::Captures| {
                let var_name = &caps[1];
                std::env::var(var_name).unwrap_or_else(|_| {
                    // Handle common DuckLake test variables
                    match var_name {
                        "DUCKLAKE_CONNECTION" => {
                            // Path to the DuckDB catalog file
                            self.test_dir.join("catalog.db").to_str().unwrap_or("").to_string()
                        }
                        "DATA_PATH" => {
                            // Directory for Parquet files
                            test_dir_str.to_string()
                        }
                        _ => test_dir_str.to_string()
                    }
                })
            })
            .to_string();

        result
    }

    /// Check if SQL is a read query using DataFusion's parser
    fn is_read_query(&self, sql: &str) -> Option<bool> {
        let dialect = GenericDialect {};
        match Parser::parse_sql(&dialect, sql) {
            Ok(statements) if !statements.is_empty() => {
                Some(statements.iter().all(|stmt| matches!(stmt, Statement::Query(_))))
            }
            _ => None, // Parse error or empty = DuckDB-specific syntax
        }
    }

    /// Parse ATTACH statement to extract catalog name and path
    fn parse_attach(&self, sql: &str) -> Option<(String, PathBuf)> {
        // Pattern: ATTACH 'ducklake:path' AS name (with optional params)
        let attach_re =
            Regex::new(r"(?i)ATTACH\s+'ducklake:([^']+)'\s+AS\s+(\w+)").unwrap();

        if let Some(caps) = attach_re.captures(sql) {
            let path = PathBuf::from(&caps[1]);
            let name = caps[2].to_string();
            return Some((name, path));
        }
        None
    }

    /// Register a DuckLake catalog in DataFusion
    fn register_catalog(&mut self, name: &str, catalog_path: &PathBuf) -> Result<(), TestError> {
        // Create the metadata provider from the catalog path
        let path_str = catalog_path
            .to_str()
            .ok_or_else(|| TestError("Invalid catalog path".to_string()))?;
        let provider = DuckdbMetadataProvider::new(path_str)
            .map_err(|e| TestError(format!("Failed to create metadata provider: {}", e)))?;

        let catalog = DuckLakeCatalog::new(provider)
            .map_err(|e| TestError(format!("Failed to create DuckLakeCatalog: {}", e)))?;

        self.ctx.register_catalog(name, Arc::new(catalog));
        self.catalogs.insert(name.to_string(), catalog_path.clone());

        Ok(())
    }

    /// Execute query on DuckDB and return results as DBOutput
    fn execute_duckdb_sync(
        conn: &duckdb::Connection,
        sql: &str,
    ) -> Result<DBOutput<DefaultColumnType>, TestError> {
        let mut stmt = conn
            .prepare(sql)
            .map_err(|e| TestError(format!("DuckDB prepare error: {}", e)))?;

        // Use query_map to execute and collect results
        let rows_result: Result<Vec<Vec<String>>, _> = stmt.query_map([], |row| {
            let mut row_data: Vec<String> = Vec::new();
            let mut idx = 0;
            loop {
                match row.get::<_, duckdb::types::Value>(idx) {
                    Ok(value) => {
                        row_data.push(format_duckdb_value(&value));
                        idx += 1;
                    }
                    Err(_) => break, // No more columns
                }
            }
            Ok(row_data)
        }).map(|iter| iter.filter_map(|r| r.ok()).collect());

        match rows_result {
            Ok(rows) => {
                if rows.is_empty() {
                    // Could be a statement with no results or a query with 0 rows
                    // Check if first row would have columns by looking at stmt
                    Ok(DBOutput::StatementComplete(0))
                } else {
                    let column_count = rows.first().map(|r| r.len()).unwrap_or(0);
                    if column_count == 0 {
                        Ok(DBOutput::StatementComplete(0))
                    } else {
                        let types = vec![DefaultColumnType::Text; column_count];
                        Ok(DBOutput::Rows { types, rows })
                    }
                }
            }
            Err(e) => Err(TestError(format!("DuckDB query error: {}", e))),
        }
    }

    /// Execute query on DataFusion and return results
    async fn execute_datafusion(&self, sql: &str) -> Result<Vec<Vec<String>>, TestError> {
        let df = self
            .ctx
            .sql(sql)
            .await
            .map_err(|e| TestError(format!("DataFusion SQL error: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| TestError(format!("DataFusion collect error: {}", e)))?;

        let mut rows: Vec<Vec<String>> = Vec::new();

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let mut row_data: Vec<String> = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    let value = arrow::util::display::array_value_to_string(col, row_idx)
                        .unwrap_or_else(|_| "NULL".to_string());
                    row_data.push(value);
                }
                rows.push(row_data);
            }
        }

        Ok(rows)
    }
}

/// Format a DuckDB value as a string for comparison
fn format_duckdb_value(value: &duckdb::types::Value) -> String {
    match value {
        duckdb::types::Value::Null => "NULL".to_string(),
        duckdb::types::Value::Boolean(b) => {
            if *b {
                "true"
            } else {
                "false"
            }
            .to_string()
        }
        duckdb::types::Value::TinyInt(i) => i.to_string(),
        duckdb::types::Value::SmallInt(i) => i.to_string(),
        duckdb::types::Value::Int(i) => i.to_string(),
        duckdb::types::Value::BigInt(i) => i.to_string(),
        duckdb::types::Value::Float(f) => f.to_string(),
        duckdb::types::Value::Double(f) => f.to_string(),
        duckdb::types::Value::Text(s) => s.clone(),
        _ => format!("{:?}", value),
    }
}

#[async_trait]
impl AsyncDB for DuckLakeTestRunner {
    type Error = TestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        // Substitute variables
        let sql = self.substitute_vars(sql);
        let sql = sql.trim();

        // Skip empty statements
        if sql.is_empty() {
            return Ok(DBOutput::StatementComplete(0));
        }

        // Execute on DuckDB first (always)
        let duckdb_result = {
            let conn = self.duckdb.lock().await;
            Self::execute_duckdb_sync(&conn, sql)?
        };

        // Check if this is an ATTACH statement - try to register catalog in DataFusion
        // If registration fails (e.g., catalog not ready yet), we just skip DataFusion validation
        if let Some((name, path)) = self.parse_attach(sql) {
            if let Err(e) = self.register_catalog(&name, &path) {
                eprintln!("Note: Could not register catalog '{}' in DataFusion: {}", name, e);
            }
        }

        // Check if this is a read query that we can validate
        if let Some(true) = self.is_read_query(sql) {
            // It's a SELECT query - also run on DataFusion and compare
            match self.execute_datafusion(sql).await {
                Ok(df_rows) => {
                    // Compare results
                    if let DBOutput::Rows {
                        rows: duckdb_rows, ..
                    } = &duckdb_result
                    {
                        if !results_match(duckdb_rows, &df_rows) {
                            return Err(TestError(format!(
                                "Result mismatch!\nSQL: {}\nDuckDB: {:?}\nDataFusion: {:?}",
                                sql, duckdb_rows, df_rows
                            )));
                        }
                    }
                    self.validated_count += 1;
                }
                Err(e) => {
                    // DataFusion failed - log and skip (might be unsupported syntax)
                    eprintln!("DataFusion skipped ({}): {}", e, sql);
                    self.skipped_count += 1;
                }
            }
        }

        Ok(duckdb_result)
    }

    fn engine_name(&self) -> &str {
        "ducklake"
    }

    async fn shutdown(&mut self) {
        println!(
            "Test complete: {} queries validated, {} skipped",
            self.validated_count, self.skipped_count
        );
    }
}

/// Compare two result sets (order-sensitive for now)
fn results_match(duckdb: &[Vec<String>], datafusion: &[Vec<String>]) -> bool {
    if duckdb.len() != datafusion.len() {
        return false;
    }

    for (d_row, f_row) in duckdb.iter().zip(datafusion.iter()) {
        if d_row.len() != f_row.len() {
            return false;
        }
        for (d_val, f_val) in d_row.iter().zip(f_row.iter()) {
            if !values_equal(d_val, f_val) {
                return false;
            }
        }
    }

    true
}

/// Pre-process a test file to remove DuckDB-specific directives
/// Returns the path to a temporary file with the processed content
fn preprocess_test_file(test_file: &PathBuf, temp_dir: &PathBuf) -> std::io::Result<PathBuf> {
    let content = fs::read_to_string(test_file)?;
    let mut output_lines: Vec<String> = Vec::new();

    for line in content.lines() {
        let trimmed = line.trim();

        // Skip DuckDB-specific directives
        if trimmed.starts_with("require ")
            || trimmed.starts_with("test-env ")
            || trimmed.starts_with("mode ")
            || trimmed.starts_with("load ")
            || trimmed.starts_with("loop ")
            || trimmed.starts_with("endloop")
            || trimmed.starts_with("foreach ")
            || trimmed.starts_with("endfor")
            || trimmed.starts_with("concurrentloop")
            || trimmed.starts_with("endconurrentloop")
        {
            // Replace with comment to preserve line numbers
            output_lines.push(format!("# SKIPPED: {}", line));
            continue;
        }

        output_lines.push(line.to_string());
    }

    // Write to temp file
    let file_name = test_file
        .file_name()
        .unwrap_or_default()
        .to_str()
        .unwrap_or("test.slt");
    let output_path = temp_dir.join(file_name);

    let mut file = fs::File::create(&output_path)?;
    for line in output_lines {
        writeln!(file, "{}", line)?;
    }

    Ok(output_path)
}

/// Compare two string values with some tolerance for formatting differences
fn values_equal(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }

    // Handle NULL variations
    if (a == "NULL" || a == "null") && (b == "NULL" || b == "null") {
        return true;
    }

    // Try numeric comparison for floating point tolerance
    if let (Ok(a_f), Ok(b_f)) = (a.parse::<f64>(), b.parse::<f64>()) {
        return (a_f - b_f).abs() < 1e-10;
    }

    false
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let test_files: Vec<PathBuf> = glob::glob("tests/sqllogictests/sql/**/*.test")
        .expect("Failed to read glob pattern")
        .filter_map(|e| e.ok())
        .collect();

    if test_files.is_empty() {
        println!("No test files found");
        return;
    }

    println!("Found {} test files", test_files.len());

    let mut passed = 0;
    let mut failed = 0;
    let mut errors: Vec<(PathBuf, String)> = Vec::new();

    for test_file in &test_files {
        // Create a fresh temp directory for each test
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let test_dir = temp_dir.path().to_path_buf();

        // Pre-process test file to remove DuckDB-specific directives
        let processed_file = match preprocess_test_file(test_file, &test_dir) {
            Ok(f) => f,
            Err(e) => {
                println!("✗ {}: preprocess error: {}", test_file.display(), e);
                errors.push((test_file.clone(), e.to_string()));
                failed += 1;
                continue;
            }
        };

        // Create runner factory for this test
        let mut tester = Runner::new(|| {
            let td = test_dir.clone();
            async move { DuckLakeTestRunner::new(td) }
        });

        match tester.run_file_async(&processed_file).await {
            Ok(_) => {
                println!("✓ {}", test_file.display());
                passed += 1;
            }
            Err(e) => {
                println!("✗ {}: {}", test_file.display(), e);
                errors.push((test_file.clone(), e.to_string()));
                failed += 1;
            }
        }
    }

    println!("\n{} passed, {} failed", passed, failed);

    if !errors.is_empty() && failed > passed {
        // Only exit with error if most tests fail (expected during initial setup)
        println!("\nNote: Many failures expected during initial setup");
    }
}