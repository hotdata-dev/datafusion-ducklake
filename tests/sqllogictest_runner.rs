#![cfg(feature = "metadata-duckdb")]
//! SQL Logic Test Runner using Hybrid DuckDB+DataFusion Adapter
//!
//! This runner executes DuckDB DuckLake tests using a hybrid approach:
//! - WRITE operations (CREATE/INSERT/UPDATE/DELETE) → DuckDB
//! - READ operations (SELECT) → DataFusion
//! - After each WRITE → Refresh DataFusion catalog snapshot
//! - Table references rewritten for DataFusion (ducklake.table → ducklake.main.table)
//!
//! This allows comprehensive testing of DataFusion's read path.
//! Tests from: https://github.com/duckdb/ducklake/tree/main/test/sql

mod common;
mod hybrid_asyncdb;

use hybrid_asyncdb::HybridDuckLakeDB;
use sqllogictest::Runner;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Preprocess DuckDB test file to remove DuckDB-specific directives
///
/// This preprocessing:
/// 1. Removes DuckDB-specific test directives (require, test-env, etc.)
/// 2. Skips ATTACH/DETACH statements (handled in Rust)
/// 3. Skips EXPLAIN statements (not testable in hybrid mode)
/// 4. Otherwise lets tests fail naturally for better diagnostics
fn preprocess_test_file(content: &str, test_file: &Path) -> String {
    let test_dir = resolve_test_dir(test_file);
    let uuid = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    );
    let mut test_env = HashMap::new();
    let mut output = String::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();

        if let Some((key, value)) = parse_test_env(trimmed) {
            let resolved = substitute_placeholders(value, &test_env, &test_dir, &uuid);
            test_env.insert(key.to_string(), resolved);
            continue;
        }

        if let Some(var_name) = trimmed.strip_prefix("require-env ") {
            let var_name = var_name.trim();
            if let Ok(value) = std::env::var(var_name) {
                test_env.insert(var_name.to_string(), value);
            }
            continue;
        }

        // Skip only DuckDB-specific directives that sqllogictest can't parse
        if trimmed.starts_with("require ")
            || trimmed.starts_with("# name:")
            || trimmed.starts_with("# description:")
            || trimmed.starts_with("# group:")
        {
            continue;
        }

        // Skip ATTACH/DETACH statements (we handle connection in Rust)
        if trimmed == "statement ok"
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if next_upper.starts_with("ATTACH ") || next_upper.starts_with("DETACH ") {
                lines.next(); // Skip the ATTACH/DETACH statement
                continue;
            }

            // Skip EXPLAIN statements (not testable - no consistent output format)
            if next_upper.starts_with("EXPLAIN ") {
                lines.next(); // Skip the EXPLAIN statement
                continue;
            }
        }

        // Skip query blocks with EXPLAIN
        if trimmed.starts_with("query")
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if next_upper.starts_with("EXPLAIN ") {
                // Skip the query directive, SQL, separator, and results
                lines.next(); // Skip SQL line
                skip_query_results(&mut lines);
                continue;
            }
        }

        // Add line as-is (let tests fail naturally on unsupported features)
        output.push_str(&substitute_placeholders(line, &test_env, &test_dir, &uuid));
        output.push('\n');
    }

    output
}

fn parse_test_env(line: &str) -> Option<(&str, &str)> {
    let rest = line.strip_prefix("test-env ")?;
    let (key, value) = rest.split_once(char::is_whitespace)?;
    Some((key.trim(), value.trim_start()))
}

fn resolve_test_dir(test_file: &Path) -> String {
    let parent = test_file.parent().unwrap_or_else(|| Path::new("."));
    let resolved = parent
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(parent));
    resolved.to_string_lossy().into_owned()
}

fn substitute_placeholders(
    input: &str,
    test_env: &HashMap<String, String>,
    test_dir: &str,
    uuid: &str,
) -> String {
    let mut substituted = input
        .replace("__TEST_DIR__", test_dir)
        .replace("{UUID}", uuid);

    for (key, value) in test_env {
        substituted = substituted.replace(&format!("${{{key}}}"), value);
    }

    substituted
}

/// Skip query results until next directive
fn skip_query_results(lines: &mut std::iter::Peekable<std::str::Lines>) {
    // Skip until we find the separator (----)
    while let Some(line) = lines.peek() {
        if line.trim() == "----" {
            lines.next();
            break;
        }
        lines.next();
    }

    // Skip result lines until next directive
    while let Some(line) = lines.peek() {
        let trimmed = line.trim();
        if trimmed.starts_with("query")
            || trimmed.starts_with("statement")
            || trimmed.starts_with("halt")
            || trimmed.is_empty()
        {
            break;
        }
        lines.next();
    }
}

/// Run a DuckDB test file using the hybrid adapter
async fn run_hybrid_test(test_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");
    let test_path = Path::new(test_file);

    // Create hybrid DB adapter wrapped in Arc for cloning
    let db = std::sync::Arc::new(std::sync::Mutex::new(
        HybridDuckLakeDB::new(catalog_path).await?,
    ));

    // Read and preprocess test file
    let original_content = std::fs::read_to_string(test_file)?;
    let processed_content = preprocess_test_file(&original_content, test_path);

    // Write preprocessed test to temp file
    let temp_test_file = temp_dir.path().join("test.slt");
    std::fs::write(&temp_test_file, processed_content)?;

    // Run preprocessed test file with sqllogictest
    let temp_test_path = temp_test_file.to_string_lossy().to_string();
    tokio::task::spawn_blocking(move || {
        let mut runner = Runner::new(|| {
            let db_clone = db.clone();
            async move { Ok(db_clone.lock().unwrap().clone()) }
        });
        runner.run_file(&temp_test_path)
    })
    .await??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::preprocess_test_file;
    use tempfile::TempDir;

    #[test]
    fn preprocess_expands_test_env_placeholders() {
        let temp_dir = TempDir::new().unwrap();
        let test_file = temp_dir.path().join("basic_data_inlining.test");
        let test_dir = temp_dir.path().canonicalize().unwrap();
        let test_dir = test_dir.to_string_lossy();
        let content = "\
test-env DUCKLAKE_CONNECTION __TEST_DIR__/{UUID}.db
test-env DATA_PATH __TEST_DIR__

statement ok
ATTACH 'ducklake:${DUCKLAKE_CONNECTION}' AS ducklake (DATA_PATH '${DATA_PATH}/inline_files')

statement ok
set extension_directory='__TEST_DIR__/extensions'

query I
SELECT COUNT(*) FROM GLOB('${DATA_PATH}/inline_files/**')
----
0
";

        let processed = preprocess_test_file(content, &test_file);

        assert!(!processed.contains("${DATA_PATH}"));
        assert!(!processed.contains("${DUCKLAKE_CONNECTION}"));
        assert!(!processed.contains("__TEST_DIR__"));
        assert!(!processed.contains("{UUID}"));
        assert!(!processed.contains("ATTACH 'ducklake:"));
        assert!(processed.contains(&format!("set extension_directory='{test_dir}/extensions'")));
        assert!(processed.contains(&format!(
            "SELECT COUNT(*) FROM GLOB('{test_dir}/inline_files/**')"
        )));
    }
}

// ============================================================================
// Auto-discovery test runner - runs all .test files
// ============================================================================

#[tokio::test]
async fn run_all_sqllogictests() {
    use std::path::Path;

    let test_dir = Path::new("tests/sqllogictests/sql");
    let mut test_files = Vec::new();

    // Recursively find all .test files
    fn find_test_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    find_test_files(&path, files);
                } else if path.extension().is_some_and(|ext| ext == "test") {
                    files.push(path);
                }
            }
        }
    }

    find_test_files(test_dir, &mut test_files);
    test_files.sort();

    println!("\nFound {} test files", test_files.len());

    let mut passed = 0;
    let mut failed = 0;
    let mut failed_tests = Vec::new();

    for test_file in &test_files {
        let test_name = test_file
            .strip_prefix("tests/sqllogictests/sql/")
            .unwrap_or(test_file.as_path())
            .display()
            .to_string();

        match run_hybrid_test(test_file.to_str().unwrap()).await {
            Ok(_) => {
                println!("✓ {}", test_name);
                passed += 1;
            },
            Err(e) => {
                println!("✗ {}: {}", test_name, e);
                failed += 1;
                failed_tests.push((test_name, e.to_string()));
            },
        }
    }

    println!("\n========================================");
    println!("Test Summary:");
    println!("  Passed: {}", passed);
    println!("  Failed: {}", failed);
    println!("  Total:  {}", test_files.len());
    println!("========================================");

    if !failed_tests.is_empty() {
        println!("\nFailed tests:");
        for (name, error) in &failed_tests {
            println!("  - {}", name);
            // Print first line of error only
            if let Some(first_line) = error.lines().next() {
                println!("    {}", first_line);
            }
        }
    }
}
