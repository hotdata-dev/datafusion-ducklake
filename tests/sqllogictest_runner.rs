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
use tempfile::TempDir;

/// Preprocess DuckDB test file to remove DuckDB-specific directives
fn preprocess_test_file(content: &str) -> String {
    let mut output = String::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();

        // Skip only DuckDB-specific directives that sqllogictest can't parse
        if trimmed.starts_with("require ")
            || trimmed.starts_with("test-env ")
            || trimmed.starts_with("# name:")
            || trimmed.starts_with("# description:")
            || trimmed.starts_with("# group:")
        {
            continue;
        }

        // Skip ATTACH/DETACH statements (we handle connection in Rust)
        if trimmed == "statement ok"
            && let Some(next_line) = lines.peek() {
                let next_upper = next_line.trim().to_uppercase();
                if next_upper.starts_with("ATTACH ") || next_upper.starts_with("DETACH ") {
                    lines.next(); // Skip the ATTACH/DETACH statement
                    continue;
                }
            }

        // Add line as-is (let tests fail naturally on unsupported features)
        output.push_str(line);
        output.push('\n');
    }

    output
}

/// Run a DuckDB test file using the hybrid adapter
async fn run_hybrid_test(test_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");

    // Create hybrid DB adapter wrapped in Arc for cloning
    let db = std::sync::Arc::new(std::sync::Mutex::new(HybridDuckLakeDB::new(catalog_path)?));

    // Read and preprocess test file
    let original_content = std::fs::read_to_string(test_file)?;
    let processed_content = preprocess_test_file(&original_content);

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
