//! Common test utilities for integration tests
//!
//! This module provides helper functions to generate DuckLake catalogs
//! for testing purposes. Each function creates a persistent DuckDB database
//! file with DuckLake extension, populates it with test data, and returns
//! the path for use with DuckdbMetadataProvider.

use anyhow::Result;
use std::path::Path;

/// Creates a catalog with a simple table (no deletes)
///
/// Table schema:
/// - users (id INT, name VARCHAR, email VARCHAR)
/// - 4 rows: Alice, Bob, Charlie, Diana
///
/// # Example
///
/// ```
/// use tempfile::TempDir;
/// let temp_dir = TempDir::new()?;
/// let catalog_path = temp_dir.path().join("no_deletes.ducklake");
/// create_catalog_no_deletes(&catalog_path)?;
/// ```
pub fn create_catalog_no_deletes(catalog_path: &Path) -> Result<()> {
    // Use in-memory database to avoid file locking issues
    let conn = duckdb::Connection::open_in_memory()?;

    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;

    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;

    conn.execute(
        "CREATE TABLE test_catalog.users (
            id INT,
            name VARCHAR,
            email VARCHAR
        );",
        [],
    )?;

    conn.execute(
        "INSERT INTO test_catalog.users VALUES
            (1, 'Alice', 'alice@example.com'),
            (2, 'Bob', 'bob@example.com'),
            (3, 'Charlie', 'charlie@example.com'),
            (4, 'Diana', 'diana@example.com');",
        [],
    )?;

    Ok(())
}

/// Creates a catalog with DELETE operations
///
/// Table schema:
/// - products (id INT, name VARCHAR, price DECIMAL(10,2), in_stock BOOLEAN)
/// - 5 rows inserted, 2 deleted (ids 2 and 4)
/// - Final result: 3 rows (ids 1, 3, 5)
///
/// This catalog demonstrates delete file functionality where some rows
/// are marked as deleted via DuckLake's delete files.
pub fn create_catalog_with_deletes(catalog_path: &Path) -> Result<()> {
    // Use in-memory database to avoid file locking issues
    let conn = duckdb::Connection::open_in_memory()?;

    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;

    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;

    conn.execute(
        "CREATE TABLE test_catalog.products (
            id INT,
            name VARCHAR,
            price DECIMAL(10,2),
            in_stock BOOLEAN
        );",
        [],
    )?;

    conn.execute(
        "INSERT INTO test_catalog.products VALUES
            (1, 'Laptop', 999.99, true),
            (2, 'Mouse', 25.50, true),
            (3, 'Keyboard', 75.00, true),
            (4, 'Monitor', 299.99, false),
            (5, 'Webcam', 89.99, true);",
        [],
    )?;

    // Create delete files
    conn.execute("DELETE FROM test_catalog.products WHERE id = 2;", [])?;
    conn.execute("DELETE FROM test_catalog.products WHERE id = 4;", [])?;

    Ok(())
}

/// Creates a catalog with UPDATE operations
///
/// Table schema:
/// - inventory (id INT, product_name VARCHAR, quantity INT, last_updated TIMESTAMP)
/// - 3 rows inserted, 2 updated (ids 1 and 3)
/// - Updates create delete files for old versions
///
/// This demonstrates the MOR (Merge-On-Read) pattern where UPDATEs
/// create delete files for old row versions.
pub fn create_catalog_with_updates(catalog_path: &Path) -> Result<()> {
    // Use in-memory database to avoid file locking issues
    let conn = duckdb::Connection::open_in_memory()?;

    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;

    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;

    conn.execute(
        "CREATE TABLE test_catalog.inventory (
            id INT,
            product_name VARCHAR,
            quantity INT,
            last_updated TIMESTAMP
        );",
        [],
    )?;

    conn.execute(
        "INSERT INTO test_catalog.inventory VALUES
            (1, 'Widget A', 100, '2024-01-01 10:00:00'),
            (2, 'Widget B', 200, '2024-01-01 10:00:00'),
            (3, 'Widget C', 150, '2024-01-01 10:00:00');",
        [],
    )?;

    // Create delete files via updates
    conn.execute(
        "UPDATE test_catalog.inventory
         SET quantity = 120, last_updated = '2024-01-02 15:30:00'
         WHERE id = 1;",
        [],
    )?;

    conn.execute(
        "UPDATE test_catalog.inventory
         SET quantity = 180, last_updated = '2024-01-02 16:00:00'
         WHERE id = 3;",
        [],
    )?;

    Ok(())
}

/// Creates a catalog for filter pushdown correctness testing
///
/// Table schema:
/// - items (id INT, value VARCHAR)
/// - 5 rows: [1,2,3,4,5]
/// - Row with id=3 deleted
///
/// This tests that WHERE filters are applied AFTER delete filtering,
/// ensuring correct query semantics.
pub fn create_catalog_filter_pushdown(catalog_path: &Path) -> Result<()> {
    // Use in-memory database to avoid file locking issues
    let conn = duckdb::Connection::open_in_memory()?;

    conn.execute("INSTALL ducklake;", [])?;
    conn.execute("LOAD ducklake;", [])?;

    let ducklake_path = format!("ducklake:{}", catalog_path.display());
    conn.execute(&format!("ATTACH '{}' AS test_catalog;", ducklake_path), [])?;

    conn.execute(
        "CREATE TABLE test_catalog.items (
            id INT,
            value VARCHAR
        );",
        [],
    )?;

    conn.execute(
        "INSERT INTO test_catalog.items VALUES
            (1, 'one'),
            (2, 'two'),
            (3, 'three'),
            (4, 'four'),
            (5, 'five');",
        [],
    )?;

    // Delete id=3 to test filter application order
    conn.execute("DELETE FROM test_catalog.items WHERE id = 3;", [])?;

    Ok(())
}

/// Helper to convert anyhow errors to DataFusion errors
///
/// This is useful for converting anyhow::Error to DataFusionError in test code.
pub fn to_datafusion_error(e: anyhow::Error) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(e.into())
}
