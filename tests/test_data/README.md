# Test Data Setup

This directory contains DuckLake catalogs used for integration testing. The test data is **not checked into git** and must be generated locally.

## Generating Test Data

### 1. Basic Catalogs

Generate the basic test catalogs (no_deletes, with_deletes, with_updates):
```bash
# TODO: Add setup script for basic catalogs
# Currently these are pre-generated binary files
```

### 2. Multiple Delete Files Catalog

Generate the catalog for testing multiple delete files per data file:

```bash
# Copy base catalog
cp -r tests/test_data/with_deletes.ducklake tests/test_data/multiple_deletes.ducklake
cp -r tests/test_data/with_deletes.ducklake.files tests/test_data/multiple_deletes.ducklake.files

# Modify delete file snapshot ranges to create overlapping delete files
duckdb tests/test_data/multiple_deletes.ducklake < tests/test_data/setup_multiple_deletes_catalog.sql
```

This creates a catalog where:
- One data file (5 rows: ids 1-5)
- Two delete files both active in snapshot 4:
  - Delete file 1: position [1] -> deletes id=2
  - Delete file 2: positions [1, 3] -> deletes id=2, id=4
- Expected result after merge: positions [1, 3] deleted, keeping ids [1, 3, 5]

## Test Catalog Contents

| Catalog | Purpose | Data Files | Delete Files | Rows |
|---------|---------|------------|--------------|------|
| `no_deletes.ducklake` | Baseline (no delete files) | 1 | 0 | 4 |
| `with_deletes.ducklake` | Basic delete filtering | 1 | 1 (per snapshot) | 3 |
| `with_updates.ducklake` | Update operations (DELETE+INSERT) | 2 | 1 | 3 |
| `multiple_deletes.ducklake` | Multiple delete files (bug test) | 1 | 2 (same snapshot) | 3 (expected) |

## Notes

- Test data is excluded from git via `.gitignore`
- Each catalog is a DuckDB database file (`.ducklake`)
- Data files are stored in corresponding `.ducklake.files/` directories
- Delete files are Parquet files with schema `(file_path: VARCHAR, pos: INT64)`
