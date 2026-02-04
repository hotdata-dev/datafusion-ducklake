# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.0.5] - 2026-02-04

### Added
- Write support with streaming API for DuckLake catalogs (`write` feature flag)
- SQL write support with `INSERT INTO` statements (`write` feature flag)
- Schema evolution support
- TPC-H and TPC-DS benchmarks comparing DuckDB-DuckLake vs DataFusion-DuckLake
- Benchmark test workflow for CI

### Changed
- Reuse DuckDB connection for metadata queries instead of creating new connection per call (performance improvement)

## [0.0.4] - 2026-01-14

### Added
- SQLite metadata provider (`metadata-sqlite` feature flag)
- Delete file CDC support in `ducklake_table_changes()` function

## [0.0.3] - 2026-01-09

### Added
- PostgreSQL metadata provider (`metadata-postgres` feature flag)
- MySQL metadata provider (`metadata-mysql` feature flag)
- Parquet Modular Encryption (PME) support for reading encrypted files (`encryption` feature flag)
- `ducklake_table_changes()` table function returning actual row data from Parquet files
- Feature flags for metadata providers
- SQLLogicTest runner for DuckDB test files

### Fixed
- Empty table queries now return empty results instead of errors
- Snapshot filtering for complete row deletion scenarios
- Column renaming via Parquet field_id → DuckLake column_id mapping
- Pinned rustc version to 1.92.0 for build stability

## [0.0.2] - 2025-12-17

### Added
- DuckDB-style table functions for catalog introspection:
  - `ducklake_snapshots()`, `ducklake_schemas()`, `ducklake_tables()`
  - `ducklake_columns()`, `ducklake_data_files()`, `ducklake_delete_files()`
- Snapshot-pinned catalog ensuring consistent reads across a query session

## [0.0.1] - 2025-10-25

Initial release.

### Added
- Read-only SQL queries against DuckLake catalogs via DataFusion
- Support for local filesystem and S3/MinIO object stores
- Row-level delete support (merge-on-read)
- Filter pushdown to Parquet
- Query-scoped snapshot isolation

[0.0.5]: https://github.com/hotdata-dev/datafusion-ducklake/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/hotdata-dev/datafusion-ducklake/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/hotdata-dev/datafusion-ducklake/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/hotdata-dev/datafusion-ducklake/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/hotdata-dev/datafusion-ducklake/releases/tag/v0.0.1