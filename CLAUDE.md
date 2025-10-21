# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DataFusion-DuckLake is a DataFusion extension that provides read-only access to DuckLake catalogs. DuckLake is an integrated data lake and catalog format that stores:
- **Metadata**: In SQL databases (DuckDB, SQLite, PostgreSQL, MySQL) as structured catalog tables
- **Data**: As Apache Parquet files on disk or object storage (S3, MinIO)

The extension integrates DuckLake with Apache DataFusion by implementing DataFusion's catalog and table provider interfaces.

## Commands

### Build and Test
```bash
# Build the project
cargo build

# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Build and run the basic query example
cargo run --example basic_query -- <catalog.db> <sql>
```

## Architecture

### Core Components

The codebase follows a layered architecture with clear separation of concerns:

1. **MetadataProvider Layer** (`src/metadata_provider.rs`, `src/metadata_provider_duckdb.rs`)
   - Abstraction for querying DuckLake catalog metadata
   - `MetadataProvider` trait defines interface for listing schemas, tables, columns, and data files
   - Also provides individual lookup methods: `get_schema_by_name()`, `get_table_by_name()`, and `table_exists()`
   - `DuckdbMetadataProvider` implements the trait using DuckDB as the catalog backend
   - Executes SQL queries against standard DuckLake catalog tables (`ducklake_snapshot`, `ducklake_schema`, `ducklake_table`, `ducklake_column`, `ducklake_data_file`, `ducklake_delete_file`, `ducklake_metadata`)
   - Thread-safe: Opens a new read-only connection for each query
   - Supports delete files: `get_table_files_for_select()` returns data files with associated delete files

2. **DataFusion Integration Layer** (`src/catalog.rs`, `src/schema.rs`, `src/table.rs`)
   - Bridges DuckLake concepts to DataFusion's catalog system
   - `DuckLakeCatalog`: Implements `CatalogProvider`, uses dynamic metadata lookup (queries on every call to `schema()` and `schema_names()`)
   - `DuckLakeSchema`: Implements `SchemaProvider`, uses dynamic metadata lookup (queries on every call to `table()` and `table_names()`)
   - `DuckLakeTable`: Implements `TableProvider`, caches table structure and file lists at creation time
   - **No HashMaps**: Catalog and schema providers query metadata on-demand rather than caching

3. **Path Resolution** (`src/path_resolver.rs`)
   - Centralized utilities for parsing object store URLs and resolving hierarchical paths
   - `parse_object_store_url()`: Parses S3, file://, or local paths into ObjectStoreUrl and path components
   - `resolve_path()`: Resolves relative or absolute paths in the catalog hierarchy
   - `PathResolver`: Maintains base URL and path for hierarchical resolution (catalog -> schema -> table -> file)
   - Handles S3, MinIO, and local filesystem paths uniformly

4. **Delete File Filtering** (`src/delete_filter.rs`)
   - `DeleteFilterExec`: Custom execution plan that wraps Parquet scans and filters deleted rows
   - Implements MOR (Merge-On-Read) pattern for row-level deletes
   - Delete files contain `(file_path: VARCHAR, pos: INT64)` schema
   - Efficiently filters rows by position during query execution
   - Supports COUNT(*) optimization (zero-column batches)

5. **Type Mapping** (`src/types.rs`)
   - Converts DuckLake type strings to Arrow DataTypes
   - Handles basic types (integers, floats, strings, dates, timestamps)
   - Supports decimals with precision/scale parsing
   - Complex types (lists, structs, maps) return proper errors instead of silently failing
   - `build_arrow_schema()` constructs Arrow schemas from DuckLake column metadata

### Dynamic Metadata Lookup

The catalog uses a **pure dynamic lookup** approach with no caching at the catalog/schema level:

- **DuckLakeCatalog** (`catalog.rs`):
  - `schema_names()`: Queries `list_schemas()` on every call
  - `schema()`: Queries `get_schema_by_name()` on every call
  - `new()`: O(1) - only fetches snapshot ID and data_path

- **DuckLakeSchema** (`schema.rs`):
  - `table_names()`: Queries `list_tables()` on every call
  - `table()`: Queries `get_table_by_name()` on every call
  - `table_exist()`: Queries `table_exists()` on every call
  - `new()`: O(1) - just stores IDs and paths

- **DuckLakeTable** (`table.rs`):
  - Still caches table structure and file lists at creation time
  - This is necessary for query planning and execution

**Benefits**:
- O(1) memory usage regardless of catalog size
- Fast catalog startup (no upfront schema/table listing)
- Always fresh metadata (no stale cache issues)
- Simple implementation (no cache invalidation logic)

**Trade-offs**:
- Small query overhead per metadata lookup (acceptable for read-only DuckDB connections)
- Future optimization: Add optional caching layer via wrapper implementation

### Data Flow

When querying a DuckLake table:
1. User creates a `SessionContext` with a `RuntimeEnv` and registers a `DuckLakeCatalog`
2. User registers required object stores (S3, MinIO, etc.) with the `RuntimeEnv`
3. SQL query references table as `catalog.schema.table`
4. DataFusion resolves path: catalog -> schema -> table (queries metadata on-demand)
5. `DuckLakeTable` queries metadata provider for table structure and data files (cached at table creation)
6. Paths are resolved hierarchically using `path_resolver` utilities:
   - Global `data_path` from `ducklake_metadata` table
   - Schema path (relative to `data_path` or absolute)
   - Table path (relative to schema path or absolute)
   - File paths (relative to table path or absolute)
7. `DuckLakeTable` resolves file paths to ObjectStoreUrl and relative paths
8. For each file, check if delete file exists (from metadata join)
9. Files without deletes are grouped into a single efficient `ParquetExec`
10. Files with deletes get individual `ParquetExec` wrapped in `DeleteFilterExec`
11. All execution plans are combined with `UnionExec` if multiple plans exist
12. DataFusion scans Parquet files using registered object stores
13. Delete filters apply row position filtering during streaming execution

### Path Resolution Hierarchy

DuckLake supports hierarchical path resolution with relative and absolute paths:
- **data_path** (from `ducklake_metadata` table): Root path for all data
- **schema.path**: May be relative to `data_path` or absolute
- **table.path**: May be relative to resolved schema path or absolute
- **file.path**: May be relative to resolved table path or absolute

See `path_resolver.rs` for centralized path resolution logic, particularly:
- `parse_object_store_url()`: Converts paths to ObjectStoreUrl + key path
- `resolve_path()`: Handles relative/absolute path resolution
- `PathResolver`: Hierarchical resolver with `child_resolver()` for multi-level paths

### Object Store Registration

Object stores must be registered with DataFusion's `RuntimeEnv` before querying:
- **Local filesystem**: Automatically available via DataFusion's default object store
- **S3/MinIO**: Must be explicitly registered using `AmazonS3Builder` and `RuntimeEnv::register_object_store()`
- Object stores are registered per-bucket (S3) or globally (local filesystem)
- See `examples/basic_query.rs` for S3/MinIO configuration examples

The `DuckLakeTable` provider handles URL resolution by:
- Using `path_resolver::resolve_path()` to resolve file paths hierarchically
- Passing resolved absolute paths to DataFusion's `PartitionedFile`
- Leveraging the `ObjectStoreUrl` from catalog initialization for all file operations

## Key Implementation Details

### Snapshot Isolation
- DuckLake uses snapshot IDs for temporal consistency
- Current implementation queries latest snapshot on catalog creation
- Tables and schemas are filtered by snapshot validity ranges

### Parquet File Scanning
- Uses DataFusion's `FileScanConfigBuilder` and `ParquetFormat`
- Files are organized into `FileGroup` for parallel scanning
- **Footer Size Optimization**: Parquet footer sizes stored in metadata and passed via `with_metadata_size_hint()`
  - Reduces I/O from 2 reads to 1 read per file (especially beneficial for S3/MinIO)
  - Applied to both data files and delete files
- Files without delete files are grouped into a single `ParquetExec` for efficiency
- Files with delete files get individual `ParquetExec` wrapped in `DeleteFilterExec`

### Delete File Implementation
- **Delete files** contain row positions to exclude: `(file_path: VARCHAR, pos: INT64)`
- Metadata join in `SQL_GET_DATA_FILES` associates delete files with data files
- `DeleteFilterExec` wraps Parquet scans and filters rows by global position
- Supports MOR (Merge-On-Read) pattern for efficient row-level deletes
- Handles edge cases: COUNT(*) optimization, empty batches, all rows deleted
- See `delete_filter.rs` and `tests/delete_filter_tests.rs` for implementation and tests

### Filter Pushdown
- Implements `supports_filters_pushdown()` returning `Inexact` for all filters
- Allows DataFusion to push filters to Parquet for:
  - Row group pruning via statistics
  - Page-level filtering with late materialization
  - Bloom filter lookups (if available)
- Marks filters as `Inexact` because delete filtering happens after Parquet scan
- DataFusion automatically reapplies filters after `DeleteFilterExec` for correctness

### Type System
- DuckLake types are stored as strings in catalog
- Type mapping handles SQL type aliases (e.g., "bigint" -> Int64, "text" -> Utf8)
- Geometry types are mapped to Binary (WKB format)
- Complex types (nested lists, structs, maps) return descriptive errors instead of silently failing

## Development Notes

### Testing with MinIO
The example in `examples/basic_query.rs` shows object store registration for MinIO:
```rust
let runtime = Arc::new(RuntimeEnv::default());
let s3: Arc<dyn ObjectStore> = Arc::new(
    AmazonS3Builder::new()
        .with_endpoint("http://localhost:9000")
        .with_bucket_name("ducklake-data")
        .with_access_key_id("minioadmin")
        .with_secret_access_key("minioadmin")
        .with_region("us-west-2")
        .with_allow_http(true)
        .build()?,
);
runtime.register_object_store(&Url::parse("s3://ducklake-data/")?, s3);
```

### Current Limitations
- Read-only access (no writes to DuckLake catalogs)
- Complex types (nested lists, structs, maps) return errors (not yet supported)
- No partition-based file pruning (TODO: add to `MetadataProvider` trait)
- Single metadata provider implementation (DuckDB only)
- No optional metadata caching layer (all lookups are dynamic)

### Testing
The project includes comprehensive tests:
- **Unit tests**: `src/delete_filter.rs` - Delete file schema and position extraction
- **Integration tests**: `tests/delete_filter_tests.rs` - End-to-end delete filtering scenarios
- **Object store tests**: `tests/object_store_integration_test.rs` - S3/MinIO integration
- Test data setup scripts in `tests/test_data/` (if applicable)

Run tests with:
```bash
cargo test                    # All tests
cargo test delete_filter      # Delete file tests only
cargo test --ignored          # Performance benchmarks
```