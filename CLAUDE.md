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
   - `DuckdbMetadataProvider` implements the trait using DuckDB as the catalog backend
   - Executes SQL queries against standard DuckLake catalog tables (`ducklake_snapshot`, `ducklake_schema`, `ducklake_table`, `ducklake_column`, `ducklake_data_file`, `ducklake_metadata`)

2. **DataFusion Integration Layer** (`src/catalog.rs`, `src/schema.rs`, `src/table.rs`)
   - Bridges DuckLake concepts to DataFusion's catalog system
   - `DuckLakeCatalog`: Implements `CatalogProvider`, manages schemas and snapshot resolution
   - `DuckLakeSchema`: Implements `SchemaProvider`, lists and provides access to tables
   - `DuckLakeTable`: Implements `TableProvider`, executes queries against Parquet files and handles object store URL resolution

3. **Type Mapping** (`src/types.rs`)
   - Converts DuckLake type strings to Arrow DataTypes
   - Handles basic types (integers, floats, strings, dates, timestamps)
   - Supports decimals with precision/scale parsing
   - Partial support for complex types (lists, structs, maps)
   - `build_arrow_schema()` constructs Arrow schemas from DuckLake column metadata

### Data Flow

When querying a DuckLake table:
1. User creates a `SessionContext` with a `RuntimeEnv` and registers a `DuckLakeCatalog`
2. User registers required object stores (S3, MinIO, etc.) with the `RuntimeEnv`
3. SQL query references table as `catalog.schema.table`
4. DataFusion resolves path: catalog -> schema -> table
5. `DuckLakeTable` queries metadata provider for table structure and data files
6. Paths are resolved hierarchically:
   - Global `data_path` from `ducklake_metadata` table
   - Schema path (relative to `data_path` or absolute)
   - Table path (relative to schema path or absolute)
   - File paths (relative to table path or absolute)
7. `DuckLakeTable` resolves file paths to ObjectStoreUrl and relative paths
8. DataFusion scans Parquet files using registered object stores

### Path Resolution Hierarchy

DuckLake supports hierarchical path resolution with relative and absolute paths:
- **data_path** (from `ducklake_metadata` table): Root path for all data
- **schema.path**: May be relative to `data_path` or absolute
- **table.path**: May be relative to resolved schema path or absolute
- **file.path**: May be relative to resolved table path or absolute

See `catalog.rs:91-106` and `table.rs:66-74` for path resolution logic.

### Object Store Registration

Object stores must be registered with DataFusion's `RuntimeEnv` before querying:
- **Local filesystem**: Automatically available via DataFusion's default object store
- **S3/MinIO**: Must be explicitly registered using `AmazonS3Builder` and `RuntimeEnv::register_object_store()`
- Object stores are registered per-bucket (S3) or globally (local filesystem)
- See `examples/basic_query.rs` for S3/MinIO configuration examples

The `DuckLakeTable` provider handles URL resolution by:
- Parsing file paths to determine storage scheme (s3://, file://, etc.)
- Extracting bucket names and relative paths for DataFusion's file scan operations
- See `table.rs:91-197` for path resolution and normalization logic

## Key Implementation Details

### Snapshot Isolation
- DuckLake uses snapshot IDs for temporal consistency
- Current implementation queries latest snapshot on catalog creation
- Tables and schemas are filtered by snapshot validity ranges

### Parquet File Scanning
- Uses DataFusion's `FileScanConfigBuilder` and `ParquetFormat`
- Files are organized into `FileGroup` for parallel scanning
- Note: File sizes are currently hardcoded in `table.rs:239` (TODO to fix)

### Type System
- DuckLake types are stored as strings in catalog
- Type mapping handles SQL type aliases (e.g., "bigint" -> Int64, "text" -> Utf8)
- Geometry types are mapped to Binary (WKB format)

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
- Complex types (nested lists, structs, maps) have minimal parsing
- File sizes hardcoded in scan operations
- No file pruning based on filters (see `metadata_provider.rs:113`)
- Single metadata provider implementation (DuckDB only)