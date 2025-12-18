# DataFusion-DuckLake

**This is an early pre-release and very much a work in progress.**

A [DataFusion](https://datafusion.apache.org/) extension for querying [DuckLake](https://ducklake.select). DuckLake is an integrated data lake and catalog format that stores metadata in SQL databases and data as Parquet files on disk or object storage.

The goal of this project is to make DuckLake a first-class, Arrow-native lakehouse format inside DataFusion.

---

## Currently Supported

- Read-only queries against DuckLake catalogs
- DuckDB catalog backend
- Local filesystem and S3-compatible object stores (MinIO, S3)
- Snapshot-based consistency
- Basic and decimal types
- Hierarchical path resolution (`data_path`, `schema`, `table`, `file`)
- Delete files for row-level deletion (MOR – Merge-On-Read)
- Parquet footer size hints for optimized I/O
- Filter pushdown to Parquet for row group pruning and page-level filtering
- Dynamic metadata lookup (no upfront catalog caching)
- SQL-queryable `information_schema` for catalog metadata (snapshots, schemas, tables, columns, files)
- DuckDB-style table functions: `ducklake_snapshots()`, `ducklake_table_info()`, `ducklake_list_files()`

---

## Known Limitations

- Complex types (nested lists, structs, maps) have minimal support
- No write operations
- No partition-based file pruning
- Single metadata provider implementation (DuckDB only)
- No time travel support

---

## Roadmap

This project is under active development. The roadmap below reflects major areas of work currently underway or planned next. For the most up-to-date view, see the open issues and pull requests in this repository.

### Metadata & Catalog Improvements

- Metadata caching to reduce repeated catalog lookups
- Pluggable metadata providers beyond DuckDB:
  - PostgreSQL
  - SQLite
  - MySQL
- Clear abstraction boundaries between catalog, metadata provider, and execution

### Query Planning & Performance

- Partition-aware file pruning
- Improved predicate pushdown
- Smarter Parquet I/O planning
- Reduced metadata round-trips during planning
- Better alignment with DataFusion optimizer rules

### Write Support

- Initial write support for DuckLake tables

### Time Travel & Versioning

- Querying historical snapshots
- Explicit snapshot selection

### Type System Expansion

- Improved support for complex and nested types
- Better alignment with DuckDB and DataFusion type semantics

### Stability & Ergonomics

- Expanded test coverage
- Improved error messages and diagnostics
- Cleaner APIs for embedding in other DataFusion-based systems
- Additional documentation and examples

---

## Usage

### Example

```bash
cargo run --example basic_query -- <catalog.db> <sql>

```

### Integration
```rust
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::*;
use datafusion_ducklake::{DuckLakeCatalog, DuckdbMetadataProvider};
use std::sync::Arc;

// Create metadata provider
let provider = DuckdbMetadataProvider::new("catalog.db")?;

// Create runtime (register object stores if using S3/MinIO)
let runtime = Arc::new(RuntimeEnv::default());

// Example: Register S3/MinIO object store
let s3: Arc<dyn ObjectStore> = Arc::new(
    AmazonS3Builder::new()
        .with_endpoint("http://localhost:9000") // Your MinIO endpoint
        .with_bucket_name("ducklake-data") // Your bucket name
        .with_access_key_id("minioadmin") // Your credentials
        .with_secret_access_key("minioadmin") // Your credentials
        .with_region("us-west-2") // Any region works for MinIO
        .with_allow_http(true) // Required for http:// endpoints
        .build()?,
    );
runtime.register_object_store(&Url::parse("s3://ducklake-data/")?, s3);

// Create DuckLake catalog
let catalog = DuckLakeCatalog::new(provider)?;

// Create session and register catalog
let ctx = SessionContext::new_with_config_rt(
    SessionConfig::new().with_default_catalog_and_schema("ducklake", "main"),
    runtime
);
ctx.register_catalog("ducklake", Arc::new(catalog));

// Query
let df = ctx.sql("SELECT * FROM ducklake.main.my_table").await?;
df.show().await?;


```
### Project Status

This project is evolving alongside DataFusion and DuckLake. APIs may change as core abstractions are refined.

Feedback, issues, and contributions are welcome.
