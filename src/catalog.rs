//! DuckLake catalog provider implementation

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::Result;
use crate::information_schema::{InformationSchemaData, InformationSchemaProvider};
use crate::metadata_provider::{
    ColumnWithTable, FileWithTable, MetadataProvider, SchemaMetadata, TableMetadata,
};
use crate::path_resolver::{parse_object_store_url, resolve_path};
use crate::schema::DuckLakeSchema;
use crate::table::DuckLakeTable;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionContext;

#[cfg(test)]
use crate::metadata_provider::{SnapshotMetadata, TableWithSchema};
#[cfg(feature = "write")]
use crate::metadata_writer::MetadataWriter;

/// Configuration for write operations (when write feature is enabled)
#[cfg(feature = "write")]
#[derive(Debug, Clone)]
struct WriteConfig {
    /// Metadata writer for catalog operations
    writer: Arc<dyn MetadataWriter>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedTable {
    pub(crate) metadata: TableMetadata,
    pub(crate) table_path: String,
    pub(crate) provider: Arc<DuckLakeTable>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedSchema {
    pub(crate) metadata: SchemaMetadata,
    pub(crate) schema_path: String,
    pub(crate) tables: Arc<RwLock<HashMap<String, CachedTable>>>,
}

#[derive(Debug)]
pub(crate) struct DuckLakeCatalogSnapshot {
    snapshot_id: i64,
    object_store_url: Arc<ObjectStoreUrl>,
    schemas: HashMap<String, CachedSchema>,
    information_schema: Arc<InformationSchemaData>,
}

impl DuckLakeCatalogSnapshot {
    pub(crate) fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub(crate) fn object_store_url(&self) -> Arc<ObjectStoreUrl> {
        Arc::clone(&self.object_store_url)
    }

    pub(crate) fn information_schema(&self) -> Arc<InformationSchemaData> {
        Arc::clone(&self.information_schema)
    }

    pub(crate) fn cached_schema(&self, name: &str) -> Option<CachedSchema> {
        self.schemas.get(name).cloned()
    }

    pub(crate) fn cached_table(&self, schema_name: &str, table_name: &str) -> Option<CachedTable> {
        let schema = self.schemas.get(schema_name)?;
        schema
            .tables
            .read()
            .expect("Catalog table cache poisoned")
            .get(table_name)
            .cloned()
    }

    fn schema_names(&self) -> Vec<String> {
        let mut names = vec!["information_schema".to_string()];
        names.extend(self.schemas.keys().cloned());
        names.sort();
        names.dedup();
        names
    }
}

/// DuckLake catalog provider
///
/// Connects to a DuckLake catalog database and exposes a snapshot-pinned, fully
/// in-memory catalog view for DataFusion planning.
#[derive(Debug, Clone)]
pub struct DuckLakeCatalog {
    /// Metadata provider used to load the snapshot and for explicit reloads
    provider: Arc<dyn MetadataProvider>,
    /// Immutable snapshot used by sync planning hooks
    snapshot: Arc<DuckLakeCatalogSnapshot>,
    /// Write configuration (when write feature is enabled)
    #[cfg(feature = "write")]
    write_config: Option<WriteConfig>,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with a metadata provider
    ///
    /// Gets the current snapshot ID at creation time and binds the catalog to it.
    /// For explicit snapshot control, use `with_snapshot()`.
    pub async fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        let snapshot_id = provider.get_current_snapshot().await?;
        Self::build(provider, snapshot_id, None).await
    }

    /// Create a catalog bound to a specific snapshot ID
    ///
    /// All schemas and tables returned will use this snapshot, guaranteeing
    /// query consistency even if multiple catalog/schema/table lookups occur
    /// during query planning.
    pub async fn with_snapshot(
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
    ) -> Result<Self> {
        Self::build(provider, snapshot_id, None).await
    }

    /// Create a catalog with write support.
    ///
    /// This constructor enables write operations (INSERT INTO, CREATE TABLE AS)
    /// by attaching a metadata writer. The catalog will pass the writer to all
    /// schemas and tables it creates.
    #[cfg(feature = "write")]
    pub async fn with_writer(
        provider: Arc<dyn MetadataProvider>,
        writer: Arc<dyn MetadataWriter>,
    ) -> Result<Self> {
        let snapshot_id = provider.get_current_snapshot().await?;
        Self::build(
            provider,
            snapshot_id,
            Some(WriteConfig {
                writer,
            }),
        )
        .await
    }

    /// Get the metadata provider used by this catalog.
    pub fn provider(&self) -> Arc<dyn MetadataProvider> {
        Arc::clone(&self.provider)
    }

    /// Get the snapshot id this catalog instance is pinned to.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot.snapshot_id()
    }

    pub(crate) fn snapshot(&self) -> Arc<DuckLakeCatalogSnapshot> {
        Arc::clone(&self.snapshot)
    }

    /// Reload the latest snapshot from the same metadata provider.
    pub async fn reload_latest(&self) -> Result<Self> {
        let snapshot_id = self.provider.get_current_snapshot().await?;
        Self::build(
            Arc::clone(&self.provider),
            snapshot_id,
            #[cfg(feature = "write")]
            self.write_config.clone(),
            #[cfg(not(feature = "write"))]
            None,
        )
        .await
    }

    /// Reload a specific snapshot from the same metadata provider.
    pub async fn reload_snapshot(&self, snapshot_id: i64) -> Result<Self> {
        Self::build(
            Arc::clone(&self.provider),
            snapshot_id,
            #[cfg(feature = "write")]
            self.write_config.clone(),
            #[cfg(not(feature = "write"))]
            None,
        )
        .await
    }

    /// Build a fresh catalog snapshot and swap it into the `SessionContext`.
    ///
    /// This also re-registers the DuckLake metadata table functions so they stay
    /// bound to the same snapshot as the catalog.
    pub async fn reload_and_swap(
        ctx: &SessionContext,
        catalog_name: &str,
        current: &DuckLakeCatalog,
    ) -> Result<Arc<DuckLakeCatalog>> {
        let new_catalog = Arc::new(current.reload_latest().await?);
        let catalog_provider: Arc<dyn CatalogProvider> = new_catalog.clone();
        ctx.register_catalog(catalog_name, catalog_provider);
        crate::table_functions::register_ducklake_functions(ctx, new_catalog.clone());
        Ok(new_catalog)
    }

    async fn build(
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
        #[cfg(feature = "write")] write_config: Option<WriteConfig>,
        #[cfg(not(feature = "write"))] _write_config: Option<()>,
    ) -> Result<Self> {
        let snapshot = Arc::new(
            Self::build_snapshot(
                Arc::clone(&provider),
                snapshot_id,
                #[cfg(feature = "write")]
                write_config.as_ref(),
                #[cfg(not(feature = "write"))]
                None,
            )
            .await?,
        );

        Ok(Self {
            provider,
            snapshot,
            #[cfg(feature = "write")]
            write_config,
        })
    }

    async fn build_snapshot(
        provider: Arc<dyn MetadataProvider>,
        snapshot_id: i64,
        #[cfg(feature = "write")] write_config: Option<&WriteConfig>,
        #[cfg(not(feature = "write"))] _write_config: Option<()>,
    ) -> Result<DuckLakeCatalogSnapshot> {
        let data_path = provider.get_data_path().await?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;
        let object_store_url = Arc::new(object_store_url);

        let snapshots = provider.list_snapshots().await?;
        let schema_metadata = provider.list_schemas(snapshot_id).await?;
        let all_tables = provider.list_all_tables(snapshot_id).await?;
        let all_columns = provider.list_all_columns(snapshot_id).await?;
        let all_files = provider.list_all_files(snapshot_id).await?;

        let mut columns_by_table: HashMap<(String, String), Vec<ColumnWithTable>> = HashMap::new();
        for column in &all_columns {
            columns_by_table
                .entry((column.schema_name.clone(), column.table_name.clone()))
                .or_default()
                .push(column.clone());
        }

        let mut files_by_table: HashMap<(String, String), Vec<FileWithTable>> = HashMap::new();
        for file in &all_files {
            files_by_table
                .entry((file.schema_name.clone(), file.table_name.clone()))
                .or_default()
                .push(file.clone());
        }

        let mut schemas: HashMap<String, CachedSchema> = HashMap::new();
        for metadata in &schema_metadata {
            let schema_path =
                resolve_path(&catalog_path, &metadata.path, metadata.path_is_relative)?;
            schemas.insert(
                metadata.schema_name.clone(),
                CachedSchema {
                    metadata: metadata.clone(),
                    schema_path,
                    tables: Arc::new(RwLock::new(HashMap::new())),
                },
            );
        }

        for table in &all_tables {
            let Some(schema) = schemas.get(&table.schema_name) else {
                tracing::warn!(
                    schema_name = %table.schema_name,
                    table_name = %table.table.table_name,
                    "Skipping table whose schema metadata was not loaded"
                );
                continue;
            };

            let table_path = resolve_path(
                &schema.schema_path,
                &table.table.path,
                table.table.path_is_relative,
            )?;
            let table_columns = columns_by_table
                .remove(&(table.schema_name.clone(), table.table.table_name.clone()))
                .unwrap_or_default()
                .into_iter()
                .map(|column| column.column)
                .collect();
            let table_files = files_by_table
                .remove(&(table.schema_name.clone(), table.table.table_name.clone()))
                .unwrap_or_default()
                .into_iter()
                .map(|file| file.file)
                .collect();

            let table_provider = DuckLakeTable::from_loaded_metadata(
                table.table.table_id,
                table.table.table_name.clone(),
                Arc::clone(&object_store_url),
                table_path.clone(),
                table_columns,
                table_files,
            )?;

            #[cfg(feature = "write")]
            let table_provider = if let Some(config) = write_config {
                table_provider.with_writer(table.schema_name.clone(), Arc::clone(&config.writer))
            } else {
                table_provider
            };

            schema
                .tables
                .write()
                .expect("Catalog table cache poisoned")
                .insert(
                    table.table.table_name.clone(),
                    CachedTable {
                        metadata: table.table.clone(),
                        table_path,
                        provider: Arc::new(table_provider),
                    },
                );
        }

        let information_schema = Arc::new(InformationSchemaData::try_new(
            snapshot_id,
            &snapshots,
            &schema_metadata,
            &all_tables,
            &all_columns,
            &all_files,
        )?);

        Ok(DuckLakeCatalogSnapshot {
            snapshot_id,
            object_store_url,
            schemas,
            information_schema,
        })
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.snapshot.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "information_schema" {
            return Some(Arc::new(InformationSchemaProvider::new(
                self.snapshot.information_schema(),
            )));
        }

        let cached = self.snapshot.cached_schema(name)?;
        let schema = DuckLakeSchema::new(
            cached.metadata.schema_name,
            Arc::clone(&cached.tables),
            self.snapshot.object_store_url(),
            cached.schema_path,
        );

        #[cfg(feature = "write")]
        let schema = if let Some(config) = self.write_config.as_ref() {
            schema.with_writer(Arc::clone(&config.writer))
        } else {
            schema
        };

        Some(Arc::new(schema) as Arc<dyn SchemaProvider>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_provider::{
        DataFileChange, DeleteFileChange, DuckLakeFileData, DuckLakeTableColumn, DuckLakeTableFile,
    };
    use arrow::array::{Array, StringArray};
    use datafusion::execution::context::SessionContext;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    #[derive(Debug)]
    struct MockMetadataProvider {
        current_snapshot: AtomicI64,
        on_demand_calls: AtomicUsize,
    }

    impl MockMetadataProvider {
        fn new(snapshot_id: i64) -> Self {
            Self {
                current_snapshot: AtomicI64::new(snapshot_id),
                on_demand_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl MetadataProvider for MockMetadataProvider {
        async fn get_current_snapshot(&self) -> Result<i64> {
            Ok(self.current_snapshot.load(Ordering::Relaxed))
        }

        async fn get_data_path(&self) -> Result<String> {
            Ok("file:///tmp/ducklake".to_string())
        }

        async fn list_snapshots(&self) -> Result<Vec<SnapshotMetadata>> {
            Ok(vec![
                SnapshotMetadata {
                    snapshot_id: 7,
                    timestamp: Some("2026-04-13T00:00:00Z".to_string()),
                },
                SnapshotMetadata {
                    snapshot_id: 8,
                    timestamp: Some("2026-04-13T00:05:00Z".to_string()),
                },
            ])
        }

        async fn list_schemas(&self, _snapshot_id: i64) -> Result<Vec<SchemaMetadata>> {
            Ok(vec![SchemaMetadata {
                schema_id: 1,
                schema_name: "main".to_string(),
                path: "main".to_string(),
                path_is_relative: true,
            }])
        }

        async fn list_tables(
            &self,
            _schema_id: i64,
            _snapshot_id: i64,
        ) -> Result<Vec<TableMetadata>> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn get_table_structure(&self, _table_id: i64) -> Result<Vec<DuckLakeTableColumn>> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn get_table_files_for_select(
            &self,
            _table_id: i64,
            _snapshot_id: i64,
        ) -> Result<Vec<DuckLakeTableFile>> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn get_schema_by_name(
            &self,
            _name: &str,
            _snapshot_id: i64,
        ) -> Result<Option<SchemaMetadata>> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }

        async fn get_table_by_name(
            &self,
            _schema_id: i64,
            _name: &str,
            _snapshot_id: i64,
        ) -> Result<Option<TableMetadata>> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }

        async fn table_exists(
            &self,
            _schema_id: i64,
            _name: &str,
            _snapshot_id: i64,
        ) -> Result<bool> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(false)
        }

        async fn list_all_tables(&self, snapshot_id: i64) -> Result<Vec<TableWithSchema>> {
            let mut tables = vec![TableWithSchema {
                schema_name: "main".to_string(),
                table: TableMetadata {
                    table_id: 11,
                    table_name: "items".to_string(),
                    path: "items".to_string(),
                    path_is_relative: true,
                },
            }];

            if snapshot_id >= 8 {
                tables.push(TableWithSchema {
                    schema_name: "main".to_string(),
                    table: TableMetadata {
                        table_id: 12,
                        table_name: "orders".to_string(),
                        path: "orders".to_string(),
                        path_is_relative: true,
                    },
                });
            }

            Ok(tables)
        }

        async fn list_all_columns(&self, snapshot_id: i64) -> Result<Vec<ColumnWithTable>> {
            let mut columns = vec![ColumnWithTable {
                schema_name: "main".to_string(),
                table_name: "items".to_string(),
                column: DuckLakeTableColumn::new(101, "id".to_string(), "int64".to_string(), false),
            }];

            if snapshot_id >= 8 {
                columns.push(ColumnWithTable {
                    schema_name: "main".to_string(),
                    table_name: "orders".to_string(),
                    column: DuckLakeTableColumn::new(
                        201,
                        "order_id".to_string(),
                        "int64".to_string(),
                        false,
                    ),
                });
            }

            Ok(columns)
        }

        async fn list_all_files(&self, _snapshot_id: i64) -> Result<Vec<FileWithTable>> {
            Ok(vec![FileWithTable {
                schema_name: "main".to_string(),
                table_name: "items".to_string(),
                file: DuckLakeTableFile {
                    file: DuckLakeFileData::new("items.parquet".to_string(), true, 123),
                    delete_file: None,
                    row_id_start: None,
                    snapshot_id: Some(7),
                    max_row_count: Some(1),
                },
            }])
        }

        async fn get_data_files_added_between_snapshots(
            &self,
            _table_id: i64,
            _start_snapshot: i64,
            _end_snapshot: i64,
        ) -> Result<Vec<DataFileChange>> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn get_delete_files_added_between_snapshots(
            &self,
            _table_id: i64,
            _start_snapshot: i64,
            _end_snapshot: i64,
        ) -> Result<Vec<DeleteFileChange>> {
            self.on_demand_calls.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn catalog_lookups_do_not_trigger_provider_round_trips() {
        let provider = Arc::new(MockMetadataProvider::new(7));
        let catalog = DuckLakeCatalog::with_snapshot(provider.clone(), 7)
            .await
            .unwrap();

        assert_eq!(catalog.schema_names(), vec!["information_schema", "main"]);

        let schema = catalog.schema("main").unwrap();
        assert_eq!(schema.table_names(), vec!["items"]);
        assert!(schema.table_exist("items"));
        assert!(schema.table("items").await.unwrap().is_some());

        assert_eq!(provider.on_demand_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn reload_latest_preserves_old_snapshot_and_loads_new_one() {
        let provider = Arc::new(MockMetadataProvider::new(7));
        let catalog = DuckLakeCatalog::with_snapshot(provider.clone(), 7)
            .await
            .unwrap();

        provider.current_snapshot.store(8, Ordering::Relaxed);
        let reloaded = catalog.reload_latest().await.unwrap();

        let old_schema = catalog.schema("main").unwrap();
        let new_schema = reloaded.schema("main").unwrap();

        assert_eq!(catalog.snapshot_id(), 7);
        assert_eq!(reloaded.snapshot_id(), 8);
        assert_eq!(old_schema.table_names(), vec!["items"]);
        assert_eq!(new_schema.table_names(), vec!["items", "orders"]);
    }

    #[tokio::test]
    async fn reload_and_swap_updates_catalog_and_functions() {
        let provider = Arc::new(MockMetadataProvider::new(7));
        let current = Arc::new(
            DuckLakeCatalog::with_snapshot(provider.clone(), 7)
                .await
                .unwrap(),
        );
        let ctx = SessionContext::new();

        ctx.register_catalog("ducklake", current.clone());
        crate::table_functions::register_ducklake_functions(&ctx, current.clone());

        let df = ctx
            .sql("SELECT table_name FROM ducklake_table_info() ORDER BY table_name")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "items");

        provider.current_snapshot.store(8, Ordering::Relaxed);
        let swapped = DuckLakeCatalog::reload_and_swap(&ctx, "ducklake", &current)
            .await
            .unwrap();

        assert_eq!(current.snapshot_id(), 7);
        assert_eq!(swapped.snapshot_id(), 8);

        let registered = ctx.catalog("ducklake").unwrap();
        let registered = registered
            .as_any()
            .downcast_ref::<DuckLakeCatalog>()
            .unwrap();
        assert_eq!(registered.snapshot_id(), 8);

        let df = ctx
            .sql("SELECT table_name FROM ducklake_table_info() ORDER BY table_name")
            .await
            .unwrap();
        let batches = df.collect().await.unwrap();
        let names = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.len(), 2);
        assert_eq!(names.value(0), "items");
        assert_eq!(names.value(1), "orders");
    }
}
