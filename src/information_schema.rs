//! Information schema implementation for DuckLake catalog metadata.
//!
//! All tables are backed by the immutable catalog snapshot loaded at
//! `DuckLakeCatalog` construction time. This keeps metadata introspection
//! consistent with the snapshot used for normal table planning.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemTable;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::ExecutionPlan;

use crate::metadata_provider::{
    ColumnWithTable, FileWithTable, SchemaMetadata, SnapshotMetadata, TableWithSchema,
};

fn snapshots_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("timestamp", DataType::Utf8, true),
    ]))
}

fn schemata_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("schema_id", DataType::Int64, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("path_is_relative", DataType::Boolean, false),
    ]))
}

fn tables_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("snapshot_id", DataType::Int64, false),
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_id", DataType::Int64, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("path_is_relative", DataType::Boolean, false),
    ]))
}

fn columns_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_id", DataType::Int64, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("column_type", DataType::Utf8, false),
    ]))
}

fn table_info_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_id", DataType::Int64, false),
        Field::new("file_count", DataType::Int64, false),
        Field::new("file_size_bytes", DataType::Int64, false),
        Field::new("delete_file_count", DataType::Int64, false),
        Field::new("delete_file_size_bytes", DataType::Int64, false),
    ]))
}

fn files_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_size_bytes", DataType::Int64, false),
        Field::new("record_count", DataType::Int64, true),
        Field::new("has_delete_file", DataType::Boolean, false),
    ]))
}

fn build_batch(schema: SchemaRef, columns: Vec<ArrayRef>) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(schema, columns)
        .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
}

fn build_snapshots_batch(rows: &[SnapshotMetadata]) -> DataFusionResult<RecordBatch> {
    build_batch(
        snapshots_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.snapshot_id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.timestamp.as_deref())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

fn build_schemata_batch(
    snapshot_id: i64,
    rows: &[SchemaMetadata],
) -> DataFusionResult<RecordBatch> {
    build_batch(
        schemata_schema(),
        vec![
            Arc::new(Int64Array::from(vec![snapshot_id; rows.len()])),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.schema_id).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.schema_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.path.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| row.path_is_relative)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

fn build_tables_batch(snapshot_id: i64, rows: &[TableWithSchema]) -> DataFusionResult<RecordBatch> {
    build_batch(
        tables_schema(),
        vec![
            Arc::new(Int64Array::from(vec![snapshot_id; rows.len()])),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.schema_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.table.table_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.table.table_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.table.path.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| row.table.path_is_relative)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

fn build_columns_batch(rows: &[ColumnWithTable]) -> DataFusionResult<RecordBatch> {
    build_batch(
        columns_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.schema_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.table_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.column.column_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.column.column_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.column.column_type.as_str())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

fn build_files_batch(rows: &[FileWithTable]) -> DataFusionResult<RecordBatch> {
    build_batch(
        files_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.schema_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.table_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|row| row.file.file.path.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.file.file.file_size_bytes)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|row| row.file.max_row_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter()
                    .map(|row| row.file.delete_file.is_some())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

fn build_table_info_batch(
    all_tables: &[TableWithSchema],
    all_files: &[FileWithTable],
) -> DataFusionResult<RecordBatch> {
    #[derive(Default, Debug)]
    struct TableStats {
        table_id: i64,
        file_count: i64,
        file_size: i64,
        delete_count: i64,
        delete_size: i64,
    }

    type TableKey = (String, String);

    let mut table_stats: HashMap<TableKey, TableStats> = HashMap::new();
    for table in all_tables {
        table_stats.insert(
            (table.schema_name.clone(), table.table.table_name.clone()),
            TableStats {
                table_id: table.table.table_id,
                ..Default::default()
            },
        );
    }

    for file in all_files {
        let entry = table_stats
            .entry((file.schema_name.clone(), file.table_name.clone()))
            .or_default();
        entry.file_count += 1;
        entry.file_size += file.file.file.file_size_bytes;
        if let Some(delete_file) = file.file.delete_file.as_ref() {
            entry.delete_count += 1;
            entry.delete_size += delete_file.file_size_bytes;
        }
    }

    let mut rows: Vec<_> = table_stats.into_iter().collect();
    rows.sort_by(|left, right| {
        left.0
            .0
            .cmp(&right.0.0)
            .then_with(|| left.0.1.cmp(&right.0.1))
    });

    build_batch(
        table_info_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|((schema_name, _), _)| schema_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|((_, table_name), _)| table_name.as_str())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, stats)| stats.table_id)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, stats)| stats.file_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, stats)| stats.file_size)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, stats)| stats.delete_count)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|(_, stats)| stats.delete_size)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
}

#[derive(Debug, Clone)]
pub(crate) struct InformationSchemaData {
    snapshots: RecordBatch,
    schemata: RecordBatch,
    tables: RecordBatch,
    columns: RecordBatch,
    table_info: RecordBatch,
    files: RecordBatch,
}

impl InformationSchemaData {
    pub fn try_new(
        current_snapshot_id: i64,
        snapshots: &[SnapshotMetadata],
        schemata: &[SchemaMetadata],
        tables: &[TableWithSchema],
        columns: &[ColumnWithTable],
        files: &[FileWithTable],
    ) -> DataFusionResult<Self> {
        Ok(Self {
            snapshots: build_snapshots_batch(snapshots)?,
            schemata: build_schemata_batch(current_snapshot_id, schemata)?,
            tables: build_tables_batch(current_snapshot_id, tables)?,
            columns: build_columns_batch(columns)?,
            table_info: build_table_info_batch(tables, files)?,
            files: build_files_batch(files)?,
        })
    }

    pub(crate) fn snapshots_table(&self) -> SnapshotsTable {
        SnapshotsTable::new(self.snapshots.clone())
    }

    pub(crate) fn schemata_table(&self) -> SchemataTable {
        SchemataTable::new(self.schemata.clone())
    }

    pub(crate) fn tables_table(&self) -> TablesTable {
        TablesTable::new(self.tables.clone())
    }

    pub(crate) fn columns_table(&self) -> ColumnsTable {
        ColumnsTable::new(self.columns.clone())
    }

    pub(crate) fn table_info_table(&self) -> TableInfoTable {
        TableInfoTable::new(self.table_info.clone())
    }

    pub(crate) fn files_table(&self) -> FilesTable {
        FilesTable::new(self.files.clone())
    }
}

#[derive(Debug, Clone)]
struct StaticBatchTable {
    batch: RecordBatch,
}

impl StaticBatchTable {
    fn new(batch: RecordBatch) -> Self {
        Self {
            batch,
        }
    }

    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::prelude::Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mem_table = MemTable::try_new(self.schema(), vec![vec![self.batch.clone()]])?;
        mem_table.scan(state, projection, filters, limit).await
    }
}

macro_rules! static_batch_table {
    ($name:ident) => {
        #[derive(Debug, Clone)]
        pub struct $name {
            inner: StaticBatchTable,
        }

        impl $name {
            pub fn new(batch: RecordBatch) -> Self {
                Self {
                    inner: StaticBatchTable::new(batch),
                }
            }
        }

        #[async_trait::async_trait]
        impl TableProvider for $name {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn schema(&self) -> SchemaRef {
                self.inner.schema()
            }

            fn table_type(&self) -> TableType {
                TableType::View
            }

            async fn scan(
                &self,
                state: &dyn Session,
                projection: Option<&Vec<usize>>,
                filters: &[datafusion::prelude::Expr],
                limit: Option<usize>,
            ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
                self.inner.scan(state, projection, filters, limit).await
            }
        }
    };
}

static_batch_table!(SnapshotsTable);
static_batch_table!(SchemataTable);
static_batch_table!(TablesTable);
static_batch_table!(ColumnsTable);
static_batch_table!(TableInfoTable);
static_batch_table!(FilesTable);

/// Schema provider for `information_schema`.
#[derive(Debug, Clone)]
pub(crate) struct InformationSchemaProvider {
    data: Arc<InformationSchemaData>,
}

impl InformationSchemaProvider {
    pub fn new(data: Arc<InformationSchemaData>) -> Self {
        Self {
            data,
        }
    }
}

#[async_trait::async_trait]
impl SchemaProvider for InformationSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        vec![
            "snapshots".to_string(),
            "schemata".to_string(),
            "tables".to_string(),
            "table_info".to_string(),
            "columns".to_string(),
            "files".to_string(),
        ]
    }

    async fn table(&self, name: &str) -> DataFusionResult<Option<Arc<dyn TableProvider>>> {
        let provider: Option<Arc<dyn TableProvider>> = match name {
            "snapshots" => Some(Arc::new(self.data.snapshots_table())),
            "schemata" => Some(Arc::new(self.data.schemata_table())),
            "tables" => Some(Arc::new(self.data.tables_table())),
            "table_info" => Some(Arc::new(self.data.table_info_table())),
            "columns" => Some(Arc::new(self.data.columns_table())),
            "files" => Some(Arc::new(self.data.files_table())),
            _ => None,
        };
        Ok(provider)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.table_names()
            .iter()
            .any(|table_name| table_name == name)
    }
}
