use crate::{DuckLakeSchema, DuckLakeTable};
use crate::metadata_provider::{DuckLakeTableColumn, DuckLakeTableFile, MetadataProvider};

pub struct DuckdbMetadataProvider {}

impl MetadataProvider for DuckdbMetadataProvider {
    fn get_current_snapshot() -> crate::Result<i64> {
        todo!()
    }

    fn list_schemas() -> crate::Result<Vec<DuckLakeSchema>> {
        todo!()
    }

    fn list_tables() -> crate::Result<Vec<DuckLakeTable>> {
        todo!()
    }

    fn get_table_structure(table_id: i64) -> crate::Result<Vec<DuckLakeTableColumn>> {
        todo!()
    }

    fn get_table_files_for_select(table_id: i64) -> crate::Result<Vec<DuckLakeTableFile>> {
        todo!()
    }
}