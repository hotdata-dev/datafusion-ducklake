//! User-Defined Table Functions (UDTFs) for DuckLake catalog metadata

use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::common::{Result as DataFusionResult, ScalarValue, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::Expr;

use crate::catalog::{DuckLakeCatalog, DuckLakeCatalogSnapshot};
use crate::metadata_provider::MetadataProvider;
use crate::table_changes::TableChangesTable;
use crate::table_deletions::TableDeletionsTable;

#[derive(Debug)]
pub struct DucklakeSnapshotsFunction {
    snapshot: Arc<DuckLakeCatalogSnapshot>,
}

impl DucklakeSnapshotsFunction {
    pub(crate) fn new(snapshot: Arc<DuckLakeCatalogSnapshot>) -> Self {
        Self {
            snapshot,
        }
    }
}

impl TableFunctionImpl for DucklakeSnapshotsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_snapshots() takes no arguments");
        }

        Ok(Arc::new(
            self.snapshot.information_schema().snapshots_table(),
        ))
    }
}

#[derive(Debug)]
pub struct DucklakeTableInfoFunction {
    snapshot: Arc<DuckLakeCatalogSnapshot>,
}

impl DucklakeTableInfoFunction {
    pub(crate) fn new(snapshot: Arc<DuckLakeCatalogSnapshot>) -> Self {
        Self {
            snapshot,
        }
    }
}

impl TableFunctionImpl for DucklakeTableInfoFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_table_info() takes no arguments");
        }

        Ok(Arc::new(
            self.snapshot.information_schema().table_info_table(),
        ))
    }
}

#[derive(Debug)]
pub struct DucklakeListFilesFunction {
    snapshot: Arc<DuckLakeCatalogSnapshot>,
}

impl DucklakeListFilesFunction {
    pub(crate) fn new(snapshot: Arc<DuckLakeCatalogSnapshot>) -> Self {
        Self {
            snapshot,
        }
    }
}

impl TableFunctionImpl for DucklakeListFilesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("ducklake_list_files() takes no arguments");
        }

        Ok(Arc::new(self.snapshot.information_schema().files_table()))
    }
}

#[derive(Debug)]
pub struct DucklakeTableChangesFunction {
    provider: Arc<dyn MetadataProvider>,
    snapshot: Arc<DuckLakeCatalogSnapshot>,
}

impl DucklakeTableChangesFunction {
    pub(crate) fn new(
        provider: Arc<dyn MetadataProvider>,
        snapshot: Arc<DuckLakeCatalogSnapshot>,
    ) -> Self {
        Self {
            provider,
            snapshot,
        }
    }

    fn parse_table_name(table_name: &str) -> (&str, &str) {
        if let Some(dot_pos) = table_name.find('.') {
            let schema = &table_name[..dot_pos];
            let table = &table_name[dot_pos + 1..];
            (schema, table)
        } else {
            ("main", table_name)
        }
    }

    fn build_provider(
        &self,
        table_name: String,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (schema_name, table_name_only) = Self::parse_table_name(&table_name);
        let table = self
            .snapshot
            .cached_table(schema_name, table_name_only)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Table not found in catalog snapshot: {schema_name}.{table_name_only}"
                ))
            })?;

        Ok(Arc::new(TableChangesTable::new(
            Arc::clone(&self.provider),
            table.metadata.table_id,
            start_snapshot,
            end_snapshot,
            self.snapshot.object_store_url(),
            table.table_path,
            table.provider.schema(),
        )))
    }
}

impl TableFunctionImpl for DucklakeTableChangesFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!(
                "ducklake_table_changes() requires 3 arguments: \
                 ducklake_table_changes('schema.table', start_snapshot, end_snapshot)"
            );
        }

        let table_name = match &exprs[0] {
            Expr::Literal(ScalarValue::Utf8(Some(name)), _) => name.clone(),
            _ => {
                return plan_err!(
                    "First argument to ducklake_table_changes() must be a string literal \
                     (e.g., 'main.users' or 'users')"
                );
            },
        };

        let start_snapshot = match &exprs[1] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
            _ => {
                return plan_err!(
                    "Second argument to ducklake_table_changes() must be an integer (start_snapshot)"
                );
            },
        };

        let end_snapshot = match &exprs[2] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
            _ => {
                return plan_err!(
                    "Third argument to ducklake_table_changes() must be an integer (end_snapshot)"
                );
            },
        };

        if start_snapshot > end_snapshot {
            return plan_err!(
                "start_snapshot ({}) must be less than or equal to end_snapshot ({})",
                start_snapshot,
                end_snapshot
            );
        }

        self.build_provider(table_name, start_snapshot, end_snapshot)
    }
}

#[derive(Debug)]
pub struct DucklakeTableDeletionsFunction {
    provider: Arc<dyn MetadataProvider>,
    snapshot: Arc<DuckLakeCatalogSnapshot>,
}

impl DucklakeTableDeletionsFunction {
    pub(crate) fn new(
        provider: Arc<dyn MetadataProvider>,
        snapshot: Arc<DuckLakeCatalogSnapshot>,
    ) -> Self {
        Self {
            provider,
            snapshot,
        }
    }

    fn parse_table_name(table_name: &str) -> (&str, &str) {
        if let Some(dot_pos) = table_name.find('.') {
            let schema = &table_name[..dot_pos];
            let table = &table_name[dot_pos + 1..];
            (schema, table)
        } else {
            ("main", table_name)
        }
    }

    fn build_provider(
        &self,
        table_name: String,
        start_snapshot: i64,
        end_snapshot: i64,
    ) -> DataFusionResult<Arc<dyn TableProvider>> {
        let (schema_name, table_name_only) = Self::parse_table_name(&table_name);
        let table = self
            .snapshot
            .cached_table(schema_name, table_name_only)
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Plan(format!(
                    "Table not found in catalog snapshot: {schema_name}.{table_name_only}"
                ))
            })?;

        Ok(Arc::new(TableDeletionsTable::new(
            Arc::clone(&self.provider),
            table.metadata.table_id,
            start_snapshot,
            end_snapshot,
            self.snapshot.object_store_url(),
            table.table_path,
            table.provider.schema(),
        )))
    }
}

impl TableFunctionImpl for DucklakeTableDeletionsFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() != 3 {
            return plan_err!(
                "ducklake_table_deletions() requires 3 arguments: \
                 ducklake_table_deletions('schema.table', start_snapshot, end_snapshot)"
            );
        }

        let table_name = match &exprs[0] {
            Expr::Literal(ScalarValue::Utf8(Some(name)), _) => name.clone(),
            _ => {
                return plan_err!(
                    "First argument to ducklake_table_deletions() must be a string literal \
                     (e.g., 'main.users' or 'users')"
                );
            },
        };

        let start_snapshot = match &exprs[1] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
            _ => {
                return plan_err!(
                    "Second argument to ducklake_table_deletions() must be an integer (start_snapshot)"
                );
            },
        };

        let end_snapshot = match &exprs[2] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) => *v,
            Expr::Literal(ScalarValue::Int32(Some(v)), _) => *v as i64,
            _ => {
                return plan_err!(
                    "Third argument to ducklake_table_deletions() must be an integer (end_snapshot)"
                );
            },
        };

        if start_snapshot > end_snapshot {
            return plan_err!(
                "start_snapshot ({}) must be less than or equal to end_snapshot ({})",
                start_snapshot,
                end_snapshot
            );
        }

        self.build_provider(table_name, start_snapshot, end_snapshot)
    }
}

/// Registers all ducklake_*() table functions with a SessionContext.
pub fn register_ducklake_functions(
    ctx: &datafusion::execution::context::SessionContext,
    catalog: Arc<DuckLakeCatalog>,
) {
    let provider = catalog.provider();
    let snapshot = catalog.snapshot();

    ctx.register_udtf(
        "ducklake_snapshots",
        Arc::new(DucklakeSnapshotsFunction::new(snapshot.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_info",
        Arc::new(DucklakeTableInfoFunction::new(snapshot.clone())),
    );
    ctx.register_udtf(
        "ducklake_list_files",
        Arc::new(DucklakeListFilesFunction::new(snapshot.clone())),
    );
    ctx.register_udtf(
        "ducklake_table_changes",
        Arc::new(DucklakeTableChangesFunction::new(
            Arc::clone(&provider),
            snapshot.clone(),
        )),
    );
    ctx.register_udtf(
        "ducklake_table_deletions",
        Arc::new(DucklakeTableDeletionsFunction::new(provider, snapshot)),
    );
}
