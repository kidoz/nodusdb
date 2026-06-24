//! Virtual-table dispatch: routes a (db, schema, table) to the matching synthesized view.
use crate::{MemExecutor, Value, parse_object_name};
use anyhow::Result;
use chrono::Utc;
use nodus_catalog::ColumnDescriptor;

impl MemExecutor {
    pub(crate) fn get_virtual_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_only: &str,
    ) -> Result<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)> {
        if schema_name.eq_ignore_ascii_case("pg_catalog") {
            if let Some(table) = self.pg_catalog_virtual_table(db_name, table_only)? {
                return Ok(table);
            }
            anyhow::bail!("relation \"pg_catalog.{}\" does not exist", table_only);
        } else if schema_name.eq_ignore_ascii_case("information_schema") {
            if let Some(table) = self.information_schema_virtual_table(db_name, table_only)? {
                return Ok(table);
            }
            anyhow::bail!(
                "relation \"information_schema.{}\" does not exist",
                table_only
            );
        } else {
            anyhow::bail!("relation \"{}.{}\" does not exist", schema_name, table_only);
        }
    }
}
