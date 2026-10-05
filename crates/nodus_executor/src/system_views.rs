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
        session: &str,
    ) -> Result<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)> {
        if schema_name.eq_ignore_ascii_case("pg_catalog") {
            if let Some(table) = self.pg_catalog_virtual_table(db_name, table_only, session)? {
                return Ok(padded(table));
            }
            anyhow::bail!("relation \"pg_catalog.{}\" does not exist", table_only);
        } else if schema_name.eq_ignore_ascii_case("information_schema") {
            if let Some(table) = self.information_schema_virtual_table(db_name, table_only)? {
                return Ok(padded(table));
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

/// A virtual table's rows, each extended to one value per column: a value a
/// row leaves out reads NULL, as an unmodelled catalog column does.
fn padded(
    (cols, rows): (Vec<ColumnDescriptor>, Vec<Vec<Value>>),
) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
    let mut rows = rows;
    for row in &mut rows {
        while row.len() < cols.len() {
            row.push(Value::Null);
        }
    }
    (cols, rows)
}
