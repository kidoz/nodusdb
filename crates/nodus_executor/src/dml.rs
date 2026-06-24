//! Data manipulation statements: INSERT / UPDATE / DELETE — row encoding,
//! unique/table constraint checks, index maintenance, and RETURNING output.

use crate::aggregates::*;
use crate::*;
use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_catalog::{ColumnDescriptor, DescriptorState};
use nodus_storage_api::{KeyRange, KvEngine};

impl MemExecutor {
    pub(crate) fn exec_insert(
        &self,
        ctx: &ExecutionContext,
        table_name: String,
        columns: Vec<String>,
        values_list: Vec<Vec<Value>>,
        returning: Vec<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Insert, ResourceRef::Table(tbl.id))?;

        let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
        let mut inserted_count = 0;
        let mut returning_rows = Vec::new();

        for values in values_list {
            // Build the Values row in table-column order...
            let mut raw = vec![Value::Null; col_names.len()];
            if columns.is_empty() {
                for (i, v) in values.iter().enumerate() {
                    if i < raw.len() {
                        raw[i] = v.clone();
                    }
                }
            } else {
                for (cname, val) in columns.iter().zip(values.iter()) {
                    if let Some(idx) = col_names.iter().position(|c| c == cname) {
                        raw[idx] = val.clone();
                    }
                }
            }
            // ...then coerce each cell to its column's type if it's Text, otherwise assume it's correctly bound.
            let mut row = Vec::new();
            for (i, c) in tbl.columns.iter().enumerate() {
                let val = match &raw[i] {
                    Value::Text(s) => coerce(s, column_type(&c.data_type)),
                    other => other.clone(),
                };
                if !c.nullable && val == Value::Null {
                    anyhow::bail!("Column {} cannot be NULL", c.name);
                }
                row.push(val);
            }

            self.check_unique_constraints(&ctx.session_id, &tbl, &row, None)?;
            self.check_table_constraints(
                ctx,
                &tbl,
                &row,
                &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )?;

            // Primary key = first column's rendered value.
            let pk = row.first().map(render).unwrap_or_default();
            let key = format!("{}:{}", tbl.id, pk);
            let encoded = serde_json::to_string(&row)?;
            self.write_row(&ctx.session_id, key, encoded)?;

            // Maintain secondary indexes.
            for idx in &tbl.indexes {
                for kcol in &idx.key_columns {
                    if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                        let index_val = row.get(pos).unwrap_or(&Value::Null);
                        self.write_index_entry(&ctx.session_id, idx.id, index_val, &pk)?;
                    }
                }
            }

            inserted_count += 1;
            if !returning.is_empty() {
                returning_rows.push(row);
            }
        }

        if returning.is_empty() {
            Ok(QueryOutput::tag(&format!("INSERT 0 {}", inserted_count)))
        } else {
            let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
            let indices: Vec<Option<usize>> = returning
                .iter()
                .map(|c| {
                    col_names
                        .iter()
                        .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                })
                .collect();
            let rows = returning_rows
                .into_iter()
                .map(|r| Row {
                    values: indices
                        .iter()
                        .map(|i| i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null))
                        .collect(),
                })
                .collect();
            Ok(QueryOutput {
                tag: format!("INSERT 0 {}", inserted_count),
                columns: returning.clone(),
                types: Self::returning_types(&tbl.columns, &returning),
                rows,
            })
        }
    }
    pub(crate) fn exec_update(
        &self,
        ctx: &ExecutionContext,
        table_name: String,
        assignments: Vec<(String, Value)>,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Update, ResourceRef::Table(tbl.id))?;
        let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();

        let mut updated = 0;
        let mut returning_rows = Vec::new();
        for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
            if !self.row_matches(ctx, &row, &tbl.columns, filter.as_ref()) {
                continue;
            }
            let old_row = row.clone();
            let old_pk_str = old_row.first().map(render).unwrap_or_default();
            let old_key = format!("{}:{}", tbl.id, old_pk_str);
            for (col, val) in &assignments {
                if let Some(idx) = col_names.iter().position(|c| c == col) {
                    let coerced = match val {
                        Value::Text(s) => coerce(s, column_type(&tbl.columns[idx].data_type)),
                        other => other.clone(),
                    };
                    if !tbl.columns[idx].nullable && coerced == Value::Null {
                        anyhow::bail!("Column {} cannot be NULL", col);
                    }
                    row[idx] = coerced;
                }
            }

            let pk_str = row.first().map(render).unwrap_or_default();
            self.check_unique_constraints(&ctx.session_id, &tbl, &row, Some(&pk_str))?;
            self.check_table_constraints(
                ctx,
                &tbl,
                &row,
                &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )?;

            let new_key = format!("{}:{}", tbl.id, pk_str);
            self.write_row(
                &ctx.session_id,
                new_key.clone(),
                serde_json::to_string(&row)?,
            )?;
            if new_key != old_key {
                self.delete_row(&ctx.session_id, old_key)?;
            }

            // Maintain secondary indexes.
            for idx in &tbl.indexes {
                for kcol in &idx.key_columns {
                    if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                        let old_index_val = old_row.get(pos).unwrap_or(&Value::Null);
                        let new_index_val = row.get(pos).unwrap_or(&Value::Null);
                        if old_index_val != new_index_val || old_pk_str != pk_str {
                            self.delete_index_entry(
                                &ctx.session_id,
                                idx.id,
                                old_index_val,
                                &old_pk_str,
                            )?;
                            self.write_index_entry(
                                &ctx.session_id,
                                idx.id,
                                new_index_val,
                                &pk_str,
                            )?;
                        }
                    }
                }
            }

            updated += 1;
            if !returning.is_empty() {
                returning_rows.push(row);
            }
        }
        if returning.is_empty() {
            Ok(QueryOutput::tag(&format!("UPDATE {updated}")))
        } else {
            let indices: Vec<Option<usize>> = returning
                .iter()
                .map(|c| {
                    col_names
                        .iter()
                        .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                })
                .collect();
            let rows = returning_rows
                .into_iter()
                .map(|r| Row {
                    values: indices
                        .iter()
                        .map(|i| i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null))
                        .collect(),
                })
                .collect();
            Ok(QueryOutput {
                tag: format!("UPDATE {updated}"),
                columns: returning.clone(),
                types: Self::returning_types(&tbl.columns, &returning),
                rows,
            })
        }
    }
    pub(crate) fn exec_delete(
        &self,
        ctx: &ExecutionContext,
        table_name: String,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;

        let mut deleted = 0;
        let mut returning_rows = Vec::new();
        for row in self.scan_rows(tbl.id, &ctx.session_id)? {
            if !self.row_matches(ctx, &row, &tbl.columns, filter.as_ref()) {
                continue;
            }
            let pk_str = row.first().map(render).unwrap_or_default();
            let key = format!("{}:{}", tbl.id, pk_str);
            self.delete_row(&ctx.session_id, key)?;

            // Maintain secondary indexes.
            for idx in &tbl.indexes {
                for kcol in &idx.key_columns {
                    if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                        let index_val = row.get(pos).unwrap_or(&Value::Null);
                        self.delete_index_entry(&ctx.session_id, idx.id, index_val, &pk_str)?;
                    }
                }
            }

            deleted += 1;
            if !returning.is_empty() {
                returning_rows.push(row);
            }
        }
        if returning.is_empty() {
            Ok(QueryOutput::tag(&format!("DELETE {deleted}")))
        } else {
            let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
            let indices: Vec<Option<usize>> = returning
                .iter()
                .map(|c| {
                    col_names
                        .iter()
                        .position(|&tc| tc == c || tc.ends_with(&format!(".{}", c)))
                })
                .collect();
            let rows = returning_rows
                .into_iter()
                .map(|r| Row {
                    values: indices
                        .iter()
                        .map(|i| i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null))
                        .collect(),
                })
                .collect();
            Ok(QueryOutput {
                tag: format!("DELETE {deleted}"),
                columns: returning.clone(),
                types: Self::returning_types(&tbl.columns, &returning),
                rows,
            })
        }
    }
}
