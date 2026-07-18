//! Data manipulation statements: INSERT / UPDATE / DELETE — row encoding,
//! unique/table constraint checks, index maintenance, and RETURNING output.

use crate::aggregates::*;
use crate::*;
use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_catalog::{ColumnDescriptor, DescriptorState};
use nodus_storage_api::{KeyRange, KvEngine};

/// A synthetic rowid for index-less tables: a nanosecond timestamp plus a
/// process-monotonic counter, zero-padded so lexical order equals insertion
/// order (scans stay in insertion order). Unique within a run via the counter,
/// and collision-free across restarts because the timestamp advances. Generated
/// on the leader, whose resulting KV write raft_kv replicates, so it's
/// deterministic across replicas.
fn synthetic_rowid() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:039}-{seq:020}")
}

impl MemExecutor {
    /// Positions (in table-column order) of the columns that form the table's
    /// declared `PRIMARY KEY`. Falls back to the first column when no primary
    /// index is present (e.g. a PK-less table), preserving the legacy rowid so
    /// existing data stays addressable.
    pub(crate) fn pk_positions(tbl: &nodus_catalog::TableDescriptor) -> Vec<usize> {
        // A composite PRIMARY KEY is modeled as one Primary index per column, so
        // gather key columns from every primary index, then order them by their
        // table-column position for a deterministic composite key.
        let mut positions: Vec<usize> = tbl
            .indexes
            .iter()
            .filter(|i| i.index_type == nodus_catalog::IndexType::Primary)
            .flat_map(|i| i.key_columns.iter())
            .filter_map(|kc| tbl.columns.iter().position(|c| c.id == kc.column_id))
            .collect();
        positions.sort_unstable();
        positions.dedup();
        if positions.is_empty() {
            // PK-less table: key by the whole row so rows sharing a first-column
            // value don't collide (PostgreSQL allows duplicate rows). The key is
            // content-derived, so it stays deterministic across Raft replicas.
            // Caveat: exact-duplicate rows still collide (the KV layer has no
            // physical tuple identity), and pre-existing PK-less data written by
            // an older binary (first-column keys) must be re-imported.
            (0..tbl.columns.len()).collect()
        } else {
            positions
        }
    }

    /// A table with no indexes at all (no PRIMARY KEY, UNIQUE, or secondary
    /// index) has no natural row identity, so each row gets a synthetic rowid
    /// key — letting it hold exact-duplicate rows (PostgreSQL heap semantics).
    /// Tables with any index keep content-derived keys, and the index-scan
    /// overlay-merge path (which re-derives keys from content) is only reachable
    /// when an index exists, so it never sees a synthetic-rowid table.
    pub(crate) fn uses_synthetic_rowid(tbl: &nodus_catalog::TableDescriptor) -> bool {
        tbl.indexes.is_empty()
    }

    /// Renders a row's primary-key string from the given column positions. A
    /// single-column key renders to exactly that column's value — identical to
    /// the legacy `render(first column)` encoding — while a composite key joins
    /// its parts with a `\u{1}` separator that cannot occur at a column
    /// boundary, so distinct keys never collide.
    pub(crate) fn row_pk(positions: &[usize], row: &[Value]) -> String {
        if let [pos] = positions {
            return row.get(*pos).map(render).unwrap_or_default();
        }
        positions
            .iter()
            .map(|&p| row.get(p).map(render).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\u{1}")
    }

    pub(crate) fn exec_insert(
        &self,
        ctx: &ExecutionContext,
        table_name: String,
        columns: Vec<String>,
        values_list: Vec<Vec<Value>>,
        returning: Vec<String>,
        on_conflict: Option<crate::plan_types::OnConflictClause>,
        default_cells: Vec<Vec<bool>>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Insert, ResourceRef::Table(tbl.id))?;

        let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
        let col_names_owned: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let pk_positions = Self::pk_positions(&tbl);
        let mut inserted_count = 0;
        let mut returning_rows = Vec::new();

        for (row_idx, values) in values_list.iter().enumerate() {
            // Build the Values row in table-column order, tracking which
            // columns actually received a value (an explicit `DEFAULT` cell
            // counts as not provided).
            let defaults_mask = default_cells.get(row_idx);
            let is_default_cell =
                |j: usize| defaults_mask.is_some_and(|m| m.get(j).copied().unwrap_or(false));
            let mut raw = vec![Value::Null; col_names.len()];
            let mut provided = vec![false; col_names.len()];
            if columns.is_empty() {
                for (i, v) in values.iter().enumerate() {
                    if i < raw.len() && !is_default_cell(i) {
                        raw[i] = v.clone();
                        provided[i] = true;
                    }
                }
            } else {
                for (j, (cname, val)) in columns.iter().zip(values.iter()).enumerate() {
                    if let Some(idx) = col_names.iter().position(|c| c == cname) {
                        if !is_default_cell(j) {
                            raw[idx] = val.clone();
                            provided[idx] = true;
                        }
                    }
                }
            }
            // Unprovided columns take their declared DEFAULT, if any.
            for (i, c) in tbl.columns.iter().enumerate() {
                if !provided[i] {
                    if let Some(json) = &c.default_expr {
                        if let Ok(expr) =
                            serde_json::from_str::<crate::plan_types::ScalarExpr>(json)
                        {
                            raw[i] = eval_scalar_expr(&expr, &[], &[]);
                        }
                    }
                }
            }
            // ...then coerce each cell to its column's type if it's Text, otherwise assume it's correctly bound.
            let mut row = Vec::new();
            for (i, c) in tbl.columns.iter().enumerate() {
                let val = crate::value::coerce_for_column(&raw[i], &c.data_type);
                if !c.nullable && val == Value::Null {
                    anyhow::bail!("Column {} cannot be NULL", c.name);
                }
                row.push(val);
            }

            // ON CONFLICT: for a keyed table, look up the target key before we
            // reach the unique-constraint check (which would raise a violation).
            // A conflict is resolved by skipping the row (DO NOTHING) or updating
            // the existing row in place (DO UPDATE). Synthetic-rowid tables have
            // no unique key, so they never conflict.
            if let Some(clause) = &on_conflict {
                if !Self::uses_synthetic_rowid(&tbl) {
                    let target_pk = Self::row_pk(&pk_positions, &row);
                    let target_key = format!("{}:{}", tbl.id, target_pk);
                    let existing = self
                        .scan_rows_keyed(tbl.id, &ctx.session_id)?
                        .into_iter()
                        .find(|(k, _)| k == &target_key);
                    if let Some((existing_key, mut existing_row)) = existing {
                        match clause {
                            crate::plan_types::OnConflictClause::DoNothing => continue,
                            crate::plan_types::OnConflictClause::DoUpdate(assignments) => {
                                let old_row = existing_row.clone();
                                for (col, expr) in assignments {
                                    if let Some(idx) = col_names.iter().position(|c| c == col) {
                                        let val =
                                            eval_scalar_expr(expr, &old_row, &col_names_owned);
                                        let coerced = crate::value::coerce_for_column(
                                            &val,
                                            &tbl.columns[idx].data_type,
                                        );
                                        if !tbl.columns[idx].nullable && coerced == Value::Null {
                                            anyhow::bail!("Column {} cannot be NULL", col);
                                        }
                                        existing_row[idx] = coerced;
                                    }
                                }
                                self.write_row(
                                    &ctx.session_id,
                                    existing_key,
                                    serde_json::to_string(&existing_row)?,
                                )?;
                                // Maintain secondary indexes for changed key columns.
                                for idx in &tbl.indexes {
                                    for kcol in &idx.key_columns {
                                        if let Some(pos) = tbl
                                            .columns
                                            .iter()
                                            .position(|c| c.id == kcol.column_id)
                                        {
                                            let old_v = old_row.get(pos).unwrap_or(&Value::Null);
                                            let new_v =
                                                existing_row.get(pos).unwrap_or(&Value::Null);
                                            if old_v != new_v {
                                                self.delete_index_entry(
                                                    &ctx.session_id,
                                                    idx.id,
                                                    old_v,
                                                    &target_pk,
                                                )?;
                                                self.write_index_entry(
                                                    &ctx.session_id,
                                                    idx.id,
                                                    new_v,
                                                    &target_pk,
                                                )?;
                                            }
                                        }
                                    }
                                }
                                inserted_count += 1;
                                if !returning.is_empty() {
                                    returning_rows.push(existing_row);
                                }
                                continue;
                            }
                        }
                    }
                }
            }

            self.check_unique_constraints(&ctx.session_id, &tbl, &row, None)?;
            self.check_table_constraints(
                ctx,
                &tbl,
                &row,
                &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )?;

            // Key: declared PRIMARY KEY / full-row content, or a synthetic rowid
            // for an index-less table (so exact-duplicate rows don't collide).
            // Exec-time uuid is replication-safe: raft_kv replicates the
            // resulting KV write, so the leader's key is what every replica sees.
            let pk = if Self::uses_synthetic_rowid(&tbl) {
                synthetic_rowid()
            } else {
                Self::row_pk(&pk_positions, &row)
            };
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
        assignments: Vec<(String, ScalarExpr)>,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Update, ResourceRef::Table(tbl.id))?;
        let col_names: Vec<&str> = tbl.columns.iter().map(|c| c.name.as_str()).collect();
        let col_names_owned: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let pk_positions = Self::pk_positions(&tbl);
        let key_prefix = format!("{}:", tbl.id);

        let mut updated = 0;
        let mut returning_rows = Vec::new();
        // Two-phase: pick the matching rows before mutating, so a subquery in
        // the filter evaluates against the pre-statement state.
        let targets: Vec<(String, Vec<Value>)> = self
            .scan_rows_keyed(tbl.id, &ctx.session_id)?
            .into_iter()
            .filter(|(_, row)| self.row_matches(ctx, row, &tbl.columns, filter.as_ref()))
            .collect();
        for (old_key, mut row) in targets {
            let old_row = row.clone();
            // The row's actual stored key (any scheme); the new key is derived
            // from the updated content, migrating old-scheme rows on write.
            let old_pk_str = old_key.strip_prefix(&key_prefix).unwrap_or(&old_key).to_string();
            for (col, expr) in &assignments {
                if let Some(idx) = col_names.iter().position(|c| c == col) {
                    // `SET col = DEFAULT` sentinel: resolve to the column's
                    // declared default (NULL when there is none).
                    let val = if matches!(expr,
                        ScalarExpr::Function { name, args } if name == "__COLUMN_DEFAULT__" && args.is_empty())
                    {
                        tbl.columns[idx]
                            .default_expr
                            .as_ref()
                            .and_then(|json| {
                                serde_json::from_str::<ScalarExpr>(json).ok()
                            })
                            .map(|e| eval_scalar_expr(&e, &[], &[]))
                            .unwrap_or(Value::Null)
                    } else {
                        // Evaluate the RHS against the row's OLD values.
                        eval_scalar_expr(expr, &old_row, &col_names_owned)
                    };
                    let coerced =
                        crate::value::coerce_for_column(&val, &tbl.columns[idx].data_type);
                    if !tbl.columns[idx].nullable && coerced == Value::Null {
                        anyhow::bail!("Column {} cannot be NULL", col);
                    }
                    row[idx] = coerced;
                }
            }

            // A synthetic rowid is the row's stable identity — keep it across the
            // update rather than re-deriving a key from the (changed) content.
            let pk_str = if Self::uses_synthetic_rowid(&tbl) {
                old_pk_str.clone()
            } else {
                Self::row_pk(&pk_positions, &row)
            };
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

        let key_prefix = format!("{}:", tbl.id);
        let mut deleted = 0;
        let mut returning_rows = Vec::new();
        // Two-phase: decide WHICH rows match before mutating anything, so a
        // subquery in the filter (e.g. `WHERE a = (SELECT max(a) ...)`) sees
        // the pre-statement state rather than partially-deleted data.
        let victims: Vec<(String, Vec<Value>)> = self
            .scan_rows_keyed(tbl.id, &ctx.session_id)?
            .into_iter()
            .filter(|(_, row)| self.row_matches(ctx, row, &tbl.columns, filter.as_ref()))
            .collect();
        for (key, row) in victims {
            // Use the row's actual stored key (works for any key scheme), and
            // derive the index-entry suffix from it.
            let pk_str = key.strip_prefix(&key_prefix).unwrap_or(&key).to_string();
            self.delete_row(&ctx.session_id, key.clone())?;

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
