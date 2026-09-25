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

/// What a data-modifying statement's conditions and expressions read: a
/// target row followed by a row of the relations the statement reads.
pub(crate) struct TargetScope {
    /// The target's columns then the relations' columns, qualified.
    pub(crate) names: Vec<String>,
    pub(crate) columns: Vec<ColumnDescriptor>,
    /// The relations' joined rows; a single empty row when the statement
    /// reads none, so each target row joins exactly once.
    pub(crate) source_rows: Vec<Vec<Value>>,
    /// How many of the columns are the target's.
    pub(crate) width: usize,
    /// The target table's own name when an alias hides it.
    pub(crate) hidden: Option<String>,
}

impl TargetScope {
    /// The declared type of a column the statement names.
    fn column_type(&self, name: &str) -> Option<String> {
        crate::filter_eval::col_pos(&self.names, name)
            .and_then(|i| self.columns.get(i))
            .map(|c| c.data_type.clone())
    }

    /// An expression with its integer arithmetic range-checked in the
    /// operands' types.
    pub(crate) fn check_ranges(&self, expr: &ScalarExpr) -> ScalarExpr {
        crate::result_types::check_integer_ranges(expr, &|name| self.column_type(name))
    }

    /// A condition with its integer arithmetic range-checked.
    pub(crate) fn check_filter_ranges(&self, filter: &FilterExpr) -> FilterExpr {
        crate::result_types::check_filter_integer_ranges(filter, &|name| self.column_type(name))
    }

    /// The column names with the target's columns (`target`) or the
    /// relations' columns made unnameable, for the parts of a `MERGE` that
    /// see only the other side.
    pub(crate) fn names_without(&self, target: bool) -> Vec<String> {
        self.names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                if (i < self.width) == target {
                    "\u{0}".to_string()
                } else {
                    name.clone()
                }
            })
            .collect()
    }

    /// The positions of the `RETURNING` items in a returned row: the target
    /// row as written, the relations' row, then the target row as it was
    /// (`old.`). `*` is every column, the target's first unless
    /// `source_first` (as `MERGE` orders them), and `x.*` every column of
    /// relation `x`.
    pub(crate) fn returning_positions(
        &self,
        items: &[String],
        source_first: bool,
    ) -> Result<Vec<usize>> {
        let old = self.names.len();
        let mut positions = Vec::new();
        for item in items {
            if item == "*" {
                // The relations' columns as `*` shows them: a USING join's
                // merged columns (named without a qualifier) once, first.
                let merged: Vec<usize> = (self.width..old)
                    .filter(|&i| !self.names[i].contains('.'))
                    .collect();
                let unqualified = |i: usize| self.names[i].rsplit('.').next().unwrap_or_default();
                let source: Vec<usize> = merged
                    .iter()
                    .copied()
                    .chain((self.width..old).filter(|&i| {
                        self.names[i].contains('.')
                            && !merged.iter().any(|&m| self.names[m] == unqualified(i))
                    }))
                    .collect();
                let target = 0..self.width;
                if source_first {
                    positions.extend(source.into_iter().chain(target));
                } else {
                    positions.extend(target.chain(source));
                }
            } else if item == "old.*" {
                positions.extend(old..old + self.width);
            } else if let Some(column) = item.strip_prefix("old.") {
                let suffix = format!(".{column}");
                let i = (0..self.width)
                    .find(|&i| self.names[i].ends_with(&suffix))
                    .ok_or_else(|| anyhow::anyhow!("column {item} does not exist"))?;
                positions.push(old + i);
            } else if let Some(relation) = item.strip_suffix(".*") {
                let inner = format!(".{relation}");
                let before = positions.len();
                positions.extend((0..old).filter(|&i| {
                    self.names[i]
                        .rsplit_once('.')
                        .is_some_and(|(q, _)| q == relation || q.ends_with(&inner))
                }));
                if positions.len() == before {
                    anyhow::bail!("missing FROM-clause entry for table \"{relation}\"");
                }
            } else {
                self.check_refs(vec![item.clone()], &self.names)?;
                positions.extend(crate::filter_eval::col_pos(&self.names, item));
            }
        }
        Ok(positions)
    }

    /// The `RETURNING` rows: each returned row's values at `positions`,
    /// under their column names.
    pub(crate) fn returning_output(
        &self,
        positions: &[usize],
        rows: Vec<Vec<Value>>,
        tag: String,
    ) -> QueryOutput {
        if positions.is_empty() {
            return QueryOutput::tag(&tag);
        }
        // An `old.` position reads the target's column.
        let column = |i: usize| {
            if i < self.names.len() {
                i
            } else {
                i - self.names.len()
            }
        };
        let name = |i: usize| {
            let name = &self.names[column(i)];
            name.rsplit('.').next().unwrap_or(name).to_string()
        };
        QueryOutput {
            columns: positions.iter().map(|&i| name(i)).collect(),
            types: positions
                .iter()
                .map(|&i| self.columns[column(i)].data_type.clone())
                .collect(),
            rows: rows
                .into_iter()
                .map(|row| Row {
                    values: positions
                        .iter()
                        .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                        .collect(),
                })
                .collect(),
            tag,
        }
    }

    /// Checks that every column reference resolves against `visible` (the
    /// scope's names, some perhaps made unnameable), and that an unqualified
    /// one does not name both a target column and a column of the relations.
    pub(crate) fn check_refs(&self, refs: Vec<String>, visible: &[String]) -> Result<()> {
        let refs: Vec<String> = refs
            .into_iter()
            .map(|name| match crate::filter_eval::parse_json_ref(&name) {
                Some((base, _, _)) => base,
                None => name,
            })
            .collect();
        for name in &refs {
            let Some((qualifier, _)) = name.rsplit_once('.') else {
                continue;
            };
            if crate::filter_eval::col_pos(visible, name).is_some() {
                continue;
            }
            // A relation the statement has, but that this part cannot see: the
            // target hidden by its alias, or a side of a MERGE join.
            let inner = format!(".{qualifier}.");
            let prefix = format!("{qualifier}.");
            let in_scope = |names: &[String]| {
                names
                    .iter()
                    .any(|c| c.starts_with(&prefix) || c.contains(&inner))
            };
            if self.hidden.as_deref() == Some(qualifier)
                || (in_scope(&self.names) && !in_scope(visible))
            {
                anyhow::bail!("invalid reference to FROM-clause entry for table \"{qualifier}\"");
            }
        }
        crate::filter_eval::check_column_refs(refs.iter().cloned(), visible)?;
        let (target, source) = visible.split_at(self.width);
        for name in refs.iter().filter(|name| !name.contains('.')) {
            let suffix = format!(".{name}");
            let names = |columns: &[String]| columns.iter().any(|c| c.ends_with(&suffix));
            if names(target) && names(source) {
                anyhow::bail!("column reference \"{name}\" is ambiguous");
            }
        }
        Ok(())
    }
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
        use crate::plan_types::OnConflictClause;
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Insert, ResourceRef::Table(tbl.id))?;
        let returning = Self::expand_returning(&tbl, returning)?;

        // Target column positions, in the order values are supplied.
        let targets: Vec<usize> = if columns.is_empty() {
            (0..tbl.columns.len()).collect()
        } else {
            let mut targets = Vec::with_capacity(columns.len());
            for name in &columns {
                let pos = Self::column_position(&tbl, name)?;
                if targets.contains(&pos) {
                    anyhow::bail!("column \"{name}\" specified more than once");
                }
                targets.push(pos);
            }
            targets
        };
        let col_names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let pk_positions = Self::pk_positions(&tbl);
        let mut inserted_count = 0;
        let mut returning_rows = Vec::new();
        // Keys this statement inserted or updated: ON CONFLICT DO UPDATE may not
        // affect the same row twice.
        let mut touched = std::collections::HashSet::new();

        for (row_idx, values) in values_list.iter().enumerate() {
            if values.len() > targets.len() {
                anyhow::bail!("INSERT has more expressions than target columns");
            }
            if !columns.is_empty() && values.len() < targets.len() {
                anyhow::bail!("INSERT has more target columns than expressions");
            }
            // Build the row in table-column order, tracking which columns
            // received a value (an explicit `DEFAULT` cell counts as omitted).
            let defaults_mask = default_cells.get(row_idx);
            let is_default_cell =
                |j: usize| defaults_mask.is_some_and(|m| m.get(j).copied().unwrap_or(false));
            let mut raw = vec![Value::Null; tbl.columns.len()];
            let mut provided = vec![false; tbl.columns.len()];
            for (j, (&pos, val)) in targets.iter().zip(values).enumerate() {
                if !is_default_cell(j) {
                    raw[pos] = val.clone();
                    provided[pos] = true;
                }
            }
            // Unprovided columns take their declared DEFAULT, if any; a
            // generated column or a `GENERATED ALWAYS` identity takes no value.
            for (i, c) in tbl.columns.iter().enumerate() {
                let default = Self::column_default(c);
                if provided[i]
                    && let Some(kind) = default.as_ref().and_then(Self::write_protection)
                {
                    anyhow::bail!(
                        "cannot insert a non-DEFAULT value into column \"{}\": {kind}",
                        c.name
                    );
                }
                if !provided[i]
                    && let Some(expr) = default
                    && Self::generation_expr(&expr).is_none()
                {
                    raw[i] = eval_scalar_expr(&expr, &[], &[]);
                }
            }
            let mut row: Vec<Value> = tbl
                .columns
                .iter()
                .enumerate()
                .map(|(i, c)| crate::value::coerce_for_column(&raw[i], &c.data_type))
                .collect();
            Self::compute_generated(&tbl, &mut row, &col_names);
            for (c, val) in tbl.columns.iter().zip(&row) {
                if !c.nullable && *val == Value::Null {
                    anyhow::bail!("Column {} cannot be NULL", c.name);
                }
            }

            if let Some(clause) = &on_conflict
                && let Some((existing_key, existing_row)) =
                    self.find_conflict(&ctx.session_id, &tbl, &row, clause.target())?
            {
                match clause {
                    OnConflictClause::DoNothing { .. } => continue,
                    OnConflictClause::DoUpdate {
                        assignments,
                        condition,
                        ..
                    } => {
                        if touched.contains(&existing_key) {
                            anyhow::bail!(
                                "ON CONFLICT DO UPDATE command cannot affect row a second time"
                            );
                        }
                        // Expressions see the existing row, plus the proposed
                        // row as `excluded.<col>`.
                        let mut scope_row = existing_row.clone();
                        scope_row.extend(row.iter().cloned());
                        let mut scope_cols = col_names.clone();
                        scope_cols.extend(col_names.iter().map(|c| format!("excluded.{c}")));
                        let column_type = |name: &str| {
                            crate::filter_eval::col_pos(&scope_cols, name)
                                .map(|i| tbl.columns[i % tbl.columns.len()].data_type.clone())
                        };
                        let check = |e: &ScalarExpr| {
                            crate::result_types::check_integer_ranges(e, &column_type)
                        };
                        if let Some(cond) = condition
                            && self.eval_expr(ctx, &check(cond), &scope_row, &scope_cols)
                                != Value::Bool(true)
                        {
                            continue;
                        }
                        let assignments: Vec<(String, ScalarExpr)> = assignments
                            .iter()
                            .map(|(column, expr)| (column.clone(), check(expr)))
                            .collect();
                        let updated = self.apply_assignments(
                            ctx,
                            &tbl,
                            &assignments,
                            &existing_row,
                            (&scope_row, &scope_cols),
                        )?;
                        let key =
                            self.replace_row(ctx, &tbl, &existing_key, &existing_row, &updated)?;
                        touched.insert(key);
                        inserted_count += 1;
                        if !returning.is_empty() {
                            returning_rows.push(updated);
                        }
                        continue;
                    }
                }
            }

            self.check_unique_constraints(&ctx.session_id, &tbl, &row, None)?;
            self.check_table_constraints(ctx, &tbl, &row, &col_names)?;

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
            self.write_row(
                &ctx.session_id,
                key.clone(),
                crate::value::encode_row(&row)?,
            )?;
            touched.insert(key);

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
        Self::returning_output(
            &tbl,
            &returning,
            returning_rows,
            format!("INSERT 0 {inserted_count}"),
        )
    }

    pub(crate) fn exec_update(
        &self,
        ctx: &ExecutionContext,
        (table_name, table_alias): (String, Option<String>),
        assignments: Vec<(String, ScalarExpr)>,
        from: Option<LogicalPlan>,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Update, ResourceRef::Table(tbl.id))?;
        Self::reject_materialized_view(&tbl)?;
        for (col, _) in &assignments {
            Self::column_position(&tbl, col)?;
        }
        let scope = self.target_scope(
            ctx,
            &tbl,
            (&table_name, table_alias.as_deref()),
            from,
            false,
        )?;
        let returning = scope.returning_positions(&returning, false)?;
        let mut refs = Vec::new();
        for (_, expr) in &assignments {
            crate::filter_eval::scalar_column_refs(expr, &mut refs);
        }
        if let Some(filter) = &filter {
            crate::filter_eval::filter_column_refs(filter, &mut refs);
        }
        scope.check_refs(refs, &scope.names)?;
        let assignments: Vec<(String, ScalarExpr)> = assignments
            .iter()
            .map(|(column, expr)| (column.clone(), scope.check_ranges(expr)))
            .collect();
        let filter = filter.as_ref().map(|f| scope.check_filter_ranges(f));

        let mut updated = 0;
        let mut returning_rows = Vec::new();
        // Two-phase: pick the matching rows before mutating, so a subquery in
        // the filter evaluates against the pre-statement state.
        for (old_key, old_row, joined) in
            self.matching_targets(ctx, &tbl, &scope, filter.as_ref())?
        {
            // Assignments evaluate against the row's OLD values, joined to
            // the first FROM row that matches it.
            let row =
                self.apply_assignments(ctx, &tbl, &assignments, &old_row, (&joined, &scope.names))?;
            self.replace_row(ctx, &tbl, &old_key, &old_row, &row)?;
            updated += 1;
            if !returning.is_empty() {
                let mut returned = joined;
                returned.splice(..row.len(), row);
                returned.extend(old_row);
                returning_rows.push(returned);
            }
        }
        Ok(scope.returning_output(&returning, returning_rows, format!("UPDATE {updated}")))
    }

    pub(crate) fn exec_delete(
        &self,
        ctx: &ExecutionContext,
        (table_name, table_alias): (String, Option<String>),
        using: Option<LogicalPlan>,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
    ) -> Result<QueryOutput> {
        let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
        let tbl = self
            .catalog_reader
            .get_table(db_name, schema_name, table_only)?;
        self.authorize(ctx, Action::Delete, ResourceRef::Table(tbl.id))?;
        Self::reject_materialized_view(&tbl)?;
        let scope = self.target_scope(
            ctx,
            &tbl,
            (&table_name, table_alias.as_deref()),
            using,
            false,
        )?;
        let returning = scope.returning_positions(&returning, false)?;
        if let Some(filter) = &filter {
            let mut refs = Vec::new();
            crate::filter_eval::filter_column_refs(filter, &mut refs);
            scope.check_refs(refs, &scope.names)?;
        }
        let filter = filter.as_ref().map(|f| scope.check_filter_ranges(f));

        let mut deleted = 0;
        let mut returning_rows = Vec::new();
        // Two-phase: decide WHICH rows match before mutating anything, so a
        // subquery in the filter (e.g. `WHERE a = (SELECT max(a) ...)`) sees
        // the pre-statement state rather than partially-deleted data.
        for (key, row, joined) in self.matching_targets(ctx, &tbl, &scope, filter.as_ref())? {
            self.remove_row(ctx, &tbl, &key, &row)?;
            deleted += 1;
            if !returning.is_empty() {
                returning_rows.push([joined, row].concat());
            }
        }
        Ok(scope.returning_output(&returning, returning_rows, format!("DELETE {deleted}")))
    }

    /// `TRUNCATE`: deletes every row of each table, then restarts the
    /// sequences their columns draw from if asked.
    pub(crate) fn exec_truncate(
        &self,
        ctx: &ExecutionContext,
        tables: Vec<String>,
        restart_identity: bool,
    ) -> Result<QueryOutput> {
        let mut sequences = Vec::new();
        for table_name in &tables {
            let (db_name, schema_name, table_only) = parse_object_name(table_name)?;
            let tbl = self
                .catalog_reader
                .get_table(db_name, schema_name, table_only)?;
            if tbl.view_query.is_some()
                || tbl.materialized_query.is_some()
                || crate::sequences::is_sequence(&tbl)
            {
                anyhow::bail!("\"{table_only}\" is not a table");
            }
            sequences.extend(
                tbl.columns
                    .iter()
                    .filter_map(Self::column_default)
                    .filter_map(|d| crate::sequences::default_sequence(&d)),
            );
        }
        for table_name in tables {
            self.exec_delete(ctx, (table_name, None), None, None, Vec::new())?;
        }
        if restart_identity {
            for sequence in sequences {
                self.sequences.restart(&sequence)?;
            }
        }
        Ok(QueryOutput::tag("TRUNCATE TABLE"))
    }

    /// The columns and rows a data-modifying statement's conditions and
    /// expressions see: each row of the target table (named by its alias, if
    /// any) joined to the rows of the relations it reads, if any.
    pub(crate) fn target_scope(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        (table_name, table_alias): (&str, Option<&str>),
        source: Option<LogicalPlan>,
        merge: bool,
    ) -> Result<TargetScope> {
        let refname = |qualified: &str| {
            qualified
                .rsplit('.')
                .next()
                .unwrap_or(qualified)
                .to_string()
        };
        let prefix = table_alias.unwrap_or(table_name);
        let mut names: Vec<String> = tbl
            .columns
            .iter()
            .map(|c| format!("{prefix}.{}", c.name))
            .collect();
        let mut columns = tbl.columns.clone();
        let hidden = table_alias.map(|_| refname(table_name));
        let Some(source) = source else {
            return Ok(TargetScope {
                names,
                columns,
                source_rows: vec![Vec::new()],
                width: tbl.columns.len(),
                hidden,
            });
        };
        let out = self.relation_rows(ctx, source)?;
        // The target's name may not name one of the relations too.
        let target = refname(prefix);
        if out
            .columns
            .iter()
            .filter_map(|c| c.rsplit_once('.'))
            .any(|(qualifier, _)| refname(qualifier) == target)
        {
            if merge {
                anyhow::bail!("name \"{target}\" specified more than once");
            }
            anyhow::bail!("table name \"{target}\" specified more than once");
        }
        let now = Utc::now();
        columns.extend(
            out.columns
                .iter()
                .zip(&out.types)
                .map(|(name, ty)| ColumnDescriptor {
                    id: nodus_catalog::ColumnId::new(),
                    name: name.clone(),
                    version: 1,
                    created_at: now,
                    updated_at: now,
                    state: DescriptorState::Public,
                    data_type: ty.clone(),
                    nullable: true,
                    default_expr: None,
                }),
        );
        names.extend(out.columns);
        Ok(TargetScope {
            names,
            columns,
            source_rows: out.rows.into_iter().map(|r| r.values).collect(),
            width: tbl.columns.len(),
            hidden,
        })
    }

    /// Each target row `filter` matches, as `(key, row, joined row)`: the
    /// joined row is the target row followed by the first source row with
    /// which it matches.
    fn matching_targets(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        scope: &TargetScope,
        filter: Option<&FilterExpr>,
    ) -> Result<Vec<(String, Vec<Value>, Vec<Value>)>> {
        let mut matches = Vec::new();
        for (key, row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            let joined = scope.source_rows.iter().find_map(|source| {
                let mut joined = row.clone();
                joined.extend(source.iter().cloned());
                self.eval_filter(ctx, &joined, &scope.names, &scope.columns, filter)
                    .unwrap_or(false)
                    .then_some(joined)
            });
            if let Some(joined) = joined {
                matches.push((key, row, joined));
            }
        }
        Ok(matches)
    }

    /// Deletes the stored row at `key` and its secondary index entries.
    pub(crate) fn remove_row(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        key: &str,
        row: &[Value],
    ) -> Result<()> {
        // Use the row's actual stored key (works for any key scheme), and
        // derive the index-entry suffix from it.
        let key_prefix = format!("{}:", tbl.id);
        let pk_str = key.strip_prefix(&key_prefix).unwrap_or(key).to_string();
        self.delete_row(&ctx.session_id, key.to_string())?;
        for idx in &tbl.indexes {
            for kcol in &idx.key_columns {
                if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                    let index_val = row.get(pos).unwrap_or(&Value::Null);
                    self.delete_index_entry(&ctx.session_id, idx.id, index_val, &pk_str)?;
                }
            }
        }
        Ok(())
    }

    /// A column's position in `tbl`, or the PostgreSQL error for an unknown one.
    pub(crate) fn column_position(
        tbl: &nodus_catalog::TableDescriptor,
        name: &str,
    ) -> Result<usize> {
        tbl.columns
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "column \"{name}\" of relation \"{}\" does not exist",
                    tbl.name
                )
            })
    }

    /// A column's declared DEFAULT expression, if any.
    pub(crate) fn column_default(column: &ColumnDescriptor) -> Option<ScalarExpr> {
        column
            .default_expr
            .as_ref()
            .and_then(|json| serde_json::from_str::<ScalarExpr>(json).ok())
    }

    /// A generated column's expression: its default is `__GENERATED__(expr)`.
    pub(crate) fn generation_expr(default: &ScalarExpr) -> Option<&ScalarExpr> {
        match default {
            ScalarExpr::Function { name, args } if name == "__GENERATED__" && args.len() == 1 => {
                args.first()
            }
            _ => None,
        }
    }

    /// Why a column rejects explicit values, if it does: it is generated, or
    /// a `GENERATED ALWAYS` identity.
    fn write_protection(default: &ScalarExpr) -> Option<&'static str> {
        if Self::generation_expr(default).is_some() {
            Some("it is a generated column")
        } else if crate::sequences::identity_kind(default) == Some(true) {
            Some("it is an identity column defined as GENERATED ALWAYS")
        } else {
            None
        }
    }

    /// Computes every generated column of `row` from its other columns.
    fn compute_generated(
        tbl: &nodus_catalog::TableDescriptor,
        row: &mut [Value],
        col_names: &[String],
    ) {
        for (i, c) in tbl.columns.iter().enumerate() {
            if let Some(default) = Self::column_default(c)
                && let Some(expr) = Self::generation_expr(&default)
            {
                let value = eval_scalar_expr(expr, row, col_names);
                row[i] = crate::value::coerce_for_column(&value, &c.data_type);
            }
        }
    }

    /// Applies `SET` assignments to a copy of `old_row`. Each expression is
    /// evaluated against `scope` (the old row, plus `excluded.*` for ON
    /// CONFLICT), and the result is coerced to the column type.
    pub(crate) fn apply_assignments(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        assignments: &[(String, ScalarExpr)],
        old_row: &[Value],
        (scope_row, scope_cols): (&[Value], &[String]),
    ) -> Result<Vec<Value>> {
        let mut row = old_row.to_vec();
        for (col, expr) in assignments {
            let idx = Self::column_position(tbl, col)?;
            let default = Self::column_default(&tbl.columns[idx]);
            let is_default = matches!(expr,
                ScalarExpr::Function { name, args } if name == "__COLUMN_DEFAULT__" && args.is_empty());
            if !is_default && default.as_ref().and_then(Self::write_protection).is_some() {
                anyhow::bail!("column \"{col}\" can only be updated to DEFAULT");
            }
            // `SET col = DEFAULT` sentinel: the declared default, else NULL. A
            // generated column is recomputed below.
            let val = if is_default {
                default
                    .filter(|d| Self::generation_expr(d).is_none())
                    .map(|e| eval_scalar_expr(&e, &[], &[]))
                    .unwrap_or(Value::Null)
            } else {
                self.eval_expr(ctx, expr, scope_row, scope_cols)
            };
            row[idx] = crate::value::coerce_for_column(&val, &tbl.columns[idx].data_type);
        }
        let col_names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        Self::compute_generated(tbl, &mut row, &col_names);
        for (c, val) in tbl.columns.iter().zip(&row) {
            if !c.nullable && *val == Value::Null {
                anyhow::bail!("Column {} cannot be NULL", c.name);
            }
        }
        Ok(row)
    }

    /// Replaces the stored row at `old_key` with `row`: re-checks uniqueness
    /// (excluding the row itself) and table constraints, moves the row when its
    /// key changes, and maintains every index. Returns the row's new key.
    pub(crate) fn replace_row(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        old_key: &str,
        old_row: &[Value],
        row: &[Value],
    ) -> Result<String> {
        let key_prefix = format!("{}:", tbl.id);
        let old_pk = old_key
            .strip_prefix(&key_prefix)
            .unwrap_or(old_key)
            .to_string();
        // A synthetic rowid is the row's stable identity — keep it across the
        // update rather than re-deriving a key from the (changed) content.
        let pk = if Self::uses_synthetic_rowid(tbl) {
            old_pk.clone()
        } else {
            Self::row_pk(&Self::pk_positions(tbl), row)
        };
        // Skip only the row being replaced: a new key that lands on another
        // existing row is a violation, not an overwrite.
        self.check_unique_constraints(&ctx.session_id, tbl, row, Some(&old_pk))?;
        let col_names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        self.check_table_constraints(ctx, tbl, row, &col_names)?;

        let new_key = format!("{}:{}", tbl.id, pk);
        self.write_row(
            &ctx.session_id,
            new_key.clone(),
            crate::value::encode_row(row)?,
        )?;
        if new_key != old_key {
            self.delete_row(&ctx.session_id, old_key.to_string())?;
        }
        for idx in &tbl.indexes {
            for kcol in &idx.key_columns {
                if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                    let old_val = old_row.get(pos).unwrap_or(&Value::Null);
                    let new_val = row.get(pos).unwrap_or(&Value::Null);
                    if old_val != new_val || old_pk != pk {
                        self.delete_index_entry(&ctx.session_id, idx.id, old_val, &old_pk)?;
                        self.write_index_entry(&ctx.session_id, idx.id, new_val, &pk)?;
                    }
                }
            }
        }
        Ok(new_key)
    }

    /// Finds the existing row an `ON CONFLICT` insert collides with: one equal
    /// on the primary key or on any unique index, or only on the key `target`
    /// names. A key containing NULL never conflicts.
    fn find_conflict(
        &self,
        session: &str,
        tbl: &nodus_catalog::TableDescriptor,
        row: &[Value],
        target: Option<&crate::plan_types::ConflictTarget>,
    ) -> Result<Option<(String, Vec<Value>)>> {
        use crate::plan_types::ConflictTarget;
        let primary = tbl
            .indexes
            .iter()
            .find(|i| i.index_type == nodus_catalog::IndexType::Primary);
        let mut keys: Vec<(&str, Vec<usize>)> = Vec::new();
        if let Some(primary) = primary {
            keys.push((primary.name.as_str(), Self::pk_positions(tbl)));
        }
        for idx in &tbl.indexes {
            if idx.unique && idx.index_type != nodus_catalog::IndexType::Primary {
                let positions = idx
                    .key_columns
                    .iter()
                    .filter_map(|kc| tbl.columns.iter().position(|c| c.id == kc.column_id))
                    .collect();
                keys.push((idx.name.as_str(), positions));
            }
        }
        match target {
            None => {}
            Some(ConflictTarget::Columns(cols)) => {
                let mut wanted = cols
                    .iter()
                    .map(|c| Self::column_position(tbl, c))
                    .collect::<Result<Vec<_>>>()?;
                wanted.sort_unstable();
                keys.retain(|(_, positions)| {
                    let mut p = positions.clone();
                    p.sort_unstable();
                    p == wanted
                });
            }
            Some(ConflictTarget::Constraint(name)) => keys.retain(|(n, _)| n == name),
        }
        if target.is_some() && keys.is_empty() {
            anyhow::bail!(
                "there is no unique or exclusion constraint matching the ON CONFLICT specification"
            );
        }
        let proposed: Vec<Option<Vec<Value>>> = keys
            .iter()
            .map(|(_, positions)| crate::constraints::key_tuple(row, positions))
            .collect();
        for (key, existing) in self.scan_rows_keyed(tbl.id, session)? {
            for ((_, positions), wanted) in keys.iter().zip(&proposed) {
                if let (Some(wanted), Some(have)) =
                    (wanted, crate::constraints::key_tuple(&existing, positions))
                    && wanted.iter().zip(&have).all(|(a, b)| values_equal(a, b))
                {
                    return Ok(Some((key, existing)));
                }
            }
        }
        Ok(None)
    }

    /// Expands `*` in a RETURNING list to every column, and rejects a name
    /// that is not a column of the table.
    pub(crate) fn expand_returning(
        tbl: &nodus_catalog::TableDescriptor,
        returning: Vec<String>,
    ) -> Result<Vec<String>> {
        let mut out = Vec::with_capacity(returning.len());
        for name in returning {
            // An INSERT reads only its table, so a qualifier names it.
            let name = name.rsplit('.').next().unwrap_or(&name).to_string();
            if name == "*" {
                out.extend(tbl.columns.iter().map(|c| c.name.clone()));
            } else {
                Self::column_position(tbl, &name)?;
                out.push(name);
            }
        }
        Ok(out)
    }

    /// The statement's result: just the command tag, or the RETURNING rows.
    pub(crate) fn returning_output(
        tbl: &nodus_catalog::TableDescriptor,
        returning: &[String],
        rows: Vec<Vec<Value>>,
        tag: String,
    ) -> Result<QueryOutput> {
        if returning.is_empty() {
            return Ok(QueryOutput::tag(&tag));
        }
        let positions = returning
            .iter()
            .map(|c| Self::column_position(tbl, c))
            .collect::<Result<Vec<_>>>()?;
        let rows = rows
            .into_iter()
            .map(|r| Row {
                values: positions
                    .iter()
                    .map(|&i| r.get(i).cloned().unwrap_or(Value::Null))
                    .collect(),
            })
            .collect();
        Ok(QueryOutput {
            tag,
            columns: returning.to_vec(),
            types: Self::returning_types(&tbl.columns, returning),
            rows,
        })
    }
}
