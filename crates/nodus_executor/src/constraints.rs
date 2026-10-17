//! Constraint enforcement on writes: unique / primary-key checks and
//! table-level CHECK and foreign-key validation, evaluated against the table's
//! current rows.

use crate::error_fields::DbError;
use crate::{ExecutionContext, MemExecutor, Value, parse_filter_expr, render, values_equal};
use anyhow::Result;

impl MemExecutor {
    /// Rejects `new_row` when it has the primary key or a unique index's key
    /// of another row (the row stored at `skip_pk`, which it replaces, aside).
    /// A key with a NULL never collides, and a partial unique index only
    /// constrains the rows its predicate holds for.
    pub(crate) fn check_unique_constraints(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        new_row: &[Value],
        skip_pk: Option<&str>,
    ) -> Result<()> {
        // Each UNIQUE index constrains its whole key tuple. Primary indexes are
        // covered by the composite primary-key comparison below (a composite
        // PRIMARY KEY is stored as one primary index per column).
        let col_names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let mut unique_keys = Vec::new();
        // A `WITHOUT OVERLAPS` key: positions, nulls equality, the period's
        // position, and a partial index's predicate.
        let mut temporal_keys: Vec<(&str, Vec<usize>, bool, usize, Option<crate::FilterExpr>)> =
            Vec::new();
        self.check_expression_keys(ctx, tbl, new_row, skip_pk)?;
        for idx in tbl.indexes.iter().filter(|idx| {
            idx.unique
                && idx.index_type != nodus_catalog::IndexType::Primary
                && !crate::index_keys::has_expressions(idx)
        }) {
            // A deferred constraint is checked when the transaction commits.
            if self.constraint_deferred(
                &ctx.session_id,
                &idx.name,
                idx.deferrable,
                idx.initially_deferred,
            ) {
                self.note_deferred_write(&ctx.session_id, tbl.id);
                continue;
            }
            let positions: Vec<usize> = idx
                .key_columns
                .iter()
                .filter_map(|kc| tbl.columns.iter().position(|c| c.id == kc.column_id))
                .collect();
            let predicate = match &idx.predicate {
                Some(p) => Some(self.index_predicate(&p.sql)?),
                None => None,
            };
            // A row the predicate leaves out is not constrained.
            if let Some(filter) = &predicate
                && self.eval_filter(ctx, new_row, &col_names, &tbl.columns, Some(filter))
                    != Some(true)
            {
                continue;
            }
            if let Some(period) = Self::period_position(tbl, idx) {
                temporal_keys.push((
                    idx.name.as_str(),
                    positions,
                    idx.nulls_not_distinct,
                    period,
                    predicate,
                ));
                continue;
            }
            unique_keys.push((
                idx.name.as_str(),
                positions,
                predicate,
                idx.nulls_not_distinct,
            ));
        }
        let mut pk_positions = Self::pk_positions_declared(tbl);
        // A temporal primary key's period position; rows conflict on the
        // other key columns with overlapping periods.
        let mut pk_period: Option<usize> = tbl
            .indexes
            .iter()
            .find(|i| i.index_type == nodus_catalog::IndexType::Primary)
            .and_then(|i| Self::period_position(tbl, i));
        if !pk_positions.is_empty()
            && let Some(primary) = tbl
                .indexes
                .iter()
                .find(|i| i.index_type == nodus_catalog::IndexType::Primary)
            && self.constraint_deferred(
                &ctx.session_id,
                &primary.name,
                primary.deferrable,
                primary.initially_deferred,
            )
        {
            self.note_deferred_write(&ctx.session_id, tbl.id);
            pk_positions = Vec::new();
            pk_period = None;
        }
        let new_pk = key_tuple(new_row, &pk_positions);
        if unique_keys.is_empty() && pk_positions.is_empty() && temporal_keys.is_empty() {
            return Ok(());
        }
        let primary_name = || {
            tbl.indexes
                .iter()
                .find(|i| i.index_type == nodus_catalog::IndexType::Primary)
                .map_or_else(|| format!("{}_pkey", tbl.name), |i| i.name.clone())
        };
        // Look the keys up where that is exact; scan the table otherwise.
        // A `NULLS NOT DISTINCT` key keeps its NULLs as values.
        let equal = |existing: &[Value], positions: &[usize], nulls_equal: bool| {
            let key = |row: &[Value]| {
                if nulls_equal {
                    key_tuple_including_nulls(row, positions)
                } else {
                    key_tuple(row, positions)
                }
            };
            matches!((key(existing), key(new_row)),
                (Some(a), Some(b)) if a.iter().zip(&b).all(|(x, y)| values_equal(x, y)))
        };
        let mut looked_up = true;
        // A temporal key's overlap cannot be looked up by its exact key.
        if pk_period.is_some() || !temporal_keys.is_empty() {
            looked_up = false;
        }
        if !pk_positions.is_empty() && pk_period.is_none() {
            match self.key_candidates(&ctx.session_id, tbl, &pk_positions, None, new_row)? {
                Some(found) => {
                    for (pk, existing) in found {
                        if Some(pk.as_str()) != skip_pk && equal(&existing, &pk_positions, false) {
                            return Err(self.duplicate_key(
                                tbl,
                                &primary_name(),
                                &pk_positions,
                                new_row,
                            ));
                        }
                    }
                }
                None => looked_up = false,
            }
        }
        for (idx_name, positions, predicate, nulls_equal) in &unique_keys {
            let index = tbl.indexes.iter().find(|i| i.name == *idx_name);
            match self.key_candidates(&ctx.session_id, tbl, positions, index, new_row)? {
                Some(found) => {
                    for (pk, existing) in found {
                        if Some(pk.as_str()) != skip_pk
                            && equal(&existing, positions, *nulls_equal)
                            && predicate.as_ref().is_none_or(|filter| {
                                self.eval_filter(
                                    ctx,
                                    &existing,
                                    &col_names,
                                    &tbl.columns,
                                    Some(filter),
                                ) == Some(true)
                            })
                        {
                            return Err(self.duplicate_key(tbl, idx_name, positions, new_row));
                        }
                    }
                }
                None => looked_up = false,
            }
        }
        if looked_up {
            return Ok(());
        }
        let prefix = format!("{}:", tbl.id);
        for (stored_key, existing) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            let stored_pk = stored_key.strip_prefix(&prefix).unwrap_or(&stored_key);
            if Some(stored_pk) == skip_pk {
                continue;
            }
            if let Some(period) = pk_period {
                if temporal_conflict(tbl, &existing, new_row, &pk_positions, period, false) {
                    let name = tbl
                        .indexes
                        .iter()
                        .find(|i| i.index_type == nodus_catalog::IndexType::Primary)
                        .map_or_else(|| format!("{}_pkey", tbl.name), |i| i.name.clone());
                    return Err(self.temporal_conflict(
                        tbl,
                        &name,
                        &pk_positions,
                        new_row,
                        &existing,
                    ));
                }
            } else if !pk_positions.is_empty()
                && let (Some(a), Some(b)) = (key_tuple(&existing, &pk_positions), &new_pk)
                && a.iter().zip(b).all(|(x, y)| values_equal(x, y))
            {
                let name = tbl
                    .indexes
                    .iter()
                    .find(|i| i.index_type == nodus_catalog::IndexType::Primary)
                    .map_or_else(|| format!("{}_pkey", tbl.name), |i| i.name.clone());
                return Err(self.duplicate_key(tbl, &name, &pk_positions, new_row));
            }
            for (idx_name, positions, nulls_equal, period, predicate) in &temporal_keys {
                if temporal_conflict(tbl, &existing, new_row, positions, *period, *nulls_equal)
                    && predicate.as_ref().is_none_or(|filter| {
                        self.eval_filter(ctx, &existing, &col_names, &tbl.columns, Some(filter))
                            == Some(true)
                    })
                {
                    return Err(
                        self.temporal_conflict(tbl, idx_name, positions, new_row, &existing)
                    );
                }
            }
            for (idx_name, positions, predicate, nulls_equal) in &unique_keys {
                let key = |row: &[Value]| {
                    if *nulls_equal {
                        key_tuple_including_nulls(row, positions)
                    } else {
                        key_tuple(row, positions)
                    }
                };
                if let (Some(a), Some(b)) = (key(&existing), key(new_row))
                    && a.iter().zip(&b).all(|(x, y)| values_equal(x, y))
                    && predicate.as_ref().is_none_or(|filter| {
                        self.eval_filter(ctx, &existing, &col_names, &tbl.columns, Some(filter))
                            == Some(true)
                    })
                {
                    return Err(self.duplicate_key(tbl, idx_name, positions, new_row));
                }
            }
        }
        Ok(())
    }

    /// The stored rows that could equal `row` on the key at `positions`,
    /// found without scanning the table: the row stored under the primary
    /// key, or those a unique `index` lists under the key's leading value.
    /// `None` when no lookup is exact, so the caller scans. A key with a NULL
    /// equals no row.
    pub(crate) fn key_candidates(
        &self,
        session: &str,
        tbl: &nodus_catalog::TableDescriptor,
        positions: &[usize],
        index: Option<&nodus_catalog::IndexDescriptor>,
        row: &[Value],
    ) -> Result<Option<Vec<(String, Vec<Value>)>>> {
        if positions
            .iter()
            .any(|&p| matches!(row.get(p), None | Some(Value::Null)))
        {
            // Under `NULLS NOT DISTINCT` a NULL key may equal a row, so the
            // caller scans; otherwise it equals none.
            if index.is_some_and(|index| index.nulls_not_distinct) {
                return Ok(None);
            }
            return Ok(Some(Vec::new()));
        }
        // A table whose rows have synthetic identities (no primary key, or a
        // deferrable one) has no row stored under its key's text.
        let declared = if Self::uses_synthetic_rowid(tbl) {
            Vec::new()
        } else {
            Self::pk_positions_declared(tbl)
        };
        if !declared.is_empty() && positions == declared.as_slice() {
            // A row is stored under its key's text, which only these types
            // spell one way.
            let exact = positions
                .iter()
                .all(|&p| matches!(row[p], Value::Int(_) | Value::Text(_) | Value::Bool(_)));
            if !exact {
                return Ok(None);
            }
            let pk = Self::row_pk(positions, row);
            return Ok(Some(
                self.get_row(tbl.id, &pk, session)?
                    .map(|found| vec![(pk, found)])
                    .unwrap_or_default(),
            ));
        }
        let Some(index) = index else {
            return Ok(None);
        };
        let Some(leading) = Self::index_leading_position(tbl, index) else {
            return Ok(None);
        };
        // A float's text does not decide equality (`-0` is `0`).
        if matches!(row[leading], Value::Float(_)) {
            return Ok(None);
        }
        let parts = crate::index_keys::index_parts(tbl, index);
        let key = crate::index_keys::key_values(tbl, &parts, row);
        Ok(Some(self.index_rows(tbl, index, &key, tbl.id, session)?))
    }

    /// Rejects `new_row` when a unique index on expressions of `tbl` has
    /// its key for another row (the row at `skip_pk` aside). A key with a
    /// NULL never collides, and a partial index only constrains the rows
    /// its predicate holds for.
    fn check_expression_keys(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        new_row: &[Value],
        skip_pk: Option<&str>,
    ) -> Result<()> {
        let col_names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        for idx in tbl
            .indexes
            .iter()
            .filter(|i| i.unique && crate::index_keys::has_expressions(i))
        {
            let predicate = match &idx.predicate {
                Some(p) => Some(self.index_predicate(&p.sql)?),
                None => None,
            };
            let holds = |row: &[Value]| {
                predicate.as_ref().is_none_or(|filter| {
                    self.eval_filter(ctx, row, &col_names, &tbl.columns, Some(filter)) == Some(true)
                })
            };
            if !holds(new_row) {
                continue;
            }
            let found = self.expression_key_matches(&ctx.session_id, tbl, idx, new_row)?;
            if found
                .iter()
                .any(|(pk, existing)| Some(pk.as_str()) != skip_pk && holds(existing))
            {
                let parts = crate::index_keys::index_parts(tbl, idx);
                let values: Vec<String> = crate::index_keys::key_values(tbl, &parts, new_row)
                    .iter()
                    .map(|value| match value {
                        Value::Null => "null".to_string(),
                        other => render(other),
                    })
                    .collect();
                return Err(DbError::new(format!(
                    "duplicate key value violates unique constraint \"{}\"",
                    idx.name
                ))
                .detail(format!(
                    "Key ({})=({}) already exists.",
                    crate::index_keys::key_names(tbl, &parts).join(", "),
                    values.join(", ")
                ))
                .schema(self.schema_name_of(tbl))
                .table(&tbl.name)
                .constraint(&idx.name)
                .into());
            }
        }
        Ok(())
    }

    /// The rows of `tbl` (by stored key) whose key in `idx`, an index on
    /// expressions, is `row`'s; none when `row`'s has a NULL.
    pub(crate) fn expression_key_matches(
        &self,
        session: &str,
        tbl: &nodus_catalog::TableDescriptor,
        idx: &nodus_catalog::IndexDescriptor,
        row: &[Value],
    ) -> Result<Vec<(String, Vec<Value>)>> {
        let parts = crate::index_keys::index_parts(tbl, idx);
        let key = crate::index_keys::key_values(tbl, &parts, row);
        if key.is_empty() {
            return Ok(Vec::new());
        }
        // Under `NULLS NOT DISTINCT` a NULL key may equal a row.
        let nulls_equal = idx.nulls_not_distinct;
        if !nulls_equal && key.iter().any(|v| matches!(v, Value::Null)) {
            return Ok(Vec::new());
        }
        // A float's text does not decide equality (`-0` is `0`), and a NULL
        // leading value has no index entry.
        let candidates = if matches!(key[0], Value::Float(_))
            || (nulls_equal && matches!(key[0], Value::Null))
        {
            let prefix = format!("{}:", tbl.id);
            self.scan_rows_keyed(tbl.id, session)?
                .into_iter()
                .map(|(k, r)| (k.strip_prefix(&prefix).unwrap_or(&k).to_string(), r))
                .collect()
        } else {
            self.index_rows(tbl, idx, &key, tbl.id, session)?
        };
        Ok(candidates
            .into_iter()
            .filter(|(_, existing)| {
                crate::index_keys::key_values(tbl, &parts, existing)
                    .iter()
                    .zip(&key)
                    .all(|(a, b)| values_equal(a, b))
            })
            .collect())
    }

    /// A partial index's predicate, as a condition over the table's rows.
    pub(crate) fn index_predicate(&self, sql: &str) -> Result<crate::FilterExpr> {
        let expr = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
            .try_with_sql(sql)
            .and_then(|mut p| p.parse_expr())
            .map_err(|e| anyhow::anyhow!("cannot parse index predicate `{sql}`: {e}"))?;
        parse_filter_expr(&expr, &[])
    }

    /// Checks `new_row` of `tbl` against its CHECK constraints and foreign
    /// keys; `old_row` is the row it replaces, if any.
    pub(crate) fn check_table_constraints(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        new_row: &[Value],
        old_row: Option<&[Value]>,
        col_names: &[String],
    ) -> Result<()> {
        for tc in &tbl.constraints {
            match tc {
                nodus_catalog::TableConstraint::Check {
                    name,
                    expr,
                    enforced,
                } => {
                    // `NOT ENFORCED`: the constraint is declared, not checked.
                    if !enforced {
                        continue;
                    }
                    let ast_expr = match sqlparser::parser::Parser::new(
                        &sqlparser::dialect::PostgreSqlDialect {},
                    )
                    .try_with_sql(expr)
                    {
                        Ok(mut p) => match p.parse_expr() {
                            Ok(e) => e,
                            Err(e) => anyhow::bail!("Failed to parse CHECK constraint expr: {}", e),
                        },
                        Err(e) => anyhow::bail!("Failed to init parser: {}", e),
                    };
                    let filter = parse_filter_expr(&ast_expr, &[]).map_err(|e| {
                        anyhow::anyhow!("CHECK constraint `{expr}` cannot be evaluated: {e}")
                    })?;
                    // The check reads its columns as their declared types
                    // (`r @> 0` on a range, enum order, interval spans).
                    let column_type = |name: &str| {
                        let bare = name.rsplit('.').next().unwrap_or(name);
                        tbl.columns
                            .iter()
                            .find(|c| c.name == bare)
                            .map(|c| c.data_type.clone())
                    };
                    let filter =
                        crate::result_types::check_filter_integer_ranges(&filter, &column_type);
                    // As in PostgreSQL, only a false result rejects the row; a
                    // NULL (unknown) result satisfies the constraint.
                    let result =
                        self.eval_filter(ctx, new_row, col_names, &tbl.columns, Some(&filter));
                    if result == Some(false) {
                        let name = name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check", tbl.name));
                        return Err(DbError::new(format!(
                            "new row for relation \"{}\" violates check constraint \"{name}\"",
                            tbl.name
                        ))
                        .detail(failing_row_typed(new_row, &tbl.columns))
                        .schema(self.schema_name_of(tbl))
                        .table(&tbl.name)
                        .constraint(&name)
                        .into());
                    }
                }
                nodus_catalog::TableConstraint::ForeignKey { .. } => {}
            }
        }
        self.check_references_from(ctx, tbl, new_row, old_row)?;
        Ok(())
    }
}

impl MemExecutor {
    /// The error for a row whose key `positions` duplicates another's under
    /// unique constraint `name`.
    /// Re-checks a deferred unique or primary key against the table's rows:
    /// any two rows sharing a key are PostgreSQL's duplicate-key violation
    /// (raised when the constraint stops being deferred).
    pub(crate) fn check_unique_holds(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        name: &str,
        positions: &[usize],
    ) -> Result<()> {
        let col_names: Vec<String> = tbl.columns.iter().map(|c| c.name.clone()).collect();
        let index = tbl.indexes.iter().find(|i| i.name == name);
        let predicate = index
            .and_then(|i| i.predicate.as_ref())
            .map(|p| self.index_predicate(&p.sql))
            .transpose()?;
        let nulls_equal = index.is_some_and(|i| i.nulls_not_distinct);
        // A temporal key conflicts pairwise: equal on the other columns with
        // overlapping periods.
        if let Some(period) = index.and_then(|i| Self::period_position(tbl, i)) {
            let rows: Vec<Vec<Value>> = self
                .scan_rows_keyed(tbl.id, &ctx.session_id)?
                .into_iter()
                .map(|(_, row)| row)
                .filter(|row| {
                    predicate.as_ref().is_none_or(|filter| {
                        self.eval_filter(ctx, row, &col_names, &tbl.columns, Some(filter))
                            == Some(true)
                    })
                })
                .collect();
            for (i, row) in rows.iter().enumerate() {
                for other in &rows[i + 1..] {
                    if temporal_conflict(tbl, row, other, positions, period, nulls_equal) {
                        // The check runs after the writes: the later stored
                        // row is the one PostgreSQL reports as the new key.
                        return Err(self.temporal_conflict(tbl, name, positions, other, row));
                    }
                }
            }
            return Ok(());
        }
        let mut seen = std::collections::HashSet::new();
        for (_key, row) in self.scan_rows_keyed(tbl.id, &ctx.session_id)? {
            // A key with a NULL never collides, unless the constraint is
            // `NULLS NOT DISTINCT`.
            let tuple = if nulls_equal {
                key_tuple_including_nulls(&row, positions)
            } else {
                key_tuple(&row, positions)
            };
            let Some(tuple) = tuple else {
                continue;
            };
            if let Some(filter) = &predicate
                && self.eval_filter(ctx, &row, &col_names, &tbl.columns, Some(filter)) != Some(true)
            {
                continue;
            }
            if !seen.insert(crate::referential::key_string(&tuple)) {
                return Err(self.duplicate_key(tbl, name, positions, &row));
            }
        }
        Ok(())
    }

    fn duplicate_key(
        &self,
        tbl: &nodus_catalog::TableDescriptor,
        name: &str,
        positions: &[usize],
        row: &[Value],
    ) -> anyhow::Error {
        let columns: Vec<&str> = positions
            .iter()
            .map(|&p| tbl.columns[p].name.as_str())
            .collect();
        let values: Vec<String> = positions
            .iter()
            .map(|&p| match row.get(p) {
                Some(Value::Null) => "null".to_string(),
                Some(value) => render(value),
                None => String::new(),
            })
            .collect();
        DbError::new(format!(
            "duplicate key value violates unique constraint \"{name}\""
        ))
        .detail(format!(
            "Key ({})=({}) already exists.",
            columns.join(", "),
            values.join(", ")
        ))
        .schema(self.schema_name_of(tbl))
        .table(&tbl.name)
        .constraint(name)
        .into()
    }

    /// The position of a temporal index's `WITHOUT OVERLAPS` period column.
    pub(crate) fn period_position(
        tbl: &nodus_catalog::TableDescriptor,
        index: &nodus_catalog::IndexDescriptor,
    ) -> Option<usize> {
        index
            .period
            .and_then(|id| tbl.columns.iter().position(|c| c.id == id))
    }

    /// The error a `WITHOUT OVERLAPS` key raises when `row` overlaps the
    /// existing row `existing`.
    fn temporal_conflict(
        &self,
        tbl: &nodus_catalog::TableDescriptor,
        name: &str,
        positions: &[usize],
        row: &[Value],
        existing: &[Value],
    ) -> anyhow::Error {
        let columns: Vec<&str> = positions
            .iter()
            .map(|&p| tbl.columns[p].name.as_str())
            .collect();
        let rendered = |values: &[Value]| {
            positions
                .iter()
                .map(|&p| match values.get(p) {
                    Some(Value::Null) => "null".to_string(),
                    Some(value) => render(value),
                    None => String::new(),
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        DbError::new(format!(
            "conflicting key value violates exclusion constraint \"{name}\""
        ))
        .detail(format!(
            "Key ({})=({}) conflicts with existing key ({})=({}).",
            columns.join(", "),
            rendered(row),
            columns.join(", "),
            rendered(existing)
        ))
        .schema(self.schema_name_of(tbl))
        .table(&tbl.name)
        .constraint(name)
        .into()
    }

    /// Rejects `row` when it has NULL in a NOT NULL column of `tbl`.
    pub(crate) fn check_not_null(
        &self,
        tbl: &nodus_catalog::TableDescriptor,
        row: &[Value],
    ) -> Result<()> {
        match tbl
            .columns
            .iter()
            .zip(row)
            .find(|(c, v)| !c.nullable && **v == Value::Null)
        {
            Some((column, _)) => Err(DbError::new(format!(
                "null value in column \"{}\" of relation \"{}\" violates not-null constraint",
                column.name, tbl.name
            ))
            .detail(failing_row_typed(row, &tbl.columns))
            .schema(self.schema_name_of(tbl))
            .table(&tbl.name)
            .column(&column.name)
            .into()),
            None => Ok(()),
        }
    }
}

/// A rejected row as PostgreSQL's DETAIL shows it: `Failing row contains
/// (1, null, x).`
pub(crate) fn failing_row(row: &[Value]) -> String {
    let values: Vec<String> = row
        .iter()
        .map(|v| match v {
            Value::Null => "null".to_string(),
            v => render(v),
        })
        .collect();
    format!("Failing row contains ({}).", values.join(", "))
}

/// As [`failing_row`], but with each value shown by its column's type where
/// PostgreSQL shows it differently from the stored form (an `inet` without
/// its full mask, money with its symbol).
pub(crate) fn failing_row_typed(
    row: &[Value],
    columns: &[nodus_catalog::ColumnDescriptor],
) -> String {
    let values: Vec<String> = row
        .iter()
        .enumerate()
        .map(|(i, v)| match v {
            // A virtual generated column holds nothing in the heap, which is
            // what PostgreSQL's report says.
            _ if columns.get(i).is_some_and(|column| {
                crate::MemExecutor::generated_kind(column).is_some_and(|(_, virtual_)| virtual_)
            }) =>
            {
                "virtual".to_string()
            }
            Value::Null => "null".to_string(),
            v => {
                let kind = columns
                    .get(i)
                    .and_then(|c| crate::net::Kind::of(&c.data_type));
                match (kind, v) {
                    (Some(kind), Value::Text(text)) => crate::net::show(kind, text),
                    _ => render(v),
                }
            }
        })
        .collect();
    format!("Failing row contains ({}).", values.join(", "))
}

/// A row's values at `positions`, or `None` if any is NULL: a key containing
/// NULL never equals another (NULLs are distinct).
/// A key keeping any NULLs as values, for a `NULLS NOT DISTINCT` index.
pub(crate) fn key_tuple_including_nulls(row: &[Value], positions: &[usize]) -> Option<Vec<Value>> {
    positions.iter().map(|&p| row.get(p).cloned()).collect()
}

pub(crate) fn key_tuple(row: &[Value], positions: &[usize]) -> Option<Vec<Value>> {
    positions
        .iter()
        .map(|&p| match row.get(p) {
            None | Some(Value::Null) => None,
            Some(v) => Some(v.clone()),
        })
        .collect()
}

/// Whether two rows conflict on a temporal key: equal on every key column
/// but the period, whose range or multirange values overlap.
pub(crate) fn temporal_conflict(
    tbl: &nodus_catalog::TableDescriptor,
    a: &[Value],
    b: &[Value],
    positions: &[usize],
    period: usize,
    nulls_equal: bool,
) -> bool {
    let others: Vec<usize> = positions
        .iter()
        .copied()
        .filter(|&position| position != period)
        .collect();
    let key = |row: &[Value]| {
        if nulls_equal {
            key_tuple_including_nulls(row, &others)
        } else {
            key_tuple(row, &others)
        }
    };
    let equal = matches!(
        (key(a), key(b)),
        (Some(x), Some(y)) if x.iter().zip(&y).all(|(l, r)| values_equal(l, r))
    );
    if !equal {
        return false;
    }
    match (a.get(period), b.get(period)) {
        (Some(Value::Null), _) | (_, Some(Value::Null)) | (None, _) | (_, None) => false,
        (Some(x), Some(y)) => ranges_overlap(&tbl.columns[period].data_type, x, y),
    }
}

/// Whether two range or multirange values overlap (`&&`).
pub(crate) fn ranges_overlap(data_type: &str, a: &Value, b: &Value) -> bool {
    let (Value::Text(a), Value::Text(b)) = (a, b) else {
        return false;
    };
    if let Some(kind) = crate::ranges::Kind::of(data_type) {
        return matches!(
            crate::ranges::operator("&&", kind, a, b, false),
            Ok(Value::Bool(true))
        );
    }
    if let Some(kind) = crate::multiranges::kind_of(data_type) {
        return matches!(
            crate::multiranges::operator("&&", kind, a, b, false),
            Ok(Value::Bool(true))
        );
    }
    false
}
