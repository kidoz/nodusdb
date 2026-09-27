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
        for idx in tbl
            .indexes
            .iter()
            .filter(|idx| idx.unique && idx.index_type != nodus_catalog::IndexType::Primary)
        {
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
            unique_keys.push((idx.name.as_str(), positions, predicate));
        }
        let pk_positions = Self::pk_positions_declared(tbl);
        let new_pk = key_tuple(new_row, &pk_positions);
        if unique_keys.is_empty() && pk_positions.is_empty() {
            return Ok(());
        }
        let primary_name = || {
            tbl.indexes
                .iter()
                .find(|i| i.index_type == nodus_catalog::IndexType::Primary)
                .map_or_else(|| format!("{}_pkey", tbl.name), |i| i.name.clone())
        };
        // Look the keys up where that is exact; scan the table otherwise.
        let equal = |existing: &[Value], positions: &[usize]| {
            matches!(
                (key_tuple(existing, positions), key_tuple(new_row, positions)),
                (Some(a), Some(b)) if a.iter().zip(&b).all(|(x, y)| values_equal(x, y))
            )
        };
        let mut looked_up = true;
        if !pk_positions.is_empty() {
            match self.key_candidates(&ctx.session_id, tbl, &pk_positions, None, new_row)? {
                Some(found) => {
                    for (pk, existing) in found {
                        if Some(pk.as_str()) != skip_pk && equal(&existing, &pk_positions) {
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
        for (idx_name, positions, predicate) in &unique_keys {
            let index = tbl.indexes.iter().find(|i| i.name == *idx_name);
            match self.key_candidates(&ctx.session_id, tbl, positions, index, new_row)? {
                Some(found) => {
                    for (pk, existing) in found {
                        if Some(pk.as_str()) != skip_pk
                            && equal(&existing, positions)
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
            if !pk_positions.is_empty()
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
            for (idx_name, positions, predicate) in &unique_keys {
                if let (Some(a), Some(b)) = (
                    key_tuple(&existing, positions),
                    key_tuple(new_row, positions),
                ) && a.iter().zip(&b).all(|(x, y)| values_equal(x, y))
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
            return Ok(Some(Vec::new()));
        }
        let declared = Self::pk_positions_declared(tbl);
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
        Ok(Some(self.index_rows(
            index.id,
            &row[leading],
            tbl.id,
            session,
        )?))
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
                nodus_catalog::TableConstraint::Check { name, expr } => {
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
                        .detail(failing_row(new_row))
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
            .map(|&p| row.get(p).map(render).unwrap_or_default())
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
            .detail(failing_row(row))
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
fn failing_row(row: &[Value]) -> String {
    let values: Vec<String> = row
        .iter()
        .map(|v| match v {
            Value::Null => "null".to_string(),
            v => render(v),
        })
        .collect();
    format!("Failing row contains ({}).", values.join(", "))
}

/// A row's values at `positions`, or `None` if any is NULL: a key containing
/// NULL never equals another (NULLs are distinct).
pub(crate) fn key_tuple(row: &[Value], positions: &[usize]) -> Option<Vec<Value>> {
    positions
        .iter()
        .map(|&p| match row.get(p) {
            None | Some(Value::Null) => None,
            Some(v) => Some(v.clone()),
        })
        .collect()
}
