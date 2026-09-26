//! Constraint enforcement on writes: unique / primary-key checks and
//! table-level CHECK and foreign-key validation, evaluated against the table's
//! current rows.

use crate::error_fields::DbError;
use crate::{ExecutionContext, MemExecutor, Value, parse_filter_expr, render, values_equal};
use anyhow::Result;

impl MemExecutor {
    pub(crate) fn check_unique_constraints(
        &self,
        session: &str,
        tbl: &nodus_catalog::TableDescriptor,
        new_row: &[Value],
        skip_pk: Option<&str>,
    ) -> Result<()> {
        // An index-less table has no PRIMARY KEY or UNIQUE constraint to enforce,
        // and its rows carry synthetic rowids — so exact-duplicate rows are
        // allowed. (The all-column key fallback below would otherwise reject
        // them as a spurious "primary key" collision.)
        if Self::uses_synthetic_rowid(tbl) {
            return Ok(());
        }
        // Each UNIQUE index constrains its whole key tuple. Primary indexes are
        // covered by the composite primary-key comparison below (a composite
        // PRIMARY KEY is stored as one primary index per column).
        let unique_keys: Vec<(&str, Vec<usize>)> = tbl
            .indexes
            .iter()
            .filter(|idx| idx.unique && idx.index_type != nodus_catalog::IndexType::Primary)
            .map(|idx| {
                let positions = idx
                    .key_columns
                    .iter()
                    .filter_map(|kc| tbl.columns.iter().position(|c| c.id == kc.column_id))
                    .collect();
                (idx.name.as_str(), positions)
            })
            .collect();
        let pk_positions = Self::pk_positions(tbl);
        let new_pk = Self::row_pk(&pk_positions, new_row);

        for existing in self.scan_rows(tbl.id, session)? {
            let pk = Self::row_pk(&pk_positions, &existing);
            if Some(pk.as_str()) == skip_pk {
                continue;
            }
            if pk == new_pk {
                let name = tbl
                    .indexes
                    .iter()
                    .find(|i| i.index_type == nodus_catalog::IndexType::Primary)
                    .map_or_else(|| format!("{}_pkey", tbl.name), |i| i.name.clone());
                return Err(self.duplicate_key(tbl, &name, &pk_positions, new_row));
            }
            for (idx_name, positions) in &unique_keys {
                if let (Some(a), Some(b)) = (
                    key_tuple(&existing, positions),
                    key_tuple(new_row, positions),
                ) && a.iter().zip(&b).all(|(x, y)| values_equal(x, y))
                {
                    return Err(self.duplicate_key(tbl, idx_name, positions, new_row));
                }
            }
        }
        Ok(())
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
