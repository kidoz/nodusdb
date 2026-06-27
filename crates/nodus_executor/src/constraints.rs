//! Constraint enforcement on writes: unique / primary-key checks and
//! table-level CHECK and foreign-key validation, evaluated against the table's
//! current rows.

use crate::{
    ExecutionContext, MemExecutor, Value, parse_filter_expr, parse_object_name, render,
    values_equal,
};
use anyhow::Result;

impl MemExecutor {
    pub(crate) fn check_unique_constraints(
        &self,
        session: &str,
        tbl: &nodus_catalog::TableDescriptor,
        new_row: &[Value],
        skip_pk: Option<&str>,
    ) -> Result<()> {
        let mut unique_col_indices = Vec::new();
        for idx in &tbl.indexes {
            if idx.unique {
                for kcol in &idx.key_columns {
                    if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                        unique_col_indices.push((idx.name.clone(), pos));
                    }
                }
            }
        }

        let pk_positions = Self::pk_positions(tbl);
        let new_pk = Self::row_pk(&pk_positions, new_row);

        for existing in self.scan_rows(tbl.id, session)? {
            let pk = Self::row_pk(&pk_positions, &existing);
            if Some(pk.as_str()) == skip_pk {
                continue;
            }
            if pk == new_pk {
                anyhow::bail!("Unique constraint violation on primary key");
            }
            for (idx_name, col_idx) in &unique_col_indices {
                let existing_val = existing.get(*col_idx).unwrap_or(&Value::Null);
                let new_val = new_row.get(*col_idx).unwrap_or(&Value::Null);
                if existing_val != &Value::Null && values_equal(existing_val, new_val) {
                    anyhow::bail!("Unique constraint violation on index '{}'", idx_name);
                }
            }
        }
        Ok(())
    }

    pub(crate) fn check_table_constraints(
        &self,
        ctx: &ExecutionContext,
        tbl: &nodus_catalog::TableDescriptor,
        new_row: &[Value],
        col_names: &[String],
    ) -> Result<()> {
        for tc in &tbl.constraints {
            match tc {
                nodus_catalog::TableConstraint::Check { name: _, expr } => {
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
                    if let Some(filter) = parse_filter_expr(&ast_expr, &[]) {
                        let result =
                            self.eval_filter(ctx, new_row, col_names, &tbl.columns, Some(&filter));
                        if result != Some(true) {
                            anyhow::bail!("violates check constraint");
                        }
                    }
                }
                nodus_catalog::TableConstraint::ForeignKey {
                    columns,
                    foreign_table,
                    referred_columns,
                    ..
                } => {
                    // Simple FK check
                    let (db_name, schema_name, table_only) = parse_object_name(foreign_table)
                        .unwrap_or(("default", "public", foreign_table));
                    let f_tbl = self
                        .catalog_reader
                        .get_table(db_name, schema_name, table_only)?;

                    let mut all_match = true;
                    for (i, c) in columns.iter().enumerate() {
                        let ref_c = &referred_columns[i];
                        let val_idx = col_names.iter().position(|name| name == c).unwrap();
                        let val = &new_row[val_idx];
                        if val == &Value::Null {
                            continue;
                        } // Nulls skip FK checks

                        let ref_idx = f_tbl
                            .columns
                            .iter()
                            .position(|name| &name.name == ref_c)
                            .unwrap();
                        let mut found = false;
                        for f_row in self.scan_rows(f_tbl.id, &ctx.session_id)? {
                            if values_equal(&f_row[ref_idx], val) {
                                found = true;
                                break;
                            }
                        }
                        if !found {
                            all_match = false;
                            break;
                        }
                    }
                    if !all_match {
                        anyhow::bail!("violates foreign key constraint");
                    }
                }
            }
        }
        Ok(())
    }
}
