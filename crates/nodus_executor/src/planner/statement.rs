//! Top-level statement planning (DDL/DML/query dispatch).
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

/// Resolves the bare column names referenced by an index-style constraint
/// (`UNIQUE`/`PRIMARY KEY`), whose columns are now `IndexColumn` entries
/// wrapping an `OrderByExpr`.
fn index_column_names(columns: &[sqlparser::ast::IndexColumn]) -> Vec<String> {
    columns
        .iter()
        .filter_map(|c| extract_col_name(&c.column.expr))
        .collect()
}

pub fn plan_statement(stmt: &sqlparser::ast::Statement, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;
    match stmt {
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            ..
        } => {
            let name = match schema_name {
                sqlparser::ast::SchemaName::Simple(name) => name.to_string(),
                _ => anyhow::bail!("Unsupported schema name format"),
            };
            Ok(LogicalPlan::CreateSchema {
                schema_name: name,
                if_not_exists: *if_not_exists,
            })
        }
        Statement::CreateTable(create_table) => {
            let name = &create_table.name;
            let columns = &create_table.columns;
            let constraints = &create_table.constraints;
            let table_name = name.to_string();
            let mut cols = Vec::new();
            let mut tbl_constraints = Vec::new();
            for c in columns {
                let mut nullable = true;
                let mut unique = false;
                let mut primary = false;
                let mut default = None;
                for opt in &c.options {
                    match &opt.option {
                        sqlparser::ast::ColumnOption::NotNull => nullable = false,
                        sqlparser::ast::ColumnOption::Default(e) => {
                            default = Some(lower_scalar(e, params).ok_or_else(|| {
                                anyhow::anyhow!(
                                    "Unsupported DEFAULT expression for column {}",
                                    c.name.value
                                )
                            })?);
                        }
                        // `PRIMARY KEY` column option implies unique + not-null.
                        sqlparser::ast::ColumnOption::PrimaryKey(_) => {
                            unique = true;
                            nullable = false;
                            primary = true;
                        }
                        sqlparser::ast::ColumnOption::Unique(_) => {
                            unique = true;
                        }
                        sqlparser::ast::ColumnOption::Check(check) => {
                            tbl_constraints.push(nodus_catalog::TableConstraint::Check {
                                name: opt.name.as_ref().map(|n| n.value.clone()),
                                expr: check.expr.to_string(),
                            });
                        }
                        sqlparser::ast::ColumnOption::ForeignKey(fk) => {
                            tbl_constraints.push(nodus_catalog::TableConstraint::ForeignKey {
                                name: opt.name.as_ref().map(|n| n.value.clone()),
                                columns: vec![c.name.value.clone()],
                                foreign_table: fk.foreign_table.to_string(),
                                referred_columns: fk
                                    .referred_columns
                                    .iter()
                                    .map(|i| i.value.clone())
                                    .collect(),
                            });
                        }
                        _ => {}
                    }
                }
                cols.push(crate::ColumnDef {
                    name: c.name.value.clone(),
                    data_type: c.data_type.to_string(),
                    nullable,
                    unique,
                    primary,
                    default,
                });
            }

            for tc in constraints {
                match tc {
                    sqlparser::ast::TableConstraint::Unique(uc) => {
                        for col in index_column_names(&uc.columns) {
                            if let Some(c) = cols.iter_mut().find(|c| c.name == col) {
                                c.unique = true;
                            }
                        }
                    }
                    sqlparser::ast::TableConstraint::PrimaryKey(pk) => {
                        for col in index_column_names(&pk.columns) {
                            if let Some(c) = cols.iter_mut().find(|c| c.name == col) {
                                c.unique = true;
                                c.nullable = false;
                                c.primary = true;
                            }
                        }
                    }
                    sqlparser::ast::TableConstraint::Check(check) => {
                        tbl_constraints.push(nodus_catalog::TableConstraint::Check {
                            name: check.name.as_ref().map(|n| n.value.clone()),
                            expr: check.expr.to_string(),
                        });
                    }
                    sqlparser::ast::TableConstraint::ForeignKey(fk) => {
                        tbl_constraints.push(nodus_catalog::TableConstraint::ForeignKey {
                            name: fk.name.as_ref().map(|n| n.value.clone()),
                            columns: fk.columns.iter().map(|c| c.value.clone()).collect(),
                            foreign_table: fk.foreign_table.to_string(),
                            referred_columns: fk
                                .referred_columns
                                .iter()
                                .map(|i| i.value.clone())
                                .collect(),
                        });
                    }
                    _ => {}
                }
            }

            Ok(LogicalPlan::CreateTable {
                name: table_name,
                columns: cols,
                constraints: tbl_constraints,
                if_not_exists: create_table.if_not_exists,
            })
        }
        Statement::CreateView(create_view) => Ok(LogicalPlan::CreateView {
            name: create_view.name.to_string(),
            query: Box::new(plan_query(&create_view.query, params)?),
        }),
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            ..
        } => {
            let name = names
                .first()
                .ok_or_else(|| anyhow::anyhow!("DROP without a name"))?
                .to_string();
            match object_type {
                sqlparser::ast::ObjectType::Table => Ok(LogicalPlan::DropTable {
                    name,
                    if_exists: *if_exists,
                }),
                sqlparser::ast::ObjectType::View => Ok(LogicalPlan::DropView {
                    name,
                    if_exists: *if_exists,
                }),
                sqlparser::ast::ObjectType::Schema => Ok(LogicalPlan::DropSchema {
                    schema_name: name,
                    if_exists: *if_exists,
                    cascade: *cascade,
                }),
                sqlparser::ast::ObjectType::Index => Ok(LogicalPlan::DropIndex {
                    name,
                    if_exists: *if_exists,
                }),
                _ => anyhow::bail!("Unsupported DROP object type: {:?}", object_type),
            }
        }
        Statement::CreateIndex(create_index) => {
            let idx_name = create_index
                .name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unnamed_idx".to_string());
            let cols = create_index
                .columns
                .iter()
                .filter_map(|c| extract_col_name(&c.column.expr))
                .collect();
            Ok(LogicalPlan::CreateIndex {
                name: idx_name,
                table_name: create_index.table_name.to_string(),
                columns: cols,
                unique: create_index.unique,
                if_not_exists: create_index.if_not_exists,
            })
        }
        Statement::CreateRole(create_role) => {
            let name = create_role
                .names
                .first()
                .ok_or_else(|| anyhow::anyhow!("CREATE ROLE without a name"))?
                .to_string();
            Ok(LogicalPlan::CreateRole { name })
        }
        Statement::Grant(grant) => {
            let privilege = match &grant.privileges {
                sqlparser::ast::Privileges::Actions(actions) => actions
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "ALL".to_string()),
                _ => "ALL".to_string(),
            };
            let grantee = grant
                .grantees
                .first()
                .ok_or_else(|| anyhow::anyhow!("GRANT without grantee"))?
                .to_string();
            if let Some(GrantObjects::Tables(tables)) = &grant.objects {
                let object_name = tables
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("GRANT without table name"))?
                    .to_string();
                Ok(LogicalPlan::Grant {
                    privilege,
                    object_name,
                    grantee,
                })
            } else {
                anyhow::bail!("Unsupported GRANT target");
            }
        }
        Statement::Revoke(revoke) => {
            let privilege = match &revoke.privileges {
                sqlparser::ast::Privileges::Actions(actions) => actions
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "ALL".to_string()),
                _ => "ALL".to_string(),
            };
            let revokee = revoke
                .grantees
                .first()
                .ok_or_else(|| anyhow::anyhow!("REVOKE without revokee"))?
                .to_string();
            if let Some(GrantObjects::Tables(tables)) = &revoke.objects {
                let object_name = tables
                    .first()
                    .ok_or_else(|| anyhow::anyhow!("REVOKE without table name"))?
                    .to_string();
                Ok(LogicalPlan::Revoke {
                    privilege,
                    object_name,
                    revokee,
                })
            } else {
                anyhow::bail!("Unsupported REVOKE target");
            }
        }
        Statement::Insert(insert) => {
            let returning = if let Some(r) = &insert.returning {
                r.iter()
                    .filter_map(|item| match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(
                            sqlparser::ast::Expr::Identifier(id),
                        ) => Some(id.value.clone()),
                        _ => None,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let table_name = match &insert.table {
                sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                other => anyhow::bail!("Unsupported INSERT target: {:?}", other),
            };
            // Use the bare identifier, not `to_string()` (which re-quotes a quoted
            // ident like `"Id"`). CREATE TABLE stores unquoted names, so a quoted
            // INSERT column list — every client that quotes identifiers, e.g. EF
            // Core — must match against those.
            let cols: Vec<String> = insert
                .columns
                .iter()
                .map(|c| {
                    c.0.last()
                        .and_then(|part| part.as_ident())
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| c.to_string())
                })
                .collect();
            // An unquoted bare `DEFAULT` in a VALUES row means "use the
            // column default" (it parses as a plain identifier).
            let is_default_kw = |e: &sqlparser::ast::Expr| {
                matches!(e, sqlparser::ast::Expr::Identifier(id)
                    if id.quote_style.is_none() && id.value.eq_ignore_ascii_case("default"))
            };
            let mut values_list = Vec::new();
            let mut default_cells: Vec<Vec<bool>> = Vec::new();
            let mut any_default = false;
            if let Some(query) = &insert.source {
                if let SetExpr::Values(vs) = &*query.body {
                    for row in &vs.rows {
                        let mut row_values = Vec::new();
                        let mut row_defaults = Vec::new();
                        for e in &row.content {
                            if is_default_kw(e) {
                                row_values.push(crate::Value::Null);
                                row_defaults.push(true);
                                any_default = true;
                            } else {
                                row_values
                                    .push(expr_to_value(e, params).unwrap_or(crate::Value::Null));
                                row_defaults.push(false);
                            }
                        }
                        values_list.push(row_values);
                        default_cells.push(row_defaults);
                    }
                }
            } else {
                // `INSERT INTO t DEFAULT VALUES` — one row of all defaults.
                values_list.push(Vec::new());
                default_cells.push(Vec::new());
            }
            if !any_default {
                default_cells.clear();
            }
            let on_conflict = match &insert.on {
                Some(sqlparser::ast::OnInsert::OnConflict(oc)) => match &oc.action {
                    sqlparser::ast::OnConflictAction::DoNothing => {
                        Some(crate::plan_types::OnConflictClause::DoNothing)
                    }
                    sqlparser::ast::OnConflictAction::DoUpdate(du) => {
                        let assigns = du
                            .assignments
                            .iter()
                            .filter_map(|a| {
                                let col = match &a.target {
                                    sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                                        name.0.last()?.as_ident()?.value.clone()
                                    }
                                    sqlparser::ast::AssignmentTarget::Tuple(_) => return None,
                                };
                                let val = lower_scalar(&a.value, params)?;
                                Some((col, val))
                            })
                            .collect();
                        Some(crate::plan_types::OnConflictClause::DoUpdate(assigns))
                    }
                },
                _ => None,
            };
            Ok(LogicalPlan::Insert {
                table_name,
                columns: cols,
                values_list,
                returning,
                on_conflict,
                default_cells,
            })
        }
        Statement::Query(query) => plan_query(query, params),
        Statement::Update(update) => {
            let returning = if let Some(r) = &update.returning {
                r.iter()
                    .filter_map(|item| match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(
                            sqlparser::ast::Expr::Identifier(id),
                        ) => Some(id.value.clone()),
                        _ => None,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let table_name = table_name_of(&update.table.relation)?;
            let assigns = update
                .assignments
                .iter()
                .filter_map(|a| {
                    // Take the last identifier of the assignment target, e.g.
                    // `t.col = ...` -> `col`.
                    let col = match &a.target {
                        sqlparser::ast::AssignmentTarget::ColumnName(name) => {
                            name.0.last()?.as_ident()?.value.clone()
                        }
                        sqlparser::ast::AssignmentTarget::Tuple(_) => return None,
                    };
                    // `SET col = DEFAULT` (an unquoted bare identifier) becomes
                    // a sentinel the executor resolves to the column default.
                    if let sqlparser::ast::Expr::Identifier(id) = &a.value {
                        if id.quote_style.is_none() && id.value.eq_ignore_ascii_case("default") {
                            return Some((
                                col,
                                crate::plan_types::ScalarExpr::Function {
                                    name: "__COLUMN_DEFAULT__".to_string(),
                                    args: vec![],
                                },
                            ));
                        }
                    }
                    // Lower the RHS to a scalar expression so `SET n = n + 1`
                    // and other computed assignments evaluate per row (a bare
                    // literal lowers to `ScalarExpr::Literal`).
                    let val = lower_scalar(&a.value, params)?;
                    Some((col, val))
                })
                .collect();
            Ok(LogicalPlan::Update {
                table_name,
                assignments: assigns,
                filter: parse_predicates(&update.selection, params),
                returning,
            })
        }
        Statement::Delete(delete) => {
            let returning = if let Some(r) = &delete.returning {
                r.iter()
                    .filter_map(|item| match item {
                        sqlparser::ast::SelectItem::UnnamedExpr(
                            sqlparser::ast::Expr::Identifier(id),
                        ) => Some(id.value.clone()),
                        _ => None,
                    })
                    .collect()
            } else {
                Vec::new()
            };
            let tables = match &delete.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            let relation = &tables
                .first()
                .ok_or_else(|| anyhow::anyhow!("DELETE without a table"))?
                .relation;
            Ok(LogicalPlan::Delete {
                table_name: table_name_of(relation)?,
                filter: parse_predicates(&delete.selection, params),
                returning,
            })
        }
        Statement::StartTransaction { .. } => Ok(LogicalPlan::Begin),
        Statement::Commit { .. } => Ok(LogicalPlan::Commit),
        Statement::Rollback { savepoint, .. } => {
            if let Some(name) = savepoint {
                Ok(LogicalPlan::RollbackToSavepoint {
                    name: name.value.clone(),
                })
            } else {
                Ok(LogicalPlan::Rollback)
            }
        }
        Statement::Savepoint { name } => Ok(LogicalPlan::Savepoint {
            name: name.value.clone(),
        }),
        Statement::ReleaseSavepoint { name } => Ok(LogicalPlan::ReleaseSavepoint {
            name: name.value.clone(),
        }),
        Statement::ShowVariable { variable } => {
            let var_name = variable
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok(LogicalPlan::ShowVariable { variable: var_name })
        }
        // The `SET` family of statements is now wrapped in `Statement::Set(Set)`.
        Statement::Set(set) => match set {
            sqlparser::ast::Set::SingleAssignment {
                variable, values, ..
            } => {
                let var_name = variable.to_string();
                let var_val = values
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                Ok(LogicalPlan::SetVariable {
                    variable: var_name,
                    value: var_val,
                })
            }
            sqlparser::ast::Set::SetTransaction { .. } => Ok(LogicalPlan::SetVariable {
                variable: "transaction_isolation".to_string(),
                value: "read committed".to_string(),
            }),
            // `SET TIME ZONE <x>` is the SQL-standard spelling of `SET timezone = <x>`;
            // route it to the same per-session variable so it persists and `SHOW
            // TimeZone` reflects it (`DEFAULT`/`LOCAL` clear the override).
            sqlparser::ast::Set::SetTimeZone { value, .. } => Ok(LogicalPlan::SetVariable {
                variable: "timezone".to_string(),
                value: value.to_string(),
            }),
            other => anyhow::bail!("Unsupported SET statement: {:?}", other),
        },
        Statement::Discard { .. } => Ok(LogicalPlan::Noop {
            tag: "DISCARD ALL".to_string(),
        }),
        Statement::Deallocate { .. } => Ok(LogicalPlan::Noop {
            tag: "DEALLOCATE".to_string(),
        }),
        Statement::AlterTable(alter_table) => {
            let table_name = alter_table.name.to_string();
            let op = alter_table
                .operations
                .first()
                .ok_or_else(|| anyhow::anyhow!("ALTER TABLE without operations"))?;
            let alter_op = match op {
                sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let mut nullable = true;
                    for opt in &column_def.options {
                        if let sqlparser::ast::ColumnOption::NotNull = &opt.option {
                            nullable = false;
                        }
                    }
                    AlterTableOp::AddColumn {
                        name: column_def.name.value.clone(),
                        data_type: column_def.data_type.to_string(),
                        nullable,
                    }
                }
                sqlparser::ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => AlterTableOp::RenameColumn {
                    old_name: old_column_name.value.clone(),
                    new_name: new_column_name.value.clone(),
                },
                sqlparser::ast::AlterTableOperation::DropColumn { column_names, .. } => {
                    let name = column_names
                        .first()
                        .ok_or_else(|| anyhow::anyhow!("DROP COLUMN without a column name"))?
                        .value
                        .clone();
                    AlterTableOp::DropColumn { name }
                }
                sqlparser::ast::AlterTableOperation::AlterColumn {
                    column_name,
                    op: sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. },
                } => AlterTableOp::AlterColumnType {
                    name: column_name.value.clone(),
                    data_type: data_type.to_string(),
                },
                sqlparser::ast::AlterTableOperation::RenameTable { table_name } => {
                    let new_name = match table_name {
                        sqlparser::ast::RenameTableNameKind::As(name)
                        | sqlparser::ast::RenameTableNameKind::To(name) => name.to_string(),
                    };
                    AlterTableOp::RenameTable { new_name }
                }
                _ => anyhow::bail!("Unsupported ALTER TABLE operation: {:?}", op),
            };
            Ok(LogicalPlan::AlterTable {
                table_name,
                operation: alter_op,
            })
        }
        _ => anyhow::bail!("Unsupported SQL statement: {:?}", stmt),
    }
}
