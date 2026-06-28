//! Top-level statement planning (DDL/DML/query dispatch).
use super::*;
use crate::*;
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub fn plan_statement(stmt: &sqlparser::ast::Statement, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;
    match stmt {
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
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
        Statement::CreateTable {
            name,
            columns,
            constraints,
            ..
        } => {
            let table_name = name.to_string();
            let mut cols = Vec::new();
            let mut tbl_constraints = Vec::new();
            for c in columns {
                let mut nullable = true;
                let mut unique = false;
                let mut primary = false;
                for opt in &c.options {
                    match &opt.option {
                        sqlparser::ast::ColumnOption::NotNull => nullable = false,
                        sqlparser::ast::ColumnOption::Unique { is_primary, .. } => {
                            unique = true;
                            if *is_primary {
                                nullable = false;
                                primary = true;
                            }
                        }
                        sqlparser::ast::ColumnOption::Check(expr) => {
                            tbl_constraints.push(nodus_catalog::TableConstraint::Check {
                                name: opt.name.as_ref().map(|n| n.value.clone()),
                                expr: expr.to_string(),
                            });
                        }
                        sqlparser::ast::ColumnOption::ForeignKey {
                            foreign_table,
                            referred_columns,
                            ..
                        } => {
                            tbl_constraints.push(nodus_catalog::TableConstraint::ForeignKey {
                                name: opt.name.as_ref().map(|n| n.value.clone()),
                                columns: vec![c.name.value.clone()],
                                foreign_table: foreign_table.to_string(),
                                referred_columns: referred_columns
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
                });
            }

            for tc in constraints {
                match tc {
                    sqlparser::ast::TableConstraint::Unique { columns, .. } => {
                        for col in columns {
                            if let Some(c) = cols.iter_mut().find(|c| c.name == col.value) {
                                c.unique = true;
                            }
                        }
                    }
                    sqlparser::ast::TableConstraint::PrimaryKey { columns, .. } => {
                        for col in columns {
                            if let Some(c) = cols.iter_mut().find(|c| c.name == col.value) {
                                c.unique = true;
                                c.nullable = false;
                                c.primary = true;
                            }
                        }
                    }
                    sqlparser::ast::TableConstraint::Check { name, expr } => {
                        tbl_constraints.push(nodus_catalog::TableConstraint::Check {
                            name: name.as_ref().map(|n| n.value.clone()),
                            expr: expr.to_string(),
                        });
                    }
                    sqlparser::ast::TableConstraint::ForeignKey {
                        name,
                        columns,
                        foreign_table,
                        referred_columns,
                        ..
                    } => {
                        tbl_constraints.push(nodus_catalog::TableConstraint::ForeignKey {
                            name: name.as_ref().map(|n| n.value.clone()),
                            columns: columns.iter().map(|c| c.value.clone()).collect(),
                            foreign_table: foreign_table.to_string(),
                            referred_columns: referred_columns
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
            })
        }
        Statement::CreateView { name, query, .. } => Ok(LogicalPlan::CreateView {
            name: name.to_string(),
            query: Box::new(plan_query(query, params)?),
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
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            unique,
            if_not_exists,
            ..
        } => {
            let idx_name = name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unnamed_idx".to_string());
            let cols = columns
                .iter()
                .filter_map(|c| extract_col_name(&c.expr))
                .collect();
            Ok(LogicalPlan::CreateIndex {
                name: idx_name,
                table_name: table_name.to_string(),
                columns: cols,
                unique: *unique,
                if_not_exists: *if_not_exists,
            })
        }
        Statement::CreateRole { names, .. } => {
            let name = names
                .first()
                .ok_or_else(|| anyhow::anyhow!("CREATE ROLE without a name"))?
                .to_string();
            Ok(LogicalPlan::CreateRole { name })
        }
        Statement::Grant {
            privileges,
            objects,
            grantees,
            ..
        } => {
            let privilege = match privileges {
                sqlparser::ast::Privileges::Actions(actions) => actions
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "ALL".to_string()),
                _ => "ALL".to_string(),
            };
            let grantee = grantees
                .first()
                .ok_or_else(|| anyhow::anyhow!("GRANT without grantee"))?
                .to_string();
            if let GrantObjects::Tables(tables) = objects {
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
        Statement::Revoke {
            privileges,
            objects,
            grantees,
            ..
        } => {
            let privilege = match privileges {
                sqlparser::ast::Privileges::Actions(actions) => actions
                    .first()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| "ALL".to_string()),
                _ => "ALL".to_string(),
            };
            let revokee = grantees
                .first()
                .ok_or_else(|| anyhow::anyhow!("REVOKE without revokee"))?
                .to_string();
            if let GrantObjects::Tables(tables) = objects {
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
        Statement::Insert {
            table_name,
            columns,
            source,
            returning,
            ..
        } => {
            let returning = if let Some(r) = returning {
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
            let cols: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();
            let mut values_list = Vec::new();
            if let Some(query) = source {
                if let SetExpr::Values(vs) = &*query.body {
                    for row in &vs.rows {
                        let mut row_values = Vec::new();
                        for e in row {
                            row_values.push(expr_to_value(e, params).unwrap_or(crate::Value::Null));
                        }
                        values_list.push(row_values);
                    }
                }
            }
            Ok(LogicalPlan::Insert {
                table_name: table_name.to_string(),
                columns: cols,
                values_list,
                returning,
            })
        }
        Statement::Query(query) => plan_query(query, params),
        Statement::Update {
            table,
            assignments,
            selection,
            returning,
            ..
        } => {
            let returning = if let Some(r) = returning {
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
            let table_name = table_name_of(&table.relation)?;
            let assigns = assignments
                .iter()
                .filter_map(|a| {
                    let col = a.id.last()?.value.clone();
                    let val = expr_to_value(&a.value, params)?;
                    Some((col, val))
                })
                .collect();
            Ok(LogicalPlan::Update {
                table_name,
                assignments: assigns,
                filter: parse_predicates(selection, params),
                returning,
            })
        }
        Statement::Delete {
            from,
            selection,
            returning,
            ..
        } => {
            let returning = if let Some(r) = returning {
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
            let tables = match from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            let relation = &tables
                .first()
                .ok_or_else(|| anyhow::anyhow!("DELETE without a table"))?
                .relation;
            Ok(LogicalPlan::Delete {
                table_name: table_name_of(relation)?,
                filter: parse_predicates(selection, params),
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
        Statement::SetVariable {
            variable, value, ..
        } => {
            let var_name = variable.to_string();
            let var_val = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            Ok(LogicalPlan::SetVariable {
                variable: var_name,
                value: var_val,
            })
        }
        Statement::SetTransaction { .. } => Ok(LogicalPlan::SetVariable {
            variable: "transaction_isolation".to_string(),
            value: "read committed".to_string(),
        }),
        // `SET TIME ZONE <x>` is the SQL-standard spelling of `SET timezone = <x>`;
        // route it to the same per-session variable so it persists and `SHOW
        // TimeZone` reflects it (`DEFAULT`/`LOCAL` clear the override).
        Statement::SetTimeZone { value, .. } => Ok(LogicalPlan::SetVariable {
            variable: "timezone".to_string(),
            value: value.to_string(),
        }),
        Statement::Discard { .. } => Ok(LogicalPlan::Noop {
            tag: "DISCARD ALL".to_string(),
        }),
        Statement::Deallocate { .. } => Ok(LogicalPlan::Noop {
            tag: "DEALLOCATE".to_string(),
        }),
        Statement::AlterTable {
            name, operations, ..
        } => {
            let table_name = name.to_string();
            let op = operations
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
                sqlparser::ast::AlterTableOperation::DropColumn { column_name, .. } => {
                    AlterTableOp::DropColumn {
                        name: column_name.value.clone(),
                    }
                }
                sqlparser::ast::AlterTableOperation::AlterColumn {
                    column_name,
                    op: sqlparser::ast::AlterColumnOperation::SetDataType { data_type, .. },
                } => AlterTableOp::AlterColumnType {
                    name: column_name.value.clone(),
                    data_type: data_type.to_string(),
                },
                sqlparser::ast::AlterTableOperation::RenameTable {
                    table_name: new_table_name,
                } => AlterTableOp::RenameTable {
                    new_name: new_table_name.to_string(),
                },
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
