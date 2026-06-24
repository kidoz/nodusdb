//! SQL AST → `LogicalPlan` planning: statement/query planning, predicate and
//! filter-expression parsing, CASE/operand extraction, and object-name
//! resolution.
//!
//! Crate types are imported explicitly (not via a glob) so they shadow the
//! function-local `use sqlparser::ast::*` imports, which also export a `Value`.

use crate::{
    AggregateOp, AlterTableOp, ColumnDef, CompareOp, FilterExpr, Join, JoinType, LogicalPlan,
    Operand, Predicate, ProjectionItem, SetOpKind, Value, coerce, column_type, literal_arg, render,
};
use anyhow::Result;
use nodus_catalog::TableConstraint;

pub fn expr_to_value(expr: &sqlparser::ast::Expr, params: &[crate::Value]) -> Option<crate::Value> {
    use sqlparser::ast::{Expr, Value as SqlValue};
    match expr {
        Expr::Value(SqlValue::SingleQuotedString(s)) => Some(crate::Value::Text(s.clone())),
        Expr::Value(SqlValue::Number(n, _)) => {
            if let Ok(i) = n.parse::<i64>() {
                Some(crate::Value::Int(i))
            } else if let Ok(f) = n.parse::<f64>() {
                Some(crate::Value::Float(f))
            } else {
                Some(crate::Value::Text(n.clone()))
            }
        }
        Expr::Value(SqlValue::Boolean(b)) => Some(crate::Value::Bool(*b)),
        Expr::Value(SqlValue::Null) => Some(crate::Value::Null),
        Expr::Value(SqlValue::Placeholder(s)) => {
            if let Some(stripped) = s.strip_prefix('$') {
                if let Ok(idx) = stripped.parse::<usize>() {
                    if idx > 0 && idx <= params.len() {
                        return Some(params[idx - 1].clone());
                    }
                }
            }
            None
        }
        Expr::Identifier(id) => Some(crate::Value::Text(id.value.clone())),
        Expr::Array(sqlparser::ast::Array { elem, .. }) => {
            let mut arr = Vec::new();
            for e in elem {
                if let Some(v) = expr_to_value(e, params) {
                    arr.push(v);
                } else {
                    return None;
                }
            }
            Some(crate::Value::Array(arr))
        }
        _ => None,
    }
}

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
                _ => anyhow::bail!("Unsupported DROP object type: {:?}", object_type),
            }
        }
        Statement::CreateIndex {
            name,
            table_name,
            columns,
            unique,
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

fn table_name_of(relation: &sqlparser::ast::TableFactor) -> Result<String> {
    match relation {
        sqlparser::ast::TableFactor::Table { name, .. } => Ok(name.to_string()),
        other => anyhow::bail!("Unsupported table relation: {:?}", other),
    }
}

pub fn parse_object_name(name: &str) -> Result<(&str, &str, &str)> {
    let parts: Vec<&str> = name.split('.').collect();
    match parts.len() {
        1 => Ok(("default", "public", parts[0].trim_matches('"'))),
        2 => Ok((
            "default",
            parts[0].trim_matches('"'),
            parts[1].trim_matches('"'),
        )),
        3 => Ok((
            parts[0].trim_matches('"'),
            parts[1].trim_matches('"'),
            parts[2].trim_matches('"'),
        )),
        _ => anyhow::bail!("Invalid object name: {}", name),
    }
}

fn compare_op(op: &sqlparser::ast::BinaryOperator) -> Option<CompareOp> {
    use sqlparser::ast::BinaryOperator::*;
    match op {
        Eq => Some(CompareOp::Eq),
        NotEq => Some(CompareOp::Ne),
        Lt => Some(CompareOp::Lt),
        LtEq => Some(CompareOp::Le),
        Gt => Some(CompareOp::Gt),
        GtEq => Some(CompareOp::Ge),
        Custom(s) if s == "@>" => Some(CompareOp::Contains),
        Custom(s) if s == "<@" => Some(CompareOp::ContainedBy),
        _ => None,
    }
}

/// Parses a `WHERE` clause into a conjunction of `column <op> literal`
/// predicates (AND only; other expressions are ignored).
fn parse_predicates(
    selection: &Option<sqlparser::ast::Expr>,
    params: &[Value],
) -> Option<FilterExpr> {
    if let Some(expr) = selection {
        parse_filter_expr(expr, params)
    } else {
        None
    }
}

pub(crate) fn parse_filter_expr(
    expr: &sqlparser::ast::Expr,
    params: &[Value],
) -> Option<FilterExpr> {
    use sqlparser::ast::{BinaryOperator, Expr};
    match expr {
        Expr::Nested(inner) => parse_filter_expr(inner, params),
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
            let l = parse_filter_expr(left, params);
            let r = parse_filter_expr(right, params);
            match (l, r) {
                (Some(l), Some(r)) => Some(FilterExpr::And(Box::new(l), Box::new(r))),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        Expr::BinaryOp { left, op, right } if *op == BinaryOperator::Or => {
            let l = parse_filter_expr(left, params);
            let r = parse_filter_expr(right, params);
            match (l, r) {
                (Some(l), Some(r)) => Some(FilterExpr::Or(Box::new(l), Box::new(r))),
                _ => None,
            }
        }
        Expr::UnaryOp { op, expr } if *op == sqlparser::ast::UnaryOperator::Not => {
            if let Some(inner) = parse_filter_expr(expr, params) {
                Some(FilterExpr::Not(Box::new(inner)))
            } else {
                None
            }
        }
        Expr::IsNull(expr) => extract_col_name(expr).map(FilterExpr::IsNull),
        Expr::IsNotNull(expr) => extract_col_name(expr).map(FilterExpr::IsNotNull),
        Expr::Like {
            negated,
            expr,
            pattern,
            ..
        } => {
            let left_col = extract_col_name(expr)?;
            let right_op = extract_operand(pattern, params)?;
            Some(FilterExpr::Like {
                left: left_col,
                right: right_op,
                negated: *negated,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let left_col = extract_col_name(expr)?;
            let mut ops = Vec::new();
            for item in list {
                if let Some(op) = extract_operand(item, params) {
                    ops.push(op);
                } else {
                    return None;
                }
            }
            Some(FilterExpr::InList {
                left: left_col,
                list: ops,
                negated: *negated,
            })
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let left_col = extract_col_name(expr)?;
            let sub_plan = plan_query(subquery, params).ok()?;
            Some(FilterExpr::InSubquery {
                left: left_col,
                subquery: Box::new(sub_plan),
                negated: *negated,
            })
        }
        Expr::BinaryOp { left, op, right } => {
            let left_col = extract_col_name(left)?;
            let right_op = extract_operand(right, params)?;
            if let Some(cmp) = compare_op(op) {
                Some(FilterExpr::Predicate(Predicate {
                    left: left_col,
                    op: cmp,
                    right: right_op,
                }))
            } else {
                None
            }
        }
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => {
            let left_col = extract_col_name(left)?;
            let right_op = extract_operand(right, params)?;
            let cmp = match operator {
                sqlparser::ast::JsonOperator::AtArrow => CompareOp::Contains,
                sqlparser::ast::JsonOperator::ArrowAt => CompareOp::ContainedBy,
                _ => return None,
            };
            Some(FilterExpr::Predicate(Predicate {
                left: left_col,
                op: cmp,
                right: right_op,
            }))
        }
        _ => None,
    }
}

fn extract_col_name(expr: &sqlparser::ast::Expr) -> Option<String> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => Some(id.value.clone()),
        Expr::CompoundIdentifier(ids) => Some(
            ids.iter()
                .map(|id| id.value.clone())
                .collect::<Vec<_>>()
                .join("."),
        ),
        Expr::JsonAccess {
            left,
            operator,
            right,
        } => {
            let left_col = extract_col_name(left)?;
            let right_val = match &**right {
                Expr::Value(v) => match v {
                    sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                    sqlparser::ast::Value::Number(n, _) => n.clone(),
                    _ => return None,
                },
                _ => return None,
            };
            let op_str = match operator {
                sqlparser::ast::JsonOperator::LongArrow => "->>",
                sqlparser::ast::JsonOperator::Arrow => "->",
                sqlparser::ast::JsonOperator::HashArrow => "#>",
                sqlparser::ast::JsonOperator::HashLongArrow => "#>>",
                _ => return None,
            };
            Some(format!("{}{}'{}'", left_col, op_str, right_val))
        }
        Expr::Cast { expr, .. } => extract_col_name(expr),
        _ => None,
    }
}

fn parse_simple_case_when_eq(
    expr: &sqlparser::ast::Expr,
    alias: Option<String>,
    params: &[Value],
) -> Option<ProjectionItem> {
    use sqlparser::ast::{BinaryOperator, Expr};
    let Expr::Case {
        operand: None,
        conditions,
        results,
        else_result: Some(else_result),
    } = expr
    else {
        return None;
    };
    let condition = conditions.first()?;
    let then_expr = results.first()?;
    let Expr::BinaryOp { left, op, right } = condition else {
        return None;
    };
    if *op != BinaryOperator::Eq {
        return None;
    }
    let left = extract_col_name(left)?;
    let equals = expr_to_value(right, params)?;
    let (then_value, then_column) = if let Some(value) = expr_to_value(then_expr, params) {
        (value, None)
    } else {
        (Value::Null, Some(extract_col_name(then_expr)?))
    };
    let else_column = extract_col_name(else_result)?;
    Some(ProjectionItem::CaseWhenEq {
        left,
        equals,
        then_value,
        then_column,
        else_column,
        alias,
    })
}

fn extract_operand(expr: &sqlparser::ast::Expr, params: &[Value]) -> Option<Operand> {
    use sqlparser::ast::Expr;
    match expr {
        Expr::Identifier(id) => Some(Operand::Ident(id.value.clone())),
        Expr::CompoundIdentifier(ids) => Some(Operand::Ident(
            ids.iter()
                .map(|id| id.value.clone())
                .collect::<Vec<_>>()
                .join("."),
        )),
        _ => {
            if let Some(val) = expr_to_value(expr, params) {
                Some(Operand::Literal(val))
            } else {
                None
            }
        }
    }
}

fn plan_query(query: &sqlparser::ast::Query, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;

    let mut ctes = Vec::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_name = cte.alias.name.value.clone();
            let cte_plan = plan_query(&cte.query, params)?;
            ctes.push((cte_name, Box::new(cte_plan)));
        }
    }

    if let SetExpr::SetOperation {
        op,
        set_quantifier,
        left,
        right,
    } = &*query.body
    {
        let kind = match op {
            SetOperator::Union => SetOpKind::Union,
            SetOperator::Intersect => SetOpKind::Intersect,
            SetOperator::Except => SetOpKind::Except,
        };
        let all = *set_quantifier == SetQuantifier::All;
        let wrap = |body: &Box<SetExpr>| Query {
            with: None,
            body: body.clone(),
            order_by: vec![],
            limit: None,
            limit_by: vec![],
            offset: None,
            fetch: None,
            locks: vec![],
            for_clause: None,
        };
        let left_plan = plan_query(&wrap(left), params)?;
        let right_plan = plan_query(&wrap(right), params)?;
        return Ok(LogicalPlan::SetOp {
            op: kind,
            all,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
        });
    }

    let SetExpr::Select(select) = &*query.body else {
        anyhow::bail!("Unsupported query body");
    };

    if select.from.is_empty() {
        let mut values = Vec::new();
        for item in &select.projection {
            let (expr, alias) = match item {
                SelectItem::UnnamedExpr(expr) => (expr, "?column?".to_string()),
                SelectItem::ExprWithAlias { expr, alias } => (expr, alias.value.to_string()),
                _ => anyhow::bail!("Unsupported scalar select item"),
            };
            if let Some(val) = expr_to_value(expr, params) {
                values.push((alias, val));
            } else if let Expr::Function(func) = expr {
                let func_name = func.name.to_string();
                let rendered = if func_name.eq_ignore_ascii_case("version") {
                    "PostgreSQL 16.0 (NodusDB)".to_string()
                } else if func_name.eq_ignore_ascii_case("current_database") {
                    "default".to_string()
                } else if func_name.eq_ignore_ascii_case("current_schema") {
                    "public".to_string()
                } else if func_name.eq_ignore_ascii_case("current_user") {
                    "nodus".to_string()
                } else if func_name.eq_ignore_ascii_case("current_schemas") {
                    "{public}".to_string()
                } else if func_name.eq_ignore_ascii_case("round") {
                    "0".to_string()
                } else {
                    func_name
                };
                values.push((alias, crate::Value::Text(rendered)));
            } else if let Expr::Identifier(id) = expr {
                let rendered = if id.value.eq_ignore_ascii_case("current_user") {
                    "nodus".to_string()
                } else {
                    id.value.to_string()
                };
                values.push((alias, crate::Value::Text(rendered)));
            } else {
                values.push((alias, crate::Value::Int(0)));
            }
        }
        return Ok(LogicalPlan::SelectLiteral { values });
    }
    let (table_name, table_alias) = match &select.from[0].relation {
        TableFactor::Table { name, alias, .. } => (
            name.to_string(),
            alias.as_ref().map(|a| a.name.value.clone()),
        ),
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            let alias = alias
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Derived table requires an alias"))?
                .name
                .value
                .clone();
            let sub_plan = plan_query(subquery, params)?;
            ctes.push((alias.clone(), Box::new(sub_plan)));
            (alias, None)
        }
        other => anyhow::bail!("Unsupported FROM relation: {:?}", other),
    };

    let mut joins = Vec::new();
    for j in &select.from[0].joins {
        let (join_table_name, join_table_alias) = match &j.relation {
            TableFactor::Table { name, alias, .. } => (
                name.to_string(),
                alias.as_ref().map(|a| a.name.value.clone()),
            ),
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let alias = alias
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("Derived join requires an alias"))?
                    .name
                    .value
                    .clone();
                let sub_plan = plan_query(subquery, params)?;
                ctes.push((alias.clone(), Box::new(sub_plan)));
                (alias, None)
            }
            other => anyhow::bail!("Unsupported join relation: {:?}", other),
        };
        let (join_type, condition) = match &j.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr)) => {
                (JoinType::Inner, parse_filter_expr(expr, params))
            }
            JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                (JoinType::LeftOuter, parse_filter_expr(expr, params))
            }
            JoinOperator::RightOuter(JoinConstraint::On(expr)) => {
                (JoinType::RightOuter, parse_filter_expr(expr, params))
            }
            JoinOperator::FullOuter(JoinConstraint::On(expr)) => {
                (JoinType::FullOuter, parse_filter_expr(expr, params))
            }
            JoinOperator::CrossJoin => (JoinType::Cross, None),
            other => anyhow::bail!("Unsupported join operator: {:?}", other),
        };
        joins.push(crate::Join {
            table_name: join_table_name,
            table_alias: join_table_alias,
            condition,
            join_type,
        });
    }

    // Projection: `*` -> empty (all); otherwise plain column identifiers.
    let mut projection = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                projection.clear();
                break;
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Expr::Function(func) = expr {
                    let fname = func.name.to_string().to_uppercase();
                    if let Some(over) = &func.over {
                        let mut partition_by = Vec::new();
                        let mut order_by = Vec::new();
                        if let sqlparser::ast::WindowType::WindowSpec(spec) = over {
                            for expr in &spec.partition_by {
                                if let Some(col) = extract_col_name(expr) {
                                    partition_by.push(col);
                                }
                            }
                            for expr in &spec.order_by {
                                if let Some(col) = extract_col_name(&expr.expr) {
                                    order_by.push((col, expr.asc.unwrap_or(true)));
                                }
                            }
                        }
                        projection.push(ProjectionItem::WindowFunction {
                            func_name: fname,
                            partition_by,
                            order_by,
                            alias: None,
                        });
                    } else if fname.starts_with("PG_CATALOG.")
                        || fname.starts_with("PG_")
                        || fname.eq_ignore_ascii_case("FORMAT_TYPE")
                    {
                        // Dummy handling for system functions during introspection, just treat it as a string literal
                        projection.push(ProjectionItem::Column(fname));
                    } else {
                        match fname.as_str() {
                            "COUNT" | "SUM" | "MIN" | "MAX" => {
                                let op = match fname.as_str() {
                                    "COUNT" => AggregateOp::Count,
                                    "SUM" => AggregateOp::Sum,
                                    "MIN" => AggregateOp::Min,
                                    "MAX" => AggregateOp::Max,
                                    _ => unreachable!(),
                                };
                                let inner = if let Some(arg) = func.args.first() {
                                    match arg {
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(
                                                Expr::Identifier(id),
                                            ),
                                        ) => id.value.clone(),
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Wildcard,
                                        ) => "*".to_string(),
                                        _ => anyhow::bail!("Unsupported aggregate argument"),
                                    }
                                } else {
                                    anyhow::bail!("Aggregate function requires an argument");
                                };
                                projection.push(ProjectionItem::Aggregate(op, inner));
                            }
                            _ => {
                                let mut args = Vec::new();
                                for arg in &func.args {
                                    if let sqlparser::ast::FunctionArg::Unnamed(
                                        sqlparser::ast::FunctionArgExpr::Expr(e),
                                    ) = arg
                                    {
                                        if let Some(col) = extract_col_name(e) {
                                            args.push(col);
                                        } else if let Some(val) = expr_to_value(e, params) {
                                            args.push(literal_arg(&val));
                                        }
                                    }
                                }
                                projection.push(ProjectionItem::ScalarFunction {
                                    func_name: fname.clone(),
                                    args,
                                    alias: None, // Will fix later for ExprWithAlias
                                });
                            }
                        }
                    }
                } else if let Expr::JsonAccess {
                    left,
                    operator,
                    right,
                } = expr
                {
                    let left_col = extract_col_name(left)
                        .ok_or_else(|| anyhow::anyhow!("Invalid JSON left"))?;
                    let right_val = match &**right {
                        Expr::Value(v) => match v {
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            _ => anyhow::bail!("Unsupported JSON path"),
                        },
                        _ => anyhow::bail!("Unsupported JSON path"),
                    };
                    let op_str = match operator {
                        sqlparser::ast::JsonOperator::LongArrow => "->>",
                        sqlparser::ast::JsonOperator::Arrow => "->",
                        sqlparser::ast::JsonOperator::HashArrow => "#>",
                        sqlparser::ast::JsonOperator::HashLongArrow => "#>>",
                        _ => anyhow::bail!("Unsupported JSON operator"),
                    };
                    projection.push(ProjectionItem::JsonAccess {
                        left: left_col,
                        operator: op_str.to_string(),
                        right: right_val,
                        alias: None,
                    });
                } else if let Expr::Case { .. } = expr {
                    if let Some(case_projection) = parse_simple_case_when_eq(expr, None, params) {
                        projection.push(case_projection);
                    } else {
                        projection.push(ProjectionItem::Literal(crate::Value::Null));
                    }
                } else if let Some(col) = extract_col_name(expr) {
                    projection.push(ProjectionItem::Column(col));
                } else if let Some(val) = expr_to_value(expr, params) {
                    projection.push(ProjectionItem::Literal(val));
                } else {
                    anyhow::bail!("Unsupported projection item: {:?}", item);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Expr::Function(func) = expr {
                    let fname = func.name.to_string().to_uppercase();
                    if let Some(over) = &func.over {
                        let mut partition_by = Vec::new();
                        let mut order_by = Vec::new();
                        if let sqlparser::ast::WindowType::WindowSpec(spec) = over {
                            for expr in &spec.partition_by {
                                if let Some(col) = extract_col_name(expr) {
                                    partition_by.push(col);
                                }
                            }
                            for expr in &spec.order_by {
                                if let Some(col) = extract_col_name(&expr.expr) {
                                    order_by.push((col, expr.asc.unwrap_or(true)));
                                }
                            }
                        }
                        projection.push(ProjectionItem::WindowFunction {
                            func_name: fname,
                            partition_by,
                            order_by,
                            alias: Some(alias.value.clone()),
                        });
                    } else if fname.starts_with("PG_CATALOG.")
                        || fname.starts_with("PG_")
                        || fname.eq_ignore_ascii_case("FORMAT_TYPE")
                    {
                        projection.push(ProjectionItem::AliasedColumn(fname, alias.value.clone()));
                    } else {
                        match fname.as_str() {
                            "COUNT" | "SUM" | "MIN" | "MAX" => {
                                let op = match fname.as_str() {
                                    "COUNT" => AggregateOp::Count,
                                    "SUM" => AggregateOp::Sum,
                                    "MIN" => AggregateOp::Min,
                                    "MAX" => AggregateOp::Max,
                                    _ => unreachable!(),
                                };
                                let inner = if let Some(arg) = func.args.first() {
                                    match arg {
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(
                                                Expr::Identifier(id),
                                            ),
                                        ) => id.value.clone(),
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Wildcard,
                                        ) => "*".to_string(),
                                        _ => anyhow::bail!("Unsupported aggregate argument"),
                                    }
                                } else {
                                    anyhow::bail!("Aggregate function requires an argument");
                                };
                                projection.push(ProjectionItem::Aggregate(op, inner));
                            }
                            _ => {
                                let mut args = Vec::new();
                                for arg in &func.args {
                                    if let sqlparser::ast::FunctionArg::Unnamed(
                                        sqlparser::ast::FunctionArgExpr::Expr(e),
                                    ) = arg
                                    {
                                        if let Some(col) = extract_col_name(e) {
                                            args.push(col);
                                        } else if let Some(val) = expr_to_value(e, params) {
                                            args.push(literal_arg(&val));
                                        }
                                    }
                                }
                                projection.push(ProjectionItem::ScalarFunction {
                                    func_name: fname.clone(),
                                    args,
                                    alias: Some(alias.value.clone()),
                                });
                            }
                        }
                    }
                } else if let Expr::JsonAccess {
                    left,
                    operator,
                    right,
                } = expr
                {
                    let left_col = extract_col_name(left)
                        .ok_or_else(|| anyhow::anyhow!("Invalid JSON left"))?;
                    let right_val = match &**right {
                        Expr::Value(v) => match v {
                            sqlparser::ast::Value::SingleQuotedString(s) => s.clone(),
                            sqlparser::ast::Value::Number(n, _) => n.clone(),
                            _ => anyhow::bail!("Unsupported JSON path"),
                        },
                        _ => anyhow::bail!("Unsupported JSON path"),
                    };
                    let op_str = match operator {
                        sqlparser::ast::JsonOperator::LongArrow => "->>",
                        sqlparser::ast::JsonOperator::Arrow => "->",
                        sqlparser::ast::JsonOperator::HashArrow => "#>",
                        sqlparser::ast::JsonOperator::HashLongArrow => "#>>",
                        _ => anyhow::bail!("Unsupported JSON operator"),
                    };
                    projection.push(ProjectionItem::JsonAccess {
                        left: left_col,
                        operator: op_str.to_string(),
                        right: right_val,
                        alias: Some(alias.value.clone()),
                    });
                } else if let Expr::Case { .. } = expr {
                    if let Some(case_projection) =
                        parse_simple_case_when_eq(expr, Some(alias.value.clone()), params)
                    {
                        projection.push(case_projection);
                    } else {
                        projection.push(ProjectionItem::AliasedLiteral(
                            crate::Value::Text("TABLE".to_string()),
                            alias.value.clone(),
                        ));
                    }
                } else if let Some(col) = extract_col_name(expr) {
                    projection.push(ProjectionItem::AliasedColumn(col, alias.value.clone()));
                } else if let Some(val) = expr_to_value(expr, params) {
                    projection.push(ProjectionItem::AliasedLiteral(val, alias.value.clone()));
                } else {
                    projection.push(ProjectionItem::AliasedLiteral(
                        crate::Value::Null,
                        alias.value.clone(),
                    ));
                }
            }
            SelectItem::QualifiedWildcard(_, _) => {
                anyhow::bail!("Qualified wildcard not supported");
            }
        }
    }

    let mut group_by = Vec::new();
    match &select.group_by {
        sqlparser::ast::GroupByExpr::Expressions(exprs) => {
            for expr in exprs {
                if let sqlparser::ast::Expr::Identifier(id) = expr {
                    group_by.push(id.value.clone());
                }
            }
        }
        _ => {}
    }

    // ORDER BY first column, if present.
    let order_by = query
        .order_by
        .iter()
        .filter_map(|o| match &o.expr {
            Expr::Identifier(id) => Some((id.value.clone(), o.asc.unwrap_or(true))),
            _ => None,
        })
        .collect();

    // LIMIT <n>.
    let limit = query
        .limit
        .as_ref()
        .and_then(|e| expr_to_value(e, params).and_then(|v| render(&v).parse::<usize>().ok()));

    // OFFSET <n>.
    let offset = query.offset.as_ref().and_then(|o| {
        expr_to_value(&o.value, params).and_then(|v| render(&v).parse::<usize>().ok())
    });

    let distinct = select.distinct.is_some();

    Ok(LogicalPlan::Select {
        ctes,
        table_name,
        table_alias,
        joins,
        projection,
        group_by,
        filter: parse_predicates(&select.selection, params),
        order_by,
        limit,
        offset,
        distinct,
    })
}
