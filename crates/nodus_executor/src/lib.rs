#![allow(warnings)]
#![allow(clippy::collapsible_if, clippy::collapsible_match)]

use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use nodus_audit::{AuditEvent, AuditSink};
use nodus_authz::{Action, AuthzContext, AuthzEngine, AuthzRequest};
use nodus_catalog::{
    AuditEventId, CatalogReader, CatalogWriter, ColumnDescriptor, CreateTableRequest,
    DescriptorState, IndexId, MemoryCatalog, PrincipalId, ResourceRef, RoleId, TableId,
};
use nodus_storage_api::{KeyRange, KvEngine, Timestamp, TxnId};
use nodus_txn::TxnManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// A column definition parsed from `CREATE TABLE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub unique: bool,
}

/// A typed cell value. Rows are stored as `Vec<Value>` in table-column order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Null,
}

/// Logical column type derived from a SQL type name.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ColumnType {
    Int,
    Float,
    Bool,
    Text,
}

fn column_type(data_type: &str) -> ColumnType {
    let t = data_type.to_uppercase();
    if t.contains("INT") || t.contains("SERIAL") {
        ColumnType::Int
    } else if t.contains("FLOAT")
        || t.contains("DOUBLE")
        || t.contains("REAL")
        || t.contains("NUMERIC")
        || t.contains("DECIMAL")
    {
        ColumnType::Float
    } else if t.contains("BOOL") {
        ColumnType::Bool
    } else {
        ColumnType::Text
    }
}

/// Coerces a literal string into a typed value for the given column type.
/// Empty strings and unparseable numerics become `Null`.
fn coerce(raw: &str, ty: ColumnType) -> Value {
    if raw.is_empty() {
        return Value::Null;
    }
    match ty {
        ColumnType::Int => raw
            .parse::<i64>()
            .map(Value::Int)
            .unwrap_or(crate::Value::Null),
        ColumnType::Float => raw
            .parse::<f64>()
            .map(Value::Float)
            .unwrap_or(crate::Value::Null),
        ColumnType::Bool => raw
            .parse::<bool>()
            .map(Value::Bool)
            .unwrap_or(crate::Value::Null),
        ColumnType::Text => Value::Text(raw.to_string()),
    }
}

fn render(value: &Value) -> String {
    match value {
        Value::Int(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
    }
}

/// Orders two values of the same logical type. Mixed/None types fall back to
/// comparing rendered strings so ordering is always total.
fn compare(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        _ => render(a).cmp(&render(b)),
    }
}

/// Operand for a WHERE predicate or JOIN condition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Operand {
    Literal(Value),
    Ident(String),
}

/// Comparison operator in a `WHERE` predicate.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A single `left <op> right` predicate; a `WHERE` clause or `ON` clause is a conjunction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Predicate {
    pub left: String,
    pub op: CompareOp,
    pub right: Operand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterExpr {
    Predicate(Predicate),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    IsNull(String),
    IsNotNull(String),
    Like {
        left: String,
        right: Operand,
        negated: bool,
    },
    InList {
        left: String,
        list: Vec<Operand>,
        negated: bool,
    },
    InSubquery {
        left: String,
        subquery: Box<LogicalPlan>,
        negated: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    LeftOuter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    pub table_name: String,
    pub condition: Option<FilterExpr>,
    pub join_type: JoinType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AggregateOp {
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ProjectionItem {
    Column(String),
    AliasedColumn(String, String),
    Aggregate(AggregateOp, String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableOp {
    AddColumn {
        name: String,
        data_type: String,
        nullable: bool,
    },
    DropColumn {
        name: String,
    },
    RenameTable {
        new_name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    CreateSchema {
        schema_name: String,
        if_not_exists: bool,
    },
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
    },
    AlterTable {
        table_name: String,
        operation: AlterTableOp,
    },
    CreateIndex {
        name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
    },
    Insert {
        table_name: String,
        /// Target column names; empty means positional (table order).
        columns: Vec<String>,
        values_list: Vec<Vec<Value>>,
        returning: Vec<String>,
    },
    Select {
        table_name: String,
        joins: Vec<Join>,
        /// Projected column names; empty means all columns (`SELECT *`).
        projection: Vec<ProjectionItem>,
        group_by: Vec<String>,
        /// Conjunction of `WHERE` predicates; empty means no filter.
        filter: Option<FilterExpr>,
        /// Optional `ORDER BY (column, ascending)`.
        order_by: Vec<(String, bool)>,
        /// Optional `LIMIT`.
        limit: Option<usize>,
        /// Optional `OFFSET`.
        offset: Option<usize>,
        /// DISTINCT
        distinct: bool,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, Value)>,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
    },
    Delete {
        table_name: String,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
    },
    Begin,
    Commit,
    Rollback,
    ShowVariable {
        variable: String,
    },
    SetVariable {
        variable: String,
        value: String,
    },
    SelectLiteral {
        value: String,
    },
}

/// Result of executing a statement: a tag for non-row commands, and column
/// names + rows for queries.
#[derive(Debug, Default)]
#[derive(Clone)]
pub struct QueryOutput {
    pub columns: Vec<String>,
    pub types: Vec<String>,
    pub rows: Vec<Row>,
    pub tag: String,
}

impl QueryOutput {
    fn tag(tag: &str) -> Self {
        Self {
            columns: vec![],
            types: vec![],
            rows: vec![],
            tag: tag.to_string(),
        }
    }
}

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
        _ => None,
    }
}

pub fn plan_statement(stmt: &sqlparser::ast::Statement, params: &[Value]) -> Result<LogicalPlan> {
    use sqlparser::ast::*;
    match stmt {
        Statement::CreateSchema { schema_name, if_not_exists } => {
            let name = match schema_name {
                sqlparser::ast::SchemaName::Simple(name) => name.to_string(),
                _ => anyhow::bail!("Unsupported schema name format"),
            };
            Ok(LogicalPlan::CreateSchema {
                schema_name: name,
                if_not_exists: *if_not_exists,
            })
        }
        Statement::CreateTable { name, columns, .. } => {
            let table_name = name.0.last().map(|i| i.value.clone()).unwrap_or_else(|| name.to_string());
            let mut cols = Vec::new();
            for c in columns {
                let mut nullable = true;
                let mut unique = false;
                for opt in &c.options {
                    match &opt.option {
                        sqlparser::ast::ColumnOption::NotNull => nullable = false,
                        sqlparser::ast::ColumnOption::Unique { is_primary, .. } => {
                            unique = true;
                            if *is_primary {
                                nullable = false;
                            }
                        }
                        _ => {}
                    }
                }
                cols.push(crate::ColumnDef {
                    name: c.name.value.clone(),
                    data_type: c.data_type.to_string(),
                    nullable,
                    unique,
                });
            }
            Ok(LogicalPlan::CreateTable {
                name: table_name,
                columns: cols,
            })
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
        Statement::Rollback { .. } => Ok(LogicalPlan::Rollback),
        Statement::ShowVariable { variable } => {
            let var_name = variable
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".");
            Ok(LogicalPlan::ShowVariable { variable: var_name })
        }
        Statement::SetVariable { variable, value, .. } => {
            let var_name = variable.to_string();
            let var_val = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(" ");
            Ok(LogicalPlan::SetVariable {
                variable: var_name,
                value: var_val,
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

fn compare_op(op: &sqlparser::ast::BinaryOperator) -> Option<CompareOp> {
    use sqlparser::ast::BinaryOperator::*;
    match op {
        Eq => Some(CompareOp::Eq),
        NotEq => Some(CompareOp::Ne),
        Lt => Some(CompareOp::Lt),
        LtEq => Some(CompareOp::Le),
        Gt => Some(CompareOp::Gt),
        GtEq => Some(CompareOp::Ge),
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

fn parse_filter_expr(expr: &sqlparser::ast::Expr, params: &[Value]) -> Option<FilterExpr> {
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
        Expr::Cast { expr, .. } => extract_col_name(expr),
        _ => None,
    }
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
    let SetExpr::Select(select) = &*query.body else {
        anyhow::bail!("Unsupported query body");
    };

    // FROM-less single-item projections are scalar/literal selects.
    if select.from.is_empty() && select.projection.len() == 1 {
        return match &select.projection[0] {
            SelectItem::UnnamedExpr(expr) => {
                if let Some(val) = expr_to_value(expr, params) {
                    Ok(LogicalPlan::SelectLiteral {
                        value: render(&val),
                    })
                } else if let Expr::Function(func) = expr {
                    Ok(LogicalPlan::SelectLiteral {
                        value: func.name.to_string(),
                    })
                } else {
                    anyhow::bail!("Unsupported scalar select")
                }
            }
            _ => anyhow::bail!("Unsupported scalar select"),
        };
    }

    if select.from.is_empty() {
        anyhow::bail!("SELECT without FROM");
    }
    let table_name = match &select.from[0].relation {
        TableFactor::Table { name, .. } => name.to_string(),
        other => anyhow::bail!("Unsupported FROM relation: {:?}", other),
    };

    let mut joins = Vec::new();
    for j in &select.from[0].joins {
        let table_name = match &j.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            other => anyhow::bail!("Unsupported join relation: {:?}", other),
        };
        let (join_type, condition) = match &j.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr)) => {
                (JoinType::Inner, parse_filter_expr(expr, params))
            }
            JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
                (JoinType::LeftOuter, parse_filter_expr(expr, params))
            }
            other => anyhow::bail!("Unsupported join operator: {:?}", other),
        };
        joins.push(crate::Join {
            table_name,
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
                    if fname.starts_with("PG_CATALOG.") || fname.starts_with("PG_") || fname.eq_ignore_ascii_case("FORMAT_TYPE") {
                        // Dummy handling for system functions during introspection, just treat it as a string literal
                        projection.push(ProjectionItem::Column(fname));
                    } else {
                        let op = match fname.as_str() {
                            "COUNT" => AggregateOp::Count,
                            "SUM" => AggregateOp::Sum,
                            "MIN" => AggregateOp::Min,
                            "MAX" => AggregateOp::Max,
                            _ => anyhow::bail!("Unsupported aggregate function: {}", fname),
                        };
                        let inner = if let Some(arg) = func.args.first() {
                            match arg {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(id)),
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
                } else if let Some(col) = extract_col_name(expr) {
                    projection.push(ProjectionItem::Column(col));
                } else {
                    anyhow::bail!("Unsupported projection item: {:?}", item);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Expr::Function(func) = expr {
                    let fname = func.name.to_string().to_uppercase();
                    if fname.starts_with("PG_CATALOG.") || fname.starts_with("PG_") || fname.eq_ignore_ascii_case("FORMAT_TYPE") {
                        projection.push(ProjectionItem::AliasedColumn(fname, alias.value.clone()));
                    } else {
                        let op = match fname.as_str() {
                            "COUNT" => AggregateOp::Count,
                            "SUM" => AggregateOp::Sum,
                            "MIN" => AggregateOp::Min,
                            "MAX" => AggregateOp::Max,
                            _ => anyhow::bail!("Unsupported aggregate function: {}", fname),
                        };
                        let inner = if let Some(arg) = func.args.first() {
                            match arg {
                                sqlparser::ast::FunctionArg::Unnamed(
                                    sqlparser::ast::FunctionArgExpr::Expr(Expr::Identifier(id)),
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
                } else if let Some(col) = extract_col_name(expr) {
                    projection.push(ProjectionItem::AliasedColumn(col, alias.value.clone()));
                } else {
                    anyhow::bail!("Unsupported projection item: {:?}", item);
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
    let order_by = query.order_by.iter().filter_map(|o| match &o.expr {
        Expr::Identifier(id) => Some((id.value.clone(), o.asc.unwrap_or(true))),
        _ => None,
    }).collect();

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
        table_name,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PhysicalPlan {
    LocalInsert {
        table_id: TableId,
        id: String,
        name_val: String,
    },
    LocalPointGet {
        table_id: TableId,
        id: String,
    },
    LocalIndexScan {
        table_id: TableId,
        index_id: IndexId,
    },
    LocalUpdate {
        table_id: TableId,
    },
    LocalDelete {
        table_id: TableId,
    },
    DistributedRoute {
        plan: Box<PhysicalPlan>,
    },
}

pub struct ExecutionContext {
    pub session_id: String,
    /// Authenticated principal making the request; used for authorization.
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub authz_catalog_version: u64,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

pub trait Executor: Send + Sync {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput>;
    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>>;
}

// MVP implementation mapping to required interfaces
#[allow(dead_code)]
pub struct MemExecutor {
    catalog_reader: Arc<dyn CatalogReader>,
    catalog_writer: Arc<dyn CatalogWriter>,
    authz: Arc<dyn AuthzEngine>,
    audit: Arc<dyn AuditSink>,
    kv: Arc<dyn KvEngine>,
    txn: Arc<dyn TxnManager>,
    /// Active explicit transaction per session id (`BEGIN`..`COMMIT`/`ROLLBACK`).
    /// Keyed by session so one connection's transaction can't affect another's.
    active_txns: std::sync::RwLock<HashMap<String, (TxnId, Timestamp)>>,
}

impl MemExecutor {
    pub fn new(
        catalog_reader: Arc<dyn CatalogReader>,
        catalog_writer: Arc<dyn CatalogWriter>,
        authz: Arc<dyn AuthzEngine>,
        audit: Arc<dyn AuditSink>,
        kv: Arc<dyn KvEngine>,
        txn: Arc<dyn TxnManager>,
    ) -> Self {
        Self {
            catalog_reader,
            catalog_writer,
            authz,
            audit,
            kv,
            txn,
            active_txns: std::sync::RwLock::new(HashMap::new()),
        }
    }

    /// Builds an executor over durable components (custom LSM + catalog snapshot)
    pub fn persistent(
        audit: Arc<dyn AuditSink>,
        data_dir: &str,
        encryption_key: Option<[u8; 32]>,
    ) -> Result<(Arc<MemExecutor>, Arc<MemoryCatalog>)> {
        let path = std::path::Path::new(data_dir);
        std::fs::create_dir_all(path)?;
        let cat_path = path.join("catalog.json");
        let cat = Arc::new(MemoryCatalog::load_from_disk(cat_path)?);

        if cat.get_database("default").is_err() {
            let db = cat.create_database(nodus_catalog::CreateDatabaseRequest {
                id: nodus_catalog::DatabaseId::new(),
                name: "default".into(),
                owner_role_id: None,
            })?;
            cat.create_schema(nodus_catalog::CreateSchemaRequest {
                id: nodus_catalog::SchemaId::new(),
                database_id: db.id,
                name: "public".into(),
                owner_role_id: None,
                managed_access: false,
            })?;
        }

        let kv = Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(
            path,
            encryption_key,
        )?);
        let txn = Arc::new(nodus_txn::MemTxnManager::new());
        let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));

        let exec = Arc::new(MemExecutor::new(
            cat.clone(),
            cat.clone(),
            authz,
            audit,
            kv,
            txn,
        ));
        Ok((exec, cat))
    }
    /// Builds an executor over fresh in-memory components and returns it
    /// together with the shared catalog, so callers (e.g. the server) can seed
    /// principals/grants and an authenticator against the same catalog. Audit
    /// events are written to `audit`.
    pub fn shared(audit: Arc<dyn AuditSink>) -> (Arc<MemExecutor>, Arc<MemoryCatalog>) {
        let cat = Arc::new(MemoryCatalog::new());

        let db = cat.create_database(nodus_catalog::CreateDatabaseRequest {
            id: nodus_catalog::DatabaseId::new(),
            name: "default".into(),
            owner_role_id: None,
        }).unwrap();
        cat.create_schema(nodus_catalog::CreateSchemaRequest {
            id: nodus_catalog::SchemaId::new(),
            database_id: db.id,
            name: "public".into(),
            owner_role_id: None,
            managed_access: false,
        }).unwrap();

        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let txn = Arc::new(nodus_txn::MemTxnManager::new());
        let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));
        let exec = Arc::new(MemExecutor::new(
            cat.clone(),
            cat.clone(),
            authz,
            audit,
            kv,
            txn,
        ));
        (exec, cat)
    }

    /// Runs one MVCC garbage-collection pass at the transaction manager's safe
    /// watermark. Returns the number of versions reclaimed.
    pub fn run_gc(&self) -> Result<usize> {
        let watermark = self.txn.gc_watermark();
        self.kv.garbage_collect(watermark)
    }

    /// Exposes the underlying key-value engine, e.g., for backup payload extraction.
    pub fn kv(&self) -> Arc<dyn KvEngine> {
        self.kv.clone()
    }

    /// Read timestamp for a session: its active transaction's snapshot, or the
    /// latest committed state when the session has no open transaction.
    fn read_ts(&self, session: &str) -> Timestamp {
        match self.active_txns.read().unwrap().get(session) {
            Some((_, ts)) => *ts,
            None => u64::MAX,
        }
    }

    /// Returns the session's active txn id. Expects a transaction to be active.
    fn txn_for(&self, session: &str) -> Result<TxnId> {
        match self.active_txns.read().unwrap().get(session) {
            Some((tid, _)) => Ok(*tid),
            None => anyhow::bail!("No active transaction for session"),
        }
    }

    /// Scans all visible rows of a table, decoding each into typed values.
    fn scan_rows(&self, table_id: TableId, session: &str) -> Result<Vec<Vec<Value>>> {
        let read_ts = self.read_ts(session);
        let start = Bytes::from(format!("{}:", table_id));
        let end = Bytes::from(format!("{};", table_id));
        let mut rows = Vec::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            rows.push(serde_json::from_slice::<Vec<Value>>(&pair.value)?);
        }
        Ok(rows)
    }

    /// Scans a secondary index to quickly look up primary keys, then fetches those rows.
    fn index_scan(
        &self,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        table_id: TableId,
        session: &str,
    ) -> Result<Vec<Vec<Value>>> {
        let read_ts = self.read_ts(session);
        let prefix = format!("i:{}:{}:", index_id, render(index_val));
        let start = Bytes::from(prefix.clone());
        let end_prefix = format!("i:{}:{};", index_id, render(index_val));
        let end = Bytes::from(end_prefix);

        let mut rows = Vec::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            let key_str = String::from_utf8_lossy(&pair.key);
            if let Some(pk) = key_str.strip_prefix(&prefix) {
                // Fetch the actual row
                let row_key = Bytes::from(format!("{}:{}", table_id, pk));
                if let Ok(Some(row_val)) = self.kv.get(&row_key, read_ts) {
                    rows.push(serde_json::from_slice::<Vec<Value>>(&row_val)?);
                }
            }
        }
        Ok(rows)
    }

    fn check_unique_constraints(
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

        let new_pk = new_row.first().map(render).unwrap_or_default();

        for existing in self.scan_rows(tbl.id, session)? {
            let pk = existing.first().map(render).unwrap_or_default();
            if Some(pk.as_str()) == skip_pk {
                continue;
            }
            if pk == new_pk {
                anyhow::bail!("Unique constraint violation on primary key");
            }
            for (idx_name, col_idx) in &unique_col_indices {
                let existing_val = existing.get(*col_idx).unwrap_or(&Value::Null);
                let new_val = new_row.get(*col_idx).unwrap_or(&Value::Null);
                if existing_val != &Value::Null && existing_val == new_val {
                    anyhow::bail!("Unique constraint violation on index '{}'", idx_name);
                }
            }
        }
        Ok(())
    }

    /// Writes a row value at `key`, using the session's txn.
    fn write_row(&self, session: &str, key: String, value: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv
            .write_intent(txn_id, Bytes::from(key), Bytes::from(value))?;
        Ok(())
    }

    /// Tombstones `key`, using the session's txn.
    fn delete_row(&self, session: &str, key: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv.delete_intent(txn_id, Bytes::from(key))?;
        Ok(())
    }

    fn index_key(index_id: nodus_catalog::IndexId, index_val: &Value, pk: &str) -> String {
        format!("i:{}:{}:{}", index_id, render(index_val), pk)
    }

    fn write_index_entry(
        &self,
        session: &str,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        pk: &str,
    ) -> Result<()> {
        let key = Self::index_key(index_id, index_val, pk);
        self.write_row(session, key, "".to_string())
    }

    fn delete_index_entry(
        &self,
        session: &str,
        index_id: nodus_catalog::IndexId,
        index_val: &Value,
        pk: &str,
    ) -> Result<()> {
        let key = Self::index_key(index_id, index_val, pk);
        self.delete_row(session, key)
    }

    /// Evaluates predicates against a joined or single row.
    fn eval_operand(
        &self,
        row: &[Value],
        col_names: &[String],
        _columns: &[ColumnDescriptor],
        op: &Operand,
        expected_type: &str,
    ) -> Value {
        match op {
            Operand::Literal(val) => {
                match val {
                    Value::Text(s) => coerce(s, column_type(expected_type)),
                    _ => val.clone(), // already typed correctly if it was binary bound
                }
            }
            Operand::Ident(col) => {
                let idx = col_names
                    .iter()
                    .position(|c| c == col || c.ends_with(&format!(".{}", col)));
                idx.and_then(|i| row.get(i))
                    .cloned()
                    .unwrap_or(crate::Value::Null)
            }
        }
    }

    /// Evaluates a FilterExpr against a joined or single row.
        fn eval_filter(
        &self,
        ctx: &ExecutionContext,
        row: &[Value],
        col_names: &[String],
        columns: &[ColumnDescriptor],
        filter: Option<&FilterExpr>,
    ) -> Option<bool> {
        let Some(expr) = filter else {
            return Some(true);
        };
        match expr {
            FilterExpr::And(left, right) => {
                let l = self.eval_filter(ctx, row, col_names, columns, Some(left));
                let r = self.eval_filter(ctx, row, col_names, columns, Some(right));
                match (l, r) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (Some(true), Some(true)) => Some(true),
                    _ => None,
                }
            }
            FilterExpr::Or(left, right) => {
                let l = self.eval_filter(ctx, row, col_names, columns, Some(left));
                let r = self.eval_filter(ctx, row, col_names, columns, Some(right));
                match (l, r) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                }
            }
            FilterExpr::Not(inner) => self.eval_filter(ctx, row, col_names, columns, Some(inner)).map(|b| !b),
            FilterExpr::IsNull(col) => {
                let idx = col_names.iter().position(|c| c == col || c.ends_with(&format!(".{}", col)));
                if let Some(i) = idx {
                    Some(row.get(i).unwrap_or(&Value::Null) == &Value::Null)
                } else {
                    Some(false)
                }
            }
            FilterExpr::IsNotNull(col) => {
                let idx = col_names.iter().position(|c| c == col || c.ends_with(&format!(".{}", col)));
                if let Some(i) = idx {
                    Some(row.get(i).unwrap_or(&Value::Null) != &Value::Null)
                } else {
                    Some(false)
                }
            }
            FilterExpr::Predicate(p) => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == &p.left || c.ends_with(&format!(".{}", p.left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);
                let right_cell =
                    self.eval_operand(row, col_names, columns, &p.right, &columns[idx].data_type);

                if left_cell == &Value::Null || right_cell == Value::Null {
                    return None;
                }

                let ord = compare(left_cell, &right_cell);
                Some(match p.op {
                    CompareOp::Eq => *left_cell == right_cell,
                    CompareOp::Ne => *left_cell != right_cell,
                    CompareOp::Lt => ord == std::cmp::Ordering::Less,
                    CompareOp::Le => ord != std::cmp::Ordering::Greater,
                    CompareOp::Gt => ord == std::cmp::Ordering::Greater,
                    CompareOp::Ge => ord != std::cmp::Ordering::Less,
                })
            }
            FilterExpr::Like {
                left,
                right,
                negated,
            } => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == left || c.ends_with(&format!(".{}", left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);
                let right_cell =
                    self.eval_operand(row, col_names, columns, right, &columns[idx].data_type);

                if left_cell == &Value::Null || right_cell == Value::Null {
                    return None;
                }

                if let (Value::Text(l), Value::Text(r)) = (left_cell, right_cell) {
                    let regex_str = format!("^{}$", r.replace('%', ".*").replace('_', "."));
                    let is_match = regex::Regex::new(&regex_str)
                        .map(|re| re.is_match(l))
                        .unwrap_or(false);
                    Some(if *negated { !is_match } else { is_match })
                } else {
                    Some(false)
                }
            }
            FilterExpr::InList {
                left,
                list,
                negated,
            } => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == left || c.ends_with(&format!(".{}", left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);
                
                if left_cell == &Value::Null {
                    return None;
                }

                let mut is_match = false;
                let mut found_null = false;
                for op in list {
                    let right_cell =
                        self.eval_operand(row, col_names, columns, op, &columns[idx].data_type);
                    if right_cell == Value::Null {
                        found_null = true;
                    } else if *left_cell == right_cell {
                        is_match = true;
                        break;
                    }
                }
                
                if *negated {
                    if is_match {
                        Some(false)
                    } else if found_null {
                        None
                    } else {
                        Some(true)
                    }
                } else {
                    if is_match {
                        Some(true)
                    } else if found_null {
                        None
                    } else {
                        Some(false)
                    }
                }
            }
            FilterExpr::InSubquery {
                left,
                subquery,
                negated,
            } => {
                let left_idx = col_names
                    .iter()
                    .position(|c| c == left || c.ends_with(&format!(".{}", left)));
                let Some(idx) = left_idx else {
                    return Some(false);
                };
                let left_cell = row.get(idx).unwrap_or(&Value::Null);

                if left_cell == &Value::Null {
                    return None;
                }

                // Blocking execution
                let exec_res = self.execute_logical_inner(ctx, *subquery.clone());
                let out = exec_res.unwrap_or(QueryOutput {
                    columns: vec![],
                    types: vec![],
                    rows: vec![],
                    tag: String::new(),
                });

                let mut matches = false;
                let mut found_null = false;
                for r in out.rows {
                    if let Some(c) = r.values.first() {
                        let right_cell = coerce(&render(c), column_type(&columns[idx].data_type));
                        if right_cell == Value::Null {
                            found_null = true;
                        } else if *left_cell == right_cell {
                            matches = true;
                            break;
                        }
                    }
                }

                if *negated {
                    if matches {
                        Some(false)
                    } else if found_null {
                        None
                    } else {
                        Some(true)
                    }
                } else {
                    if matches {
                        Some(true)
                    } else if found_null {
                        None
                    } else {
                        Some(false)
                    }
                }
            }
        }
    }

fn row_matches(
        &self,
        ctx: &ExecutionContext,
        row: &[Value],
        columns: &[ColumnDescriptor],
        filter: Option<&FilterExpr>,
    ) -> bool {
        let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
        self.eval_filter(ctx, row, &col_names, columns, filter).unwrap_or(false)
    }

    /// Deny-by-default authorization gate for a single action on a resource.
    fn authorize(
        &self,
        ctx: &ExecutionContext,
        action: Action,
        resource: ResourceRef,
    ) -> Result<()> {
        let decision = self.authz.authorize(AuthzRequest {
            principal_id: ctx.principal_id,
            active_roles: ctx.active_roles.clone(),
            action: action.clone(),
            resource: resource.clone(),
            context: AuthzContext { database_id: None },
        })?;

        // Record every authorization decision to the audit trail.
        let _ = self.audit.record_event(AuditEvent {
            id: AuditEventId::new(),
            time: Utc::now(),
            actor: ctx.principal_id,
            action: action.to_privilege().to_string(),
            resource: Some(resource),
            source_ip: None,
            request_id: None,
            session_id: Some(ctx.session_id.clone()),
            query_id: None,
            reason: Some(format!("{:?}", decision.reason)),
            result: if decision.allowed {
                "Success".to_string()
            } else {
                "Denied".to_string()
            },
            error: if decision.allowed {
                None
            } else {
                Some("permission denied".to_string())
            },
            authz_catalog_version: Some(decision.catalog_version),
        });

        if !decision.allowed {
            anyhow::bail!("permission denied");
        }
        Ok(())
    }
}

// Temporary default constructor so we don't break existing setups
impl Default for MemExecutor {
    fn default() -> Self {
        let cat = Arc::new(nodus_catalog::MemoryCatalog::new());
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let txn = Arc::new(nodus_txn::MemTxnManager::new());
        let authz = Arc::new(nodus_authz::DefaultAuthzEngine::new(cat.clone()));
        let audit = Arc::new(nodus_audit::LogAuditSink);

        Self::new(cat.clone(), cat, authz, audit, kv, txn)
    }
}

impl Executor for MemExecutor {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput> {
        let is_txn_control = matches!(
            plan,
            LogicalPlan::Begin | LogicalPlan::Commit | LogicalPlan::Rollback
        );
        let is_read_only = matches!(
            plan,
            LogicalPlan::Select { .. } | LogicalPlan::SelectLiteral { .. }
        );
        let mut implicit_txn = None;

        if !is_txn_control
            && self
                .active_txns
                .read()
                .unwrap()
                .get(&ctx.session_id)
                .is_none()
        {
            let txn_record = self.txn.begin_txn()?;
            self.active_txns.write().unwrap().insert(
                ctx.session_id.clone(),
                (txn_record.txn_id, txn_record.read_ts),
            );
            implicit_txn = Some(txn_record.txn_id);
        }

        let result = self.execute_logical_inner(ctx, plan);

        if let Some(txn_id) = implicit_txn {
            self.active_txns.write().unwrap().remove(&ctx.session_id);
            match &result {
                Ok(_) => {
                    let commit_ts = self.txn.commit_txn(txn_id)?;
                    if !is_read_only {
                        self.kv.commit(txn_id, commit_ts)?;
                    }
                }
                Err(_) => {
                    let _ = self.txn.abort_txn(txn_id);
                    if !is_read_only {
                        let _ = self.kv.abort(txn_id);
                    }
                }
            }
        }
        result
    }

    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>> {
        self.execute_physical_inner(ctx, plan)
    }
}

impl MemExecutor {
    fn execute_logical_inner(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> Result<QueryOutput> {
        match plan {
            LogicalPlan::CreateSchema { schema_name, if_not_exists } => {
                let db = self.catalog_reader.get_database("default")?;
                self.authorize(ctx, Action::CreateSchema, ResourceRef::Database(db.id))?;
                match self.catalog_writer.create_schema(nodus_catalog::CreateSchemaRequest {
                    id: nodus_catalog::SchemaId::new(),
                    database_id: db.id,
                    name: schema_name,
                    owner_role_id: None,
                    managed_access: false,
                }) {
                    Ok(_) => Ok(QueryOutput::tag("CREATE SCHEMA")),
                    Err(e) => {
                        if if_not_exists && e.to_string().contains("already exists") {
                            Ok(QueryOutput::tag("CREATE SCHEMA"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::CreateTable { name, columns } => {
                let db = self.catalog_reader.get_database("default")?;
                let sch = self.catalog_reader.get_schema("default", "public")?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;
                let descriptors: Vec<_> = columns
                    .iter()
                    .map(|c| ColumnDescriptor {
                        id: nodus_catalog::ColumnId::new(),
                        name: c.name.clone(),
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        data_type: c.data_type.clone(),
                        nullable: c.nullable,
                    })
                    .collect();

                let mut unique_cols = Vec::new();
                for (c, d) in columns.iter().zip(descriptors.iter()) {
                    if c.unique {
                        unique_cols.push(d.clone());
                    }
                }

                let tbl = self.catalog_writer.create_table(CreateTableRequest {
                    id: nodus_catalog::TableId::new(),
                    database_id: db.id,
                    schema_id: sch.id,
                    name: name.clone(),
                    columns: descriptors,
                })?;

                for col in unique_cols {
                    let index = nodus_catalog::IndexDescriptor {
                        id: nodus_catalog::IndexId::new(),
                        name: format!("{}_{}_idx", name, col.name),
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        index_type: nodus_catalog::IndexType::Unique,
                        index_state: nodus_catalog::IndexState::Ready,
                        key_columns: vec![nodus_catalog::IndexColumn {
                            column_id: col.id,
                            descending: false,
                        }],
                        include_columns: vec![],
                        unique: true,
                        global: false,
                        predicate: None,
                        expressions: vec![],
                    };
                    self.catalog_writer.update_table_descriptor(
                        nodus_catalog::TableDescriptorChange::AddIndex {
                            table_id: tbl.id,
                            index,
                        },
                    )?;
                }

                Ok(QueryOutput::tag("CREATE TABLE"))
            }
            LogicalPlan::Insert {
                table_name,
                columns,
                values_list,

                returning,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
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

                    // Primary key = first column's rendered value.
                    let pk = row.first().map(render).unwrap_or_default();
                    let key = format!("{}:{}", tbl.id, pk);
                    let encoded = serde_json::to_string(&row)?;
                    self.write_row(&ctx.session_id, key, encoded)?;

                    // Maintain secondary indexes.
                    for idx in &tbl.indexes {
                        for kcol in &idx.key_columns {
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
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
                    let col_names: Vec<&str> =
                        tbl.columns.iter().map(|c| c.name.as_str()).collect();
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
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("INSERT 0 {}", inserted_count),
                        columns: returning.clone(),
                        types: returning.iter().map(|_| "VARCHAR".to_string()).collect(),
                        rows,
                        })
                }
            }
            LogicalPlan::Select {
                table_name,
                joins,
                projection,
                group_by,
                filter,
                order_by,
                limit,
                offset,
                distinct,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
                self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;

                let mut joined_columns = tbl.columns.clone();
                let mut col_names: Vec<String> = tbl
                    .columns
                    .iter()
                    .map(|c| format!("{}.{}", table_name, c.name))
                    .collect();

                let mut stored_rows = None;
                if let Some(FilterExpr::Predicate(Predicate {
                    left,
                    op: CompareOp::Eq,
                    right,
                })) = filter.as_ref()
                {
                    let col_name = left.split('.').last().unwrap_or(left);
                    if let Some(col) = tbl.columns.iter().find(|c| c.name == *col_name) {
                        for idx in &tbl.indexes {
                            if idx.key_columns.iter().any(|kc| kc.column_id == col.id) {
                                let val = self.eval_operand(&[], &[], &[], right, &col.data_type);
                                if let Ok(rows) =
                                    self.index_scan(idx.id, &val, tbl.id, &ctx.session_id)
                                {
                                    stored_rows = Some(rows);
                                    break;
                                }
                            }
                        }
                    }
                }
                let mut stored_rows = match stored_rows {
                    Some(rows) => rows,
                    None => self.scan_rows(tbl.id, &ctx.session_id)?,
                };

                for join in &joins {
                    let j_tbl =
                        self.catalog_reader
                            .get_table("default", "public", &join.table_name)?;
                    self.authorize(ctx, Action::Select, ResourceRef::Table(j_tbl.id))?;

                    let j_rows = self.scan_rows(j_tbl.id, &ctx.session_id)?;
                    let j_col_names: Vec<String> = j_tbl
                        .columns
                        .iter()
                        .map(|c| format!("{}.{}", join.table_name, c.name))
                        .collect();

                    let mut combined_cols = col_names.clone();
                    combined_cols.extend(j_col_names.clone());

                    let mut combined_desc = joined_columns.clone();
                    combined_desc.extend(j_tbl.columns.clone());

                    let mut next_rows = Vec::new();
                    for r1 in &stored_rows {
                        let mut matched = false;
                        for r2 in &j_rows {
                            let mut combined_row = r1.clone();
                            combined_row.extend(r2.clone());
                            if self.eval_filter(
                                ctx,
                                &combined_row,
                                &combined_cols,
                                &combined_desc,
                                join.condition.as_ref(),
                            ).unwrap_or(false) {
                                next_rows.push(combined_row);
                                matched = true;
                            }
                        }
                        if !matched && matches!(join.join_type, JoinType::LeftOuter) {
                            let mut combined_row = r1.clone();
                            // Left join requires filling the right side with NULLs
                            let num_nulls = j_tbl.columns.len();
                            combined_row.extend(vec![Value::Null; num_nulls]);
                            next_rows.push(combined_row);
                        }
                    }
                    stored_rows = next_rows;
                    col_names = combined_cols;
                    joined_columns = combined_desc;
                }

                // WHERE: conjunction of typed predicates.
                stored_rows.retain(|r| {
                    self.eval_filter(ctx, r, &col_names, &joined_columns, filter.as_ref()).unwrap_or(false)
                });

                // GROUP BY & Aggregation
                let is_agg = !group_by.is_empty()
                    || projection
                        .iter()
                        .any(|p| matches!(p, ProjectionItem::Aggregate(_, _)));

                if !is_agg {
                    if !order_by.is_empty() {
                        let mut order_indices = Vec::new();
                        for (ocol, asc) in &order_by {
                            let idx = col_names
                                .iter()
                                .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                            if let Some(i) = idx {
                                order_indices.push((i, *asc));
                            }
                        }
                        stored_rows.sort_by(|a, b| {
                            for (idx, asc) in &order_indices {
                                let ord = compare(
                                    a.get(*idx).unwrap_or(&crate::Value::Null),
                                    b.get(*idx).unwrap_or(&crate::Value::Null),
                                );
                                if ord != std::cmp::Ordering::Equal {
                                    return if *asc { ord } else { ord.reverse() };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                }

                let mut out_rows = Vec::new();
                let mut out_cols = Vec::new();

                if is_agg {
                    let mut groups: std::collections::BTreeMap<Vec<Vec<u8>>, Vec<Vec<Value>>> =
                        std::collections::BTreeMap::new();

                    let group_by_indices: Vec<Option<usize>> = group_by
                        .iter()
                        .map(|c| {
                            col_names
                                .iter()
                                .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                        })
                        .collect();

                    if stored_rows.is_empty() && group_by.is_empty() {
                        // Empty set but scalar agg like COUNT(*), yields one row
                        groups.insert(vec![], vec![]);
                    } else {
                        for r in stored_rows {
                            let key = group_by_indices
                                .iter()
                                .map(|i| {
                                    let val = i.and_then(|idx| r.get(idx)).unwrap_or(&Value::Null);
                                    // serialize for BTreeMap key
                                    serde_json::to_vec(val).unwrap_or_default()
                                })
                                .collect::<Vec<_>>();
                            groups.entry(key).or_default().push(r);
                        }
                    }

                    for (_k, group_rows) in groups {
                        let mut out_row = Vec::new();
                        for proj_item in &projection {
                            match proj_item {
                                ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                                    let idx = col_names
                                        .iter()
                                        .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)));
                                    // Take from first row of group
                                    out_row.push(
                                        group_rows
                                            .first()
                                            .and_then(|r| idx.and_then(|i| r.get(i)))
                                            .cloned()
                                            .unwrap_or(crate::Value::Null),
                                    );
                                }
                                ProjectionItem::Aggregate(op, inner) => {
                                    let mut idx = col_names.iter().position(|tc| {
                                        tc == inner || tc.ends_with(&format!(".{}", inner))
                                    });
                                    if inner == "*" {
                                        idx = Some(0); // For COUNT(*)
                                    }

                                    match op {
                                        AggregateOp::Count => {
                                            let count = if inner == "*" {
                                                group_rows.len() as i64
                                            } else {
                                                group_rows
                                                    .iter()
                                                    .filter(|r| {
                                                        idx.and_then(|i| r.get(i))
                                                            .map_or(false, |v| {
                                                                !matches!(v, Value::Null)
                                                            })
                                                    })
                                                    .count()
                                                    as i64
                                            };
                                            out_row.push(Value::Int(count));
                                        }
                                        AggregateOp::Sum => {
                                            let mut sum_int = 0i64;
                                            let mut sum_float = 0f64;
                                            let mut is_float = false;
                                            for r in &group_rows {
                                                if let Some(v) = idx.and_then(|i| r.get(i)) {
                                                    match v {
                                                        Value::Int(n) => {
                                                            if is_float {
                                                                sum_float += *n as f64
                                                            } else {
                                                                sum_int += n
                                                            }
                                                        }
                                                        Value::Float(f) => {
                                                            if !is_float {
                                                                sum_float = sum_int as f64;
                                                                is_float = true;
                                                            }
                                                            sum_float += f;
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                            if group_rows.is_empty() {
                                                out_row.push(Value::Null);
                                            } else if is_float {
                                                out_row.push(Value::Float(sum_float));
                                            } else {
                                                out_row.push(Value::Int(sum_int));
                                            }
                                        }
                                        AggregateOp::Min => {
                                            let mut min_val: Option<Value> = None;
                                            for r in &group_rows {
                                                if let Some(v) = idx.and_then(|i| r.get(i)) {
                                                    if !matches!(v, Value::Null) {
                                                        if let Some(cur) = &min_val {
                                                            if compare(v, cur)
                                                                == std::cmp::Ordering::Less
                                                            {
                                                                min_val = Some(v.clone());
                                                            }
                                                        } else {
                                                            min_val = Some(v.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            out_row.push(min_val.unwrap_or(crate::Value::Null));
                                        }
                                        AggregateOp::Max => {
                                            let mut max_val: Option<Value> = None;
                                            for r in &group_rows {
                                                if let Some(v) = idx.and_then(|i| r.get(i)) {
                                                    if !matches!(v, Value::Null) {
                                                        if let Some(cur) = &max_val {
                                                            if compare(v, cur)
                                                                == std::cmp::Ordering::Greater
                                                            {
                                                                max_val = Some(v.clone());
                                                            }
                                                        } else {
                                                            max_val = Some(v.clone());
                                                        }
                                                    }
                                                }
                                            }
                                            out_row.push(max_val.unwrap_or(crate::Value::Null));
                                        }
                                    }
                                }
                            }
                        }
                        out_rows.push(out_row);
                    }

                    out_cols = if projection.is_empty() {
                        col_names.clone()
                    } else {
                        projection
                            .iter()
                            .map(|p| match p {
                                ProjectionItem::Column(c) => c.clone(),
                                ProjectionItem::AliasedColumn(_, a) => a.clone(),
                                ProjectionItem::Aggregate(op, inner) => {
                                    format!("{:?}({})", op, inner)
                                }
                            })
                            .collect()
                    };
                } else {
                    out_cols = if projection.is_empty() {
                        col_names.clone()
                    } else {
                        projection
                            .iter()
                            .filter_map(|p| match p {
                                ProjectionItem::Column(c) => Some(c.clone()),
                                ProjectionItem::AliasedColumn(_, a) => Some(a.clone()),
                                _ => None,
                            })
                            .collect()
                    };

                    let indices: Vec<Option<usize>> = out_cols
                        .iter()
                        .enumerate()
                        .map(|(pi, c)| {
                            if projection.is_empty() {
                                col_names.iter().position(|tc| tc == c || tc.ends_with(&format!(".{}", c)))
                            } else {
                                let actual_col = match &projection[pi] {
                                    ProjectionItem::Column(c) => c,
                                    ProjectionItem::AliasedColumn(c, _) => c,
                                    _ => c,
                                };
                                col_names.iter().position(|tc| tc == actual_col || tc.ends_with(&format!(".{}", actual_col)))
                            }
                        })
                        .collect();

                    out_rows = stored_rows
                        .into_iter()
                        .map(|r| {
                            indices
                                .iter()
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx))
                                        .cloned()
                                        .unwrap_or(crate::Value::Null)
                                })
                                .collect::<Vec<Value>>()
                        })
                        .collect::<Vec<_>>();
                }

                // ORDER BY for aggregates (uses out_cols). For non-aggregates, it was already sorted.
                if is_agg {
                    if !order_by.is_empty() {
                        let mut order_indices = Vec::new();
                        for (ocol, asc) in &order_by {
                            let idx = out_cols
                                .iter()
                                .position(|c| c == ocol || c.ends_with(&format!(".{}", ocol)));
                            if let Some(i) = idx {
                                order_indices.push((i, *asc));
                            }
                        }
                        out_rows.sort_by(|a, b| {
                            for (idx, asc) in &order_indices {
                                let ord = compare(
                                    a.get(*idx).unwrap_or(&crate::Value::Null),
                                    b.get(*idx).unwrap_or(&crate::Value::Null),
                                );
                                if ord != std::cmp::Ordering::Equal {
                                    return if *asc { ord } else { ord.reverse() };
                                }
                            }
                            std::cmp::Ordering::Equal
                        });
                    }
                }

                // DISTINCT
                if distinct {
                    let mut seen = Vec::new();
                    out_rows.retain(|r| {
                        let is_seen = seen.iter().any(|s: &Vec<Value>| {
                            s.iter()
                                .zip(r.iter())
                                .all(|(va, vb)| compare(va, vb) == std::cmp::Ordering::Equal)
                        });
                        if is_seen {
                            false
                        } else {
                            seen.push(r.clone());
                            true
                        }
                    });
                }

                // OFFSET
                if let Some(o) = offset {
                    if o < out_rows.len() {
                        out_rows.drain(0..o);
                    } else {
                        out_rows.clear();
                    }
                }

                // LIMIT
                if let Some(n) = limit {
                    out_rows.truncate(n);
                }

                let rows = out_rows
                    .into_iter()
                    .map(|r| Row {
                        values: r,
                    })
                    .collect::<Vec<_>>();

                let tag = format!("SELECT {}", rows.len());
                let mut types = Vec::new();
                for c in &out_cols {
                    // Quick lookup for type. Default to VARCHAR.
                    let mut ty = "VARCHAR".to_string();
                    if let Ok(tbl_desc) = self.catalog_reader.get_table("default", "public", &table_name) {
                        if let Some(col_desc) = tbl_desc.columns.iter().find(|x| x.name == *c || format!("{}.{}", table_name, x.name) == *c) {
                            ty = col_desc.data_type.clone();
                        }
                    }
                    types.push(ty);
                }
                Ok(QueryOutput {
                    columns: out_cols,
                    types,
                    rows,
                    tag,
                    })
            }
            LogicalPlan::Update {
                table_name,
                assignments,
                filter,

                returning,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
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
                                Value::Text(s) => {
                                    coerce(s, column_type(&tbl.columns[idx].data_type))
                                }
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
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
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
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("UPDATE {updated}"),
                        columns: returning.clone(),
                        types: returning.iter().map(|_| "VARCHAR".to_string()).collect(),
                        rows,
                    })
                }
            }
            LogicalPlan::Delete {
                table_name,
                filter,
                returning,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
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
                            if let Some(pos) =
                                tbl.columns.iter().position(|c| c.id == kcol.column_id)
                            {
                                let index_val = row.get(pos).unwrap_or(&Value::Null);
                                self.delete_index_entry(
                                    &ctx.session_id,
                                    idx.id,
                                    index_val,
                                    &pk_str,
                                )?;
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
                    let col_names: Vec<&str> =
                        tbl.columns.iter().map(|c| c.name.as_str()).collect();
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
                                .map(|i| {
                                    i.and_then(|idx| r.get(idx)).cloned().unwrap_or(Value::Null)
                                })
                                .collect(),
                        })
                        .collect();
                    Ok(QueryOutput {
                        tag: format!("DELETE {deleted}"),
                        columns: returning.clone(),
                        types: returning.iter().map(|_| "VARCHAR".to_string()).collect(),
                        rows,
                    })
                }
            }
            LogicalPlan::AlterTable {
                table_name,
                operation,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;

                let change = match operation {
                    AlterTableOp::AddColumn {
                        name,
                        data_type,
                        nullable,
                    } => {
                        let column = ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name,
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type,
                            nullable,
                        };

                        // Migrate existing data to include the new column (as NULL)
                        for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                            let pk_str = row.first().map(render).unwrap_or_default();
                            let key = format!("{}:{}", tbl.id, pk_str);
                            row.push(Value::Null); // Append null for the new column
                            self.write_row(&ctx.session_id, key, serde_json::to_string(&row)?)?;
                        }

                        nodus_catalog::TableDescriptorChange::AddColumn {
                            table_id: tbl.id,
                            column,
                        }
                    }
                    AlterTableOp::DropColumn { name } => {
                        if let Some(col_idx) = tbl.columns.iter().position(|c| c.name == name) {
                            // Cannot drop primary key (assuming first column is PK for now)
                            if col_idx == 0 {
                                anyhow::bail!("Cannot drop primary key column");
                            }

                            // Migrate existing data to remove the column
                            for mut row in self.scan_rows(tbl.id, &ctx.session_id)? {
                                let pk_str = row.first().map(render).unwrap_or_default();
                                let key = format!("{}:{}", tbl.id, pk_str);
                                if col_idx < row.len() {
                                    row.remove(col_idx);
                                }
                                self.write_row(&ctx.session_id, key, serde_json::to_string(&row)?)?;
                            }
                        } else {
                            anyhow::bail!("Column {} not found", name);
                        }

                        nodus_catalog::TableDescriptorChange::DropColumn {
                            table_id: tbl.id,
                            column_name: name,
                        }
                    }
                    AlterTableOp::RenameTable { new_name } => {
                        nodus_catalog::TableDescriptorChange::RenameTable {
                            table_id: tbl.id,
                            new_name,
                        }
                    }
                };
                self.catalog_writer.update_table_descriptor(change)?;
                Ok(QueryOutput::tag("ALTER TABLE"))
            }
            LogicalPlan::CreateIndex {
                name,
                table_name,
                columns,
                unique,
            } => {
                let tbl = self
                    .catalog_reader
                    .get_table("default", "public", &table_name)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;

                let mut index_cols = Vec::new();
                for c in &columns {
                    if let Some(col) = tbl.columns.iter().find(|tc| tc.name == *c) {
                        index_cols.push(nodus_catalog::IndexColumn {
                            column_id: col.id,
                            descending: false,
                        });
                    } else {
                        anyhow::bail!("Column not found for index: {}", c);
                    }
                }

                let idx_type = if unique {
                    nodus_catalog::IndexType::Unique
                } else {
                    nodus_catalog::IndexType::LocalSecondary
                };

                let index = nodus_catalog::IndexDescriptor {
                    id: nodus_catalog::IndexId::new(),
                    name,
                    version: 1,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    state: DescriptorState::Public,
                    index_type: idx_type,
                    index_state: nodus_catalog::IndexState::Creating,
                    key_columns: index_cols,
                    include_columns: vec![],
                    unique,
                    global: false,
                    predicate: None,
                    expressions: vec![],
                };

                let change = nodus_catalog::TableDescriptorChange::AddIndex {
                    table_id: tbl.id,
                    index: index.clone(),
                };
                self.catalog_writer.update_table_descriptor(change)?;

                // Backfill existing rows into the new index
                let mut seen_values = std::collections::HashSet::new();
                for row in self.scan_rows(tbl.id, &ctx.session_id)? {
                    let pk_str = row.first().map(render).unwrap_or_default();
                    for kcol in &index.key_columns {
                        if let Some(pos) = tbl.columns.iter().position(|c| c.id == kcol.column_id) {
                            let index_val = row.get(pos).unwrap_or(&Value::Null);

                            if unique {
                                let val_str = render(index_val);
                                if val_str != "NULL" && !seen_values.insert(val_str) {
                                    // Set state to Failed/Dropping or just error out. We'd need to drop it, but we can just error for now.
                                    let _ = self.catalog_writer.update_index_state(
                                        tbl.id,
                                        index.id,
                                        nodus_catalog::IndexState::Dropping,
                                    );
                                    anyhow::bail!(
                                        "Unique constraint violation during index backfill for value: {:?}",
                                        index_val
                                    );
                                }
                            }

                            self.write_index_entry(&ctx.session_id, index.id, index_val, &pk_str)?;
                        }
                    }
                }

                self.catalog_writer.update_index_state(
                    tbl.id,
                    index.id,
                    nodus_catalog::IndexState::Ready,
                )?;
                Ok(QueryOutput::tag("CREATE INDEX"))
            }
            LogicalPlan::Begin => {
                let txn_record = self.txn.begin_txn()?;
                self.active_txns.write().unwrap().insert(
                    ctx.session_id.clone(),
                    (txn_record.txn_id, txn_record.read_ts),
                );
                Ok(QueryOutput::tag("BEGIN"))
            }
            LogicalPlan::Commit => {
                if let Some((txn_id, _)) = self.active_txns.write().unwrap().remove(&ctx.session_id)
                {
                    let commit_ts = self.txn.commit_txn(txn_id)?;
                    self.kv.commit(txn_id, commit_ts)?;
                }
                Ok(QueryOutput::tag("COMMIT"))
            }
            LogicalPlan::Rollback => {
                if let Some((txn_id, _)) = self.active_txns.write().unwrap().remove(&ctx.session_id)
                {
                    self.txn.abort_txn(txn_id)?;
                    self.kv.abort(txn_id)?;
                }
                Ok(QueryOutput::tag("ROLLBACK"))
            }
            LogicalPlan::ShowVariable { variable } => {
                let value = if variable.eq_ignore_ascii_case("search_path") {
                    "public".to_string()
                } else {
                    String::new()
                };
                Ok(QueryOutput {
                    columns: vec![variable],
                    types: vec!["VARCHAR".to_string()],
                    rows: vec![Row {
                        values: vec![Value::Text(value)],
                    }],
                    tag: "SHOW".into(),
                })
            }
            LogicalPlan::SetVariable { variable, value: _ } => {
                // Acknowledging SET requests to support clients like JDBC
                Ok(QueryOutput::tag("SET"))
            }
            LogicalPlan::SelectLiteral { value } => {
                let rendered = if value.eq_ignore_ascii_case("version") {
                    "PostgreSQL 16.0 (NodusDB)".to_string()
                } else {
                    value
                };
                Ok(QueryOutput {
                    columns: vec!["?column?".into()],
                    types: vec!["VARCHAR".to_string()],
                    rows: vec![Row {
                        values: vec![Value::Text(rendered)],
                    }],
                    tag: "SELECT 1".into(),
                })
            }
        }
    }

    fn execute_physical_inner(
        &self,
        ctx: &ExecutionContext,
        plan: PhysicalPlan,
    ) -> Result<Vec<Row>> {
        // Retained for the point-get path used by lower layers/tests.
        match plan {
            PhysicalPlan::LocalPointGet { table_id, id } => {
                let read_ts = self.read_ts(&ctx.session_id);
                let key = format!("{}:{}", table_id, id);
                match self.kv.get(key.as_bytes(), read_ts)? {
                    Some(val) => {
                        let row: Vec<Value> = serde_json::from_slice(&val).unwrap_or_default();
                        Ok(vec![Row {
                            values: row.into_iter().collect(),
                        }])
                    }
                    None => Ok(vec![]),
                }
            }
            _ => Ok(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
use super::render;

fn render_row(row: &Row) -> Vec<String> {
    row.values.iter().map(render).collect()
}

    use super::*;
    use nodus_audit::{AuditQuery, AuditQueryable, MemoryAuditSink};
    use nodus_catalog::{CreateRoleRequest, GrantPrivilegeRequest, PrincipalType};

    fn ctx_for(principal: PrincipalId) -> ExecutionContext {
        ExecutionContext {
            session_id: "test".to_string(),
            principal_id: principal,
            active_roles: vec![],
            authz_catalog_version: 1,
        }
    }

    pub fn cols(names: &[(&str, &str)]) -> Vec<ColumnDef> {
        names
            .iter()
            .map(|(n, t)| ColumnDef {
                name: n.to_string(),
                data_type: t.to_string(),
                nullable: true,
                unique: false,
            })
            .collect()
    }

    fn eq(col: &str, val: &str) -> Option<FilterExpr> {
        Some(FilterExpr::Predicate(Predicate {
            left: col.to_string(),
            op: CompareOp::Eq,
            right: Operand::Literal(crate::Value::Text(val.to_string())),
        }))
    }

    #[test]
    fn create_table_denied_then_allowed_by_grant() {
        let audit = Arc::new(MemoryAuditSink::new());
        let (exec, cat) = MemExecutor::shared(audit.clone());
        let user = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "bob".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        let ctx = ctx_for(user.id);
        let plan = || LogicalPlan::CreateTable {
            name: "t1".into(),
            columns: cols(&[("id", "INT"), ("name", "TEXT")]),
        };

        assert!(exec.execute_logical(&ctx, plan()).is_err());

        let sch = cat.get_schema("default", "public").unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: user.id,
            resource: ResourceRef::Schema(sch.id),
            privilege: "CREATE".into(),
        })
        .unwrap();
        assert!(exec.execute_logical(&ctx, plan()).is_ok());

        assert_eq!(
            audit
                .query(&AuditQuery {
                    result: Some("Denied".into()),
                    ..Default::default()
                })
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn create_insert_select_round_trip() {
        // Superuser so authz passes for all actions.
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "books".into(),
                columns: cols(&[("id", "INT"), ("title", "TEXT"), ("author", "TEXT")]),
            },
        )
        .unwrap();

        for (id, title, author) in [("1", "Dune", "Herbert"), ("2", "Foundation", "Asimov")] {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "books".into(),
                    columns: vec!["id".into(), "title".into(), "author".into()],
                    values_list: vec![vec![
                        Value::Text(id.into()),
                        Value::Text(title.into()),
                        Value::Text(author.into()),
                    ]],

                    returning: vec![],
                },
            )
            .unwrap();
        }

        // SELECT * returns all rows with all columns.
        let all = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    group_by: vec![],
                    table_name: "books".into(),
                    joins: vec![],
                    projection: vec![],
                    filter: None,
                    order_by: vec![],
                    limit: None,

                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(all.columns, vec!["books.id", "books.title", "books.author"]);
        assert_eq!(all.rows.len(), 2);

        // Projection + filter.
        let one = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    group_by: vec![],
                    table_name: "books".into(),
                    joins: vec![],
                    projection: vec![
                        ProjectionItem::Column("title".into()),
                        ProjectionItem::Column("author".into()),
                    ],
                    filter: eq("id", "2"),
                    order_by: vec![],
                    limit: None,

                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(one.columns, vec!["title", "author"]);
        assert_eq!(one.rows.len(), 1);
        assert_eq!(render_row(&one.rows[0]), vec!["Foundation", "Asimov"]);
    }

    #[test]
    fn typed_values_round_trip_and_filter_by_int() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "items".into(),
                columns: cols(&[("id", "INT"), ("name", "TEXT"), ("active", "BOOL")]),
            },
        )
        .unwrap();
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "items".into(),
                columns: vec!["id".into(), "name".into(), "active".into()],
                values_list: vec![vec![
                    Value::Text("7".into()),
                    Value::Text("widget".into()),
                    Value::Text("true".into()),
                ]],

                returning: vec![],
            },
        )
        .unwrap();

        // Filter on an INT column coerces the literal numerically.
        let out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    group_by: vec![],
                    table_name: "items".into(),
                    joins: vec![],
                    projection: vec![],
                    filter: eq("id", "7"),
                    order_by: vec![],
                    limit: None,

                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(out.rows.len(), 1);
        // Int renders without quotes, bool as true/false.
        assert_eq!(render_row(&out.rows[0]), vec!["7", "widget", "true"]);
    }

    #[test]
    fn update_and_delete_rows() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "t".into(),
                columns: cols(&[("id", "INT"), ("name", "TEXT")]),
            },
        )
        .unwrap();
        for (id, name) in [("1", "a"), ("2", "b"), ("3", "c")] {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "t".into(),
                    columns: vec!["id".into(), "name".into()],
                    values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                    returning: vec![],
                },
            )
            .unwrap();
        }

        // UPDATE one row.
        let out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Update {
                    table_name: "t".into(),
                    assignments: vec![("name".into(), Value::Text("B".into()))],
                    filter: eq("id", "2"),

                    returning: vec![],
                },
            )
            .unwrap();
        assert_eq!(out.tag, "UPDATE 1");

        let read = |filter: Option<FilterExpr>| {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Select {
                    group_by: vec![],
                    table_name: "t".into(),
                    joins: vec![],
                    projection: vec![ProjectionItem::Column("name".into())],
                    filter,
                    order_by: vec![],
                    limit: None,

                    offset: None,
                    distinct: false,
                },
            )
            .unwrap()
        };
        assert_eq!(render_row(&read(eq("id", "2")).rows[0]), vec!["B"]);

        // DELETE one row, then confirm it's gone and the rest remain.
        let out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Delete {
                    table_name: "t".into(),
                    filter: eq("id", "1"),

                    returning: vec![],
                },
            )
            .unwrap();
        assert_eq!(out.tag, "DELETE 1");
        assert_eq!(read(eq("id", "1")).rows.len(), 0);
        assert_eq!(read(None).rows.len(), 2);
    }

    #[test]
    fn test_join_execution() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        // Create authors
        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "authors".into(),
                columns: cols(&[("id", "INT"), ("name", "TEXT")]),
            },
        )
        .unwrap();

        // Create books
        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "books".into(),
                columns: cols(&[("id", "INT"), ("title", "TEXT"), ("author_id", "INT")]),
            },
        )
        .unwrap();

        for (id, name) in [("1", "Herbert"), ("2", "Asimov")] {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "authors".into(),
                    columns: vec!["id".into(), "name".into()],
                    values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                    returning: vec![],
                },
            )
            .unwrap();
        }

        for (id, title, author_id) in [
            ("10", "Dune", "1"),
            ("11", "Foundation", "2"),
            ("12", "Dune Messiah", "1"),
        ] {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "books".into(),
                    columns: vec!["id".into(), "title".into(), "author_id".into()],
                    values_list: vec![vec![
                        Value::Text(id.into()),
                        Value::Text(title.into()),
                        Value::Text(author_id.into()),
                    ]],

                    returning: vec![],
                },
            )
            .unwrap();
        }

        let join_plan = LogicalPlan::Select {
            group_by: vec![],
            table_name: "books".into(),
            joins: vec![Join {
                table_name: "authors".into(),
                condition: Some(FilterExpr::Predicate(Predicate {
                    left: "books.author_id".into(),
                    op: CompareOp::Eq,
                    right: Operand::Ident("authors.id".into()),
                })),
                join_type: JoinType::Inner,
            }],
            projection: vec![
                ProjectionItem::Column("books.title".into()),
                ProjectionItem::Column("authors.name".into()),
            ],
            filter: Some(FilterExpr::Predicate(Predicate {
                left: "authors.name".into(),
                op: CompareOp::Eq,
                right: Operand::Literal(Value::Text("Herbert".into())),
            })),
            order_by: vec![("books.id".into(), true)],
            limit: None,

            offset: None,
            distinct: false,
        };

        let out = exec.execute_logical(&ctx, join_plan).unwrap();
        assert_eq!(out.columns, vec!["books.title", "authors.name"]);
        assert_eq!(out.rows.len(), 2);
        assert_eq!(render_row(&out.rows[0]), vec!["Dune", "Herbert"]);
        assert_eq!(render_row(&out.rows[1]), vec!["Dune Messiah", "Herbert"]);
    }

    #[test]
    fn transactions_are_isolated_per_session() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();

        let ctx_a = ExecutionContext {
            session_id: "sess-a".into(),
            principal_id: admin.id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };
        let ctx_b = ExecutionContext {
            session_id: "sess-b".into(),
            principal_id: admin.id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };

        exec.execute_logical(
            &ctx_a,
            LogicalPlan::CreateTable {
                name: "t".into(),
                columns: cols(&[("id", "INT"), ("name", "TEXT")]),
            },
        )
        .unwrap();

        // Session A opens a transaction; session B does NOT.
        exec.execute_logical(&ctx_a, LogicalPlan::Begin).unwrap();

        // Session B auto-commits an insert while A's txn is open.
        exec.execute_logical(
            &ctx_b,
            LogicalPlan::Insert {
                table_name: "t".into(),
                columns: vec!["id".into(), "name".into()],
                values_list: vec![vec![Value::Text("1".into()), Value::Text("b".into())]],

                returning: vec![],
            },
        )
        .unwrap();

        // B sees its own committed row immediately (B has no open snapshot).
        let read = |ctx: &ExecutionContext| {
            exec.execute_logical(
                ctx,
                LogicalPlan::Select {
                    group_by: vec![],
                    table_name: "t".into(),
                    joins: vec![],
                    projection: vec![],
                    filter: None,
                    order_by: vec![],
                    limit: None,

                    offset: None,
                    distinct: false,
                },
            )
            .unwrap()
            .rows
            .len()
        };
        assert_eq!(
            read(&ctx_b),
            1,
            "session B sees its own auto-committed write"
        );

        // A COMMIT from B must not touch A's still-open transaction.
        exec.execute_logical(&ctx_b, LogicalPlan::Commit).unwrap();
        // A's transaction is still open and independently committable.
        exec.execute_logical(&ctx_a, LogicalPlan::Commit).unwrap();
        assert_eq!(read(&ctx_a), 1);
    }

    #[test]
    fn run_gc_is_safe_with_no_active_txns() {
        let exec = MemExecutor::default();
        assert_eq!(exec.run_gc().unwrap(), 0);
    }
    #[test]
    fn test_complex_filters() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "t".into(),
                columns: cols(&[("id", "int"), ("name", "text"), ("status", "text")]),
            },
        )
        .unwrap();

        let insert = |id: &str, name: &str, status: &str| {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "t".into(),
                    columns: vec![],
                    values_list: vec![vec![
                        Value::Text(id.into()),
                        Value::Text(name.into()),
                        Value::Text(status.into()),
                    ]],

                    returning: vec![],
                },
            )
            .unwrap();
        };

        insert("1", "alice", "active");
        insert("2", "bob", "inactive");
        insert("3", "charlie", "active");
        insert("4", "dave", "banned");

        let read = |sql: &str| {
            let mut stmts = nodus_sql::parse_sql(sql).unwrap();
            let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
            exec.execute_logical(&ctx, plan).unwrap().rows.len()
        };

        assert_eq!(
            read("SELECT * FROM t WHERE status = 'active' OR status = 'banned'"),
            3
        );
        assert_eq!(
            read("SELECT * FROM t WHERE status IN ('active', 'banned')"),
            3
        );
        assert_eq!(read("SELECT * FROM t WHERE name LIKE 'a%'"), 1);
        assert_eq!(read("SELECT * FROM t WHERE name LIKE '%e'"), 3); // alice, charlie, dave
        assert_eq!(read("SELECT * FROM t WHERE NOT status = 'active'"), 2);
    }

    #[test]
    fn test_left_outer_join() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = ctx_for(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "users".into(),
                columns: cols(&[("id", "int"), ("name", "text")]),
            },
        )
        .unwrap();

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "orders".into(),
                columns: cols(&[("id", "int"), ("user_id", "int"), ("amount", "int")]),
            },
        )
        .unwrap();

        let insert_user = |id: &str, name: &str| {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "users".into(),
                    columns: vec![],
                    values_list: vec![vec![Value::Text(id.into()), Value::Text(name.into())]],

                    returning: vec![],
                },
            )
            .unwrap();
        };

        let insert_order = |id: &str, uid: &str, amt: &str| {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "orders".into(),
                    columns: vec![],
                    values_list: vec![vec![
                        Value::Text(id.into()),
                        Value::Text(uid.into()),
                        Value::Text(amt.into()),
                    ]],

                    returning: vec![],
                },
            )
            .unwrap();
        };

        insert_user("1", "Alice");
        insert_user("2", "Bob");
        insert_user("3", "Charlie");

        insert_order("101", "1", "500");
        insert_order("102", "1", "300");
        insert_order("103", "3", "700");

        let read = |sql: &str| {
            let mut stmts = nodus_sql::parse_sql(sql).unwrap();
            let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
            exec.execute_logical(&ctx, plan).unwrap()
        };

        // Inner Join
        let inner = read("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        assert_eq!(inner.rows.len(), 3); // 2 for Alice, 0 for Bob, 1 for Charlie

        // Left Join
        let left = read("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
        assert_eq!(left.rows.len(), 4); // 2 for Alice, 1 for Bob (NULLs), 1 for Charlie

        // Let's verify Bob's row has NULLs
        let bob_row = left.rows.iter().find(|r| r.values[1] == Value::Text("Bob".to_string())).unwrap();
        assert_eq!(bob_row.values.len(), 5); // users(id, name) + orders(id, user_id, amount)
        assert_eq!(bob_row.values[2], Value::Null); // order.id
        assert_eq!(bob_row.values[3], Value::Null); // order.user_id
        assert_eq!(bob_row.values[4], Value::Null); // order.amount
    }
}

#[cfg(test)]
mod phase1_tests {
    use super::*;
    use crate::tests::cols;
    use nodus_audit::MemoryAuditSink;

    fn render_row(row: &Row) -> Vec<String> {
        row.values.iter().map(render).collect()
    }

    fn test_ctx(admin_id: PrincipalId) -> ExecutionContext {
        ExecutionContext {
            session_id: "test".into(),
            principal_id: admin_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        }
    }

    #[test]
    fn test_offset_distinct_returning() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: nodus_catalog::ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = test_ctx(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "t".into(),
                columns: cols(&[("id", "int"), ("val", "text")]),
            },
        )
        .unwrap();

        // 1. Multi-row INSERT with RETURNING
        let insert_out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "t".into(),
                    columns: vec!["id".into(), "val".into()],
                    values_list: vec![
                        vec![Value::Text("1".into()), Value::Text("A".into())],
                        vec![Value::Text("2".into()), Value::Text("B".into())],
                        vec![Value::Text("3".into()), Value::Text("A".into())],
                        vec![Value::Text("4".into()), Value::Text("C".into())],
                    ],
                    returning: vec!["id".into(), "val".into()],
                },
            )
            .unwrap();

        assert_eq!(insert_out.tag, "INSERT 0 4");
        assert_eq!(insert_out.rows.len(), 4);
        assert_eq!(render_row(&insert_out.rows[0]), vec!["1", "A"]);
        assert_eq!(render_row(&insert_out.rows[3]), vec!["4", "C"]);

        let read =
            |offset: Option<usize>, limit: Option<usize>, distinct: bool, proj: Vec<&str>| {
                let out = exec
                    .execute_logical(
                        &ctx,
                        LogicalPlan::Select {
                            group_by: vec![],
                            table_name: "t".into(),
                            joins: vec![],
                            projection: proj
                                .into_iter()
                                .map(|s| ProjectionItem::Column(s.to_string()))
                                .collect(),
                            filter: None,
                            order_by: vec![],
                            limit,
                            offset,
                            distinct,
                        },
                    )
                    .unwrap();
                out.rows
                    .into_iter()
                    .map(|r| render_row(&r).join(","))
                    .collect::<Vec<_>>()
            };

        // 2. OFFSET and LIMIT
        let p1 = read(None, Some(2), false, vec![]);
        assert_eq!(p1, vec!["1,A", "2,B"]);

        let p2 = read(Some(2), Some(2), false, vec![]);
        assert_eq!(p2, vec!["3,A", "4,C"]);

        let p3 = read(Some(3), None, false, vec![]);
        assert_eq!(p3, vec!["4,C"]);

        // 3. DISTINCT
        let dist = read(None, None, true, vec!["val"]);
        // Should only be A, B, C (3 items)
        assert_eq!(dist.len(), 3);
        assert!(dist.contains(&"A".to_string()));
        assert!(dist.contains(&"B".to_string()));
        assert!(dist.contains(&"C".to_string()));

        // 4. RETURNING on UPDATE
        let update_out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Update {
                    table_name: "t".into(),
                    assignments: vec![("val".into(), Value::Text("Z".into()))],
                    filter: Some(FilterExpr::Predicate(Predicate {
                        left: "id".into(),
                        op: CompareOp::Eq,
                        right: Operand::Literal(Value::Text("2".into())),
                    })),
                    returning: vec!["id".into(), "val".into()],
                },
            )
            .unwrap();
        assert_eq!(update_out.tag, "UPDATE 1");
        assert_eq!(update_out.rows.len(), 1);
        assert_eq!(render_row(&update_out.rows[0]), vec!["2", "Z"]);
    }
}

#[cfg(test)]
mod phase2_tests {
use super::{render, Row};

fn render_row(row: &Row) -> Vec<String> {
    row.values.iter().map(render).collect()
}

    use super::*;
    use crate::tests::cols;
    use nodus_audit::MemoryAuditSink;

    fn test_ctx(admin_id: PrincipalId) -> ExecutionContext {
        ExecutionContext {
            session_id: "test".into(),
            principal_id: admin_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        }
    }

    #[test]
    fn test_group_by_aggregates() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: nodus_catalog::ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = test_ctx(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "sales".into(),
                columns: cols(&[("id", "int"), ("category", "text"), ("amount", "int")]),
            },
        )
        .unwrap();

        let insert = |id: &str, cat: &str, amt: &str| {
            exec.execute_logical(
                &ctx,
                LogicalPlan::Insert {
                    table_name: "sales".into(),
                    columns: vec![],
                    values_list: vec![vec![
                        Value::Text(id.into()),
                        Value::Text(cat.into()),
                        Value::Text(amt.into()),
                    ]],
                    returning: vec![],
                },
            )
            .unwrap();
        };

        insert("1", "A", "10");
        insert("2", "A", "20");
        insert("3", "B", "15");
        insert("4", "C", "5");
        insert("5", "C", "5");
        insert("6", "C", "5");

        let read = |sql: &str| {
            let mut stmts = nodus_sql::parse_sql(sql).unwrap();
            let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
            let out = exec.execute_logical(&ctx, plan).unwrap();

            // To ignore unpredictable hashmap/btree iteration order of groups, we'll sort the output strings.
            let mut res: Vec<String> = out.rows.into_iter().map(|r| render_row(&r).join(",")).collect();
            res.sort();
            res
        };

        // 1. Group By with COUNT and SUM
        let p1 = read("SELECT category, COUNT(id), SUM(amount) FROM sales GROUP BY category");
        assert_eq!(p1, vec!["A,2,30", "B,1,15", "C,3,15",]);

        // 2. MIN / MAX
        let p2 = read("SELECT category, MIN(amount), MAX(amount) FROM sales GROUP BY category");
        assert_eq!(p2, vec!["A,10,20", "B,15,15", "C,5,5",]);

        // 3. Scalar Aggregation without GROUP BY
        let p3 = read("SELECT COUNT(*), SUM(amount), MAX(amount) FROM sales");
        assert_eq!(p3, vec!["6,60,20"]);

        // 4. Scalar empty aggregation
        // Delete all rows
        exec.execute_logical(
            &ctx,
            LogicalPlan::Delete {
                table_name: "sales".into(),
                filter: None,
                returning: vec![],
            },
        )
        .unwrap();

        let p4 = read("SELECT COUNT(*) FROM sales");
        assert_eq!(p4, vec!["0"]);
    }
}

#[cfg(test)]
mod phase3_tests {
use super::{render, Row};

fn render_row(row: &Row) -> Vec<String> {
    row.values.iter().map(render).collect()
}

    use super::*;
    use crate::tests::cols;
    use nodus_audit::MemoryAuditSink;

    fn test_ctx(admin_id: PrincipalId) -> ExecutionContext {
        ExecutionContext {
            session_id: "test".into(),
            principal_id: admin_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        }
    }

    #[test]
    fn test_ddl_and_subqueries() {
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: nodus_catalog::ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = test_ctx(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "employees".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: false,
                    },
                    ColumnDef {
                        name: "dept_id".into(),
                        data_type: "INT".into(),
                        nullable: true,
                        unique: false,
                    },
                ],
            },
        )
        .unwrap();

        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "employees".into(),
                columns: vec![],
                values_list: vec![
                    vec![
                        Value::Text("1".into()),
                        Value::Text("Alice".into()),
                        Value::Text("100".into()),
                    ],
                    vec![
                        Value::Text("2".into()),
                        Value::Text("Bob".into()),
                        Value::Text("200".into()),
                    ],
                ],
                returning: vec![],
            },
        )
        .unwrap();

        exec.execute_logical(
            &ctx,
            LogicalPlan::AlterTable {
                table_name: "employees".into(),
                operation: AlterTableOp::AddColumn {
                    name: "salary".into(),
                    data_type: "INT".into(),
                    nullable: true,
                },
            },
        )
        .unwrap();

        let tbl = cat.get_table("default", "public", "employees").unwrap();
        assert_eq!(tbl.columns.len(), 4);
        assert_eq!(tbl.columns[3].name, "salary");
        assert_eq!(tbl.indexes.len(), 1);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateIndex {
                name: "idx_emp_dept".into(),
                table_name: "employees".into(),
                columns: vec!["dept_id".into()],
                unique: false,
            },
        )
        .unwrap();

        let tbl = cat.get_table("default", "public", "employees").unwrap();
        assert_eq!(tbl.indexes.len(), 2);
        assert_eq!(tbl.indexes[1].name, "idx_emp_dept");

        exec.execute_logical(
            &ctx,
            LogicalPlan::AlterTable {
                table_name: "employees".into(),
                operation: AlterTableOp::RenameTable {
                    new_name: "staff".into(),
                },
            },
        )
        .unwrap();

        assert!(cat.get_table("default", "public", "employees").is_err());
        assert!(cat.get_table("default", "public", "staff").is_ok());

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "departments".into(),
                columns: cols(&[("id", "int"), ("name", "text")]),
            },
        )
        .unwrap();

        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "departments".into(),
                columns: vec![],
                values_list: vec![vec![
                    Value::Text("200".into()),
                    Value::Text("Engineering".into()),
                ]],
                returning: vec![],
            },
        )
        .unwrap();

        let subquery = LogicalPlan::Select {
            group_by: vec![],
            table_name: "departments".into(),
            joins: vec![],
            projection: vec![ProjectionItem::Column("id".into())],
            filter: None,
            order_by: vec![],
            limit: None,
            offset: None,
            distinct: false,
        };

        let check_out = exec.execute_logical(&ctx, subquery.clone()).unwrap();
        // Debugging output removed

        let filter = FilterExpr::InSubquery {
            left: "dept_id".into(),
            subquery: Box::new(subquery),
            negated: false,
        };

        let out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    group_by: vec![],
                    table_name: "staff".into(),
                    joins: vec![],
                    projection: vec![ProjectionItem::Column("name".into())],
                    filter: Some(filter),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();

        assert_eq!(out.rows.len(), 1);
        assert_eq!(render_row(&out.rows[0])[0], "Bob");
    }

    #[test]
    fn test_unique_constraints() {
        use super::*;
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: nodus_catalog::ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = test_ctx(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "users".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                    },
                    ColumnDef {
                        name: "email".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: true,
                    },
                ],
            },
        )
        .unwrap();

        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "users".into(),
                columns: vec![],
                values_list: vec![
                    vec![Value::Int(1), Value::Text("a@a.com".into())],
                    vec![Value::Int(2), Value::Text("b@b.com".into())],
                ],
                returning: vec![],
            },
        )
        .unwrap();

        let res = exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "users".into(),
                columns: vec![],
                values_list: vec![vec![Value::Int(1), Value::Text("c@c.com".into())]],
                returning: vec![],
            },
        );
        assert!(res.is_err());
        assert!(
            res.unwrap_err()
                .to_string()
                .contains("Unique constraint violation")
        );

        let res2 = exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "users".into(),
                columns: vec![],
                values_list: vec![vec![Value::Int(3), Value::Text("b@b.com".into())]],
                returning: vec![],
            },
        );
        assert!(res2.is_err());

        let res3 = exec.execute_logical(
            &ctx,
            LogicalPlan::Update {
                table_name: "users".into(),
                assignments: vec![("email".into(), Value::Text("a@a.com".into()))],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "id".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Int(2)),
                })),
                returning: vec![],
            },
        );
        assert!(res3.is_err());

        let res4 = exec.execute_logical(
            &ctx,
            LogicalPlan::Update {
                table_name: "users".into(),
                assignments: vec![("email".into(), Value::Text("b@b.com".into()))],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "id".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Int(2)),
                })),
                returning: vec![],
            },
        );
        assert!(res4.is_ok());
    }

    #[test]
    fn test_secondary_indexing() {
        use super::*;
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: nodus_catalog::ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = test_ctx(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "products".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                    },
                    ColumnDef {
                        name: "category".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: false,
                    },
                ],
            },
        )
        .unwrap();

        // 1. Insert rows before indexing
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "products".into(),
                columns: vec![],
                values_list: vec![
                    vec![Value::Int(1), Value::Text("A".into())],
                    vec![Value::Int(2), Value::Text("B".into())],
                    vec![Value::Int(3), Value::Text("A".into())],
                ],
                returning: vec![],
            },
        )
        .unwrap();

        // 2. Create index on category (should backfill)
        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateIndex {
                name: "idx_cat".into(),
                table_name: "products".into(),
                columns: vec!["category".into()],
                unique: false,
            },
        )
        .unwrap();

        // 3. Insert rows after indexing (should synchronously maintain index)
        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "products".into(),
                columns: vec![],
                values_list: vec![
                    vec![Value::Int(4), Value::Text("C".into())],
                    vec![Value::Int(5), Value::Text("A".into())],
                ],
                returning: vec![],
            },
        )
        .unwrap();

        // 4. Query using index
        let out = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "products".into(),
                    joins: vec![],
                    projection: vec![ProjectionItem::Column("id".into())],
                    group_by: vec![],
                    filter: Some(FilterExpr::Predicate(Predicate {
                        left: "category".into(),
                        op: CompareOp::Eq,
                        right: Operand::Literal(Value::Text("A".into())),
                    })),
                    order_by: vec![("id".into(), true)],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();

        assert_eq!(out.rows.len(), 3);
        assert_eq!(render_row(&out.rows[0])[0], "1");
        assert_eq!(render_row(&out.rows[1])[0], "3");
        assert_eq!(render_row(&out.rows[2])[0], "5");

        // 5. Update row (change category from A to B)
        exec.execute_logical(
            &ctx,
            LogicalPlan::Update {
                table_name: "products".into(),
                assignments: vec![("category".into(), Value::Text("B".into()))],
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "id".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Int(1)),
                })),
                returning: vec![],
            },
        )
        .unwrap();

        // Query category A again
        let out_a = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "products".into(),
                    joins: vec![],
                    projection: vec![ProjectionItem::Column("id".into())],
                    group_by: vec![],
                    filter: Some(FilterExpr::Predicate(Predicate {
                        left: "category".into(),
                        op: CompareOp::Eq,
                        right: Operand::Literal(Value::Text("A".into())),
                    })),
                    order_by: vec![("id".into(), true)],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(out_a.rows.len(), 2); // 3 and 5

        // Query category B
        let out_b = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "products".into(),
                    joins: vec![],
                    projection: vec![ProjectionItem::Column("id".into())],
                    group_by: vec![],
                    filter: Some(FilterExpr::Predicate(Predicate {
                        left: "category".into(),
                        op: CompareOp::Eq,
                        right: Operand::Literal(Value::Text("B".into())),
                    })),
                    order_by: vec![("id".into(), true)],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(out_b.rows.len(), 2); // 1 and 2

        // 6. Delete row
        exec.execute_logical(
            &ctx,
            LogicalPlan::Delete {
                table_name: "products".into(),
                filter: Some(FilterExpr::Predicate(Predicate {
                    left: "id".into(),
                    op: CompareOp::Eq,
                    right: Operand::Literal(Value::Int(2)),
                })),
                returning: vec![],
            },
        )
        .unwrap();

        // Query category B again
        let out_b2 = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "products".into(),
                    joins: vec![],
                    projection: vec![ProjectionItem::Column("id".into())],
                    group_by: vec![],
                    filter: Some(FilterExpr::Predicate(Predicate {
                        left: "category".into(),
                        op: CompareOp::Eq,
                        right: Operand::Literal(Value::Text("B".into())),
                    })),
                    order_by: vec![("id".into(), true)],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(out_b2.rows.len(), 1); // Only 1 should be left
        assert_eq!(render_row(&out_b2.rows[0])[0], "1");
    }

    #[test]
    fn test_alter_table_migrations() {
        use super::*;
        let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
        let admin = cat
            .create_role(nodus_catalog::CreateRoleRequest {
                id: nodus_catalog::PrincipalId::new(),
                name: "admin".into(),
                principal_type: nodus_catalog::PrincipalType::User,
                database_id: None,
            })
            .unwrap();
        cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
            id: nodus_catalog::GrantId::new(),
            principal_id: admin.id,
            resource: nodus_catalog::ResourceRef::System,
            privilege: "ALL".into(),
        })
        .unwrap();
        let ctx = test_ctx(admin.id);

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                name: "users".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: false,
                    },
                ],
            },
        )
        .unwrap();

        exec.execute_logical(
            &ctx,
            LogicalPlan::Insert {
                table_name: "users".into(),
                columns: vec![],
                values_list: vec![vec![Value::Int(1), Value::Text("Alice".into())]],
                returning: vec![],
            },
        )
        .unwrap();

        // Add column
        exec.execute_logical(
            &ctx,
            LogicalPlan::AlterTable {
                table_name: "users".into(),
                operation: AlterTableOp::AddColumn {
                    name: "age".into(),
                    data_type: "INT".into(),
                    nullable: true,
                },
            },
        )
        .unwrap();

        // Read to ensure column exists and is NULL
        let out1 = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "users".into(),
                    joins: vec![],
                    projection: vec![],
                    group_by: vec![],
                    filter: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(out1.rows[0].values.len(), 3);
        assert_eq!(out1.rows[0].values[2], Value::Null);

        // Update the new column
        exec.execute_logical(
            &ctx,
            LogicalPlan::Update {
                table_name: "users".into(),
                assignments: vec![("age".into(), Value::Int(30))],
                filter: None,
                returning: vec![],
            },
        )
        .unwrap();

        let out2 = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "users".into(),
                    joins: vec![],
                    projection: vec![],
                    group_by: vec![],
                    filter: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(render_row(&out2.rows[0])[2], "30");

        // Drop the column
        exec.execute_logical(
            &ctx,
            LogicalPlan::AlterTable {
                table_name: "users".into(),
                operation: AlterTableOp::DropColumn { name: "age".into() },
            },
        )
        .unwrap();

        let out3 = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    table_name: "users".into(),
                    joins: vec![],
                    projection: vec![],
                    group_by: vec![],
                    filter: None,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    distinct: false,
                },
            )
            .unwrap();
        assert_eq!(out3.rows[0].values.len(), 2);
    }
}
