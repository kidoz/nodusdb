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
use nodus_storage_api::{IntentReplacement, KeyRange, KvEngine, Timestamp, TxnId};
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
    pub primary: bool,
}

/// A typed cell value. Rows are stored as `Vec<Value>` in table-column order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Array(Vec<Value>),
    Jsonb(serde_json::Value),
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
        Value::Bool(b) => {
            if *b {
                "t".to_string()
            } else {
                "f".to_string()
            }
        }
        Value::Array(a) => {
            let rendered: Vec<String> = a.iter().map(render).collect();
            format!("{{{}}}", rendered.join(","))
        }
        Value::Jsonb(j) => j.to_string(),
        Value::Null => String::new(),
    }
}

/// Encodes a literal projection-function argument back into the string form the
/// planner stores: `'text'` for strings, plain digits for numbers/bools. (The
/// projection model stores args as strings; [`resolve_scalar_arg`] parses them
/// back at evaluation time.)
fn literal_arg(value: &Value) -> String {
    match value {
        Value::Text(s) => format!("'{s}'"),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

/// Resolves one scalar-function argument to a value for a given row: a quoted
/// `'…'` literal, a numeric literal, or otherwise a column reference.
fn resolve_scalar_arg(arg: &str, row: &[Value], col_names: &[String]) -> Value {
    if arg.len() >= 2 && arg.starts_with('\'') && arg.ends_with('\'') {
        Value::Text(arg[1..arg.len() - 1].to_string())
    } else if let Ok(i) = arg.parse::<i64>() {
        Value::Int(i)
    } else if let Ok(f) = arg.parse::<f64>() {
        Value::Float(f)
    } else {
        col_names
            .iter()
            .position(|tc| tc == arg || tc.ends_with(&format!(".{arg}")))
            .and_then(|i| row.get(i))
            .cloned()
            .unwrap_or(Value::Null)
    }
}

/// Evaluates a scalar SQL function over already-resolved argument values.
/// Unknown functions yield `Null` (the prior behaviour). NULL propagation
/// follows SQL: most functions return NULL on a NULL primary argument.
fn eval_scalar_function(name: &str, args: &[Value]) -> Value {
    let as_text = |v: &Value| -> Option<String> {
        match v {
            Value::Null => None,
            Value::Text(s) => Some(s.clone()),
            other => Some(render(other)),
        }
    };
    let as_num = |v: &Value| -> Option<f64> {
        match v {
            Value::Int(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            Value::Text(s) => s.parse().ok(),
            _ => None,
        }
    };
    match name {
        "CONCAT" => Value::Text(
            args.iter()
                .filter(|v| **v != Value::Null)
                .map(render)
                .collect(),
        ),
        "UPPER" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.to_uppercase())),
        "LOWER" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.to_lowercase())),
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Int(s.chars().count() as i64)),
        "TRIM" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.trim().to_string())),
        "LTRIM" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.trim_start().to_string())),
        "RTRIM" => args
            .first()
            .and_then(&as_text)
            .map_or(Value::Null, |s| Value::Text(s.trim_end().to_string())),
        "COALESCE" => args
            .iter()
            .find(|v| **v != Value::Null)
            .cloned()
            .unwrap_or(Value::Null),
        "NULLIF" => {
            if args.len() == 2 && args[0] == args[1] {
                Value::Null
            } else {
                args.first().cloned().unwrap_or(Value::Null)
            }
        }
        "ABS" => match args.first() {
            Some(Value::Int(i)) => Value::Int(i.abs()),
            Some(Value::Float(f)) => Value::Float(f.abs()),
            _ => Value::Null,
        },
        "ROUND" => match args.first().and_then(&as_num) {
            Some(x) => {
                let digits = args.get(1).and_then(&as_num).unwrap_or(0.0) as i32;
                let factor = 10f64.powi(digits);
                Value::Float((x * factor).round() / factor)
            }
            None => Value::Null,
        },
        "REPLACE" => {
            if let (Some(s), Some(from), Some(to)) = (
                args.first().and_then(&as_text),
                args.get(1).and_then(&as_text),
                args.get(2).and_then(&as_text),
            ) {
                Value::Text(s.replace(&from, &to))
            } else {
                Value::Null
            }
        }
        "SUBSTR" | "SUBSTRING" => {
            let Some(s) = args.first().and_then(&as_text) else {
                return Value::Null;
            };
            let chars: Vec<char> = s.chars().collect();
            let start = args.get(1).and_then(&as_num).unwrap_or(1.0) as i64; // 1-based
            let start_idx = (start.max(1) - 1) as usize;
            let out: String = match args.get(2).and_then(&as_num) {
                Some(len) => chars
                    .iter()
                    .skip(start_idx)
                    .take(len.max(0.0) as usize)
                    .collect(),
                None => chars.iter().skip(start_idx).collect(),
            };
            Value::Text(out)
        }
        _ => Value::Null,
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
    Contains,    // @>
    ContainedBy, // <@
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
    RightOuter,
    FullOuter,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    pub table_name: String,
    pub table_alias: Option<String>,
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
    ScalarFunction {
        func_name: String,
        args: Vec<String>,
        alias: Option<String>,
    },
    JsonAccess {
        left: String,
        operator: String,
        right: String,
        alias: Option<String>,
    },
    CaseWhenEq {
        left: String,
        equals: crate::Value,
        then_value: crate::Value,
        then_column: Option<String>,
        else_column: String,
        alias: Option<String>,
    },
    WindowFunction {
        func_name: String,
        partition_by: Vec<String>,
        order_by: Vec<(String, bool)>, // (col_name, ascending)
        alias: Option<String>,
    },
    Literal(crate::Value),
    AliasedLiteral(crate::Value, String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableOp {
    AddColumn {
        name: String,
        data_type: String,
        nullable: bool,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
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
    DropSchema {
        schema_name: String,
        if_exists: bool,
        cascade: bool,
    },
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<nodus_catalog::TableConstraint>,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    CreateView {
        name: String,
        query: Box<LogicalPlan>,
    },
    DropView {
        name: String,
        if_exists: bool,
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
    CreateRole {
        name: String,
    },
    Grant {
        privilege: String,
        object_name: String,
        grantee: String,
    },
    Revoke {
        privilege: String,
        object_name: String,
        revokee: String,
    },
    Insert {
        table_name: String,
        /// Target column names; empty means positional (table order).
        columns: Vec<String>,
        values_list: Vec<Vec<Value>>,
        returning: Vec<String>,
    },
    Select {
        ctes: Vec<(String, Box<LogicalPlan>)>,
        table_name: String,
        table_alias: Option<String>,
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
    Savepoint {
        name: String,
    },
    RollbackToSavepoint {
        name: String,
    },
    ReleaseSavepoint {
        name: String,
    },
    ShowVariable {
        variable: String,
    },
    SetVariable {
        variable: String,
        value: String,
    },
    Noop {
        tag: String,
    },
    SelectLiteral {
        values: Vec<(String, crate::Value)>,
    },
    UnionAll {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
}

/// Result of executing a statement: a tag for non-row commands, and column
/// names + rows for queries.
#[derive(Debug, Default, Clone)]
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
        if *op == SetOperator::Union && *set_quantifier == SetQuantifier::All {
            let left_plan = plan_query(
                &Query {
                    with: None,
                    body: left.clone(),
                    order_by: vec![],
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                },
                params,
            )?;
            let right_plan = plan_query(
                &Query {
                    with: None,
                    body: right.clone(),
                    order_by: vec![],
                    limit: None,
                    limit_by: vec![],
                    offset: None,
                    fetch: None,
                    locks: vec![],
                    for_clause: None,
                },
                params,
            )?;
            return Ok(LogicalPlan::UnionAll {
                left: Box::new(left_plan),
                right: Box::new(right_plan),
            });
        }
        anyhow::bail!("Unsupported set operation");
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

#[derive(Clone)]
pub struct ExecutionContext {
    pub session_id: String,
    /// Authenticated principal making the request; used for authorization.
    pub principal_id: PrincipalId,
    pub active_roles: Vec<RoleId>,
    pub authz_catalog_version: u64,
}

#[derive(Debug, Clone)]
struct SavepointState {
    name: String,
    write_log_len: usize,
    overlay: HashMap<String, Option<String>>,
}

#[derive(Debug, Clone)]
struct ActiveTxn {
    txn_id: TxnId,
    read_ts: Timestamp,
    write_log: Vec<String>,
    overlay: HashMap<String, Option<String>>,
    savepoints: Vec<SavepointState>,
}

impl ActiveTxn {
    fn new(txn_id: TxnId, read_ts: Timestamp) -> Self {
        Self {
            txn_id,
            read_ts,
            write_log: Vec::new(),
            overlay: HashMap::new(),
            savepoints: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Value>,
}

pub trait Executor: Send + Sync {
    fn execute_logical(&self, ctx: &ExecutionContext, plan: LogicalPlan) -> Result<QueryOutput>;
    fn execute_physical(&self, ctx: &ExecutionContext, plan: PhysicalPlan) -> Result<Vec<Row>>;
}

/// Reserved KV key under which the catalog's serialized state is stored. The
/// leading NUL keeps it out of any `{table_id}:{pk}` row key space.
const CATALOG_STATE_KEY: &[u8] = b"\x00catalog\x00state";

/// A [`nodus_catalog::CatalogStore`] backed by a [`KvEngine`], so the catalog
/// persists into the same (crash-safe) store as user data — one durable
/// mechanism, one recovery path. Should wrap the node's *local* engine (a direct
/// materialization, like the meta store), not the routing engine.
pub struct KvCatalogStore {
    kv: Arc<dyn KvEngine>,
    last_ts: std::sync::atomic::AtomicU64,
}

impl KvCatalogStore {
    pub fn new(kv: Arc<dyn KvEngine>) -> Self {
        Self {
            kv,
            last_ts: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl nodus_catalog::CatalogStore for KvCatalogStore {
    fn load(&self) -> Option<Vec<u8>> {
        self.kv
            .get(CATALOG_STATE_KEY, u64::MAX)
            .ok()
            .flatten()
            .map(|b| b.to_vec())
    }

    fn save(&self, bytes: &[u8]) -> Result<()> {
        // Strictly-monotonic commit ts so a later save always supersedes earlier
        // ones (reads use `u64::MAX`); wall-clock alone could collide within a µs.
        use std::sync::atomic::Ordering::SeqCst;
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        let ts = loop {
            let last = self.last_ts.load(SeqCst);
            let next = wall.max(last + 1);
            if self
                .last_ts
                .compare_exchange(last, next, SeqCst, SeqCst)
                .is_ok()
            {
                break next;
            }
        };
        let txn = TxnId::new();
        self.kv.write_intent(
            txn,
            Bytes::from_static(CATALOG_STATE_KEY),
            Bytes::copy_from_slice(bytes),
        )?;
        self.kv.commit(txn, ts)?;
        Ok(())
    }
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
    active_txns: std::sync::RwLock<HashMap<String, ActiveTxn>>,
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
        // Build the KV engine first, then back the catalog with it so both share
        // one durable store and recovery path.
        let kv = Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(
            path,
            encryption_key,
        )?);
        let cat = Arc::new(MemoryCatalog::with_store(Arc::new(KvCatalogStore::new(
            kv.clone(),
        ))));

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

        let db = cat
            .create_database(nodus_catalog::CreateDatabaseRequest {
                id: nodus_catalog::DatabaseId::new(),
                name: "default".into(),
                owner_role_id: None,
            })
            .unwrap();
        cat.create_schema(nodus_catalog::CreateSchemaRequest {
            id: nodus_catalog::SchemaId::new(),
            database_id: db.id,
            name: "public".into(),
            owner_role_id: None,
            managed_access: false,
        })
        .unwrap();

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
            Some(txn) => txn.read_ts,
            None => u64::MAX,
        }
    }

    /// Returns the session's active txn id. Expects a transaction to be active.
    fn txn_for(&self, session: &str) -> Result<TxnId> {
        match self.active_txns.read().unwrap().get(session) {
            Some(txn) => Ok(txn.txn_id),
            None => anyhow::bail!("No active transaction for session"),
        }
    }

    /// Scans all visible rows of a table, decoding each into typed values.
    fn scan_rows(&self, table_id: TableId, session: &str) -> Result<Vec<Vec<Value>>> {
        let read_ts = self.read_ts(session);
        let start = Bytes::from(format!("{}:", table_id));
        let end = Bytes::from(format!("{};", table_id));
        let mut keyed_rows = std::collections::BTreeMap::new();
        for pair in self.kv.scan(KeyRange { start, end }, read_ts)? {
            let pair = pair?;
            keyed_rows.insert(
                String::from_utf8_lossy(&pair.key).to_string(),
                serde_json::from_slice::<Vec<Value>>(&pair.value)?,
            );
        }
        if let Some(txn) = self.active_txns.read().unwrap().get(session) {
            let start = format!("{}:", table_id);
            let end = format!("{};", table_id);
            for (key, value) in &txn.overlay {
                if key >= &start && key < &end {
                    match value {
                        Some(encoded) => {
                            keyed_rows.insert(key.clone(), serde_json::from_str(encoded)?);
                        }
                        None => {
                            keyed_rows.remove(key);
                        }
                    }
                }
            }
        }
        Ok(keyed_rows.into_values().collect())
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

    fn check_table_constraints(
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
                            if &f_row[ref_idx] == val {
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

    /// Writes a row value at `key`, using the session's txn.
    fn write_row(&self, session: &str, key: String, value: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv
            .write_intent(txn_id, Bytes::from(key.clone()), Bytes::from(value.clone()))?;
        if let Some(txn) = self.active_txns.write().unwrap().get_mut(session) {
            txn.write_log.push(key.clone());
            txn.overlay.insert(key, Some(value));
        }
        Ok(())
    }

    /// Tombstones `key`, using the session's txn.
    fn delete_row(&self, session: &str, key: String) -> Result<()> {
        let txn_id = self.txn_for(session)?;
        self.txn.track_write(txn_id, key.as_bytes().to_vec())?;
        self.kv.delete_intent(txn_id, Bytes::from(key.clone()))?;
        if let Some(txn) = self.active_txns.write().unwrap().get_mut(session) {
            txn.write_log.push(key.clone());
            txn.overlay.insert(key, None);
        }
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
            FilterExpr::Not(inner) => self
                .eval_filter(ctx, row, col_names, columns, Some(inner))
                .map(|b| !b),
            FilterExpr::IsNull(col) => {
                let idx = col_names
                    .iter()
                    .position(|c| c == col || c.ends_with(&format!(".{}", col)));
                if let Some(i) = idx {
                    Some(row.get(i).unwrap_or(&Value::Null) == &Value::Null)
                } else {
                    Some(false)
                }
            }
            FilterExpr::IsNotNull(col) => {
                let idx = col_names
                    .iter()
                    .position(|c| c == col || c.ends_with(&format!(".{}", col)));
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
                    CompareOp::Contains => {
                        println!("Contains: left={:?}, right={:?}", left_cell, right_cell);
                        match (left_cell, &right_cell) {
                            (Value::Array(l), Value::Array(r)) => {
                                r.iter().all(|r_item| l.contains(r_item))
                            }
                            (Value::Text(l), Value::Text(r))
                                if l.starts_with('{') || l.starts_with('[') =>
                            {
                                // Simplified JSONB @> eval for MVP text-encoded JSON
                                let l_json: Result<serde_json::Value, _> = serde_json::from_str(l);
                                let r_json: Result<serde_json::Value, _> = serde_json::from_str(r);
                                if let (Ok(l_obj), Ok(r_obj)) = (l_json, r_json) {
                                    if let (Some(l_map), Some(r_map)) =
                                        (l_obj.as_object(), r_obj.as_object())
                                    {
                                        let matched = r_map.iter().all(|(k, v)| {
                                            if let Some(lv) = l_map.get(k) {
                                                lv == v
                                            } else {
                                                false
                                            }
                                        });
                                        println!(
                                            "JSONB @> l='{}', r='{}', matched={}",
                                            l, r, matched
                                        );
                                        matched
                                    } else {
                                        false
                                    }
                                } else {
                                    false
                                }
                            }
                            (Value::Text(l), Value::Array(r)) if l.starts_with('{') => {
                                // Array check against text representing array: "{login,signup}"
                                let mut l_str = l.clone();
                                if l_str.starts_with('{') && l_str.ends_with('}') {
                                    l_str = l_str[1..l_str.len() - 1].to_string();
                                }
                                let l_items: Vec<&str> = l_str.split(',').collect();
                                r.iter().all(|r_item| {
                                    let r_str = render(r_item);
                                    l_items
                                        .iter()
                                        .any(|&s| s == r_str || s == format!("'{}'", r_str))
                                })
                            }
                            (Value::Array(l), Value::Text(r)) => {
                                // Right might be a text parsing failure for ARRAY[] in some ASTs?
                                // Actually, `right_cell` should be evaluated correctly if it's an ARRAY[] literal.
                                false
                            }
                            (Value::Jsonb(l), Value::Jsonb(r)) => {
                                if let (Some(l_map), Some(r_map)) = (l.as_object(), r.as_object()) {
                                    r_map.iter().all(|(k, v)| l_map.get(k) == Some(v))
                                } else {
                                    false
                                }
                            }
                            _ => false,
                        }
                    }
                    CompareOp::ContainedBy => {
                        // <@
                        false // Simplified MVP
                    }
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
        self.eval_filter(ctx, row, &col_names, columns, filter)
            .unwrap_or(false)
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
            LogicalPlan::Begin
                | LogicalPlan::Commit
                | LogicalPlan::Rollback
                | LogicalPlan::Savepoint { .. }
                | LogicalPlan::RollbackToSavepoint { .. }
                | LogicalPlan::ReleaseSavepoint { .. }
        );
        let is_read_only = matches!(
            plan,
            LogicalPlan::Select { .. }
                | LogicalPlan::SelectLiteral { .. }
                | LogicalPlan::UnionAll { .. }
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
                ActiveTxn::new(txn_record.txn_id, txn_record.read_ts),
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
    /// Builds a column descriptor for a synthesized pg_catalog table.
    fn virtual_column(name: &str, data_type: &str) -> ColumnDescriptor {
        ColumnDescriptor {
            id: nodus_catalog::ColumnId::new(),
            name: name.into(),
            version: 1,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            state: nodus_catalog::DescriptorState::Public,
            data_type: data_type.into(),
            nullable: true,
        }
    }

    fn virtual_columns(columns: &[(&str, &str)]) -> Vec<ColumnDescriptor> {
        columns
            .iter()
            .map(|(name, data_type)| Self::virtual_column(name, data_type))
            .collect()
    }

    fn stable_oid(seed: &str, base: i64) -> i64 {
        let mut hash = 0xcbf29ce484222325_u64;
        for byte in seed.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        base + (hash % 1_000_000_000) as i64
    }

    fn database_oid(db_name: &str) -> i64 {
        Self::stable_oid(&format!("database:{db_name}"), 10_000)
    }

    fn schema_oid(db_name: &str, schema_name: &str) -> i64 {
        match schema_name {
            "pg_catalog" => 11,
            "public" => 2200,
            "information_schema" => 13_337,
            _ => Self::stable_oid(&format!("schema:{db_name}.{schema_name}"), 20_000),
        }
    }

    fn table_oid(db_name: &str, schema_name: &str, table_name: &str) -> i64 {
        Self::stable_oid(
            &format!("table:{db_name}.{schema_name}.{table_name}"),
            100_000,
        )
    }

    fn index_oid(db_name: &str, schema_name: &str, table_name: &str, index_name: &str) -> i64 {
        Self::stable_oid(
            &format!("index:{db_name}.{schema_name}.{table_name}.{index_name}"),
            2_000_000_000,
        )
    }

    fn constraint_oid(
        db_name: &str,
        schema_name: &str,
        table_name: &str,
        constraint_name: &str,
    ) -> i64 {
        Self::stable_oid(
            &format!("constraint:{db_name}.{schema_name}.{table_name}.{constraint_name}"),
            1_000_000_000,
        )
    }

    fn pg_type_oid(data_type: &str) -> i64 {
        let normalized = data_type
            .trim()
            .trim_matches('"')
            .to_ascii_uppercase()
            .replace("CHARACTER VARYING", "VARCHAR")
            .replace("DOUBLE PRECISION", "DOUBLE")
            .replace("TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ")
            .replace("TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP");
        let is_array = normalized.ends_with("[]");
        let base = normalized
            .trim_end_matches("[]")
            .split('(')
            .next()
            .unwrap_or("TEXT")
            .trim();
        if is_array {
            return match base {
                "BOOL" | "BOOLEAN" => 1000,
                "BYTEA" => 1001,
                "PG_CHAR" => 1002,
                "CHAR" | "CHARACTER" | "BPCHAR" => 1014,
                "INT2" | "SMALLINT" => 1005,
                "INT4" | "INT" | "INTEGER" | "SERIAL" => 1007,
                "INT8" | "BIGINT" => 1016,
                "TEXT" => 1009,
                "VARCHAR" => 1015,
                "OID" => 1028,
                "FLOAT4" | "REAL" => 1021,
                "FLOAT8" | "FLOAT" | "DOUBLE" => 1022,
                "NUMERIC" | "DECIMAL" => 1231,
                "DATE" => 1182,
                "TIME" => 1183,
                "TIMESTAMP" => 1115,
                "TIMESTAMPTZ" => 1185,
                "UUID" => 2951,
                "JSON" => 199,
                "JSONB" => 3807,
                "REGTYPE" => 2211,
                _ => 1009,
            };
        }
        match base {
            "BOOL" | "BOOLEAN" => 16,
            "BYTEA" => 17,
            "PG_CHAR" => 18,
            "CHAR" | "CHARACTER" | "BPCHAR" => 1042,
            "INT2" | "SMALLINT" => 21,
            "INT4" | "INT" | "INTEGER" | "SERIAL" => 23,
            "INT8" | "BIGINT" => 20,
            "TEXT" => 25,
            "OID" => 26,
            "FLOAT4" | "REAL" => 700,
            "FLOAT8" | "FLOAT" | "DOUBLE" => 701,
            "VARCHAR" => 1043,
            "DATE" => 1082,
            "TIME" => 1083,
            "TIMESTAMP" => 1114,
            "TIMESTAMPTZ" => 1184,
            "NUMERIC" | "DECIMAL" => 1700,
            "UUID" => 2950,
            "JSON" => 114,
            "JSONB" => 3802,
            "NAME" => 19,
            "REGPROC" => 24,
            "REGOPER" => 2203,
            "REGOPERATOR" => 2204,
            "REGCLASS" => 2205,
            "REGTYPE" => 2206,
            "REGROLE" => 4096,
            "REGNAMESPACE" => 4089,
            "REGCONFIG" => 3734,
            "REGDICTIONARY" => 3769,
            _ => 25,
        }
    }

    fn pg_type_name(data_type: &str) -> String {
        match Self::pg_type_oid(data_type) {
            16 => "bool",
            17 => "bytea",
            18 => "char",
            19 => "name",
            20 => "int8",
            21 => "int2",
            23 => "int4",
            25 => "text",
            26 => "oid",
            700 => "float4",
            701 => "float8",
            1042 => "bpchar",
            1043 => "varchar",
            1082 => "date",
            1083 => "time",
            1114 => "timestamp",
            1184 => "timestamptz",
            1700 => "numeric",
            2206 => "regtype",
            2950 => "uuid",
            3802 => "jsonb",
            _ => "text",
        }
        .to_string()
    }

    fn pg_type_length(data_type: &str) -> i64 {
        match Self::pg_type_oid(data_type) {
            16 => 1,
            20 | 701 | 1083 | 1114 | 1184 => 8,
            18 => 1,
            21 => 2,
            23 | 26 | 700 | 1082 | 2206 => 4,
            2950 => 16,
            _ => -1,
        }
    }

    fn is_virtual_schema(schema_name: &str) -> bool {
        schema_name.eq_ignore_ascii_case("pg_catalog")
            || schema_name.eq_ignore_ascii_case("information_schema")
    }

    fn is_pg_catalog_virtual_table_name(table_name: &str) -> bool {
        matches!(
            table_name.to_ascii_lowercase().as_str(),
            "pg_database"
                | "pg_namespace"
                | "pg_class"
                | "pg_attribute"
                | "pg_index"
                | "pg_constraint"
                | "pg_type"
                | "pg_proc"
                | "pg_range"
                | "pg_settings"
                | "pg_roles"
                | "pg_user"
                | "pg_tables"
                | "pg_indexes"
                | "pg_attrdef"
                | "pg_description"
                | "pg_shdescription"
                | "pg_enum"
                | "pg_collation"
                | "pg_am"
                | "pg_operator"
                | "pg_cast"
                | "pg_locks"
        )
    }

    fn schema_name_by_id(
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        schema_id: nodus_catalog::SchemaId,
    ) -> String {
        schemas
            .iter()
            .find(|schema| schema.id == schema_id)
            .map(|schema| schema.name.clone())
            .unwrap_or_else(|| {
                let _ = db_name;
                "public".to_string()
            })
    }

    fn returning_types(columns: &[ColumnDescriptor], returning: &[String]) -> Vec<String> {
        returning
            .iter()
            .map(|name| {
                columns
                    .iter()
                    .find(|column| column.name.eq_ignore_ascii_case(name))
                    .map(|column| column.data_type.clone())
                    .unwrap_or_else(|| "VARCHAR".to_string())
            })
            .collect()
    }

    fn pg_catalog_virtual_table(
        &self,
        db_name: &str,
        table_only: &str,
    ) -> Result<Option<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)>> {
        let schemas = self
            .catalog_reader
            .list_schemas(db_name)
            .unwrap_or_default();
        let tables = self
            .catalog_reader
            .list_all_tables(db_name)
            .unwrap_or_default();
        let table = table_only.to_ascii_lowercase();
        let result = match table.as_str() {
            "pg_database" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("datname", "NAME"),
                    ("datdba", "OID"),
                    ("encoding", "INT"),
                    ("datlocprovider", "PG_CHAR"),
                    ("datistemplate", "BOOL"),
                    ("datallowconn", "BOOL"),
                    ("datconnlimit", "INT"),
                    ("datcollate", "TEXT"),
                    ("datctype", "TEXT"),
                    ("daticulocale", "TEXT"),
                    ("datcollversion", "TEXT"),
                    ("datacl", "TEXT[]"),
                ]);
                let rows = vec![vec![
                    Value::Int(Self::database_oid(db_name)),
                    Value::Text(db_name.to_string()),
                    Value::Int(10),
                    Value::Int(6),
                    Value::Text("c".into()),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Int(-1),
                    Value::Text("C".into()),
                    Value::Text("C".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]];
                Some((cols, rows))
            }
            "pg_namespace" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("nspname", "NAME"),
                    ("nspowner", "OID"),
                    ("nspacl", "TEXT[]"),
                ]);
                let mut rows = vec![
                    vec![
                        Value::Int(Self::schema_oid(db_name, "pg_catalog")),
                        Value::Text("pg_catalog".into()),
                        Value::Int(10),
                        Value::Null,
                    ],
                    vec![
                        Value::Int(Self::schema_oid(db_name, "information_schema")),
                        Value::Text("information_schema".into()),
                        Value::Int(10),
                        Value::Null,
                    ],
                ];
                rows.extend(schemas.iter().map(|schema| {
                    vec![
                        Value::Int(Self::schema_oid(db_name, &schema.name)),
                        Value::Text(schema.name.clone()),
                        Value::Int(10),
                        Value::Null,
                    ]
                }));
                Some((cols, rows))
            }
            "pg_class" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("relname", "NAME"),
                    ("relnamespace", "OID"),
                    ("reltype", "OID"),
                    ("reloftype", "OID"),
                    ("relowner", "OID"),
                    ("relam", "OID"),
                    ("relfilenode", "OID"),
                    ("reltablespace", "OID"),
                    ("relpages", "INT"),
                    ("reltuples", "FLOAT4"),
                    ("relallvisible", "INT"),
                    ("reltoastrelid", "OID"),
                    ("relhasindex", "BOOL"),
                    ("relisshared", "BOOL"),
                    ("relpersistence", "PG_CHAR"),
                    ("relkind", "PG_CHAR"),
                    ("relnatts", "INT"),
                    ("relchecks", "INT"),
                    ("relhasrules", "BOOL"),
                    ("relhastriggers", "BOOL"),
                    ("relhassubclass", "BOOL"),
                    ("relrowsecurity", "BOOL"),
                    ("relforcerowsecurity", "BOOL"),
                    ("relispopulated", "BOOL"),
                    ("relreplident", "PG_CHAR"),
                    ("relispartition", "BOOL"),
                    ("relrewrite", "OID"),
                    ("relfrozenxid", "INT"),
                    ("relminmxid", "INT"),
                    ("relacl", "TEXT[]"),
                    ("reloptions", "TEXT[]"),
                    ("relpartbound", "TEXT"),
                ]);
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let oid = Self::table_oid(db_name, &schema_name, &table.name);
                    rows.push(vec![
                        Value::Int(oid),
                        Value::Text(table.name.clone()),
                        Value::Int(Self::schema_oid(db_name, &schema_name)),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(10),
                        Value::Int(0),
                        Value::Int(oid),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Float(0.0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Bool(!table.indexes.is_empty()),
                        Value::Bool(false),
                        Value::Text("p".into()),
                        Value::Text(if table.view_query.is_some() { "v" } else { "r" }.into()),
                        Value::Int(table.columns.len() as i64),
                        Value::Int(
                            table
                                .constraints
                                .iter()
                                .filter(|constraint| {
                                    matches!(
                                        constraint,
                                        nodus_catalog::TableConstraint::Check { .. }
                                    )
                                })
                                .count() as i64,
                        ),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        Value::Text("d".into()),
                        Value::Bool(false),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Int(0),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ]);
                    for index in &table.indexes {
                        let index_oid =
                            Self::index_oid(db_name, &schema_name, &table.name, &index.name);
                        rows.push(vec![
                            Value::Int(index_oid),
                            Value::Text(index.name.clone()),
                            Value::Int(Self::schema_oid(db_name, &schema_name)),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(10),
                            Value::Int(403),
                            Value::Int(index_oid),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Float(0.0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Text("p".into()),
                            Value::Text("i".into()),
                            Value::Int(index.key_columns.len() as i64),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Text("n".into()),
                            Value::Bool(false),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
                Some((cols, rows))
            }
            "pg_attribute" => {
                let cols = Self::virtual_columns(&[
                    ("attrelid", "OID"),
                    ("attname", "NAME"),
                    ("atttypid", "OID"),
                    ("attstattarget", "INT"),
                    ("attlen", "INT"),
                    ("attnum", "INT"),
                    ("attndims", "INT"),
                    ("attcacheoff", "INT"),
                    ("atttypmod", "INT"),
                    ("attbyval", "BOOL"),
                    ("attstorage", "PG_CHAR"),
                    ("attalign", "PG_CHAR"),
                    ("attnotnull", "BOOL"),
                    ("atthasdef", "BOOL"),
                    ("atthasmissing", "BOOL"),
                    ("attidentity", "PG_CHAR"),
                    ("attgenerated", "PG_CHAR"),
                    ("attisdropped", "BOOL"),
                    ("attislocal", "BOOL"),
                    ("attinhcount", "INT"),
                    ("attcollation", "OID"),
                    ("attacl", "TEXT[]"),
                    ("attoptions", "TEXT[]"),
                    ("attfdwoptions", "TEXT[]"),
                    ("attmissingval", "TEXT"),
                ]);
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema_name, &table.name);
                    for (idx, column) in table.columns.iter().enumerate() {
                        let type_oid = Self::pg_type_oid(&column.data_type);
                        rows.push(vec![
                            Value::Int(relid),
                            Value::Text(column.name.clone()),
                            Value::Int(type_oid),
                            Value::Int(-1),
                            Value::Int(Self::pg_type_length(&column.data_type)),
                            Value::Int((idx + 1) as i64),
                            Value::Int(if column.data_type.ends_with("[]") {
                                1
                            } else {
                                0
                            }),
                            Value::Int(-1),
                            Value::Int(-1),
                            Value::Bool(matches!(type_oid, 16 | 20 | 21 | 23 | 26 | 700 | 701)),
                            Value::Text("x".into()),
                            Value::Text("i".into()),
                            Value::Bool(!column.nullable),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Int(0),
                            Value::Int(100),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
                Some((cols, rows))
            }
            "pg_index" => {
                let cols = Self::virtual_columns(&[
                    ("indexrelid", "OID"),
                    ("indrelid", "OID"),
                    ("indnatts", "INT"),
                    ("indnkeyatts", "INT"),
                    ("indisunique", "BOOL"),
                    ("indisprimary", "BOOL"),
                    ("indisexclusion", "BOOL"),
                    ("indimmediate", "BOOL"),
                    ("indisclustered", "BOOL"),
                    ("indisvalid", "BOOL"),
                    ("indcheckxmin", "BOOL"),
                    ("indisready", "BOOL"),
                    ("indislive", "BOOL"),
                    ("indisreplident", "BOOL"),
                    ("indkey", "TEXT"),
                    ("indcollation", "TEXT"),
                    ("indclass", "TEXT"),
                    ("indoption", "TEXT"),
                    ("indexprs", "TEXT"),
                    ("indpred", "TEXT"),
                ]);
                let mut rows = Vec::new();
                for table in &tables {
                    let schema_name = Self::schema_name_by_id(db_name, &schemas, table.schema_id);
                    let relid = Self::table_oid(db_name, &schema_name, &table.name);
                    for index in &table.indexes {
                        let keys = index
                            .key_columns
                            .iter()
                            .filter_map(|key| {
                                table
                                    .columns
                                    .iter()
                                    .position(|column| column.id == key.column_id)
                                    .map(|pos| (pos + 1).to_string())
                            })
                            .collect::<Vec<_>>()
                            .join(" ");
                        rows.push(vec![
                            Value::Int(Self::index_oid(
                                db_name,
                                &schema_name,
                                &table.name,
                                &index.name,
                            )),
                            Value::Int(relid),
                            Value::Int(index.key_columns.len() as i64),
                            Value::Int(index.key_columns.len() as i64),
                            Value::Bool(index.unique),
                            Value::Bool(matches!(
                                index.index_type,
                                nodus_catalog::IndexType::Primary
                            )),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Bool(true),
                            Value::Bool(false),
                            Value::Text(keys),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Text(String::new()),
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
                Some((cols, rows))
            }
            "pg_constraint" => {
                let cols = Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("conname", "NAME"),
                    ("connamespace", "OID"),
                    ("contype", "PG_CHAR"),
                    ("condeferrable", "BOOL"),
                    ("condeferred", "BOOL"),
                    ("convalidated", "BOOL"),
                    ("conrelid", "OID"),
                    ("contypid", "OID"),
                    ("conindid", "OID"),
                    ("conparentid", "OID"),
                    ("confrelid", "OID"),
                    ("confupdtype", "PG_CHAR"),
                    ("confdeltype", "PG_CHAR"),
                    ("confmatchtype", "PG_CHAR"),
                    ("conislocal", "BOOL"),
                    ("coninhcount", "INT"),
                    ("connoinherit", "BOOL"),
                    ("conkey", "INT[]"),
                    ("confkey", "INT[]"),
                    ("conpfeqop", "OID[]"),
                    ("conppeqop", "OID[]"),
                    ("conffeqop", "OID[]"),
                    ("confdelsetcols", "INT[]"),
                    ("conexclop", "OID[]"),
                    ("conbin", "TEXT"),
                ]);
                Some((cols, self.pg_constraint_rows(db_name, &schemas, &tables)))
            }
            "pg_type" => Some(self.pg_type_virtual_table(db_name)),
            "pg_proc" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("proname", "NAME"),
                    ("pronamespace", "OID"),
                    ("proowner", "OID"),
                    ("prolang", "OID"),
                    ("procost", "FLOAT4"),
                    ("prorows", "FLOAT4"),
                    ("provariadic", "OID"),
                    ("prosupport", "REGPROC"),
                    ("prokind", "PG_CHAR"),
                    ("prosecdef", "BOOL"),
                    ("proleakproof", "BOOL"),
                    ("proisstrict", "BOOL"),
                    ("proretset", "BOOL"),
                    ("provolatile", "PG_CHAR"),
                    ("proparallel", "PG_CHAR"),
                    ("pronargs", "INT"),
                    ("pronargdefaults", "INT"),
                    ("prorettype", "OID"),
                    ("proargtypes", "OID[]"),
                    ("proallargtypes", "OID[]"),
                    ("proargmodes", "PG_CHAR[]"),
                    ("proargnames", "TEXT[]"),
                    ("proargdefaults", "TEXT"),
                    ("protrftypes", "OID[]"),
                    ("prosrc", "TEXT"),
                    ("probin", "TEXT"),
                    ("prosqlbody", "TEXT"),
                    ("proconfig", "TEXT[]"),
                    ("proacl", "TEXT[]"),
                ]),
                vec![vec![
                    Value::Int(750),
                    Value::Text("array_recv".into()),
                    Value::Int(Self::schema_oid(db_name, "pg_catalog")),
                    Value::Int(10),
                    Value::Int(12),
                    Value::Float(1.0),
                    Value::Float(0.0),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text("f".into()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Text("i".into()),
                    Value::Text("s".into()),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Array(Vec::new()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("array_recv".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]],
            )),
            "pg_range" => Some((
                Self::virtual_columns(&[
                    ("rngtypid", "OID"),
                    ("rngsubtype", "OID"),
                    ("rngmultitypid", "OID"),
                    ("rngcollation", "OID"),
                    ("rngsubopc", "OID"),
                    ("rngcanonical", "REGPROC"),
                    ("rngsubdiff", "REGPROC"),
                ]),
                Vec::new(),
            )),
            "pg_settings" => Some(self.pg_settings_virtual_table()),
            "pg_roles" => Some(self.pg_roles_virtual_table()),
            "pg_user" => Some(self.pg_user_virtual_table()),
            "pg_tables" => Some(self.pg_tables_virtual_table(db_name, &schemas, &tables)),
            "pg_indexes" => Some(self.pg_indexes_virtual_table(db_name, &schemas, &tables)),
            "pg_attrdef" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("adrelid", "OID"),
                    ("adnum", "INT"),
                    ("adbin", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_description" => Some((
                Self::virtual_columns(&[
                    ("objoid", "OID"),
                    ("classoid", "OID"),
                    ("objsubid", "INT"),
                    ("description", "TEXT"),
                ]),
                Vec::new(),
            )),
            // Shared-object comments. NodusDB has no COMMENT ON support, so this
            // is synthesized empty like pg_description; pgjdbc/DataGrip join it
            // during introspection and tolerate zero rows.
            "pg_shdescription" => Some((
                Self::virtual_columns(&[
                    ("objoid", "OID"),
                    ("classoid", "OID"),
                    ("description", "TEXT"),
                ]),
                Vec::new(),
            )),
            "pg_enum" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("enumtypid", "OID"),
                    ("enumsortorder", "FLOAT4"),
                    ("enumlabel", "NAME"),
                ]),
                Vec::new(),
            )),
            "pg_collation" => Some(self.pg_collation_virtual_table(db_name)),
            "pg_am" => Some((
                Self::virtual_columns(&[
                    ("oid", "OID"),
                    ("amname", "NAME"),
                    ("amhandler", "REGPROC"),
                    ("amtype", "PG_CHAR"),
                ]),
                vec![vec![
                    Value::Int(403),
                    Value::Text("btree".into()),
                    Value::Text("-".into()),
                    Value::Text("i".into()),
                ]],
            )),
            "pg_operator" => Some(self.pg_operator_virtual_table(db_name)),
            "pg_cast" => Some(self.pg_cast_virtual_table()),
            "pg_locks" => Some((
                Self::virtual_columns(&[
                    ("locktype", "TEXT"),
                    ("database", "OID"),
                    ("relation", "OID"),
                    ("transactionid", "INT8"),
                    ("pid", "INT"),
                    ("mode", "TEXT"),
                    ("granted", "BOOL"),
                ]),
                Vec::new(),
            )),
            _ => None,
        };
        Ok(result)
    }

    fn pg_type_virtual_table(&self, db_name: &str) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("typname", "NAME"),
            ("typnamespace", "OID"),
            ("typowner", "OID"),
            ("typlen", "INT"),
            ("typbyval", "BOOL"),
            ("typtype", "PG_CHAR"),
            ("typcategory", "PG_CHAR"),
            ("typispreferred", "BOOL"),
            ("typisdefined", "BOOL"),
            ("typdelim", "PG_CHAR"),
            ("typrelid", "OID"),
            ("typelem", "OID"),
            ("typarray", "OID"),
            ("typinput", "REGPROC"),
            ("typoutput", "REGPROC"),
            ("typreceive", "REGPROC"),
            ("typsend", "REGPROC"),
            ("typmodin", "REGPROC"),
            ("typmodout", "REGPROC"),
            ("typanalyze", "REGPROC"),
            ("typalign", "PG_CHAR"),
            ("typstorage", "PG_CHAR"),
            ("typnotnull", "BOOL"),
            ("typbasetype", "OID"),
            ("typtypmod", "INT"),
            ("typndims", "INT"),
            ("typcollation", "OID"),
            ("typdefaultbin", "TEXT"),
            ("typdefault", "TEXT"),
            ("typacl", "TEXT[]"),
        ]);
        let pg_ns = Self::schema_oid(db_name, "pg_catalog");
        let type_specs = [
            (16, "bool", 1, 1000, "_bool"),
            (17, "bytea", -1, 1001, "_bytea"),
            (18, "char", 1, 1002, "_char"),
            (19, "name", 64, 1003, "_name"),
            (20, "int8", 8, 1016, "_int8"),
            (21, "int2", 2, 1005, "_int2"),
            (23, "int4", 4, 1007, "_int4"),
            (25, "text", -1, 1009, "_text"),
            (26, "oid", 4, 1028, "_oid"),
            (700, "float4", 4, 1021, "_float4"),
            (701, "float8", 8, 1022, "_float8"),
            (1042, "bpchar", -1, 1014, "_bpchar"),
            (1043, "varchar", -1, 1015, "_varchar"),
            (1082, "date", 4, 1182, "_date"),
            (1083, "time", 8, 1183, "_time"),
            (1114, "timestamp", 8, 1115, "_timestamp"),
            (1184, "timestamptz", 8, 1185, "_timestamptz"),
            (1700, "numeric", -1, 1231, "_numeric"),
            (2206, "regtype", 4, 2211, "_regtype"),
            (2950, "uuid", 16, 2951, "_uuid"),
            (3802, "jsonb", -1, 3807, "_jsonb"),
        ];
        let mut rows = Vec::new();
        for (oid, name, len, array, _) in type_specs {
            rows.push(vec![
                Value::Int(oid),
                Value::Text(name.into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Int(len),
                Value::Bool(matches!(len, 1 | 2 | 4 | 8)),
                Value::Text("b".into()),
                Value::Text("U".into()),
                Value::Bool(false),
                Value::Bool(true),
                Value::Text(",".into()),
                Value::Int(0),
                Value::Int(0),
                Value::Int(array),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Text("i".into()),
                Value::Text("p".into()),
                Value::Bool(false),
                Value::Int(0),
                Value::Int(-1),
                Value::Int(0),
                Value::Int(100),
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
        for (elem_oid, _, _, array_oid, array_name) in type_specs {
            rows.push(vec![
                Value::Int(array_oid),
                Value::Text(array_name.into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Int(-1),
                Value::Bool(false),
                Value::Text("a".into()),
                Value::Text("A".into()),
                Value::Bool(false),
                Value::Bool(true),
                Value::Text(",".into()),
                Value::Int(0),
                Value::Int(elem_oid),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(750),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Text("i".into()),
                Value::Text("x".into()),
                Value::Bool(false),
                Value::Int(0),
                Value::Int(-1),
                Value::Int(1),
                Value::Int(100),
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
        (cols, rows)
    }

    fn pg_settings_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("name", "TEXT"),
            ("setting", "TEXT"),
            ("unit", "TEXT"),
            ("category", "TEXT"),
            ("short_desc", "TEXT"),
            ("extra_desc", "TEXT"),
            ("context", "TEXT"),
            ("vartype", "TEXT"),
            ("source", "TEXT"),
            ("min_val", "TEXT"),
            ("max_val", "TEXT"),
            ("enumvals", "TEXT[]"),
            ("boot_val", "TEXT"),
            ("reset_val", "TEXT"),
            ("sourcefile", "TEXT"),
            ("sourceline", "INT"),
            ("pending_restart", "BOOL"),
        ]);
        let settings = [
            ("application_name", "", "string"),
            ("client_encoding", "UTF8", "string"),
            ("DateStyle", "ISO, MDY", "string"),
            ("integer_datetimes", "on", "bool"),
            ("IntervalStyle", "postgres", "string"),
            ("is_superuser", "on", "bool"),
            ("server_encoding", "UTF8", "string"),
            ("server_version", "16.0", "string"),
            ("server_version_num", "160000", "integer"),
            ("standard_conforming_strings", "on", "bool"),
            ("statement_timeout", "0", "integer"),
            ("TimeZone", "UTC", "string"),
        ];
        let rows = settings
            .into_iter()
            .map(|(name, setting, vartype)| {
                vec![
                    Value::Text(name.into()),
                    Value::Text(setting.into()),
                    Value::Null,
                    Value::Text("Client Connection Defaults".into()),
                    Value::Text(name.into()),
                    Value::Null,
                    Value::Text("user".into()),
                    Value::Text(vartype.into()),
                    Value::Text("default".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text(setting.into()),
                    Value::Text(setting.into()),
                    Value::Null,
                    Value::Null,
                    Value::Bool(false),
                ]
            })
            .collect();
        (cols, rows)
    }

    fn pg_roles_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("rolname", "NAME"),
            ("rolsuper", "BOOL"),
            ("rolinherit", "BOOL"),
            ("rolcreaterole", "BOOL"),
            ("rolcreatedb", "BOOL"),
            ("rolcanlogin", "BOOL"),
            ("rolreplication", "BOOL"),
            ("rolconnlimit", "INT"),
            ("rolpassword", "TEXT"),
            ("rolvaliduntil", "TIMESTAMPTZ"),
            ("rolbypassrls", "BOOL"),
            ("rolconfig", "TEXT[]"),
        ]);
        let rows = vec![vec![
            Value::Int(10),
            Value::Text("nodus".into()),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Int(-1),
            Value::Text("********".into()),
            Value::Null,
            Value::Bool(false),
            Value::Null,
        ]];
        (cols, rows)
    }

    fn pg_user_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("usename", "NAME"),
            ("usesysid", "OID"),
            ("usecreatedb", "BOOL"),
            ("usesuper", "BOOL"),
            ("userepl", "BOOL"),
            ("usebypassrls", "BOOL"),
            ("passwd", "TEXT"),
            ("valuntil", "TIMESTAMPTZ"),
            ("useconfig", "TEXT[]"),
        ]);
        let rows = vec![vec![
            Value::Text("nodus".into()),
            Value::Int(10),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
            Value::Text("********".into()),
            Value::Null,
            Value::Null,
        ]];
        (cols, rows)
    }

    fn pg_tables_virtual_table(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("schemaname", "NAME"),
            ("tablename", "NAME"),
            ("tableowner", "NAME"),
            ("tablespace", "NAME"),
            ("hasindexes", "BOOL"),
            ("hasrules", "BOOL"),
            ("hastriggers", "BOOL"),
            ("rowsecurity", "BOOL"),
        ]);
        let rows = tables
            .iter()
            .filter(|table| table.view_query.is_none())
            .map(|table| {
                vec![
                    Value::Text(Self::schema_name_by_id(db_name, schemas, table.schema_id)),
                    Value::Text(table.name.clone()),
                    Value::Text("nodus".into()),
                    Value::Null,
                    Value::Bool(!table.indexes.is_empty()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                ]
            })
            .collect();
        (cols, rows)
    }

    fn pg_indexes_virtual_table(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("schemaname", "NAME"),
            ("tablename", "NAME"),
            ("indexname", "NAME"),
            ("tablespace", "NAME"),
            ("indexdef", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                let key_cols = index
                    .key_columns
                    .iter()
                    .filter_map(|key| {
                        table
                            .columns
                            .iter()
                            .find(|column| column.id == key.column_id)
                            .map(|column| column.name.clone())
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                rows.push(vec![
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(index.name.clone()),
                    Value::Null,
                    Value::Text(format!(
                        "CREATE {}INDEX {} ON {}.{} ({})",
                        if index.unique { "UNIQUE " } else { "" },
                        index.name,
                        schema_name,
                        table.name,
                        key_cols
                    )),
                ]);
            }
        }
        (cols, rows)
    }

    fn pg_constraint_rows(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> Vec<Vec<Value>> {
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            let relid = Self::table_oid(db_name, &schema_name, &table.name);
            let namespace = Self::schema_oid(db_name, &schema_name);
            for index in &table.indexes {
                if !index.unique {
                    continue;
                }
                let conname = index.name.clone();
                let key_nums = index
                    .key_columns
                    .iter()
                    .filter_map(|key| {
                        table
                            .columns
                            .iter()
                            .position(|column| column.id == key.column_id)
                            .map(|pos| Value::Int((pos + 1) as i64))
                    })
                    .collect::<Vec<_>>();
                rows.push(vec![
                    Value::Int(Self::constraint_oid(
                        db_name,
                        &schema_name,
                        &table.name,
                        &conname,
                    )),
                    Value::Text(conname.clone()),
                    Value::Int(namespace),
                    Value::Text(
                        if matches!(index.index_type, nodus_catalog::IndexType::Primary) {
                            "p"
                        } else {
                            "u"
                        }
                        .into(),
                    ),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(true),
                    Value::Int(relid),
                    Value::Int(0),
                    Value::Int(Self::index_oid(
                        db_name,
                        &schema_name,
                        &table.name,
                        &index.name,
                    )),
                    Value::Int(0),
                    Value::Int(0),
                    Value::Text("a".into()),
                    Value::Text("a".into()),
                    Value::Text("s".into()),
                    Value::Bool(true),
                    Value::Int(0),
                    Value::Bool(false),
                    Value::Array(key_nums),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]);
            }
            for (idx, constraint) in table.constraints.iter().enumerate() {
                match constraint {
                    nodus_catalog::TableConstraint::Check { name, expr } => {
                        let conname = name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table.name, idx + 1));
                        rows.push(vec![
                            Value::Int(Self::constraint_oid(
                                db_name,
                                &schema_name,
                                &table.name,
                                &conname,
                            )),
                            Value::Text(conname),
                            Value::Int(namespace),
                            Value::Text("c".into()),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Int(relid),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Text("a".into()),
                            Value::Text("a".into()),
                            Value::Text("s".into()),
                            Value::Bool(true),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Text(expr.clone()),
                        ]);
                    }
                    nodus_catalog::TableConstraint::ForeignKey {
                        name,
                        columns,
                        foreign_table,
                        referred_columns,
                    } => {
                        let conname = name.clone().unwrap_or_else(|| {
                            format!("{}_{}_fkey", table.name, columns.join("_"))
                        });
                        let (ref_db, ref_schema, ref_table) = parse_object_name(foreign_table)
                            .unwrap_or((db_name, "public", foreign_table));
                        let confrelid = Self::table_oid(ref_db, ref_schema, ref_table);
                        let conkey = columns
                            .iter()
                            .filter_map(|name| {
                                table
                                    .columns
                                    .iter()
                                    .position(|column| column.name == *name)
                                    .map(|pos| Value::Int((pos + 1) as i64))
                            })
                            .collect::<Vec<_>>();
                        let confkey = referred_columns
                            .iter()
                            .enumerate()
                            .map(|(pos, _)| Value::Int((pos + 1) as i64))
                            .collect::<Vec<_>>();
                        rows.push(vec![
                            Value::Int(Self::constraint_oid(
                                db_name,
                                &schema_name,
                                &table.name,
                                &conname,
                            )),
                            Value::Text(conname),
                            Value::Int(namespace),
                            Value::Text("f".into()),
                            Value::Bool(false),
                            Value::Bool(false),
                            Value::Bool(true),
                            Value::Int(relid),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(0),
                            Value::Int(confrelid),
                            Value::Text("a".into()),
                            Value::Text("a".into()),
                            Value::Text("s".into()),
                            Value::Bool(true),
                            Value::Int(0),
                            Value::Bool(false),
                            Value::Array(conkey),
                            Value::Array(confkey),
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                            Value::Null,
                        ]);
                    }
                }
            }
        }
        rows
    }

    fn pg_collation_virtual_table(
        &self,
        db_name: &str,
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("collname", "NAME"),
            ("collnamespace", "OID"),
            ("collowner", "OID"),
            ("collprovider", "PG_CHAR"),
            ("collisdeterministic", "BOOL"),
            ("collencoding", "INT"),
            ("collcollate", "TEXT"),
            ("collctype", "TEXT"),
            ("colliculocale", "TEXT"),
            ("collversion", "TEXT"),
        ]);
        let pg_ns = Self::schema_oid(db_name, "pg_catalog");
        let rows = vec![
            vec![
                Value::Int(100),
                Value::Text("default".into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Text("d".into()),
                Value::Bool(true),
                Value::Int(-1),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ],
            vec![
                Value::Int(950),
                Value::Text("C".into()),
                Value::Int(pg_ns),
                Value::Int(10),
                Value::Text("c".into()),
                Value::Bool(true),
                Value::Int(-1),
                Value::Text("C".into()),
                Value::Text("C".into()),
                Value::Null,
                Value::Null,
            ],
        ];
        (cols, rows)
    }

    fn pg_operator_virtual_table(&self, db_name: &str) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("oprname", "NAME"),
            ("oprnamespace", "OID"),
            ("oprowner", "OID"),
            ("oprkind", "PG_CHAR"),
            ("oprcanmerge", "BOOL"),
            ("oprcanhash", "BOOL"),
            ("oprleft", "OID"),
            ("oprright", "OID"),
            ("oprresult", "OID"),
            ("oprcom", "OID"),
            ("oprnegate", "OID"),
            ("oprcode", "REGPROC"),
            ("oprrest", "REGPROC"),
            ("oprjoin", "REGPROC"),
        ]);
        let ns = Self::schema_oid(db_name, "pg_catalog");
        let rows = [
            (96, "=", 23, 23, 16),
            (97, "<", 23, 23, 16),
            (521, ">", 23, 23, 16),
            (98, "=", 25, 25, 16),
        ]
        .into_iter()
        .map(|(oid, name, left, right, result)| {
            vec![
                Value::Int(oid),
                Value::Text(name.into()),
                Value::Int(ns),
                Value::Int(10),
                Value::Text("b".into()),
                Value::Bool(false),
                Value::Bool(name == "="),
                Value::Int(left),
                Value::Int(right),
                Value::Int(result),
                Value::Int(0),
                Value::Int(0),
                Value::Text("-".into()),
                Value::Text("-".into()),
                Value::Text("-".into()),
            ]
        })
        .collect();
        (cols, rows)
    }

    fn pg_cast_virtual_table(&self) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("oid", "OID"),
            ("castsource", "OID"),
            ("casttarget", "OID"),
            ("castfunc", "OID"),
            ("castcontext", "PG_CHAR"),
            ("castmethod", "PG_CHAR"),
        ]);
        let casts = [
            (23, 20),
            (23, 25),
            (20, 25),
            (25, 23),
            (25, 20),
            (1043, 25),
            (25, 1043),
            (114, 3802),
        ];
        let rows = casts
            .into_iter()
            .enumerate()
            .map(|(idx, (source, target))| {
                vec![
                    Value::Int(10_000 + idx as i64),
                    Value::Int(source),
                    Value::Int(target),
                    Value::Int(0),
                    Value::Text("a".into()),
                    Value::Text("f".into()),
                ]
            })
            .collect();
        (cols, rows)
    }

    fn information_schema_virtual_table(
        &self,
        db_name: &str,
        table_only: &str,
    ) -> Result<Option<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)>> {
        let schemas = self
            .catalog_reader
            .list_schemas(db_name)
            .unwrap_or_default();
        let tables = self
            .catalog_reader
            .list_all_tables(db_name)
            .unwrap_or_default();
        let result = match table_only.to_ascii_lowercase().as_str() {
            "tables" => Some(self.information_schema_tables(db_name, &schemas, &tables)),
            "columns" => Some(self.information_schema_columns(db_name, &schemas, &tables)),
            "table_constraints" | "constraints" => {
                Some(self.information_schema_table_constraints(db_name, &schemas, &tables))
            }
            "key_column_usage" => {
                Some(self.information_schema_key_column_usage(db_name, &schemas, &tables))
            }
            "constraint_column_usage" => Some(self.information_schema_constraint_column_usage()),
            "indexes" => Some(self.information_schema_indexes(db_name, &schemas, &tables)),
            "schemata" => Some(self.information_schema_schemata(db_name, &schemas)),
            _ => None,
        };
        Ok(result)
    }

    fn information_schema_tables(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("table_type", "TEXT"),
            ("self_referencing_column_name", "TEXT"),
            ("reference_generation", "TEXT"),
            ("user_defined_type_catalog", "TEXT"),
            ("user_defined_type_schema", "TEXT"),
            ("user_defined_type_name", "TEXT"),
            ("is_insertable_into", "TEXT"),
            ("is_typed", "TEXT"),
            ("commit_action", "TEXT"),
        ]);
        let rows = tables
            .iter()
            .map(|table| {
                vec![
                    Value::Text(db_name.into()),
                    Value::Text(Self::schema_name_by_id(db_name, schemas, table.schema_id)),
                    Value::Text(table.name.clone()),
                    Value::Text(
                        if table.view_query.is_some() {
                            "VIEW"
                        } else {
                            "BASE TABLE"
                        }
                        .into(),
                    ),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text("YES".into()),
                    Value::Text("NO".into()),
                    Value::Null,
                ]
            })
            .collect();
        (cols, rows)
    }

    fn information_schema_columns(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("column_name", "TEXT"),
            ("ordinal_position", "INT"),
            ("column_default", "TEXT"),
            ("is_nullable", "TEXT"),
            ("data_type", "TEXT"),
            ("character_maximum_length", "INT"),
            ("numeric_precision", "INT"),
            ("numeric_scale", "INT"),
            ("datetime_precision", "INT"),
            ("udt_catalog", "TEXT"),
            ("udt_schema", "TEXT"),
            ("udt_name", "TEXT"),
            ("is_identity", "TEXT"),
            ("identity_generation", "TEXT"),
            ("is_generated", "TEXT"),
            ("generation_expression", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for (idx, column) in table.columns.iter().enumerate() {
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(column.name.clone()),
                    Value::Int((idx + 1) as i64),
                    Value::Null,
                    Value::Text(if column.nullable { "YES" } else { "NO" }.into()),
                    Value::Text(column.data_type.to_ascii_lowercase()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Text(db_name.into()),
                    Value::Text("pg_catalog".into()),
                    Value::Text(Self::pg_type_name(&column.data_type)),
                    Value::Text("NO".into()),
                    Value::Null,
                    Value::Text("NEVER".into()),
                    Value::Null,
                ]);
            }
        }
        (cols, rows)
    }

    fn information_schema_table_constraints(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("constraint_type", "TEXT"),
            ("is_deferrable", "TEXT"),
            ("initially_deferred", "TEXT"),
            ("enforced", "TEXT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                if index.unique {
                    rows.push(vec![
                        Value::Text(db_name.into()),
                        Value::Text(schema_name.clone()),
                        Value::Text(index.name.clone()),
                        Value::Text(schema_name.clone()),
                        Value::Text(table.name.clone()),
                        Value::Text(
                            if matches!(index.index_type, nodus_catalog::IndexType::Primary) {
                                "PRIMARY KEY"
                            } else {
                                "UNIQUE"
                            }
                            .into(),
                        ),
                        Value::Text("NO".into()),
                        Value::Text("NO".into()),
                        Value::Text("YES".into()),
                    ]);
                }
            }
            for (idx, constraint) in table.constraints.iter().enumerate() {
                let (name, constraint_type) = match constraint {
                    nodus_catalog::TableConstraint::Check { name, .. } => (
                        name.clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table.name, idx + 1)),
                        "CHECK",
                    ),
                    nodus_catalog::TableConstraint::ForeignKey { name, columns, .. } => (
                        name.clone().unwrap_or_else(|| {
                            format!("{}_{}_fkey", table.name, columns.join("_"))
                        }),
                        "FOREIGN KEY",
                    ),
                };
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(name),
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(constraint_type.into()),
                    Value::Text("NO".into()),
                    Value::Text("NO".into()),
                    Value::Text("YES".into()),
                ]);
            }
        }
        (cols, rows)
    }

    fn information_schema_key_column_usage(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("column_name", "TEXT"),
            ("ordinal_position", "INT"),
            ("position_in_unique_constraint", "INT"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                if !index.unique {
                    continue;
                }
                for (idx, key) in index.key_columns.iter().enumerate() {
                    if let Some(column) = table
                        .columns
                        .iter()
                        .find(|column| column.id == key.column_id)
                    {
                        rows.push(vec![
                            Value::Text(db_name.into()),
                            Value::Text(schema_name.clone()),
                            Value::Text(index.name.clone()),
                            Value::Text(db_name.into()),
                            Value::Text(schema_name.clone()),
                            Value::Text(table.name.clone()),
                            Value::Text(column.name.clone()),
                            Value::Int((idx + 1) as i64),
                            Value::Null,
                        ]);
                    }
                }
            }
        }
        (cols, rows)
    }

    fn information_schema_constraint_column_usage(
        &self,
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("column_name", "TEXT"),
            ("constraint_catalog", "TEXT"),
            ("constraint_schema", "TEXT"),
            ("constraint_name", "TEXT"),
        ]);
        (cols, Vec::new())
    }

    fn information_schema_indexes(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
        tables: &[nodus_catalog::TableDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("table_catalog", "TEXT"),
            ("table_schema", "TEXT"),
            ("table_name", "TEXT"),
            ("index_name", "TEXT"),
            ("is_unique", "BOOL"),
        ]);
        let mut rows = Vec::new();
        for table in tables {
            let schema_name = Self::schema_name_by_id(db_name, schemas, table.schema_id);
            for index in &table.indexes {
                rows.push(vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema_name.clone()),
                    Value::Text(table.name.clone()),
                    Value::Text(index.name.clone()),
                    Value::Bool(index.unique),
                ]);
            }
        }
        (cols, rows)
    }

    fn information_schema_schemata(
        &self,
        db_name: &str,
        schemas: &[nodus_catalog::SchemaDescriptor],
    ) -> (Vec<ColumnDescriptor>, Vec<Vec<Value>>) {
        let cols = Self::virtual_columns(&[
            ("catalog_name", "TEXT"),
            ("schema_name", "TEXT"),
            ("schema_owner", "TEXT"),
            ("default_character_set_catalog", "TEXT"),
            ("default_character_set_schema", "TEXT"),
            ("default_character_set_name", "TEXT"),
            ("sql_path", "TEXT"),
        ]);
        let rows = schemas
            .iter()
            .map(|schema| {
                vec![
                    Value::Text(db_name.into()),
                    Value::Text(schema.name.clone()),
                    Value::Text("nodus".into()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ]
            })
            .collect();
        (cols, rows)
    }

    fn get_virtual_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_only: &str,
    ) -> Result<(Vec<ColumnDescriptor>, Vec<Vec<Value>>)> {
        if schema_name.eq_ignore_ascii_case("pg_catalog") {
            if let Some(table) = self.pg_catalog_virtual_table(db_name, table_only)? {
                return Ok(table);
            }
            anyhow::bail!("relation \"pg_catalog.{}\" does not exist", table_only);
        } else if schema_name.eq_ignore_ascii_case("information_schema") {
            if let Some(table) = self.information_schema_virtual_table(db_name, table_only)? {
                return Ok(table);
            }
            anyhow::bail!(
                "relation \"information_schema.{}\" does not exist",
                table_only
            );
        } else {
            anyhow::bail!("relation \"{}.{}\" does not exist", schema_name, table_only);
        }
    }

    fn execute_logical_inner(
        &self,
        ctx: &ExecutionContext,
        plan: LogicalPlan,
    ) -> Result<QueryOutput> {
        match plan {
            LogicalPlan::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                let db = self.catalog_reader.get_database("default")?;
                self.authorize(ctx, Action::CreateSchema, ResourceRef::Database(db.id))?;
                match self
                    .catalog_writer
                    .create_schema(nodus_catalog::CreateSchemaRequest {
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
            LogicalPlan::DropSchema {
                schema_name,
                if_exists,
                cascade: _,
            } => {
                let db_name = "default";
                match self.catalog_reader.get_schema(db_name, &schema_name) {
                    Ok(sch) => {
                        self.authorize(ctx, Action::CreateSchema, ResourceRef::Schema(sch.id))?;
                        self.catalog_writer.drop_schema(sch.id)?;
                        Ok(QueryOutput::tag("DROP SCHEMA"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP SCHEMA"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::CreateTable {
                name,
                columns,
                constraints,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&name)?;
                let db = self.catalog_reader.get_database(db_name)?;
                let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
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
                        unique_cols.push((d.clone(), c.primary));
                    }
                }

                let tbl = self.catalog_writer.create_table(CreateTableRequest {
                    id: nodus_catalog::TableId::new(),
                    database_id: db.id,
                    schema_id: sch.id,
                    name: table_only.to_string(),
                    columns: descriptors,
                    constraints,
                    view_query: None,
                })?;

                for (col, primary) in unique_cols {
                    let index = nodus_catalog::IndexDescriptor {
                        id: nodus_catalog::IndexId::new(),
                        name: if primary {
                            format!("{}_pkey", table_only)
                        } else {
                            format!("{}_{}_idx", name, col.name)
                        },
                        version: 1,
                        created_at: Utc::now(),
                        updated_at: Utc::now(),
                        state: DescriptorState::Public,
                        index_type: if primary {
                            nodus_catalog::IndexType::Primary
                        } else {
                            nodus_catalog::IndexType::Unique
                        },
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
            LogicalPlan::CreateView { name, query } => {
                let (db_name, schema_name, view_only) = parse_object_name(&name)?;
                let db = self.catalog_reader.get_database(db_name)?;
                let sch = self.catalog_reader.get_schema(db_name, schema_name)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Schema(sch.id))?;

                // Resolve the schema of the view by planning/executing a dummy pass or just full execute
                // For MVP, we can just execute the query and take its output columns.
                let mut is_valid = false;
                let mut view_cols = Vec::new();

                // Hack: serialize the logical plan to store it
                let view_query_json = serde_json::to_string(&*query)?;

                // Run query to get shape
                if let Ok(out) = self.execute_logical_inner(ctx, *query) {
                    for (i, cname) in out.columns.iter().enumerate() {
                        let ty = out.types.get(i).unwrap_or(&"VARCHAR".to_string()).clone();
                        view_cols.push(ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: cname.clone(),
                            version: 1,
                            created_at: Utc::now(),
                            updated_at: Utc::now(),
                            state: DescriptorState::Public,
                            data_type: ty,
                            nullable: true,
                        });
                    }
                    is_valid = true;
                }

                if !is_valid {
                    anyhow::bail!("Failed to resolve view query schema");
                }

                self.catalog_writer.create_table(CreateTableRequest {
                    id: nodus_catalog::TableId::new(),
                    database_id: db.id,
                    schema_id: sch.id,
                    name: view_only.to_string(),
                    columns: view_cols,
                    constraints: vec![],
                    view_query: Some(view_query_json),
                })?;

                Ok(QueryOutput::tag("CREATE VIEW"))
            }
            LogicalPlan::DropView { name, if_exists } => {
                let (db_name, schema_name, view_only) = parse_object_name(&name)?;
                match self
                    .catalog_reader
                    .get_table(db_name, schema_name, view_only)
                {
                    Ok(tbl) => {
                        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                        if tbl.view_query.is_none() {
                            anyhow::bail!("{} is not a view", name);
                        }
                        self.catalog_writer.drop_table(tbl.id)?;
                        Ok(QueryOutput::tag("DROP VIEW"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP VIEW"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::DropTable { name, if_exists } => {
                let (db_name, schema_name, table_only) = parse_object_name(&name)?;
                match self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)
                {
                    Ok(tbl) => {
                        self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                        self.catalog_writer.drop_table(tbl.id)?;
                        Ok(QueryOutput::tag("DROP TABLE"))
                    }
                    Err(e) => {
                        if if_exists {
                            Ok(QueryOutput::tag("DROP TABLE"))
                        } else {
                            Err(anyhow::anyhow!(e))
                        }
                    }
                }
            }
            LogicalPlan::Insert {
                table_name,
                columns,
                values_list,

                returning,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
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
                    self.check_table_constraints(
                        ctx,
                        &tbl,
                        &row,
                        &col_names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    )?;

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
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::Select {
                ctes,
                table_name,
                table_alias,
                joins,
                projection,
                group_by,
                filter,
                order_by,
                limit,
                offset,
                distinct,
            } => {
                if table_name.eq_ignore_ascii_case("pg_stat_ssl") {
                    return Ok(QueryOutput {
                        columns: vec!["ssl".to_string()],
                        types: vec!["BOOL".to_string()],
                        rows: vec![Row {
                            values: vec![Value::Bool(false)],
                        }],
                        tag: "SELECT 1".into(),
                    });
                }

                let mut cte_results = std::collections::HashMap::new();
                for (name, cte_plan) in ctes {
                    let out = self.execute_logical_inner(ctx, *cte_plan)?;
                    cte_results.insert(name, out);
                }

                let (tbl_cols, mut col_names, mut stored_rows) = if let Some(cte_out) =
                    cte_results.get(&table_name)
                {
                    let mut cols = Vec::new();
                    for (i, c) in cte_out.columns.iter().enumerate() {
                        let ty = cte_out
                            .types
                            .get(i)
                            .unwrap_or(&"VARCHAR".to_string())
                            .clone();
                        cols.push(ColumnDescriptor {
                            id: nodus_catalog::ColumnId::new(),
                            name: c.clone(),
                            version: 1,
                            created_at: chrono::Utc::now(),
                            updated_at: chrono::Utc::now(),
                            state: nodus_catalog::DescriptorState::Public,
                            data_type: ty,
                            nullable: true,
                        });
                    }
                    let prefix = table_alias.as_deref().unwrap_or(&table_name);
                    let col_names = cols
                        .iter()
                        .map(|c| format!("{}.{}", prefix, c.name))
                        .collect();
                    (
                        cols,
                        col_names,
                        Some(cte_out.rows.iter().map(|r| r.values.clone()).collect()),
                    )
                } else {
                    let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                    let schema_name = if schema_name.eq_ignore_ascii_case("public")
                        && Self::is_pg_catalog_virtual_table_name(table_only)
                    {
                        "pg_catalog"
                    } else {
                        schema_name
                    };
                    let (tbl_cols, col_names, rows) = if Self::is_virtual_schema(schema_name) {
                        let (cols, rows) =
                            self.get_virtual_table(db_name, schema_name, table_only)?;
                        let prefix = table_alias.as_deref().unwrap_or(&table_name);
                        let col_names: Vec<String> = cols
                            .iter()
                            .map(|c| format!("{}.{}", prefix, c.name))
                            .collect();
                        (cols, col_names, rows)
                    } else {
                        let tbl =
                            self.catalog_reader
                                .get_table(db_name, schema_name, table_only)?;
                        self.authorize(ctx, Action::Select, ResourceRef::Table(tbl.id))?;

                        let prefix = table_alias.as_deref().unwrap_or(&table_name);
                        let col_names: Vec<String> = tbl
                            .columns
                            .iter()
                            .map(|c| format!("{}.{}", prefix, c.name))
                            .collect();

                        let mut rows = None;
                        let has_session_overlay = self
                            .active_txns
                            .read()
                            .unwrap()
                            .get(&ctx.session_id)
                            .map(|txn| !txn.overlay.is_empty())
                            .unwrap_or(false);
                        if !has_session_overlay
                            && let Some(FilterExpr::Predicate(Predicate {
                                left,
                                op: CompareOp::Eq,
                                right,
                            })) = filter.as_ref()
                        {
                            let col_name = left.split('.').last().unwrap_or(left);
                            if let Some(col) = tbl.columns.iter().find(|c| c.name == *col_name) {
                                for idx in &tbl.indexes {
                                    if idx.key_columns.iter().any(|kc| kc.column_id == col.id) {
                                        let val =
                                            self.eval_operand(&[], &[], &[], right, &col.data_type);
                                        if let Ok(indexed_rows) =
                                            self.index_scan(idx.id, &val, tbl.id, &ctx.session_id)
                                        {
                                            rows = Some(indexed_rows);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        let rows = match rows {
                            Some(r) => r,
                            None => {
                                if let Some(vq) = &tbl.view_query {
                                    let plan: LogicalPlan = serde_json::from_str(vq)?;
                                    let out = self.execute_logical_inner(ctx, plan)?;
                                    out.rows.iter().map(|r| r.values.clone()).collect()
                                } else {
                                    self.scan_rows(tbl.id, &ctx.session_id)?
                                }
                            }
                        };
                        (tbl.columns.clone(), col_names, rows)
                    };
                    (tbl_cols, col_names, Some(rows))
                };

                let mut joined_columns = tbl_cols;
                let mut stored_rows = stored_rows.unwrap();

                for join in &joins {
                    let (j_cols, j_rows) = if let Some(cte_out) = cte_results.get(&join.table_name)
                    {
                        let mut cols = Vec::new();
                        for (i, c) in cte_out.columns.iter().enumerate() {
                            let ty = cte_out
                                .types
                                .get(i)
                                .unwrap_or(&"VARCHAR".to_string())
                                .clone();
                            cols.push(ColumnDescriptor {
                                id: nodus_catalog::ColumnId::new(),
                                name: c.clone(),
                                version: 1,
                                created_at: chrono::Utc::now(),
                                updated_at: chrono::Utc::now(),
                                state: nodus_catalog::DescriptorState::Public,
                                data_type: ty,
                                nullable: true,
                            });
                        }
                        (
                            cols,
                            cte_out.rows.iter().map(|r| r.values.clone()).collect(),
                        )
                    } else {
                        let (j_db, j_sch, j_tbl_name) = parse_object_name(&join.table_name)?;
                        let j_sch = if j_sch.eq_ignore_ascii_case("public")
                            && Self::is_pg_catalog_virtual_table_name(j_tbl_name)
                        {
                            "pg_catalog"
                        } else {
                            j_sch
                        };
                        if Self::is_virtual_schema(j_sch) {
                            let (cols, rows) = self.get_virtual_table(j_db, j_sch, j_tbl_name)?;
                            (cols, rows)
                        } else {
                            let j_tbl = self.catalog_reader.get_table(j_db, j_sch, j_tbl_name)?;
                            self.authorize(ctx, Action::Select, ResourceRef::Table(j_tbl.id))?;
                            let j_rows = if let Some(vq) = &j_tbl.view_query {
                                let plan: LogicalPlan = serde_json::from_str(vq)?;
                                let out = self.execute_logical_inner(ctx, plan)?;
                                out.rows.iter().map(|r| r.values.clone()).collect()
                            } else {
                                self.scan_rows(j_tbl.id, &ctx.session_id)?
                            };
                            (j_tbl.columns.clone(), j_rows)
                        }
                    };

                    let j_prefix = join.table_alias.as_deref().unwrap_or(&join.table_name);
                    let j_col_names: Vec<String> = j_cols
                        .iter()
                        .map(|c| format!("{}.{}", j_prefix, c.name))
                        .collect();

                    let mut combined_cols = col_names.clone();
                    combined_cols.extend(j_col_names.clone());

                    let mut combined_desc = joined_columns.clone();
                    combined_desc.extend(j_cols.clone());

                    let mut next_rows = Vec::new();
                    let mut right_matched = vec![false; j_rows.len()];
                    for r1 in &stored_rows {
                        let mut matched = false;
                        for (j_idx, r2) in j_rows.iter().enumerate() {
                            let mut combined_row = r1.clone();
                            combined_row.extend(r2.clone());
                            if self
                                .eval_filter(
                                    ctx,
                                    &combined_row,
                                    &combined_cols,
                                    &combined_desc,
                                    join.condition.as_ref(),
                                )
                                .unwrap_or(false)
                            {
                                next_rows.push(combined_row);
                                matched = true;
                                right_matched[j_idx] = true;
                            }
                        }
                        if !matched
                            && matches!(join.join_type, JoinType::LeftOuter | JoinType::FullOuter)
                        {
                            let mut combined_row = r1.clone();
                            // Left or Full join requires filling the right side with NULLs
                            let num_nulls = j_cols.len();
                            combined_row.extend(vec![Value::Null; num_nulls]);
                            next_rows.push(combined_row);
                        }
                    }
                    if matches!(join.join_type, JoinType::RightOuter | JoinType::FullOuter) {
                        let left_len = col_names.len();
                        for (j_idx, matched) in right_matched.into_iter().enumerate() {
                            if !matched {
                                let mut combined_row = vec![Value::Null; left_len];
                                combined_row.extend(j_rows[j_idx].clone());
                                next_rows.push(combined_row);
                            }
                        }
                    }
                    stored_rows = next_rows;
                    col_names = combined_cols;
                    joined_columns = combined_desc;
                }

                // WHERE: conjunction of typed predicates.
                stored_rows.retain(|r| {
                    self.eval_filter(ctx, r, &col_names, &joined_columns, filter.as_ref())
                        .unwrap_or(false)
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
                                ProjectionItem::Literal(v)
                                | ProjectionItem::AliasedLiteral(v, _) => {
                                    out_row.push(v.clone());
                                }
                                ProjectionItem::Column(c) | ProjectionItem::AliasedColumn(c, _) => {
                                    let idx = col_names
                                        .iter()
                                        .position(|tc| tc == c || tc.ends_with(&format!(".{}", c)));
                                    // Take from first row of group
                                    out_row.push(
                                        group_rows
                                            .first()
                                            .and_then(|r| idx.and_then(|i| r.get(i)))
                                            .map(|v| v.clone())
                                            .unwrap_or(crate::Value::Null),
                                    );
                                }
                                ProjectionItem::WindowFunction { .. }
                                | ProjectionItem::ScalarFunction { .. }
                                | ProjectionItem::JsonAccess { .. }
                                | ProjectionItem::CaseWhenEq { .. } => {
                                    out_row.push(Value::Null); // MVP fallback
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
                                                                sum_float += (*n) as f64
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
                                                            if compare(&v, cur)
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
                                                            if compare(&v, cur)
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
                                ProjectionItem::Column(c) => {
                                    c.split('.').last().unwrap_or(c).to_string()
                                }
                                ProjectionItem::AliasedColumn(_, a) => a.clone(),
                                ProjectionItem::Literal(_) => "?column?".to_string(),
                                ProjectionItem::AliasedLiteral(_, a) => a.clone(),
                                ProjectionItem::WindowFunction {
                                    func_name, alias, ..
                                } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                ProjectionItem::ScalarFunction {
                                    func_name, alias, ..
                                } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                ProjectionItem::JsonAccess {
                                    left,
                                    operator,
                                    right,
                                    alias,
                                } => alias
                                    .clone()
                                    .unwrap_or_else(|| format!("{}{}{}", left, operator, right)),
                                ProjectionItem::CaseWhenEq {
                                    else_column, alias, ..
                                } => alias.clone().unwrap_or_else(|| {
                                    else_column
                                        .split('.')
                                        .last()
                                        .unwrap_or(else_column)
                                        .to_string()
                                }),
                                ProjectionItem::Aggregate(op, inner) => {
                                    format!("{:?}({})", op, inner)
                                }
                            })
                            .collect()
                    };
                } else {
                    out_cols =
                        if projection.is_empty() {
                            col_names
                                .iter()
                                .map(|c| c.split('.').last().unwrap_or(c).to_string())
                                .collect()
                        } else {
                            projection
                                .iter()
                                .filter_map(|p| match p {
                                    ProjectionItem::Column(c) => {
                                        Some(c.split('.').last().unwrap_or(c).to_string())
                                    }
                                    ProjectionItem::AliasedColumn(_, a) => Some(a.clone()),
                                    ProjectionItem::Literal(_) => Some("?column?".to_string()),
                                    ProjectionItem::AliasedLiteral(_, a) => Some(a.clone()),
                                    ProjectionItem::WindowFunction {
                                        alias, func_name, ..
                                    } => Some(alias.clone().unwrap_or_else(|| func_name.clone())),
                                    ProjectionItem::ScalarFunction {
                                        alias, func_name, ..
                                    } => Some(alias.clone().unwrap_or_else(|| func_name.clone())),
                                    ProjectionItem::JsonAccess {
                                        left,
                                        operator,
                                        right,
                                        alias,
                                    } => Some(alias.clone().unwrap_or_else(|| {
                                        format!("{}{}{}", left, operator, right)
                                    })),
                                    ProjectionItem::CaseWhenEq {
                                        else_column, alias, ..
                                    } => Some(alias.clone().unwrap_or_else(|| {
                                        else_column
                                            .split('.')
                                            .last()
                                            .unwrap_or(else_column)
                                            .to_string()
                                    })),
                                    _ => None,
                                })
                                .collect()
                        };

                    // Evaluate Window Functions and Scalar Expressions before projecting
                    for proj_item in projection.iter() {
                        match proj_item {
                            ProjectionItem::WindowFunction {
                                func_name,
                                partition_by,
                                order_by: w_order_by,
                                alias,
                            } => {
                                let p_indices: Vec<usize> = partition_by
                                    .iter()
                                    .filter_map(|c| {
                                        col_names.iter().position(|tc| {
                                            tc == c || tc.ends_with(&format!(".{}", c))
                                        })
                                    })
                                    .collect();

                                let o_indices: Vec<(usize, bool)> = w_order_by
                                    .iter()
                                    .filter_map(|(c, asc)| {
                                        col_names
                                            .iter()
                                            .position(|tc| {
                                                tc == c || tc.ends_with(&format!(".{}", c))
                                            })
                                            .map(|idx| (idx, *asc))
                                    })
                                    .collect();

                                let mut row_indices: Vec<usize> = (0..stored_rows.len()).collect();
                                row_indices.sort_by(|&a_idx, &b_idx| {
                                    let a = &stored_rows[a_idx];
                                    let b = &stored_rows[b_idx];
                                    for &p_idx in &p_indices {
                                        let cmp = compare(
                                            a.get(p_idx).unwrap_or(&Value::Null),
                                            b.get(p_idx).unwrap_or(&Value::Null),
                                        );
                                        if cmp != std::cmp::Ordering::Equal {
                                            return cmp;
                                        }
                                    }
                                    for &(o_idx, asc) in &o_indices {
                                        let mut cmp = compare(
                                            a.get(o_idx).unwrap_or(&Value::Null),
                                            b.get(o_idx).unwrap_or(&Value::Null),
                                        );
                                        if !asc {
                                            cmp = cmp.reverse();
                                        }
                                        if cmp != std::cmp::Ordering::Equal {
                                            return cmp;
                                        }
                                    }
                                    std::cmp::Ordering::Equal
                                });

                                let mut results = vec![Value::Null; stored_rows.len()];
                                if func_name == "ROW_NUMBER" {
                                    let mut current_partition: Vec<Value> = Vec::new();
                                    let mut row_num = 1i64;
                                    for &row_idx in &row_indices {
                                        let row = &stored_rows[row_idx];
                                        let partition_key: Vec<Value> = p_indices
                                            .iter()
                                            .map(|&idx| {
                                                row.get(idx).unwrap_or(&Value::Null).clone()
                                            })
                                            .collect();
                                        if partition_key != current_partition {
                                            current_partition = partition_key;
                                            row_num = 1;
                                        }
                                        results[row_idx] = Value::Int(row_num);
                                        row_num += 1;
                                    }
                                } else {
                                    anyhow::bail!("Unsupported window function: {}", func_name);
                                }

                                // Append the result to `stored_rows`
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias.clone().unwrap_or_else(|| func_name.clone());
                                col_names.push(w_col_name);
                            }
                            ProjectionItem::ScalarFunction {
                                func_name,
                                args,
                                alias,
                            } => {
                                let mut results = vec![Value::Null; stored_rows.len()];
                                for (row_idx, row) in stored_rows.iter().enumerate() {
                                    let resolved: Vec<Value> = args
                                        .iter()
                                        .map(|a| resolve_scalar_arg(a, row, &col_names))
                                        .collect();
                                    results[row_idx] = eval_scalar_function(func_name, &resolved);
                                }
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias.clone().unwrap_or_else(|| func_name.clone());
                                col_names.push(w_col_name);
                            }
                            ProjectionItem::JsonAccess {
                                left,
                                operator,
                                right,
                                alias,
                            } => {
                                let mut results = vec![Value::Null; stored_rows.len()];
                                for (row_idx, row) in stored_rows.iter().enumerate() {
                                    let c_idx = col_names.iter().position(|tc| {
                                        tc == left || tc.ends_with(&format!(".{}", left))
                                    });
                                    if let Some(i) = c_idx {
                                        if let Some(v) = row.get(i) {
                                            if operator == "->>" {
                                                let json_str = match v {
                                                    Value::Jsonb(j) => j.to_string(),
                                                    Value::Text(s) => s.clone(),
                                                    _ => "".to_string(),
                                                };
                                                if let Ok(json) =
                                                    serde_json::from_str::<serde_json::Value>(
                                                        &json_str,
                                                    )
                                                {
                                                    if let Some(obj) = json.as_object() {
                                                        if let Some(val) = obj.get(right) {
                                                            results[row_idx] = match val {
                                                                serde_json::Value::String(s) => {
                                                                    Value::Text(s.clone())
                                                                }
                                                                _ => Value::Text(val.to_string()),
                                                            };
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                for (row_idx, row) in stored_rows.iter_mut().enumerate() {
                                    row.push(results[row_idx].clone());
                                }
                                let w_col_name = alias
                                    .clone()
                                    .unwrap_or_else(|| format!("{}{}{}", left, operator, right));
                                col_names.push(w_col_name);
                            }
                            _ => {}
                        }
                    }

                    let indices: Vec<Option<usize>> = out_cols
                        .iter()
                        .enumerate()
                        .map(|(pi, c)| {
                            if projection.is_empty() {
                                // `out_cols` mirrors `col_names` positionally (just unqualified);
                                // resolving by name would collapse duplicate column names across joined tables.
                                Some(pi)
                            } else {
                                let actual_col = match &projection[pi] {
                                    ProjectionItem::Column(c) => c.clone(),
                                    ProjectionItem::AliasedColumn(c, _) => c.clone(),
                                    ProjectionItem::Literal(_)
                                    | ProjectionItem::AliasedLiteral(_, _) => "".to_string(),
                                    ProjectionItem::WindowFunction {
                                        func_name, alias, ..
                                    } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                    ProjectionItem::ScalarFunction {
                                        func_name, alias, ..
                                    } => alias.clone().unwrap_or_else(|| func_name.clone()),
                                    ProjectionItem::JsonAccess {
                                        left,
                                        operator,
                                        right,
                                        alias,
                                    } => alias.clone().unwrap_or_else(|| {
                                        format!("{}{}{}", left, operator, right)
                                    }),
                                    ProjectionItem::CaseWhenEq {
                                        else_column, alias, ..
                                    } => alias.clone().unwrap_or_else(|| else_column.clone()),
                                    _ => c.clone(),
                                };
                                col_names.iter().position(|tc| {
                                    tc == &actual_col || tc.ends_with(&format!(".{}", actual_col))
                                })
                            }
                        })
                        .collect();

                    out_rows = stored_rows
                        .into_iter()
                        .map(|r| {
                            if projection.is_empty() {
                                indices
                                    .iter()
                                    .map(|i| {
                                        i.and_then(|idx| r.get(idx))
                                            .cloned()
                                            .unwrap_or(crate::Value::Null)
                                    })
                                    .collect()
                            } else {
                                projection
                                    .iter()
                                    .enumerate()
                                    .map(|(pi, proj)| match proj {
                                        ProjectionItem::Literal(v)
                                        | ProjectionItem::AliasedLiteral(v, _) => v.clone(),
                                        ProjectionItem::CaseWhenEq {
                                            left,
                                            equals,
                                            then_value,
                                            then_column,
                                            else_column,
                                            ..
                                        } => {
                                            let left_idx = col_names.iter().position(|tc| {
                                                tc == left || tc.ends_with(&format!(".{}", left))
                                            });
                                            let else_idx = col_names.iter().position(|tc| {
                                                tc == else_column
                                                    || tc.ends_with(&format!(".{}", else_column))
                                            });
                                            let left_value = left_idx
                                                .and_then(|idx| r.get(idx))
                                                .unwrap_or(&Value::Null);
                                            if compare(left_value, equals)
                                                == std::cmp::Ordering::Equal
                                            {
                                                if let Some(then_column) = then_column {
                                                    col_names
                                                        .iter()
                                                        .position(|tc| {
                                                            tc == then_column
                                                                || tc.ends_with(&format!(
                                                                    ".{}",
                                                                    then_column
                                                                ))
                                                        })
                                                        .and_then(|idx| r.get(idx))
                                                        .cloned()
                                                        .unwrap_or(Value::Null)
                                                } else {
                                                    then_value.clone()
                                                }
                                            } else {
                                                else_idx
                                                    .and_then(|idx| r.get(idx))
                                                    .cloned()
                                                    .unwrap_or(Value::Null)
                                            }
                                        }
                                        _ => indices[pi]
                                            .and_then(|idx| r.get(idx))
                                            .cloned()
                                            .unwrap_or(crate::Value::Null),
                                    })
                                    .collect()
                            }
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
                    .map(|r| Row { values: r })
                    .collect::<Vec<_>>();

                let tag = format!("SELECT {}", rows.len());
                let mut types = Vec::new();
                for (i, c) in out_cols.iter().enumerate() {
                    // Quick lookup for type. Default to VARCHAR.
                    let mut ty = "VARCHAR".to_string();

                    if projection.is_empty() {
                        if let Some(col_desc) = joined_columns.get(i) {
                            ty = col_desc.data_type.clone();
                        }
                    } else if let Some(source_col) = projection.get(i).and_then(|item| match item {
                        ProjectionItem::Column(col) | ProjectionItem::AliasedColumn(col, _) => {
                            Some(col)
                        }
                        _ => None,
                    }) {
                        if let Some(source_idx) = col_names.iter().position(|candidate| {
                            candidate == source_col
                                || candidate.ends_with(&format!(".{}", source_col))
                        }) {
                            if let Some(col_desc) = joined_columns.get(source_idx) {
                                ty = col_desc.data_type.clone();
                            }
                        }
                    }

                    if ty == "VARCHAR" && !rows.is_empty() {
                        if let Some(val) = rows[0].values.get(i) {
                            match val {
                                Value::Int(_) => ty = "INTEGER".to_string(),
                                Value::Float(_) => ty = "DOUBLE".to_string(),
                                Value::Bool(_) => ty = "BOOLEAN".to_string(),
                                Value::Text(_) => ty = "VARCHAR".to_string(),
                                Value::Null => ty = "VARCHAR".to_string(),
                                Value::Array(_) => ty = "VARCHAR".to_string(),
                                Value::Jsonb(_) => ty = "VARCHAR".to_string(),
                            }
                        }
                    } else if ty == "VARCHAR" {
                        // Also try to deduce from projection items if available
                        if i < projection.len() {
                            match &projection[i] {
                                ProjectionItem::Literal(Value::Int(_))
                                | ProjectionItem::AliasedLiteral(Value::Int(_), _) => {
                                    ty = "INTEGER".to_string()
                                }
                                ProjectionItem::Literal(Value::Float(_))
                                | ProjectionItem::AliasedLiteral(Value::Float(_), _) => {
                                    ty = "DOUBLE".to_string()
                                }
                                ProjectionItem::Literal(Value::Bool(_))
                                | ProjectionItem::AliasedLiteral(Value::Bool(_), _) => {
                                    ty = "BOOLEAN".to_string()
                                }
                                _ => {}
                            }
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
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
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
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::Delete {
                table_name,
                filter,
                returning,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
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
                        types: Self::returning_types(&tbl.columns, &returning),
                        rows,
                    })
                }
            }
            LogicalPlan::AlterTable {
                table_name,
                operation,
            } => {
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
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
                    AlterTableOp::RenameColumn { old_name, new_name } => {
                        nodus_catalog::TableDescriptorChange::RenameColumn {
                            table_id: tbl.id,
                            old_name,
                            new_name,
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
                let (db_name, schema_name, table_only) = parse_object_name(&table_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
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
            LogicalPlan::CreateRole { name } => {
                // Must be superuser or have create role privilege in a real system
                self.catalog_writer
                    .create_role(nodus_catalog::CreateRoleRequest {
                        id: nodus_catalog::PrincipalId::new(),
                        name: name.clone(),
                        principal_type: nodus_catalog::PrincipalType::Role,
                        database_id: None,
                    })?;
                Ok(QueryOutput::tag("CREATE ROLE"))
            }
            LogicalPlan::Grant {
                privilege,
                object_name,
                grantee,
            } => {
                let role = self.catalog_reader.get_principal_by_name(&grantee)?;
                let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                // Typically you need to own the table or have grant option
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                self.catalog_writer
                    .grant_privileges(nodus_catalog::GrantPrivilegesRequest {
                        id: nodus_catalog::GrantId::new(),
                        principal_id: role.id,
                        resource: ResourceRef::Table(tbl.id),
                        privilege: privilege.clone(),
                    })?;
                Ok(QueryOutput::tag("GRANT"))
            }
            LogicalPlan::Revoke {
                privilege,
                object_name,
                revokee,
            } => {
                let role = self.catalog_reader.get_principal_by_name(&revokee)?;
                let (db_name, schema_name, table_only) = parse_object_name(&object_name)?;
                let tbl = self
                    .catalog_reader
                    .get_table(db_name, schema_name, table_only)?;
                self.authorize(ctx, Action::CreateTable, ResourceRef::Table(tbl.id))?;
                self.catalog_writer
                    .revoke_privileges(nodus_catalog::RevokePrivilegesRequest {
                        principal_id: role.id,
                        resource: ResourceRef::Table(tbl.id),
                        privilege: privilege.clone(),
                    })?;
                Ok(QueryOutput::tag("REVOKE"))
            }
            LogicalPlan::Begin => {
                let txn_record = self.txn.begin_txn()?;
                self.active_txns.write().unwrap().insert(
                    ctx.session_id.clone(),
                    ActiveTxn::new(txn_record.txn_id, txn_record.read_ts),
                );
                Ok(QueryOutput::tag("BEGIN"))
            }
            LogicalPlan::Commit => {
                if let Some(txn) = self.active_txns.write().unwrap().remove(&ctx.session_id) {
                    let commit_ts = self.txn.commit_txn(txn.txn_id)?;
                    self.kv.commit(txn.txn_id, commit_ts)?;
                }
                Ok(QueryOutput::tag("COMMIT"))
            }
            LogicalPlan::Rollback => {
                if let Some(txn) = self.active_txns.write().unwrap().remove(&ctx.session_id) {
                    self.txn.abort_txn(txn.txn_id)?;
                    self.kv.abort(txn.txn_id)?;
                }
                Ok(QueryOutput::tag("SAVEPOINT"))
            }
            LogicalPlan::Savepoint { name } => {
                let mut guard = self.active_txns.write().unwrap();
                let txn = guard.get_mut(&ctx.session_id).ok_or_else(|| {
                    anyhow::anyhow!("SAVEPOINT can only be used in transaction blocks")
                })?;
                txn.savepoints.push(SavepointState {
                    name,
                    write_log_len: txn.write_log.len(),
                    overlay: txn.overlay.clone(),
                });
                Ok(QueryOutput::tag("SAVEPOINT"))
            }
            LogicalPlan::RollbackToSavepoint { name } => {
                let (txn_id, affected, snapshot, keep_len, keep_savepoints) = {
                    let guard = self.active_txns.read().unwrap();
                    let txn = guard.get(&ctx.session_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "ROLLBACK TO SAVEPOINT can only be used in transaction blocks"
                        )
                    })?;
                    let savepoint_idx = txn
                        .savepoints
                        .iter()
                        .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(&name))
                        .ok_or_else(|| anyhow::anyhow!("savepoint \"{}\" does not exist", name))?;
                    let savepoint = txn.savepoints[savepoint_idx].clone();
                    let affected = txn.write_log[savepoint.write_log_len..].to_vec();
                    (
                        txn.txn_id,
                        affected,
                        savepoint.overlay,
                        savepoint.write_log_len,
                        savepoint_idx + 1,
                    )
                };

                let mut unique_keys = affected;
                unique_keys.sort();
                unique_keys.dedup();
                for key in unique_keys {
                    let replacement = match snapshot.get(&key) {
                        Some(Some(value)) => IntentReplacement::Put(Bytes::from(value.clone())),
                        Some(None) => IntentReplacement::Delete,
                        None => IntentReplacement::Clear,
                    };
                    self.kv
                        .replace_intent(txn_id, Bytes::from(key), replacement)?;
                }

                let mut guard = self.active_txns.write().unwrap();
                if let Some(txn) = guard.get_mut(&ctx.session_id) {
                    txn.overlay = snapshot;
                    txn.write_log.truncate(keep_len);
                    txn.savepoints.truncate(keep_savepoints);
                }
                Ok(QueryOutput::tag("ROLLBACK"))
            }
            LogicalPlan::ReleaseSavepoint { name } => {
                let mut guard = self.active_txns.write().unwrap();
                let txn = guard.get_mut(&ctx.session_id).ok_or_else(|| {
                    anyhow::anyhow!("RELEASE SAVEPOINT can only be used in transaction blocks")
                })?;
                let savepoint_idx = txn
                    .savepoints
                    .iter()
                    .rposition(|savepoint| savepoint.name.eq_ignore_ascii_case(&name))
                    .ok_or_else(|| anyhow::anyhow!("savepoint \"{}\" does not exist", name))?;
                txn.savepoints.truncate(savepoint_idx);
                Ok(QueryOutput::tag("RELEASE"))
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
            LogicalPlan::Noop { tag } => Ok(QueryOutput::tag(&tag)),
            LogicalPlan::SelectLiteral { values } => {
                let mut columns = Vec::new();
                let mut types = Vec::new();
                let mut row_values = Vec::new();

                for (alias, value) in values {
                    columns.push(alias);
                    types.push(match &value {
                        Value::Int(_) => "INTEGER".to_string(),
                        Value::Float(_) => "DOUBLE".to_string(),
                        Value::Bool(_) => "BOOLEAN".to_string(),
                        _ => "VARCHAR".to_string(),
                    });
                    row_values.push(value);
                }

                Ok(QueryOutput {
                    columns,
                    types,
                    rows: vec![Row { values: row_values }],
                    tag: "SELECT 1".into(),
                })
            }
            LogicalPlan::UnionAll { left, right } => {
                let mut left_out = self.execute_logical_inner(ctx, *left)?;
                let right_out = self.execute_logical_inner(ctx, *right)?;
                // Assume columns match in count.
                left_out.rows.extend(right_out.rows);
                Ok(left_out)
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
                primary: false,
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
            constraints: vec![],
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
                constraints: vec![],
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
                    ctes: vec![],
                    table_alias: None,
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
        assert_eq!(all.columns, vec!["id", "title", "author"]);
        assert_eq!(all.rows.len(), 2);

        // Projection + filter.
        let one = exec
            .execute_logical(
                &ctx,
                LogicalPlan::Select {
                    ctes: vec![],
                    table_alias: None,
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
                constraints: vec![],
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
                    ctes: vec![],
                    table_alias: None,
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
        assert_eq!(render_row(&out.rows[0]), vec!["7", "widget", "t"]);
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
                constraints: vec![],
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
                    ctes: vec![],
                    table_alias: None,
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
                constraints: vec![],
                name: "authors".into(),
                columns: cols(&[("id", "INT"), ("name", "TEXT")]),
            },
        )
        .unwrap();

        // Create books
        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                constraints: vec![],
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
            ctes: vec![],
            table_alias: None,
            group_by: vec![],
            table_name: "books".into(),
            joins: vec![Join {
                table_alias: None,
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
        assert_eq!(out.columns, vec!["title", "name"]);
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
                constraints: vec![],
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
                    ctes: vec![],
                    table_alias: None,
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
                constraints: vec![],
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
                constraints: vec![],
                name: "users".into(),
                columns: cols(&[("id", "int"), ("name", "text")]),
            },
        )
        .unwrap();

        exec.execute_logical(
            &ctx,
            LogicalPlan::CreateTable {
                constraints: vec![],
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
        let bob_row = left
            .rows
            .iter()
            .find(|r| r.values[1] == Value::Text("Bob".to_string()))
            .unwrap();
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
                constraints: vec![],
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
                            ctes: vec![],
                            table_alias: None,
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
    use super::{Row, render};

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
                constraints: vec![],
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
            let mut res: Vec<String> = out
                .rows
                .into_iter()
                .map(|r| render_row(&r).join(","))
                .collect();
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

    #[test]
    fn test_scalar_functions() {
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

        let run = |sql: &str| {
            let mut stmts = nodus_sql::parse_sql(sql).unwrap();
            let plan = plan_statement(&stmts.remove(0), &[]).unwrap();
            exec.execute_logical(&ctx, plan).unwrap()
        };

        run("CREATE TABLE t (id INT, name TEXT)");
        run("INSERT INTO t (id, name) VALUES (1, 'Alice')");

        // Column args resolve per row; string/numeric literal args (e.g. SUBSTR
        // start/len, ROUND digits) are now captured by the planner.
        let out = run(
            "SELECT UPPER(name), LOWER(name), LENGTH(name), SUBSTR(name, 1, 3), \
             COALESCE(name, 'x'), CONCAT(name, '!'), REPLACE(name, 'lic', 'LIC'), \
             ROUND(12.345, 1) FROM t",
        );
        assert_eq!(out.rows.len(), 1);
        let row = render_row(&out.rows[0]);
        assert_eq!(row[0], "ALICE"); // UPPER
        assert_eq!(row[1], "alice"); // LOWER
        assert_eq!(row[2], "5"); // LENGTH
        assert_eq!(row[3], "Ali"); // SUBSTR(name, 1, 3)
        assert_eq!(row[4], "Alice"); // COALESCE(name, 'x')
        assert_eq!(row[5], "Alice!"); // CONCAT(name, '!')
        assert_eq!(row[6], "ALICe"); // REPLACE(name, 'lic', 'LIC')
        assert_eq!(row[7], "12.3"); // ROUND(12.345, 1)
    }
}

#[cfg(test)]
mod phase3_tests {
    use super::{Row, render};

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
                constraints: vec![],
                name: "employees".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                        primary: true,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: false,
                        primary: false,
                    },
                    ColumnDef {
                        name: "dept_id".into(),
                        data_type: "INT".into(),
                        nullable: true,
                        unique: false,
                        primary: false,
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
                constraints: vec![],
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
            ctes: vec![],
            table_alias: None,
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
                    ctes: vec![],
                    table_alias: None,
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
                constraints: vec![],
                name: "users".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                        primary: true,
                    },
                    ColumnDef {
                        name: "email".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: true,
                        primary: false,
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
                constraints: vec![],
                name: "products".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                        primary: true,
                    },
                    ColumnDef {
                        name: "category".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: false,
                        primary: false,
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
                    ctes: vec![],
                    table_alias: None,
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
                    ctes: vec![],
                    table_alias: None,
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
                    ctes: vec![],
                    table_alias: None,
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
                    ctes: vec![],
                    table_alias: None,
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
                constraints: vec![],
                name: "users".into(),
                columns: vec![
                    ColumnDef {
                        name: "id".into(),
                        data_type: "INT".into(),
                        nullable: false,
                        unique: true,
                        primary: true,
                    },
                    ColumnDef {
                        name: "name".into(),
                        data_type: "TEXT".into(),
                        nullable: false,
                        unique: false,
                        primary: false,
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
                    ctes: vec![],
                    table_alias: None,
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
                    ctes: vec![],
                    table_alias: None,
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
                    ctes: vec![],
                    table_alias: None,
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
