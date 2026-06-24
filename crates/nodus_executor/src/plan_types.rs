//! Logical plan and query AST types: predicates, filter expressions, joins,
//! aggregates, projection items, DDL alterations, and the `LogicalPlan` tree
//! the planner produces and the executor consumes.

use crate::{ColumnDef, Value};
use serde::{Deserialize, Serialize};

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
    /// Cartesian product (`CROSS JOIN`); carries no `ON` condition.
    Cross,
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
        /// Function arguments: the target column for LAG/LEAD and aggregate
        /// windows, plus an optional offset literal for LAG/LEAD.
        args: Vec<String>,
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
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
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
        /// `HAVING` predicate applied to groups after aggregation.
        having: Option<FilterExpr>,
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
    SetOp {
        op: SetOpKind,
        /// `ALL` keeps duplicates; otherwise the result is a distinct multiset.
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
}

/// The kind of set operation combining two query results.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}
