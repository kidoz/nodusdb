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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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

/// A set-returning function used in `FROM` (e.g. `unnest(arr)`,
/// `generate_series(a, b)`), optionally with `WITH ORDINALITY`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableFnSpec {
    /// Lowercased function name (`"unnest"`, `"generate_series"`).
    pub name: String,
    /// Arguments: an [`Operand::Literal`] for a constant/parameter, or an
    /// [`Operand::Ident`] for a column reference — the latter makes the call
    /// *lateral* (resolved against each driving row).
    pub args: Vec<Operand>,
    /// `WITH ORDINALITY` / `WITH OFFSET`: append a 1-based index column.
    pub with_ordinality: bool,
    /// Output relation alias (and default value-column name).
    pub alias: Option<String>,
    /// Explicit column names from `AS alias(col[, ord])`.
    pub column_aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    pub table_name: String,
    pub table_alias: Option<String>,
    pub condition: Option<FilterExpr>,
    pub join_type: JoinType,
    /// When set, this join's right side is a (possibly lateral) table function
    /// evaluated per driving row rather than a base/CTE relation.
    #[serde(default)]
    pub table_fn: Option<TableFnSpec>,
    /// Columns named in a `USING (...)` clause. The join matches rows whose values
    /// are equal in each named column on both sides; resolved against the actual
    /// row schemas at execution time (so it composes with chained joins).
    #[serde(default)]
    pub using_columns: Vec<String>,
    /// `true` for a `NATURAL JOIN`: an equi-join over every column name common to
    /// both inputs, also resolved at execution time.
    #[serde(default)]
    pub natural: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AggregateOp {
    Count,
    Sum,
    Min,
    Max,
    // New variants are appended so older serialized plans still decode.
    Avg,
}

/// A general scalar expression tree for computed SELECT-list items. Kept
/// serializable (it rides on the replicated `LogicalPlan`); new variants are
/// appended so older encodings still decode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ScalarExpr {
    Literal(Value),
    /// A column reference, resolved by name against the row at evaluation time.
    Column(String),
    Unary {
        op: ScalarUnaryOp,
        expr: Box<ScalarExpr>,
    },
    Binary {
        op: ScalarBinaryOp,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    /// `expr::target` — `target` is the SQL type name (e.g. `FLOAT8`).
    Cast {
        expr: Box<ScalarExpr>,
        target: String,
    },
    /// A scalar function call; `name` is upper-cased.
    Function {
        name: String,
        args: Vec<ScalarExpr>,
    },
    IsNull {
        expr: Box<ScalarExpr>,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ScalarUnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum ScalarBinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
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
    /// Searched or simple `CASE`: the first branch whose predicate matches yields
    /// its result; otherwise `else_result` (or NULL). Results are literals or
    /// column references.
    Case {
        /// Each `(predicate, result)`: the first matching predicate's result is
        /// used. Predicates are single comparisons (the common CASE shape).
        branches: Vec<(Predicate, Operand)>,
        else_result: Option<Operand>,
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
    /// A computed scalar expression over the row (arithmetic, comparisons,
    /// casts, string ops, nested function calls). Appended last so older
    /// serialized plans still decode.
    Expr {
        expr: ScalarExpr,
        alias: Option<String>,
    },
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
    AlterColumnType {
        name: String,
        data_type: String,
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
        /// `CREATE TABLE IF NOT EXISTS` — a no-op when the table already exists.
        /// Defaulted so plans serialized before this field decode.
        #[serde(default)]
        if_not_exists: bool,
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
        /// Each `SET col = <expr>`; the expression is evaluated per matched row
        /// against that row's *old* values.
        assignments: Vec<(String, ScalarExpr)>,
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
    /// A standalone (non-lateral) set-returning function in `FROM`, e.g.
    /// `SELECT * FROM generate_series(1, 5)`. Lateral table functions are carried
    /// on [`Join::table_fn`] instead.
    TableFunction(TableFnSpec),
}

/// The kind of set operation combining two query results.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}
