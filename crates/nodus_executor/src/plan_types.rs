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
    InList {
        left: String,
        list: Vec<Operand>,
        negated: bool,
    },
    InSubquery {
        left: String,
        subquery: Box<LogicalPlan>,
        negated: bool,
        /// When the left side is a literal rather than a column (e.g.
        /// `1 IN (SELECT ...)`), its value; `left` is then unused.
        #[serde(default)]
        left_value: Option<crate::Value>,
    },
    /// `col <op> (<scalar subquery>)` — the subquery yields a single value.
    CompareSubquery {
        left: String,
        op: CompareOp,
        subquery: Box<LogicalPlan>,
    },
    /// `<expr> <op> <expr>` where at least one side is a computed scalar
    /// expression rather than a bare column/literal (e.g. `n % 2 = 0`,
    /// `a = b + 1`). Both sides evaluate per row via `eval_scalar_expr`.
    ExprCmp {
        left: ScalarExpr,
        op: CompareOp,
        right: ScalarExpr,
    },
    /// `[NOT] EXISTS (<subquery>)`. The subquery may reference outer columns
    /// (correlated); those references are substituted with the current row's
    /// values before the subquery is executed, and the predicate is true iff
    /// the subquery yields at least one row (negated flips the result).
    Exists {
        subquery: Box<LogicalPlan>,
        negated: bool,
    },
    /// Any other boolean condition, evaluated per row as a scalar expression;
    /// the row passes only when it yields `true` (NULL and false filter it out).
    Scalar(ScalarExpr),
    /// `left <op> ANY|ALL (<subquery>)`, comparing against every value of the
    /// subquery's single column; the subquery may be correlated.
    QuantifiedSubquery {
        left: ScalarExpr,
        op: ScalarBinaryOp,
        subquery: Box<LogicalPlan>,
        all: bool,
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
    /// The types a column definition list gives them (`AS r(a int, b
    /// text)`, for `jsonb_to_record`).
    #[serde(default)]
    pub column_types: Vec<Option<String>>,
    /// The arguments as expressions (`generate_series(1, n + 1)`), evaluated
    /// against the driving row; when present they replace `args`. Defaulted
    /// so older plans decode.
    #[serde(default)]
    pub arg_exprs: Vec<ScalarExpr>,
    /// `ROWS FROM (f(), g())`: these functions run together, their rows
    /// paired up in order and the shorter padded with NULL; when present,
    /// `name` and the arguments are unused. Defaulted so older plans decode.
    #[serde(default)]
    pub rows_from: Vec<TableFnSpec>,
    /// `JSON_TABLE(...)`: the row path and the columns, with the context
    /// document (and the `PASSING` variables) in `args`/`arg_exprs`.
    /// Defaulted so older plans decode.
    #[serde(default)]
    pub json_table: Option<JsonTableSpec>,
    /// `XMLTABLE(...)`: the row path, the namespaces, and the columns, with
    /// the document in `args`/`arg_exprs`. Defaulted so older plans decode.
    #[serde(default)]
    pub xml_table: Option<XmlTableSpec>,
    /// The `table_to_xml` family: the mapping's rows are gathered by this
    /// plan. Defaulted so older plans decode.
    #[serde(default)]
    pub xml_mapping: Option<XmlMappingSpec>,
    /// A `lo_*`/`loread`/`lowrite` call: the large-object operation runs
    /// where the plan runs, in the statement's transaction. Defaulted so
    /// older plans decode.
    #[serde(default)]
    pub large_object: Option<LargeObjectSpec>,
}

/// The large-object call of a [`TableFnSpec`]: the function's name, its
/// arguments, and the type it returns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LargeObjectSpec {
    /// The function's name (lower-cased), which the result column takes.
    pub function: String,
    /// The call's arguments, evaluated where the plan runs.
    pub args: Vec<ScalarExpr>,
    /// The type the call returns (`OID`, `BYTEA`, `INTEGER`, `BIGINT`, or
    /// `VOID`).
    pub return_type: String,
}

/// The SQL-to-XML mapping of a [`TableFnSpec`] (`table_to_xml`,
/// `query_to_xml`, `cursor_to_xml`, `schema_to_xml`, `database_to_xml`, and
/// their `*_xmlschema` and `*_and_xmlschema` forms): the mapping produces
/// one `xml` value, its rows read where the plan runs.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XmlMappingSpec {
    /// The function's name, which the result column takes.
    pub function: String,
    /// Where the mapped rows come from.
    pub kind: XmlMappingKind,
    /// `nulls`: write a null as an `xsi:nil` element.
    pub nulls: ScalarExpr,
    /// `tableforest`: one element per row, instead of a `<table>` root.
    pub tableforest: ScalarExpr,
    /// `targetns`: the target namespace, declared on the top-level element.
    pub targetns: ScalarExpr,
    /// Which of the three forms the function writes.
    pub form: XmlMappingForm,
}

/// Where an [`XmlMappingSpec`]'s rows come from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum XmlMappingKind {
    /// `table_to_xml(tbl, ...)`: a relation, by name or OID.
    Table(ScalarExpr),
    /// `query_to_xml(query, ...)`: a query, read when the mapping runs.
    Query(ScalarExpr),
    /// `cursor_to_xml(name, count, ...)`: rows fetched from an open cursor.
    Cursor { name: ScalarExpr, count: ScalarExpr },
    /// `schema_to_xml(schema, ...)`: every table of the schema.
    Schema(ScalarExpr),
    /// `database_to_xml(...)`: every table of the database.
    Database,
}

/// Which of the family's three forms a mapping writes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum XmlMappingForm {
    /// The mapped rows (`table_to_xml`, ...).
    Data,
    /// Only the XSD schema (`table_to_xmlschema`, ...).
    Schema,
    /// The rows with the schema after the root element's start
    /// (`table_to_xml_and_xmlschema`, ...).
    AndSchema,
}

/// The `XMLTABLE(...)` clause of a [`TableFnSpec`]: the row path, the
/// namespace prefixes, and the columns in the order they were written.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XmlTableSpec {
    /// The row path: the XPath expression the rows are selected with.
    pub path: ScalarExpr,
    /// `XMLNAMESPACES('uri' AS prefix, ...)`: the prefixes the paths may
    /// use; `None` is a `DEFAULT` item, which PostgreSQL refuses.
    #[serde(default)]
    pub namespaces: Vec<XmlTableNamespace>,
    /// The columns of `COLUMNS (...)`, in order.
    pub columns: Vec<XmlTableColumn>,
    /// A clause PostgreSQL refuses when it parses the call, as the error it
    /// raises; evaluating the table raises it before any row.
    #[serde(default)]
    pub refuse: Option<String>,
}

/// One item of `XMLTABLE`'s `XMLNAMESPACES` clause.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XmlTableNamespace {
    /// The prefix, or `None` for `DEFAULT`.
    pub prefix: Option<String>,
    /// The namespace URI, a text expression.
    pub uri: ScalarExpr,
}

/// One `XMLTABLE` column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XmlTableColumn {
    /// The output column name (also the default path, when none is given).
    pub name: String,
    /// What the column extracts.
    pub kind: XmlTableColumnKind,
}

/// How an `XMLTABLE` column produces its value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum XmlTableColumnKind {
    /// `name FOR ORDINALITY`: the 1-based number of the row.
    Ordinality,
    /// `name type [PATH path] [DEFAULT expr] [NOT NULL | NULL]`: the node
    /// the path selects as the column's type.
    Value {
        /// The column's declared type.
        column_type: String,
        /// The path, defaulting to the column name.
        #[serde(default)]
        path: Option<ScalarExpr>,
        /// `DEFAULT expr`, already coerced to the column type.
        #[serde(default)]
        default: Option<ScalarExpr>,
        /// `NOT NULL`: a null value is refused.
        #[serde(default)]
        not_null: bool,
    },
}

/// The `JSON_TABLE` clause of a [`TableFnSpec`]: the row path with its
/// `ON ERROR` behavior, and the columns in the order they were written.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonTableSpec {
    /// The row path (a `jsonpath`), evaluated against the context document.
    pub path: ScalarExpr,
    /// `{ ERROR | EMPTY [ARRAY] } ON ERROR`; the default is `EMPTY`.
    pub on_error: String,
    /// The columns of `COLUMNS (...)`, in order (nested groups included).
    pub columns: Vec<JsonTableColumn>,
    /// A clause PostgreSQL refuses when it parses the call, as the error it
    /// raises; evaluating the table raises it before any row.
    #[serde(default)]
    pub refuse: Option<String>,
}

/// One `JSON_TABLE` column.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonTableColumn {
    /// The output column name (also the default path's key).
    pub name: String,
    /// What the column extracts.
    pub kind: JsonTableColumnKind,
}

/// How a `JSON_TABLE` column produces its value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JsonTableColumnKind {
    /// `name FOR ORDINALITY`: the 1-based number of the row.
    Ordinality,
    /// `name type EXISTS [PATH ...]`: whether the path has an item.
    Exists {
        /// The column's declared type (the boolean is cast to it).
        column_type: String,
        /// The path, defaulting to `$.name`.
        path: ScalarExpr,
        /// `{ ERROR | TRUE | FALSE | UNKNOWN } ON ERROR`; the default is
        /// `FALSE`.
        on_error: String,
    },
    /// `name type [FORMAT JSON] [PATH ...] [wrapper] [quotes] [behavior ON
    /// EMPTY|ERROR]`: the item as the column's type.
    Scalar {
        /// The column's declared type.
        column_type: String,
        /// `FORMAT JSON`: the item is read as a JSON document, its text
        /// converted to the column's type.
        format: bool,
        /// The `ENCODING name` clause of `FORMAT JSON`, which only a
        /// `bytea` column takes.
        #[serde(default)]
        encoded: bool,
        /// The path, defaulting to `$.name`.
        path: ScalarExpr,
        /// `WITHOUT | WITH [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER`.
        wrapper: String,
        /// `KEEP | OMIT QUOTES [ON SCALAR STRING]`.
        quotes: String,
        /// `{ ERROR | NULL | DEFAULT expr [EMPTY ARRAY|OBJECT] } ON EMPTY`.
        on_empty: String,
        #[serde(default)]
        on_empty_default: Option<ScalarExpr>,
        /// The same behaviors `ON ERROR`.
        on_error: String,
        #[serde(default)]
        on_error_default: Option<ScalarExpr>,
    },
    /// `NESTED [PATH] path [AS name] COLUMNS (...)`: a nested group, whose
    /// rows join the parent's.
    Nested {
        /// The nested row path.
        path: ScalarExpr,
        /// The nested group's columns.
        columns: Vec<JsonTableColumn>,
    },
}

impl TableFnSpec {
    /// Whether the function returns a single value a row, rather than a row
    /// of several.
    pub(crate) fn returns_scalar(&self) -> bool {
        self.rows_from.is_empty()
            && !self.with_ordinality
            && match self.name.as_str() {
                "unnest" => self.args.len().max(self.arg_exprs.len()) == 1,
                "generate_series"
                | "jsonb_array_elements"
                | "jsonb_array_elements_text"
                | "json_array_elements"
                | "json_array_elements_text"
                | "regexp_split_to_table"
                | "regexp_matches"
                | "string_to_table"
                | "generate_subscripts"
                | "jsonb_object_keys"
                | "json_object_keys"
                | "pg_partition_ancestors" => true,
                _ => false,
            }
    }
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
    /// A `LATERAL` subquery: run once for each row of the left side, with that
    /// row's values for its outer references. Its rows are this join's right
    /// side for that left row.
    #[serde(default)]
    pub lateral: Option<Box<LogicalPlan>>,
}

/// The unique key an `ON CONFLICT` clause arbitrates on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConflictTarget {
    /// `ON CONFLICT (a, b)`: the unique key over exactly these columns.
    Columns(Vec<String>),
    /// `ON CONFLICT ON CONSTRAINT name`.
    Constraint(String),
}

/// `ON CONFLICT` action for an INSERT that hits an existing key. Without a
/// target, a collision on the primary key or any unique index counts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OnConflictClause {
    /// `DO NOTHING` — skip the conflicting row.
    DoNothing { target: Option<ConflictTarget> },
    /// `DO UPDATE SET … [WHERE …]` — update the existing row. Expressions see
    /// the existing row's columns and the proposed row as `excluded.<col>`;
    /// the update applies only where `condition` holds.
    DoUpdate {
        target: Option<ConflictTarget>,
        assignments: Vec<(String, ScalarExpr)>,
        condition: Option<ScalarExpr>,
    },
}

impl OnConflictClause {
    pub fn target(&self) -> Option<&ConflictTarget> {
        match self {
            OnConflictClause::DoNothing { target } | OnConflictClause::DoUpdate { target, .. } => {
                target.as_ref()
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AggregateOp {
    Count,
    Sum,
    Min,
    Max,
    // New variants are appended so older serialized plans still decode.
    Avg,
    StringAgg,
    ArrayAgg,
    BoolAnd,
    BoolOr,
    JsonAgg,
    JsonbAgg,
    JsonObjectAgg,
    JsonbObjectAgg,
    StddevSamp,
    StddevPop,
    VarSamp,
    VarPop,
    BitAnd,
    BitOr,
    /// `percentile_cont(fraction) WITHIN GROUP (ORDER BY x)`, and the other
    /// ordered-set aggregates: their direct argument is the first extra
    /// argument, their input sorted by the `WITHIN GROUP` order.
    PercentileCont,
    PercentileDisc,
    Mode,
    /// `rank(h) WITHIN GROUP (ORDER BY x)` and the other hypothetical-set
    /// aggregates: extra arguments `h`, whether ascending, and `NULLS FIRST`.
    HypotheticalRank,
    HypotheticalDenseRank,
    HypotheticalPercentRank,
    HypotheticalCumeDist,
    /// `corr(y, x)` and the other statistics of (y, x) pairs.
    Corr,
    CovarPop,
    CovarSamp,
    RegrSlope,
    RegrIntercept,
    RegrCount,
    RegrR2,
    RegrAvgX,
    RegrAvgY,
    RegrSxx,
    RegrSyy,
    RegrSxy,
    BitXor,
    AnyValue,
    /// `range_agg(x)`: the values' ranges or multiranges merged into one
    /// multirange.
    RangeAgg,
    /// `range_intersect_agg(x)`: the ranges common to every value.
    RangeIntersectAgg,
    /// `xmlagg(x)`: the values concatenated, their XML declarations merged.
    XmlAgg,
    /// `json_arrayagg(x [ORDER BY ...] [NULL|ABSENT ON NULL] [RETURNING
    /// type])`: the values as a JSON array, with the null clause and the
    /// RETURNING type as its further arguments.
    SqlJsonArrayAgg,
    /// `json_objectagg(k, v ...)`: the keys and values as a JSON object, with
    /// the null clause, the unique-keys clause, and the RETURNING type as its
    /// further arguments.
    SqlJsonObjectAgg,
}

impl AggregateOp {
    /// The function's SQL name, which is also its default output column name.
    pub fn sql_name(&self) -> &'static str {
        match self {
            AggregateOp::Count => "count",
            AggregateOp::Sum => "sum",
            AggregateOp::Min => "min",
            AggregateOp::Max => "max",
            AggregateOp::Avg => "avg",
            AggregateOp::StringAgg => "string_agg",
            AggregateOp::ArrayAgg => "array_agg",
            AggregateOp::BoolAnd => "bool_and",
            AggregateOp::BoolOr => "bool_or",
            AggregateOp::JsonAgg => "json_agg",
            AggregateOp::JsonbAgg => "jsonb_agg",
            AggregateOp::JsonObjectAgg => "json_object_agg",
            AggregateOp::JsonbObjectAgg => "jsonb_object_agg",
            AggregateOp::StddevSamp => "stddev_samp",
            AggregateOp::StddevPop => "stddev_pop",
            AggregateOp::VarSamp => "var_samp",
            AggregateOp::VarPop => "var_pop",
            AggregateOp::BitAnd => "bit_and",
            AggregateOp::BitOr => "bit_or",
            AggregateOp::PercentileCont => "percentile_cont",
            AggregateOp::PercentileDisc => "percentile_disc",
            AggregateOp::Mode => "mode",
            AggregateOp::HypotheticalRank => "rank",
            AggregateOp::HypotheticalDenseRank => "dense_rank",
            AggregateOp::HypotheticalPercentRank => "percent_rank",
            AggregateOp::HypotheticalCumeDist => "cume_dist",
            AggregateOp::Corr => "corr",
            AggregateOp::CovarPop => "covar_pop",
            AggregateOp::CovarSamp => "covar_samp",
            AggregateOp::RegrSlope => "regr_slope",
            AggregateOp::RegrIntercept => "regr_intercept",
            AggregateOp::RegrCount => "regr_count",
            AggregateOp::RegrR2 => "regr_r2",
            AggregateOp::RegrAvgX => "regr_avgx",
            AggregateOp::RegrAvgY => "regr_avgy",
            AggregateOp::RegrSxx => "regr_sxx",
            AggregateOp::RegrSyy => "regr_syy",
            AggregateOp::RegrSxy => "regr_sxy",
            AggregateOp::XmlAgg => "xmlagg",
            AggregateOp::SqlJsonArrayAgg => "json_arrayagg",
            AggregateOp::SqlJsonObjectAgg => "json_objectagg",
            AggregateOp::BitXor => "bit_xor",
            AggregateOp::AnyValue => "any_value",
            AggregateOp::RangeAgg => "range_agg",
            AggregateOp::RangeIntersectAgg => "range_intersect_agg",
        }
    }

    /// How many arguments the aggregate takes (`count(*)` counts as one).
    pub fn arity(&self) -> usize {
        match self {
            AggregateOp::StringAgg
            | AggregateOp::JsonObjectAgg
            | AggregateOp::JsonbObjectAgg
            | AggregateOp::PercentileCont
            | AggregateOp::PercentileDisc
            | AggregateOp::Corr
            | AggregateOp::CovarPop
            | AggregateOp::CovarSamp
            | AggregateOp::RegrSlope
            | AggregateOp::RegrIntercept
            | AggregateOp::RegrCount
            | AggregateOp::RegrR2
            | AggregateOp::RegrAvgX
            | AggregateOp::RegrAvgY
            | AggregateOp::RegrSxx
            | AggregateOp::RegrSyy
            | AggregateOp::RegrSxy => 2,
            AggregateOp::HypotheticalRank
            | AggregateOp::HypotheticalDenseRank
            | AggregateOp::HypotheticalPercentRank
            | AggregateOp::HypotheticalCumeDist => 4,
            // The array aggregate's element, null clause, and RETURNING type.
            AggregateOp::SqlJsonArrayAgg => 3,
            // The object aggregate's key, value, null clause, unique-keys
            // clause, and RETURNING type.
            AggregateOp::SqlJsonObjectAgg => 5,
            _ => 1,
        }
    }
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
    /// `EXTRACT(<field> FROM <expr>)` over an ISO date/time text value.
    Extract {
        field: String,
        expr: Box<ScalarExpr>,
    },
    /// An aggregate call (e.g. `sum(a)`) nested inside a scalar expression such
    /// as `sum(a) + 1`; evaluated over a group, not a single row. When the
    /// aggregate's argument is itself an expression (e.g. `sum(a + 1)`),
    /// `arg_expr` carries it and `arg` is empty.
    Aggregate {
        op: AggregateOp,
        arg: String,
        #[serde(default)]
        arg_expr: Option<Box<ScalarExpr>>,
        /// `agg(DISTINCT ...)` — aggregate over the distinct argument values.
        #[serde(default)]
        distinct: bool,
        /// Arguments after the first: `string_agg`'s delimiter,
        /// `json_object_agg`'s value.
        #[serde(default)]
        extra_args: Vec<ScalarExpr>,
        /// `FILTER (WHERE ...)`: only rows where it is true are aggregated.
        #[serde(default)]
        filter: Option<Box<ScalarExpr>>,
        /// `agg(x ORDER BY key [DESC] [NULLS FIRST|LAST], ...)`: the order in
        /// which values are fed to an order-sensitive aggregate.
        #[serde(default)]
        order_by: Vec<(ScalarExpr, bool, Option<bool>)>,
    },
    /// `date/timestamp ± INTERVAL`, resolved to a (months, days, seconds) offset
    /// applied to the base's ISO text value.
    DateOffset {
        base: Box<ScalarExpr>,
        months: i64,
        days: i64,
        seconds: i64,
    },
    /// `CASE [operand] WHEN cond THEN result ... [ELSE else] END`. With an
    /// operand this is simple CASE (each condition compares equal to the
    /// operand); without, each condition must evaluate to boolean true.
    Case {
        operand: Option<Box<ScalarExpr>>,
        branches: Vec<(ScalarExpr, ScalarExpr)>,
        else_result: Option<Box<ScalarExpr>>,
    },
    /// A text pattern match: `[NOT] LIKE`/`ILIKE` or `[NOT] SIMILAR TO` (with
    /// an optional `ESCAPE`), or a POSIX regex operator (`~`, `~*`, `!~`, `!~*`).
    PatternMatch {
        expr: Box<ScalarExpr>,
        pattern: Box<ScalarExpr>,
        kind: PatternKind,
        case_insensitive: bool,
        negated: bool,
        #[serde(default)]
        escape: Option<char>,
    },
    /// `left IS [NOT] DISTINCT FROM right`: NULL-safe (in)equality.
    IsDistinctFrom {
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
        negated: bool,
    },
    /// `expr IS [NOT] TRUE | FALSE | UNKNOWN`; `value` is `None` for UNKNOWN.
    IsBool {
        expr: Box<ScalarExpr>,
        value: Option<bool>,
        negated: bool,
    },
    /// `expr [NOT] IN (e1, e2, ...)`.
    InList {
        expr: Box<ScalarExpr>,
        list: Vec<ScalarExpr>,
        negated: bool,
    },
    /// `left <op> ANY (array)` or `left <op> ALL (array)`.
    Quantified {
        left: Box<ScalarExpr>,
        op: ScalarBinaryOp,
        right: Box<ScalarExpr>,
        all: bool,
    },
    /// A row constructor `(a, b, ...)` / `ROW(a, b, ...)`, compared
    /// element-wise by the comparison operators.
    Row(Vec<ScalarExpr>),
    /// A subquery used as a value. The executor runs it for each row (with the
    /// row's values for its outer references) before the expression is
    /// evaluated; see [`SubqueryKind`].
    Subquery {
        plan: SubPlan,
        kind: SubqueryKind,
    },
    /// A window function call, computed over the query's rows (or groups)
    /// before the expression around it is evaluated.
    Window(Box<WindowCall>),
}

/// A window function call: `func(args) [FILTER (WHERE filter)] OVER
/// (PARTITION BY ... ORDER BY ... frame)`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowCall {
    /// The upper-cased function name (`ROW_NUMBER`, `SUM`, `LAG`).
    pub func: String,
    pub args: Vec<ScalarExpr>,
    #[serde(default)]
    pub filter: Option<ScalarExpr>,
    pub partition_by: Vec<ScalarExpr>,
    /// `(key, ascending, nulls_first)`.
    pub order_by: Vec<(ScalarExpr, bool, Option<bool>)>,
    /// The frame; without one, an ordered window ends at the current row's
    /// last peer and an unordered one is the whole partition.
    #[serde(default)]
    pub frame: Option<FrameSpec>,
}

/// A window frame: `ROWS | RANGE | GROUPS BETWEEN start AND end [EXCLUDE ...]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FrameSpec {
    pub units: WindowFrameUnits,
    pub start: FrameBound,
    pub end: FrameBound,
    #[serde(default)]
    pub exclude: FrameExclude,
}

/// A frame boundary; an offset is an expression (rows or peer groups for
/// `ROWS`/`GROUPS`, a distance in the ordering key's values for `RANGE`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(ScalarExpr),
    CurrentRow,
    Following(ScalarExpr),
    UnboundedFollowing,
}

/// The rows a frame leaves out: `EXCLUDE CURRENT ROW | GROUP | TIES`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
pub enum FrameExclude {
    #[default]
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

impl WindowCall {
    /// The expressions the call evaluates per row, in order.
    pub fn children(&self) -> Vec<&ScalarExpr> {
        fn bound(b: &FrameBound) -> Option<&ScalarExpr> {
            match b {
                FrameBound::Preceding(e) | FrameBound::Following(e) => Some(e),
                _ => None,
            }
        }
        let frame: Vec<&ScalarExpr> = self
            .frame
            .iter()
            .flat_map(|f| [bound(&f.start), bound(&f.end)])
            .flatten()
            .collect();
        self.args
            .iter()
            .chain(self.filter.iter())
            .chain(self.partition_by.iter())
            .chain(self.order_by.iter().map(|(e, _, _)| e))
            .chain(frame)
            .collect()
    }

    /// Rebuilds the call with every expression replaced by `f(expr)`.
    pub fn map_exprs(&self, f: &mut dyn FnMut(&ScalarExpr) -> ScalarExpr) -> WindowCall {
        let mut bound = |b: &FrameBound| match b {
            FrameBound::Preceding(e) => FrameBound::Preceding(f(e)),
            FrameBound::Following(e) => FrameBound::Following(f(e)),
            other => other.clone(),
        };
        let frame = self.frame.as_ref().map(|fr| FrameSpec {
            units: fr.units.clone(),
            start: bound(&fr.start),
            end: bound(&fr.end),
            exclude: fr.exclude,
        });
        WindowCall {
            func: self.func.clone(),
            args: self.args.iter().map(&mut *f).collect(),
            filter: self.filter.as_ref().map(&mut *f),
            partition_by: self.partition_by.iter().map(&mut *f).collect(),
            order_by: self
                .order_by
                .iter()
                .map(|(e, asc, nulls)| (f(e), *asc, *nulls))
                .collect(),
            frame,
        }
    }
}

/// What a subquery in an expression yields.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SubqueryKind {
    /// `(SELECT ...)`: its one value; NULL for no row, an error for several.
    Scalar,
    /// `EXISTS (SELECT ...)`: whether it returns a row.
    Exists,
    /// Its first column's values, as an array: `x IN (SELECT ...)`,
    /// `x = ANY (SELECT ...)`, and `ARRAY(SELECT ...)`.
    Array,
}

/// A subquery's plan inside an expression. Plans have no equality of their
/// own; two are equal when they serialize alike.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubPlan(pub Box<LogicalPlan>);

impl PartialEq for SubPlan {
    fn eq(&self, other: &Self) -> bool {
        serde_json::to_string(&self.0).ok() == serde_json::to_string(&other.0).ok()
    }
}

impl ScalarExpr {
    /// The immediate sub-expressions, in evaluation order.
    pub fn children(&self) -> Vec<&ScalarExpr> {
        match self {
            ScalarExpr::Literal(_) | ScalarExpr::Column(_) | ScalarExpr::Subquery { .. } => {
                Vec::new()
            }
            ScalarExpr::Unary { expr, .. }
            | ScalarExpr::Cast { expr, .. }
            | ScalarExpr::IsNull { expr, .. }
            | ScalarExpr::Extract { expr, .. }
            | ScalarExpr::IsBool { expr, .. } => vec![expr],
            ScalarExpr::DateOffset { base, .. } => vec![base],
            ScalarExpr::Aggregate {
                arg_expr,
                extra_args,
                filter,
                order_by,
                ..
            } => arg_expr
                .iter()
                .map(|e| &**e)
                .chain(extra_args.iter())
                .chain(filter.iter().map(|e| &**e))
                .chain(order_by.iter().map(|(e, _, _)| e))
                .collect(),
            ScalarExpr::Binary { left, right, .. }
            | ScalarExpr::IsDistinctFrom { left, right, .. }
            | ScalarExpr::Quantified { left, right, .. } => vec![left, right],
            ScalarExpr::PatternMatch { expr, pattern, .. } => vec![expr, pattern],
            ScalarExpr::Function { args: items, .. } | ScalarExpr::Row(items) => {
                items.iter().collect()
            }
            ScalarExpr::InList { expr, list, .. } => {
                std::iter::once(&**expr).chain(list.iter()).collect()
            }
            ScalarExpr::Case {
                operand,
                branches,
                else_result,
            } => operand
                .iter()
                .map(|e| &**e)
                .chain(branches.iter().flat_map(|(c, r)| [c, r]))
                .chain(else_result.iter().map(|e| &**e))
                .collect(),
            ScalarExpr::Window(call) => call.children(),
        }
    }

    /// Rebuilds this node with every immediate sub-expression replaced by
    /// `f(child)`; leaves are returned unchanged.
    pub fn map_children(&self, f: &mut dyn FnMut(&ScalarExpr) -> ScalarExpr) -> ScalarExpr {
        let mut boxed = |e: &ScalarExpr| Box::new(f(e));
        match self {
            ScalarExpr::Literal(_) | ScalarExpr::Column(_) | ScalarExpr::Subquery { .. } => {
                self.clone()
            }
            ScalarExpr::Unary { op, expr } => ScalarExpr::Unary {
                op: *op,
                expr: boxed(expr),
            },
            ScalarExpr::Binary { op, left, right } => ScalarExpr::Binary {
                op: *op,
                left: boxed(left),
                right: boxed(right),
            },
            ScalarExpr::Cast { expr, target } => ScalarExpr::Cast {
                expr: boxed(expr),
                target: target.clone(),
            },
            ScalarExpr::Function { name, args } => ScalarExpr::Function {
                name: name.clone(),
                args: args.iter().map(|a| *boxed(a)).collect(),
            },
            ScalarExpr::IsNull { expr, negated } => ScalarExpr::IsNull {
                expr: boxed(expr),
                negated: *negated,
            },
            ScalarExpr::Extract { field, expr } => ScalarExpr::Extract {
                field: field.clone(),
                expr: boxed(expr),
            },
            ScalarExpr::Aggregate {
                op,
                arg,
                arg_expr,
                distinct,
                extra_args,
                filter,
                order_by,
            } => ScalarExpr::Aggregate {
                op: op.clone(),
                arg: arg.clone(),
                arg_expr: arg_expr.as_ref().map(|e| Box::new(f(e))),
                distinct: *distinct,
                extra_args: extra_args.iter().map(&mut *f).collect(),
                filter: filter.as_ref().map(|e| Box::new(f(e))),
                order_by: order_by
                    .iter()
                    .map(|(e, asc, nulls_first)| (f(e), *asc, *nulls_first))
                    .collect(),
            },
            ScalarExpr::DateOffset {
                base,
                months,
                days,
                seconds,
            } => ScalarExpr::DateOffset {
                base: boxed(base),
                months: *months,
                days: *days,
                seconds: *seconds,
            },
            ScalarExpr::Case {
                operand,
                branches,
                else_result,
            } => ScalarExpr::Case {
                operand: operand.as_ref().map(|e| boxed(e)),
                branches: branches
                    .iter()
                    .map(|(c, r)| (*boxed(c), *boxed(r)))
                    .collect(),
                else_result: else_result.as_ref().map(|e| boxed(e)),
            },
            ScalarExpr::PatternMatch {
                expr,
                pattern,
                kind,
                case_insensitive,
                negated,
                escape,
            } => ScalarExpr::PatternMatch {
                expr: boxed(expr),
                pattern: boxed(pattern),
                kind: *kind,
                case_insensitive: *case_insensitive,
                negated: *negated,
                escape: *escape,
            },
            ScalarExpr::IsDistinctFrom {
                left,
                right,
                negated,
            } => ScalarExpr::IsDistinctFrom {
                left: boxed(left),
                right: boxed(right),
                negated: *negated,
            },
            ScalarExpr::IsBool {
                expr,
                value,
                negated,
            } => ScalarExpr::IsBool {
                expr: boxed(expr),
                value: *value,
                negated: *negated,
            },
            ScalarExpr::InList {
                expr,
                list,
                negated,
            } => ScalarExpr::InList {
                expr: boxed(expr),
                list: list.iter().map(|e| *boxed(e)).collect(),
                negated: *negated,
            },
            ScalarExpr::Quantified {
                left,
                op,
                right,
                all,
            } => ScalarExpr::Quantified {
                left: boxed(left),
                op: *op,
                right: boxed(right),
                all: *all,
            },
            ScalarExpr::Row(items) => ScalarExpr::Row(items.iter().map(|e| *boxed(e)).collect()),
            ScalarExpr::Window(call) => ScalarExpr::Window(Box::new(call.map_exprs(&mut |e| f(e)))),
        }
    }
}

/// One `ORDER BY` key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortKey {
    pub target: SortTarget,
    pub ascending: bool,
    /// An explicit `NULLS FIRST` (`Some(true)`) or `NULLS LAST`; `None` uses
    /// PostgreSQL's default: NULLs sort as larger than any value, so last when
    /// ascending and first when descending.
    pub nulls_first: Option<bool>,
    /// Set on the keys of a query whose `FETCH ... WITH TIES` also returns
    /// the rows that tie with its last row on them.
    #[serde(default)]
    pub with_ties: bool,
}

/// What an `ORDER BY` (or `DISTINCT ON`) key sorts by.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SortTarget {
    /// The output column at this 0-based position (`ORDER BY 2`).
    Output(usize),
    /// A bare name: an output column of that name, else an input column.
    /// A qualified name (`t.a`) always means the input column.
    Name(String),
    /// An expression over the input row, or over the group when grouping
    /// (so it may contain aggregates).
    Expr(ScalarExpr),
}

impl SortKey {
    /// Reads a key as older plans encode it in `order_by`.
    pub fn from_legacy((name, ascending, nulls_first): (String, bool, Option<bool>)) -> Self {
        SortKey {
            target: SortTarget::Name(name),
            ascending,
            nulls_first,
            with_ties: false,
        }
    }
}

/// A FROM-less select item evaluated at execution time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeferredItem {
    Scalar(ScalarExpr),
    /// A scalar subquery: one column, at most one row (none is NULL).
    Subquery(Box<LogicalPlan>),
    /// `[NOT] EXISTS (subquery)`.
    Exists {
        plan: Box<LogicalPlan>,
        negated: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum PatternKind {
    Like,
    SimilarTo,
    Regex,
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
    /// `->` / `->>`: JSON object field or array element, as JSON / as text.
    JsonGet,
    JsonGetText,
    /// `#>` / `#>>`: JSON value at a key path, as JSON / as text.
    JsonPath,
    JsonPathText,
    /// `?` / `?|` / `?&`: the JSON value has the key (one, any, all).
    JsonHasKey,
    JsonHasAnyKey,
    JsonHasAllKeys,
    /// `@>` / `<@`: JSONB or array containment.
    Contains,
    ContainedBy,
    /// `@?` / `@@`: does the jsonpath find anything / match as a predicate.
    JsonPathExists,
    JsonPathMatch,
    /// `<->`: a text-search phrase (the same symbol as geometric distance).
    TsPhrase,
    /// `&&`: the arrays share an element.
    Overlap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        /// Explicit `ROWS`/`RANGE BETWEEN …` frame, if any. Appended last so
        /// older serialized plans still decode. When absent, aggregate windows
        /// span the whole partition.
        #[serde(default)]
        frame: Option<WindowFrame>,
        /// `FILTER (WHERE ...)` of an aggregate window: only the frame's rows
        /// where it is true are aggregated.
        #[serde(default)]
        filter: Option<ScalarExpr>,
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
    /// A scalar subquery, run for each output row with the row's values
    /// substituted for its outer references.
    Subquery {
        plan: Box<LogicalPlan>,
        alias: Option<String>,
    },
}

/// A window frame clause (`ROWS`/`RANGE BETWEEN <start> AND <end>`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowBound,
    pub end: WindowBound,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    /// Peer groups of the ordering key.
    Groups,
}

/// A single frame boundary. Numeric offsets are only honoured for `ROWS`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WindowBound {
    UnboundedPreceding,
    Preceding(i64),
    CurrentRow,
    Following(i64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlterTableOp {
    AddColumn {
        name: String,
        data_type: String,
        nullable: bool,
        /// Lowered `DEFAULT` expression: stored on the column and backfilled
        /// into existing rows. Defaulted so older serialized plans decode.
        #[serde(default)]
        default: Option<ScalarExpr>,
        /// `ADD COLUMN IF NOT EXISTS`.
        #[serde(default)]
        if_not_exists: bool,
        /// A `serial` or identity column's sequence, made with the column
        /// (named for the table and column), and for an identity whether it
        /// is `GENERATED ALWAYS`.
        #[serde(default)]
        sequence: Option<crate::sequences::SequenceSpec>,
        #[serde(default)]
        identity: Option<bool>,
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
        #[serde(default)]
        if_exists: bool,
        /// `CASCADE`: the foreign keys referencing the column go too.
        #[serde(default)]
        cascade: bool,
    },
    RenameTable {
        new_name: String,
    },
    /// `ALTER COLUMN column SET DEFAULT expr` / `DROP DEFAULT`.
    SetDefault {
        column: String,
        default: Option<ScalarExpr>,
    },
    /// `ALTER COLUMN column SET NOT NULL` / `DROP NOT NULL`.
    SetNotNull {
        column: String,
        not_null: bool,
    },
    /// `ADD [CONSTRAINT name] ...`; with `not_valid`, existing rows are not
    /// checked.
    AddConstraint {
        constraint: NewConstraint,
        not_valid: bool,
    },
    /// `DROP CONSTRAINT [IF EXISTS] name [CASCADE]`.
    DropConstraint {
        name: String,
        if_exists: bool,
        cascade: bool,
    },
    /// `RENAME CONSTRAINT old_name TO new_name`.
    RenameConstraint {
        old_name: String,
        new_name: String,
    },
    /// `VALIDATE CONSTRAINT name`: checks the existing rows.
    ValidateConstraint {
        name: String,
    },
    /// `OWNER TO role`: NodusDB tracks no owners, so this changes nothing.
    OwnerTo,
}

/// A constraint `ALTER TABLE ... ADD` creates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NewConstraint {
    Check {
        name: Option<String>,
        expr: String,
    },
    /// A foreign key, completed when it is added.
    ForeignKey(nodus_catalog::TableConstraint),
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogicalPlan {
    CreateSchema {
        schema_name: String,
        if_not_exists: bool,
        /// `AUTHORIZATION role`: the role that owns it.
        #[serde(default)]
        authorization: Option<String>,
        /// The statements creating objects in it with it (`CREATE SCHEMA s
        /// CREATE TABLE ...`), run with it first on the search path.
        #[serde(default)]
        elements: Vec<LogicalPlan>,
    },
    /// `DROP SCHEMA names`: with `cascade`, what they hold goes with them.
    DropSchema {
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },
    /// `ALTER SCHEMA name RENAME TO new_name` or `OWNER TO owner`.
    AlterSchema {
        name: String,
        new_name: Option<String>,
        owner: Option<String>,
    },
    /// `ALTER <kind> [IF EXISTS] name SET SCHEMA schema`, `kind` one of
    /// `TABLE`, `VIEW`, `MATERIALIZED VIEW`, `SEQUENCE`, `TYPE`, `DOMAIN`.
    SetSchema {
        kind: String,
        name: String,
        schema: String,
        if_exists: bool,
    },
    CreateTable {
        name: String,
        columns: Vec<ColumnDef>,
        constraints: Vec<nodus_catalog::TableConstraint>,
        /// `CREATE TABLE IF NOT EXISTS` — a no-op when the table already exists.
        /// Defaulted so plans serialized before this field decode.
        #[serde(default)]
        if_not_exists: bool,
        /// Table-level `UNIQUE (a, b, ...)` constraints over two or more
        /// columns; each becomes one unique index over the whole column tuple.
        #[serde(default)]
        unique_constraints: Vec<Vec<String>>,
        /// The names given to the primary key and unique constraints, by
        /// their columns; the others get PostgreSQL's names.
        #[serde(default)]
        key_names: Vec<(Vec<String>, String)>,
        /// A temporary table's `ON COMMIT` action: `DROP` or `DELETE ROWS`.
        #[serde(default)]
        on_commit: Option<String>,
        /// `LIKE source [INCLUDING DEFAULTS]`: a table whose columns lead
        /// the new one's, and whether their defaults come too.
        #[serde(default)]
        like: Vec<(String, bool)>,
    },
    /// `DROP TABLE names` or, with `materialized`, `DROP MATERIALIZED
    /// VIEW names`; with `cascade`, the objects depending on them go too.
    DropTable {
        names: Vec<String>,
        if_exists: bool,
        #[serde(default)]
        materialized: bool,
        #[serde(default)]
        cascade: bool,
    },
    CreateView {
        name: String,
        query: Box<LogicalPlan>,
        /// `CREATE OR REPLACE VIEW`. Defaulted so older plans decode.
        #[serde(default)]
        or_replace: bool,
        /// `WITH [CASCADED | LOCAL] CHECK OPTION`: `cascaded` or `local`.
        #[serde(default)]
        check_option: Option<String>,
    },
    /// A view's stored query with its `WITH CHECK OPTION` (`cascaded` or
    /// `local`); it runs as the query does.
    ViewCheckOption {
        query: Box<LogicalPlan>,
        option: String,
    },
    DropView {
        names: Vec<String>,
        if_exists: bool,
        #[serde(default)]
        cascade: bool,
    },
    /// `ALTER TABLE [IF EXISTS] name op, ...`: the operations apply in turn.
    AlterTable {
        table_name: String,
        operations: Vec<AlterTableOp>,
        #[serde(default)]
        if_exists: bool,
    },
    CreateIndex {
        /// Empty for an unnamed index, which gets PostgreSQL's name for it.
        name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
        /// A partial index's `WHERE` condition. Defaulted so older plans
        /// decode.
        #[serde(default)]
        predicate: Option<String>,
        /// Parallel to `columns`: the SQL of each key part that is an
        /// expression (whose `columns` entry is what the index's default
        /// name calls it).
        #[serde(default)]
        expressions: Vec<Option<String>>,
        /// Parallel to `columns`: whether each key part sorts descending.
        #[serde(default)]
        descending: Vec<bool>,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    /// `ALTER INDEX name RENAME TO new_name`.
    RenameIndex {
        name: String,
        new_name: String,
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
        /// For each `returning` item that is an expression (its output name
        /// then in `returning`), the expression; empty when every item is a
        /// column. Defaulted so older plans decode.
        #[serde(default)]
        returning_exprs: Vec<Option<ReturningExpr>>,
        /// `ON CONFLICT` behaviour when a row collides with an existing key.
        /// Defaulted so plans serialized before this field decode.
        #[serde(default)]
        on_conflict: Option<OnConflictClause>,
        /// Per-row mask marking cells written as the `DEFAULT` keyword (parallel
        /// to `values_list`; empty = no DEFAULT cells anywhere). Such cells take
        /// the column default as if the column had been omitted.
        #[serde(default)]
        default_cells: Vec<Vec<bool>>,
        /// `INSERT ... SELECT`: a query whose rows are inserted instead of
        /// `values_list`.
        #[serde(default)]
        source: Option<Box<LogicalPlan>>,
        /// `OVERRIDING SYSTEM VALUE` (`SYSTEM`: identity columns take the
        /// values given) or `OVERRIDING USER VALUE` (`USER`: they generate
        /// theirs regardless).
        #[serde(default)]
        overriding: Option<String>,
        /// `INSERT INTO t AS alias`: the name RETURNING and ON CONFLICT
        /// qualify the target's columns by.
        #[serde(default)]
        alias: Option<String>,
    },
    /// `CREATE TABLE ... AS <query>` / `SELECT ... INTO`: a table shaped like
    /// the query's output, filled with its rows.
    CreateTableAs {
        name: String,
        query: Box<LogicalPlan>,
        if_not_exists: bool,
        /// `WITH NO DATA`: the table is created empty. Defaulted so older
        /// plans decode.
        #[serde(default)]
        no_data: bool,
        /// `CREATE MATERIALIZED VIEW`: the table keeps the query for
        /// `REFRESH`.
        #[serde(default)]
        materialized: bool,
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
        /// Expanded `ROLLUP`/`CUBE`/`GROUPING SETS` — the list of grouping
        /// column-sets to aggregate by. `group_by` holds the union of all
        /// columns mentioned (for output resolution); a column absent from a
        /// given set is emitted as NULL for that set's rows. `None` means plain
        /// aggregation over `group_by`. Appended last so older plans decode.
        #[serde(default)]
        grouping_sets: Option<Vec<Vec<String>>>,
        /// `ORDER BY (column, ascending, nulls_first_override)` as older plans
        /// encode it; the planner now writes [`SortKey`]s to `sort` instead,
        /// and this is read only when `sort` is empty.
        order_by: Vec<(String, bool, Option<bool>)>,
        /// Optional `LIMIT`.
        limit: Option<usize>,
        /// Optional `OFFSET`.
        offset: Option<usize>,
        /// DISTINCT
        distinct: bool,
        /// `ORDER BY` keys, applied to the output rows. Defaulted so older
        /// plans (which use `order_by`) decode.
        #[serde(default)]
        sort: Vec<SortKey>,
        /// Grouping keys that are expressions: each `(name, expr)` is computed
        /// per input row before grouping, as a column `name` that `group_by`
        /// refers to. A name that is also an input column stays that column,
        /// since a `GROUP BY` name means the input column before an output
        /// alias. Defaulted so older plans decode.
        #[serde(default)]
        group_exprs: Vec<(String, ScalarExpr)>,
        /// `DISTINCT ON` keys: after sorting, only the first row of each
        /// distinct key is kept. Defaulted so older plans decode.
        #[serde(default)]
        distinct_on: Vec<SortTarget>,
    },
    Update {
        table_name: String,
        /// Each `SET col = <expr>`; the expression is evaluated per matched row
        /// against that row's *old* values.
        assignments: Vec<(String, ScalarExpr)>,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
        /// For each `returning` item that is an expression (its output name
        /// then in `returning`), the expression; empty when every item is a
        /// column. Defaulted so older plans decode.
        #[serde(default)]
        returning_exprs: Vec<Option<ReturningExpr>>,
        /// The name the target table goes by (`UPDATE t AS x`). Defaulted so
        /// older plans decode.
        #[serde(default)]
        table_alias: Option<String>,
        /// `FROM`: the relations joined to each target row, whose columns the
        /// filter and assignments may read.
        #[serde(default)]
        from: Option<Box<LogicalPlan>>,
    },
    Delete {
        table_name: String,
        filter: Option<FilterExpr>,
        returning: Vec<String>,
        /// For each `returning` item that is an expression (its output name
        /// then in `returning`), the expression; empty when every item is a
        /// column. Defaulted so older plans decode.
        #[serde(default)]
        returning_exprs: Vec<Option<ReturningExpr>>,
        /// The name the target table goes by (`DELETE FROM t AS x`).
        #[serde(default)]
        table_alias: Option<String>,
        /// `USING`: the relations joined to each target row, whose columns
        /// the filter may read.
        #[serde(default)]
        using: Option<Box<LogicalPlan>>,
    },
    /// `BEGIN [READ ONLY | READ WRITE] [ISOLATION LEVEL ...]`, or with
    /// `start` `START TRANSACTION ...`.
    Begin {
        read_only: Option<bool>,
        isolation: Option<String>,
        #[serde(default)]
        start: bool,
    },
    /// `COMMIT AND CHAIN` (or with `rollback`, `ROLLBACK AND CHAIN`): ends
    /// the transaction and starts another like it.
    Chain {
        rollback: bool,
    },
    /// `SET TRANSACTION ...`, or with `session` `SET SESSION CHARACTERISTICS
    /// AS TRANSACTION ...` (the defaults of later transactions).
    SetTransaction {
        read_only: Option<bool>,
        isolation: Option<String>,
        session: bool,
    },
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
        /// `SET LOCAL`: until the transaction ends.
        #[serde(default)]
        local: bool,
    },
    /// `RESET name`, or `RESET ALL` without one.
    ResetVariable {
        variable: Option<String>,
    },
    Noop {
        tag: String,
    },
    /// `DISCARD ALL | PLANS | SEQUENCES | TEMP`.
    Discard {
        what: String,
    },
    /// `PREPARE name [(types)] AS statement`: the statement's text, planned
    /// when it is executed.
    Prepare {
        name: String,
        param_types: Vec<String>,
        statement: String,
    },
    /// `EXECUTE name [(parameters)]`.
    Execute {
        name: String,
        params: Vec<ScalarExpr>,
    },
    /// `DEALLOCATE [PREPARE] name`, or `DEALLOCATE ALL` without one.
    Deallocate {
        name: Option<String>,
    },
    /// `CREATE TYPE name AS ENUM (...)` or `CREATE DOMAIN name AS ...`.
    CreateType {
        name: String,
        definition: crate::user_types::TypeDefinition,
    },
    /// `ALTER TYPE name ...` or, with `domain`, `ALTER DOMAIN name ...`.
    AlterType {
        name: String,
        change: crate::user_types::TypeChange,
        domain: bool,
    },
    /// `DROP TYPE names` or, with `domain`, `DROP DOMAIN names`.
    DropType {
        names: Vec<String>,
        if_exists: bool,
        cascade: bool,
        domain: bool,
    },
    /// `DECLARE name [BINARY] [[NO] SCROLL] CURSOR [WITH HOLD] FOR query`;
    /// `statement` is its text, as `pg_cursors` shows it.
    DeclareCursor {
        name: String,
        query: Box<LogicalPlan>,
        scroll: Option<bool>,
        hold: bool,
        binary: bool,
        statement: String,
    },
    /// `FETCH direction FROM name`, or `MOVE` with `move_only`.
    FetchCursor {
        name: String,
        direction: crate::cursors::FetchDirection,
        move_only: bool,
    },
    /// `CLOSE name`, or `CLOSE ALL` without one.
    CloseCursor {
        name: Option<String>,
    },
    /// `LOCK [TABLE] names [IN mode MODE] [NOWAIT]`.
    LockTable {
        tables: Vec<String>,
    },
    /// `LISTEN channel`.
    Listen {
        channel: String,
    },
    /// `UNLISTEN channel`, or `UNLISTEN *` without one.
    Unlisten {
        channel: Option<String>,
    },
    /// `NOTIFY channel [, payload]`.
    Notify {
        channel: String,
        payload: String,
    },
    SelectLiteral {
        /// `(column alias, value, optional SQL type hint)`. The hint (from a
        /// CAST) types the column even when the value is NULL.
        values: Vec<(String, crate::Value, Option<String>)>,
        /// `WHERE` on a FROM-less SELECT: with no rows to bind, the predicate
        /// is constant — the row is returned iff it evaluates true. Defaulted
        /// so plans serialized before this field decode.
        #[serde(default)]
        filter: Option<FilterExpr>,
        /// Items computed when the statement runs (parallel to `values`, whose
        /// entry is a placeholder): session functions, volatile functions, and
        /// scalar subqueries.
        #[serde(default)]
        deferred: Vec<Option<DeferredItem>>,
    },
    SetOp {
        op: SetOpKind,
        /// `ALL` keeps duplicates; otherwise the result is a distinct multiset.
        all: bool,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    },
    /// A `WITH RECURSIVE` CTE: the `seed` runs once, then `recursive_term`
    /// re-runs against the rows produced by the previous step (the working
    /// table) until it yields nothing new. `all` selects UNION ALL vs UNION
    /// (distinct) accumulation; `column_aliases` names the output columns.
    /// Only ever appears as a CTE body, materialized by the CTE loop.
    RecursiveCte {
        all: bool,
        column_aliases: Vec<String>,
        seed: Box<LogicalPlan>,
        recursive_term: Box<LogicalPlan>,
    },
    /// A pre-computed relation injected as a CTE (used to feed the working
    /// table into a recursive term). Executing it just yields these rows.
    InlineRows {
        columns: Vec<String>,
        types: Vec<String>,
        rows: Vec<Vec<crate::Value>>,
    },
    /// A standalone (non-lateral) set-returning function in `FROM`, e.g.
    /// `SELECT * FROM generate_series(1, 5)`. Lateral table functions are carried
    /// on [`Join::table_fn`] instead.
    TableFunction(TableFnSpec),
    /// `CREATE SEQUENCE`.
    CreateSequence {
        name: String,
        if_not_exists: bool,
        spec: crate::sequences::SequenceSpec,
    },
    /// `ALTER SEQUENCE [IF EXISTS] name options`.
    AlterSequence {
        name: String,
        if_exists: bool,
        change: crate::sequences::SequenceChange,
    },
    /// `DROP SEQUENCE`.
    DropSequence {
        names: Vec<String>,
        if_exists: bool,
    },
    /// A `VALUES` list used as a query: its rows' expressions are evaluated
    /// when it runs, into columns `column1`, `column2`, ...
    Values {
        rows: Vec<Vec<ScalarExpr>>,
    },
    /// `input` with its leading columns renamed, for a column alias list
    /// (`FROM (...) AS t(a, b)`, `WITH x(a, b) AS (...)`).
    Renamed {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
    /// `MERGE INTO table USING source ON on WHEN ...`: each target row is
    /// joined to the source rows `on` matches, and the first clause that
    /// applies to each joined, unmatched source, or unmatched target row acts.
    Merge {
        table_name: String,
        table_alias: Option<String>,
        /// The source relation, read with its columns qualified.
        source: Box<LogicalPlan>,
        on: Option<FilterExpr>,
        clauses: Vec<MergeClause>,
        /// `RETURNING` columns of the inserted, updated, or deleted rows.
        returning: Vec<String>,
        /// For each `returning` item that is an expression (its output name
        /// then in `returning`), the expression; empty when every item is a
        /// column. Defaulted so older plans decode.
        #[serde(default)]
        returning_exprs: Vec<Option<ReturningExpr>>,
    },
    /// A data-modifying statement (`body`) with a `WITH` list whose queries
    /// it can read.
    With {
        ctes: Vec<(String, Box<LogicalPlan>)>,
        body: Box<LogicalPlan>,
    },
    /// `TRUNCATE`: empties each table, and with `RESTART IDENTITY` restarts
    /// the sequences its columns draw from.
    Truncate {
        tables: Vec<String>,
        restart_identity: bool,
        /// `CASCADE`: also empties the tables whose foreign keys reference
        /// these. Defaulted so older plans decode.
        #[serde(default)]
        cascade: bool,
    },
    /// `REFRESH MATERIALIZED VIEW name [WITH [NO] DATA]`.
    RefreshMaterializedView {
        name: String,
        with_data: bool,
    },
    /// `EXPLAIN [ANALYZE] statement`.
    Explain {
        plan: Box<LogicalPlan>,
        options: crate::explain::ExplainOptions,
    },
    /// `COMMENT ON kind relation[.column] IS comment`; a `NULL` or empty
    /// comment removes it.
    Comment {
        /// `TABLE`, `VIEW`, `MATERIALIZED VIEW`, `SEQUENCE`, or `COLUMN`.
        kind: String,
        relation: String,
        column: Option<String>,
        comment: Option<String>,
    },
}

/// An expression in a `RETURNING` list, evaluated over each returned row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReturningExpr {
    pub expr: ScalarExpr,
}

/// One `WHEN` clause of a `MERGE`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeClause {
    pub kind: MergeKind,
    /// `AND <condition>`, over the joined target and source columns.
    pub condition: Option<FilterExpr>,
    pub action: MergeAction,
}

/// Which rows of a `MERGE` join a `WHEN` clause applies to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MergeKind {
    /// A target row joined to a source row.
    Matched,
    /// A target row no source row joins.
    NotMatchedBySource,
    /// A source row no target row joins.
    NotMatchedByTarget,
}

/// What a `MERGE` clause does to its row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MergeAction {
    /// `UPDATE SET ...`, evaluated against the joined row.
    Update(Vec<(String, ScalarExpr)>),
    Delete,
    /// `INSERT [(columns)] VALUES (...)` of values computed from the source
    /// row; a `None` value is `DEFAULT`. No columns and no values is
    /// `INSERT DEFAULT VALUES`.
    Insert {
        columns: Vec<String>,
        values: Vec<Option<ScalarExpr>>,
    },
    Nothing,
}

/// The kind of set operation combining two query results.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}
