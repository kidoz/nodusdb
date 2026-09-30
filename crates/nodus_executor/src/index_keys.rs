//! Index keys. An index's key parts are table columns and, for an index on
//! expressions (`CREATE INDEX ON users (lower(email))`), expressions over
//! the row. As in PostgreSQL's `pg_index.indkey`, a key part that is an
//! expression has no column: its `column_id` is [`EXPRESSION_KEY`], and the
//! index's `expressions` hold those parts' SQL in order.

use std::cell::RefCell;
use std::collections::HashMap;

use nodus_catalog::{ColumnId, IndexColumn, IndexDescriptor, TableDescriptor};

use crate::{MemExecutor, ScalarExpr, Value};

/// The column of a key part that is an expression.
pub(crate) const EXPRESSION_KEY: ColumnId = ColumnId(uuid::Uuid::nil());

/// One part of an index's key.
pub(crate) enum KeyPart {
    /// The table column at this position.
    Column(usize),
    /// An expression, and its SQL.
    Expression(ScalarExpr, String),
}

/// Whether an index has expressions among its key parts.
pub(crate) fn has_expressions(index: &IndexDescriptor) -> bool {
    index.key_columns.iter().any(is_expression_key)
}

pub(crate) fn is_expression_key(key: &IndexColumn) -> bool {
    key.column_id == EXPRESSION_KEY
}

/// An expression's SQL as an expression over a row, parsed once per thread.
pub(crate) fn parse_expression(sql: &str) -> Option<ScalarExpr> {
    thread_local! {
        static PARSED: RefCell<HashMap<String, Option<ScalarExpr>>> = RefCell::new(HashMap::new());
    }
    if let Some(hit) = PARSED.with(|p| p.borrow().get(sql).cloned()) {
        return hit;
    }
    let parsed = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .ok()
        .and_then(|e| crate::planner::lower_scalar(&e, &[]))
        .map(|e| crate::result_types::check_integer_ranges(&e, &|_: &str| None));
    PARSED.with(|p| {
        let mut p = p.borrow_mut();
        if p.len() >= 1024 {
            p.clear();
        }
        p.insert(sql.to_string(), parsed.clone());
    });
    parsed
}

/// An index's key parts, in order.
pub(crate) fn index_parts(table: &TableDescriptor, index: &IndexDescriptor) -> Vec<KeyPart> {
    let mut expressions = index.expressions.iter();
    index
        .key_columns
        .iter()
        .filter_map(|key| {
            if is_expression_key(key) {
                let sql = expressions.next()?.sql.clone();
                let expr = parse_expression(&sql).unwrap_or(ScalarExpr::Literal(Value::Null));
                Some(KeyPart::Expression(expr, sql))
            } else {
                table
                    .columns
                    .iter()
                    .position(|c| c.id == key.column_id)
                    .map(KeyPart::Column)
            }
        })
        .collect()
}

/// The key parts' values for a row of the table.
pub(crate) fn key_values(table: &TableDescriptor, parts: &[KeyPart], row: &[Value]) -> Vec<Value> {
    let names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
    parts
        .iter()
        .map(|part| match part {
            KeyPart::Column(p) => row.get(*p).cloned().unwrap_or(Value::Null),
            KeyPart::Expression(expr, _) => crate::eval_scalar_expr(expr, row, &names),
        })
        .collect()
}

/// The key parts as a unique violation's `DETAIL` names them.
pub(crate) fn key_names(table: &TableDescriptor, parts: &[KeyPart]) -> Vec<String> {
    parts
        .iter()
        .map(|part| match part {
            KeyPart::Column(p) => table.columns[*p].name.clone(),
            KeyPart::Expression(_, sql) => deparse_sql(sql),
        })
        .collect()
}

/// A key's parts written as SQL (`lower(email), id`), each as the parser
/// prints it back, in order of that text: what an `ON CONFLICT` target is
/// matched on.
pub(crate) fn key_list(sql: &str) -> Vec<String> {
    let parsed = sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_comma_separated(sqlparser::parser::Parser::parse_expr));
    let mut list: Vec<String> = match parsed {
        Ok(exprs) => exprs.iter().map(canonical_text).collect(),
        Err(_) => vec![sql.to_string()],
    };
    list.sort();
    list
}

/// An index's key parts as [`key_list`] gives a target's.
pub(crate) fn index_key_list(table: &TableDescriptor, index: &IndexDescriptor) -> Vec<String> {
    let mut list: Vec<String> = index_parts(table, index)
        .iter()
        .map(|part| match part {
            KeyPart::Column(p) => table.columns[*p].name.clone(),
            KeyPart::Expression(_, sql) => {
                parse_sql_expr(sql).map_or_else(|| sql.clone(), |e| canonical_text(&e))
            }
        })
        .collect();
    list.sort();
    list
}

fn canonical_text(expr: &sqlparser::ast::Expr) -> String {
    let mut expr = expr;
    while let sqlparser::ast::Expr::Nested(inner) = expr {
        expr = inner;
    }
    match expr {
        sqlparser::ast::Expr::Identifier(id) => id.value.clone(),
        other => other.to_string(),
    }
}

/// What an index on an expression is named after, as PostgreSQL names it:
/// a function call's function, else `expr`.
pub(crate) fn expression_label(expr: &sqlparser::ast::Expr) -> String {
    let mut expr = expr;
    while let sqlparser::ast::Expr::Nested(inner) = expr {
        expr = inner;
    }
    match expr {
        sqlparser::ast::Expr::Function(f) => f
            .name
            .0
            .last()
            .and_then(|p| p.as_ident())
            .map_or_else(|| "expr".to_string(), |i| i.value.clone()),
        _ => "expr".to_string(),
    }
}

/// An index key part's expression as `pg_get_indexdef` shows it: a
/// function call as it is, anything else in parentheses.
pub(crate) fn index_expression_text(sql: &str) -> String {
    let Some(expr) = parse_sql_expr(sql) else {
        return sql.to_string();
    };
    let mut bare = &expr;
    while let sqlparser::ast::Expr::Nested(inner) = bare {
        bare = inner;
    }
    let text = deparse(bare);
    match bare {
        sqlparser::ast::Expr::Function(_) => text,
        _ => format!("({text})"),
    }
}

fn parse_sql_expr(sql: &str) -> Option<sqlparser::ast::Expr> {
    sqlparser::parser::Parser::new(&sqlparser::dialect::PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .ok()
}

/// An expression's SQL as PostgreSQL prints it back (`pg_get_indexdef`,
/// `pg_get_constraintdef`): each operation in parentheses, a string
/// literal with its type.
pub(crate) fn deparse_sql(sql: &str) -> String {
    parse_sql_expr(sql).map_or_else(|| sql.to_string(), |e| deparse(&e))
}

fn deparse(expr: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::{BinaryOperator as B, Expr, Value as V};
    match expr {
        Expr::Identifier(id) => quote_ident(&id.value),
        Expr::CompoundIdentifier(ids) => ids
            .iter()
            .map(|i| quote_ident(&i.value))
            .collect::<Vec<_>>()
            .join("."),
        Expr::Value(v) => match &v.value {
            V::SingleQuotedString(s) => format!("'{}'::text", s.replace('\'', "''")),
            other => other.to_string(),
        },
        Expr::Nested(inner) => deparse(inner),
        Expr::BinaryOp { left, op, right } => {
            let op = match op {
                B::NotEq => "<>".to_string(),
                other => other.to_string(),
            };
            format!("({} {op} {})", deparse(left), deparse(right))
        }
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char: None,
            any: false,
        } => format!(
            "({} {} {})",
            deparse(expr),
            if *negated { "!~~" } else { "~~" },
            deparse(pattern)
        ),
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char: None,
            any: false,
        } => format!(
            "({} {} {})",
            deparse(expr),
            if *negated { "!~~*" } else { "~~*" },
            deparse(pattern)
        ),
        Expr::IsNull(inner) => format!("({} IS NULL)", deparse(inner)),
        Expr::IsNotNull(inner) => format!("({} IS NOT NULL)", deparse(inner)),
        Expr::Function(f) => {
            let args = match &f.args {
                sqlparser::ast::FunctionArguments::List(list) => list
                    .args
                    .iter()
                    .map(|arg| match arg {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(e),
                        ) => deparse(e),
                        other => other.to_string(),
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                sqlparser::ast::FunctionArguments::None => return f.to_string(),
                other => other.to_string(),
            };
            format!("{}({args})", f.name.to_string().to_ascii_lowercase())
        }
        other => other.to_string(),
    }
}

/// An identifier as PostgreSQL prints it: quoted unless it is a plain
/// lower-case name.
fn quote_ident(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

impl MemExecutor {
    /// The value an index's entry for a row is kept under: its leading key
    /// part's (`None` for an index without parts).
    pub(crate) fn index_leading_value(
        tbl: &TableDescriptor,
        idx: &IndexDescriptor,
        row: &[Value],
    ) -> Option<Value> {
        let first = idx.key_columns.first()?;
        if is_expression_key(first) {
            let parts = index_parts(tbl, idx);
            return key_values(tbl, &parts[..1.min(parts.len())], row)
                .into_iter()
                .next();
        }
        let pos = tbl.columns.iter().position(|c| c.id == first.column_id)?;
        Some(row.get(pos).cloned().unwrap_or(Value::Null))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expressions_print_as_postgresql_prints_them() {
        assert_eq!(index_expression_text("lower(email)"), "lower(email)");
        assert_eq!(
            index_expression_text("(first || ' ' || last)"),
            "(((first || ' '::text) || last))"
        );
        assert_eq!(deparse_sql("lower(value)"), "lower(value)");
        assert_eq!(
            expression_label(&parse_sql_expr("(lower(e))").unwrap()),
            "lower"
        );
        assert_eq!(expression_label(&parse_sql_expr("a + b").unwrap()), "expr");
    }
}
