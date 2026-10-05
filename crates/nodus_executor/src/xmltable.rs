//! `XMLTABLE` in `FROM`: the conversions of PostgreSQL's `XmlTableGetValue`
//! that turn the rows' selected nodes into the columns' types, over the
//! `xpath` engine.

use crate::Value;
use crate::xpath::{XpDoc, XpError, XpExpr, XpValue};

/// An XPath failure as PostgreSQL's `XMLTABLE` reports one: the engine's
/// message — an expression it cannot compile, or a failure to build the
/// XPath object while evaluating — with `2200S`
/// (invalid_argument_for_xquery).
pub(crate) fn table_error(error: XpError) -> String {
    let XpError {
        message, detail, ..
    } = error;
    let mut error = crate::error_fields::DbError::new(message).code("2200S");
    if let Some(detail) = detail {
        error = error.detail(detail);
    }
    error.into_text()
}

/// The text one column takes from the node the row selected, or `None` when
/// the path selects nothing.
pub(crate) fn column_text(
    doc: &XpDoc,
    expr: &XpExpr,
    node: usize,
    column_type: &str,
) -> Result<Option<String>, String> {
    let value = doc.value_at(expr, node).map_err(table_error)?;
    Ok(match value {
        XpValue::Nodes(nodes) if nodes.is_empty() => None,
        // An XML column takes every selected node, concatenated.
        XpValue::Nodes(nodes) if crate::xml::is_type(column_type) => {
            Some(nodes.iter().map(|&node| doc.node_xml(node)).collect())
        }
        XpValue::Nodes(nodes) if nodes.len() > 1 => {
            return Err(crate::error_fields::DbError::new(
                "more than one value returned by column XPath expression",
            )
            .code("21000")
            .into_text());
        }
        // Any other column takes the string value of the one node.
        XpValue::Nodes(nodes) => Some(doc.node_string(nodes[0])),
        // A string result is escaped when the target is XML, so an XML
        // column reads it as text and not as markup.
        XpValue::Str(text) if crate::xml::is_type(column_type) => {
            Some(crate::xml::escape_xml(&text))
        }
        XpValue::Str(text) => Some(text),
        // A boolean converts to a number for a numeric column, and to
        // `true`/`false` for every other type.
        XpValue::Bool(value) => Some(match (value, is_numeric_category(column_type)) {
            (true, true) => "1".to_string(),
            (false, true) => "0".to_string(),
            (true, false) => "true".to_string(),
            (false, false) => "false".to_string(),
        }),
        XpValue::Num(number) => Some(crate::xpath::number_text(number)),
    })
}

/// A column's text as the column's type: its input function, applied the
/// way PostgreSQL's `InputFunctionCall` does — a character type enforces
/// its length rather than cutting.
pub(crate) fn convert_text(text: &str, column_type: &str) -> Result<Value, String> {
    if crate::value::character_limit(column_type).is_some() {
        return crate::value::fit_character(text, column_type, false).map(Value::Text);
    }
    crate::planner::try_cast(Value::Text(text.to_string()), column_type)
}

/// Whether a type is of the numeric category, whose booleans convert to
/// numbers.
fn is_numeric_category(column_type: &str) -> bool {
    let upper = column_type.trim().to_ascii_uppercase();
    let base = upper.split('(').next().unwrap_or_default().trim();
    matches!(
        base,
        "SMALLINT"
            | "INT2"
            | "INTEGER"
            | "INT"
            | "INT4"
            | "BIGINT"
            | "INT8"
            | "REAL"
            | "FLOAT4"
            | "DOUBLE PRECISION"
            | "FLOAT8"
            | "FLOAT"
            | "NUMERIC"
            | "DECIMAL"
            | "OID"
            | "MONEY"
    )
}
