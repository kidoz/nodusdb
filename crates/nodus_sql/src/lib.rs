use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug)]
pub struct SessionState {
    pub session_id: String,
    pub session_user: String,
    pub database_name: String,
    pub search_path: Vec<String>,
    pub active_roles: Vec<String>,
    pub application_name: String,
    pub transaction_status: String,
}

impl Default for SessionState {
    fn default() -> Self {
        Self {
            session_id: "default_session".to_string(),
            session_user: "nodus".to_string(),
            database_name: "default".to_string(),
            search_path: vec!["public".to_string()],
            active_roles: vec![],
            application_name: "nodusctl".to_string(),
            transaction_status: "idle".to_string(),
        }
    }
}

pub fn parse_sql(
    sql: &str,
) -> Result<Vec<sqlparser::ast::Statement>, sqlparser::parser::ParserError> {
    let dialect = PostgreSqlDialect {};
    Parser::parse_sql(&dialect, sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_placeholder_parsing() {
        let sql = "SELECT * FROM users WHERE id = $1 AND name = $2";
        let _stmts = parse_sql(sql).unwrap();
        // Debugging output removed
    }
    use proptest::prelude::*;

    #[test]
    fn test_parse_simple() {
        let stmts = parse_sql("SELECT 1;").unwrap();
        assert_eq!(stmts.len(), 1);
    }

    proptest! {
        #[test]
        fn test_parser_no_panic_on_garbage(ref s in "\\PC*") {
            // The parser should return an Error rather than panicking.
            let _ = parse_sql(s);
        }

        #[test]
        fn test_parser_valid_select(ref c in "[a-zA-Z_][a-zA-Z0-9_]*", ref t in "[a-zA-Z_][a-zA-Z0-9_]*") {
            // Test that generating a syntactically valid SELECT statement always parses successfully
            let query = format!("SELECT {} FROM {};", c, t);
            let res = parse_sql(&query);
            prop_assert!(res.is_ok(), "Failed to parse: {}", query);
        }
    }
}
