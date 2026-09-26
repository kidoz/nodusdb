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

/// Parses SQL text into statements. As in PostgreSQL, an unquoted identifier
/// is folded to lower case (ASCII letters only) before parsing, so `Users`,
/// `USERS`, and `users` name the same object while `"Users"` names another.
pub fn parse_sql(
    sql: &str,
) -> Result<Vec<sqlparser::ast::Statement>, sqlparser::parser::ParserError> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let dialect = PostgreSqlDialect {};
    let mut tokens = Tokenizer::new(&dialect, sql)
        .with_unescape(true)
        .tokenize_with_location()?;
    for token in &mut tokens {
        if let Token::Word(word) = &mut token.token
            && word.quote_style.is_none()
        {
            word.value.make_ascii_lowercase();
        }
    }
    let tokens = reorder_identity_options(reorder_sequence_options(rewrite_data_clauses(tokens)));
    Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

/// The storage parameter that records `WITH NO DATA` on `CREATE TABLE ...
/// AS` and `CREATE MATERIALIZED VIEW`, which the parser does not accept.
pub const NO_DATA_OPTION: &str = "nodus_with_no_data";

/// The function call `REFRESH MATERIALIZED VIEW name [WITH [NO] DATA]` is
/// written as — `pg_catalog.nodus_refresh_materialized_view('name', true)`
/// — since the parser has no such statement.
pub const REFRESH_FUNCTION: &str = "pg_catalog.nodus_refresh_materialized_view";

/// The function call `ALTER SEQUENCE [IF EXISTS] name options` is written
/// as — `pg_catalog.nodus_alter_sequence('name', false, 'options')` — since
/// the parser has no such statement.
pub const ALTER_SEQUENCE_FUNCTION: &str = "pg_catalog.nodus_alter_sequence";

/// Rewrites what the parser lacks: a trailing `WITH [NO] DATA` on `CREATE
/// TABLE ... AS` or `CREATE MATERIALIZED VIEW` becomes the storage
/// parameter [`NO_DATA_OPTION`] (for `NO DATA`), and `REFRESH MATERIALIZED
/// VIEW` a call of [`REFRESH_FUNCTION`].
fn rewrite_data_clauses(
    tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::Token;
    let mut out = Vec::with_capacity(tokens.len());
    let mut statement = Vec::new();
    for token in tokens {
        let end = token.token == Token::SemiColon || token.token == Token::EOF;
        statement.push(token);
        if end {
            out.extend(rewrite_data_clause(std::mem::take(&mut statement)));
        }
    }
    out.extend(rewrite_data_clause(statement));
    out
}

fn rewrite_data_clause(
    statement: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};
    let word = |t: &TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant: Vec<usize> = statement
        .iter()
        .enumerate()
        .filter(|(_, t)| {
            !matches!(
                t.token,
                Token::Whitespace(_) | Token::SemiColon | Token::EOF
            )
        })
        .map(|(i, _)| i)
        .collect();
    let words: Vec<Option<String>> = significant.iter().map(|&i| word(&statement[i])).collect();
    let is = |at: usize, w: &str| words.get(at).and_then(|x| x.as_deref()) == Some(w);
    let tail: Vec<TokenWithSpan> = statement
        .iter()
        .skip(significant.last().map_or(0, |&i| i + 1))
        .cloned()
        .collect();
    // A trailing `WITH DATA` / `WITH NO DATA`.
    let n = significant.len();
    let data_clause = if n >= 3 && is(n - 3, "with") && is(n - 2, "no") && is(n - 1, "data") {
        Some((n - 3, false))
    } else if n >= 2 && is(n - 2, "with") && is(n - 1, "data") {
        Some((n - 2, true))
    } else {
        None
    };

    if is(0, "refresh") && is(1, "materialized") && is(2, "view") {
        let mut at = 3;
        if is(at, "concurrently") {
            at += 1;
        }
        let name_end = data_clause.map_or(n, |(start, _)| start);
        let name: String = significant[at..name_end]
            .iter()
            .map(|&i| match &statement[i].token {
                Token::Word(w) if w.quote_style == Some('"') => {
                    format!("\"{}\"", w.value.replace('"', "\"\""))
                }
                Token::Word(w) => w.value.clone(),
                other => other.to_string(),
            })
            .collect();
        let with_data = data_clause.is_none_or(|(_, with)| with);
        let sql = format!(
            "SELECT {REFRESH_FUNCTION}('{}', {with_data})",
            name.replace('\'', "''")
        );
        let dialect = PostgreSqlDialect {};
        return match Tokenizer::new(&dialect, &sql).tokenize_with_location() {
            Ok(mut tokens) => {
                tokens.retain(|t| t.token != Token::EOF);
                tokens.extend(tail);
                tokens
            }
            Err(_) => statement,
        };
    }

    if is(0, "alter") && is(1, "sequence") {
        let if_exists = is(2, "if") && is(3, "exists");
        let at = if if_exists { 4 } else { 2 };
        // The name: words joined by periods.
        let mut name_end = at + 1;
        while name_end + 1 < n
            && statement
                .get(significant[name_end])
                .is_some_and(|t| t.token == Token::Period)
        {
            name_end += 2;
        }
        let render = |t: &TokenWithSpan| match &t.token {
            Token::Word(w) if w.quote_style == Some('"') => {
                format!("\"{}\"", w.value.replace('"', "\"\""))
            }
            Token::Word(w) => w.value.clone(),
            other => other.to_string(),
        };
        let name: String = significant[at..name_end.min(n)]
            .iter()
            .map(|&i| render(&statement[i]))
            .collect();
        let options: String = match (significant.get(name_end), significant.last()) {
            (Some(&first), Some(&last)) => statement[first..=last].iter().map(render).collect(),
            _ => String::new(),
        };
        let sql = format!(
            "SELECT {ALTER_SEQUENCE_FUNCTION}('{}', {if_exists}, '{}')",
            name.replace('\'', "''"),
            options.replace('\'', "''")
        );
        let dialect = PostgreSqlDialect {};
        return match Tokenizer::new(&dialect, &sql).tokenize_with_location() {
            Ok(mut tokens) => {
                tokens.retain(|t| t.token != Token::EOF);
                tokens.extend(tail);
                tokens
            }
            Err(_) => statement,
        };
    }

    let creates = is(0, "create")
        && ((is(1, "materialized") && is(2, "view"))
            || (1..4).any(|at| is(at, "table"))
                && words.iter().any(|w| w.as_deref() == Some("as")));
    let Some((clause_start, with_data)) = data_clause.filter(|_| creates) else {
        return statement;
    };
    let mut out: Vec<TokenWithSpan> = statement[..significant[clause_start]].to_vec();
    if !with_data {
        // `WITH (nodus_with_no_data = true)` before the top-level `AS`.
        let mut depth = 0i32;
        let as_at = out.iter().position(|t| {
            match &t.token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            depth == 0 && word(t).as_deref() == Some("as")
        });
        let has_with = out[..as_at.unwrap_or(0)]
            .iter()
            .any(|t| word(t).as_deref() == Some("with"));
        if let (Some(as_at), false) = (as_at, has_with) {
            let dialect = PostgreSqlDialect {};
            if let Ok(mut option) =
                Tokenizer::new(&dialect, &format!("WITH ({NO_DATA_OPTION} = true) "))
                    .tokenize_with_location()
            {
                option.retain(|t| t.token != Token::EOF);
                out.splice(as_at..as_at, option);
            }
        }
    }
    out.extend(tail);
    out
}

/// PostgreSQL accepts `CREATE SEQUENCE` options in any order (pg_dump writes
/// `START WITH` before `INCREMENT BY`), while the parser takes them in one
/// fixed order. Each such statement's options are put in that order; any
/// statement this does not recognize is left as it is.
fn reorder_sequence_options(
    tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::Token;
    let mut out = Vec::with_capacity(tokens.len());
    let mut statement = Vec::new();
    for token in tokens {
        let end = token.token == Token::SemiColon || token.token == Token::EOF;
        statement.push(token);
        if end {
            out.extend(reorder_statement(std::mem::take(&mut statement)));
        }
    }
    out.extend(reorder_statement(statement));
    out
}

fn reorder_statement(
    statement: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::{Token, TokenWithSpan};
    fn word(t: &TokenWithSpan) -> Option<&str> {
        match &t.token {
            Token::Word(w) if w.quote_style.is_none() => Some(w.value.as_str()),
            _ => None,
        }
    }
    let significant: Vec<usize> = statement
        .iter()
        .enumerate()
        .filter(|(_, t)| !matches!(t.token, Token::Whitespace(_)))
        .map(|(i, _)| i)
        .collect();
    // `CREATE [TEMP | TEMPORARY] SEQUENCE [IF NOT EXISTS] name ...`
    let mut at = 0;
    let mut expect = |words: &[&str]| -> bool {
        match significant.get(at).and_then(|&i| word(&statement[i])) {
            Some(w) if words.contains(&w) => {
                at += 1;
                true
            }
            _ => false,
        }
    };
    if !expect(&["create"]) {
        return statement;
    }
    expect(&["temp", "temporary"]);
    if !expect(&["sequence"]) {
        return statement;
    }
    if expect(&["if"]) && !(expect(&["not"]) && expect(&["exists"])) {
        return statement;
    }
    // The name: identifiers joined by periods.
    let mut name_end = at;
    while let Some(&i) = significant.get(name_end) {
        let is_part = matches!(statement[i].token, Token::Word(_) | Token::Period);
        if !is_part || (name_end > at && word(&statement[i]).is_some_and(is_option_keyword)) {
            break;
        }
        name_end += 1;
    }
    let Some(&options_start) = significant.get(name_end) else {
        return statement;
    };
    let options_end = significant
        .iter()
        .rev()
        .find(|&&i| !matches!(statement[i].token, Token::SemiColon | Token::EOF))
        .map_or(statement.len(), |&i| i + 1);
    if options_start >= options_end {
        return statement;
    }
    match order_options(&statement[options_start..options_end]) {
        Some(options) => {
            let mut out: Vec<TokenWithSpan> = statement[..options_start].to_vec();
            out.extend(options);
            out.extend(statement[options_end..].iter().cloned());
            out
        }
        None => statement,
    }
}

/// Puts a run of sequence options (`START WITH 5 INCREMENT BY 2 ...`) in the
/// parser's order; `None` if the run holds anything but options.
fn order_options(
    region: &[sqlparser::tokenizer::TokenWithSpan],
) -> Option<Vec<sqlparser::tokenizer::TokenWithSpan>> {
    use sqlparser::tokenizer::{Token, TokenWithSpan, Whitespace};
    fn word(t: &TokenWithSpan) -> Option<&str> {
        match &t.token {
            Token::Word(w) if w.quote_style.is_none() => Some(w.value.as_str()),
            _ => None,
        }
    }
    // Group the options by kind; each group runs to the next option keyword.
    let rank = |w: &str| match w {
        "as" => Some(0),
        "increment" => Some(1),
        "minvalue" => Some(2),
        "maxvalue" => Some(3),
        "start" => Some(4),
        "cache" => Some(5),
        "cycle" => Some(6),
        "owned" => Some(7),
        _ => None,
    };
    let mut groups: Vec<(usize, Vec<TokenWithSpan>)> = Vec::new();
    let mut i = 0;
    while i < region.len() {
        let token = &region[i];
        if matches!(token.token, Token::Whitespace(_)) {
            if let Some((_, group)) = groups.last_mut() {
                group.push(token.clone());
            }
            i += 1;
            continue;
        }
        let w = word(token);
        // `NO MINVALUE`, `NO MAXVALUE`, `NO CYCLE`: ranked by the next word.
        let starts = match w {
            Some("no") => region[i + 1..]
                .iter()
                .find(|t| !matches!(t.token, Token::Whitespace(_)))
                .and_then(word)
                .and_then(rank),
            Some(w) => rank(w),
            None => None,
        };
        match (starts, groups.last_mut()) {
            (Some(r), _) => groups.push((r, vec![token.clone()])),
            (None, Some((_, group))) => group.push(token.clone()),
            // Something other than an option comes first.
            (None, None) => return None,
        }
        // The word after `NO` belongs to the same group.
        if w == Some("no") {
            i += 1;
            while i < region.len() && matches!(region[i].token, Token::Whitespace(_)) {
                i += 1;
            }
            if i < region.len() {
                groups.last_mut()?.1.push(region[i].clone());
            }
        }
        i += 1;
    }
    groups.sort_by_key(|(r, _)| *r);
    let space = TokenWithSpan::wrap(Token::Whitespace(Whitespace::Space));
    let mut out = Vec::with_capacity(region.len());
    for (_, group) in groups {
        out.extend(
            group
                .into_iter()
                .skip_while(|t| matches!(t.token, Token::Whitespace(_))),
        );
        out.push(space.clone());
    }
    Some(out)
}

/// Reorders the option list of each `... AS IDENTITY (options)`.
fn reorder_identity_options(
    mut tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::Token;
    let mut i = 0;
    while i < tokens.len() {
        let is_identity = matches!(&tokens[i].token,
            Token::Word(w) if w.quote_style.is_none() && w.value == "identity");
        if is_identity {
            let open = (i + 1..tokens.len())
                .find(|&j| !matches!(tokens[j].token, Token::Whitespace(_)))
                .filter(|&j| tokens[j].token == Token::LParen);
            let close = open
                .and_then(|o| (o + 1..tokens.len()).find(|&j| tokens[j].token == Token::RParen));
            if let (Some(open), Some(close)) = (open, close)
                && let Some(options) = order_options(&tokens[open + 1..close])
            {
                let rest = tokens.split_off(close);
                tokens.truncate(open + 1);
                tokens.extend(options);
                tokens.extend(rest);
            }
        }
        i += 1;
    }
    tokens
}

fn is_option_keyword(w: &str) -> bool {
    matches!(
        w,
        "as" | "increment" | "minvalue" | "maxvalue" | "start" | "cache" | "cycle" | "owned" | "no"
    )
}

/// Extracts `(name, value)` from a parsed `SET <name> = <value>` statement when
/// the value is a single scalar. Returns `None` for non-`SET` statements and for
/// list- or multi-valued sets (e.g. `SET search_path TO a, b`). The value keeps
/// the parser's rendering (quotes included); callers normalize as needed.
///
/// The wire layer uses this to decide whether a successful `SET` should be
/// echoed back as a `ParameterStatus` message (for `GUC_REPORT` variables),
/// without re-parsing the raw SQL text.
pub fn set_variable_parts(stmt: &sqlparser::ast::Statement) -> Option<(String, String)> {
    use sqlparser::ast::{Set, Statement};
    match stmt {
        Statement::Set(Set::SingleAssignment {
            variable, values, ..
        }) => {
            let rendered: Vec<String> = values.iter().map(|v| v.to_string()).collect();
            if rendered.len() != 1 {
                return None;
            }
            Some((variable.to_string(), rendered.into_iter().next()?))
        }
        // `SET TIME ZONE <x>` is the SQL-standard spelling of `SET timezone = <x>`.
        Statement::Set(Set::SetTimeZone { value, .. }) => {
            Some(("timezone".to_string(), value.to_string()))
        }
        _ => None,
    }
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
    fn unquoted_identifiers_fold_to_lower_case() {
        let stmts = parse_sql(r#"SELECT Name, "Name" FROM Users WHERE ÄB = 'Keep'"#).unwrap();
        assert_eq!(
            stmts[0].to_string(),
            r#"SELECT name, "Name" FROM users WHERE Äb = 'Keep'"#
        );
    }

    #[test]
    fn sequence_options_parse_in_any_order() {
        let stmts = parse_sql(
            "CREATE SEQUENCE public.t_id_seq\n    AS integer\n    START WITH 5\n    INCREMENT BY 2\n    NO MINVALUE\n    NO MAXVALUE\n    CACHE 1; SELECT 1",
        )
        .unwrap();
        assert_eq!(stmts.len(), 2);
        let text = stmts[0].to_string();
        assert!(text.contains("INCREMENT BY 2"), "{text}");
        assert!(text.contains("START WITH 5"), "{text}");
        assert!(parse_sql("CREATE SEQUENCE s START 5 INCREMENT 2 NO CYCLE").is_ok());
        let identity = parse_sql(
            "CREATE TABLE t (id int GENERATED BY DEFAULT AS IDENTITY (START WITH 10 INCREMENT BY 5) PRIMARY KEY)",
        )
        .unwrap();
        assert!(identity[0].to_string().contains("INCREMENT BY 5"));
    }

    #[test]
    fn data_clauses_and_refresh_parse() {
        let statements = parse_sql(
            "CREATE MATERIALIZED VIEW mv AS SELECT 1 WITH NO DATA; \
             CREATE TABLE t AS SELECT 1 WITH DATA; \
             REFRESH MATERIALIZED VIEW CONCURRENTLY \"My\".mv WITH NO DATA",
        )
        .unwrap();
        assert_eq!(statements.len(), 3);
        assert!(statements[0].to_string().contains(NO_DATA_OPTION));
        assert!(!statements[1].to_string().contains(NO_DATA_OPTION));
        assert_eq!(
            statements[2].to_string(),
            format!("SELECT {REFRESH_FUNCTION}('\"My\".mv', false)")
        );
    }

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
