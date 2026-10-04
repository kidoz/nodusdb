use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

mod numeric_literals;

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
    let sql = numeric_literals::normalize(sql);
    let mut tokens = Tokenizer::new(&dialect, &sql)
        .with_unescape(true)
        .tokenize_with_location()?;
    for token in &mut tokens {
        if let Token::Word(word) = &mut token.token
            && word.quote_style.is_none()
        {
            word.value.make_ascii_lowercase();
        }
    }
    let tokens = reorder_identity_options(reorder_sequence_options(rewrite_data_clauses(
        rewrite_record_star(rewrite_xml_syntax(rewrite_query_syntax(tokens))),
    )));
    Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

/// The function a window frame's `EXCLUDE CURRENT ROW | GROUP | TIES` is
/// written as, in the window's `PARTITION BY` (upper-cased as planned):
/// `pg_catalog.__exclude__('ties')`, since the parser has no `EXCLUDE`.
pub const EXCLUDE_MARKER: &str = "__EXCLUDE__";

/// The function `x BETWEEN SYMMETRIC a AND b` marks its low bound with
/// (upper-cased as planned): `x BETWEEN pg_catalog.__symmetric__(a) AND b`,
/// since the parser has no `SYMMETRIC`.
pub const SYMMETRIC_MARKER: &str = "__SYMMETRIC__";

/// The suffix an `INSERT`'s target alias carries for `OVERRIDING SYSTEM
/// VALUE` (and [`OVERRIDING_USER`] for `OVERRIDING USER VALUE`), which the
/// parser lacks: `INSERT INTO t AS __overriding_system__ ...`.
pub const OVERRIDING_SYSTEM: &str = "__overriding_system__";
pub const OVERRIDING_USER: &str = "__overriding_user__";

/// The function `xmlparse(document|content <value>)` is written as
/// (upper-cased as planned): `pg_catalog.__xmlparse__(value, document)`,
/// which the parser cannot read as the SQL/XML construct it is.
pub const XML_PARSE_MARKER: &str = "__xmlparse__";

/// The function `xmlserialize(content|document <value> AS <type> [INDENT])`
/// is written as: `pg_catalog.__xmlserialize__(value, document, indent,
/// '<type>')`.
pub const XML_SERIALIZE_MARKER: &str = "__xmlserialize__";

/// The function `xml IS [NOT] DOCUMENT` is written as:
/// `pg_catalog.__xml_is_document__(value)`.
pub const XML_IS_DOCUMENT_MARKER: &str = "__xml_is_document__";

/// The XML syntax the parser lacks: `xmlparse`, `xmlserialize`, and
/// `IS [NOT] DOCUMENT`, each rewritten to the marker function the planner
/// knows ([`XML_PARSE_MARKER`], [`XML_SERIALIZE_MARKER`],
/// [`XML_IS_DOCUMENT_MARKER`]).
fn rewrite_xml_syntax(
    mut tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::{Token, TokenWithSpan};
    let word = |t: &TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant = |tokens: &[TokenWithSpan], from: usize| {
        (from..tokens.len()).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
    };
    let significant_back = |tokens: &[TokenWithSpan], from: usize| {
        (0..from)
            .rev()
            .find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
    };
    // The `)` that closes the `(` at `open`.
    let matching_paren = |tokens: &[TokenWithSpan], open: usize| {
        let mut depth = 0i32;
        for (i, token) in tokens.iter().enumerate().skip(open) {
            match token.token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        None
    };
    let kind_at = |tokens: &[TokenWithSpan], open: usize| match significant(tokens, open + 1) {
        Some(at) => match word(&tokens[at]).as_deref() {
            Some(kind @ ("document" | "content")) => Some((at, kind == "document")),
            _ => None,
        },
        None => None,
    };
    // `xmlparse(document|content <value>)`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("xmlparse")
            && let Some(open) = significant(&tokens, i + 1)
            && tokens[open].token == Token::LParen
            && let Some((at, document)) = kind_at(&tokens, open)
            && let Some(close) = matching_paren(&tokens, open)
        {
            let value = render_tokens(&tokens[at + 1..close]);
            let replacement = format!("pg_catalog.{XML_PARSE_MARKER}({value}, {document})",);
            if let Some(snippet) = snippet_tokens(&replacement) {
                let len = snippet.len();
                tokens.splice(i..=close, snippet);
                i += len;
                continue;
            }
        }
        i += 1;
    }
    // `xmlserialize(content|document <value> AS <type> [INDENT])`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("xmlserialize")
            && let Some(open) = significant(&tokens, i + 1)
            && tokens[open].token == Token::LParen
            && let Some((at, document)) = kind_at(&tokens, open)
            && let Some(close) = matching_paren(&tokens, open)
        {
            // The last `AS` at the top level separates the value from the type.
            let mut depth = 0i32;
            let mut as_at = None;
            for (j, token) in tokens.iter().enumerate().take(close).skip(at + 1) {
                match token.token {
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => {}
                }
                if depth == 0 && word(token).as_deref() == Some("as") {
                    as_at = Some(j);
                }
            }
            if let Some(as_at) = as_at {
                // A trailing `INDENT` asks for the formatted form.
                let mut type_end = close;
                let mut indent = false;
                if let Some(last) = significant_back(&tokens, close)
                    && word(&tokens[last]).as_deref() == Some("indent")
                {
                    indent = true;
                    type_end = last;
                }
                let value = render_tokens(&tokens[at + 1..as_at]);
                let target = render_tokens(&tokens[as_at + 1..type_end]);
                let target = target.trim();
                if !target.is_empty() {
                    let replacement = format!(
                        "pg_catalog.{XML_SERIALIZE_MARKER}({value}, {document}, {indent}, \
                         '{target}')"
                    );
                    if let Some(snippet) = snippet_tokens(&replacement) {
                        let len = snippet.len();
                        tokens.splice(i..=close, snippet);
                        i += len;
                        continue;
                    }
                }
            }
        }
        i += 1;
    }
    // `xml IS [NOT] DOCUMENT`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("is") {
            let mut negated = false;
            let mut at = i;
            if let Some(not_at) = significant(&tokens, i + 1)
                && word(&tokens[not_at]).as_deref() == Some("not")
            {
                negated = true;
                at = not_at;
            }
            let document_at = significant(&tokens, at + 1)
                .filter(|&k| word(&tokens[k]).as_deref() == Some("document"));
            if let Some(document_at) = document_at
                && let Some(start) = operand_start(&tokens, i)
            {
                let value = render_tokens(&tokens[start..i]);
                let replacement = format!(
                    "{}pg_catalog.{XML_IS_DOCUMENT_MARKER}({value})",
                    if negated { "not " } else { "" }
                );
                if let Some(snippet) = snippet_tokens(&replacement) {
                    let len = snippet.len();
                    tokens.splice(start..=document_at, snippet);
                    i = start + len;
                    continue;
                }
            }
        }
        i += 1;
    }
    tokens
}

/// The first token of the expression ending just before `before`: a value
/// with its `::type` casts, dotted names, and parenthesized or called parts.
/// `None` when the expression is not one of those shapes, which leaves the
/// construct to the parser's own error.
fn operand_start(tokens: &[sqlparser::tokenizer::TokenWithSpan], before: usize) -> Option<usize> {
    use sqlparser::tokenizer::Token;
    let significant_back = |from: usize| {
        (0..from)
            .rev()
            .find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
    };
    let word = |t: &sqlparser::tokenizer::TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let mut pos = significant_back(before)?;
    loop {
        // One unit: a value, a parenthesized expression, or a call.
        match &tokens[pos].token {
            Token::RParen => {
                // The `(` that closes it.
                let mut depth = 0i32;
                loop {
                    match tokens[pos].token {
                        Token::RParen => depth += 1,
                        Token::LParen => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    if pos == 0 {
                        return None;
                    }
                    pos -= 1;
                }
            }
            Token::Word(_)
            | Token::SingleQuotedString(_)
            | Token::DoubleQuotedString(_)
            | Token::Number(_, _)
            | Token::Placeholder(_)
            | Token::EscapedStringLiteral(_) => {}
            _ => return None,
        }
        // A function name or a dotted name may precede a `(`.
        if tokens[pos].token == Token::LParen
            && let Some(name) = significant_back(pos)
            && matches!(tokens[name].token, Token::Word(_))
        {
            pos = name;
        }
        // A dotted name.
        while let Some(dot) = significant_back(pos)
            && tokens[dot].token == Token::Period
        {
            let Some(name) = significant_back(dot) else {
                return Some(pos);
            };
            match &tokens[name].token {
                Token::Word(_) => pos = name,
                _ => return Some(pos),
            }
        }
        // `value::type`: the unit just consumed was the type name, so the
        // value precedes it — and may itself be cast or dotted.
        let Some(colons) = significant_back(pos) else {
            return Some(pos);
        };
        if tokens[colons].token != Token::DoubleColon {
            return Some(pos);
        }
        // The type: a name, optionally with a modifier or dotted parts.
        let mut ty = significant_back(colons)?;
        if tokens[ty].token == Token::RParen {
            let mut depth = 0i32;
            loop {
                match tokens[ty].token {
                    Token::RParen => depth += 1,
                    Token::LParen => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                ty = significant_back(ty)?;
            }
            ty = significant_back(ty)?;
        }
        // `character varying` and `double precision` are two words.
        if let Some(prev) = significant_back(ty)
            && let (Some(second), Some(first)) = (word(&tokens[ty]), word(&tokens[prev]))
            && matches!(
                (first.as_str(), second.as_str()),
                ("character" | "bit", "varying") | ("double", "precision")
            )
        {
            ty = prev;
        }
        pos = ty;
    }
}

/// `(value).*`, a record expanded into its fields
/// ([`EXPAND_RECORD_FUNCTION`]).
fn rewrite_record_star(
    mut tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::Token;
    let mut i = 0;
    while i + 2 < tokens.len() {
        if matches!(tokens[i].token, Token::RParen)
            && matches!(tokens[i + 1].token, Token::Period)
            && matches!(tokens[i + 2].token, Token::Mul)
        {
            // The parenthesis the `.*` applies to.
            let mut depth = 1i32;
            let mut open = None;
            for j in (0..i).rev() {
                match tokens[j].token {
                    Token::RParen => depth += 1,
                    Token::LParen => {
                        depth -= 1;
                        if depth == 0 {
                            open = Some(j);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            if let Some(open) = open {
                let inner = render_tokens(&tokens[open + 1..i]);
                if let Some(replacement) =
                    snippet_tokens(&format!("{EXPAND_RECORD_FUNCTION}({inner})"))
                {
                    tokens.splice(open..=i + 2, replacement);
                    continue;
                }
            }
        }
        i += 1;
    }
    tokens
}

/// Query syntax the parser lacks: `TABLE name` as a query (`SELECT * FROM
/// name`), `BETWEEN [A]SYMMETRIC` ([`SYMMETRIC_MARKER`]), and a window
/// frame's `EXCLUDE` ([`EXCLUDE_MARKER`]).
fn rewrite_query_syntax(
    mut tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};
    let snippet = |sql: &str| -> Vec<TokenWithSpan> {
        let dialect = PostgreSqlDialect {};
        Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .map(|mut t| {
                t.retain(|t| t.token != Token::EOF);
                t
            })
            .unwrap_or_default()
    };
    let word = |t: &TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant = |tokens: &[TokenWithSpan], from: usize| {
        (from..tokens.len()).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
    };
    // `TABLE name` where a query may start.
    let mut i = 0;
    let mut previous: Option<usize> = None;
    while i < tokens.len() {
        if matches!(tokens[i].token, Token::Whitespace(_)) {
            i += 1;
            continue;
        }
        let starts_query = match previous {
            None => true,
            Some(p) => {
                matches!(tokens[p].token, Token::SemiColon | Token::LParen)
                    || matches!(
                        word(&tokens[p]).as_deref(),
                        Some("union" | "intersect" | "except" | "all" | "distinct")
                    )
            }
        };
        if starts_query && word(&tokens[i]).as_deref() == Some("table") {
            let replacement = snippet("SELECT * FROM");
            let len = replacement.len();
            tokens.splice(i..=i, replacement);
            previous = Some(i + len - 1);
            i += len;
            continue;
        }
        previous = Some(i);
        i += 1;
    }
    // `INSERT INTO t [AS a] ... OVERRIDING {SYSTEM|USER} VALUE`: the clause
    // goes, and the target's alias carries it.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("overriding")
            && let Some(a) = significant(&tokens, i + 1)
            && let Some(b) = significant(&tokens, a + 1)
            && word(&tokens[b]).as_deref() == Some("value")
            && let Some(kind @ ("system" | "user")) = word(&tokens[a]).as_deref()
        {
            let marker = if kind == "system" {
                OVERRIDING_SYSTEM
            } else {
                OVERRIDING_USER
            };
            tokens.drain(i..=b);
            // The target: the name after `INSERT INTO`, dotted parts included.
            let into = (0..i)
                .rev()
                .find(|&k| word(&tokens[k]).as_deref() == Some("into"));
            if let Some(into) = into
                && let Some(first) = significant(&tokens, into + 1)
            {
                let mut last = first;
                while let Some(dot) = significant(&tokens, last + 1)
                    && tokens[dot].token == Token::Period
                    && let Some(part) = significant(&tokens, dot + 1)
                {
                    last = part;
                }
                let alias = significant(&tokens, last + 1)
                    .filter(|&k| word(&tokens[k]).as_deref() == Some("as"))
                    .and_then(|k| significant(&tokens, k + 1));
                match alias {
                    Some(k) => {
                        if let Token::Word(w) = &mut tokens[k].token {
                            w.value.push_str(marker);
                        }
                    }
                    None => {
                        tokens.splice(last + 1..last + 1, snippet(&format!(" AS {marker}")));
                    }
                }
            }
            continue;
        }
        i += 1;
    }
    // `FOR NO KEY UPDATE` and `FOR KEY SHARE` lock as `FOR UPDATE` and
    // `FOR SHARE` do.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("for")
            && let Some(a) = significant(&tokens, i + 1)
        {
            let second = significant(&tokens, a + 1);
            let third = second.and_then(|b| significant(&tokens, b + 1));
            match (
                word(&tokens[a]).as_deref(),
                second.and_then(|b| word(&tokens[b])).as_deref(),
                third.and_then(|c| word(&tokens[c])).as_deref(),
            ) {
                (Some("no"), Some("key"), Some("update")) => {
                    tokens.drain(a..third.unwrap_or(a));
                }
                (Some("key"), Some("share"), _) => {
                    tokens.drain(a..second.unwrap_or(a));
                }
                _ => {}
            }
        }
        i += 1;
    }
    // `BETWEEN [A]SYMMETRIC low AND high`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("between")
            && let Some(next) = significant(&tokens, i + 1)
            && let Some(kind @ ("symmetric" | "asymmetric")) = word(&tokens[next]).as_deref()
        {
            let symmetric = kind == "symmetric";
            tokens.remove(next);
            if symmetric {
                // The low bound runs to the next `AND` at its depth.
                let mut depth = 0i32;
                let mut end = None;
                for (j, token) in tokens.iter().enumerate().skip(next) {
                    match token.token {
                        Token::LParen => depth += 1,
                        Token::RParen => depth -= 1,
                        _ => {}
                    }
                    if depth == 0 && word(token).as_deref() == Some("and") {
                        end = Some(j);
                        break;
                    }
                }
                if let Some(end) = end {
                    tokens.splice(end..end, snippet(")"));
                    tokens.splice(
                        next..next,
                        snippet(&format!(
                            "pg_catalog.{}(",
                            SYMMETRIC_MARKER.to_ascii_lowercase()
                        )),
                    );
                }
            }
        }
        i += 1;
    }
    // A window frame's `EXCLUDE ...`, from the last one back so earlier
    // positions stay put.
    let excludes: Vec<usize> = (0..tokens.len())
        .filter(|&i| word(&tokens[i]).as_deref() == Some("exclude"))
        .collect();
    for &at in excludes.iter().rev() {
        let mut words = Vec::new();
        let mut j = at;
        while words.len() < 2 {
            match significant(&tokens, j + 1) {
                Some(k) => {
                    words.push((k, word(&tokens[k]).unwrap_or_default()));
                    j = k;
                }
                None => break,
            }
        }
        let (kind, last) = match words.as_slice() {
            [(_, a), (k, b)] if a == "current" && b == "row" => ("current row", *k),
            [(_, a), (k, b)] if a == "no" && b == "others" => ("no others", *k),
            [(k, a), ..] if a == "group" || a == "ties" => (a.as_str(), *k),
            _ => continue,
        };
        let kind = kind.to_string();
        tokens.drain(at..=last);
        if kind == "no others" {
            continue;
        }
        // The window specification's opening parenthesis.
        let mut depth = 0i32;
        let Some(open) = (0..at).rev().find(|&k| match tokens[k].token {
            Token::RParen => {
                depth += 1;
                false
            }
            Token::LParen if depth == 0 => true,
            Token::LParen => {
                depth -= 1;
                false
            }
            _ => false,
        }) else {
            continue;
        };
        let marker = format!(
            "pg_catalog.{}('{kind}')",
            EXCLUDE_MARKER.to_ascii_lowercase()
        );
        let first = significant(&tokens, open + 1);
        let first_word = first.and_then(|k| word(&tokens[k]));
        match first_word.as_deref() {
            Some("partition") => {
                if let Some(by) = first.and_then(|k| significant(&tokens, k + 1)) {
                    tokens.splice(by + 1..by + 1, snippet(&format!(" {marker},")));
                }
            }
            Some("order" | "rows" | "range" | "groups") | None => {
                tokens.splice(
                    open + 1..open + 1,
                    snippet(&format!("PARTITION BY {marker} ")),
                );
            }
            // A named window being refined: after its name.
            Some(_) => {
                let name = first.unwrap_or(open + 1);
                tokens.splice(
                    name + 1..name + 1,
                    snippet(&format!(" PARTITION BY {marker} ")),
                );
            }
        }
    }
    tokens
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

/// The function call a maintenance statement the parser lacks is written as
/// — `pg_catalog.nodus_utility('CHECKPOINT')` — for `CHECKPOINT`,
/// `REINDEX`, `CLUSTER`, `LOAD`, `VACUUM` with options or tables, and
/// `PREPARE TRANSACTION`.
pub const UTILITY_FUNCTION: &str = "pg_catalog.nodus_utility";

/// The function call `FETCH` and `MOVE` are written as — `SELECT
/// pg_catalog.nodus_cursor('FETCH', 'relative -2', 'c')`, with the
/// direction's words — since the parser takes them only in part.
pub const CURSOR_FUNCTION: &str = "pg_catalog.nodus_cursor";

/// The function call `ALTER DOMAIN` and `DROP DOMAIN` are written as —
/// `SELECT pg_catalog.nodus_domain('ALTER DOMAIN d SET NOT NULL')`, with
/// the statement's text — since the parser has no such statements.
pub const DOMAIN_FUNCTION: &str = "pg_catalog.nodus_domain";

/// The check a domain's `NOT NULL` is written as in `CREATE DOMAIN` —
/// `CHECK (nodus_domain_not_null)` — which the parser does not accept.
pub const DOMAIN_NOT_NULL: &str = "nodus_domain_not_null";

/// The constraint name `ON CONFLICT (expressions)` is written as — `ON
/// CONFLICT ON CONSTRAINT "nodus_conflict:lower(email)"` — since the
/// parser takes only column names there.
pub const CONFLICT_EXPRESSIONS: &str = "nodus_conflict:";

/// The function call `ALTER {TABLE | VIEW | MATERIALIZED VIEW | SEQUENCE |
/// TYPE | DOMAIN} [IF EXISTS] name SET SCHEMA schema` is written as —
/// `SELECT pg_catalog.nodus_set_schema('TABLE', false, 'name', 'schema')` —
/// since the parser takes it for none of them.
pub const SET_SCHEMA_FUNCTION: &str = "pg_catalog.nodus_set_schema";

/// The function call `CREATE SCHEMA` with the objects it creates is
/// written as — `SELECT pg_catalog.nodus_create_schema('create schema s',
/// 'create table t (a int)', ...)`, with each statement's text — since the
/// parser takes no schema elements.
pub const CREATE_SCHEMA_FUNCTION: &str = "pg_catalog.nodus_create_schema";

/// The function call `ALTER TYPE ... ADD | DROP | RENAME ATTRIBUTE` is
/// written as — `SELECT pg_catalog.nodus_alter_type('add', 'pair', 'c',
/// 'int')` — since the parser takes only enum operations.
pub const TYPE_FUNCTION: &str = "pg_catalog.nodus_alter_type";

/// The function `(value).*` — a record expanded into its fields — is
/// written as — `pg_catalog.nodus_expand_record(value)` — since the parser
/// has no `.*` after a parenthesized expression.
pub const EXPAND_RECORD_FUNCTION: &str = "pg_catalog.nodus_expand_record";

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
            out.extend(rewrite_data_clause(rewrite_statement_form(std::mem::take(
                &mut statement,
            ))));
        }
    }
    out.extend(rewrite_data_clause(rewrite_statement_form(statement)));
    out
}

/// Tokens as SQL text again: strings and quoted names quoted as written
/// (their quotes doubled), anything else as it is.
fn render_tokens(tokens: &[sqlparser::tokenizer::TokenWithSpan]) -> String {
    use sqlparser::tokenizer::Token;
    tokens
        .iter()
        .map(|t| match &t.token {
            Token::Word(w) if w.quote_style == Some('"') => {
                format!("\"{}\"", w.value.replace('"', "\"\""))
            }
            Token::SingleQuotedString(s) | Token::EscapedStringLiteral(s) => {
                format!("'{}'", s.replace('\'', "''"))
            }
            Token::EOF | Token::SemiColon => String::new(),
            other => other.to_string(),
        })
        .collect()
}

/// SQL text as tokens, to splice into a statement.
fn snippet_tokens(sql: &str) -> Option<Vec<sqlparser::tokenizer::TokenWithSpan>> {
    use sqlparser::tokenizer::{Token, Tokenizer};
    let mut tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    tokens.retain(|t| t.token != Token::EOF);
    Some(tokens)
}

/// Statements the parser lacks or takes only in part: `FETCH` and `MOVE`
/// ([`CURSOR_FUNCTION`]), `ALTER DOMAIN` and `DROP DOMAIN`
/// ([`DOMAIN_FUNCTION`]), a domain's `NOT NULL` ([`DOMAIN_NOT_NULL`]), a
/// view's `WITH [CASCADED | LOCAL] CHECK OPTION` (its `check_option`), and
/// `ON CONFLICT (expressions)` ([`CONFLICT_EXPRESSIONS`]).
fn rewrite_statement_form(
    mut statement: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::{Token, TokenWithSpan};
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
    let n = significant.len();
    let is = |at: usize, w: &str| {
        significant
            .get(at)
            .and_then(|&i| word(&statement[i]))
            .as_deref()
            == Some(w)
    };
    let tail = |statement: &[TokenWithSpan]| -> Vec<TokenWithSpan> {
        statement
            .iter()
            .skip(significant.last().map_or(0, |&i| i + 1))
            .cloned()
            .collect()
    };
    let quote = |s: &str| s.replace('\'', "''");

    if (is(0, "fetch") || is(0, "move")) && n >= 2 {
        let name = match &statement[significant[n - 1]].token {
            Token::Word(w) => w.value.clone(),
            other => other.to_string(),
        };
        let direction_end = if n >= 3 && (is(n - 2, "from") || is(n - 2, "in")) {
            n - 2
        } else {
            n - 1
        };
        let direction: Vec<String> = significant[1..direction_end]
            .iter()
            .map(|&i| render_tokens(&statement[i..=i]))
            .collect();
        let command = if is(0, "move") { "MOVE" } else { "FETCH" };
        let sql = format!(
            "SELECT {CURSOR_FUNCTION}('{command}', '{}', '{}')",
            quote(&direction.join(" ")),
            quote(&name)
        );
        if let Some(mut tokens) = snippet_tokens(&sql) {
            tokens.extend(tail(&statement));
            return tokens;
        }
        return statement;
    }

    // `ALTER <kind> [IF EXISTS] name SET SCHEMA schema`.
    if is(0, "alter") && n >= 5 && is(n - 3, "set") && is(n - 2, "schema") {
        let (kind, mut at) = if is(1, "materialized") && is(2, "view") {
            ("MATERIALIZED VIEW", 3)
        } else if is(1, "table") {
            ("TABLE", 2)
        } else if is(1, "view") {
            ("VIEW", 2)
        } else if is(1, "sequence") {
            ("SEQUENCE", 2)
        } else if is(1, "type") {
            ("TYPE", 2)
        } else if is(1, "domain") {
            ("DOMAIN", 2)
        } else {
            ("", 0)
        };
        if !kind.is_empty() {
            let if_exists = is(at, "if") && is(at + 1, "exists");
            if if_exists {
                at += 2;
            }
            let name = render_tokens(&statement[significant[at]..=significant[n - 4]]);
            let schema = match &statement[significant[n - 1]].token {
                Token::Word(w) => w.value.clone(),
                other => other.to_string(),
            };
            let sql = format!(
                "SELECT {SET_SCHEMA_FUNCTION}('{kind}', {if_exists}, '{}', '{}')",
                quote(name.trim()),
                quote(&schema)
            );
            if let Some(mut tokens) = snippet_tokens(&sql) {
                tokens.extend(tail(&statement));
                return tokens;
            }
            return statement;
        }
    }

    // `ALTER TYPE <name> ADD | DROP | RENAME ATTRIBUTE ...`.
    if is(0, "alter") && is(1, "type") && n >= 5 {
        let operation = (3..n).find(|&k| is(k, "attribute")).and_then(|k| {
            match (is(k - 1, "add"), is(k - 1, "drop"), is(k - 1, "rename")) {
                (true, _, _) => Some(("add", k)),
                (_, true, _) => Some(("drop", k)),
                (_, _, true) => Some(("rename", k)),
                _ => None,
            }
        });
        if let Some((operation, at)) = operation {
            let name = render_tokens(&statement[significant[2]..=significant[at - 2]]);
            let word_at = |k: usize| render_tokens(&statement[significant[k]..=significant[k]]);
            let (first, second) = match operation {
                // `ADD ATTRIBUTE <name> <type>`.
                "add" if at + 2 < n => (
                    word_at(at + 1),
                    render_tokens(&statement[significant[at + 2]..=significant[n - 1]])
                        .trim()
                        .to_string(),
                ),
                // `DROP ATTRIBUTE [IF EXISTS] <name>`.
                "drop" => {
                    let if_exists = is(at + 1, "if") && is(at + 2, "exists");
                    (
                        word_at(at + if if_exists { 3 } else { 1 }),
                        if_exists.to_string(),
                    )
                }
                // `RENAME ATTRIBUTE <from> TO <to>`.
                "rename" if n >= 2 && is(n - 2, "to") => (word_at(at + 1), word_at(n - 1)),
                _ => return statement,
            };
            let sql = format!(
                "SELECT {TYPE_FUNCTION}('{operation}', '{}', '{}', '{}')",
                quote(name.trim()),
                quote(&first),
                quote(&second)
            );
            if let Some(mut tokens) = snippet_tokens(&sql) {
                tokens.extend(tail(&statement));
                return tokens;
            }
            return statement;
        }
    }

    // `CREATE SCHEMA ... CREATE TABLE ... GRANT ...`: the schema, then each
    // statement creating something in it.
    if is(0, "create") && is(1, "schema") {
        let mut depth = 0i32;
        let mut starts = vec![0];
        for k in 2..n {
            match statement[significant[k]].token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            if depth == 0 && (is(k, "create") || is(k, "grant")) {
                starts.push(k);
            }
        }
        if starts.len() > 1 {
            let end = significant.last().map_or(statement.len(), |&i| i + 1);
            let parts: Vec<String> = starts
                .iter()
                .enumerate()
                .map(|(c, &k)| {
                    let from = significant[k];
                    let to = starts.get(c + 1).map_or(end, |&next| significant[next]);
                    format!("'{}'", quote(render_tokens(&statement[from..to]).trim()))
                })
                .collect();
            let sql = format!("SELECT {CREATE_SCHEMA_FUNCTION}({})", parts.join(", "));
            if let Some(mut tokens) = snippet_tokens(&sql) {
                tokens.extend(tail(&statement));
                return tokens;
            }
            return statement;
        }
    }

    if (is(0, "alter") || is(0, "drop")) && is(1, "domain") {
        let (Some(&first), Some(&last)) = (significant.first(), significant.last()) else {
            return statement;
        };
        let text = render_tokens(&statement[first..=last]);
        let sql = format!("SELECT {DOMAIN_FUNCTION}('{}')", quote(&text));
        if let Some(mut tokens) = snippet_tokens(&sql) {
            tokens.extend(tail(&statement));
            return tokens;
        }
        return statement;
    }

    if is(0, "create") && is(1, "domain") {
        // The parser takes a domain's clauses in one order (`COLLATE`,
        // `DEFAULT`, then its checks) and no `NOT NULL`: the clauses are
        // put in that order, `NOT NULL` becomes a check the planner knows,
        // and `NULL` goes.
        let starts_clause = |k: usize| {
            is(k, "collate")
                || is(k, "default")
                || is(k, "constraint")
                || is(k, "check")
                || is(k, "null")
                || (is(k, "not") && is(k + 1, "null"))
        };
        let mut depth = 0i32;
        let mut starts = Vec::new();
        let mut k = 3;
        while k < n {
            match statement[significant[k]].token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            if depth == 0 && starts_clause(k) {
                starts.push(k);
                // A default's first token, and a named constraint's name and
                // kind, belong to the clause.
                k += if is(k, "default") {
                    2
                } else if is(k, "constraint") {
                    3
                } else {
                    1
                };
                continue;
            }
            k += 1;
        }
        let Some(&first) = starts.first() else {
            return statement;
        };
        let rest = tail(&statement);
        let end = significant.last().map_or(statement.len(), |&i| i + 1);
        let clause = |c: usize| {
            let from = significant[starts[c]];
            let to = starts.get(c + 1).map_or(end, |&next| significant[next]);
            (starts[c], statement[from..to].to_vec())
        };
        let (mut collate, mut default, mut checks) = (Vec::new(), Vec::new(), Vec::new());
        let mut not_null = false;
        for c in 0..starts.len() {
            let (k, tokens) = clause(c);
            let kind = if is(k, "constraint") { k + 2 } else { k };
            if is(kind, "collate") {
                collate.extend(tokens);
            } else if is(kind, "default") {
                default.extend(tokens);
            } else if is(kind, "not") {
                not_null = true;
            } else if is(kind, "check") {
                checks.extend(tokens);
            }
        }
        let mut out: Vec<TokenWithSpan> = statement[..significant[first]].to_vec();
        out.extend(collate);
        out.extend(default);
        out.extend(checks);
        if not_null && let Some(check) = snippet_tokens(&format!(" CHECK ({DOMAIN_NOT_NULL})")) {
            out.extend(check);
        }
        out.extend(rest);
        return out;
    }

    let creates_view = is(0, "create")
        && (1..5).any(|at| is(at, "view"))
        && !(1..5).any(|at| is(at, "materialized"));
    if creates_view
        && n >= 4
        && is(n - 1, "option")
        && is(n - 2, "check")
        && (is(n - 3, "with") || (n >= 5 && is(n - 4, "with")))
    {
        let (option, clause) = match significant.get(n - 3).and_then(|&i| word(&statement[i])) {
            Some(w) if w == "local" => ("local", n - 4),
            Some(w) if w == "cascaded" => ("cascaded", n - 4),
            _ => ("cascaded", n - 3),
        };
        let rest = tail(&statement);
        statement.truncate(significant[clause]);
        // Before the view's `AS`, outside its column list.
        let mut depth = 0i32;
        let as_at = statement.iter().position(|t| {
            match &t.token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            depth == 0 && word(t).as_deref() == Some("as")
        });
        if let (Some(as_at), Some(option)) = (
            as_at,
            snippet_tokens(&format!("WITH (check_option = '{option}') ")),
        ) {
            statement.splice(as_at..as_at, option);
        }
        statement.extend(rest);
        return statement;
    }

    // `ON CONFLICT (expressions)`: a target with a parenthesized part.
    if let Some(k) = (0..n.saturating_sub(2)).find(|&k| {
        is(k, "on") && is(k + 1, "conflict") && statement[significant[k + 2]].token == Token::LParen
    }) {
        let open = significant[k + 2];
        let mut depth = 0i32;
        let mut nested = false;
        let close = (open..statement.len()).find(|&i| {
            match statement[i].token {
                Token::LParen => {
                    depth += 1;
                    nested |= depth > 1;
                }
                Token::RParen => depth -= 1,
                _ => {}
            }
            depth == 0
        });
        if let (Some(close), true) = (close, nested) {
            let text = render_tokens(&statement[open + 1..close]);
            let name = format!("{CONFLICT_EXPRESSIONS}{}", text.trim()).replace('"', "\"\"");
            if let Some(target) = snippet_tokens(&format!("ON CONSTRAINT \"{name}\"")) {
                statement.splice(open..=close, target);
            }
        }
    }
    statement
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

    // Maintenance statements, by their command tag.
    let utility = if is(0, "checkpoint") {
        Some("CHECKPOINT")
    } else if is(0, "reindex") {
        Some("REINDEX")
    } else if is(0, "cluster") {
        Some("CLUSTER")
    } else if is(0, "load") {
        Some("LOAD")
    } else if is(0, "vacuum") && n > 1 {
        Some("VACUUM")
    } else if is(0, "prepare") && is(1, "transaction") {
        Some("PREPARE TRANSACTION")
    } else {
        None
    };
    if let Some(tag) = utility {
        let sql = format!("SELECT {UTILITY_FUNCTION}('{tag}')");
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
    fn missing_query_syntax_is_rewritten() {
        let one = |sql: &str| parse_sql(sql).unwrap().remove(0).to_string();
        assert_eq!(one("TABLE t ORDER BY a"), "SELECT * FROM t ORDER BY a");
        assert_eq!(
            one("SELECT 1 UNION TABLE t"),
            "SELECT 1 UNION SELECT * FROM t"
        );
        assert_eq!(
            one("SELECT a BETWEEN SYMMETRIC 5 + 1 AND 2 FROM t"),
            "SELECT a BETWEEN pg_catalog.__symmetric__(5 + 1) AND 2 FROM t"
        );
        assert_eq!(
            one("SELECT a BETWEEN ASYMMETRIC 1 AND 2"),
            "SELECT a BETWEEN 1 AND 2"
        );
        assert_eq!(
            one("SELECT sum(a) OVER (ORDER BY a ROWS UNBOUNDED PRECEDING EXCLUDE TIES) FROM t"),
            "SELECT sum(a) OVER (PARTITION BY pg_catalog.__exclude__('ties') ORDER BY a ROWS UNBOUNDED PRECEDING) FROM t"
        );
        assert_eq!(
            one("SELECT sum(a) OVER (PARTITION BY b ROWS CURRENT ROW EXCLUDE CURRENT ROW) FROM t"),
            "SELECT sum(a) OVER (PARTITION BY pg_catalog.__exclude__('current row'), b ROWS CURRENT ROW) FROM t"
        );
        // Tables keep their own TABLE keyword.
        assert_eq!(one("CREATE TABLE t (a INT)"), "CREATE TABLE t (a INT)");
    }

    #[test]
    fn maintenance_locking_and_overriding_forms_are_rewritten() {
        let one = |sql: &str| parse_sql(sql).unwrap().remove(0).to_string();
        assert_eq!(
            one("CHECKPOINT"),
            format!("SELECT {UTILITY_FUNCTION}('CHECKPOINT')")
        );
        assert_eq!(
            one("VACUUM (VERBOSE, ANALYZE) t"),
            format!("SELECT {UTILITY_FUNCTION}('VACUUM')")
        );
        assert_eq!(
            one("REINDEX TABLE t"),
            format!("SELECT {UTILITY_FUNCTION}('REINDEX')")
        );
        assert_eq!(
            one("SELECT a FROM t FOR NO KEY UPDATE"),
            "SELECT a FROM t FOR UPDATE"
        );
        assert_eq!(
            one("SELECT a FROM t FOR KEY SHARE"),
            "SELECT a FROM t FOR SHARE"
        );
        assert_eq!(
            one("INSERT INTO s.t (a) OVERRIDING SYSTEM VALUE VALUES (1)"),
            format!("INSERT INTO s.t AS {OVERRIDING_SYSTEM} (a) VALUES (1)")
        );
        assert_eq!(
            one("INSERT INTO t AS x OVERRIDING USER VALUE VALUES (1)"),
            format!("INSERT INTO t AS x{OVERRIDING_USER} VALUES (1)")
        );
    }

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
    fn cursor_domain_view_and_conflict_forms_parse() {
        let one = |sql: &str| parse_sql(sql).unwrap()[0].to_string();
        assert_eq!(
            one("FETCH RELATIVE -2 FROM c"),
            format!("SELECT {CURSOR_FUNCTION}('FETCH', 'relative - 2', 'c')")
        );
        assert_eq!(
            one("MOVE c"),
            format!("SELECT {CURSOR_FUNCTION}('MOVE', '', 'c')")
        );
        let domain = format!(
            "{:?}",
            parse_sql("ALTER DOMAIN d ADD CHECK (VALUE <> 'it''s')").unwrap()
        );
        assert!(
            domain.contains(r#"SingleQuotedString("alter domain d add check (value <> 'it''s')")"#),
            "{domain}"
        );
        assert!(
            one("CREATE DOMAIN d AS text NULL CONSTRAINT c CHECK (VALUE <> '') NOT NULL")
                .contains(&format!("CHECK ({DOMAIN_NOT_NULL})"))
        );
        assert_eq!(
            one("CREATE DOMAIN d AS varchar(3) NOT NULL CHECK (VALUE <> 'x') DEFAULT 'abc'"),
            format!(
                "CREATE DOMAIN d AS VARCHAR(3) DEFAULT 'abc' CHECK (value <> 'x') CHECK ({DOMAIN_NOT_NULL})"
            )
        );
        assert!(
            one("CREATE VIEW v (a) AS SELECT 1 AS a WITH LOCAL CHECK OPTION")
                .contains("check_option = 'local'")
        );
        assert!(
            one("INSERT INTO t VALUES (1) ON CONFLICT (lower(e)) DO NOTHING")
                .contains(&format!("ON CONSTRAINT \"{CONFLICT_EXPRESSIONS}lower(e)\""))
        );
    }

    #[test]
    fn set_schema_and_schema_elements_parse() {
        let one = |sql: &str| parse_sql(sql).unwrap()[0].to_string();
        assert_eq!(
            one("ALTER MATERIALIZED VIEW IF EXISTS s.\"Mv\" SET SCHEMA t"),
            format!("SELECT {SET_SCHEMA_FUNCTION}('MATERIALIZED VIEW', true, 's.\"Mv\"', 't')")
        );
        assert_eq!(
            one("ALTER TABLE t SET SCHEMA s"),
            format!("SELECT {SET_SCHEMA_FUNCTION}('TABLE', false, 't', 's')")
        );
        assert_eq!(
            one("CREATE SCHEMA s CREATE TABLE t (a int) CREATE VIEW v AS SELECT * FROM t"),
            format!(
                "SELECT {CREATE_SCHEMA_FUNCTION}('create schema s', 'create table t (a int)', 'create view v as select * from t')"
            )
        );
        // A plain one is left to the parser.
        assert!(one("CREATE SCHEMA s AUTHORIZATION nodus").starts_with("CREATE SCHEMA"));
    }

    #[test]
    fn record_star_and_type_attributes_parse() {
        let one = |sql: &str| parse_sql(sql).unwrap()[0].to_string();
        assert_eq!(
            one("SELECT (p).* FROM ct"),
            format!("SELECT {EXPAND_RECORD_FUNCTION}(p) FROM ct")
        );
        assert_eq!(
            one("SELECT ((p)) FROM ct"),
            "SELECT ((p)) FROM ct".to_string()
        );
        assert_eq!(
            one("ALTER TYPE pair ADD ATTRIBUTE c int"),
            format!("SELECT {TYPE_FUNCTION}('add', 'pair', 'c', 'int')")
        );
        assert_eq!(
            one("ALTER TYPE pair DROP ATTRIBUTE IF EXISTS c"),
            format!("SELECT {TYPE_FUNCTION}('drop', 'pair', 'c', 'true')")
        );
        assert_eq!(
            one("ALTER TYPE s.pair RENAME ATTRIBUTE c TO d"),
            format!("SELECT {TYPE_FUNCTION}('rename', 's.pair', 'c', 'd')")
        );
        // A plain enum operation is left to the parser.
        assert!(one("ALTER TYPE mood ADD VALUE 'x'").starts_with("ALTER TYPE"));
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
