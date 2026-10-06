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
        rewrite_record_star(rewrite_json_table(rewrite_sql_json(
            rewrite_json_constructors(rewrite_xmlexists(rewrite_xmltable(
                rewrite_xml_constructors(rewrite_xml_syntax(rewrite_query_syntax(rewrite_only(
                    tokens,
                )))),
            ))),
        ))),
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

/// The functions the SQL/XML constructors are written as:
/// `XMLPI(NAME t [, v])` — `pg_catalog.__xmlpi__('t' [, v])`;
/// `XMLROOT(x, VERSION v, STANDALONE s)` —
/// `pg_catalog.__xmlroot__(x, v, s)` (s: 0 yes, 1 no, 2 no value, 3 omitted);
/// `XMLELEMENT(NAME t, ...)` —
/// `pg_catalog.__xmlelement__('t', attrs, content...)`;
/// `XMLFOREST(...)` — `pg_catalog.__xmlforest__(v, 'n', b, ...)`;
/// `XMLATTRIBUTES(...)` — `pg_catalog.__xmlattributes__(v, 'n', b, ...)`.
pub const XMLPI_MARKER: &str = "__xmlpi__";
pub const XMLROOT_MARKER: &str = "__xmlroot__";
pub const XMLELEMENT_MARKER: &str = "__xmlelement__";
pub const XMLFOREST_MARKER: &str = "__xmlforest__";
pub const XMLATTRIBUTES_MARKER: &str = "__xmlattributes__";

/// The SQL/XML constructors, which the parser cannot read: `XMLPI`,
/// `XMLROOT`, `XMLELEMENT` (with `XMLATTRIBUTES`), and `XMLFOREST`. An
/// argument name that is not an `AS` label must be a column reference, as
/// PostgreSQL requires; otherwise the name is NULL and the planner reports
/// "unnamed XML attribute value must be a column reference".
fn rewrite_xml_constructors(
    mut tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::tokenizer::{Token, TokenWithSpan};
    let word = |t: &TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let identifier = |t: &TokenWithSpan| match &t.token {
        Token::Word(w) => Some(w.value.clone()),
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
    // The ranges of the top-level `,`-separated items of a token range.
    let split = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut start = None;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                Token::Comma if depth == 0 => {
                    items.push((start.unwrap_or(from), i));
                    start = None;
                    continue;
                }
                _ => {}
            }
            if start.is_none() && !matches!(token.token, Token::Whitespace(_)) {
                start = Some(i);
            }
        }
        if let Some(start) = start {
            items.push((start, to));
        }
        items
    };
    // The last top-level `AS` of an item, and the name the item carries.
    let labelled = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut depth = 0i32;
        let mut as_at = None;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ => {}
            }
            if depth == 0 && word(token).as_deref() == Some("as") {
                as_at = Some(i);
            }
        }
        if let Some(as_at) = as_at {
            let label = significant(tokens, as_at + 1)?;
            return Some((from, as_at, Some(identifier(&tokens[label])?), false));
        }
        // Without a label the item must be a column reference; its name is
        // the last identifier, and it is fully escaped.
        let mut last = None;
        for token in tokens.iter().take(to).skip(from) {
            match &token.token {
                Token::Word(w) => last = Some(w.value.clone()),
                Token::Period | Token::Whitespace(_) => {}
                _ => return None,
            }
        }
        Some((from, to, last, true))
    };
    let escape = |text: &str| text.replace('\'', "''");
    // `XMLPI(NAME target [, value])`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("xmlpi")
            && let Some(open) = significant(&tokens, i + 1)
            && tokens[open].token == Token::LParen
            && let Some(close) = matching_paren(&tokens, open)
            && let Some(name_at) = significant(&tokens, open + 1)
            && word(&tokens[name_at]).as_deref() == Some("name")
            && let Some(label_at) = significant(&tokens, name_at + 1)
            && let Some(label) = identifier(&tokens[label_at])
        {
            let value = significant(&tokens, label_at + 1)
                .filter(|&k| tokens[k].token == Token::Comma)
                .map(|comma| render_tokens(&tokens[comma + 1..close]));
            let replacement = match value {
                Some(value) => {
                    format!("pg_catalog.{XMLPI_MARKER}('{}', {value})", escape(&label))
                }
                None => format!("pg_catalog.{XMLPI_MARKER}('{}')", escape(&label)),
            };
            if let Some(snippet) = snippet_tokens(&replacement) {
                let len = snippet.len();
                tokens.splice(i..=close, snippet);
                i += len;
                continue;
            }
        }
        i += 1;
    }
    // `XMLROOT(x, VERSION v [, STANDALONE s])`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("xmlroot")
            && let Some(open) = significant(&tokens, i + 1)
            && tokens[open].token == Token::LParen
            && let Some(close) = matching_paren(&tokens, open)
        {
            let items = split(&tokens, open + 1, close);
            if items.len() == 2 || items.len() == 3 {
                let value = render_tokens(&tokens[items[0].0..items[0].1]);
                let (vfrom, vto) = items[1];
                let version = if word(&tokens[vfrom]).as_deref() == Some("version")
                    && significant(&tokens, vfrom + 1).is_some_and(|no| {
                        word(&tokens[no]).as_deref() == Some("no")
                            && significant(&tokens, no + 1).is_some_and(|value| {
                                word(&tokens[value]).as_deref() == Some("value")
                            })
                    }) {
                    "NULL".to_string()
                } else {
                    let from = significant(&tokens, vfrom + 1).unwrap_or(vto);
                    render_tokens(&tokens[from..vto])
                };
                let standalone = items
                    .get(2)
                    .and_then(|(sfrom, _)| {
                        significant(&tokens, sfrom + 1)
                            .and_then(|k| word(&tokens[k]).map(|w| (k, w)))
                    })
                    .and_then(|(k, w)| match w.as_str() {
                        "yes" => Some(0),
                        "no" => Some(
                            if significant(&tokens, k + 1)
                                .is_some_and(|v| word(&tokens[v]).as_deref() == Some("value"))
                            {
                                2
                            } else {
                                1
                            },
                        ),
                        _ => None,
                    })
                    .unwrap_or(3);
                let replacement =
                    format!("pg_catalog.{XMLROOT_MARKER}({value}, {version}, {standalone})");
                if let Some(snippet) = snippet_tokens(&replacement) {
                    let len = snippet.len();
                    tokens.splice(i..=close, snippet);
                    i += len;
                    continue;
                }
            }
        }
        i += 1;
    }
    // `XMLELEMENT(NAME name [, XMLATTRIBUTES(...)] [, content...])`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("xmlelement")
            && let Some(open) = significant(&tokens, i + 1)
            && tokens[open].token == Token::LParen
            && let Some(close) = matching_paren(&tokens, open)
            && let Some(name_at) = significant(&tokens, open + 1)
            && word(&tokens[name_at]).as_deref() == Some("name")
            && let Some(label_at) = significant(&tokens, name_at + 1)
            && let Some(label) = identifier(&tokens[label_at])
        {
            let mut attrs = "NULL".to_string();
            let mut content = Vec::new();
            let mut ok = true;
            let rest = significant(&tokens, label_at + 1)
                .filter(|&k| tokens[k].token == Token::Comma)
                .map(|comma| comma + 1);
            if let Some(rest) = rest {
                for (from, to) in split(&tokens, rest, close) {
                    if word(&tokens[from]).as_deref() == Some("xmlattributes")
                        && let Some(aopen) = significant(&tokens, from + 1)
                        && tokens[aopen].token == Token::LParen
                        && let Some(aclose) = matching_paren(&tokens, aopen)
                    {
                        let mut parts = vec![format!(
                            "pg_catalog.{}(",
                            XMLATTRIBUTES_MARKER.to_ascii_lowercase()
                        )];
                        for (index, (efrom, eto)) in
                            split(&tokens, aopen + 1, aclose).into_iter().enumerate()
                        {
                            let Some((efrom, eend, name, fully)) = labelled(&tokens, efrom, eto)
                            else {
                                ok = false;
                                break;
                            };
                            let value = render_tokens(&tokens[efrom..eend]);
                            let name = match name {
                                Some(name) => format!("'{}'", escape(&name)),
                                None => "NULL".to_string(),
                            };
                            if index > 0 {
                                parts.push(", ".to_string());
                            }
                            parts.push(format!("{value}, {name}, {fully}"));
                        }
                        parts.push(")".to_string());
                        attrs = parts.concat();
                    } else {
                        content.push(render_tokens(&tokens[from..to]));
                    }
                }
            }
            if ok {
                let mut replacement = format!(
                    "pg_catalog.{XMLELEMENT_MARKER}('{}', {attrs}",
                    escape(&label)
                );
                for item in &content {
                    replacement.push_str(", ");
                    replacement.push_str(item);
                }
                replacement.push(')');
                if let Some(snippet) = snippet_tokens(&replacement) {
                    let len = snippet.len();
                    tokens.splice(i..=close, snippet);
                    i += len;
                    continue;
                }
            }
        }
        i += 1;
    }
    // `XMLFOREST(value [AS name], ...)`.
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() == Some("xmlforest")
            && let Some(open) = significant(&tokens, i + 1)
            && tokens[open].token == Token::LParen
            && let Some(close) = matching_paren(&tokens, open)
        {
            let mut parts = vec![format!(
                "pg_catalog.{}(",
                XMLFOREST_MARKER.to_ascii_lowercase()
            )];
            let mut ok = true;
            for (index, (from, to)) in split(&tokens, open + 1, close).into_iter().enumerate() {
                let Some((from, eend, name, fully)) = labelled(&tokens, from, to) else {
                    ok = false;
                    break;
                };
                let value = render_tokens(&tokens[from..eend]);
                let name = match name {
                    Some(name) => format!("'{}'", escape(&name)),
                    None => "NULL".to_string(),
                };
                if index > 0 {
                    parts.push(", ".to_string());
                }
                parts.push(format!("{value}, {name}, {fully}"));
            }
            parts.push(")".to_string());
            if ok {
                let replacement = parts.concat();
                if let Some(snippet) = snippet_tokens(&replacement) {
                    let len = snippet.len();
                    tokens.splice(i..=close, snippet);
                    i += len;
                    continue;
                }
            }
        }
        i += 1;
    }
    // A stray `XMLATTRIBUTES` outside `XMLELEMENT` is left to the parser.
    let _ = significant_back(&tokens, 0);
    tokens
}

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

/// The SQL/JSON query functions, which the parser cannot read with their
/// clauses: `JSON_EXISTS`, `JSON_VALUE`, and `JSON_QUERY`, each rewritten to
/// the marker function the planner knows (see `sqljson.rs`). The shapes are:
/// `__json_exists__(doc, format, path, vars, on_error)`,
/// `__json_value__(doc, format, path, vars, returning, retformat, on_empty,
/// on_empty_default, on_error, on_error_default)`,
/// `__json_query__(doc, format, path, vars, returning, retformat, wrapper,
/// quotes, on_empty, on_empty_default, on_error, on_error_default)`, with
/// `__json_vars__('name', value, ...)` for a `PASSING` clause.
const SQL_JSON_FUNCTIONS: &[&str] = &["json_exists", "json_value", "json_query"];

fn rewrite_sql_json(
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
    // The ranges of the top-level `,`-separated items of a token range.
    let split = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut start = None;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                Token::Comma if depth == 0 => {
                    items.push((start.unwrap_or(from), i));
                    start = None;
                    continue;
                }
                _ => {}
            }
            if start.is_none() && !matches!(token.token, Token::Whitespace(_)) {
                start = Some(i);
            }
        }
        if let Some(start) = start {
            items.push((start, to));
        }
        items
    };
    // The clause keywords of the path item, at its top level: the clauses
    // proper, and the behavior words that begin a `... ON EMPTY|ERROR` one.
    let clause_word = |w: &str| {
        matches!(
            w,
            "passing" | "returning" | "with" | "without" | "keep" | "omit" | "default" | "on"
        )
    };
    let clause_or_behavior = |w: &str| {
        clause_word(w) || matches!(w, "error" | "null" | "true" | "false" | "unknown" | "empty")
    };
    // The JSON value constructors: `JSON(expr [WITH|WITHOUT UNIQUE [KEYS]])`,
    // `JSON_SCALAR(expr)`, and `JSON_SERIALIZE(expr [FORMAT JSON [ENCODING n]]
    // [RETURNING type [FORMAT JSON]])`.
    let mut i = 0;
    while i < tokens.len() {
        let name = word(&tokens[i]).unwrap_or_default();
        if !matches!(name.as_str(), "json" | "json_scalar" | "json_serialize") {
            i += 1;
            continue;
        }
        let Some(open) = significant(&tokens, i + 1) else {
            i += 1;
            continue;
        };
        if tokens[open].token != Token::LParen {
            i += 1;
            continue;
        }
        let Some(close) = matching_paren(&tokens, open) else {
            i += 1;
            continue;
        };
        // A trailing `WITH|WITHOUT UNIQUE [KEYS]` clause, for `JSON`.
        let mut unique = String::new();
        let mut end = close;
        if name == "json" {
            for (at, token) in tokens.iter().enumerate().take(close).skip(open + 1) {
                if matches!(word(token).as_deref(), Some("with" | "without"))
                    && let Some(next) = significant(&tokens, at + 1)
                    && word(&tokens[next]).as_deref() == Some("unique")
                {
                    unique = if word(token).as_deref() == Some("with") {
                        "unique".to_string()
                    } else {
                        "not-unique".to_string()
                    };
                    end = at;
                    break;
                }
            }
        }
        let mut failed = false;
        // A `FORMAT JSON [ENCODING name]` clause, for `JSON_SERIALIZE`.
        let mut format = String::new();
        let mut doc_end = end;
        let mut returning = String::new();
        let mut returning_end = end;
        if name == "json_serialize" {
            // The RETURNING clause, if any, comes last.
            for (at, token) in tokens.iter().enumerate().take(end).skip(open + 1) {
                if word(token).as_deref() == Some("returning") {
                    returning_end = at;
                    break;
                }
            }
            let has_returning = returning_end < end;
            if has_returning {
                let mut type_end = end;
                for (at, token) in tokens.iter().enumerate().take(end).skip(returning_end + 1) {
                    if word(token).as_deref() == Some("format") {
                        // Only `FORMAT JSON` is a returning format; anything
                        // else is the parser's own error.
                        let shape = significant(&tokens, at + 1);
                        if !shape.is_some_and(|s| word(&tokens[s]).as_deref() == Some("json")) {
                            failed = true;
                            break;
                        }
                        type_end = at;
                        break;
                    }
                }
                if failed {
                    i += 1;
                    continue;
                }
                returning = render_tokens(&tokens[returning_end + 1..type_end])
                    .trim()
                    .to_string();
            }
            if has_returning {
                doc_end = returning_end;
            }
            for (at, token) in tokens.iter().enumerate().take(returning_end).skip(open + 1) {
                if word(token).as_deref() == Some("format") {
                    doc_end = at;
                    let rest: String = render_tokens(&tokens[at..returning_end]);
                    format = match rest.split_whitespace().nth(2) {
                        Some(encoding) if rest.contains("encoding") => format!(
                            "json:{}",
                            encoding.trim_matches(|c: char| !c.is_alphanumeric())
                        ),
                        _ => "json".to_string(),
                    };
                    format = format.replace("utf8", "utf-8");
                    break;
                }
            }
        }
        let value = render_tokens(&tokens[open + 1..doc_end]);
        let replacement = match name.as_str() {
            "json" => format!("pg_catalog.__json__({value}, '{unique}')"),
            "json_scalar" => format!("pg_catalog.__json_scalar__({value}, false)"),
            _ => format!("pg_catalog.__json_serialize__({value}, '{format}', '{returning}')"),
        };
        if let Some(snippet) = snippet_tokens(&replacement) {
            let len = snippet.len();
            tokens.splice(i..=close, snippet);
            i += len;
            continue;
        }
        i += 1;
    }
    let mut i = 0;
    while i < tokens.len() {
        let name = word(&tokens[i]).unwrap_or_default();
        if !SQL_JSON_FUNCTIONS.contains(&name.as_str()) {
            i += 1;
            continue;
        }
        let Some(open) = significant(&tokens, i + 1) else {
            i += 1;
            continue;
        };
        if tokens[open].token != Token::LParen {
            i += 1;
            continue;
        }
        let Some(close) = matching_paren(&tokens, open) else {
            i += 1;
            continue;
        };
        let items = split(&tokens, open + 1, close);
        if items.len() < 2 {
            i += 1;
            continue;
        }
        // The document, with its `FORMAT JSON [ENCODING name]` clause.
        let (df, dto) = items[0];
        let mut format = String::new();
        let mut doc_end = dto;
        for (at, token) in tokens.iter().enumerate().take(dto).skip(df) {
            if word(token).as_deref() == Some("format") {
                doc_end = at;
                let rest: String = render_tokens(&tokens[at..dto]);
                let rest = rest.trim().to_ascii_lowercase();
                format = match rest.split_whitespace().nth(2) {
                    Some(encoding) if rest.contains("encoding") => {
                        format!(
                            "json:{}",
                            encoding.trim_matches(|c: char| !c.is_alphanumeric())
                        )
                    }
                    _ => "json".to_string(),
                };
                format = format.replace("utf8", "utf-8");
                break;
            }
        }
        let doc = render_tokens(&tokens[df..doc_end]);
        // The path, which runs to the first clause keyword.
        let (pf, pto) = items[1];
        let mut path_end = pto;
        for (at, token) in tokens.iter().enumerate().take(pto).skip(pf) {
            if let Some(w) = word(token)
                && clause_or_behavior(&w)
            {
                path_end = at;
                break;
            }
        }
        let path = render_tokens(&tokens[pf..path_end]);
        // The clauses, left to right.
        let mut failed = false;
        let mut vars = "NULL".to_string();
        let mut returning = String::new();
        let mut retformat = String::new();
        let mut wrapper = String::new();
        let mut quotes = String::new();
        let mut on_empty = String::new();
        let mut on_empty_default = "NULL".to_string();
        let mut on_error = String::new();
        let mut on_error_default = "NULL".to_string();
        let mut at = path_end;
        // The end of a clause expression: the next clause keyword at this
        // level, or the closing parenthesis.
        let clause_end = |tokens: &[TokenWithSpan], from: usize| {
            let mut depth = 0i32;
            for (at, token) in tokens.iter().enumerate().take(pto).skip(from) {
                match token.token {
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => {}
                }
                if depth == 0
                    && let Some(w) = word(token)
                    && clause_or_behavior(&w)
                {
                    return at;
                }
            }
            pto
        };
        let default_end = |tokens: &[TokenWithSpan], from: usize| {
            let mut depth = 0i32;
            for (at, token) in tokens.iter().enumerate().take(pto).skip(from) {
                match token.token {
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => {}
                }
                if depth == 0 && word(token).as_deref() == Some("on") {
                    return at;
                }
            }
            pto
        };
        while at < pto {
            let Some(key) = significant(&tokens, at) else {
                break;
            };
            match word(&tokens[key]).as_deref() {
                Some("passing") => {
                    let end = clause_end(&tokens, key + 1);
                    let mut parts = vec![format!(
                        "pg_catalog.{}(",
                        crate::SQL_JSON_VARS_MARKER.to_ascii_lowercase()
                    )];
                    for (index, (vf, vto)) in split(&tokens, key + 1, end).into_iter().enumerate() {
                        // `<value> AS <name>`.
                        let mut as_at = None;
                        let mut depth = 0i32;
                        for (j, token) in tokens.iter().enumerate().take(vto).skip(vf) {
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
                            let value = render_tokens(&tokens[vf..as_at]);
                            let _ = &value;
                            let name = render_tokens(&tokens[as_at + 1..vto]);
                            if index > 0 {
                                parts.push(", ".to_string());
                            }
                            let name = name.trim().replace('\'', "''");
                            if name.is_empty() {
                                failed = true;
                                break;
                            }
                            parts.push(format!("'{name}', {value}"));
                        }
                    }
                    parts.push(")".to_string());
                    vars = parts.concat();
                    at = end;
                }
                Some("returning") => {
                    let end = clause_end(&tokens, key + 1);
                    // A trailing `FORMAT JSON` belongs to the returning clause.
                    let mut type_end = end;
                    let mut depth = 0i32;
                    for (j, token) in tokens.iter().enumerate().take(end).skip(key + 1) {
                        match token.token {
                            Token::LParen => depth += 1,
                            Token::RParen => depth -= 1,
                            _ => {}
                        }
                        if depth == 0 && word(token).as_deref() == Some("format") {
                            type_end = j;
                            retformat = "json".to_string();
                            break;
                        }
                    }
                    returning = render_tokens(&tokens[key + 1..type_end]).trim().to_string();
                    at = end;
                }
                Some(kind @ ("with" | "without")) => {
                    // `[WITH|WITHOUT] [CONDITIONAL|UNCONDITIONAL] [ARRAY] WRAPPER`.
                    let mut j = significant(&tokens, key + 1).unwrap_or(pto);
                    let mut conditional = false;
                    if matches!(
                        word(&tokens[j]).as_deref(),
                        Some("conditional" | "unconditional")
                    ) {
                        conditional = word(&tokens[j]).as_deref() == Some("conditional");
                        j = significant(&tokens, j + 1).unwrap_or(pto);
                    }
                    if word(&tokens[j]).as_deref() == Some("array") {
                        j = significant(&tokens, j + 1).unwrap_or(pto);
                    }
                    if word(&tokens[j]).as_deref() == Some("wrapper") {
                        wrapper = if kind == "without" {
                            "without".to_string()
                        } else if conditional {
                            "conditional".to_string()
                        } else {
                            "with".to_string()
                        };
                        at = j + 1;
                    } else {
                        failed = true;
                        break;
                    }
                }
                Some(kind @ ("keep" | "omit")) => {
                    let mut j = significant(&tokens, key + 1).unwrap_or(pto);
                    if word(&tokens[j]).as_deref() == Some("quotes") {
                        j = significant(&tokens, j + 1).unwrap_or(pto);
                        if word(&tokens[j]).as_deref() == Some("on") {
                            j = significant(&tokens, j + 1).unwrap_or(pto);
                            if word(&tokens[j]).as_deref() == Some("scalar") {
                                j = significant(&tokens, j + 1).unwrap_or(pto);
                            }
                            if word(&tokens[j]).as_deref() == Some("string") {
                                j += 1;
                            }
                        }
                        quotes = kind.to_string();
                        at = j;
                    } else {
                        failed = true;
                        break;
                    }
                }
                Some("default") => {
                    let end = default_end(&tokens, key + 1);
                    if end == pto {
                        failed = true;
                        break;
                    }
                    let value = render_tokens(&tokens[key + 1..end]).trim().to_string();
                    let behavior_at = significant(&tokens, end).unwrap_or(pto);
                    let behavior = word(&tokens[behavior_at]).unwrap_or_default();
                    let on = significant(&tokens, behavior_at + 1).unwrap_or(pto);
                    let which = word(&tokens[on]).unwrap_or_default();
                    if which == "empty" {
                        on_empty = "default".to_string();
                        on_empty_default = value;
                    } else {
                        on_error = "default".to_string();
                        on_error_default = value;
                    }
                    let _ = behavior;
                    at = on + 1;
                }
                Some(kind) => {
                    // A behavior type: NULL, ERROR, TRUE, FALSE, UNKNOWN,
                    // EMPTY [ARRAY|OBJECT], followed by `ON EMPTY|ERROR`.
                    let mut j = key;
                    let mut behavior = kind.to_string();
                    if matches!(kind, "empty") {
                        let next = significant(&tokens, j + 1).unwrap_or(pto);
                        match word(&tokens[next]).as_deref() {
                            Some(shape @ ("array" | "object")) => {
                                behavior = format!("empty-{shape}");
                                j = next;
                            }
                            _ => behavior = "empty-array".to_string(),
                        }
                    }
                    let on = significant(&tokens, j + 1).unwrap_or(pto);
                    let which = significant(&tokens, on + 1).unwrap_or(pto);
                    if word(&tokens[on]).as_deref() != Some("on") {
                        failed = true;
                        break;
                    }
                    match word(&tokens[which]).as_deref() {
                        Some("empty") => {
                            on_empty = behavior;
                            at = which + 1;
                        }
                        Some("error") => {
                            on_error = behavior;
                            at = which + 1;
                        }
                        _ => {
                            failed = true;
                            break;
                        }
                    }
                }
                None => break,
            }
        }
        if failed {
            i += 1;
            continue;
        }
        let replacement = match name.as_str() {
            "json_exists" => format!(
                "pg_catalog.__json_exists__({doc}, '{format}', {path}, {vars}, '{on_error}')"
            ),
            "json_value" => format!(
                "pg_catalog.__json_value__({doc}, '{format}', {path}, {vars}, '{returning}', \
                 '{retformat}', '{on_empty}', {on_empty_default}, '{on_error}', \
                 {on_error_default})"
            ),
            _ => format!(
                "pg_catalog.__json_query__({doc}, '{format}', {path}, {vars}, '{returning}', \
                 '{retformat}', '{wrapper}', '{quotes}', '{on_empty}', {on_empty_default}, \
                 '{on_error}', {on_error_default})"
            ),
        };
        if let Some(snippet) = snippet_tokens(&replacement) {
            let len = snippet.len();
            tokens.splice(i..=close, snippet);
            i += len;
            continue;
        }
        i += 1;
    }
    tokens
}

/// Whether a word starts an `XMLTABLE` column option.
fn xmltable_option_word(word: &str) -> bool {
    matches!(word, "path" | "default" | "not" | "null")
}

/// One `XMLNAMESPACES` item: `'uri' AS prefix` or `DEFAULT 'uri'`, as the
/// prefix (lowercased when it was not quoted) and the URI expression.
fn xmltable_namespace(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
    from: usize,
    to: usize,
) -> Option<(Option<String>, String)> {
    use sqlparser::tokenizer::Token;
    let word = |t: &sqlparser::tokenizer::TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant =
        |from: usize| (from..to).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)));
    let at = significant(from)?;
    if word(&tokens[at]).as_deref() == Some("default") {
        let uri_from = significant(at + 1)?;
        let uri = render_tokens(&tokens[uri_from..to]);
        if uri.trim().is_empty() {
            return None;
        }
        return Some((None, uri));
    }
    // The top-level `AS`: the prefix follows it, the URI is before it.
    let mut as_at = None;
    let mut depth = 0i32;
    for (index, token) in tokens.iter().enumerate().take(to).skip(at) {
        match token.token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => depth -= 1,
            _ => {}
        }
        if depth == 0 && word(token).as_deref() == Some("as") {
            as_at = Some(index);
            break;
        }
    }
    let as_at = as_at?;
    let uri = render_tokens(&tokens[at..as_at]);
    if uri.trim().is_empty() {
        return None;
    }
    let prefix_at = significant(as_at + 1)?;
    let Token::Word(prefix) = &tokens[prefix_at].token else {
        return None;
    };
    if significant(prefix_at + 1).is_some_and(|at| at < to) {
        return None;
    }
    let prefix = if prefix.quote_style.is_some() {
        prefix.value.clone()
    } else {
        prefix.value.to_lowercase()
    };
    Some((Some(prefix), uri))
}

/// The expression of an `XMLTABLE` column option, as the SQL it was written
/// as: one `b_expr` up to the next option keyword. Returns it with the
/// index it ends at, or `None` when there is none.
fn xmltable_option_expression(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
    from: usize,
    to: usize,
) -> Option<(String, usize)> {
    use sqlparser::tokenizer::Token;
    let word = |t: &sqlparser::tokenizer::TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant =
        |from: usize| (from..to).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)));
    let start = significant(from)?;
    // A leading `NULL` is an expression of its own, so that a `default
    // null` and the option after it both read right.
    let end = if word(&tokens[start]).as_deref() == Some("null") {
        start + 1
    } else {
        let mut at = start;
        let mut depth = 0i32;
        while at < to {
            match tokens[at].token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                _ => {}
            }
            if depth == 0
                && at > start
                && xmltable_option_word(&word(&tokens[at]).unwrap_or_default())
            {
                break;
            }
            at += 1;
        }
        at
    };
    let text = render_tokens(&tokens[start..end]);
    if text.trim().is_empty() {
        return None;
    }
    Some((text, end))
}

/// The columns of an `XMLTABLE` call, between `from` and `to`, as the
/// marker calls the table-function planner reads. `None` when a column is
/// not one PostgreSQL would take, leaving the call to the parser's error;
/// a clause PostgreSQL's grammar refuses when it reads the column is left
/// in `refuse`, for the planner to raise before any row.
fn xmltable_columns(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
    from: usize,
    to: usize,
    refuse: &mut String,
) -> Option<String> {
    use sqlparser::tokenizer::Token;
    let word = |t: &sqlparser::tokenizer::TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant =
        |from: usize| (from..to).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)));
    let set_refuse = |refuse: &mut String, message: String| {
        if refuse.is_empty() {
            *refuse = message;
        }
    };
    let mut parts = Vec::new();
    for (cf, cto) in split_top_level(tokens, from, to) {
        let mut at = significant(cf)?;
        let Token::Word(name) = &tokens[at].token else {
            return None;
        };
        let column_name = if name.quote_style.is_some() {
            name.value.clone()
        } else {
            name.value.to_lowercase()
        };
        at = significant(at + 1)?;
        // `name FOR ORDINALITY`.
        if word(&tokens[at]).as_deref() == Some("for") {
            at = significant(at + 1)?;
            if word(&tokens[at]).as_deref() != Some("ordinality") {
                return None;
            }
            if significant(at + 1).is_some_and(|at| at < cto) {
                return None;
            }
            parts.push(format!(
                "pg_catalog.__xt_col_ordinality__('{}')",
                column_name.replace('\'', "''")
            ));
            continue;
        }
        // The column's type runs to the first option keyword.
        let type_from = at;
        while at < cto && !xmltable_option_word(&word(&tokens[at]).unwrap_or_default()) {
            at += 1;
        }
        let column_type = render_tokens(&tokens[type_from..at]).trim().to_string();
        if column_type.is_empty() {
            return None;
        }
        // A column cannot be `SETOF`.
        if column_type
            .split_whitespace()
            .next()
            .is_some_and(|word| word.eq_ignore_ascii_case("setof"))
        {
            set_refuse(
                refuse,
                format!("column \"{column_name}\" cannot be declared SETOF"),
            );
        }
        // `[PATH expr] [DEFAULT expr] [NOT NULL | NULL]`, any order.
        let mut path: Option<String> = None;
        let mut default: Option<String> = None;
        let mut not_null = false;
        let mut nullability_seen = false;
        while let Some(key) = significant(at).filter(|&at| at < cto) {
            let at_next;
            match word(&tokens[key]).as_deref() {
                Some("path") => {
                    let (expr, next) = xmltable_option_expression(tokens, key + 1, cto)?;
                    if path.is_some() {
                        set_refuse(
                            refuse,
                            "only one PATH value per column is allowed".to_string(),
                        );
                    }
                    path = Some(expr);
                    at_next = next;
                }
                Some("default") => {
                    let (expr, next) = xmltable_option_expression(tokens, key + 1, cto)?;
                    if default.is_some() {
                        set_refuse(refuse, "only one DEFAULT value is allowed".to_string());
                    }
                    default = Some(expr);
                    at_next = next;
                }
                Some("not") => {
                    let null_at = significant(key + 1).filter(|&at| at < cto)?;
                    if word(&tokens[null_at]).as_deref() != Some("null") {
                        return None;
                    }
                    if nullability_seen {
                        set_refuse(
                            refuse,
                            format!(
                                "conflicting or redundant NULL / NOT NULL declarations \
                                 for column \"{column_name}\""
                            ),
                        );
                    }
                    not_null = true;
                    nullability_seen = true;
                    at_next = null_at + 1;
                }
                Some("null") => {
                    if nullability_seen {
                        set_refuse(
                            refuse,
                            format!(
                                "conflicting or redundant NULL / NOT NULL declarations \
                                 for column \"{column_name}\""
                            ),
                        );
                    }
                    not_null = false;
                    nullability_seen = true;
                    at_next = key + 1;
                }
                Some(option) => {
                    // An unknown option, `name value`, which PostgreSQL
                    // refuses as it reads the column.
                    let (_, next) = xmltable_option_expression(tokens, key + 1, cto)?;
                    set_refuse(refuse, format!("unrecognized column option \"{option}\""));
                    at_next = next;
                }
                None => return None,
            }
            at = at_next;
        }
        let type_literal = column_type.replace('\'', "''");
        let path = path.unwrap_or_else(|| "NULL".to_string());
        let default = default.unwrap_or_else(|| "NULL".to_string());
        parts.push(format!(
            "pg_catalog.__xt_col__('{}', '{type_literal}', {path}, {default}, {not_null})",
            column_name.replace('\'', "''")
        ));
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join(", "))
}

/// The SQL/XML `XMLTABLE(...)` table function, which the parser cannot read
/// with its `COLUMNS` clause, rewritten to the marker function the
/// table-function planner knows (see `xmltable.rs`):
/// `pg_catalog.__xml_table__(document, path, refuse, namespace..., column...)`.
/// Each `XMLNAMESPACES` item becomes
/// `pg_catalog.__xt_ns__('prefix' | NULL, uri)`, and each column
/// `pg_catalog.__xt_col_ordinality__('name')` or
/// `pg_catalog.__xt_col__('name', 'type', path, default, not-null)`. The
/// `refuse` string carries a clause PostgreSQL rejects when its grammar or
/// parse analysis reads the call, so the error is raised when the table is
/// evaluated.
fn rewrite_xmltable(
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
    // The top-level word `expected` of a token range.
    let top_level_word = |tokens: &[TokenWithSpan], from: usize, to: usize, expected: &str| {
        let mut depth = 0i32;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                _ => {}
            }
            if depth == 0 && word(token).as_deref() == Some(expected) {
                return Some(i);
            }
        }
        None
    };
    // `by {ref|value}`, as the index its keyword starts at.
    let by_kind = |tokens: &[TokenWithSpan], at: usize, limit: usize| -> Option<usize> {
        let by = significant(tokens, at).filter(|&k| k < limit)?;
        if word(&tokens[by]).as_deref() != Some("by") {
            return None;
        }
        let kind = significant(tokens, by + 1).filter(|&k| k < limit)?;
        matches!(word(&tokens[kind]).as_deref(), Some("ref" | "value")).then_some(by)
    };
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() != Some("xmltable") {
            i += 1;
            continue;
        }
        let Some(open) = significant(&tokens, i + 1) else {
            i += 1;
            continue;
        };
        if tokens[open].token != Token::LParen {
            i += 1;
            continue;
        }
        let Some(close) = matching_paren(&tokens, open) else {
            i += 1;
            continue;
        };
        let mut at = open + 1;
        let mut refuse = String::new();
        // The optional `XMLNAMESPACES('uri' AS prefix, ...),` lead.
        let mut namespaces: Vec<(Option<String>, String)> = Vec::new();
        if let Some(first) = significant(&tokens, at).filter(|&k| k < close)
            && word(&tokens[first]).as_deref() == Some("xmlnamespaces")
        {
            let Some(nopen) = significant(&tokens, first + 1).filter(|&k| k < close) else {
                i += 1;
                continue;
            };
            if tokens[nopen].token != Token::LParen {
                i += 1;
                continue;
            }
            let Some(nclose) = matching_paren(&tokens, nopen).filter(|&k| k < close) else {
                i += 1;
                continue;
            };
            let Some(comma) = significant(&tokens, nclose + 1).filter(|&k| k < close) else {
                i += 1;
                continue;
            };
            if tokens[comma].token != Token::Comma {
                i += 1;
                continue;
            }
            let mut ok = true;
            for (nf, nto) in split_top_level(&tokens, nopen + 1, nclose) {
                match xmltable_namespace(&tokens, nf, nto) {
                    Some(namespace) => namespaces.push(namespace),
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                i += 1;
                continue;
            }
            at = comma + 1;
        }
        // The row expression, up to `PASSING`.
        let Some(passing) = top_level_word(&tokens, at, close, "passing") else {
            i += 1;
            continue;
        };
        let path = render_tokens(&tokens[at..passing]);
        if path.trim().is_empty() {
            i += 1;
            continue;
        }
        // `PASSING [BY {REF|VALUE}] document [BY {REF|VALUE}] COLUMNS ...`.
        let Some(columns) = top_level_word(&tokens, passing + 1, close, "columns") else {
            i += 1;
            continue;
        };
        let mut document_from = passing + 1;
        if let Some(by) = by_kind(&tokens, document_from, columns)
            && let Some(kind) = significant(&tokens, by + 1)
        {
            document_from = significant(&tokens, kind + 1).unwrap_or(columns);
        }
        // A trailing `BY {REF|VALUE}` ends the document.
        let mut rest = columns;
        let mut by_at = document_from;
        while by_at < columns {
            match by_kind(&tokens, by_at, columns) {
                Some(by) => {
                    rest = by;
                    break;
                }
                None => by_at += 1,
            }
        }
        let document = render_tokens(&tokens[document_from..rest]);
        if document.trim().is_empty() {
            i += 1;
            continue;
        }
        // `COLUMNS` is not parenthesized, as PostgreSQL's grammar has it:
        // the columns run to the table's closing parenthesis.
        let Some(columns) = xmltable_columns(&tokens, columns + 1, close, &mut refuse) else {
            i += 1;
            continue;
        };
        let mut namespace_args = String::new();
        for (prefix, uri) in &namespaces {
            let prefix = match prefix {
                Some(prefix) => format!("'{}'", prefix.replace('\'', "''")),
                None => "NULL".to_string(),
            };
            namespace_args.push_str(&format!(", pg_catalog.__xt_ns__({prefix}, {uri})"));
        }
        let replacement = format!(
            "pg_catalog.__xml_table__({document}, {path}, '{}', {columns}{namespace_args})",
            refuse.replace('\'', "''")
        );
        if let Some(snippet) = snippet_tokens(&replacement) {
            tokens.splice(i..=close, snippet);
            // The arguments may hold further XMLTABLE calls; scan again.
            i = 0;
            continue;
        }
        i += 1;
    }
    tokens
}

/// The top-level `,`-separated items of a token range.
fn split_top_level(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
    from: usize,
    to: usize,
) -> Vec<(usize, usize)> {
    use sqlparser::tokenizer::Token;
    let mut items = Vec::new();
    let mut depth = 0i32;
    let mut start = None;
    for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
        match token.token {
            Token::LParen | Token::LBracket => depth += 1,
            Token::RParen | Token::RBracket => depth -= 1,
            Token::Comma if depth == 0 => {
                items.push((start.unwrap_or(from), i));
                start = None;
                continue;
            }
            _ => {}
        }
        if start.is_none() && !matches!(token.token, Token::Whitespace(_)) {
            start = Some(i);
        }
    }
    if let Some(start) = start {
        items.push((start, to));
    }
    items
}

/// `XMLEXISTS(expression PASSING [BY {REF|VALUE}] document [BY {REF|VALUE}])`
/// — optionally led by an `XMLNAMESPACES(...)` clause — which the parser
/// cannot read, rewritten to the `xpath_exists` call it means (see
/// `xpath.rs`): `pg_catalog.xpath_exists(expression, document
/// [, ARRAY[ARRAY[prefix, uri], ...]])`.
fn rewrite_xmlexists(
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
    let split = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut start = None;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                Token::Comma if depth == 0 => {
                    items.push((start.unwrap_or(from), i));
                    start = None;
                    continue;
                }
                _ => {}
            }
            if start.is_none() && !matches!(token.token, Token::Whitespace(_)) {
                start = Some(i);
            }
        }
        if let Some(start) = start {
            items.push((start, to));
        }
        items
    };
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() != Some("xmlexists") {
            i += 1;
            continue;
        }
        let Some(open) = significant(&tokens, i + 1) else {
            i += 1;
            continue;
        };
        if tokens[open].token != Token::LParen {
            i += 1;
            continue;
        }
        let Some(close) = matching_paren(&tokens, open) else {
            i += 1;
            continue;
        };
        let items = split(&tokens, open + 1, close);
        let mut at = 0;
        // `XMLNAMESPACES('uri' AS prefix, DEFAULT 'uri', ...)`.
        let mut namespaces: Vec<(String, String)> = Vec::new();
        if items
            .first()
            .is_some_and(|(from, _)| word(&tokens[*from]).as_deref() == Some("xmlnamespaces"))
        {
            let Some((from, _)) = items.first().copied() else {
                i += 1;
                continue;
            };
            let Some(nopen) = significant(&tokens, from + 1) else {
                i += 1;
                continue;
            };
            let Some(nclose) = matching_paren(&tokens, nopen) else {
                i += 1;
                continue;
            };
            let mut ok = true;
            for (nf, nto) in split(&tokens, nopen + 1, nclose) {
                let text = render_tokens(&tokens[nf..nto]);
                let item = text.trim();
                // `'uri' AS prefix` or `DEFAULT 'uri'`; the URI stays the
                // SQL literal it was written as.
                let lower = item.to_ascii_lowercase();
                let parsed = if let Some(rest) = lower.rfind(" as ") {
                    let prefix = item[rest + 4..].trim().to_string();
                    let uri = item[..rest].trim().to_string();
                    Some((prefix, uri))
                } else {
                    lower.strip_prefix("default ").map(|rest| {
                        (
                            String::new(),
                            item[item.len() - rest.len()..].trim().to_string(),
                        )
                    })
                };
                let Some((prefix, uri)) = parsed.filter(|(_, uri)| !uri.is_empty()) else {
                    ok = false;
                    break;
                };
                namespaces.push((prefix, uri));
            }
            if !ok {
                i += 1;
                continue;
            }
            at = 1;
        }
        let Some((pf, pto)) = items.get(at).copied() else {
            i += 1;
            continue;
        };
        // The expression runs to the `PASSING` keyword.
        let mut path_end = pto;
        for (index, token) in tokens.iter().enumerate().take(pto).skip(pf) {
            if word(token).as_deref() == Some("passing") {
                path_end = index;
                break;
            }
        }
        let path = render_tokens(&tokens[pf..path_end]);
        if path.trim().is_empty() || path_end == pto {
            i += 1;
            continue;
        }
        // `PASSING [BY {REF|VALUE}] document [BY {REF|VALUE}]`.
        let mut document_from = path_end + 1;
        let by_kind = |at: usize| -> Option<usize> {
            let by = significant(&tokens, at).filter(|&k| k < pto)?;
            if word(&tokens[by]).as_deref() != Some("by") {
                return None;
            }
            let kind = significant(&tokens, by + 1).filter(|&k| k < pto)?;
            matches!(word(&tokens[kind]).as_deref(), Some("ref" | "value")).then_some(kind + 1)
        };
        if let Some(after) = by_kind(path_end + 1) {
            document_from = after;
        }
        // A trailing `BY {REF|VALUE}` ends the document.
        let mut rest = pto;
        let mut at = document_from;
        while at < pto {
            match by_kind(at) {
                Some(_) => {
                    rest = at;
                    break;
                }
                None => at += 1,
            }
        }
        let document = render_tokens(&tokens[document_from..rest]);
        if document.trim().is_empty() {
            i += 1;
            continue;
        }
        let replacement = if namespaces.is_empty() {
            format!("pg_catalog.__xmlexists__({path}, {document})")
        } else {
            let pairs: Vec<String> = namespaces
                .iter()
                .map(|(prefix, uri)| format!("ARRAY['{}', {uri}]", prefix.replace('\'', "''")))
                .collect();
            format!(
                "pg_catalog.__xmlexists__({path}, {document}, ARRAY[{}])",
                pairs.join(", ")
            )
        };
        if let Some(snippet) = snippet_tokens(&replacement) {
            tokens.splice(i..=close, snippet);
            i = 0;
            continue;
        }
        i += 1;
    }
    tokens
}

/// The SQL/JSON array and object constructors and their aggregates, which the
/// parser cannot read with their clauses: `JSON_ARRAY`, `JSON_OBJECT`,
/// `JSON_ARRAYAGG`, and `JSON_OBJECTAGG`, each rewritten to the marker
/// function the planner knows (see `sqljson.rs`). The shapes are:
/// `__json_array__(absent, returning, element...)`,
/// `__json_object__(absent, unique, returning, key, value, ...)`,
/// `__json_arrayagg__(element, absent, returning)`,
/// `__json_objectagg__(key, value, absent, unique, returning)`, with the
/// aggregate's `ORDER BY` kept inside its call, and an element under
/// `FORMAT JSON` as `pg_catalog.__json_format__(value)`.
fn rewrite_json_constructors(
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
    // The ranges of the top-level `,`-separated items of a token range; a
    // `[...]` (an array literal or subscript) holds its commas.
    let split = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut start = None;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                Token::Comma if depth == 0 => {
                    items.push((start.unwrap_or(from), i));
                    start = None;
                    continue;
                }
                _ => {}
            }
            if start.is_none() && !matches!(token.token, Token::Whitespace(_)) {
                start = Some(i);
            }
        }
        if let Some(start) = start {
            items.push((start, to));
        }
        items
    };
    // The `:` or `value` separating an object's key from its value, at the
    // item's top level. A `:` anywhere is the separator; otherwise the first
    // `value` keyword is.
    let pair_at = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut value_at = None;
        let mut depth = 0i32;
        for (at, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                Token::Colon if depth == 0 => return Some(at),
                _ => {}
            }
            if value_at.is_none() && depth == 0 && word(token).as_deref() == Some("value") {
                value_at = Some(at);
            }
        }
        value_at
    };
    // The last top-level `word` of a token range, if any.
    let last_top = |tokens: &[TokenWithSpan], from: usize, to: usize, wanted: &str| {
        let mut found = None;
        let mut depth = 0i32;
        for (at, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                _ => {}
            }
            if depth == 0 && word(token).as_deref() == Some(wanted) {
                found = Some(at);
            }
        }
        found
    };
    // A trailing `FORMAT JSON [ENCODING name]` clause of the item ending at
    // `to`: the value expression ends just before it, and the flag says
    // whether an `ENCODING` clause was written.
    let format_end = |tokens: &[TokenWithSpan], to: usize| -> Option<(usize, bool)> {
        let mut end = to;
        let mut encoding = false;
        if let Some(name) = significant_back(tokens, end)
            && let Some(at) = significant_back(tokens, name)
            && word(&tokens[at]).as_deref() == Some("encoding")
        {
            encoding = true;
            end = at;
        }
        if let Some(last) = significant_back(tokens, end)
            && word(&tokens[last]).as_deref() == Some("json")
            && let Some(at) = significant_back(tokens, last)
            && word(&tokens[at]).as_deref() == Some("format")
        {
            return Some((at, encoding));
        }
        None
    };
    // A value expression as the marker takes it: under `FORMAT JSON` it is
    // read as a JSON document by the runtime, `encoding` marking an
    // `ENCODING name` clause (only a `bytea` may take one).
    let element =
        |tokens: &[TokenWithSpan], from: usize, to: usize, format: Option<bool>| -> String {
            let text = render_tokens(&tokens[from..to]);
            match format {
                Some(encoding) => {
                    format!("pg_catalog.__json_format__({text}, {encoding})")
                }
                None => text,
            }
        };
    let mut i = 0;
    while i < tokens.len() {
        let name = word(&tokens[i]).unwrap_or_default();
        if !matches!(
            name.as_str(),
            "json_array" | "json_object" | "json_arrayagg" | "json_objectagg"
        ) {
            i += 1;
            continue;
        }
        let Some(open) = significant(&tokens, i + 1) else {
            i += 1;
            continue;
        };
        if tokens[open].token != Token::LParen {
            i += 1;
            continue;
        }
        let aggregate = matches!(name.as_str(), "json_arrayagg" | "json_objectagg");
        let object = matches!(name.as_str(), "json_object" | "json_objectagg");
        let Some(close) = matching_paren(&tokens, open) else {
            i += 1;
            continue;
        };
        // `json_array(SELECT ...)`: the subquery form, which PostgreSQL
        // reads as the array aggregate over the subquery's one column
        // (`json_arrayagg` with `ABSENT ON NULL`).
        if name == "json_array"
            && let Some(first) = significant(&tokens, open + 1).filter(|&at| at < close)
            && matches!(
                word(&tokens[first]).as_deref(),
                Some("select" | "with" | "values")
            )
        {
            let mut end = close;
            let mut returning = String::new();
            if let Some(at) = last_top(&tokens, first, end, "returning") {
                // The type ends before a trailing `FORMAT JSON`, as
                // PostgreSQL reads the clause.
                let type_end = format_end(&tokens, end).map_or(end, |(at, _)| at);
                returning = render_tokens(&tokens[at + 1..type_end]).trim().to_string();
                end = at;
            }
            let mut format = None;
            if let Some((at, encoding)) = format_end(&tokens, end) {
                format = Some(encoding);
                end = at;
            }
            let query = render_tokens(&tokens[first..end]);
            if query.trim().is_empty() {
                i += 1;
                continue;
            }
            let element = match format {
                Some(encoding) => format!("pg_catalog.__json_format__(q.a, {encoding})"),
                None => "q.a".to_string(),
            };
            let returning = returning.replace('\'', "''");
            let replacement = format!(
                "(select pg_catalog.__json_array_query__({element}, true, '{returning}') \
                 from ({query}) as q(a))"
            );
            if let Some(snippet) = snippet_tokens(&replacement) {
                tokens.splice(i..=close, snippet);
                // The subquery may hold further constructors; scan again.
                i = 0;
                continue;
            }
            i += 1;
            continue;
        }
        // `DISTINCT` is a syntax error in PostgreSQL for these functions;
        // leave it to the parser.
        if significant(&tokens, open + 1)
            .and_then(|at| word(&tokens[at]))
            .as_deref()
            == Some("distinct")
        {
            i += 1;
            continue;
        }
        let mut items = split(&tokens, open + 1, close);
        let mut absent = String::new();
        let mut unique = String::new();
        let mut returning = String::new();
        let mut order: Option<String> = None;
        let mut failed = false;
        // The clauses that follow the last item, in PostgreSQL's order: the
        // aggregate's `ORDER BY`, the null clause, the unique-keys clause,
        // and the RETURNING type.
        if let Some((last_from, last_to)) = items.last().copied() {
            let mut end = last_to;
            let mut returning_at = None;
            let mut depth = 0i32;
            for (at, token) in tokens.iter().enumerate().take(last_to).skip(last_from) {
                match token.token {
                    Token::LParen | Token::LBracket => depth += 1,
                    Token::RParen | Token::RBracket => depth -= 1,
                    _ => {}
                }
                if depth == 0 && word(token).as_deref() == Some("returning") {
                    returning_at = Some(at);
                }
            }
            if let Some(at) = returning_at {
                let type_end = format_end(&tokens, end).map_or(end, |(at, _)| at);
                returning = render_tokens(&tokens[at + 1..type_end]).trim().to_string();
                end = at;
            }
            if let Some(last) = significant_back(&tokens, end) {
                let u = if word(&tokens[last]).as_deref() == Some("keys") {
                    significant_back(&tokens, last)
                } else {
                    Some(last)
                };
                if let Some(u) = u.filter(|&u| word(&tokens[u]).as_deref() == Some("unique"))
                    && let Some(kind_at) = significant_back(&tokens, u)
                    && matches!(word(&tokens[kind_at]).as_deref(), Some("with" | "without"))
                {
                    unique = word(&tokens[kind_at]).unwrap();
                    end = kind_at;
                }
            }
            if let Some(last) = significant_back(&tokens, end)
                && word(&tokens[last]).as_deref() == Some("null")
                && let Some(on_at) = significant_back(&tokens, last)
                && word(&tokens[on_at]).as_deref() == Some("on")
                && let Some(kind_at) = significant_back(&tokens, on_at)
                && matches!(word(&tokens[kind_at]).as_deref(), Some("null" | "absent"))
            {
                absent = word(&tokens[kind_at]).unwrap();
                end = kind_at;
            }
            // An `ORDER BY` belongs to `json_arrayagg` alone; anywhere else
            // it is a syntax error in PostgreSQL, which the parser reports
            // when the call is left as it is.
            let mut depth = 0i32;
            let mut order_at = None;
            for (at, token) in tokens.iter().enumerate().take(end).skip(last_from) {
                match token.token {
                    Token::LParen | Token::LBracket => depth += 1,
                    Token::RParen | Token::RBracket => depth -= 1,
                    _ => {}
                }
                if depth == 0
                    && word(token).as_deref() == Some("order")
                    && significant(&tokens, at + 1)
                        .is_some_and(|n| word(&tokens[n]).as_deref() == Some("by"))
                {
                    order_at = Some(at);
                }
            }
            if let Some(at) = order_at {
                if name == "json_arrayagg" {
                    let by_at = significant(&tokens, at + 1).expect("checked above");
                    order = Some(render_tokens(&tokens[by_at + 1..end]));
                    end = at;
                } else {
                    failed = true;
                }
            }
            if end > last_from {
                if let Some(item) = items.last_mut() {
                    *item = (last_from, end);
                }
            } else if returning_at == Some(last_from)
                && absent.is_empty()
                && unique.is_empty()
                && !aggregate
                && items.len() == 1
            {
                // `<constructor>(returning type)`: no elements or pairs, and
                // no other clause, as PostgreSQL takes it.
                items.pop();
            } else {
                // A clause with no value before it (`, null on null`), which
                // PostgreSQL takes as a syntax error; leaving it as it is
                // lets the parser report one.
                failed = true;
            }
        }
        if aggregate && items.len() != 1 {
            failed = true;
        }
        // The elements or key-value pairs, each with its own `FORMAT JSON`.
        let mut parts: Vec<String> = Vec::new();
        if !failed {
            for (from, to) in items.iter().copied() {
                let (end, format) = format_end(&tokens, to)
                    .map_or((to, None), |(at, encoding)| (at, Some(encoding)));
                if object {
                    let Some(sep) = pair_at(&tokens, from, end) else {
                        failed = true;
                        break;
                    };
                    let key = render_tokens(&tokens[from..sep]);
                    parts.push(format!("{key}, {}", element(&tokens, sep + 1, end, format)));
                } else {
                    parts.push(element(&tokens, from, end, format));
                }
            }
        }
        if failed {
            i += 1;
            continue;
        }
        // The null clause defaults: ABSENT ON NULL for the arrays, NULL ON
        // NULL for the objects.
        let absent_flag = match absent.as_str() {
            "absent" => "true",
            "null" => "false",
            _ if object => "false",
            _ => "true",
        };
        let unique_flag = if unique == "with" { "unique" } else { "" };
        // An empty RETURNING type is `json`, and (as PostgreSQL decides) the
        // canonical spelling when a value is a `jsonb`.
        let returning = returning.replace('\'', "''");
        let args = if parts.is_empty() {
            String::new()
        } else {
            format!(", {}", parts.join(", "))
        };
        let replacement = match name.as_str() {
            "json_array" => {
                format!("pg_catalog.__json_array__({absent_flag}, '{returning}'{args})")
            }
            "json_object" => format!(
                "pg_catalog.__json_object__({absent_flag}, '{unique_flag}', '{returning}'{args})"
            ),
            "json_arrayagg" => {
                let order = order.map_or(String::new(), |keys| format!(" order by {keys}"));
                format!(
                    "pg_catalog.__json_arrayagg__({}, {absent_flag}, '{returning}'{order})",
                    parts[0]
                )
            }
            _ => format!(
                "pg_catalog.__json_objectagg__({}, {absent_flag}, '{unique_flag}', \
                 '{returning}')",
                parts[0]
            ),
        };
        if let Some(snippet) = snippet_tokens(&replacement) {
            tokens.splice(i..=close, snippet);
            // The arguments the call carried may hold further constructors;
            // scan them from the start again.
            i = 0;
            continue;
        }
        i += 1;
    }
    tokens
}

/// The SQL/JSON `JSON_TABLE(...)` call, which the parser cannot read with
/// its `COLUMNS` clause, rewritten to the marker function the table-function
/// planner knows (see `sqljson.rs`):
/// `pg_catalog.__json_table__(context, path, on-error, vars, column...)`.
/// Each column becomes `pg_catalog.__jt_col_ordinality__('name')`,
/// `pg_catalog.__jt_col_exists__('name', 'type', path, on-error)`,
/// `pg_catalog.__jt_col_scalar__('name', 'type', format, path, wrapper,
/// quotes, on-empty, on-empty-default, on-error, on-error-default)`, or
/// `pg_catalog.__jt_nested__(path, column...)`.
fn rewrite_json_table(
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
    // The ranges of the top-level `,`-separated items of a token range.
    let split = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut start = None;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                Token::Comma if depth == 0 => {
                    items.push((start.unwrap_or(from), i));
                    start = None;
                    continue;
                }
                _ => {}
            }
            if start.is_none() && !matches!(token.token, Token::Whitespace(_)) {
                start = Some(i);
            }
        }
        if let Some(start) = start {
            items.push((start, to));
        }
        items
    };
    // A `FORMAT JSON [ENCODING name]` clause of the item ending at `to`:
    // the value expression ends just before it.
    let format_end = |tokens: &[TokenWithSpan], to: usize| -> Option<usize> {
        let mut end = to;
        if let Some(name) = significant_back(tokens, end)
            && let Some(encoding) = significant_back(tokens, name)
            && word(&tokens[encoding]).as_deref() == Some("encoding")
        {
            end = encoding;
        }
        if let Some(last) = significant_back(tokens, end)
            && word(&tokens[last]).as_deref() == Some("json")
            && let Some(at) = significant_back(tokens, last)
            && word(&tokens[at]).as_deref() == Some("format")
        {
            return Some(at);
        }
        None
    };
    let mut i = 0;
    while i < tokens.len() {
        if word(&tokens[i]).as_deref() != Some("json_table") {
            i += 1;
            continue;
        }
        let Some(open) = significant(&tokens, i + 1) else {
            i += 1;
            continue;
        };
        if tokens[open].token != Token::LParen {
            i += 1;
            continue;
        }
        let Some(close) = matching_paren(&tokens, open) else {
            i += 1;
            continue;
        };
        let items = split(&tokens, open + 1, close);
        if items.len() != 2 {
            i += 1;
            continue;
        }
        // The context item, with its `FORMAT JSON` clause.
        let (df, dto) = items[0];
        let mut doc_end = dto;
        let mut format = false;
        if let Some(at) = format_end(&tokens, dto) {
            format = true;
            doc_end = at;
        }
        let doc = render_tokens(&tokens[df..doc_end]);
        if doc.trim().is_empty() {
            i += 1;
            continue;
        }
        // The row path, its `AS` name, the `PASSING` variables, the
        // `COLUMNS` clause, and the table's `ON ERROR` clause.
        let (pf, pto) = items[1];
        let mut at = pf;
        let mut path_end = pto;
        while at < pto {
            if let Some(w) = word(&tokens[at])
                && matches!(w.as_str(), "as" | "passing" | "columns")
            {
                path_end = at;
                break;
            }
            at += 1;
        }
        let path = render_tokens(&tokens[pf..path_end]);
        if path.trim().is_empty() {
            i += 1;
            continue;
        }
        at = significant(&tokens, path_end).unwrap_or(pto);
        if word(&tokens[at]).as_deref() == Some("as") {
            let Some(name) = significant(&tokens, at + 1).filter(|&i| i < pto) else {
                i += 1;
                continue;
            };
            at = significant(&tokens, name + 1).unwrap_or(pto);
        }
        let mut vars = "NULL".to_string();
        if word(&tokens[at]).as_deref() == Some("passing") {
            // `<value> AS <name>` pairs run to the `COLUMNS` keyword.
            let mut end = pto;
            for (index, token) in tokens.iter().enumerate().take(pto).skip(at + 1) {
                if word(token).as_deref() == Some("columns") {
                    end = index;
                    break;
                }
            }
            let mut parts = vec![format!(
                "pg_catalog.{}(",
                crate::SQL_JSON_VARS_MARKER.to_ascii_lowercase()
            )];
            let mut ok = true;
            for (index, (vf, vto)) in split(&tokens, at + 1, end).into_iter().enumerate() {
                let mut as_at = None;
                let mut depth = 0i32;
                for (j, token) in tokens.iter().enumerate().take(vto).skip(vf) {
                    match token.token {
                        Token::LParen | Token::LBracket => depth += 1,
                        Token::RParen | Token::RBracket => depth -= 1,
                        _ => {}
                    }
                    if depth == 0 && word(token).as_deref() == Some("as") {
                        as_at = Some(j);
                    }
                }
                let Some(as_at) = as_at else {
                    ok = false;
                    break;
                };
                let value = render_tokens(&tokens[vf..as_at]);
                let name = render_tokens(&tokens[as_at + 1..vto])
                    .trim()
                    .replace('\'', "''");
                if name.is_empty() {
                    ok = false;
                    break;
                }
                if index > 0 {
                    parts.push(", ".to_string());
                }
                parts.push(format!("'{name}', {value}"));
            }
            parts.push(")".to_string());
            if !ok {
                i += 1;
                continue;
            }
            vars = parts.concat();
            at = significant(&tokens, end).unwrap_or(pto);
        }
        if word(&tokens[at]).as_deref() != Some("columns") {
            i += 1;
            continue;
        }
        let Some(copen) = significant(&tokens, at + 1).filter(|&i| i < pto) else {
            i += 1;
            continue;
        };
        if tokens[copen].token != Token::LParen {
            i += 1;
            continue;
        }
        let Some(cclose) = matching_paren(&tokens, copen) else {
            i += 1;
            continue;
        };
        let Some(columns) = json_table_columns(&tokens, copen + 1, cclose) else {
            i += 1;
            continue;
        };
        // The table's `{ ERROR | EMPTY [ARRAY] } ON ERROR`.
        let mut on_error = String::new();
        let mut end = significant(&tokens, cclose + 1).unwrap_or(pto);
        if end < pto {
            let Some((kind, which, _, next)) = json_table_behavior(&tokens, end, pto) else {
                i += 1;
                continue;
            };
            if which != "error" || !matches!(kind.as_str(), "error" | "empty-array") {
                i += 1;
                continue;
            }
            on_error = kind;
            end = next;
        }
        if end < pto {
            // Anything after the table's `ON ERROR` is not a JSON_TABLE.
            i += 1;
            continue;
        }
        let doc = if format {
            format!("pg_catalog.__json_format__({doc}, false)")
        } else {
            doc
        };
        let replacement =
            format!("pg_catalog.__json_table__({doc}, {path}, '{on_error}', {vars}, {columns})");
        if let Some(snippet) = snippet_tokens(&replacement) {
            tokens.splice(i..=close, snippet);
            // The arguments may hold further JSON_TABLE calls; scan again.
            i = 0;
            continue;
        }
        i += 1;
    }
    tokens
}

/// The columns of a `JSON_TABLE` call, between `from` and `to`, as the
/// marker calls the table-function planner reads. `None` when a column is
/// not one PostgreSQL would take, leaving the call to the parser's error.
fn json_table_columns(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
    from: usize,
    to: usize,
) -> Option<String> {
    use sqlparser::tokenizer::{Token, TokenWithSpan};
    let word = |t: &TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant = |tokens: &[TokenWithSpan], from: usize| {
        (from..tokens.len()).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
    };
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
    let split = |tokens: &[TokenWithSpan], from: usize, to: usize| {
        let mut items = Vec::new();
        let mut depth = 0i32;
        let mut start = None;
        for (i, token) in tokens.iter().enumerate().take(to).skip(from) {
            match token.token {
                Token::LParen | Token::LBracket => depth += 1,
                Token::RParen | Token::RBracket => depth -= 1,
                Token::Comma if depth == 0 => {
                    items.push((start.unwrap_or(from), i));
                    start = None;
                    continue;
                }
                _ => {}
            }
            if start.is_none() && !matches!(token.token, Token::Whitespace(_)) {
                start = Some(i);
            }
        }
        if let Some(start) = start {
            items.push((start, to));
        }
        items
    };
    let clause_word = |w: &str| {
        matches!(
            w,
            "path"
                | "exists"
                | "format"
                | "keep"
                | "omit"
                | "error"
                | "null"
                | "default"
                | "empty"
                | "true"
                | "false"
                | "unknown"
                | "with"
                | "without"
        )
    };
    let mut parts: Vec<String> = Vec::new();
    for (cf, cto) in split(tokens, from, to) {
        let mut at = significant(tokens, cf).filter(|&i| i < cto)?;
        // `NESTED [PATH] path [AS name] COLUMNS (...)`.
        if word(&tokens[at]).as_deref() == Some("nested") {
            at = significant(tokens, at + 1).filter(|&i| i < cto)?;
            if word(&tokens[at]).as_deref() == Some("path") {
                at = significant(tokens, at + 1).filter(|&i| i < cto)?;
            }
            let path_from = at;
            while at < cto {
                if matches!(word(&tokens[at]).as_deref(), Some("as" | "columns")) {
                    break;
                }
                at += 1;
            }
            let path = render_tokens(&tokens[path_from..at]);
            if path.trim().is_empty() {
                return None;
            }
            let mut at = significant(tokens, at).filter(|&i| i < cto)?;
            if word(&tokens[at]).as_deref() == Some("as") {
                at = significant(tokens, at + 1).filter(|&i| i < cto)?;
                at = significant(tokens, at + 1).unwrap_or(cto);
            }
            if word(&tokens[at]).as_deref() != Some("columns") {
                return None;
            }
            let open = significant(tokens, at + 1).filter(|&i| i < cto)?;
            if tokens[open].token != Token::LParen {
                return None;
            }
            let close = matching_paren(tokens, open).filter(|&i| i < cto)?;
            let columns = json_table_columns(tokens, open + 1, close)?;
            parts.push(format!("pg_catalog.__jt_nested__({path}, {columns})"));
            continue;
        }
        // `name FOR ORDINALITY`.
        let Token::Word(name) = &tokens[at].token else {
            return None;
        };
        let quoted = name.quote_style.is_some();
        let column_name = name.value.clone();
        at = significant(tokens, at + 1).filter(|&i| i < cto)?;
        if word(&tokens[at]).as_deref() == Some("for") {
            at = significant(tokens, at + 1).filter(|&i| i < cto)?;
            if word(&tokens[at]).as_deref() != Some("ordinality") {
                return None;
            }
            at = significant(tokens, at + 1).unwrap_or(cto);
            if at < cto {
                return None;
            }
            parts.push(format!("pg_catalog.__jt_col_ordinality__('{column_name}')"));
            continue;
        }
        // The column's type runs to the first clause keyword.
        let type_from = at;
        while at < cto {
            if clause_word(&word(&tokens[at]).unwrap_or_default())
                && !(word(&tokens[at]).as_deref() == Some("with")
                    && significant(tokens, at + 1)
                        .is_some_and(|n| word(&tokens[n]).as_deref() == Some("time")))
            {
                break;
            }
            at += 1;
        }
        let column_type = render_tokens(&tokens[type_from..at]).trim().to_string();
        if column_type.is_empty() {
            return None;
        }
        let escaped_type = column_type.replace('\'', "''");
        let name_path = if quoted || !column_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            format!(
                "'$.\"{}\"'",
                column_name.replace('\\', "\\\\").replace('"', "\\\"")
            )
        } else {
            format!("'$.{column_name}'")
        };
        // `EXISTS [PATH p] [behavior ON ERROR]`.
        if word(&tokens[at]).as_deref() == Some("exists") {
            let mut at = significant(tokens, at + 1).unwrap_or(cto);
            let mut path = name_path;
            if word(&tokens[at]).as_deref() == Some("path") {
                at = significant(tokens, at + 1).filter(|&i| i < cto)?;
                let from = at;
                while at < cto {
                    if clause_word(&word(&tokens[at]).unwrap_or_default()) {
                        break;
                    }
                    at += 1;
                }
                let text = render_tokens(&tokens[from..at]);
                if text.trim().is_empty() {
                    return None;
                }
                path = text;
            }
            let mut on_error = String::new();
            let mut at = significant(tokens, at).unwrap_or(cto);
            if at < cto {
                let (kind, _, _, next) = json_table_behavior(tokens, at, cto)?;
                if !matches!(kind.as_str(), "error" | "true" | "false" | "unknown") {
                    return None;
                }
                on_error = kind;
                at = next;
            }
            if at < cto {
                return None;
            }
            parts.push(format!(
                "pg_catalog.__jt_col_exists__('{column_name}', '{escaped_type}', {path}, \
                 '{on_error}')"
            ));
            continue;
        }
        // `[FORMAT JSON [ENCODING name]] [PATH p] [wrapper] [quotes]
        // [behavior ON EMPTY] [behavior ON ERROR]`.
        let mut format = "false";
        let mut encoded = "false";
        let mut path = name_path;
        let mut wrapper = String::new();
        let mut quotes = String::new();
        let mut on_empty = String::new();
        let mut on_empty_default = "NULL".to_string();
        let mut on_error = String::new();
        let mut on_error_default = "NULL".to_string();
        // The clauses appear in PostgreSQL's order; one out of place is a
        // syntax error there, so the call is left to the parser.
        let mut stage = 0;
        while let Some(key) = significant(tokens, at).filter(|&i| i < cto) {
            match word(&tokens[key]).as_deref() {
                Some("format") if stage <= 1 => {
                    stage = 2;
                    let json_at = significant(tokens, key + 1).filter(|&i| i < cto)?;
                    if word(&tokens[json_at]).as_deref() != Some("json") {
                        return None;
                    }
                    format = "true";
                    let mut next = significant(tokens, json_at + 1).unwrap_or(cto);
                    if next < cto && word(&tokens[next]).as_deref() == Some("encoding") {
                        encoded = "true";
                        let name = significant(tokens, next + 1).filter(|&i| i < cto)?;
                        next = significant(tokens, name + 1).unwrap_or(cto);
                    }
                    at = next;
                }
                Some("path") if stage <= 2 => {
                    stage = 3;
                    let from = significant(tokens, key + 1).filter(|&i| i < cto)?;
                    let mut to = from;
                    while to < cto {
                        if clause_word(&word(&tokens[to]).unwrap_or_default()) {
                            break;
                        }
                        to += 1;
                    }
                    let text = render_tokens(&tokens[from..to]);
                    if text.trim().is_empty() {
                        return None;
                    }
                    path = text;
                    at = to;
                }
                Some(kind @ ("with" | "without")) if stage <= 3 => {
                    stage = 4;
                    let mut next = significant(tokens, key + 1).filter(|&i| i < cto)?;
                    let mut conditional = false;
                    if matches!(
                        word(&tokens[next]).as_deref(),
                        Some("conditional" | "unconditional")
                    ) {
                        conditional = word(&tokens[next]).as_deref() == Some("conditional");
                        next = significant(tokens, next + 1).filter(|&i| i < cto)?;
                    }
                    if word(&tokens[next]).as_deref() == Some("array") {
                        next = significant(tokens, next + 1).filter(|&i| i < cto)?;
                    }
                    if word(&tokens[next]).as_deref() != Some("wrapper") {
                        return None;
                    }
                    wrapper = if kind == "without" {
                        "without".to_string()
                    } else if conditional {
                        "conditional".to_string()
                    } else {
                        "with".to_string()
                    };
                    at = significant(tokens, next + 1).unwrap_or(cto);
                }
                Some(kind @ ("keep" | "omit")) if stage <= 4 => {
                    stage = 5;
                    let quotes_at = significant(tokens, key + 1).filter(|&i| i < cto)?;
                    if word(&tokens[quotes_at]).as_deref() != Some("quotes") {
                        return None;
                    }
                    let mut next = significant(tokens, quotes_at + 1).unwrap_or(cto);
                    if next < cto && word(&tokens[next]).as_deref() == Some("on") {
                        // `ON SCALAR STRING`.
                        let scalar = significant(tokens, next + 1).filter(|&i| i < cto)?;
                        if word(&tokens[scalar]).as_deref() != Some("scalar") {
                            return None;
                        }
                        let string = significant(tokens, scalar + 1).filter(|&i| i < cto)?;
                        if word(&tokens[string]).as_deref() != Some("string") {
                            return None;
                        }
                        next = significant(tokens, string + 1).unwrap_or(cto);
                    }
                    quotes = kind.to_string();
                    at = next;
                }
                _ if stage <= 5 => {
                    let (kind, which, value, next) = json_table_behavior(tokens, key, cto)?;
                    stage = if which == "empty" { 6 } else { 7 };
                    if which == "empty" {
                        on_empty = kind;
                        if let Some((vf, vto)) = value {
                            on_empty_default = render_tokens(&tokens[vf..vto]);
                        }
                    } else {
                        on_error = kind;
                        if let Some((vf, vto)) = value {
                            on_error_default = render_tokens(&tokens[vf..vto]);
                        }
                    }
                    at = next;
                }
                _ => return None,
            }
        }
        parts.push(format!(
            "pg_catalog.__jt_col_scalar__('{column_name}', '{escaped_type}', {format}, {encoded}, \
             {path}, '{wrapper}', '{quotes}', '{on_empty}', {on_empty_default}, '{on_error}', \
             {on_error_default})"
        ));
    }
    Some(parts.join(", "))
}

/// A behavior clause (`NULL | ERROR | DEFAULT expr | EMPTY [ARRAY|OBJECT] |
/// TRUE | FALSE | UNKNOWN`) followed by `ON EMPTY|ERROR`: its kind, the case
/// it handles (`empty` or `error`), the default expression's token range,
/// and the position after the clause.
type JsonTableBehavior = (String, String, Option<(usize, usize)>, usize);

fn json_table_behavior(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
    at: usize,
    to: usize,
) -> Option<JsonTableBehavior> {
    use sqlparser::tokenizer::Token;
    let word = |t: &sqlparser::tokenizer::TokenWithSpan| match &t.token {
        Token::Word(w) if w.quote_style.is_none() => Some(w.value.clone()),
        _ => None,
    };
    let significant = |tokens: &[sqlparser::tokenizer::TokenWithSpan], from: usize| {
        (from..tokens.len()).find(|&i| !matches!(tokens[i].token, Token::Whitespace(_)))
    };
    let kind = word(&tokens[at])?;
    let (kind, mut next) = match kind.as_str() {
        "error" | "null" | "true" | "false" | "unknown" => (kind.clone(), at + 1),
        "empty" => {
            let after = significant(tokens, at + 1).filter(|&i| i < to);
            match after.and_then(|i| word(&tokens[i])).as_deref() {
                Some(shape @ ("array" | "object")) => {
                    (format!("empty-{shape}"), significant(tokens, at + 1)? + 1)
                }
                _ => ("empty-array".to_string(), at + 1),
            }
        }
        "default" => {
            let mut depth = 0i32;
            let mut on_at = None;
            for (i, token) in tokens.iter().enumerate().take(to).skip(at + 1) {
                match token.token {
                    Token::LParen | Token::LBracket => depth += 1,
                    Token::RParen | Token::RBracket => depth -= 1,
                    _ => {}
                }
                if depth == 0 && word(token).as_deref() == Some("on") {
                    on_at = Some(i);
                    break;
                }
            }
            (kind.clone(), on_at?)
        }
        _ => return None,
    };
    let on = significant(tokens, next).filter(|&i| i < to)?;
    if word(&tokens[on]).as_deref() != Some("on") {
        return None;
    }
    let which_at = significant(tokens, on + 1).filter(|&i| i < to)?;
    let which = word(&tokens[which_at])?;
    if !matches!(which.as_str(), "empty" | "error") {
        return None;
    }
    next = significant(tokens, which_at + 1).unwrap_or(to);
    let value = if kind == "default" {
        Some((at + 1, on))
    } else {
        None
    };
    Some((kind, which, value, next))
}

/// The call a `PASSING` clause is rewritten to:
/// `pg_catalog.__json_vars__('name', value, ...)`.
pub const SQL_JSON_VARS_MARKER: &str = "__json_vars__";

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

/// The mark `ONLY <name>` is left behind as, for the planner to strip: the
/// parser cannot take `ONLY` before a table name (it reads `only` as the
/// name and the real name as its alias, or fails on `ONLY t AS x`).
pub const ONLY_MARK: &str = "\u{0}only:";

/// Rewrites `FROM/JOIN/UPDATE/DELETE ONLY <name>` into a marked name the
/// planner recognizes. A `TRUNCATE ... ONLY` is left alone: the parser
/// understands that form itself.
fn rewrite_only(
    tokens: Vec<sqlparser::tokenizer::TokenWithSpan>,
) -> Vec<sqlparser::tokenizer::TokenWithSpan> {
    use sqlparser::keywords::Keyword;
    use sqlparser::tokenizer::{Token, Word};
    let mut out: Vec<sqlparser::tokenizer::TokenWithSpan> = Vec::with_capacity(tokens.len());
    let mut statement: Option<String> = None;
    let mut i = 0;
    while i < tokens.len() {
        match &tokens[i].token {
            Token::SemiColon => statement = None,
            Token::Word(word) => {
                if statement.is_none() {
                    statement = Some(word.value.clone());
                }
                let mut previous: Option<&str> = None;
                for token in out.iter().rev() {
                    match &token.token {
                        Token::Whitespace(_) => continue,
                        Token::Word(w) => previous = Some(w.value.as_str()),
                        Token::Comma => previous = Some(","),
                        _ => {}
                    }
                    break;
                }
                let reads_only = matches!(
                    previous,
                    Some("from") | Some("join") | Some("update") | Some(",")
                );
                if word.value == "only"
                    && word.quote_style.is_none()
                    && statement.as_deref() != Some("truncate")
                    && reads_only
                    && let Some((name, next)) = object_name(&tokens, i + 1)
                {
                    out.push(sqlparser::tokenizer::TokenWithSpan {
                        token: Token::Word(Word {
                            value: format!("{ONLY_MARK}{name}"),
                            quote_style: Some('"'),
                            keyword: Keyword::NoKeyword,
                        }),
                        span: tokens[i].span,
                    });
                    i = next;
                    continue;
                }
            }
            _ => {}
        }
        out.push(tokens[i].clone());
        i += 1;
    }
    out
}

/// The `[schema.]name` at `at`, with each part written as the planner reads
/// it (quotes kept, so a quoted name keeps its case), and the index after it.
fn object_name(
    tokens: &[sqlparser::tokenizer::TokenWithSpan],
    at: usize,
) -> Option<(String, usize)> {
    use sqlparser::tokenizer::Token;
    // Whitespace and comments between the parts carry no meaning.
    let significant = |mut i: usize| -> Option<usize> {
        while let Some(token) = tokens.get(i) {
            match &token.token {
                Token::Whitespace(_) => i += 1,
                _ => return Some(i),
            }
        }
        None
    };
    let mut parts: Vec<String> = Vec::new();
    let mut i = significant(at)?;
    while let Token::Word(word) = &tokens[i].token {
        parts.push(match word.quote_style {
            Some(_) => format!("\"{}\"", word.value.replace('"', "\"\"")),
            None => word.value.clone(),
        });
        let after = match significant(i + 1) {
            Some(j) if matches!(tokens[j].token, Token::Period) => j + 1,
            _ => break,
        };
        match significant(after) {
            Some(j) if matches!(tokens[j].token, Token::Word(_)) => i = j,
            _ => break,
        }
    }
    if parts.is_empty() {
        return None;
    }
    Some((parts.join("."), i + 1))
}

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

/// The function `ALTER TABLE child INHERIT | NO INHERIT parent` is written
/// as — `SELECT pg_catalog.nodus_inherit('child', 'parent', true)` — since
/// the parser takes no inheritance operation.
pub const INHERIT_FUNCTION: &str = "pg_catalog.nodus_inherit";

/// The function `ALTER TABLE parent ATTACH PARTITION child FOR VALUES ...`
/// is written as — `SELECT pg_catalog.nodus_attach_partition('parent',
/// 'child', 'FOR VALUES ...')` — since the parser's `ATTACH PARTITION` is
/// the storage-engine form, not PostgreSQL's.
pub const ATTACH_PARTITION_FUNCTION: &str = "pg_catalog.nodus_attach_partition";

/// The function `ALTER TABLE parent DETACH PARTITION child` is written as
/// — `SELECT pg_catalog.nodus_detach_partition('parent', 'child')`.
pub const DETACH_PARTITION_FUNCTION: &str = "pg_catalog.nodus_detach_partition";

/// The name a *column* constraint's `UNIQUE NULLS NOT DISTINCT` is written
/// with — `unique __nulls_not_distinct__` — since the parser reads the
/// NULLS clause only on table constraints. It stands in for the
/// constraint's (unused) index name.
pub const NULLS_NOT_DISTINCT_MARK: &str = "__nulls_not_distinct__";

/// The function `GRANT role [, ...] TO member [, ...]` (and the `REVOKE`)
/// is written as — `SELECT pg_catalog.nodus_grant_role('roles', 'members',
/// 'grant')` — since the parser reads only privilege grants.
pub const GRANT_ROLE_FUNCTION: &str = "pg_catalog.nodus_grant_role";

/// The function `SET CONSTRAINTS {ALL | names} {DEFERRED | IMMEDIATE}` is
/// written as — `SELECT pg_catalog.nodus_set_constraints(true, 'deferred',
/// '')` — since the parser reads `SET CONSTRAINTS` as a variable SET.
pub const SET_CONSTRAINTS_FUNCTION: &str = "pg_catalog.nodus_set_constraints";

/// The function `ALTER TABLE t ALTER CONSTRAINT name ...` is written as —
/// `SELECT pg_catalog.nodus_alter_constraint('t', 'name', true, true)` —
/// since the parser has no `ALTER CONSTRAINT`.
pub const ALTER_CONSTRAINT_FUNCTION: &str = "pg_catalog.nodus_alter_constraint";

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
    // `UNIQUE NULLS NOT DISTINCT` on a *column* — the parser takes the
    // clause only on table constraints — becomes a marker-named constraint
    // of the same column (`unique constraint __nulls_not_distinct__ unique`)
    // which the planner reads as the flag. `NULLS DISTINCT` is the default
    // and goes. A table constraint's clause is left alone (its column list
    // follows the words).
    {
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
        let at_word = |at: usize| significant.get(at).and_then(|&i| word(&statement[i]));
        let first = at_word(0);
        let second = at_word(1);
        let table_statement = matches!(first.as_deref(), Some("create") | Some("alter"))
            && second.as_deref() == Some("table");
        if table_statement {
            let mut edits: Vec<(usize, usize, Option<&'static str>)> = Vec::new();
            for at in 0..significant.len() {
                if at_word(at).as_deref() != Some("nulls") {
                    continue;
                }
                // Only a column option's clause: `unique` precedes it and no
                // `(` (a table constraint's column list) follows it.
                if at == 0 || at_word(at - 1).as_deref() != Some("unique") {
                    continue;
                }
                let next = at_word(at + 1);
                let (end, not_distinct) = if next.as_deref() == Some("not")
                    && at_word(at + 2).as_deref() == Some("distinct")
                {
                    (at + 2, true)
                } else if next.as_deref() == Some("distinct") {
                    (at + 1, false)
                } else {
                    continue;
                };
                // A table constraint's column list follows its words.
                let followed_by_columns = significant
                    .get(end + 1)
                    .is_some_and(|&i| matches!(statement[i].token, Token::LParen));
                if followed_by_columns {
                    continue;
                }
                let marker = not_distinct.then_some(NULLS_NOT_DISTINCT_MARK);
                let (start, stop) = (significant[at], significant[end]);
                if marker.is_none() {
                    edits.push((start, stop, None));
                    continue;
                }
                edits.push((start, stop, Some(NULLS_NOT_DISTINCT_MARK)));
            }
            if !edits.is_empty() {
                let mut out = Vec::with_capacity(statement.len());
                let mut i = 0;
                let mut edit = edits.into_iter().peekable();
                while i < statement.len() {
                    if let Some((start, stop, marker)) = edit.peek().copied()
                        && i == start
                    {
                        if let Some(marker) = marker
                            && let Some(tokens) =
                                snippet_tokens(&format!("constraint {marker} unique"))
                        {
                            out.extend(tokens);
                        }
                        i = stop + 1;
                        edit.next();
                        continue;
                    }
                    out.push(statement[i].clone());
                    i += 1;
                }
                statement = out;
            }
        }
    }
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

    // `TEMP` in a privilege list (`GRANT TEMP ON ...`): the parser knows
    // only the `TEMPORARY` spelling. Only the words before the `ON` are
    // privileges; anything parenthesized is a column list.
    if (is(0, "grant") || is(0, "revoke")) && (2..n).any(|at| is(at, "on")) {
        let end = (2..n).find(|&at| is(at, "on")).unwrap_or(n);
        let mut temp = None;
        let mut depth = 0;
        for at in 1..end {
            match statement[significant[at]].token {
                Token::LParen => depth += 1,
                Token::RParen => depth -= 1,
                _ if depth == 0 && is(at, "temp") => temp = Some(significant[at]),
                _ => {}
            }
        }
        if let Some(at) = temp
            && let Some(tokens) = snippet_tokens("temporary")
        {
            // The word the parser wants; a space only where none follows.
            let space = !matches!(
                statement.get(at + 1).map(|token| &token.token),
                Some(Token::Whitespace(_))
            );
            let mut out = Vec::with_capacity(statement.len() + 2);
            out.extend_from_slice(&statement[..at]);
            out.extend(tokens);
            if space && let Some(pad) = snippet_tokens(" ") {
                out.extend(pad);
            }
            out.extend_from_slice(&statement[at + 1..]);
            return out;
        }
    }

    // `CONNECTION LIMIT -1`: the parser reads the option's number without a
    // sign, so the minus folds into the literal.
    if (is(0, "create") || is(0, "alter")) && (is(1, "role") || is(1, "user")) {
        let mut fold = None;
        for at in 0..significant.len().saturating_sub(3) {
            if !(is(at, "connection") && is(at + 1, "limit")) {
                continue;
            }
            if statement[significant[at + 2]].token != Token::Minus {
                continue;
            }
            if let Token::Number(text, long) = &statement[significant[at + 3]].token {
                fold = Some((
                    significant[at + 2],
                    significant[at + 3],
                    format!("-{text}"),
                    *long,
                ));
            }
        }
        if let Some((minus_at, number_at, text, long)) = fold {
            let mut out = statement.clone();
            out[number_at].token = Token::Number(text, long);
            out.remove(minus_at);
            return out;
        }
    }

    // `GRANT role [, ...] TO member [, ...] [WITH ADMIN OPTION]` and
    // `REVOKE [ADMIN OPTION FOR] role [, ...] FROM member [, ...]`. A
    // privilege grant always names objects with `ON`.
    if (is(0, "grant") || is(0, "revoke")) && !(2..n).any(|k| is(k, "on")) {
        let separator = if is(0, "grant") { "to" } else { "from" };
        if let Some(split) = (1..n).find(|&k| is(k, separator)) {
            let mode = if is(0, "grant") {
                let admin = (split..n).any(|k| is(k, "with") && is(k + 1, "admin"));
                if admin { "grant_admin" } else { "grant" }
            } else {
                let admin_only = is(1, "admin") && is(2, "option");
                if admin_only { "revoke_admin" } else { "revoke" }
            };
            let names_at = if is(0, "grant") {
                1
            } else if is(1, "admin") {
                4
            } else {
                1
            };
            let roles = render_tokens(&statement[significant[names_at]..=significant[split - 1]]);
            let tail_end = if is(0, "grant") {
                (split + 1..n)
                    .find(|&k| is(k, "with"))
                    .map_or(n - 1, |k| k - 1)
            } else {
                (split + 1..n)
                    .find(|&k| is(k, "cascade") || is(k, "restrict") || is(k, "granted"))
                    .map_or(n - 1, |k| k - 1)
            };
            let members = render_tokens(&statement[significant[split + 1]..=significant[tail_end]]);
            let sql = format!(
                "SELECT {GRANT_ROLE_FUNCTION}('{}', '{}', '{mode}')",
                quote(roles.trim()),
                quote(members.trim())
            );
            if let Some(mut tokens) = snippet_tokens(&sql) {
                tokens.extend(tail(&statement));
                return tokens;
            }
            return statement;
        }
    }

    // `SET CONSTRAINTS {ALL | name [, ...]} {DEFERRED | IMMEDIATE}`.
    if is(0, "set") && is(1, "constraints") && n >= 4 {
        let mut at = 2;
        let all = is(at, "all");
        if all {
            at += 1;
        }
        let mode_at = n - 1;
        let mode = render_tokens(&statement[significant[mode_at]..=significant[mode_at]]);
        let names = if all {
            String::new()
        } else {
            render_tokens(&statement[significant[at]..=significant[mode_at - 1]])
        };
        let sql = format!(
            "SELECT {SET_CONSTRAINTS_FUNCTION}({all}, '{}', '{}')",
            quote(mode.trim()),
            quote(names.trim())
        );
        if let Some(mut tokens) = snippet_tokens(&sql) {
            tokens.extend(tail(&statement));
            return tokens;
        }
        return statement;
    }

    // `ALTER TABLE [ONLY] t ALTER CONSTRAINT name ...`.
    if is(0, "alter") && is(1, "table") && n >= 6 {
        let at = if is(2, "only") { 3 } else { 2 };
        let keyword = (at..n - 1).find(|&k| is(k, "alter") && is(k + 1, "constraint"));
        if let Some(k) = keyword {
            let name = render_tokens(&statement[significant[at]..=significant[k - 1]]);
            let constraint = match &statement[significant[k + 2]].token {
                Token::Word(w) => w.value.clone(),
                other => other.to_string(),
            };
            // The characteristics: absent ones keep PostgreSQL's defaults
            // (`NOT DEFERRABLE`, `INITIALLY IMMEDIATE`). Anything else is
            // left to the parser's own error.
            let (mut deferrable, mut initially) = (false, false);
            let mut c = k + 3;
            let mut ok = true;
            while c < n && ok {
                if is(c, "not") && is(c + 1, "deferrable") {
                    deferrable = false;
                    c += 2;
                } else if is(c, "deferrable") {
                    deferrable = true;
                    c += 1;
                } else if is(c, "initially") && is(c + 1, "deferred") {
                    deferrable = true;
                    initially = true;
                    c += 2;
                } else if is(c, "initially") && is(c + 1, "immediate") {
                    c += 2;
                } else {
                    ok = false;
                }
            }
            if !ok {
                return statement;
            }
            let sql = format!(
                "SELECT {ALTER_CONSTRAINT_FUNCTION}('{}', '{}', {deferrable}, {initially})",
                quote(name.trim()),
                quote(&constraint)
            );
            if let Some(mut tokens) = snippet_tokens(&sql) {
                tokens.extend(tail(&statement));
                return tokens;
            }
            return statement;
        }
    }

    // `ALTER TABLE [ONLY] parent ATTACH | DETACH PARTITION child ...`.
    if is(0, "alter") && is(1, "table") && n >= 5 {
        let at = if is(2, "only") { 3 } else { 2 };
        let keyword =
            (at..n - 1).find(|&k| (is(k, "attach") || is(k, "detach")) && is(k + 1, "partition"));
        if let Some(k) = keyword {
            let name = render_tokens(&statement[significant[at]..=significant[k - 1]]);
            let child_end = (k + 2..n)
                .find(|&j| {
                    is(j, "for") || is(j, "default") || is(j, "concurrently") || is(j, "finalize")
                })
                .unwrap_or(n);
            let child = render_tokens(&statement[significant[k + 2]..=significant[child_end - 1]]);
            let sql = if is(k, "attach") {
                let bound = if child_end < n {
                    render_tokens(&statement[significant[child_end]..=significant[n - 1]])
                } else {
                    String::new()
                };
                format!(
                    "SELECT {ATTACH_PARTITION_FUNCTION}('{}', '{}', '{}')",
                    quote(name.trim()),
                    quote(child.trim()),
                    quote(bound.trim())
                )
            } else {
                format!(
                    "SELECT {DETACH_PARTITION_FUNCTION}('{}', '{}')",
                    quote(name.trim()),
                    quote(child.trim())
                )
            };
            if let Some(mut tokens) = snippet_tokens(&sql) {
                tokens.extend(tail(&statement));
                return tokens;
            }
            return statement;
        }
    }

    // `ALTER TABLE [ONLY] child INHERIT | NO INHERIT parent`.
    if is(0, "alter") && is(1, "table") && n >= 4 {
        let at = if is(2, "only") { 3 } else { 2 };
        let keyword = (at..n).find(|&k| is(k, "inherit"));
        if let Some(k) = keyword {
            let attach = !(k > at && is(k - 1, "no"));
            let keyword_at = if attach { k } else { k - 1 };
            let name = render_tokens(&statement[significant[at]..=significant[keyword_at - 1]]);
            let parent = render_tokens(&statement[significant[k + 1]..=significant[n - 1]]);
            let sql = format!(
                "SELECT {INHERIT_FUNCTION}('{}', '{}', {attach})",
                quote(name.trim()),
                quote(parent.trim())
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
    #[test]
    fn negative_connection_limit_is_folded() {
        let one = |sql: &str| parse_sql(sql).unwrap().remove(0).to_string();
        assert_eq!(
            one("ALTER ROLE r CONNECTION LIMIT -1"),
            "ALTER ROLE r WITH CONNECTION LIMIT -1"
        );
        assert_eq!(
            one("ALTER ROLE r NOLOGIN CONNECTION LIMIT -1"),
            "ALTER ROLE r WITH NOLOGIN CONNECTION LIMIT -1"
        );
        assert_eq!(
            one("CREATE ROLE r CONNECTION LIMIT -1"),
            "CREATE ROLE r CONNECTION LIMIT -1"
        );
    }

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
    fn only_marks_names() {
        let one = |sql: &str| parse_sql(sql).unwrap()[0].to_string();
        assert_eq!(
            one("SELECT a FROM ONLY ip"),
            "SELECT a FROM \"\u{0}only:ip\""
        );
        assert_eq!(
            one("SELECT a FROM ONLY ip AS x"),
            "SELECT a FROM \"\u{0}only:ip\" AS x"
        );
        assert_eq!(
            one("UPDATE ONLY ip SET b = 'z'"),
            "UPDATE \"\u{0}only:ip\" SET b = 'z'"
        );
        assert_eq!(one("SELECT a FROM only"), "SELECT a FROM only");
        // A quoted `only` is a table of that name, and TRUNCATE keeps its own.
        assert_eq!(one("SELECT a FROM \"only\""), "SELECT a FROM \"only\"");
        assert_eq!(one("TRUNCATE ONLY t"), "TRUNCATE ONLY t");
        assert_eq!(
            one("ALTER TABLE ic INHERIT ip"),
            "SELECT pg_catalog.nodus_inherit('ic', 'ip', true)"
        );
        assert_eq!(
            one("ALTER TABLE ic NO INHERIT ip"),
            "SELECT pg_catalog.nodus_inherit('ic', 'ip', false)"
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

#[cfg(test)]
mod rewrite_tests {
    use crate::*;

    /// The SQL a statement is tokenized, rewritten, and rendered as.
    fn rewritten_by(
        sql: &str,
        rewriter: fn(
            Vec<sqlparser::tokenizer::TokenWithSpan>,
        ) -> Vec<sqlparser::tokenizer::TokenWithSpan>,
    ) -> String {
        let dialect = sqlparser::dialect::PostgreSqlDialect {};
        let mut tokens = sqlparser::tokenizer::Tokenizer::new(&dialect, sql)
            .tokenize_with_location()
            .expect("tokenize");
        for token in &mut tokens {
            if let sqlparser::tokenizer::Token::Word(word) = &mut token.token
                && word.quote_style.is_none()
            {
                word.value.make_ascii_lowercase();
            }
        }
        render_tokens(&rewriter(tokens))
    }

    fn rewritten(sql: &str) -> String {
        rewritten_by(sql, rewrite_json_table)
    }

    fn xmlexists_rewritten(sql: &str) -> String {
        rewritten_by(sql, rewrite_xmlexists)
    }

    fn xmltable_rewritten(sql: &str) -> String {
        rewritten_by(sql, rewrite_xmltable)
    }

    #[test]
    fn json_array_query_becomes_its_aggregate() {
        // `json_array(SELECT ...)` is the array aggregate over the
        // subquery's one column, with `ABSENT ON NULL` and the clauses.
        let constructors = |sql: &str| rewritten_by(sql, rewrite_json_constructors);
        assert_eq!(
            constructors("select json_array(select a from t order by a)"),
            "select (select pg_catalog.__json_array_query__(q.a, true, '') from \
             (select a from t order by a) as q(a))"
        );
        assert!(
            constructors("select json_array(select a from t format json returning jsonb)")
                .contains("pg_catalog.__json_format__(q.a, false), true, 'jsonb'")
        );
    }

    #[test]
    fn xmltable_becomes_its_markers() {
        assert_eq!(
            xmltable_rewritten(
                "select * from xmltable('/a/b' passing '<a><b>1</b></a>' columns v int path '.')"
            ),
            "select * from pg_catalog.__xml_table__( '<a><b>1</b></a>' , '/a/b' , '', \
             pg_catalog.__xt_col__('v', 'int', '.', NULL, false))"
        );
        assert_eq!(
            xmltable_rewritten(
                "select * from xmltable(xmlnamespaces('urn:x' as x), '/x:a' passing by value \
                 '<x:a/>' columns n for ordinality, v text path 'b' default 'd' not null)"
            ),
            "select * from pg_catalog.__xml_table__('<x:a/>' ,  '/x:a' , '', \
             pg_catalog.__xt_col_ordinality__('n'), pg_catalog.__xt_col__('v', 'text', 'b' , 'd' , \
             true), pg_catalog.__xt_ns__('x', 'urn:x' ))"
        );
    }

    #[test]
    fn xmltable_refuses_a_bad_column_when_evaluated() {
        // A clause PostgreSQL's grammar refuses when it reads the column
        // travels as the table's `refuse`, for the planner to raise.
        assert!(
            xmltable_rewritten(
                "select * from xmltable('/a' passing '<a/>' columns v int path 'a' path 'b')"
            )
            .contains("'only one PATH value per column is allowed'")
        );
    }

    #[test]
    fn xmlexists_becomes_xpath_exists() {
        assert_eq!(
            xmlexists_rewritten("select xmlexists('/a' passing '<a/>')"),
            "select pg_catalog.__xmlexists__('/a' ,  '<a/>')"
        );
        assert_eq!(
            xmlexists_rewritten("select xmlexists('/a' passing by value '<a/>' by value)"),
            "select pg_catalog.__xmlexists__('/a' ,  '<a/>')"
        );
        assert_eq!(
            xmlexists_rewritten(
                "select xmlexists(xmlnamespaces('urn:x' as x), '/x:a' passing '<x:a xmlns:x=\"urn:x\"/>')"
            ),
            "select pg_catalog.__xmlexists__('/x:a' ,  '<x:a xmlns:x=\"urn:x\"/>', \
             ARRAY[ARRAY['x', 'urn:x']])"
        );
    }

    #[test]
    fn json_table_becomes_its_markers() {
        assert_eq!(
            rewritten("select * from json_table('{\"a\":1}', '$' columns (a int))"),
            "select * from pg_catalog.__json_table__('{\"a\":1}', '$' , '', NULL, \
             pg_catalog.__jt_col_scalar__('a', 'int', false, false, '$.a', '', '', '', \
             NULL, '', NULL))"
        );
        // A column's clauses reach the marker; the `PASSING` variables become
        // the `__json_vars__` call the query functions take.
        let sql = rewritten(
            "select * from json_table(doc, '$.x[*]' passing 1 as n columns (\
             ord for ordinality, v int path '$.v' default 0 on empty, \
             e boolean exists path '$.e'))",
        );
        assert!(sql.contains("pg_catalog.__json_vars__('n', 1"), "{sql}");
        assert!(
            sql.contains("pg_catalog.__jt_col_ordinality__('ord')"),
            "{sql}"
        );
        assert!(sql.contains("'default',  0 , '', NULL)"), "{sql}");
        assert!(
            sql.contains("pg_catalog.__jt_col_exists__('e', 'boolean', '$.e', '')"),
            "{sql}"
        );
    }

    #[test]
    fn json_table_nested_paths_become_their_markers() {
        let sql = rewritten(
            "select * from json_table(doc, '$' columns (nested path '$.n[*]' as g \
             columns (w text)))",
        );
        assert!(
            sql.contains(
                "pg_catalog.__jt_nested__('$.n[*]' , \
                 pg_catalog.__jt_col_scalar__('w', 'text', false, false, '$.w',"
            ),
            "{sql}"
        );
    }
}
