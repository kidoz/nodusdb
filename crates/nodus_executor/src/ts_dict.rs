//! The text search dictionary layer: PostgreSQL's `simple` and `english`
//! configurations, as `to_tsvector` applies them. Each parser token is
//! mapped to its dictionary by token type, the dictionary turns it into
//! lexemes (lowercasing, dropping stop words, and stemming for English), and
//! the lexemes with their positions become the vector.

use crate::ts_parse;

/// The maximum length of a lexeme, as PostgreSQL's `MAXSTRLEN`.
const MAXSTRLEN: usize = 2047;
/// The most positions one lexeme keeps.
const MAXNUMPOS: usize = 256;
/// The largest position a lexeme can hold (`MAXENTRYPOS`).
const MAXENTRYPOS: usize = 16384;
/// The longest string the stemmer is handed (`dsnowball_lexize`).
const STEM_LIMIT: usize = 1000;

/// The text search configurations NodusDB provides.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Config {
    /// Lowercase only.
    Simple,
    /// The English Snowball stemmer with its stop-word list.
    English,
}

impl Config {
    /// The configuration a name denotes, as PostgreSQL's regconfig lookup
    /// finds it (`english`, `pg_catalog.english`, ...).
    pub(crate) fn of(name: &str) -> Option<Config> {
        let bare = name.trim();
        let bare = bare
            .strip_prefix("pg_catalog.")
            .or_else(|| bare.strip_prefix("public."))
            .unwrap_or(bare);
        if bare.eq_ignore_ascii_case("simple") {
            return Some(Config::Simple);
        }
        if bare.eq_ignore_ascii_case("english") {
            return Some(Config::English);
        }
        None
    }

    /// The name PostgreSQL prints for it.
    pub(crate) fn name(self) -> &'static str {
        match self {
            Config::Simple => "simple",
            Config::English => "english",
        }
    }

    /// The configuration's OID.
    pub(crate) fn oid(self) -> i64 {
        match self {
            Config::Simple => 3748,
            Config::English => 13282,
        }
    }

    /// The name of the dictionary it stands for (`pg_ts_dict`): the simple
    /// dictionary, or the English Snowball dictionary.
    pub(crate) fn dictionary_name(self) -> &'static str {
        match self {
            Config::Simple => "simple",
            Config::English => "english_stem",
        }
    }

    /// The dictionary's OID.
    pub(crate) fn dictionary_oid(self) -> i64 {
        match self {
            Config::Simple => 3765,
            Config::English => 13281,
        }
    }
}

/// The dictionary a `regdictionary` input string names, as PostgreSQL's
/// input function finds it.
pub(crate) fn config_of_dictionary_name(name: &str) -> Result<Config, String> {
    let Some(parts) = split_identifier_string(name) else {
        return Err(crate::error_fields::DbError::new("invalid name syntax")
            .code("42602")
            .into_text());
    };
    let joined = parts.join(".");
    let bare = joined.strip_prefix("pg_catalog.").unwrap_or(&joined);
    match bare {
        "simple" => Ok(Config::Simple),
        "english_stem" => Ok(Config::English),
        _ => Err(crate::error_fields::DbError::new(format!(
            "text search dictionary \"{joined}\" does not exist"
        ))
        .code("42704")
        .into_text()),
    }
}

/// The dictionary an OID names, or `None` as PostgreSQL's failed cache
/// lookup.
pub(crate) fn dictionary_by_oid(oid: i64) -> Option<Config> {
    match oid {
        3765 => Some(Config::Simple),
        13281 => Some(Config::English),
        _ => None,
    }
}

/// The dictionaries a token type is mapped to by a configuration, in order
/// (`pg_ts_config_map`): none for a type the configuration leaves alone.
pub(crate) fn token_dictionaries(config: Config, ty: u8) -> Vec<Config> {
    match dictionary_for(config, ty) {
        Some(dictionary) => vec![dictionary],
        None => Vec::new(),
    }
}

/// `pg_ts_config_map`: the configurations' token type to dictionary rows,
/// as (configuration OID, token type, dictionary OID).
pub(crate) fn config_map_rows() -> Vec<(i64, u8, i64)> {
    let mut rows = Vec::new();
    for config in [Config::Simple, Config::English] {
        for (ty, ..) in crate::ts_parse::token_types() {
            for dictionary in token_dictionaries(config, ty) {
                rows.push((config.oid(), ty, dictionary.dictionary_oid()));
            }
        }
    }
    rows
}

/// The configuration a `regconfig` value names: an OID, or a name (an
/// untyped literal resolves like a name).
pub(crate) fn config_value(value: &crate::value::Value) -> Result<Config, String> {
    use crate::value::Value;
    match value {
        Value::Int(oid) => config_by_oid(*oid)
            .ok_or_else(|| format!("cache lookup failed for text search configuration {oid}")),
        Value::Text(name) => config_of_qualified_name(name),
        other => Err(format!(
            "cannot cast {} to regconfig",
            crate::value::value_type_name(other)
        )),
    }
}

/// The configuration an OID names, or `None`.
pub(crate) fn config_by_oid(oid: i64) -> Option<Config> {
    match oid {
        3748 => Some(Config::Simple),
        13282 => Some(Config::English),
        _ => None,
    }
}

/// `ts_lexize`: the dictionary's lexemes for a token — an empty list for a
/// stop word (or the empty token), the token itself where a Snowball
/// dictionary refuses to stem it, and the lexeme otherwise.
pub(crate) fn lexize_token(config: Config, token: &str) -> Vec<String> {
    match config {
        Config::Simple => {
            if token.is_empty() {
                Vec::new()
            } else {
                vec![ascii_lower(token)]
            }
        }
        Config::English => snowball_lexize(token),
    }
}

/// `SplitIdentifierString`: PostgreSQL's qualified-name syntax, as the
/// `regconfig` input function reads it. Leading and trailing whitespace is
/// skipped, parts are separated by dots, unquoted parts are downcased and
/// end at a dot or whitespace, and a quoted part keeps its case and may
/// hold any character (`""` is one quote). `None` is PostgreSQL's
/// "invalid name syntax".
pub(crate) fn split_identifier_string(raw: &str) -> Option<Vec<String>> {
    fn space(c: u8) -> bool {
        c == b' ' || (0x09..=0x0d).contains(&c)
    }
    let bytes = raw.as_bytes();
    let mut parts = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() && space(bytes[i]) {
        i += 1;
    }
    if i >= bytes.len() {
        return None; // the empty string is no name
    }
    loop {
        let part: String;
        if bytes[i] == b'"' {
            let mut text = Vec::new();
            i += 1;
            loop {
                if i >= bytes.len() {
                    return None; // mismatched quotes
                }
                if bytes[i] == b'"' {
                    if i + 1 < bytes.len() && bytes[i + 1] == b'"' {
                        text.push(b'"');
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    text.push(bytes[i]);
                    i += 1;
                }
            }
            part = String::from_utf8_lossy(&text).into_owned();
        } else {
            let start = i;
            while i < bytes.len() && bytes[i] != b'.' && !space(bytes[i]) {
                i += 1;
            }
            if i == start {
                return None; // an empty unquoted name
            }
            part = ascii_lower(&raw[start..i]);
        }
        parts.push(part);
        while i < bytes.len() && space(bytes[i]) {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b'.' {
            i += 1;
            while i < bytes.len() && space(bytes[i]) {
                i += 1;
            }
            if i >= bytes.len() {
                return None; // an empty item after the dot
            }
        } else if i >= bytes.len() {
            break;
        } else {
            return None; // unexpected junk after the name
        }
    }
    Some(parts)
}

/// The configuration a `regconfig` input string names, as PostgreSQL's
/// input function finds it: the name's syntax is checked first, unquoted
/// parts are downcased, and only the shipped configurations resolve.
pub(crate) fn config_of_qualified_name(name: &str) -> Result<Config, String> {
    let Some(parts) = split_identifier_string(name) else {
        return Err(crate::error_fields::DbError::new("invalid name syntax")
            .code("42602")
            .into_text());
    };
    let joined = parts.join(".");
    let bare = joined.strip_prefix("pg_catalog.").unwrap_or(&joined);
    match bare {
        "simple" => Ok(Config::Simple),
        "english" => Ok(Config::English),
        _ => Err(crate::error_fields::DbError::new(format!(
            "text search configuration \"{joined}\" does not exist"
        ))
        .code("42704")
        .into_text()),
    }
}

/// The English stop words (`english.stop`), sorted for lookup.
const ENGLISH_STOP: &str = include_str!("english_stop.txt");

fn is_stopword(word: &str) -> bool {
    ENGLISH_STOP.lines().any(|line| line == word)
}

/// PostgreSQL's `str_tolower` under the C collation: ASCII letters lower.
pub(crate) fn ascii_lower(text: &str) -> String {
    text.to_ascii_lowercase()
}

/// The lexemes one parser token leaves, or `None` when its type has no
/// mapping (so it leaves no trace and takes no position).
pub(crate) fn token_lexemes(config: Config, ty: u8, text: &str) -> Option<Vec<String>> {
    let dict = dictionary_for(config, ty)?;
    Some(match dict {
        Config::Simple => vec![ascii_lower(text)],
        Config::English => snowball_lexize(text),
    })
}

/// Which dictionary a token type is mapped to, per configuration. `None`
/// means the type has no mapping (a blank, a tag, ...), so the token leaves
/// no trace and takes no position.
fn dictionary_for(config: Config, ty: u8) -> Option<Config> {
    use ts_parse::*;
    if config == Config::Simple {
        return match ty {
            SPACE | TAG_T | PROTOCOL | XMLENTITY => None,
            _ => Some(Config::Simple),
        };
    }
    match ty {
        // The word-like tokens are stemmed.
        ASCIIWORD | WORD_T | ASCIIHWORD | HWORD | ASCIIPARTHWORD | PARTHWORD => {
            Some(Config::English)
        }
        // Everything else a dictionary accepts is only lowercased.
        NUMWORD | NUMHWORD | NUMPARTHWORD | EMAIL | URL_T | HOST | SCIENTIFIC | VERSIONNUMBER
        | URLPATH | FILEPATH | DECIMAL_T | SIGNEDINT | UNSIGNEDINT => Some(Config::Simple),
        _ => None,
    }
}

/// `dsnowball_lexize`: the English stemmer dictionary.
fn snowball_lexize(word: &str) -> Vec<String> {
    let lowered = ascii_lower(word);
    if lowered.is_empty() || is_stopword(&lowered) {
        // Recognized as a stop word: no lexeme, but the position counts.
        return Vec::new();
    }
    if word.len() > STEM_LIMIT {
        // Too long to be a word: lowercased, but not stemmed.
        return vec![lowered];
    }
    vec![crate::english_stem::stem(&lowered)]
}

/// `LIMITPOS`.
fn limitpos(pos: usize) -> u16 {
    pos.min(MAXENTRYPOS - 1) as u16
}

/// The lexemes a text leaves under the configuration, each with its
/// (unlimited) position: the dictionary layer of `parsetext`.
pub(crate) fn lexize_words(config: Config, text: &str) -> Vec<(String, usize)> {
    let mut words = Vec::new();
    lexize_into(config, text, 0, &mut words);
    words
}

/// Appends a text's lexemes to `words`, their positions numbered from
/// `start`, and returns the next position.
pub(crate) fn lexize_into(
    config: Config,
    text: &str,
    start: usize,
    words: &mut Vec<(String, usize)>,
) -> usize {
    let mut pos = start;
    for token in ts_parse::parse(text) {
        let Some(dict) = dictionary_for(config, token.ty) else {
            continue;
        };
        if token.text.len() >= MAXSTRLEN {
            crate::session_env::notice(
                crate::error_fields::DbError::new("word is too long to be indexed")
                    .detail(format!(
                        "Words longer than {} characters are ignored.",
                        MAXSTRLEN
                    ))
                    .code("54000"),
            );
            continue;
        }
        let lexemes = match dict {
            Config::Simple => vec![ascii_lower(&token.text)],
            Config::English => snowball_lexize(&token.text),
        };
        pos += 1;
        for lexeme in lexemes {
            words.push((lexeme, pos));
        }
    }
    pos
}

/// `to_tsvector`: parses a text and turns it into a vector under the
/// configuration, as a canonical tsvector.
pub(crate) fn to_tsvector(config: Config, text: &str) -> String {
    build_vector(lexize_words(config, text))
}

/// The vector the collected words form, as a canonical tsvector.
pub(crate) fn build_vector(mut words: Vec<(String, usize)>) -> String {
    // `uniqueWORD`: sort by word and position, then merge each word's
    // positions, dropping duplicates and capping both counts.
    words.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let mut entries: Vec<crate::textsearch::Entry> = Vec::new();
    for (word, raw_pos) in words {
        let capped = limitpos(raw_pos);
        match entries.last_mut() {
            Some(entry) if entry.lexeme == word => {
                let count = entry.positions.len();
                if count < MAXNUMPOS - 1
                    && entry.positions.last().map(|p| p.value) != Some(MAXENTRYPOS as u16 - 1)
                    && entry.positions.last().map(|p| p.value) != Some(capped)
                {
                    entry.positions.push(crate::textsearch::Pos {
                        value: capped,
                        weight: 0,
                    });
                }
            }
            _ => entries.push(crate::textsearch::Entry {
                lexeme: word,
                positions: vec![crate::textsearch::Pos {
                    value: capped,
                    weight: 0,
                }],
            }),
        }
    }
    crate::textsearch::print_tsvector(&crate::textsearch::TsVector { entries })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_to_tsvector_matches_postgresql() {
        let doc = |text: &str| serde_json::from_str::<serde_json::Value>(text).expect("json");
        let dog = doc(r#"{"a": "The Fat Cats", "b": ["dogs", 42, true, null]}"#);
        assert_eq!(
            jsonb_to_tsvector(Config::English, &dog, JTI_STRING),
            "'cat':3 'dog':5 'fat':2"
        );
        assert_eq!(
            jsonb_to_tsvector(Config::English, &dog, JTI_STRING | JTI_NUMERIC),
            "'42':7 'cat':3 'dog':5 'fat':2"
        );
        assert_eq!(
            jsonb_to_tsvector(
                Config::English,
                &doc(r#"{"a": "fat", "b": 42}"#),
                JTI_STRING | JTI_NUMERIC
            ),
            "'42':3 'fat':1"
        );
        assert_eq!(
            jsonb_to_tsvector(
                Config::English,
                &doc(r#"{"fat cats": "dogs"}"#),
                JTI_KEY | JTI_STRING
            ),
            "'cat':2 'dog':4 'fat':1"
        );
        assert_eq!(
            jsonb_to_tsvector(
                Config::English,
                &doc(r#"{"a": 1.5, "b": false}"#),
                JTI_NUMERIC | JTI_BOOL
            ),
            "'1.5':1 'fals':3"
        );
        assert_eq!(
            jsonb_to_tsvector(Config::English, &doc("[1, 2]"), JTI_NUMERIC),
            "'1':1 '2':3"
        );
        assert_eq!(
            jsonb_to_tsvector(
                Config::English,
                &doc(r#"{"a": "x", "b": [1, "y", {"c": "z"}]}"#),
                JTI_STRING
            ),
            "'x':1 'y':3 'z':5"
        );
        // A jsonb object visits its keys in jsonb's order.
        assert_eq!(
            jsonb_to_tsvector(Config::English, &doc(r#"{"b": "x", "a": "y"}"#), JTI_STRING),
            "'x':3 'y':1"
        );
        assert_eq!(
            jsonb_to_tsvector(Config::English, &doc(r#"{"a": "x"}"#), JTI_ALL),
            "'x':2"
        );
        // A `json` document keeps its own order, and a stop word's position
        // is consumed without an element break.
        assert_eq!(
            json_to_tsvector(
                Config::English,
                r#"{"b": "x", "a": "y"}"#,
                JTI_KEY | JTI_STRING
            )
            .expect("parses"),
            "'b':1 'x':3 'y':6"
        );
        assert_eq!(
            parse_index_flags(&doc(r#"["string", "NUMERIC"]"#)).expect("flags"),
            JTI_STRING | JTI_NUMERIC
        );
        assert_eq!(parse_index_flags(&doc(r#""all""#)).expect("flags"), JTI_ALL);
        assert!(
            parse_index_flags(&doc(r#"["nope"]"#))
                .unwrap_err()
                .contains("wrong flag in flag array: \"nope\"")
        );
        assert!(
            parse_index_flags(&doc("[1]"))
                .unwrap_err()
                .contains("flag array element is not a string")
        );
        assert!(
            check_flags_shape(&doc("{}"))
                .unwrap_err()
                .contains("wrong flag type, only arrays and scalars are allowed")
        );
    }

    #[test]
    fn dictionaries_and_configs_match_postgresql() {
        // The dictionary lookups and their lexize rules.
        assert_eq!(config_of_dictionary_name("simple"), Ok(Config::Simple));
        assert_eq!(
            config_of_dictionary_name("pg_catalog.english_stem"),
            Ok(Config::English)
        );
        assert!(
            config_of_dictionary_name("nope")
                .unwrap_err()
                .contains("text search dictionary \"nope\" does not exist")
        );
        assert!(
            config_of_dictionary_name("a b")
                .unwrap_err()
                .contains("invalid name syntax")
        );
        assert_eq!(
            lexize_token(Config::Simple, "Cats"),
            vec!["cats".to_string()]
        );
        assert_eq!(
            lexize_token(Config::English, "cats"),
            vec!["cat".to_string()]
        );
        assert!(lexize_token(Config::English, "the").is_empty());
        assert!(lexize_token(Config::Simple, "").is_empty());
        // The configuration input function checks the name's syntax.
        assert_eq!(config_of_qualified_name("simple"), Ok(Config::Simple));
        assert_eq!(config_of_qualified_name("SIMPLE"), Ok(Config::Simple));
        assert_eq!(
            config_of_qualified_name("pg_catalog.english"),
            Ok(Config::English)
        );
        // A quoted name keeps its case: "simple" resolves, "SIMPLE" does not.
        assert_eq!(config_of_qualified_name("\"simple\""), Ok(Config::Simple));
        assert!(
            config_of_qualified_name("\"SIMPLE\"")
                .unwrap_err()
                .contains("text search configuration \"SIMPLE\" does not exist")
        );
        assert!(
            config_of_qualified_name("public.simple")
                .unwrap_err()
                .contains("text search configuration \"public.simple\" does not exist")
        );
        assert!(
            config_of_qualified_name("a fat cat")
                .unwrap_err()
                .contains("invalid name syntax")
        );
        // Junk after an unquoted name is a syntax error, not a lookup.
        assert!(
            config_of_qualified_name("SIMPLE x")
                .unwrap_err()
                .contains("invalid name syntax")
        );
        // The configuration map's rows are the catalog's.
        let rows = config_map_rows();
        assert_eq!(rows.len(), 19 + 19);
        assert_eq!(rows[0], (3748, 1, 3765));
        assert!(rows.contains(&(13282, 1, 13281)));
        assert!(rows.contains(&(13282, 3, 3765)));
    }

    #[test]
    fn to_tsvector_matches_postgresql() {
        assert_eq!(
            to_tsvector(Config::Simple, "FOO 42 V1.2 a-café-x"),
            "'42':2 'a':5 'a-café-x':4 'café':6 'foo':1 'v1.2':3 'x':7"
        );
        assert_eq!(
            to_tsvector(Config::English, "The Fat Cats are eating rats"),
            "'cat':3 'eat':5 'fat':2 'rat':6"
        );
        assert_eq!(
            to_tsvector(Config::English, "fat cats ate rats"),
            "'ate':3 'cat':2 'fat':1 'rat':4"
        );
        assert_eq!(
            to_tsvector(
                Config::English,
                "The Quick-Brown FOXes jumped-over 42 LAZY dogs"
            ),
            "'42':9 'brown':4 'dog':11 'fox':5 'jump':7 'jumped-ov':6 'lazi':10 'quick':3 \
             'quick-brown':2"
        );
        assert_eq!(
            to_tsvector(
                Config::English,
                "user@example.com http://example.com/path 3.5e-2 v1.2.3"
            ),
            "'/path':4 '3.5e-2':5 'example.com':3 'example.com/path':2 'user@example.com':1 \
             'v1.2.3':6"
        );
        assert_eq!(to_tsvector(Config::English, ""), "");
        assert_eq!(to_tsvector(Config::English, "the are"), "");
        assert_eq!(
            to_tsvector(Config::Simple, "a b a b a"),
            "'a':1,3,5 'b':2,4"
        );
    }
}

// ------------------------------------------------------------- json values

/// The `json(b)_to_tsvector` flags, as PostgreSQL's `jti*` bits.
pub(crate) const JTI_KEY: u32 = 1 << 0;
pub(crate) const JTI_STRING: u32 = 1 << 1;
pub(crate) const JTI_NUMERIC: u32 = 1 << 2;
pub(crate) const JTI_BOOL: u32 = 1 << 3;
/// Every flag (`"all"`).
pub(crate) const JTI_ALL: u32 = JTI_KEY | JTI_STRING | JTI_NUMERIC | JTI_BOOL;

/// `parse_jsonb_index_flags`: the flags a `jsonb` value names. A scalar
/// stands for a one-element array, as jsonb represents scalars.
pub(crate) fn parse_index_flags(value: &serde_json::Value) -> Result<u32, String> {
    use serde_json::Value as J;
    let mut flags = 0;
    let elements: Vec<&J> = match value {
        J::Array(items) => items.iter().collect(),
        // A scalar reads as a one-element array.
        other => vec![other],
    };
    for element in elements {
        let J::String(name) = element else {
            return Err(db_error(
                "flag array element is not a string",
                "Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".",
            ));
        };
        if name.eq_ignore_ascii_case("all") {
            flags |= JTI_KEY | JTI_STRING | JTI_NUMERIC | JTI_BOOL;
        } else if name.eq_ignore_ascii_case("key") {
            flags |= JTI_KEY;
        } else if name.eq_ignore_ascii_case("string") {
            flags |= JTI_STRING;
        } else if name.eq_ignore_ascii_case("numeric") {
            flags |= JTI_NUMERIC;
        } else if name.eq_ignore_ascii_case("boolean") {
            flags |= JTI_BOOL;
        } else {
            return Err(db_error(
                &format!("wrong flag in flag array: \"{name}\""),
                "Possible values are: \"string\", \"numeric\", \"boolean\", \"key\", and \"all\".",
            ));
        }
    }
    Ok(flags)
}

/// The error of a bad flags value (an object or a non-string element).
fn db_error(message: &str, hint: &str) -> String {
    crate::error_fields::DbError::new(message)
        .code("22023")
        .hint(hint)
        .into_text()
}

/// The errors a badly shaped flags value raises, as PostgreSQL's iterator
/// does: an object (or any non-array, non-scalar root) is refused.
pub(crate) fn check_flags_shape(value: &serde_json::Value) -> Result<(), String> {
    if matches!(value, serde_json::Value::Object(_)) {
        return Err(crate::error_fields::DbError::new(
            "wrong flag type, only arrays and scalars are allowed",
        )
        .code("22023")
        .into_text());
    }
    Ok(())
}

/// `jsonb_to_tsvector_worker` / `json_to_tsvector_worker`: the JSON values
/// the flags name, each through the dictionary with the artificial break
/// between elements.
fn json_words(
    config: Config,
    value: &serde_json::Value,
    flags: u32,
    jsonb: bool,
    words: &mut Vec<(String, usize)>,
    pos: &mut usize,
) {
    use serde_json::Value as J;
    fn add(config: Config, text: &str, pos: &mut usize, words: &mut Vec<(String, usize)>) {
        let before = words.len();
        *pos = lexize_into(config, text, *pos, words);
        if words.len() > before {
            // An artificial break: the next element is not adjacent.
            *pos += 1;
        }
    }
    match value {
        J::Object(map) => {
            // A jsonb object visits its keys in jsonb's order (shortest
            // first, then bytewise); a `json` document keeps its own.
            let entries: Vec<(&String, &J)> = if jsonb {
                let mut sorted: Vec<(&String, &J)> = map.iter().collect();
                sorted.sort_by(|a, b| a.0.len().cmp(&b.0.len()).then_with(|| a.0.cmp(b.0)));
                sorted
            } else {
                map.iter().collect()
            };
            for (key, item) in entries {
                if flags & JTI_KEY != 0 {
                    add(config, key, pos, words);
                }
                json_words(config, item, flags, jsonb, words, pos);
            }
        }
        J::Array(items) => {
            for item in items {
                json_words(config, item, flags, jsonb, words, pos);
            }
        }
        J::String(text) => {
            if flags & JTI_STRING != 0 {
                add(config, text, pos, words);
            }
        }
        J::Number(number) => {
            if flags & JTI_NUMERIC != 0 {
                add(config, &number.to_string(), pos, words);
            }
        }
        J::Bool(b) => {
            if flags & JTI_BOOL != 0 {
                add(config, if *b { "true" } else { "false" }, pos, words);
            }
        }
        J::Null => {}
    }
}

/// `jsonb_to_tsvector_worker`: a jsonb value's vector under the flags.
pub(crate) fn jsonb_to_tsvector(config: Config, value: &serde_json::Value, flags: u32) -> String {
    let mut words = Vec::new();
    let mut pos = 0usize;
    json_words(config, value, flags, true, &mut words, &mut pos);
    build_vector(words)
}

/// `json_to_tsvector_worker`: a `json` document's vector under the flags.
/// Its numbers keep the literal spelling the document wrote.
pub(crate) fn json_to_tsvector(config: Config, text: &str, flags: u32) -> Result<String, String> {
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| json_error(text, Some(&e)))?;
    let mut words = Vec::new();
    let mut pos = 0usize;
    json_words(config, &value, flags, false, &mut words, &mut pos);
    Ok(build_vector(words))
}

/// PostgreSQL's error for a malformed `json` document.
fn json_error(text: &str, error: Option<&serde_json::Error>) -> String {
    let mut message = String::from("invalid input syntax for type json");
    if let Some(error) = error {
        // The lexer's position becomes PostgreSQL's line/context fields for
        // the common cases; the message itself is what callers match.
        let _ = text;
        message.push_str(&format!(" (byte {})", error.column()));
    }
    crate::error_fields::DbError::new(message)
        .code("22P02")
        .into_text()
}
