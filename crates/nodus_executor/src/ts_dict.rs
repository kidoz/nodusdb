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
    let mut words: Vec<(String, usize)> = Vec::new();
    let mut pos = 0usize;
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
    words
}

/// `to_tsvector`: parses a text and turns it into a vector under the
/// configuration, as a canonical tsvector.
pub(crate) fn to_tsvector(config: Config, text: &str) -> String {
    let mut words = lexize_words(config, text);

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
