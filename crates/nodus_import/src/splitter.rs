//! Streaming, line-aware tokenizer that splits a plain-format `pg_dump` script
//! into a flat sequence of [`RawUnit`]s without fully parsing SQL.
//!
//! It is *not* a SQL parser: it only tracks enough lexical state (string
//! literals, dollar-quoting, comments, statement terminators) to slice the dump
//! into statements, `COPY ... FROM stdin` data blocks, and `psql` meta lines.
//! Everything it yields borrows from the input, so splitting allocates only the
//! `Vec` of slice references — never a copy of the dump body.

/// A lexical unit of a dump: a SQL statement, a `COPY FROM STDIN` block (header
/// plus its raw tab-separated body), or a `psql` backslash meta line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawUnit<'a> {
    /// A single SQL statement, trimmed and stripped of its trailing `;`.
    Statement(&'a str),
    /// A `COPY <table> (...) FROM stdin;` header and the raw data block that
    /// follows it (excluding the terminating `\.` line).
    Copy { header: &'a str, body: &'a str },
    /// A `psql` meta-command line (e.g. `\connect`, `\.`), trimmed.
    Meta(&'a str),
}

/// Returns `true` if `stmt` is a `COPY ... FROM STDIN` statement, whose data is
/// streamed inline after the header rather than expressed as SQL.
pub fn is_copy_from_stdin(stmt: &str) -> bool {
    let upper = stmt.trim_start().to_ascii_uppercase();
    upper.starts_with("COPY ") && upper.contains(" FROM STDIN")
}

/// Splits a plain-format dump into its lexical units in source order.
pub fn split(dump: &str) -> Vec<RawUnit<'_>> {
    let bytes = dump.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;
    let mut units = Vec::new();

    while i < n {
        // Skip inter-statement whitespace and comments.
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if c == b'-' && i + 1 < n && bytes[i + 1] == b'-' {
            i = skip_to_line_end(bytes, i);
            continue;
        }
        if c == b'/' && i + 1 < n && bytes[i + 1] == b'*' {
            i = skip_block_comment(bytes, i + 2);
            continue;
        }
        // A backslash at statement-start is a psql meta line (\connect, \., ...).
        if c == b'\\' {
            let start = i;
            i = skip_to_line_end(bytes, i);
            units.push(RawUnit::Meta(dump[start..i].trim()));
            continue;
        }

        // Scan a full statement up to a `;` at depth zero.
        let stmt_start = i;
        i = scan_statement_end(bytes, i);
        let raw = dump[stmt_start..i].trim();
        let stmt = raw.trim_end_matches(';').trim();
        if stmt.is_empty() {
            continue;
        }

        if is_copy_from_stdin(stmt) {
            // Consume to end of the header line, then capture raw rows until a
            // lone `\.` terminator line.
            i = skip_to_line_end(bytes, i);
            if i < n {
                i += 1; // step over the newline after the header
            }
            let body_start = i;
            let (body_end, next) = find_copy_terminator(bytes, i);
            units.push(RawUnit::Copy {
                header: stmt,
                body: &dump[body_start..body_end],
            });
            i = next;
        } else {
            units.push(RawUnit::Statement(stmt));
        }
    }

    units
}

fn skip_to_line_end(bytes: &[u8], mut i: usize) -> usize {
    while i < bytes.len() && bytes[i] != b'\n' {
        i += 1;
    }
    i
}

/// Skips a `/* ... */` comment body (handles PostgreSQL's nested comments).
/// `i` points just past the opening `/*`.
fn skip_block_comment(bytes: &[u8], mut i: usize) -> usize {
    let n = bytes.len();
    let mut depth = 1usize;
    while i < n && depth > 0 {
        if i + 1 < n && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            depth += 1;
            i += 2;
        } else if i + 1 < n && bytes[i] == b'*' && bytes[i + 1] == b'/' {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// Scans from the start of a statement and returns the index just past the
/// terminating `;` at lexical depth zero (or end of input). Tracks single- and
/// double-quoted strings, dollar-quoted bodies, and comments so that `;`
/// characters inside them are not treated as terminators.
fn scan_statement_end(bytes: &[u8], mut i: usize) -> usize {
    let n = bytes.len();
    while i < n {
        match bytes[i] {
            b';' => return i + 1,
            b'\'' => i = skip_quoted(bytes, i, b'\''),
            b'"' => i = skip_quoted(bytes, i, b'"'),
            b'$' => {
                if let Some((next, _tag_len)) = dollar_tag(bytes, i) {
                    i = skip_dollar_body(bytes, next, i);
                } else {
                    i += 1;
                }
            }
            b'-' if i + 1 < n && bytes[i + 1] == b'-' => i = skip_to_line_end(bytes, i),
            b'/' if i + 1 < n && bytes[i + 1] == b'*' => i = skip_block_comment(bytes, i + 2),
            _ => i += 1,
        }
    }
    n
}

/// Skips a quoted string starting at the opening `quote` byte, honoring the
/// SQL doubled-quote escape (`''` / `""`). Returns the index past the close.
fn skip_quoted(bytes: &[u8], mut i: usize, quote: u8) -> usize {
    let n = bytes.len();
    i += 1; // opening quote
    while i < n {
        if bytes[i] == quote {
            if i + 1 < n && bytes[i + 1] == quote {
                i += 2; // doubled quote escape
            } else {
                return i + 1;
            }
        } else {
            i += 1;
        }
    }
    n
}

/// If `i` points at the start of a dollar-quote tag (`$tag$` or `$$`), returns
/// the index just past the opening tag and the tag length; otherwise `None`.
fn dollar_tag(bytes: &[u8], i: usize) -> Option<(usize, usize)> {
    let n = bytes.len();
    debug_assert_eq!(bytes[i], b'$');
    let mut j = i + 1;
    while j < n && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    if j < n && bytes[j] == b'$' {
        Some((j + 1, j + 1 - i))
    } else {
        None
    }
}

/// Skips a dollar-quoted body. `body_start` is just past the opening tag and
/// `tag_start` points at the opening `$`, used to match the closing tag.
fn skip_dollar_body(bytes: &[u8], body_start: usize, tag_start: usize) -> usize {
    let n = bytes.len();
    let tag = &bytes[tag_start..body_start]; // includes both `$`
    let mut i = body_start;
    while i < n {
        if bytes[i] == b'$' && i + tag.len() <= n && &bytes[i..i + tag.len()] == tag {
            return i + tag.len();
        }
        i += 1;
    }
    n
}

/// Finds the `\.` end-of-data marker for a COPY block. Returns the byte index
/// where the data body ends (start of the terminator line) and the index to
/// resume scanning after the terminator line.
fn find_copy_terminator(bytes: &[u8], start: usize) -> (usize, usize) {
    let n = bytes.len();
    let mut line_start = start;
    while line_start < n {
        let mut line_end = line_start;
        while line_end < n && bytes[line_end] != b'\n' {
            line_end += 1;
        }
        let line = &bytes[line_start..line_end];
        if line == b"\\." || line == b"\\.\r" {
            let next = if line_end < n { line_end + 1 } else { line_end };
            return (line_start, next);
        }
        if line_end >= n {
            break;
        }
        line_start = line_end + 1;
    }
    (n, n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_simple_statements() {
        let units = split("CREATE TABLE t (a int);\nINSERT INTO t VALUES (1);\n");
        assert_eq!(
            units,
            vec![
                RawUnit::Statement("CREATE TABLE t (a int)"),
                RawUnit::Statement("INSERT INTO t VALUES (1)"),
            ]
        );
    }

    #[test]
    fn ignores_semicolons_inside_strings_and_dollar_quotes() {
        let units = split("INSERT INTO t VALUES ('a;b');\nSELECT $$x;y$$;\n");
        assert_eq!(
            units,
            vec![
                RawUnit::Statement("INSERT INTO t VALUES ('a;b')"),
                RawUnit::Statement("SELECT $$x;y$$"),
            ]
        );
    }

    #[test]
    fn handles_dollar_quote_with_tag() {
        let units = split("SELECT $tag$ body; with ; semis $tag$ ;");
        assert_eq!(
            units,
            vec![RawUnit::Statement("SELECT $tag$ body; with ; semis $tag$")]
        );
    }

    #[test]
    fn strips_line_and_block_comments_between_statements() {
        let dump = "-- a comment\n/* block ; comment */\nSELECT 1;";
        assert_eq!(split(dump), vec![RawUnit::Statement("SELECT 1")]);
    }

    #[test]
    fn captures_copy_block_body_until_terminator() {
        let dump = "COPY t (a, b) FROM stdin;\n1\talpha\n2\tbeta\n\\.\nSELECT 1;\n";
        let units = split(dump);
        assert_eq!(
            units,
            vec![
                RawUnit::Copy {
                    header: "COPY t (a, b) FROM stdin",
                    body: "1\talpha\n2\tbeta\n",
                },
                RawUnit::Statement("SELECT 1"),
            ]
        );
    }

    #[test]
    fn surfaces_backslash_meta_lines() {
        let units = split("\\connect mydb\nSELECT 1;");
        assert_eq!(
            units,
            vec![
                RawUnit::Meta("\\connect mydb"),
                RawUnit::Statement("SELECT 1"),
            ]
        );
    }

    #[test]
    fn doubled_quote_escape_does_not_end_string() {
        let units = split("INSERT INTO t VALUES ('it''s; fine');");
        assert_eq!(
            units,
            vec![RawUnit::Statement("INSERT INTO t VALUES ('it''s; fine')")]
        );
    }
}
