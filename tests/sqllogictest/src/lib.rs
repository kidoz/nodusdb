//! Minimal sqllogictest-style parser for NodusDB's PostgreSQL-compatibility
//! regression suite.
//!
//! The `.slt` dialect understood here is a deliberately small, self-authored
//! subset (NodusDB owns every case file, so the full upstream sqllogictest
//! grammar is unnecessary):
//!
//! ```text
//! # comment
//! statement ok
//! CREATE TABLE t (a INT);
//!
//! statement error
//! INSERT INTO t VALUES ('not an int');
//!
//! query I rowsort
//! SELECT a FROM t ORDER BY a;
//! ----
//! 1
//! 2
//! ```
//!
//! Records are separated by blank lines. A `query` block lists its expected
//! result rows after a `----` marker; each result row is rendered by the harness
//! as its columns joined by a single TAB, with SQL `NULL` shown as `NULL`.
//! `rowsort` on the `query` header sorts both expected and actual rows before
//! comparison, for `SELECT`s whose row order is unspecified.

use std::path::{Path, PathBuf};

/// What a record asserts about executing its SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expect {
    /// The statement must succeed.
    StatementOk,
    /// The statement must fail; if `contains` is set, the error text must
    /// include that substring.
    StatementError { contains: Option<String> },
    /// The query must succeed and return exactly `rows` (as TAB-joined,
    /// server-rendered text). When `sort` is set, rows are compared order-
    /// insensitively.
    Query { sort: bool, rows: Vec<String> },
}

/// A single parsed `.slt` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub sql: String,
    pub expect: Expect,
    /// 1-based line of the directive keyword, for diagnostics.
    pub line: usize,
}

/// Parses a `.slt` case body into its records.
pub fn parse(input: &str) -> Result<Vec<Record>, String> {
    let lines: Vec<&str> = input.lines().collect();
    let mut records = Vec::new();
    let mut i = 0;

    while i < lines.len() {
        let raw = lines[i];
        let trimmed = raw.trim();
        let directive_line = i + 1;

        // Skip blank lines and comments between records.
        if trimmed.is_empty() || trimmed.starts_with('#') {
            i += 1;
            continue;
        }

        if let Some(rest) = trimmed.strip_prefix("statement") {
            let mode = rest.trim();
            let expect = if mode == "ok" {
                Expect::StatementOk
            } else if mode == "error" || mode.starts_with("error ") {
                let contains = mode.strip_prefix("error").map(str::trim).and_then(|s| {
                    if s.is_empty() {
                        None
                    } else {
                        Some(s.to_string())
                    }
                });
                Expect::StatementError { contains }
            } else {
                return Err(format!(
                    "line {directive_line}: expected 'statement ok' or 'statement error', got '{trimmed}'"
                ));
            };

            i += 1;
            let (sql, next) = take_sql(&lines, i);
            if sql.is_empty() {
                return Err(format!("line {directive_line}: statement has no SQL"));
            }
            records.push(Record {
                sql,
                expect,
                line: directive_line,
            });
            i = next;
        } else if let Some(rest) = trimmed.strip_prefix("query") {
            let sort = rest.split_whitespace().any(|tok| tok == "rowsort");

            i += 1;
            // SQL runs until the `----` result separator.
            let mut sql_lines = Vec::new();
            let mut found_sep = false;
            while i < lines.len() {
                if lines[i].trim() == "----" {
                    found_sep = true;
                    i += 1;
                    break;
                }
                if lines[i].trim().is_empty() {
                    break;
                }
                sql_lines.push(lines[i]);
                i += 1;
            }
            if !found_sep {
                return Err(format!(
                    "line {directive_line}: query block is missing its '----' result separator"
                ));
            }
            let sql = sql_lines.join("\n").trim().to_string();
            if sql.is_empty() {
                return Err(format!("line {directive_line}: query has no SQL"));
            }

            // Expected rows run until a blank line or EOF.
            let mut rows = Vec::new();
            while i < lines.len() && !lines[i].trim().is_empty() {
                rows.push(lines[i].trim_end_matches(['\r', '\n']).to_string());
                i += 1;
            }
            records.push(Record {
                sql,
                expect: Expect::Query { sort, rows },
                line: directive_line,
            });
        } else {
            return Err(format!(
                "line {directive_line}: expected 'statement' or 'query' directive, got '{trimmed}'"
            ));
        }
    }

    Ok(records)
}

/// Collects statement SQL from `start` until a blank line or EOF, returning the
/// joined SQL and the index of the line after it.
fn take_sql(lines: &[&str], start: usize) -> (String, usize) {
    let mut i = start;
    let mut sql = Vec::new();
    while i < lines.len() && !lines[i].trim().is_empty() {
        sql.push(lines[i]);
        i += 1;
    }
    (sql.join("\n").trim().to_string(), i)
}

/// Compares expected vs actual result rows, honoring `sort`. On mismatch,
/// returns a human-readable diff.
pub fn compare(sort: bool, expected: &[String], actual: &[String]) -> Result<(), String> {
    let (mut exp, mut act) = (expected.to_vec(), actual.to_vec());
    if sort {
        exp.sort();
        act.sort();
    }
    if exp == act {
        return Ok(());
    }
    let render = |rows: &[String]| {
        if rows.is_empty() {
            "  <no rows>".to_string()
        } else {
            rows.iter()
                .map(|r| format!("  {}", r.replace('\t', " | ")))
                .collect::<Vec<_>>()
                .join("\n")
        }
    };
    Err(format!(
        "result mismatch (rowsort={sort})\nexpected ({} rows):\n{}\nactual ({} rows):\n{}",
        exp.len(),
        render(&exp),
        act.len(),
        render(&act),
    ))
}

/// Returns the sorted list of top-level `*.slt` files under `root` (does not
/// recurse, so `cases/_legacy/` is ignored).
pub fn case_paths(root: impl AsRef<Path>) -> std::io::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in std::fs::read_dir(root)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "slt") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_statement_and_query() {
        let records = parse(
            "statement ok\nCREATE TABLE t (a INT);\n\nquery I\nSELECT a FROM t;\n----\n1\n2\n",
        )
        .unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].expect, Expect::StatementOk);
        assert_eq!(records[0].sql, "CREATE TABLE t (a INT);");
        assert_eq!(
            records[1].expect,
            Expect::Query {
                sort: false,
                rows: vec!["1".to_string(), "2".to_string()],
            }
        );
    }

    #[test]
    fn parses_statement_error_with_message() {
        let records = parse("statement error duplicate\nINSERT INTO t VALUES (1);\n").unwrap();
        assert_eq!(
            records[0].expect,
            Expect::StatementError {
                contains: Some("duplicate".to_string()),
            }
        );
    }

    #[test]
    fn rowsort_flag_is_detected() {
        let records = parse("query II rowsort\nSELECT a, b FROM t;\n----\n1\t2\n").unwrap();
        match &records[0].expect {
            Expect::Query { sort, rows } => {
                assert!(sort);
                assert_eq!(rows, &vec!["1\t2".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn missing_separator_is_an_error() {
        let err = parse("query I\nSELECT 1;\n").unwrap_err();
        assert!(err.contains("----"), "{err}");
    }

    #[test]
    fn comments_and_blanks_are_skipped() {
        let records = parse("# header\n\nstatement ok\nSELECT 1;\n").unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn compare_respects_rowsort() {
        assert!(compare(true, &["1".into(), "2".into()], &["2".into(), "1".into()]).is_ok());
        assert!(compare(false, &["1".into(), "2".into()], &["2".into(), "1".into()]).is_err());
    }
}
