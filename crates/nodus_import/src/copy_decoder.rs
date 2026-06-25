//! Decoder for the data body of a `COPY <table> (...) FROM stdin` block.
//!
//! Implements PostgreSQL's text format (tab delimiter, `\N` NULL, backslash
//! escapes) and a pragmatic CSV mode (comma delimiter, double-quote quoting).
//! The header parser extracts the target table, column list, and format so the
//! driver can synthesize `INSERT` statements.

use anyhow::{Result, bail};

/// A single decoded COPY cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Null,
    Text(String),
}

/// On-the-wire COPY format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
}

/// Parsed `COPY` header: where the rows go and how they are encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySpec {
    pub table: String,
    pub columns: Vec<String>,
    pub format: CopyFormat,
}

/// Parses a `COPY <table> [(col, ...)] FROM stdin [WITH] [(options)]` header.
pub fn parse_copy_header(header: &str) -> Result<CopySpec> {
    let trimmed = header.trim();
    let rest = trimmed
        .get(4..)
        .filter(|_| trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("COPY"))
        .ok_or_else(|| anyhow::anyhow!("not a COPY statement: {header}"))?
        .trim_start();

    // Table name runs up to the column-list `(` or the FROM keyword.
    let paren = rest.find('(');
    let from = find_keyword(rest, "FROM");
    let (table_part, after_table) = match (paren, from) {
        (Some(p), Some(f)) if p < f => (&rest[..p], &rest[p..]),
        (_, Some(f)) => (&rest[..f], &rest[f..]),
        _ => bail!("COPY header missing FROM: {header}"),
    };
    let table = strip_identifier(table_part.trim());

    let mut columns = Vec::new();
    let mut tail = after_table;
    if after_table.starts_with('(') {
        let close = after_table
            .find(')')
            .ok_or_else(|| anyhow::anyhow!("unterminated COPY column list: {header}"))?;
        columns = after_table[1..close]
            .split(',')
            .map(|c| strip_identifier(c.trim()))
            .filter(|c| !c.is_empty())
            .collect();
        tail = &after_table[close + 1..];
    }

    let upper = tail.to_ascii_uppercase();
    let format = if upper.contains("FORMAT CSV") || upper.contains("CSV") {
        CopyFormat::Csv
    } else {
        CopyFormat::Text
    };

    Ok(CopySpec {
        table,
        columns,
        format,
    })
}

/// Decodes a COPY data body into rows of cells for the given format.
pub fn decode_rows(body: &str, format: CopyFormat) -> Result<Vec<Vec<Cell>>> {
    match format {
        CopyFormat::Text => Ok(decode_text(body)),
        CopyFormat::Csv => decode_csv(body),
    }
}

fn decode_text(body: &str) -> Vec<Vec<Cell>> {
    let mut rows = Vec::new();
    for line in body.split('\n') {
        // The body excludes the `\.` terminator; a trailing newline yields a
        // final empty segment that is not a row.
        if line.is_empty() {
            continue;
        }
        let line = line.strip_suffix('\r').unwrap_or(line);
        let mut row = Vec::new();
        for field in line.split('\t') {
            if field == "\\N" {
                row.push(Cell::Null);
            } else {
                row.push(Cell::Text(unescape_text(field)));
            }
        }
        rows.push(row);
    }
    rows
}

/// Applies PostgreSQL text-format backslash unescaping (`\t`, `\n`, `\r`,
/// `\\`, `\b`, `\f`, `\v`, octal `\NNN`, hex `\xHH`).
fn unescape_text(field: &str) -> String {
    if !field.contains('\\') {
        return field.to_string();
    }
    let mut out = String::with_capacity(field.len());
    let bytes = field.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' || i + 1 >= bytes.len() {
            // Safe: we only index ASCII bytes; multi-byte UTF-8 passes through.
            out.push(field[i..].chars().next().unwrap());
            i += field[i..].chars().next().unwrap().len_utf8();
            continue;
        }
        let c = bytes[i + 1];
        match c {
            b'b' => {
                out.push('\u{0008}');
                i += 2;
            }
            b'f' => {
                out.push('\u{000C}');
                i += 2;
            }
            b'n' => {
                out.push('\n');
                i += 2;
            }
            b'r' => {
                out.push('\r');
                i += 2;
            }
            b't' => {
                out.push('\t');
                i += 2;
            }
            b'v' => {
                out.push('\u{000B}');
                i += 2;
            }
            b'\\' => {
                out.push('\\');
                i += 2;
            }
            b'x' => {
                let mut j = i + 2;
                let mut val = 0u32;
                let mut digits = 0;
                while j < bytes.len() && digits < 2 && bytes[j].is_ascii_hexdigit() {
                    val = val * 16 + (bytes[j] as char).to_digit(16).unwrap();
                    j += 1;
                    digits += 1;
                }
                if digits == 0 {
                    out.push('\\');
                    out.push('x');
                    i += 2;
                } else {
                    out.push(char::from_u32(val).unwrap_or('\u{FFFD}'));
                    i = j;
                }
            }
            b'0'..=b'7' => {
                let mut j = i + 1;
                let mut val = 0u32;
                let mut digits = 0;
                while j < bytes.len() && digits < 3 && (b'0'..=b'7').contains(&bytes[j]) {
                    val = val * 8 + (bytes[j] - b'0') as u32;
                    j += 1;
                    digits += 1;
                }
                out.push(char::from_u32(val).unwrap_or('\u{FFFD}'));
                i = j;
            }
            _ => {
                // Unknown escape: keep the following character literally.
                out.push(c as char);
                i += 2;
            }
        }
    }
    out
}

fn decode_csv(body: &str) -> Result<Vec<Vec<Cell>>> {
    let mut rows = Vec::new();
    let mut chars = body.chars().peekable();
    let mut row: Vec<Cell> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut field_quoted = false;
    let mut started = false;

    while let Some(c) = chars.next() {
        started = true;
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        match c {
            '"' => {
                in_quotes = true;
                field_quoted = true;
            }
            ',' => {
                row.push(csv_cell(std::mem::take(&mut field), field_quoted));
                field_quoted = false;
            }
            '\n' => {
                row.push(csv_cell(std::mem::take(&mut field), field_quoted));
                field_quoted = false;
                rows.push(std::mem::take(&mut row));
            }
            '\r' => {}
            _ => field.push(c),
        }
    }
    if in_quotes {
        bail!("unterminated quoted field in CSV COPY body");
    }
    // Flush a final row that did not end in a newline.
    if started && (!field.is_empty() || !row.is_empty() || field_quoted) {
        row.push(csv_cell(field, field_quoted));
        rows.push(row);
    }
    Ok(rows)
}

/// In CSV, an unquoted empty field is NULL; a quoted empty field is the empty
/// string (PostgreSQL's default CSV NULL handling).
fn csv_cell(value: String, quoted: bool) -> Cell {
    if !quoted && value.is_empty() {
        Cell::Null
    } else {
        Cell::Text(value)
    }
}

/// Finds a whole-word keyword (case-insensitive) and returns its byte offset.
fn find_keyword(haystack: &str, keyword: &str) -> Option<usize> {
    let upper = haystack.to_ascii_uppercase();
    let mut from = 0;
    while let Some(rel) = upper[from..].find(keyword) {
        let idx = from + rel;
        let before_ok = idx == 0 || !upper.as_bytes()[idx - 1].is_ascii_alphanumeric();
        let after = idx + keyword.len();
        let after_ok = after >= upper.len() || !upper.as_bytes()[after].is_ascii_alphanumeric();
        if before_ok && after_ok {
            return Some(idx);
        }
        from = idx + keyword.len();
    }
    None
}

/// Strips surrounding double-quotes and any schema qualifier from an
/// identifier, returning the bare (optionally schema-qualified) name.
fn strip_identifier(ident: &str) -> String {
    ident.trim().trim_matches('"').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_header_with_columns() {
        let spec = parse_copy_header("COPY public.t (a, b, c) FROM stdin").unwrap();
        assert_eq!(spec.table, "public.t");
        assert_eq!(spec.columns, vec!["a", "b", "c"]);
        assert_eq!(spec.format, CopyFormat::Text);
    }

    #[test]
    fn parses_header_without_columns_and_csv_format() {
        let spec = parse_copy_header("COPY t FROM stdin WITH (FORMAT CSV)").unwrap();
        assert_eq!(spec.table, "t");
        assert!(spec.columns.is_empty());
        assert_eq!(spec.format, CopyFormat::Csv);
    }

    #[test]
    fn decodes_text_rows_with_null_and_escapes() {
        let rows = decode_rows("1\talpha\t\\N\n2\ta\\tb\\nc\t\\\\\n", CopyFormat::Text).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![
                    Cell::Text("1".into()),
                    Cell::Text("alpha".into()),
                    Cell::Null,
                ],
                vec![
                    Cell::Text("2".into()),
                    Cell::Text("a\tb\nc".into()),
                    Cell::Text("\\".into()),
                ],
            ]
        );
    }

    #[test]
    fn decodes_octal_and_hex_escapes() {
        let rows = decode_rows("\\101\t\\x42\n", CopyFormat::Text).unwrap();
        assert_eq!(
            rows,
            vec![vec![Cell::Text("A".into()), Cell::Text("B".into())]]
        );
    }

    #[test]
    fn decodes_csv_rows_with_quotes_and_nulls() {
        let rows = decode_rows("1,\"a,b\",\n2,\"\",x\n", CopyFormat::Csv).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Cell::Text("1".into()), Cell::Text("a,b".into()), Cell::Null,],
                vec![
                    Cell::Text("2".into()),
                    Cell::Text("".into()),
                    Cell::Text("x".into()),
                ],
            ]
        );
    }
}
