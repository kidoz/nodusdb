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
    /// PostgreSQL binary COPY (`FORMAT BINARY`). Decoded by
    /// [`decode_binary_rows`], which needs the column types — unlike the
    /// self-describing text formats — so it is not handled by [`decode_rows`].
    Binary,
}

/// Parsed `COPY` header: where the rows go and how they are encoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopySpec {
    pub table: String,
    pub columns: Vec<String>,
    pub format: CopyFormat,
    /// The first line (text) or record (CSV) is a header and is skipped.
    pub header: bool,
    /// Field separator: tab for text and comma for CSV unless overridden.
    pub delimiter: char,
    /// The unquoted field that denotes NULL: `\N` for text, empty for CSV.
    pub null: String,
    /// CSV quoting character (`"` unless overridden).
    pub quote: char,
    /// CSV escape character inside quotes (the quote character by default).
    pub escape: char,
}

impl CopySpec {
    /// The PostgreSQL defaults for `format`.
    pub fn new(table: impl Into<String>, columns: Vec<String>, format: CopyFormat) -> Self {
        let csv = format == CopyFormat::Csv;
        Self {
            table: table.into(),
            columns,
            format,
            header: false,
            delimiter: if csv { ',' } else { '\t' },
            null: if csv {
                String::new()
            } else {
                "\\N".to_string()
            },
            quote: '"',
            escape: '"',
        }
    }
}

/// Parses a `COPY <table> [(col, ...)] FROM stdin [[WITH] (options)]` header.
/// Supported options are honoured (`FORMAT`, `HEADER`, `DELIMITER`, `NULL`,
/// `QUOTE`, `ESCAPE`, `ENCODING 'UTF8'`, `FREEZE`, and their legacy
/// spellings); any other option is an error rather than silently ignored.
pub fn parse_copy_header(header: &str) -> Result<CopySpec> {
    use sqlparser::ast::{
        CopyLegacyCsvOption, CopyLegacyOption, CopyOption, CopySource, CopyTarget, Statement,
    };
    let statements = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::PostgreSqlDialect {},
        header.trim(),
    )
    .map_err(|e| anyhow::anyhow!("unparsable COPY header: {e}"))?;
    let [
        Statement::Copy {
            source:
                CopySource::Table {
                    table_name,
                    columns,
                },
            to: false,
            target: CopyTarget::Stdin,
            options,
            legacy_options,
            ..
        },
    ] = statements.as_slice()
    else {
        bail!("only COPY <table> FROM STDIN is supported: {header}");
    };
    let table = table_name
        .0
        .iter()
        .map(|part| {
            part.as_ident()
                .map(|i| i.value.clone())
                .unwrap_or_else(|| part.to_string())
        })
        .collect::<Vec<_>>()
        .join(".");
    let columns: Vec<String> = columns.iter().map(|c| c.value.clone()).collect();

    // The table and column names are interpolated into a synthesized INSERT
    // (and on the wire COPY path the header is client-controlled), so reject any
    // identifier that isn't a plain dotted name. This neutralizes SQL/identifier
    // injection at the single choke point both ingestion paths share.
    if !is_safe_qualified_name(&table) {
        bail!("unsafe COPY target identifier: {table:?}");
    }
    for column in &columns {
        if !is_safe_identifier(column) {
            bail!("unsafe COPY column identifier: {column:?}");
        }
    }

    // First the format (it decides the defaults), then the other options.
    let mut format = CopyFormat::Text;
    for option in options {
        if let CopyOption::Format(name) = option {
            format = match name.value.to_ascii_lowercase().as_str() {
                "text" => CopyFormat::Text,
                "csv" => CopyFormat::Csv,
                "binary" => CopyFormat::Binary,
                _ => bail!("unsupported COPY format: {name}"),
            };
        }
    }
    for option in legacy_options {
        match option {
            CopyLegacyOption::Binary => format = CopyFormat::Binary,
            CopyLegacyOption::Csv(_) => format = CopyFormat::Csv,
            _ => {}
        }
    }
    let mut spec = CopySpec::new(table, columns, format);
    let mut csv_only_option = false;
    for option in options {
        match option {
            CopyOption::Format(_) | CopyOption::Freeze(_) => {}
            CopyOption::Header(on) => spec.header = *on,
            CopyOption::Delimiter(c) => spec.delimiter = *c,
            CopyOption::Null(null) => spec.null = null.clone(),
            CopyOption::Quote(c) => {
                spec.quote = *c;
                csv_only_option = true;
            }
            CopyOption::Escape(c) => {
                spec.escape = *c;
                csv_only_option = true;
            }
            CopyOption::Encoding(enc)
                if enc.eq_ignore_ascii_case("utf8") || enc.eq_ignore_ascii_case("utf-8") => {}
            other => bail!("unsupported COPY option: {other}"),
        }
    }
    for option in legacy_options {
        match option {
            CopyLegacyOption::Binary => {}
            CopyLegacyOption::Header => spec.header = true,
            CopyLegacyOption::Delimiter(c) => spec.delimiter = *c,
            CopyLegacyOption::Null(null) => spec.null = null.clone(),
            CopyLegacyOption::Csv(csv) => {
                for option in csv {
                    match option {
                        CopyLegacyCsvOption::Header => spec.header = true,
                        CopyLegacyCsvOption::Quote(c) => spec.quote = *c,
                        CopyLegacyCsvOption::Escape(c) => spec.escape = *c,
                        other => bail!("unsupported COPY option: {other}"),
                    }
                }
            }
            other => bail!("unsupported COPY option: {other}"),
        }
    }
    if format != CopyFormat::Csv && csv_only_option {
        bail!("COPY QUOTE and ESCAPE are available only in CSV mode");
    }
    if format == CopyFormat::Binary && (spec.header || spec.delimiter != '\t' || spec.null != "\\N")
    {
        bail!("COPY HEADER, DELIMITER, and NULL are not available in BINARY mode");
    }
    Ok(spec)
}

/// A bare SQL identifier safe to interpolate unquoted: non-empty, ASCII
/// alphanumeric plus `_` (and a leading non-digit). Deliberately conservative —
/// exotic quoted identifiers are rejected rather than risk injection.
fn is_safe_identifier(ident: &str) -> bool {
    let mut chars = ident.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A `schema.table`-style name: every dot-separated part is a safe identifier.
fn is_safe_qualified_name(name: &str) -> bool {
    !name.is_empty() && name.split('.').all(is_safe_identifier)
}

/// Decodes a text/CSV COPY data body into rows of cells. Binary bodies are not
/// self-describing (the wire carries no type tags), so they go through
/// [`decode_binary_rows`] with the column types instead.
pub fn decode_rows(body: &str, spec: &CopySpec) -> Result<Vec<Vec<Cell>>> {
    match spec.format {
        CopyFormat::Text => Ok(decode_text(body, spec)),
        CopyFormat::Csv => decode_csv(body, spec),
        CopyFormat::Binary => {
            bail!("binary COPY must be decoded via decode_binary_rows with column types")
        }
    }
}

/// The 11-byte signature that opens every PostgreSQL binary COPY stream.
const BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// OID-inclusion bit in the binary COPY header flags field.
const BINARY_FLAG_HAS_OIDS: i32 = 1 << 16;

/// The engine's four logical column kinds, which a SQL type name collapses to.
/// Binary COPY fields carry no type tag, so the declared column type selects how
/// each field's bytes are interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryKind {
    Int,
    Float,
    Bool,
    Text,
}

/// Mirrors the executor's `column_type`: maps a SQL type name to a logical kind.
fn binary_kind(type_name: &str) -> BinaryKind {
    let t = type_name.to_ascii_uppercase();
    if t.contains("INT") || t.contains("SERIAL") {
        BinaryKind::Int
    } else if t.contains("FLOAT")
        || t.contains("DOUBLE")
        || t.contains("REAL")
        || t.contains("NUMERIC")
        || t.contains("DECIMAL")
    {
        BinaryKind::Float
    } else if t.contains("BOOL") {
        BinaryKind::Bool
    } else {
        BinaryKind::Text
    }
}

/// Decodes a PostgreSQL binary-format COPY body into text cells, using the
/// declared column types (in field order) to interpret each field's bytes.
///
/// The binary wire format carries no per-field type, so `type_names` must list
/// the target columns' types in the same order the rows were written. Only the
/// engine's four logical kinds are supported (int, float, bool, text); the
/// field byte length disambiguates integer/float widths. Columns whose type is
/// none of these are read as UTF-8 text, matching the engine's simplified model.
pub fn decode_binary_rows(bytes: &[u8], type_names: &[String]) -> Result<Vec<Vec<Cell>>> {
    let kinds: Vec<BinaryKind> = type_names.iter().map(|t| binary_kind(t)).collect();
    let mut cur = ByteReader::new(bytes);

    if cur.read(BINARY_SIGNATURE.len())? != BINARY_SIGNATURE {
        bail!("invalid binary COPY signature");
    }
    let flags = cur.read_i32()?;
    if flags & BINARY_FLAG_HAS_OIDS != 0 {
        bail!("binary COPY with OIDs is not supported");
    }
    let ext_len = cur.read_i32()?;
    if ext_len < 0 {
        bail!("invalid binary COPY header extension length: {ext_len}");
    }
    cur.read(ext_len as usize)?;

    let mut rows = Vec::new();
    loop {
        let field_count = cur.read_i16()?;
        if field_count == -1 {
            break; // The -1 tuple field-count is the end-of-data trailer.
        }
        if field_count < 0 {
            bail!("invalid binary COPY field count: {field_count}");
        }
        let mut row = Vec::with_capacity(field_count as usize);
        for i in 0..field_count as usize {
            let len = cur.read_i32()?;
            if len == -1 {
                row.push(Cell::Null);
                continue;
            }
            if len < 0 {
                bail!("invalid binary COPY field length: {len}");
            }
            let field = cur.read(len as usize)?;
            let kind = kinds.get(i).copied().unwrap_or(BinaryKind::Text);
            row.push(decode_binary_field(field, kind)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

/// Renders one binary field as the text form `synthesize_insert` will quote and
/// the executor will coerce back to the column's type.
fn decode_binary_field(bytes: &[u8], kind: BinaryKind) -> Result<Cell> {
    let text = match kind {
        BinaryKind::Int => match bytes.len() {
            1 => (bytes[0] as i8 as i64).to_string(),
            2 => i16::from_be_bytes([bytes[0], bytes[1]]).to_string(),
            4 => i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string(),
            8 => i64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
            .to_string(),
            n => bail!("unsupported binary integer width: {n} bytes"),
        },
        BinaryKind::Float => match bytes.len() {
            4 => f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]).to_string(),
            8 => f64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
            .to_string(),
            n => bail!("unsupported binary float width: {n} bytes"),
        },
        BinaryKind::Bool => match bytes {
            [0] => "false".to_string(),
            [_] => "true".to_string(),
            _ => bail!("binary bool must be 1 byte, got {}", bytes.len()),
        },
        BinaryKind::Text => String::from_utf8_lossy(bytes).into_owned(),
    };
    Ok(Cell::Text(text))
}

/// A minimal big-endian byte cursor with bounds-checked reads, so a truncated or
/// malformed binary COPY body errors instead of panicking on a slice index.
struct ByteReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> ByteReader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(n)
            .filter(|end| *end <= self.buf.len())
            .ok_or_else(|| anyhow::anyhow!("binary COPY truncated at offset {}", self.pos))?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn read_i16(&mut self) -> Result<i16> {
        let b = self.read(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    fn read_i32(&mut self) -> Result<i32> {
        let b = self.read(4)?;
        Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }
}

fn decode_text(body: &str, spec: &CopySpec) -> Vec<Vec<Cell>> {
    let mut rows = Vec::new();
    // The body excludes the `\.` terminator; a trailing newline yields a
    // final empty segment that is not a row.
    let lines = body.split('\n').filter(|line| !line.is_empty());
    for line in lines.skip(usize::from(spec.header)) {
        let line = line.strip_suffix('\r').unwrap_or(line);
        let mut row = Vec::new();
        for field in line.split(spec.delimiter) {
            // The NULL marker is matched before backslash processing.
            if field == spec.null {
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

fn decode_csv(body: &str, spec: &CopySpec) -> Result<Vec<Vec<Cell>>> {
    // An unquoted field equal to the NULL string (empty by default) is NULL; a
    // quoted field never is (PostgreSQL's CSV NULL handling).
    let cell = |value: String, quoted: bool| {
        if !quoted && value == spec.null {
            Cell::Null
        } else {
            Cell::Text(value)
        }
    };
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
            // The escape character makes a following quote (or escape) literal;
            // with the default escape this is the doubled-quote rule.
            let escapes_next = chars.peek().is_some_and(|&next| {
                next == spec.quote || (next == spec.escape && spec.escape != spec.quote)
            });
            if c == spec.escape && escapes_next {
                field.extend(chars.next());
            } else if c == spec.quote {
                in_quotes = false;
            } else {
                field.push(c);
            }
            continue;
        }
        if c == spec.quote {
            in_quotes = true;
            field_quoted = true;
        } else if c == spec.delimiter {
            row.push(cell(std::mem::take(&mut field), field_quoted));
            field_quoted = false;
        } else if c == '\n' {
            row.push(cell(std::mem::take(&mut field), field_quoted));
            field_quoted = false;
            rows.push(std::mem::take(&mut row));
        } else if c != '\r' {
            field.push(c);
        }
    }
    if in_quotes {
        bail!("unterminated quoted field in CSV COPY body");
    }
    // Flush a final row that did not end in a newline.
    if started && (!field.is_empty() || !row.is_empty() || field_quoted) {
        row.push(cell(field, field_quoted));
        rows.push(row);
    }
    if spec.header && !rows.is_empty() {
        rows.remove(0);
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text_spec() -> CopySpec {
        CopySpec::new("t", vec![], CopyFormat::Text)
    }

    fn csv_spec() -> CopySpec {
        CopySpec::new("t", vec![], CopyFormat::Csv)
    }

    #[test]
    fn honours_header_delimiter_null_and_quoting_options() {
        let spec = parse_copy_header(
            "COPY t (a, b) FROM STDIN WITH (FORMAT csv, HEADER true, DELIMITER ';', NULL 'NA', QUOTE '''', ESCAPE '\\')",
        )
        .unwrap();
        assert!(spec.header);
        let rows = decode_rows("a;b\n1;'x;\\'y'\nNA;''\n", &spec).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![Cell::Text("1".into()), Cell::Text("x;'y".into())],
                vec![Cell::Null, Cell::Text(String::new())],
            ]
        );

        let legacy = parse_copy_header("COPY t FROM STDIN DELIMITER '|' NULL 'x'").unwrap();
        let rows = decode_rows("1|x\n", &legacy).unwrap();
        assert_eq!(rows, vec![vec![Cell::Text("1".into()), Cell::Null]]);
    }

    #[test]
    fn rejects_unsupported_options_instead_of_ignoring_them() {
        for header in [
            "COPY t FROM STDIN WITH (FORMAT csv, FORCE_NULL (a))",
            "COPY t FROM STDIN WITH (ENCODING 'LATIN1')",
            "COPY t FROM STDIN WITH (FORMAT text, QUOTE '\"')",
            "COPY t FROM STDIN WITH (FORMAT csv, ON_ERROR ignore)",
            "COPY t FROM '/etc/passwd'",
        ] {
            assert!(parse_copy_header(header).is_err(), "{header}");
        }
    }

    #[test]
    fn parses_header_with_columns() {
        let spec = parse_copy_header("COPY public.t (a, b, c) FROM stdin").unwrap();
        assert_eq!(spec.table, "public.t");
        assert_eq!(spec.columns, vec!["a", "b", "c"]);
        assert_eq!(spec.format, CopyFormat::Text);
    }

    #[test]
    fn rejects_injection_in_table_and_column_identifiers() {
        // Any identifier reaching the synthesized INSERT must be a plain dotted
        // name; these all carry SQL punctuation and must be rejected.
        assert!(parse_copy_header("COPY t;DROP TABLE x FROM stdin").is_err());
        assert!(parse_copy_header("COPY evil drop FROM stdin").is_err());
        assert!(parse_copy_header("COPY t (a b) FROM stdin").is_err());
        assert!(parse_copy_header("COPY t (id, x-1) FROM stdin").is_err());
    }

    #[test]
    fn safe_identifier_classifies_correctly() {
        assert!(is_safe_identifier("users"));
        assert!(is_safe_identifier("_x9"));
        assert!(is_safe_qualified_name("public.users"));
        assert!(!is_safe_identifier("9x"));
        assert!(!is_safe_identifier("a b"));
        assert!(!is_safe_identifier("a;b"));
        assert!(!is_safe_qualified_name(""));
    }

    #[test]
    fn parses_binary_format_header() {
        let spec = parse_copy_header("COPY t (id, name) FROM stdin (FORMAT BINARY)").unwrap();
        assert_eq!(spec.columns, vec!["id", "name"]);
        assert_eq!(spec.format, CopyFormat::Binary);
    }

    /// Builds a one-row binary COPY stream the way a driver's binary importer
    /// does, then round-trips it through the decoder against declared types.
    #[test]
    fn decodes_binary_int_text_bool_float_and_null() {
        let mut body = Vec::new();
        body.extend_from_slice(BINARY_SIGNATURE);
        body.extend_from_slice(&0i32.to_be_bytes()); // flags
        body.extend_from_slice(&0i32.to_be_bytes()); // header extension length

        // One row: int4=1, text="one", bool=true, float8=1.5, NULL.
        body.extend_from_slice(&5i16.to_be_bytes());
        body.extend_from_slice(&4i32.to_be_bytes());
        body.extend_from_slice(&1i32.to_be_bytes());
        body.extend_from_slice(&3i32.to_be_bytes());
        body.extend_from_slice(b"one");
        body.extend_from_slice(&1i32.to_be_bytes());
        body.push(1);
        body.extend_from_slice(&8i32.to_be_bytes());
        body.extend_from_slice(&1.5f64.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes()); // NULL field
        body.extend_from_slice(&(-1i16).to_be_bytes()); // trailer

        let types = ["INT", "TEXT", "BOOL", "DOUBLE", "TEXT"].map(String::from);
        let rows = decode_binary_rows(&body, &types).unwrap();
        assert_eq!(
            rows,
            vec![vec![
                Cell::Text("1".into()),
                Cell::Text("one".into()),
                Cell::Text("true".into()),
                Cell::Text("1.5".into()),
                Cell::Null,
            ]]
        );
    }

    #[test]
    fn rejects_truncated_and_unsigned_binary_streams() {
        // Bad signature.
        assert!(decode_binary_rows(b"NOTPGCOPY..", &[]).is_err());
        // Valid header but the tuple is cut off mid-field — must error, not panic.
        let mut body = Vec::new();
        body.extend_from_slice(BINARY_SIGNATURE);
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&0i32.to_be_bytes());
        body.extend_from_slice(&1i16.to_be_bytes()); // one field
        body.extend_from_slice(&4i32.to_be_bytes()); // claims 4 bytes
        body.extend_from_slice(&[0u8, 0]); // but only 2 present
        assert!(decode_binary_rows(&body, &[String::from("INT")]).is_err());
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
        let rows = decode_rows("1\talpha\t\\N\n2\ta\\tb\\nc\t\\\\\n", &text_spec()).unwrap();
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
        let rows = decode_rows("\\101\t\\x42\n", &text_spec()).unwrap();
        assert_eq!(
            rows,
            vec![vec![Cell::Text("A".into()), Cell::Text("B".into())]]
        );
    }

    #[test]
    fn decodes_csv_rows_with_quotes_and_nulls() {
        let rows = decode_rows("1,\"a,b\",\n2,\"\",x\n", &csv_spec()).unwrap();
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
