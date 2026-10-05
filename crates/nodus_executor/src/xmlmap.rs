//! PostgreSQL's SQL-to-XML mapping functions (`table_to_xml`,
//! `query_to_xml`, `cursor_to_xml`, `schema_to_xml`, `database_to_xml`, and
//! the `*_xmlschema`/`*_and_xmlschema` forms): the SQL/XML:2008 mapping of
//! rows and catalogs to XML, as `xml.c` writes it.

use crate::Value;

/// The XSD-instance namespace every mapping declares.
const XSI: &str = "http://www.w3.org/2001/XMLSchema-instance";

/// Whether a character may start an XML name (`is_valid_xml_namefirst`).
fn is_namefirst(c: char) -> bool {
    c == '_' || c == ':' || c.is_alphabetic()
}

/// Whether a character may continue an XML name (`is_valid_xml_namechar`).
fn is_namechar(c: char) -> bool {
    is_namefirst(c) || c.is_ascii_digit() || matches!(c, '.' | '-' | '_' | ':')
}

/// `map_sql_identifier_to_xml_name(ident, true, false)`: a SQL identifier
/// as an XML name, escaping what XML names cannot hold.
pub(crate) fn identifier_to_xml_name(ident: &str) -> String {
    escaped_identifier(ident, false)
}

/// `map_multipart_sql_identifier_to_xml_name`: dotted parts, each escaped
/// (a period inside a part is escaped too).
pub(crate) fn multipart_name(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|part| escaped_identifier(part, true))
        .collect::<Vec<_>>()
        .join(".")
}

fn escaped_identifier(ident: &str, escape_period: bool) -> String {
    let chars: Vec<char> = ident.chars().collect();
    let mut out = String::with_capacity(ident.len());
    for (at, &c) in chars.iter().enumerate() {
        if c == ':' {
            out.push_str("_x003A_");
        } else if c == '_' && chars.get(at + 1) == Some(&'x') {
            out.push_str("_x005F_");
        } else if at == 0
            && chars.len() >= 3
            && chars[..3]
                .iter()
                .collect::<String>()
                .eq_ignore_ascii_case("xml")
        {
            // A name that starts with `xml`, whatever its case.
            out.push_str(if c == 'x' { "_x0078_" } else { "_x0058_" });
        } else if escape_period && c == '.' {
            out.push_str("_x002E_");
        } else if (at == 0 && !is_namefirst(c)) || (at > 0 && !is_namechar(c)) {
            out.push_str(&format!("_x{:04X}_", c as u32));
        } else {
            out.push(c);
        }
    }
    out
}

/// `xmldata_root_element_start`: the root element's start tag, with the
/// namespace declarations the mapping adds.
pub(crate) fn root_element_start(
    out: &mut String,
    eltname: &str,
    xmlschema: Option<&str>,
    targetns: &str,
    top_level: bool,
) {
    out.push('<');
    out.push_str(eltname);
    if top_level {
        out.push_str(" xmlns:xsi=\"");
        out.push_str(XSI);
        out.push('"');
        if !targetns.is_empty() {
            out.push_str(" xmlns=\"");
            out.push_str(targetns);
            out.push('"');
        }
    }
    if xmlschema.is_some() {
        if targetns.is_empty() {
            out.push_str(" xsi:noNamespaceSchemaLocation=\"#\"");
        } else {
            out.push_str(&format!(" xsi:schemaLocation=\"{targetns} #\""));
        }
    }
    out.push_str(">\n");
}

/// `xmldata_root_element_end`.
pub(crate) fn root_element_end(out: &mut String, eltname: &str) {
    out.push_str("</");
    out.push_str(eltname);
    out.push_str(">\n");
}

/// A base64 encoding, as libxml2's `xmlTextWriterWriteBase64` writes one:
/// lines of 72 characters, with CRLF between them.
fn base64(data: &[u8], out: &mut String) {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut line = 0;
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let code = [
            ALPHABET[(b[0] >> 2) as usize],
            ALPHABET[(((b[0] & 0x03) << 4) | (b[1] >> 4)) as usize],
            if chunk.len() > 1 {
                ALPHABET[(((b[1] & 0x0F) << 2) | (b[2] >> 6)) as usize]
            } else {
                b'='
            },
            if chunk.len() > 2 {
                ALPHABET[(b[2] & 0x3F) as usize]
            } else {
                b'='
            },
        ];
        if line >= 72 {
            out.push_str("\r\n");
            line = 0;
        }
        for c in code {
            out.push(c as char);
        }
        line += 4;
    }
}

/// `map_sql_value_to_xml_value(value, type, true)`: a value as the mapping
/// writes it into an element.
pub(crate) fn value_to_xml(value: &Value, data_type: &str) -> Result<String, String> {
    let base = data_type.trim().trim_end_matches("[]");
    let base_name = base
        .split('(')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    match value {
        // A null is written by the caller as `xsi:nil`; a nested null in an
        // array is left out.
        Value::Null => Ok(String::new()),
        Value::Bool(flag) => Ok(if *flag { "true" } else { "false" }.to_string()),
        Value::Bytea(data) => {
            let mut out = String::new();
            base64(data, &mut out);
            Ok(out)
        }
        Value::Array(items) => {
            let mut out = String::new();
            for item in items {
                if matches!(item, Value::Null) {
                    continue;
                }
                out.push_str("<element>");
                out.push_str(&value_to_xml(item, base)?);
                out.push_str("</element>");
            }
            Ok(out)
        }
        Value::Text(text) => {
            let is_xml = crate::xml::is_type(base);
            let mapped = match base_name.as_str() {
                // XSD's date and date-time forms: a `T` between the parts
                // and a signed `HH:MM` zone; infinite values have none.
                "date" => xsd_date(text)?,
                "timestamp" => xsd_timestamp(text, false)?,
                "timestamptz" => xsd_timestamp(text, true)?,
                _ => text.clone(),
            };
            // An XML value is written as it is; anything else is escaped.
            Ok(if is_xml {
                mapped
            } else {
                crate::xml::escape_xml(&mapped)
            })
        }
        other => {
            let text = crate::render(other);
            Ok(crate::xml::escape_xml(&text))
        }
    }
}

/// A `date` in the mapping's form: its usual text; an infinite one has
/// none.
fn xsd_date(text: &str) -> Result<String, String> {
    if text.starts_with("infinity") || text.starts_with("-infinity") {
        return Err(crate::error_fields::DbError::new("date out of range")
            .code("22008")
            .detail("XML does not support infinite date values.")
            .into_text());
    }
    Ok(text.to_string())
}

/// A timestamp in XSD's form: `YYYY-MM-DDTHH:MM:SS[.fff]`,
/// `±HH:MM` after it for a zoned one.
fn xsd_timestamp(text: &str, zoned: bool) -> Result<String, String> {
    if text.starts_with("infinity") || text.starts_with("-infinity") {
        return Err(crate::error_fields::DbError::new("timestamp out of range")
            .code("22008")
            .detail("XML does not support infinite timestamp values.")
            .into_text());
    }
    // `2020-01-02 03:04:05.5+02` (the shown form) becomes
    // `2020-01-02T03:04:05.5+02:00`.
    let Some(space) = text.find(' ') else {
        return Ok(text.to_string());
    };
    let (date, rest) = text.split_at(space);
    let rest = &rest[1..];
    let mut out = format!("{date}T");
    // The zone, if any, ends the rest; expand `+02` and `+0200` to `+02:00`.
    if zoned
        && let Some(zone_at) = rest.rfind(['+', '-'])
        && zone_at > 0
    {
        let (clock, zone) = rest.split_at(zone_at);
        out.push_str(clock);
        out.push_str(&xsd_zone(zone));
        return Ok(out);
    }
    out.push_str(rest);
    Ok(out)
}

/// A zone offset as XSD writes it: `±HH:MM` (or with seconds, as given).
fn xsd_zone(zone: &str) -> String {
    let (sign, rest) = zone.split_at(1);
    let digits: Vec<char> = rest.chars().filter(|c| c.is_ascii_digit()).collect();
    let colon = rest.contains(':');
    match digits.len() {
        2 => format!("{sign}{}:00", digits.iter().collect::<String>()),
        4 if colon => zone.to_string(),
        4 => format!(
            "{sign}{}:{}",
            digits[..2].iter().collect::<String>(),
            digits[2..].iter().collect::<String>()
        ),
        6 if colon => zone.to_string(),
        6 => format!(
            "{sign}{}:{}:{}",
            digits[..2].iter().collect::<String>(),
            digits[2..4].iter().collect::<String>(),
            digits[4..].iter().collect::<String>()
        ),
        _ => zone.to_string(),
    }
}

/// `SPI_sql_row_to_xmlelement`: one row of the mapping, appended to `out`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn row_to_xml(
    out: &mut String,
    row: &[Value],
    columns: &[(String, String)],
    tablename: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    top_level: bool,
) -> Result<(), String> {
    let xmltn = match tablename {
        Some(name) => identifier_to_xml_name(name),
        None if tableforest => "row".to_string(),
        None => "table".to_string(),
    };
    if tableforest {
        root_element_start(out, &xmltn, None, targetns, top_level);
    } else {
        out.push_str("<row>\n");
    }
    for (index, (name, data_type)) in columns.iter().enumerate() {
        let colname = identifier_to_xml_name(name);
        let value = row.get(index).unwrap_or(&Value::Null);
        if matches!(value, Value::Null) {
            if nulls {
                out.push_str(&format!("  <{colname} xsi:nil=\"true\"/>\n"));
            }
        } else {
            let mapped = value_to_xml(value, data_type)?;
            out.push_str(&format!("  <{colname}>{mapped}</{colname}>\n"));
        }
    }
    if tableforest {
        root_element_end(out, &xmltn);
        out.push('\n');
    } else {
        out.push_str("</row>\n\n");
    }
    Ok(())
}

/// `query_to_xml_internal` with no schema: the mapped rows of a query, with
/// its columns as they were read.
pub(crate) fn map_rows(
    columns: &[(String, String)],
    rows: &[Vec<Value>],
    tablename: Option<&str>,
    nulls: bool,
    tableforest: bool,
    targetns: &str,
    xmlschema: Option<&str>,
    top_level: bool,
) -> Result<String, String> {
    let mut out = String::new();
    let xmltn = match tablename {
        Some(name) => identifier_to_xml_name(name),
        None => "table".to_string(),
    };
    if !tableforest {
        root_element_start(&mut out, &xmltn, xmlschema, targetns, top_level);
        out.push('\n');
    }
    if let Some(xmlschema) = xmlschema {
        out.push_str(xmlschema);
        out.push_str("\n\n");
    }
    for row in rows {
        row_to_xml(
            &mut out,
            row,
            columns,
            tablename,
            nulls,
            tableforest,
            targetns,
            top_level,
        )?;
    }
    if !tableforest {
        root_element_end(&mut out, &xmltn);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_match_postgresql() {
        // Escapes of the SQL/XML mapping: a space, a colon, `_x`, a leading
        // `xml`, and (for multipart names) a period.
        assert_eq!(identifier_to_xml_name("a b"), "a_x0020_b");
        assert_eq!(identifier_to_xml_name("xmlx"), "_x0078_mlx");
        assert_eq!(identifier_to_xml_name("XMLx"), "_x0058_MLx");
        assert_eq!(identifier_to_xml_name("a:b"), "a_x003A_b");
        assert_eq!(identifier_to_xml_name("a_x"), "a_x005F_x");
        assert_eq!(identifier_to_xml_name("a.b"), "a.b");
        assert_eq!(
            multipart_name(&["TableType", "default", "public", "a.b"]),
            "TableType.default.public.a_x002E_b"
        );
    }
}
