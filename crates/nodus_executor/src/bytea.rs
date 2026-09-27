//! `bytea`: its input and output forms, `encode`/`decode`, the encoding
//! conversions, and the checksums the bytea functions compute.

/// Parses `bytea` input: `\x` then hex digit pairs (spaces between pairs
/// allowed), or the escape form, where `\\` is a backslash and `\ooo` an
/// octal byte.
pub fn parse_input(text: &str) -> Result<Vec<u8>, String> {
    if let Some(hex) = text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))
    {
        return decode_hex(hex);
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        match bytes.get(i + 1..i + 4) {
            _ if bytes.get(i + 1) == Some(&b'\\') => {
                out.push(b'\\');
                i += 2;
            }
            Some([a @ b'0'..=b'3', b @ b'0'..=b'7', c @ b'0'..=b'7']) => {
                out.push((a - b'0') * 64 + (b - b'0') * 8 + (c - b'0'));
                i += 4;
            }
            _ => return Err("invalid input syntax for type bytea".into()),
        }
    }
    Ok(out)
}

/// Hex digit pairs, which spaces may separate.
fn decode_hex(hex: &str) -> Result<Vec<u8>, String> {
    let digit = |c: u8| -> Result<u8, String> {
        (c as char)
            .to_digit(16)
            .map(|d| d as u8)
            .ok_or_else(|| format!("invalid hexadecimal digit: \"{}\"", c as char))
    };
    let mut out = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.bytes().filter(|b| !b.is_ascii_whitespace());
    while let Some(high) = chars.next() {
        let high = digit(high)?;
        let Some(low) = chars.next() else {
            return Err("invalid hexadecimal data: odd number of digits".into());
        };
        out.push(high << 4 | digit(low)?);
    }
    Ok(out)
}

/// Bytes as `bytea` output shows them: `\x` and hex digits.
pub fn hex_text(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    out.push_str(&hex(bytes));
    out
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

/// Bytes in the escape form: printable ASCII as it is, a backslash doubled,
/// and any other byte as `\ooo`.
fn escape(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        match b {
            b'\\' => out.push_str("\\\\"),
            0x20..=0x7e => out.push(b as char),
            _ => out.push_str(&format!("\\{b:03o}")),
        }
    }
    out
}

const BASE64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Base64 in lines of at most 76 characters, as PostgreSQL writes it.
fn base64(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 4 / 3 + 4);
    let mut line = 0;
    for chunk in bytes.chunks(3) {
        if line == 76 {
            out.push('\n');
            line = 0;
        }
        let n = chunk
            .iter()
            .enumerate()
            .fold(0u32, |n, (i, b)| n | u32::from(*b) << (16 - 8 * i));
        for i in 0..4 {
            out.push(if i <= chunk.len() {
                BASE64[(n >> (18 - 6 * i) & 0x3f) as usize] as char
            } else {
                '='
            });
        }
        line += 4;
    }
    out
}

fn decode_base64(text: &str) -> Result<Vec<u8>, String> {
    let corrupt = |message: &str| {
        crate::error_fields::DbError::new(message)
            .hint("Input data is missing padding, is truncated, or is otherwise corrupted.")
            .into_text()
    };
    let mut out = Vec::with_capacity(text.len() * 3 / 4);
    let (mut acc, mut bits, mut padding, mut count) = (0u32, 0, 0, 0);
    for c in text.bytes().filter(|b| !b.is_ascii_whitespace()) {
        count += 1;
        if c == b'=' {
            padding += 1;
            continue;
        }
        if padding > 0 {
            return Err(corrupt("invalid base64 end sequence"));
        }
        let Some(value) = BASE64.iter().position(|&b| b == c) else {
            return Err(format!(
                "invalid symbol \"{}\" found while decoding base64 sequence",
                c as char
            ));
        };
        acc = acc << 6 | value as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    if count % 4 != 0 || padding > 2 {
        return Err(corrupt("invalid base64 end sequence"));
    }
    Ok(out)
}

/// `encode(bytes, format)`.
pub(crate) fn encode(bytes: &[u8], format: &str) -> Result<String, String> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => Ok(hex(bytes)),
        "base64" => Ok(base64(bytes)),
        "escape" => Ok(escape(bytes)),
        _ => Err(format!("unrecognized encoding: \"{format}\"")),
    }
}

/// `decode(text, format)`.
pub(crate) fn decode(text: &str, format: &str) -> Result<Vec<u8>, String> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => decode_hex(text),
        "base64" => decode_base64(text),
        "escape" => parse_input(text),
        _ => Err(format!("unrecognized encoding: \"{format}\"")),
    }
}

/// An encoding name in its canonical form (`UTF8`, `LATIN1`, `SQL_ASCII`),
/// matched as PostgreSQL matches them: ignoring case and punctuation.
fn encoding(name: &str) -> Option<&'static str> {
    let key: String = name
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect::<String>()
        .to_ascii_lowercase();
    Some(match key.as_str() {
        "utf8" | "unicode" => "UTF8",
        "latin1" | "iso88591" => "LATIN1",
        "sqlascii" => "SQL_ASCII",
        _ => return None,
    })
}

/// `convert_to(text, encoding)`.
pub(crate) fn convert_to(text: &str, to: &str) -> Result<Vec<u8>, String> {
    match encoding(to) {
        Some("UTF8" | "SQL_ASCII") => Ok(text.as_bytes().to_vec()),
        Some(_) => text
            .chars()
            .map(|c| {
                u8::try_from(u32::from(c)).map_err(|_| {
                    let mut buf = [0u8; 4];
                    let sequence = c
                        .encode_utf8(&mut buf)
                        .bytes()
                        .map(|b| format!("0x{b:02x}"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!(
                        "character with byte sequence {sequence} in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\""
                    )
                })
            })
            .collect(),
        None => Err(format!("invalid destination encoding name \"{to}\"")),
    }
}

/// `convert_from(bytes, encoding)`.
pub(crate) fn convert_from(bytes: &[u8], from: &str) -> Result<String, String> {
    match encoding(from) {
        Some("UTF8" | "SQL_ASCII") => String::from_utf8(bytes.to_vec()).map_err(|e| {
            let bad = bytes[e.utf8_error().valid_up_to()];
            format!("invalid byte sequence for encoding \"UTF8\": 0x{bad:02x}")
        }),
        Some(_) => Ok(bytes.iter().map(|&b| char::from(b)).collect()),
        None => Err(format!("invalid source encoding name \"{from}\"")),
    }
}

/// `convert(bytes, from, to)`.
pub(crate) fn convert(bytes: &[u8], from: &str, to: &str) -> Result<Vec<u8>, String> {
    if encoding(to).is_none() {
        return Err(format!("invalid destination encoding name \"{to}\""));
    }
    convert_to(&convert_from(bytes, from)?, to)
}

/// A CRC-32 with the reflected polynomial `poly` (IEEE's for `crc32`,
/// Castagnoli's for `crc32c`).
pub(crate) fn crc32(bytes: &[u8], castagnoli: bool) -> u32 {
    let poly: u32 = if castagnoli { 0x82F6_3B78 } else { 0xEDB8_8320 };
    let mut crc = !0u32;
    for &b in bytes {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 == 1 {
                crc >> 1 ^ poly
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// A value stored in a `bytea` column: `\x` hex, or (written before bytes
/// were kept as bytes) the text's own bytes.
pub(crate) fn from_stored(text: &str) -> Vec<u8> {
    text.strip_prefix("\\x")
        .and_then(|hex| decode_hex(hex).ok())
        .unwrap_or_else(|| text.as_bytes().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_and_writes_postgresql_forms() {
        assert_eq!(parse_input("\\x61 62").unwrap(), b"ab");
        assert_eq!(parse_input("a\\101b\\\\").unwrap(), b"aAb\\");
        assert!(parse_input("a\\9b").is_err());
        assert_eq!(
            parse_input("\\xZZ").unwrap_err(),
            "invalid hexadecimal digit: \"Z\""
        );
        assert_eq!(hex_text(b"abc"), "\\x616263");
        assert_eq!(encode(b"\x00\xff\\'", "escape").unwrap(), "\\000\\377\\\\'");
        assert_eq!(encode(b"abc", "base64").unwrap(), "YWJj");
        assert_eq!(encode(&[b'x'; 60], "base64").unwrap().lines().count(), 2);
        assert_eq!(decode(" YW Jj ", "base64").unwrap(), b"abc");
        assert_eq!(decode("+/8=", "base64").unwrap(), vec![0xfb, 0xff]);
        assert!(decode("YW=Jj", "base64").is_err());
        assert_eq!(convert_to("é", "latin1").unwrap(), vec![0xe9]);
        assert!(convert_to("€", "LATIN1").is_err());
        assert_eq!(convert_from(&[0xe9], "LATIN1").unwrap(), "é");
        assert_eq!(crc32(b"abc", false), 891_568_578);
        assert_eq!(crc32(b"abc", true), 910_901_175);
    }
}
