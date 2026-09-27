//! Numeric literal forms PostgreSQL 16 added that the tokenizer lacks:
//! `0x1F`, `0o17`, and `0b101` integers, and digits grouped by underscores
//! (`1_000_000`). They are rewritten to plain decimal digits before
//! tokenizing, skipping strings, quoted identifiers, and comments.

use std::borrow::Cow;

/// `sql` with each non-decimal or underscore-grouped numeric literal in
/// plain decimal digits.
pub(crate) fn normalize(sql: &str) -> Cow<'_, str> {
    let bytes = sql.as_bytes();
    // Most statements have neither form.
    let candidate = bytes.windows(2).any(|w| {
        (w[0].is_ascii_digit() && w[1] == b'_')
            || (w[0] == b'0' && matches!(w[1], b'x' | b'X' | b'o' | b'O' | b'b' | b'B'))
    });
    if !candidate {
        return Cow::Borrowed(sql);
    }
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        match bytes[i] {
            b'\'' => {
                let escapes = i > 0
                    && matches!(bytes[i - 1], b'e' | b'E')
                    && (i < 2 || !is_word_byte(bytes[i - 2]));
                i = skip_quoted(bytes, i, b'\'', escapes);
            }
            b'"' => i = skip_quoted(bytes, i, b'"', false),
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = bytes[i..]
                    .iter()
                    .position(|&b| b == b'\n')
                    .map_or(bytes.len(), |p| i + p);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => i = skip_block_comment(bytes, i),
            b'$' => i = skip_dollar_quoted(bytes, i),
            b if is_word_byte(b) && !b.is_ascii_digit() => {
                while i < bytes.len() && (is_word_byte(bytes[i]) || bytes[i] == b'$') {
                    i += 1;
                }
            }
            b if b.is_ascii_digit() => {
                while i < bytes.len() && (is_word_byte(bytes[i]) || bytes[i] == b'.') {
                    // An exponent's sign belongs to the number.
                    if matches!(bytes[i], b'e' | b'E')
                        && matches!(bytes.get(i + 1), Some(b'+' | b'-'))
                        && !sql[start..i].starts_with("0x")
                        && !sql[start..i].starts_with("0X")
                    {
                        i += 1;
                    }
                    i += 1;
                }
                if let Some(plain) = plain_number(&sql[start..i]) {
                    out.push_str(&plain);
                    continue;
                }
            }
            _ => {
                i += sql[i..].chars().next().map_or(1, char::len_utf8);
            }
        }
        out.push_str(&sql[start..i]);
    }
    Cow::Owned(out)
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b >= 0x80
}

fn skip_quoted(bytes: &[u8], start: usize, quote: u8, escapes: bool) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if escapes && bytes[i] == b'\\' {
            i += 2;
            continue;
        }
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

fn skip_block_comment(bytes: &[u8], start: usize) -> usize {
    let (mut i, mut depth) = (start + 2, 1);
    while i < bytes.len() && depth > 0 {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
        } else {
            i += 1;
        }
    }
    i
}

/// Past a `$tag$ ... $tag$` string, or just past a `$` that starts none
/// (a `$1` parameter).
fn skip_dollar_quoted(bytes: &[u8], start: usize) -> usize {
    let mut j = start + 1;
    if bytes.get(j).is_some_and(u8::is_ascii_digit) {
        return j;
    }
    while j < bytes.len() && is_word_byte(bytes[j]) {
        j += 1;
    }
    if bytes.get(j) != Some(&b'$') {
        return start + 1;
    }
    let tag = &bytes[start..=j];
    let body = j + 1;
    bytes[body..]
        .windows(tag.len())
        .position(|w| w == tag)
        .map_or(bytes.len(), |p| body + p + tag.len())
}

/// A numeric literal's plain decimal form, if it needs one: a `0x`, `0o`,
/// or `0b` integer in decimal, or a decimal without its underscores. `None`
/// leaves the text for the tokenizer (to read, or to reject).
fn plain_number(text: &str) -> Option<String> {
    let radix = match text.as_bytes() {
        [b'0', b'x' | b'X', ..] => 16,
        [b'0', b'o' | b'O', ..] => 8,
        [b'0', b'b' | b'B', ..] => 2,
        _ => 10,
    };
    let body = if radix == 10 { text } else { &text[2..] };
    if radix == 10 && !body.contains('_') {
        return None;
    }
    // Each underscore stands between two digits (or right after a prefix).
    let bytes = body.as_bytes();
    let digit = |b: u8| (b as char).is_digit(radix);
    for (k, &b) in bytes.iter().enumerate() {
        if b == b'_' {
            let before = if k == 0 {
                radix != 10
            } else {
                digit(bytes[k - 1])
            };
            if !before || !bytes.get(k + 1).is_some_and(|&n| digit(n)) {
                return None;
            }
        }
    }
    let digits: String = body.chars().filter(|&c| c != '_').collect();
    if radix == 10 {
        return Some(digits);
    }
    if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
        return None;
    }
    Some(to_decimal(&digits, radix))
}

/// Digits in `radix` as decimal digits, however many there are.
fn to_decimal(digits: &str, radix: u32) -> String {
    // Little-endian limbs of 10^9.
    let mut limbs: Vec<u64> = vec![0];
    for c in digits.chars() {
        let mut carry = u64::from(c.to_digit(radix).unwrap_or(0));
        for limb in &mut limbs {
            let v = *limb * u64::from(radix) + carry;
            *limb = v % 1_000_000_000;
            carry = v / 1_000_000_000;
        }
        while carry > 0 {
            limbs.push(carry % 1_000_000_000);
            carry /= 1_000_000_000;
        }
    }
    let mut out = limbs.last().copied().unwrap_or(0).to_string();
    for limb in limbs.iter().rev().skip(1) {
        out.push_str(&format!("{limb:09}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_postgresql_16_numeric_literals() {
        assert_eq!(
            normalize("SELECT 0x1F, 0o17, 0b101, 1_000_000, 0x_1F, 1_000.5_5"),
            "SELECT 31, 15, 5, 1000000, 31, 1000.55"
        );
        assert_eq!(
            normalize("SELECT 0xFFFFFFFFFFFFFFFF"),
            "SELECT 18446744073709551615"
        );
        // Strings, identifiers, comments, and bit strings stay as written.
        let untouched =
            "SELECT '1_000', \"0x1F\", x_1, X'1F', B'101' -- 0x10\n/* 1_0 */ $$0x1$$, $1";
        assert_eq!(normalize(untouched), untouched);
        assert_eq!(normalize("SELECT 1.5e3, 12"), "SELECT 1.5e3, 12");
        // Malformed groupings are left for the tokenizer to reject.
        assert_eq!(normalize("SELECT 1__0"), "SELECT 1__0");
    }
}
