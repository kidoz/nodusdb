//! Bit strings (`bit(n)` and `bit varying(n)`), kept as their text of `0`s
//! and `1`s, which concatenation, `substring`, `position`, `length`, and
//! comparison already treat as PostgreSQL treats the bits. The rest (casts,
//! the bitwise operators, `get_bit`) read their static type.

/// A bit string type's length (`None`: any) and whether it varies; `bit`
/// alone is `bit(1)`.
pub(crate) fn bit_type(data_type: &str) -> Option<(Option<usize>, bool)> {
    let upper = data_type
        .trim()
        .to_ascii_uppercase()
        .replace("BIT VARYING", "VARBIT");
    let (base, length) = match upper.split_once('(') {
        Some((base, rest)) => (
            base.trim().to_string(),
            Some(rest.strip_suffix(')')?.trim().parse::<usize>().ok()?),
        ),
        None => (upper.trim().to_string(), None),
    };
    match base.as_str() {
        "BIT" => Some((Some(length.unwrap_or(1)), false)),
        "VARBIT" => Some((length, true)),
        _ => None,
    }
}

/// Bit string input: `0`s and `1`s (after an optional `B`), or hex digits
/// after an `X`.
pub(crate) fn parse(text: &str) -> Result<String, String> {
    let (hex, digits) = match text.as_bytes().first() {
        Some(b'x' | b'X') => (true, &text[1..]),
        Some(b'b' | b'B') => (false, &text[1..]),
        _ => (false, text),
    };
    if hex {
        return hex_bits(digits).ok_or_else(|| {
            let bad = digits
                .chars()
                .find(|c| !c.is_ascii_hexdigit())
                .unwrap_or(' ');
            format!("\"{bad}\" is not a valid hexadecimal digit")
        });
    }
    match digits.chars().find(|c| !matches!(c, '0' | '1')) {
        Some(bad) => Err(format!("\"{bad}\" is not a valid binary digit")),
        None => Ok(digits.to_string()),
    }
}

/// The bits of hex digits (`X'1F'`), four to a digit.
pub(crate) fn hex_bits(hex: &str) -> Option<String> {
    hex.chars()
        .map(|c| c.to_digit(16).map(|d| format!("{d:04b}")))
        .collect()
}

/// Bits fitted to a bit string type: an explicit cast pads a `bit(n)` with
/// zeros or cuts it, and cuts a `varbit(n)`; a stored value must have the
/// `bit(n)`'s length, or at most the `varbit(n)`'s.
pub(crate) fn fit(bits: &str, data_type: &str, explicit: bool) -> Result<String, String> {
    let Some((Some(length), varying)) = bit_type(data_type) else {
        return Ok(bits.to_string());
    };
    let actual = bits.len();
    if explicit {
        return Ok(if actual >= length {
            bits[..length].to_string()
        } else if varying {
            bits.to_string()
        } else {
            format!("{bits}{}", "0".repeat(length - actual))
        });
    }
    if varying && actual > length {
        return Err(format!(
            "bit string too long for type bit varying({length})"
        ));
    }
    if !varying && actual != length {
        return Err(format!(
            "bit string length {actual} does not match type bit({length})"
        ));
    }
    Ok(bits.to_string())
}

/// An integer as `length` bits, its two's complement extended as needed.
pub(crate) fn from_int(value: i64, length: usize) -> String {
    (0..length)
        .rev()
        .map(|i| {
            let bit = if i >= 64 {
                value < 0
            } else {
                value >> i & 1 == 1
            };
            if bit { '1' } else { '0' }
        })
        .collect()
}

/// Bits as an integer `width` bits wide (two's complement).
pub(crate) fn to_int(bits: &str, width: usize) -> Result<i64, String> {
    if bits.len() > width {
        return Err(if width > 32 {
            "bigint out of range"
        } else {
            "integer out of range"
        }
        .into());
    }
    let unsigned = bits
        .bytes()
        .fold(0u64, |n, b| n << 1 | u64::from(b == b'1'));
    Ok(if width >= 64 {
        unsigned as i64
    } else if bits.len() == width {
        // The top bit is the sign.
        (unsigned as i64) << (64 - width) >> (64 - width)
    } else {
        unsigned as i64
    })
}

/// `&`, `|`, or `#` of two bit strings of one length.
pub(crate) fn binary(symbol: &str, a: &str, b: &str) -> Result<String, String> {
    if a.len() != b.len() {
        let verb = match symbol {
            "&" => "AND",
            "|" => "OR",
            _ => "XOR",
        };
        return Err(format!("cannot {verb} bit strings of different sizes"));
    }
    Ok(a.bytes()
        .zip(b.bytes())
        .map(|(x, y)| {
            let (x, y) = (x == b'1', y == b'1');
            let bit = match symbol {
                "&" => x && y,
                "|" => x || y,
                _ => x != y,
            };
            if bit { '1' } else { '0' }
        })
        .collect())
}

/// `~bits`.
pub(crate) fn not(a: &str) -> String {
    a.chars()
        .map(|c| if c == '1' { '0' } else { '1' })
        .collect()
}

/// `bits << n` (or `>>`), keeping the length and filling with zeros.
pub(crate) fn shift(a: &str, n: i64, left: bool) -> String {
    let (n, left) = if n < 0 { (-n, !left) } else { (n, left) };
    let len = a.len();
    let n = (n as usize).min(len);
    if left {
        format!("{}{}", &a[n..], "0".repeat(n))
    } else {
        format!("{}{}", "0".repeat(n), &a[..len - n])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bit_strings_follow_postgresql() {
        assert_eq!(hex_bits("1F").unwrap(), "00011111");
        assert_eq!(fit("101", "BIT(4)", true).unwrap(), "1010");
        assert_eq!(fit("10101", "BIT(4)", true).unwrap(), "1010");
        assert_eq!(fit("1010", "BIT", true).unwrap(), "1");
        assert_eq!(fit("10101", "VARBIT(3)", true).unwrap(), "101");
        assert!(fit("101", "BIT(4)", false).is_err());
        assert!(fit("101010", "BIT VARYING(5)", false).is_err());
        assert_eq!(from_int(5, 4), "0101");
        assert_eq!(from_int(-1, 8), "11111111");
        assert_eq!(from_int(300, 8), "00101100");
        assert_eq!(to_int(&"1".repeat(32), 32).unwrap(), -1);
        assert_eq!(to_int("101", 64).unwrap(), 5);
        assert_eq!(binary("#", "1010", "0110").unwrap(), "1100");
        assert!(binary("&", "10", "101").is_err());
        assert_eq!(shift("1010", 1, true), "0100");
        assert_eq!(shift("1010", 2, false), "0010");
        assert_eq!(
            parse("102").unwrap_err(),
            "\"2\" is not a valid binary digit"
        );
    }
}
