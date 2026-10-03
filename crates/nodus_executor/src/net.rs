//! Network address types (`inet`, `cidr`, `macaddr`, `macaddr8`) and
//! `money`: canonical text forms, operators, and functions. Values stay
//! canonical text, like ranges and timestamps; the declared type tells them
//! apart.
//!
//! An address's canonical form carries its mask (`192.168.1.5/32`);
//! PostgreSQL's display omits the longest mask, which [`show`] strips at the
//! edges. A money value is kept as its plain decimal (`1234.56`); its
//! display adds the currency symbol and thousands separators.

use crate::Value;
use std::net::{Ipv4Addr, Ipv6Addr};

/// A network-family type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kind {
    Inet,
    Cidr,
    MacAddr,
    MacAddr8,
    Money,
}

impl Kind {
    /// The type a declared name is (`inet`, `pg_catalog.cidr`, ...).
    pub(crate) fn of(data_type: &str) -> Option<Kind> {
        let upper = data_type.trim().to_ascii_uppercase();
        let name = upper.rsplit_once('.').map_or(upper.as_str(), |(_, n)| n);
        Some(match name.trim().trim_matches('"') {
            "INET" => Kind::Inet,
            "CIDR" => Kind::Cidr,
            "MACADDR" => Kind::MacAddr,
            "MACADDR8" => Kind::MacAddr8,
            "MONEY" => Kind::Money,
            _ => return None,
        })
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Kind::Inet => "inet",
            Kind::Cidr => "cidr",
            Kind::MacAddr => "macaddr",
            Kind::MacAddr8 => "macaddr8",
            Kind::Money => "money",
        }
    }

    pub(crate) fn oid(self) -> i64 {
        match self {
            Kind::Inet => 869,
            Kind::Cidr => 650,
            Kind::MacAddr => 829,
            Kind::MacAddr8 => 774,
            Kind::Money => 790,
        }
    }

    pub(crate) fn array_oid(self) -> i64 {
        match self {
            Kind::Inet => 1041,
            Kind::Cidr => 651,
            Kind::MacAddr => 1040,
            Kind::MacAddr8 => 775,
            Kind::Money => 791,
        }
    }

    pub(crate) fn of_oid(oid: i64) -> Option<Kind> {
        [
            Kind::Inet,
            Kind::Cidr,
            Kind::MacAddr,
            Kind::MacAddr8,
            Kind::Money,
        ]
        .into_iter()
        .find(|k| k.oid() == oid)
    }
}

/// Every network-family type, for the catalogs.
pub(crate) const KINDS: [Kind; 5] = [
    Kind::Inet,
    Kind::Cidr,
    Kind::MacAddr,
    Kind::MacAddr8,
    Kind::Money,
];

/// Whether a declared type names one of them.
pub(crate) fn is_net_type(data_type: &str) -> bool {
    Kind::of(data_type).is_some()
}

fn invalid(kind: Kind, text: &str) -> String {
    crate::error_fields::DbError::new(format!(
        "invalid input syntax for type {}: \"{text}\"",
        kind.name()
    ))
    .code("22P02")
    .into_text()
}

/// A literal as its type takes it, canonical.
pub(crate) fn from_literal(kind: Kind, text: &str) -> Result<String, String> {
    match kind {
        Kind::Inet => Ok(parse_addr(text)?.text()),
        Kind::Cidr => {
            let addr = parse_addr(text)?;
            if addr.host_bits() != 0 {
                return Err(crate::error_fields::DbError::new(format!(
                    "invalid cidr value: \"{}\"",
                    text.trim()
                ))
                .code("22P02")
                .detail("Value has bits set to right of mask.")
                .into_text());
            }
            Ok(addr.text())
        }
        Kind::MacAddr => Ok(mac_text(&parse_mac(text)?)),
        Kind::MacAddr8 => Ok(mac_text(&parse_mac8(text)?)),
        Kind::Money => Ok(money_text(parse_money(text)?)),
    }
}

/// The display form: PostgreSQL omits the longest mask (`192.168.1.5`, not
/// `192.168.1.5/32`), and money shows its symbol and separators.
pub(crate) fn show(kind: Kind, text: &str) -> String {
    match kind {
        Kind::Inet => match parse_addr(text) {
            Ok(addr) if addr.masklen == addr.width() => addr.address_text(),
            Ok(addr) => addr.text(),
            Err(_) => text.to_string(),
        },
        Kind::Money => match parse_money(text) {
            Ok(cents) => money_display(cents),
            Err(_) => text.to_string(),
        },
        _ => text.to_string(),
    }
}

// --- inet and cidr ---------------------------------------------------------

/// An `inet`/`cidr` value: the address bits and the mask length.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Addr {
    pub(crate) v6: bool,
    pub(crate) bits: u128,
    pub(crate) masklen: u32,
}

impl Addr {
    fn width(&self) -> u32 {
        if self.v6 { 128 } else { 32 }
    }

    /// The low `width` bits, one.
    fn width_mask(&self) -> u128 {
        if self.v6 {
            u128::MAX
        } else {
            u128::from(u32::MAX)
        }
    }

    fn mask(&self) -> u128 {
        if self.masklen == 0 {
            0
        } else if self.masklen == 128 {
            u128::MAX
        } else {
            self.width_mask() & !(self.width_mask() >> self.masklen)
        }
    }

    fn host_bits(&self) -> u128 {
        self.bits & self.width_mask() & !self.mask()
    }

    /// The leading `bits` bits of the address, as PostgreSQL compares them.
    fn prefix(&self, bits: u32) -> u128 {
        if bits >= self.width() {
            self.bits & self.width_mask()
        } else if bits == 0 {
            0
        } else {
            let all = self.width_mask();
            self.bits & all & !(all >> bits)
        }
    }

    fn address_text(&self) -> String {
        if self.v6 {
            Ipv6Addr::from(self.bits).to_string()
        } else {
            Ipv4Addr::from(self.bits as u32).to_string()
        }
    }

    /// The canonical text: the address with its mask.
    fn text(&self) -> String {
        format!("{}/{}", self.address_text(), self.masklen)
    }

    /// The same address with a full-length mask (`netmask`, `hostmask`).
    fn full(&self, bits: u128) -> Addr {
        Addr {
            bits: bits & self.width_mask(),
            masklen: self.width(),
            ..*self
        }
    }

    /// The address the mask selects, keeping the mask length.
    fn with_bits(&self, bits: u128) -> Addr {
        Addr {
            bits: bits & self.width_mask(),
            ..*self
        }
    }
}

fn parse_addr(text: &str) -> Result<Addr, String> {
    let text = text.trim();
    let (address, masklen) = match text.split_once('/') {
        Some((address, mask)) => (
            address.trim(),
            Some(
                mask.trim()
                    .parse::<u32>()
                    .map_err(|_| invalid(Kind::Inet, text))?,
            ),
        ),
        None => (text, None),
    };
    let (v6, bits, width) = if let Ok(v4) = address.parse::<Ipv4Addr>() {
        (false, u128::from(u32::from(v4)), 32)
    } else if let Ok(v6) = address.parse::<Ipv6Addr>() {
        (true, u128::from(v6), 128)
    } else {
        return Err(invalid(Kind::Inet, text));
    };
    let masklen = masklen.unwrap_or(width);
    if masklen > width {
        return Err(invalid(Kind::Inet, text));
    }
    Ok(Addr { v6, bits, masklen })
}

/// The ordering of two addresses: by family, then address, then mask.
pub(crate) fn cmp_inet(a: &str, b: &str) -> std::cmp::Ordering {
    let (Ok(a), Ok(b)) = (parse_addr(a), parse_addr(b)) else {
        return a.cmp(b);
    };
    a.v6.cmp(&b.v6)
        .then(a.bits.cmp(&b.bits))
        .then(a.masklen.cmp(&b.masklen))
}

fn same_family(a: &Addr, b: &Addr) -> Result<(), String> {
    if a.v6 != b.v6 {
        return Err(crate::error_fields::DbError::new(
            "cannot compare addresses of different families",
        )
        .code("22023")
        .into_text());
    }
    Ok(())
}

/// The `inet`/`cidr` operator `op`, or a comparison. `right_is_int` tells
/// `+`/`-` from the address-difference form.
pub(crate) fn inet_operator(
    op: &str,
    left: &str,
    right: &str,
    right_is_int: bool,
) -> Result<Value, String> {
    let a = parse_addr(left)?;
    let result = match op {
        "-" if !right_is_int => return inet_difference(left, right),
        "+" | "-" => {
            let offset: i128 = right
                .trim()
                .parse()
                .map_err(|_| invalid(Kind::Inet, right))?;
            let delta = if op == "+" { offset } else { -offset };
            let value = if delta >= 0 {
                a.bits.checked_add(delta as u128)
            } else {
                a.bits.checked_sub(delta.unsigned_abs())
            }
            .filter(|value| *value <= a.width_mask());
            let Some(value) = value else {
                return Err(crate::error_fields::DbError::new(format!(
                    "{} out of range",
                    if a.v6 { "bigint" } else { "integer" }
                ))
                .code("22003")
                .into_text());
            };
            Value::Text(a.with_bits(value).text())
        }
        "&" | "|" => {
            let b = parse_addr(right)?;
            same_family(&a, &b)?;
            let bits = if op == "&" {
                a.bits & b.bits
            } else {
                a.bits | b.bits
            };
            // As PostgreSQL: a full mask, unlike `~`, which keeps it.
            Value::Text(
                Addr {
                    bits: bits & a.width_mask(),
                    masklen: a.width(),
                    ..a
                }
                .text(),
            )
        }
        "~" => Value::Text(a.with_bits(!a.bits).text()),
        "=" | "<>" | "<" | ">" | "<=" | ">=" => {
            let ord = cmp_inet(left, right);
            use std::cmp::Ordering::*;
            Value::Bool(match op {
                "=" => ord == Equal,
                "<>" => ord != Equal,
                "<" => ord == Less,
                ">" => ord == Greater,
                "<=" => ord != Greater,
                _ => ord != Less,
            })
        }
        "<<=" | ">>=" | "&&" => {
            let b = parse_addr(right)?;
            same_family(&a, &b)?;
            let holds = match op {
                // Contained in (or equal to): the longer-masked address sits
                // in the shorter's network.
                "<<=" => network_sub(&a, &b),
                ">>=" => network_sub(&b, &a),
                // Overlap: the addresses agree down to the shorter mask.
                _ => {
                    let bits = a.masklen.min(b.masklen);
                    a.prefix(bits) == b.prefix(bits)
                }
            };
            Value::Bool(holds)
        }
        other => return Err(format!("unsupported inet operator {other}")),
    };
    Ok(result)
}

/// Whether `a` is a subnet of `b`: its mask is at least as long, and its
/// address agrees with `b`'s down to `b`'s mask.
fn network_sub(a: &Addr, b: &Addr) -> bool {
    a.masklen >= b.masklen && a.prefix(b.masklen) == b.prefix(b.masklen)
}

/// `inet - inet`: the difference of two addresses of one family.
pub(crate) fn inet_difference(left: &str, right: &str) -> Result<Value, String> {
    let a = parse_addr(left)?;
    let b = parse_addr(right)?;
    same_family(&a, &b)?;
    Ok(Value::Int((a.bits as i128 - b.bits as i128) as i64))
}

/// An `inet`/`cidr` function; `None` for any other name. `second` is the
/// second argument's text, where the call has one.
pub(crate) fn inet_function(
    kind: Kind,
    name: &str,
    text: &str,
    second: Option<&str>,
) -> Option<Result<Value, String>> {
    let addr = || parse_addr(text);
    Some(match name {
        "HOST" => addr().map(|a| Value::Text(a.address_text())),
        "NETMASK" => addr().map(|a| Value::Text(a.full(a.mask()).text())),
        "HOSTMASK" => addr().map(|a| Value::Text(a.full(a.mask() ^ a.width_mask()).text())),
        "BROADCAST" => addr().map(|a| {
            let host_ones = a.width_mask() & !a.mask();
            Value::Text(a.with_bits(a.bits | host_ones).text())
        }),
        "NETWORK" => addr().map(|a| Value::Text(a.with_bits(a.bits & a.mask()).text())),
        "MASKLEN" => addr().map(|a| Value::Int(i64::from(a.masklen))),
        "ABBREV" => addr().map(|a| {
            let text = if kind == Kind::Cidr {
                abbrev(&a)
            } else {
                a.text()
            };
            Value::Text(text)
        }),
        "FAMILY" => addr().map(|a| Value::Int(if a.v6 { 6 } else { 4 })),
        "SET_MASKLEN" => match second.and_then(|s| s.trim().parse::<u32>().ok()) {
            Some(masklen) => addr().and_then(|a| {
                if masklen > a.width() {
                    return Err(invalid(Kind::Inet, &a.text()));
                }
                let a = Addr { masklen, ..a };
                // A `cidr`'s host bits go; an `inet` keeps its address.
                let bits = if kind == Kind::Cidr {
                    a.bits & a.mask()
                } else {
                    a.bits
                };
                Ok(Value::Text(a.with_bits(bits).text()))
            }),
            None => return None,
        },
        "INET_SAME_FAMILY" => {
            let Some(b) = second else { return None };
            match (addr(), parse_addr(b)) {
                (Ok(a), Ok(b)) => Ok(Value::Bool(a.v6 == b.v6)),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        }
        "INET_MERGE" => {
            let Some(b) = second else { return None };
            let b = parse_addr(b);
            addr().and_then(|a| {
                let b = b?;
                same_family(&a, &b)?;
                // The smallest network covering both: shrink the shared mask
                // until both addresses agree on it.
                let mut masklen = a.width();
                while masklen > 0
                    && (a.bits & Addr { masklen, ..a }.mask())
                        != (b.bits & Addr { masklen, ..a }.mask())
                {
                    masklen -= 1;
                }
                Ok(Value::Text(
                    Addr {
                        masklen,
                        bits: a.bits,
                        ..a
                    }
                    .with_bits(a.bits & Addr { masklen, ..a }.mask())
                    .text(),
                ))
            })
        }
        "TEXT" => addr().map(|a| Value::Text(a.text())),
        _ => return None,
    })
}

/// `abbrev`: a `cidr`'s trailing zero octets dropped (`192.168.1/24`).
fn abbrev(addr: &Addr) -> String {
    if addr.v6 {
        return addr.text();
    }
    let octets = [
        (addr.bits >> 24) as u8,
        (addr.bits >> 16) as u8,
        (addr.bits >> 8) as u8,
        addr.bits as u8,
    ];
    let significant = ((addr.masklen as usize) + 7) / 8;
    let kept: Vec<String> = octets[..significant.max(1)]
        .iter()
        .map(|o| o.to_string())
        .collect();
    format!("{}/{}", kept.join("."), addr.masklen)
}

// --- MAC addresses ---------------------------------------------------------

/// Parses the input forms into `want` bytes: colon, hyphen, Cisco, bare,
/// and dotted.
fn parse_mac_hex(text: &str, want: usize) -> Option<Vec<u8>> {
    let text = text.trim();
    let digits: Vec<char> = text
        .chars()
        .filter(|c| *c != ':' && *c != '-' && *c != '.')
        .collect();
    if digits.len() != want * 2 || !digits.iter().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(
        (0..want)
            .map(|i| {
                let pair: String = digits[i * 2..i * 2 + 2].iter().collect();
                u8::from_str_radix(&pair, 16).expect("checked hex digits")
            })
            .collect(),
    )
}

fn parse_mac(text: &str) -> Result<Vec<u8>, String> {
    parse_mac_hex(text, 6).ok_or_else(|| invalid(Kind::MacAddr, text.trim()))
}

/// `macaddr8` also takes a 6-byte address, expanded to the modified EUI-64
/// form (`ff:fe` inserted, the seventh bit set).
fn parse_mac8(text: &str) -> Result<Vec<u8>, String> {
    if let Some(bytes) = parse_mac_hex(text, 8) {
        return Ok(bytes);
    }
    if let Some(bytes) = parse_mac_hex(text, 6) {
        let mut out = vec![bytes[0], bytes[1], bytes[2], 0xff, 0xfe];
        out.extend_from_slice(&bytes[3..]);
        return Ok(out);
    }
    Err(invalid(Kind::MacAddr8, text.trim()))
}

/// The canonical text of a MAC address: lowercase hex, colon-separated.
fn mac_text(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// `macaddr8` to `macaddr`: only the modified EUI-64 form converts.
pub(crate) fn mac8_to_mac(text: &str) -> Result<Value, String> {
    let bytes = parse_mac8(text)?;
    if bytes.len() != 8 || bytes[3] != 0xff || bytes[4] != 0xfe {
        return Err(crate::error_fields::DbError::new(
            "macaddr8 data out of range to convert to macaddr",
        )
        .code("22003")
        .hint(
            "Only addresses that have FF and FE as values in the 4th and 5th bytes from the \
             left, for example xx:xx:xx:ff:fe:xx:xx:xx, are eligible to be converted from \
             macaddr8 to macaddr.",
        )
        .into_text());
    }
    let mut mac = vec![bytes[0], bytes[1], bytes[2]];
    mac.extend_from_slice(&bytes[5..]);
    Ok(Value::Text(mac_text(&mac)))
}

/// The ordering of two values of a network-family type, by value.
pub(crate) fn cmp(kind: Kind, a: &str, b: &str) -> std::cmp::Ordering {
    match kind {
        Kind::Inet | Kind::Cidr => cmp_inet(a, b),
        Kind::MacAddr => cmp_mac(a, b, false),
        Kind::MacAddr8 => cmp_mac(a, b, true),
        Kind::Money => cmp_money(a, b),
    }
}

/// The ordering of two MAC addresses, by their bytes.
pub(crate) fn cmp_mac(a: &str, b: &str, eight: bool) -> std::cmp::Ordering {
    let parse = |text: &str| {
        if eight {
            parse_mac8(text)
        } else {
            parse_mac(text)
        }
    };
    match (parse(a), parse(b)) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

// --- money -----------------------------------------------------------------

/// A money value in cents: an exact decimal, rounded half away from zero.
fn parse_money(text: &str) -> Result<i64, String> {
    let text = text.trim();
    let parenthesized = text.starts_with('(') && text.ends_with(')');
    let mut digits: String = text
        .trim_matches(['(', ')'])
        .chars()
        .filter(|c| !matches!(c, '$' | ','))
        .collect();
    if parenthesized && !digits.starts_with('-') {
        digits.insert(0, '-');
    }
    let Some(decimal) = crate::value::parse_decimal(&digits) else {
        return Err(invalid(Kind::Money, text));
    };
    let rendered = decimal.round(2).to_string();
    let (sign, rest) = match rendered.strip_prefix('-') {
        Some(rest) => (-1i128, rest),
        None => (1i128, rendered.as_str()),
    };
    let (whole, fraction) = rest.split_once('.').unwrap_or((rest, ""));
    let whole: i128 = whole.parse().map_err(|_| invalid(Kind::Money, text))?;
    let fraction: i128 = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<2}")
            .chars()
            .take(2)
            .collect::<String>()
            .parse()
            .map_err(|_| invalid(Kind::Money, text))?
    };
    Ok((sign * (whole * 100 + fraction)) as i64)
}

/// The canonical value text: a plain decimal with two digits.
fn money_text(cents: i64) -> String {
    let negative = cents < 0;
    let cents = cents.unsigned_abs();
    format!(
        "{}{}.{:02}",
        if negative { "-" } else { "" },
        cents / 100,
        cents % 100
    )
}

/// Money as text: the cast `money::text` and concatenation show its symbol.
pub(crate) fn money_as_text(text: &str) -> String {
    show(Kind::Money, text)
}

/// The cents of a canonical money value (`sum(money)` adds these).
pub(crate) fn money_cents(text: &str) -> Option<i64> {
    parse_money(text).ok()
}

/// A canonical money value from cents.
pub(crate) fn money_from_cents(cents: i64) -> String {
    money_text(cents)
}

/// `cash_words`: the amount in English words, as PostgreSQL spells it.
pub(crate) fn cash_words(cents: i64) -> String {
    const WORDS: [&str; 28] = [
        "zero",
        "one",
        "two",
        "three",
        "four",
        "five",
        "six",
        "seven",
        "eight",
        "nine",
        "ten",
        "eleven",
        "twelve",
        "thirteen",
        "fourteen",
        "fifteen",
        "sixteen",
        "seventeen",
        "eighteen",
        "nineteen",
        "twenty",
        "thirty",
        "forty",
        "fifty",
        "sixty",
        "seventy",
        "eighty",
        "ninety",
    ];
    // The tens words follow the ones, so `18 + n / 10` names them.
    let tens = |n: i64| WORDS[(18 + n / 10) as usize];
    // A group of up to three digits: "and" appears before a below-twenty
    // remainder only.
    let group = |value: i64| -> String {
        let tu = value % 100;
        if value <= 20 {
            return WORDS[value as usize].to_string();
        }
        if tu == 0 {
            return format!("{} hundred", WORDS[(value / 100) as usize]);
        }
        if value > 99 {
            return if tu % 10 == 0 && tu > 10 {
                format!("{} hundred {}", WORDS[(value / 100) as usize], tens(tu))
            } else if tu < 20 {
                format!(
                    "{} hundred and {}",
                    WORDS[(value / 100) as usize],
                    WORDS[tu as usize]
                )
            } else {
                format!(
                    "{} hundred {} {}",
                    WORDS[(value / 100) as usize],
                    tens(tu),
                    WORDS[(tu % 10) as usize]
                )
            };
        }
        if tu % 10 == 0 {
            return tens(tu).to_string();
        }
        format!("{} {}", tens(tu), WORDS[(tu % 10) as usize])
    };

    let amount = cents.unsigned_abs();
    let dollars = (amount / 100) as i64;
    let cent = (amount % 100) as i64;
    let mut groups = [0i64; 6];
    let mut rest = dollars;
    for group_value in &mut groups {
        *group_value = rest % 1000;
        rest /= 1000;
    }
    const SCALES: [&str; 5] = [
        " thousand ",
        " million ",
        " billion ",
        " trillion ",
        " quadrillion ",
    ];
    let mut words = String::new();
    if cents < 0 {
        words.push_str("minus ");
    }
    for (i, scale) in SCALES.iter().enumerate().rev() {
        if groups[i + 1] != 0 {
            words.push_str(&group(groups[i + 1]));
            words.push_str(scale);
        }
    }
    if groups[0] != 0 {
        words.push_str(&group(groups[0]));
    }
    if dollars == 0 {
        words.push_str("zero");
    }
    words.push_str(if dollars == 1 {
        " dollar and "
    } else {
        " dollars and "
    });
    words.push_str(&if cent == 0 {
        "zero".to_string()
    } else {
        group(cent)
    });
    words.push_str(if cent == 1 { " cent" } else { " cents" });
    let mut chars = words.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => words,
    }
}

/// The display text: a currency symbol, thousands separators, two digits.
fn money_display(cents: i64) -> String {
    let negative = cents < 0;
    let cents = cents.unsigned_abs();
    let whole = (cents / 100).to_string();
    let mut grouped = String::new();
    for (i, c) in whole.chars().enumerate() {
        if i > 0 && (whole.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    format!(
        "{}${grouped}.{:02}",
        if negative { "-" } else { "" },
        cents % 100
    )
}

/// The ordering of two money values, by their amounts.
pub(crate) fn cmp_money(a: &str, b: &str) -> std::cmp::Ordering {
    match (parse_money(a), parse_money(b)) {
        (Ok(a), Ok(b)) => a.cmp(&b),
        _ => a.cmp(b),
    }
}

/// The money operators: `+`, `-` (money or number), `*`, `/`, and
/// comparisons. `right_is_money` tells `money / money` (a ratio) from
/// `money / number`.
pub(crate) fn money_operator(
    op: &str,
    left: &str,
    right: &str,
    right_is_money: bool,
) -> Result<Value, String> {
    let a = parse_money(left)?;
    let result = match op {
        "+" | "-" if right_is_money => {
            let b = parse_money(right)?;
            Value::Text(money_text(if op == "+" { a + b } else { a - b }))
        }
        "*" | "/" => {
            if op == "/" && right_is_money {
                let b = parse_money(right)?;
                if b == 0 {
                    return Err(crate::error_fields::DbError::new("division by zero")
                        .code("22012")
                        .into_text());
                }
                return Ok(Value::Text(crate::value::float_text(a as f64 / b as f64)));
            }
            let b: f64 = right
                .trim()
                .parse()
                .map_err(|_| invalid(Kind::Money, right))?;
            if op == "/" && b == 0.0 {
                return Err(crate::error_fields::DbError::new("division by zero")
                    .code("22012")
                    .into_text());
            }
            let cents = if op == "*" {
                a as f64 * b
            } else {
                a as f64 / b
            };
            Value::Text(money_text(cents.round() as i64))
        }
        "=" | "<>" | "<" | ">" | "<=" | ">=" => {
            let ord = cmp_money(left, right);
            use std::cmp::Ordering::*;
            Value::Bool(match op {
                "=" => ord == Equal,
                "<>" => ord != Equal,
                "<" => ord == Less,
                ">" => ord == Greater,
                "<=" => ord != Greater,
                _ => ord != Less,
            })
        }
        other => return Err(format!("unsupported money operator {other}")),
    };
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lit(kind: Kind, text: &str) -> String {
        from_literal(kind, text).unwrap()
    }

    #[test]
    fn addresses_take_postgresqls_forms() {
        assert_eq!(lit(Kind::Inet, "192.168.1.5"), "192.168.1.5/32");
        assert_eq!(lit(Kind::Inet, "192.168.1.5/24"), "192.168.1.5/24");
        assert_eq!(lit(Kind::Inet, "::1"), "::1/128");
        assert_eq!(lit(Kind::Inet, "2001:db8::1/64"), "2001:db8::1/64");
        assert_eq!(lit(Kind::Cidr, "192.168.1.0/24"), "192.168.1.0/24");
        assert_eq!(lit(Kind::Cidr, "2001:db8::/32"), "2001:db8::/32");
        let err = from_literal(Kind::Cidr, "192.168.1.5/24").unwrap_err();
        assert!(crate::error_message(&err).contains("invalid cidr value"));
        assert!(from_literal(Kind::Inet, "x").is_err());
        assert!(from_literal(Kind::Inet, "192.168.1.256").is_err());
        assert!(from_literal(Kind::Inet, "192.168.1.5/33").is_err());
        // The display omits the longest mask; the canonical text keeps it.
        assert_eq!(show(Kind::Inet, "192.168.1.5/32"), "192.168.1.5");
        assert_eq!(show(Kind::Inet, "192.168.1.5/24"), "192.168.1.5/24");
        assert_eq!(lit(Kind::Inet, "192.168.1.5"), "192.168.1.5/32");
    }

    #[test]
    fn addresses_compare_and_operate() {
        assert_eq!(cmp_inet("9.0.0.1", "10.0.0.1"), std::cmp::Ordering::Less);
        assert_eq!(cmp_inet("::1", "0.0.0.1"), std::cmp::Ordering::Greater);
        assert_eq!(
            inet_operator("=", "192.168.1.5/32", "192.168.1.5", false).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            inet_operator("-", "192.168.1.5/32", "192.168.1.1/32", false).unwrap(),
            Value::Int(4)
        );
        assert_eq!(
            inet_operator("+", "192.168.1.5/24", "5", true).unwrap(),
            Value::Text("192.168.1.10/24".into())
        );
        assert_eq!(
            inet_operator("&", "192.168.1.0/24", "255.255.255.0/32", false).unwrap(),
            Value::Text("192.168.1.0/32".into())
        );
        assert_eq!(
            inet_operator("|", "192.168.1.0/24", "0.0.0.255/32", false).unwrap(),
            Value::Text("192.168.1.255/32".into())
        );
        assert_eq!(
            inet_operator("~", "192.168.1.0/24", "", false).unwrap(),
            Value::Text("63.87.254.255/24".into())
        );
        assert_eq!(
            inet_operator("<<=", "192.168.1.0/24", "192.168.0.0/16", false).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            inet_operator("&&", "192.168.1.0/24", "192.168.1.128/25", false).unwrap(),
            Value::Bool(true)
        );
    }

    #[test]
    fn address_functions_match_postgresql() {
        let call = |name: &str, text: &str, second: Option<&str>| {
            inet_function(Kind::Inet, name, text, second)
                .unwrap()
                .unwrap()
        };
        assert_eq!(
            call("HOST", "192.168.1.5/24", None),
            Value::Text("192.168.1.5".into())
        );
        assert_eq!(
            call("NETMASK", "192.168.1.5/24", None),
            Value::Text("255.255.255.0/32".into())
        );
        assert_eq!(
            call("HOSTMASK", "192.168.1.5/24", None),
            Value::Text("0.0.0.255/32".into())
        );
        assert_eq!(
            call("BROADCAST", "192.168.1.5/24", None),
            Value::Text("192.168.1.255/24".into())
        );
        assert_eq!(
            call("NETWORK", "192.168.1.5/24", None),
            Value::Text("192.168.1.0/24".into())
        );
        assert_eq!(call("MASKLEN", "192.168.1.5/24", None), Value::Int(24));
        // `abbrev` shortens a `cidr`; an `inet` stays whole.
        assert_eq!(
            inet_function(Kind::Cidr, "ABBREV", "192.168.1.0/24", None)
                .unwrap()
                .unwrap(),
            Value::Text("192.168.1/24".into())
        );
        assert_eq!(
            call("ABBREV", "192.168.1.5/24", None),
            Value::Text("192.168.1.5/24".into())
        );
        assert_eq!(call("FAMILY", "::1/128", None), Value::Int(6));
        assert_eq!(
            call("SET_MASKLEN", "192.168.1.5/24", Some("16")),
            Value::Text("192.168.1.5/16".into())
        );
        assert_eq!(
            inet_function(Kind::Cidr, "SET_MASKLEN", "192.168.1.0/24", Some("16"))
                .unwrap()
                .unwrap(),
            Value::Text("192.168.0.0/16".into())
        );
        assert_eq!(
            call("INET_MERGE", "192.168.1.0/24", Some("192.168.2.0/24")),
            Value::Text("192.168.0.0/22".into())
        );
        assert_eq!(
            call("INET_MERGE", "192.168.1.0/24", Some("10.0.0.0/8")),
            Value::Text("0.0.0.0/0".into())
        );
        assert_eq!(
            call("NETMASK", "::1/64", None),
            Value::Text("ffff:ffff:ffff:ffff::/128".into())
        );
    }

    #[test]
    fn mac_addresses_take_their_forms() {
        for text in [
            "08:00:2b:01:02:03",
            "08-00-2b-01-02-03",
            "08002b:010203",
            "08002b010203",
            "0800.2b01.0203",
        ] {
            assert_eq!(lit(Kind::MacAddr, text), "08:00:2b:01:02:03", "{text}");
        }
        assert!(from_literal(Kind::MacAddr, "08:00:2b:01:02").is_err());
        assert!(from_literal(Kind::MacAddr, "zz:00:2b:01:02:03").is_err());
        // 6-byte input becomes the modified EUI-64 form.
        assert_eq!(
            lit(Kind::MacAddr8, "08:00:2b:01:02:03"),
            "08:00:2b:ff:fe:01:02:03"
        );
        assert_eq!(
            lit(Kind::MacAddr8, "0800.2b01.0203"),
            "08:00:2b:ff:fe:01:02:03"
        );
        assert_eq!(
            mac8_to_mac("0a:00:2b:ff:fe:01:02:03").unwrap(),
            Value::Text("0a:00:2b:01:02:03".into())
        );
        assert!(mac8_to_mac("08:00:2b:01:02:03:04:05").is_err());
        assert_eq!(
            cmp_mac("08:00:2b:01:02:03", "09:00:2b:01:02:03", false),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn money_takes_its_forms_and_operates() {
        assert_eq!(lit(Kind::Money, "1234.56"), "1234.56");
        assert_eq!(lit(Kind::Money, "$1,234.56"), "1234.56");
        assert_eq!(lit(Kind::Money, "1.005"), "1.01");
        assert_eq!(lit(Kind::Money, "1234.567"), "1234.57");
        assert_eq!(lit(Kind::Money, "-1234.56"), "-1234.56");
        assert!(from_literal(Kind::Money, "abc").is_err());
        assert_eq!(show(Kind::Money, "1234.56"), "$1,234.56");
        assert_eq!(show(Kind::Money, "-1234.56"), "-$1,234.56");
        assert_eq!(show(Kind::Money, "9.00"), "$9.00");
        let op = |op: &str, a: &str, b: &str, money: bool| money_operator(op, a, b, money).unwrap();
        assert_eq!(
            op("+", "1234.56", "1.00", true),
            Value::Text("1235.56".into())
        );
        assert_eq!(
            op("-", "1234.56", "1.00", true),
            Value::Text("1233.56".into())
        );
        assert_eq!(
            op("*", "1234.56", "2", false),
            Value::Text("2469.12".into())
        );
        assert_eq!(op("/", "1234.56", "2", false), Value::Text("617.28".into()));
        assert_eq!(
            op("/", "1234.56", "2.00", true),
            Value::Text("617.28".into())
        );
        assert_eq!(op("<", "9.00", "100.00", true), Value::Bool(true));
    }

    #[test]
    fn containment_reads_address_prefixes() {
        let holds = |op: &str, a: &str, b: &str| {
            inet_operator(op, a, b, false).unwrap() == Value::Bool(true)
        };
        // A subnet compares the addresses down to the shorter mask.
        assert!(holds("<<=", "192.168.1.5/24", "192.168.0.0/16"));
        assert!(!holds("<<=", "192.168.1.5/16", "192.168.1.0/24"));
        assert!(holds("<<=", "192.168.1.5/24", "192.168.1.0/24"));
        assert!(holds("<<=", "192.168.1.5/32", "192.168.1.0/24"));
        assert!(!holds("<<=", "192.168.1.5/8", "192.168.0.0/16"));
        assert!(holds("<<=", "192.168.1.5/24", "192.168.2.1/16"));
        assert!(holds("<<=", "10.0.0.1/8", "10.1.0.1/8"));
        assert!(!holds("<<=", "10.0.0.1/24", "10.1.0.1/24"));
        // The reverse reads the same relation with the operands swapped.
        assert!(holds(">>=", "192.168.1.0/24", "192.168.1.5/32"));
        assert!(holds(">>=", "192.168.1.5/16", "192.168.1.0/24"));
        // Overlap compares the addresses down to the shorter mask too.
        assert!(holds("&&", "192.168.1.5/24", "192.168.2.1/16"));
        assert!(!holds("&&", "192.168.1.5/24", "10.1.0.1/16"));
        assert!(holds("&&", "10.0.0.1/8", "10.1.0.0/16"));
        // Families never mix.
        assert!(inet_operator("<<=", "192.168.1.5/24", "2001:db8::/32", false).is_err());
    }

    #[test]
    fn money_sums_and_spells_amounts() {
        assert_eq!(money_cents("1234.56"), Some(123456));
        assert_eq!(money_from_cents(123456), "1234.56");
        assert_eq!(money_from_cents(-5), "-0.05");
        // PostgreSQL's wording, including the double space after a scale
        // word that ends the dollar part.
        assert_eq!(cash_words(0), "Zero dollars and zero cents");
        assert_eq!(cash_words(1), "Zero dollars and one cent");
        assert_eq!(cash_words(100), "One dollar and zero cents");
        assert_eq!(cash_words(10_000), "One hundred dollars and zero cents");
        assert_eq!(cash_words(1234), "Twelve dollars and thirty four cents");
        assert_eq!(
            cash_words(-1234),
            "Minus twelve dollars and thirty four cents"
        );
        assert_eq!(
            cash_words(123_456_789),
            "One million two hundred thirty four thousand five hundred sixty seven dollars and eighty nine cents"
        );
        assert_eq!(
            cash_words(100_000_000),
            "One million  dollars and zero cents"
        );
        assert_eq!(cash_words(100_000), "One thousand  dollars and zero cents");
        assert_eq!(
            cash_words(11_000),
            "One hundred and ten dollars and zero cents"
        );
        assert_eq!(
            cash_words(10_500),
            "One hundred and five dollars and zero cents"
        );
        assert_eq!(
            cash_words(12_000),
            "One hundred twenty dollars and zero cents"
        );
        assert_eq!(cash_words(2_100), "Twenty one dollars and zero cents");
        assert_eq!(cash_words(-50), "Minus zero dollars and fifty cents");
        assert_eq!(cash_words(1_01), "One dollar and one cent");
        assert_eq!(
            cash_words(99_999_999_999_999),
            "Nine hundred ninety nine billion nine hundred ninety nine million nine hundred ninety nine thousand nine hundred ninety nine dollars and ninety nine cents"
        );
    }
}

/// A cast between two of the network-family types (`inet` to `cidr` zeroes
/// the host bits; a 6-byte `macaddr` becomes its modified EUI-64 form).
pub(crate) fn cast_between(from: Kind, to: Kind, text: &str) -> Result<Value, String> {
    match (from, to) {
        (Kind::Inet, Kind::Cidr) => {
            let addr = parse_addr(text)?;
            Ok(Value::Text(addr.with_bits(addr.bits & addr.mask()).text()))
        }
        (Kind::Cidr, Kind::Inet) => Ok(Value::Text(parse_addr(text)?.text())),
        (Kind::MacAddr, Kind::MacAddr8) => {
            let bytes = parse_mac(text)?;
            let mut out = vec![bytes[0], bytes[1], bytes[2], 0xff, 0xfe];
            out.extend_from_slice(&bytes[3..]);
            Ok(Value::Text(mac_text(&out)))
        }
        (Kind::MacAddr8, Kind::MacAddr) => mac8_to_mac(text),
        (Kind::Money, Kind::Money) => Ok(Value::Text(money_text(parse_money(text)?))),
        _ => Err(crate::error_fields::DbError::new(format!(
            "cannot cast type {} to {}",
            from.name(),
            to.name()
        ))
        .code("42846")
        .into_text()),
    }
}
