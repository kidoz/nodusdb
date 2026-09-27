//! Arbitrary-precision `numeric`, as PostgreSQL's: exact decimals of any
//! size that keep their display scale (`1.50`), and `NaN` and `±Infinity`.
//! Division and the mathematical functions choose their result scales by
//! PostgreSQL's rules and round correctly, so they give PostgreSQL's digits.

use std::cmp::Ordering;
use std::fmt;

use num_bigint::{BigInt, Sign};
use num_traits::{One, Signed, ToPrimitive, Zero};

/// The most digits a value keeps after the decimal point (`NUMERIC_DSCALE_MAX`).
const MAX_SCALE: u32 = 16383;
/// The most digits before the decimal point (`NUMERIC_WEIGHT_MAX` base-10000
/// digits).
const MAX_INTEGER_DIGITS: i64 = 131072;
/// Result scales chosen for division and the functions stop here.
const MAX_DISPLAY_SCALE: i64 = 1000;
/// Division and the functions give at least this many significant digits.
const MIN_SIG_DIGITS: i64 = 16;
/// `round` and `trunc` clamp their digit counts to this.
const MAX_RESULT_SCALE: i64 = 2000;
/// Extra digits the functions compute beyond the ones they return.
const GUARD_DIGITS: i64 = 10;

const OVERFLOW: &str = "value overflows numeric format";

/// A `numeric` value.
#[derive(Clone, Debug)]
pub enum Numeric {
    /// `digits / 10^scale`; the scale is the display scale (`1.50` is 150
    /// at scale 2).
    Finite {
        digits: BigInt,
        scale: u32,
    },
    NaN,
    Infinity,
    NegInfinity,
}

fn pow10(n: u64) -> BigInt {
    if n < 20 {
        BigInt::from(10u64.pow(n as u32))
    } else {
        BigInt::from(10u32).pow(u32::try_from(n).unwrap_or(u32::MAX))
    }
}

/// `m * 10^n`.
fn shifted(m: &BigInt, n: u32) -> BigInt {
    match n {
        0 => m.clone(),
        1..20 => m * 10u64.pow(n),
        _ => m * pow10(u64::from(n)),
    }
}

/// `m * 10^n` for any `n`, rounding half away from zero when `n` is negative.
fn shifted_rounded(m: &BigInt, n: i64) -> BigInt {
    if n >= 0 {
        shifted(m, n as u32)
    } else {
        div_round(m, &pow10(n.unsigned_abs()))
    }
}

/// `n / d`, rounded half away from zero.
fn div_round(n: &BigInt, d: &BigInt) -> BigInt {
    let (q, r) = (n / d, n % d);
    if r.is_zero() || (r.abs() << 1u8) < d.abs() {
        q
    } else if (n.sign() == Sign::Minus) != (d.sign() == Sign::Minus) {
        q - 1
    } else {
        q + 1
    }
}

/// The number of decimal digits in `|m|` (none for zero).
fn decimal_digits(m: &BigInt) -> i64 {
    if m.is_zero() {
        return 0;
    }
    let estimate = ((m.bits() - 1) as f64 * std::f64::consts::LOG10_2) as u64 + 1;
    if m.magnitude() >= pow10(estimate).magnitude() {
        estimate as i64 + 1
    } else {
        estimate as i64
    }
}

/// The power of ten of `m / 10^scale`'s leading digit (`m` is not zero).
fn leading_exponent(m: &BigInt, scale: u32) -> i64 {
    decimal_digits(m) - 1 - i64::from(scale)
}

/// `|m / 10^scale|` in base-10000 digits, as PostgreSQL stores it: the weight
/// (the power of 10000) of the first digit, and the digits without leading
/// or trailing zeros. Zero has no digits.
fn base_10000(m: &BigInt, scale: u32) -> (i64, Vec<u16>) {
    if m.is_zero() {
        return (0, Vec::new());
    }
    let scale = scale as usize;
    let mut text = m.magnitude().to_string();
    let fraction_pad = (4 - scale % 4) % 4;
    text.extend(std::iter::repeat_n('0', fraction_pad));
    let fraction_groups = (scale + fraction_pad) / 4;
    let lead = (4 - text.len() % 4) % 4;
    let text = "0".repeat(lead) + &text;
    let mut groups: Vec<u16> = text
        .as_bytes()
        .chunks(4)
        .map(|c| c.iter().fold(0u16, |n, d| n * 10 + u16::from(d - b'0')))
        .collect();
    let mut weight = groups.len() as i64 - fraction_groups as i64 - 1;
    let zeros = groups.iter().take_while(|g| **g == 0).count();
    groups.drain(..zeros);
    weight -= zeros as i64;
    while groups.last() == Some(&0) {
        groups.pop();
    }
    (weight, groups)
}

/// A decimal kept to a number of significant digits: `m * 10^e`.
struct Approx {
    m: BigInt,
    e: i64,
}

impl Approx {
    fn new(m: BigInt, e: i64, digits: i64) -> Approx {
        let excess = decimal_digits(&m) - digits;
        if excess > 0 {
            Approx {
                m: div_round(&m, &pow10(excess as u64)),
                e: e + excess,
            }
        } else {
            Approx { m, e }
        }
    }

    fn mul(&self, other: &Approx, digits: i64) -> Approx {
        Approx::new(&self.m * &other.m, self.e + other.e, digits)
    }

    /// The value rounded to `scale` digits after the decimal point.
    fn to_numeric(&self, scale: i64) -> Numeric {
        Numeric::new(shifted_rounded(&self.m, self.e + scale), scale as u32)
    }

    /// One over the value, rounded to `scale` digits.
    fn reciprocal(&self, scale: i64) -> Numeric {
        let k = scale - self.e;
        let digits = if k >= 0 {
            div_round(&pow10(k as u64), &self.m)
        } else {
            div_round(&BigInt::one(), &shifted(&self.m, (-k) as u32))
        };
        Numeric::new(digits, scale as u32)
    }
}

/// Clamps a chosen result scale as PostgreSQL does.
fn result_scale(scale: i64) -> i64 {
    scale.clamp(0, MAX_DISPLAY_SCALE)
}

impl Numeric {
    pub fn new(digits: impl Into<BigInt>, scale: u32) -> Numeric {
        Numeric::Finite {
            digits: digits.into(),
            scale,
        }
    }

    pub fn zero() -> Numeric {
        Numeric::new(0, 0)
    }

    pub fn one() -> Numeric {
        Numeric::new(1, 0)
    }

    pub fn is_finite(&self) -> bool {
        matches!(self, Numeric::Finite { .. })
    }

    pub fn is_nan(&self) -> bool {
        matches!(self, Numeric::NaN)
    }

    pub fn is_zero(&self) -> bool {
        matches!(self, Numeric::Finite { digits, .. } if digits.is_zero())
    }

    /// -1, 0, or 1 (0 for NaN too).
    pub fn signum(&self) -> i32 {
        match self {
            Numeric::Finite { digits, .. } => match digits.sign() {
                Sign::Minus => -1,
                Sign::NoSign => 0,
                Sign::Plus => 1,
            },
            Numeric::Infinity => 1,
            Numeric::NegInfinity => -1,
            Numeric::NaN => 0,
        }
    }

    pub fn is_sign_negative(&self) -> bool {
        self.signum() < 0
    }

    /// The display scale: digits after the decimal point (none for NaN and
    /// the infinities).
    pub fn scale(&self) -> u32 {
        match self {
            Numeric::Finite { scale, .. } => *scale,
            _ => 0,
        }
    }

    /// Whether the value is a whole number.
    pub fn is_integral(&self) -> bool {
        match self {
            Numeric::Finite { digits, scale } => {
                *scale == 0 || (digits % pow10(u64::from(*scale))).is_zero()
            }
            _ => false,
        }
    }

    pub fn abs(&self) -> Numeric {
        match self {
            Numeric::Finite { digits, scale } => Numeric::new(digits.abs(), *scale),
            Numeric::NegInfinity => Numeric::Infinity,
            other => other.clone(),
        }
    }

    /// The two finite values at a common scale.
    fn aligned<'a>(
        (a, sa): (&'a BigInt, u32),
        (b, sb): (&'a BigInt, u32),
    ) -> (
        std::borrow::Cow<'a, BigInt>,
        std::borrow::Cow<'a, BigInt>,
        u32,
    ) {
        use std::borrow::Cow;
        match sa.cmp(&sb) {
            Ordering::Equal => (Cow::Borrowed(a), Cow::Borrowed(b), sa),
            Ordering::Less => (Cow::Owned(shifted(a, sb - sa)), Cow::Borrowed(b), sb),
            Ordering::Greater => (Cow::Borrowed(a), Cow::Owned(shifted(b, sa - sb)), sa),
        }
    }

    fn sum(&self, other: &Numeric, negate: bool) -> Numeric {
        use Numeric::*;
        let flip = |n: &Numeric| if negate { -n } else { n.clone() };
        match (self, other) {
            (NaN, _) | (_, NaN) => NaN,
            (
                Finite {
                    digits: a,
                    scale: sa,
                },
                Finite {
                    digits: b,
                    scale: sb,
                },
            ) => {
                let (a, b, scale) = Numeric::aligned((a, *sa), (b, *sb));
                let digits = if negate {
                    a.as_ref() - b.as_ref()
                } else {
                    a.as_ref() + b.as_ref()
                };
                Numeric::new(digits, scale)
            }
            (Finite { .. }, _) => flip(other),
            (_, Finite { .. }) => self.clone(),
            _ if self.signum() == flip(other).signum() => self.clone(),
            _ => NaN,
        }
    }

    fn product(&self, other: &Numeric) -> Numeric {
        use Numeric::*;
        match (self, other) {
            (NaN, _) | (_, NaN) => NaN,
            (
                Finite {
                    digits: a,
                    scale: sa,
                },
                Finite {
                    digits: b,
                    scale: sb,
                },
            ) => {
                let product = Numeric::new(a * b, sa + sb);
                if sa + sb > MAX_SCALE {
                    product.round(i64::from(MAX_SCALE))
                } else {
                    product
                }
            }
            _ => match self.signum() * other.signum() {
                0 => NaN,
                1 => Infinity,
                _ => NegInfinity,
            },
        }
    }

    /// The quotient at PostgreSQL's result scale (`select_div_scale`):
    /// enough digits for 16 significant ones, and at least either operand's
    /// scale.
    pub fn checked_div(&self, other: &Numeric) -> Result<Numeric, String> {
        match (self, other) {
            (
                Numeric::Finite {
                    digits: a,
                    scale: sa,
                },
                Numeric::Finite {
                    digits: b,
                    scale: sb,
                },
            ) => {
                let first = |m: &BigInt, s: u32| {
                    let (weight, groups) = base_10000(m, s);
                    (weight, groups.first().copied().unwrap_or(0))
                };
                let ((w1, f1), (w2, f2)) = (first(a, *sa), first(b, *sb));
                let qweight = w1 - w2 - i64::from(f1 <= f2);
                let rscale = result_scale(
                    (MIN_SIG_DIGITS - qweight * 4)
                        .max(i64::from(*sa))
                        .max(i64::from(*sb)),
                );
                self.div_rounded(other, rscale)
            }
            _ => self.special_div(other),
        }
    }

    /// Division with an infinite or NaN operand.
    fn special_div(&self, other: &Numeric) -> Result<Numeric, String> {
        use Numeric::*;
        Ok(match (self, other) {
            (NaN, _) | (_, NaN) => NaN,
            (Infinity | NegInfinity, Finite { .. }) => match other.signum() {
                0 => return Err("division by zero".into()),
                s if s == self.signum() => Infinity,
                _ => NegInfinity,
            },
            (Infinity | NegInfinity, _) => NaN,
            _ => Numeric::zero(),
        })
    }

    /// The quotient rounded half away from zero to `rscale` digits.
    pub fn div_rounded(&self, other: &Numeric, rscale: i64) -> Result<Numeric, String> {
        let (
            Numeric::Finite {
                digits: a,
                scale: sa,
            },
            Numeric::Finite {
                digits: b,
                scale: sb,
            },
        ) = (self, other)
        else {
            return self.special_div(other);
        };
        if b.is_zero() {
            return Err("division by zero".into());
        }
        // a/10^sa / (b/10^sb) * 10^rscale = a * 10^(rscale + sb - sa) / b
        let k = rscale + i64::from(*sb) - i64::from(*sa);
        let q = if k >= 0 {
            div_round(&shifted(a, k as u32), b)
        } else {
            div_round(a, &shifted(b, (-k) as u32))
        };
        Ok(Numeric::new(q, rscale as u32))
    }

    /// The quotient truncated to a whole number, as `div(numeric, numeric)`.
    pub fn div_trunc(&self, other: &Numeric) -> Result<Numeric, String> {
        let (
            Numeric::Finite {
                digits: a,
                scale: sa,
            },
            Numeric::Finite {
                digits: b,
                scale: sb,
            },
        ) = (self, other)
        else {
            return self.special_div(other);
        };
        if b.is_zero() {
            return Err("division by zero".into());
        }
        let (a, b, _) = Numeric::aligned((a, *sa), (b, *sb));
        Ok(Numeric::new(a.as_ref() / b.as_ref(), 0))
    }

    /// The greatest common divisor (or with `lcm`, the least common
    /// multiple) of two numerics, at the larger scale.
    pub fn gcd_lcm(&self, other: &Numeric, lcm: bool) -> Result<Numeric, String> {
        let (
            Numeric::Finite {
                digits: a,
                scale: sa,
            },
            Numeric::Finite {
                digits: b,
                scale: sb,
            },
        ) = (self, other)
        else {
            return Ok(Numeric::NaN);
        };
        let (a, b, scale) = Numeric::aligned((a, *sa), (b, *sb));
        let (a, b) = (a.abs(), b.abs());
        let (mut x, mut y) = (a.clone(), b.clone());
        while !y.is_zero() {
            let r = &x % &y;
            x = y;
            y = r;
        }
        if !lcm {
            return Ok(Numeric::new(x, scale));
        }
        if x.is_zero() {
            return Ok(Numeric::new(0, scale));
        }
        Numeric::new(a / &x * b, scale).checked_range()
    }

    /// The remainder of truncating division, with the dividend's sign, at
    /// the larger operand scale.
    pub fn checked_rem(&self, other: &Numeric) -> Result<Numeric, String> {
        use Numeric::*;
        match (self, other) {
            (NaN, _) | (_, NaN) => Ok(NaN),
            (Infinity | NegInfinity, _) if other.is_zero() => Err("division by zero".into()),
            (Infinity | NegInfinity, _) => Ok(NaN),
            (Finite { .. }, Infinity | NegInfinity) => Ok(self.clone()),
            (
                Finite {
                    digits: a,
                    scale: sa,
                },
                Finite {
                    digits: b,
                    scale: sb,
                },
            ) => {
                if b.is_zero() {
                    return Err("division by zero".into());
                }
                let (a, b, scale) = Numeric::aligned((a, *sa), (b, *sb));
                Ok(Numeric::new(a.as_ref() % b.as_ref(), scale))
            }
        }
    }

    /// Rounded half away from zero to `scale` digits after the decimal point
    /// (before it when negative), as `round(numeric, int)`: the result shows
    /// `max(scale, 0)` digits.
    pub fn round(&self, scale: i64) -> Numeric {
        self.round_with(scale, true)
    }

    /// Truncated to `scale` digits, as `trunc(numeric, int)`.
    pub fn trunc(&self, scale: i64) -> Numeric {
        self.round_with(scale, false)
    }

    fn round_with(&self, scale: i64, round: bool) -> Numeric {
        let Numeric::Finite { digits, scale: s } = self else {
            return self.clone();
        };
        let scale = scale.clamp(-MAX_RESULT_SCALE, MAX_RESULT_SCALE);
        let target = scale.max(0) as u32;
        if scale >= i64::from(*s) {
            return Numeric::new(shifted(digits, target - s), target);
        }
        let unit = pow10((i64::from(*s) - scale) as u64);
        let q = if round {
            div_round(digits, &unit)
        } else {
            digits / &unit
        };
        if scale >= 0 {
            Numeric::new(q, target)
        } else {
            Numeric::new(shifted(&q, (-scale) as u32), 0)
        }
    }

    /// The largest whole number not above the value.
    pub fn floor(&self) -> Numeric {
        self.to_whole(false)
    }

    /// The smallest whole number not below the value.
    pub fn ceil(&self) -> Numeric {
        self.to_whole(true)
    }

    fn to_whole(&self, up: bool) -> Numeric {
        let Numeric::Finite { digits, scale } = self else {
            return self.clone();
        };
        let unit = pow10(u64::from(*scale));
        let (q, r) = (digits / &unit, digits % &unit);
        let q = match r.sign() {
            Sign::Plus if up => q + 1,
            Sign::Minus if !up => q - 1,
            _ => q,
        };
        Numeric::new(q, 0)
    }

    /// The value without trailing fractional zeros (`1.500` is `1.5`), as
    /// `trim_scale`.
    pub fn normalize(&self) -> Numeric {
        let Numeric::Finite { digits, scale } = self else {
            return self.clone();
        };
        if digits.is_zero() {
            return Numeric::zero();
        }
        let (mut digits, mut scale) = (digits.clone(), *scale);
        let ten = BigInt::from(10);
        while scale > 0 && (&digits % &ten).is_zero() {
            digits /= &ten;
            scale -= 1;
        }
        Numeric::new(digits, scale)
    }

    /// The value shown with `scale` digits after the decimal point, rounding
    /// when that drops digits.
    pub fn with_scale(&self, scale: u32) -> Numeric {
        self.round(i64::from(scale))
    }

    /// The nearest whole number (halves away from zero) as an `i64`, if the
    /// value is finite and it fits.
    pub fn to_i64(&self) -> Option<i64> {
        match self.round(0) {
            Numeric::Finite { digits, .. } => digits.to_i64(),
            _ => None,
        }
    }

    /// The error converting the value to an integer type fails with.
    pub fn integer_error(&self, type_name: &str) -> String {
        match self {
            Numeric::NaN => format!("cannot convert NaN to {type_name}"),
            Numeric::Infinity | Numeric::NegInfinity => {
                format!("cannot convert infinity to {type_name}")
            }
            Numeric::Finite { .. } => format!("{type_name} out of range"),
        }
    }

    /// The nearest float, read from the digits as PostgreSQL converts.
    pub fn to_f64(&self) -> f64 {
        match self {
            Numeric::Finite { digits, scale } => {
                if *scale <= 22
                    && let Some(d) = digits.to_i64()
                    && d.unsigned_abs() < 1 << 53
                {
                    // Both exact, so the one division rounds correctly.
                    return d as f64 / 10f64.powi(*scale as i32);
                }
                self.to_string().parse().unwrap_or(f64::NAN)
            }
            Numeric::NaN => f64::NAN,
            Numeric::Infinity => f64::INFINITY,
            Numeric::NegInfinity => f64::NEG_INFINITY,
        }
    }

    /// A `double precision` as a numeric, as PostgreSQL converts it: to 15
    /// significant digits.
    pub fn from_f64(value: f64) -> Numeric {
        Numeric::from_float(value, 15)
    }

    /// A `real` as a numeric, to its 6 significant digits.
    pub fn from_f32(value: f32) -> Numeric {
        Numeric::from_float(f64::from(value), 6)
    }

    fn from_float(value: f64, significant: usize) -> Numeric {
        if value.is_nan() {
            return Numeric::NaN;
        }
        if value.is_infinite() {
            return if value > 0.0 {
                Numeric::Infinity
            } else {
                Numeric::NegInfinity
            };
        }
        let text = format!("{:.*e}", significant - 1, value);
        let (mantissa, exponent) = text.split_once('e').unwrap_or((&text, "0"));
        let mantissa = if mantissa.contains('.') {
            mantissa.trim_end_matches('0').trim_end_matches('.')
        } else {
            mantissa
        };
        Numeric::parse(&format!("{mantissa}e{exponent}")).unwrap_or_else(|_| Numeric::zero())
    }

    /// Exactly the float's binary value, for comparing floats with numerics.
    pub fn from_f64_exact(value: f64) -> Option<Numeric> {
        if !value.is_finite() {
            return None;
        }
        Numeric::parse(&format!("{value:e}")).ok()
    }

    /// Parses `numeric` input text as PostgreSQL does: an optionally signed
    /// decimal with an exponent (`-1.5e3`), a `0x`, `0o`, or `0b` integer,
    /// digits grouped by underscores (`1_000`), `NaN`, and `Infinity`/`inf`.
    pub fn parse(text: &str) -> Result<Numeric, String> {
        let invalid = || format!("invalid input syntax for type numeric: \"{text}\"");
        let t =
            text.trim_matches(|c: char| matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c'));
        match t.to_ascii_lowercase().as_str() {
            "nan" => return Ok(Numeric::NaN),
            "infinity" | "+infinity" | "inf" | "+inf" => return Ok(Numeric::Infinity),
            "-infinity" | "-inf" => return Ok(Numeric::NegInfinity),
            _ => {}
        }
        let (negative, body) = match t.as_bytes().first() {
            Some(b'-') => (true, &t[1..]),
            Some(b'+') => (false, &t[1..]),
            _ => (false, t),
        };
        let sign = |m: BigInt| if negative { -m } else { m };
        let radix = match body.as_bytes() {
            [b'0', b'x' | b'X', ..] => 16,
            [b'0', b'o' | b'O', ..] => 8,
            [b'0', b'b' | b'B', ..] => 2,
            _ => 10,
        };
        if radix != 10 {
            let digits = ungrouped(&body[2..], radix).ok_or_else(invalid)?;
            let m = BigInt::parse_bytes(digits.as_bytes(), radix).ok_or_else(invalid)?;
            return Ok(Numeric::new(sign(m), 0));
        }
        let (mantissa, exponent) = match body.find(['e', 'E']) {
            Some(i) => (&body[..i], Some(&body[i + 1..])),
            None => (body, None),
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        let whole = if whole.is_empty() {
            String::new()
        } else {
            ungrouped(whole, 10).ok_or_else(invalid)?
        };
        let fraction = if fraction.is_empty() {
            String::new()
        } else {
            ungrouped(fraction, 10).ok_or_else(invalid)?
        };
        if whole.is_empty() && fraction.is_empty() {
            return Err(invalid());
        }
        let exponent: i64 = match exponent {
            None => 0,
            Some(e) => {
                let (negative, digits) = match e.as_bytes().first() {
                    Some(b'-') => (true, &e[1..]),
                    Some(b'+') => (false, &e[1..]),
                    _ => (false, e),
                };
                let digits = ungrouped(digits, 10).ok_or_else(invalid)?;
                let n: i64 = digits.parse().map_err(|_| OVERFLOW.to_string())?;
                if n > i64::from(i32::MAX / 2) {
                    return Err(OVERFLOW.into());
                }
                if negative { -n } else { n }
            }
        };
        let m =
            BigInt::parse_bytes(format!("{whole}{fraction}").as_bytes(), 10).ok_or_else(invalid)?;
        let scale = fraction.len() as i64 - exponent;
        let value = if scale >= 0 {
            Numeric::new(sign(m), scale as u32)
        } else {
            Numeric::new(shifted(&sign(m), (-scale) as u32), 0)
        };
        value.checked_range()
    }

    /// The value, or the overflow error when it is beyond what a numeric
    /// holds.
    fn checked_range(self) -> Result<Numeric, String> {
        if let Numeric::Finite { digits, scale } = &self {
            if *scale > MAX_SCALE {
                return Err(OVERFLOW.into());
            }
            if !digits.is_zero() && leading_exponent(digits, *scale) >= MAX_INTEGER_DIGITS {
                return Err(OVERFLOW.into());
            }
        }
        Ok(self)
    }

    /// Applies a `numeric(precision, scale)` type modifier: rounds to the
    /// scale and rejects a value that then needs more than `precision -
    /// scale` digits before the decimal point.
    pub fn apply_typmod(&self, precision: i64, scale: i64) -> Result<Numeric, String> {
        let overflow = |detail: String| {
            crate::error_fields::DbError::new("numeric field overflow")
                .detail(detail)
                .into_text()
        };
        match self {
            Numeric::NaN => Ok(Numeric::NaN),
            Numeric::Infinity | Numeric::NegInfinity => Err(overflow(format!(
                "A field with precision {precision}, scale {scale} cannot hold an infinite value."
            ))),
            Numeric::Finite { .. } => {
                let rounded = self.round(scale);
                let max_digits = precision - scale;
                if let Numeric::Finite { digits, scale: s } = &rounded
                    && !digits.is_zero()
                    && leading_exponent(digits, *s) + 1 > max_digits
                {
                    let bound = if max_digits == 0 {
                        "1".to_string()
                    } else {
                        format!("10^{max_digits}")
                    };
                    return Err(overflow(format!(
                        "A field with precision {precision}, scale {scale} must round to an absolute value less than {bound}."
                    )));
                }
                Ok(rounded)
            }
        }
    }

    /// The value in PostgreSQL's binary `numeric` format.
    pub fn to_binary(&self) -> Vec<u8> {
        let (weight, groups, sign, scale) = match self {
            Numeric::Finite { digits, scale } => {
                let (weight, groups) = base_10000(digits, *scale);
                let sign = if digits.is_negative() { 0x4000 } else { 0 };
                (weight as i16, groups, sign, *scale as u16)
            }
            Numeric::NaN => (0, Vec::new(), 0xC000u16, 0),
            Numeric::Infinity => (0, Vec::new(), 0xD000, 0),
            Numeric::NegInfinity => (0, Vec::new(), 0xF000, 0),
        };
        let mut out = Vec::with_capacity(8 + groups.len() * 2);
        out.extend_from_slice(&(groups.len() as i16).to_be_bytes());
        out.extend_from_slice(&weight.to_be_bytes());
        out.extend_from_slice(&sign.to_be_bytes());
        out.extend_from_slice(&scale.to_be_bytes());
        for g in groups {
            out.extend_from_slice(&g.to_be_bytes());
        }
        out
    }

    /// Reads PostgreSQL's binary `numeric` format.
    pub fn from_binary(bytes: &[u8]) -> Option<Numeric> {
        let word = |i: usize| -> Option<u16> {
            Some(u16::from_be_bytes([*bytes.get(i)?, *bytes.get(i + 1)?]))
        };
        let count = word(0)? as usize;
        let weight = i64::from(word(2)? as i16);
        let sign = word(4)?;
        let scale = u32::from(word(6)?);
        match sign {
            0xC000 => return Some(Numeric::NaN),
            0xD000 => return Some(Numeric::Infinity),
            0xF000 => return Some(Numeric::NegInfinity),
            _ => {}
        }
        let mut m = BigInt::zero();
        for i in 0..count {
            m = m * 10000u32 + word(8 + i * 2)?;
        }
        if sign == 0x4000 {
            m = -m;
        }
        // m * 10000^(weight - count + 1), shown at `scale`.
        let exponent = (weight - count as i64 + 1) * 4;
        Some(Numeric::new(
            shifted_rounded(&m, exponent + i64::from(scale)),
            scale,
        ))
    }

    /// The square root, to at least 16 significant digits and the argument's
    /// scale.
    pub fn sqrt(&self) -> Result<Numeric, String> {
        let negative = || "cannot take square root of a negative number".to_string();
        let Numeric::Finite { digits, scale } = self else {
            return match self {
                Numeric::NegInfinity => Err(negative()),
                other => Ok(other.clone()),
            };
        };
        if digits.is_negative() {
            return Err(negative());
        }
        let (weight, _) = base_10000(digits, *scale);
        let sweight = (weight + 1) * 2 - 1;
        let rscale = result_scale((MIN_SIG_DIGITS - sweight).max(i64::from(*scale)));
        Ok(self.sqrt_to(rscale))
    }

    /// The square root of a non-negative value, rounded to `rscale` digits.
    pub fn sqrt_to(&self, rscale: i64) -> Numeric {
        let Numeric::Finite { digits, scale } = self else {
            return self.clone();
        };
        // floor(sqrt(x) * 10^(rscale + 1)), then rounded at the last digit.
        let p = rscale + 1;
        let radicand = {
            let shift = 2 * p - i64::from(*scale);
            if shift >= 0 {
                shifted(digits, shift as u32)
            } else {
                digits / pow10(shift.unsigned_abs())
            }
        };
        let root = (radicand.abs().sqrt() + 5) / 10;
        Numeric::new(root, rscale as u32)
    }

    /// `e` raised to the value, to at least 16 significant digits and the
    /// argument's scale.
    pub fn exp(&self) -> Result<Numeric, String> {
        match self {
            Numeric::Finite { scale, .. } => {
                let weight = (self.to_f64() * std::f64::consts::LOG10_E).clamp(-2000.0, 2000.0);
                let rscale = result_scale((MIN_SIG_DIGITS - weight as i64).max(i64::from(*scale)));
                self.exp_to(rscale)
            }
            Numeric::NegInfinity => Ok(Numeric::zero()),
            other => Ok(other.clone()),
        }
    }

    /// `e` raised to the (finite) value, rounded to `rscale` digits.
    fn exp_to(&self, rscale: i64) -> Result<Numeric, String> {
        let Numeric::Finite { digits, scale } = self else {
            return Ok(self.clone());
        };
        let x = self.to_f64();
        if x.abs() >= 3.0 * MAX_RESULT_SCALE as f64 {
            return if x > 0.0 {
                Err(OVERFLOW.into())
            } else {
                Ok(Numeric::new(0, rscale as u32))
            };
        }
        if digits.is_zero() {
            return Ok(Numeric::new(pow10(rscale as u64), rscale as u32));
        }
        // exp(|x|) = exp(|x| / 2^n)^(2^n), with |x| / 2^n at most 0.1.
        let halvings = if x.abs() > 0.1 {
            (x.abs() * 10.0).log2().ceil() as u32
        } else {
            0
        };
        let integer_digits = if x > 0.0 {
            (x * std::f64::consts::LOG10_E).floor() as i64 + 1
        } else {
            0
        };
        let work = rscale
            + integer_digits.max(0)
            + GUARD_DIGITS
            + (f64::from(halvings) * std::f64::consts::LOG10_2).ceil() as i64;
        let one = pow10(work as u64);
        let r = div_round(
            &shifted_rounded(&digits.abs(), work - i64::from(*scale)),
            &(BigInt::one() << halvings),
        );
        let (mut sum, mut term) = (one.clone(), one.clone());
        let mut k = 1u32;
        loop {
            term = div_round(&(&term * &r), &(&one * k));
            if term.is_zero() {
                break;
            }
            sum += &term;
            k += 1;
        }
        for _ in 0..halvings {
            sum = div_round(&(&sum * &sum), &one);
        }
        if digits.is_negative() {
            sum = div_round(&(&one * &one), &sum);
        }
        Ok(Numeric::new(sum, work as u32).round(rscale))
    }

    /// The natural logarithm, to at least 16 significant digits and the
    /// argument's scale.
    pub fn ln(&self) -> Result<Numeric, String> {
        match self {
            Numeric::Finite { scale, .. } => {
                check_log_argument(self)?;
                let rscale = result_scale(
                    (MIN_SIG_DIGITS - estimate_ln_weight(self)).max(i64::from(*scale)),
                );
                Ok(self.ln_to(rscale))
            }
            Numeric::NegInfinity => Err("cannot take logarithm of a negative number".into()),
            other => Ok(other.clone()),
        }
    }

    /// The natural logarithm of the (finite, positive) value, rounded to
    /// `rscale` digits.
    fn ln_to(&self, rscale: i64) -> Numeric {
        let Numeric::Finite { digits, scale } = self else {
            return self.clone();
        };
        if digits == &pow10(u64::from(*scale)) {
            return Numeric::new(0, rscale as u32);
        }
        // x = y * 10^e with 1 <= y < 10; ln(x) = ln(y) + e * ln(10).
        let e = leading_exponent(digits, *scale);
        let work = rscale + GUARD_DIGITS + 4 + decimal_digits(&BigInt::from(e)) + 1;
        let one = pow10(work as u64);
        let y = shifted_rounded(digits, work - i64::from(*scale) - e);
        let mut ln = ln_reduced(&y, &one);
        if e != 0 {
            ln += ln_reduced(&(&one * 10u32), &one) * e;
        }
        Numeric::new(ln, work as u32).round(rscale)
    }

    /// The logarithm of `num` to base `self`, as `log(b, x)`.
    pub fn log(&self, num: &Numeric) -> Result<Numeric, String> {
        use Numeric::*;
        let (base, x) = (self, num);
        if !base.is_finite() || !x.is_finite() {
            if base.is_nan() || x.is_nan() {
                return Ok(NaN);
            }
            if base.is_sign_negative() || x.is_sign_negative() {
                return Err("cannot take logarithm of a negative number".into());
            }
            if base.is_zero() || x.is_zero() {
                return Err("cannot take logarithm of zero".into());
            }
            return Ok(match (base, x) {
                (Infinity, Infinity) => NaN,
                (Infinity, _) => Numeric::zero(),
                _ => Infinity,
            });
        }
        check_log_argument(base)?;
        check_log_argument(x)?;
        let base_weight = estimate_ln_weight(base);
        let x_weight = estimate_ln_weight(x);
        let result_weight = x_weight - base_weight;
        let rscale = result_scale(
            (MIN_SIG_DIGITS - result_weight)
                .max(i64::from(base.scale()))
                .max(i64::from(x.scale())),
        );
        let ln_base = base.ln_to((rscale + result_weight - base_weight + 8).max(0));
        let ln_x = x.ln_to((rscale + result_weight - x_weight + 8).max(0));
        ln_x.div_rounded(&ln_base, rscale)
    }

    /// The value raised to `exp`, as `power(numeric, numeric)` and `^`.
    pub fn power(&self, exp: &Numeric) -> Result<Numeric, String> {
        if !self.is_finite() || !exp.is_finite() {
            return self.special_power(exp);
        }
        let complex = || {
            "a negative number raised to a non-integer power yields a complex result".to_string()
        };
        if self.is_zero() && exp.is_sign_negative() {
            return Err("zero raised to a negative power is undefined".into());
        }
        let whole = if exp.is_integral() {
            match exp.round(0) {
                Numeric::Finite { digits, .. } => Some(digits),
                _ => None,
            }
        } else {
            None
        };
        if let Some(n) = whole.as_ref().and_then(|n| n.to_i32()) {
            return self.power_int(n, exp.scale());
        }
        if self.is_zero() {
            return Ok(Numeric::new(0, MIN_SIG_DIGITS as u32));
        }
        let negate = if self.is_sign_negative() {
            match &whole {
                None => return Err(complex()),
                Some(n) => (n % 2u32).is_one() || (n % 2u32) == BigInt::from(-1),
            }
        } else {
            false
        };
        let base = self.abs();
        // Estimate the result's weight from a low-precision exp * ln(base),
        // then compute ln(base) to enough digits for the result's.
        let ln_weight = estimate_ln_weight(&base);
        let low = (8 - ln_weight).max(0);
        let estimate = (&base.ln_to(low) * exp).round(low).to_f64();
        if estimate.abs() > 3.01 * MAX_RESULT_SCALE as f64 {
            return if estimate > 0.0 {
                Err(OVERFLOW.into())
            } else {
                Ok(Numeric::new(0, MAX_DISPLAY_SCALE as u32))
            };
        }
        let weight = estimate * std::f64::consts::LOG10_E;
        let rscale = result_scale(
            (MIN_SIG_DIGITS - weight as i64)
                .max(i64::from(base.scale()))
                .max(i64::from(exp.scale())),
        );
        let significant = (rscale + weight as i64).max(0);
        let local = (significant - ln_weight + 8).max(0);
        let ln_num = (&base.ln_to(local) * exp).round(local);
        let result = ln_num.exp_to(rscale)?;
        Ok(if negate && !result.is_zero() {
            -result
        } else {
            result
        })
    }

    /// A finite value raised to a whole power.
    fn power_int(&self, exp: i32, exp_scale: u32) -> Result<Numeric, String> {
        let Numeric::Finite { digits, scale } = self else {
            return Ok(self.clone());
        };
        // base ~= f * 10^p, so the result's decimal weight is about
        // exp * (log10(f) + p).
        let (weight, groups) = base_10000(digits, *scale);
        let f = if groups.is_empty() {
            0.0
        } else {
            let mut f = f64::from(groups[0]);
            let mut p = weight * 4;
            for g in groups.iter().skip(1).take(3) {
                f = f * 10000.0 + f64::from(*g);
                p -= 4;
            }
            f64::from(exp) * (f.log10() + p as f64)
        };
        if f > ((32767 + 1) * 4) as f64 {
            return Err(OVERFLOW.into());
        }
        if f + 1.0 < -(MAX_DISPLAY_SCALE as f64) {
            return Ok(Numeric::new(0, MAX_DISPLAY_SCALE as u32));
        }
        let rscale = result_scale(
            (MIN_SIG_DIGITS - f as i64)
                .max(i64::from(*scale))
                .max(i64::from(exp_scale)),
        );
        match exp {
            0 => return Ok(Numeric::new(pow10(rscale as u64), rscale as u32)),
            1 => return Ok(self.round(rscale)),
            -1 => return Numeric::one().div_rounded(self, rscale),
            2 => return Ok((self * self).round(rscale)),
            _ => {}
        }
        if digits.is_zero() {
            return if exp < 0 {
                Err("division by zero".into())
            } else {
                Ok(Numeric::new(0, rscale as u32))
            };
        }
        let significant =
            1 + rscale + f as i64 + (f64::from(exp).abs().ln() as i64) + 8 + GUARD_DIGITS;
        let mut base = Approx::new(digits.clone(), -i64::from(*scale), significant);
        let mut result = Approx {
            m: BigInt::one(),
            e: 0,
        };
        let mut n = exp.unsigned_abs();
        loop {
            if n & 1 == 1 {
                result = result.mul(&base, significant);
            }
            n >>= 1;
            if n == 0 {
                break;
            }
            base = base.mul(&base, significant);
        }
        Ok(if exp < 0 {
            result.reciprocal(rscale)
        } else {
            result.to_numeric(rscale)
        })
    }

    /// `power` with a NaN or infinite operand, by the POSIX `pow` rules.
    fn special_power(&self, exp: &Numeric) -> Result<Numeric, String> {
        use Numeric::*;
        let one = Numeric::one();
        if self.is_nan() {
            return Ok(if exp.is_zero() { one } else { NaN });
        }
        if exp.is_nan() {
            return Ok(if *self == one { one } else { NaN });
        }
        if self.is_zero() && exp.is_sign_negative() {
            return Err("zero raised to a negative power is undefined".into());
        }
        if self.is_sign_negative() && !exp.is_integral() && exp.is_finite() {
            return Err(
                "a negative number raised to a non-integer power yields a complex result".into(),
            );
        }
        if *self == one || exp.is_zero() {
            return Ok(one);
        }
        if self.is_zero() && exp.signum() > 0 {
            return Ok(Numeric::zero());
        }
        if !exp.is_finite() {
            if *self == -&one {
                return Ok(one);
            }
            let above_one = !self.is_finite() || self.abs() > one;
            return Ok(if above_one == (exp.signum() > 0) {
                Infinity
            } else {
                Numeric::zero()
            });
        }
        if matches!(self, Infinity) {
            return Ok(if exp.signum() > 0 {
                Infinity
            } else {
                Numeric::zero()
            });
        }
        // -Infinity to a finite power.
        if exp.signum() < 0 {
            return Ok(Numeric::zero());
        }
        let odd = exp.is_integral()
            && matches!(exp.round(0), Finite { ref digits, .. } if !(digits % 2u32).is_zero());
        Ok(if odd { NegInfinity } else { Infinity })
    }
}

/// Fails for the arguments a logarithm is undefined at.
fn check_log_argument(x: &Numeric) -> Result<(), String> {
    match x.signum() {
        0 => Err("cannot take logarithm of zero".into()),
        -1 => Err("cannot take logarithm of a negative number".into()),
        _ => Ok(()),
    }
}

/// PostgreSQL's estimate of the decimal weight of `ln(x)`
/// (`estimate_ln_dweight`), which sets the scales logarithms work at.
fn estimate_ln_weight(x: &Numeric) -> i64 {
    let Numeric::Finite { digits, scale } = x else {
        return 0;
    };
    if !digits.is_positive() {
        return 0;
    }
    let near_one = |bound: (i64, u32)| Numeric::new(bound.0, bound.1);
    if *x >= near_one((9, 1)) && *x <= near_one((11, 1)) {
        // ln(1 + d) ~= d.
        let d = x - &Numeric::one();
        return match &d {
            Numeric::Finite { digits, scale } if !digits.is_zero() => {
                let (weight, groups) = base_10000(digits, *scale);
                weight * 4 + f64::from(groups[0]).log10() as i64
            }
            _ => 0,
        };
    }
    let (weight, groups) = base_10000(digits, *scale);
    let mut leading = f64::from(groups[0]);
    let mut dweight = weight * 4;
    if let Some(next) = groups.get(1) {
        leading = leading * 10000.0 + f64::from(*next);
        dweight -= 4;
    }
    let ln = leading.ln() + dweight as f64 * std::f64::consts::LN_10;
    ln.abs().log10() as i64
}

/// `ln(y)` for `y >= 1`, both fixed-point with `one` as 1: square roots
/// bring `y` near 1, where `ln(z) = 2 atanh((z - 1) / (z + 1))` converges
/// fast.
fn ln_reduced(y: &BigInt, one: &BigInt) -> BigInt {
    let limit = one + one / 100u32;
    let mut z = y.clone();
    let mut roots = 0u32;
    while z > limit {
        z = (&z * one).sqrt();
        roots += 1;
    }
    let t = div_round(&((&z - one) * one), &(&z + one));
    let t2 = div_round(&(&t * &t), one);
    let mut sum = t.clone();
    let mut power = t;
    let mut k = 1u32;
    loop {
        power = div_round(&(&power * &t2), one);
        if power.is_zero() {
            break;
        }
        sum += div_round(&power, &BigInt::from(2 * k + 1));
        k += 1;
    }
    sum << (roots + 1)
}

/// Removes the underscores grouping digits (`1_000`), which may only stand
/// between two digits, or after a `0x` prefix (`0x_1F`); `None` if the text
/// is not such digits.
pub(crate) fn ungrouped(text: &str, radix: u32) -> Option<String> {
    let bytes = text.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    let mut out = String::with_capacity(bytes.len());
    for (i, &b) in bytes.iter().enumerate() {
        if b == b'_' {
            let digit = |c: Option<&u8>| c.is_some_and(|c| (*c as char).is_digit(radix));
            let after_prefix = i == 0 && radix != 10;
            if !(after_prefix || (i > 0 && digit(bytes.get(i - 1)))) || !digit(bytes.get(i + 1)) {
                return None;
            }
        } else if (b as char).is_digit(radix) {
            out.push(b as char);
        } else {
            return None;
        }
    }
    Some(out)
}

impl Default for Numeric {
    fn default() -> Numeric {
        Numeric::zero()
    }
}

impl fmt::Display for Numeric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Numeric::Finite { digits, scale } => {
                if digits.is_negative() {
                    f.write_str("-")?;
                }
                let text = digits.magnitude().to_string();
                let scale = *scale as usize;
                if scale == 0 {
                    f.write_str(&text)
                } else if text.len() > scale {
                    let (whole, fraction) = text.split_at(text.len() - scale);
                    write!(f, "{whole}.{fraction}")
                } else {
                    write!(f, "0.{}{text}", "0".repeat(scale - text.len()))
                }
            }
            Numeric::NaN => f.write_str("NaN"),
            Numeric::Infinity => f.write_str("Infinity"),
            Numeric::NegInfinity => f.write_str("-Infinity"),
        }
    }
}

impl std::str::FromStr for Numeric {
    type Err = String;

    fn from_str(text: &str) -> Result<Numeric, String> {
        Numeric::parse(text)
    }
}

macro_rules! from_integer {
    ($($t:ty),*) => {$(
        impl From<$t> for Numeric {
            fn from(value: $t) -> Numeric {
                Numeric::new(value, 0)
            }
        }
    )*};
}
from_integer!(i32, i64, i128, u32, u64, usize);

impl PartialEq for Numeric {
    fn eq(&self, other: &Numeric) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Numeric {}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Numeric) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Numeric order: NaN above everything (and equal to itself), then
/// Infinity, the finite values, and -Infinity.
impl Ord for Numeric {
    fn cmp(&self, other: &Numeric) -> Ordering {
        let rank = |n: &Numeric| match n {
            Numeric::NegInfinity => 0,
            Numeric::Finite { .. } => 1,
            Numeric::Infinity => 2,
            Numeric::NaN => 3,
        };
        match (self, other) {
            (
                Numeric::Finite {
                    digits: a,
                    scale: sa,
                },
                Numeric::Finite {
                    digits: b,
                    scale: sb,
                },
            ) => {
                let (a, b, _) = Numeric::aligned((a, *sa), (b, *sb));
                a.cmp(&b)
            }
            _ => rank(self).cmp(&rank(other)),
        }
    }
}

impl std::ops::Neg for &Numeric {
    type Output = Numeric;

    fn neg(self) -> Numeric {
        match self {
            Numeric::Finite { digits, scale } => Numeric::new(-digits, *scale),
            Numeric::Infinity => Numeric::NegInfinity,
            Numeric::NegInfinity => Numeric::Infinity,
            Numeric::NaN => Numeric::NaN,
        }
    }
}

impl std::ops::Neg for Numeric {
    type Output = Numeric;

    fn neg(self) -> Numeric {
        -&self
    }
}

macro_rules! binary_operator {
    ($trait:ident, $method:ident, $body:expr) => {
        impl std::ops::$trait<&Numeric> for &Numeric {
            type Output = Numeric;

            fn $method(self, other: &Numeric) -> Numeric {
                $body(self, other)
            }
        }

        impl std::ops::$trait for Numeric {
            type Output = Numeric;

            fn $method(self, other: Numeric) -> Numeric {
                $body(&self, &other)
            }
        }
    };
}
binary_operator!(Add, add, |a: &Numeric, b| a.sum(b, false));
binary_operator!(Sub, sub, |a: &Numeric, b| a.sum(b, true));
binary_operator!(Mul, mul, |a: &Numeric, b| a.product(b));

impl std::ops::AddAssign<&Numeric> for Numeric {
    fn add_assign(&mut self, other: &Numeric) {
        *self = self.sum(other, false);
    }
}

impl serde::Serialize for Numeric {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for Numeric {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Numeric, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = Numeric;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a numeric")
            }

            fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Numeric, E> {
                Numeric::parse(text).map_err(E::custom)
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Numeric, E> {
                Ok(Numeric::from(value))
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Numeric, E> {
                Ok(Numeric::from(value))
            }

            fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Numeric, E> {
                Ok(Numeric::from_f64(value))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(text: &str) -> Numeric {
        Numeric::parse(text).unwrap()
    }

    fn show(result: Result<Numeric, String>) -> String {
        result.map_or_else(|e| format!("ERROR: {e}"), |v| v.to_string())
    }

    #[test]
    fn parses_and_shows_postgresql_numeric_text() {
        for (input, shown) in [
            ("1_000", "1000"),
            ("0x1F", "31"),
            (" 1.5e3 ", "1500"),
            (".5", "0.5"),
            ("5.", "5"),
            ("-0", "0"),
            ("+.5e-3", "0.0005"),
            ("1.50e1", "15.0"),
            ("1e-3", "0.001"),
            ("-0.0", "0.0"),
            ("nan", "NaN"),
            ("+inf", "Infinity"),
            ("-Infinity", "-Infinity"),
            (
                "12345678901234567890123456789012345678901234567890",
                "12345678901234567890123456789012345678901234567890",
            ),
        ] {
            assert_eq!(n(input).to_string(), shown, "{input}");
        }
        for bad in [
            "", "abc", "1_", "_1", "1__0", "1e", "0x", ".", "1.2.3", "0x1.5",
        ] {
            assert!(Numeric::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(
            Numeric::parse("1e-20000").unwrap_err(),
            "value overflows numeric format"
        );
    }

    #[test]
    fn arithmetic_keeps_postgresql_scales() {
        assert_eq!((&n("1.23") * &n("4.5600")).to_string(), "5.608800");
        assert_eq!((&n("0.00") * &n("1.5")).to_string(), "0.000");
        assert_eq!((&n("0.1") + &n("0.25")).to_string(), "0.35");
        assert_eq!(n("7.5").checked_rem(&n("2")).unwrap().to_string(), "1.5");
        assert_eq!(n("-7.5").checked_rem(&n("2")).unwrap().to_string(), "-1.5");
        assert_eq!(n("7").checked_rem(&n("-2.5")).unwrap().to_string(), "2.0");
        assert_eq!(show(n("1").checked_div(&n("3"))), "0.33333333333333333333");
        assert_eq!(show(n("10").checked_div(&n("4"))), "2.5000000000000000");
        assert_eq!(
            show(n("1").checked_div(&n("3e30"))),
            "0.000000000000000000000000000000333333333333333333"
        );
        assert_eq!(show(n("1").checked_div(&n("0"))), "ERROR: division by zero");
        assert!(n("1.0") == n("1.00"));
        assert!(n("NaN") > n("Infinity") && n("Infinity") > n("1e100"));
        assert!(n("-Infinity") < n("-1e100"));
    }

    #[test]
    fn special_values_follow_postgresql() {
        assert_eq!((&n("NaN") + &n("1")).to_string(), "NaN");
        assert_eq!((&n("Infinity") + &n("1")).to_string(), "Infinity");
        assert_eq!((&n("Infinity") - &n("Infinity")).to_string(), "NaN");
        assert_eq!((&n("Infinity") * &n("0")).to_string(), "NaN");
        assert_eq!(show(n("1").checked_div(&n("Infinity"))), "0");
        assert_eq!(
            show(n("Infinity").checked_div(&n("0"))),
            "ERROR: division by zero"
        );
        assert_eq!(show(n("Infinity").checked_rem(&n("2"))), "NaN");
        assert_eq!(show(n("5").checked_rem(&n("Infinity"))), "5");
    }

    #[test]
    fn rounds_half_away_from_zero() {
        assert_eq!(n("1234.5678").round(-2).to_string(), "1200");
        assert_eq!(n("1234.5678").round(2).to_string(), "1234.57");
        assert_eq!(n("1234.5678").trunc(-2).to_string(), "1200");
        assert_eq!(n("-0.5").round(0).to_string(), "-1");
        assert_eq!(n("-0.5").trunc(0).to_string(), "0");
        assert_eq!(n("1.5").round(3).to_string(), "1.500");
        assert_eq!(n("-1.5").floor().to_string(), "-2");
        assert_eq!(n("1.000").ceil().to_string(), "1");
        assert_eq!(n("2.5").to_i64(), Some(3));
        assert_eq!(n("-2.5").to_i64(), Some(-3));
    }

    #[test]
    fn converts_floats_as_postgresql() {
        assert_eq!(Numeric::from_f64(0.1).to_string(), "0.1");
        assert_eq!(
            Numeric::from_f64(1.0 / 3.0).to_string(),
            "0.333333333333333"
        );
        assert_eq!(Numeric::from_f64(1e20).to_string(), "100000000000000000000");
        assert_eq!(
            Numeric::from_f64(123456789.123456789).to_string(),
            "123456789.123457"
        );
        assert_eq!(
            Numeric::from_f64(1e-20).to_string(),
            "0.00000000000000000001"
        );
        assert_eq!(n("0.1").to_f64(), 0.1);
        assert_eq!(n("2.7182818284590452").to_f64(), std::f64::consts::E);
    }

    #[test]
    fn applies_type_modifiers() {
        assert_eq!(n("1.005").apply_typmod(10, 2).unwrap().to_string(), "1.01");
        assert_eq!(n("123").apply_typmod(2, -1).unwrap().to_string(), "120");
        assert_eq!(
            n("0.000123").apply_typmod(3, 5).unwrap().to_string(),
            "0.00012"
        );
        assert_eq!(n("1e-3").apply_typmod(3, 5).unwrap().to_string(), "0.00100");
        assert!(n("1234567").apply_typmod(5, 2).is_err());
        assert!(n("0.01").apply_typmod(3, 5).is_err());
        assert_eq!(n("NaN").apply_typmod(5, 2).unwrap().to_string(), "NaN");
    }

    #[test]
    fn binary_format_round_trips() {
        for text in [
            "0",
            "0.00",
            "1.5",
            "-12345678.0001",
            "1e-10",
            "NaN",
            "-Infinity",
            "10000",
        ] {
            let value = n(text);
            let back = Numeric::from_binary(&value.to_binary()).unwrap();
            assert_eq!(back.to_string(), value.to_string(), "{text}");
        }
        // 1.5: one digit group of 1 at weight 0 and 5000 at weight -1.
        assert_eq!(
            n("1.5").to_binary(),
            vec![0, 2, 0, 0, 0, 0, 0, 1, 0, 1, 0x13, 0x88]
        );
    }

    #[test]
    fn functions_give_postgresql_digits() {
        let cases = [
            (n("2").sqrt(), "1.414213562373095"),
            (n("16").sqrt(), "4.000000000000000"),
            (n("1e-10").sqrt(), "0.000010000000000000000"),
            (n("12345678901234567890").sqrt(), "3513641828.8201443"),
            (n("1").exp(), "2.7182818284590452"),
            (n("0.5").exp(), "1.6487212707001281"),
            (n("10").exp(), "22026.465794806717"),
            (n("-1").exp(), "0.3678794411714423"),
            (n("2.5").exp(), "12.182493960703473"),
            (n("10").ln(), "2.3025850929940457"),
            (n("2").ln(), "0.6931471805599453"),
            (n("0.5").ln(), "-0.6931471805599453"),
            (n("1e10").ln(), "23.025850929940457"),
            (n("10").log(&n("1000")), "3.0000000000000000"),
            (n("10").log(&n("0.001")), "-3.0000000000000000"),
            (n("2").log(&n("1024")), "10.0000000000000000"),
            (n("2.0").log(&n("64.0")), "6.0000000000000000"),
            (n("2.5").power(&n("2")), "6.2500000000000000"),
            (n("2").power(&n("0.5")), "1.4142135623730950"),
            (n("2").power(&n("10")), "1024.0000000000000"),
            (n("1.1").power(&n("100")), "13780.612339822270"),
            (n("10").power(&n("-3")), "0.0010000000000000000"),
            (n("2").power(&n("-0.5")), "0.7071067811865475"),
            (n("1.5").power(&n("3")), "3.3750000000000000"),
            (n("0.0").power(&n("0.0")), "1.0000000000000000"),
            (n("4").power(&n("0.5")), "2.0000000000000000"),
            (
                n("27").power(&n("0.33333333333333333333")),
                "2.99999999999999999997",
            ),
            (n("10").power(&n("30")), "1000000000000000000000000000000"),
            (
                n("2").power(&n("200")),
                "1606938044258990275541962092341162602522202993782792835301376",
            ),
            (
                n("-8").power(&n("0.333")),
                "ERROR: a negative number raised to a non-integer power yields a complex result",
            ),
            (
                n("0").power(&n("-1")),
                "ERROR: zero raised to a negative power is undefined",
            ),
            (n("0").ln(), "ERROR: cannot take logarithm of zero"),
            (n("Infinity").sqrt(), "Infinity"),
            (n("-Infinity").exp(), "0"),
            (n("1").ln(), "0.0000000000000000"),
        ];
        for (result, expected) in cases {
            assert_eq!(show(result), expected);
        }
    }
}
