//! Dates, times, timestamps, and intervals, which NodusDB keeps as their
//! PostgreSQL text: reading that text, arithmetic between them (`timestamp -
//! timestamp`, `date + interval`, `interval * 2`), their fields (`EXTRACT`),
//! truncation (`date_trunc`), and `age`.

use crate::Value;
use chrono::{Datelike, Duration, Months, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;
pub(crate) const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
pub(crate) const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
pub(crate) const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const DAYS_PER_MONTH: i64 = 30;

/// A PostgreSQL interval: months, days, and microseconds, each with its own
/// sign (`-1 days +02:00:00`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Interval {
    pub(crate) months: i64,
    pub(crate) days: i64,
    pub(crate) micros: i64,
}

impl Interval {
    pub(crate) fn new(months: i64, days: i64, micros: i64) -> Interval {
        Interval {
            months,
            days,
            micros,
        }
    }

    /// Reads interval input: `1 day 02:00:00`, `-1 days`, `1.5 hours`,
    /// `@ 3 days ago`, `2 weeks`, ISO 8601 `P1Y2M3DT4H5M6S`, or a bare
    /// number of seconds.
    pub(crate) fn parse(text: &str) -> Option<Interval> {
        let trimmed = text.trim();
        if let Some(iso) = trimmed
            .strip_prefix('P')
            .or_else(|| trimmed.strip_prefix('p'))
        {
            return parse_iso_interval(iso);
        }
        let lower = trimmed.to_ascii_lowercase();
        let (body, ago) = match lower.strip_suffix("ago") {
            Some(body) => (body.trim().to_string(), true),
            None => (lower.clone(), false),
        };
        let body = body.strip_prefix('@').unwrap_or(&body).trim();
        // Split `1day` / `10h` into number and unit.
        let mut tokens: Vec<String> = Vec::new();
        for word in body.split_whitespace() {
            let split = word
                .char_indices()
                .find(|(i, c)| c.is_ascii_alphabetic() && *i > 0)
                .map(|(i, _)| i);
            match split {
                Some(at) if word[..at].parse::<f64>().is_ok() => {
                    tokens.push(word[..at].to_string());
                    tokens.push(word[at..].to_string());
                }
                _ => tokens.push(word.to_string()),
            }
        }
        if tokens.is_empty() {
            return None;
        }
        let mut interval = Interval::default();
        let mut i = 0;
        while i < tokens.len() {
            let token = tokens[i].as_str();
            if token.contains(':') {
                interval.micros = interval.micros.checked_add(parse_clock(token)?)?;
                i += 1;
                continue;
            }
            // SQL's year-month form, `1-2`.
            if let Some((years, months)) = year_month(token) {
                interval.months = interval.months.checked_add(years * 12 + months)?;
                i += 1;
                continue;
            }
            let amount: f64 = token.parse().ok()?;
            match tokens.get(i + 1) {
                Some(unit) if unit.chars().all(|c| c.is_ascii_alphabetic()) => {
                    interval.add_unit(amount, unit)?;
                    i += 2;
                }
                // A number before a time of day is days (`3 04:05:06`).
                Some(clock) if clock.contains(':') => {
                    interval.add_unit(amount, "day")?;
                    i += 1;
                }
                // A number without a unit is seconds.
                _ => {
                    interval.add_unit(amount, "second")?;
                    i += 1;
                }
            }
        }
        Some(if ago { interval.negate() } else { interval })
    }

    /// Adds `amount` of `unit`, a fraction of a larger unit carrying into
    /// the smaller ones (a month as 30 days, a day as 24 hours).
    fn add_unit(&mut self, amount: f64, unit: &str) -> Option<()> {
        let unit = unit.to_ascii_lowercase();
        let unit = unit.as_str();
        let months_per = match unit {
            "millennium" | "millennia" | "millenniums" | "mil" | "mils" => Some(12_000.0),
            "century" | "centuries" | "c" | "cent" => Some(1_200.0),
            "decade" | "decades" | "dec" | "decs" => Some(120.0),
            "year" | "years" | "y" | "yr" | "yrs" => Some(12.0),
            "month" | "months" | "mon" | "mons" => Some(1.0),
            _ => None,
        };
        if let Some(per) = months_per {
            let months = amount * per;
            let whole = months.trunc();
            self.months = self.months.checked_add(whole as i64)?;
            return self.add_days((months - whole) * DAYS_PER_MONTH as f64);
        }
        let days_per = match unit {
            "week" | "weeks" | "w" => Some(7.0),
            "day" | "days" | "d" => Some(1.0),
            _ => None,
        };
        if let Some(per) = days_per {
            return self.add_days(amount * per);
        }
        let micros_per = match unit {
            "hour" | "hours" | "h" | "hr" | "hrs" => MICROS_PER_HOUR,
            "minute" | "minutes" | "m" | "min" | "mins" => MICROS_PER_MINUTE,
            "second" | "seconds" | "s" | "sec" | "secs" => MICROS_PER_SECOND,
            "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => 1_000,
            "microsecond" | "microseconds" | "us" | "usec" | "usecs" => 1,
            _ => return None,
        };
        self.micros = self
            .micros
            .checked_add((amount * micros_per as f64).round() as i64)?;
        Some(())
    }

    fn add_days(&mut self, days: f64) -> Option<()> {
        let whole = days.trunc();
        self.days = self.days.checked_add(whole as i64)?;
        self.micros = self
            .micros
            .checked_add(((days - whole) * MICROS_PER_DAY as f64).round() as i64)?;
        Some(())
    }

    pub(crate) fn negate(self) -> Interval {
        Interval::new(-self.months, -self.days, -self.micros)
    }

    pub(crate) fn add(self, other: Interval) -> Interval {
        Interval::new(
            self.months + other.months,
            self.days + other.days,
            self.micros + other.micros,
        )
    }

    /// `interval * factor`: a fraction of a month carries into days, and of
    /// a day into time, as PostgreSQL computes it.
    pub(crate) fn mul(self, factor: f64) -> Result<Interval, String> {
        let months = self.months as f64 * factor;
        let days = self.days as f64 * factor;
        if !months.is_finite() || !days.is_finite() || months.abs() > i32::MAX as f64 {
            return Err("interval out of range".to_string());
        }
        let whole_months = months.trunc();
        let whole_days = days.trunc();
        let month_remainder_days = round_micro((months - whole_months) * DAYS_PER_MONTH as f64);
        let mut second_remainder = round_micro(
            (days - whole_days + month_remainder_days - month_remainder_days.trunc()) * 86_400.0,
        );
        let mut day_count = whole_days as i64;
        if second_remainder.abs() >= 86_400.0 {
            let carried = (second_remainder / 86_400.0).trunc();
            day_count += carried as i64;
            second_remainder -= carried * 86_400.0;
        }
        day_count += month_remainder_days.trunc() as i64;
        let micros =
            (self.micros as f64 * factor + second_remainder * MICROS_PER_SECOND as f64).round();
        if !micros.is_finite() || micros.abs() > i64::MAX as f64 {
            return Err("interval out of range".to_string());
        }
        Ok(Interval::new(whole_months as i64, day_count, micros as i64))
    }

    pub(crate) fn div(self, divisor: f64) -> Result<Interval, String> {
        if divisor == 0.0 {
            return Err("division by zero".to_string());
        }
        self.mul(1.0 / divisor)
    }

    /// The interval's length, with a month as 30 days, for ordering.
    pub(crate) fn span(&self) -> i128 {
        (self.months as i128 * DAYS_PER_MONTH as i128 + self.days as i128) * MICROS_PER_DAY as i128
            + self.micros as i128
    }

    /// `justify_hours`: whole days of time as days.
    pub(crate) fn justify_hours(self) -> Interval {
        let mut days = self.days + self.micros / MICROS_PER_DAY;
        let mut micros = self.micros % MICROS_PER_DAY;
        if days > 0 && micros < 0 {
            micros += MICROS_PER_DAY;
            days -= 1;
        } else if days < 0 && micros > 0 {
            micros -= MICROS_PER_DAY;
            days += 1;
        }
        Interval::new(self.months, days, micros)
    }

    /// `justify_days`: whole 30-day months of days as months.
    pub(crate) fn justify_days(self) -> Interval {
        let mut months = self.months + self.days / DAYS_PER_MONTH;
        let mut days = self.days % DAYS_PER_MONTH;
        if months > 0 && days < 0 {
            days += DAYS_PER_MONTH;
            months -= 1;
        } else if months < 0 && days > 0 {
            days -= DAYS_PER_MONTH;
            months += 1;
        }
        Interval::new(months, days, self.micros)
    }

    /// `justify_interval`: both, with every part given one sign.
    pub(crate) fn justify(self) -> Interval {
        let (mut months, mut days, mut micros) = (self.months, self.days, self.micros);
        months += days / DAYS_PER_MONTH;
        days %= DAYS_PER_MONTH;
        days += micros / MICROS_PER_DAY;
        micros %= MICROS_PER_DAY;
        months += days / DAYS_PER_MONTH;
        days %= DAYS_PER_MONTH;
        if months > 0 && (days < 0 || (days == 0 && micros < 0)) {
            days += DAYS_PER_MONTH;
            months -= 1;
        } else if months < 0 && (days > 0 || (days == 0 && micros > 0)) {
            days -= DAYS_PER_MONTH;
            months += 1;
        }
        if days > 0 && micros < 0 {
            micros += MICROS_PER_DAY;
            days -= 1;
        } else if days < 0 && micros > 0 {
            micros -= MICROS_PER_DAY;
            days += 1;
        }
        Interval::new(months, days, micros)
    }

    /// PostgreSQL's interval text: `1 year 2 mons -3 days +04:05:06.5`.
    pub(crate) fn format(&self) -> String {
        let mut out = String::new();
        let mut is_zero = true;
        let mut is_before = false;
        let mut part = |value: i64, unit: &str, out: &mut String| {
            if value == 0 {
                return;
            }
            if !is_zero {
                out.push(' ');
            }
            if is_before && value > 0 {
                out.push('+');
            }
            out.push_str(&format!(
                "{value} {unit}{}",
                if value != 1 { "s" } else { "" }
            ));
            is_before = value < 0;
            is_zero = false;
        };
        part(self.months / 12, "year", &mut out);
        part(self.months % 12, "mon", &mut out);
        part(self.days, "day", &mut out);
        if is_zero || self.micros != 0 {
            if !is_zero {
                out.push(' ');
            }
            if self.micros < 0 {
                out.push('-');
            } else if is_before {
                out.push('+');
            }
            let micros = self.micros.unsigned_abs() as i64;
            out.push_str(&format!(
                "{:02}:{:02}:",
                micros / MICROS_PER_HOUR,
                micros % MICROS_PER_HOUR / MICROS_PER_MINUTE
            ));
            out.push_str(&seconds_text(micros % MICROS_PER_MINUTE));
        }
        out
    }
}

/// Rounds to whole microseconds of the unit.
fn round_micro(x: f64) -> f64 {
    (x * 1e6).round() / 1e6
}

/// `SS[.ffffff]`, trailing zeros of the fraction dropped.
fn seconds_text(micros: i64) -> String {
    let mut out = format!("{:02}", micros / MICROS_PER_SECOND);
    let fraction = micros % MICROS_PER_SECOND;
    if fraction != 0 {
        out.push('.');
        out.push_str(format!("{fraction:06}").trim_end_matches('0'));
    }
    out
}

/// `[-]HH:MM[:SS[.fraction]]` as microseconds.
fn parse_clock(token: &str) -> Option<i64> {
    let (negative, body) = match token.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, token.strip_prefix('+').unwrap_or(token)),
    };
    let parts: Vec<&str> = body.split(':').collect();
    if !(2..=3).contains(&parts.len()) {
        return None;
    }
    let hours: i64 = parts[0].parse().ok()?;
    let minutes: i64 = parts[1].parse().ok()?;
    let seconds: f64 = match parts.get(2) {
        Some(s) => s.parse().ok()?,
        None => 0.0,
    };
    if !(0..60).contains(&minutes) || !(0.0..60.0).contains(&seconds) {
        return None;
    }
    let micros = hours * MICROS_PER_HOUR
        + minutes * MICROS_PER_MINUTE
        + (seconds * MICROS_PER_SECOND as f64).round() as i64;
    Some(if negative { -micros } else { micros })
}

/// The years and months of SQL's `Y-M` interval form, signed as a whole.
fn year_month(token: &str) -> Option<(i64, i64)> {
    let (negative, body) = match token.strip_prefix('-') {
        Some(body) => (true, body),
        None => (false, token.strip_prefix('+').unwrap_or(token)),
    };
    let (years, months) = body.split_once('-')?;
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    if !digits(years) || !digits(months) {
        return None;
    }
    let (years, months): (i64, i64) = (years.parse().ok()?, months.parse().ok()?);
    Some(if negative {
        (-years, -months)
    } else {
        (years, months)
    })
}

/// ISO 8601 `P[nY][nM][nW][nD][T[nH][nM][nS]]`, after the `P`.
fn parse_iso_interval(text: &str) -> Option<Interval> {
    let mut interval = Interval::default();
    let mut in_time = false;
    let mut number = String::new();
    for c in text.chars() {
        match c {
            'T' | 't' => in_time = true,
            '0'..='9' | '.' | '-' | '+' => number.push(c),
            unit => {
                let amount: f64 = number.parse().ok()?;
                number.clear();
                let unit = match (unit.to_ascii_uppercase(), in_time) {
                    ('Y', false) => "year",
                    ('M', false) => "month",
                    ('W', false) => "week",
                    ('D', false) => "day",
                    ('H', true) => "hour",
                    ('M', true) => "minute",
                    ('S', true) => "second",
                    _ => return None,
                };
                interval.add_unit(amount, unit)?;
            }
        }
    }
    number.is_empty().then_some(interval)
}

/// The kinds of date/time value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
}

impl Kind {
    /// The kind a declared type names, if it names one.
    pub(crate) fn of_type(data_type: &str) -> Option<Kind> {
        if let Some(t) = crate::value::temporal_type(data_type) {
            return Some(match t {
                crate::value::Temporal::Date => Kind::Date,
                crate::value::Temporal::Time => Kind::Time,
                crate::value::Temporal::Timestamp => Kind::Timestamp,
                crate::value::Temporal::TimestampTz => Kind::TimestampTz,
            });
        }
        let upper = data_type.trim().to_ascii_uppercase();
        let base = upper.split('(').next().unwrap_or_default().trim();
        (base == "INTERVAL" || base.starts_with("INTERVAL ")).then_some(Kind::Interval)
    }

    /// The SQL name of the kind, as a declared type.
    pub(crate) fn type_name(self) -> &'static str {
        match self {
            Kind::Date => "DATE",
            Kind::Time => "TIME",
            Kind::Timestamp => "TIMESTAMP",
            Kind::TimestampTz => "TIMESTAMPTZ",
            Kind::Interval => "INTERVAL",
        }
    }

    /// The name PostgreSQL's messages give the kind.
    fn sql_name(self) -> &'static str {
        match self {
            Kind::Date => "date",
            Kind::Time => "time without time zone",
            Kind::Timestamp => "timestamp without time zone",
            Kind::TimestampTz => "timestamp with time zone",
            Kind::Interval => "interval",
        }
    }
}

/// A date/time value, read from its text.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Temporal {
    Date(NaiveDate),
    /// Microseconds since midnight.
    Time(i64),
    Timestamp(NaiveDateTime),
    /// The instant, in UTC.
    TimestampTz(NaiveDateTime),
    Interval(Interval),
    /// `infinity` / `-infinity` of a date or timestamp kind.
    Infinite(Kind, bool),
}

impl Temporal {
    pub(crate) fn kind(&self) -> Kind {
        match self {
            Temporal::Date(_) => Kind::Date,
            Temporal::Time(_) => Kind::Time,
            Temporal::Timestamp(_) => Kind::Timestamp,
            Temporal::TimestampTz(_) => Kind::TimestampTz,
            Temporal::Interval(_) => Kind::Interval,
            Temporal::Infinite(kind, _) => *kind,
        }
    }

    /// Reads `text` as a value of `kind`.
    pub(crate) fn parse_as(text: &str, kind: Kind) -> Option<Temporal> {
        let lower = text.trim().to_ascii_lowercase();
        if matches!(kind, Kind::Date | Kind::Timestamp | Kind::TimestampTz) {
            match lower.as_str() {
                "infinity" | "+infinity" => return Some(Temporal::Infinite(kind, false)),
                "-infinity" => return Some(Temporal::Infinite(kind, true)),
                _ => {}
            }
        }
        match kind {
            Kind::Interval => Interval::parse(text).map(Temporal::Interval),
            Kind::Time => time_micros(text).map(Temporal::Time),
            _ => {
                let parsed = crate::value::parse_temporal(text)?;
                Some(match kind {
                    Kind::Date => Temporal::Date(parsed.date),
                    Kind::Timestamp => {
                        Temporal::Timestamp(parsed.date.and_time(parsed.time.unwrap_or_default()))
                    }
                    _ => Temporal::TimestampTz(parsed.utc()),
                })
            }
        }
    }

    /// Reads `text` as the kind its shape shows: a date, a timestamp (with
    /// a zone, a zoned one), or an interval (a bare `HH:MM:SS` too).
    pub(crate) fn classify(text: &str) -> Option<Temporal> {
        let lower = text.trim().to_ascii_lowercase();
        match lower.as_str() {
            "infinity" | "+infinity" => return Some(Temporal::Infinite(Kind::Timestamp, false)),
            "-infinity" => return Some(Temporal::Infinite(Kind::Timestamp, true)),
            _ => {}
        }
        if let Some(parsed) = crate::value::parse_temporal(text) {
            return Some(match (parsed.time, parsed.offset) {
                (None, _) => Temporal::Date(parsed.date),
                (Some(time), None) => Temporal::Timestamp(parsed.date.and_time(time)),
                (Some(_), Some(_)) => Temporal::TimestampTz(parsed.utc()),
            });
        }
        // What is not a date may be an interval (`1 day`, `02:00:00`).
        if lower.chars().any(|c| c.is_ascii_digit()) {
            return Interval::parse(text).map(Temporal::Interval);
        }
        None
    }

    /// Reads a value of static kind `kind` (when known), else by its shape.
    pub(crate) fn read(value: &Value, kind: Option<Kind>) -> Option<Temporal> {
        let text = match value {
            Value::Text(s) => s.as_str(),
            _ => return None,
        };
        match kind {
            Some(kind) => Temporal::parse_as(text, kind),
            None => Temporal::classify(text),
        }
    }

    /// The value's PostgreSQL text.
    pub(crate) fn to_value(self) -> Value {
        Value::Text(match self {
            Temporal::Date(d) => format_date(d),
            Temporal::Time(micros) => format_time(micros),
            Temporal::Timestamp(ts) => crate::value::format_timestamp(ts, false),
            Temporal::TimestampTz(ts) => crate::value::format_timestamp(ts, true),
            Temporal::Interval(iv) => iv.format(),
            Temporal::Infinite(_, negative) => {
                if negative { "-infinity" } else { "infinity" }.to_string()
            }
        })
    }

    /// The value as a timestamp (a date at midnight), if it is one.
    fn as_timestamp(&self) -> Option<NaiveDateTime> {
        match self {
            Temporal::Date(d) => d.and_hms_opt(0, 0, 0),
            Temporal::Timestamp(ts) | Temporal::TimestampTz(ts) => Some(*ts),
            _ => None,
        }
    }
}

/// `YYYY-MM-DD`, with ` BC` for years before 1.
pub(crate) fn format_date(date: NaiveDate) -> String {
    if date.year() <= 0 {
        format!(
            "{:04}-{:02}-{:02} BC",
            1 - date.year(),
            date.month(),
            date.day()
        )
    } else {
        date.format("%Y-%m-%d").to_string()
    }
}

/// `HH:MM:SS[.fraction]` for microseconds since midnight.
pub(crate) fn format_time(micros: i64) -> String {
    format!(
        "{:02}:{:02}:{}",
        micros / MICROS_PER_HOUR,
        micros % MICROS_PER_HOUR / MICROS_PER_MINUTE,
        seconds_text(micros % MICROS_PER_MINUTE)
    )
}

/// A time of day (`HH:MM[:SS[.f]]`, or a timestamp's) as microseconds since
/// midnight; `24:00:00` is allowed, as in PostgreSQL.
pub(crate) fn time_micros(text: &str) -> Option<i64> {
    let trimmed = text.trim();
    let clock = trimmed
        .split_once([' ', 'T'])
        .filter(|(date, _)| date.contains('-'))
        .map_or(trimmed, |(_, time)| time.trim());
    // A zone after the time (`10:00+02`) is not part of it.
    let clock = clock
        .find(|c: char| c == '+' || c == '-' || c.is_ascii_alphabetic())
        .map_or(clock, |at| &clock[..at])
        .trim();
    let micros = parse_clock(clock)?;
    (0..=MICROS_PER_DAY).contains(&micros).then_some(micros)
}

fn time_of(ts: &NaiveDateTime) -> i64 {
    time_micros_of(ts.time())
}

/// A time of day as microseconds since midnight.
pub(crate) fn time_micros_of(time: chrono::NaiveTime) -> i64 {
    time.num_seconds_from_midnight() as i64 * MICROS_PER_SECOND
        + (time.nanosecond() / 1_000) as i64 % MICROS_PER_SECOND
}

/// A zoned timestamp (UTC) plus an interval: its months and days in the
/// session's local time, so a day across a daylight-saving change is 23 or
/// 25 hours, then its time.
pub(crate) fn add_interval_zoned(utc: NaiveDateTime, iv: Interval) -> Option<NaiveDateTime> {
    let dated = if iv.months == 0 && iv.days == 0 {
        utc
    } else {
        let (local, _) = crate::timezone::to_session_local(utc);
        let shifted = add_interval(local, Interval::new(iv.months, iv.days, 0))?;
        crate::timezone::from_session_local(shifted)
    };
    dated.checked_add_signed(Duration::microseconds(iv.micros))
}

/// `ts + interval`: months first (clamping the day to the month's end),
/// then days, then time.
pub(crate) fn add_interval(ts: NaiveDateTime, iv: Interval) -> Option<NaiveDateTime> {
    let shifted = if iv.months >= 0 {
        ts.checked_add_months(Months::new(u32::try_from(iv.months).ok()?))?
    } else {
        ts.checked_sub_months(Months::new(u32::try_from(-iv.months).ok()?))?
    };
    shifted
        .checked_add_signed(Duration::days(iv.days))?
        .checked_add_signed(Duration::microseconds(iv.micros))
}

/// `a - b` of timestamps: whole days as days, the rest as time.
fn timestamp_difference(a: NaiveDateTime, b: NaiveDateTime) -> Option<Interval> {
    let micros = (a - b).num_microseconds()?;
    Some(Interval::new(0, 0, micros).justify_hours())
}

/// A number operand of date/time arithmetic.
fn number(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::Float(f) => Some(*f),
        Value::Numeric(d) => Some(crate::value::decimal_to_f64(d)),
        _ => None,
    }
}

/// The type of a date/time operator's result, given its operands' kinds
/// (`None` for a number), or `None` when there is no such operator.
pub(crate) fn result_kind(
    op: &str,
    left: Option<Kind>,
    right: Option<Kind>,
) -> Option<&'static str> {
    use Kind::*;
    Some(match (op, left, right) {
        ("+", Some(Date), None) | ("+", None, Some(Date)) | ("-", Some(Date), None) => "DATE",
        ("-", Some(Date), Some(Date)) => "INTEGER",
        ("+", Some(Date), Some(Interval))
        | ("+", Some(Interval), Some(Date))
        | ("-", Some(Date), Some(Interval))
        | ("+", Some(Date), Some(Time))
        | ("+", Some(Time), Some(Date)) => "TIMESTAMP",
        ("+", Some(Timestamp), Some(Interval))
        | ("+", Some(Interval), Some(Timestamp))
        | ("-", Some(Timestamp), Some(Interval)) => "TIMESTAMP",
        ("+", Some(TimestampTz), Some(Interval))
        | ("+", Some(Interval), Some(TimestampTz))
        | ("-", Some(TimestampTz), Some(Interval)) => "TIMESTAMPTZ",
        ("-", Some(Date | Timestamp | TimestampTz), Some(Date | Timestamp | TimestampTz)) => {
            "INTERVAL"
        }
        ("+", Some(Time), Some(Interval))
        | ("+", Some(Interval), Some(Time))
        | ("-", Some(Time), Some(Interval)) => "TIME",
        ("-", Some(Time), Some(Time)) => "INTERVAL",
        ("+" | "-", Some(Interval), Some(Interval)) => "INTERVAL",
        ("*", Some(Interval), None) | ("*", None, Some(Interval)) | ("/", Some(Interval), None) => {
            "INTERVAL"
        }
        ("neg", Some(Interval), _) => "INTERVAL",
        _ => return None,
    })
}

/// A date/time operator (`+`, `-`, `*`, `/`, or `neg`) over `left` and
/// `right`, each of the kind its static type names (when known) or else of
/// the kind its text shows.
pub(crate) fn arith(
    op: &str,
    left: &Value,
    left_kind: Option<Kind>,
    right: &Value,
    right_kind: Option<Kind>,
) -> Result<Value, String> {
    if matches!(left, Value::Null) || (op != "neg" && matches!(right, Value::Null)) {
        return Ok(Value::Null);
    }
    let l = Temporal::read(left, left_kind);
    let r = if op == "neg" {
        None
    } else {
        Temporal::read(right, right_kind)
    };
    let out_of_range = || "timestamp out of range".to_string();
    use Temporal as T;
    let result = match (op, l, r) {
        ("neg", Some(T::Interval(iv)), _) => T::Interval(iv.negate()),
        // date ± integer, date - date.
        ("+" | "-", Some(T::Date(d)), None) if number(right).is_some() => {
            let days = number(right).unwrap_or(0.0) as i64;
            let days = if op == "-" { -days } else { days };
            T::Date(
                d.checked_add_signed(Duration::days(days))
                    .ok_or("date out of range")?,
            )
        }
        ("+", None, Some(T::Date(d))) if number(left).is_some() => {
            let days = number(left).unwrap_or(0.0) as i64;
            T::Date(
                d.checked_add_signed(Duration::days(days))
                    .ok_or("date out of range")?,
            )
        }
        ("-", Some(T::Date(a)), Some(T::Date(b))) => return Ok(Value::Int((a - b).num_days())),
        // interval ± interval, interval * number, interval / number.
        ("+", Some(T::Interval(a)), Some(T::Interval(b))) => T::Interval(a.add(b)),
        ("-", Some(T::Interval(a)), Some(T::Interval(b))) => T::Interval(a.add(b.negate())),
        ("*", Some(T::Interval(iv)), None) if number(right).is_some() => {
            T::Interval(iv.mul(number(right).unwrap_or(0.0))?)
        }
        ("*", None, Some(T::Interval(iv))) if number(left).is_some() => {
            T::Interval(iv.mul(number(left).unwrap_or(0.0))?)
        }
        ("/", Some(T::Interval(iv)), None) if number(right).is_some() => {
            T::Interval(iv.div(number(right).unwrap_or(0.0))?)
        }
        // time ± interval (within the day), time - time.
        ("+", Some(T::Time(t)), Some(T::Interval(iv)))
        | ("+", Some(T::Interval(iv)), Some(T::Time(t))) => {
            T::Time((t + iv.micros).rem_euclid(MICROS_PER_DAY))
        }
        ("-", Some(T::Time(t)), Some(T::Interval(iv))) => {
            T::Time((t - iv.micros).rem_euclid(MICROS_PER_DAY))
        }
        ("-", Some(T::Time(a)), Some(T::Time(b))) => T::Interval(Interval::new(0, 0, a - b)),
        // date + time.
        ("+", Some(T::Date(d)), Some(T::Time(t))) | ("+", Some(T::Time(t)), Some(T::Date(d))) => {
            T::Timestamp(
                d.and_hms_opt(0, 0, 0)
                    .and_then(|ts| ts.checked_add_signed(Duration::microseconds(t)))
                    .ok_or_else(out_of_range)?,
            )
        }
        // An infinite timestamp stays infinite.
        ("+" | "-", Some(T::Infinite(kind, negative)), Some(T::Interval(_)))
        | ("+", Some(T::Interval(_)), Some(T::Infinite(kind, negative))) => T::Infinite(
            if kind == Kind::Date {
                Kind::Timestamp
            } else {
                kind
            },
            negative,
        ),
        // timestamp ± interval (a date as its midnight).
        (
            "+" | "-",
            Some(ts @ (T::Date(_) | T::Timestamp(_) | T::TimestampTz(_))),
            Some(T::Interval(iv)),
        )
        | (
            "+",
            Some(T::Interval(iv)),
            Some(ts @ (T::Date(_) | T::Timestamp(_) | T::TimestampTz(_))),
        ) => {
            let base = ts.as_timestamp().ok_or_else(out_of_range)?;
            let iv = if op == "-" { iv.negate() } else { iv };
            match ts {
                T::TimestampTz(_) => {
                    T::TimestampTz(add_interval_zoned(base, iv).ok_or_else(out_of_range)?)
                }
                _ => T::Timestamp(add_interval(base, iv).ok_or_else(out_of_range)?),
            }
        }
        // timestamp - timestamp.
        ("-", Some(a), Some(b)) if a.as_timestamp().is_some() && b.as_timestamp().is_some() => {
            let (a, b) = (
                a.as_timestamp().unwrap_or_default(),
                b.as_timestamp().unwrap_or_default(),
            );
            T::Interval(timestamp_difference(a, b).ok_or("interval out of range")?)
        }
        _ => {
            let name = |t: Option<T>, v: &Value, k: Option<Kind>| match (t, k) {
                (Some(t), _) => t.kind().sql_name().to_string(),
                (None, Some(k)) => k.sql_name().to_string(),
                (None, None) => crate::value::value_type_name(v).to_string(),
            };
            let (left_name, right_name) = (name(l, left, left_kind), name(r, right, right_kind));
            let operator = if op == "neg" {
                format!("- {left_name}")
            } else {
                format!("{left_name} {op} {right_name}")
            };
            // `time + time` could be `time + interval` either way round.
            let error = if matches!((op, l, r), ("+", Some(T::Time(_)), Some(T::Time(_)))) {
                crate::error_fields::DbError::new(format!("operator is not unique: {operator}"))
                    .hint(
                        "Could not choose a best candidate operator. \
                     You might need to add explicit type casts.",
                    )
            } else {
                crate::error_fields::DbError::new(format!("operator does not exist: {operator}"))
                    .hint(
                        "No operator matches the given name and argument types. \
                     You might need to add explicit type casts.",
                    )
            };
            return Err(error.into_text());
        }
    };
    Ok(result.to_value())
}

/// The unit a field name (`years`, `mon`, `msec`) stands for, as
/// `EXTRACT` and `date_trunc` read it.
fn unit_name(field: &str) -> Option<&'static str> {
    Some(match field {
        "millennium" | "millennia" | "millenniums" | "mil" | "mils" => "millennium",
        "century" | "centuries" | "c" | "cent" => "century",
        "decade" | "decades" | "dec" | "decs" => "decade",
        "year" | "years" | "y" | "yr" | "yrs" => "year",
        "quarter" | "qtr" => "quarter",
        "month" | "months" | "mon" | "mons" => "month",
        "week" | "weeks" | "w" => "week",
        "day" | "days" | "d" => "day",
        "hour" | "hours" | "h" | "hr" | "hrs" => "hour",
        "minute" | "minutes" | "m" | "min" | "mins" => "minute",
        "second" | "seconds" | "s" | "sec" | "secs" => "second",
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" | "mseconds" | "millisecon" => {
            "milliseconds"
        }
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" | "useconds" | "microsecon" => {
            "microseconds"
        }
        "dow" => "dow",
        "doy" => "doy",
        "isodow" => "isodow",
        "isoyear" => "isoyear",
        "epoch" => "epoch",
        "julian" | "j" => "julian",
        "timezone" => "timezone",
        "timezone_h" | "timezone_hour" => "timezone_hour",
        "timezone_m" | "timezone_minute" => "timezone_minute",
        _ => return None,
    })
}

/// `EXTRACT(field FROM value)` (a numeric) or, with `float`, `date_part`
/// (a double precision), for a value of static kind `kind` when known.
pub(crate) fn extract(
    field: &str,
    value: &Value,
    kind: Option<Kind>,
    float: bool,
) -> Result<Value, String> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let Some(temporal) = Temporal::read(value, kind) else {
        return Err(format!(
            "invalid input syntax for type {}: \"{}\"",
            kind.map_or("timestamp", |k| k.sql_name()),
            crate::render(value)
        ));
    };
    let written = field.trim().trim_matches('\'').to_ascii_lowercase();
    let unsupported = |kind: Kind| {
        format!(
            "unit \"{written}\" not supported for type {}",
            kind.sql_name()
        )
    };
    let Some(field) = unit_name(&written) else {
        return Err(format!(
            "unit \"{written}\" not recognized for type {}",
            temporal.kind().sql_name()
        ));
    };
    // A value in microseconds, shown with `scale` decimals, or an integer.
    let micros = |v: i64, scale: u32| -> Value {
        if float {
            Value::Float(v as f64 / 10f64.powi(6))
        } else {
            let decimal = rust_decimal::Decimal::new(v, 6);
            let mut rescaled = decimal;
            rescaled.rescale(scale);
            Value::Numeric(rescaled)
        }
    };
    let whole = |v: i64| -> Value {
        if float {
            Value::Float(v as f64)
        } else {
            Value::Numeric(rust_decimal::Decimal::from(v))
        }
    };
    Ok(match temporal {
        Temporal::Infinite(kind, negative) => {
            if float {
                Value::Float(if negative {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                })
            } else if matches!(
                field,
                "epoch" | "year" | "decade" | "century" | "millennium" | "julian" | "isoyear"
            ) {
                // A numeric infinity, which shows as a float's.
                Value::Float(if negative {
                    f64::NEG_INFINITY
                } else {
                    f64::INFINITY
                })
            } else {
                Value::Null
            }
        }
        Temporal::Interval(iv) => {
            let seconds_micros = iv.micros % MICROS_PER_MINUTE;
            match field {
                "millennium" | "millennia" => whole(iv.months / 12 / 1000),
                "century" | "centuries" => whole(iv.months / 12 / 100),
                "decade" | "decades" => whole(iv.months / 12 / 10),
                "year" | "years" => whole(iv.months / 12),
                "quarter" => whole(iv.months % 12 / 3 + 1),
                "month" | "months" => whole(iv.months % 12),
                "day" | "days" => whole(iv.days),
                "hour" | "hours" => whole(iv.micros / MICROS_PER_HOUR),
                "minute" | "minutes" => whole(iv.micros % MICROS_PER_HOUR / MICROS_PER_MINUTE),
                "second" | "seconds" => micros(seconds_micros, 6),
                "milliseconds" | "millisecond" => micros(seconds_micros * 1000, 3),
                "microseconds" | "microsecond" => whole(seconds_micros),
                "epoch" => {
                    // A year as 365.25 days and a month as 30, as PostgreSQL has it.
                    let years = iv.months / 12;
                    let months = iv.months % 12;
                    let total = years as i128 * 36_525 * MICROS_PER_DAY as i128 / 100
                        + (months * DAYS_PER_MONTH + iv.days) as i128 * MICROS_PER_DAY as i128
                        + iv.micros as i128;
                    micros(total as i64, 6)
                }
                _ => return Err(unsupported(Kind::Interval)),
            }
        }
        Temporal::Time(t) => match field {
            "hour" | "hours" => whole(t / MICROS_PER_HOUR),
            "minute" | "minutes" => whole(t % MICROS_PER_HOUR / MICROS_PER_MINUTE),
            "second" | "seconds" => micros(t % MICROS_PER_MINUTE, 6),
            "milliseconds" | "millisecond" => micros(t % MICROS_PER_MINUTE * 1000, 3),
            "microseconds" | "microsecond" => whole(t % MICROS_PER_MINUTE),
            "epoch" => micros(t, 6),
            _ => return Err(unsupported(Kind::Time)),
        },
        date_or_timestamp => {
            let is_date = matches!(date_or_timestamp, Temporal::Date(_));
            // A zoned timestamp's fields are its local time in the session's zone.
            let (ts, offset) = match date_or_timestamp {
                Temporal::TimestampTz(utc) => crate::timezone::to_session_local(utc),
                other => (other.as_timestamp().unwrap_or_default(), 0),
            };
            let date = ts.date();
            let time = time_of(&ts);
            let year = date.year() as i64;
            let time_field = matches!(
                field,
                "hour"
                    | "hours"
                    | "minute"
                    | "minutes"
                    | "second"
                    | "seconds"
                    | "milliseconds"
                    | "millisecond"
                    | "microseconds"
                    | "microsecond"
            );
            if is_date && time_field {
                return Err(unsupported(Kind::Date));
            }
            match field {
                "millennium" | "millennia" => whole(if year > 0 {
                    (year + 999) / 1000
                } else {
                    -((999 - year) / 1000)
                }),
                "century" | "centuries" => whole(if year > 0 {
                    (year + 99) / 100
                } else {
                    -((99 - year) / 100)
                }),
                "decade" | "decades" => whole(year.div_euclid(10)),
                "year" | "years" => whole(if year > 0 { year } else { year - 1 }),
                "isoyear" => whole(date.iso_week().year() as i64),
                "quarter" => whole((date.month() as i64 - 1) / 3 + 1),
                "month" | "months" => whole(date.month() as i64),
                "week" | "weeks" => whole(date.iso_week().week() as i64),
                "day" | "days" => whole(date.day() as i64),
                "dow" => whole(date.weekday().num_days_from_sunday() as i64),
                "isodow" => whole(date.weekday().number_from_monday() as i64),
                "doy" => whole(date.ordinal() as i64),
                "julian" => {
                    let jd = date.num_days_from_ce() as i64 + 1_721_425;
                    if is_date {
                        whole(jd)
                    } else {
                        let v = jd as f64 + time as f64 / MICROS_PER_DAY as f64;
                        if float {
                            Value::Float(v)
                        } else {
                            Value::Numeric(
                                rust_decimal::Decimal::from_f64_retain(v).unwrap_or_default(),
                            )
                        }
                    }
                }
                "hour" | "hours" => whole(time / MICROS_PER_HOUR),
                "minute" | "minutes" => whole(time % MICROS_PER_HOUR / MICROS_PER_MINUTE),
                "second" | "seconds" => micros(time % MICROS_PER_MINUTE, 6),
                "milliseconds" | "millisecond" => micros(time % MICROS_PER_MINUTE * 1000, 3),
                "microseconds" | "microsecond" => whole(time % MICROS_PER_MINUTE),
                "epoch" => {
                    let epoch = ts.and_utc().timestamp_micros() - offset * MICROS_PER_SECOND;
                    if is_date {
                        whole(epoch / MICROS_PER_SECOND)
                    } else {
                        micros(epoch, 6)
                    }
                }
                "timezone" if matches!(date_or_timestamp, Temporal::TimestampTz(_)) => {
                    whole(offset)
                }
                "timezone_hour" if matches!(date_or_timestamp, Temporal::TimestampTz(_)) => {
                    whole(offset / 3600)
                }
                "timezone_minute" if matches!(date_or_timestamp, Temporal::TimestampTz(_)) => {
                    whole(offset % 3600 / 60)
                }
                _ => return Err(unsupported(date_or_timestamp.kind())),
            }
        }
    })
}

/// `date_trunc(field, value)`: a timestamp (or interval) with the parts
/// finer than `field` zeroed.
pub(crate) fn trunc(field: &str, value: &Value, kind: Option<Kind>) -> Result<Value, String> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }
    let written = field.trim().to_ascii_lowercase();
    let temporal = Temporal::read(value, kind).ok_or_else(|| {
        format!(
            "invalid input syntax for type timestamp: \"{}\"",
            crate::render(value)
        )
    })?;
    let unit_error = |k: Kind| {
        if k == Kind::Interval && written == "week" {
            crate::error_fields::DbError::new(format!(
                "unit \"{written}\" not supported for type interval"
            ))
            .detail("Months usually have fractional weeks.")
            .into_text()
        } else {
            format!(
                "unit \"{written}\" not recognized for type {}",
                k.sql_name()
            )
        }
    };
    let field = unit_name(&written).unwrap_or_default().to_string();
    let result = match temporal {
        Temporal::Infinite(..) => temporal,
        Temporal::Interval(iv) => {
            let year_months = |per: i64| iv.months / (12 * per) * 12 * per;
            Temporal::Interval(match field.as_str() {
                "millennium" => Interval::new(year_months(1000), 0, 0),
                "century" => Interval::new(year_months(100), 0, 0),
                "decade" => Interval::new(year_months(10), 0, 0),
                "year" => Interval::new(year_months(1), 0, 0),
                "quarter" => Interval::new(iv.months / 3 * 3, 0, 0),
                "month" => Interval::new(iv.months, 0, 0),
                "day" => Interval::new(iv.months, iv.days, 0),
                "hour" => Interval::new(
                    iv.months,
                    iv.days,
                    iv.micros / MICROS_PER_HOUR * MICROS_PER_HOUR,
                ),
                "minute" => Interval::new(
                    iv.months,
                    iv.days,
                    iv.micros / MICROS_PER_MINUTE * MICROS_PER_MINUTE,
                ),
                "second" => Interval::new(
                    iv.months,
                    iv.days,
                    iv.micros / MICROS_PER_SECOND * MICROS_PER_SECOND,
                ),
                "milliseconds" => Interval::new(iv.months, iv.days, iv.micros / 1000 * 1000),
                "microseconds" => iv,
                _ => return Err(unit_error(Kind::Interval)),
            })
        }
        Temporal::Time(_) => return Err(unit_error(Kind::Time)),
        other => {
            // A zoned timestamp truncates in the session's local time, and a
            // date as its local midnight.
            let ts = match other {
                Temporal::TimestampTz(utc) => crate::timezone::to_session_local(utc).0,
                _ => other.as_timestamp().unwrap_or_default(),
            };
            let d = ts.date();
            let year = d.year();
            let start = |y: i32, m: u32, day: u32| {
                NaiveDate::from_ymd_opt(y, m, day).and_then(|x| x.and_hms_opt(0, 0, 0))
            };
            let at_time = |micros: i64| {
                d.and_hms_opt(0, 0, 0)
                    .map(|x| x + Duration::microseconds(micros))
            };
            let time = time_of(&ts);
            let truncated = match field.as_str() {
                "millennium" => start((year - 1).div_euclid(1000) * 1000 + 1, 1, 1),
                "century" => start((year - 1).div_euclid(100) * 100 + 1, 1, 1),
                "decade" => start(year.div_euclid(10) * 10, 1, 1),
                "year" => start(year, 1, 1),
                "quarter" => start(year, (d.month() - 1) / 3 * 3 + 1, 1),
                "month" => start(year, d.month(), 1),
                "week" => d
                    .checked_sub_signed(Duration::days(d.weekday().num_days_from_monday() as i64))
                    .and_then(|x| x.and_hms_opt(0, 0, 0)),
                "day" => at_time(0),
                "hour" => at_time(time / MICROS_PER_HOUR * MICROS_PER_HOUR),
                "minute" => at_time(time / MICROS_PER_MINUTE * MICROS_PER_MINUTE),
                "second" => at_time(time / MICROS_PER_SECOND * MICROS_PER_SECOND),
                "milliseconds" => at_time(time / 1000 * 1000),
                "microseconds" => at_time(time),
                _ => return Err(unit_error(other.kind())),
            }
            .ok_or("timestamp out of range")?;
            // A date is truncated as a zoned timestamp, as PostgreSQL casts it.
            match other {
                Temporal::TimestampTz(_) | Temporal::Date(_) => {
                    Temporal::TimestampTz(crate::timezone::from_session_local(truncated))
                }
                _ => Temporal::Timestamp(truncated),
            }
        }
    };
    Ok(result.to_value())
}

/// `(s1, e1) OVERLAPS (s2, e2)`: whether two periods share a moment, each
/// the half-open range from its earlier to its later end (an end may be an
/// interval after the start); periods starting together always overlap.
/// `kinds` are the arguments' static kinds, when known.
pub(crate) fn overlaps(args: &[Value], kinds: &[Option<Kind>]) -> Result<Value, String> {
    if args.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(Value::Null);
    }
    let invalid = |_: &Value| {
        let types: Vec<_> = args.iter().map(crate::value::value_type_name).collect();
        format!("function overlaps({}) does not exist", types.join(", "))
    };
    // A moment as microseconds: a time of day, or a (UTC) timestamp.
    let moment = |t: &Temporal| match t {
        Temporal::Time(micros) => Some(*micros as i128),
        other => other
            .as_timestamp()
            .map(|ts| ts.and_utc().timestamp_micros() as i128),
    };
    let kind = |i: usize| kinds.get(i).copied().flatten();
    let period = |i: usize| -> Result<(i128, i128), String> {
        let (start, end) = (&args[i], &args[i + 1]);
        let from = Temporal::read(start, kind(i)).ok_or_else(|| invalid(start))?;
        let to = match Temporal::read(end, kind(i + 1)).ok_or_else(|| invalid(end))? {
            Temporal::Interval(iv) => match from {
                Temporal::Time(micros) => {
                    Temporal::Time((micros + iv.micros).rem_euclid(MICROS_PER_DAY))
                }
                ref other => {
                    let ts = other.as_timestamp().ok_or_else(|| invalid(start))?;
                    Temporal::Timestamp(add_interval(ts, iv).ok_or("timestamp out of range")?)
                }
            },
            other => other,
        };
        let (a, b) = (
            moment(&from).ok_or_else(|| invalid(start))?,
            moment(&to).ok_or_else(|| invalid(end))?,
        );
        Ok(if b < a { (b, a) } else { (a, b) })
    };
    let (s1, e1) = period(0)?;
    let (s2, e2) = period(2)?;
    Ok(Value::Bool(if s1 > s2 {
        s1 < e2
    } else if s2 > s1 {
        s2 < e1
    } else {
        true
    }))
}

/// `age(a, b)`: `a - b` in years, months, and days (and time), borrowing a
/// month's days from the earlier value's month, as PostgreSQL does.
pub(crate) fn age(a: &Value, b: &Value) -> Result<Value, String> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    // Zoned timestamps compare as local times in the session's zone.
    let read = |v: &Value| {
        Temporal::read(v, None)
            .and_then(|t| match t {
                Temporal::TimestampTz(utc) => Some(crate::timezone::to_session_local(utc).0),
                other => other.as_timestamp(),
            })
            .ok_or_else(|| {
                format!(
                    "invalid input syntax for type timestamp: \"{}\"",
                    crate::render(v)
                )
            })
    };
    let (t1, t2) = (read(a)?, read(b)?);
    let (later, earlier, sign) = if t1 >= t2 { (t1, t2, 1) } else { (t2, t1, -1) };
    let mut micros = time_of(&later) - time_of(&earlier);
    let mut days = later.day() as i64 - earlier.day() as i64;
    let mut months = later.month() as i64 - earlier.month() as i64;
    let mut years = later.year() as i64 - earlier.year() as i64;
    if micros < 0 {
        micros += MICROS_PER_DAY;
        days -= 1;
    }
    while days < 0 {
        days += days_in_month(earlier.year(), earlier.month());
        months -= 1;
    }
    while months < 0 {
        months += 12;
        years -= 1;
    }
    let interval = Interval::new(years * 12 + months, days, micros);
    Ok(Temporal::Interval(if sign < 0 {
        interval.negate()
    } else {
        interval
    })
    .to_value())
}

fn days_in_month(year: i32, month: u32) -> i64 {
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    match (
        NaiveDate::from_ymd_opt(year, month, 1),
        NaiveDate::from_ymd_opt(next_year, next_month, 1),
    ) {
        (Some(a), Some(b)) => (b - a).num_days(),
        _ => 30,
    }
}

/// `date_bin(stride, source, origin)`: the start of the stride-long bin
/// `source` falls in, counting bins from `origin`.
pub(crate) fn date_bin(stride: &Value, source: &Value, origin: &Value) -> Result<Value, String> {
    if [stride, source, origin]
        .iter()
        .any(|v| matches!(v, Value::Null))
    {
        return Ok(Value::Null);
    }
    let Some(Temporal::Interval(stride)) = Temporal::read(stride, Some(Kind::Interval)) else {
        return Err("invalid input syntax for type interval".to_string());
    };
    if stride.months != 0 {
        return Err(
            "timestamps cannot be binned into intervals containing months or years".to_string(),
        );
    }
    let stride_micros = stride.days * MICROS_PER_DAY + stride.micros;
    if stride_micros <= 0 {
        return Err("stride must be greater than zero".to_string());
    }
    let src = Temporal::read(source, None).ok_or("invalid input syntax for type timestamp")?;
    let org = Temporal::read(origin, None)
        .and_then(|t| t.as_timestamp())
        .ok_or("invalid input syntax for type timestamp")?;
    let ts = src
        .as_timestamp()
        .ok_or("invalid input syntax for type timestamp")?;
    let delta = (ts - org)
        .num_microseconds()
        .ok_or("interval out of range")?;
    let binned = org + Duration::microseconds(delta.div_euclid(stride_micros) * stride_micros);
    Ok(match src {
        Temporal::TimestampTz(_) => Temporal::TimestampTz(binned),
        _ => Temporal::Timestamp(binned),
    }
    .to_value())
}

/// `generate_series(start, stop, step)` of timestamps: a date starts a
/// series of zoned timestamps, as PostgreSQL resolves it.
pub(crate) fn series(
    start: &Value,
    stop: &Value,
    step: &Value,
) -> Result<(String, Vec<Value>), String> {
    let start_t = Temporal::read(start, None).ok_or("invalid input syntax for type timestamp")?;
    let zoned = !matches!(start_t, Temporal::Timestamp(_));
    // A zoned series steps from instants: a date is its local midnight.
    let instant = |t: Temporal| match t {
        Temporal::Date(_) | Temporal::Timestamp(_) if zoned => {
            t.as_timestamp().map(crate::timezone::from_session_local)
        }
        other => other.as_timestamp(),
    };
    let first = instant(start_t).ok_or("invalid input syntax for type timestamp")?;
    let last = Temporal::read(stop, None)
        .and_then(instant)
        .ok_or("invalid input syntax for type timestamp")?;
    let Some(Temporal::Interval(step)) = Temporal::read(step, Some(Kind::Interval)) else {
        return Err("invalid input syntax for type interval".to_string());
    };
    if step.span() == 0 {
        return Err("step size cannot equal zero".to_string());
    }
    let ascending = step.span() > 0;
    let mut values = Vec::new();
    let mut current = first;
    while (ascending && current <= last) || (!ascending && current >= last) {
        values.push(
            if zoned {
                Temporal::TimestampTz(current)
            } else {
                Temporal::Timestamp(current)
            }
            .to_value(),
        );
        current = if zoned {
            add_interval_zoned(current, step)
        } else {
            add_interval(current, step)
        }
        .ok_or("timestamp out of range")?;
        if values.len() > 10_000_000 {
            return Err("generate_series produced too many rows".to_string());
        }
    }
    Ok((
        if zoned { "TIMESTAMPTZ" } else { "TIMESTAMP" }.to_string(),
        values,
    ))
}

/// A time from its parts (`make_time`).
pub(crate) fn make_time(hour: i64, minute: i64, second: f64) -> Result<Value, String> {
    let micros =
        hour * MICROS_PER_HOUR + minute * MICROS_PER_MINUTE + (second * 1e6).round() as i64;
    if !(0..24).contains(&hour)
        || !(0..60).contains(&minute)
        || !(0.0..60.0).contains(&second)
        || micros > MICROS_PER_DAY
    {
        return Err(format!(
            "time field value out of range: {hour}:{minute:02}:{second:02}"
        ));
    }
    Ok(Temporal::Time(micros).to_value())
}

/// Whether a date, timestamp, or interval is finite (`isfinite`).
pub(crate) fn is_finite(value: &Value) -> Option<bool> {
    match Temporal::read(value, None)? {
        Temporal::Infinite(..) => Some(false),
        _ => Some(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iv(text: &str) -> Interval {
        Interval::parse(text).unwrap_or_else(|| panic!("{text}"))
    }

    #[test]
    fn intervals_read_and_print_as_postgresql_does() {
        assert_eq!(iv("1 day 2 hours").format(), "1 day 02:00:00");
        assert_eq!(iv("-1 day 2 hours").format(), "-1 days +02:00:00");
        assert_eq!(iv("1.5 hours").format(), "01:30:00");
        assert_eq!(iv("1 day 02:03:04.5").format(), "1 day 02:03:04.5");
        assert_eq!(
            iv("P1Y2M3DT4H5M6S").format(),
            "1 year 2 mons 3 days 04:05:06"
        );
        assert_eq!(iv("2 weeks").format(), "14 days");
        assert_eq!(iv("1 year 13 months").format(), "2 years 1 mon");
        assert_eq!(iv("@ 3 days ago").format(), "-3 days");
        assert_eq!(iv("1 year -1 day").format(), "1 year -1 days");
        assert_eq!(iv("90 minutes").format(), "01:30:00");
        assert_eq!(Interval::default().format(), "00:00:00");
        assert_eq!(iv("-00:00:01").format(), "-00:00:01");
        assert!(Interval::parse("2024-01-01").is_none());
        // SQL's year-month and day-time forms.
        assert_eq!(iv("1-2").format(), "1 year 2 mons");
        assert_eq!(iv("-1-2").format(), "-1 years -2 mons");
        assert_eq!(iv("3 4:05:06").format(), "3 days 04:05:06");
        assert_eq!(
            iv("1-2 3 4:05:06").format(),
            "1 year 2 mons 3 days 04:05:06"
        );
    }

    #[test]
    fn units_read_by_any_of_their_names() {
        let t = |s: &str| Value::Text(s.to_string());
        let date = Some(Kind::Date);
        let n = |v: i64| Value::Numeric(rust_decimal::Decimal::from(v));
        assert_eq!(
            extract("years", &t("2024-05-06"), date, false).unwrap(),
            n(2024)
        );
        assert_eq!(extract("qtr", &t("2024-05-06"), date, false).unwrap(), n(2));
        assert!(
            extract("foo", &t("2024-05-06"), date, false)
                .unwrap_err()
                .contains("not recognized for type date")
        );
        assert!(
            extract("hour", &t("2024-05-06"), date, false)
                .unwrap_err()
                .contains("not supported for type date")
        );
        assert_eq!(
            extract("epoch", &t("infinity"), date, false).unwrap(),
            Value::Float(f64::INFINITY)
        );
        assert_eq!(
            trunc("mons", &t("2024-05-06 10:00:00"), Some(Kind::Timestamp)).unwrap(),
            t("2024-05-01 00:00:00")
        );
        assert!(
            trunc("dow", &t("2024-05-06 10:00:00"), Some(Kind::Timestamp))
                .unwrap_err()
                .contains("not recognized")
        );
    }

    #[test]
    fn periods_overlap_on_the_half_open_range() {
        let t = |s: &str| Value::Text(s.to_string());
        let run = |args: [&str; 4], kinds: &[Option<Kind>]| overlaps(&args.map(t), kinds).unwrap();
        assert_eq!(
            run(
                ["2024-01-01", "2024-02-01", "2024-01-15", "2024-03-01"],
                &[]
            ),
            Value::Bool(true)
        );
        // Touching ends do not overlap; an interval end counts from the start.
        assert_eq!(
            run(["2024-01-01", "1 day", "2024-01-02", "2024-01-03"], &[]),
            Value::Bool(false)
        );
        let time = Some(Kind::Time);
        assert_eq!(
            run(
                ["10:00:00", "11:00:00", "10:30:00", "12:00:00"],
                &[time, time, time, time]
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn interval_arithmetic_carries_fractions_down() {
        assert_eq!(iv("1 day").mul(1.5).unwrap().format(), "1 day 12:00:00");
        assert_eq!(iv("1 month").div(3.0).unwrap().format(), "10 days");
        assert_eq!(
            iv("1 day 2 hours").mul(2.0).unwrap().format(),
            "2 days 04:00:00"
        );
        assert_eq!(iv("35 days").justify_days().format(), "1 mon 5 days");
        assert_eq!(iv("27 hours").justify_hours().format(), "1 day 03:00:00");
        assert_eq!(iv("1 mon -1 hour").justify().format(), "29 days 23:00:00");
    }

    #[test]
    fn operators_follow_their_operand_kinds() {
        let t = |s: &str| Value::Text(s.to_string());
        let run = |op, l: Value, lk, r: Value, rk| arith(op, &l, lk, &r, rk).unwrap();
        assert_eq!(
            run(
                "-",
                t("2024-03-01 10:00:00"),
                None,
                t("2024-02-28 08:30:00"),
                None
            ),
            t("2 days 01:30:00")
        );
        assert_eq!(
            run(
                "+",
                t("2024-01-31"),
                Some(Kind::Date),
                t("1 mon"),
                Some(Kind::Interval)
            ),
            t("2024-02-29 00:00:00")
        );
        assert_eq!(
            run(
                "+",
                t("23:30:00"),
                Some(Kind::Time),
                t("01:00:00"),
                Some(Kind::Interval)
            ),
            t("00:30:00")
        );
        assert_eq!(
            run("-", t("2024-03-01"), None, t("2024-02-01"), None),
            Value::Int(29)
        );
        assert_eq!(
            run("*", t("1 day 02:00:00"), None, Value::Int(2), None),
            t("2 days 04:00:00")
        );
        assert!(
            arith(
                "+",
                &t("2024-01-01"),
                Some(Kind::Date),
                &t("2024-01-02"),
                Some(Kind::Date)
            )
            .is_err()
        );
    }

    #[test]
    fn age_borrows_days_from_the_earlier_month() {
        let t = |s: &str| Value::Text(s.to_string());
        assert_eq!(
            age(&t("2024-03-01"), &t("2020-01-15")).unwrap(),
            t("4 years 1 mon 17 days")
        );
        assert_eq!(
            age(&t("2020-01-15"), &t("2024-03-01")).unwrap(),
            t("-4 years -1 mons -17 days")
        );
    }
}
