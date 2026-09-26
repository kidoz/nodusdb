//! Formatting by template: `to_char` of dates, timestamps, intervals, and
//! numbers, and the reverse, `to_date`, `to_timestamp`, and `to_number`, with
//! PostgreSQL's template patterns (`YYYY-MM-DD HH24:MI:SS`, `FM9,999.00`).

use crate::Value;
use crate::datetime::{
    Interval, Kind, MICROS_PER_HOUR, MICROS_PER_MINUTE, MICROS_PER_SECOND, Temporal,
};
use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

pub(crate) const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
const DAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
const ROMAN: [&str; 12] = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI", "XII",
];

/// The date/time patterns, longest first so each matches greedily.
const PATTERNS: &[&str] = &[
    "Y,YYY", "SSSSS", "MONTH", "HH24", "HH12", "IDDD", "IYYY", "SSSS", "A.D.", "A.M.", "B.C.",
    "P.M.", "DAY", "DDD", "IYY", "MON", "TZH", "TZM", "YYYY", "YYY", "FF1", "FF2", "FF3", "FF4",
    "FF5", "FF6", "AD", "AM", "BC", "CC", "DD", "DY", "FX", "HH", "ID", "IW", "IY", "MI", "MM",
    "MS", "OF", "PM", "RM", "SS", "TZ", "US", "WW", "YY", "D", "I", "J", "Q", "W", "Y",
];

/// How a text pattern was written: `MONTH`, `month`, or `Month`.
#[derive(Clone, Copy, PartialEq)]
enum Case {
    Upper,
    Lower,
    Capital,
}

enum Node {
    Literal(String),
    Field {
        pattern: &'static str,
        case: Case,
        fill: bool,
        ordinal: Option<Case>,
    },
}

fn parse_template(template: &str) -> Vec<Node> {
    let mut nodes = Vec::new();
    let chars: Vec<char> = template.chars().collect();
    let mut literal = String::new();
    let mut fill = false;
    let mut i = 0;
    let rest = |i: usize| chars[i..].iter().collect::<String>();
    while i < chars.len() {
        if chars[i] == '"' {
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' && i + 1 < chars.len() {
                    i += 1;
                }
                literal.push(chars[i]);
                i += 1;
            }
            i += 1;
            continue;
        }
        // Outside quotes a backslash is itself, but before a double quote.
        if chars[i] == '\\' && chars.get(i + 1) == Some(&'"') {
            literal.push('"');
            i += 2;
            continue;
        }
        let tail = rest(i);
        let upper = tail.to_ascii_uppercase();
        if upper.starts_with("FM") {
            fill = true;
            i += 2;
            continue;
        }
        if upper.starts_with("TM") && PATTERNS.iter().any(|p| upper[2..].starts_with(p)) {
            i += 2;
            continue;
        }
        if let Some(pattern) = PATTERNS.iter().find(|p| upper.starts_with(*p)) {
            if !literal.is_empty() {
                nodes.push(Node::Literal(std::mem::take(&mut literal)));
            }
            let written: String = chars[i..i + pattern.chars().count()].iter().collect();
            let case = case_of(&written);
            i += pattern.chars().count();
            let suffix = rest(i);
            let ordinal = if suffix.starts_with("TH") {
                Some(Case::Upper)
            } else if suffix.starts_with("th") {
                Some(Case::Lower)
            } else {
                None
            };
            if ordinal.is_some() {
                i += 2;
            }
            nodes.push(Node::Field {
                pattern,
                case,
                fill,
                ordinal,
            });
            fill = false;
            continue;
        }
        literal.push(chars[i]);
        i += 1;
    }
    if !literal.is_empty() {
        nodes.push(Node::Literal(literal));
    }
    nodes
}

fn case_of(written: &str) -> Case {
    let letters: Vec<char> = written
        .chars()
        .filter(|c| c.is_ascii_alphabetic())
        .collect();
    if letters.iter().all(|c| c.is_ascii_uppercase()) {
        Case::Upper
    } else if letters.iter().all(|c| c.is_ascii_lowercase()) {
        Case::Lower
    } else {
        Case::Capital
    }
}

fn cased(text: &str, case: Case) -> String {
    match case {
        Case::Upper => text.to_uppercase(),
        Case::Lower => text.to_lowercase(),
        Case::Capital => text.to_string(),
    }
}

/// `1st`, `2nd`, `3rd`, `4th`, ... (`11th`, `12th`, `13th`).
fn ordinal_suffix(n: i64, case: Case) -> &'static str {
    let suffix = match (n % 100, n % 10) {
        (11..=13, _) => "th",
        (_, 1) => "st",
        (_, 2) => "nd",
        (_, 3) => "rd",
        _ => "th",
    };
    match (case, suffix) {
        (Case::Upper, "st") => "ST",
        (Case::Upper, "nd") => "ND",
        (Case::Upper, "rd") => "RD",
        (Case::Upper, _) => "TH",
        (_, s) => s,
    }
}

/// What a date/time `to_char` formats: a timestamp's fields, or an
/// interval's.
struct Fields {
    date: Option<NaiveDate>,
    year: i64,
    month: i64,
    day: i64,
    hours: i64,
    micros_of_hour: i64,
    zoned: bool,
}

impl Fields {
    fn of(temporal: &Temporal) -> Option<Fields> {
        let from_timestamp = |ts: NaiveDateTime, zoned: bool| Fields {
            date: Some(ts.date()),
            year: ts.year() as i64,
            month: ts.month() as i64,
            day: ts.day() as i64,
            hours: ts.hour() as i64,
            micros_of_hour: ts.minute() as i64 * MICROS_PER_MINUTE
                + ts.second() as i64 * MICROS_PER_SECOND
                + (ts.nanosecond() / 1000) as i64 % MICROS_PER_SECOND,
            zoned,
        };
        Some(match temporal {
            Temporal::Date(d) => from_timestamp(d.and_hms_opt(0, 0, 0)?, false),
            Temporal::Timestamp(ts) => from_timestamp(*ts, false),
            Temporal::TimestampTz(ts) => from_timestamp(*ts, true),
            Temporal::Time(t) => Fields {
                date: None,
                year: 0,
                month: 0,
                day: 0,
                hours: t / MICROS_PER_HOUR,
                micros_of_hour: t % MICROS_PER_HOUR,
                zoned: false,
            },
            Temporal::Interval(Interval {
                months,
                days,
                micros,
            }) => Fields {
                date: None,
                year: months / 12,
                month: months % 12,
                day: *days,
                hours: micros / MICROS_PER_HOUR,
                micros_of_hour: micros % MICROS_PER_HOUR,
                zoned: false,
            },
            Temporal::Infinite(..) => return None,
        })
    }
}

/// `to_char(date/timestamp/interval, template)`.
pub(crate) fn format_temporal(temporal: &Temporal, template: &str) -> String {
    let Some(f) = Fields::of(temporal) else {
        return String::new();
    };
    let mut out = String::new();
    let minute = f.micros_of_hour / MICROS_PER_MINUTE;
    let second = f.micros_of_hour % MICROS_PER_MINUTE / MICROS_PER_SECOND;
    let fraction = f.micros_of_hour % MICROS_PER_SECOND;
    for node in parse_template(template) {
        let (pattern, case, fill, ordinal) = match node {
            Node::Literal(text) => {
                out.push_str(&text);
                continue;
            }
            Node::Field {
                pattern,
                case,
                fill,
                ordinal,
            } => (pattern, case, fill, ordinal),
        };
        // A number, zero-padded to `width` (its sign included) unless filled.
        let num = |n: i64, width: usize| -> (String, Option<i64>) {
            let text = if fill {
                n.to_string()
            } else {
                format!("{n:0width$}")
            };
            (text, Some(n))
        };
        // A time field keeps two digits after a sign.
        let clock = |n: i64| num(n, if n < 0 { 3 } else { 2 });
        // Text, padded to `width` unless filled.
        let word = |text: String, width: usize| -> (String, Option<i64>) {
            let text = cased(&text, case);
            if fill {
                (text, None)
            } else {
                (format!("{text:<width$}"), None)
            }
        };
        let date = f.date.unwrap_or_default();
        let hour12 = match f.hours % 12 {
            0 => 12,
            h => h,
        };
        let meridiem = |long: bool| {
            let pm = f.hours % 24 >= 12;
            let text = match (pm, long) {
                (false, false) => "AM",
                (true, false) => "PM",
                (false, true) => "A.M.",
                (true, true) => "P.M.",
            };
            (cased(text, case), None)
        };
        let (text, number) = match pattern {
            "HH" | "HH12" => num(hour12, 2),
            "HH24" => clock(f.hours),
            "MI" => clock(minute),
            "SS" => clock(second),
            "MS" => num(fraction / 1000, 3),
            "US" => num(fraction, 6),
            "FF1" | "FF2" | "FF3" | "FF4" | "FF5" | "FF6" => {
                let digits: usize = pattern[2..].parse().unwrap_or(6);
                num(fraction / 10i64.pow(6 - digits as u32), digits)
            }
            "SSSS" | "SSSSS" => num(f.hours * 3600 + minute * 60 + second, 0),
            "AM" | "PM" => meridiem(false),
            "A.M." | "P.M." => meridiem(true),
            "Y,YYY" => {
                let text = format!("{},{:03}", f.year / 1000, f.year % 1000);
                (text, Some(f.year))
            }
            "YYYY" => num(f.year, 4),
            "YYY" => num(f.year % 1000, 3),
            "YY" => num(f.year % 100, 2),
            "Y" => num(f.year % 10, 1),
            "IYYY" => num(date.iso_week().year() as i64, 4),
            "IYY" => num(date.iso_week().year() as i64 % 1000, 3),
            "IY" => num(date.iso_week().year() as i64 % 100, 2),
            "I" => num(date.iso_week().year() as i64 % 10, 1),
            "BC" | "AD" => (cased(if f.year > 0 { "AD" } else { "BC" }, case), None),
            "B.C." | "A.D." => (cased(if f.year > 0 { "A.D." } else { "B.C." }, case), None),
            "MONTH" if f.month >= 1 => word(MONTHS[f.month as usize - 1].to_string(), 9),
            "MON" if f.month >= 1 => (cased(&MONTHS[f.month as usize - 1][..3], case), None),
            "MM" => num(f.month, 2),
            "DAY" => word(
                DAYS[date.weekday().num_days_from_sunday() as usize].to_string(),
                9,
            ),
            "DY" => (
                cased(
                    &DAYS[date.weekday().num_days_from_sunday() as usize][..3],
                    case,
                ),
                None,
            ),
            "DDD" => num(date.ordinal() as i64, 3),
            "IDDD" => {
                let week = date.iso_week();
                let n = (week.week() as i64 - 1) * 7 + date.weekday().number_from_monday() as i64;
                num(n, 3)
            }
            "DD" => num(f.day, 2),
            "D" => num(date.weekday().num_days_from_sunday() as i64 + 1, 1),
            "ID" => num(date.weekday().number_from_monday() as i64, 1),
            "W" => num((f.day - 1) / 7 + 1, 1),
            "WW" => num((date.ordinal() as i64 - 1) / 7 + 1, 2),
            "IW" => num(date.iso_week().week() as i64, 2),
            "CC" => num(
                if f.year > 0 {
                    (f.year + 99) / 100
                } else {
                    -((99 - f.year) / 100)
                },
                2,
            ),
            "J" => num(date.num_days_from_ce() as i64 + 1_721_425, 0),
            "Q" => num((f.month - 1).max(0) / 3 + 1, 1),
            "RM" if f.month >= 1 => word(ROMAN[f.month as usize - 1].to_string(), 4),
            "TZ" => (
                if f.zoned {
                    cased("UTC", case)
                } else {
                    String::new()
                },
                None,
            ),
            "OF" => (
                if f.zoned {
                    "+00".to_string()
                } else {
                    String::new()
                },
                None,
            ),
            "TZH" => (
                if f.zoned {
                    "+00".to_string()
                } else {
                    String::new()
                },
                None,
            ),
            "TZM" => (
                if f.zoned {
                    "00".to_string()
                } else {
                    String::new()
                },
                None,
            ),
            _ => (String::new(), None),
        };
        out.push_str(&text);
        if let (Some(case), Some(n)) = (ordinal, number) {
            out.push_str(ordinal_suffix(n, case));
        }
    }
    out
}

/// `to_date(text, template)` and `to_timestamp(text, template)`: reads
/// `text` by the template's fields; a literal in the template matches any
/// one character of the text, and spaces are skipped.
pub(crate) fn parse_by_template(
    text: &str,
    template: &str,
    date_only: bool,
) -> Result<Value, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut at = 0;
    let (mut year, mut month, mut day) = (None::<i64>, None::<i64>, None::<i64>);
    let (mut hour, mut minute, mut second, mut micros) = (0i64, 0i64, 0i64, 0i64);
    let mut ordinal_day: Option<i64> = None;
    let mut pm: Option<bool> = None;
    let mut hour12 = false;
    // East of UTC, from `TZH`, `TZM`, or `OF`.
    let mut offset_seconds = 0i64;
    let skip_spaces = |at: &mut usize| {
        while *at < chars.len() && chars[*at].is_whitespace() {
            *at += 1;
        }
    };
    let bad = |what: &str, value: &str| format!("invalid value \"{value}\" for \"{what}\"");
    let nodes = parse_template(template);
    for (index, node) in nodes.iter().enumerate() {
        // A year right before another field has its four digits only.
        let year_width = match nodes.get(index + 1) {
            Some(Node::Field { .. }) => 4,
            _ => 0,
        };
        match *node {
            Node::Literal(ref literal) => {
                for c in literal.chars() {
                    if c.is_whitespace() {
                        skip_spaces(&mut at);
                    } else if at < chars.len() && !chars[at].is_ascii_alphanumeric() {
                        at += 1;
                    } else if at < chars.len() && chars[at] == c {
                        at += 1;
                    }
                }
            }
            Node::Field { pattern, .. } => {
                skip_spaces(&mut at);
                match pattern {
                    "YYYY" | "IYYY" => {
                        year = Some(read_digits(&chars, &mut at, year_width, pattern)?)
                    }
                    "Y,YYY" => {
                        let thousands = read_digits(&chars, &mut at, 0, pattern)?;
                        if at < chars.len() && chars[at] == ',' {
                            at += 1;
                        }
                        year = Some(thousands * 1000 + read_digits(&chars, &mut at, 3, pattern)?);
                    }
                    "YYY" | "IYY" => year = Some(2000 + read_digits(&chars, &mut at, 3, pattern)?),
                    "YY" | "IY" => {
                        let yy = read_digits(&chars, &mut at, 2, pattern)?;
                        year = Some(if yy < 70 { 2000 + yy } else { 1900 + yy });
                    }
                    "Y" | "I" => year = Some(2000 + read_digits(&chars, &mut at, 1, pattern)?),
                    "MM" => month = Some(read_digits(&chars, &mut at, 2, pattern)?),
                    "DD" => day = Some(read_digits(&chars, &mut at, 2, pattern)?),
                    "DDD" | "IDDD" => ordinal_day = Some(read_digits(&chars, &mut at, 3, pattern)?),
                    "HH" | "HH12" => {
                        hour = read_digits(&chars, &mut at, 2, pattern)?;
                        hour12 = true;
                    }
                    "HH24" => hour = read_digits(&chars, &mut at, 2, pattern)?,
                    "MI" => minute = read_digits(&chars, &mut at, 2, pattern)?,
                    "SS" => second = read_digits(&chars, &mut at, 2, pattern)?,
                    "MS" => {
                        let start = at;
                        let ms = read_digits(&chars, &mut at, 3, pattern)?;
                        micros = ms * 10i64.pow(3 - (at - start).min(3) as u32) * 1000;
                    }
                    "US" | "FF1" | "FF2" | "FF3" | "FF4" | "FF5" | "FF6" => {
                        let start = at;
                        let us = read_digits(&chars, &mut at, 6, pattern)?;
                        micros = us * 10i64.pow(6 - (at - start).min(6) as u32);
                    }
                    "SSSS" | "SSSSS" => {
                        let total = read_digits(&chars, &mut at, 0, pattern)?;
                        hour = total / 3600;
                        minute = total % 3600 / 60;
                        second = total % 60;
                    }
                    "AM" | "PM" | "A.M." | "P.M." => {
                        let rest: String =
                            chars[at..].iter().collect::<String>().to_ascii_uppercase();
                        let (value, len) = if rest.starts_with("A.M.") {
                            (false, 4)
                        } else if rest.starts_with("P.M.") {
                            (true, 4)
                        } else if rest.starts_with("AM") {
                            (false, 2)
                        } else if rest.starts_with("PM") {
                            (true, 2)
                        } else {
                            return Err(bad(pattern, &rest.chars().take(2).collect::<String>()));
                        };
                        pm = Some(value);
                        at += len;
                    }
                    "MONTH" | "MON" => {
                        let rest: String =
                            chars[at..].iter().collect::<String>().to_ascii_lowercase();
                        let found = MONTHS.iter().enumerate().find_map(|(i, m)| {
                            let full = m.to_ascii_lowercase();
                            if pattern == "MONTH" && rest.starts_with(&full) {
                                Some((i, full.len()))
                            } else if rest.starts_with(&full[..3]) {
                                Some((i, 3))
                            } else {
                                None
                            }
                        });
                        let Some((index, len)) = found else {
                            return Err(bad(pattern, &rest.chars().take(3).collect::<String>()));
                        };
                        month = Some(index as i64 + 1);
                        at += len;
                    }
                    "DAY" | "DY" => {
                        let rest: String =
                            chars[at..].iter().collect::<String>().to_ascii_lowercase();
                        let len = DAYS
                            .iter()
                            .find_map(|d| {
                                let full = d.to_ascii_lowercase();
                                if pattern == "DAY" && rest.starts_with(&full) {
                                    Some(full.len())
                                } else if rest.starts_with(&full[..3]) {
                                    Some(3)
                                } else {
                                    None
                                }
                            })
                            .ok_or_else(|| {
                                bad(pattern, &rest.chars().take(3).collect::<String>())
                            })?;
                        at += len;
                    }
                    "D" | "ID" | "W" | "WW" | "IW" | "Q" | "CC" => {
                        read_digits(&chars, &mut at, 2, pattern)?;
                    }
                    "J" => {
                        let julian = read_digits(&chars, &mut at, 0, pattern)?;
                        let date =
                            NaiveDate::from_num_days_from_ce_opt((julian - 1_721_425) as i32)
                                .ok_or_else(|| bad(pattern, &julian.to_string()))?;
                        year = Some(date.year() as i64);
                        month = Some(date.month() as i64);
                        day = Some(date.day() as i64);
                    }
                    "AD" | "BC" | "A.D." | "B.C." => {
                        let len = pattern.len().min(chars.len() - at);
                        at += len;
                    }
                    "TZH" | "OF" => {
                        let negative = chars.get(at) == Some(&'-');
                        let hours = read_digits(&chars, &mut at, 3, pattern)?;
                        let mut minutes = 0;
                        if pattern == "OF" && chars.get(at) == Some(&':') {
                            at += 1;
                            minutes = read_digits(&chars, &mut at, 2, pattern)?;
                        }
                        let sign = if negative { -1 } else { 1 };
                        offset_seconds = hours * 3600 + sign * minutes * 60;
                    }
                    "TZM" => {
                        let minutes = read_digits(&chars, &mut at, 2, pattern)?;
                        offset_seconds += if offset_seconds < 0 {
                            -minutes
                        } else {
                            minutes
                        } * 60;
                    }
                    "TZ" => {
                        while at < chars.len() && !chars[at].is_whitespace() {
                            at += 1;
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    if hour12 {
        if hour > 12 || hour < 1 {
            return Err(format!("hour \"{hour}\" is invalid for the 12-hour clock"));
        }
        hour %= 12;
    }
    if pm == Some(true) && hour < 12 {
        hour += 12;
    }
    let year = year.unwrap_or(1) as i32;
    let date = match ordinal_day {
        Some(n) if month.is_none() && day.is_none() => NaiveDate::from_yo_opt(year, n as u32),
        _ => NaiveDate::from_ymd_opt(year, month.unwrap_or(1) as u32, day.unwrap_or(1) as u32),
    }
    .ok_or_else(|| format!("date/time field value out of range: \"{text}\""))?;
    if date_only {
        return Ok(Temporal::Date(date).to_value());
    }
    let ts = date
        .and_hms_micro_opt(hour as u32, minute as u32, second as u32, micros as u32)
        .ok_or_else(|| format!("date/time field value out of range: \"{text}\""))?;
    Ok(Temporal::TimestampTz(ts - chrono::Duration::seconds(offset_seconds)).to_value())
}

/// Reads a number of at most `width` digits (any number for 0), signed,
/// at `at` in `chars`, for template field `pattern`.
fn read_digits(chars: &[char], at: &mut usize, width: usize, pattern: &str) -> Result<i64, String> {
    let start = *at;
    if *at < chars.len() && (chars[*at] == '-' || chars[*at] == '+') {
        *at += 1;
    }
    while *at < chars.len() && chars[*at].is_ascii_digit() && (width == 0 || *at - start < width) {
        *at += 1;
    }
    let taken: String = chars[start..*at].iter().collect();
    taken.parse::<i64>().map_err(|_| {
        let shown: String = chars[start..].iter().take(width.max(1)).collect();
        format!("invalid value \"{shown}\" for \"{pattern}\"")
    })
}

/// Where a template puts a number's sign.
#[derive(Default, Clone, Copy, PartialEq)]
enum SignMark {
    /// A space or `-` just before the digits.
    #[default]
    Default,
    /// `S`: `+` or `-` next to the digits.
    Anchored,
    /// `SG`: `+` or `-` at its place.
    Always,
    /// `MI`: `-` or a space at its place.
    Minus,
    /// `PL`: `+` or a space at its place, besides the default sign.
    Plus,
}

/// A number template: its digit positions and marks.
#[derive(Default)]
struct NumberTemplate {
    /// Before the decimal point, each digit position (`true` for `0`) and
    /// group separator (`None`).
    integer: Vec<Option<bool>>,
    /// After it, each digit position (`true` for `0`).
    fraction: Vec<bool>,
    point: Option<char>,
    fill: bool,
    /// The sign marks before and after the digits.
    lead: Option<SignMark>,
    trail: Option<SignMark>,
    angle: bool,
    exponent: bool,
    ordinal: Option<Case>,
    roman: Option<Case>,
}

impl NumberTemplate {
    /// Whether the number carries the default sign (a space or `-`).
    fn default_sign(&self) -> bool {
        !self.angle
            && [self.lead, self.trail]
                .iter()
                .flatten()
                .all(|mark| *mark == SignMark::Plus)
    }
}

fn parse_number_template(template: &str) -> Result<NumberTemplate, String> {
    let mut t = NumberTemplate::default();
    let upper = template.to_ascii_uppercase();
    let chars: Vec<char> = upper.chars().collect();
    let mut i = 0;
    let sign = |t: &mut NumberTemplate, mark: SignMark| {
        if t.integer.is_empty() && t.point.is_none() {
            t.lead = Some(mark);
        } else {
            t.trail = Some(mark);
        }
    };
    while i < chars.len() {
        let rest: String = chars[i..].iter().collect();
        if rest.starts_with("FM") {
            t.fill = true;
            i += 2;
        } else if rest.starts_with("EEEE") {
            t.exponent = true;
            i += 4;
        } else if rest.starts_with("PR") {
            t.angle = true;
            i += 2;
        } else if rest.starts_with("MI") {
            sign(&mut t, SignMark::Minus);
            i += 2;
        } else if rest.starts_with("PL") {
            sign(&mut t, SignMark::Plus);
            i += 2;
        } else if rest.starts_with("SG") {
            sign(&mut t, SignMark::Always);
            i += 2;
        } else if rest.starts_with("RN") {
            t.roman = Some(if template[i..].starts_with("rn") {
                Case::Lower
            } else {
                Case::Upper
            });
            i += 2;
        } else if rest.starts_with("TH") {
            t.ordinal = Some(if template[i..].starts_with("th") {
                Case::Lower
            } else {
                Case::Upper
            });
            i += 2;
        } else {
            match chars[i] {
                '9' | '0' => {
                    if t.angle {
                        return Err(format!("\"{}\" must be ahead of \"PR\"", chars[i]));
                    }
                    let zero = chars[i] == '0';
                    if t.point.is_some() {
                        t.fraction.push(zero);
                    } else {
                        t.integer.push(Some(zero));
                    }
                }
                '.' | 'D' => t.point = Some('.'),
                ',' | 'G' => {
                    if t.point.is_none() {
                        t.integer.push(None);
                    }
                }
                'S' => sign(&mut t, SignMark::Anchored),
                _ => {}
            }
            i += 1;
        }
    }
    Ok(t)
}

/// `n` (1 to 3999) in Roman numerals.
fn roman(mut n: i64) -> String {
    const NUMERALS: &[(i64, &str)] = &[
        (1000, "M"),
        (900, "CM"),
        (500, "D"),
        (400, "CD"),
        (100, "C"),
        (90, "XC"),
        (50, "L"),
        (40, "XL"),
        (10, "X"),
        (9, "IX"),
        (5, "V"),
        (4, "IV"),
        (1, "I"),
    ];
    let mut out = String::new();
    for (value, numeral) in NUMERALS {
        while n >= *value {
            out.push_str(numeral);
            n -= value;
        }
    }
    out
}

/// `to_char(number, template)`.
pub(crate) fn format_number(
    value: f64,
    exact: Option<rust_decimal::Decimal>,
    template: &str,
) -> Result<String, String> {
    let t = parse_number_template(template)?;
    let negative = value < 0.0 || exact.is_some_and(|d| d.is_sign_negative() && !d.is_zero());
    if let Some(case) = t.roman {
        let n = value.round() as i64;
        if !(1..=3999).contains(&n) {
            return Ok("#".repeat(15));
        }
        let numerals = cased(&roman(n), case);
        return Ok(if t.fill {
            numerals
        } else {
            format!("{numerals:>15}")
        });
    }
    let scale = t.fraction.len() as u32;
    if t.exponent {
        let formatted = format!("{:.*e}", scale as usize, value.abs());
        let (mantissa, exponent) = formatted.split_once('e').unwrap_or((&formatted, "0"));
        let exponent: i32 = exponent.parse().unwrap_or(0);
        let sign = if negative {
            "-"
        } else if t.fill {
            ""
        } else {
            " "
        };
        return Ok(format!(
            "{sign}{mantissa}e{}{:02}",
            if exponent < 0 { '-' } else { '+' },
            exponent.abs()
        ));
    }
    // The digits, rounded half away from zero to the template's scale.
    let rounded = match exact {
        Some(d) => d
            .abs()
            .round_dp_with_strategy(scale, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
            .to_string(),
        None => format!("{:.*}", scale as usize, value.abs()),
    };
    let (int_digits, frac_digits) = rounded.split_once('.').unwrap_or((&rounded, ""));
    let int_digits = int_digits.trim_start_matches('0');
    let positions = t.integer.iter().filter(|p| p.is_some()).count();
    let overflow = int_digits.len() > positions;
    // The integer part, right to left. A `0` position prints a digit, and
    // so does every position after it; a zero value keeps its last digit.
    let last_position = t.integer.iter().rposition(|p| p.is_some());
    let zero_value = int_digits.is_empty() && frac_digits.chars().all(|c| c == '0');
    let mut integer = String::new();
    let mut digits = int_digits.chars().rev();
    let mut remaining = int_digits.len();
    for (index, position) in t.integer.iter().enumerate().rev() {
        let zero_from_here = t.integer[..=index].iter().flatten().any(|z| *z);
        match position {
            Some(_) => {
                let c = if overflow {
                    '#'
                } else if let Some(d) = digits.next() {
                    remaining -= 1;
                    d
                } else if zero_from_here
                    || (zero_value && t.point.is_none() && Some(index) == last_position)
                {
                    '0'
                } else {
                    ' '
                };
                integer.push(c);
            }
            // A separator shows between digits, else as a space.
            None => integer.push(if overflow || remaining > 0 || zero_from_here {
                ','
            } else {
                ' '
            }),
        }
    }
    let mut integer: String = integer.chars().rev().collect();
    // The fraction.
    let mut fraction = String::new();
    if t.point.is_some() {
        fraction.push('.');
        for i in 0..t.fraction.len() {
            fraction.push(if overflow {
                '#'
            } else {
                frac_digits.chars().nth(i).unwrap_or('0')
            });
        }
        if t.fill {
            // Fill mode drops the trailing zeros of `9` positions.
            while fraction.len() > 1 && fraction.ends_with('0') && !t.fraction[fraction.len() - 2] {
                fraction.pop();
            }
        }
    }
    if t.fill {
        integer = integer.trim_start().to_string();
    }
    let body = format!("{integer}{fraction}");
    // A sign next to the digits: in front of the first digit, the number
    // keeping its width plus one.
    let anchored = |mark: char| {
        let placed = place_before_digits(&body, mark);
        if t.fill {
            placed
        } else {
            format!("{placed:>width$}", width = body.len() + 1)
        }
    };
    let signed = if t.lead == Some(SignMark::Anchored) {
        anchored(if negative { '-' } else { '+' })
    } else if t.angle {
        if negative {
            let placed = place_before_digits(&body, '<') + ">";
            if t.fill {
                placed
            } else {
                format!("{placed:>width$}", width = body.len() + 2)
            }
        } else if t.fill {
            body
        } else {
            format!(" {body} ")
        }
    } else if !t.default_sign() {
        body
    } else if negative {
        anchored('-')
    } else if t.fill {
        body
    } else {
        format!(" {body}")
    };
    let mark = |mark: Option<SignMark>| match mark {
        Some(SignMark::Always) => {
            if negative {
                "-"
            } else {
                "+"
            }
        }
        Some(SignMark::Anchored) if t.trail == mark => {
            if negative {
                "-"
            } else {
                "+"
            }
        }
        Some(SignMark::Minus) => {
            if negative {
                "-"
            } else {
                " "
            }
        }
        Some(SignMark::Plus) => {
            if negative {
                " "
            } else {
                "+"
            }
        }
        _ => "",
    };
    let lead = if t.lead == Some(SignMark::Anchored) {
        ""
    } else {
        mark(t.lead)
    };
    let mut out = format!("{lead}{signed}{}", mark(t.trail));
    if let (Some(case), Some(n)) = (t.ordinal, (!negative).then_some(value as i64)) {
        out.push_str(ordinal_suffix(n, case));
    }
    Ok(out)
}

/// `text` with `mark` in place of the space just before its first digit
/// (or before its point), or in front when there is none.
fn place_before_digits(text: &str, mark: char) -> String {
    let first = text
        .find(|c: char| c.is_ascii_digit() || c == '.' || c == '#')
        .unwrap_or(0);
    let (head, tail) = text.split_at(first);
    let head = head.strip_suffix(' ').unwrap_or(head);
    format!("{head}{mark}{tail}")
}

/// `to_number(text, template)`, read as PostgreSQL reads it: each digit
/// position takes one character of `text` (after one space), a digit or the
/// point; a group separator takes only a `,`; a sign mark takes a sign; and
/// any other template character skips one character. A sign may also lead
/// the digits, and decimals past the template's are dropped.
pub(crate) fn parse_number(text: &str, template: &str) -> Result<Value, String> {
    let t = parse_number_template(template)?;
    if t.exponent {
        return Err("\"EEEE\" not supported for input".to_string());
    }
    let input: Vec<char> = text.chars().collect();
    let pattern: Vec<char> = template.to_ascii_uppercase().chars().collect();
    let (mut at, mut i) = (0, 0);
    let mut digits = String::new();
    let mut decimals: Option<usize> = None;
    let mut negative = false;
    while i < pattern.len() && at < input.len() {
        let rest: String = pattern[i..].iter().collect();
        let mark = ["FM", "PR", "MI", "PL", "SG", "TH", "RN"]
            .into_iter()
            .find(|m| rest.starts_with(m));
        if let Some(mark) = mark {
            i += 2;
            match (mark, input[at]) {
                ("MI" | "SG", '-') | ("PR", '<') => {
                    negative = true;
                    at += 1;
                }
                ("PL" | "SG", '+') => at += 1,
                _ => {}
            }
            continue;
        }
        let c = pattern[i];
        i += 1;
        match c {
            '9' | '0' | '.' | 'D' => {
                if input[at] == ' ' {
                    at += 1;
                }
                // A sign (or `<` for `PR`) before the first digit.
                if digits.is_empty()
                    && at < input.len()
                    && (matches!(input[at], '-' | '+') || (t.angle && input[at] == '<'))
                {
                    negative = input[at] != '+';
                    at += 1;
                }
                match input.get(at) {
                    Some(d @ '0'..='9') => match decimals.as_mut() {
                        Some(n) if *n >= t.fraction.len() => {}
                        Some(n) => {
                            *n += 1;
                            digits.push(*d);
                        }
                        None => digits.push(*d),
                    },
                    Some('.') if t.point.is_some() && decimals.is_none() => {
                        decimals = Some(0);
                        digits.push('.');
                    }
                    _ => {}
                }
                at += 1;
            }
            ',' | 'G' => {
                if input[at] == ',' {
                    at += 1;
                }
            }
            'S' => {
                if matches!(input[at], '-' | '+') {
                    negative = input[at] == '-';
                    at += 1;
                }
            }
            'L' => {
                if input[at] == '$' {
                    at += 1;
                }
            }
            _ => at += 1,
        }
    }
    if !digits.bytes().any(|b| b.is_ascii_digit()) {
        return Err("invalid input syntax for type numeric: \" \"".to_string());
    }
    let mut number: rust_decimal::Decimal = digits
        .parse()
        .map_err(|_| format!("invalid input syntax for type numeric: \"{text}\""))?;
    if negative {
        number = -number;
    }
    Ok(Value::Numeric(number))
}

/// `to_char(value, template)`: a date/time value by the date/time
/// patterns, a number by the number patterns.
pub(crate) fn to_char(value: &Value, template: &str, kind: Option<Kind>) -> Result<Value, String> {
    Ok(match value {
        Value::Null => Value::Null,
        Value::Int(i) => Value::Text(format_number(
            *i as f64,
            Some(rust_decimal::Decimal::from(*i)),
            template,
        )?),
        Value::Numeric(d) => Value::Text(format_number(
            crate::value::decimal_to_f64(d),
            Some(*d),
            template,
        )?),
        Value::Float(f) => Value::Text(format_number(*f, None, template)?),
        other => match Temporal::read(other, kind) {
            Some(temporal) => Value::Text(format_temporal(&temporal, template)),
            None => {
                return Err(format!(
                    "function to_char({}, unknown) does not exist",
                    crate::value::value_type_name(other)
                ));
            }
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(text: &str) -> Temporal {
        Temporal::classify(text).unwrap()
    }

    #[test]
    fn dates_format_by_template() {
        assert_eq!(
            format_temporal(
                &ts("2024-03-05 14:07:09.123456"),
                "YYYY-MM-DD HH24:MI:SS.US"
            ),
            "2024-03-05 14:07:09.123456"
        );
        assert_eq!(
            format_temporal(&ts("2024-03-05 14:07:09"), "HH12:MI AM Dy Mon DD YYYY"),
            "02:07 PM Tue Mar 05 2024"
        );
        assert_eq!(
            format_temporal(&ts("2024-03-05"), "Month DD, YYYY"),
            "March     05, 2024"
        );
        assert_eq!(
            format_temporal(&ts("2024-03-05"), "FMMonth FMDD, YYYY"),
            "March 5, 2024"
        );
        assert_eq!(
            format_temporal(&ts("2024-03-05"), "DDD D ID IW WW Q CC J"),
            "065 3 2 10 10 1 21 2460375"
        );
        assert_eq!(
            format_temporal(&ts("2024-03-05"), "fmDDth \"of\" month"),
            "5th of march    "
        );
        assert_eq!(
            format_temporal(&ts("2024-03-05 00:07:09"), "HH HH12 am a.m. MS SSSS"),
            "12 12 am a.m. 000 429"
        );
        assert_eq!(
            format_temporal(
                &Temporal::Interval(Interval::new(0, 1, 7_384_000_000)),
                "DD HH24:MI:SS"
            ),
            "01 02:03:04"
        );
    }

    #[test]
    fn numbers_format_by_template() {
        let n = |v: f64, t: &str| {
            format_number(v, rust_decimal::Decimal::from_f64_retain(v), t).unwrap()
        };
        assert_eq!(n(1234.5, "9999.99"), " 1234.50");
        assert_eq!(n(42.0, "000"), " 042");
        assert_eq!(n(1_234_567.891, "FM9,999,999.00"), "1,234,567.89");
        assert_eq!(n(-5.0, "999"), "  -5");
        assert_eq!(n(-1234.5, "S9999.99"), "-1234.50");
        assert_eq!(n(0.5, "0.99"), " 0.50");
        assert_eq!(n(0.5, "9.99"), "  .50");
        assert_eq!(n(12.0, "9"), " #");
        assert_eq!(n(1.5, "FM9.99"), "1.5");
        assert_eq!(n(123.0, "99999PR"), "   123 ");
        assert_eq!(n(-123.0, "99999PR"), "  <123>");
        assert_eq!(n(-123.0, "99999MI"), "  123-");
        assert_eq!(n(7.0, "FM999th"), "7th");
        assert_eq!(n(0.0, "999"), "   0");
        assert_eq!(n(1_234_567.0, "9,999"), " #,###");
        // Sign marks: anchored (`S`), at their place (`SG`, `MI`, `PL`).
        assert_eq!(n(5.0, "S999"), "  +5");
        assert_eq!(n(5.0, "SG999"), "+  5");
        assert_eq!(n(-5.0, "MI999"), "-  5");
        assert_eq!(n(5.0, "PL999"), "+   5");
        assert_eq!(n(-5.0, "999PL"), "  -5 ");
        assert_eq!(n(5.0, "FM999PL"), "5+");
        // Roman numerals, within 1 to 3999.
        assert_eq!(n(1994.0, "FMRN"), "MCMXCIV");
        assert_eq!(n(4.0, "rn"), "             iv");
        assert_eq!(n(0.0, "RN"), "###############");
        assert!(format_number(5.0, None, "PR999").is_err());
    }

    #[test]
    fn numbers_read_one_character_per_position() {
        let num = |t: &str, f: &str| parse_number(t, f).unwrap();
        let d = |s: &str| Value::Numeric(s.parse().unwrap());
        assert_eq!(num("  123  ", "999"), d("12"));
        assert_eq!(num("12,34", "9999"), d("123"));
        assert_eq!(num("1234.5678", "9999.99"), d("1234.56"));
        assert_eq!(num("-42", "99"), d("-42"));
        assert_eq!(num("12.5-", "99.9S"), d("-12.5"));
        assert_eq!(num("<42>", "99PR"), d("-42"));
        assert_eq!(num("$1,000", "L9,999"), d("1000"));
        assert_eq!(num("USD123", "AAA999"), d("123"));
        assert!(parse_number("abc", "999").is_err());
        assert!(parse_number("1.2e3", "9.9EEEE").is_err());
    }

    #[test]
    fn text_reads_by_template() {
        let date = |t: &str, f: &str| parse_by_template(t, f, true).unwrap();
        let text = |s: &str| Value::Text(s.to_string());
        assert_eq!(date("05 Mar 2024", "DD Mon YYYY"), text("2024-03-05"));
        assert_eq!(date("20240305", "YYYYMMDD"), text("2024-03-05"));
        assert_eq!(date("March 5, 2024", "Month DD, YYYY"), text("2024-03-05"));
        assert_eq!(date("2024 065", "YYYY DDD"), text("2024-03-05"));
        assert_eq!(
            parse_by_template("05/03/2024 02:30 PM", "DD/MM/YYYY HH12:MI AM", false).unwrap(),
            text("2024-03-05 14:30:00+00")
        );
        assert!(parse_by_template("2024-13-01", "YYYY-MM-DD", true).is_err());
        assert_eq!(
            parse_by_template("2024-05-06 07:08:09+02", "YYYY-MM-DD HH24:MI:SSTZH", false).unwrap(),
            text("2024-05-06 05:08:09+00")
        );
        assert_eq!(
            parse_number("1,234.5", "9,999.9").unwrap(),
            Value::Numeric("1234.5".parse().unwrap())
        );
    }
}
