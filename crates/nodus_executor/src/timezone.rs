//! Time zones for `AT TIME ZONE` and `timezone()`: UTC, fixed offsets
//! (POSIX-signed, as PostgreSQL reads `'+02'`), intervals, and named zones
//! from the system's tz database (TZif files, with their POSIX rule for
//! times past the last transition).

use chrono::{Datelike, NaiveDate, NaiveDateTime};

/// A time zone: the UTC offset (seconds east) in effect at each instant.
#[derive(Debug, Clone)]
pub(crate) enum Zone {
    Fixed(i64),
    Named(NamedZone),
}

impl Zone {
    /// Resolves a zone as PostgreSQL does: `UTC`, a POSIX offset (`'+02'`
    /// is two hours *west*), `UTC+3`-style, or a tz database name.
    pub(crate) fn resolve(name: &str) -> Result<Zone, String> {
        let trimmed = name.trim();
        if [
            "utc",
            "gmt",
            "z",
            "zulu",
            "etc/utc",
            "etc/gmt",
            "universal",
            "uct",
        ]
        .contains(&trimmed.to_ascii_lowercase().as_str())
        {
            return Ok(Zone::Fixed(0));
        }
        if let Some(offset) = posix_offset(trimmed) {
            return Ok(Zone::Fixed(offset));
        }
        NamedZone::load(trimmed)
            .map(Zone::Named)
            .ok_or_else(|| format!("time zone \"{trimmed}\" not recognized"))
    }

    /// The offset (seconds east of UTC) in effect at the UTC instant `utc`.
    pub(crate) fn offset_at_utc(&self, utc: NaiveDateTime) -> i64 {
        match self {
            Zone::Fixed(offset) => *offset,
            Zone::Named(zone) => zone.offset_at(utc.and_utc().timestamp()),
        }
    }

    /// The offset in effect at local time `local`. As PostgreSQL resolves a
    /// local time a transition skips, it takes the offset before the change;
    /// one a transition repeats, the offset after it.
    pub(crate) fn offset_at_local(&self, local: NaiveDateTime) -> i64 {
        match self {
            Zone::Fixed(offset) => *offset,
            Zone::Named(zone) => {
                let guess = local.and_utc().timestamp();
                // The offsets in effect a day either side.
                let before = zone.offset_at(guess - 86_400);
                let after = zone.offset_at(guess + 86_400);
                let gives_local = |offset: i64| zone.offset_at(guess - offset) == offset;
                match (gives_local(before), gives_local(after)) {
                    (_, true) => after,
                    (true, false) | (false, false) => before,
                }
            }
        }
    }
}

/// Whether a declared type is `time with time zone`.
pub(crate) fn is_zoned_time_type(data_type: &str) -> bool {
    let upper = data_type.to_ascii_uppercase();
    let words = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    words.starts_with("TIMETZ")
        || (words.starts_with("TIME")
            && !words.starts_with("TIMESTAMP")
            && words.ends_with("WITH TIME ZONE"))
}

/// `value AT TIME ZONE zone` (`timezone(zone, value)`): a zoned timestamp
/// (or a date) as the local time in `zone`, a local timestamp as the instant
/// it is in `zone`, and a time of day (zoned, or taken as UTC) as the zoned
/// time in `zone`. An interval zone is east of UTC. The arguments' declared
/// types, when known, tell how to read them.
pub(crate) fn at_time_zone(
    zone: &crate::Value,
    value: &crate::Value,
    zone_type: Option<&str>,
    value_type: Option<&str>,
) -> Result<crate::Value, String> {
    use crate::datetime::{Interval, Kind, Temporal};
    if matches!(zone, crate::Value::Null) || matches!(value, crate::Value::Null) {
        return Ok(crate::Value::Null);
    }
    let name = crate::render(zone);
    let interval_zone = |iv: Interval| {
        if iv.months != 0 || iv.days != 0 {
            return Err(format!(
                "interval time zone \"{}\" must not include months or days",
                iv.format()
            ));
        }
        Ok(Zone::Fixed(iv.micros / crate::datetime::MICROS_PER_SECOND))
    };
    let zone = if zone_type.and_then(Kind::of_type) == Some(Kind::Interval) {
        interval_zone(
            Interval::parse(&name)
                .ok_or_else(|| format!("invalid input syntax for type interval: \"{name}\""))?,
        )?
    } else {
        match Zone::resolve(&name) {
            Ok(zone) => zone,
            Err(error) => match (name.contains(':')
                || name.chars().any(|c| c.is_ascii_alphabetic()))
            .then(|| Interval::parse(&name))
            .flatten()
            {
                Some(iv) => interval_zone(iv)?,
                None => return Err(error),
            },
        }
    };
    let seconds = chrono::Duration::seconds;
    // A time of day, in `zone` as of now.
    let zoned_time = |micros: i64, offset: i64| {
        let now = chrono::Utc::now().naive_utc();
        let target = zone.offset_at_utc(now);
        let local = (micros + (target - offset) * crate::datetime::MICROS_PER_SECOND)
            .rem_euclid(crate::datetime::MICROS_PER_DAY);
        crate::Value::Text(format!(
            "{}{}",
            crate::datetime::format_time(local),
            offset_text(target)
        ))
    };
    if value_type.is_some_and(is_zoned_time_type) {
        let text = crate::render(value);
        let (micros, offset) = parse_zoned_time(&text).ok_or_else(|| {
            format!("invalid input syntax for type time with time zone: \"{text}\"")
        })?;
        return Ok(zoned_time(micros, offset));
    }
    let kind = value_type.and_then(Kind::of_type);
    Ok(match Temporal::read(value, kind) {
        Some(Temporal::TimestampTz(utc)) => {
            Temporal::Timestamp(utc + seconds(zone.offset_at_utc(utc))).to_value()
        }
        Some(Temporal::Date(date)) => {
            let utc = date.and_hms_opt(0, 0, 0).unwrap_or_default();
            Temporal::Timestamp(utc + seconds(zone.offset_at_utc(utc))).to_value()
        }
        Some(Temporal::Timestamp(local)) => {
            Temporal::TimestampTz(local - seconds(zone.offset_at_local(local))).to_value()
        }
        Some(Temporal::Time(micros)) => zoned_time(micros, 0),
        Some(Temporal::Infinite(kind, negative)) => Temporal::Infinite(
            if kind == Kind::Timestamp {
                Kind::TimestampTz
            } else {
                Kind::Timestamp
            },
            negative,
        )
        .to_value(),
        _ => {
            return Err(format!(
                "function timezone(unknown, {}) does not exist",
                crate::value::value_type_name(value)
            ));
        }
    })
}

/// A UTC offset as PostgreSQL shows it: `+09`, `-04`, `+05:30`.
fn offset_text(offset: i64) -> String {
    let sign = if offset < 0 { '-' } else { '+' };
    let (hours, rest) = (offset.abs() / 3600, offset.abs() % 3600);
    match (rest / 60, rest % 60) {
        (0, 0) => format!("{sign}{hours:02}"),
        (minutes, 0) => format!("{sign}{hours:02}:{minutes:02}"),
        (minutes, secs) => format!("{sign}{hours:02}:{minutes:02}:{secs:02}"),
    }
}

/// `HH:MM[:SS[.f]]±HH[:MM]`: a zoned time's microseconds and offset.
fn parse_zoned_time(text: &str) -> Option<(i64, i64)> {
    let text = text.trim();
    let at = text.rfind(['+', '-'])?;
    let micros = crate::datetime::time_micros(&text[..at])?;
    let sign = if text[at..].starts_with('-') { -1 } else { 1 };
    let mut parts = text[at + 1..].split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next().map_or(Some(0), |m| m.parse().ok())?;
    Some((micros, sign * (hours * 3600 + minutes * 60)))
}

/// A POSIX-style offset (`+02`, `-05:30`, `UTC+3`, `GMT-1`) as seconds
/// east of UTC: the sign is inverted, as in POSIX.
fn posix_offset(name: &str) -> Option<i64> {
    let upper = name.to_ascii_uppercase();
    let rest = ["UTC", "GMT"]
        .iter()
        .find_map(|p| upper.strip_prefix(p))
        .unwrap_or(&upper);
    let (sign, digits) = match rest.as_bytes().first()? {
        b'+' => (1, &rest[1..]),
        b'-' => (-1, &rest[1..]),
        b'0'..=b'9' if rest.len() == upper.len() => (1, rest),
        _ => return None,
    };
    let mut parts = digits.split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next().map_or(Some(0), |m| m.parse().ok())?;
    let seconds: i64 = parts.next().map_or(Some(0), |s| s.parse().ok())?;
    if hours > 168 || minutes >= 60 || seconds >= 60 {
        return None;
    }
    Some(-sign * (hours * 3600 + minutes * 60 + seconds))
}

/// A zone of the tz database.
#[derive(Debug, Clone)]
pub(crate) struct NamedZone {
    /// Each transition's instant (Unix seconds) and the offset from then on.
    transitions: Vec<(i64, i64)>,
    /// The offset before the first transition.
    initial: i64,
    /// The rule for instants after the last transition.
    rule: Option<PosixRule>,
}

impl NamedZone {
    fn load(name: &str) -> Option<NamedZone> {
        if name.is_empty()
            || name.starts_with('/')
            || name.split('/').any(|part| part == ".." || part.is_empty())
        {
            return None;
        }
        let directories = std::env::var("TZDIR").ok().into_iter().chain([
            "/usr/share/zoneinfo".to_string(),
            "/usr/lib/zoneinfo".to_string(),
        ]);
        for directory in directories {
            let path = std::path::Path::new(&directory).join(name);
            if let Ok(bytes) = std::fs::read(&path) {
                return parse_tzif(&bytes);
            }
            // Names are matched regardless of case (`europe/berlin`).
            if let Some(found) = find_case_insensitive(std::path::Path::new(&directory), name)
                && let Ok(bytes) = std::fs::read(found)
            {
                return parse_tzif(&bytes);
            }
        }
        None
    }

    fn offset_at(&self, instant: i64) -> i64 {
        match self.transitions.partition_point(|(at, _)| *at <= instant) {
            0 => self.initial,
            n if n == self.transitions.len() => match &self.rule {
                Some(rule) => rule.offset_at(instant),
                None => self.transitions[n - 1].1,
            },
            n => self.transitions[n - 1].1,
        }
    }
}

fn find_case_insensitive(directory: &std::path::Path, name: &str) -> Option<std::path::PathBuf> {
    let mut path = directory.to_path_buf();
    for part in name.split('/') {
        let entry = std::fs::read_dir(&path)
            .ok()?
            .filter_map(Result::ok)
            .find(|e| e.file_name().to_string_lossy().eq_ignore_ascii_case(part))?;
        path = entry.path();
    }
    Some(path)
}

/// Reads a TZif file (version 1, or the 64-bit data of version 2 and on,
/// with its POSIX rule footer).
fn parse_tzif(bytes: &[u8]) -> Option<NamedZone> {
    let header = |at: usize| -> Option<(u8, [usize; 6])> {
        if bytes.get(at..at + 4)? != b"TZif" {
            return None;
        }
        let version = *bytes.get(at + 4)?;
        let mut counts = [0usize; 6];
        for (i, count) in counts.iter_mut().enumerate() {
            let start = at + 20 + i * 4;
            *count = u32::from_be_bytes(bytes.get(start..start + 4)?.try_into().ok()?) as usize;
        }
        Some((version, counts))
    };
    let (version, counts) = header(0)?;
    // isutcnt, isstdcnt, leapcnt, timecnt, typecnt, charcnt.
    let v1_len = |c: [usize; 6], time_size: usize| {
        c[3] * time_size + c[3] + c[4] * 6 + c[5] + c[2] * (time_size + 4) + c[1] + c[0]
    };
    let (start, counts, time_size) = if version >= b'2' {
        let second = 44 + v1_len(counts, 4);
        let (_, counts2) = header(second)?;
        (second + 44, counts2, 8)
    } else {
        (44, counts, 4)
    };
    let [_, _, _, timecnt, typecnt, _] = counts;
    let times_at = start;
    let indexes_at = times_at + timecnt * time_size;
    let types_at = indexes_at + timecnt;
    let read_time = |i: usize| -> Option<i64> {
        let at = times_at + i * time_size;
        Some(if time_size == 8 {
            i64::from_be_bytes(bytes.get(at..at + 8)?.try_into().ok()?)
        } else {
            i32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?) as i64
        })
    };
    let type_offset = |t: usize| -> Option<i64> {
        let at = types_at + t * 6;
        Some(i32::from_be_bytes(bytes.get(at..at + 4)?.try_into().ok()?) as i64)
    };
    let mut transitions = Vec::with_capacity(timecnt);
    for i in 0..timecnt {
        let index = *bytes.get(indexes_at + i)? as usize;
        if index >= typecnt {
            return None;
        }
        transitions.push((read_time(i)?, type_offset(index)?));
    }
    let initial = type_offset(0).unwrap_or(0);
    let rule = if version >= b'2' {
        let footer_at = start + v1_len(counts, time_size);
        let footer = bytes.get(footer_at..)?;
        let text = std::str::from_utf8(footer).ok()?.trim_matches('\n');
        let text = text.split('\n').next().unwrap_or_default();
        PosixRule::parse(text)
    } else {
        None
    };
    Some(NamedZone {
        transitions,
        initial,
        rule,
    })
}

/// A POSIX TZ rule (`CET-1CEST,M3.5.0,M10.5.0/3`): a standard offset and,
/// optionally, a daylight offset with the dates it starts and ends.
#[derive(Debug, Clone)]
struct PosixRule {
    standard: i64,
    daylight: Option<(i64, RuleDate, RuleDate)>,
}

/// When a daylight period starts or ends: a day of the year and the local
/// time (seconds) of the change.
#[derive(Debug, Clone)]
enum RuleDate {
    /// `Mm.w.d`: day `d` (0 = Sunday) of week `w` (5 = last) of month `m`.
    MonthWeekDay(u32, u32, u32, i64),
    /// `Jn`: day `n` (1..365), February 29th never counted.
    Julian(u32, i64),
    /// `n`: day `n` (0..365), February 29th counted.
    Ordinal(u32, i64),
}

impl PosixRule {
    fn parse(text: &str) -> Option<PosixRule> {
        let mut rest = text;
        skip_name(&mut rest)?;
        let standard = -parse_rule_offset(&mut rest)?;
        if rest.is_empty() {
            return Some(PosixRule {
                standard,
                daylight: None,
            });
        }
        skip_name(&mut rest)?;
        let daylight = if rest.starts_with(',') || rest.is_empty() {
            standard + 3600
        } else {
            -parse_rule_offset(&mut rest)?
        };
        let rest = rest.strip_prefix(',')?;
        let (start, end) = rest.split_once(',')?;
        Some(PosixRule {
            standard,
            daylight: Some((daylight, RuleDate::parse(start)?, RuleDate::parse(end)?)),
        })
    }

    fn offset_at(&self, instant: i64) -> i64 {
        let Some((daylight, start, end)) = &self.daylight else {
            return self.standard;
        };
        let year =
            chrono::DateTime::from_timestamp(instant + self.standard, 0).map_or(1970, |d| d.year());
        // The changes' instants: the start in standard time, the end in
        // daylight time.
        let starts = start.instant(year) - self.standard;
        let ends = end.instant(year) - daylight;
        let in_daylight = if starts < ends {
            instant >= starts && instant < ends
        } else {
            // A southern-hemisphere rule spans the new year.
            instant >= starts || instant < ends
        };
        if in_daylight {
            *daylight
        } else {
            self.standard
        }
    }
}

impl RuleDate {
    fn parse(text: &str) -> Option<RuleDate> {
        let (date, time) = match text.split_once('/') {
            Some((date, time)) => (date, parse_rule_time(time)?),
            None => (text, 7200),
        };
        if let Some(mwd) = date.strip_prefix('M') {
            let mut parts = mwd.split('.');
            let month = parts.next()?.parse().ok()?;
            let week = parts.next()?.parse().ok()?;
            let day = parts.next()?.parse().ok()?;
            Some(RuleDate::MonthWeekDay(month, week, day, time))
        } else if let Some(julian) = date.strip_prefix('J') {
            Some(RuleDate::Julian(julian.parse().ok()?, time))
        } else {
            Some(RuleDate::Ordinal(date.parse().ok()?, time))
        }
    }

    /// The local instant (Unix seconds, as if UTC) of the change in `year`.
    fn instant(&self, year: i32) -> i64 {
        let midnight = |date: Option<NaiveDate>| {
            date.and_then(|d| d.and_hms_opt(0, 0, 0))
                .map_or(0, |d| d.and_utc().timestamp())
        };
        match *self {
            RuleDate::MonthWeekDay(month, week, weekday, time) => {
                let first = NaiveDate::from_ymd_opt(year, month, 1);
                let Some(first) = first else { return 0 };
                let offset =
                    (7 + weekday as i64 - first.weekday().num_days_from_sunday() as i64) % 7;
                let mut day = 1 + offset + (week as i64 - 1) * 7;
                let days_in_month = (first
                    .checked_add_months(chrono::Months::new(1))
                    .unwrap_or(first)
                    - first)
                    .num_days();
                while day > days_in_month {
                    day -= 7;
                }
                midnight(NaiveDate::from_ymd_opt(year, month, day as u32)) + time
            }
            RuleDate::Julian(n, time) => {
                let leap = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
                let ordinal = if leap && n >= 60 { n + 1 } else { n };
                midnight(NaiveDate::from_yo_opt(year, ordinal)) + time
            }
            RuleDate::Ordinal(n, time) => midnight(NaiveDate::from_yo_opt(year, n + 1)) + time,
        }
    }
}

/// Skips a zone abbreviation: letters, or `<...>`.
fn skip_name(rest: &mut &str) -> Option<()> {
    if let Some(quoted) = rest.strip_prefix('<') {
        let end = quoted.find('>')?;
        *rest = &quoted[end + 1..];
        return Some(());
    }
    let end = rest
        .find(|c: char| !c.is_ascii_alphabetic())
        .unwrap_or(rest.len());
    if end < 3 {
        return None;
    }
    *rest = &rest[end..];
    Some(())
}

/// A rule's `[+-]hh[:mm[:ss]]` offset (west positive) as seconds.
fn parse_rule_offset(rest: &mut &str) -> Option<i64> {
    let end = rest
        .find(|c: char| !(c.is_ascii_digit() || c == ':' || c == '+' || c == '-'))
        .unwrap_or(rest.len());
    let value = parse_rule_time(&rest[..end])?;
    *rest = &rest[end..];
    Some(value)
}

/// `[+-]hh[:mm[:ss]]` as seconds.
fn parse_rule_time(text: &str) -> Option<i64> {
    let (sign, body) = match text.as_bytes().first()? {
        b'-' => (-1, &text[1..]),
        b'+' => (1, &text[1..]),
        _ => (1, text),
    };
    let mut parts = body.split(':');
    let hours: i64 = parts.next()?.parse().ok()?;
    let minutes: i64 = parts.next().map_or(Some(0), |m| m.parse().ok())?;
    let seconds: i64 = parts.next().map_or(Some(0), |s| s.parse().ok())?;
    Some(sign * (hours * 3600 + minutes * 60 + seconds))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").unwrap()
    }

    #[test]
    fn fixed_offsets_read_posix_signs() {
        assert_eq!(posix_offset("+02"), Some(-7200));
        assert_eq!(posix_offset("-05:30"), Some(19_800));
        assert_eq!(posix_offset("UTC+3"), Some(-10_800));
        assert!(posix_offset("Europe/Berlin").is_none());
    }

    #[test]
    fn posix_rules_switch_on_their_dates() {
        let rule = PosixRule::parse("CET-1CEST,M3.5.0,M10.5.0/3").unwrap();
        let instant = |t: &str| at(t).and_utc().timestamp();
        assert_eq!(rule.offset_at(instant("2030-01-15 12:00:00")), 3600);
        assert_eq!(rule.offset_at(instant("2030-07-15 12:00:00")), 7200);
        // Daylight time starts at 01:00 UTC on the last Sunday of March.
        assert_eq!(rule.offset_at(instant("2030-03-31 00:59:59")), 3600);
        assert_eq!(rule.offset_at(instant("2030-03-31 01:00:00")), 7200);
    }

    #[test]
    fn named_zones_come_from_the_tz_database() {
        let Ok(zone) = Zone::resolve("Europe/Berlin") else {
            // No tz database here.
            return;
        };
        assert_eq!(zone.offset_at_utc(at("2024-03-01 10:00:00")), 3600);
        assert_eq!(zone.offset_at_utc(at("2024-07-01 10:00:00")), 7200);
        assert_eq!(zone.offset_at_local(at("2024-07-01 12:00:00")), 7200);
        // A skipped local time takes the offset before the change, and a
        // repeated one the offset after it.
        assert_eq!(zone.offset_at_local(at("2024-03-31 02:30:00")), 3600);
        assert_eq!(zone.offset_at_local(at("2024-10-27 02:30:00")), 3600);
        assert!(Zone::resolve("Nowhere/Nothing").is_err());
    }

    #[test]
    fn zones_apply_by_the_argument_types() {
        let t = |s: &str| crate::Value::Text(s.to_string());
        // An interval zone is east of UTC; a POSIX one west.
        assert_eq!(
            at_time_zone(
                &t("05:30:00"),
                &t("2024-01-01 00:00:00+00"),
                Some("INTERVAL"),
                None
            )
            .unwrap(),
            t("2024-01-01 05:30:00")
        );
        assert_eq!(
            at_time_zone(&t("UTC+3"), &t("2024-01-01 12:00:00+00"), None, None).unwrap(),
            t("2024-01-01 09:00:00")
        );
        // A time of day becomes a zoned time.
        assert_eq!(
            at_time_zone(&t("+02"), &t("10:00:00"), None, Some("TIME")).unwrap(),
            t("08:00:00-02")
        );
        assert_eq!(
            at_time_zone(&t("UTC"), &t("10:00:00+02"), None, Some("TIMETZ")).unwrap(),
            t("08:00:00+00")
        );
        assert!(at_time_zone(&t("1 mon"), &t("2024-01-01"), Some("INTERVAL"), None).is_err());
    }
}
