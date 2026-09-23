//! Replica text for Postgres temporal values, identical whether it is
//! rendered from the binary send format or parsed from Postgres's text
//! output. Years run from 1 to 9999.

#[cfg(any(feature = "wal2json", feature = "maxwell", feature = "pg-walstream"))]
mod text;

use alloc::string::{String, ToString};
use core::fmt::Write;

use super::error::DecodeError;
use crate::encoding::Value;
#[cfg(any(feature = "wal2json", feature = "maxwell", feature = "pg-walstream"))]
pub(crate) use text::decode_temporal_text;
#[cfg(feature = "maxwell")]
pub(crate) use text::decode_utc_timestamptz_text;

const USECS_PER_SEC: i64 = 1_000_000;
const USECS_PER_MINUTE: i64 = 60 * USECS_PER_SEC;
const USECS_PER_HOUR: i64 = 60 * USECS_PER_MINUTE;
const USECS_PER_DAY: i64 = 24 * USECS_PER_HOUR;
/// Days from 1970-01-01 to 2000-01-01, the Postgres epoch.
const PG_EPOCH_UNIX_DAYS: i64 = 10_957;
/// Largest UTC offset magnitude Postgres accepts, 15:59:59.
const MAX_OFFSET_SECS: i64 = 16 * 3600 - 1;
const UTC_SUFFIX: &str = "+00:00";

/// Which temporal type a decoder renders.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Temporal {
    Date,
    /// `time`, or `timetz` when the value carries an offset.
    Time,
    Timestamp,
    TimestampTz,
    Interval,
}

impl Temporal {
    fn name(self) -> &'static str {
        match self {
            Temporal::Date => "date",
            Temporal::Time => "time",
            Temporal::Timestamp => "timestamp",
            Temporal::TimestampTz => "timestamptz",
            Temporal::Interval => "interval",
        }
    }

    fn expected_binary(self) -> &'static str {
        match self {
            Temporal::Date => "date binary (4 bytes)",
            Temporal::Time => "time or timetz binary (8 or 12 bytes) within one day",
            Temporal::Timestamp => "timestamp binary (8 bytes)",
            Temporal::TimestampTz => "timestamptz binary (8 bytes)",
            Temporal::Interval => "interval binary (16 bytes)",
        }
    }
}

/// Why a value has no replica text.
enum Failure {
    Malformed,
    Unrepresentable,
}

/// Render a temporal value's binary send format as replica text.
///
/// # Errors
///
/// [`DecodeError::WrongPayloadKind`] for bytes Postgres would not send,
/// [`DecodeError::UnrepresentableTemporal`] for a value outside the
/// replica form.
pub(crate) fn decode_temporal_binary<S, B>(
    column: &str,
    kind: Temporal,
    bytes: &[u8],
) -> Result<Value<S, B>, DecodeError>
where
    S: From<String>,
{
    let rendered = match kind {
        Temporal::Date => date_from_binary(bytes),
        Temporal::Time => time_from_binary(bytes),
        Temporal::Timestamp => timestamp_from_binary(bytes, false),
        Temporal::TimestampTz => timestamp_from_binary(bytes, true),
        Temporal::Interval => interval_from_binary(bytes),
    };
    match rendered {
        Ok(s) => Ok(Value::Text(S::from(s))),
        Err(Failure::Malformed) => Err(DecodeError::WrongPayloadKind {
            column: column.to_string(),
            expected: kind.expected_binary(),
            actual: "other binary contents",
        }),
        Err(Failure::Unrepresentable) => {
            let mut value = String::with_capacity(kind.name().len() + 8 + 2 * bytes.len());
            write!(value, "{} binary ", kind.name()).unwrap();
            for byte in bytes {
                write!(value, "{byte:02x}").unwrap();
            }
            Err(DecodeError::UnrepresentableTemporal {
                column: column.to_string(),
                value,
            })
        }
    }
}

// ------------------------------------------------------------------
// Rendering
// ------------------------------------------------------------------

/// Proleptic Gregorian `(year, month, day)` of a day count from the Unix
/// epoch.
fn civil_from_days(unix_days: i64) -> (i64, i64, i64) {
    let z = unix_days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Append `YYYY-MM-DD` for a day count from the Postgres epoch.
fn push_date(out: &mut String, pg_days: i64) -> Result<(), Failure> {
    let (year, month, day) = civil_from_days(pg_days + PG_EPOCH_UNIX_DAYS);
    if !(1..=9999).contains(&year) {
        return Err(Failure::Unrepresentable);
    }
    write!(out, "{year:04}-{month:02}-{day:02}").unwrap();
    Ok(())
}

/// Append `HH:MM:SS.ffffff` for microseconds within one day.
fn push_time_of_day(out: &mut String, micros: i64) {
    let secs = micros / USECS_PER_SEC;
    let frac = micros % USECS_PER_SEC;
    write!(
        out,
        "{:02}:{:02}:{:02}.{frac:06}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60
    )
    .unwrap();
}

fn render_date(pg_days: i64) -> Result<String, Failure> {
    let mut out = String::with_capacity(10);
    push_date(&mut out, pg_days)?;
    Ok(out)
}

fn render_timestamp(pg_micros: i64, utc: bool) -> Result<String, Failure> {
    let mut out = String::with_capacity(32);
    push_date(&mut out, pg_micros.div_euclid(USECS_PER_DAY))?;
    out.push(' ');
    push_time_of_day(&mut out, pg_micros.rem_euclid(USECS_PER_DAY));
    if utc {
        out.push_str(UTC_SUFFIX);
    }
    Ok(out)
}

fn render_time(micros: i64) -> Result<String, Failure> {
    if micros == USECS_PER_DAY {
        return Err(Failure::Unrepresentable);
    }
    let mut out = String::with_capacity(15);
    push_time_of_day(&mut out, micros);
    Ok(out)
}

/// `micros` is the local time of day, `offset_secs` the zone east of UTC.
fn render_timetz(micros: i64, offset_secs: i64) -> String {
    let mut out = String::with_capacity(21);
    push_time_of_day(
        &mut out,
        (micros - offset_secs * USECS_PER_SEC).rem_euclid(USECS_PER_DAY),
    );
    out.push_str(UTC_SUFFIX);
    out
}

/// Append `value` and its ISO 8601 designator unless `value` is zero.
fn push_part(out: &mut String, value: i64, unit: char) {
    if value != 0 {
        write!(out, "{value}{unit}").unwrap();
    }
}

/// Postgres's `EncodeInterval` under `IntervalStyle = iso_8601`.
fn render_interval(months: i32, days: i32, time: i64) -> Result<String, Failure> {
    let infinite = (months, days, time) == (i32::MAX, i32::MAX, i64::MAX)
        || (months, days, time) == (i32::MIN, i32::MIN, i64::MIN);
    if infinite {
        return Err(Failure::Unrepresentable);
    }
    let (years, months) = (months / 12, months % 12);
    let hours = time / USECS_PER_HOUR;
    let minutes = time % USECS_PER_HOUR / USECS_PER_MINUTE;
    let secs = time % USECS_PER_MINUTE / USECS_PER_SEC;
    let usecs = time % USECS_PER_SEC;
    if years == 0 && months == 0 && days == 0 && time == 0 {
        return Ok(String::from("PT0S"));
    }
    let mut out = String::from("P");
    push_part(&mut out, i64::from(years), 'Y');
    push_part(&mut out, i64::from(months), 'M');
    push_part(&mut out, i64::from(days), 'D');
    if time != 0 {
        out.push('T');
    }
    push_part(&mut out, hours, 'H');
    push_part(&mut out, minutes, 'M');
    if secs != 0 || usecs != 0 {
        if secs < 0 || usecs < 0 {
            out.push('-');
        }
        write!(out, "{}", secs.abs()).unwrap();
        if usecs != 0 {
            let frac_start = out.len() + 1;
            write!(out, ".{:06}", usecs.abs()).unwrap();
            let kept = out[frac_start..].trim_end_matches('0').len();
            out.truncate(frac_start + kept);
        }
        out.push('S');
    }
    Ok(out)
}

// ------------------------------------------------------------------
// Binary send format
// ------------------------------------------------------------------

fn date_from_binary(bytes: &[u8]) -> Result<String, Failure> {
    let days = i32::from_be_bytes(bytes.try_into().map_err(|_| Failure::Malformed)?);
    render_date(i64::from(days))
}

fn timestamp_from_binary(bytes: &[u8], utc: bool) -> Result<String, Failure> {
    let micros = i64::from_be_bytes(bytes.try_into().map_err(|_| Failure::Malformed)?);
    render_timestamp(micros, utc)
}

/// `time` is 8 bytes of microseconds, `timetz` adds 4 bytes of zone
/// seconds west of UTC.
fn time_from_binary(bytes: &[u8]) -> Result<String, Failure> {
    let (micros, zone) = match bytes.len() {
        8 => (bytes, None),
        12 => {
            let (micros, zone) = bytes.split_at(8);
            (micros, Some(i32::from_be_bytes(zone.try_into().unwrap())))
        }
        _ => return Err(Failure::Malformed),
    };
    let micros = i64::from_be_bytes(micros.try_into().unwrap());
    if !(0..=USECS_PER_DAY).contains(&micros) {
        return Err(Failure::Malformed);
    }
    match zone {
        None => render_time(micros),
        Some(west) if i64::from(west).abs() <= MAX_OFFSET_SECS => {
            Ok(render_timetz(micros, -i64::from(west)))
        }
        Some(_) => Err(Failure::Malformed),
    }
}

/// Microseconds, then days, then months.
fn interval_from_binary(bytes: &[u8]) -> Result<String, Failure> {
    let bytes: &[u8; 16] = bytes.try_into().map_err(|_| Failure::Malformed)?;
    let time = i64::from_be_bytes(bytes[..8].try_into().unwrap());
    let days = i32::from_be_bytes(bytes[8..12].try_into().unwrap());
    let months = i32::from_be_bytes(bytes[12..].try_into().unwrap());
    render_interval(months, days, time)
}
