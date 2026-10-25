//! Oracle DATE and TIMESTAMP encoding and decoding
//!
//! Oracle DATE format (7 bytes):
//! - Byte 0: Century (value + 100)
//! - Byte 1: Year in century (value + 100)
//! - Byte 2: Month (1-12)
//! - Byte 3: Day (1-31)
//! - Byte 4: Hour + 1 (1-24)
//! - Byte 5: Minute + 1 (1-60)
//! - Byte 6: Second + 1 (1-60)
//!
//! Oracle TIMESTAMP adds (4 more bytes):
//! - Bytes 7-10: Fractional seconds (nanoseconds as big-endian u32)
//!
//! Oracle TIMESTAMP WITH TIME ZONE adds (2 more bytes):
//! - Byte 11: Time zone hour offset + 20
//! - Byte 12: Time zone minute offset + 60

use crate::error::{Error, Result};

/// Decoded Oracle DATE
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleDate {
    /// Year (e.g., 2024)
    pub year: i32,
    /// Month (1-12)
    pub month: u8,
    /// Day (1-31)
    pub day: u8,
    /// Hour (0-23)
    pub hour: u8,
    /// Minute (0-59)
    pub minute: u8,
    /// Second (0-59)
    pub second: u8,
}

impl OracleDate {
    /// Create a new Oracle date
    pub fn new(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Self {
        Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
        }
    }

    /// Create a date-only value (time set to 00:00:00)
    pub fn date(year: i32, month: u8, day: u8) -> Self {
        Self::new(year, month, day, 0, 0, 0)
    }

    /// Encode to Oracle wire format (7 bytes)
    ///
    /// Plenora fork: total and exact. A date outside Oracle's calendar
    /// (years -4712..=9999 without year 0, Julian up to 1582-10-04,
    /// Gregorian from 1582-10-15) or a time field out of range is refused:
    /// the upstream `as u8` casts stored year 25601 as 0001 and panicked on
    /// negative years, and the server does not validate binary DATE bytes.
    pub fn to_oracle_bytes(&self) -> Result<[u8; 7]> {
        validate_fields(
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
        )?;
        // Oracle stores century and year-of-century with truncating division,
        // both biased by 100: -4712 is (53, 88), -1 is (100, 99). Rust `/` and
        // `%` truncate as well, and the validated range keeps both in 1..=199.
        let century = wire_byte(self.year / 100 + 100)?;
        let year_in_century = wire_byte(self.year % 100 + 100)?;
        Ok([
            century,
            year_in_century,
            self.month,
            self.day,
            self.hour + 1,
            self.minute + 1,
            self.second + 1,
        ])
    }
}

impl Default for OracleDate {
    fn default() -> Self {
        Self::new(1, 1, 1, 0, 0, 0)
    }
}

/// Decoded Oracle TIMESTAMP (with optional timezone)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OracleTimestamp {
    /// Year (e.g., 2024)
    pub year: i32,
    /// Month (1-12)
    pub month: u8,
    /// Day (1-31)
    pub day: u8,
    /// Hour (0-23)
    pub hour: u8,
    /// Minute (0-59)
    pub minute: u8,
    /// Second (0-59)
    pub second: u8,
    /// Fractional seconds in microseconds (0-999999)
    pub microsecond: u32,
    /// Timezone hour offset (-12 to +14)
    pub tz_hour_offset: i8,
    /// Timezone minute offset (-59 to +59)
    pub tz_minute_offset: i8,
}

impl OracleTimestamp {
    /// Create a new timestamp without timezone
    pub fn new(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    ) -> Self {
        Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            tz_hour_offset: 0,
            tz_minute_offset: 0,
        }
    }

    /// Create a timestamp with timezone offset
    pub fn with_timezone(
        year: i32,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
        tz_hour_offset: i8,
        tz_minute_offset: i8,
    ) -> Self {
        Self {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
            tz_hour_offset,
            tz_minute_offset,
        }
    }

    /// Check if this timestamp has a timezone
    pub fn has_timezone(&self) -> bool {
        self.tz_hour_offset != 0 || self.tz_minute_offset != 0
    }

    /// Convert to OracleDate (loses fractional seconds and timezone)
    pub fn to_date(&self) -> OracleDate {
        OracleDate::new(
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
        )
    }

    /// Encode to Oracle wire format (11 bytes for TIMESTAMP)
    ///
    /// Plenora fork: the date part is validated as in
    /// [`OracleDate::to_oracle_bytes`] and the fraction must be below one
    /// second (a chrono leap second carries 1_000_000 microseconds or more,
    /// which used to reach the wire as an invalid fraction).
    pub fn to_oracle_bytes(&self) -> Result<[u8; 11]> {
        let date_bytes = self.to_date().to_oracle_bytes()?;
        if self.microsecond >= 1_000_000 {
            return Err(Error::DataConversionError(
                "Oracle TIMESTAMP fraction out of range".to_string(),
            ));
        }
        let nanos = self.microsecond * 1000;
        let nano_bytes = nanos.to_be_bytes();

        Ok([
            date_bytes[0],
            date_bytes[1],
            date_bytes[2],
            date_bytes[3],
            date_bytes[4],
            date_bytes[5],
            date_bytes[6],
            nano_bytes[0],
            nano_bytes[1],
            nano_bytes[2],
            nano_bytes[3],
        ])
    }
}

impl Default for OracleTimestamp {
    fn default() -> Self {
        Self::new(1, 1, 1, 0, 0, 0, 0)
    }
}

impl From<OracleDate> for OracleTimestamp {
    fn from(date: OracleDate) -> Self {
        Self::new(
            date.year,
            date.month,
            date.day,
            date.hour,
            date.minute,
            date.second,
            0,
        )
    }
}

/// Timezone hour offset constant
const TZ_HOUR_OFFSET: i8 = 20;
/// Timezone minute offset constant
const TZ_MINUTE_OFFSET: i8 = 60;
/// Flag indicating named timezone (not supported)
const HAS_REGION_ID: u8 = 0x80;

/// Earliest year of Oracle's calendar (4712 BC, Julian day 0).
pub const ORACLE_MIN_YEAR: i32 = -4712;
/// Latest year of Oracle's calendar.
pub const ORACLE_MAX_YEAR: i32 = 9999;

/// Whether `year` (Oracle numbering: no year 0, -1 is 1 BC) is a leap year
/// in Oracle's calendar: Julian up to 1582, Gregorian afterwards. Measured on
/// Oracle 23ai with `VALIDATE_CONVERSION` on every February 29: the Julian
/// rule applies to Oracle's own year number (-4 and -4708 are leap years, -1
/// and -5 are not), except for the first year, -4712, which is not.
fn is_oracle_leap_year(year: i32) -> bool {
    if year > 1582 {
        (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
    } else {
        year % 4 == 0 && year != ORACLE_MIN_YEAR
    }
}

/// Days of `month` in `year` of Oracle's calendar (0 for an invalid month).
fn days_in_month(year: i32, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_oracle_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Whether a calendar date exists in Oracle's calendar.
pub fn is_oracle_calendar_date(year: i32, month: u8, day: u8) -> bool {
    (ORACLE_MIN_YEAR..=ORACLE_MAX_YEAR).contains(&year)
        && year != 0
        && day >= 1
        && day <= days_in_month(year, month)
        // The Julian-to-Gregorian switch skips 1582-10-05..=1582-10-14.
        && !(year == 1582 && month == 10 && (5..=14).contains(&day))
}

/// Every field inside Oracle's calendar and clock, or an explicit error.
fn validate_fields(year: i32, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> Result<()> {
    if !is_oracle_calendar_date(year, month, day) {
        return Err(Error::DataConversionError(
            "date outside the Oracle calendar".to_string(),
        ));
    }
    if hour > 23 || minute > 59 || second > 59 {
        return Err(Error::DataConversionError(
            "time of day outside the Oracle range".to_string(),
        ));
    }
    Ok(())
}

fn wire_byte(value: i32) -> Result<u8> {
    u8::try_from(value)
        .map_err(|_| Error::DataConversionError("date outside the Oracle calendar".to_string()))
}

/// Decode an Oracle DATE from wire format bytes (7 bytes)
///
/// Plenora fork: exactly seven bytes, and accepted only when re-encoding the
/// decoded fields gives the same bytes back. The upstream decoder read any
/// prefix and turned a zero time byte into 0 with `saturating_sub`.
pub fn decode_oracle_date(data: &[u8]) -> Result<OracleDate> {
    let data: &[u8; 7] = data.try_into().map_err(|_| {
        Error::DataConversionError("Oracle DATE requires exactly 7 bytes".to_string())
    })?;
    let invalid = || Error::DataConversionError("invalid Oracle DATE bytes".to_string());
    let century = i32::from(data[0]) - 100;
    let year_in_century = i32::from(data[1]) - 100;
    let date = OracleDate {
        year: century * 100 + year_in_century,
        month: data[2],
        day: data[3],
        hour: data[4].checked_sub(1).ok_or_else(invalid)?,
        minute: data[5].checked_sub(1).ok_or_else(invalid)?,
        second: data[6].checked_sub(1).ok_or_else(invalid)?,
    };
    if date.to_oracle_bytes().map_err(|_| invalid())? != *data {
        return Err(invalid());
    }
    Ok(date)
}

/// Decode an Oracle TIMESTAMP from wire format bytes
///
/// Handles DATE (7 bytes), TIMESTAMP (11 bytes), and TIMESTAMP WITH TIME ZONE (13 bytes)
///
/// Plenora fork: only the three wire lengths are accepted, the fraction must
/// be a whole number of microseconds below one second, and the offset of a
/// TIMESTAMP WITH TIME ZONE must be Oracle's (-12..=14 hours) or a refused
/// region id: a region whose low byte is zero used to read as UTC.
pub fn decode_oracle_timestamp(data: &[u8]) -> Result<OracleTimestamp> {
    if !matches!(data.len(), 7 | 11 | 13) {
        return Err(Error::DataConversionError(
            "Oracle TIMESTAMP requires 7, 11 or 13 bytes".to_string(),
        ));
    }

    let date = decode_oracle_date(&data[..7])?;

    // Fractional seconds (bytes 7-10). Il tipo porta il microsecondo: un
    // valore con nanosecondi non si tronca in silenzio, si rifiuta.
    let fsecond = if data.len() >= 11 {
        let nanos = u32::from_be_bytes([data[7], data[8], data[9], data[10]]);
        if nanos % 1000 != 0 {
            return Err(Error::DataConversionError(
                "Oracle TIMESTAMP oltre il microsecondo".to_string(),
            ));
        }
        nanos / 1000
    } else {
        0
    };

    if fsecond >= 1_000_000 {
        return Err(Error::DataConversionError(
            "Oracle TIMESTAMP fraction out of range".to_string(),
        ));
    }

    // Timezone (bytes 11-12)
    let (tz_hour, tz_minute) = if data.len() == 13 {
        decode_tz_offset(data[11], data[12])?
    } else {
        (0, 0)
    };

    Ok(OracleTimestamp {
        year: date.year,
        month: date.month,
        day: date.day,
        hour: date.hour,
        minute: date.minute,
        second: date.second,
        microsecond: fsecond,
        tz_hour_offset: tz_hour,
        tz_minute_offset: tz_minute,
    })
}

/// The two offset bytes of a TIMESTAMP WITH TIME ZONE: both zero means no
/// offset, a region id is refused, anything else must be a valid offset.
pub(crate) fn decode_tz_offset(hour_byte: u8, minute_byte: u8) -> Result<(i8, i8)> {
    if hour_byte & HAS_REGION_ID != 0 {
        return Err(Error::DataConversionError(
            "Named timezone regions are not supported".to_string(),
        ));
    }
    if hour_byte == 0 && minute_byte == 0 {
        return Ok((0, 0));
    }
    let invalid = || Error::DataConversionError("invalid Oracle time zone offset".to_string());
    let hour = i8::try_from(hour_byte).map_err(|_| invalid())? - TZ_HOUR_OFFSET;
    let minute = i8::try_from(minute_byte).map_err(|_| invalid())? - TZ_MINUTE_OFFSET;
    if !(-12..=14).contains(&hour) || !(-59..=59).contains(&minute) {
        return Err(invalid());
    }
    Ok((hour, minute))
}

/// Encode an Oracle DATE to wire format (7 bytes)
pub fn encode_oracle_date(date: &OracleDate) -> Result<Vec<u8>> {
    Ok(date.to_oracle_bytes()?.to_vec())
}

/// Encode an Oracle TIMESTAMP to wire format
///
/// Returns 7 bytes for DATE, 11 bytes for TIMESTAMP, or 13 bytes for TIMESTAMP WITH TIME ZONE
pub fn encode_oracle_timestamp(ts: &OracleTimestamp, include_tz: bool) -> Result<Vec<u8>> {
    let bytes = ts.to_oracle_bytes()?;
    let mut result = bytes[..7].to_vec();

    // Add fractional seconds if non-zero or if we need timezone
    if ts.microsecond > 0 || include_tz {
        result.extend_from_slice(&bytes[7..]);
    }

    // Add timezone if requested
    if include_tz {
        let invalid = || Error::DataConversionError("invalid Oracle time zone offset".to_string());
        if !(-12..=14).contains(&ts.tz_hour_offset) || !(-59..=59).contains(&ts.tz_minute_offset) {
            return Err(invalid());
        }
        result.push(u8::try_from(ts.tz_hour_offset + TZ_HOUR_OFFSET).map_err(|_| invalid())?);
        result.push(u8::try_from(ts.tz_minute_offset + TZ_MINUTE_OFFSET).map_err(|_| invalid())?);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_date() {
        // 2024-03-15 14:30:45
        let data = vec![
            120, // century: 20 + 100 = 120
            124, // year: 24 + 100 = 124
            3,   // month: 3
            15,  // day: 15
            15,  // hour: 14 + 1 = 15
            31,  // minute: 30 + 1 = 31
            46,  // second: 45 + 1 = 46
        ];

        let date = decode_oracle_date(&data).unwrap();
        assert_eq!(date.year, 2024);
        assert_eq!(date.month, 3);
        assert_eq!(date.day, 15);
        assert_eq!(date.hour, 14);
        assert_eq!(date.minute, 30);
        assert_eq!(date.second, 45);
    }

    #[test]
    fn test_encode_date() {
        let date = OracleDate::new(2024, 3, 15, 14, 30, 45);
        let encoded = encode_oracle_date(&date).unwrap();
        let decoded = decode_oracle_date(&encoded).unwrap();

        assert_eq!(decoded.year, date.year);
        assert_eq!(decoded.month, date.month);
        assert_eq!(decoded.day, date.day);
        assert_eq!(decoded.hour, date.hour);
        assert_eq!(decoded.minute, date.minute);
        assert_eq!(decoded.second, date.second);
    }

    #[test]
    fn test_date_roundtrip() {
        let original = OracleDate::new(1999, 12, 31, 23, 59, 59);
        let encoded = encode_oracle_date(&original).unwrap();
        let decoded = decode_oracle_date(&encoded).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_decode_timestamp_with_fractional() {
        // Build timestamp data: 2024-03-15 14:30:45.123456
        let mut data = vec![120, 124, 3, 15, 15, 31, 46];
        // Add nanoseconds (123456000 = 0x075BCA00)
        let nanos: u32 = 123456000;
        data.extend_from_slice(&nanos.to_be_bytes());

        let ts = decode_oracle_timestamp(&data).unwrap();
        assert_eq!(ts.year, 2024);
        assert_eq!(ts.microsecond, 123456);
    }

    #[test]
    fn test_decode_timestamp_with_timezone() {
        // Build timestamp data: 2024-03-15 14:30:45 +05:30
        let mut data = vec![120, 124, 3, 15, 15, 31, 46];
        data.extend_from_slice(&[0, 0, 0, 0]); // No fractional seconds
        data.push(25); // tz_hour: 5 + 20 = 25
        data.push(90); // tz_minute: 30 + 60 = 90

        let ts = decode_oracle_timestamp(&data).unwrap();
        assert_eq!(ts.tz_hour_offset, 5);
        assert_eq!(ts.tz_minute_offset, 30);
        assert!(ts.has_timezone());
    }

    #[test]
    fn test_timestamp_to_date_conversion() {
        let ts = OracleTimestamp::new(2024, 3, 15, 14, 30, 45, 123456);
        let date = ts.to_date();

        assert_eq!(date.year, 2024);
        assert_eq!(date.month, 3);
        assert_eq!(date.day, 15);
        assert_eq!(date.hour, 14);
        assert_eq!(date.minute, 30);
        assert_eq!(date.second, 45);
    }

    #[test]
    fn test_date_to_timestamp_conversion() {
        let date = OracleDate::new(2024, 3, 15, 14, 30, 45);
        let ts: OracleTimestamp = date.into();

        assert_eq!(ts.year, 2024);
        assert_eq!(ts.microsecond, 0);
        assert!(!ts.has_timezone());
    }

    #[test]
    fn test_negative_year() {
        // Year -100 (100 BC)
        let data = vec![
            99,  // century: -1 + 100 = 99
            100, // year: 0 + 100 = 100
            1, 1, 1, 1, 1,
        ];

        let date = decode_oracle_date(&data).unwrap();
        assert_eq!(date.year, -100);
    }
}
