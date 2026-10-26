//! Oracle NUMBER encoding and decoding
//!
//! Oracle NUMBER is stored in a variable-length format:
//! - First byte: exponent (with sign encoding)
//! - Subsequent bytes: mantissa digits in base-100
//!
//! For positive numbers: exponent byte has high bit set, mantissa bytes are value + 1
//! For negative numbers: exponent byte is inverted, mantissa bytes are 101 - value,
//!                       and a trailing 102 byte is added (if not at max digits)

use crate::error::{Error, Result};

/// Maximum number of digits in an Oracle NUMBER
const MAX_DIGITS: usize = 40;

/// Maximum number of base-100 mantissa bytes in an Oracle NUMBER
const MAX_MANTISSA_BYTES: usize = 20;

/// Maximum characters in a number string representation
const MAX_STRING_CHARS: usize = 172;

/// Decoded Oracle NUMBER as a string representation
#[derive(Debug, Clone)]
pub struct OracleNumber {
    /// String representation of the number
    pub value: String,
    /// Whether the number is an integer (no decimal point)
    pub is_integer: bool,
    /// Whether this is the maximum negative value (-1e126)
    pub is_max_negative: bool,
}

impl OracleNumber {
    /// Create a new Oracle number from string representation
    pub fn new(value: impl Into<String>) -> Self {
        let value = value.into();
        let is_integer = !value.contains('.');
        Self {
            value,
            is_integer,
            is_max_negative: false,
        }
    }

    /// Get the string value
    pub fn as_str(&self) -> &str {
        &self.value
    }

    /// Try to convert to i64
    pub fn to_i64(&self) -> Result<i64> {
        if self.is_max_negative {
            return Err(Error::DataConversionError(
                "Maximum negative Oracle number cannot be represented as i64".to_string(),
            ));
        }
        self.value
            .parse()
            .map_err(|e| Error::DataConversionError(format!("Cannot parse as i64: {}", e)))
    }

    /// Try to convert to f64
    pub fn to_f64(&self) -> Result<f64> {
        if self.is_max_negative {
            return Ok(-1e126);
        }
        self.value
            .parse()
            .map_err(|e| Error::DataConversionError(format!("Cannot parse as f64: {}", e)))
    }
}

/// Decode an Oracle NUMBER from wire format bytes
///
/// Oracle NUMBER format:
/// - Byte 0: Exponent byte (with sign encoding)
/// - Bytes 1..n: Mantissa digits in base-100 format
pub fn decode_oracle_number(data: &[u8]) -> Result<OracleNumber> {
    if data.is_empty() {
        return Err(Error::DataConversionError(
            "Empty data for Oracle NUMBER".to_string(),
        ));
    }

    let exponent_byte = data[0];
    let is_positive = (exponent_byte & 0x80) != 0;

    // Decode the exponent
    let exponent = if is_positive {
        (exponent_byte as i16) - 193
    } else {
        (!exponent_byte as i16) - 193
    };

    let mut decimal_point_index = (exponent * 2 + 2) as i32;

    // Special case: a single byte is zero (0x80) or -infinity (0x00).
    //
    // Plenora fork: upstream returned -infinity as an empty value flagged
    // `is_max_negative`, which callers reading `value` saw as "" and then as
    // 0. Infinity is not a decimal number, and any other single byte is not an
    // Oracle NUMBER: both are refused.
    if data.len() == 1 {
        if exponent_byte == 128 {
            return Ok(OracleNumber::new("0"));
        }
        return Err(Error::DataConversionError(
            "Oracle NUMBER infinity or invalid single byte".to_string(),
        ));
    }

    // Check for trailing 102 byte in negative numbers
    let mantissa_len = if !is_positive && data.len() > 1 && data[data.len() - 1] == 102 {
        data.len() - 2
    } else {
        data.len() - 1
    };

    // Plenora fork: a negative value made only of the exponent and the 102
    // terminator, or a mantissa longer than 20 base-100 digits, is not an
    // Oracle NUMBER. Decoding it would print "0" or digits Oracle never
    // stored, so it is refused.
    if mantissa_len == 0 || mantissa_len > MAX_MANTISSA_BYTES {
        return Err(Error::DataConversionError(
            "Invalid Oracle NUMBER mantissa length".to_string(),
        ));
    }

    // Decode mantissa digits
    let mut digits = Vec::with_capacity(MAX_DIGITS);
    for i in 0..mantissa_len {
        let byte = data[i + 1];
        // Plenora fork: a base-100 digit is 0..=99, stored as 1..=100 when
        // positive and 2..=101 when negative. Upstream also took the value
        // 100 (positive 101, negative 1) as an "overflow" digit: that is how
        // +infinity ([255, 101]) decoded to a finite 1e126 and [193, 2, 101]
        // to 11. Any other byte used to wrap around and print a non-digit.
        let value = match (is_positive, byte) {
            (true, 1..=100) => byte - 1,
            (false, 2..=101) => 101 - byte,
            _ => {
                return Err(Error::DataConversionError(
                    "Invalid Oracle NUMBER mantissa byte".to_string(),
                ))
            }
        };

        // First digit of the pair
        let digit1 = value / 10;
        // Handle leading zeros
        if digit1 == 0 && digits.is_empty() {
            decimal_point_index -= 1;
        } else if digit1 != 0 || !digits.is_empty() {
            digits.push(digit1);
        }

        // Second digit of the pair
        let digit2 = value % 10;
        if digit2 != 0 || i < mantissa_len - 1 {
            digits.push(digit2);
        }
    }

    // Remove trailing zeros (for integer detection)
    while !digits.is_empty() && digits[digits.len() - 1] == 0 {
        if (digits.len() as i32) <= decimal_point_index {
            break;
        }
        digits.pop();
    }

    // Build string representation
    let mut result = String::with_capacity(MAX_STRING_CHARS);

    if !is_positive {
        result.push('-');
    }

    let is_integer;
    if decimal_point_index <= 0 {
        result.push('0');
        result.push('.');
        is_integer = false;
        for _ in decimal_point_index..0 {
            result.push('0');
        }
        for d in &digits {
            result.push(char::from(b'0' + d));
        }
    } else {
        is_integer = decimal_point_index as usize >= digits.len();
        for (i, d) in digits.iter().enumerate() {
            if i > 0 && i as i32 == decimal_point_index {
                result.push('.');
            }
            result.push(char::from(b'0' + d));
        }
        // Add trailing zeros for integers
        if decimal_point_index as usize > digits.len() {
            for _ in digits.len()..decimal_point_index as usize {
                result.push('0');
            }
        }
    }

    if result.is_empty() || result == "-" {
        result = "0".to_string();
    }

    Ok(OracleNumber {
        value: result,
        is_integer,
        is_max_negative: false,
    })
}

/// Plenora fork: true when two decimal texts (plain or with an exponent)
/// denote the same value. Used to refuse a lossy `f64` instead of returning
/// it as if it were the stored NUMBER.
pub(crate) fn same_decimal_value(left: &str, right: &str) -> bool {
    match (canonical_decimal(left), canonical_decimal(right)) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// Sign, significant digits and the decimal exponent of their first digit;
/// zero is `(false, [], 0)`. `None` for text that is not a finite decimal.
fn canonical_decimal(text: &str) -> Option<(bool, Vec<u8>, i64)> {
    let (negative, unsigned) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => (
            &unsigned[..index],
            unsigned[index + 1..].parse::<i64>().ok()?,
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let mut digits = Vec::with_capacity(whole.len() + fraction.len());
    for byte in whole.bytes().chain(fraction.bytes()) {
        if !byte.is_ascii_digit() {
            return None;
        }
        digits.push(byte - b'0');
    }
    let mut point = i64::try_from(whole.len()).ok()?.checked_add(exponent)?;
    let leading = digits.iter().take_while(|digit| **digit == 0).count();
    digits.drain(..leading);
    point -= i64::try_from(leading).ok()?;
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        return Some((false, digits, 0));
    }
    Some((negative, digits, point))
}

/// Encode a number string to Oracle NUMBER wire format
pub fn encode_oracle_number(value: &str) -> Result<Vec<u8>> {
    let value = value.trim();

    if value.is_empty() {
        return Err(Error::DataConversionError(
            "Empty string cannot be encoded as Oracle NUMBER".to_string(),
        ));
    }

    if value.len() > MAX_STRING_CHARS {
        return Err(Error::DataConversionError(
            "Number string too long for Oracle NUMBER".to_string(),
        ));
    }

    let bytes = value.as_bytes();
    let mut pos = 0;

    // Check for negative sign
    let is_negative = bytes.first() == Some(&b'-');
    // Plenora fork: an explicit '+' is accepted, as the decimal validation of
    // the Oracle provider already does.
    if is_negative || bytes.first() == Some(&b'+') {
        pos += 1;
    }

    // Parse digits before decimal point
    let mut digits = Vec::with_capacity(MAX_DIGITS);
    let mut decimal_point_index: i32;
    // Plenora fork: count every mantissa digit, zeros included, so that "-",
    // "." or "e5" are refused instead of being encoded as zero.
    let mut mantissa_digits = 0_usize;

    while pos < bytes.len() {
        let b = bytes[pos];
        if b == b'.' || b == b'e' || b == b'E' {
            break;
        }
        if !b.is_ascii_digit() {
            return Err(Error::DataConversionError(format!(
                "Invalid character '{}' in number",
                char::from(b)
            )));
        }
        let digit = b - b'0';
        if digit != 0 || !digits.is_empty() {
            digits.push(digit);
        }
        mantissa_digits += 1;
        pos += 1;
    }
    decimal_point_index = digits.len() as i32;

    // Parse digits after decimal point
    if pos < bytes.len() && bytes[pos] == b'.' {
        pos += 1;
        while pos < bytes.len() {
            let b = bytes[pos];
            if b == b'e' || b == b'E' {
                break;
            }
            if !b.is_ascii_digit() {
                return Err(Error::DataConversionError(format!(
                    "Invalid character '{}' in number",
                    char::from(b)
                )));
            }
            let digit = b - b'0';
            if digit == 0 && digits.is_empty() {
                decimal_point_index -= 1;
            } else {
                digits.push(digit);
            }
            mantissa_digits += 1;
            pos += 1;
        }
    }

    if mantissa_digits == 0 {
        return Err(Error::DataConversionError(
            "Number has no digits".to_string(),
        ));
    }

    // Parse exponent
    if pos < bytes.len() && (bytes[pos] == b'e' || bytes[pos] == b'E') {
        pos += 1;
        let exp_negative = if pos < bytes.len() && bytes[pos] == b'-' {
            pos += 1;
            true
        } else {
            if pos < bytes.len() && bytes[pos] == b'+' {
                pos += 1;
            }
            false
        };

        let exp_start = pos;
        while pos < bytes.len() && bytes[pos].is_ascii_digit() {
            pos += 1;
        }

        if exp_start == pos {
            return Err(Error::DataConversionError(
                "Missing exponent value".to_string(),
            ));
        }

        let exp: i32 = std::str::from_utf8(&bytes[exp_start..pos])
            .unwrap()
            .parse()
            .map_err(|_| Error::DataConversionError("Invalid exponent".to_string()))?;

        // Plenora fork: checked, so an extreme exponent is refused instead of
        // wrapping (release) or panicking (debug).
        decimal_point_index = if exp_negative {
            decimal_point_index.checked_sub(exp)
        } else {
            decimal_point_index.checked_add(exp)
        }
        .ok_or_else(|| {
            Error::DataConversionError("Number out of range for Oracle NUMBER".to_string())
        })?;
    }

    // Plenora fork: trailing bytes after the mantissa or the exponent
    // ("1e5x", "1e5.3") were silently dropped.
    if pos != bytes.len() {
        return Err(Error::DataConversionError(
            "Invalid character in number".to_string(),
        ));
    }

    // Remove trailing zeros
    while !digits.is_empty() && digits[digits.len() - 1] == 0 {
        digits.pop();
    }

    // Check bounds
    if digits.len() > MAX_DIGITS || decimal_point_index > 126 || decimal_point_index < -129 {
        return Err(Error::DataConversionError(
            "Number out of range for Oracle NUMBER".to_string(),
        ));
    }

    // Zero is a special case
    if digits.is_empty() {
        return Ok(vec![128]);
    }

    // Adjust for odd exponent.
    //
    // Plenora fork: python-oracledb writes `decimal_point_index % 2 == 1`,
    // where Python's `%` is never negative. Rust's `%` keeps the sign of the
    // dividend, so `-1 % 2 == -1` and every value in (-1, 1) with an odd
    // number of zeros after the point skipped the alignment: 0.05 was sent as
    // 0.5. `rem_euclid` is the Python modulo.
    let prepend_zero = decimal_point_index.rem_euclid(2) == 1;
    if prepend_zero && !digits.is_empty() {
        digits.push(0);
        decimal_point_index += 1;
    }

    // Ensure even number of digits
    if digits.len() % 2 == 1 {
        digits.push(0);
    }

    let num_pairs = digits.len() / 2;

    // Plenora fork: 40 significant digits that are not aligned to a base-100
    // pair need 21 mantissa bytes, which Oracle cannot store. Refused rather
    // than sent.
    if num_pairs > MAX_MANTISSA_BYTES {
        return Err(Error::DataConversionError(
            "Number has too many digits for Oracle NUMBER".to_string(),
        ));
    }

    // Build result
    let mut result = Vec::with_capacity(num_pairs + 2);

    // Encode exponent. After the alignment above the index is even, so the
    // division is exact for negative indexes as well (Rust `/` truncates
    // toward zero, Python `//` floors: they only agree on exact quotients).
    debug_assert_eq!(decimal_point_index.rem_euclid(2), 0);
    let exponent_on_wire = (decimal_point_index.div_euclid(2) + 192) as i8;
    let exponent_byte = if is_negative {
        !exponent_on_wire as u8
    } else {
        exponent_on_wire as u8
    };
    result.push(exponent_byte);

    // Encode mantissa
    let mut digit_pos = 0;
    for pair_num in 0..num_pairs {
        let pair_value = if pair_num == 0 && prepend_zero {
            let v = digits[digit_pos];
            digit_pos += 1;
            v
        } else {
            let v = digits[digit_pos] * 10 + digits[digit_pos + 1];
            digit_pos += 2;
            v
        };

        let encoded = if is_negative {
            101 - pair_value
        } else {
            pair_value + 1
        };
        result.push(encoded);
    }

    // Add trailing 102 for negative numbers (if not at max length)
    if is_negative && num_pairs < 20 {
        result.push(102);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_zero() {
        let data = vec![128];
        let num = decode_oracle_number(&data).unwrap();
        assert_eq!(num.value, "0");
        assert!(num.is_integer);
    }

    #[test]
    fn test_decode_positive_integer() {
        // 123 encoded
        let data = vec![0xc2, 0x02, 0x18]; // exponent=1, digits=[1,23]
        let num = decode_oracle_number(&data).unwrap();
        assert_eq!(num.value, "123");
        assert!(num.is_integer);
    }

    #[test]
    fn test_decode_negative_integer() {
        // -123 encoded
        let data = vec![0x3d, 0x64, 0x4e, 0x66]; // ~exponent, 101-digits, 102
        let num = decode_oracle_number(&data).unwrap();
        assert_eq!(num.value, "-123");
    }

    #[test]
    fn test_decode_decimal() {
        // 1.5 encoded
        let data = vec![0xc1, 0x02, 0x33]; // exponent=0, digits=[1,50]
        let num = decode_oracle_number(&data).unwrap();
        assert_eq!(num.value, "1.5");
        assert!(!num.is_integer);
    }

    #[test]
    fn test_encode_zero() {
        let encoded = encode_oracle_number("0").unwrap();
        assert_eq!(encoded, vec![128]);
    }

    #[test]
    fn test_encode_positive_integer() {
        let encoded = encode_oracle_number("123").unwrap();
        let decoded = decode_oracle_number(&encoded).unwrap();
        assert_eq!(decoded.value, "123");
    }

    #[test]
    fn test_encode_negative_integer() {
        let encoded = encode_oracle_number("-456").unwrap();
        let decoded = decode_oracle_number(&encoded).unwrap();
        assert_eq!(decoded.value, "-456");
    }

    #[test]
    fn test_encode_decimal() {
        let encoded = encode_oracle_number("3.14159").unwrap();
        let decoded = decode_oracle_number(&encoded).unwrap();
        assert!(decoded.value.starts_with("3.14159"));
    }

    #[test]
    fn test_encode_scientific() {
        let encoded = encode_oracle_number("1.5e10").unwrap();
        let decoded = decode_oracle_number(&encoded).unwrap();
        assert_eq!(decoded.value, "15000000000");
    }

    #[test]
    fn test_roundtrip_various_numbers() {
        let test_values = ["1", "99", "100", "999", "1000", "-1", "-99", "-100"];

        for val in test_values {
            let encoded = encode_oracle_number(val).unwrap();
            let decoded = decode_oracle_number(&encoded).unwrap();
            // Remove trailing zeros for comparison
            let expected = val.trim_start_matches('0');
            let got = decoded.value.trim_start_matches('0');
            assert!(
                expected == got || val == decoded.value,
                "Roundtrip failed for {}: got {}",
                val,
                decoded.value
            );
        }
    }

    #[test]
    fn test_oracle_number_to_i64() {
        let num = OracleNumber::new("12345");
        assert_eq!(num.to_i64().unwrap(), 12345);
    }

    #[test]
    fn test_oracle_number_to_f64() {
        let num = OracleNumber::new("3.14");
        let f = num.to_f64().unwrap();
        assert!((f - 3.14).abs() < 0.001);
    }

    // =========================================================================
    // WIRE-LEVEL PROTOCOL TESTS
    // These tests document specific protocol details learned during development.
    // They serve as reference for anyone implementing Oracle/TNS protocols.
    // =========================================================================

    /// Oracle NUMBER wire format:
    ///
    /// Byte 0: Exponent/sign byte
    ///   - Positive: 0x80 + (exponent + 65)
    ///   - Negative: ~(0x80 + (exponent + 65))
    ///   - Zero: 0x80
    ///
    /// Bytes 1+: Base-100 mantissa digits
    ///   - Positive: digit + 1 (so 00-99 becomes 01-100)
    ///   - Negative: 101 - digit (so 00-99 becomes 101-02)
    ///
    /// CRITICAL: Negative numbers require terminator byte 0x66 (102)
    /// unless all 20 mantissa positions are used.
    #[test]
    fn test_wire_number_negative_terminator_0x66() {
        // -123 should have terminator byte 0x66
        let encoded = encode_oracle_number("-123").unwrap();

        // Format: [exponent] [digit1] [digit2] [terminator]
        // -123 = -1.23 × 10² = exponent 2 (value 1, digits 01 23)
        // Exponent byte: ~(0x80 + 2 + 65) = ~0xC3 = 0x3C (but actual calc differs)

        // Last byte MUST be 0x66 (102) for negative numbers
        assert_eq!(
            *encoded.last().unwrap(),
            0x66,
            "Negative numbers must end with terminator byte 0x66 (102)"
        );

        // Verify it's not present for positive numbers
        let pos_encoded = encode_oracle_number("123").unwrap();
        assert_ne!(
            *pos_encoded.last().unwrap(),
            0x66,
            "Positive numbers must NOT have terminator byte"
        );
    }

    /// Oracle NUMBER exponent byte calculation:
    ///
    /// The exponent byte encodes both the exponent AND the sign:
    ///   - Base value: 0x80 (128)
    ///   - Exponent offset: 65
    ///   - Positive: 0x80 + exponent + 65 = 0xC1 + exponent
    ///   - Negative: ~(0x80 + exponent + 65) = 0x3E - exponent
    ///
    /// Zero is special: just 0x80 with no mantissa.
    #[test]
    fn test_wire_number_exponent_encoding() {
        // Zero
        let zero = encode_oracle_number("0").unwrap();
        assert_eq!(zero, vec![0x80], "Zero must be single byte 0x80");

        // Positive single-digit (exponent = 0)
        // 5 → exponent=0, mantissa=05 → [0xC1, 0x06] (5+1)
        let five = encode_oracle_number("5").unwrap();
        assert_eq!(
            five[0], 0xC1,
            "Single digit positive has exponent byte 0xC1"
        );

        // Positive three-digit (exponent = 1)
        // 123 → exponent=1, mantissa=01 23 → [0xC2, 0x02, 0x18]
        let one23 = encode_oracle_number("123").unwrap();
        assert_eq!(
            one23[0], 0xC2,
            "Three digit positive has exponent byte 0xC2"
        );

        // Negative (inverted)
        // -5 → exponent=0, mantissa=05 → [~0xC1, 101-5, 0x66] = [0x3E, 0x60, 0x66]
        let neg5 = encode_oracle_number("-5").unwrap();
        assert_eq!(
            neg5[0], 0x3E,
            "Single digit negative has exponent byte 0x3E (~0xC1)"
        );
    }

    /// Oracle NUMBER mantissa uses base-100 encoding
    ///
    /// Each mantissa byte represents two decimal digits as base-100:
    ///   - 00-99 → stored as value (for positive: +1, for negative: 101-value)
    ///   - "12345" → [01, 23, 45] (three base-100 digits)
    ///
    /// For odd digit counts, implicit leading zero:
    ///   - "123" → [01, 23] (not [1, 23]!)
    #[test]
    fn test_wire_number_base100_encoding() {
        // 12 → single base-100 digit: 12
        // Encoded as [exponent, 12+1] = [0xC1, 0x0D]
        let twelve = encode_oracle_number("12").unwrap();
        assert_eq!(twelve.len(), 2);
        assert_eq!(
            twelve[1], 13,
            "12 encoded as base-100 digit 12, stored as 13 (12+1)"
        );

        // 99 → single base-100 digit: 99
        // Encoded as [exponent, 99+1] = [0xC1, 0x64]
        let ninetynine = encode_oracle_number("99").unwrap();
        assert_eq!(
            ninetynine[1], 100,
            "99 encoded as base-100 digit 99, stored as 100 (99+1)"
        );

        // 100 → CRITICAL: Oracle NUMBER removes trailing zeros!
        // The digits [1, 0, 0] become [1] after trailing zero removal.
        // Since decimal_point_index=3 is odd, prepend_zero=true, padding adds 0 → [1, 0]
        // But when prepend_zero is set and it's the first pair, only one digit is taken (1, not 10)
        // Exponent adjusted for decimal position: (3+1)/2 + 192 = 194 = 0xC2
        // Encoded as [0xC2, 0x02] where 0x02 = 2 = 1 + 1 (digit "1" encoded)
        let hundred = encode_oracle_number("100").unwrap();
        assert_eq!(
            hundred.len(),
            2,
            "100 is just 2 bytes - trailing zeros removed"
        );
        assert_eq!(hundred[0], 0xC2, "Exponent byte: 194 = (4/2) + 192");
        assert_eq!(
            hundred[1], 2,
            "Single digit 1 (with prepend_zero), stored as 2 (1+1)"
        );

        // 1234 → two base-100 digits: 12, 34
        // No trailing zeros to remove
        // Encoded as [0xC2, 13, 35] (exponent 2, digits 12+1 and 34+1)
        let twelve34 = encode_oracle_number("1234").unwrap();
        assert_eq!(twelve34.len(), 3);
        assert_eq!(twelve34[1], 13, "First base-100 digit 12, stored as 13");
        assert_eq!(twelve34[2], 35, "Second base-100 digit 34, stored as 35");
    }

    /// Plenora fork: bytes checked against `DUMP()` on Oracle Free 23ai.
    /// Before the fix every value with an odd count of zeros after the point
    /// was encoded as ten times itself (0.05 as [192, 51], i.e. 0.5).
    #[test]
    fn plenora_fork_values_below_one_match_oracle_dump() {
        let cases: [(&str, &[u8]); 12] = [
            ("0.05", &[192, 6]),
            ("-0.05", &[63, 96, 102]),
            ("0.5", &[192, 51]),
            ("-0.5", &[63, 51, 102]),
            ("0.0005", &[191, 6]),
            ("0.005", &[191, 51]),
            ("0.07", &[192, 8]),
            ("0.0123", &[192, 2, 24]),
            ("1E-130", &[128, 2]),
            ("5e-130", &[128, 6]),
            ("-1E-130", &[127, 100, 102]),
            ("9.9e125", &[255, 100]),
        ];
        for (text, expected) in cases {
            let encoded = encode_oracle_number(text).unwrap();
            assert_eq!(encoded, expected, "{text}");
        }
        assert_eq!(decode_oracle_number(&[192, 6]).unwrap().value, "0.05");
        assert_eq!(decode_oracle_number(&[63, 96, 102]).unwrap().value, "-0.05");
    }

    #[test]
    fn plenora_fork_malformed_text_and_bytes_are_refused() {
        for text in [
            "-",
            "+",
            ".",
            "-.",
            "e5",
            "1e5x",
            "1e5.3",
            "1.2.3",
            "1e2147483647",
            "0.1e-2147483648",
            "1e126",
            "1e-131",
        ] {
            assert!(encode_oracle_number(text).is_err(), "{text}");
        }
        assert_eq!(
            encode_oracle_number("+5").unwrap(),
            encode_oracle_number("5").unwrap()
        );
        // 40 significant digits that do not fill whole base-100 pairs need 21
        // mantissa bytes: refused, never sent.
        assert!(encode_oracle_number("1.234567890123456789012345678901234567891").is_err());
        assert!(encode_oracle_number("12.34567890123456789012345678901234567891").is_ok());
        for bytes in [
            &[193_u8, 0][..],
            &[193, 102],
            &[62, 102],
            &[62, 0, 102],
            // -infinity, +infinity, the old "overflow" digit anywhere.
            &[0],
            &[255, 101],
            &[193, 2, 101],
            &[193, 101],
            &[62, 1, 102],
            &[62, 100, 1, 102],
            // A single byte other than zero.
            &[129],
            &[127],
            &[
                193, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
            ],
        ] {
            assert!(decode_oracle_number(bytes).is_err(), "{bytes:?}");
        }
    }

    #[test]
    fn plenora_fork_same_decimal_value_compares_by_value() {
        assert!(same_decimal_value("0.05", "5e-2"));
        assert!(same_decimal_value("-0.0", "0"));
        assert!(same_decimal_value(
            "1.2345678901234568e29",
            "123456789012345680000000000000"
        ));
        assert!(!same_decimal_value(
            "1.2345678901234568e29",
            "123456789012345678901234567890"
        ));
        assert!(!same_decimal_value("0.5", "0.05"));
        assert!(!same_decimal_value("-1", "1"));
        assert!(!same_decimal_value("", "0"));
        assert!(!same_decimal_value("-", "0"));
    }
}
