use super::{canonical_uuid, exact_f32, reject_nul, reject_sub_microsecond, DecimalLiteral};
use crate::plan::ProviderKind;
use crate::ErrorCategory;

const P: ProviderKind = ProviderKind::Db2;

#[test]
fn sub_microsecond_digits_are_refused_on_the_text() {
    for accepted in [
        "2024-01-01T10:20:30",
        "2024-01-01T10:20:30.123456",
        "2024-01-01T10:20:30.123456000",
        "2024-01-01 10:20:30.1",
        "2024-01-01T10:20:30.123456+02:00",
        "2024-01-01",
        "2024-01-01-10.20.30.123456",
        "2024-01-01-10.20.30",
    ] {
        assert!(reject_sub_microsecond(P, accepted).is_ok(), "{accepted}");
    }
    for refused in [
        "2024-01-01T10:20:30.1234567",
        "2024-01-01T10:20:30.1234560001",
        "2024-01-01T10:20:30.1234561+02:00",
        "2024-01-01 10:20:30.0000001Z",
        "2024-01-01-10.20.30.123456789012",
    ] {
        let error = reject_sub_microsecond(P, refused).expect_err(refused);
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert!(!error.message.contains("2024"));
    }
}

#[test]
fn nul_and_uuid_forms_are_checked() {
    assert!(reject_nul(P, "a\0b").is_err());
    assert!(reject_nul(P, "ab").is_ok());
    assert!(canonical_uuid(P, "6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60").is_ok());
    for refused in [
        "6f1c2a2e7-d3b-4c55-9a1e-0b2c3d4e5f60",
        "6f1c2a2e7d3b4c559a1e0b2c3d4e5f60",
        "6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f6g",
        "{6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f6}",
    ] {
        assert!(canonical_uuid(P, refused).is_err(), "{refused}");
    }
}

#[test]
fn exact_f32_keeps_only_representable_values() {
    assert_eq!(exact_f32(0.5), Some(0.5));
    assert_eq!(exact_f32(0.1), None);
    assert_eq!(exact_f32(f64::INFINITY), Some(f32::INFINITY));
    assert!(exact_f32(f64::NAN).is_some_and(f32::is_nan));
    assert_eq!(
        exact_f32(-0.0).map(f32::to_bits),
        Some((-0.0_f32).to_bits())
    );
    assert_eq!(exact_f32(f64::MAX), None);
}

#[test]
fn decimal_literals_parse_fit_and_split() {
    let value = DecimalLiteral::parse(P, "-0012345.67890").expect("decimal");
    assert!(value.negative);
    assert_eq!(value.integer, "12345");
    assert_eq!(value.significant_fraction(), "6789");
    assert!(value.fits(12, 4));
    assert!(!value.fits(12, 3));
    assert!(!value.fits(8, 4));
    assert_eq!(
        value.mantissa_and_scale(P).expect("mantissa"),
        (-123_456_789, 4)
    );
    let zero = DecimalLiteral::parse(P, ".000").expect("zero");
    assert_eq!(zero.mantissa_and_scale(P).expect("zero"), (0, 0));
    assert!(DecimalLiteral::parse(P, "5.").is_ok());
    for refused in [
        "", "-", ".", "1.5E2", "1e3", "1,5", " 1", "1.2.3", "0x10", "1_000",
    ] {
        assert!(DecimalLiteral::parse(P, refused).is_err(), "{refused}");
    }
    let wide = DecimalLiteral::parse(P, &"9".repeat(40)).expect("parse");
    assert!(wide.mantissa_and_scale(P).is_err());
}
