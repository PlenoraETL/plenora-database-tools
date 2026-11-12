use plenora_database_core::arrow::array::{
    Array, BinaryArray, Float64Array, Int32Array, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::execution_loss::Basis::{Conservative, Exact};
use plenora_database_core::loss_ledger::Classification::{
    Exact as Unchanged, HardReject, WouldChange,
};
use plenora_database_core::store_rewrite::{StoreRewrite as R, TextLengthUnit as U};

#[test]
fn shared_text_predicates_boundaries_null_and_conservative_false_positives() {
    for (rule, samples, expected) in [
        (R::EmptyToNull, ["", "a", " "], [true, false, false]),
        (
            R::PadText { chars: 2 },
            ["é", "😀x", "abc"],
            [true, false, false],
        ),
        (R::StripTrailing, ["", "a ", "a"], [false, true, false]),
        (
            R::ExcessSpaces { bytes: 2 },
            ["", "a", "a "],
            [true, true, true],
        ),
        (R::NonAscii, ["", "é", "abc"], [false, true, false]),
        (R::JsonNumbers, ["123", "1.5", "-0"], [false, true, true]),
        (R::Every, ["", "a", "abc"], [true, true, true]),
    ] {
        let array = StringArray::from(vec![
            Some(samples[0]),
            Some(samples[1]),
            Some(samples[2]),
            None,
        ]);
        let basis = if matches!(
            rule,
            R::ExcessSpaces { .. } | R::NonAscii | R::JsonNumbers | R::Every
        ) {
            Conservative
        } else {
            Exact
        };
        for (row, positive) in expected.into_iter().enumerate() {
            assert_eq!(
                rule.classify(&array, row, U::Characters).unwrap(),
                if positive {
                    WouldChange(basis)
                } else {
                    Unchanged(basis)
                }
            );
        }
        assert_eq!(
            rule.classify(&array, 3, U::Characters).unwrap(),
            Unchanged(basis)
        );
        assert!(rule.classify(&array, 4, U::Characters).is_err());
    }
    let array = StringArray::from(vec!["é", "😀"]);
    assert_eq!(
        R::PadText { chars: 2 }
            .classify(&array, 0, U::SingleByte)
            .unwrap(),
        WouldChange(Conservative)
    );
    assert_eq!(
        R::PadText { chars: 2 }
            .classify(&array, 1, U::Utf16)
            .unwrap(),
        Unchanged(Exact)
    );
    assert!(R::JsonNumbers
        .classify(
            &StringArray::from(vec!["{invalid payload"]),
            0,
            U::Characters
        )
        .is_err());
    let huge = format!("1{}", "0".repeat(400));
    assert_eq!(
        R::JsonNumbers
            .classify(&StringArray::from(vec![huge]), 0, U::Characters)
            .unwrap(),
        WouldChange(Conservative)
    );
}
#[test]
fn shared_binary_numeric_and_temporal_predicates() {
    let binary = BinaryArray::from(vec![
        Some(b"".as_slice()),
        Some(b"a\0".as_slice()),
        Some(b"abc".as_slice()),
        None,
    ]);
    for (row, positive) in [true, true, false, false].into_iter().enumerate() {
        assert_eq!(
            R::PadBinary { bytes: 3 }
                .classify(&binary, row, U::Characters)
                .unwrap(),
            if positive {
                WouldChange(Exact)
            } else {
                Unchanged(Exact)
            }
        );
    }
    assert_eq!(
        R::StripTrailing
            .classify(&binary, 1, U::Characters)
            .unwrap(),
        WouldChange(Exact)
    );
    assert_eq!(
        R::EmptyToNull.classify(&binary, 0, U::Characters).unwrap(),
        WouldChange(Exact)
    );
    let floats = Float64Array::from(vec![
        Some(0.0),
        Some(-0.0),
        Some(1e-130),
        Some(1e-131),
        Some(f64::NAN),
        None,
    ]);
    assert_eq!(
        R::NegativeZero.classify(&floats, 1, U::Characters).unwrap(),
        WouldChange(Exact)
    );
    for (row, expected) in [
        (0, Unchanged(Exact)),
        (2, Unchanged(Exact)),
        (3, WouldChange(Exact)),
        (4, HardReject),
        (5, Unchanged(Exact)),
    ] {
        assert_eq!(
            R::Underflow { min_abs: 1e-130 }
                .classify(&floats, row, U::Characters)
                .unwrap(),
            expected
        );
    }
    let times = TimestampMicrosecondArray::from(vec![Some(-1), Some(-1000), Some(0), None]);
    for (row, expected) in [
        (0, WouldChange(Exact)),
        (1, Unchanged(Exact)),
        (2, Unchanged(Exact)),
        (3, Unchanged(Exact)),
    ] {
        assert_eq!(
            R::FractionalSeconds { digits: 3 }
                .classify(&times, row, U::Characters)
                .unwrap(),
            expected
        );
    }
    assert!(R::FractionalSeconds { digits: 10 }
        .classify(&times, 3, U::Characters)
        .is_err());
    assert!(R::Underflow { min_abs: 0.0 }
        .classify(&floats, 5, U::Characters)
        .is_err());
    let unsupported = Int32Array::from(vec![None]);
    assert!(R::PadBinary { bytes: 2 }
        .classify(&unsupported, 0, U::Characters)
        .is_err());
    assert_eq!(unsupported.null_count(), 1);
}
