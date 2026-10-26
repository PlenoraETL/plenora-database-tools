use super::{exact_f64, model, values, Kind, Model, Num, ServerFacts, Set, Stored, Unit};
use plenora_database_core::provider::ParameterValue as V;

fn exact(value: &V, kind: Kind) -> bool {
    matches!(model(value, kind, ServerFacts::default()), Model::Exact(_))
}

#[test]
fn numbers_parse_and_expand_exactly() {
    assert_eq!(Num::parse(".05").map(|n| n.text()), Some("0.05".to_owned()));
    assert_eq!(
        Num::parse("-.0500").map(|n| n.text()),
        Some("-0.05".to_owned())
    );
    assert_eq!(
        Num::parse("1E+30").map(|n| n.text()),
        Some(format!("1{}", "0".repeat(30)))
    );
    assert_eq!(
        Num::parse("1.5E-3").map(|n| n.text()),
        Some("0.0015".to_owned())
    );
    assert_eq!(Num::parse("-0").map(|n| n.text()), Some("0".to_owned()));
    assert_eq!(Num::parse("abc"), None);
    assert_eq!(
        exact_f64(0.1).map(|n| n.text()),
        Some("0.1000000000000000055511151231257827021181583404541015625".to_owned())
    );
    assert_eq!(exact_f64(-2.5).map(|n| n.text()), Some("-2.5".to_owned()));
    assert_eq!(
        exact_f64(9_007_199_254_740_992.0).map(|n| n.text()),
        Some("9007199254740992".to_owned())
    );
    assert_eq!(exact_f64(5e-324).map(|n| n.fraction_digits()), Some(1074));
}

#[test]
fn the_model_is_independent_of_the_admission_rule() {
    let decimal = Kind::Decimal {
        precision: 12,
        scale: 4,
    };
    assert!(exact(&V::Decimal("1.2345".to_owned()), decimal));
    assert!(!exact(&V::Decimal("1.23456".to_owned()), decimal));
    assert!(!exact(&V::F64(0.1), decimal));
    assert!(exact(&V::F64(0.5), decimal));
    assert!(exact(&V::Decimal("0.0005".to_owned()), decimal));
    let rounded = Kind::Decimal {
        precision: 6,
        scale: -2,
    };
    assert!(exact(&V::I64(12_300), rounded));
    assert!(!exact(&V::I64(12_345), rounded));
    // `FLOAT(7, 1)`: 1.25 si arrotonda.
    let float71 = Kind::ScaledFloat {
        bits: 24,
        precision: 7,
        scale: 1,
    };
    assert!(!exact(&V::F64(1.25), float71));
    assert!(exact(&V::F64(1.5), float71));
    assert!(!exact(&V::Decimal("2.75".to_owned()), float71));
    let double = Kind::ScaledFloat {
        bits: 53,
        precision: 10,
        scale: 1,
    };
    assert!(!exact(&V::F64(0.1), double));
    assert!(exact(&V::F64(-2.5), double));
    let text = Kind::Text {
        length: Some(3),
        padded: false,
        unit: Unit::Characters,
        set: Set::Unicode,
    };
    assert!(!exact(&V::String("abc   ".to_owned()), text));
    assert!(exact(&V::String("abc".to_owned()), text));
    let cp1252 = Kind::Text {
        length: Some(20),
        padded: false,
        unit: Unit::Bytes,
        set: Set::Windows1252,
    };
    assert!(!exact(&V::String("\u{142}x".to_owned()), cp1252));
    assert!(exact(&V::String("\u{20AC}\u{E8}".to_owned()), cp1252));
    let padded = Kind::Text {
        length: Some(4),
        padded: true,
        unit: Unit::Characters,
        set: Set::Unicode,
    };
    assert_eq!(
        model(&V::String("ab".to_owned()), padded, ServerFacts::default()),
        Model::Exact(Stored::Text("ab  ".to_owned()))
    );
    let timestamp = Kind::Timestamp { fraction: 0 };
    assert!(!exact(
        &V::Timestamp("2024-01-01T10:20:30.6".to_owned()),
        timestamp
    ));
    assert!(exact(
        &V::Timestamp("2024-01-01T10:20:30.000".to_owned()),
        timestamp
    ));
    let facts = ServerFacts {
        normalizes_negative_zero: true,
        ..ServerFacts::default()
    };
    assert!(matches!(
        model(&V::F64(-0.0), Kind::Float { bits: 53 }, facts),
        Model::Declared(_)
    ));
}

#[test]
fn every_family_generates_the_classes_the_points_missed() {
    let numeric = values(Kind::Decimal {
        precision: 12,
        scale: 4,
    });
    for needed in ["0.05", "0.0005", "-0.005", "1.2300", "12345.67891"] {
        assert!(
            numeric.contains(&V::Decimal(needed.to_owned())),
            "manca {needed}"
        );
    }
    let text = values(Kind::Text {
        length: Some(3),
        padded: false,
        unit: Unit::Characters,
        set: Set::Unicode,
    });
    for needed in ["", "xxxx", "xxx ", "\u{1F600}", "\u{142}\u{119}\u{65E5}x"] {
        assert!(
            text.contains(&V::String(needed.to_owned())),
            "manca {needed:?}"
        );
    }
    let temporal = values(Kind::Timestamp { fraction: 3 });
    for digits in 0..=9 {
        let fraction = if digits == 0 {
            String::new()
        } else {
            format!(".{}", &"123456789"[..digits])
        };
        assert!(
            temporal.contains(&V::Timestamp(format!("2024-01-01T10:20:30{fraction}"))),
            "manca la frazione di {digits} cifre"
        );
    }
}
