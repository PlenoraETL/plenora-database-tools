use super::{canon, neighbours, Neighbour};
use plenora_database_core::provider::ParameterValue as V;

#[test]
fn canonical_forms_compare_values_not_spellings() {
    assert_eq!(
        canon(&V::Decimal("012.500".to_owned())),
        canon(&V::Decimal("12.5".to_owned()))
    );
    assert_eq!(
        canon(&V::I64(150)),
        canon(&V::Decimal("150.0000".to_owned()))
    );
    assert_ne!(canon(&V::F64(0.0)), canon(&V::F64(-0.0)));
    assert_eq!(
        canon(&V::Timestamp("2024-01-01 10:20:30.1".to_owned())),
        canon(&V::Timestamp("2024-01-01T10:20:30.100000".to_owned()))
    );
    assert_eq!(
        canon(&V::TimestampTz("2024-01-01T10:20:30+02:00".to_owned())),
        canon(&V::TimestampTz("2024-01-01T08:20:30.000000Z".to_owned()))
    );
}

#[test]
fn neighbours_cover_each_family() {
    let two_53 = 1_i64 << 53;
    let integer = neighbours(&V::I64(two_53 + 1));
    assert!(integer.contains(&(V::I64(two_53 + 2), Neighbour::SameType)));
    #[allow(clippy::cast_precision_loss)]
    let rounded = V::F64((two_53 + 1) as f64);
    assert!(integer.contains(&(rounded, Neighbour::CrossType)));
    let float = neighbours(&V::F64(0.1));
    assert!(float
        .iter()
        .any(|(value, kind)| *kind == Neighbour::CrossType
            && matches!(value, V::Decimal(text) if text.starts_with("0.1000000000000000055511151231257827"))));
    let decimal = neighbours(&V::Decimal("12345.6789".to_owned()));
    assert!(decimal.contains(&(
        V::Decimal("12345.6789000001".to_owned()),
        Neighbour::SameType
    )));
    let stamp = neighbours(&V::Timestamp("2024-02-29T10:00:00".to_owned()));
    assert!(stamp.contains(&(V::Date("2024-02-29".to_owned()), Neighbour::CrossType)));
    assert!(stamp.contains(&(
        V::Timestamp("2024-02-29T10:00:00.000001".to_owned()),
        Neighbour::SameType
    )));
    let text = neighbours(&V::String("ab".to_owned()));
    assert!(text.contains(&(V::String("ab ".to_owned()), Neighbour::Collation)));
}
