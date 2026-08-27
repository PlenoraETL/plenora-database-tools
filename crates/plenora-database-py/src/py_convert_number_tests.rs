use super::{classify_number, NumberKind};

/// Il confine fra intero con segno e intero senza: un conteggio oltre
/// `i64::MAX` deve restare un intero, non diventare un float.
#[test]
fn a_count_beyond_i64_stays_an_exact_integer() {
    let beyond = serde_json::Number::from(u64::try_from(i64::MAX).unwrap_or(0) + 1);
    assert_eq!(
        classify_number(&beyond),
        NumberKind::Unsigned(9_223_372_036_854_775_808)
    );

    let largest = serde_json::Number::from(u64::MAX);
    assert_eq!(classify_number(&largest), NumberKind::Unsigned(u64::MAX));
}

#[test]
fn the_signed_range_is_still_signed_and_floats_are_still_floats() {
    assert_eq!(
        classify_number(&serde_json::Number::from(i64::MIN)),
        NumberKind::Signed(i64::MIN)
    );
    assert_eq!(
        classify_number(&serde_json::Number::from(0)),
        NumberKind::Signed(0)
    );
    assert_eq!(
        classify_number(&serde_json::Number::from(i64::MAX)),
        NumberKind::Signed(i64::MAX)
    );

    let half = serde_json::Number::from_f64(0.5).expect("0.5 e finito");
    assert_eq!(classify_number(&half), NumberKind::Float(0.5));
}
