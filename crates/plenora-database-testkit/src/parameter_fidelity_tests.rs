use super::{canon, neighbours, Neighbour};
use plenora_database_core::provider::ParameterValue as V;

#[test]
fn refusal_before_send_and_after_prepare_require_distinct_envelopes() {
    use super::{refused, refused_after_prepare};
    use plenora_database_core::{
        DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
    };

    let category = ErrorCategory::DataMapping;
    let local = DatabaseError::new(category, ErrorPhase::Prepare, None, "bind incompatibile");
    assert!(refused(&Err(local.clone()), category).is_ok());
    assert!(refused_after_prepare(&Err(local.clone()), category).is_err());
    let sent = local.after_unproven_send();
    assert!(refused(&Err(sent.clone()), category).is_err());
    assert!(refused_after_prepare(&Err(sent.clone()), category).is_ok());
    for invalid in [
        DatabaseError {
            category: ErrorCategory::Unsupported,
            ..sent.clone()
        },
        DatabaseError {
            phase: ErrorPhase::Write,
            ..sent.clone()
        },
        DatabaseError {
            remote_effect: RemoteEffect::RolledBack,
            ..sent.clone()
        },
        DatabaseError {
            retry: RetryDisposition::Never,
            ..sent
        },
    ] {
        assert!(refused_after_prepare(&Err(invalid), category).is_err());
    }
    for rows in [0, 1] {
        assert!(refused(&Ok(rows), category).is_err());
        assert!(refused_after_prepare(&Ok(rows), category).is_err());
    }
}

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

#[test]
fn comparison_refusal_is_distinct_from_zero_rows() {
    use super::{judge, Comparison};
    use plenora_database_core::ErrorCategory;
    let comparison = Comparison {
        value: V::Json(serde_json::json!({"a":1})),
        expected: 1,
        kind: Neighbour::SameType,
    };
    let mut notes = Vec::new();
    let refusal = Err("InvalidPlan/Prepare: OPERAND_FAMILY_MISMATCH".into());
    assert!(judge(&comparison, &refusal, None, false, &mut notes, "eq").is_err());
    assert!(judge(
        &comparison,
        &refusal,
        Some(ErrorCategory::InvalidPlan),
        false,
        &mut notes,
        "eq"
    )
    .is_ok());
    for count in [0, 1, 2] {
        assert!(judge(
            &comparison,
            &Ok(count),
            Some(ErrorCategory::InvalidPlan),
            false,
            &mut notes,
            "eq"
        )
        .is_err());
    }
    assert!(judge(&comparison, &Ok(0), None, false, &mut notes, "eq").is_err());
    assert!(judge(&comparison, &Ok(1), None, false, &mut notes, "eq").is_ok());
    assert!(notes
        .iter()
        .all(|n| n.contains("rifiuto esplicito") && !n.contains("None righe")));
}

#[test]
fn refusal_requires_expected_category_prepare_and_no_remote_effect() {
    use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect};
    let error = DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Prepare,
        None,
        "rifiuto",
    );
    assert!(super::refused(&Err(error.clone()), ErrorCategory::DataMapping).is_ok());
    assert!(super::refused(&Ok(1), ErrorCategory::DataMapping).is_err());
    assert!(super::refused(&Err(error.clone()), ErrorCategory::Unsupported).is_err());
    for effect in [RemoteEffect::RolledBack, RemoteEffect::Unknown] {
        let mut late = error.clone();
        late.remote_effect = effect;
        assert!(super::refused(&Err(late), ErrorCategory::DataMapping).is_err());
    }
    let mut late = error;
    late.phase = ErrorPhase::Write;
    assert!(super::refused(&Err(late), ErrorCategory::DataMapping).is_err());
}
