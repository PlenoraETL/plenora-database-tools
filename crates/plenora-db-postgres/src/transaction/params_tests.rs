use super::*;
use plenora_database_core::ErrorCategory;

fn encode(v: &ParameterValue) -> Result<SqlParam> {
    encode_param(v)
}

#[test]
fn scalar_variants_are_encoded_end_to_end() {
    assert!(matches!(
        encode(&ParameterValue::Bool(true)).unwrap(),
        SqlParam::Bool(true)
    ));
    assert!(matches!(
        encode(&ParameterValue::I32(42)).unwrap(),
        SqlParam::I32(42)
    ));
    assert!(matches!(
        encode(&ParameterValue::I64(-42)).unwrap(),
        SqlParam::I64(-42)
    ));
    assert!(matches!(
        encode(&ParameterValue::F64(3.5)).unwrap(),
        SqlParam::F64(_)
    ));
    assert!(matches!(
        encode(&ParameterValue::String("s".into())).unwrap(),
        SqlParam::String(_)
    ));
    assert!(matches!(
        encode(&ParameterValue::Bytes(vec![1, 2])).unwrap(),
        SqlParam::Bytes(_)
    ));
    assert!(matches!(
        encode(&ParameterValue::Json(serde_json::json!({"k": "v"}))).unwrap(),
        SqlParam::Json(_)
    ));
}

#[test]
fn temporal_scalars_require_iso8601_or_rfc3339() {
    assert!(matches!(
        encode(&ParameterValue::Date("2026-08-12".into())).unwrap(),
        SqlParam::Date(_)
    ));
    assert!(matches!(
        encode(&ParameterValue::Timestamp("2026-08-12T10:00:00".into())).unwrap(),
        SqlParam::Timestamp(_)
    ));
    assert!(matches!(
        encode(&ParameterValue::TimestampTz("2026-08-12T10:00:00Z".into())).unwrap(),
        SqlParam::TimestampTz(_)
    ));

    assert_eq!(
        encode(&ParameterValue::Date("12/08/2026".into()))
            .unwrap_err()
            .category,
        ErrorCategory::Unsupported
    );
    assert_eq!(
        encode(&ParameterValue::Timestamp("nope".into()))
            .unwrap_err()
            .category,
        ErrorCategory::Unsupported
    );
    assert_eq!(
        encode(&ParameterValue::TimestampTz("2026-08-12 10:00:00".into()))
            .unwrap_err()
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn uuid_validates_length_36() {
    let ok = "11111111-2222-3333-4444-555555555555";
    assert!(matches!(
        encode(&ParameterValue::Uuid(ok.into())).unwrap(),
        SqlParam::Uuid { .. }
    ));

    let short = "not-a-uuid";
    assert_eq!(
        encode(&ParameterValue::Uuid(short.into()))
            .unwrap_err()
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn enum_is_encoded_as_text_label() {
    let encoded = encode(&ParameterValue::Enum {
        type_name: "mood".into(),
        label: "sad".into(),
    })
    .unwrap();
    match encoded {
        SqlParam::String(s) => assert_eq!(s, "sad"),
        _ => panic!("enum deve essere encoded come text"),
    }
}

#[test]
fn wkb_is_rejected_from_oltp_path() {
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    let wkb_err = encode(&ParameterValue::Wkb {
        bytes: vec![1, 2, 3],
        srid: Some(4326),
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    })
    .unwrap_err();
    assert_eq!(wkb_err.category, ErrorCategory::Unsupported);
}

#[test]
fn decimal_is_encoded_with_dual_representation() {
    let encoded = encode(&ParameterValue::Decimal("1234.56".into())).unwrap();
    match encoded {
        SqlParam::Decimal { text, .. } => assert_eq!(text, "1234.56"),
        _ => panic!("Decimal deve essere encoded come SqlParam::Decimal"),
    }
}

#[test]
fn decimal_invalid_format_is_rejected() {
    let err = encode(&ParameterValue::Decimal("non-numerico".into())).unwrap_err();
    assert_eq!(err.category, ErrorCategory::Unsupported);
}

#[test]
fn uuid_is_encoded_with_dual_representation() {
    let encoded = encode(&ParameterValue::Uuid(
        "550e8400-e29b-41d4-a716-446655440000".into(),
    ))
    .unwrap();
    match encoded {
        SqlParam::Uuid { text, binary } => {
            assert_eq!(text, "550e8400-e29b-41d4-a716-446655440000");
            assert_eq!(binary.len(), 16);
            // Primo byte del UUID di test: 0x55.
            assert_eq!(binary[0], 0x55);
            assert_eq!(binary[15], 0x00);
        }
        _ => panic!("Uuid deve essere encoded come SqlParam::Uuid"),
    }
}

#[test]
fn uuid_invalid_hex_is_rejected() {
    let err = encode(&ParameterValue::Uuid(
        "ZZZe8400-e29b-41d4-a716-446655440000".into(),
    ))
    .unwrap_err();
    assert_eq!(err.category, ErrorCategory::Unsupported);
}

#[test]
fn null_type_hint_maps_to_pg_type_with_text_fallback() {
    assert_eq!(map_null_type("bool"), Type::BOOL);
    assert_eq!(map_null_type("BOOLEAN"), Type::BOOL);
    assert_eq!(map_null_type("integer"), Type::INT4);
    assert_eq!(map_null_type("int8"), Type::INT8);
    assert_eq!(map_null_type("float8"), Type::FLOAT8);
    assert_eq!(map_null_type("text"), Type::TEXT);
    assert_eq!(map_null_type("varchar"), Type::TEXT);
    assert_eq!(map_null_type("bytea"), Type::BYTEA);
    assert_eq!(map_null_type("date"), Type::DATE);
    assert_eq!(map_null_type("timestamp"), Type::TIMESTAMP);
    assert_eq!(map_null_type("timestamptz"), Type::TIMESTAMPTZ);
    assert_eq!(map_null_type("uuid"), Type::UUID);
    assert_eq!(map_null_type("json"), Type::JSON);
    assert_eq!(map_null_type("jsonb"), Type::JSONB);
    // fallback dichiarato: qualsiasi type hint sconosciuto → TEXT.
    assert_eq!(map_null_type("hstore"), Type::TEXT);
    assert_eq!(map_null_type(""), Type::TEXT);
}

#[test]
fn null_variant_is_encoded_with_the_declared_type() {
    let encoded = encode(&ParameterValue::Null {
        type_name: "uuid".into(),
    })
    .unwrap();
    match encoded {
        SqlParam::Null(t) => assert_eq!(t, Type::UUID),
        _ => panic!("Null deve essere encoded come SqlParam::Null"),
    }
}

#[test]
fn encode_params_preserves_order_and_length() {
    let vs = vec![
        ParameterValue::I32(1),
        ParameterValue::String("two".into()),
        ParameterValue::Bool(false),
    ];
    let encoded = encode_params(&vs).unwrap();
    assert_eq!(encoded.len(), 3);
    assert!(matches!(encoded[0], SqlParam::I32(1)));
    assert!(matches!(&encoded[1], SqlParam::String(s) if s == "two"));
    assert!(matches!(encoded[2], SqlParam::Bool(false)));
}

#[test]
fn encode_params_short_circuits_on_first_error() {
    let vs = vec![
        ParameterValue::I32(1),
        ParameterValue::Uuid("bad".into()),
        ParameterValue::I32(2),
    ];
    assert!(encode_params(&vs).is_err());
}

#[test]
fn debug_impl_redacts_the_value() {
    let s = format!(
        "{:?}",
        SqlParam::String("segreto-che-non-deve-comparire".into())
    );
    assert!(
        !s.contains("segreto"),
        "Debug non deve rivelare i valori: {s}"
    );
    assert!(s.contains("REDACTED"));
}

#[test]
fn small_i32_is_serialized_with_the_prepared_int8_width() {
    let mut encoded = BytesMut::new();
    SqlParam::I32(1)
        .to_sql(&Type::INT8, &mut encoded)
        .expect("i32 ampliabile a int8");
    assert_eq!(encoded.len(), 8);
}

#[test]
fn bind_validation_reports_only_position_and_types() {
    let sentinel = 8_675_309_i64;
    let error = validate_parameter_targets(&[ParameterValue::I64(sentinel)], &[Type::UUID])
        .expect_err("i64 non e un UUID");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(error.message.contains("parametro 1"));
    assert!(error.message.contains("i64"));
    assert!(error.message.contains("uuid"));
    assert!(!error.message.contains(&sentinel.to_string()));
}

#[test]
fn integer_narrowing_is_range_checked_before_serialization() {
    let sentinel = i64::from(i32::MAX) + 1;
    let error = validate_parameter_targets(&[ParameterValue::I64(sentinel)], &[Type::INT4])
        .expect_err("i64 fuori range int4");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(!error.message.contains(&sentinel.to_string()));
}
