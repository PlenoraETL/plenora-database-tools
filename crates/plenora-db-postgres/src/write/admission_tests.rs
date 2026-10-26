use super::*;
use arrow_schema::{DataType, TimeUnit};

fn server() -> ServerFacts {
    ServerFacts {
        major: "16".to_owned(),
        version: "16.4".to_owned(),
        encoding: "UTF8".to_owned(),
    }
}

fn leaf(format: &str, typtype: &str, typmod: i32, shape: SessionShape) -> LeafType {
    LeafType {
        format: format.to_owned(),
        typtype: typtype.to_owned(),
        typmod,
        shape,
    }
}

#[test]
fn la_chiave_porta_forma_tipo_con_typmod_ed_encoding() {
    let (key, dimensions) = admission_key(
        &DataType::Decimal128(10, 2),
        &[leaf("numeric(10,2)", "b", 655_366, SessionShape::Scalar)],
        &server(),
    );
    assert_eq!(key.arrow, "decimal128(10,2)");
    assert_eq!(key.nativo, "scalare:numeric(10,2);UTF8");
    assert_eq!(key.dimensione, None);
    assert_eq!(dimensions, TargetDimensions::default());
    let (key, _) = admission_key(
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        &[leaf(
            "timestamp(3) without time zone",
            "b",
            3,
            SessionShape::Array,
        )],
        &server(),
    );
    assert_eq!(key.nativo, "array:timestamp(3) without time zone;UTF8");
}

#[test]
fn la_lunghezza_e_simbolica_e_va_nella_dimensione() {
    for (format, symbolic) in [
        ("character varying(255)", "character varying(n)"),
        ("character(3)", "character(n)"),
        ("bit varying(7)", "bit varying(n)"),
        ("bit(7)", "bit(n)"),
    ] {
        let (key, dimensions) = admission_key(
            &DataType::Utf8,
            &[leaf(format, "b", 0, SessionShape::Scalar)],
            &server(),
        );
        assert_eq!(key.nativo, format!("scalare:{symbolic};UTF8"), "{format}");
        assert!(key.dimensione.is_some(), "{format}");
        assert_eq!(dimensions.chars, key.dimensione);
    }
    let (key, _) = admission_key(
        &DataType::Utf8,
        &[leaf("character varying", "b", -1, SessionShape::Scalar)],
        &server(),
    );
    assert_eq!(key.nativo, "scalare:character varying;UTF8");
    assert_eq!(key.dimensione, None);
}

#[test]
fn ogni_enum_ha_la_stessa_chiave_e_un_composto_non_e_misurabile() {
    let (key, _) = admission_key(
        &DataType::Utf8,
        &[leaf("public.umore", "e", -1, SessionShape::Scalar)],
        &server(),
    );
    assert_eq!(key.nativo, "scalare:enum;UTF8");
    let (key, _) = admission_key(
        &DataType::Utf8,
        &[
            leaf("integer", "b", -1, SessionShape::Nested),
            leaf("text", "b", -1, SessionShape::Nested),
        ],
        &server(),
    );
    assert_eq!(key.nativo, "annidato:integer,text;UTF8");
}

#[test]
fn il_fixture_incorporato_e_valido() {
    admission_table().expect("fixture della regola A");
}

#[test]
fn una_major_non_misurata_si_rifiuta_con_ogni_politica() {
    let error = admission_table()
        .expect("fixture")
        .server("13")
        .expect_err("PostgreSQL 13 non e misurato");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert!(error
        .message
        .contains(plenora_database_core::write_admission::UNPROVEN_SERVER));
}
