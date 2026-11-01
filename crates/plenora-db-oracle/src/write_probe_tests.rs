//! Preparazione e diagnostica delle sonde, verificabili senza server.
use plenora_database_core::arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::{DataType, Field};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase};
use plenora_database_testkit::write_probe::ProbeSpec;

// Solo la famiglia statica della sonda: mai DDL, valori o testo del driver.
pub fn probe_kind(spec: &ProbeSpec) -> &'static str {
    match spec
        .native_ddl
        .split_whitespace()
        .next()
        .unwrap_or_default()
    {
        "JSON" => "JSON",
        "NCLOB" => "NCLOB",
        "CLOB" => "CLOB",
        "BLOB" => "BLOB",
        "LONG" => "LONG",
        _ => "scalare",
    }
}

pub fn probe_failure(spec: &ProbeSpec, row: usize, error: &DatabaseError) -> String {
    format!(
        "sonda Oracle tipo={} indice={row}: categoria={:?} fase={:?} effetto={:?}",
        probe_kind(spec),
        error.category,
        error.phase,
        error.remote_effect
    )
}

#[test]
fn probe_diagnostics_do_not_expose_payload_or_driver_messages() {
    let spec = ProbeSpec::new("NCLOB", DataType::Utf8);
    let error = DatabaseError::new(
        ErrorCategory::Io,
        ErrorPhase::Read,
        Some(plenora_database_core::plan::ProviderKind::Oracle),
        "SECRET",
    );
    let diagnostic = probe_failure(&spec, 7, &error);
    assert!(diagnostic.contains("tipo=NCLOB indice=7"));
    assert!(diagnostic.contains("Read"));
    assert!(!diagnostic.contains("SECRET"));
}

pub fn probe_value(
    _spec: &ProbeSpec,
    values: &dyn Array,
    row: usize,
) -> plenora_database_core::Result<plenora_database_core::provider::ParameterValue> {
    let field = Field::new("V0", values.data_type().clone(), true);
    if matches!(values.data_type(), DataType::Timestamp(_, Some(_))) && !values.is_null(row) {
        let array = values
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp");
        let instant = chrono::DateTime::from_timestamp_micros(array.value(row)).expect("istante");
        Ok(plenora_database_core::provider::ParameterValue::String(
            instant.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string(),
        ))
    } else {
        let value =
            plenora_database_engine::arrow_write::arrow_parameter_value(values, &field, row, 'T')?;
        Ok(value)
    }
}

/// Il locator resta CLOB sul wire; la conversione nazionale e del server.
/// Non qualifica il writer NCLOB: la sonda deve ancora rileggere il valore.
pub fn probe_operand(spec: &ProbeSpec) -> &'static str {
    if probe_kind(spec) == "NCLOB" {
        "TO_NCLOB(:1)"
    } else {
        ":1"
    }
}

#[test]
fn nclob_probe_uses_explicit_server_conversion_without_national_locator() {
    let spec = ProbeSpec::new("NCLOB", DataType::Utf8);
    for text in ["x".repeat(4_001), "\u{e9}".repeat(2_001)] {
        let array = StringArray::from(vec![Some(text.clone())]);
        let value = probe_value(&spec, &array, 0).unwrap();
        assert_eq!(
            value,
            plenora_database_core::provider::ParameterValue::String(text)
        );
        assert_eq!(probe_operand(&spec), "TO_NCLOB(:1)");
    }
    let boundary = StringArray::from(vec![Some("x".repeat(4_000)), None]);
    assert!(probe_value(&spec, &boundary, 0).is_ok());
    assert!(probe_value(&spec, &boundary, 1).is_ok());
    for native in ["CLOB", "BLOB", "VARCHAR2(10)"] {
        assert_eq!(probe_operand(&ProbeSpec::new(native, DataType::Utf8)), ":1");
    }
}

/// Il server serializza JSON senza passare dal decoder OSON del driver.
pub fn probe_projection(spec: &ProbeSpec) -> &'static str {
    if spec.document {
        "JSON_SERIALIZE(V0 RETURNING CLOB)"
    } else {
        "V0"
    }
}

#[test]
fn json_measurement_uses_server_text_without_changing_other_types() {
    let json = ProbeSpec::new("JSON", DataType::Utf8).document();
    assert_eq!(probe_kind(&json), "JSON");
    assert_eq!(probe_projection(&json), "JSON_SERIALIZE(V0 RETURNING CLOB)");
    for native in ["CLOB", "NCLOB", "VARCHAR2(10)"] {
        assert_eq!(
            probe_projection(&ProbeSpec::new(native, DataType::Utf8)),
            "V0"
        );
    }
}
