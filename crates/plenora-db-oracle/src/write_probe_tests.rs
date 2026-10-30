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
    spec: &ProbeSpec,
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
        // Sopra 4000 byte la sonda promuove String a un locator CLOB.
        // Il fork non sa legarlo come NCLOB (write_admission): non e una
        // misura del server. Si interrompe prima del checkout, senza referto.
        if probe_kind(spec) == "NCLOB"
            && matches!(&value, plenora_database_core::provider::ParameterValue::String(text) if text.len() > 4_000)
        {
            return Err(DatabaseError::unsupported(
                plenora_database_core::plan::ProviderKind::Oracle,
                ErrorPhase::Prepare,
                "sonda NCLOB senza trasporto nazionale LOB qualificato",
            ));
        }
        Ok(value)
    }
}

#[test]
fn nclob_probe_cannot_promote_text_to_an_unqualified_clob_locator() {
    let spec = ProbeSpec::new("NCLOB", DataType::Utf8);
    for text in ["x".repeat(4_001), "\u{e9}".repeat(2_001)] {
        let array = StringArray::from(vec![Some(text)]);
        let error = probe_value(&spec, &array, 0).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }
    let boundary = StringArray::from(vec![Some("x".repeat(4_000)), None]);
    assert!(probe_value(&spec, &boundary, 0).is_ok());
    assert!(probe_value(&spec, &boundary, 1).is_ok());
    let clob = ProbeSpec::new("CLOB", DataType::Utf8);
    assert!(probe_value(&clob, &StringArray::from(vec!["x".repeat(4_001)]), 0).is_ok());
}
