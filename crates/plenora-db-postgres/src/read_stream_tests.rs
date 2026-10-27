use super::*;
use plenora_database_core::{RemoteEffect, RetryDisposition};

fn column(name: &str) -> ColumnSpec {
    ColumnSpec {
        name: name.to_owned(),
        native_type: "geometry".to_owned(),
        nullable: true,
        numeric_precision: None,
        numeric_scale: None,
        spatial_srid: None,
        spatial_dimensions: None,
        spatial_type: None,
        spatial_crs_id: None,
        default_expression: None,
        identity_kind: None,
        generated_kind: None,
        native_declaration: None,
        type_kind: None,
        composite_fields: Vec::new(),
        enum_labels: Vec::new(),
        domain_base_type: None,
        domain_constraints: Vec::new(),
        collation: None,
        source: None,
        registered_field_id: None,
        kind: ColumnKind::Geometry,
    }
}

/// Un difetto di conversione osservato prima della consegna del batch
/// pubblica l'indice sorgente assoluto del result set.
#[test]
fn a_read_conversion_defect_publishes_the_absolute_source_index() {
    let mut tracker = ReadDiagnosticsTracker::default();
    tracker.publish_batch(4_096).expect("batch pubblicato");
    let columns = [column("parcel_id"), column("shape")];

    let error = attribute_conversion_defect(
        &tracker,
        &columns,
        read_mapping_error("valore PostgreSQL non rappresentabile"),
        Some(903),
        Some(1),
    );
    assert_eq!(error.phase, ErrorPhase::Read);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    let report = error.row_diagnostics().expect("diagnostica PostgreSQL");
    report.validate().expect("documento valido");
    assert_eq!(
        serde_json::to_value(report).expect("documento serializzabile"),
        serde_json::json!({
            "contract": "plenora-row-diagnostics-v1",
            "scope": "read",
            "index_basis": "source_row_zero_based",
            "completeness": "partial",
            "knowledge_limits": [
                "read.batches_already_published",
                "read.scan_stopped_at_first_defect"
            ],
            "observed_total": 1,
            "counts": {"conversion.value_not_representable": 1},
            "examples_limit": 10,
            "examples_truncated": false,
            "examples": [{
                "source_index": 4_999,
                "cause": "conversion.value_not_representable",
                "column": "shape"
            }]
        })
    );
}

/// Provenienza e completezza non vengono inventate: né su una riga
/// sconosciuta né su un errore che non è un difetto di conversione.
#[test]
fn unattributable_read_failures_never_invent_provenance() {
    let tracker = ReadDiagnosticsTracker::default();
    let columns = [column("shape")];

    let unknown_row = attribute_conversion_defect(
        &tracker,
        &columns,
        read_mapping_error("difetto"),
        None,
        Some(0),
    );
    let report = unknown_row
        .row_diagnostics()
        .expect("diagnostica PostgreSQL");
    report.validate().expect("documento valido");
    assert_eq!(
        report.completeness,
        plenora_database_core::row_diagnostics::Completeness::Unknown
    );
    assert!(report.examples.is_empty());

    let missing_column = attribute_conversion_defect(
        &tracker,
        &columns,
        read_mapping_error("difetto"),
        Some(2),
        Some(9),
    );
    let report = missing_column
        .row_diagnostics()
        .expect("diagnostica PostgreSQL");
    assert_eq!(report.examples[0].source_index, 2);
    assert!(report.examples[0].column.is_none());

    let budget = attribute_conversion_defect(
        &tracker,
        &columns,
        DatabaseError::resource_limit("budget PostgreSQL esaurito"),
        Some(2),
        Some(0),
    );
    assert_eq!(budget.category, ErrorCategory::ResourceLimit);
    assert!(budget.row_diagnostics().is_none());
}

/// Un punto EWKB XY little endian, con lo SRID se dato.
fn ewkb_point(srid: Option<u32>) -> Vec<u8> {
    let mut bytes = vec![1_u8];
    let code = 1_u32 | if srid.is_some() { 0x2000_0000 } else { 0 };
    bytes.extend_from_slice(&code.to_le_bytes());
    if let Some(srid) = srid {
        bytes.extend_from_slice(&srid.to_le_bytes());
    }
    bytes.extend_from_slice(&[0_u8; 16]);
    bytes
}

/// GEO-009, regola del produttore: una colonna senza SRID nel catalogo (senza
/// typmod) si pubblica `crs_resolution=missing`, e un suo valore con SRID si
/// rifiuta `crs` in lettura invece di uscire in uno schema non conforme. Fino
/// al 24/10 usciva, e la scrittura PostgreSQL lo rifiutava rileggendolo.
#[test]
fn a_typmod_less_column_with_srid_values_is_refused_at_read() {
    let tracker = ReadDiagnosticsTracker::default();
    let untyped = column("shape");
    let mut typed = column("shape");
    typed.spatial_srid = Some(4_326);
    let batch = |values: Vec<Option<Vec<u8>>>| {
        let array = arrow_array::BinaryArray::from_iter(values);
        RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "shape",
                arrow_schema::DataType::Binary,
                true,
            )])),
            vec![Arc::new(array)],
        )
        .expect("batch")
    };
    let limits = |columns: &[ColumnSpec], batch: &RecordBatch| {
        enforce_batch_limits(batch, columns, u64::MAX, u64::MAX, 1_000, 8, &tracker)
    };
    let with_srid = batch(vec![Some(ewkb_point(Some(4_326))), None]);
    let error =
        limits(std::slice::from_ref(&untyped), &with_srid).expect_err("SRID senza dichiarazione");
    assert_eq!(error.category, ErrorCategory::Crs);
    assert_eq!(error.phase, ErrorPhase::Read);
    // Senza SRID nei valori (SRID 0, che `ST_AsEWKB` omette) la colonna passa.
    limits(
        std::slice::from_ref(&untyped),
        &batch(vec![Some(ewkb_point(None))]),
    )
    .expect("nessuno SRID");
    // Con uno SRID dichiarato: lo stesso passa, uno diverso o nessuno no.
    limits(std::slice::from_ref(&typed), &with_srid).expect("SRID dichiarato");
    for other in [Some(3_857), None] {
        let other = batch(vec![Some(ewkb_point(other))]);
        assert_eq!(
            limits(std::slice::from_ref(&typed), &other)
                .expect_err("SRID diverso o assente")
                .category,
            ErrorCategory::Crs
        );
    }
}
