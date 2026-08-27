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
