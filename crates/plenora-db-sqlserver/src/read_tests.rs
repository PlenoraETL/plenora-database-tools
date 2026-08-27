use super::*;
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::{RemoteEffect, ResourceLimits, RetryDisposition};

#[test]
fn batch_row_configuration_is_bounded() {
    assert!(validate_batch_rows(0).is_err());
    assert!(validate_batch_rows(1).is_ok());
    assert!(validate_batch_rows(MAX_CONFIGURED_BATCH_ROWS).is_ok());
    assert!(validate_batch_rows(MAX_CONFIGURED_BATCH_ROWS + 1).is_err());
}

#[test]
fn reservations_fail_before_zero_or_missing_geometry_budget() {
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    assert!(reserve_batch(&budget, 0, &[]).is_err());
}

fn column(name: &str) -> crate::SqlServerColumnSpec {
    crate::SqlServerColumnSpec {
        name: name.to_owned(),
        native_type: "geometry".to_owned(),
        native_declaration: "geometry".to_owned(),
        nullable: true,
        collation: None,
        kind: crate::SqlServerColumnKind::Geometry,
        spatial_srid: None,
        spatial_dimensions: None,
        wire_encoding: crate::SqlServerWireEncoding::Projected,
    }
}

/// Un difetto di conversione osservato prima della consegna del batch
/// pubblica l'indice sorgente assoluto del result set.
#[test]
fn a_read_conversion_defect_publishes_the_absolute_source_index() {
    let mut tracker = ReadDiagnosticsTracker::default();
    tracker.publish_batch(1_024).expect("batch pubblicato");
    let columns = [column("parcel_id"), column("shape")];

    let error = attribute_conversion_defect(
        &tracker,
        &columns,
        read_error(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            "valore SQL Server non rappresentabile",
        ),
        Some(7),
        Some(1),
    );
    assert_eq!(error.phase, ErrorPhase::Read);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    let report = error.row_diagnostics().expect("diagnostica SQL Server");
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
                "source_index": 1_031,
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
        read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
        None,
        Some(0),
    );
    let report = unknown_row
        .row_diagnostics()
        .expect("diagnostica SQL Server");
    report.validate().expect("documento valido");
    assert_eq!(
        report.completeness,
        plenora_database_core::row_diagnostics::Completeness::Unknown
    );
    assert!(report.examples.is_empty());

    let missing_column = attribute_conversion_defect(
        &tracker,
        &columns,
        read_error(ErrorCategory::DataMapping, ErrorPhase::Read, "difetto"),
        Some(4),
        Some(9),
    );
    let report = missing_column
        .row_diagnostics()
        .expect("diagnostica SQL Server");
    assert_eq!(report.examples[0].source_index, 4);
    assert!(report.examples[0].column.is_none());

    let protocol = attribute_conversion_defect(
        &tracker,
        &columns,
        read_error(
            ErrorCategory::Protocol,
            ErrorPhase::Read,
            "result set inatteso",
        ),
        Some(4),
        Some(0),
    );
    assert_eq!(protocol.category, ErrorCategory::Protocol);
    assert!(protocol.row_diagnostics().is_none());
}

/// Un batch che non supera la validazione spaziale chiude lo stream.
///
/// Senza terminalizzazione una `next_batch` successiva ripartirebbe dalle
/// righe seguenti, saltando in silenzio quelle del batch fallito: è
/// esattamente il drop silenzioso che il contratto vieta.
#[tokio::test]
async fn a_failed_spatial_batch_terminalizes_the_stream() {
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::arrow::{DataType, Field, Schema};

    // La colonna è dichiarata spatial nel piano ma l'array non è Binary:
    // `validate_spatial_batch` fallisce sul downcast, senza bisogno di
    // righe TDS reali.
    let columns = vec![column("shape")];
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "shape",
        DataType::Int64,
        true,
    )]));
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (sender, receiver) = mpsc::channel(1);
    let cancellation = CancellationToken::new();
    let reservation = reserve_batch(&budget, 8, &columns).expect("reservation");
    let mut stream = SqlServerBatchStream {
        receiver,
        columns,
        schema: Arc::clone(&schema),
        batch_rows: 8,
        budget: budget.clone(),
        cancellation: cancellation.clone(),
        deadline_task: tokio::spawn(async {}),
        _operation_lease: budget
            .try_lease(ResourceKind::ConcurrentOperations, 1)
            .expect("operation lease"),
        _columns_lease: budget.try_lease(ResourceKind::Columns, 1).expect("columns"),
        finished: false,
        read_diagnostics: ReadDiagnosticsTracker::default(),
    };

    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64]))])
        .expect("batch non spaziale");
    let error = stream
        .finish_batch(batch, reservation)
        .expect_err("il batch non supera la validazione spaziale");
    assert_eq!(error.category, ErrorCategory::Internal);

    assert!(stream.finished, "lo stream deve restare chiuso");
    assert!(
        cancellation.is_cancelled(),
        "il batch fallito deve cancellare lo stream"
    );
    assert_eq!(
        stream.read_diagnostics.published_rows(),
        0,
        "un batch mai consegnato non ha righe pubblicate"
    );

    // Una riga resta in coda: se lo stream ripartisse, la consumerebbe
    // saltando il batch fallito.
    drop(sender);
    assert!(
        stream
            .next_batch(&cancellation)
            .await
            .expect("stream chiuso")
            .is_none(),
        "una next_batch successiva non deve riprendere dopo un batch fallito"
    );
}

#[test]
fn spatial_dimension_profile_is_exact_and_fail_closed() {
    assert_eq!(
        spatial_dimensions_from_profile(0, None).expect("empty"),
        Dimensions::Unknown
    );
    assert_eq!(
        spatial_dimensions_from_profile(1, Some(0)).expect("xy"),
        Dimensions::Xy
    );
    assert_eq!(
        spatial_dimensions_from_profile(1, Some(1)).expect("xym"),
        Dimensions::Xym
    );
    assert_eq!(
        spatial_dimensions_from_profile(1, Some(2)).expect("xyz"),
        Dimensions::Xyz
    );
    assert_eq!(
        spatial_dimensions_from_profile(1, Some(3)).expect("xyzm"),
        Dimensions::Xyzm
    );
    assert_eq!(
        spatial_dimensions_from_profile(2, Some(0))
            .expect_err("mixed profiles")
            .category,
        ErrorCategory::DataMapping
    );
    for incoherent in [
        spatial_dimensions_from_profile(-1, None),
        spatial_dimensions_from_profile(0, Some(0)),
        spatial_dimensions_from_profile(1, None),
        spatial_dimensions_from_profile(1, Some(4)),
    ] {
        assert_eq!(
            incoherent.expect_err("incoherent profile").category,
            ErrorCategory::Protocol
        );
    }
}
