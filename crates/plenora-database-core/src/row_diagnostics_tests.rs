use super::*;

#[test]
fn absolute_source_offsets_are_checked_once_for_every_provider() {
    assert_eq!(checked_source_row_end(1_000, 25), Some(1_025));
    assert_eq!(checked_source_row_end(u64::MAX, 1), None);
}

fn policy() -> RowDiagnosticsPolicy {
    RowDiagnosticsPolicy {
        key_field: Some("parcel_id".to_owned()),
        constraint_column: Some("area_m2".to_owned()),
        examples_limit: DEFAULT_EXAMPLES_LIMIT,
    }
}

fn tracker_at(staged: u64) -> WriteDiagnosticsTracker {
    let mut tracker = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
    tracker.stage_rows(staged).expect("righe messe in scena");
    tracker
}

fn rejected() -> RejectedRow {
    RejectedRow {
        source_index: 4_999,
        cause: CAUSE_CONSTRAINT_VIOLATION.to_owned(),
        column: None,
    }
}

/// Un rollback confermato partiziona l'intero input in quantità certe: la
/// somma deve tornare esattamente a `input_total`.
#[test]
fn a_confirmed_rollback_partitions_the_whole_input_into_certain_counts() {
    let report = tracker_at(4_999)
        .reject_row(&rejected(), RollbackEvidence::Confirmed)
        .expect("diagnostica pubblicabile");
    report.validate().expect("documento valido");

    let partition = report.write_outcome.expect("partizione");
    assert_eq!(partition.certainly_rejected.known(), Some(1));
    assert_eq!(partition.certainly_not_attempted.known(), Some(200));
    assert_eq!(partition.certainly_rolled_back.known(), Some(4_999));
    assert_eq!(partition.effect_unknown.known(), Some(0));
    assert_eq!(report.input_total, Some(5_200));
    assert_eq!(report.observed_total, 1);
    assert_eq!(report.completeness, Completeness::Complete);
    assert_eq!(
        report.counts,
        BTreeMap::from([(CAUSE_CONSTRAINT_VIOLATION.to_owned(), 1)])
    );

    let example = &report.examples[0];
    assert_eq!(example.source_index, 4_999);
    assert_eq!(example.write_state, Some(WriteState::CertainlyRejected));
    let key = example.key.as_ref().expect("chiave configurata");
    assert_eq!(key.field, "parcel_id");
    assert_eq!(key.state, KeyState::Redacted);
    assert!(key.value.is_none(), "nessun valore di riga nel documento");
}

/// Un acknowledgement di rollback perso non autorizza a dichiarare righe
/// annullate: le quantità remote restano esplicitamente ignote.
#[test]
fn a_lost_rollback_acknowledgement_keeps_the_remote_counts_unknown() {
    let report = tracker_at(4_999)
        .reject_row(&rejected(), RollbackEvidence::Lost)
        .expect("diagnostica pubblicabile");
    report.validate().expect("documento valido");

    let partition = report.write_outcome.expect("partizione");
    assert_eq!(partition.certainly_rejected.known(), Some(1));
    assert_eq!(partition.certainly_not_attempted.known(), Some(200));
    assert_eq!(partition.certainly_rolled_back, PartitionCount::Unknown);
    assert_eq!(partition.effect_unknown, PartitionCount::Unknown);
    assert_eq!(
        report
            .diagnostic_state_counts
            .expect("stati")
            .certainly_rolled_back,
        0,
        "nessuna riga è diagnosticata come annullata senza conferma"
    );
}

/// L'indice pubblicato è quello della sorgente: attraversa i batch e non
/// viene ricostruito dalla posizione dentro l'ultimo batch.
#[test]
fn the_published_index_is_the_source_offset_across_batches() {
    let mut tracker = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
    for _ in 0..3 {
        tracker.stage_rows(1_300).expect("batch messo in scena");
    }
    assert_eq!(tracker.staged_rows(), 3_900);
    tracker.stage_rows(1_099).expect("chunk parziale");
    assert_eq!(tracker.staged_rows(), 4_999);

    let report = tracker
        .reject_row(&rejected(), RollbackEvidence::Confirmed)
        .expect("diagnostica pubblicabile");
    assert_eq!(report.examples[0].source_index, 4_999);
    assert_eq!(report.index_basis, INDEX_BASIS);
}

/// L'indice non può essere dedotto: solo la prima riga non ancora
/// applicata è stata davvero provata da uno statement per riga.
#[test]
fn an_index_that_was_not_proven_row_by_row_is_refused() {
    let tracker = tracker_at(4_000);
    assert!(tracker
        .reject_row(&rejected(), RollbackEvidence::Confirmed)
        .is_err());

    let mut beyond = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
    beyond.stage_rows(5_200).expect("input completo");
    assert!(beyond
        .reject_row(
            &RejectedRow {
                source_index: 5_200,
                cause: CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                column: None,
            },
            RollbackEvidence::Confirmed,
        )
        .is_err());
}

/// La contabilità è controllata: nessun conteggio può uscire dall'input
/// dichiarato né andare in overflow.
#[test]
fn staging_counts_are_checked_against_the_declared_input() {
    assert!(WriteDiagnosticsTracker::new(0, policy()).is_err());
    assert!(WriteDiagnosticsTracker::new(
        1,
        RowDiagnosticsPolicy {
            key_field: None,
            constraint_column: None,
            examples_limit: 0,
        }
    )
    .is_err());

    let mut tracker = WriteDiagnosticsTracker::new(10, policy()).expect("tracker");
    assert!(tracker.stage_rows(11).is_err());
    assert!(tracker.stage_rows(u64::MAX).is_err());
    tracker.stage_rows(10).expect("input completo");
    assert_eq!(tracker.staged_rows(), 10);
}

/// Il documento serializzato non contiene i campi che il contratto vieta e
/// pubblica la chiave senza valore.
#[test]
fn the_serialized_document_matches_the_write_contract_shape() {
    let report = tracker_at(4_999)
        .reject_row(&rejected(), RollbackEvidence::Lost)
        .expect("diagnostica pubblicabile");
    let value = serde_json::to_value(&report).expect("documento serializzabile");

    assert_eq!(value["contract"], CONTRACT);
    assert_eq!(value["scope"], "write");
    assert_eq!(value["index_basis"], INDEX_BASIS);
    assert_eq!(value["completeness"], "complete");
    assert!(value.get("knowledge_limits").is_none());
    assert_eq!(value["examples"][0]["key"]["state"], "redacted");
    assert!(value["examples"][0]["key"].get("value").is_none());
    assert!(value["examples"][0].get("column").is_none());
    assert_eq!(
        value["write_outcome"]["certainly_rolled_back"]["state"],
        "unknown"
    );
    assert!(value["write_outcome"]["certainly_rolled_back"]
        .get("value")
        .is_none());

    let restored: RowDiagnostics =
        serde_json::from_value(value).expect("documento deserializzabile");
    assert_eq!(restored, report);
}

fn valid_write_report() -> RowDiagnostics {
    tracker_at(4_999)
        .reject_row(&rejected(), RollbackEvidence::Confirmed)
        .expect("diagnostica pubblicabile")
}

/// Ogni mutazione dell'aritmetica del contratto deve essere rifiutata:
/// un documento che non torna non è pubblicabile.
#[test]
fn arithmetic_mutations_are_rejected_before_publication() {
    let mut counts_drift = valid_write_report();
    counts_drift.counts.insert("database.other".to_owned(), 1);
    assert!(counts_drift.validate().is_err());

    let mut fabricated_unknown = valid_write_report();
    fabricated_unknown.write_outcome = Some(WriteOutcomePartition {
        certainly_rejected: PartitionCount::Known { value: 1 },
        certainly_not_attempted: PartitionCount::Known { value: 200 },
        certainly_rolled_back: PartitionCount::Known { value: 5_000 },
        effect_unknown: PartitionCount::Known { value: 0 },
    });
    assert!(fabricated_unknown.validate().is_err());

    let mut duplicated = valid_write_report();
    let example = duplicated.examples[0].clone();
    duplicated.examples.push(example);
    assert!(duplicated.validate().is_err());

    let mut read_shaped = valid_write_report();
    read_shaped.scope = DiagnosticScope::Read;
    assert!(read_shaped.validate().is_err());

    let mut foreign_contract = valid_write_report();
    foreign_contract.contract = "plenora-row-diagnostics-v2".to_owned();
    assert!(foreign_contract.validate().is_err());

    let mut foreign_basis = valid_write_report();
    foreign_basis.index_basis = "batch_row_zero_based".to_owned();
    assert!(foreign_basis.validate().is_err());

    let mut truncation_drift = valid_write_report();
    truncation_drift.examples_truncated = true;
    assert!(truncation_drift.validate().is_err());

    let mut leaked_key = valid_write_report();
    leaked_key.examples[0].key = Some(RowKey {
        field: "parcel_id".to_owned(),
        state: KeyState::Redacted,
        value: Some(KeyValue::Integer(4_999)),
    });
    assert!(leaked_key.validate().is_err());

    let mut states_drift = valid_write_report();
    states_drift.diagnostic_state_counts = Some(DiagnosticStateCounts {
        certainly_rejected: 1,
        certainly_not_attempted: 0,
        certainly_rolled_back: 4_999,
        effect_unknown: 0,
    });
    assert!(states_drift.validate().is_err());
}

/// Un documento di lettura non dichiara campi di scrittura e non può
/// dichiarare uno stato per riga.
#[test]
fn read_documents_stay_outside_the_write_partition() {
    let read = RowDiagnostics {
        contract: CONTRACT.to_owned(),
        scope: DiagnosticScope::Read,
        index_basis: INDEX_BASIS.to_owned(),
        completeness: Completeness::Complete,
        knowledge_limits: Vec::new(),
        observed_total: 2,
        total: Some(2),
        input_total: None,
        counts: BTreeMap::from([("conversion.invalid_date".to_owned(), 2)]),
        examples_limit: 10,
        examples_truncated: false,
        examples: vec![
            RowExample {
                source_index: 4,
                cause: "conversion.invalid_date".to_owned(),
                column: Some("effective_date".to_owned()),
                key: None,
                write_state: None,
            },
            RowExample {
                source_index: 1_004,
                cause: "conversion.invalid_date".to_owned(),
                column: Some("effective_date".to_owned()),
                key: None,
                write_state: None,
            },
        ],
        diagnostic_state_counts: None,
        write_outcome: None,
    };
    read.validate().expect("documento di lettura valido");

    let mut with_write_state = read.clone();
    with_write_state.examples[0].write_state = Some(WriteState::CertainlyRejected);
    assert!(with_write_state.validate().is_err());

    let mut with_input_total = read;
    with_input_total.input_total = Some(1_025);
    assert!(with_input_total.validate().is_err());
}

/// Un documento non completo deve dichiarare limiti di conoscenza validi e
/// non può dichiarare un totale quando la completezza è ignota.
#[test]
fn incomplete_documents_must_declare_valid_knowledge_limits() {
    let mut partial = valid_write_report();
    partial.completeness = Completeness::Partial;
    assert!(partial.validate().is_err());

    partial.knowledge_limits = vec!["server.truncated_diagnostics".to_owned()];
    partial.validate().expect("documento parziale valido");

    let mut repeated = partial.clone();
    repeated
        .knowledge_limits
        .push("server.truncated_diagnostics".to_owned());
    assert!(repeated.validate().is_err());

    let mut malformed = partial.clone();
    malformed.knowledge_limits = vec!["Server..Truncated".to_owned()];
    assert!(malformed.validate().is_err());

    let mut unknown = partial;
    unknown.completeness = Completeness::Unknown;
    assert!(unknown.validate().is_err(), "total resta dichiarato");
    unknown.total = None;
    unknown.validate().expect("documento ignoto valido");
}

/// Un valore di chiave al limite dell'intero i64 non deve far panicare la
/// validazione: il confronto è su un intervallo chiuso, non su `abs()`.
#[test]
fn extreme_key_integers_are_refused_without_panicking() {
    for refused in [
        i64::MIN,
        i64::MAX,
        -MAX_EXACT_INTEGER - 1,
        MAX_EXACT_INTEGER + 1,
    ] {
        let key = RowKey {
            field: "parcel_id".to_owned(),
            state: KeyState::Value,
            value: Some(KeyValue::Integer(refused)),
        };
        assert!(validate_key(Some(&key)).is_err(), "{refused}");
    }
    for accepted in [-MAX_EXACT_INTEGER, 0, MAX_EXACT_INTEGER] {
        let key = RowKey {
            field: "parcel_id".to_owned(),
            state: KeyState::Value,
            value: Some(KeyValue::Integer(accepted)),
        };
        validate_key(Some(&key)).expect("valore di chiave rappresentabile");
    }
}

#[test]
fn unicode_limits_are_counted_in_characters_not_utf8_bytes() {
    let field_at_limit = "é".repeat(MAX_FIELD_CHARS);
    validate_optional_field(Some(&field_at_limit), "colonna")
        .expect("256 caratteri Unicode sono ammessi");
    assert!(validate_optional_field(Some(&"é".repeat(MAX_FIELD_CHARS + 1)), "colonna").is_err());

    validate_key_value(&KeyValue::Text("界".repeat(MAX_KEY_TEXT_CHARS)))
        .expect("1024 caratteri Unicode sono ammessi");
    assert!(validate_key_value(&KeyValue::Text("界".repeat(MAX_KEY_TEXT_CHARS + 1))).is_err());
}

/// La pubblicazione è fail-closed: un documento incoerente non produce
/// JSON, uno valido produce esattamente il documento validato.
#[test]
fn serialization_refuses_to_publish_an_invalid_document() {
    let report = valid_write_report();
    let encoded = report.to_json().expect("documento pubblicabile");
    let decoded: RowDiagnostics = serde_json::from_str(&encoded).expect("documento rileggibile");
    assert_eq!(decoded, report);

    let mut broken = report;
    broken.observed_total = 9;
    assert!(
        broken.to_json().is_err(),
        "nessun JSON da un documento rotto"
    );
    assert!(
        serde_json::to_value(&broken).is_err(),
        "il trait Serialize pubblico non deve aggirare la validazione"
    );
}

/// Esecutore di prova del seam: applica le righe una per volta e rifiuta
/// esattamente l'indice sorgente programmato.
struct ScriptedWriter {
    reject_at: u64,
    rollback: RollbackEvidence,
    applied: Vec<u64>,
    rolled_back: bool,
    finish_error: bool,
}

impl RowScopedWriter for ScriptedWriter {
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
        let rejected = source_index == self.reject_at;
        if !rejected {
            self.applied.push(source_index);
        }
        Box::pin(async move {
            Ok(if rejected {
                RowApplication::Rejected(RowRejection {
                    cause: CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                    column: Some("area_m2".to_owned()),
                })
            } else {
                RowApplication::Applied
            })
        })
    }

    fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>> {
        let failed = self.finish_error;
        Box::pin(async move {
            if failed {
                Err(invalid("input oltre il totale dichiarato"))
            } else {
                Ok(())
            }
        })
    }

    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
        self.rolled_back = true;
        let evidence = self.rollback;
        Box::pin(async move { evidence })
    }
}

/// Esecutore minimo senza runtime: il seam resta agnostico e i test
/// offline non introducono dipendenze.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct Idle;
    impl Wake for Idle {
        fn wake(self: Arc<Self>) {}
    }

    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Idle));
    let mut context = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
}

fn drive(reject_at: u64, rollback: RollbackEvidence) -> (ScriptedWriter, RowRejectionOutcome) {
    let mut writer = ScriptedWriter {
        reject_at,
        rollback,
        applied: Vec::new(),
        rolled_back: false,
        finish_error: false,
    };
    let mut tracker = WriteDiagnosticsTracker::new(5_200, policy()).expect("tracker");
    let outcome = block_on(diagnose_row_scoped_write(&mut writer, &mut tracker))
        .expect("diagnosi eseguibile")
        .expect("riga rifiutata");
    (writer, outcome)
}

/// Caso `write-constraint-confirmed-rollback`: 5200 righe, vincolo certo a
/// 4999, annullamento confermato e partizione interamente certa.
#[test]
fn the_confirmed_rollback_case_is_driven_row_by_row() {
    let (writer, outcome) = drive(4_999, RollbackEvidence::Confirmed);
    assert_eq!(
        writer.applied.len(),
        4_999,
        "una riga per statement fino al rifiuto"
    );
    assert_eq!(writer.applied.first(), Some(&0));
    assert_eq!(writer.applied.last(), Some(&4_998));
    assert!(writer.rolled_back);

    assert_eq!(
        outcome.axes(),
        (
            ErrorPhase::Write,
            RemoteEffect::RolledBack,
            RetryDisposition::Never
        )
    );
    let report = outcome.diagnostics().clone();
    report.validate().expect("documento valido");
    assert_eq!(report.input_total, Some(5_200));
    assert_eq!(report.examples[0].source_index, 4_999);
    assert_eq!(report.examples[0].column.as_deref(), Some("area_m2"));
    let partition = report.write_outcome.expect("partizione");
    assert_eq!(partition.certainly_rejected.known(), Some(1));
    assert_eq!(partition.certainly_not_attempted.known(), Some(200));
    assert_eq!(partition.certainly_rolled_back.known(), Some(4_999));
    assert_eq!(partition.effect_unknown.known(), Some(0));

    let error = outcome
        .into_error(Some(ProviderKind::Mysql), Some("execution-3".to_owned()))
        .expect("errore con diagnostica");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert!(!error.is_retryable());
    assert!(
        !error.message.contains("4999"),
        "l'indice vive nel documento, non nel messaggio"
    );
}

/// Caso `write-constraint-rollback-outcome-unknown`: stesso rifiuto, ma
/// l'acknowledgement dell'annullamento è perso.
#[test]
fn the_lost_rollback_case_keeps_the_remote_effect_unknown() {
    let (_, outcome) = drive(4_999, RollbackEvidence::Lost);
    assert_eq!(
        outcome.axes(),
        (
            ErrorPhase::Rollback,
            RemoteEffect::Unknown,
            RetryDisposition::Quarantine
        )
    );
    let report = outcome.diagnostics().clone();
    report.validate().expect("documento valido");
    let partition = report.write_outcome.expect("partizione");
    assert_eq!(partition.certainly_rejected.known(), Some(1));
    assert_eq!(partition.certainly_not_attempted.known(), Some(200));
    assert_eq!(partition.certainly_rolled_back, PartitionCount::Unknown);
    assert_eq!(partition.effect_unknown, PartitionCount::Unknown);

    let error = outcome
        .into_error(Some(ProviderKind::Mysql), None)
        .expect("errore con diagnostica");
    assert!(!error.is_retryable(), "la quarantena non è un retry");
}

/// Un input applicato per intero non produce diagnostica: il documento
/// esiste solo quando c'è un rifiuto provato.
#[test]
fn a_fully_applied_input_produces_no_document() {
    let mut writer = ScriptedWriter {
        reject_at: u64::MAX,
        rollback: RollbackEvidence::Confirmed,
        applied: Vec::new(),
        rolled_back: false,
        finish_error: false,
    };
    let mut tracker = WriteDiagnosticsTracker::new(16, policy()).expect("tracker");
    let outcome = block_on(diagnose_row_scoped_write(&mut writer, &mut tracker)).expect("diagnosi");
    assert!(outcome.is_none());
    assert_eq!(writer.applied.len(), 16);
    assert!(!writer.rolled_back, "nessun annullamento senza rifiuto");
    assert_eq!(tracker.staged_rows(), 16);
}

#[test]
fn rows_beyond_the_declared_total_fail_closed_before_success() {
    let mut writer = ScriptedWriter {
        reject_at: u64::MAX,
        rollback: RollbackEvidence::Confirmed,
        applied: Vec::new(),
        rolled_back: false,
        finish_error: true,
    };
    let mut tracker = WriteDiagnosticsTracker::new(16, policy()).expect("tracker");
    assert!(
        block_on(diagnose_row_scoped_write(&mut writer, &mut tracker)).is_err(),
        "righe extra non devono essere ignorate"
    );
    assert_eq!(writer.applied.len(), 16);
}

fn read_policy() -> ReadDiagnosticsPolicy {
    ReadDiagnosticsPolicy {
        key_field: Some("parcel_id".to_owned()),
        examples_limit: DEFAULT_EXAMPLES_LIMIT,
    }
}

/// L'indice pubblicato in lettura è l'offset assoluto nel result set:
/// batch già pubblicati più la posizione dentro il batch corrente.
#[test]
fn a_read_defect_publishes_the_absolute_source_index() {
    let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    tracker
        .publish_batch(4_096)
        .expect("primo batch pubblicato");
    assert_eq!(tracker.published_rows(), 4_096);

    let report = tracker
        .attributed_defect(903, CAUSE_VALUE_NOT_REPRESENTABLE, Some("area_m2"))
        .expect("diagnostica pubblicabile");
    report.validate().expect("documento valido");

    assert_eq!(report.scope, DiagnosticScope::Read);
    assert_eq!(report.index_basis, INDEX_BASIS);
    assert_eq!(report.observed_total, 1);
    assert_eq!(report.examples[0].source_index, 4_999);
    assert_eq!(report.examples[0].column.as_deref(), Some("area_m2"));
    assert_eq!(
        report.examples[0].key.as_ref().map(|key| key.state),
        Some(KeyState::Redacted)
    );
    assert!(report.examples[0].write_state.is_none());
    assert!(report.input_total.is_none());
    assert!(report.write_outcome.is_none());
    assert!(report.diagnostic_state_counts.is_none());
}

/// La conoscenza non si estende oltre ciò che è stato osservato: la
/// scansione si ferma al primo difetto e i batch già pubblicati non sono
/// più ispezionabili.
#[test]
fn a_read_report_never_claims_completeness_it_cannot_observe() {
    let first = ReadDiagnosticsTracker::new(read_policy())
        .expect("tracker")
        .attributed_defect(4, CAUSE_VALUE_NOT_REPRESENTABLE, None)
        .expect("diagnostica pubblicabile");
    assert_eq!(first.completeness, Completeness::Partial);
    assert_eq!(
        first.knowledge_limits,
        vec![LIMIT_SCAN_STOPPED_AT_FIRST_DEFECT.to_owned()]
    );
    assert!(first.total.is_none(), "nessun totale osservato in lettura");

    let mut later = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    later.publish_batch(4_096).expect("batch pubblicato");
    let report = later
        .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
        .expect("diagnostica pubblicabile");
    assert_eq!(
        report.knowledge_limits,
        vec![
            LIMIT_BATCHES_ALREADY_PUBLISHED.to_owned(),
            LIMIT_SCAN_STOPPED_AT_FIRST_DEFECT.to_owned(),
        ]
    );
    report.validate().expect("documento valido");
}

/// Un difetto non attribuibile non inventa né indice né causa: resta un
/// documento ignoto con conteggi ed esempi vuoti.
#[test]
fn an_unattributable_read_defect_publishes_an_unknown_report() {
    let report = ReadDiagnosticsTracker::new(read_policy())
        .expect("tracker")
        .unattributable_defect()
        .expect("diagnostica pubblicabile");
    report.validate().expect("documento valido");

    assert_eq!(report.completeness, Completeness::Unknown);
    assert_eq!(report.observed_total, 0);
    assert!(report.total.is_none());
    assert!(report.counts.is_empty());
    assert!(report.examples.is_empty());
    assert!(!report.examples_truncated);
    assert_eq!(
        report.knowledge_limits,
        vec![LIMIT_ROW_ATTRIBUTION_UNAVAILABLE.to_owned()]
    );
}

/// L'offset assoluto è aritmetica controllata: un cursore che uscirebbe
/// dall'intervallo rappresentabile fallisce invece di avvolgersi.
#[test]
fn read_source_indexes_are_checked_arithmetic() {
    let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    tracker.publish_batch(u64::MAX).expect("cursore al limite");
    assert!(tracker.publish_batch(1).is_err());
    assert!(tracker.source_index(1).is_err());
    assert!(tracker
        .attributed_defect(1, CAUSE_VALUE_NOT_REPRESENTABLE, None)
        .is_err());

    let mut small = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    small.publish_batch(7).expect("batch pubblicato");
    assert_eq!(small.source_index(3).expect("offset assoluto"), 10);
}

/// La contabilità di default è quella dichiarata dalla politica di
/// default: i percorsi che non possono propagare un errore in costruzione
/// non hanno bisogno di aggirare la validazione.
#[test]
fn the_default_tracker_matches_the_default_policy() {
    let built = ReadDiagnosticsTracker::new(ReadDiagnosticsPolicy::default()).expect("tracker");
    let default = ReadDiagnosticsTracker::default();
    assert_eq!(default.published_rows(), built.published_rows());
    assert_eq!(
        default
            .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
            .expect("documento"),
        built
            .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
            .expect("documento")
    );
    assert_eq!(
        default
            .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, None)
            .expect("documento")
            .examples_limit,
        DEFAULT_EXAMPLES_LIMIT
    );
}

/// La causa deve restare un identificatore di contratto: nessun testo
/// vendor può diventare una causa pubblicata.
#[test]
fn a_read_cause_outside_the_contract_pattern_is_refused() {
    let tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    for refused in [
        "",
        "Conversion.Invalid",
        "conversion..invalid",
        "conversion.",
    ] {
        assert!(
            tracker.attributed_defect(0, refused, None).is_err(),
            "{refused}"
        );
    }
    assert!(tracker
        .attributed_defect(0, CAUSE_VALUE_NOT_REPRESENTABLE, Some(""))
        .is_err());
}

fn conversion_error() -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Read,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Mysql),
        execution_id: None,
        message: "valore non rappresentabile".to_owned(),
        diagnostics: None,
    }
}

/// Il seam condiviso dai provider chiude il difetto osservato durante la
/// costruzione del batch senza che ciascuno riscriva la contabilità.
#[test]
fn a_conversion_defect_becomes_a_read_rejection() {
    let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    tracker.publish_batch(8_192).expect("batch pubblicato");

    let error =
        tracker.reject_conversion_defect(conversion_error(), Some(12), Some("effective_date"));
    let report = error.row_diagnostics().expect("diagnostica allegata");
    report.validate().expect("documento valido");
    assert_eq!(error.phase, ErrorPhase::Read);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(report.examples[0].source_index, 8_204);
    assert_eq!(report.examples[0].cause, CAUSE_VALUE_NOT_REPRESENTABLE);
    assert_eq!(report.examples[0].column.as_deref(), Some("effective_date"));
}

/// Un errore che non è un difetto di conversione non viene riclassificato
/// e non riceve una riga che non gli appartiene.
#[test]
fn a_non_conversion_error_never_receives_a_source_row() {
    let tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    for error in [
        DatabaseError::resource_limit("budget esaurito"),
        DatabaseError::cancelled(Some(ProviderKind::Mysql), ErrorPhase::Read, "cancellata"),
        DatabaseError::invalid_plan("piano non valido"),
    ] {
        let category = error.category;
        let closed = tracker.reject_conversion_defect(error, Some(3), Some("area_m2"));
        assert_eq!(closed.category, category);
        assert!(closed.row_diagnostics().is_none());
    }
}

/// Una riga che il percorso non sa individuare non riceve un indice
/// plausibile: il documento dichiara che l'identità non è osservabile.
#[test]
fn an_unobservable_row_position_publishes_an_unknown_report() {
    let tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    let error = tracker.reject_conversion_defect(conversion_error(), None, Some("area_m2"));
    let report = error.row_diagnostics().expect("diagnostica allegata");
    report.validate().expect("documento valido");
    assert_eq!(report.completeness, Completeness::Unknown);
    assert_eq!(report.observed_total, 0);
    assert!(report.examples.is_empty());
    assert_eq!(
        report.knowledge_limits,
        vec![LIMIT_ROW_ATTRIBUTION_UNAVAILABLE.to_owned()]
    );
}

/// Una posizione non rappresentabile o una colonna fuori contratto non
/// producono un indice inventato: l'indice provato resta, il resto no.
#[test]
fn an_unattributable_position_degrades_to_an_unknown_report() {
    let mut tracker = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    tracker.publish_batch(u64::MAX).expect("cursore al limite");
    let error = tracker.reject_conversion_defect(conversion_error(), Some(1), Some("area_m2"));
    let report = error.row_diagnostics().expect("diagnostica allegata");
    assert_eq!(report.completeness, Completeness::Unknown);
    assert!(report.examples.is_empty());

    let bounded = ReadDiagnosticsTracker::new(read_policy()).expect("tracker");
    let error = bounded.reject_conversion_defect(conversion_error(), Some(5), Some(""));
    let report = error.row_diagnostics().expect("diagnostica allegata");
    assert_eq!(report.examples[0].source_index, 5, "l'indice provato resta");
    assert!(
        report.examples[0].column.is_none(),
        "una colonna fuori contratto non viene pubblicata"
    );
}

/// Il rifiuto di lettura porta gli assi dichiarati dalla campagna e non
/// riclassifica errori che non sono difetti di conversione.
#[test]
fn a_read_rejection_carries_the_declared_axes() {
    let report = ReadDiagnosticsTracker::new(read_policy())
        .expect("tracker")
        .attributed_defect(4, CAUSE_VALUE_NOT_REPRESENTABLE, Some("effective_date"))
        .expect("diagnostica pubblicabile");

    let error = into_read_rejection(
        DatabaseError {
            category: ErrorCategory::DataMapping,
            phase: ErrorPhase::Prepare,
            remote_effect: RemoteEffect::Unknown,
            retry: RetryDisposition::RequiresRecovery,
            provider: Some(ProviderKind::Mysql),
            execution_id: None,
            message: "valore non rappresentabile".to_owned(),
            diagnostics: None,
        },
        report.clone(),
    )
    .expect("rifiuto di lettura");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, ErrorPhase::Read);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(error.row_diagnostics(), Some(&report));

    let untouched = into_read_rejection(
        DatabaseError::resource_limit("budget esaurito"),
        report.clone(),
    )
    .expect("errore non riclassificato");
    assert_eq!(untouched.category, ErrorCategory::ResourceLimit);
    assert!(
        untouched.row_diagnostics().is_none(),
        "un errore che non è un difetto di riga non porta diagnostica"
    );

    let mut write_shaped = report;
    write_shaped.scope = DiagnosticScope::Write;
    assert!(into_read_rejection(
        DatabaseError {
            category: ErrorCategory::DataMapping,
            phase: ErrorPhase::Read,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: None,
            execution_id: None,
            message: "documento di scrittura".to_owned(),
            diagnostics: None,
        },
        write_shaped,
    )
    .is_err());
}

#[test]
fn contract_identifiers_follow_the_declared_pattern() {
    for accepted in [
        "database.constraint_violation",
        "a",
        "shapefile.inner_ring_without_outer",
        "conversion.invalid-date",
    ] {
        assert!(is_contract_identifier(accepted), "{accepted}");
    }
    for refused in [
        "",
        "1database",
        "Database.violation",
        "database..violation",
        "database.",
        "database violation",
        &"a".repeat(129),
    ] {
        assert!(!is_contract_identifier(refused), "{refused}");
    }
}
