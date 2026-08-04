//! Conformità offline a `plenora-row-diagnostics-v1`.
//!
//! I due casi di scrittura della campagna `row-diagnostics-v1` sono riprodotti
//! guidando il seam row-scoped con l'esecutore programmabile del testkit: non
//! servono database, container o credenziali. I documenti attesi sono quelli
//! normativi della campagna e vengono confrontati campo per campo, così che una
//! deriva di forma o di aritmetica non passi inosservata.

use plenora_database_core::plan::ProviderKind;
use plenora_database_core::row_diagnostics::{
    diagnose_row_scoped_write, RowDiagnosticsPolicy, WriteDiagnosticsTracker,
    DEFAULT_EXAMPLES_LIMIT,
};
use plenora_database_core::{
    ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition, RollbackEvidence,
};
use plenora_database_testkit::{block_on, RowWriteScript, ScriptedRowWriter};
use serde_json::{json, Value};

/// Righe dichiarate dalla sorgente `fixtures/write-constraint.csv`.
const INPUT_TOTAL: u64 = 5_200;

/// Indice sorgente assoluto della riga che viola il vincolo `area_m2 >= 0`.
///
/// Cade oltre il primo batch: l'indice pubblicato deve essere quello della
/// sorgente, non la posizione dentro l'ultimo batch.
const REJECTED_INDEX: u64 = 4_999;

fn policy() -> RowDiagnosticsPolicy {
    RowDiagnosticsPolicy {
        key_field: Some("parcel_id".to_owned()),
        constraint_column: Some("area_m2".to_owned()),
        examples_limit: DEFAULT_EXAMPLES_LIMIT,
    }
}

fn run(rollback: RollbackEvidence) -> (ScriptedRowWriter, Value, ErrorCategory, Value) {
    let mut writer = ScriptedRowWriter::new(RowWriteScript::constraint_violation(
        REJECTED_INDEX,
        "area_m2",
        rollback,
    ));
    let mut tracker = WriteDiagnosticsTracker::new(INPUT_TOTAL, policy()).expect("tracker");
    let outcome = block_on(diagnose_row_scoped_write(&mut writer, &mut tracker))
        .expect("diagnosi eseguibile")
        .expect("la riga rifiutata chiude la diagnosi");

    let document: Value = serde_json::from_str(
        &outcome
            .diagnostics()
            .to_json()
            .expect("documento pubblicabile"),
    )
    .expect("documento rileggibile");
    let (phase, remote_effect, retry) = outcome.axes();
    let error = outcome
        .into_error(Some(ProviderKind::Mysql), None)
        .expect("errore con diagnostica");
    let axes = json!({
        "phase": phase,
        "remote_effect": remote_effect,
        "retry": retry,
    });
    assert_eq!(
        serde_json::to_value(error.phase).expect("fase"),
        axes["phase"],
        "gli assi dell'errore devono coincidere con quelli dichiarati"
    );
    assert_eq!(
        serde_json::to_value(error.remote_effect).expect("effetto"),
        axes["remote_effect"]
    );
    assert_eq!(
        serde_json::to_value(error.retry).expect("retry"),
        axes["retry"]
    );
    assert!(
        !error.message.contains(&REJECTED_INDEX.to_string())
            && !error.message.contains("parcel_id")
            && !error.message.contains("area_m2"),
        "il messaggio d'errore non trasporta identificatori di riga: {}",
        error.message
    );
    (writer, document, error.category, axes)
}

/// Caso `write-constraint-confirmed-rollback` della campagna.
#[test]
fn confirmed_rollback_publishes_a_fully_certain_partition() {
    let (writer, document, category, axes) = run(RollbackEvidence::Confirmed);

    writer
        .verify_one_statement_per_row(REJECTED_INDEX)
        .expect("uno statement per riga fino al rifiuto");
    assert_eq!(writer.rollbacks(), 1);

    assert_eq!(category, ErrorCategory::DataMapping);
    assert_eq!(
        axes,
        json!({
            "phase": serde_json::to_value(ErrorPhase::Write).expect("fase"),
            "remote_effect": serde_json::to_value(RemoteEffect::RolledBack).expect("effetto"),
            "retry": serde_json::to_value(RetryDisposition::Never).expect("retry"),
        })
    );

    assert_eq!(
        document,
        json!({
            "contract": "plenora-row-diagnostics-v1",
            "scope": "write",
            "index_basis": "source_row_zero_based",
            "completeness": "complete",
            "observed_total": 1,
            "total": 1,
            "input_total": 5200,
            "counts": {"database.constraint_violation": 1},
            "examples_limit": 10,
            "examples_truncated": false,
            "examples": [
                {
                    "source_index": 4999,
                    "cause": "database.constraint_violation",
                    "column": "area_m2",
                    "key": {"field": "parcel_id", "state": "redacted"},
                    "write_state": "certainly_rejected"
                }
            ],
            "diagnostic_state_counts": {
                "certainly_rejected": 1,
                "certainly_not_attempted": 0,
                "certainly_rolled_back": 0,
                "effect_unknown": 0
            },
            "write_outcome": {
                "certainly_rejected": {"state": "known", "value": 1},
                "certainly_not_attempted": {"state": "known", "value": 200},
                "certainly_rolled_back": {"state": "known", "value": 4999},
                "effect_unknown": {"state": "known", "value": 0}
            }
        })
    );
}

/// Caso `write-constraint-rollback-outcome-unknown` della campagna.
#[test]
fn a_lost_rollback_acknowledgement_never_claims_rolled_back_rows() {
    let (writer, document, category, axes) = run(RollbackEvidence::Lost);

    writer
        .verify_one_statement_per_row(REJECTED_INDEX)
        .expect("uno statement per riga fino al rifiuto");
    assert_eq!(writer.rollbacks(), 1);

    assert_eq!(category, ErrorCategory::DataMapping);
    assert_eq!(
        axes,
        json!({
            "phase": serde_json::to_value(ErrorPhase::Rollback).expect("fase"),
            "remote_effect": serde_json::to_value(RemoteEffect::Unknown).expect("effetto"),
            "retry": serde_json::to_value(RetryDisposition::Quarantine).expect("retry"),
        })
    );

    assert_eq!(
        document,
        json!({
            "contract": "plenora-row-diagnostics-v1",
            "scope": "write",
            "index_basis": "source_row_zero_based",
            "completeness": "complete",
            "observed_total": 1,
            "total": 1,
            "input_total": 5200,
            "counts": {"database.constraint_violation": 1},
            "examples_limit": 10,
            "examples_truncated": false,
            "examples": [{
                "source_index": 4999,
                "cause": "database.constraint_violation",
                "column": "area_m2",
                "key": {"field": "parcel_id", "state": "redacted"},
                "write_state": "certainly_rejected"
            }],
            "diagnostic_state_counts": {
                "certainly_rejected": 1,
                "certainly_not_attempted": 0,
                "certainly_rolled_back": 0,
                "effect_unknown": 0
            },
            "write_outcome": {
                "certainly_rejected": {"state": "known", "value": 1},
                "certainly_not_attempted": {"state": "known", "value": 200},
                "certainly_rolled_back": {"state": "unknown"},
                "effect_unknown": {"state": "unknown"}
            }
        }),
        "l'oracle rollback-lost deve coincidere campo per campo"
    );
}
