use super::*;

#[test]
fn unknown_effect_is_not_a_cause_category() {
    let error = DatabaseError {
        category: ErrorCategory::Timeout,
        phase: ErrorPhase::Commit,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        provider: Some(ProviderKind::Postgres),
        execution_id: Some("execution-1".to_owned()),
        message: "esito commit non verificabile".to_owned(),
        diagnostics: None,
    };

    assert_eq!(error.category, ErrorCategory::Timeout);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    // Un commit dall'esito ignoto non e ritentabile *da solo*: prima
    // qualcuno deve verificare cosa e successo davvero al remoto.
    assert!(!error.is_retryable());
    assert!(error.requires_manual_recovery());
}

/// Le due risposte partizionano le disposizioni, e nessuna disposizione
/// e insieme automatica e da recuperare a mano.
#[test]
fn automatic_retry_and_manual_recovery_partition_the_dispositions() {
    let dispositions = [
        (RetryDisposition::Never, false, false),
        (RetryDisposition::Quarantine, false, false),
        (RetryDisposition::Safe, true, false),
        (RetryDisposition::After(10), true, false),
        (RetryDisposition::RequiresIdempotencyKey, false, true),
        (RetryDisposition::RequiresRecovery, false, true),
    ];
    for (retry, automatic, manual) in dispositions {
        let error = DatabaseError {
            category: ErrorCategory::Transient,
            phase: ErrorPhase::Write,
            remote_effect: RemoteEffect::None,
            retry,
            provider: None,
            execution_id: None,
            message: "classificazione".to_owned(),
            diagnostics: None,
        };
        assert_eq!(error.is_retryable(), automatic, "{retry:?}");
        assert_eq!(error.requires_manual_recovery(), manual, "{retry:?}");
        assert!(!(error.is_retryable() && error.requires_manual_recovery()));
    }
}

/// La quarantena non è un retry rimandato: nessun tentativo automatico è
/// autorizzato finché l'effetto remoto non è stato verificato.
#[test]
fn quarantine_is_not_a_retryable_disposition() {
    let error = DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Rollback,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::Quarantine,
        provider: Some(ProviderKind::Mysql),
        execution_id: Some("execution-2".to_owned()),
        message: "annullamento non confermato".to_owned(),
        diagnostics: None,
    };
    assert!(!error.is_retryable());
    assert_eq!(
        serde_json::to_value(error.retry).expect("retry serializzabile"),
        serde_json::json!({"kind": "quarantine"})
    );
}

/// Il carrier non è un campo libero: una diagnostica che non supera il
/// contratto non può viaggiare con l'errore.
#[test]
fn the_row_diagnostics_carrier_refuses_an_invalid_document() {
    let mut tracker = crate::row_diagnostics::WriteDiagnosticsTracker::new(
        5_200,
        crate::row_diagnostics::RowDiagnosticsPolicy {
            key_field: Some("parcel_id".to_owned()),
            constraint_column: None,
            examples_limit: 10,
        },
    )
    .expect("tracker");
    tracker.stage_rows(4_999).expect("righe messe in scena");
    let report = tracker
        .reject_row(
            &crate::row_diagnostics::RejectedRow {
                source_index: 4_999,
                cause: crate::row_diagnostics::CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                column: None,
            },
            crate::row_diagnostics::RollbackEvidence::Confirmed,
        )
        .expect("diagnostica");

    let carried = DatabaseError::invalid_plan("riga rifiutata")
        .with_row_diagnostics(report.clone())
        .expect("carrier valido");
    assert_eq!(carried.row_diagnostics(), Some(&report));
    let encoded = serde_json::to_value(&carried).expect("errore serializzabile");
    assert_eq!(encoded["diagnostics"]["scope"], "write");
    assert!(!encoded["diagnostics"].to_string().contains("4999.0"));

    let mut broken = report;
    broken.observed_total = 7;
    assert!(DatabaseError::invalid_plan("riga rifiutata")
        .with_row_diagnostics(broken)
        .is_err());

    let plain = DatabaseError::invalid_plan("nessuna diagnostica");
    let encoded = serde_json::to_value(&plain).expect("errore serializzabile");
    assert!(
        encoded.get("diagnostics").is_none(),
        "un errore senza diagnostica non pubblica il campo"
    );
}

#[test]
fn cancelled_error_is_never_retryable_by_default() {
    let error = DatabaseError::cancelled(None, ErrorPhase::Read, "annullata");
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert!(!error.is_retryable());
}

#[test]
fn new_builds_the_safe_local_envelope() {
    let error = DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        Some(ProviderKind::Sqlserver),
        "conversione redatta",
    );
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    assert_eq!(error.provider, Some(ProviderKind::Sqlserver));
    assert!(error.execution_id.is_none());
    assert!(error.diagnostics.is_none());
}

#[test]
fn public_projection_nests_diagnostics_and_bounds_the_message() {
    let tracker = crate::row_diagnostics::WriteDiagnosticsTracker::new(
        1,
        crate::row_diagnostics::RowDiagnosticsPolicy {
            key_field: None,
            constraint_column: None,
            examples_limit: 1,
        },
    )
    .expect("tracker");
    let report = tracker
        .reject_row(
            &crate::row_diagnostics::RejectedRow {
                source_index: 0,
                cause: crate::row_diagnostics::CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                column: None,
            },
            crate::row_diagnostics::RollbackEvidence::Confirmed,
        )
        .expect("report");
    let mut error = DatabaseError::invalid_plan("x".repeat(PUBLIC_MESSAGE_MAX_CHARS + 5));
    error.diagnostics = Some(Box::new(report));

    let encoded = serde_json::to_value(error.public_projection()).expect("JSON");
    assert!(encoded.get("diagnostics").is_none());
    assert_eq!(
        encoded["message"]
            .as_str()
            .expect("message")
            .chars()
            .count(),
        PUBLIC_MESSAGE_MAX_CHARS
    );
    assert_eq!(
        encoded["details"]["row_diagnostics"]["contract"],
        "plenora-row-diagnostics-v1"
    );
}

#[test]
fn public_projection_never_serializes_automatic_retry_for_unknown_effect() {
    let mut error = DatabaseError::invalid_plan("ambiguous");
    error.remote_effect = RemoteEffect::Unknown;
    error.retry = RetryDisposition::After(1);

    assert_eq!(
        error.public_projection().retry,
        RetryDisposition::RequiresRecovery
    );
}

/// ERR-003 ed ERR-014: un rollback non confermato rende l'effetto ignoto e
/// diventa l'ultima fase avviata; un `quarantine` gia deciso resta.
#[test]
fn an_unconfirmed_rollback_is_the_last_phase_and_leaves_the_effect_unknown() {
    let shaped = DatabaseError::new(
        ErrorCategory::Conflict,
        ErrorPhase::Write,
        None,
        "vincolo violato",
    )
    .after_unconfirmed_rollback();
    assert_eq!(shaped.category, ErrorCategory::Conflict);
    assert_eq!(shaped.phase, ErrorPhase::Rollback);
    assert_eq!(shaped.remote_effect, RemoteEffect::Unknown);
    assert_eq!(shaped.retry, RetryDisposition::RequiresRecovery);
    let quarantined = DatabaseError {
        retry: RetryDisposition::Quarantine,
        ..DatabaseError::invalid_plan("x")
    }
    .after_unconfirmed_rollback();
    assert_eq!(quarantined.retry, RetryDisposition::Quarantine);
}

/// Regola unica dell'interruzione dopo l'invio: con il comando in volo, in
/// una fase mutante, l'effetto non e mai `none`. Le fasi non mutanti restano
/// senza effetto; la categoria resta quella della causa. `interrupted` da
/// solo descrive l'interruzione prima dell'invio.
#[test]
fn an_interruption_in_flight_never_reports_no_effect_in_a_mutating_phase() {
    let cancelled = crate::CancellationToken::new();
    cancelled.cancel();
    for phase in [ErrorPhase::Write, ErrorPhase::Commit, ErrorPhase::Rollback] {
        let before = DatabaseError::interrupted(&cancelled, None, phase, "interrotta");
        assert_eq!(before.remote_effect, RemoteEffect::None, "{phase:?}");
        let error = before.after_interrupted_send();
        assert_eq!(error.remote_effect, RemoteEffect::Unknown, "{phase:?}");
        assert_eq!(error.retry, RetryDisposition::RequiresRecovery, "{phase:?}");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        assert_eq!(error.phase, phase);
    }
    for phase in [ErrorPhase::Read, ErrorPhase::Connect, ErrorPhase::Validate] {
        let error = DatabaseError::interrupted(&cancelled, None, phase, "interrotta")
            .after_interrupted_send();
        assert_eq!(error.remote_effect, RemoteEffect::None, "{phase:?}");
    }
    let quarantined = DatabaseError {
        retry: RetryDisposition::Quarantine,
        ..DatabaseError::new(ErrorCategory::Timeout, ErrorPhase::Rollback, None, "x")
    }
    .after_interrupted_send();
    assert_eq!(quarantined.retry, RetryDisposition::Quarantine);
    assert_eq!(quarantined.remote_effect, RemoteEffect::Unknown);
}
