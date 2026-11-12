use super::bind_error_context;

#[test]
fn bind_context_extracts_only_structural_metadata() {
    assert_eq!(
        bind_error_context(
            "bind PostgreSQL incompatibile al parametro 2: tipo portabile i64, target uuid"
        ),
        Some((2, "i64".to_owned(), "uuid".to_owned()))
    );
}

#[test]
fn unrelated_public_messages_do_not_invent_bind_metadata() {
    assert_eq!(bind_error_context("operazione PostgreSQL fallita"), None);
}

#[test]
fn prepared_error_exposes_loss_report_without_changing_remote_axes() {
    use plenora_database_core::{
        DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
    };
    use pyo3::prelude::*;
    Python::initialize();
    Python::attach(|py| {
        let mut error = DatabaseError::new(
            ErrorCategory::Io,
            ErrorPhase::Commit,
            None,
            "commit non confermato",
        );
        error.remote_effect = RemoteEffect::Unknown;
        error.retry = RetryDisposition::RequiresRecovery;
        let report: plenora_database_core::execution_loss::ExecutionLossReport =
            serde_json::from_str(include_str!(
                "../../../contracts/v2/examples/execution-loss-v3.json"
            ))
            .unwrap();
        error.loss_report = Some(Box::new(report.into()));
        let exception = super::to_py_err(error);
        let bound = exception.value(py);
        assert_eq!(
            bound
                .getattr("remote_effect")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "unknown"
        );
        assert_eq!(
            bound
                .getattr("retry")
                .unwrap()
                .get_item("kind")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "requires_recovery"
        );
        assert!(bound
            .getattr("loss_report")
            .unwrap()
            .eq(bound
                .getattr("details")
                .unwrap()
                .get_item("loss_report")
                .unwrap())
            .unwrap());
    });
}
