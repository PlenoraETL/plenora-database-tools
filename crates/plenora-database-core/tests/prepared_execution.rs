//! Regressioni del consenso: il contatore rappresenta il primo effetto remoto.
use plenora_database_core::arrow::{DataType, Field};
use plenora_database_core::loss::{
    LossCategory, LossReport, LossSeverity, MappingLoss, MappingPolicy,
};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{
    ObjectRef, ProviderKind, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::provider::PreparedWrite;
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLimits};
use plenora_database_core::{prepared_execution, ErrorCategory, ErrorPhase, RemoteEffect};
use std::cell::Cell;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn prepared(report: LossReport) -> PreparedWrite {
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    PreparedWrite::new(
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("public".into()),
                object: "target".into(),
            },
            mode: WriteMode::Append,
            mapping_policy: report.policy,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: vec![],
            update_columns: vec![],
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        },
        plenora_database_core::protocol::contract_schema(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])
        .unwrap(),
        report,
        budget.clone(),
        budget
            .try_lease(ResourceKind::ConcurrentOperations, 1)
            .unwrap(),
        budget.try_lease(ResourceKind::Columns, 1).unwrap(),
    )
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("esecutore offline senza attese"),
    }
}

#[test]
fn consent_matrix_rejects_before_invoking_executor_and_preserves_reports() {
    for provider in [ProviderKind::Postgres, ProviderKind::Sqlserver] {
        for policy in [
            MappingPolicy::Strict,
            MappingPolicy::Compatible,
            MappingPolicy::Lossy,
            MappingPolicy::Native,
        ] {
            for severity in [
                LossSeverity::Information,
                LossSeverity::Warning,
                LossSeverity::DataLoss,
            ] {
                for accept in [false, true] {
                    let report = LossReport {
                        schema_version: 2,
                        policy,
                        losses: vec![MappingLoss {
                            field_id: 1,
                            category: LossCategory::Precision,
                            severity,
                            reason: "precisione ridotta".into(),
                            source_type: None,
                            target_type: None,
                        }],
                    };
                    let calls = Cell::new(0);
                    let result = ready(prepared_execution::execute(
                        prepared(report.clone()),
                        accept,
                        |_| {
                            calls.set(calls.get() + 1);
                            async {
                                let mut outcome: WriteOutcome = serde_json::from_str(include_str!(
                                    "../../../contracts/v2/examples/outcome-committed.json"
                                ))
                                .unwrap();
                                outcome.provider = provider;
                                Ok(outcome)
                            }
                        },
                    ));
                    if severity == LossSeverity::DataLoss
                        && (!accept || policy == MappingPolicy::Strict)
                    {
                        assert_eq!(calls.get(), 0);
                        let error = result.unwrap_err();
                        assert_eq!(error.category, ErrorCategory::DataMapping);
                        assert_eq!(error.phase, ErrorPhase::Prepare);
                        assert_eq!(error.remote_effect, RemoteEffect::None);
                    } else {
                        assert_eq!(calls.get(), 1);
                        let outcome = result.unwrap();
                        outcome.validate().unwrap();
                        assert_eq!(outcome.schema_version, 3);
                        assert_eq!(outcome.loss_report, Some(report));
                    }
                }
            }
        }
    }
}

#[test]
fn empty_is_not_absent_and_uncertain_outcomes_keep_the_report() {
    for source in [
        include_str!("../../../contracts/v2/examples/outcome-committed.json"),
        include_str!("../../../contracts/v2/examples/outcome-unknown.json"),
    ] {
        let old: WriteOutcome = serde_json::from_str(source).unwrap();
        assert!(old.loss_report.is_none());
        let report = LossReport {
            schema_version: 2,
            policy: MappingPolicy::Strict,
            losses: vec![],
        };
        let outcome = ready(prepared_execution::execute(
            prepared(report.clone()),
            false,
            |_| async { Ok(old) },
        ))
        .unwrap();
        assert_eq!(outcome.loss_report, Some(report));
        outcome.validate().unwrap();
        let document = serde_json::to_value(&outcome).unwrap();
        assert_eq!(document["loss_report"]["losses"], serde_json::json!([]));
        if let Err(unsettled) = outcome.settle() {
            assert!(unsettled.outcome().unwrap().loss_report.is_some());
        }
    }
}

#[test]
fn errors_after_preparation_keep_report_and_all_remote_axes() {
    use plenora_database_core::{DatabaseError, RetryDisposition};
    for effect in [
        RemoteEffect::None,
        RemoteEffect::RolledBack,
        RemoteEffect::Committed,
        RemoteEffect::Partial,
        RemoteEffect::Unknown,
    ] {
        let report = LossReport {
            schema_version: 2,
            policy: MappingPolicy::Compatible,
            losses: vec![],
        };
        let mut original = DatabaseError::new(
            ErrorCategory::Execution,
            ErrorPhase::Commit,
            Some(ProviderKind::Postgres),
            "errore dopo preparazione",
        );
        original.remote_effect = effect;
        original.retry = RetryDisposition::RequiresRecovery;
        original.execution_id = Some("prepared-error".into());
        let error = ready(prepared_execution::execute(
            prepared(report.clone()),
            true,
            |_| async { Err(original.clone()) },
        ))
        .unwrap_err();
        assert_eq!(error.loss_report.as_deref(), Some(&report));
        assert_eq!(error.remote_effect, original.remote_effect);
        assert_eq!(error.retry, original.retry);
        assert_eq!(error.phase, original.phase);
        assert_eq!(error.execution_id, original.execution_id);
        assert_eq!(
            serde_json::to_value(error.public_projection()).unwrap()["details"]["loss_report"],
            serde_json::to_value(report).unwrap()
        );
    }
}
