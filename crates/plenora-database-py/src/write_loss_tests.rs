use super::*;
use plenora_database_core::execution_loss::{Basis, ExecutionLossReport};

#[test]
fn warning_filters_preserve_the_committed_outcome() {
    Python::initialize();
    Python::attach(|py| {
        let warnings = py.import("warnings").unwrap();
        let manager = warnings.call_method0("catch_warnings").unwrap();
        manager.call_method0("__enter__").unwrap();
        let mut outcome: WriteOutcome = serde_json::from_str(include_str!(
            "../../../contracts/v2/examples/outcome-v3-committed.json"
        ))
        .unwrap();
        let report: ExecutionLossReport = serde_json::from_str(include_str!(
            "../../../contracts/v2/examples/execution-loss-v3.json"
        ))
        .unwrap();
        for filter in ["default", "ignore", "error"] {
            warnings
                .call_method1(
                    "simplefilter",
                    (filter, py.get_type::<crate::errors::MappingLossWarning>()),
                )
                .unwrap();
            // Legacy, risk e valutazione positiva non attestano un commit con perdita.
            assert!(outcome_into_py(py, &outcome).is_ok());
            let mut risk_only = report.clone();
            risk_only.effective.clear();
            outcome.loss_report = Some(risk_only.into());
            assert!(outcome_into_py(py, &outcome).is_ok());
            for basis in [Basis::Exact, Basis::Conservative] {
                let measured = measured_report(&report, basis);
                outcome.loss_report = Some(measured.into());
                let result = outcome_into_py(py, &outcome);
                if filter == "error" {
                    let error = result.unwrap_err();
                    assert!(error.is_instance_of::<crate::errors::MappingLossWarning>(py));
                    let document = error.value(py).getattr("write_outcome").unwrap();
                    assert_eq!(
                        document
                            .get_item("status")
                            .unwrap()
                            .extract::<String>()
                            .unwrap(),
                        "committed"
                    );
                    assert!(error.to_string().contains("commit avvenuto"));
                    assert_eq!(
                        error.to_string().contains("conservativamente"),
                        basis == Basis::Conservative
                    );
                } else {
                    assert!(result.is_ok());
                }
            }
            outcome.loss_report = Some(report.clone().into());
            let mut uncertain: WriteOutcome = serde_json::from_str(include_str!(
                "../../../contracts/v2/examples/outcome-v3-unknown.json"
            ))
            .unwrap();
            let mut pending = report.clone();
            pending.effective[0].committed = 0;
            pending.effective[0].uncertain = 1;
            uncertain.loss_report = Some(pending.into());
            let error = outcome_into_py(py, &uncertain).unwrap_err();
            assert!(!error.is_instance_of::<crate::errors::MappingLossWarning>(py));
            assert!(error.cause(py).is_none());
            assert!(error
                .value(py)
                .getattr("details")
                .unwrap()
                .get_item("write_outcome")
                .is_ok());
            // Commit parziale: il filtro error non maschera l'errore primario.
            uncertain.status = plenora_database_core::outcome::WriteStatus::PartiallyCommitted;
            uncertain.rows.confirmed = 1;
            uncertain.loss_report = outcome.loss_report.clone();
            let error = outcome_into_py(py, &uncertain).unwrap_err();
            assert!(!error.is_instance_of::<crate::errors::MappingLossWarning>(py));
            if filter == "error" {
                assert!(error
                    .cause(py)
                    .unwrap()
                    .is_instance_of::<crate::errors::MappingLossWarning>(py));
            }
            outcome.loss_report = Some(
                plenora_database_core::loss::LossReport {
                    schema_version: 2,
                    policy: report.policy,
                    losses: report
                        .risk
                        .iter()
                        .map(plenora_database_core::execution_loss::RiskEntry::legacy)
                        .collect(),
                }
                .into(),
            );
        }
        manager
            .call_method1("__exit__", (py.None(), py.None(), py.None()))
            .unwrap();
    });
}

fn measured_report(report: &ExecutionLossReport, basis: Basis) -> ExecutionLossReport {
    let mut measured = report.clone();
    if basis == Basis::Conservative {
        use plenora_database_core::loss_registry::RuleId;
        measured.risk[0] = plenora_database_core::execution_loss::RiskEntry::new(
            0,
            RuleId::SsXml,
            measured.policy,
            None,
            None,
        );
        measured.evaluation.entries[0].rule_id = RuleId::SsXml;
        measured.effective[0].rule_id = RuleId::SsXml;
        measured.effective[0].category = measured.risk[0].category;
    }
    measured.evaluation.entries[0].basis = basis;
    measured.effective[0].basis = basis;
    measured.validate().unwrap();
    measured
}
