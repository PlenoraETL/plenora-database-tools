use super::*;
use plenora_database_core::execution_loss::{Basis, ExecutionLossReport};

// Python 3.11 warning filters are process-global. Serialize the two tests
// before acquiring the GIL so catch_warnings cannot restore another test's filter.
static WARNING_FILTERS: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn mysql_surface_vectors_warning_and_error_parity() {
    let _filters = WARNING_FILTERS.lock().expect("warning filter isolation");
    Python::initialize();
    Python::attach(|py| {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/mysql_permissive/surfaces.json"
        ))
        .unwrap();
        let warnings = py.import("warnings").unwrap();
        let manager = warnings.call_method0("catch_warnings").unwrap();
        manager.call_method0("__enter__").unwrap();
        for filter in ["default", "ignore", "error"] {
            warnings
                .call_method1(
                    "simplefilter",
                    (filter, py.get_type::<crate::errors::MappingLossWarning>()),
                )
                .unwrap();
            for case in vectors["cases"].as_array().unwrap() {
                let outcome: WriteOutcome =
                    serde_json::from_value(case["outcome"].clone()).unwrap();
                outcome.validate().unwrap();
                let converted = outcome_into_py(py, &outcome);
                if case["name"] == "committed" && filter == "error" {
                    let error = converted.unwrap_err();
                    assert!(error.is_instance_of::<crate::errors::MappingLossWarning>(py));
                    let document = error.value(py).getattr("write_outcome").unwrap();
                    assert_eq!(
                        document
                            .get_item("provider")
                            .unwrap()
                            .extract::<String>()
                            .unwrap(),
                        "mysql"
                    );
                    assert!(error.to_string().contains("non ripetere automaticamente"));
                } else if matches!(case["name"].as_str(), Some("exact" | "committed")) {
                    assert!(converted.is_ok());
                } else {
                    let error = converted.unwrap_err();
                    assert!(!error.is_instance_of::<crate::errors::MappingLossWarning>(py));
                    assert!(error.cause(py).is_none());
                    assert!(!error.to_string().contains("P6_SECRET_SENTINEL"));
                }
            }
        }
        manager
            .call_method1("__exit__", (py.None(), py.None(), py.None()))
            .unwrap();
    });
}

#[test]
fn warning_filters_preserve_the_committed_outcome() {
    let _filters = WARNING_FILTERS.lock().expect("warning filter isolation");
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
