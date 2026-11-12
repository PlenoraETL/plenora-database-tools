use super::*;

#[test]
fn warning_filters_preserve_the_committed_outcome() {
    Python::initialize();
    Python::attach(|py| {
        let warnings = py.import("warnings").unwrap();
        let manager = warnings.call_method0("catch_warnings").unwrap();
        manager.call_method0("__enter__").unwrap();
        warnings
            .call_method1(
                "simplefilter",
                ("error", py.get_type::<crate::errors::MappingLossWarning>()),
            )
            .unwrap();
        let mut outcome: WriteOutcome = serde_json::from_str(include_str!(
            "../../../contracts/v2/examples/outcome-v3-committed.json"
        ))
        .unwrap();
        assert!(outcome_into_py(py, &outcome).is_ok());
        for severity in [
            plenora_database_core::loss::LossSeverity::Information,
            plenora_database_core::loss::LossSeverity::Warning,
            plenora_database_core::loss::LossSeverity::DataLoss,
        ] {
            let plenora_database_core::execution_loss::VersionedLossReport::Legacy(report) =
                outcome.loss_report.as_mut().unwrap()
            else {
                panic!("legacy fixture");
            };
            report.losses = vec![plenora_database_core::loss::MappingLoss {
                field_id: 1,
                category: plenora_database_core::loss::LossCategory::Precision,
                severity,
                reason: "precisione ridotta".into(),
                source_type: None,
                target_type: None,
            }];
            let result = outcome_into_py(py, &outcome);
            if severity == plenora_database_core::loss::LossSeverity::Information {
                assert!(result.is_ok());
            } else {
                let error = result.unwrap_err();
                assert!(error.is_instance_of::<crate::errors::MappingLossWarning>(py));
                assert!(error.is_instance_of::<pyo3::exceptions::PyUserWarning>(py));
                let document = error.value(py).getattr("write_outcome").unwrap();
                assert_eq!(
                    document
                        .get_item("status")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    "committed"
                );
                assert!(!error.to_string().contains("precisione ridotta"));
            }
        }
        let mut uncertain: WriteOutcome = serde_json::from_str(include_str!(
            "../../../contracts/v2/examples/outcome-v3-loss.json"
        ))
        .unwrap();
        uncertain.loss_report = outcome.loss_report.clone();
        let error = outcome_into_py(py, &uncertain).unwrap_err();
        assert!(!error.is_instance_of::<crate::errors::MappingLossWarning>(py));
        assert!(error
            .cause(py)
            .unwrap()
            .is_instance_of::<crate::errors::MappingLossWarning>(py));
        assert!(error
            .value(py)
            .getattr("details")
            .unwrap()
            .get_item("write_outcome")
            .is_ok());
        manager
            .call_method1("__exit__", (py.None(), py.None(), py.None()))
            .unwrap();
    });
}
