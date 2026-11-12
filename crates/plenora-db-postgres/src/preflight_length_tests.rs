use super::*;

/// Un cast limitato puo perdere contenuto: non e soltanto un avviso sugli spazi.
#[test]
fn bounded_report_declares_content_loss() {
    let field = arrow_schema::Field::new("v", arrow_schema::DataType::Utf8, true);
    for native in ["character varying(10)", "character(10)"] {
        let loss = bounded_loss(0, &field, Some(native.to_owned())).unwrap();
        assert_eq!(loss.severity, LossSeverity::DataLoss);
        assert!(loss.reason.contains("contenuto oltre"));
        assert!(loss.reason.contains("non spazio"));
    }
}

#[test]
fn nullability_is_a_rejection_risk_not_data_alteration() {
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        let loss = nullability_loss(0, policy).unwrap();
        assert_eq!(loss.category, LossCategory::Nullability);
        assert_eq!(
            loss.severity,
            if policy == MappingPolicy::Strict {
                LossSeverity::DataLoss
            } else {
                LossSeverity::Information
            }
        );
    }
}
