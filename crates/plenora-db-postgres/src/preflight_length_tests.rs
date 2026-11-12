use super::*;
use plenora_database_core::loss::LossCategory;

/// Un cast limitato puo perdere contenuto: non e soltanto un avviso sugli spazi.
#[test]
fn bounded_report_declares_content_loss() {
    let field = arrow_schema::Field::new("v", arrow_schema::DataType::Utf8, true);
    for native in ["character varying(10)", "character(10)"] {
        let loss = RuleId::PgTextLimit.legacy(
            0,
            Some(field.data_type().to_string()),
            Some(native.to_owned()),
        );
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

#[test]
fn synthetic_leaf_plans_cover_create_existing_strict_non_strict_and_keys() {
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for key in [false, true] {
            for (leaf, id) in [
                (LeafLoss::ReducedTemporal, RuleId::PgTemporal),
                (LeafLoss::Bounded, RuleId::PgTextLimit),
                (LeafLoss::Padded, RuleId::PgPadText),
                (LeafLoss::Rounding, RuleId::PgNumeric),
                (LeafLoss::Jsonb, RuleId::PgJsonb),
            ] {
                assert!(select_leaf_rules(policy, key, [(leaf, false)]).is_empty());
                let expected = if (policy == MappingPolicy::Strict && id != RuleId::PgTemporal)
                    || (key && id == RuleId::PgPadText)
                {
                    vec![]
                } else {
                    vec![id]
                };
                assert_eq!(select_leaf_rules(policy, key, [(leaf, true)]), expected);
            }
        }
    }
}
