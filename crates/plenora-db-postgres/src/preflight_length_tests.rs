use super::*;
use plenora_database_core::loss::{LossCategory, LossSeverity};

#[test]
fn catalog_crs_enrichment_does_not_change_declared_srid() {
    use plenora_database_core::loss::LossConsent;
    let srid = "4326".to_owned();
    let epsg = "EPSG:4326".to_owned();
    let other = "3857".to_owned();
    let local = "POSTGIS:4326".to_owned();
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        assert!(!crs_changes(
            policy,
            Some(&epsg),
            Some(&epsg),
            Some(&srid),
            Some(&srid)
        ));
        assert!(crs_changes(
            policy,
            None,
            Some(&epsg),
            Some(&other),
            Some(&srid)
        ));
        assert!(crs_changes(policy, None, Some(&epsg), None, Some(&srid)));
        assert!(crs_changes(
            policy,
            Some(&local),
            Some(&epsg),
            Some(&srid),
            Some(&srid)
        ));
        let changes = crs_changes(policy, None, Some(&epsg), Some(&srid), Some(&srid));
        let losses = if changes {
            vec![RuleId::PgCrs.legacy(0, None, Some(epsg.clone()))]
        } else {
            vec![]
        };
        let report = LossReport {
            schema_version: 2,
            policy,
            losses,
        };
        assert_eq!(changes, policy == MappingPolicy::Strict);
        assert_eq!(report.permits_execution(), policy != MappingPolicy::Strict);
        if changes {
            let loss = &report.losses[0];
            assert_eq!(loss.category, LossCategory::Crs);
            assert_eq!(loss.severity, LossSeverity::DataLoss);
            assert_eq!(
                loss.reason,
                "identificatore CRS sorgente e target differente"
            );
            assert_eq!(loss.source_type, None);
            assert_eq!(loss.target_type, Some(epsg.clone()));
        } else {
            plenora_database_core::prepared_execution::authorize_legacy(&report, LossConsent::Deny)
                .unwrap();
            assert!(report.losses.is_empty());
        }
    }
}

/// Predicati congelati dal preflight 99ec6332: nessuna esenzione B1-B6 in Strict.
#[test]
fn strict_selectors_match_903_baseline_enumeration() {
    let values = [
        None,
        Some("EPSG:4326".to_owned()),
        Some("POSTGIS:4326".to_owned()),
    ];
    let srids = [None, Some("4326".to_owned()), Some("3857".to_owned())];
    for source in &values {
        for target in &values {
            for source_srid in &srids {
                for target_srid in &srids {
                    let baseline = source != target && (source.is_some() || target.is_some());
                    assert_eq!(
                        crs_changes(
                            MappingPolicy::Strict,
                            source.as_ref(),
                            target.as_ref(),
                            source_srid.as_ref(),
                            target_srid.as_ref()
                        ),
                        baseline
                    );
                }
            }
        }
    }
    for key in [false, true] {
        for mask in 0_u8..32 {
            let leaves = [
                LeafLoss::ReducedTemporal,
                LeafLoss::Bounded,
                LeafLoss::Padded,
                LeafLoss::Rounding,
                LeafLoss::Jsonb,
            ];
            let actual = select_leaf_rules(
                MappingPolicy::Strict,
                key,
                leaves
                    .into_iter()
                    .enumerate()
                    .map(|(i, leaf)| (leaf, mask & (1 << i) != 0)),
            );
            // 9.0.3: temporal prima del continue Strict, altre foglie solo non-strict.
            let baseline = if mask & 1 != 0 {
                vec![RuleId::PgTemporal]
            } else {
                vec![]
            };
            assert_eq!(actual, baseline);
        }
    }
    assert_eq!(
        nullability_loss(0, MappingPolicy::Strict)
            .unwrap()
            .risk(MappingPolicy::Strict)
            .severity,
        LossSeverity::DataLoss
    );
}

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
        let loss = nullability_loss(0, policy).unwrap().risk(policy);
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
