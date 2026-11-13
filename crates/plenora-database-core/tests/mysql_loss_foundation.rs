//! Risoluzione MySQL indipendente dal writer ancora chiuso.
use arrow_schema::{DataType, TimeUnit};
use plenora_database_core::arrow::array::{Float32Array, Float64Array, TimestampMicrosecondArray};
use plenora_database_core::execution_loss::{
    Basis, ExecutionLossReport, RiskEntry, VersionedLossReport,
};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::loss_ledger::Classification;
use plenora_database_core::loss_registry::{
    classify_rewrite, compile_instances, resolve_rewrite, RuleId, RuleInstance, RuleParameters,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::store_rewrite::{StoreRewrite, TextLengthUnit};
use plenora_database_core::write_admission::{instantiate, AdmissionTable, TargetDimensions};
use std::collections::BTreeSet;

#[test]
fn every_mysql_fixture_rewrite_resolves_contextually_without_orphans() {
    let table = AdmissionTable::parse(
        ProviderKind::Mysql,
        include_bytes!("../../../tests/fixtures/ammissione_scrittura/mysql.json"),
    )
    .unwrap();
    let mut ids = BTreeSet::new();
    for measurement in &table.fixture().misure {
        let mut count = 0;
        for key in &measurement.chiavi {
            for &rewrite in &key.riscritture {
                let arrow = match key.arrow.as_str() {
                    "float32" => DataType::Float32,
                    "float64" => DataType::Float64,
                    "timestamp[us]" => DataType::Timestamp(TimeUnit::Microsecond, None),
                    _ => panic!("unexpected rewrite source"),
                };
                let rewrite =
                    instantiate(ProviderKind::Mysql, rewrite, TargetDimensions::default()).unwrap();
                let id =
                    resolve_rewrite(ProviderKind::Mysql, &arrow, &key.nativo, rewrite).unwrap();
                assert!(id.as_str().starts_with("mysql."));
                ids.insert(id);
                count += 1;
            }
        }
        assert_eq!(count, 20);
    }
    assert_eq!(
        ids,
        RuleId::ALL
            .iter()
            .copied()
            .filter(|id| id.as_str().starts_with("mysql."))
            .collect()
    );
}
#[test]
fn closed_rules_preserve_hard_reject_and_null_without_false_losses() {
    for native in [
        "double(10,2)",
        "double(16,4)",
        "double(22,0)",
        "double(255,30)",
    ] {
        let rewrite = if native == "double(255,30)" {
            StoreRewrite::Underflow { min_abs: 1e-10 }
        } else {
            StoreRewrite::Every
        };
        let array =
            Float64Array::from(vec![None, Some(f64::NAN), Some(f64::INFINITY), Some(1e-12)]);
        assert_eq!(
            classify_rewrite(
                ProviderKind::Mysql,
                native,
                rewrite,
                &array,
                0,
                TextLengthUnit::Characters
            )
            .unwrap(),
            Classification::Exact(Basis::Exact)
        );
        for row in 1..4 {
            assert_eq!(
                classify_rewrite(
                    ProviderKind::Mysql,
                    native,
                    rewrite,
                    &array,
                    row,
                    TextLengthUnit::Characters
                )
                .unwrap(),
                Classification::HardReject
            );
        }
    }
    let array = Float32Array::from(vec![0.0, 1.0, 0.1]);
    for row in 0..3 {
        assert_eq!(
            classify_rewrite(
                ProviderKind::Mysql,
                "float(7,3)",
                StoreRewrite::Every,
                &array,
                row,
                TextLengthUnit::Characters
            )
            .unwrap(),
            Classification::HardReject
        );
    }
}
#[test]
fn exact_predicates_and_unknown_contexts_are_fail_closed() {
    let array = Float64Array::from(vec![-0.0, 0.0]);
    for (row, expected) in [
        Classification::WouldChange(Basis::Exact),
        Classification::Exact(Basis::Exact),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            classify_rewrite(
                ProviderKind::Mysql,
                "double(255,30)",
                StoreRewrite::NegativeZero,
                &array,
                row,
                TextLengthUnit::Characters
            )
            .unwrap(),
            expected
        );
    }
    let array = TimestampMicrosecondArray::from(vec![Some(-1), Some(-1000), None]);
    for (row, expected) in [
        Classification::WouldChange(Basis::Exact),
        Classification::Exact(Basis::Exact),
        Classification::Exact(Basis::Exact),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(
            classify_rewrite(
                ProviderKind::Mysql,
                "datetime(3)",
                StoreRewrite::FractionalSeconds { digits: 3 },
                &array,
                row,
                TextLengthUnit::Characters
            )
            .unwrap(),
            expected
        );
    }
    assert!(resolve_rewrite(
        ProviderKind::Mysql,
        &DataType::Float64,
        "double",
        StoreRewrite::NegativeZero
    )
    .is_err());
    assert!(resolve_rewrite(
        ProviderKind::Mysql,
        &DataType::Float64,
        "double(255,30)",
        StoreRewrite::Underflow { min_abs: 1e-20 }
    )
    .is_err());
    let shared = RuleInstance {
        subject: None,
        field_id: 0,
        rule_id: RuleId::StoreEvery,
        parameters: RuleParameters::default(),
        source_type: None,
        target_type: None,
    };
    assert!(
        compile_instances(ProviderKind::Mysql, MappingPolicy::Compatible, vec![shared]).is_err()
    );
    let mysql = RuleInstance {
        subject: None,
        field_id: 0,
        rule_id: RuleId::MysqlNegativeZero,
        parameters: RuleParameters::default(),
        source_type: None,
        target_type: None,
    };
    for provider in [
        ProviderKind::Mariadb,
        ProviderKind::Postgres,
        ProviderKind::Sqlserver,
        ProviderKind::Db2,
        ProviderKind::Oracle,
    ] {
        assert!(
            compile_instances(provider, MappingPolicy::Compatible, vec![mysql.clone()]).is_err()
        );
    }
}
#[test]
fn new_reader_preserves_v3_and_legacy_without_promoting_missing_or_null() {
    let report = ExecutionLossReport::planned(
        MappingPolicy::Compatible,
        vec![RiskEntry::new(
            0,
            RuleId::MysqlNegativeZero,
            MappingPolicy::Compatible,
            None,
            None,
        )],
    )
    .unwrap();
    let json = serde_json::to_string(&report).unwrap();
    let read: VersionedLossReport = serde_json::from_str(&json).unwrap();
    assert!(matches!(read, VersionedLossReport::Execution(r) if r == report));
    assert!(serde_json::from_str::<VersionedLossReport>("null").is_err());
    assert!(serde_json::from_str::<VersionedLossReport>("{}").is_err());
    let legacy = r#"{"schema_version":2,"policy":"compatible","losses":[]}"#;
    assert!(matches!(
        serde_json::from_str::<VersionedLossReport>(legacy).unwrap(),
        VersionedLossReport::Legacy(_)
    ));
}
