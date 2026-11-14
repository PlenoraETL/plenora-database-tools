//! P1 non registra nuovi ID pubblici: confronta il progetto con A effettiva.
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::write_admission::{AdmissionKey, AdmissionTable, TargetDimensions};
use serde_json::{json, Value};
use std::collections::BTreeSet;

#[test]
fn mysql_fixture_and_declared_rules_are_bidirectional() {
    let table = AdmissionTable::parse(
        ProviderKind::Mysql,
        include_bytes!("../../../tests/fixtures/ammissione_scrittura/mysql.json"),
    )
    .unwrap();
    let declarations: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/mysql_permissive/declarations.json"
    ))
    .unwrap();
    let mut declared = BTreeSet::new();
    for rule in declarations["rules"].as_array().unwrap() {
        assert!(!rule["predicate"].as_str().unwrap().is_empty());
        for target in rule["targets"].as_array().unwrap() {
            assert!(declared
                .insert(json!([rule["arrow"], target["native"], target["rewrite"]]).to_string()));
        }
        for measurement in &table.fixture().misure {
            assert!(rule["proofs"][&measurement.server_major]["baseline"].is_string());
        }
    }
    for measurement in &table.fixture().misure {
        let server = table.server(&measurement.server_major).unwrap();
        let mut measured = BTreeSet::new();
        for key in &measurement.chiavi {
            let admission = AdmissionKey {
                arrow: key.arrow.clone(),
                nativo: key.nativo.clone(),
                dimensione: key.dimensione,
            };
            let dimensions = TargetDimensions {
                chars: key.dimensione,
                bytes: key.dimensione,
            };
            let actual = server.admit(&admission, dimensions).unwrap();
            assert_eq!(actual.len(), key.riscritture.len());
            for rewrite in &key.riscritture {
                assert!(measured.insert(json!([key.arrow, key.nativo, rewrite]).to_string()));
                assert!(actual.contains(
                    &plenora_database_core::write_admission::instantiate(
                        ProviderKind::Mysql,
                        *rewrite,
                        dimensions
                    )
                    .unwrap()
                ));
            }
        }
        assert_eq!(measured, declared);
        assert_eq!(measured.len(), 20);
    }
}
