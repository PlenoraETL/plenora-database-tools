//! Vocabolario, rapporti e autorizzazioni pre-P2 congelati dalla 10.0.0.
use plenora_database_core::execution_loss::{Basis, ExecutionLossReport, RiskEntry};
use plenora_database_core::loss::{LossConsent, LossReport, MappingPolicy};
use plenora_database_core::loss_ledger::Classification;
use plenora_database_core::loss_registry::RuleId;
use plenora_database_core::plan::ProviderKind;
use serde_json::json;
#[path = "../../../tests/support/p1_snapshot.rs"]
mod snapshot;

#[test]
fn p1_registry_reports_and_authorization_baseline() {
    let mut observed = Vec::new();
    for provider in [
        ProviderKind::Mysql,
        ProviderKind::Postgres,
        ProviderKind::Sqlserver,
    ] {
        for &id in RuleId::ALL {
            for policy in [
                MappingPolicy::Strict,
                MappingPolicy::Compatible,
                MappingPolicy::Lossy,
                MappingPolicy::Native,
            ] {
                let canonical = id.canonical(provider);
                let risk = RiskEntry::new(
                    0,
                    canonical,
                    policy,
                    Some("source".into()),
                    Some("target".into()),
                );
                let legacy = LossReport {
                    schema_version: 2,
                    policy,
                    losses: vec![risk.legacy()],
                };
                let report = ExecutionLossReport::planned(policy, vec![risk]).unwrap();
                let mut decisions = Vec::new();
                for class in [
                    Classification::Exact(Basis::Exact),
                    Classification::WouldChange(Basis::Exact),
                    Classification::WouldChange(Basis::Conservative),
                    Classification::HardReject,
                ] {
                    for consent in [LossConsent::Deny, LossConsent::Accept] {
                        for key in [false, true] {
                            decisions.push(json!({"class": format!("{class:?}"), "consent": format!("{consent:?}"), "key": key,
                                "result": format!("{:?}", class.authorize(policy, consent, key))}));
                        }
                    }
                }
                observed.push(json!({"provider": provider, "id": id, "canonical": canonical,
                    "predicate": format!("{:?}", id.definition().predicate), "report": report,
                    "legacy": legacy, "permits": legacy.permits_execution(), "decisions": decisions}));
            }
        }
    }
    snapshot::check(
        std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../..")),
        "core_baseline",
        &json!(observed),
    );
}
