use super::*;

#[test]
fn check_profile_pass_when_all_verified() {
    let evidence: Vec<_> = APPLICATION_OLTP_V1
        .required
        .iter()
        .map(|c| CapabilityEvidence::verified(*c))
        .collect();
    let report = check_profile(&APPLICATION_OLTP_V1, &evidence);
    assert_eq!(report.status, ProfileStatus::Pass);
    assert!(report.missing.is_empty());
    assert!(report.failed.is_empty());
}

#[test]
fn check_profile_fails_on_missing_capability() {
    let evidence: Vec<_> = APPLICATION_OLTP_V1
        .required
        .iter()
        .skip(1)
        .map(|c| CapabilityEvidence::verified(*c))
        .collect();
    let report = check_profile(&APPLICATION_OLTP_V1, &evidence);
    assert_eq!(report.status, ProfileStatus::Fail);
    assert_eq!(report.missing, vec![APPLICATION_OLTP_V1.required[0]]);
}

#[test]
fn check_profile_fails_on_failed_capability() {
    let evidence = vec![
        CapabilityEvidence::verified(Capability::Transactions),
        CapabilityEvidence::failed(Capability::Savepoints, "test failure"),
    ];
    let profile = ConformanceProfile {
        name: "T",
        required: &[Capability::Transactions, Capability::Savepoints],
    };
    let report = check_profile(&profile, &evidence);
    assert_eq!(report.status, ProfileStatus::Fail);
    assert!(report.missing.is_empty());
    assert_eq!(report.failed, vec![Capability::Savepoints]);
}

#[test]
fn evidence_serializes_snake_case() {
    let e = CapabilityEvidence::verified(Capability::OptimisticConcurrency);
    let json = serde_json::to_string(&e).unwrap();
    assert!(json.contains("optimistic_concurrency"));
    assert!(json.contains("verified"));
}
