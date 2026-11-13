//! La stessa matrice e verificata contro JSON Schema dal gate phase0.
use plenora_database_core::outcome::WriteOutcome;

#[test]
fn rust_acceptance_matches_the_shared_contract_matrix() {
    let cases: serde_json::Value =
        serde_json::from_str(include_str!("write_outcome_validation.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let accepted = serde_json::from_value::<WriteOutcome>(case["document"].clone())
            .is_ok_and(|outcome| outcome.validate().is_ok());
        assert_eq!(
            accepted,
            case["valid"].as_bool().unwrap(),
            "{}",
            case["name"]
        );
    }
}
