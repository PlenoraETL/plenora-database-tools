use super::*;

#[test]
fn probe_contract_is_serializable_without_driver_types() {
    let probe = SqlServerProbe {
        product_version: "16.0".to_owned(),
        product_level: "RTM".to_owned(),
        edition: "Developer".to_owned(),
        engine_edition: 3,
        hadr_enabled: false,
        database: "test".to_owned(),
        compatibility_level: 160,
        collation: "Latin1_General_100_CI_AS_SC".to_owned(),
        read_committed_snapshot: false,
        snapshot_isolation_state: 0,
        geometry_type_id: Some(240),
        geography_type_id: Some(241),
        polybase_installed: false,
    };
    let json = serde_json::to_value(probe).expect("serialize probe");
    assert_eq!(json["compatibility_level"], 160);
}
