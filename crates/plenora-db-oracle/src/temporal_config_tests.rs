use super::*;

#[test]
fn temporal_config_rejects_duration_max_before_io() {
    let mut config = OracleConfig::new("localhost", "db", "user");
    config.connect_timeout = Duration::MAX;
    assert!(config.validate().is_err());
    config.connect_timeout = Duration::from_secs(10);
    config.operation_timeout = Duration::MAX;
    assert!(config.validate().is_err());
    config.operation_timeout = Duration::from_secs(30);
    config.acquire_timeout = Duration::MAX;
    assert!(config.validate().is_err());
}
