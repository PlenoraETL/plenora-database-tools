use super::*;
use plenora_database_core::provider::SecretString;
use std::time::Duration;

#[test]
fn zero_capacity_is_rejected_without_network() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("secret"),
    );
    let error = MysqlPool::new(&config, 0).expect_err("zero pool capacity");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidConfiguration
    );
}

#[test]
fn checkout_preserves_the_independent_acquire_budget() {
    let config = MysqlConfig::new(
        "mysql.example.test",
        "warehouse",
        "loader",
        SecretString::new("secret"),
    )
    .with_timeouts(
        Duration::from_secs(2),
        Duration::from_secs(30),
        Duration::from_secs(11),
    );
    let pool = MysqlPool::new(&config, 1).expect("pool");
    assert_eq!(pool.checkout_timeout, Duration::from_secs(11));
    assert_eq!(pool.connect_timeout, Duration::from_secs(2));
}
