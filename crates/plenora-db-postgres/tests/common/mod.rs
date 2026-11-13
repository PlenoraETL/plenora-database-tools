//! Configuration shared by the ignored integration campaigns.
pub fn secret() -> plenora_database_core::provider::SecretString {
    plenora_database_core::provider::SecretString::new(
        std::env::var("PLENORA_TEST_POSTGRES_DSN")
            .expect("live test requires PLENORA_TEST_POSTGRES_DSN"),
    )
}
