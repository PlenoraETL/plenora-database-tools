use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_db_sqlserver::{
    CertificatePolicy, SqlServerConfig, SqlServerInsertMode, SqlServerProvider,
    SqlServerSchemaEvolution, MAX_BIND_PARAMETERS, MAX_IDENTIFIER_CHARACTERS,
};

const fn assert_provider<T: Provider>() {}

#[test]
fn common_provider_surface_is_public_and_typed() {
    assert_provider::<SqlServerProvider>();
    let config = SqlServerConfig::new(
        "sql.example.test",
        "warehouse",
        "loader",
        SecretString::new("constructor-secret"),
    )
    .with_certificate_policy(CertificatePolicy::TrustServerCertificate);
    let provider = SqlServerProvider::new(config, 1_024, 4)
        .expect("provider")
        .with_insert_mode(SqlServerInsertMode::TdsBulk)
        .with_schema_evolution(SqlServerSchemaEvolution::AddNullableColumns);
    assert_eq!(Provider::kind(&provider), ProviderKind::Sqlserver);
    assert_eq!(MAX_BIND_PARAMETERS, 2_100);
    assert_eq!(MAX_IDENTIFIER_CHARACTERS, 128);
}
