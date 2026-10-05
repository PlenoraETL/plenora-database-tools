use super::*;
use plenora_database_core::ErrorCategory;

fn target(secret: &str, arguments: Vec<String>) -> TargetRequest {
    TargetRequest {
        provider: "postgres".to_owned(),
        secret_environment: secret.to_owned(),
        provider_arguments: arguments,
    }
}

#[test]
fn target_applies_the_schema_pattern_and_limits() {
    let valid = target("PLENORA_DSN_1", vec!["--host".to_owned()])
        .validate()
        .unwrap();
    assert_eq!(valid.provider, ProviderKind::Postgres);
    for secret in [
        "",
        "1DSN",
        "DSN-PROD",
        "postgres://user:pw@host/db",
        &"A".repeat(129),
    ] {
        let error = target(secret, Vec::new()).validate().unwrap_err();
        assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
        // Il valore ricevuto puo essere una DSN: il messaggio nomina il campo.
        assert!(!error.message.contains("user:pw"));
    }
    assert!(target(&"A".repeat(128), Vec::new()).validate().is_ok());
    assert!(target("DSN", vec![String::new(); 33]).validate().is_err());
    assert!(target("DSN", vec!["x".repeat(4097)]).validate().is_err());
    assert!(target("DSN", vec!["x".repeat(4096); 32]).validate().is_ok());
    let unknown = TargetRequest {
        provider: "Postgres".to_owned(),
        ..target("DSN", Vec::new())
    };
    assert_eq!(
        unknown.validate().unwrap_err().category,
        ErrorCategory::InvalidConfiguration
    );
}

#[test]
fn requests_reject_unknown_keys_and_a_null_parameters_path() {
    let base = serde_json::json!({
        "provider": "postgres",
        "secret_environment": "DSN",
        "operation_path": "artifact://plans/read",
    });
    let request: OperationRequest = serde_json::from_value(base.clone()).unwrap();
    assert_eq!(request.parameters_path, None);
    let mut unknown = base.clone();
    unknown["sql"] = Value::from("SELECT 1");
    assert!(serde_json::from_value::<OperationRequest>(unknown).is_err());
    let mut null_parameters = base.clone();
    null_parameters["parameters_path"] = Value::Null;
    assert!(serde_json::from_value::<OperationRequest>(null_parameters).is_err());
    let mut null_arguments = base;
    null_arguments["provider_arguments"] = Value::Null;
    assert!(serde_json::from_value::<OperationRequest>(null_arguments).is_err());
}

#[test]
fn a_null_catalog_is_the_default_catalog() {
    let request: ListSchemasRequest = serde_json::from_value(serde_json::json!({
        "provider": "postgres",
        "secret_environment": "DSN",
        "catalog": null,
    }))
    .unwrap();
    assert_eq!(
        request.operation(),
        Operation::DatabaseListSchemas { source: None }
    );
}

#[test]
fn empty_schema_or_object_is_not_a_wildcard() {
    let request = DescribeObjectRequest {
        provider: "postgres".to_owned(),
        secret_environment: "DSN".to_owned(),
        provider_arguments: Vec::new(),
        catalog: None,
        schema: " ".to_owned(),
        object: "t".to_owned(),
    };
    assert_eq!(
        request.operation().unwrap_err().category,
        ErrorCategory::InvalidConfiguration
    );
}

#[test]
fn inspection_document_refuses_a_reserved_field() {
    let inspection = Inspection {
        operation: "database.list_schemas".to_owned(),
        document: serde_json::json!({"provider": "x"}),
    };
    assert_eq!(
        inspection_document(ProviderKind::Postgres, inspection)
            .unwrap_err()
            .category,
        ErrorCategory::Internal
    );
    let inspection = Inspection {
        operation: "database.list_schemas".to_owned(),
        document: serde_json::json!({"schemas": ["public"]}),
    };
    let document = inspection_document(ProviderKind::Postgres, inspection).unwrap();
    assert_eq!(document["schemas"][0], "public");
    assert_eq!(document["provider"], "postgres");
}
