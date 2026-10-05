use super::*;

const REQUEST: &str = "018f3d84-7b2c-7f00-8000-000000000101";

#[test]
fn the_result_identity_is_a_new_deterministic_uuid() {
    let first = result_message_id(REQUEST);
    assert_eq!(first, result_message_id(REQUEST));
    assert_ne!(first, REQUEST);
    assert_ne!(
        first,
        result_message_id("018f3d84-7b2c-7f00-8000-000000000102")
    );
    assert!(canonical_uuid(&first));
    // Versione 8 e variante RFC 9562.
    assert_eq!(&first[14..15], "8");
    assert!(matches!(&first[19..20], "8" | "9" | "a" | "b"));
    assert_eq!(result_message_id("not-a-uuid"), NIL_UUID);
}

#[test]
fn only_canonical_decimal_versions_are_accepted() {
    assert_eq!(parse_version("1").unwrap(), 1);
    assert_eq!(parse_version("12").unwrap(), 12);
    for invalid in ["", "0", "01", "+1", "-1", "1.0", " 1", "99999999999"] {
        let error = parse_version(invalid).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Protocol, "{invalid}");
        assert_eq!(error.remote_effect, RemoteEffect::None);
    }
}

#[test]
fn uppercase_or_unhyphenated_uuids_are_not_aliases() {
    assert!(canonical_uuid(REQUEST));
    assert!(!canonical_uuid(&REQUEST.to_uppercase()));
    assert!(!canonical_uuid(&REQUEST.replace('-', "")));
    assert!(!canonical_uuid(&format!("{{{REQUEST}}}")));
    assert_eq!(public_uuid("x"), NIL_UUID);
}

#[test]
fn artifact_references_are_opaque_uris() {
    for valid in [
        "artifact://plans/read",
        "s3+v2://bucket/key",
        "secret.v1://x",
    ] {
        assert!(ensure_artifact_reference(valid).is_ok(), "{valid}");
    }
    for invalid in [
        "",
        "plans/read.json",
        "/abs/read.json",
        "C:\\read.json",
        "C:/read.json",
        "file:///read.json",
        "Artifact://x",
        "artifact://",
        "artifact://a\\b",
        "artifact://a/../b",
    ] {
        assert!(ensure_artifact_reference(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn deadlines_are_absolute_utc_instants() {
    assert!(deadline_instant("2999-01-01T00:00:00Z").is_ok());
    assert!(deadline_instant("2999-01-01T00:00:00+00:00").is_ok());
    for invalid in [
        "2999-01-01T01:00:00+01:00",
        "2999-01-01",
        "tomorrow",
        "2999-01-01T00:00:00",
    ] {
        let error = deadline_instant(invalid).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Protocol, "{invalid}");
    }
    let expired = deadline_instant("2000-01-01T00:00:00Z").unwrap_err();
    assert_eq!(expired.category, ErrorCategory::Timeout);
    assert_eq!(expired.phase, ErrorPhase::Validate);
    assert_eq!(expired.remote_effect, RemoteEffect::None);
}

#[test]
fn a_null_optional_metadata_value_is_not_an_absent_one() {
    let metadata = serde_json::json!({
        "plenora.message.id": REQUEST,
        "plenora.capability.name": CAPABILITY_NAME,
        "plenora.capability.version": "1",
        "plenora.capability.operation": "database.read",
        "plenora.operation.version": "1",
        "plenora.input.contract": "plenora-database-read-input-v1",
        "plenora.trace.correlation_id": REQUEST,
    });
    assert!(serde_json::from_value::<RuntimeRequestMetadata>(metadata.clone()).is_ok());
    for key in [
        "plenora.execution.deadline",
        "plenora.execution.idempotency_key",
        "plenora.message.causation_id",
    ] {
        let mut with_null = metadata.clone();
        with_null[key] = Value::Null;
        assert!(
            serde_json::from_value::<RuntimeRequestMetadata>(with_null).is_err(),
            "{key}: null letto come assente"
        );
    }
}

#[test]
fn host_errors_keep_their_axes_and_lose_their_text() {
    let host = DatabaseError {
        category: ErrorCategory::NotFound,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: None,
        execution_id: Some("host-internal".to_owned()),
        message: "s3://private-bucket/key resolved to /srv/data".to_owned(),
        diagnostics: None,
    };
    let redacted = redact_host_error(&host, "risoluzione rifiutata");
    assert_eq!(redacted.category, ErrorCategory::NotFound);
    assert_eq!(redacted.phase, ErrorPhase::Prepare);
    assert_eq!(redacted.message, "risoluzione rifiutata");
    assert_eq!(redacted.execution_id, None);
}

#[test]
fn the_runtime_discovery_document_binds_every_advertised_operation() {
    let document = runtime_capabilities();
    assert_eq!(document.interfaces.len(), 1);
    assert_eq!(document.interfaces[0].artifact, CAPABILITY_NAME);
    assert_eq!(
        entrypoint("database.read", 1),
        "plenora.database-tools#database.read@1"
    );
    assert!(document.operations.iter().all(|operation| operation
        .input
        .content_types
        .iter()
        .any(|value| value == JSON_CONTENT_TYPE)));
}
