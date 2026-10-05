use super::*;

const REQUEST: &str = "018f3d84-7b2c-7f00-8000-000000000101";

#[test]
fn the_result_identity_is_a_new_deterministic_uuid() {
    let first = derived_uuid(REQUEST.as_bytes());
    assert_eq!(first, derived_uuid(REQUEST.as_bytes()));
    assert_ne!(first, REQUEST);
    assert_ne!(first, derived_uuid(b"other"));
    assert!(canonical_uuid(&first));
    // Versione 8 e variante RFC 9562.
    assert_eq!(&first[14..15], "8");
    assert!(matches!(&first[19..20], "8" | "9" | "a" | "b"));
    // Anche da byte che non sono un'identita valida.
    assert!(canonical_uuid(&derived_uuid(b"not json")));
}

#[test]
fn only_canonical_decimal_versions_are_canonical() {
    for valid in ["1", "12", "99999999999"] {
        assert!(canonical_version(valid), "{valid}");
    }
    for invalid in ["", "0", "01", "+1", "-1", "1.0", " 1", "uno"] {
        assert!(!canonical_version(invalid), "{invalid}");
    }
}

#[test]
fn routing_values_follow_the_vector_schema_patterns() {
    assert!(well_formed_operation("database.read"));
    assert!(well_formed_operation("database.transaction.begin"));
    for invalid in [
        "database",
        "Database.read",
        "database.",
        ".read",
        "database..read",
        "database.Read",
    ] {
        assert!(!well_formed_operation(invalid), "{invalid}");
    }
    assert!(well_formed_capability_name("plenora.storage-tools"));
    for invalid in [
        "plenora.database",
        "Plenora.database-tools",
        "plenora.-tools",
        "plenora.Db-tools",
    ] {
        assert!(!well_formed_capability_name(invalid), "{invalid}");
    }
    assert!(well_formed_contract("plenora-database-read-input-v1"));
    for invalid in [
        "plenora-database-read-input",
        "plenora--v1",
        "plenora-x-v01",
        "database-read-input-v1",
    ] {
        assert!(!well_formed_contract(invalid), "{invalid}");
    }
    assert!(well_formed_media_type(
        "application/vnd.apache.arrow.stream"
    ));
    for invalid in [
        "application",
        "application/json; charset=utf-8",
        "/json",
        "text/",
    ] {
        assert!(!well_formed_media_type(invalid), "{invalid}");
    }
}

#[test]
fn uppercase_or_unhyphenated_uuids_are_not_aliases() {
    assert!(canonical_uuid(REQUEST));
    assert!(!canonical_uuid(&REQUEST.to_uppercase()));
    assert!(!canonical_uuid(&REQUEST.replace('-', "")));
    assert!(!canonical_uuid(&format!("{{{REQUEST}}}")));
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
fn deadlines_follow_one_utc_grammar() {
    for valid in [
        "2999-01-01T00:00:00Z",
        "2999-01-01T00:00:00.5Z",
        "2999-01-01T00:00:00.123456789Z",
    ] {
        assert!(deadline_instant(valid).is_ok(), "{valid}");
    }
    for invalid in [
        "2999-01-01T00:00:00+00:00",
        "2999-01-01T00:00:00-00:00",
        "2999-01-01T01:00:00+01:00",
        "2999-01-01t00:00:00Z",
        "2999-01-01T00:00:00z",
        "2999-01-01 00:00:00Z",
        "2999-01-01T00:00:00.Z",
        "2999-01-01T00:00:00.1234567890Z",
        "2999-12-31T23:59:60Z",
        "2999-02-30T00:00:00Z",
        "2999-01-01",
        "tomorrow",
        "2999-01-01T00:00:00",
    ] {
        let error = deadline_instant(invalid).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Protocol, "{invalid}");
        assert_eq!(error.retry, RetryDisposition::Never, "{invalid}");
    }
    let expired = deadline_instant("2000-01-01T00:00:00Z").unwrap_err();
    assert_eq!(expired.category, ErrorCategory::Timeout);
    assert_eq!(expired.phase, ErrorPhase::Validate);
    assert_eq!(expired.remote_effect, RemoteEffect::None);
    assert_eq!(expired.retry, RetryDisposition::Never);
}

#[test]
fn a_null_metadata_value_is_not_an_absent_one() {
    let base = serde_json::json!({
        "content_type": "application/json",
        "metadata": {"plenora.message.id": REQUEST},
        "payload": {},
    });
    assert!(RuntimeInvocation::from_json(base.to_string().as_bytes()).is_ok());
    for key in [
        "plenora.execution.deadline",
        "plenora.execution.idempotency_key",
        "plenora.message.causation_id",
    ] {
        let mut with_null = base.clone();
        with_null["metadata"][key] = Value::Null;
        let error = RuntimeInvocation::from_json(with_null.to_string().as_bytes())
            .expect_err("null letto come assente");
        assert_eq!(error.category, ErrorCategory::Protocol, "{key}");
    }
}

#[test]
fn bytes_on_the_sink_make_a_later_error_partial() {
    let error = artifact_error(ErrorPhase::Write, "x");
    assert_eq!(
        after_sink_bytes(error.clone(), 0).remote_effect,
        RemoteEffect::None
    );
    let partial = after_sink_bytes(error, 10);
    assert_eq!(partial.remote_effect, RemoteEffect::Partial);
    assert_eq!(partial.retry, RetryDisposition::Never);
    let unknown = DatabaseError {
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        ..artifact_error(ErrorPhase::Read, "y")
    };
    assert_eq!(
        after_sink_bytes(unknown, 10).remote_effect,
        RemoteEffect::Unknown
    );
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
