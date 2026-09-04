use super::*;
// Rileggono l'artefatto IPC: servono ai soli test del percorso
// PostgreSQL che lo producono.
#[cfg(feature = "postgres")]
use arrow_ipc::reader::FileReader;
#[cfg(feature = "postgres")]
use plenora_database_core::arrow::array::{ArrayRef, BinaryArray};
#[cfg(feature = "postgres")]
use plenora_database_core::arrow::schema::{Field, Schema};
#[cfg(feature = "postgres")]
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
#[cfg(feature = "postgres")]
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};
#[cfg(feature = "postgres")]
use std::collections::{HashMap, VecDeque};
#[cfg(feature = "postgres")]
use std::fs::File;
#[cfg(feature = "postgres")]
use std::path::PathBuf;
#[cfg(feature = "postgres")]
use std::sync::Arc;

#[test]
fn canonical_requests_reject_fields_from_another_operation() {
    let request = json!({
        "provider": "postgres",
        "secret_environment": "PLENORA_TEST_DSN",
        "catalog": "warehouse"
    });
    assert!(serde_json::from_value::<CanonicalTarget>(request).is_err());

    let request = json!({
        "provider": "postgres",
        "secret_environment": "PLENORA_TEST_DSN",
        "operation_path": "read.json",
        "sql": "SELECT 1"
    });
    assert!(serde_json::from_value::<CanonicalReadRequest>(request).is_err());
}

#[test]
fn canonical_schema_inspection_preserves_the_optional_catalog() {
    let request: CanonicalListSchemasRequest = serde_json::from_value(json!({
        "provider": "postgres",
        "secret_environment": "PLENORA_TEST_DSN",
        "catalog": "warehouse"
    }))
    .expect("richiesta list_schemas pubblica");
    assert_eq!(request.catalog.as_deref(), Some("warehouse"));
}

/// Le posizionali di `database-describe` si leggono nell'ordine scritto.
///
/// L'inversione e il difetto che questo test esiste per escludere, e non
/// e teorico: `<schema> <object>` sono due stringhe, quindi scambiarle
/// non produce nessun errore di tipo e nessun errore di parsing. Si
/// scoprirebbe contro un server, come "oggetto inesistente" su un nome
/// che esiste.
#[test]
fn describe_reads_the_schema_before_the_object() {
    let mut args = ["vendite", "fatture", "host"]
        .map(str::to_owned)
        .into_iter();
    let operation = describe_source(&mut args).expect("sorgente");
    let Operation::DatabaseDescribeObject { source } = operation else {
        panic!("operazione inattesa");
    };
    assert_eq!(source.schema.as_deref(), Some("vendite"));
    assert_eq!(source.object, "fatture");
    // Cio che resta e degli argomenti del provider, che li consuma dopo.
    assert_eq!(args.next().as_deref(), Some("host"));
}

/// Un valore vuoto non e un carattere jolly.
///
/// `DatabaseListObjects` con schema vuoto significa "tutti gli oggetti":
/// accettando la stringa vuota, uno schema dimenticato — o una variabile
/// di shell non espansa — diventerebbe in silenzio una domanda piu larga
/// di quella scritta, e la risposta avrebbe l'aria di essere quella
/// giusta.
#[test]
fn an_empty_schema_is_refused_instead_of_widening_the_question() {
    for empty in ["", "   "] {
        let mut args = std::iter::once(empty.to_owned());
        assert!(
            list_objects_source(&mut args).is_err(),
            "schema {empty:?} accettato"
        );
    }
}

/// Le due operazioni senza posizionali non ne consumano.
///
/// Se le consumassero, il primo argomento del provider — l'host —
/// sparirebbe dentro l'operazione e il parser del provider vedrebbe una
/// riga piu corta di quella scritta.
#[test]
fn the_operations_without_positionals_leave_the_provider_arguments_alone() {
    for source in [
        list_catalogs_source as fn(&mut dyn Iterator<Item = String>) -> CliResult<Operation>,
        list_schemas_source,
    ] {
        let mut args = ["host", "db", "utente"].map(str::to_owned).into_iter();
        source(&mut args).expect("sorgente");
        assert_eq!(args.count(), 3);
    }
}

#[test]
fn provider_neutral_inspection_has_one_flat_self_describing_envelope() {
    let output = inspection_output(
        ProviderKind::Mysql,
        Inspection {
            operation: "database.list_schemas".to_owned(),
            document: json!({"schemas": ["application"]}),
        },
    )
    .expect("inspection envelope");
    assert_eq!(output["schema_version"], 1);
    assert_eq!(output["provider"], "mysql");
    assert_eq!(output["operation"], "database.list_schemas");
    assert_eq!(output["schemas"], json!(["application"]));
    assert!(output.get("document").is_none());
}

#[test]
fn provider_neutral_inspection_rejects_reserved_document_fields() {
    let error = inspection_output(
        ProviderKind::Postgres,
        Inspection {
            operation: "database.list_schemas".to_owned(),
            document: json!({"provider": "forged"}),
        },
    )
    .expect_err("reserved field collision");
    assert!(format!("{error:?}").contains("campo CLI riservato"));
}

#[cfg(feature = "postgres")]
struct TestStream {
    schema: SchemaRef,
    outcomes: VecDeque<plenora_database_core::Result<Option<RecordBatch>>>,
}

#[cfg(feature = "postgres")]
impl BatchStream for TestStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a plenora_database_core::CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        let outcome = self.outcomes.pop_front().unwrap_or(Ok(None));
        Box::pin(async move { outcome })
    }
}

// Nomina le directory temporanee con la sequenza del percorso IPC:
// esiste per i test di materializzazione, che sono PostgreSQL.
#[cfg(feature = "postgres")]
struct TestDirectory(PathBuf);

#[cfg(feature = "postgres")]
impl TestDirectory {
    fn new(name: &str) -> Self {
        let sequence = IPC_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "plenora-database-cli-{name}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create isolated test directory");
        Self(path)
    }

    fn output(&self) -> PathBuf {
        self.0.join("output.arrow")
    }
}

#[cfg(feature = "postgres")]
impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(feature = "postgres")]
fn partial_artifacts(output: &Path) -> Vec<PathBuf> {
    let prefix = format!(
        ".{}.partial-",
        output.file_name().expect("output name").to_string_lossy()
    );
    fs::read_dir(output.parent().expect("output parent"))
        .expect("read test directory")
        .map(|entry| entry.expect("directory entry").path())
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        })
        .collect()
}

#[cfg(feature = "postgres")]
fn stream_with_outcomes(
    outcomes: VecDeque<plenora_database_core::Result<Option<RecordBatch>>>,
) -> TestStream {
    let field = Field::new("geom", plenora_database_core::arrow::DataType::Binary, true)
        .with_metadata(HashMap::from([
            ("ARROW:extension:name".to_owned(), "geoarrow.wkb".to_owned()),
            (
                "plenora.geometry.axis_order".to_owned(),
                "unknown".to_owned(),
            ),
            ("plenora.geometry.crs_id".to_owned(), "EPSG:4326".to_owned()),
            (
                "plenora.geometry.crs_resolution".to_owned(),
                "resolved".to_owned(),
            ),
            ("plenora.geometry.dimensions".to_owned(), "xy".to_owned()),
            ("plenora.geometry.encoding".to_owned(), "wkb".to_owned()),
            ("plenora.geometry.srid".to_owned(), "4326".to_owned()),
            ("plenora.geometry.types".to_owned(), "point".to_owned()),
        ]));
    let schema = Arc::new(Schema::new_with_metadata(
        vec![field],
        HashMap::from([("plenora.contract.version".to_owned(), "1".to_owned())]),
    ));
    TestStream { schema, outcomes }
}

#[cfg(feature = "postgres")]
fn test_batch(schema: SchemaRef) -> RecordBatch {
    let values: ArrayRef = Arc::new(BinaryArray::from(vec![
        Some(
            &[
                1_u8, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ][..],
        ),
        None,
    ]));
    RecordBatch::try_new(schema, vec![values]).expect("record batch")
}

#[test]
fn crs_error_envelope_matches_protocol_v2() {
    let envelope = CliError::Fatal(DatabaseError {
        category: ErrorCategory::Crs,
        phase: ErrorPhase::Validate,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: "identificatore CRS e SRID numerico divergenti".to_owned(),
        diagnostics: None,
    })
    .to_json()
    .expect("serializable error");
    let value: serde_json::Value = serde_json::from_str(&envelope).expect("valid JSON");
    assert_eq!(
        value,
        json!({
            "status": "error",
            "protocol_version": 2,
            "component": "plenora-database-tools",
            "component_version": env!("CARGO_PKG_VERSION"),
            "contract": "plenora-error-v1",
            "command": "output",
            "error": {
                "category": "crs",
                "phase": "validate",
                "remote_effect": "none",
                "retry": {"kind": "never"},
                "provider": null,
                "execution_id": null,
                "message": "identificatore CRS e SRID numerico divergenti"
            }
        })
    );
}

#[test]
fn delayed_retry_is_explicit_and_keeps_the_delay() {
    let envelope = CliError::Fatal(DatabaseError {
        category: ErrorCategory::Transient,
        phase: ErrorPhase::Connect,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::After(250),
        provider: None,
        execution_id: None,
        message: "servizio temporaneamente non disponibile".to_owned(),
        diagnostics: None,
    })
    .to_json()
    .expect("serializable error");
    let value: serde_json::Value = serde_json::from_str(&envelope).expect("valid JSON");
    assert_eq!(value["error"]["retry"]["kind"], "after");
    assert_eq!(value["error"]["retry"]["delay_ms"], 250);
}

#[test]
fn serialization_fallback_is_a_canonical_error_envelope() {
    let value: serde_json::Value =
        serde_json::from_str(ERROR_SERIALIZATION_FALLBACK).expect("valid fallback JSON");
    assert_eq!(value["status"], "error");
    assert_eq!(value["protocol_version"], 2);
    assert_eq!(value["component"], "plenora-database-tools");
    assert_eq!(value["error"]["category"], "internal");
    assert_eq!(value["error"]["phase"], "finalize");
    assert_eq!(value["error"]["remote_effect"], "none");
    assert_eq!(value["error"]["retry"]["kind"], "never");
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn ipc_materialization_preserves_schema_metadata_and_rows() {
    let directory = TestDirectory::new("success");
    let output = directory.output();
    let mut stream = stream_with_outcomes(VecDeque::new());
    let batch = test_batch(stream.schema());
    stream.outcomes = VecDeque::from([Ok(Some(batch)), Ok(None)]);

    let report = write_stream_to_ipc(&output, &mut stream, &CancellationToken::new())
        .await
        .expect("materialize IPC");

    assert_eq!(report["rows"], 2);
    assert_eq!(report["batches"], 1);
    assert_eq!(report["format"], "arrow_ipc_file");
    assert_eq!(report["status"], "materialized");
    assert_eq!(report["schema_version"], 1);
    assert!(matches!(
        report["durability"].as_str(),
        Some("confirmed" | "unconfirmed")
    ));
    assert_eq!(report["staging_cleanup"], "complete");
    let reader =
        FileReader::try_new(File::open(&output).expect("open IPC"), None).expect("read IPC");
    assert_eq!(reader.schema().metadata()["plenora.contract.version"], "1");
    assert_eq!(
        reader.schema().field(0).metadata()["plenora.geometry.crs_id"],
        "EPSG:4326"
    );
    assert_eq!(
        reader.schema().field(0).metadata()["plenora.geometry.srid"],
        "4326"
    );
    assert_eq!(
        reader.schema().field(0).metadata()["plenora.geometry.axis_order"],
        "unknown"
    );
    assert!(partial_artifacts(&output).is_empty());
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn ipc_materialization_never_publishes_partial_output() {
    let directory = TestDirectory::new("failure");
    let output = directory.output();
    let mut stream = stream_with_outcomes(VecDeque::new());
    let batch = test_batch(stream.schema());
    stream.outcomes = VecDeque::from([
        Ok(Some(batch)),
        Err(DatabaseError::cancelled(
            Some(ProviderKind::Postgres),
            ErrorPhase::Read,
            "fixture cancellation",
        )),
    ]);

    let error = write_stream_to_ipc(&output, &mut stream, &CancellationToken::new())
        .await
        .expect_err("stream failure");

    assert_eq!(error.database_error().category, ErrorCategory::Cancelled);
    assert!(!output.exists());
    assert!(partial_artifacts(&output).is_empty());
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn ipc_materialization_never_overwrites_existing_output() {
    let directory = TestDirectory::new("conflict");
    let output = directory.output();
    fs::write(&output, b"existing artifact").expect("write existing output");
    let mut stream = stream_with_outcomes(VecDeque::new());

    let error = write_stream_to_ipc(&output, &mut stream, &CancellationToken::new())
        .await
        .expect_err("existing output must be rejected");

    assert_eq!(error.database_error().category, ErrorCategory::Conflict);
    assert_eq!(error.database_error().phase, ErrorPhase::Validate);
    assert_eq!(error.database_error().provider, None);
    assert_eq!(
        fs::read(&output).expect("existing output"),
        b"existing artifact"
    );
    assert!(partial_artifacts(&output).is_empty());
}

#[cfg(feature = "postgres")]
#[test]
fn ipc_options_are_bounded_and_caller_configurable() {
    let defaults = parse_ipc_options(&mut std::iter::empty()).expect("default IPC options");
    assert_ne!(defaults.limits.rows, u64::MAX);
    assert_ne!(defaults.limits.output_bytes, u64::MAX);
    assert!(defaults.limits.duration_ms > 30_000);
    assert!(defaults.order_by.is_empty());

    let mut arguments = [
        "--max-rows",
        "123",
        "--max-output-bytes",
        "456789",
        "--timeout-ms",
        "90000",
        "--order-by",
        "event_id",
    ]
    .into_iter()
    .map(str::to_owned);
    let configured = parse_ipc_options(&mut arguments).expect("configured IPC options");

    assert_eq!(configured.limits.rows, 123);
    assert_eq!(configured.limits.output_bytes, 456_789);
    assert_eq!(configured.limits.duration_ms, 90_000);
    assert_eq!(configured.order_by.len(), 1);
    assert_eq!(configured.order_by[0].field, "event_id");
}

#[cfg(feature = "postgres")]
#[test]
fn ipc_options_reject_zero_invalid_and_unknown_values() {
    for arguments in [
        vec!["--max-rows", "0"],
        vec!["--timeout-ms", "not-a-number"],
        vec!["--unknown", "1"],
        vec!["--order-by"],
    ] {
        let mut arguments = arguments.into_iter().map(str::to_owned);
        assert!(parse_ipc_options(&mut arguments).is_err());
    }
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_probe_parser_accepts_private_ca_and_complete_client_identity_env_names() {
    let mut args = [
        "--tls-ca-path-env",
        "PG_CA_PATH_ENV",
        "--tls-client-cert-path-env",
        "PG_CERT_PATH_ENV",
        "--tls-client-key-path-env",
        "PG_KEY_PATH_ENV",
    ]
    .into_iter()
    .map(str::to_owned);
    assert_eq!(
        parse_provider_arguments(ProviderKind::Postgres, &mut args)
            .expect("PostgreSQL private CA/mTLS env names"),
        ProviderArguments::Postgres {
            tls: TlsPathEnvironments {
                ca: Some("PG_CA_PATH_ENV".to_owned()),
                client_certificate: Some("PG_CERT_PATH_ENV".to_owned()),
                client_key: Some("PG_KEY_PATH_ENV".to_owned()),
                mode: None,
            },
        }
    );
}

#[test]
fn public_provider_parser_covers_the_complete_contract_catalog() {
    for (name, expected) in [
        ("postgres", ProviderKind::Postgres),
        ("mysql", ProviderKind::Mysql),
        ("mariadb", ProviderKind::Mariadb),
        ("sqlserver", ProviderKind::Sqlserver),
        ("oracle", ProviderKind::Oracle),
        ("db2", ProviderKind::Db2),
        ("sqlite", ProviderKind::Sqlite),
        ("duckdb", ProviderKind::Duckdb),
    ] {
        assert_eq!(parse_provider_kind(name).expect("known provider"), expected);
    }
    assert!(parse_provider_kind("unknown").is_err());
}

#[test]
fn provider_factories_resolve_private_ca_paths_from_environment() {
    let secret = SecretString::new("test-only-secret");
    #[allow(clippy::vec_init_then_push)] // le push sono cfg-gated
    let matrix: Vec<(ProviderKind, Vec<&str>)> = {
        // Ogni riga esiste soltanto quando l'adapter corrispondente è
        // compilato; altrimenti il test misurerebbe "adapter assente"
        // invece del fail-close TLS.
        #[allow(unused_mut)]
        let mut m: Vec<(ProviderKind, Vec<&str>)> = Vec::new();
        #[cfg(feature = "postgres")]
        m.push((ProviderKind::Postgres, Vec::new()));
        #[cfg(feature = "mysql")]
        m.push((
            ProviderKind::Mysql,
            vec!["db.example.test", "warehouse", "loader"],
        ));
        #[cfg(feature = "sqlserver")]
        m.push((
            ProviderKind::Sqlserver,
            vec!["db.example.test", "warehouse", "loader"],
        ));
        #[cfg(feature = "oracle")]
        m.push((
            ProviderKind::Oracle,
            vec!["db.example.test", "warehouse", "loader"],
        ));
        #[cfg(feature = "db2")]
        m.push((
            ProviderKind::Db2,
            vec!["db.example.test", "warehouse", "loader"],
        ));
        m
    };
    for (kind, positional) in matrix {
        let mut values = positional
            .into_iter()
            .chain([
                "--tls-ca-path-env",
                "PLENORA_TEST_DELIBERATELY_MISSING_TLS_CA_PATH_7219",
            ])
            .map(str::to_owned);
        let error = build_provider(kind, &secret, &mut values)
            .err()
            .expect("missing TLS CA path environment must fail closed");
        assert_eq!(error.database_error().category, ErrorCategory::InvalidPlan);
        assert_eq!(error.database_error().message, "variabile path TLS assente");
    }
}

#[test]
fn implemented_provider_factories_are_offline_and_typed() {
    let secret = SecretString::new("test-only-secret");
    // Stessa ragione del test qui sopra: un binario senza la feature
    // `postgres` non ha quel provider da costruire, e pretenderlo
    // rendeva il test rosso per l'assenza dell'adapter invece che per
    // cio che verifica.
    #[cfg(feature = "postgres")]
    {
        let mut postgres_args = std::iter::empty();
        assert_eq!(
            build_provider(ProviderKind::Postgres, &secret, &mut postgres_args)
                .expect("PostgreSQL provider")
                .kind(),
            ProviderKind::Postgres
        );
    }

    #[allow(clippy::vec_init_then_push)] // le push sono cfg-gated
    let structured: Vec<ProviderKind> = {
        #[allow(unused_mut)]
        let mut v: Vec<ProviderKind> = Vec::new();
        #[cfg(feature = "mysql")]
        v.push(ProviderKind::Mysql);
        #[cfg(feature = "sqlserver")]
        v.push(ProviderKind::Sqlserver);
        #[cfg(feature = "oracle")]
        v.push(ProviderKind::Oracle);
        #[cfg(feature = "db2")]
        v.push(ProviderKind::Db2);
        v
    };
    for kind in structured {
        let mut args = ["db.example.test", "warehouse", "loader"]
            .into_iter()
            .map(str::to_owned);
        assert_eq!(
            build_provider(kind, &secret, &mut args)
                .expect("structured provider")
                .kind(),
            kind
        );
    }
}

#[cfg(feature = "oracle")]
#[test]
fn oracle_provider_arguments_preserve_defaults_and_explicit_plaintext() {
    let mut default_args = ["db.example.test", "freepdb1", "loader"]
        .into_iter()
        .map(str::to_owned);
    assert_eq!(
        parse_provider_arguments(ProviderKind::Oracle, &mut default_args)
            .expect("default Oracle arguments"),
        ProviderArguments::Oracle {
            host: "db.example.test".to_owned(),
            service: "freepdb1".to_owned(),
            username: "loader".to_owned(),
            port: None,
            tls: TlsPathEnvironments::default(),
        }
    );

    let secret = SecretString::new("test-only-secret");
    let mut plaintext_args = [
        "db.example.test",
        "freepdb1",
        "loader",
        "1521",
        "--tls-mode",
        "disable",
    ]
    .into_iter()
    .map(str::to_owned);
    assert_eq!(
        build_provider(ProviderKind::Oracle, &secret, &mut plaintext_args)
            .expect("Oracle provider with explicit plaintext")
            .kind(),
        ProviderKind::Oracle
    );

    let mut invalid_mode = [
        "db.example.test",
        "freepdb1",
        "loader",
        "--tls-mode",
        "opportunistic",
    ]
    .into_iter()
    .map(str::to_owned);
    let error = build_provider(ProviderKind::Oracle, &secret, &mut invalid_mode)
        .err()
        .expect("unknown Oracle TLS mode");
    assert_eq!(error.database_error().category, ErrorCategory::InvalidPlan);
}

#[cfg(feature = "db2")]
#[test]
fn db2_provider_arguments_preserve_defaults_and_explicit_port() {
    let mut default_args = ["db.example.test", "warehouse", "loader"]
        .into_iter()
        .map(str::to_owned);
    assert_eq!(
        parse_provider_arguments(ProviderKind::Db2, &mut default_args)
            .expect("porta Db2 di default"),
        ProviderArguments::Db2 {
            host: "db.example.test".to_owned(),
            database: "warehouse".to_owned(),
            username: "loader".to_owned(),
            port: None,
            tls: TlsPathEnvironments::default(),
        }
    );

    let secret = SecretString::new("test-only-secret");
    let mut explicit_args = ["db.example.test", "warehouse", "loader", "50001"]
        .into_iter()
        .map(str::to_owned);
    assert_eq!(
        build_provider(ProviderKind::Db2, &secret, &mut explicit_args)
            .expect("provider Db2 con porta esplicita")
            .kind(),
        ProviderKind::Db2
    );

    let mut plaintext_args = [
        "db.example.test",
        "warehouse",
        "loader",
        "--tls-mode",
        "disable",
    ]
    .into_iter()
    .map(str::to_owned);
    assert_eq!(
        parse_provider_arguments(ProviderKind::Db2, &mut plaintext_args)
            .expect("opt-out plaintext Db2 esplicito"),
        ProviderArguments::Db2 {
            host: "db.example.test".to_owned(),
            database: "warehouse".to_owned(),
            username: "loader".to_owned(),
            port: None,
            tls: TlsPathEnvironments {
                mode: Some("disable".to_owned()),
                ..TlsPathEnvironments::default()
            },
        }
    );

    let mut invalid_mode = [
        "db.example.test",
        "warehouse",
        "loader",
        "--tls-mode",
        "opportunistic",
    ]
    .into_iter()
    .map(str::to_owned);
    let error = build_provider(ProviderKind::Db2, &secret, &mut invalid_mode)
        .err()
        .expect("mode TLS Db2 sconosciuto");
    assert_eq!(error.database_error().category, ErrorCategory::InvalidPlan);
}

#[cfg(feature = "postgres")]
#[test]
fn legacy_postgres_probe_requires_the_same_verified_tls_policy() {
    let provider = legacy_postgres_probe_provider();
    assert!(
        format!("{provider:?}").contains("tls_mode: Require"),
        "legacy PostgreSQL probe must not silently downgrade to plaintext: {provider:?}"
    );
}

#[cfg(feature = "postgres")]
#[test]
fn provider_neutral_postgres_probe_requires_verified_tls() {
    let provider = postgres_provider_for_probe();
    assert!(
        format!("{provider:?}").contains("tls_mode: Require"),
        "PostgreSQL probe must not silently downgrade to plaintext: {provider:?}"
    );
}

#[cfg(feature = "postgres")]
#[test]
fn provider_neutral_postgres_probe_honors_only_unambiguous_insecure_local() {
    let provider =
        postgres_provider_for_probe_with_tls_policy(&TlsPathEnvironments::default(), true)
            .expect("explicit local opt-out");
    assert!(
        format!("{provider:?}").contains("tls_mode: Disabled"),
        "explicit local opt-out must select plaintext: {provider:?}"
    );

    let configured_tls = TlsPathEnvironments {
        ca: Some("PLENORA_TEST_CA".to_owned()),
        client_certificate: None,
        client_key: None,
        mode: None,
    };
    let error = postgres_provider_for_probe_with_tls_policy(&configured_tls, true)
        .expect_err("insecure-local plus TLS material must fail closed");
    assert!(format!("{error:?}").contains(pfm::POSTGRES_INSECURE_LOCAL_ENV));
}

#[cfg(feature = "mysql")]
#[test]
fn provider_port_parser_rejects_invalid_boundaries() {
    for port in ["0", "-1", "65536", "not-a-port"] {
        let mut args = ["db.example.test", "warehouse", "loader", port]
            .into_iter()
            .map(str::to_owned);
        assert!(
            parse_provider_arguments(ProviderKind::Mysql, &mut args).is_err(),
            "invalid provider port accepted: {port}"
        );
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn provider_argument_parser_preserves_default_and_explicit_ports() {
    let mut default_args = ["db.example.test", "warehouse", "loader"]
        .into_iter()
        .map(str::to_owned);
    assert_eq!(
        parse_provider_arguments(ProviderKind::Mysql, &mut default_args)
            .expect("default MySQL port"),
        ProviderArguments::Mysql {
            host: "db.example.test".to_owned(),
            database: "warehouse".to_owned(),
            username: "loader".to_owned(),
            port: None,
            tls: TlsPathEnvironments::default(),
        }
    );

    let mut explicit_args = ["db.example.test", "warehouse", "loader", "65535"]
        .into_iter()
        .map(str::to_owned);
    assert_eq!(
        parse_provider_arguments(ProviderKind::Sqlserver, &mut explicit_args)
            .expect("explicit SQL Server port"),
        ProviderArguments::Sqlserver {
            host: "db.example.test".to_owned(),
            database: "warehouse".to_owned(),
            username: "loader".to_owned(),
            port: Some(65_535),
            tls: TlsPathEnvironments::default(),
        }
    );
}

#[cfg(feature = "mysql")]
#[test]
fn provider_argument_parser_rejects_partial_and_trailing_configuration() {
    for values in [
        vec!["host"],
        vec!["host", "database"],
        vec!["host", "database", "username", "3306", "trailing"],
    ] {
        let mut args = values.into_iter().map(str::to_owned);
        assert!(parse_provider_arguments(ProviderKind::Mysql, &mut args).is_err());
    }
}

#[cfg(all(feature = "mysql", feature = "sqlserver"))]
#[test]
fn structured_provider_factories_accept_an_explicit_nonzero_port() {
    let secret = SecretString::new("test-only-secret");
    for (kind, port) in [
        (ProviderKind::Mysql, "3307"),
        (ProviderKind::Sqlserver, "1434"),
    ] {
        let mut args = ["db.example.test", "warehouse", "loader", port]
            .into_iter()
            .map(str::to_owned);
        assert_eq!(
            build_provider(kind, &secret, &mut args)
                .expect("provider with explicit port")
                .kind(),
            kind
        );
    }
}
