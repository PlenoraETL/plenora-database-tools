//! Runtime Vectors 1.0: i vettori `database-*` della revisione adottata di
//! `plenora-contracts`, eseguiti attraverso il binding runtime pubblico.
//!
//! Le copie stanno in `contracts/upstream` con lo SHA-256 in `source.json`;
//! il job `public-contract` le confronta con il checkout fissato. I payload
//! dei vettori sono illustrativi (RUNTIME-VECTORS-1.0, sezione 5): il
//! componente li valida con i propri schemi immutabili. Questi test provano
//! ammissione, instradamento, invocazione e mappatura di risultati ed errori,
//! con un provider scriptato che non apre connessioni.
//!
//! Con `PLENORA_RUNTIME_EVIDENCE=<directory>` i risultati prodotti vengono
//! scritti come documenti a forma di vettore: `scripts/check_runtime_evidence.py`
//! li valida con gli schemi del pin (vettori, errori, capability, bundle del
//! componente).

use arrow_ipc::reader::StreamReader;
use plenora_database_core::arrow::array::Int64Array;
use plenora_database_core::arrow::{DataType, Field, RecordBatch, Schema, SchemaRef};
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, ParameterBag, PreparedWrite, Provider, ProviderFuture,
    SecretString,
};
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use plenora_database_engine::runtime_binding::{
    entrypoint, runtime_capabilities, ArrowSink, ArtifactResolver, ConnectionResolver,
    RuntimeBinding, RuntimeBody, RuntimeConnection, RuntimeInvocation, RuntimeRequestMetadata,
    RuntimeResult, RuntimeTarget, ARROW_STREAM_CONTENT_TYPE, CAPABILITY_NAME, DISCOVERY_ENTRYPOINT,
    ERROR_CONTENT_TYPE, ERROR_CONTRACT, JSON_CONTENT_TYPE,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const READ_REFERENCE: &str = "artifact://plans/database-read-vector";
const QUERY_REFERENCE: &str = "artifact://plans/database-query-vector";
const WRITE_REFERENCE: &str = "artifact://plans/database-write-vector";
const SECRET_REFERENCE: &str = "DATABASE_EXAMPLE";

fn contracts_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../contracts")
}

fn read_json(path: &PathBuf) -> Value {
    let bytes = fs::read(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|error| panic!("{}: {error}", path.display()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut text, byte| {
            use std::fmt::Write as _;
            write!(text, "{byte:02x}").expect("scrittura su String");
            text
        })
}

/// Un file copiato dal pin, verificato contro revisione e SHA-256.
fn upstream(name: &str) -> Value {
    let source = read_json(&contracts_root().join("upstream/source.json"));
    let adoption = read_json(&contracts_root().join("adoption-source.json"));
    assert_eq!(
        source["revision"], adoption["contracts_source"]["revision"],
        "copie upstream da una revisione diversa dal pin di adozione"
    );
    let path = contracts_root().join("upstream").join(name);
    let bytes = fs::read(&path).unwrap_or_else(|error| panic!("{name}: {error}"));
    assert_eq!(
        sha256_hex(&bytes),
        source["files"][name]["sha256"]
            .as_str()
            .expect("sha256 dichiarato"),
        "{name}: copia diversa dal file fissato"
    );
    serde_json::from_slice(&bytes).expect("JSON")
}

// ---------------------------------------------------------------- provider

/// Le chiamate ai resolver e al provider, in ordine.
#[derive(Default)]
struct Calls(Mutex<Vec<String>>);

impl Calls {
    fn record(&self, call: impl Into<String>) {
        self.0.lock().expect("lock").push(call.into());
    }

    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().expect("lock"))
    }
}

fn id_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

fn id_batch() -> RecordBatch {
    RecordBatch::try_new(id_schema(), vec![Arc::new(Int64Array::from(vec![1_i64]))]).expect("batch")
}

/// Uno stream con i batch dati, che rispetta la cancellazione.
struct Batches {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl BatchStream for Batches {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(DatabaseError::interrupted(
                    cancellation,
                    None,
                    ErrorPhase::Read,
                    "stream annullato",
                ));
            }
            Ok((!self.batches.is_empty()).then(|| self.batches.remove(0)))
        })
    }
}

#[derive(Clone, Copy)]
enum WriteScript {
    Committed,
    CommitOutcomeUnknown,
}

/// Provider `postgres` scriptato con gli esiti dei vettori.
struct Scripted {
    calls: Arc<Calls>,
    write: WriteScript,
}

impl Provider for Scripted {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn test_connection<'a>(
        &'a self,
        _: &'a SecretString,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        self.calls.record("provider.test_connection");
        Box::pin(async {
            Ok(ConnectionInfo {
                provider: ProviderKind::Postgres,
                server_version: "17.5".to_owned(),
                connection_identity: None,
            })
        })
    }

    fn probe_capabilities<'a>(
        &'a self,
        _: &'a SecretString,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        self.calls.record("provider.probe_capabilities");
        Box::pin(async {
            let document =
                read_json(&contracts_root().join("v2/examples/capabilities-postgres.json"));
            Ok(serde_json::from_value(document).expect("capability fixture"))
        })
    }

    fn inspect<'a>(
        &'a self,
        _: &'a SecretString,
        operation: &'a Operation,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        self.calls.record("provider.inspect");
        Box::pin(async move {
            let (id, document) = match operation {
                Operation::DatabaseListCatalogs => {
                    ("database.list_catalogs", json!({"catalogs": ["example"]}))
                }
                Operation::DatabaseListSchemas { .. } => {
                    ("database.list_schemas", json!({"schemas": ["public"]}))
                }
                Operation::DatabaseListObjects { .. } => (
                    "database.list_objects",
                    json!({"objects": [{"name": "example"}]}),
                ),
                Operation::DatabaseDescribeObject { .. } => (
                    "database.describe_object",
                    json!({"columns": [{"name": "id"}]}),
                ),
                _ => panic!("ispezione inattesa"),
            };
            Ok(Inspection {
                operation: id.to_owned(),
                document,
            })
        })
    }

    fn read<'a>(
        &'a self,
        _: &'a SecretString,
        operation: &'a ReadOperation,
        _: &'a ParameterBag,
        budget: &'a ResourceBudget,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        self.calls.record("provider.read");
        Box::pin(async move {
            // Il documento risolto e quello che il vettore descrive.
            assert_eq!(operation.source.schema.as_deref(), Some("public"));
            assert_eq!(operation.source.object, "example");
            assert_eq!(operation.projection, ["id", "geometry"]);
            assert_eq!(operation.row_limit, Some(100));
            // Il budget della lettura e quello della CLI, ristretto alla
            // deadline del vettore.
            assert_eq!(budget.remaining(ResourceKind::Rows), 10_000_000);
            Ok(Box::new(Batches {
                schema: id_schema(),
                batches: vec![id_batch()],
            }) as Box<dyn BatchStream>)
        })
    }

    fn query<'a>(
        &'a self,
        _: &'a SecretString,
        _: &'a QueryOperation,
        _: &'a ParameterBag,
        _: &'a ResourceBudget,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        self.calls.record("provider.query");
        Box::pin(async {
            Ok(Box::new(Batches {
                schema: id_schema(),
                batches: vec![id_batch()],
            }) as Box<dyn BatchStream>)
        })
    }

    fn prepare_write<'a>(
        &'a self,
        _: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        self.calls.record("provider.prepare_write");
        Box::pin(async move {
            Ok(PreparedWrite::new(
                operation.clone(),
                input_schema,
                serde_json::from_value(
                    json!({"schema_version": 1, "policy": "strict", "losses": []}),
                )
                .expect("loss report"),
                budget.clone(),
                budget.try_lease(ResourceKind::ConcurrentOperations, 1)?,
                budget.try_lease(ResourceKind::Columns, 1)?,
            ))
        })
    }

    fn write<'a>(
        &'a self,
        _: &'a SecretString,
        _: PreparedWrite,
        _: Box<dyn BatchStream>,
        _: &'a ResourceBudget,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        self.calls.record("provider.write");
        let script = self.write;
        Box::pin(async move {
            match script {
                WriteScript::Committed => Ok(serde_json::from_value(json!({
                    "schema_version": 2,
                    "status": "committed",
                    "execution_id": "vector-execution-1",
                    "provider": "postgres",
                    "rows": {"received": 1, "confirmed": 1, "inserted": 1, "updated": null,
                             "deleted": null, "failed": 0, "skipped": 0}
                }))
                .expect("outcome")),
                // L'esito che il vettore `database-write-error` descrive.
                WriteScript::CommitOutcomeUnknown => Err(DatabaseError {
                    category: ErrorCategory::Internal,
                    phase: ErrorPhase::Commit,
                    remote_effect: RemoteEffect::Unknown,
                    retry: RetryDisposition::RequiresRecovery,
                    provider: Some(ProviderKind::Postgres),
                    execution_id: Some("vector-execution-1".to_owned()),
                    message: "Commit outcome is unknown; reconcile by execution identity."
                        .to_owned(),
                    diagnostics: None,
                }),
            }
        })
    }
}

// --------------------------------------------------------------- resolver

struct Host {
    calls: Arc<Calls>,
    documents: BTreeMap<String, Vec<u8>>,
    sink: Arc<Mutex<Vec<u8>>>,
    write: WriteScript,
}

impl Host {
    fn new(write: WriteScript) -> Self {
        let read = json!({
            "source": {"schema": "public", "object": "example"},
            "projection": ["id", "geometry"],
            "order_by": [],
            "row_limit": 100
        });
        let query = json!({
            "source": {"object": {"schema": "public", "object": "example"}, "alias": null},
            "projection": [{"expression": {"kind": "column", "column": {"relation": null, "field": "id"}}, "alias": null}],
            "filter": null,
            "having": null,
            "row_limit": null
        });
        let write_plan = json!({
            "target": {"schema": "public", "object": "example"},
            "mode": "append",
            "mapping_policy": "strict",
            "transaction_profile": "single_transaction"
        });
        Self {
            calls: Arc::new(Calls::default()),
            documents: [
                (READ_REFERENCE, read),
                (QUERY_REFERENCE, query),
                (WRITE_REFERENCE, write_plan),
            ]
            .into_iter()
            .map(|(reference, document)| {
                (
                    reference.to_owned(),
                    serde_json::to_vec(&document).expect("json"),
                )
            })
            .collect(),
            sink: Arc::new(Mutex::new(Vec::new())),
            write,
        }
    }
}

impl ConnectionResolver for Host {
    fn open<'a>(&'a self, target: &'a RuntimeTarget) -> ProviderFuture<'a, RuntimeConnection> {
        self.calls.record("host.open");
        Box::pin(async move {
            assert_eq!(target.provider, ProviderKind::Postgres);
            assert_eq!(target.secret_environment, SECRET_REFERENCE);
            Ok(RuntimeConnection {
                provider: Arc::new(Scripted {
                    calls: Arc::clone(&self.calls),
                    write: self.write,
                }),
                secret: SecretString::new("resolved by the host"),
            })
        })
    }
}

struct MemorySink(Arc<Mutex<Vec<u8>>>);

impl Write for MemorySink {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl ArrowSink for MemorySink {
    fn finish(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    fn abort(self: Box<Self>) {
        self.0.lock().expect("lock").clear();
    }
}

impl ArtifactResolver for Host {
    fn read_document<'a>(&'a self, reference: &'a str, _: usize) -> ProviderFuture<'a, Vec<u8>> {
        self.calls.record(format!("host.read_document {reference}"));
        Box::pin(async move {
            self.documents.get(reference).cloned().ok_or_else(|| {
                DatabaseError::new(
                    ErrorCategory::NotFound,
                    ErrorPhase::Prepare,
                    None,
                    reference,
                )
            })
        })
    }

    fn open_write_input<'a>(
        &'a self,
        _: &'a RuntimeRequestMetadata,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        self.calls.record("host.open_write_input");
        Box::pin(async {
            Ok(Box::new(Batches {
                schema: id_schema(),
                batches: vec![id_batch()],
            }) as Box<dyn BatchStream>)
        })
    }

    fn open_read_sink<'a>(
        &'a self,
        _: &'a RuntimeRequestMetadata,
    ) -> ProviderFuture<'a, Box<dyn ArrowSink>> {
        self.calls.record("host.open_read_sink");
        Box::pin(
            async move { Ok(Box::new(MemorySink(Arc::clone(&self.sink))) as Box<dyn ArrowSink>) },
        )
    }
}

// ---------------------------------------------------------------- helpers

fn request_metadata(
    operation: &str,
    input_contract: &str,
    message: &str,
    correlation: &str,
) -> Value {
    json!({
        "plenora.message.id": message,
        "plenora.capability.name": CAPABILITY_NAME,
        "plenora.capability.version": "1",
        "plenora.capability.operation": operation,
        "plenora.operation.version": "1",
        "plenora.input.contract": input_contract,
        "plenora.trace.correlation_id": correlation,
    })
}

fn invocation(content_type: &Value, metadata: &Value, payload: &Value) -> RuntimeInvocation {
    let bytes = serde_json::to_vec(&json!({
        "content_type": content_type,
        "metadata": metadata,
        "payload": payload,
    }))
    .expect("json");
    RuntimeInvocation::from_json(&bytes).expect("invocazione runtime")
}

fn target_payload() -> Value {
    json!({"provider": "postgres", "secret_environment": SECRET_REFERENCE})
}

fn with(mut base: Value, extra: &Value) -> Value {
    for (key, value) in extra.as_object().expect("oggetto") {
        base[key] = value.clone();
    }
    base
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
}

fn invoke(host: &Host, invocation: &RuntimeInvocation) -> RuntimeResult {
    runtime()
        .block_on(RuntimeBinding::new(host, host).invoke(invocation, &CancellationToken::new()))
}

fn json_body(result: &RuntimeResult) -> &Value {
    match &result.body {
        RuntimeBody::Json(value) => value,
        RuntimeBody::ArrowStream(_) => panic!("corpo Arrow inatteso"),
    }
}

/// Il risultato nella forma di un vettore `plenora-runtime-vector-v1`, per
/// la validazione con gli schemi del pin.
fn as_vector(result: &RuntimeResult, payload: &Value) -> Value {
    json!({
        "schema_version": 1,
        "contract": "plenora-runtime-vector-v1",
        "kind": if result.is_error() { "error" } else { "success" },
        "content_type": result.content_type,
        "metadata": result.metadata,
        "payload": payload,
    })
}

fn evidence(name: &str, document: &Value) {
    if let Some(directory) = std::env::var_os("PLENORA_RUNTIME_EVIDENCE") {
        let directory = PathBuf::from(directory);
        fs::create_dir_all(&directory).expect("directory delle evidenze");
        fs::write(
            directory.join(name),
            serde_json::to_vec_pretty(document).expect("json"),
        )
        .expect("evidenza");
    }
}

// ------------------------------------------------------------------ tests

#[test]
fn runtime_discovery_matches_the_pinned_catalog_and_bindings() {
    let catalog = upstream("catalogs/database-tools-v1.json");
    let bindings = upstream("bindings/runtime-v1.json");
    let document = runtime_capabilities();
    evidence(
        "capabilities.json",
        &serde_json::to_value(&document).expect("json"),
    );

    let selected = catalog["operations"]
        .as_array()
        .expect("operazioni")
        .iter()
        .filter(|operation| {
            operation["surfaces"]
                .as_array()
                .expect("surfaces")
                .contains(&json!("runtime"))
        })
        .collect::<Vec<_>>();
    assert_eq!(document.operations.len(), selected.len());
    for (advertised, expected) in document.operations.iter().zip(&selected) {
        let advertised = serde_json::to_value(advertised).expect("json");
        assert_eq!(advertised["id"], expected["id"]);
        for field in ["version", "side_effect", "controls"] {
            assert_eq!(
                advertised[field], expected[field],
                "{}: {field}",
                expected["id"]
            );
        }
        for field in ["input", "output"] {
            assert_eq!(advertised[field]["contract"], expected[field]["contract"]);
            assert_eq!(
                advertised[field]["content_types"],
                expected[field]["content_types"]
            );
        }
        assert_eq!(advertised["surfaces"], json!(["runtime"]));
    }

    let component = bindings["components"]
        .as_array()
        .expect("components")
        .iter()
        .find(|item| item["component"] == "plenora-database-tools")
        .expect("binding database");
    assert_eq!(component["artifact"], CAPABILITY_NAME);
    assert_eq!(component["discovery"], json!([DISCOVERY_ENTRYPOINT]));
    let bound = component["bindings"].as_array().expect("bindings");
    assert_eq!(bound.len(), document.operations.len());
    for (binding, operation) in bound.iter().zip(&document.operations) {
        assert_eq!(binding["operation"], operation.id.as_str());
        assert_eq!(binding["version"], operation.version);
        assert_eq!(
            binding["entrypoints"],
            json!([entrypoint(&operation.id, operation.version)])
        );
    }
}

/// `database-read-request`: l'instradamento del vettore e ammesso, il suo
/// payload illustrativo e rifiutato dallo schema del componente prima di
/// qualunque resolver; con un payload conforme, la stessa richiesta legge.
#[test]
fn database_read_request_vector_is_admitted_routed_and_executed() {
    let vector = upstream("runtime-v1/database-read-request.json");
    assert_eq!(vector["kind"], "request");

    let host = Host::new(WriteScript::Committed);
    let illustrative = invoke(
        &host,
        &invocation(
            &vector["content_type"],
            &vector["metadata"],
            &vector["payload"],
        ),
    );
    assert!(illustrative.is_error());
    let error = json_body(&illustrative);
    assert_eq!(error["category"], "invalid_plan");
    assert_eq!(error["phase"], "validate");
    assert_eq!(error["remote_effect"], "none");
    assert_eq!(illustrative.metadata.operation, "database.read");
    assert_eq!(
        illustrative.metadata.correlation_id,
        vector["metadata"]["plenora.trace.correlation_id"]
    );
    assert!(
        host.calls.take().is_empty(),
        "nessun resolver prima della validazione"
    );

    let conforming = with(target_payload(), &json!({"operation_path": READ_REFERENCE}));
    let result = invoke(
        &host,
        &invocation(&vector["content_type"], &vector["metadata"], &conforming),
    );
    assert!(!result.is_error(), "{:?}", result.body);
    assert_eq!(result.content_type, ARROW_STREAM_CONTENT_TYPE);
    assert_eq!(result.metadata.operation, "database.read");
    assert_eq!(result.metadata.operation_version, "1");
    assert_eq!(
        result.metadata.output_contract,
        "plenora-database-read-result-v1"
    );
    assert_eq!(
        result.metadata.correlation_id,
        vector["metadata"]["plenora.trace.correlation_id"]
    );
    assert_eq!(
        result.metadata.causation_id.as_deref(),
        vector["metadata"]["plenora.message.id"].as_str()
    );
    assert_ne!(
        result.metadata.message_id,
        result.metadata.causation_id.clone().unwrap_or_default()
    );
    let RuntimeBody::ArrowStream(delivered) = &result.body else {
        panic!("risultato di read senza stream Arrow");
    };
    let bytes = host.sink.lock().expect("lock").clone();
    assert_eq!(
        delivered.byte_count,
        u64::try_from(bytes.len()).expect("len")
    );
    assert_eq!(delivered.checksum, sha256_hex(&bytes));
    let batches = StreamReader::try_new(bytes.as_slice(), None)
        .expect("stream Arrow IPC")
        .collect::<std::result::Result<Vec<_>, _>>()
        .expect("batch");
    assert_eq!(batches, [id_batch()]);
    assert_eq!(
        host.calls.take(),
        [
            format!("host.read_document {READ_REFERENCE}"),
            "host.open".to_owned(),
            "provider.read".to_owned(),
            "host.open_read_sink".to_owned(),
        ]
    );
    evidence(
        "database-read-success.json",
        &as_vector(
            &result,
            &json!({"fixture": "arrow-stream-bytes-are-supplied-by-the-adopting-test"}),
        ),
    );
}

/// Le mutazioni negative richieste da RUNTIME-VECTORS-1.0 per ogni vettore di
/// richiesta: capability, operazione, versione e input contract mancanti o
/// invalidi si rifiutano prima di qualunque resolver, senza effetto remoto.
#[test]
fn routing_mutations_of_the_read_request_fail_closed_before_invocation() {
    let vector = upstream("runtime-v1/database-read-request.json");
    let payload = with(target_payload(), &json!({"operation_path": READ_REFERENCE}));
    let keys = [
        ("plenora.capability.name", json!("plenora.storage-tools")),
        ("plenora.capability.version", json!("2")),
        ("plenora.capability.version", json!("01")),
        ("plenora.capability.operation", json!("database.execute")),
        ("plenora.capability.operation", json!("database.unknown")),
        ("plenora.operation.version", json!("2")),
        ("plenora.operation.version", json!("+1")),
        (
            "plenora.input.contract",
            json!("plenora-database-query-input-v1"),
        ),
        ("plenora.execution.deadline", json!("2001-01-01T00:00:00Z")),
        (
            "plenora.execution.deadline",
            json!("2030-01-01T01:00:00+01:00"),
        ),
        ("plenora.execution.idempotency_key", json!("key-1")),
        (
            "plenora.message.id",
            json!("018F3D84-7B2C-7F00-8000-000000000101"),
        ),
        ("plenora.trace.correlation_id", json!("not-a-uuid")),
    ];
    let host = Host::new(WriteScript::Committed);
    for (key, value) in keys {
        let mut metadata = vector["metadata"].clone();
        metadata[key] = value.clone();
        let result = invoke(
            &host,
            &invocation(&vector["content_type"], &metadata, &payload),
        );
        let error = json_body(&result);
        assert!(result.is_error(), "{key}={value}");
        assert_eq!(result.metadata.output_contract, ERROR_CONTRACT);
        assert!(
            ["protocol", "unsupported", "timeout"]
                .contains(&error["category"].as_str().unwrap_or("")),
            "{key}={value}: {error}"
        );
        assert_eq!(error["remote_effect"], "none", "{key}={value}");
        assert_eq!(error["phase"], "validate", "{key}={value}");
        assert!(host.calls.take().is_empty(), "{key}={value}: invocato");
    }
    for key in [
        "plenora.capability.name",
        "plenora.capability.version",
        "plenora.capability.operation",
        "plenora.operation.version",
        "plenora.input.contract",
    ] {
        let mut metadata = vector["metadata"].clone();
        metadata.as_object_mut().expect("oggetto").remove(key);
        let bytes = serde_json::to_vec(&json!({
            "content_type": vector["content_type"], "metadata": metadata, "payload": payload,
        }))
        .expect("json");
        // Un metadato di routing mancante non e una richiesta: il DTO lo
        // rifiuta prima del binding, come errore di protocollo.
        let error = RuntimeInvocation::from_json(&bytes).expect_err(key);
        assert_eq!(error.category, ErrorCategory::Protocol);
        assert_eq!(error.remote_effect, RemoteEffect::None);
    }
    for content_type in [
        json!("application/vnd.apache.arrow.stream"),
        json!("text/plain"),
    ] {
        let result = invoke(
            &host,
            &invocation(&content_type, &vector["metadata"], &payload),
        );
        assert_eq!(json_body(&result)["category"], "protocol");
        assert!(host.calls.take().is_empty());
    }
}

/// `database-query-success`: stessa operazione, versione, output contract,
/// content type e correlazione del vettore; il payload e il risultato
/// `plenora-database-query-result-v1` del componente (riepilogo: righe e
/// campi), che descrive gli stessi dati del payload illustrativo.
#[test]
fn database_query_success_vector_is_reproduced() {
    let vector = upstream("runtime-v1/database-query-success.json");
    assert_eq!(vector["kind"], "success");
    let metadata = request_metadata(
        "database.query",
        "plenora-database-query-input-v1",
        "018f3d84-7b2c-7f00-8000-000000000207",
        vector["metadata"]["plenora.trace.correlation_id"]
            .as_str()
            .expect("correlation"),
    );
    let payload = with(
        target_payload(),
        &json!({"operation_path": QUERY_REFERENCE}),
    );
    let host = Host::new(WriteScript::Committed);
    let result = invoke(
        &host,
        &invocation(&json!(JSON_CONTENT_TYPE), &metadata, &payload),
    );
    assert!(!result.is_error(), "{:?}", result.body);
    assert_eq!(result.content_type, vector["content_type"]);
    for (key, actual) in [
        ("plenora.capability.operation", &result.metadata.operation),
        (
            "plenora.operation.version",
            &result.metadata.operation_version,
        ),
        ("plenora.output.contract", &result.metadata.output_contract),
        (
            "plenora.trace.correlation_id",
            &result.metadata.correlation_id,
        ),
    ] {
        assert_eq!(vector["metadata"][key], actual.as_str(), "{key}");
    }
    let summary = json_body(&result);
    let expected_columns = vector["payload"]["columns"].as_array().expect("columns");
    let fields = summary["fields"].as_array().expect("fields");
    assert_eq!(fields.len(), expected_columns.len());
    for (field, column) in fields.iter().zip(expected_columns) {
        assert_eq!(field["name"], column["name"]);
        assert_eq!(column["type"], "int64");
        assert_eq!(field["data_type"], "Int64");
    }
    assert_eq!(
        summary["rows"],
        json!(vector["payload"]["rows"].as_array().expect("rows").len())
    );
    evidence("database-query-success.json", &as_vector(&result, summary));
}

/// `database-write-error`: un commit dall'esito ignoto produce l'envelope
/// d'errore del vettore, con tutti gli assi comuni e la correlazione.
#[test]
fn database_write_error_vector_is_reproduced() {
    let vector = upstream("runtime-v1/database-write-error.json");
    assert_eq!(vector["kind"], "error");
    let metadata = request_metadata(
        "database.write",
        "plenora-database-write-input-v1",
        "018f3d84-7b2c-7f00-8000-000000000203",
        vector["metadata"]["plenora.trace.correlation_id"]
            .as_str()
            .expect("correlation"),
    );
    let payload = with(
        target_payload(),
        &json!({"operation_path": WRITE_REFERENCE}),
    );
    let host = Host::new(WriteScript::CommitOutcomeUnknown);
    let result = invoke(
        &host,
        &invocation(&json!(JSON_CONTENT_TYPE), &metadata, &payload),
    );
    assert!(result.is_error());
    assert_eq!(result.content_type, vector["content_type"]);
    assert_eq!(result.content_type, ERROR_CONTENT_TYPE);
    for (key, actual) in [
        ("plenora.capability.operation", &result.metadata.operation),
        (
            "plenora.operation.version",
            &result.metadata.operation_version,
        ),
        ("plenora.output.contract", &result.metadata.output_contract),
        (
            "plenora.trace.correlation_id",
            &result.metadata.correlation_id,
        ),
    ] {
        assert_eq!(vector["metadata"][key], actual.as_str(), "{key}");
    }
    let error = json_body(&result);
    for axis in [
        "category",
        "phase",
        "remote_effect",
        "retry",
        "provider",
        "execution_id",
    ] {
        assert_eq!(error[axis], vector["payload"][axis], "{axis}");
    }
    assert_eq!(
        host.calls.take(),
        [
            format!("host.read_document {WRITE_REFERENCE}"),
            "host.open".to_owned(),
            "host.open_write_input".to_owned(),
            "provider.prepare_write".to_owned(),
            "provider.write".to_owned(),
        ]
    );
    evidence("database-write-error.json", &as_vector(&result, error));
}

/// Le altre operazioni legate al runtime: ognuna restituisce il proprio
/// risultato pubblico, mai un ack vuoto (RT-008).
#[test]
fn every_bound_operation_returns_its_public_result() {
    let host = Host::new(WriteScript::Committed);
    let cases = [
        (
            "database.test_connection",
            "plenora-database-connection-test-input-v1",
            target_payload(),
        ),
        (
            "database.list_catalogs",
            "plenora-database-list-catalogs-input-v1",
            target_payload(),
        ),
        (
            "database.list_schemas",
            "plenora-database-list-schemas-input-v1",
            with(target_payload(), &json!({"catalog": null})),
        ),
        (
            "database.list_objects",
            "plenora-database-list-objects-input-v1",
            with(target_payload(), &json!({"schema": "public"})),
        ),
        (
            "database.describe_object",
            "plenora-database-describe-object-input-v1",
            with(
                target_payload(),
                &json!({"schema": "public", "object": "example"}),
            ),
        ),
        (
            "database.write",
            "plenora-database-write-input-v1",
            with(
                target_payload(),
                &json!({"operation_path": WRITE_REFERENCE}),
            ),
        ),
    ];
    for (index, (operation, contract, payload)) in cases.into_iter().enumerate() {
        let metadata = request_metadata(
            operation,
            contract,
            &format!("018f3d84-7b2c-7f00-8000-0000000003{index:02}"),
            "018f3d84-7b2c-7f00-8000-000000000300",
        );
        let result = invoke(
            &host,
            &invocation(&json!(JSON_CONTENT_TYPE), &metadata, &payload),
        );
        assert!(!result.is_error(), "{operation}: {:?}", result.body);
        assert_eq!(result.content_type, JSON_CONTENT_TYPE);
        assert_eq!(result.metadata.operation, operation);
        let body = json_body(&result);
        assert!(
            body.as_object().is_some_and(|object| !object.is_empty()),
            "{operation}"
        );
        evidence(&format!("{operation}.json"), &as_vector(&result, body));
        host.calls.take();
    }
}

/// RT-013: un percorso locale al posto di un riferimento opaco si rifiuta
/// prima di chiedere all'applicazione di risolverlo.
#[test]
fn local_paths_do_not_cross_the_runtime_boundary() {
    let host = Host::new(WriteScript::Committed);
    let metadata = request_metadata(
        "database.read",
        "plenora-database-read-input-v1",
        "018f3d84-7b2c-7f00-8000-000000000401",
        "018f3d84-7b2c-7f00-8000-000000000400",
    );
    for path in [
        "/etc/plenora/read.json",
        "C:\\plans\\read.json",
        "plans/read.json",
        "file:///etc/plenora/read.json",
        "artifact://plans/../secrets",
        "artifact://",
    ] {
        let payload = with(target_payload(), &json!({"operation_path": path}));
        let result = invoke(
            &host,
            &invocation(&json!(JSON_CONTENT_TYPE), &metadata, &payload),
        );
        assert_eq!(json_body(&result)["category"], "invalid_plan", "{path}");
        let message = json_body(&result)["message"].as_str().unwrap_or("");
        assert!(
            !message.contains(path),
            "{path}: percorso riflesso nel messaggio"
        );
        assert!(host.calls.take().is_empty(), "{path}: risolto");
    }
}

/// Il testo di un errore dell'applicazione non attraversa il binding: un
/// riferimento sconosciuto produce `not_found` senza nominarlo.
#[test]
fn host_error_text_does_not_reach_the_result() {
    let host = Host::new(WriteScript::Committed);
    let metadata = request_metadata(
        "database.read",
        "plenora-database-read-input-v1",
        "018f3d84-7b2c-7f00-8000-000000000501",
        "018f3d84-7b2c-7f00-8000-000000000500",
    );
    let payload = with(
        target_payload(),
        &json!({"operation_path": "artifact://plans/missing-canary"}),
    );
    let result = invoke(
        &host,
        &invocation(&json!(JSON_CONTENT_TYPE), &metadata, &payload),
    );
    let error = json_body(&result);
    assert_eq!(error["category"], "not_found");
    assert!(!error.to_string().contains("missing-canary"));
}

/// Una chiave ripetuta nell'invocazione e un errore, non l'ultima occorrenza.
#[test]
fn a_repeated_key_in_the_invocation_is_rejected() {
    let text = r#"{"content_type":"application/json","content_type":"application/json","metadata":{},"payload":{}}"#;
    assert_eq!(
        RuntimeInvocation::from_json(text.as_bytes())
            .unwrap_err()
            .category,
        ErrorCategory::Protocol
    );
}
