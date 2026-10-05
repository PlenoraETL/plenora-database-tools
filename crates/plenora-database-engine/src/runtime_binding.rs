//! Runtime Binding 1.0: il confine transport-neutral di `plenora.database-tools`.
//!
//! Un'applicazione finale avvolge [`RuntimeBinding`] nel proprio handler di
//! capability (per esempio il `CapabilityHandler` di runtime-tools); questo
//! modulo non dipende da runtime-tools (RT-015). Trasporto, autorizzazione,
//! risoluzione dei segreti e degli artefatti restano all'applicazione, dietro
//! [`ConnectionResolver`] e [`ArtifactResolver`].
//!
//! Le operazioni sono quelle che il catalogo del pin lega alla superficie
//! `runtime` (`bindings/runtime-v1.json`): `database.test_connection`, le
//! quattro ispezioni, `database.read`, `database.write` e `database.query`.
//! Il routing si confronta con il documento di Capability Discovery che lo
//! stesso binding pubblica ([`runtime_capabilities`]), cosi una richiesta non
//! puo raggiungere un'operazione che il documento non dichiara (RT-004).
//!
//! Gli input sono i documenti `plenora-database-*-input-v1` della CLI, letti
//! dagli stessi tipi ([`crate::public_ops`]). Sul runtime `operation_path` e
//! `parameters_path` sono riferimenti opachi ad artefatti (RT-013), risolti
//! dal [`ArtifactResolver`]; un percorso locale si rifiuta prima di
//! risolverlo.

use crate::public_ops::{
    self, DescribeObjectRequest, ListObjectsRequest, ListSchemasRequest, OperationRequest,
    TargetRequest, ValidatedTarget, WriteRequest,
};
use crate::Engine;
use arrow_ipc::writer::StreamWriter;
use plenora_database_core::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::public_contract::{
    public_capabilities, PublicCapabilities, PublicOperation, PublicSurface, RUNTIME_CAPABILITY,
};
use plenora_database_core::relational::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::{
    strict_json, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

/// Nome della capability runtime (RT-001).
pub const CAPABILITY_NAME: &str = RUNTIME_CAPABILITY;
/// Versione del binding runtime (RT-002), indipendente dal rilascio.
pub const RUNTIME_BINDING_VERSION: u32 = 1;
/// Entrypoint di discovery dichiarato in `bindings/runtime-v1.json`.
pub const DISCOVERY_ENTRYPOINT: &str = "plenora.database-tools#capabilities@1";
/// Envelope JSON delle richieste e dei risultati strutturati.
pub const JSON_CONTENT_TYPE: &str = "application/json";
/// Risultato tabellare di `database.read`, consegnato al sink dell'host.
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";
/// Envelope d'errore (RT-010).
pub const ERROR_CONTENT_TYPE: &str = "application/vnd.plenora.error+json";
/// Contratto dell'envelope d'errore.
pub const ERROR_CONTRACT: &str = "plenora-error-v1";
/// Limite dei documenti risolti da un riferimento (operazione, parametri).
pub const MAX_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;

const MAX_DEADLINE_HORIZON: Duration = Duration::from_hours(365 * 24);
const NIL_UUID: &str = "00000000-0000-0000-0000-000000000000";
const UNKNOWN_OPERATION: &str = "database.unknown";

/// Il documento Capability Discovery 2.0 della superficie runtime.
#[must_use]
pub fn runtime_capabilities() -> PublicCapabilities {
    public_capabilities(PublicSurface::Runtime, CAPABILITY_NAME, None)
}

/// L'entrypoint di un'operazione, nella forma di `bindings/runtime-v1.json`.
#[must_use]
pub fn entrypoint(operation: &str, version: u32) -> String {
    format!("{CAPABILITY_NAME}#{operation}@{version}")
}

/// Richiesta runtime: content type, metadati riservati e payload JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeInvocation {
    pub content_type: String,
    pub metadata: RuntimeRequestMetadata,
    pub payload: Value,
}

impl RuntimeInvocation {
    /// Legge un'invocazione serializzata rifiutando le chiavi ripetute: un
    /// `Value` tiene l'ultima occorrenza, e due lettori dello stesso messaggio
    /// vedrebbero due richieste.
    ///
    /// # Errors
    ///
    /// `Protocol` per JSON non valido, chiavi ripetute, chiavi sconosciute o
    /// un metadato opzionale scritto `null`. Il messaggio porta riga e
    /// colonna, mai il testo.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        strict_json::from_slice(bytes).map_err(|error| {
            route_error(
                ErrorCategory::Protocol,
                format!(
                    "invocazione runtime non leggibile a riga {}, colonna {}",
                    error.line(),
                    error.column()
                ),
            )
        })
    }
}

/// Metadati riservati della richiesta (RT-003, RT-012, controlli).
///
/// I campi opzionali si omettono quando assenti: `null` non e una stringa
/// del trasporto e si rifiuta, invece di valere assente (una deadline `null`
/// farebbe partire l'operazione senza scadenza).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRequestMetadata {
    #[serde(rename = "plenora.message.id")]
    pub message_id: String,
    #[serde(
        rename = "plenora.message.causation_id",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub causation_id: Option<String>,
    #[serde(rename = "plenora.capability.name")]
    pub capability_name: String,
    #[serde(rename = "plenora.capability.version")]
    pub capability_version: String,
    #[serde(rename = "plenora.capability.operation")]
    pub operation: String,
    #[serde(rename = "plenora.operation.version")]
    pub operation_version: String,
    #[serde(rename = "plenora.input.contract")]
    pub input_contract: String,
    #[serde(
        rename = "plenora.execution.deadline",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub deadline: Option<String>,
    #[serde(
        rename = "plenora.execution.idempotency_key",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key: Option<String>,
    #[serde(rename = "plenora.trace.correlation_id")]
    pub correlation_id: String,
}

fn present<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    String::deserialize(deserializer).map(Some)
}

/// Metadati del risultato (RT-008, RT-010, RT-012).
///
/// `message_id` e un'identita nuova, derivata in modo deterministico da
/// quella della richiesta; `causation_id` e la richiesta stessa, causa
/// diretta del risultato; `correlation_id` e quello della richiesta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeResultMetadata {
    #[serde(rename = "plenora.message.id")]
    pub message_id: String,
    #[serde(
        rename = "plenora.message.causation_id",
        default,
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub causation_id: Option<String>,
    #[serde(rename = "plenora.capability.operation")]
    pub operation: String,
    #[serde(rename = "plenora.operation.version")]
    pub operation_version: String,
    #[serde(rename = "plenora.output.contract")]
    pub output_contract: String,
    #[serde(rename = "plenora.trace.correlation_id")]
    pub correlation_id: String,
}

/// Il corpo di un risultato.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeBody {
    /// Documento JSON: risultato strutturato o errore `plenora-error-v1`.
    Json(Value),
    /// Stream Arrow IPC gia consegnato al sink dell'host (RT-009, RT-014):
    /// qui ne restano conteggio dei byte e SHA-256 calcolati sui byte scritti.
    ArrowStream(DeliveredArtifact),
}

/// Byte e checksum di un artefatto consegnato al sink dell'host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeliveredArtifact {
    pub byte_count: u64,
    pub checksum_algorithm: &'static str,
    pub checksum: String,
}

/// Risultato terminale di un'invocazione: successo o errore, mai un ack vuoto.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeResult {
    pub content_type: String,
    pub metadata: RuntimeResultMetadata,
    pub body: RuntimeBody,
}

impl RuntimeResult {
    /// Se il risultato e l'envelope d'errore.
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.content_type == ERROR_CONTENT_TYPE
    }
}

/// Destinazione validata che il resolver dell'applicazione deve aprire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeTarget {
    pub provider: ProviderKind,
    /// Il riferimento protetto della richiesta: il resolver lo autorizza e
    /// lo risolve, il binding non legge mai l'ambiente.
    pub secret_environment: String,
    pub provider_arguments: Vec<String>,
}

/// Connessione aperta dal resolver: provider e segreto gia risolto.
pub struct RuntimeConnection {
    pub provider: Arc<dyn Provider>,
    pub secret: SecretString,
}

/// Apertura delle connessioni, posseduta dall'applicazione.
pub trait ConnectionResolver: Send + Sync {
    /// Autorizza il riferimento protetto e apre il provider richiesto.
    ///
    /// # Errors
    ///
    /// Un rifiuto o un fallimento dell'applicazione. Il binding ne conserva
    /// categoria, fase, effetto e retry, e ne sostituisce il testo: un
    /// messaggio dell'host puo nominare cio a cui il riferimento si e
    /// risolto.
    fn open<'a>(&'a self, target: &'a RuntimeTarget) -> ProviderFuture<'a, RuntimeConnection>;
}

/// Sink posseduto dall'host per il risultato Arrow di `database.read`.
pub trait ArrowSink: Write + Send {
    /// Chiamato una volta dopo l'ultimo byte; la pubblicazione e dell'host.
    ///
    /// # Errors
    ///
    /// Un fallimento della pubblicazione.
    fn finish(self: Box<Self>) -> Result<()>;
    /// Chiamato su ogni errore dopo l'apertura: l'host scarta i byte parziali.
    fn abort(self: Box<Self>);
}

/// Risoluzione degli artefatti, posseduta dall'applicazione (RT-013, RT-015).
pub trait ArtifactResolver: Send + Sync {
    /// I byte del documento indicato da un riferimento opaco (operazione o
    /// parametri), al piu `max_bytes`.
    ///
    /// # Errors
    ///
    /// Riferimento sconosciuto, non autorizzato o illeggibile.
    fn read_document<'a>(
        &'a self,
        reference: &'a str,
        max_bytes: usize,
    ) -> ProviderFuture<'a, Vec<u8>>;

    /// I dati Arrow legati a questa invocazione di `database.write`.
    ///
    /// Il documento `plenora-database-write-input-v1` non nomina i dati, come
    /// sulla CLI dove arrivano da `--data`: li consegna il trasporto
    /// dell'applicazione insieme al messaggio.
    ///
    /// # Errors
    ///
    /// Dati assenti, non autorizzati o non leggibili come Arrow.
    fn open_write_input<'a>(
        &'a self,
        request: &'a RuntimeRequestMetadata,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>>;

    /// Il sink dello stream Arrow prodotto da questa invocazione di
    /// `database.read`.
    ///
    /// # Errors
    ///
    /// Sink non autorizzato o non apribile.
    fn open_read_sink<'a>(
        &'a self,
        request: &'a RuntimeRequestMetadata,
    ) -> ProviderFuture<'a, Box<dyn ArrowSink>>;
}

/// Binding runtime sopra i resolver dell'applicazione.
pub struct RuntimeBinding<'a> {
    connections: &'a dyn ConnectionResolver,
    artifacts: &'a dyn ArtifactResolver,
}

struct Admitted {
    operation: PublicOperation,
    deadline: Option<Instant>,
}

impl<'a> RuntimeBinding<'a> {
    /// Lega i resolver senza aprire risorse.
    #[must_use]
    pub const fn new(
        connections: &'a dyn ConnectionResolver,
        artifacts: &'a dyn ArtifactResolver,
    ) -> Self {
        Self {
            connections,
            artifacts,
        }
    }

    /// L'entrypoint di discovery (`plenora.database-tools#capabilities@1`).
    #[must_use]
    pub fn capabilities(&self) -> PublicCapabilities {
        runtime_capabilities()
    }

    /// Valida, instrada ed esegue un'invocazione, restituendo un risultato
    /// correlato: il risultato dell'operazione oppure l'errore redatto.
    ///
    /// `cancellation` e la cancellazione del contesto d'invocazione
    /// dell'host.
    pub async fn invoke(
        &self,
        invocation: &RuntimeInvocation,
        cancellation: &CancellationToken,
    ) -> RuntimeResult {
        let request = &invocation.metadata;
        let advertised = runtime_capabilities()
            .operations
            .into_iter()
            .find(|operation| operation.id == request.operation);
        let identity = RuntimeResultMetadata {
            message_id: result_message_id(&request.message_id),
            causation_id: canonical_uuid(&request.message_id).then(|| request.message_id.clone()),
            operation: advertised
                .as_ref()
                .map_or(UNKNOWN_OPERATION, |operation| operation.id.as_str())
                .to_owned(),
            operation_version: parse_version(&request.operation_version)
                .map_or_else(|_| "0".to_owned(), |version| version.to_string()),
            output_contract: ERROR_CONTRACT.to_owned(),
            correlation_id: public_uuid(&request.correlation_id),
        };
        match self.invoke_admitted(invocation, cancellation).await {
            Ok((operation, content_type, body)) => RuntimeResult {
                content_type: content_type.to_owned(),
                metadata: RuntimeResultMetadata {
                    output_contract: operation.output.contract,
                    ..identity
                },
                body,
            },
            Err(error) => RuntimeResult {
                content_type: ERROR_CONTENT_TYPE.to_owned(),
                metadata: identity,
                body: RuntimeBody::Json(error_document(&error)),
            },
        }
    }

    async fn invoke_admitted(
        &self,
        invocation: &RuntimeInvocation,
        cancellation: &CancellationToken,
    ) -> Result<(PublicOperation, &'static str, RuntimeBody)> {
        let admitted = admit(invocation)?;
        if cancellation.is_cancelled() {
            return Err(DatabaseError::interrupted(
                cancellation,
                None,
                ErrorPhase::Validate,
                "invocazione annullata prima dell'esecuzione",
            ));
        }
        let token = cancellation.child_token_with_deadline(admitted.deadline);
        let (content_type, body) = match admitted.operation.id.as_str() {
            "database.test_connection" => {
                let target: TargetRequest = decode(&invocation.payload)?;
                let connection = self.connect(validated(target)?, admitted.deadline).await?;
                (
                    JSON_CONTENT_TYPE,
                    RuntimeBody::Json(connection.test_connection(&token).await?),
                )
            }
            "database.list_catalogs" => {
                let target: TargetRequest = decode(&invocation.payload)?;
                self.inspect(
                    target,
                    Operation::DatabaseListCatalogs,
                    admitted.deadline,
                    &token,
                )
                .await?
            }
            "database.list_schemas" => {
                let request: ListSchemasRequest = decode(&invocation.payload)?;
                let operation = request.operation();
                self.inspect(request.target(), operation, admitted.deadline, &token)
                    .await?
            }
            "database.list_objects" => {
                let request: ListObjectsRequest = decode(&invocation.payload)?;
                let operation = request.operation()?;
                self.inspect(request.target(), operation, admitted.deadline, &token)
                    .await?
            }
            "database.describe_object" => {
                let request: DescribeObjectRequest = decode(&invocation.payload)?;
                let operation = request.operation()?;
                self.inspect(request.target(), operation, admitted.deadline, &token)
                    .await?
            }
            "database.read" => self.read(invocation, admitted.deadline, &token).await?,
            "database.write" => self.write(invocation, admitted.deadline, &token).await?,
            "database.query" => self.query(invocation, admitted.deadline, &token).await?,
            _ => {
                return Err(route_error(
                    ErrorCategory::Unsupported,
                    "operazione runtime non supportata",
                ))
            }
        };
        Ok((admitted.operation, content_type, body))
    }

    async fn connect(
        &self,
        target: ValidatedTarget,
        deadline: Option<Instant>,
    ) -> Result<Connected> {
        let runtime_target = RuntimeTarget {
            provider: target.provider,
            secret_environment: target.secret_environment,
            provider_arguments: target.provider_arguments,
        };
        let connection = self
            .connections
            .open(&runtime_target)
            .await
            .map_err(|error| {
                redact_host_error(
                    &error,
                    "apertura della connessione rifiutata dall'applicazione",
                )
            })?;
        if connection.provider.kind() != runtime_target.provider {
            return Err(DatabaseError::new(
                ErrorCategory::InvalidConfiguration,
                ErrorPhase::Connect,
                Some(runtime_target.provider),
                "il resolver ha aperto un provider diverso da quello richiesto",
            ));
        }
        Ok(Connected {
            kind: runtime_target.provider,
            engine: Engine::new(Arc::clone(&connection.provider), connection.secret.clone()),
            provider: connection.provider,
            secret: connection.secret,
            deadline,
        })
    }

    async fn inspect(
        &self,
        target: TargetRequest,
        operation: Operation,
        deadline: Option<Instant>,
        cancellation: &CancellationToken,
    ) -> Result<(&'static str, RuntimeBody)> {
        let connected = self.connect(validated(target)?, deadline).await?;
        let session = connected.engine.session()?;
        let token = CancellationToken::linked(&[cancellation, &session.cancellation_token()]);
        let inspection = connected
            .provider
            .inspect(&connected.secret, &operation, &token)
            .await?;
        Ok((
            JSON_CONTENT_TYPE,
            RuntimeBody::Json(public_ops::inspection_document(connected.kind, inspection)?),
        ))
    }

    async fn documents<T: DeserializeOwned>(
        &self,
        request: &OperationRequest,
    ) -> Result<(T, ParameterBag)> {
        let operation_path =
            public_ops::required_value(request.operation_path.clone(), "operation_path")?;
        let operation: T = self.document(&operation_path, "operazione").await?;
        let parameters = match &request.parameters_path {
            Some(reference) => self.document(reference, "parametri").await?,
            None => ParameterBag::default(),
        };
        Ok((operation, parameters))
    }

    async fn document<T: DeserializeOwned>(&self, reference: &str, label: &str) -> Result<T> {
        ensure_artifact_reference(reference)?;
        let bytes = self
            .artifacts
            .read_document(reference, MAX_DOCUMENT_BYTES)
            .await
            .map_err(|error| {
                redact_host_error(
                    &error,
                    "risoluzione dell'artefatto rifiutata dall'applicazione",
                )
            })?;
        if bytes.len() > MAX_DOCUMENT_BYTES {
            return Err(DatabaseError::resource_limit(format!(
                "documento di {label} oltre il limite del binding runtime"
            )));
        }
        strict_json::from_slice(&bytes).map_err(|error| {
            DatabaseError::invalid_plan(format!(
                "documento di {label} non parsabile a riga {}, colonna {}",
                error.line(),
                error.column()
            ))
        })
    }

    async fn read(
        &self,
        invocation: &RuntimeInvocation,
        deadline: Option<Instant>,
        cancellation: &CancellationToken,
    ) -> Result<(&'static str, RuntimeBody)> {
        let request: OperationRequest = decode(&invocation.payload)?;
        let target = validated(request.target())?;
        let (operation, parameters): (ReadOperation, ParameterBag) =
            self.documents(&request).await?;
        let connected = self.connect(target, deadline).await?;
        let budget = connected.budget(public_ops::read_limits())?;
        let session = connected.engine.session()?;
        let token = CancellationToken::linked(&[cancellation, &session.cancellation_token()]);
        let mut stream = connected
            .provider
            .read(&connected.secret, &operation, &parameters, &budget, &token)
            .await?;
        let mut sink = self
            .artifacts
            .open_read_sink(&invocation.metadata)
            .await
            .map_err(|error| {
                redact_host_error(&error, "apertura del sink rifiutata dall'applicazione")
            })?;
        let delivered = match write_arrow_stream(&mut sink, stream.as_mut(), &token).await {
            Ok(delivered) => delivered,
            Err(error) => {
                sink.abort();
                return Err(error);
            }
        };
        sink.finish().map_err(|error| {
            redact_host_error(
                &error,
                "pubblicazione del risultato rifiutata dall'applicazione",
            )
        })?;
        Ok((
            ARROW_STREAM_CONTENT_TYPE,
            RuntimeBody::ArrowStream(delivered),
        ))
    }

    async fn write(
        &self,
        invocation: &RuntimeInvocation,
        deadline: Option<Instant>,
        cancellation: &CancellationToken,
    ) -> Result<(&'static str, RuntimeBody)> {
        let request: WriteRequest = decode(&invocation.payload)?;
        let target = validated(request.target())?;
        let operation_path = public_ops::required_value(request.operation_path, "operation_path")?;
        let operation: WriteOperation = self.document(&operation_path, "operazione").await?;
        let connected = self.connect(target, deadline).await?;
        let input = self
            .artifacts
            .open_write_input(&invocation.metadata)
            .await
            .map_err(|error| {
                redact_host_error(
                    &error,
                    "dati Arrow della scrittura rifiutati dall'applicazione",
                )
            })?;
        let budget = connected.budget(ResourceLimits::default())?;
        let session = connected.engine.session()?;
        let token = CancellationToken::linked(&[cancellation, &session.cancellation_token()]);
        let prepared = connected
            .provider
            .prepare_write(
                &connected.secret,
                &operation,
                input.schema(),
                &budget,
                &token,
            )
            .await?;
        let outcome = connected
            .provider
            .write(&connected.secret, prepared, input, &budget, &token)
            .await?;
        let document = serde_json::to_value(outcome)
            .map_err(|_| internal("esito della scrittura non serializzabile"))?;
        Ok((JSON_CONTENT_TYPE, RuntimeBody::Json(document)))
    }

    async fn query(
        &self,
        invocation: &RuntimeInvocation,
        deadline: Option<Instant>,
        cancellation: &CancellationToken,
    ) -> Result<(&'static str, RuntimeBody)> {
        let request: OperationRequest = decode(&invocation.payload)?;
        let target = validated(request.target())?;
        let (operation, parameters): (QueryOperation, ParameterBag) =
            self.documents(&request).await?;
        let connected = self.connect(target, deadline).await?;
        let budget = connected.budget(ResourceLimits::default())?;
        let session = connected.engine.session()?;
        let token = CancellationToken::linked(&[cancellation, &session.cancellation_token()]);
        let mut stream = connected
            .provider
            .query(&connected.secret, &operation, &parameters, &budget, &token)
            .await?;
        let summary = public_ops::summarize_stream(connected.kind, stream.as_mut(), &token).await?;
        Ok((JSON_CONTENT_TYPE, RuntimeBody::Json(summary)))
    }
}

struct Connected {
    kind: ProviderKind,
    engine: Engine,
    provider: Arc<dyn Provider>,
    secret: SecretString,
    deadline: Option<Instant>,
}

impl Connected {
    async fn test_connection(&self, cancellation: &CancellationToken) -> Result<Value> {
        self.engine.health_check(cancellation).await?;
        let provider_capabilities = self.engine.capabilities(false, cancellation).await?;
        let capabilities = public_capabilities(
            PublicSurface::Runtime,
            CAPABILITY_NAME,
            Some(&provider_capabilities),
        );
        Ok(json!({
            "verified": true,
            "provider": self.kind,
            "capabilities": capabilities,
        }))
    }

    /// Il budget della superficie, con la durata ristretta alla deadline
    /// della richiesta: i provider la applicano con lo stesso
    /// `DeadlineGuard` della CLI, quindi la scadenza diventa `timeout`.
    fn budget(&self, mut limits: ResourceLimits) -> Result<ResourceBudget> {
        if let Some(deadline) = self.deadline {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|remaining| !remaining.is_zero())
                .ok_or_else(deadline_expired)?;
            let remaining_ms = u64::try_from(remaining.as_millis())
                .unwrap_or(u64::MAX)
                .max(1);
            limits.duration_ms = limits.duration_ms.min(remaining_ms);
        }
        ResourceBudget::new(limits)
    }
}

/// Routing e controlli prima di qualunque resolver o provider (RT-004,
/// RT-005, RT-006, RT-011, RT-012). Ogni rifiuto ha `remote_effect: none`.
fn admit(invocation: &RuntimeInvocation) -> Result<Admitted> {
    let request = &invocation.metadata;
    if !canonical_uuid(&request.message_id)
        || !canonical_uuid(&request.correlation_id)
        || request
            .causation_id
            .as_deref()
            .is_some_and(|value| !canonical_uuid(value))
    {
        return Err(route_error(
            ErrorCategory::Protocol,
            "le identita runtime devono essere UUID canonici minuscoli con trattini",
        ));
    }
    if request.capability_name != CAPABILITY_NAME
        || parse_version(&request.capability_version)? != RUNTIME_BINDING_VERSION
    {
        return Err(route_error(
            ErrorCategory::Protocol,
            "identita della capability runtime non supportata",
        ));
    }
    let operation = runtime_capabilities()
        .operations
        .into_iter()
        .find(|operation| operation.id == request.operation)
        .ok_or_else(|| {
            route_error(
                ErrorCategory::Unsupported,
                "operazione runtime non supportata",
            )
        })?;
    if parse_version(&request.operation_version)? != operation.version {
        return Err(route_error(
            ErrorCategory::Unsupported,
            "versione dell'operazione runtime non supportata",
        ));
    }
    if request.input_contract != operation.input.contract {
        return Err(route_error(
            ErrorCategory::Protocol,
            "contratto d'ingresso diverso da quello dell'operazione",
        ));
    }
    // L'envelope porta il documento JSON della richiesta; per
    // `database.write` i dati Arrow, che il catalogo dichiara fra i content
    // type d'ingresso, arrivano dal resolver come sulla CLI da `--data`.
    if invocation.content_type != JSON_CONTENT_TYPE
        || !operation
            .input
            .content_types
            .iter()
            .any(|content_type| content_type == JSON_CONTENT_TYPE)
    {
        return Err(route_error(
            ErrorCategory::Protocol,
            "content type del payload non ammesso per l'operazione",
        ));
    }
    if request.idempotency_key.is_some() && !operation.controls.idempotency_key {
        return Err(route_error(
            ErrorCategory::Unsupported,
            "l'operazione non accetta chiavi di idempotenza",
        ));
    }
    let deadline = match request.deadline.as_deref() {
        None => None,
        Some(_) if !operation.controls.deadline => {
            return Err(route_error(
                ErrorCategory::Unsupported,
                "l'operazione non accetta una deadline",
            ))
        }
        Some(text) => Some(deadline_instant(text)?),
    };
    Ok(Admitted {
        operation,
        deadline,
    })
}

/// Una deadline RFC 3339 assoluta in UTC (`Z` o `+00:00`), convertita in un
/// istante monotono. Gia scaduta: `timeout` prima dell'esecuzione.
fn deadline_instant(text: &str) -> Result<Instant> {
    let parsed = chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .filter(|value| value.offset().local_minus_utc() == 0)
        .ok_or_else(|| {
            route_error(
                ErrorCategory::Protocol,
                "plenora.execution.deadline deve essere un istante RFC 3339 in UTC",
            )
        })?;
    // Secondi e nanosecondi separati: `timestamp_nanos_opt` sta in un `i64`
    // solo fino al 2262, e una deadline valida oltre quella data non e un
    // errore di protocollo.
    let seconds = u64::try_from(parsed.timestamp()).map_err(|_| deadline_expired())?;
    let deadline = SystemTime::UNIX_EPOCH
        .checked_add(Duration::new(seconds, parsed.timestamp_subsec_nanos()))
        .ok_or_else(|| route_error(ErrorCategory::Protocol, "deadline fuori intervallo"))?;
    let remaining = deadline
        .duration_since(SystemTime::now())
        .ok()
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(deadline_expired)?;
    // Oltre un anno la deadline non puo scadere prima del budget di ogni
    // operazione (al piu dieci minuti, `public_ops::read_limits`): si tiene
    // l'anno, che `Instant` rappresenta su ogni piattaforma.
    Instant::now()
        .checked_add(remaining.min(MAX_DEADLINE_HORIZON))
        .ok_or_else(|| internal("orologio monotono fuori intervallo"))
}

fn deadline_expired() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Timeout,
        ErrorPhase::Validate,
        None,
        "deadline gia trascorsa prima dell'esecuzione",
    )
}

/// La destinazione validata contro lo schema, prima di ogni resolver.
fn validated(target: TargetRequest) -> Result<ValidatedTarget> {
    let target = target.validate()?;
    ensure_contract_provider(&target)?;
    Ok(target)
}

/// `sqlite` e `duckdb` sono nomi di [`ProviderKind`] ma non dello schema
/// `target`: come la CLI, li rifiuta come adapter non disponibile.
fn ensure_contract_provider(target: &ValidatedTarget) -> Result<()> {
    if matches!(target.provider, ProviderKind::Sqlite | ProviderKind::Duckdb) {
        return Err(DatabaseError::unsupported(
            target.provider,
            ErrorPhase::Prepare,
            "provider dichiarato dal contratto ma adapter non disponibile",
        ));
    }
    Ok(())
}

/// RT-013: un riferimento ad artefatto e opaco, nella forma `schema://...`.
/// Un percorso locale (assoluto, relativo, `file:`) non attraversa il
/// confine runtime.
fn ensure_artifact_reference(reference: &str) -> Result<()> {
    let opaque = reference.split_once("://").is_some_and(|(scheme, rest)| {
        let mut characters = scheme.chars();
        characters
            .next()
            .is_some_and(|first| first.is_ascii_lowercase())
            && characters.all(|next| {
                next.is_ascii_lowercase()
                    || next.is_ascii_digit()
                    || matches!(next, '+' | '-' | '.')
            })
            && scheme != "file"
            && !rest.is_empty()
            && !rest.contains('\\')
            && !rest.split('/').any(|segment| segment == "..")
    });
    if opaque {
        Ok(())
    } else {
        Err(DatabaseError::invalid_plan(
            "sul runtime operation_path e parameters_path sono riferimenti opachi ad artefatti, non percorsi locali",
        ))
    }
}

fn decode<T: DeserializeOwned>(payload: &Value) -> Result<T> {
    T::deserialize(payload).map_err(|_| {
        DatabaseError::invalid_plan("payload diverso dal contratto d'ingresso dell'operazione")
    })
}

/// Scrive lo stream come Arrow IPC nel sink, contando byte e SHA-256 dei
/// byte effettivamente scritti.
async fn write_arrow_stream(
    sink: &mut Box<dyn ArrowSink>,
    stream: &mut dyn BatchStream,
    cancellation: &CancellationToken,
) -> Result<DeliveredArtifact> {
    let schema = stream.schema();
    let mut counted = CountingWriter {
        inner: sink,
        bytes: 0,
        digest: Sha256::new(),
    };
    {
        let mut writer = StreamWriter::try_new(&mut counted, &schema).map_err(|_| {
            artifact_error(ErrorPhase::Write, "writer Arrow IPC non inizializzabile")
        })?;
        while let Some(batch) = stream.next_batch(cancellation).await? {
            writer.write(&batch).map_err(|_| {
                artifact_error(ErrorPhase::Write, "RecordBatch Arrow IPC non scrivibile")
            })?;
        }
        writer.finish().map_err(|_| {
            artifact_error(ErrorPhase::Finalize, "stream Arrow IPC non finalizzabile")
        })?;
    }
    counted
        .flush()
        .map_err(|_| artifact_error(ErrorPhase::Finalize, "sink Arrow non svuotabile"))?;
    let checksum = hex(&counted.digest.finalize());
    Ok(DeliveredArtifact {
        byte_count: counted.bytes,
        checksum_algorithm: "sha256",
        checksum,
    })
}

struct CountingWriter<'a> {
    inner: &'a mut Box<dyn ArrowSink>,
    bytes: u64,
    digest: Sha256,
}

impl Write for CountingWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        let accepted = buffer.get(..written).unwrap_or(buffer);
        self.digest.update(accepted);
        self.bytes = self
            .bytes
            .checked_add(u64::try_from(written).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("conteggio byte oltre u64"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn artifact_error(phase: ErrorPhase, message: &'static str) -> DatabaseError {
    DatabaseError::new(ErrorCategory::Io, phase, None, message)
}

/// L'errore di un resolver dell'applicazione: restano gli assi, il testo si
/// sostituisce. Un effetto remoto che l'host dichiara diverso da `none` si
/// conserva, e un retry automatico non sopravvive a un effetto ignoto.
fn redact_host_error(error: &DatabaseError, message: &'static str) -> DatabaseError {
    DatabaseError {
        category: error.category,
        phase: error.phase,
        remote_effect: error.remote_effect,
        retry: error.retry,
        provider: error.provider,
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

fn route_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        ..DatabaseError::new(category, ErrorPhase::Validate, None, message)
    }
}

fn internal(message: &'static str) -> DatabaseError {
    DatabaseError::new(ErrorCategory::Internal, ErrorPhase::Validate, None, message)
}

fn error_document(error: &DatabaseError) -> Value {
    serde_json::to_value(error.public_projection()).unwrap_or_else(|_| {
        json!({
            "category": "internal",
            "phase": "finalize",
            "remote_effect": "unknown",
            "retry": {"kind": "requires_recovery"},
            "provider": null,
            "execution_id": null,
            "message": "errore runtime non serializzabile",
        })
    })
}

/// Solo la forma canonica decimale (`^[1-9][0-9]*$` dello schema dei
/// vettori): `u32::from_str` accetterebbe anche `+1` e `01`.
fn parse_version(value: &str) -> Result<u32> {
    let canonical = !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && !value.starts_with('0');
    canonical
        .then(|| value.parse().ok())
        .flatten()
        .ok_or_else(|| route_error(ErrorCategory::Protocol, "versione runtime non canonica"))
}

fn canonical_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

/// Un'identita della richiesta che non e un UUID canonico non si riflette:
/// il risultato porta l'UUID nil.
fn public_uuid(value: &str) -> String {
    if canonical_uuid(value) {
        value.to_owned()
    } else {
        NIL_UUID.to_owned()
    }
}

/// L'identita del messaggio di risultato: un UUID versione 8 (RFC 9562)
/// derivato da SHA-256 dell'identita della richiesta. Stessa richiesta, stesso
/// risultato; richieste diverse, identita diverse.
fn result_message_id(request_message_id: &str) -> String {
    if !canonical_uuid(request_message_id) {
        return NIL_UUID.to_owned();
    }
    let digest = Sha256::new()
        .chain_update(b"plenora.database-tools/runtime-result\0")
        .chain_update(request_message_id.as_bytes())
        .finalize();
    let mut bytes = [0_u8; 16];
    for (target, source) in bytes.iter_mut().zip(digest.iter()) {
        *target = *source;
    }
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    let hex = hex(&bytes);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push(char::from(DIGITS[usize::from(byte >> 4)]));
        text.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    text
}

#[cfg(test)]
#[path = "runtime_binding_tests.rs"]
mod tests;
