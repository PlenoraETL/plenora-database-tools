//! Richieste e risultati delle operazioni pubbliche, condivisi dalle superfici.
//!
//! La CLI (`test-connection`, `list-*`, `describe-object`, `read`, `write`,
//! `query`) e il binding runtime leggono gli stessi documenti
//! `plenora-database-*-input-v1` e producono gli stessi risultati: SURF-017
//! chiede che la stessa versione di un'operazione abbia su ogni superficie la
//! stessa validazione dell'input, gli stessi risultati e gli stessi assi
//! d'errore. Due copie dei tipi lo prometterebbero soltanto; una copia sola lo
//! ottiene per costruzione.
//!
//! I tipi descrivono `contracts/v2/public-operation-contracts.schema.json`:
//! chiavi sconosciute rifiutate, `catalog` che ammette `null` (lo schema lo
//! tipizza `["string", "null"]`), `parameters_path` e `provider_arguments`
//! che non lo ammettono.

use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind};
use plenora_database_core::provider::{BatchStream, Inspection};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase, Result};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Map, Value};

/// `maxItems` e `maxLength` di `provider_arguments` nello schema `target`.
const MAX_PROVIDER_ARGUMENTS: usize = 32;
const MAX_PROVIDER_ARGUMENT_CHARS: usize = 4096;
/// `pattern` di `secret_environment`: un identificatore di al piu 128
/// caratteri.
const MAX_SECRET_ENVIRONMENT_CHARS: usize = 128;

/// Il budget di `database.read`: dieci milioni di righe, 10 GiB di output,
/// dieci minuti. Le altre risorse sono quelle di [`ResourceLimits::default`].
#[must_use]
pub fn read_limits() -> ResourceLimits {
    ResourceLimits {
        rows: 10_000_000,
        output_bytes: 10 * 1024 * 1024 * 1024,
        duration_ms: 10 * 60 * 1_000,
        ..ResourceLimits::default()
    }
}

/// Destinazione comune: provider, riferimento protetto e argomenti.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TargetRequest {
    pub provider: String,
    pub secret_environment: String,
    #[serde(default)]
    pub provider_arguments: Vec<String>,
}

/// Destinazione validata contro lo schema `target`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedTarget {
    pub provider: ProviderKind,
    /// Nome del riferimento protetto: una variabile d'ambiente per la CLI,
    /// la chiave che il resolver dell'applicazione autorizza sul runtime.
    pub secret_environment: String,
    pub provider_arguments: Vec<String>,
}

impl TargetRequest {
    /// Applica enum, pattern e limiti dello schema `target`.
    ///
    /// Il provider si accetta fra i nomi di [`ProviderKind`]: quelli che lo
    /// schema non elenca (`sqlite`, `duckdb`) li rifiuta poi ogni superficie
    /// come adapter non disponibile, con la stessa categoria.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per un provider sconosciuto, un riferimento al segreto
    /// fuori pattern o argomenti oltre i limiti. Il messaggio nomina il
    /// campo, mai il valore ricevuto.
    pub fn validate(self) -> Result<ValidatedTarget> {
        let provider = parse_provider(&self.provider)?;
        if !is_identifier(&self.secret_environment, MAX_SECRET_ENVIRONMENT_CHARS) {
            return Err(invalid_request(
                "secret_environment deve essere un identificatore di al piu 128 caratteri",
            ));
        }
        if self.provider_arguments.len() > MAX_PROVIDER_ARGUMENTS
            || self
                .provider_arguments
                .iter()
                .any(|argument| argument.chars().count() > MAX_PROVIDER_ARGUMENT_CHARS)
        {
            return Err(invalid_request(
                "provider_arguments oltre 32 elementi o 4096 caratteri",
            ));
        }
        Ok(ValidatedTarget {
            provider,
            secret_environment: self.secret_environment,
            provider_arguments: self.provider_arguments,
        })
    }
}

/// Il nome di un provider, fra quelli di [`ProviderKind`].
///
/// # Errors
///
/// `InvalidPlan` per un nome che nessuna variante porta.
pub fn parse_provider(value: &str) -> Result<ProviderKind> {
    match value {
        "postgres" => Ok(ProviderKind::Postgres),
        "mysql" => Ok(ProviderKind::Mysql),
        "mariadb" => Ok(ProviderKind::Mariadb),
        "sqlserver" => Ok(ProviderKind::Sqlserver),
        "oracle" => Ok(ProviderKind::Oracle),
        "db2" => Ok(ProviderKind::Db2),
        "sqlite" => Ok(ProviderKind::Sqlite),
        "duckdb" => Ok(ProviderKind::Duckdb),
        _ => Err(invalid_request("provider sconosciuto")),
    }
}

fn is_identifier(value: &str, max_chars: usize) -> bool {
    let mut characters = value.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|next| next.is_ascii_alphanumeric() || next == '_')
        && value.len() <= max_chars
}

/// `database.list_schemas`: `catalog` assente o `null` vale il predefinito.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListSchemasRequest {
    pub provider: String,
    pub secret_environment: String,
    #[serde(default)]
    pub provider_arguments: Vec<String>,
    #[serde(default)]
    pub catalog: Option<String>,
}

/// `database.list_objects`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListObjectsRequest {
    pub provider: String,
    pub secret_environment: String,
    #[serde(default)]
    pub provider_arguments: Vec<String>,
    #[serde(default)]
    pub catalog: Option<String>,
    pub schema: String,
}

/// `database.describe_object`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DescribeObjectRequest {
    pub provider: String,
    pub secret_environment: String,
    #[serde(default)]
    pub provider_arguments: Vec<String>,
    #[serde(default)]
    pub catalog: Option<String>,
    pub schema: String,
    pub object: String,
}

/// `database.read` e `database.query`: il documento dell'operazione e,
/// facoltativo, quello dei parametri.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRequest {
    pub provider: String,
    pub secret_environment: String,
    #[serde(default)]
    pub provider_arguments: Vec<String>,
    pub operation_path: String,
    /// Solo stringa nello schema: `null` si rifiuta invece di valere assente.
    #[serde(default, deserialize_with = "present_not_null")]
    pub parameters_path: Option<String>,
}

/// `database.write`: il documento dell'operazione; i dati Arrow arrivano
/// fuori da questo documento (`--data` sulla CLI, il resolver sul runtime).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteRequest {
    pub provider: String,
    pub secret_environment: String,
    #[serde(default)]
    pub provider_arguments: Vec<String>,
    pub operation_path: String,
}

macro_rules! target_of {
    ($($request:ty),*) => {$(
        impl $request {
            /// La destinazione della richiesta, da validare.
            #[must_use]
            pub fn target(&self) -> TargetRequest {
                TargetRequest {
                    provider: self.provider.clone(),
                    secret_environment: self.secret_environment.clone(),
                    provider_arguments: self.provider_arguments.clone(),
                }
            }
        }
    )*};
}

target_of!(
    ListSchemasRequest,
    ListObjectsRequest,
    DescribeObjectRequest,
    OperationRequest,
    WriteRequest
);

/// Legge un campo opzionale distinguendo l'assenza da `null`.
///
/// Con `#[serde(default)]` l'assenza la decide il default, e questa funzione
/// riceve solo un valore presente, che quindi non puo essere `null`.
fn present_not_null<'de, D>(deserializer: D) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

/// Un campo che lo schema vuole non vuoto (`minLength: 1`) e che nessuna
/// operazione puo dedurre.
///
/// # Errors
///
/// `InvalidPlan` se il valore e vuoto o di soli spazi: vuoto non e un
/// carattere jolly, e uno schema vuoto in `list_objects` significherebbe
/// «tutti».
pub fn required_value(value: String, label: &str) -> Result<String> {
    if value.trim().is_empty() {
        return Err(invalid_request(format!("{label} vuoto")));
    }
    Ok(value)
}

impl ListSchemasRequest {
    /// L'ispezione che la richiesta descrive.
    #[must_use]
    pub fn operation(&self) -> Operation {
        Operation::DatabaseListSchemas {
            source: self.catalog.clone().map(|catalog| ObjectRef {
                catalog: Some(catalog),
                schema: None,
                object: String::new(),
            }),
        }
    }
}

impl ListObjectsRequest {
    /// L'ispezione che la richiesta descrive.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per uno schema vuoto.
    pub fn operation(&self) -> Result<Operation> {
        Ok(Operation::DatabaseListObjects {
            source: Some(ObjectRef {
                catalog: self.catalog.clone(),
                schema: Some(required_value(self.schema.clone(), "schema")?),
                object: String::new(),
            }),
        })
    }
}

impl DescribeObjectRequest {
    /// L'ispezione che la richiesta descrive.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per uno schema o un oggetto vuoti.
    pub fn operation(&self) -> Result<Operation> {
        Ok(Operation::DatabaseDescribeObject {
            source: ObjectRef {
                catalog: self.catalog.clone(),
                schema: Some(required_value(self.schema.clone(), "schema")?),
                object: required_value(self.object.clone(), "object")?,
            },
        })
    }
}

/// Il documento di un'ispezione: provider e operazione, poi i campi che
/// l'adapter ha prodotto (`schemas`, `objects`, `columns`...).
///
/// # Errors
///
/// `Internal` se l'adapter non ha prodotto un oggetto o ne usa un campo
/// riservato.
pub fn inspection_document(kind: ProviderKind, inspection: Inspection) -> Result<Value> {
    let Value::Object(document) = inspection.document else {
        return Err(internal("documento di introspezione non strutturato"));
    };
    let mut output = Map::new();
    output.insert("schema_version".to_owned(), json!(1));
    output.insert("provider".to_owned(), json!(kind));
    output.insert("operation".to_owned(), json!(inspection.operation));
    for (key, value) in document {
        if output.insert(key, value).is_some() {
            return Err(internal(
                "documento di introspezione usa un campo riservato",
            ));
        }
    }
    Ok(Value::Object(output))
}

/// I campi Arrow di uno stream, con tipo, nullabilita e metadati.
#[must_use]
pub fn stream_fields(stream: &dyn BatchStream) -> Vec<Value> {
    stream
        .schema()
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                "data_type": field.data_type().to_string(),
                "nullable": field.is_nullable(),
                "metadata": field.metadata(),
            })
        })
        .collect()
}

/// Consuma lo stream e ne rende il riepilogo di `database.query`: batch,
/// righe e campi, mai i valori.
///
/// # Errors
///
/// L'errore dello stream, oppure `ResourceLimit` se un conteggio supera
/// `u64`.
pub async fn summarize_stream(
    kind: ProviderKind,
    stream: &mut dyn BatchStream,
    cancellation: &CancellationToken,
) -> Result<Value> {
    let fields = stream_fields(stream);
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    while let Some(batch) = stream.next_batch(cancellation).await? {
        batches = batches
            .checked_add(1)
            .ok_or_else(|| DatabaseError::resource_limit("conteggio batch oltre u64"))?;
        rows = u64::try_from(batch.num_rows())
            .ok()
            .and_then(|count| rows.checked_add(count))
            .ok_or_else(|| DatabaseError::resource_limit("conteggio righe oltre u64"))?;
    }
    Ok(json!({
        "schema_version": 1,
        "status": "ok",
        "provider": kind,
        "batches": batches,
        "rows": rows,
        "fields": fields,
    }))
}

/// Una richiesta che non soddisfa il proprio input contract (SURF-007).
///
/// `invalid_configuration`, `validate`, `none`, `never`. E la categoria che la
/// matrice runtime comune propone (caso 4) e vale per CLI e runtime insieme;
/// `invalid_plan` resta ai documenti di operazione, che sono piani.
#[must_use]
pub fn invalid_request(message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        plenora_database_core::ErrorCategory::InvalidConfiguration,
        ErrorPhase::Validate,
        None,
        message,
    )
}

fn internal(message: &'static str) -> DatabaseError {
    DatabaseError::new(
        plenora_database_core::ErrorCategory::Internal,
        ErrorPhase::Finalize,
        None,
        message,
    )
}

#[cfg(test)]
#[path = "public_ops_tests.rs"]
mod tests;
