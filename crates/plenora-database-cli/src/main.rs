#![allow(clippy::redundant_pub_crate)]

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use plenora_database_core::plan::ProviderKind;
#[cfg(feature = "postgres")]
use plenora_database_core::plan::{ObjectRef, OrderBy, SortDirection};
// Il percorso `postgres-read-ipc` e l'unico che pianifica una lettura e ne
// misura il budget: fuori dalla feature questi nomi non hanno un chiamante.
#[cfg(feature = "postgres")]
use plenora_database_core::plan::{Operation, ReadOperation};
use plenora_database_core::provider::{Provider, SecretString};
// Lo streaming a batch esce solo dal percorso IPC.
#[cfg(feature = "postgres")]
use plenora_database_core::provider::BatchStream;
#[cfg(feature = "postgres")]
use plenora_database_core::provider::ParameterBag;
#[cfg(feature = "postgres")]
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::CommitOutcome;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase};
// Non piu dietro `postgres`: il giudizio sul commit incerto e comune a tutti i
// provider, e vive negli helper qui sotto.
use plenora_database_core::{ErrorCategory, RemoteEffect, RetryDisposition};
use plenora_database_engine::parse_and_validate;
#[cfg(feature = "mysql")]
use plenora_db_mysql::{MysqlConfig, MysqlProvider};
#[cfg(feature = "postgres")]
use plenora_db_postgres::{PostgresProvider, PostgresTlsConfig, PostgresTlsMode};
#[cfg(feature = "sqlserver")]
use plenora_db_sqlserver::{SqlServerConfig, SqlServerProvider};
use rustls::{pki_types::CertificateDer, RootCertStore};
use serde_json::json;
use std::env;
#[cfg(feature = "postgres")]
use std::fs::OpenOptions;
use std::fs::{self, File};
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
#[cfg(feature = "postgres")]
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(feature = "postgres")]
use arrow_ipc::writer::FileWriter;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(CliError::Silent) => {
            // Fix review #10: comandi ops/doctor/pool-status hanno già
            // stampato il JSON con `status: unhealthy | fail | ...`
            // sopra. Qui serve solo trasmettere l'exit code non-zero
            // per rendere affidabile l'uso in CI, senza duplicare il
            // JSON con un secondo blocco `status: error`.
            ExitCode::FAILURE
        }
        Err(CliError::Fatal(error)) => {
            println!(
                "{}",
                CliError::Fatal(error)
                    .to_json()
                    .unwrap_or_else(|_| ERROR_SERIALIZATION_FALLBACK.to_owned())
            );
            ExitCode::FAILURE
        }
    }
}

const ERROR_SERIALIZATION_FALLBACK: &str = concat!(
    r#"{"status":"error","protocol_version":1,"error":{"category":"internal","#,
    r#""phase":"finalize","remote_effect":"none","retry":{"kind":"never"},"#,
    r#""provider":null,"execution_id":null,"message":"errore non serializzabile"}}"#
);

#[derive(Debug)]
pub(crate) enum CliError {
    /// Errore fatale — main stampa il JSON `status: error` + exit=1.
    Fatal(DatabaseError),
    /// Fallimento logico già stampato dal sottocomando (es. doctor →
    /// `status: unhealthy`). Main emette solo exit=1 senza duplicare
    /// output. Fix review #10.
    // Costruita da `diagnose`, che vive dietro la feature `postgres`. La
    // variante resta nell'enum perche i suoi rami di `match` sono neutri:
    // spostarla dietro la feature significherebbe cfg-are anche quelli.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    Silent,
}

/// Cosa la CLI dice di un `commit()`, e con quale codice di uscita.
///
/// `CommitOutcome::OutcomeUnknown` e un `Ok`: il commit e stato **emesso** e
/// l'esito remoto non e verificabile. Chi scriveva `tx.commit(...).await?` lo
/// riceveva come successo, e i comandi stampavano `"status": "ok"` con uscita
/// 0 — cioe dicevano a un'automazione che la mutazione era andata a buon fine
/// mentre il contratto chiedeva quarantena e verifica fuori banda. Un retry su
/// quella base puo raddoppiare una scrittura gia applicata.
///
/// Qui l'incertezza ha un nome suo — `outcome_unknown` — e non e mai `ok`.
pub(crate) const fn commit_status(outcome: &CommitOutcome) -> &'static str {
    match outcome {
        CommitOutcome::Committed => "ok",
        CommitOutcome::OutcomeUnknown { .. } => "outcome_unknown",
    }
}

/// L'uscita che accompagna un esito gia stampato: zero solo se certo.
pub(crate) const fn commit_exit(outcome: &CommitOutcome) -> CliResult<()> {
    match outcome {
        CommitOutcome::Committed => Ok(()),
        CommitOutcome::OutcomeUnknown { .. } => Err(CliError::Silent),
    }
}

/// Un commit che *deve* essere certo perche il comando prosegua.
///
/// Serve dove l'esito non viene stampato: preparazione di uno schema
/// effimero, setup di un benchmark, helper amministrativi. Li proseguire su un
/// commit incerto significa costruire il resto del comando su uno stato
/// remoto che nessuno ha verificato.
///
/// # Errors
///
/// `Fatal` con effetto remoto ignoto e disposizione `RequiresRecovery`.
pub(crate) fn require_committed(outcome: &CommitOutcome) -> CliResult<()> {
    match outcome {
        CommitOutcome::Committed => Ok(()),
        CommitOutcome::OutcomeUnknown { .. } => Err(CliError::Fatal(DatabaseError {
            category: ErrorCategory::Internal,
            phase: ErrorPhase::Commit,
            remote_effect: RemoteEffect::Unknown,
            retry: RetryDisposition::RequiresRecovery,
            provider: None,
            execution_id: None,
            message: "commit emesso senza conferma: verificare lo stato remoto \
                      prima di ogni retry"
                .to_owned(),
            diagnostics: None,
        })),
    }
}

pub(crate) type CliResult<T> = std::result::Result<T, CliError>;

#[cfg(feature = "postgres")]
static IPC_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[cfg(feature = "postgres")]
const IPC_DEFAULT_MAX_ROWS: u64 = 10_000_000;
#[cfg(feature = "postgres")]
const IPC_DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024 * 1024;
#[cfg(feature = "postgres")]
const IPC_DEFAULT_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
const TLS_MATERIAL_MAX_BYTES: u64 = 1024 * 1024;

#[cfg(feature = "postgres")]
struct IpcOptions {
    limits: ResourceLimits,
    order_by: Vec<OrderBy>,
    projection: Vec<String>,
    filter: Option<plenora_database_core::plan::FilterExpression>,
    row_limit: Option<u64>,
    parameters: plenora_database_core::provider::ParameterBag,
}

impl CliError {
    /// Accesso al `DatabaseError` sottostante per test / mutazioni
    /// diagnostiche. Panica per `Silent` (uso post-review #10).
    #[cfg(test)]
    fn database_error(&self) -> &DatabaseError {
        match self {
            Self::Fatal(db_err) => db_err,
            Self::Silent => panic!("CliError::Silent non ha DatabaseError"),
        }
    }

    /// Accesso mutabile al `DatabaseError` sottostante. Panica per `Silent`.
    // Serve al solo percorso IPC, che riscrive `remote_effect` dopo un
    // fallimento di pubblicazione dell'artefatto locale.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    fn database_error_mut(&mut self) -> &mut DatabaseError {
        match self {
            Self::Fatal(db_err) => db_err,
            Self::Silent => panic!("CliError::Silent non ha DatabaseError"),
        }
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        match self {
            Self::Fatal(db_err) => {
                let error = serde_json::to_value(db_err)?;
                serde_json::to_string(&json!({
                    "status": "error",
                    "protocol_version": 1,
                    "error": error,
                }))
            }
            Self::Silent => Ok(String::new()),
        }
    }
}

impl From<DatabaseError> for CliError {
    fn from(error: DatabaseError) -> Self {
        Self::Fatal(error)
    }
}

impl From<String> for CliError {
    fn from(message: String) -> Self {
        Self::Fatal(DatabaseError::invalid_plan(message))
    }
}

impl From<&str> for CliError {
    fn from(message: &str) -> Self {
        Self::Fatal(DatabaseError::invalid_plan(message))
    }
}

#[allow(clippy::needless_collect, clippy::too_many_lines)]
// std::env::Args non è Send: materializzare prima degli await mantiene il
// future del main compatibile con il runtime multi-thread.
async fn run() -> CliResult<()> {
    let raw = env::args().skip(1).collect::<Vec<_>>();
    let after_format = format::strip_output_format(raw)?;
    let after_safety = safety::strip_safety_flags(after_format)?;
    #[cfg(feature = "postgres")]
    let after_session = session_ctx::strip_session_context(after_safety)?;
    #[cfg(not(feature = "postgres"))]
    let after_session = after_safety;
    let mut args = after_session.into_iter();
    let command = args.next().ok_or_else(|| CliError::from(usage()))?;
    format::set_active_command(&command);
    match command.as_str() {
        "inspect-dataset" => inspect_dataset(&mut args),
        "validate-plan" => validate_plan(&mut args),
        "database-probe" => database_probe(&mut args).await,
        #[cfg(feature = "postgres")]
        "postgres-probe" => postgres_probe(&mut args).await,
        #[cfg(feature = "postgres")]
        "postgres-describe" => postgres_describe(&mut args).await,
        #[cfg(feature = "postgres")]
        "postgres-read-summary" => postgres_read_summary(&mut args).await,
        #[cfg(feature = "postgres")]
        "postgres-read-ipc" => postgres_read_ipc(&mut args).await,
        #[cfg(feature = "postgres")]
        "profile-check" => profile_check(&mut args).await,
        #[cfg(feature = "postgres")]
        "profile-list" => profile_list(&mut args),
        #[cfg(feature = "postgres")]
        "doctor" => doctor(&mut args).await,
        #[cfg(feature = "postgres")]
        "diagnose" => diagnose::diagnose(&mut args).await,
        #[cfg(feature = "postgres")]
        "execute-ddl" => execute_ddl_cmd(&mut args).await,
        #[cfg(feature = "postgres")]
        "execute-sql" => execute_sql_cmd(&mut args).await,
        #[cfg(feature = "postgres")]
        "transaction-test" => transaction_test(&mut args).await,
        #[cfg(feature = "postgres")]
        "session-context-test" => session_context_test(&mut args).await,
        #[cfg(feature = "postgres")]
        "test-cancellation" => test_cancellation(&mut args).await,
        #[cfg(feature = "postgres")]
        "test-streaming" => test_streaming(&mut args).await,
        #[cfg(feature = "postgres")]
        "test-spatial" => test_spatial(&mut args).await,
        #[cfg(feature = "postgres")]
        "test-concurrency" => test_concurrency(&mut args).await,
        #[cfg(feature = "postgres")]
        "inspect-database" => inspect::inspect_database(&mut args).await,
        #[cfg(feature = "postgres")]
        "inspect-schemas" => inspect::inspect_schemas(&mut args).await,
        #[cfg(feature = "postgres")]
        "inspect-tables" => inspect::inspect_tables(&mut args).await,
        #[cfg(feature = "postgres")]
        "inspect-catalogs" => inspect::inspect_catalogs(&mut args).await,
        #[cfg(feature = "postgres")]
        "inspect-objects" => inspect::inspect_objects(&mut args).await,
        #[cfg(feature = "postgres")]
        "postgres-query" => query_cmd::postgres_query(&mut args).await,
        #[cfg(feature = "postgres")]
        "portable-compile" => query_cmd::portable_compile(&mut args),
        #[cfg(feature = "postgres")]
        "portable-execute" => query_cmd::portable_execute(&mut args).await,
        #[cfg(feature = "postgres")]
        "bulk-write" => write_cmd::bulk_write(&mut args).await,
        #[cfg(feature = "postgres")]
        "postgres-write-ipc" => write_cmd::postgres_write_ipc(&mut args).await,
        #[cfg(feature = "postgres")]
        "execute-scalar" => ops_cmd::execute_scalar(&mut args).await,
        #[cfg(feature = "postgres")]
        "conditional-update" => ops_cmd::conditional_update(&mut args).await,
        #[cfg(feature = "postgres")]
        "pool-status" => ops_cmd::pool_status(&mut args).await,
        #[cfg(feature = "postgres")]
        "explain" => ops_cmd::explain(&mut args).await,
        #[cfg(feature = "postgres")]
        "benchmark-oltp" => benchmark_oltp(&mut args).await,
        #[cfg(feature = "postgres")]
        "benchmark-read" => benchmark_read(&mut args).await,
        #[cfg(feature = "postgres")]
        "benchmark-write" => benchmark::benchmark_write(&mut args).await,
        #[cfg(feature = "postgres")]
        "benchmark-spatial" => benchmark_spatial(&mut args).await,
        // MySQL v1.2 subset — parity iniziale col path Postgres.
        #[cfg(feature = "mysql")]
        "mysql-probe" => mysql_cmd::mysql_probe(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-describe" => mysql_cmd::mysql_describe(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-inspect-schemas" => mysql_cmd::mysql_inspect_schemas(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-inspect-tables" => mysql_cmd::mysql_inspect_tables(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-execute-sql" => mysql_cmd::mysql_execute_sql(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-execute-ddl" => mysql_cmd::mysql_execute_ddl(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-execute-scalar" => mysql_cmd::mysql_execute_scalar(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-transaction-test" => mysql_cmd::mysql_transaction_test(&mut args).await,
        #[cfg(feature = "mysql")]
        "mysql-conditional-update" => mysql_cmd::mysql_conditional_update(&mut args).await,
        _ => Err(unknown_command(&command)),
    }
}

fn inspect_dataset(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let path = one_argument(args, "manca il percorso del dataset Arrow IPC")?;
    let report = inspect_dataset::inspect(&path)?;
    print_json(&report)
}

/// `validate-plan <file.json> [--capabilities <file.json>]`
///
/// Senza `--capabilities` verifica solo cio che il piano dichiara di se: la
/// forma, i limiti, i riferimenti. Con, esegue anche la **preparazione**, cioe
/// il confronto fra cio che il piano chiede e cio che un provider pubblicizza.
///
/// La seconda meta esisteva gia in `plenora_database_engine::prepare`, e non
/// aveva chiamanti: nessuna superficie la raggiungeva, quindi la matrice
/// piano-capability non veniva mai eseguita e nessun test poteva accorgersi
/// che fosse incompleta. Il documento capability e un file, non una
/// connessione, quindi la verifica resta offline.
fn validate_plan(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let path = args
        .next()
        .ok_or_else(|| "manca il percorso del piano".to_owned())?;
    let mut capabilities_path: Option<String> = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--capabilities" => {
                capabilities_path = Some(
                    args.next()
                        .ok_or_else(|| "--capabilities richiede un percorso".to_owned())?,
                );
            }
            other => {
                if let Some(value) = other.strip_prefix("--capabilities=") {
                    capabilities_path = Some(value.to_owned());
                } else {
                    // Nessun `{other}`: un argomento fuori posto puo essere
                    // qualunque cosa la riga di comando abbia raccolto, DSN
                    // compresi, e questo messaggio finisce in stderr JSON.
                    return Err("argomento inatteso per validate-plan".into());
                }
            }
        }
    }

    let input = fs::read(path).map_err(|_| "piano non leggibile".to_owned())?;
    let validated = parse_and_validate(&input)?;
    let provider = validated.plan().provider;
    let fingerprint = validated.fingerprint().to_owned();

    let Some(capabilities_path) = capabilities_path else {
        return print_json(&json!({
            "schema_version": 1,
            "status": "validated",
            "provider": provider,
            "fingerprint": fingerprint
        }));
    };

    let document =
        fs::read(&capabilities_path).map_err(|_| "capability non leggibili".to_owned())?;
    let capabilities: plenora_database_core::capabilities::ProviderCapabilities =
        serde_json::from_slice(&document).map_err(|e| {
            format!(
                "capability non parsabili a riga {}, colonna {}",
                e.line(),
                e.column()
            )
        })?;
    let prepared = plenora_database_engine::prepare(validated, capabilities)?;
    print_json(&json!({
        "schema_version": 1,
        "status": "prepared",
        "provider": provider,
        "fingerprint": fingerprint,
        "provider_version": prepared.capabilities().provider_version,
    }))
}

async fn database_probe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let provider_name = args.next().ok_or_else(|| "manca il provider".to_owned())?;
    let kind = parse_provider_kind(&provider_name)?;
    if !matches!(
        kind,
        ProviderKind::Postgres | ProviderKind::Mysql | ProviderKind::Sqlserver
    ) {
        return Err(CliError::Fatal(DatabaseError::unsupported(
            kind,
            ErrorPhase::Prepare,
            "provider dichiarato dal contratto ma adapter non disponibile",
        )));
    }
    let env_name = args
        .next()
        .ok_or_else(|| "manca il nome della variabile secret".to_owned())?;
    let provider_arguments = parse_provider_arguments(kind, args)?;
    let provider_arguments = prepare_provider_arguments(provider_arguments)?;
    let secret = secret_from_env(&env_name)?;
    let provider = build_provider_from_prepared_arguments(provider_arguments, &secret)?;
    let cancellation = CancellationToken::new();
    let connection = provider.test_connection(&secret, &cancellation).await?;
    let capabilities = provider.probe_capabilities(&secret, &cancellation).await?;
    print_json(&json!({
        "schema_version": 1,
        "connection": connection,
        "capabilities": capabilities
    }))
}

#[cfg(feature = "postgres")]
async fn postgres_probe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let env_name = args
        .next()
        .ok_or_else(|| "manca il nome della variabile DSN".to_owned())?;
    let tls = parse_tls_path_environments(args)?;
    let provider = postgres_provider_for_probe_with_tls(&tls)?;
    let secret = secret_from_env(&env_name)?;
    let cancellation = CancellationToken::new();
    let connection = provider.test_connection(&secret, &cancellation).await?;
    let capabilities = provider.probe_capabilities(&secret, &cancellation).await?;
    print_json(&json!({
        "schema_version": 1,
        "connection": connection,
        "capabilities": capabilities
    }))
}

#[cfg(feature = "postgres")]
async fn postgres_describe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let env_name = args
        .next()
        .ok_or_else(|| "manca il nome della variabile DSN".to_owned())?;
    let schema = args.next().ok_or_else(|| "manca lo schema".to_owned())?;
    let object = args.next().ok_or_else(|| "manca l'oggetto".to_owned())?;
    ensure_end(args)?;
    let secret = secret_from_env(&env_name)?;
    let provider = pfm::postgres_provider_for_pfm()?;
    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: object_ref(schema, object),
            },
            &CancellationToken::new(),
        )
        .await?;
    print_json(
        &serde_json::to_value(inspection).map_err(|_| "output non serializzabile".to_owned())?,
    )
}

#[cfg(feature = "postgres")]
async fn postgres_read_summary(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let env_name = args
        .next()
        .ok_or_else(|| "manca il nome della variabile DSN".to_owned())?;
    let schema = args.next().ok_or_else(|| "manca lo schema".to_owned())?;
    let object = args.next().ok_or_else(|| "manca l'oggetto".to_owned())?;
    ensure_end(args)?;
    let secret = secret_from_env(&env_name)?;
    let provider = pfm::postgres_provider_for_pfm()?;
    let operation = ReadOperation {
        source: object_ref(schema, object),
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        filter: None,
    };
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .read(
            &secret,
            &operation,
            &ParameterBag::default(),
            &ResourceBudget::new(ResourceLimits::default())?,
            &cancellation,
        )
        .await?;
    let schema = stream.schema();
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                "data_type": field.data_type().to_string(),
                "nullable": field.is_nullable(),
                "metadata": field.metadata()
            })
        })
        .collect::<Vec<_>>();
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        batches += 1;
        rows +=
            u64::try_from(batch.num_rows()).map_err(|_| "conteggio righe oltre u64".to_owned())?;
    }
    print_json(&json!({
        "schema_version": 1,
        "provider": ProviderKind::Postgres,
        "batches": batches,
        "rows": rows,
        "fields": fields
    }))
}

#[cfg(feature = "postgres")]
async fn postgres_read_ipc(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let env_name = args
        .next()
        .ok_or_else(|| "manca il nome della variabile DSN".to_owned())?;
    let schema = args.next().ok_or_else(|| "manca lo schema".to_owned())?;
    let object = args.next().ok_or_else(|| "manca l'oggetto".to_owned())?;
    let output = args
        .next()
        .ok_or_else(|| "manca il percorso output Arrow IPC".to_owned())?;
    let options = parse_ipc_options(args)?;
    let secret = secret_from_env(&env_name)?;
    let provider = pfm::postgres_provider_for_pfm()?;
    let operation = ReadOperation {
        source: object_ref(schema, object),
        projection: options.projection.clone(),
        order_by: options.order_by.clone(),
        row_limit: options.row_limit,
        filter: options.filter.clone(),
    };
    let row_order = if options.order_by.is_empty() {
        "unspecified"
    } else {
        "deterministic"
    };
    let limits_report = json!({
        "max_rows": options.limits.rows,
        "max_output_bytes": options.limits.output_bytes,
        "timeout_ms": options.limits.duration_ms,
    });
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .read(
            &secret,
            &operation,
            &options.parameters,
            &ResourceBudget::new(options.limits)?,
            &cancellation,
        )
        .await?;
    let mut report =
        write_stream_to_ipc(Path::new(&output), stream.as_mut(), &cancellation).await?;
    // Un solo `as_object_mut`: le due `expect` che seguivano ripetevano un
    // controllo gia' fatto qui sopra, e ripeterlo con un panico invece che con
    // un errore era l'unico punto del binario che poteva abbattere il processo.
    let oggetto = report
        .as_object_mut()
        .ok_or_else(|| CliError::from("report Arrow IPC non valido"))?;
    oggetto.insert("provider".to_owned(), json!(ProviderKind::Postgres));
    oggetto.insert("row_order".to_owned(), json!(row_order));
    oggetto.insert("limits".to_owned(), limits_report);
    print_json(&report)
}

#[cfg(feature = "postgres")]
fn parse_ipc_options(args: &mut impl Iterator<Item = String>) -> CliResult<IpcOptions> {
    let mut limits = ResourceLimits {
        rows: IPC_DEFAULT_MAX_ROWS,
        output_bytes: IPC_DEFAULT_MAX_OUTPUT_BYTES,
        duration_ms: IPC_DEFAULT_TIMEOUT_MS,
        ..ResourceLimits::default()
    };
    let mut order_by = Vec::new();
    let mut projection: Vec<String> = Vec::new();
    let mut filter: Option<plenora_database_core::plan::FilterExpression> = None;
    let mut row_limit: Option<u64> = None;
    let mut params_map: std::collections::BTreeMap<
        String,
        plenora_database_core::provider::ParameterValue,
    > = std::collections::BTreeMap::new();
    while let Some(option) = args.next() {
        let value = args
            .next()
            .ok_or("manca il valore per l'ultima opzione della riga di comando")?;
        match option.as_str() {
            "--max-rows" => limits.rows = parse_positive_u64(&option, &value)?,
            "--max-output-bytes" => {
                limits.output_bytes = parse_positive_u64(&option, &value)?;
            }
            "--timeout-ms" => limits.duration_ms = parse_positive_u64(&option, &value)?,
            "--order-by" => {
                if value.is_empty() {
                    return Err("--order-by richiede un campo non vuoto".into());
                }
                order_by.push(OrderBy {
                    field: value,
                    direction: SortDirection::Asc,
                });
            }
            "--project" => {
                if value.is_empty() {
                    return Err("--project richiede una lista non vuota".into());
                }
                projection = value.split(',').map(|s| s.trim().to_owned()).collect();
                if projection.iter().any(String::is_empty) {
                    return Err("--project ha token vuoto".into());
                }
            }
            "--filter" => {
                // Il valore è un percorso a un JSON che deserializza in
                // FilterExpression.
                // Il percorso resta nel messaggio: e contesto operativo, cioe
                // cio che `DatabaseError::message` ammette per contratto, e
                // l'ha scritto il chiamante sulla riga di comando. Il
                // *contenuto* del file no: quello e payload.
                let content = fs::read(&value)
                    .map_err(|_| format!("--filter file non leggibile: {value}"))?;
                let parsed: plenora_database_core::plan::FilterExpression =
                    serde_json::from_slice(&content).map_err(|e| {
                        format!(
                            "--filter JSON non parsabile a riga {}, colonna {}",
                            e.line(),
                            e.column()
                        )
                    })?;
                filter = Some(parsed);
            }
            "--limit" => {
                row_limit = Some(parse_positive_u64(&option, &value)?);
            }
            "--parameter" => {
                // Sintassi: --parameter NAME=VALUE:TYPE. Aggiunge un parametro
                // referenziabile dal filtro come `field=NAME`.
                let (name, val) = typed_params::parse_named_value_type(&value)?;
                if params_map.insert(name.clone(), val).is_some() {
                    return Err("--parameter duplicato".into());
                }
            }
            _ => return Err("opzione postgres-read-ipc sconosciuta".into()),
        }
    }
    limits.validate()?;
    Ok(IpcOptions {
        limits,
        order_by,
        projection,
        filter,
        row_limit,
        parameters: plenora_database_core::provider::ParameterBag::new(params_map),
    })
}

#[cfg(feature = "postgres")]
fn parse_positive_u64(option: &str, value: &str) -> CliResult<u64> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("valore non valido per {option}"))?;
    if parsed == 0 {
        return Err(format!("{option} deve essere maggiore di zero").into());
    }
    Ok(parsed)
}

#[cfg(feature = "postgres")]
async fn write_stream_to_ipc(
    output: &Path,
    stream: &mut dyn BatchStream,
    cancellation: &CancellationToken,
) -> CliResult<serde_json::Value> {
    if output.exists() {
        return Err(local_artifact_error(
            ErrorCategory::Conflict,
            ErrorPhase::Validate,
            RemoteEffect::None,
            RetryDisposition::Never,
            "output Arrow IPC gia' esistente",
        ));
    }
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let name = output
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CliError::from("percorso output Arrow IPC non valido"))?;
    let (temporary, mut file) = create_ipc_temporary(parent, name)?;
    let result = write_ipc_batches(&mut file, stream, cancellation).await;
    let (batches, rows) = match result {
        Ok(counts) => counts,
        Err(mut error) => {
            drop(file);
            match fs::remove_file(&temporary) {
                Ok(()) => {
                    error.database_error_mut().remote_effect = RemoteEffect::RolledBack;
                    return Err(error);
                }
                Err(cleanup_error) => {
                    return Err(local_artifact_error(
                        ErrorCategory::Io,
                        ErrorPhase::Cleanup,
                        RemoteEffect::Partial,
                        RetryDisposition::RequiresRecovery,
                        format!(
                            "rollback artifact temporaneo fallito; recovery richiesta per {}: \
                             {cleanup_error}",
                            temporary.display()
                        ),
                    ));
                }
            }
        }
    };
    drop(file);
    if let Err(error) = fs::hard_link(&temporary, output) {
        let category = match error.kind() {
            std::io::ErrorKind::AlreadyExists => ErrorCategory::Conflict,
            std::io::ErrorKind::Unsupported => ErrorCategory::Unsupported,
            _ => ErrorCategory::Io,
        };
        let mut publish_error = local_artifact_error(
            category,
            ErrorPhase::Commit,
            RemoteEffect::None,
            RetryDisposition::Never,
            format!("pubblicazione Arrow IPC fallita: {}", error.kind()),
        );
        return match fs::remove_file(&temporary) {
            Ok(()) => {
                publish_error.database_error_mut().remote_effect = RemoteEffect::RolledBack;
                Err(publish_error)
            }
            Err(cleanup_error) => Err(local_artifact_error(
                ErrorCategory::Io,
                ErrorPhase::Cleanup,
                RemoteEffect::Partial,
                RetryDisposition::RequiresRecovery,
                format!(
                    "publish fallito e rollback artifact temporaneo fallito; recovery richiesta \
                     per {}: {cleanup_error}",
                    temporary.display()
                ),
            )),
        };
    }
    let staging_cleanup = if fs::remove_file(&temporary).is_ok() {
        "complete"
    } else {
        "required"
    };
    let durability = if sync_parent_directory(parent) {
        "confirmed"
    } else {
        "unconfirmed"
    };
    Ok(json!({
        "schema_version": 1,
        "status": "materialized",
        "format": "arrow_ipc_file",
        "batches": batches,
        "rows": rows,
        "durability": durability,
        "staging_cleanup": staging_cleanup,
    }))
}

#[cfg(feature = "postgres")]
#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> bool {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .is_ok()
}

#[cfg(feature = "postgres")]
#[cfg(windows)]
fn sync_parent_directory(parent: &Path) -> bool {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(parent)
        .and_then(|directory| directory.sync_all())
        .is_ok()
}

#[cfg(feature = "postgres")]
fn create_ipc_temporary(parent: &Path, name: &str) -> CliResult<(PathBuf, File)> {
    for _ in 0..100 {
        let sequence = IPC_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".{name}.partial-{}-{sequence}", std::process::id()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&candidate) {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(local_artifact_error(
                    ErrorCategory::Io,
                    ErrorPhase::Write,
                    RemoteEffect::None,
                    RetryDisposition::Never,
                    format!(
                        "artifact temporaneo Arrow IPC non creabile: {}",
                        error.kind()
                    ),
                ));
            }
        }
    }
    Err(local_artifact_error(
        ErrorCategory::Conflict,
        ErrorPhase::Write,
        RemoteEffect::None,
        RetryDisposition::Safe,
        "nessun nome temporaneo Arrow IPC disponibile",
    ))
}

#[cfg(feature = "postgres")]
async fn write_ipc_batches(
    file: &mut File,
    stream: &mut dyn BatchStream,
    cancellation: &CancellationToken,
) -> CliResult<(u64, u64)> {
    let schema = stream.schema();
    let mut writer = FileWriter::try_new_buffered(&mut *file, &schema).map_err(|_| {
        local_artifact_error(
            ErrorCategory::Io,
            ErrorPhase::Write,
            RemoteEffect::None,
            RetryDisposition::Never,
            "writer Arrow IPC non inizializzabile",
        )
    })?;
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    while let Some(batch) = stream.next_batch(cancellation).await? {
        batches = batches.checked_add(1).ok_or_else(|| {
            local_artifact_error(
                ErrorCategory::ResourceLimit,
                ErrorPhase::Read,
                RemoteEffect::None,
                RetryDisposition::Never,
                "conteggio batch Arrow oltre u64",
            )
        })?;
        rows = rows
            .checked_add(u64::try_from(batch.num_rows()).map_err(|_| {
                local_artifact_error(
                    ErrorCategory::ResourceLimit,
                    ErrorPhase::Read,
                    RemoteEffect::None,
                    RetryDisposition::Never,
                    "conteggio righe Arrow oltre u64",
                )
            })?)
            .ok_or_else(|| {
                local_artifact_error(
                    ErrorCategory::ResourceLimit,
                    ErrorPhase::Read,
                    RemoteEffect::None,
                    RetryDisposition::Never,
                    "conteggio righe Arrow oltre u64",
                )
            })?;
        writer.write(&batch).map_err(|_| {
            local_artifact_error(
                ErrorCategory::Io,
                ErrorPhase::Write,
                RemoteEffect::None,
                RetryDisposition::Never,
                "RecordBatch Arrow IPC non scrivibile",
            )
        })?;
    }
    writer.finish().map_err(|_| {
        local_artifact_error(
            ErrorCategory::Io,
            ErrorPhase::Finalize,
            RemoteEffect::None,
            RetryDisposition::Never,
            "Arrow IPC non finalizzabile",
        )
    })?;
    drop(writer);
    file.sync_all().map_err(|_| {
        local_artifact_error(
            ErrorCategory::Io,
            ErrorPhase::Finalize,
            RemoteEffect::None,
            RetryDisposition::Never,
            "Arrow IPC non sincronizzabile",
        )
    })?;
    Ok((batches, rows))
}

#[cfg(feature = "postgres")]
fn local_artifact_error(
    category: ErrorCategory,
    phase: ErrorPhase,
    remote_effect: RemoteEffect,
    retry: RetryDisposition,
    message: impl Into<String>,
) -> CliError {
    CliError::Fatal(DatabaseError {
        category,
        phase,
        remote_effect,
        retry,
        provider: None,
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    })
}

#[cfg(feature = "postgres")]
const fn object_ref(schema: String, object: String) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some(schema),
        object,
    }
}

pub(crate) fn secret_from_env(name: &str) -> CliResult<SecretString> {
    if name.is_empty() || name.contains('=') {
        return Err("nome variabile DSN non valido".into());
    }
    Ok(env::var(name)
        .map(SecretString::new)
        .map_err(|_| "variabile DSN assente".to_owned())?)
}

fn parse_provider_kind(value: &str) -> CliResult<ProviderKind> {
    match value {
        "postgres" => Ok(ProviderKind::Postgres),
        "mysql" => Ok(ProviderKind::Mysql),
        "mariadb" => Ok(ProviderKind::Mariadb),
        "sqlserver" => Ok(ProviderKind::Sqlserver),
        "oracle" => Ok(ProviderKind::Oracle),
        "db2" => Ok(ProviderKind::Db2),
        "sqlite" => Ok(ProviderKind::Sqlite),
        "duckdb" => Ok(ProviderKind::Duckdb),
        _ => Err("provider sconosciuto".into()),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TlsPathEnvironments {
    ca: Option<String>,
    client_certificate: Option<String>,
    client_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProviderArguments {
    #[cfg(feature = "postgres")]
    Postgres { tls: TlsPathEnvironments },
    #[cfg(feature = "mysql")]
    Mysql {
        host: String,
        database: String,
        username: String,
        port: Option<u16>,
        tls: TlsPathEnvironments,
    },
    #[cfg(feature = "sqlserver")]
    Sqlserver {
        host: String,
        database: String,
        username: String,
        port: Option<u16>,
        tls: TlsPathEnvironments,
    },
}

enum PreparedProviderArguments {
    #[cfg(feature = "postgres")]
    Postgres(PostgresProvider),
    #[cfg(feature = "mysql")]
    Mysql(MysqlConfig),
    #[cfg(feature = "sqlserver")]
    Sqlserver(SqlServerConfig),
}

fn parse_tls_path_environments(
    args: &mut impl Iterator<Item = String>,
) -> CliResult<TlsPathEnvironments> {
    let mut tls = TlsPathEnvironments::default();
    while let Some(flag) = args.next() {
        let target = match flag.as_str() {
            "--tls-ca-path-env" => &mut tls.ca,
            "--tls-client-cert-path-env" => &mut tls.client_certificate,
            "--tls-client-key-path-env" => &mut tls.client_key,
            _ if flag.starts_with("--") => return Err("opzione TLS provider sconosciuta".into()),
            _ => return Err("troppi argomenti".into()),
        };
        let value = args
            .next()
            .ok_or_else(|| format!("manca il nome variabile ambiente per {flag}"))?;
        if value.is_empty() || value.contains('=') {
            return Err("nome variabile ambiente TLS non valido".into());
        }
        if target.replace(value).is_some() {
            return Err("opzione TLS provider duplicata".into());
        }
    }
    match (&tls.client_certificate, &tls.client_key, &tls.ca) {
        (None, None, _) | (Some(_), Some(_), Some(_)) => Ok(tls),
        (Some(_), Some(_), None) => Err("identità client TLS richiede una CA privata".into()),
        _ => Err("certificato e chiave client TLS devono essere forniti insieme".into()),
    }
}

fn parse_provider_arguments(
    kind: ProviderKind,
    args: &mut impl Iterator<Item = String>,
) -> CliResult<ProviderArguments> {
    match kind {
        #[cfg(feature = "postgres")]
        ProviderKind::Postgres => {
            let tls = parse_tls_path_environments(args)?;
            Ok(ProviderArguments::Postgres { tls })
        }
        #[cfg(any(feature = "mysql", feature = "sqlserver"))]
        ProviderKind::Mysql | ProviderKind::Sqlserver => {
            let host = args
                .next()
                .ok_or_else(|| "manca host provider".to_owned())?;
            let database = args
                .next()
                .ok_or_else(|| "manca database provider".to_owned())?;
            let username = args
                .next()
                .ok_or_else(|| "manca username provider".to_owned())?;
            let mut remaining = args.collect::<Vec<_>>().into_iter().peekable();
            let port = remaining
                .next_if(|value| !value.starts_with("--"))
                .map(|value| {
                    value
                        .parse::<u16>()
                        .ok()
                        .filter(|port| *port > 0)
                        .ok_or_else(|| "porta provider non valida".to_owned())
                })
                .transpose()?;
            let tls = parse_tls_path_environments(&mut remaining)?;
            if tls.client_certificate.is_some() || tls.client_key.is_some() {
                return Err("identità client TLS supportata solo per PostgreSQL".into());
            }
            match kind {
                #[cfg(feature = "mysql")]
                ProviderKind::Mysql => Ok(ProviderArguments::Mysql {
                    host,
                    database,
                    username,
                    port,
                    tls,
                }),
                #[cfg(feature = "sqlserver")]
                ProviderKind::Sqlserver => Ok(ProviderArguments::Sqlserver {
                    host,
                    database,
                    username,
                    port,
                    tls,
                }),
                // Copre i rami disabilitati a feature-time e qualsiasi altra
                // variante futura non prevista: errore controllato invece di
                // panic.
                _ => Err(CliError::Fatal(DatabaseError::unsupported(
                    kind,
                    ErrorPhase::Prepare,
                    "provider dichiarato dal contratto ma adapter non disponibile \
                     (ricompilare con la feature di quel provider: --features mysql, \n                     --features sqlserver, oppure --features full per tutti)",
                ))),
            }
        }
        unsupported_kind => Err(CliError::Fatal(DatabaseError::unsupported(
            unsupported_kind,
            ErrorPhase::Prepare,
            "provider dichiarato dal contratto ma adapter non disponibile \
             (ricompilare con la feature di quel provider: --features mysql, \n                     --features sqlserver, oppure --features full per tutti)",
        ))),
    }
}

#[cfg(test)]
fn build_provider(
    kind: ProviderKind,
    secret: &SecretString,
    args: &mut impl Iterator<Item = String>,
) -> CliResult<Box<dyn Provider>> {
    let provider_arguments = parse_provider_arguments(kind, args)?;
    let provider_arguments = prepare_provider_arguments(provider_arguments)?;
    build_provider_from_prepared_arguments(provider_arguments, secret)
}

fn prepare_provider_arguments(
    arguments: ProviderArguments,
) -> CliResult<PreparedProviderArguments> {
    match arguments {
        #[cfg(feature = "postgres")]
        ProviderArguments::Postgres { tls } => Ok(PreparedProviderArguments::Postgres(
            postgres_provider_for_probe_with_tls(&tls)?,
        )),
        #[cfg(feature = "mysql")]
        ProviderArguments::Mysql {
            host,
            database,
            username,
            port,
            tls,
        } => {
            let mut config = MysqlConfig::new(host, database, username, SecretString::new(""));
            if let Some(port) = port {
                config = config.with_port(port);
            }
            if let Some(pem) = prepare_private_ca_material(tls.ca.as_deref())? {
                config = config.with_private_ca_certificate_pem(pem);
            }
            config.validate_without_password()?;
            Ok(PreparedProviderArguments::Mysql(config))
        }
        #[cfg(feature = "sqlserver")]
        ProviderArguments::Sqlserver {
            host,
            database,
            username,
            port,
            tls,
        } => {
            let mut config = SqlServerConfig::new(host, database, username, SecretString::new(""));
            if let Some(port) = port {
                config = config.with_port(port);
            }
            if let Some(pem) = prepare_private_ca_material(tls.ca.as_deref())? {
                validate_sqlserver_private_ca_material(&pem)?;
                config = config.with_private_ca_certificate_pem(&pem)?;
            }
            config.validate_without_password()?;
            Ok(PreparedProviderArguments::Sqlserver(config))
        }
    }
}

// Il tipo di ritorno è Result perché il ramo MySQL/SQL Server può fallire
// nella costruzione del provider; con solo Postgres attivo il match è
// infallibile ma la firma resta la stessa per uniformità.
#[allow(clippy::unnecessary_wraps)]
fn build_provider_from_prepared_arguments(
    arguments: PreparedProviderArguments,
    #[cfg_attr(
        not(any(feature = "mysql", feature = "sqlserver")),
        allow(unused_variables)
    )]
    secret: &SecretString,
) -> CliResult<Box<dyn Provider>> {
    match arguments {
        #[cfg(feature = "postgres")]
        PreparedProviderArguments::Postgres(provider) => Ok(Box::new(provider)),
        #[cfg(feature = "mysql")]
        PreparedProviderArguments::Mysql(config) => Ok(Box::new(MysqlProvider::new(
            config.with_password(secret.clone()),
            8,
        )?)),
        #[cfg(feature = "sqlserver")]
        PreparedProviderArguments::Sqlserver(config) => Ok(Box::new(SqlServerProvider::new(
            config.with_password(secret.clone()),
            1_024,
            8,
        )?)),
    }
}

fn tls_path_from_environment(env_name: Option<&str>) -> CliResult<Option<PathBuf>> {
    env_name
        .map(|name| {
            env::var_os(name)
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .ok_or_else(|| CliError::from("variabile path TLS assente"))
        })
        .transpose()
}

fn read_bounded_tls_material(path: &Path) -> CliResult<Vec<u8>> {
    let file = File::open(path).map_err(|_| CliError::from("materiale TLS non leggibile"))?;
    let metadata = file
        .metadata()
        .map_err(|_| CliError::from("materiale TLS non leggibile"))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > TLS_MATERIAL_MAX_BYTES {
        return Err("materiale TLS vuoto o oltre 1 MiB".into());
    }
    let capacity = usize::try_from(metadata.len())
        .map_err(|_| CliError::from("materiale TLS oltre i limiti della piattaforma"))?;
    let mut material = Vec::with_capacity(capacity);
    file.take(TLS_MATERIAL_MAX_BYTES + 1)
        .read_to_end(&mut material)
        .map_err(|_| CliError::from("materiale TLS non leggibile"))?;
    if material.is_empty() || material.len() as u64 > TLS_MATERIAL_MAX_BYTES {
        return Err("materiale TLS vuoto o oltre 1 MiB".into());
    }
    Ok(material)
}

/// Percorso TLS **opzionale**: `None` quando la variabile non e impostata.
///
/// Diverso da [`tls_path_from_environment`], che serve `database-probe`: li il
/// nome della variabile arriva da un argomento della riga di comando, quindi
/// averlo indicato e non averla impostata e un errore del chiamante. Per i
/// sottocomandi le variabili sono opzionali e la loro assenza e la
/// configurazione di default, non un difetto.
#[cfg(feature = "postgres")]
fn optional_tls_path(env_name: &str) -> Option<PathBuf> {
    env::var_os(env_name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// Materiale TLS opzionale, letto dal percorso in `env_name`.
///
/// # Errors
///
/// Fallisce quando la variabile e impostata ma il percorso non e leggibile o
/// eccede il limite di dimensione del materiale TLS.
#[cfg(feature = "postgres")]
pub(crate) fn optional_tls_material(env_name: &str) -> CliResult<Option<Vec<u8>>> {
    let Some(path) = optional_tls_path(env_name) else {
        return Ok(None);
    };
    Ok(Some(read_bounded_tls_material(&path)?))
}

/// CA privata opzionale, letta e validata dal percorso in `env_name`.
///
/// # Errors
///
/// Fallisce quando la variabile e impostata ma il materiale non e una CA
/// valida.
#[cfg(feature = "postgres")]
pub(crate) fn optional_private_ca_material(env_name: &str) -> CliResult<Option<Vec<u8>>> {
    let Some(path) = optional_tls_path(env_name) else {
        return Ok(None);
    };
    let material = read_bounded_tls_material(&path)?;
    Ok(Some(validate_and_normalize_private_ca_material(
        &path, &material,
    )?))
}

fn prepare_private_ca_material(env_name: Option<&str>) -> CliResult<Option<Vec<u8>>> {
    let Some(path) = tls_path_from_environment(env_name)? else {
        return Ok(None);
    };
    let material = read_bounded_tls_material(&path)?;
    Ok(Some(validate_and_normalize_private_ca_material(
        &path, &material,
    )?))
}

fn validate_and_normalize_private_ca_material(path: &Path, material: &[u8]) -> CliResult<Vec<u8>> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    if !matches!(extension.as_deref(), Some("pem" | "crt" | "der")) {
        return Err("estensione CA TLS non supportata".into());
    }
    let first_non_whitespace = material
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(material.len());
    let trimmed = &material[first_non_whitespace..];
    let certificates: Vec<CertificateDer<'static>> = if trimmed.starts_with(b"-----BEGIN") {
        rustls_pemfile::certs(&mut Cursor::new(trimmed))
            .collect::<Result<_, _>>()
            .map_err(|_| CliError::from("materiale CA TLS non valido"))?
    } else {
        vec![CertificateDer::from(material.to_vec())]
    };
    if certificates.is_empty() {
        return Err("materiale CA TLS non valido".into());
    }
    let mut roots = RootCertStore::empty();
    for certificate in &certificates {
        roots
            .add(certificate.clone())
            .map_err(|_| CliError::from("materiale CA TLS non valido"))?;
    }
    let mut normalized = Vec::new();
    for certificate in certificates {
        normalized.extend_from_slice(b"-----BEGIN CERTIFICATE-----\n");
        let encoded = BASE64_STANDARD.encode(certificate.as_ref());
        for line in encoded.as_bytes().chunks(64) {
            normalized.extend_from_slice(line);
            normalized.push(b'\n');
        }
        normalized.extend_from_slice(b"-----END CERTIFICATE-----\n");
    }
    if normalized.len() as u64 > TLS_MATERIAL_MAX_BYTES {
        return Err("materiale TLS vuoto o oltre 1 MiB".into());
    }
    Ok(normalized)
}

#[cfg(feature = "sqlserver")]
fn validate_sqlserver_private_ca_material(pem: &[u8]) -> CliResult<()> {
    let certificates = rustls_pemfile::certs(&mut Cursor::new(pem))
        .take(2)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| CliError::from("materiale CA TLS non valido"))?;
    if certificates.len() != 1 {
        return Err(
            "configurazione SQL Server: CA privata deve contenere esattamente un certificato"
                .into(),
        );
    }
    Ok(())
}

#[cfg(feature = "postgres")]
fn postgres_provider_for_probe_with_tls(tls: &TlsPathEnvironments) -> CliResult<PostgresProvider> {
    let Some(ca) = prepare_private_ca_material(tls.ca.as_deref())? else {
        return Ok(postgres_provider_for_probe());
    };
    let tls_config = match (
        tls_path_from_environment(tls.client_certificate.as_deref())?,
        tls_path_from_environment(tls.client_key.as_deref())?,
    ) {
        (None, None) => PostgresTlsConfig::private_ca_pem(&ca)?,
        (Some(certificate_path), Some(key_path)) => {
            let certificate = read_bounded_tls_material(&certificate_path)?;
            let key = read_bounded_tls_material(&key_path)?;
            PostgresTlsConfig::private_ca_with_client_identity_pem(&ca, &certificate, &key)?
        }
        // Il parser accetta certificato e chiave solo insieme, quindi qui non
        // si arriva. La coppia di `Option` pero' non lo dice al compilatore, e
        // un errore e' preferibile al processo abbattuto se quell'invariante
        // dovesse cambiare.
        _ => {
            return Err("l'identità client TLS richiede certificato e chiave insieme".into());
        }
    };
    Ok(PostgresProvider::default()
        .with_tls_mode(PostgresTlsMode::Require)
        .with_tls_config(tls_config))
}

#[cfg(feature = "postgres")]
fn postgres_provider_for_probe() -> PostgresProvider {
    PostgresProvider::default().with_tls_mode(PostgresTlsMode::Require)
}

#[cfg(test)]
#[cfg(feature = "postgres")]
fn legacy_postgres_probe_provider() -> PostgresProvider {
    postgres_provider_for_probe()
}

pub(crate) fn one_argument(
    args: &mut impl Iterator<Item = String>,
    missing: &str,
) -> CliResult<String> {
    let value = args.next().ok_or_else(|| missing.to_owned())?;
    ensure_end(args)?;
    Ok(value)
}

pub(crate) fn ensure_end(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    if args.next().is_some() {
        Err("troppi argomenti".into())
    } else {
        Ok(())
    }
}

pub(crate) fn print_json(value: &serde_json::Value) -> CliResult<()> {
    format::print_active(value)
}

#[allow(clippy::too_many_lines)]
/// Il catalogo dei comandi: nome e feature che lo porta, `None` se c'e sempre.
///
/// E la sola fonte di cosa il binario sa fare. `usage` mostra le sezioni delle
/// feature compilate, l'errore da comando sconosciuto distingue "non esiste"
/// da "non compilato qui", e un test lo confronta con i rami del dispatch.
/// Senza, le tre cose derivano — ed erano gia derivate: l'aiuto elencava i
/// comandi `PostgreSQL` anche in un binario che non li aveva compilati, e
/// dichiarava che `MySQL` richiedesse `--features full` quando
/// `--features mysql` basta.
const COMMAND_CATALOGUE: &[(&str, Option<&str>)] = &[
    ("database-probe", None),
    ("inspect-dataset", None),
    ("validate-plan", None),
    ("benchmark-oltp", Some("postgres")),
    ("benchmark-read", Some("postgres")),
    ("benchmark-spatial", Some("postgres")),
    ("benchmark-write", Some("postgres")),
    ("bulk-write", Some("postgres")),
    ("conditional-update", Some("postgres")),
    ("diagnose", Some("postgres")),
    ("doctor", Some("postgres")),
    ("execute-ddl", Some("postgres")),
    ("execute-scalar", Some("postgres")),
    ("execute-sql", Some("postgres")),
    ("explain", Some("postgres")),
    ("inspect-catalogs", Some("postgres")),
    ("inspect-database", Some("postgres")),
    ("inspect-objects", Some("postgres")),
    ("inspect-schemas", Some("postgres")),
    ("inspect-tables", Some("postgres")),
    ("pool-status", Some("postgres")),
    ("portable-compile", Some("postgres")),
    ("portable-execute", Some("postgres")),
    ("postgres-describe", Some("postgres")),
    ("postgres-probe", Some("postgres")),
    ("postgres-query", Some("postgres")),
    ("postgres-read-ipc", Some("postgres")),
    ("postgres-read-summary", Some("postgres")),
    ("postgres-write-ipc", Some("postgres")),
    ("profile-check", Some("postgres")),
    ("profile-list", Some("postgres")),
    ("session-context-test", Some("postgres")),
    ("test-cancellation", Some("postgres")),
    ("test-concurrency", Some("postgres")),
    ("test-spatial", Some("postgres")),
    ("test-streaming", Some("postgres")),
    ("transaction-test", Some("postgres")),
    ("mysql-conditional-update", Some("mysql")),
    ("mysql-describe", Some("mysql")),
    ("mysql-execute-ddl", Some("mysql")),
    ("mysql-execute-scalar", Some("mysql")),
    ("mysql-execute-sql", Some("mysql")),
    ("mysql-inspect-schemas", Some("mysql")),
    ("mysql-inspect-tables", Some("mysql")),
    ("mysql-probe", Some("mysql")),
    ("mysql-transaction-test", Some("mysql")),
];

/// Se la feature indicata e stata compilata in **questo** binario.
///
/// Serve alla guardia, non al binario: il catalogo lo consuma
/// `unknown_command`, mentre la vista filtrata esiste per confrontare cio che
/// il dispatch compila con cio che l'aiuto elenca.
#[cfg(test)]
fn feature_is_compiled(feature: &str) -> bool {
    // Una tabella, non un predicato: `cfg!` va valutato per **ogni** nome,
    // altrimenti la riga della feature assente sparirebbe insieme al ramo.
    [
        ("postgres", cfg!(feature = "postgres")),
        ("mysql", cfg!(feature = "mysql")),
        ("sqlserver", cfg!(feature = "sqlserver")),
    ]
    .into_iter()
    .any(|(name, compiled)| name == feature && compiled)
}

/// I comandi che questo binario espone davvero.
#[cfg(test)]
fn compiled_commands() -> Vec<&'static str> {
    COMMAND_CATALOGUE
        .iter()
        .filter(|(_, feature)| feature.is_none_or(feature_is_compiled))
        .map(|(name, _)| *name)
        .collect()
}

/// L'errore per un comando che il dispatch non riconosce.
///
/// Un comando che esiste nel progetto ma non e in questo binario merita una
/// risposta diversa da uno che non esiste: la prima si risolve ricostruendo,
/// la seconda no. Prima erano la stessa cosa — l'intero testo di aiuto — e
/// quel testo elencava comandi che il binario non aveva.
fn unknown_command(command: &str) -> CliError {
    if let Some((_, Some(feature))) = COMMAND_CATALOGUE.iter().find(|(name, _)| *name == command) {
        return CliError::from(format!(
            "comando '{command}' non compilato in questo binario: ricostruire \
             con --features {feature}\n\n{}",
            usage()
        ));
    }
    CliError::from(usage())
}

/// I provider che `database-probe` puo davvero istanziare qui.
///
/// Il contratto ne enumera nove; questo binario ne porta quelli compilati. La
/// riga di aiuto diceva "mysql/sqlserver richiedono --features full", falso in
/// due modi: `--features mysql` basta, e in un binario senza `PostgreSQL`
/// nemmeno `postgres` e disponibile.
fn compiled_providers() -> Vec<&'static str> {
    let mut names = Vec::new();
    if cfg!(feature = "postgres") {
        names.push("postgres");
    }
    if cfg!(feature = "mysql") {
        names.push("mysql");
    }
    if cfg!(feature = "sqlserver") {
        names.push("sqlserver");
    }
    names
}

fn usage() -> String {
    #[allow(unused_mut)]
    let mut sections = vec![common_usage()];
    #[cfg(feature = "postgres")]
    sections.push(postgres_usage());
    #[cfg(feature = "mysql")]
    sections.push(mysql_usage());
    #[cfg(feature = "sqlserver")]
    sections.push(sqlserver_usage());
    sections.push(global_flags_usage());
    sections.join("\n")
}

/// Cio che c'e in qualunque binario, comunque sia stato costruito.
fn common_usage() -> String {
    let providers = compiled_providers();
    let compiled = if providers.is_empty() {
        "nessuno: questo binario non ha adapter compilati".to_owned()
    } else {
        providers.join(" | ")
    };
    [
        "uso: plenora-database [flag-globali] COMANDO [args...]".to_owned(),
        String::new(),
        "== sempre disponibili ==".to_owned(),
        "  database-probe <provider> <secret-env> [args tls]".to_owned(),
        format!("    provider compilati in questo binario: {compiled}"),
        "  inspect-dataset <file.arrow>".to_owned(),
        "  validate-plan <file.json> [--capabilities <file.json>]".to_owned(),
    ]
    .join("\n")
}

#[cfg(feature = "postgres")]
fn postgres_usage() -> String {
    [
        "",
        "== PostgreSQL: diagnostica ==",
        "  postgres-probe <dsn-env> [--tls-ca-path-env NAME] [--tls-client-cert-path-env NAME \
         --tls-client-key-path-env NAME]",
        "    verifica connessione + capabilities",
        "  doctor <dsn-env>",
        "    aggregato: connessione + capabilities + i 3 profili conformance",
        "  diagnose <dsn-env>",
        "    superset di doctor: connect_ms, config server, findings + suggerimenti",
        "  pool-status <dsn-env>",
        "    stato acquisizione connessione dal pool (acquire_ms + connection identity)",
        "  profile-list",
        "  profile-check <dsn-env> <APPLICATION_OLTP_V1|PFM_CORE_V1|PFM_GIS_V1>",
        "",
        "== PostgreSQL: inspection (metadata) ==",
        "  inspect-database <dsn-env>          — version/encoding/timezone/size/extensions",
        "  inspect-catalogs <dsn-env>          — via Provider::inspect (raw)",
        "  inspect-schemas <dsn-env>           — schemi utente (filtrati)",
        "  inspect-objects <dsn-env> <schema>  — via Provider::inspect (raw)",
        "  inspect-tables <dsn-env> <schema>   — tabelle/view/mv con rowcount + size",
        "  postgres-describe <dsn-env> <schema> <object>",
        "    metadata dettagliati oggetto (columns/constraints/indexes/policies/privileges)",
        "",
        "== PostgreSQL: read (Arrow bulk) ==",
        "  postgres-read-summary <dsn-env> <schema> <object>",
        "    schema Arrow + rowcount, senza materializzare",
        "  postgres-read-ipc <dsn-env> <schema> <object> <output.arrow> [opzioni]",
        "    opzioni: --project COL1,COL2 --filter FILTER.json --limit N --order-by FIELD",
        "             --parameter NAME=VALUE:TYPE --max-rows N --max-output-bytes N --timeout-ms N",
        "  postgres-query <dsn-env> <QUERY.json>",
        "    esegue QueryOperation AST via Provider::query, summary schema+rows",
        "",
        "== PostgreSQL: write (Arrow bulk + DML) ==",
        "  bulk-write <dsn-env> <WRITE_OP.json> <INPUT.arrow> [--dry-run]",
        "    esegue WriteOperation plan-based (create/append/replace/upsert/…) su input Arrow IPC",
        "  postgres-write-ipc <dsn-env> <schema> <object> <INPUT.arrow> [--mode X] [--keys K1,K2] \
         [--update-columns C1,C2] [--dry-run]",
        "    wrapper high-level di bulk-write; --mode: create|append|replace|truncate-insert|update|upsert|delete-by-keys (default append)",
        "  execute-ddl <dsn-env> <sql>",
        "    DDL fuori tx (CREATE INDEX CONCURRENTLY, VACUUM, ecc.)",
        "  execute-sql <dsn-env> <sql> [--param VALUE:TYPE ...]",
        "    esegue in una tx; SELECT/WITH/VALUES/TABLE/SHOW → rows JSON, altrimenti affected_rows",
        "    --param NAME=VALUE:TYPE (con NAME opzionale) per bind position ($1, $2, ...)",
        "    tipi: bool|int|bigint|float|string|uuid|json|date|timestamp|timestamptz|bytes-hex|null:<sub>",
        "  execute-scalar <dsn-env> <sql> --type=TYPE [--param VALUE:TYPE ...]",
        "    one-shot lettura di 1 riga × 1 colonna (bool|i32|i64|f64|string|uuid|json|bytes|date|timestamp|timestamptz)",
        "  conditional-update <dsn-env> <UPDATE_SQL> <PROBE_SQL> <EXPECTED_AFFECTED> [--param VALUE:TYPE ...]",
        "    verifica optimistic concurrency: se affected != expected, PROBE distingue NotFound da ConcurrentModification",
        "  explain <dsn-env> <sql> [--analyze] [--verbose] [--format=text|json|yaml|xml] [--param ...]",
        "    wrapper EXPLAIN [ANALYZE]",
        "",
        "== PostgreSQL: portable AST ==",
        "  portable-compile <postgres|mysql|sqlserver> <PORTABLE.json>",
        "    stampa SQL + numero parametri compilati (per debug pipeline PFM)",
        "  portable-execute <dsn-env> <PORTABLE.json>",
        "    compila per Postgres, esegue in una tx, ritorna rows o affected_rows",
        "",
        "== PostgreSQL: transazioni / concorrenza (test) ==",
        "  transaction-test <dsn-env>              — smoke: begin + savepoint + release + commit",
        "  session-context-test <dsn-env>          — isolamento context su pool reuse",
        "  test-cancellation <dsn-env>             — statement_timeout → Cancelled",
        "  test-streaming <dsn-env> [rows] [batch] — cursor server-side",
        "  test-spatial <dsn-env>                  — portable spatial AST end-to-end (PostGIS)",
        "  test-concurrency <dsn-env>              — 2 tx competono (richiede --allow-write-tests)",
        "",
        "== PostgreSQL: benchmark ==",
        "  benchmark-oltp <dsn-env> [iterations=100]",
        "  benchmark-read <dsn-env> <sql> [iterations=100]",
        "  benchmark-write <dsn-env> [iterations=200] [batch_size=10]  (--allow-write-tests)",
        "  benchmark-spatial <dsn-env> [iterations=50]",
    ]
    .join("\n")
}

#[cfg(feature = "mysql")]
fn mysql_usage() -> String {
    [
        "",
        "== MySQL (v1.2, subset iniziale) ==",
        "  args comuni: <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>]",
        "  mysql-probe <args...>              — test_connection + probe_capabilities",
        "  mysql-describe <args...> <schema> <object>  — describe target (colonne, tipi, keys)",
        "  mysql-inspect-schemas <args...>    — list schemas",
        "  mysql-inspect-tables <args...> <schema>  — list objects in schema",
        "  mysql-execute-sql <args...> <sql>  — DML raw in una tx (INSERT/UPDATE/DELETE)",
        "  mysql-execute-ddl <args...> <sql>  — DDL raw (CREATE/DROP/ALTER, autocommit MySQL)",
        "  mysql-execute-scalar <args...> <sql>  — SELECT scalare (1 riga × 1 colonna)",
        "  mysql-transaction-test <args...>   — smoke OLTP: begin + savepoint + rollback_to + commit",
        "  mysql-conditional-update <args...> <UPDATE_SQL> <EXPECTED_AFFECTED>  — pattern optimistic-lock",
        "  nota: MariaDB non e qualificata — la probe la riconosce e la rifiuta (ADR 0014)",
    ]
    .join("\n")
}

#[cfg(feature = "sqlserver")]
fn sqlserver_usage() -> String {
    [
        "",
        "== SQL Server ==",
        "  nessun sotto-comando dedicato: l'adapter si raggiunge da",
        "  `database-probe sqlserver <secret-env>`",
    ]
    .join("\n")
}

fn global_flags_usage() -> String {
    [
        "",
        "== flag globali (accettati in qualsiasi posizione) ==",
        "  --format json|markdown|junit         formato di output (default: json)",
        "  --allow-write-tests                  gate esplicito per test che creano oggetti sul DB",
        "  --ephemeral-schema NAME              crea/droppa schema NAME attorno ai test destructive",
        "  --session-context KEY=VALUE:TYPE     imposta setting session (namespaced), multipli ok",
    ]
    .join("\n")
}

/// Comandi compilati, comandi documentati e rami del dispatch devono
/// coincidere — nella configurazione con cui il binario e stato costruito.
///
/// La guardia sta qui e non in uno script Python perche e l'unica che puo
/// vedere le `cfg`: da fuori il sorgente mostra tutti i rami, e un binario
/// `MySQL`-only che elenca i comandi `PostgreSQL` sembrerebbe corretto. E
/// quello che faceva.
#[cfg(test)]
mod usage_surface_tests {
    use super::{compiled_commands, unknown_command, usage, COMMAND_CATALOGUE};

    /// I nomi dei rami del `match` del dispatch, con la feature che li porta.
    ///
    /// Letti dal sorgente: un `match` non si enumera a runtime, e la
    /// alternativa — una tabella di puntatori a funzione — sposterebbe il
    /// problema senza risolverlo, perche resterebbe da provare che la tabella
    /// e il `match` dicano la stessa cosa.
    fn dispatch_arms() -> Vec<(String, Option<String>)> {
        let source = include_str!("main.rs");
        let start = source
            .find("let command = args.next()")
            .expect("inizio del dispatch");
        let end = source[start..]
            .find("_ => Err(unknown_command(")
            .expect("fine del dispatch")
            + start;
        let mut pending: Option<String> = None;
        let mut arms = Vec::new();
        for line in source[start..end].lines() {
            let trimmed = line.trim();
            if let Some(rest) = trimmed.strip_prefix("#[cfg(feature = \"") {
                if let Some(feature) = rest.split('"').next() {
                    pending = Some(feature.to_owned());
                }
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix('"') {
                if let Some(name) = rest.split('"').next() {
                    if rest[name.len()..]
                        .trim_start_matches('"')
                        .trim_start()
                        .starts_with("=>")
                    {
                        arms.push((name.to_owned(), pending.take()));
                        continue;
                    }
                }
            }
            pending = None;
        }
        arms
    }

    #[test]
    fn the_catalogue_matches_the_dispatch() {
        let mut from_dispatch: Vec<(String, Option<String>)> = dispatch_arms();
        from_dispatch.sort();
        assert!(
            from_dispatch.len() >= 3,
            "dispatch non riconosciuto: {from_dispatch:?}"
        );
        let mut from_catalogue: Vec<(String, Option<String>)> = COMMAND_CATALOGUE
            .iter()
            .map(|(name, feature)| {
                (
                    (*name).to_owned(),
                    feature.map(std::string::ToString::to_string),
                )
            })
            .collect();
        from_catalogue.sort();
        assert_eq!(
            from_dispatch, from_catalogue,
            "il catalogo e il dispatch non elencano gli stessi comandi"
        );
    }

    #[test]
    fn usage_lists_every_compiled_command_and_only_those() {
        let text = usage();
        for name in compiled_commands() {
            assert!(
                documents(&text, name),
                "comando compilato e non documentato: {name}"
            );
        }
        let compiled = compiled_commands();
        for (name, _) in COMMAND_CATALOGUE {
            if compiled.contains(name) {
                continue;
            }
            assert!(
                !documents(&text, name),
                "l'aiuto elenca {name}, che questo binario non ha compilato"
            );
        }
    }

    /// Se l'aiuto presenta `name` **come comando**.
    ///
    /// Il confronto e per riga e non per sottostringa: `execute-sql` compare
    /// dentro `mysql-execute-sql`, quindi un `contains` direbbe che un binario
    /// `MySQL`-only documenta i comandi `PostgreSQL`.
    fn documents(text: &str, name: &str) -> bool {
        text.lines().any(|line| {
            line.strip_prefix("  ")
                .and_then(|rest| rest.strip_prefix(name))
                .is_some_and(|rest| rest.is_empty() || rest.starts_with(' '))
        })
    }

    #[test]
    fn usage_declares_only_the_providers_this_binary_can_build() {
        let text = usage();
        let line = text
            .lines()
            .find(|line| line.contains("provider compilati in questo binario"))
            .expect("riga dei provider");
        for (feature, name) in [
            (cfg!(feature = "postgres"), "postgres"),
            (cfg!(feature = "mysql"), "mysql"),
            (cfg!(feature = "sqlserver"), "sqlserver"),
        ] {
            assert_eq!(
                line.contains(name),
                feature,
                "la riga dei provider non riflette le feature: {line}"
            );
        }
        // L'affermazione che ha reso necessaria questa guardia.
        assert!(!text.contains("--features full"));
    }

    #[test]
    fn an_uncompiled_command_is_told_apart_from_one_that_does_not_exist() {
        let missing = COMMAND_CATALOGUE
            .iter()
            .find(|(name, feature)| feature.is_some() && !compiled_commands().contains(name))
            .map(|(name, _)| *name);
        if let Some(name) = missing {
            let message = format!("{:?}", unknown_command(name));
            assert!(
                message.contains("non compilato in questo binario"),
                "comando non compilato trattato come inesistente: {message}"
            );
        }
        let message = format!("{:?}", unknown_command("comando-che-non-esiste"));
        assert!(
            !message.contains("non compilato in questo binario"),
            "un comando inesistente non va spacciato per non compilato"
        );
    }
}

// ============================================================================
//  PFM subcommands: espongono le API application plane / conformance / DDL
//  di Fase A/B/C/F1/P1/P2 come tool CLI. Tutti prendono il DSN Postgres via
//  variabile ambiente (mai in CLI argument per non finire in shell history).
// ============================================================================

#[cfg(feature = "postgres")]
mod benchmark;
#[cfg(feature = "postgres")]
mod context;
#[cfg(feature = "postgres")]
mod diagnose;
// Emette SQL PostgreSQL attraverso il provider PFM: sta dietro la feature,
// mentre il parsing dei flag che lo governano resta in `safety`, neutro.
#[cfg(feature = "postgres")]
mod ephemeral_schema;
mod format;
#[cfg(feature = "postgres")]
mod inspect;
mod inspect_dataset;
mod mysql_cmd;
#[cfg(feature = "postgres")]
mod ops_cmd;
#[cfg(feature = "postgres")]
mod pfm;
#[cfg(feature = "postgres")]
mod query_cmd;
mod safety;
#[cfg(feature = "postgres")]
mod session_ctx;
#[cfg(feature = "postgres")]
mod testing;
// Provider-neutral: legge `NAME=VALUE:TYPE` e produce `ParameterValue` del
// core. Non conosce nessun provider, e infatti `postgres-read-ipc` non era
// l'unico a usarlo — dietro la feature `postgres` rendeva il binario
// MySQL-only non compilabile.
mod typed_params;
#[cfg(feature = "postgres")]
mod write_cmd;

#[cfg(feature = "postgres")]
use benchmark::{benchmark_oltp, benchmark_read, benchmark_spatial};
#[cfg(feature = "postgres")]
use pfm::{
    doctor, execute_ddl_cmd, execute_sql_cmd, profile_check, session_context_test, transaction_test,
};
#[cfg(feature = "postgres")]
use testing::{profile_list, test_cancellation, test_concurrency, test_spatial, test_streaming};

#[cfg(test)]
mod tests {
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
    fn crs_error_envelope_matches_protocol_v1() {
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
                "protocol_version": 1,
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
        assert_eq!(value["protocol_version"], 1);
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
            #[allow(unused_mut)]
            let mut m: Vec<(ProviderKind, Vec<&str>)> = vec![(ProviderKind::Postgres, Vec::new())];
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
        let mut postgres_args = std::iter::empty();
        assert_eq!(
            build_provider(ProviderKind::Postgres, &secret, &mut postgres_args)
                .expect("PostgreSQL provider")
                .kind(),
            ProviderKind::Postgres
        );

        #[allow(clippy::vec_init_then_push)] // le push sono cfg-gated
        let structured: Vec<ProviderKind> = {
            #[allow(unused_mut)]
            let mut v: Vec<ProviderKind> = Vec::new();
            #[cfg(feature = "mysql")]
            v.push(ProviderKind::Mysql);
            #[cfg(feature = "sqlserver")]
            v.push(ProviderKind::Sqlserver);
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
}
