#![allow(clippy::redundant_pub_crate)]

#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use plenora_database_core::plan::ProviderKind;
// La famiglia `database-*` costruisce piani per qualunque provider compilato.
use plenora_database_core::plan::{ObjectRef, Operation, ReadOperation, WriteOperation};
#[cfg(feature = "postgres")]
use plenora_database_core::plan::{OrderBy, SortDirection};
use plenora_database_core::relational::QueryOperation;
// Il percorso `postgres-read-ipc` e l'unico che pianifica una lettura e ne
// misura il budget: fuori dalla feature questo nome non ha un chiamante.
use plenora_database_core::provider::{Inspection, Provider, SecretString};
// Lo streaming a batch esce solo dal percorso IPC.
use plenora_database_core::provider::BatchStream;
use plenora_database_core::provider::ParameterBag;
// Anche i comandi `database-*` comuni applicano budget ai provider compilati.
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::CommitOutcome;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase};
// Il giudizio sul commit incerto e comune a tutti i provider.
use plenora_database_core::{ErrorCategory, RemoteEffect, RetryDisposition};
use plenora_database_engine::parse_and_validate;
#[cfg(feature = "db2")]
use plenora_db_db2::{Db2Config, Db2Provider, Db2TlsMode};
#[cfg(feature = "mysql")]
use plenora_db_mysql::{MariadbProvider, MysqlConfig, MysqlProvider};
#[cfg(feature = "postgres")]
use plenora_db_postgres::{PostgresProvider, PostgresTlsConfig, PostgresTlsMode};
#[cfg(feature = "sqlserver")]
use plenora_db_sqlserver::{SqlServerConfig, SqlServerProvider};
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
use rustls::{pki_types::CertificateDer, RootCertStore};
use serde_json::json;
use std::env;
use std::fs::OpenOptions;
use std::fs::{self, File};
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
use std::io::{Cursor, Read};
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_ipc::writer::FileWriter;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(CliError::Silent) => {
            // Il sottocomando ha gia stampato il JSON con lo stato logico;
            // qui si trasmette soltanto l'exit code non-zero
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
    /// output.
    // Costruita da `diagnose`, che vive dietro la feature `postgres`. La
    // variante resta nell'enum perche i suoi rami di `match` sono neutri:
    // spostarla dietro la feature significherebbe cfg-are anche quelli.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    Silent,
}

/// Cosa la CLI dice di un `commit()`, e con quale codice di uscita.
///
/// `CommitOutcome::OutcomeUnknown` indica che il commit e stato **emesso** ma
/// l'esito remoto non e verificabile. Trattarlo come successo o ritentare alla
/// cieca puo duplicare una scrittura gia applicata.
///
/// Qui l'incertezza ha un nome suo — `outcome_unknown` — e non e mai `ok`.
///
/// La funzione resta indipendente dalle feature perche la semantica del commit
/// non cambia con il provider compilato.
#[cfg_attr(
    not(any(
        feature = "postgres",
        feature = "mysql",
        feature = "sqlserver",
        feature = "db2"
    )),
    allow(dead_code, reason = "nessun adapter compilato apre una transazione")
)]
pub(crate) const fn commit_status(outcome: &CommitOutcome) -> &'static str {
    match outcome {
        CommitOutcome::Committed => "ok",
        CommitOutcome::OutcomeUnknown { .. } => "outcome_unknown",
    }
}

/// L'uscita che accompagna un esito gia stampato: zero solo se certo.
///
/// Stesso attributo e stessa ragione di [`commit_status`], che accompagna
/// sempre.
#[cfg_attr(
    not(any(
        feature = "postgres",
        feature = "mysql",
        feature = "sqlserver",
        feature = "db2"
    )),
    allow(dead_code, reason = "nessun adapter compilato apre una transazione")
)]
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
///
/// I quattro chiamanti — benchmark, schema effimero, harness PFM, testing —
/// stanno tutti dietro `postgres`, quindi il predicato e piu stretto di quello
/// delle due funzioni qui sopra. Vale la stessa nota: e l'assenza di
/// chiamanti a essere dichiarata, non una proprieta del giudizio.
#[cfg_attr(
    not(feature = "postgres"),
    allow(
        dead_code,
        reason = "i quattro chiamanti stanno dietro la feature postgres"
    )
)]
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

static IPC_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

const IPC_DEFAULT_MAX_ROWS: u64 = 10_000_000;
const IPC_DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const IPC_DEFAULT_TIMEOUT_MS: u64 = 10 * 60 * 1_000;
#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
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
    /// diagnostiche. Panica per `Silent`.
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
        "database-execute-sql" => database_execute_sql(&mut args).await,
        "database-execute-scalar" => database_execute_scalar(&mut args).await,
        "database-execute-ddl" => database_execute_ddl(&mut args).await,
        "database-query-summary" => database_query_summary(&mut args).await,
        "database-read-summary" => database_read(&mut args, None).await,
        "database-read-ipc" => database_read_ipc(&mut args).await,
        "database-write-ipc" => database_write_ipc(&mut args).await,
        "database-inspect-catalogs" => database_inspect(&mut args, list_catalogs_source).await,
        "database-inspect-schemas" => database_inspect(&mut args, list_schemas_source).await,
        "database-inspect-objects" => database_inspect(&mut args, list_objects_source).await,
        "database-describe" => database_inspect(&mut args, describe_source).await,
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
        "portable-compile" => portable_cmd::portable_compile(&mut args),
        #[cfg(feature = "postgres")]
        "portable-execute" => query_cmd::portable_execute(&mut args).await,
        "database-portable-execute" => database_portable_execute(&mut args).await,
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
        // Comandi della famiglia MySQL.
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
/// Il documento capability e un file, non una connessione, quindi anche la
/// preparazione resta interamente offline.
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

/// Il provider nominato ha un adapter in **questo** binario.
///
/// `ProviderKind` viene dal contratto, che enumera anche cio che nessun crate
/// implementa — `Mariadb` e li da prima che esistesse una misura. Accettare il
/// nome e fallire dopo, alla connessione, direbbe al chiamante che il server
/// non risponde quando il problema e che l'adapter non e stato compilato.
fn ensure_adapter_available(kind: ProviderKind) -> CliResult<()> {
    // Una tabella e non un predicato, per la stessa ragione di
    // `feature_is_compiled`: `cfg!` va valutato per **ogni** riga, altrimenti
    // quella della feature assente sparisce insieme al ramo.
    //
    // L'elenco riflette le feature compilate, cosi un adapter assente viene
    // diagnosticato prima di leggere secret o argomenti di connessione.
    let compiled = [
        (ProviderKind::Postgres, cfg!(feature = "postgres")),
        // `MariaDB` arriva con lo stesso crate di `MySQL`: chi ha compilato
        // l'uno ha compilato l'altro, e sono due provider distinti dentro la
        // stessa feature.
        (ProviderKind::Mysql, cfg!(feature = "mysql")),
        (ProviderKind::Mariadb, cfg!(feature = "mysql")),
        (ProviderKind::Sqlserver, cfg!(feature = "sqlserver")),
        (ProviderKind::Db2, cfg!(feature = "db2")),
    ]
    .into_iter()
    .any(|(candidate, compiled)| candidate == kind && compiled);
    if compiled {
        return Ok(());
    }
    // Il testo distingue i due casi come li distingue `unknown_command` per i
    // sotto-comandi: un adapter che nessun crate implementa non si risolve
    // ricostruendo, uno non compilato si.
    let message = if matches!(
        kind,
        ProviderKind::Postgres
            | ProviderKind::Mysql
            | ProviderKind::Mariadb
            | ProviderKind::Sqlserver
            | ProviderKind::Db2
    ) {
        "adapter non compilato in questo binario: ricostruire con la feature \
         di quel provider (--features mysql per mysql e mariadb, \
         --features sqlserver, --features db2, oppure --features full per tutti)"
    } else {
        "provider dichiarato dal contratto ma adapter non disponibile"
    };
    Err(CliError::Fatal(DatabaseError::unsupported(
        kind,
        ErrorPhase::Prepare,
        message,
    )))
}

/// Destinazione comune dei comandi `database-*`.
///
/// Separa il prefisso stabile della riga di comando dagli argomenti specifici
/// dell'operazione. L'apertura resta intenzionalmente successiva al loro parse:
/// gli argomenti del provider hanno lunghezza variabile e consumano tutto cio
/// che rimane.
struct ProviderTarget {
    kind: ProviderKind,
    secret_environment: String,
}

impl ProviderTarget {
    fn parse(args: &mut impl Iterator<Item = String>) -> CliResult<Self> {
        let provider = args.next().ok_or_else(|| "manca il provider".to_owned())?;
        let kind = parse_provider_kind(&provider)?;
        ensure_adapter_available(kind)?;
        let secret_environment = args
            .next()
            .ok_or_else(|| "manca il nome della variabile secret".to_owned())?;
        Ok(Self {
            kind,
            secret_environment,
        })
    }

    fn open(
        &self,
        args: &mut impl Iterator<Item = String>,
    ) -> CliResult<(SecretString, Box<dyn Provider>)> {
        let arguments = prepare_provider_arguments(parse_provider_arguments(self.kind, args)?)?;
        let secret = secret_from_env(&self.secret_environment)?;
        let provider = build_provider_from_prepared_arguments(arguments, &secret)?;
        Ok((secret, provider))
    }
}

async fn database_probe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let (secret, provider) = target.open(args)?;
    let cancellation = CancellationToken::new();
    let connection = provider.test_connection(&secret, &cancellation).await?;
    let capabilities = provider.probe_capabilities(&secret, &cancellation).await?;
    print_json(&json!({
        "schema_version": 1,
        "connection": connection,
        "capabilities": capabilities
    }))
}

/// `database-execute-sql <provider> <SECRET_ENV> <sql> [--allow-raw] <argomenti provider>`
///
/// Esegue uno statement dentro una transazione e ne stampa le righe toccate.
///
/// # Perche e generica
///
/// Gli adapter implementano tutti `TransactionScope`, quindi una superficie
/// condivisa evita rami e copie specifici del prodotto.
///
/// # `--allow-raw`
///
/// Senza, la transazione usa il profilo PFM, che restringe gli statement
/// ammessi. E' il default perche un CLI che accetta qualunque SQL su un
/// endpoint di produzione e una superficie piu larga di quella che il
/// contratto descrive.
///
/// # Errors
///
/// Se il provider non e riconosciuto o non e compilato in questo binario, se
/// il secret non c'e, se la connessione fallisce, o se lo statement viene
/// rifiutato dalla policy o dal server.
async fn database_execute_sql(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let kind = target.kind;
    let sql = args
        .next()
        .ok_or_else(|| "manca lo statement SQL".to_owned())?;
    // Il flag sta **prima** degli argomenti del provider, che consumano tutto
    // cio che resta: dopo di loro non arriverebbe mai.
    let mut peekable = args.peekable();
    let allow_raw = peekable.next_if(|value| value == "--allow-raw").is_some();
    let (secret, provider) = target.open(&mut peekable)?;

    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let options = if allow_raw {
        plenora_database_core::transaction::TransactionOptions::default()
    } else {
        plenora_database_core::transaction::TransactionOptions::pfm_defaults()
    };
    let mut transaction = provider
        .begin_transaction(&secret, &options, &budget, &cancellation)
        .await?;
    let statement = plenora_database_core::transaction::Statement {
        sql,
        params: Vec::new(),
    };
    let affected = transaction.execute(&statement, &cancellation).await?;
    let commit = transaction.commit(&cancellation).await?;
    print_json(&json!({
        "provider": kind,
        "status": commit_status(&commit),
        "affected_rows": affected,
        "commit": commit,
    }))?;
    commit_exit(&commit)
}

/// `database-portable-execute <provider> <SECRET_ENV> <PORTABLE.json> <argomenti provider>`
///
/// Compila un `PortableStatement` per il dialetto del provider e lo esegue in
/// una transazione, stampando le righe se lo statement ne restituisce.
///
/// # Perche generico
///
/// Il compilatore supporta quattro dialetti e la facade dispatcha su
/// `tx.provider_kind()`, quindi non servono comandi duplicati per provider.
///
/// # Le due forme
///
/// Uno statement con `RETURNING` — o una `SELECT` — rende righe, e va
/// attraversato con `execute_portable_returning`; gli altri rendono un
/// conteggio. Sceglierne una sola avrebbe significato rifiutare meta degli
/// statement che il contratto ammette, e chiedere al chiamante di sapere quale
/// comando usare per una distinzione che il piano gia dichiara.
///
/// # Errors
///
/// Se il provider non e riconosciuto o non e compilato in questo binario, se
/// il file non e leggibile o non e un AST valido, se il compilatore rifiuta il
/// piano per quel dialetto, o se il server lo rifiuta.
async fn database_portable_execute(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let kind = target.kind;
    let path = args
        .next()
        .ok_or_else(|| "manca il percorso di PORTABLE.json".to_owned())?;
    let (secret, provider) = target.open(args)?;

    let source = std::fs::read_to_string(&path).map_err(|error| {
        format!(
            "lettura dello statement portabile fallita: {}",
            error.kind()
        )
    })?;
    let statement: plenora_database_core::portable::PortableStatement =
        serde_json::from_str(&source).map_err(|error| {
            format!(
                "PortableStatement JSON non parsabile a riga {}, colonna {}",
                error.line(),
                error.column()
            )
        })?;

    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    // La forma decide quale meta della facade serve, e la decide il **piano**:
    // il chiamante non deve saperlo.
    // La domanda la pone il core, che e anche chi la usa per rifiutare la meta
    // sbagliata: una seconda copia qui smetterebbe di seguirlo in silenzio.
    let outcome = if plenora_database_core::facade::statement_returns_rows(&statement) {
        plenora_database_core::facade::execute_portable_returning(
            transaction.as_mut(),
            &statement,
            &cancellation,
        )
        .await
        .map(|rows| {
            let count = rows.len();
            let rows = rows
                .iter()
                .map(|row| json!({"columns": row.columns(), "values": row.values()}))
                .collect::<Vec<_>>();
            json!({"rows": rows, "count": count})
        })
    } else {
        plenora_database_core::facade::execute_portable(
            transaction.as_mut(),
            &statement,
            &cancellation,
        )
        .await
        .map(|affected| json!({"affected_rows": affected}))
    };
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(error) => {
            // Un rifiuto del compilatore o del server lascia la transazione
            // aperta: chiuderla e responsabilita di chi l'ha aperta, e un
            // rollback silenzioso e la sola cosa onesta da fare qui.
            let _ = transaction.rollback(&cancellation).await;
            return Err(error.into());
        }
    };
    let commit = transaction.commit(&cancellation).await?;
    print_json(&json!({
        "provider": kind,
        "status": commit_status(&commit),
        "outcome": outcome,
        "commit": commit,
    }))?;
    commit_exit(&commit)
}

/// `database-execute-scalar <provider> <SECRET_ENV> <sql> <argomenti provider>`
///
/// Una `SELECT` che rende **al piu una riga, esattamente una colonna**.
///
/// Piu di una riga, o piu di una colonna, sono un errore: non una scelta
/// arbitraria del primo valore. Una query sbagliata che rendesse un risultato
/// plausibile e peggio di una che dice di esserlo.
///
/// # Errors
///
/// Come `database-execute-sql`, piu il rifiuto se il result set non e scalare.
async fn database_execute_scalar(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let kind = target.kind;
    let sql = args
        .next()
        .ok_or_else(|| "manca lo statement SQL".to_owned())?;
    let (secret, provider) = target.open(args)?;

    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await?;
    let statement = plenora_database_core::transaction::Statement {
        sql,
        params: Vec::new(),
    };
    let outcome = transaction.query(&statement, &cancellation).await;
    // La transazione si chiude comunque: un rifiuto di forma non deve lasciare
    // aperta una lettura sul server.
    let value = match outcome {
        Ok(rows) if rows.len() > 1 => {
            let _ = transaction.rollback(&cancellation).await;
            return Err("la query scalare ha reso piu di una riga".to_owned().into());
        }
        Ok(rows) => match rows.first() {
            None => None,
            Some(row) if row.values().len() != 1 => {
                let _ = transaction.rollback(&cancellation).await;
                return Err("la query scalare ha reso piu di una colonna"
                    .to_owned()
                    .into());
            }
            Some(row) => Some(row.values()[0].clone()),
        },
        Err(error) => {
            let _ = transaction.rollback(&cancellation).await;
            return Err(error.into());
        }
    };
    let commit = transaction.commit(&cancellation).await?;
    print_json(&json!({
        "provider": kind,
        "status": commit_status(&commit),
        "value": value,
        "commit": commit,
    }))?;
    commit_exit(&commit)
}

/// Esegue DDL attraverso il bordo comune del provider, fuori transazione.
async fn database_execute_ddl(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let kind = target.kind;
    let sql = args
        .next()
        .ok_or_else(|| "manca lo statement DDL".to_owned())?;
    let (secret, provider) = target.open(args)?;
    provider
        .execute_ddl(&secret, &sql, &CancellationToken::new())
        .await?;
    print_json(&json!({"provider": kind, "status": "ok"}))
}

fn read_parameters(path: &str) -> CliResult<ParameterBag> {
    if path == "-" {
        return Ok(ParameterBag::default());
    }
    let contents = fs::read(path).map_err(|_| "PARAMETERS.json non leggibile".to_owned())?;
    serde_json::from_slice(&contents).map_err(|error| {
        format!(
            "PARAMETERS.json non parsabile a riga {}, colonna {}",
            error.line(),
            error.column()
        )
        .into()
    })
}

fn stream_fields(stream: &dyn BatchStream) -> Vec<serde_json::Value> {
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

async fn consume_summary(
    kind: ProviderKind,
    stream: &mut dyn BatchStream,
    cancellation: &CancellationToken,
) -> CliResult<()> {
    let fields = stream_fields(stream);
    let mut batches = 0_u64;
    let mut rows = 0_u64;
    while let Some(batch) = stream.next_batch(cancellation).await? {
        batches = batches
            .checked_add(1)
            .ok_or_else(|| CliError::from("conteggio batch oltre u64"))?;
        rows = rows
            .checked_add(
                u64::try_from(batch.num_rows())
                    .map_err(|_| CliError::from("conteggio righe oltre u64"))?,
            )
            .ok_or_else(|| CliError::from("conteggio righe oltre u64"))?;
    }
    print_json(&json!({
        "schema_version": 1,
        "status": "ok",
        "provider": kind,
        "batches": batches,
        "rows": rows,
        "fields": fields,
    }))
}

/// Esegue un `QueryOperation` serializzato e rende uno summary bounded.
async fn database_query_summary(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let kind = target.kind;
    let query_path = args
        .next()
        .ok_or_else(|| "manca il percorso QUERY.json".to_owned())?;
    let parameters_path = args
        .next()
        .ok_or_else(|| "manca PARAMETERS.json (usa - per nessun parametro)".to_owned())?;
    let query_contents = fs::read(query_path).map_err(|_| "QUERY.json non leggibile".to_owned())?;
    let operation: QueryOperation = serde_json::from_slice(&query_contents).map_err(|error| {
        format!(
            "QUERY.json non parsabile a riga {}, colonna {}",
            error.line(),
            error.column()
        )
    })?;
    let parameters = read_parameters(&parameters_path)?;
    let (secret, provider) = target.open(args)?;
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .query(&secret, &operation, &parameters, &budget, &cancellation)
        .await?;
    consume_summary(kind, stream.as_mut(), &cancellation).await
}

async fn database_read(
    args: &mut impl Iterator<Item = String>,
    output: Option<String>,
) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let kind = target.kind;
    let read_path = args
        .next()
        .ok_or_else(|| "manca il percorso READ.json".to_owned())?;
    let parameters_path = args
        .next()
        .ok_or_else(|| "manca PARAMETERS.json (usa - per nessun parametro)".to_owned())?;
    let contents = fs::read(read_path).map_err(|_| "READ.json non leggibile".to_owned())?;
    let operation: ReadOperation = serde_json::from_slice(&contents).map_err(|error| {
        format!(
            "READ.json non parsabile a riga {}, colonna {}",
            error.line(),
            error.column()
        )
    })?;
    let parameters = read_parameters(&parameters_path)?;
    let (secret, provider) = target.open(args)?;
    let budget = ResourceBudget::new(ResourceLimits {
        rows: IPC_DEFAULT_MAX_ROWS,
        output_bytes: IPC_DEFAULT_MAX_OUTPUT_BYTES,
        duration_ms: IPC_DEFAULT_TIMEOUT_MS,
        ..ResourceLimits::default()
    })?;
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .read(&secret, &operation, &parameters, &budget, &cancellation)
        .await?;
    if let Some(path) = output {
        let mut report =
            write_stream_to_ipc(Path::new(&path), stream.as_mut(), &cancellation).await?;
        let document = report
            .as_object_mut()
            .ok_or_else(|| CliError::from("report Arrow IPC non valido"))?;
        document.insert("provider".to_owned(), json!(kind));
        document.insert(
            "row_order".to_owned(),
            json!(if operation.order_by.is_empty() {
                "unspecified"
            } else {
                "deterministic"
            }),
        );
        return print_json(&report);
    }
    consume_summary(kind, stream.as_mut(), &cancellation).await
}

async fn database_read_ipc(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let collected = args.by_ref().collect::<Vec<_>>();
    if collected.len() < 5 {
        return Err(
            "database-read-ipc richiede provider, secret, READ.json, PARAMETERS.json e output"
                .into(),
        );
    }
    let output = collected[4].clone();
    let mut forwarded = collected
        .into_iter()
        .enumerate()
        .filter_map(|(index, value)| (index != 4).then_some(value));
    database_read(&mut forwarded, Some(output)).await
}

/// Scrive un file Arrow IPC tramite il contratto `prepare_write` + `write`.
async fn database_write_ipc(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let operation_path = args
        .next()
        .ok_or_else(|| "manca il percorso WRITE.json".to_owned())?;
    let input_path = args
        .next()
        .ok_or_else(|| "manca il percorso INPUT.arrow".to_owned())?;
    let contents = fs::read(operation_path).map_err(|_| "WRITE.json non leggibile".to_owned())?;
    let operation: WriteOperation = serde_json::from_slice(&contents).map_err(|error| {
        format!(
            "WRITE.json non parsabile a riga {}, colonna {}",
            error.line(),
            error.column()
        )
    })?;
    let (secret, provider) = target.open(args)?;
    let stream = ipc_input::IpcFileBatchStream::open(&input_path)?;
    let input_schema = stream.schema();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let cancellation = CancellationToken::new();
    let prepared = provider
        .prepare_write(&secret, &operation, input_schema, &budget, &cancellation)
        .await?;
    let outcome = provider
        .write(&secret, prepared, Box::new(stream), &budget, &cancellation)
        .await?;
    print_json(&serde_json::to_value(outcome).map_err(|_| "outcome non serializzabile".to_owned())?)
}

/// Un'introspezione qualunque, sul provider nominato dal primo argomento.
///
/// # Perche generica invece di una famiglia per prodotto
///
/// `Provider::inspect` offre le stesse operazioni su ogni adapter compilato;
/// una sola superficie evita che le varianti per prodotto divergano.
///
/// # L'ordine degli argomenti
///
/// Le posizionali dell'operazione — schema, oggetto — stanno **prima** degli
/// argomenti del provider, e non e una scelta di gusto:
/// [`parse_provider_arguments`] consuma l'iteratore fino in fondo, perche per
/// `PostgreSQL` sono i soli flag TLS mentre per gli altri due sono host,
/// database, utente, porta e TLS, in numero variabile. Dopo di essi una
/// posizionale non esiste. Prima, entrambe si leggono senza ambiguita.
async fn database_inspect(
    args: &mut impl Iterator<Item = String>,
    source: fn(&mut dyn Iterator<Item = String>) -> CliResult<Operation>,
) -> CliResult<()> {
    let target = ProviderTarget::parse(args)?;
    let operation = source(args)?;
    let (secret, provider) = target.open(args)?;
    let kind = provider.kind();
    let inspection = provider
        .inspect(&secret, &operation, &CancellationToken::new())
        .await?;
    print_json(&inspection_output(kind, inspection)?)
}

/// Envelope CLI stabile per tutti i documenti di introspezione.
///
/// `Inspection::document` e sempre un oggetto nei quattro adapter. Appiattirlo
/// mantiene comodi `schemas`, `objects` e `columns`, mentre provider e
/// operazione rendono la risposta auto-descrittiva come gli altri comandi
/// `database-*`.
fn inspection_output(kind: ProviderKind, inspection: Inspection) -> CliResult<serde_json::Value> {
    let serde_json::Value::Object(document) = inspection.document else {
        return Err("documento di introspezione non strutturato".into());
    };
    let mut output = serde_json::Map::new();
    output.insert("schema_version".to_owned(), json!(1));
    output.insert("provider".to_owned(), json!(kind));
    output.insert("operation".to_owned(), json!(inspection.operation));
    for (key, value) in document {
        if output.insert(key, value).is_some() {
            return Err("documento di introspezione usa un campo CLI riservato".into());
        }
    }
    Ok(serde_json::Value::Object(output))
}

/// Il nome dello schema, che nessuna operazione puo dedurre.
///
/// Vuoto e un errore, non un carattere jolly: `DatabaseListObjects` con uno
/// schema vuoto significa "tutti", e un argomento dimenticato diventerebbe
/// silenziosamente una domanda piu larga di quella scritta.
fn required(args: &mut dyn Iterator<Item = String>, missing: &str) -> CliResult<String> {
    let value = args.next().ok_or_else(|| missing.to_owned())?;
    if value.trim().is_empty() {
        return Err(format!("{missing}: il valore e vuoto").into());
    }
    Ok(value)
}

// Le quattro sorgenti sono funzioni con un nome e non chiusure dentro il
// dispatch: cosi il test le esercita come le esercita il binario, invece di
// riscriverle e verificare la propria copia.

// Le due senza posizionali non hanno modo di fallire, e il `Result` e
// comunque parte della firma che tutte e quattro condividono: e cio che
// permette a `database_inspect` di prenderne una qualunque. Toglierlo qui
// vorrebbe dire due firme, quindi due percorsi, per la sola ragione che oggi
// nessuna delle due legge un argomento.
#[allow(
    clippy::unnecessary_wraps,
    reason = "firma condivisa dalle quattro sorgenti di `database_inspect`"
)]
fn list_catalogs_source(_: &mut dyn Iterator<Item = String>) -> CliResult<Operation> {
    Ok(Operation::DatabaseListCatalogs)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "firma condivisa dalle quattro sorgenti di `database_inspect`"
)]
fn list_schemas_source(_: &mut dyn Iterator<Item = String>) -> CliResult<Operation> {
    Ok(Operation::DatabaseListSchemas { source: None })
}

fn list_objects_source(args: &mut dyn Iterator<Item = String>) -> CliResult<Operation> {
    Ok(Operation::DatabaseListObjects {
        source: Some(ObjectRef {
            catalog: None,
            schema: Some(required(args, "manca lo schema")?),
            // L'oggetto non partecipa a una lista: il contratto vuole
            // comunque un `ObjectRef`, e questo campo e la parte che
            // l'operazione non legge.
            object: String::new(),
        }),
    })
}

fn describe_source(args: &mut dyn Iterator<Item = String>) -> CliResult<Operation> {
    // Lo schema si legge per primo e si tiene: valutare i due `required` come
    // argomenti della stessa espressione lascerebbe l'ordine alla
    // valutazione, e `<schema> <object>` diventerebbero intercambiabili.
    let schema = required(args, "manca lo schema")?;
    Ok(Operation::DatabaseDescribeObject {
        source: ObjectRef {
            catalog: None,
            schema: Some(schema),
            object: required(args, "manca l'oggetto")?,
        },
    })
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
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
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
        row_offset: None,
        filter: options.filter.clone(),
        declared_crs: Vec::new(),
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

#[cfg(unix)]
fn sync_parent_directory(parent: &Path) -> bool {
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .is_ok()
}

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

#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TlsPathEnvironments {
    ca: Option<String>,
    client_certificate: Option<String>,
    client_key: Option<String>,
    mode: Option<String>,
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
    /// `MariaDB`, che ha gli stessi argomenti di `MySQL` e un provider
    /// diverso.
    ///
    /// Una variante sua e non un flag su quella di `MySQL`: la scelta del
    /// prodotto e cio che decide quale provider viene costruito, e ADR 0014
    /// vieta che sia il server a deciderlo. Un booleano dentro `Mysql`
    /// avrebbe reso possibile costruire il provider sbagliato dimenticando di
    /// leggerlo.
    #[cfg(feature = "mysql")]
    Mariadb {
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
    #[cfg(feature = "db2")]
    Db2 {
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
    // Stessa configurazione, provider diverso: i due prodotti parlano lo
    // stesso protocollo, e cio che li distingue vive nel profilo che il
    // costruttore del provider seleziona.
    #[cfg(feature = "mysql")]
    Mariadb(MysqlConfig),
    #[cfg(feature = "sqlserver")]
    Sqlserver(SqlServerConfig),
    #[cfg(feature = "db2")]
    Db2(Db2Config),
}

#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
fn parse_tls_path_environments(
    args: &mut impl Iterator<Item = String>,
) -> CliResult<TlsPathEnvironments> {
    let mut tls = TlsPathEnvironments::default();
    while let Some(flag) = args.next() {
        let target = match flag.as_str() {
            "--tls-ca-path-env" => &mut tls.ca,
            "--tls-client-cert-path-env" => &mut tls.client_certificate,
            "--tls-client-key-path-env" => &mut tls.client_key,
            "--tls-mode" => &mut tls.mode,
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

#[cfg_attr(
    not(any(
        feature = "postgres",
        feature = "mysql",
        feature = "sqlserver",
        feature = "db2"
    )),
    allow(clippy::needless_pass_by_ref_mut)
)]
fn parse_provider_arguments(
    kind: ProviderKind,
    args: &mut impl Iterator<Item = String>,
) -> CliResult<ProviderArguments> {
    #[cfg(not(any(
        feature = "postgres",
        feature = "mysql",
        feature = "sqlserver",
        feature = "db2"
    )))]
    let _ = args;
    match kind {
        #[cfg(feature = "postgres")]
        ProviderKind::Postgres => {
            let tls = parse_tls_path_environments(args)?;
            if tls.mode.is_some() {
                return Err("--tls-mode nella famiglia generica e supportato solo per Db2".into());
            }
            Ok(ProviderArguments::Postgres { tls })
        }
        #[cfg(any(feature = "mysql", feature = "sqlserver", feature = "db2"))]
        ProviderKind::Mysql
        | ProviderKind::Mariadb
        | ProviderKind::Sqlserver
        | ProviderKind::Db2 => {
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
            if kind != ProviderKind::Db2 && tls.mode.is_some() {
                return Err("--tls-mode nella famiglia generica e supportato solo per Db2".into());
            }
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
                #[cfg(feature = "mysql")]
                ProviderKind::Mariadb => Ok(ProviderArguments::Mariadb {
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
                #[cfg(feature = "db2")]
                ProviderKind::Db2 => Ok(ProviderArguments::Db2 {
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

#[cfg_attr(
    not(any(
        feature = "postgres",
        feature = "mysql",
        feature = "sqlserver",
        feature = "db2"
    )),
    allow(clippy::missing_const_for_fn, clippy::needless_pass_by_value)
)]
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
        } => Ok(PreparedProviderArguments::Mysql(mysql_family_config(
            host, database, username, port, &tls,
        )?)),
        #[cfg(feature = "mysql")]
        ProviderArguments::Mariadb {
            host,
            database,
            username,
            port,
            tls,
        } => Ok(PreparedProviderArguments::Mariadb(mysql_family_config(
            host, database, username, port, &tls,
        )?)),
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
        #[cfg(feature = "db2")]
        ProviderArguments::Db2 {
            host,
            database,
            username,
            port,
            tls,
        } => {
            let mut config = Db2Config::new(host, database, username);
            if let Some(port) = port {
                config = config.with_port(port);
            }
            if let Some(path) = prepare_db2_private_ca_path(tls.ca.as_deref())? {
                config = config.with_private_ca_certificate(path);
            }
            config = match tls.mode.as_deref() {
                None | Some("verify") => config,
                Some("disable") => config.with_tls_mode(Db2TlsMode::Disable),
                Some(_) => {
                    return Err(
                        "tls mode Db2 non riconosciuto: valori ammessi verify|disable".into(),
                    )
                }
            };
            Ok(PreparedProviderArguments::Db2(config))
        }
    }
}

// Il tipo di ritorno è Result perché il ramo MySQL/SQL Server può fallire
// nella costruzione del provider; con solo Postgres attivo il match è
// infallibile ma la firma resta la stessa per uniformità.
#[allow(clippy::unnecessary_wraps)]
/// La configurazione comune ai due prodotti della famiglia `MySQL`.
///
/// Sta in un posto solo perche i due percorsi devono restare identici: la
/// differenza fra `MysqlProvider` e `MariadbProvider` e il profilo che il
/// costruttore seleziona, non come si legge la riga di comando. Due copie
/// divergerebbero alla prima correzione applicata a una sola — ed e proprio il
/// fail-close TLS a non poterselo permettere.
#[cfg(feature = "mysql")]
fn mysql_family_config(
    host: String,
    database: String,
    username: String,
    port: Option<u16>,
    tls: &TlsPathEnvironments,
) -> CliResult<MysqlConfig> {
    let mut config = MysqlConfig::new(host, database, username, SecretString::new(""));
    if let Some(port) = port {
        config = config.with_port(port);
    }
    if let Some(pem) = prepare_private_ca_material(tls.ca.as_deref())? {
        config = config.with_private_ca_certificate_pem(pem);
    }
    config.validate_without_password()?;
    Ok(config)
}

// Il `Result` serve dove esiste un adapter che puo fallire a costruirsi —
// `MysqlProvider`, `MariadbProvider`, `SqlServerProvider` validano pool e
// configurazione — e in un binario di solo PostgreSQL non ne resta nessuno: li
// il ramo unico restituisce un valore gia costruito. Togliere il `Result`
// dalla firma lo toglierebbe anche dove serve; dichiarare qui in quale
// configurazione non serve e la sola forma che non mente in nessuna delle
// quattro.
#[cfg_attr(
    not(any(feature = "mysql", feature = "sqlserver", feature = "db2")),
    allow(
        clippy::unnecessary_wraps,
        reason = "senza altri adapter resta un solo ramo, e non puo fallire"
    )
)]
#[cfg_attr(
    not(any(
        feature = "postgres",
        feature = "mysql",
        feature = "sqlserver",
        feature = "db2"
    )),
    allow(clippy::needless_pass_by_value)
)]
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
        #[cfg(feature = "mysql")]
        PreparedProviderArguments::Mariadb(config) => Ok(Box::new(MariadbProvider::new(
            config.with_password(secret.clone()),
            8,
        )?)),
        #[cfg(feature = "sqlserver")]
        PreparedProviderArguments::Sqlserver(config) => Ok(Box::new(SqlServerProvider::new(
            config.with_password(secret.clone()),
            1_024,
            8,
        )?)),
        #[cfg(feature = "db2")]
        PreparedProviderArguments::Db2(config) => Ok(Box::new(Db2Provider::new(config)?)),
    }
}

#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
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

#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
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

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
fn prepare_private_ca_material(env_name: Option<&str>) -> CliResult<Option<Vec<u8>>> {
    let Some(path) = tls_path_from_environment(env_name)? else {
        return Ok(None);
    };
    let material = read_bounded_tls_material(&path)?;
    Ok(Some(validate_and_normalize_private_ca_material(
        &path, &material,
    )?))
}

/// Risolve e valida la CA privata senza copiarla: il client IBM riceve un
/// percorso assoluto tramite `SSLSERVERCERTIFICATE` e puo rileggerlo quando
/// apre una nuova connessione.
#[cfg(feature = "db2")]
fn prepare_db2_private_ca_path(env_name: Option<&str>) -> CliResult<Option<PathBuf>> {
    let Some(path) = tls_path_from_environment(env_name)? else {
        return Ok(None);
    };
    let material = read_bounded_tls_material(&path)?;
    validate_and_normalize_private_ca_material(&path, &material)?;
    let absolute =
        fs::canonicalize(path).map_err(|_| CliError::from("materiale TLS non leggibile"))?;
    Ok(Some(absolute))
}

#[cfg(any(
    feature = "postgres",
    feature = "mysql",
    feature = "sqlserver",
    feature = "db2"
))]
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
    postgres_provider_for_probe_with_tls_policy(
        tls,
        std::env::var_os(pfm::POSTGRES_INSECURE_LOCAL_ENV).is_some(),
    )
}

#[cfg(feature = "postgres")]
fn postgres_provider_for_probe_with_tls_policy(
    tls: &TlsPathEnvironments,
    insecure_local: bool,
) -> CliResult<PostgresProvider> {
    if insecure_local {
        if tls.ca.is_some() || tls.client_certificate.is_some() || tls.client_key.is_some() {
            return Err(format!(
                "{} disattiva TLS e non puo convivere con opzioni TLS provider",
                pfm::POSTGRES_INSECURE_LOCAL_ENV
            )
            .into());
        }
        return postgres_provider_from_tls_material(true, None, None, None);
    }
    let Some(ca) = prepare_private_ca_material(tls.ca.as_deref())? else {
        return postgres_provider_from_tls_material(false, None, None, None);
    };
    match (
        tls_path_from_environment(tls.client_certificate.as_deref())?,
        tls_path_from_environment(tls.client_key.as_deref())?,
    ) {
        (None, None) => postgres_provider_from_tls_material(false, Some(ca), None, None),
        (Some(certificate_path), Some(key_path)) => {
            let certificate = read_bounded_tls_material(&certificate_path)?;
            let key = read_bounded_tls_material(&key_path)?;
            postgres_provider_from_tls_material(false, Some(ca), Some(certificate), Some(key))
        }
        // Il parser accetta certificato e chiave solo insieme, quindi qui non
        // si arriva. La coppia di `Option` pero' non lo dice al compilatore, e
        // un errore e' preferibile al processo abbattuto se quell'invariante
        // dovesse cambiare.
        _ => Err("l'identità client TLS richiede certificato e chiave insieme".into()),
    }
}

/// Unica decisione TLS `PostgreSQL` condivisa dai comandi storici e dalla
/// superficie provider-neutral. I chiamanti differiscono solo nel modo in cui
/// nominano i percorsi; una volta letto il materiale, la policy non diverge.
#[cfg(feature = "postgres")]
pub(crate) fn postgres_provider_from_tls_material(
    insecure_local: bool,
    ca: Option<Vec<u8>>,
    certificate: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
) -> CliResult<PostgresProvider> {
    if insecure_local {
        if ca.is_some() || certificate.is_some() || key.is_some() {
            return Err("l'opt-out TLS locale non puo convivere con materiale TLS".into());
        }
        return Ok(PostgresProvider::insecure_local());
    }

    let Some(ca) = ca else {
        if certificate.is_some() || key.is_some() {
            return Err("l'identita client TLS richiede anche una CA privata".into());
        }
        return Ok(postgres_provider_for_probe());
    };
    let tls_config = match (certificate, key) {
        (None, None) => PostgresTlsConfig::private_ca_pem(&ca)?,
        (Some(certificate), Some(key)) => {
            PostgresTlsConfig::private_ca_with_client_identity_pem(&ca, &certificate, &key)?
        }
        _ => return Err("l'identita client TLS richiede certificato e chiave insieme".into()),
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
/// Il catalogo impedisce che dispatch, aiuto ed errori dichiarino insiemi
/// diversi di comandi.
const COMMAND_CATALOGUE: &[(&str, Option<&str>)] = &[
    ("database-describe", None),
    ("database-execute-ddl", None),
    ("database-execute-scalar", None),
    ("database-execute-sql", None),
    ("database-inspect-catalogs", None),
    ("database-inspect-objects", None),
    ("database-inspect-schemas", None),
    ("database-query-summary", None),
    ("database-read-ipc", None),
    ("database-read-summary", None),
    ("database-probe", None),
    ("database-write-ipc", None),
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
    // Puro: legge un AST e stampa SQL senza dipendere da un adapter compilato.
    ("portable-compile", None),
    ("portable-execute", Some("postgres")),
    ("database-portable-execute", None),
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
        ("db2", cfg!(feature = "db2")),
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
/// la seconda no.
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
        // Stesso crate, stessa feature, provider diverso: `plenora-db-mysql`
        // pubblica `MysqlProvider` e `MariadbProvider`, e chi ha compilato
        // l'uno ha compilato l'altro. Elencarne uno solo direbbe che l'altro
        // va ricostruito, che e falso.
        names.push("mariadb");
    }
    if cfg!(feature = "sqlserver") {
        names.push("sqlserver");
    }
    if cfg!(feature = "db2") {
        names.push("db2");
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
    #[cfg(feature = "db2")]
    sections.push(db2_usage());
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
        "  database-probe <provider> <secret-env> [args provider]".to_owned(),
        format!("    provider compilati in questo binario: {compiled}"),
        "  database-execute-sql <provider> <secret-env> <sql> [--allow-raw] [args provider]"
            .to_owned(),
        "  database-execute-scalar <provider> <secret-env> <sql> [args provider]".to_owned(),
        "  database-execute-ddl <provider> <secret-env> <sql> [args provider]".to_owned(),
        "  database-query-summary <provider> <secret-env> <QUERY.json> <PARAMETERS.json|-> [args provider]".to_owned(),
        "  database-read-summary <provider> <secret-env> <READ.json> <PARAMETERS.json|-> [args provider]".to_owned(),
        "  database-read-ipc <provider> <secret-env> <READ.json> <PARAMETERS.json|-> <OUTPUT.arrow> [args provider]".to_owned(),
        "  database-write-ipc <provider> <secret-env> <WRITE.json> <INPUT.arrow> [args provider]".to_owned(),
        "  database-inspect-catalogs <provider> <secret-env> [args provider]".to_owned(),
        "  database-inspect-schemas <provider> <secret-env> [args provider]".to_owned(),
        "  database-inspect-objects <provider> <secret-env> <schema> [args provider]".to_owned(),
        "  database-describe <provider> <secret-env> <schema> <object> [args provider]".to_owned(),
        "    introspezione via Provider::inspect, uguale su tutti i provider compilati".to_owned(),
        "    le posizionali stanno prima degli args del provider, che sono in numero variabile"
            .to_owned(),
        "  portable-compile <postgres|mysql|mariadb|sqlserver|db2> <PORTABLE.json>".to_owned(),
        "    compila e stampa SQL + numero parametri, senza aprire una connessione".to_owned(),
        "  database-portable-execute <provider> <secret-env> <PORTABLE.json> [args provider]"
            .to_owned(),
        "    compila per il provider, esegue in una transazione e rende rows o affected_rows"
            .to_owned(),
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
        "  portable-execute <dsn-env> <PORTABLE.json>",
        "    compatibilita del comando storico PostgreSQL; la famiglia generica e sopra",
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
        "== MySQL / MariaDB ==",
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
        "  nota: questi comandi sono di MySQL. MariaDB ha un provider suo, e",
        "  `mysql-probe` la rifiuta: si raggiunge dalla famiglia generica,",
        "  `database-probe mariadb <args...>` (ADR 0014, nessuna selezione automatica)",
    ]
    .join("\n")
}

#[cfg(feature = "sqlserver")]
fn sqlserver_usage() -> String {
    [
        "",
        "== SQL Server ==",
        "  args comuni: <secret-env> <host> <database> <user> [port] [--tls-ca-path-env <name>]",
        "  nessun sotto-comando `sqlserver-*`: l'adapter si raggiunge dalla famiglia",
        "  generica, che gira identica sui tre provider —",
        "    database-probe sqlserver <args...>",
        "    database-inspect-catalogs sqlserver <args...>",
        "    database-inspect-schemas sqlserver <args...>",
        "    database-inspect-objects sqlserver <secret-env> <schema> <resto args...>",
        "    database-describe sqlserver <secret-env> <schema> <object> <resto args...>",
        "  esecuzione e transazioni passano dagli stessi comandi `database-*` comuni",
        "  documentati sopra; `execute_ddl` resta fuori dalla superficie del provider.",
    ]
    .join("\n")
}

#[cfg(feature = "db2")]
fn db2_usage() -> String {
    [
        "",
        "== IBM Db2 LUW ==",
        "  args comuni: <secret-env> <host> <database> <user> [port] [--tls-ca-path-env <name>] [--tls-mode verify|disable]",
        "  il default richiede TLS e verifica il server; la CA privata resta un file",
        "  leggibile dal client IBM per l'intera durata del processo.",
        "  l'adapter si raggiunge dai comandi generici:",
        "    database-probe db2 <args...>",
        "    database-inspect-catalogs db2 <args...>",
        "    database-inspect-schemas db2 <args...>",
        "    database-inspect-objects db2 <secret-env> <schema> <resto args...>",
        "    database-describe db2 <secret-env> <schema> <object> <resto args...>",
        "    database-portable-execute db2 <secret-env> <PORTABLE.json> <resto args...>",
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
#[path = "main_usage_surface_tests.rs"]
mod usage_surface_tests;

// ============================================================================
//  I sottocomandi PFM espongono application plane, conformance e DDL.
//  Tutti prendono il DSN Postgres via
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
mod ipc_input;
mod mysql_cmd;
#[cfg(feature = "postgres")]
mod ops_cmd;
#[cfg(feature = "postgres")]
mod pfm;
mod portable_cmd;
#[cfg(feature = "postgres")]
mod query_cmd;
mod safety;
#[cfg(feature = "postgres")]
mod session_ctx;
#[cfg(feature = "postgres")]
mod testing;
// Provider-neutral: legge `NAME=VALUE:TYPE` e produce `ParameterValue` del
// core senza dipendere da feature di adapter.
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
#[path = "main_tests.rs"]
mod tests;
