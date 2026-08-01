use plenora_database_core::plan::{
    ObjectRef, Operation, OrderBy, ProviderKind, ReadOperation, SortDirection,
};
use plenora_database_core::provider::{BatchStream, ParameterBag, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition,
};
use plenora_database_engine::parse_and_validate;
use plenora_db_postgres::PostgresProvider;
use serde_json::json;
use std::env;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_ipc::writer::FileWriter;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            println!(
                "{}",
                error
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
struct CliError(DatabaseError);

type CliResult<T> = std::result::Result<T, CliError>;

static IPC_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(1);

const IPC_DEFAULT_MAX_ROWS: u64 = 10_000_000;
const IPC_DEFAULT_MAX_OUTPUT_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const IPC_DEFAULT_TIMEOUT_MS: u64 = 10 * 60 * 1_000;

struct IpcOptions {
    limits: ResourceLimits,
    order_by: Vec<OrderBy>,
}

impl CliError {
    fn to_json(&self) -> Result<String, serde_json::Error> {
        let error = serde_json::to_value(&self.0)?;
        serde_json::to_string(&json!({
            "status": "error",
            "protocol_version": 1,
            "error": error,
        }))
    }
}

impl From<DatabaseError> for CliError {
    fn from(error: DatabaseError) -> Self {
        Self(error)
    }
}

impl From<String> for CliError {
    fn from(message: String) -> Self {
        Self(DatabaseError::invalid_plan(message))
    }
}

impl From<&str> for CliError {
    fn from(message: &str) -> Self {
        Self(DatabaseError::invalid_plan(message))
    }
}

#[allow(clippy::needless_collect)]
// std::env::Args non è Send: materializzare prima degli await mantiene il
// future del main compatibile con il runtime multi-thread.
async fn run() -> CliResult<()> {
    let collected = env::args().skip(1).collect::<Vec<_>>();
    let mut args = collected.into_iter();
    let command = args.next().ok_or_else(|| CliError::from(usage()))?;
    match command.as_str() {
        "inspect-dataset" => inspect_dataset(&mut args),
        "validate-plan" => validate_plan(&mut args),
        "postgres-probe" => postgres_probe(&mut args).await,
        "postgres-describe" => postgres_describe(&mut args).await,
        "postgres-read-summary" => postgres_read_summary(&mut args).await,
        "postgres-read-ipc" => postgres_read_ipc(&mut args).await,
        _ => Err(usage().into()),
    }
}

fn inspect_dataset(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let path = one_argument(args, "manca il percorso del dataset Arrow IPC")?;
    let report = inspect_dataset::inspect(&path)?;
    print_json(&report)
}

fn validate_plan(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let path = one_argument(args, "manca il percorso del piano")?;
    let input = fs::read(path).map_err(|_| "piano non leggibile".to_owned())?;
    let validated = parse_and_validate(&input)?;
    print_json(&json!({
        "schema_version": 1,
        "status": "validated",
        "provider": validated.plan().provider,
        "fingerprint": validated.fingerprint()
    }))
}

async fn postgres_probe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let env_name = one_argument(args, "manca il nome della variabile DSN")?;
    let secret = secret_from_env(&env_name)?;
    let provider = PostgresProvider::default();
    let cancellation = CancellationToken::new();
    let connection = provider.test_connection(&secret, &cancellation).await?;
    let capabilities = provider.probe_capabilities(&secret, &cancellation).await?;
    print_json(&json!({
        "schema_version": 1,
        "connection": connection,
        "capabilities": capabilities
    }))
}

async fn postgres_describe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let env_name = args
        .next()
        .ok_or_else(|| "manca il nome della variabile DSN".to_owned())?;
    let schema = args.next().ok_or_else(|| "manca lo schema".to_owned())?;
    let object = args.next().ok_or_else(|| "manca l'oggetto".to_owned())?;
    ensure_end(args)?;
    let secret = secret_from_env(&env_name)?;
    let provider = PostgresProvider::default();
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

async fn postgres_read_summary(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let env_name = args
        .next()
        .ok_or_else(|| "manca il nome della variabile DSN".to_owned())?;
    let schema = args.next().ok_or_else(|| "manca lo schema".to_owned())?;
    let object = args.next().ok_or_else(|| "manca l'oggetto".to_owned())?;
    ensure_end(args)?;
    let secret = secret_from_env(&env_name)?;
    let provider = PostgresProvider::default();
    let operation = ReadOperation {
        source: object_ref(schema, object),
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        filter: None,
    };
    let mut stream = provider
        .read(
            &secret,
            &operation,
            &ParameterBag::default(),
            &ResourceBudget::new(ResourceLimits::default())?,
            &CancellationToken::new(),
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
    while let Some(batch) = stream.next_batch().await? {
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
    let provider = PostgresProvider::default();
    let operation = ReadOperation {
        source: object_ref(schema, object),
        projection: Vec::new(),
        order_by: options.order_by.clone(),
        row_limit: None,
        filter: None,
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
    let mut stream = provider
        .read(
            &secret,
            &operation,
            &ParameterBag::default(),
            &ResourceBudget::new(options.limits)?,
            &CancellationToken::new(),
        )
        .await?;
    let mut report = write_stream_to_ipc(Path::new(&output), stream.as_mut()).await?;
    report
        .as_object_mut()
        .ok_or_else(|| CliError::from("report Arrow IPC non valido"))?
        .insert("provider".to_owned(), json!(ProviderKind::Postgres));
    report
        .as_object_mut()
        .expect("report IPC costruito come oggetto")
        .insert("row_order".to_owned(), json!(row_order));
    report
        .as_object_mut()
        .expect("report IPC costruito come oggetto")
        .insert("limits".to_owned(), limits_report);
    print_json(&report)
}

fn parse_ipc_options(args: &mut impl Iterator<Item = String>) -> CliResult<IpcOptions> {
    let mut limits = ResourceLimits {
        rows: IPC_DEFAULT_MAX_ROWS,
        output_bytes: IPC_DEFAULT_MAX_OUTPUT_BYTES,
        duration_ms: IPC_DEFAULT_TIMEOUT_MS,
        ..ResourceLimits::default()
    };
    let mut order_by = Vec::new();
    while let Some(option) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("manca il valore per {option}"))?;
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
            _ => return Err(format!("opzione postgres-read-ipc sconosciuta: {option}").into()),
        }
    }
    limits.validate()?;
    Ok(IpcOptions { limits, order_by })
}

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
    let result = write_ipc_batches(&mut file, stream).await;
    let (batches, rows) = match result {
        Ok(counts) => counts,
        Err(mut error) => {
            drop(file);
            match fs::remove_file(&temporary) {
                Ok(()) => {
                    error.0.remote_effect = RemoteEffect::RolledBack;
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
            format!("pubblicazione Arrow IPC fallita: {error}"),
        );
        return match fs::remove_file(&temporary) {
            Ok(()) => {
                publish_error.0.remote_effect = RemoteEffect::RolledBack;
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
                    format!("artifact temporaneo Arrow IPC non creabile: {error}"),
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

async fn write_ipc_batches(file: &mut File, stream: &mut dyn BatchStream) -> CliResult<(u64, u64)> {
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
    while let Some(batch) = stream.next_batch().await? {
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
    CliError(DatabaseError {
        category,
        phase,
        remote_effect,
        retry,
        provider: None,
        execution_id: None,
        message: message.into(),
    })
}

const fn object_ref(schema: String, object: String) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some(schema),
        object,
        layer_id: None,
    }
}

fn secret_from_env(name: &str) -> CliResult<SecretString> {
    if name.is_empty() || name.contains('=') {
        return Err("nome variabile DSN non valido".into());
    }
    Ok(env::var(name)
        .map(SecretString::new)
        .map_err(|_| "variabile DSN assente".to_owned())?)
}

fn one_argument(args: &mut impl Iterator<Item = String>, missing: &str) -> CliResult<String> {
    let value = args.next().ok_or_else(|| missing.to_owned())?;
    ensure_end(args)?;
    Ok(value)
}

fn ensure_end(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    if args.next().is_some() {
        Err("troppi argomenti".into())
    } else {
        Ok(())
    }
}

fn print_json(value: &serde_json::Value) -> CliResult<()> {
    println!(
        "{}",
        serde_json::to_string(&value).map_err(|_| "output non serializzabile".to_owned())?
    );
    Ok(())
}

fn usage() -> String {
    [
        "uso:",
        "  plenora-database inspect-dataset <file.arrow>",
        "  plenora-database validate-plan <file>",
        "  plenora-database postgres-probe <dsn-env>",
        "  plenora-database postgres-describe <dsn-env> <schema> <object>",
        "  plenora-database postgres-read-summary <dsn-env> <schema> <object>",
        "  plenora-database postgres-read-ipc <dsn-env> <schema> <object> <output.arrow> \
         [--max-rows N] [--max-output-bytes N] [--timeout-ms N] [--order-by FIELD]",
    ]
    .join("\n")
}
mod inspect_dataset;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_ipc::reader::FileReader;
    use plenora_database_core::arrow::array::{ArrayRef, BinaryArray};
    use plenora_database_core::arrow::schema::{Field, Schema};
    use plenora_database_core::arrow::{RecordBatch, SchemaRef};
    use plenora_database_core::provider::{BatchStream, ProviderFuture};
    use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};
    use std::collections::{HashMap, VecDeque};
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Arc;

    struct TestStream {
        schema: SchemaRef,
        outcomes: VecDeque<plenora_database_core::Result<Option<RecordBatch>>>,
    }

    impl BatchStream for TestStream {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>> {
            let outcome = self.outcomes.pop_front().unwrap_or(Ok(None));
            Box::pin(async move { outcome })
        }
    }

    struct TestDirectory(PathBuf);

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

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

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
        let envelope = CliError(DatabaseError {
            category: ErrorCategory::Crs,
            phase: ErrorPhase::Validate,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: None,
            execution_id: None,
            message: "identificatore CRS e SRID numerico divergenti".to_owned(),
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
        let envelope = CliError(DatabaseError {
            category: ErrorCategory::Transient,
            phase: ErrorPhase::Connect,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::After(250),
            provider: None,
            execution_id: None,
            message: "servizio temporaneamente non disponibile".to_owned(),
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

    #[tokio::test]
    async fn ipc_materialization_preserves_schema_metadata_and_rows() {
        let directory = TestDirectory::new("success");
        let output = directory.output();
        let mut stream = stream_with_outcomes(VecDeque::new());
        let batch = test_batch(stream.schema());
        stream.outcomes = VecDeque::from([Ok(Some(batch)), Ok(None)]);

        let report = write_stream_to_ipc(&output, &mut stream)
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

        let error = write_stream_to_ipc(&output, &mut stream)
            .await
            .expect_err("stream failure");

        assert_eq!(error.0.category, ErrorCategory::Cancelled);
        assert!(!output.exists());
        assert!(partial_artifacts(&output).is_empty());
    }

    #[tokio::test]
    async fn ipc_materialization_never_overwrites_existing_output() {
        let directory = TestDirectory::new("conflict");
        let output = directory.output();
        fs::write(&output, b"existing artifact").expect("write existing output");
        let mut stream = stream_with_outcomes(VecDeque::new());

        let error = write_stream_to_ipc(&output, &mut stream)
            .await
            .expect_err("existing output must be rejected");

        assert_eq!(error.0.category, ErrorCategory::Conflict);
        assert_eq!(error.0.phase, ErrorPhase::Validate);
        assert_eq!(error.0.provider, None);
        assert_eq!(
            fs::read(&output).expect("existing output"),
            b"existing artifact"
        );
        assert!(partial_artifacts(&output).is_empty());
    }

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
}
