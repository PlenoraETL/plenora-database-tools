use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind, ReadOperation};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::CancellationToken;
use plenora_database_engine::parse_and_validate;
use plenora_db_postgres::PostgresProvider;
use serde_json::json;
use std::env;
use std::fs;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::needless_collect)]
// std::env::Args non è Send: materializzare prima degli await mantiene il
// future del main compatibile con il runtime multi-thread.
async fn run() -> Result<(), String> {
    let collected = env::args().skip(1).collect::<Vec<_>>();
    let mut args = collected.into_iter();
    let command = args.next().ok_or_else(usage)?;
    match command.as_str() {
        "inspect-dataset" => inspect_dataset(&mut args),
        "validate-plan" => validate_plan(&mut args),
        "postgres-probe" => postgres_probe(&mut args).await,
        "postgres-describe" => postgres_describe(&mut args).await,
        "postgres-read-summary" => postgres_read_summary(&mut args).await,
        _ => Err(usage()),
    }
}

fn inspect_dataset(args: &mut impl Iterator<Item = String>) -> Result<(), String> {
    let path = one_argument(args, "manca il percorso del dataset Arrow IPC")?;
    let report = inspect_dataset::inspect(&path)?;
    print_json(&report)
}

fn validate_plan(args: &mut impl Iterator<Item = String>) -> Result<(), String> {
    let path = one_argument(args, "manca il percorso del piano")?;
    let input = fs::read(path).map_err(|_| "piano non leggibile".to_owned())?;
    let validated = parse_and_validate(&input).map_err(|error| error.to_string())?;
    print_json(&json!({
        "schema_version": 1,
        "status": "validated",
        "provider": validated.plan().provider,
        "fingerprint": validated.fingerprint()
    }))
}

async fn postgres_probe(args: &mut impl Iterator<Item = String>) -> Result<(), String> {
    let env_name = one_argument(args, "manca il nome della variabile DSN")?;
    let secret = secret_from_env(&env_name)?;
    let provider = PostgresProvider::default();
    let cancellation = CancellationToken::new();
    let connection = provider
        .test_connection(&secret, &cancellation)
        .await
        .map_err(|error| error.to_string())?;
    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .map_err(|error| error.to_string())?;
    print_json(&json!({
        "schema_version": 1,
        "connection": connection,
        "capabilities": capabilities
    }))
}

async fn postgres_describe(args: &mut impl Iterator<Item = String>) -> Result<(), String> {
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
        .await
        .map_err(|error| error.to_string())?;
    print_json(
        &serde_json::to_value(inspection).map_err(|_| "output non serializzabile".to_owned())?,
    )
}

async fn postgres_read_summary(args: &mut impl Iterator<Item = String>) -> Result<(), String> {
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
            &ResourceBudget::new(ResourceLimits::default()).map_err(|error| error.to_string())?,
            &CancellationToken::new(),
        )
        .await
        .map_err(|error| error.to_string())?;
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
    while let Some(batch) = stream
        .next_batch()
        .await
        .map_err(|error| error.to_string())?
    {
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

const fn object_ref(schema: String, object: String) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some(schema),
        object,
        layer_id: None,
    }
}

fn secret_from_env(name: &str) -> Result<SecretString, String> {
    if name.is_empty() || name.contains('=') {
        return Err("nome variabile DSN non valido".to_owned());
    }
    env::var(name)
        .map(SecretString::new)
        .map_err(|_| "variabile DSN assente".to_owned())
}

fn one_argument(args: &mut impl Iterator<Item = String>, missing: &str) -> Result<String, String> {
    let value = args.next().ok_or_else(|| missing.to_owned())?;
    ensure_end(args)?;
    Ok(value)
}

fn ensure_end(args: &mut impl Iterator<Item = String>) -> Result<(), String> {
    if args.next().is_some() {
        Err("troppi argomenti".to_owned())
    } else {
        Ok(())
    }
}

fn print_json(value: &serde_json::Value) -> Result<(), String> {
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
    ]
    .join("\n")
}
mod inspect_dataset;
