//! Sub-comandi MySQL v1.2 — parity iniziale col path Postgres.
//!
//! Sette comandi essenziali:
//! - `mysql-probe`: test connessione + capabilities
//! - `mysql-describe`: describe object (columns, types, keys)
//! - `mysql-execute-scalar`: SELECT scalare
//! - `mysql-execute-sql`: INSERT/UPDATE/DELETE raw
//! - `mysql-execute-ddl`: CREATE/DROP/ALTER
//! - `mysql-inspect-schemas`: list schemas
//! - `mysql-inspect-tables`: list tables in schema
//!
//! Pattern argomenti standardizzato:
//! `mysql-<cmd> <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>] <cmd-args>`
//!
//! Il pool è dimensionato piccolo (2 conn) — questi sono comandi one-shot
//! diagnostici / operativi, non long-running.

#![cfg(feature = "mysql")]
#![allow(
    clippy::doc_markdown,
    clippy::option_if_let_else,
)]

use crate::{secret_from_env, CliResult};
use plenora_database_core::plan::{Operation, ProviderKind};
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::CancellationToken;
use plenora_db_mysql::{MysqlCertificatePolicy, MysqlConfig, MysqlProvider};
use serde_json::json;
use std::path::PathBuf;

use crate::print_json;

/// Costruisce un `MysqlProvider` parsando gli argomenti standard:
/// `<PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>]`.
///
/// Restituisce `(provider, secret, remaining_args)` — il caller consuma
/// gli argomenti successivi (SQL, schema/object, ecc.).
fn mysql_provider_from_args(
    args: &mut impl Iterator<Item = String>,
) -> CliResult<(MysqlProvider, SecretString, Vec<String>)> {
    let pwd_env = args
        .next()
        .ok_or_else(|| "manca il nome della variabile PASSWORD".to_owned())?;
    let host = args.next().ok_or_else(|| "manca host MySQL".to_owned())?;
    let database = args
        .next()
        .ok_or_else(|| "manca database MySQL".to_owned())?;
    let username = args
        .next()
        .ok_or_else(|| "manca username MySQL".to_owned())?;
    let mut remaining: Vec<String> = args.collect();
    // Port (opzionale, primo argomento se numerico e non inizia con --)
    let port: Option<u16> = if remaining
        .first()
        .is_some_and(|v| !v.starts_with("--") && v.parse::<u16>().is_ok())
    {
        Some(
            remaining
                .remove(0)
                .parse::<u16>()
                .map_err(|_| "porta MySQL non valida".to_owned())?,
        )
    } else {
        None
    };
    // TLS CA path (opzionale)
    let ca_pem: Option<Vec<u8>> = if remaining.first().map(String::as_str)
        == Some("--tls-ca-path-env")
    {
        remaining.remove(0);
        let ca_env = remaining
            .first()
            .ok_or_else(|| "--tls-ca-path-env richiede il nome della variabile".to_owned())?
            .clone();
        remaining.remove(0);
        let path_str = std::env::var(&ca_env)
            .map_err(|_| format!("variabile TLS CA env non trovata: {ca_env}"))?;
        let path = PathBuf::from(path_str);
        let pem = std::fs::read(&path)
            .map_err(|e| format!("lettura CA MySQL fallita: {e}"))?;
        if pem.len() > 1024 * 1024 {
            return Err("CA PEM MySQL oltre 1 MiB".to_owned().into());
        }
        Some(pem)
    } else {
        None
    };
    let secret = secret_from_env(&pwd_env)?;
    let mut config = MysqlConfig::new(host, database, username, secret.clone());
    if let Some(p) = port {
        config = config.with_port(p);
    }
    if let Some(pem) = ca_pem {
        config = config.with_private_ca_certificate_pem(pem);
    } else {
        // Default sviluppo: trust server certificate. Produzione dovrebbe
        // sempre specificare una CA privata via --tls-ca-path-env.
        config = config.with_certificate_policy(MysqlCertificatePolicy::TrustServerCertificate);
    }
    let provider = MysqlProvider::new(config, 2)?;
    Ok((provider, secret, remaining))
}

/// `mysql-probe <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>]`
pub async fn mysql_probe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    if !remaining.is_empty() {
        return Err(format!("argomenti extra non attesi: {remaining:?}").into());
    }
    let cancellation = CancellationToken::new();
    let connection = provider.test_connection(&secret, &cancellation).await?;
    let capabilities = provider.probe_capabilities(&secret, &cancellation).await?;
    print_json(&json!({
        "schema_version": 1,
        "provider": "mysql",
        "connection": connection,
        "capabilities": capabilities,
    }))
}

/// `mysql-describe <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>] <schema> <object>`
pub async fn mysql_describe(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    let mut it = remaining.into_iter();
    let schema = it.next().ok_or_else(|| "manca lo schema".to_owned())?;
    let object = it.next().ok_or_else(|| "manca l'oggetto".to_owned())?;
    if it.next().is_some() {
        return Err("argomenti extra dopo <object>".to_owned().into());
    }
    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: plenora_database_core::plan::ObjectRef {
                    catalog: None,
                    schema: Some(schema),
                    object,
                    layer_id: None,
                },
            },
            &CancellationToken::new(),
        )
        .await?;
    print_json(
        &serde_json::to_value(inspection).map_err(|_| "output non serializzabile".to_owned())?,
    )
}

/// `mysql-inspect-schemas <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>]`
pub async fn mysql_inspect_schemas(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    if !remaining.is_empty() {
        return Err(format!("argomenti extra non attesi: {remaining:?}").into());
    }
    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseListSchemas { source: None },
            &CancellationToken::new(),
        )
        .await?;
    print_json(
        &serde_json::to_value(inspection).map_err(|_| "output non serializzabile".to_owned())?,
    )
}

/// `mysql-inspect-tables <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>] <schema>`
pub async fn mysql_inspect_tables(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    let mut it = remaining.into_iter();
    let schema = it.next().ok_or_else(|| "manca lo schema".to_owned())?;
    if it.next().is_some() {
        return Err("argomenti extra dopo <schema>".to_owned().into());
    }
    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseListObjects {
                source: Some(plenora_database_core::plan::ObjectRef {
                    catalog: None,
                    schema: Some(schema),
                    object: String::new(),
                    layer_id: None,
                }),
            },
            &CancellationToken::new(),
        )
        .await?;
    print_json(
        &serde_json::to_value(inspection).map_err(|_| "output non serializzabile".to_owned())?,
    )
}

/// `mysql-execute-sql <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>] <sql>`
///
/// Statement DML raw (INSERT/UPDATE/DELETE). Nessun parametro. Uso una
/// transazione applicativa dedicata (BEGIN/COMMIT via `begin_transaction`)
/// per garantire outcome deterministico.
pub async fn mysql_execute_sql(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    let mut it = remaining.into_iter();
    let sql = it.next().ok_or_else(|| "manca lo statement SQL".to_owned())?;
    if it.next().is_some() {
        return Err("argomenti extra dopo <sql>".to_owned().into());
    }
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut tx = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let stmt = plenora_database_core::transaction::Statement {
        sql,
        params: Vec::new(),
    };
    let affected = tx.execute(&stmt, &cancellation).await?;
    let commit_outcome = tx.commit(&cancellation).await?;
    print_json(&json!({
        "provider": "mysql",
        "affected_rows": affected,
        "commit": format!("{commit_outcome:?}").to_lowercase(),
    }))
}

/// `mysql-execute-ddl <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>] <sql>`
///
/// DDL raw (CREATE/DROP/ALTER). MySQL fa autocommit implicito su DDL.
pub async fn mysql_execute_ddl(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    let mut it = remaining.into_iter();
    let sql = it.next().ok_or_else(|| "manca lo statement DDL".to_owned())?;
    if it.next().is_some() {
        return Err("argomenti extra dopo <sql>".to_owned().into());
    }
    provider
        .execute_ddl(&secret, &sql, &CancellationToken::new())
        .await?;
    print_json(&json!({
        "provider": "mysql",
        "status": "ok",
    }))
}

/// `mysql-execute-scalar <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>] <sql>`
///
/// SELECT scalare (una sola riga/colonna). Stampa il valore come JSON.
pub async fn mysql_execute_scalar(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    let mut it = remaining.into_iter();
    let sql = it.next().ok_or_else(|| "manca lo statement SQL".to_owned())?;
    if it.next().is_some() {
        return Err("argomenti extra dopo <sql>".to_owned().into());
    }
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut tx = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let stmt = plenora_database_core::transaction::Statement {
        sql,
        params: Vec::new(),
    };
    let rows = tx.query(&stmt, &cancellation).await?;
    let _ = tx.rollback(&cancellation).await;
    let value = rows.first().and_then(|row| row.values().first()).map_or_else(
        || "null".to_owned(),
        |param| format!("{param:?}"),
    );
    print_json(&json!({
        "provider": "mysql",
        "value": value,
        "rows_returned": rows.len(),
    }))
}

/// `mysql-conditional-update <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>] <UPDATE_SQL> <EXPECTED_AFFECTED>`
///
/// Pattern optimistic-lock: esegue UPDATE in una tx dedicata, verifica
/// affected_rows == expected. Se mismatch → `ConcurrentModification`.
pub async fn mysql_conditional_update(
    args: &mut impl Iterator<Item = String>,
) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    let mut it = remaining.into_iter();
    let update_sql = it
        .next()
        .ok_or_else(|| "manca UPDATE_SQL".to_owned())?;
    let expected_str = it
        .next()
        .ok_or_else(|| "manca EXPECTED_AFFECTED (numero intero)".to_owned())?;
    let expected: u64 = expected_str
        .parse()
        .map_err(|_| format!("EXPECTED_AFFECTED non numerico: {expected_str}"))?;
    if it.next().is_some() {
        return Err("argomenti extra dopo EXPECTED_AFFECTED".to_owned().into());
    }
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut tx = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let update = plenora_database_core::transaction::Statement {
        sql: update_sql,
        params: Vec::new(),
    };
    let request = plenora_database_core::transaction::ConditionalUpdate {
        update: &update,
        key_probe: None,
        expected_affected_rows: expected,
    };
    let result = tx.execute_conditional_update(request, &cancellation).await;
    match result {
        Ok(()) => {
            let commit = tx.commit(&cancellation).await?;
            print_json(&json!({
                "provider": "mysql",
                "outcome": "matched",
                "expected_affected": expected,
                "commit": format!("{commit:?}").to_lowercase(),
            }))
        }
        Err(e) if e.category == plenora_database_core::ErrorCategory::ConcurrentModification => {
            let _ = tx.rollback(&cancellation).await;
            print_json(&json!({
                "provider": "mysql",
                "outcome": "concurrent_modification",
                "expected_affected": expected,
                "message": e.message,
            }))
        }
        Err(e) => {
            let _ = tx.rollback(&cancellation).await;
            Err(e.into())
        }
    }
}

/// `mysql-portable-execute` — **non disponibile** (v1.2).
///
/// Il facade `execute_portable` del core supporta il compile-portable solo
/// per Postgres. Estendere a MySQL richiede refactor cross-crate del
/// facade + un dispatcher `compile_portable_for_provider(provider_kind)`
/// non ancora implementato. Deferito a giro futuro.
#[allow(dead_code)]
async fn mysql_portable_execute_unused(
    args: &mut impl Iterator<Item = String>,
) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    let mut it = remaining.into_iter();
    let path = it
        .next()
        .ok_or_else(|| "manca PORTABLE.json path".to_owned())?;
    if it.next().is_some() {
        return Err("argomenti extra dopo PORTABLE.json".to_owned().into());
    }
    let json_str = std::fs::read_to_string(&path)
        .map_err(|e| format!("lettura {path} fallita: {e}"))?;
    let portable: plenora_database_core::portable::PortableStatement =
        serde_json::from_str(&json_str)
            .map_err(|e| format!("parse PortableStatement JSON fallito: {e}"))?;
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut tx = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    // Semplificazione: solo Select ritorna rows deterministicamente.
    // Insert/Update/Delete/Upsert con RETURNING (Postgres-only) non
    // sono tipicamente usati con MySQL — se emerge use case, il caller
    // può usare execute_portable_returning direttamente.
    let returns_rows = matches!(
        portable,
        plenora_database_core::portable::PortableStatement::Select(_)
    );
    let result = if returns_rows {
        let rows = plenora_database_core::facade::execute_portable_returning(
            tx.as_mut(),
            &portable,
            &cancellation,
        )
        .await?;
        let commit = tx.commit(&cancellation).await?;
        let json_rows: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|row| {
                let names: Vec<String> = row.columns().to_vec();
                let values: Vec<plenora_database_core::provider::ParameterValue> =
                    row.values().to_vec();
                let mut obj = serde_json::Map::new();
                for (name, value) in names.iter().zip(values.iter()) {
                    obj.insert(
                        name.clone(),
                        serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
                    );
                }
                serde_json::Value::Object(obj)
            })
            .collect();
        json!({
            "provider": "mysql",
            "kind": "select",
            "rows": json_rows,
            "row_count": json_rows.len(),
            "commit": format!("{commit:?}").to_lowercase(),
        })
    } else {
        let affected = plenora_database_core::facade::execute_portable(
            tx.as_mut(),
            &portable,
            &cancellation,
        )
        .await?;
        let commit = tx.commit(&cancellation).await?;
        json!({
            "provider": "mysql",
            "kind": "dml",
            "affected_rows": affected,
            "commit": format!("{commit:?}").to_lowercase(),
        })
    };
    print_json(&result)
}

/// `mysql-transaction-test <PWD_ENV> <host> <database> <user> [port] [--tls-ca-path-env <name>]`
///
/// Test smoke OLTP end-to-end: crea temp table, INSERT + SAVEPOINT +
/// INSERT + ROLLBACK TO SAVEPOINT + COMMIT + verifica riga persistita.
/// Cleanup del temp table alla fine.
pub async fn mysql_transaction_test(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let (provider, secret, remaining) = mysql_provider_from_args(args)?;
    if !remaining.is_empty() {
        return Err(format!("argomenti extra non attesi: {remaining:?}").into());
    }
    let cancellation = CancellationToken::new();
    let table = format!("_cli_txtest_{}", std::process::id());
    // Setup
    provider
        .execute_ddl(&secret, &format!("DROP TABLE IF EXISTS {table}"), &cancellation)
        .await?;
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
            &cancellation,
        )
        .await?;
    // TX scenario
    let budget = ResourceBudget::new(ResourceLimits::default())?;
    let mut tx = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let stmt1 = plenora_database_core::transaction::Statement {
        sql: format!("INSERT INTO {table} (id) VALUES (1)"),
        params: Vec::new(),
    };
    tx.execute(&stmt1, &cancellation).await?;
    tx.savepoint("sp1", &cancellation).await?;
    let stmt2 = plenora_database_core::transaction::Statement {
        sql: format!("INSERT INTO {table} (id) VALUES (2)"),
        params: Vec::new(),
    };
    tx.execute(&stmt2, &cancellation).await?;
    tx.rollback_to_savepoint("sp1", &cancellation).await?;
    tx.release_savepoint("sp1", &cancellation).await?;
    let commit = tx.commit(&cancellation).await?;
    // Verify
    let mut verify_tx = provider
        .begin_transaction(
            &secret,
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let count_stmt = plenora_database_core::transaction::Statement {
        sql: format!("SELECT COUNT(*) FROM {table}"),
        params: Vec::new(),
    };
    let rows = verify_tx.query(&count_stmt, &cancellation).await?;
    let _ = verify_tx.rollback(&cancellation).await;
    let count_str = rows.first().and_then(|row| row.values().first()).map_or_else(
        || "?".to_owned(),
        |v| format!("{v:?}"),
    );
    // Cleanup
    provider
        .execute_ddl(&secret, &format!("DROP TABLE {table}"), &cancellation)
        .await?;
    print_json(&json!({
        "provider": "mysql",
        "kind": ProviderKind::Mysql,
        "scenario": "insert+savepoint+rollback_to_sp+commit",
        "expected_rows_after_commit": 1,
        "actual_rows": count_str,
        "commit": format!("{commit:?}").to_lowercase(),
        "status": "ok",
    }))
}
