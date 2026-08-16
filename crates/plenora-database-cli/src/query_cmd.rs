#![allow(clippy::doc_markdown)]
//! Comandi CLI legati alla `QueryOperation` AST + Portable AST:
//! `postgres-query`, `portable-compile`, `portable-execute`.

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliError, CliResult};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::portable::{compile_portable, PortableStatement};
use plenora_database_core::provider::{ParameterBag, Provider};
use plenora_database_core::query::QueryOperation;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use serde_json::{json, Value};
use std::fs;

/// `postgres-query QUERY.json`: legge un `QueryOperation` da file JSON,
/// esegue via `Provider::query` (Arrow stream), materializza in memoria e
/// stampa summary (schema + rowcount). Per output Arrow IPC su file, usa
/// `postgres-read-ipc` (che va tramite `ReadOperation`).
pub(crate) async fn postgres_query(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let query_path = args.next().ok_or("manca il percorso QUERY.json")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let contents =
        fs::read(&query_path).map_err(|_| format!("QUERY.json non leggibile: {query_path}"))?;
    let operation: QueryOperation =
        serde_json::from_slice(&contents).map_err(|e| format!("QUERY.json non parsabile: {e}"))?;

    let provider = postgres_provider_for_pfm();
    let budget = ResourceBudget::new(ResourceLimits::default()).map_err(CliError::from)?;
    let cancel = CancellationToken::new();

    let mut stream = provider
        .query(
            &secret,
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancel,
        )
        .await?;

    let schema = stream.schema();
    let fields: Vec<Value> = schema
        .fields()
        .iter()
        .map(|f| {
            json!({
                "name": f.name(),
                "data_type": f.data_type().to_string(),
                "nullable": f.is_nullable(),
            })
        })
        .collect();

    let mut batches = 0_u64;
    let mut rows = 0_u64;
    while let Some(batch) = stream.next_batch(&cancel).await? {
        batches += 1;
        rows += u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    }

    print_json(&json!({
        "status": "ok",
        "provider": ProviderKind::Postgres,
        "batches": batches,
        "rows": rows,
        "fields": fields,
    }))
}

/// `portable-compile PROVIDER PORTABLE.json`: compila il `PortableStatement`
/// per il provider target (postgres/mysql/sqlserver) e stampa SQL + numero
/// di parametri. Utile per debug pipeline PFM senza dover eseguire.
pub(crate) fn portable_compile(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let provider = args
        .next()
        .ok_or("manca il provider (postgres|mysql|sqlserver)")?;
    let portable_path = args.next().ok_or("manca il percorso PORTABLE.json")?;
    ensure_end(args)?;

    let kind = match provider.as_str() {
        "postgres" => ProviderKind::Postgres,
        "mysql" => ProviderKind::Mysql,
        "sqlserver" => ProviderKind::Sqlserver,
        other => return Err(format!("provider sconosciuto: {other}").into()),
    };
    let contents = fs::read(&portable_path)
        .map_err(|_| format!("PORTABLE.json non leggibile: {portable_path}"))?;
    let ast: PortableStatement = serde_json::from_slice(&contents)
        .map_err(|e| format!("PORTABLE.json non parsabile: {e}"))?;
    let compiled = compile_portable(kind, &ast)?;

    print_json(&json!({
        "status": "ok",
        "provider": kind,
        "sql": compiled.sql,
        "param_count": compiled.params.len(),
        // Non stampiamo i params: possono contenere valori sensibili.
    }))
}

/// `portable-execute DSN_ENV PORTABLE.json`: compila per Postgres, apre una
/// tx, esegue e stampa il risultato come JSON (rows per SELECT/*RETURNING,
/// affected_rows altrimenti).
pub(crate) async fn portable_execute(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let portable_path = args.next().ok_or("manca il percorso PORTABLE.json")?;
    ensure_end(args)?;

    let contents = fs::read(&portable_path)
        .map_err(|_| format!("PORTABLE.json non leggibile: {portable_path}"))?;
    let ast: PortableStatement = serde_json::from_slice(&contents)
        .map_err(|e| format!("PORTABLE.json non parsabile: {e}"))?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let cancel = CancellationToken::new();
    let budget = pfm_budget()?;

    let opts = TransactionOptions {
        context: crate::session_ctx::active(),
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(&secret, &opts, &budget, &cancel)
        .await?;

    let compiled = compile_portable(ProviderKind::Postgres, &ast)?;
    let stmt = Statement::new(compiled.sql.clone()).with_params(compiled.params);

    // Determina se lo statement produce righe:
    // - PortableStatement::Select → sempre
    // - Insert/Update/Delete/Upsert con RETURNING non vuoto → sì
    let has_rows = match &ast {
        PortableStatement::Select(_) => true,
        PortableStatement::Insert(s) => !s.returning.is_empty(),
        PortableStatement::Update(s) => !s.returning.is_empty(),
        PortableStatement::Delete(s) => !s.returning.is_empty(),
        PortableStatement::Upsert(s) => !s.returning.is_empty(),
    };
    let payload = if has_rows {
        let rows = tx.query(&stmt, &cancel).await?;
        let out: Vec<Value> = rows
            .iter()
            .map(|r| {
                let mut obj = serde_json::Map::new();
                for (i, col) in r.columns().iter().enumerate() {
                    let value = r
                        .get_index(i)
                        .and_then(|v| serde_json::to_value(v).ok())
                        .unwrap_or(Value::Null);
                    obj.insert(col.clone(), value);
                }
                Value::Object(obj)
            })
            .collect();
        json!({ "kind": "rows", "rows": out, "count": rows.len() })
    } else {
        let affected = tx.execute(&stmt, &cancel).await?;
        json!({ "kind": "affected_rows", "count": affected })
    };
    let commit = tx.commit(&cancel).await?;
    print_json(&json!({
        "status": "ok",
        "provider": ProviderKind::Postgres,
        "compiled_sql": compiled.sql,
        "commit": commit,
        "result": payload,
    }))
}
