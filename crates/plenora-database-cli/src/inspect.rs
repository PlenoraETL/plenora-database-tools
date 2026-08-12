//! Sottocomandi introspection: inspect-database, inspect-schemas,
//! inspect-tables. Il consumer PFM li usa per esplorare uno schema on-premise
//! durante bootstrap/troubleshooting senza aprire una sessione psql.

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{ensure_end, print_json, secret_from_env, CliResult};
use plenora_database_core::provider::{ParameterValue, Provider};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use serde_json::{json, Value};

pub(crate) async fn inspect_database(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;

    let meta_rows = tx
        .query(
            &Statement::new(
                "SELECT current_database()::TEXT, current_user::TEXT, \
                 version(), current_setting('server_version_num')::TEXT, \
                 current_setting('server_encoding'), current_setting('TimeZone'), \
                 pg_size_pretty(pg_database_size(current_database()))",
            ),
            &cancel,
        )
        .await?;
    let meta = meta_rows.first();
    let database = value_at_string(meta, 0);
    let user = value_at_string(meta, 1);
    let version = value_at_string(meta, 2);
    let version_num = value_at_string(meta, 3);
    let encoding = value_at_string(meta, 4);
    let timezone = value_at_string(meta, 5);
    let size = value_at_string(meta, 6);

    let ext_rows = tx
        .query(
            &Statement::new(
                "SELECT extname::TEXT, extversion::TEXT \
                 FROM pg_extension ORDER BY extname",
            ),
            &cancel,
        )
        .await?;
    let extensions: Vec<Value> = ext_rows
        .iter()
        .map(|r| {
            json!({
                "name": value_at_string(Some(r), 0),
                "version": value_at_string(Some(r), 1),
            })
        })
        .collect();

    let _ = tx.rollback(&cancel).await;
    print_json(&json!({
        "database": database,
        "user": user,
        "version": version,
        "version_num": version_num,
        "encoding": encoding,
        "timezone": timezone,
        "size": size,
        "extensions": extensions,
    }))
}

pub(crate) async fn inspect_schemas(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    let rows = tx
        .query(
            &Statement::new(
                "SELECT n.nspname::TEXT, r.rolname::TEXT, \
                 obj_description(n.oid, 'pg_namespace')::TEXT \
                 FROM pg_namespace n JOIN pg_roles r ON n.nspowner = r.oid \
                 WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast') \
                   AND n.nspname NOT LIKE 'pg_temp_%' \
                   AND n.nspname NOT LIKE 'pg_toast_temp_%' \
                 ORDER BY n.nspname",
            ),
            &cancel,
        )
        .await?;
    let _ = tx.rollback(&cancel).await;

    let schemas: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "name": value_at_string(Some(r), 0),
                "owner": value_at_string(Some(r), 1),
                "comment": value_at_string(Some(r), 2),
            })
        })
        .collect();
    print_json(&json!({ "count": schemas.len(), "schemas": schemas }))
}

pub(crate) async fn inspect_tables(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let dsn_env = args.next().ok_or("manca variabile ambiente DSN")?;
    let schema = args.next().ok_or("manca lo schema")?;
    ensure_end(args)?;

    let secret = secret_from_env(&dsn_env)?;
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();

    let mut tx = provider
        .begin_transaction(&secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    let rows = tx
        .query(
            &Statement::new(
                "SELECT c.relname::TEXT, \
                        CASE c.relkind \
                          WHEN 'r' THEN 'table' \
                          WHEN 'p' THEN 'partitioned_table' \
                          WHEN 'v' THEN 'view' \
                          WHEN 'm' THEN 'materialized_view' \
                          WHEN 'f' THEN 'foreign_table' \
                          ELSE c.relkind::TEXT END, \
                        c.reltuples::BIGINT, \
                        pg_size_pretty(pg_total_relation_size(c.oid)), \
                        obj_description(c.oid, 'pg_class')::TEXT \
                 FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid \
                 WHERE n.nspname = $1 \
                   AND c.relkind IN ('r','p','v','m','f') \
                 ORDER BY c.relname",
            )
            .with_params(vec![ParameterValue::String(schema.clone())]),
            &cancel,
        )
        .await?;
    let _ = tx.rollback(&cancel).await;

    let tables: Vec<Value> = rows
        .iter()
        .map(|r| {
            json!({
                "name": value_at_string(Some(r), 0),
                "kind": value_at_string(Some(r), 1),
                "estimated_rows": value_at_i64(Some(r), 2),
                "total_size": value_at_string(Some(r), 3),
                "comment": value_at_string(Some(r), 4),
            })
        })
        .collect();
    print_json(&json!({
        "schema": schema,
        "count": tables.len(),
        "tables": tables,
    }))
}

fn value_at_string(row: Option<&plenora_database_core::Row>, idx: usize) -> Value {
    match row.and_then(|r| r.get_index(idx)) {
        Some(ParameterValue::String(s)) => Value::String(s.clone()),
        Some(ParameterValue::Null { .. }) | None => Value::Null,
        Some(other) => serde_json::to_value(other).unwrap_or(Value::Null),
    }
}

fn value_at_i64(row: Option<&plenora_database_core::Row>, idx: usize) -> Value {
    match row.and_then(|r| r.get_index(idx)) {
        Some(ParameterValue::I64(v)) => json!(v),
        Some(ParameterValue::I32(v)) => json!(v),
        Some(ParameterValue::Null { .. }) | None => Value::Null,
        Some(other) => serde_json::to_value(other).unwrap_or(Value::Null),
    }
}
