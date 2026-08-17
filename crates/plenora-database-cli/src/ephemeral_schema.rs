//! Schema effimero dei comandi di test `PostgreSQL`.
//!
//! Separato da `safety`, che resta provider-neutral: qui si emette SQL —
//! `CREATE SCHEMA`, `DROP SCHEMA ... CASCADE` — attraverso il provider PFM,
//! quindi il modulo esiste solo con la feature `postgres`. Finche stava
//! insieme al parsing dei flag, `--allow-write-tests` non si poteva leggere
//! senza compilare `PostgreSQL`, e il binario con il solo adapter `MySQL` non si
//! costruiva.

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::safety::active;
use crate::{secret_from_env, CliResult};
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;

/// Crea lo schema effimero se richiesto. È no-op quando `--ephemeral-schema`
/// non è impostato. Il chiamante deve poi chiamare `drop_ephemeral_schema`
/// al termine (best-effort).
pub(crate) async fn ensure_ephemeral_schema(dsn_env: &str) -> CliResult<Option<String>> {
    let Some(name) = active().ephemeral_schema else {
        return Ok(None);
    };
    let secret = secret_from_env(dsn_env)?;
    run_admin_stmt(&secret, &format!("CREATE SCHEMA IF NOT EXISTS \"{name}\"")).await?;
    Ok(Some(name))
}

pub(crate) async fn drop_ephemeral_schema(dsn_env: &str, name: &str) {
    if let Ok(secret) = secret_from_env(dsn_env) {
        let _ = run_admin_stmt(
            &secret,
            &format!("DROP SCHEMA IF EXISTS \"{name}\" CASCADE"),
        )
        .await;
    }
}

async fn run_admin_stmt(secret: &SecretString, sql: &str) -> CliResult<()> {
    let provider = postgres_provider_for_pfm()?;
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    tx.execute(&Statement::new(sql.to_owned()), &cancel).await?;
    tx.commit(&cancel).await?;
    Ok(())
}
