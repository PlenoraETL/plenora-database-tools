//! Bulk write MySQL via `Provider::prepare_write` + `Provider::write`.
//!
//! Chiamato da `MysqlSession.copy_from`. Riusa gli helper generici in
//! `crate::write` (parse_mode/parse_profile/parse_mapping_policy,
//! decode_ipc_stream, make_operation, default_budget, VecBatchStream,
//! outcome_into_py, wrap_outcome) — differisce solo per il tipo di
//! provider (`Arc<MysqlProvider>` vs `Arc<PostgresProvider>`).

#![allow(
    clippy::doc_markdown,
    clippy::missing_const_for_fn,
    clippy::needless_pass_by_value,
    clippy::future_not_send,
    clippy::significant_drop_tightening,
    clippy::redundant_pub_crate,
    clippy::too_many_arguments,
)]

use crate::runtime;
use crate::write::{
    decode_ipc_stream, default_budget, make_operation, parse_mapping_policy, parse_mode,
    parse_profile, VecBatchStream,
};
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::CancellationToken;
use plenora_database_core::DatabaseError;
use plenora_db_mysql::MysqlProvider;
use std::sync::Arc;

async fn do_copy_from_async_mysql(
    provider: Arc<MysqlProvider>,
    secret: SecretString,
    schema_name: String,
    table_name: String,
    ipc_bytes: Vec<u8>,
    mode: &str,
    transaction_profile: &str,
    mapping_policy: &str,
    keys: Vec<String>,
    update_columns: Vec<String>,
) -> Result<WriteOutcome, DatabaseError> {
    let mode_enum = parse_mode(mode)?;
    let profile_enum = parse_profile(transaction_profile)?;
    let policy_enum = parse_mapping_policy(mapping_policy)?;
    let (input_schema, batches, declared_rows) = decode_ipc_stream(&ipc_bytes)?;
    let stream = VecBatchStream {
        schema: Arc::clone(&input_schema),
        batches,
        declared_rows,
    };
    let operation = make_operation(
        &schema_name,
        &table_name,
        mode_enum,
        profile_enum,
        policy_enum,
        keys,
        update_columns,
    )?;
    let budget = default_budget();
    let cancel = CancellationToken::new();
    let prepared = provider
        .prepare_write(&secret, &operation, input_schema, &budget, &cancel)
        .await?;
    let outcome = provider
        .write(&secret, prepared, Box::new(stream), &budget, &cancel)
        .await?;
    Ok(outcome)
}

/// Bulk write MySQL sync (blocca il thread Python sul runtime tokio globale).
///
/// # Errors
///
/// `DatabaseError` in caso di IPC malformato, mode/profile/policy invalidi,
/// keys mancanti per mode che le richiedono, o errore del provider durante
/// prepare/write.
pub(crate) fn copy_from_sync_mysql(
    provider: &Arc<MysqlProvider>,
    secret: &SecretString,
    schema: &str,
    table: &str,
    ipc_bytes: &[u8],
    mode: &str,
    transaction_profile: &str,
    mapping_policy: &str,
    keys: Vec<String>,
    update_columns: Vec<String>,
) -> Result<WriteOutcome, DatabaseError> {
    let provider_arc = Arc::clone(provider);
    let secret_owned = secret.clone();
    let schema_owned = schema.to_owned();
    let table_owned = table.to_owned();
    let ipc_owned = ipc_bytes.to_vec();
    let mode_owned = mode.to_owned();
    let profile_owned = transaction_profile.to_owned();
    let policy_owned = mapping_policy.to_owned();
    runtime().block_on(async move {
        do_copy_from_async_mysql(
            provider_arc,
            secret_owned,
            schema_owned,
            table_owned,
            ipc_owned,
            &mode_owned,
            &profile_owned,
            &policy_owned,
            keys,
            update_columns,
        )
        .await
    })
}
