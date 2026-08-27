//! Orchestrazione transaction-per-call condivisa dalle sessioni Python.

use plenora_database_core::provider::{Provider, ProviderFuture, SecretString};
use plenora_database_core::transaction::{AccessMode, TransactionOptions, TransactionScope};
use plenora_database_core::{CancellationToken, Result};
use pyo3::PyResult;
use std::sync::Arc;

/// Traduce una sola volta le opzioni comuni alle quattro superfici sessione.
pub fn transaction_options(
    isolation: Option<&str>,
    read_only: Option<bool>,
    deferrable: Option<bool>,
    statement_timeout_ms: Option<u64>,
    context: Option<crate::session_context_py::PySessionContext>,
    native_query_policy: Option<&str>,
) -> PyResult<TransactionOptions> {
    Ok(TransactionOptions {
        isolation: isolation
            .map(crate::transaction::parse_isolation)
            .transpose()?,
        access_mode: read_only.map(|value| {
            if value {
                AccessMode::ReadOnly
            } else {
                AccessMode::ReadWrite
            }
        }),
        deferrable,
        statement_timeout_ms,
        context: context.map_or_else(Default::default, |value| value.inner),
        native_query_policy: native_query_policy
            .map(crate::transaction::parse_native_query_policy)
            .transpose()?
            .unwrap_or_default(),
    })
}

/// Esegue una callback in una transazione dedicata e pubblica il risultato
/// soltanto dopo una conferma certa del commit.
pub async fn run_transaction<R, F>(
    provider: Arc<dyn Provider>,
    secret: SecretString,
    work: F,
) -> Result<R>
where
    F: for<'a> FnOnce(&'a mut dyn TransactionScope, &'a CancellationToken) -> ProviderFuture<'a, R>
        + Send,
    R: Send,
{
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &crate::budget::session_budget(),
            &cancellation,
        )
        .await?;
    let result = work(transaction.as_mut(), &cancellation).await;
    match result {
        Ok(value) => {
            let provider_kind = transaction.provider_kind();
            let outcome = transaction.commit(&cancellation).await?;
            if !outcome.is_committed() {
                return Err(crate::errors_commit::commit_outcome_unknown(provider_kind));
            }
            Ok(value)
        }
        Err(error) => {
            let _ = transaction.rollback(&cancellation).await;
            Err(error)
        }
    }
}
