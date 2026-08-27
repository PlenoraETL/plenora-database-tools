//! Retry executor che rispetta `RetryDisposition` di `DatabaseError`.
//!
//! Il core classifica ogni errore con la disposition di retry canonica; questo
//! modulo esegue l'orchestrazione:
//!
//! - `Safe` → retry immediato fino a `max_attempts`
//! - `After(ms)` → sleep + retry
//! - `Never`, `Quarantine`, `RequiresRecovery`, `RequiresIdempotencyKey`
//!   → propaga l'errore senza retry (il consumer deve gestire fuori banda)
//!
//! Il helper è **cancellation-aware**: interrompe attesa e retry se il
//! `CancellationToken` viene cancellato.

use plenora_database_core::{CancellationToken, DatabaseError, Result, RetryDisposition};
use std::future::Future;
use std::time::Duration;

/// Politica retry: parametri di alto livello.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    /// Numero massimo di *tentativi* (comprensivo del primo). `1` = nessun retry.
    pub max_attempts: u32,
    /// Cap opzionale al ritardo di ciascun retry (bounded backoff).
    /// Se `None`, rispetta esattamente il valore di `RetryDisposition::After`.
    pub max_delay_ms: Option<u64>,
}

impl RetryPolicy {
    #[must_use]
    pub const fn new(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            max_delay_ms: None,
        }
    }

    /// Politica minima: 3 tentativi totali, delay capped a 5 secondi.
    #[must_use]
    pub const fn default_conservative() -> Self {
        Self {
            max_attempts: 3,
            max_delay_ms: Some(5_000),
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::default_conservative()
    }
}

/// Esegue `op` fino a `policy.max_attempts` volte, rispettando la
/// `RetryDisposition` di ciascun errore.
///
/// `op` riceve il numero di tentativo (0-based) e ritorna un Future.
/// L'ultimo errore osservato è propagato se tutti i tentativi falliscono.
///
/// # Errors
///
/// Propaga l'errore dell'ultimo tentativo (o l'errore non-retriable se
/// incontrato prima).
///
/// # Panics
///
/// Non panics: gli `expect()` interni sono protetti dai controlli sui
/// campi `last_error` che precedono le loro chiamate.
pub async fn retry_with_policy<F, Fut, T>(
    op: F,
    policy: RetryPolicy,
    cancellation: &CancellationToken,
) -> Result<T>
where
    F: Fn(u32) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    if policy.max_attempts == 0 {
        return Err(DatabaseError::invalid_plan(
            "RetryPolicy.max_attempts deve essere >= 1",
        ));
    }
    let mut last_error: Option<DatabaseError> = None;
    for attempt in 0..policy.max_attempts {
        if cancellation.is_cancelled() {
            return Err(interrupted_error(last_error.as_ref(), cancellation));
        }
        match op(attempt).await {
            Ok(value) => return Ok(value),
            Err(error) => {
                let disposition = error.retry;
                last_error = Some(error);
                let is_last = attempt + 1 >= policy.max_attempts;
                if is_last {
                    break;
                }
                match disposition {
                    RetryDisposition::Safe => {
                        // Continue immediately.
                    }
                    RetryDisposition::After(ms) => {
                        let ms = policy.max_delay_ms.map_or(ms, |cap| ms.min(cap));
                        if sleep_cancellable(cancellation, ms).await {
                            return Err(interrupted_error(last_error.as_ref(), cancellation));
                        }
                    }
                    RetryDisposition::Never
                    | RetryDisposition::Quarantine
                    | RetryDisposition::RequiresRecovery
                    | RetryDisposition::RequiresIdempotencyKey => {
                        // Non-retriable: propaga subito.
                        return Err(last_error.expect("appena assegnato"));
                    }
                }
            }
        }
    }
    Err(last_error.expect("almeno un tentativo eseguito"))
}

/// Sleeps for `ms` milliseconds unless the cancellation token fires first.
///
/// Returns `true` se lo sleep è stato interrotto dalla cancellation.
async fn sleep_cancellable(cancellation: &CancellationToken, ms: u64) -> bool {
    if ms == 0 {
        return cancellation.is_cancelled();
    }
    let sleep = tokio::time::sleep(Duration::from_millis(ms));
    tokio::select! {
        () = sleep => cancellation.is_cancelled(),
        _ = cancellation.cancelled() => true,
    }
}

/// L'errore con cui il retry si arrende, con la causa che l'ha fermato.
///
/// Questo strato sta sopra tutti e tre i provider, quindi perdeva la deadline
/// per tutti insieme: un budget esaurito durante il backoff usciva come
/// `Cancelled`, indistinguibile da un annullamento del chiamante.
fn interrupted_error(
    last: Option<&DatabaseError>,
    cancellation: &CancellationToken,
) -> DatabaseError {
    DatabaseError::interrupted(
        cancellation,
        last.and_then(|e| e.provider),
        plenora_database_core::ErrorPhase::Cleanup,
        "retry interrotto",
    )
}

#[cfg(test)]
#[path = "retry_tests.rs"]
mod tests;
