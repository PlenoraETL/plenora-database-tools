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
mod tests {
    use super::*;
    use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn mk_error(retry: RetryDisposition) -> DatabaseError {
        DatabaseError {
            category: ErrorCategory::Transient,
            phase: ErrorPhase::Write,
            remote_effect: RemoteEffect::RolledBack,
            retry,
            provider: None,
            execution_id: None,
            message: "test".to_owned(),
            diagnostics: None,
        }
    }

    #[tokio::test]
    async fn immediate_success_returns_first_result() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = Arc::clone(&count);
        let cancel = CancellationToken::new();
        let out = retry_with_policy(
            move |_attempt| {
                let c = Arc::clone(&count_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, DatabaseError>(42)
                }
            },
            RetryPolicy::new(3),
            &cancel,
        )
        .await
        .expect("ok");
        assert_eq!(out, 42);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn safe_retry_reattempts_until_ok_or_exhausted() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = Arc::clone(&count);
        let cancel = CancellationToken::new();
        let out = retry_with_policy(
            move |attempt| {
                let c = Arc::clone(&count_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        Err(mk_error(RetryDisposition::Safe))
                    } else {
                        Ok(7)
                    }
                }
            },
            RetryPolicy::new(5),
            &cancel,
        )
        .await
        .expect("recupero al 3° tentativo");
        assert_eq!(out, 7);
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn never_disposition_short_circuits() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = Arc::clone(&count);
        let cancel = CancellationToken::new();
        let err = retry_with_policy(
            move |_attempt| {
                let c = Arc::clone(&count_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, _>(mk_error(RetryDisposition::Never))
                }
            },
            RetryPolicy::new(5),
            &cancel,
        )
        .await
        .expect_err("never non deve retry");
        assert_eq!(err.category, ErrorCategory::Transient);
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn quarantine_short_circuits_like_never() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = Arc::clone(&count);
        let cancel = CancellationToken::new();
        let _ = retry_with_policy(
            move |_attempt| {
                let c = Arc::clone(&count_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, _>(mk_error(RetryDisposition::Quarantine))
                }
            },
            RetryPolicy::new(5),
            &cancel,
        )
        .await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn max_attempts_exhausts_and_returns_last_error() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = Arc::clone(&count);
        let cancel = CancellationToken::new();
        let err = retry_with_policy(
            move |_attempt| {
                let c = Arc::clone(&count_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, _>(mk_error(RetryDisposition::Safe))
                }
            },
            RetryPolicy::new(3),
            &cancel,
        )
        .await
        .expect_err("esausto");
        assert_eq!(err.category, ErrorCategory::Transient);
        assert_eq!(count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn after_delay_is_capped_by_policy() {
        let start = std::time::Instant::now();
        let cancel = CancellationToken::new();
        let policy = RetryPolicy {
            max_attempts: 2,
            max_delay_ms: Some(20), // cap forte
        };
        let _ = retry_with_policy(
            |_attempt| async move { Err::<i32, _>(mk_error(RetryDisposition::After(5_000))) },
            policy,
            &cancel,
        )
        .await;
        let elapsed = start.elapsed();
        // Con cap 20ms, il retry deve essere veloce (non 5 secondi).
        assert!(
            elapsed < Duration::from_millis(500),
            "delay non capped: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn cancellation_interrupts_sleep() {
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            cancel_clone.cancel();
        });
        let start = std::time::Instant::now();
        let err = retry_with_policy(
            |_attempt| async move { Err::<i32, _>(mk_error(RetryDisposition::After(10_000))) },
            RetryPolicy::new(3),
            &cancel,
        )
        .await
        .expect_err("cancel deve interrompere");
        let elapsed = start.elapsed();
        assert_eq!(err.category, ErrorCategory::Cancelled);
        assert!(
            elapsed < Duration::from_millis(500),
            "sleep non interrotto dalla cancellation: {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn zero_max_attempts_is_invalid_plan() {
        let cancel = CancellationToken::new();
        let err = retry_with_policy(
            |_attempt| async move { Ok::<_, DatabaseError>(1) },
            RetryPolicy::new(0),
            &cancel,
        )
        .await
        .expect_err("0 non valido");
        assert_eq!(err.category, ErrorCategory::InvalidPlan);
    }

    #[tokio::test]
    async fn requires_recovery_short_circuits() {
        let count = Arc::new(AtomicU32::new(0));
        let count_clone = Arc::clone(&count);
        let cancel = CancellationToken::new();
        let _ = retry_with_policy(
            move |_attempt| {
                let c = Arc::clone(&count_clone);
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Err::<i32, _>(mk_error(RetryDisposition::RequiresRecovery))
                }
            },
            RetryPolicy::new(5),
            &cancel,
        )
        .await;
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    /// `DatabaseError::is_retryable()` e questo executor devono dire la stessa
    /// cosa. Hanno divergiuto: il metodo pubblico rispondeva `true` per
    /// `RequiresRecovery` e `RequiresIdempotencyKey`, che qui non sono mai
    /// stati ritentati. Un consumer che si fidava della risposta e non
    /// dell'executor poteva duplicare una scrittura dall'esito ignoto.
    ///
    /// La guardia non ispeziona il codice: conta i tentativi davvero eseguiti.
    #[tokio::test]
    async fn is_retryable_agrees_with_the_executor() {
        for retry in [
            RetryDisposition::Never,
            RetryDisposition::Quarantine,
            RetryDisposition::Safe,
            RetryDisposition::After(0),
            RetryDisposition::RequiresIdempotencyKey,
            RetryDisposition::RequiresRecovery,
        ] {
            let count = Arc::new(AtomicU32::new(0));
            let count_clone = Arc::clone(&count);
            let cancel = CancellationToken::new();
            let _ = retry_with_policy(
                move |_attempt| {
                    let c = Arc::clone(&count_clone);
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Err::<i32, _>(mk_error(retry))
                    }
                },
                RetryPolicy::new(3),
                &cancel,
            )
            .await;
            let attempts = count.load(Ordering::SeqCst);
            let executor_retried = attempts > 1;
            assert_eq!(
                executor_retried,
                mk_error(retry).is_retryable(),
                "{retry:?}: l'executor ha eseguito {attempts} tentativi"
            );
        }
    }
}
