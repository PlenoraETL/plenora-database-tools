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
