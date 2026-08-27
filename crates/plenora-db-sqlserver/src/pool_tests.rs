use super::*;
use plenora_database_core::provider::SecretString;

#[test]
fn rejects_zero_capacity_without_network() {
    let config = SqlServerConfig::new(
        "sql.example.test",
        "warehouse",
        "loader",
        SecretString::new("secret"),
    );
    let error = SqlServerPool::new(config, 0).expect_err("zero capacity");
    assert_eq!(error.category, ErrorCategory::InvalidConfiguration);
}

#[test]
fn poisoned_idle_lock_is_recovered() {
    let config = SqlServerConfig::new(
        "sql.example.test",
        "warehouse",
        "loader",
        SecretString::new("secret"),
    );
    let pool = SqlServerPool::new(config, 1).expect("pool fixture");
    let poisoned = std::panic::catch_unwind({
        let pool = Arc::clone(&pool);
        move || {
            let _guard = pool.idle.lock().unwrap_or_else(PoisonError::into_inner);
            panic!("poison fixture");
        }
    });
    assert!(poisoned.is_err());
    assert_eq!(pool.idle_connections(), 0);
}

/// La contesa sul pool e locale e transitoria: nessuno statement e
/// partito, quindi un nuovo tentativo e sicuro e viene classificato `Safe`.
///
/// Il test non apre nessuna connessione: satura il semaforo, cosi il
/// timeout scatta sull'acquisizione del permit e il TDS non viene mai
/// toccato.
#[tokio::test]
async fn pool_acquire_timeout_is_safe_to_retry() {
    let config = SqlServerConfig::new(
        "sql.example.test",
        "warehouse",
        "loader",
        SecretString::new("secret"),
    )
    .with_timeouts(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_secs(10),
        std::time::Duration::from_millis(20),
    );
    let pool = SqlServerPool::new(config, 1).expect("pool fixture");
    let _held = Arc::clone(&pool.semaphore)
        .acquire_owned()
        .await
        .expect("permit");

    // `expect_err` chiederebbe `Debug` sulla sessione, che non ce l'ha:
    // il match dice la stessa cosa senza obbligare un tipo pubblico a
    // derivare un tratto solo per un test.
    let Err(error) = pool.checkout(&CancellationToken::new()).await else {
        panic!("il semaforo e saturo: il checkout non puo riuscire");
    };
    assert_eq!(error.category, ErrorCategory::Timeout);
    assert_eq!(error.retry, RetryDisposition::Safe);
    assert!(error.is_retryable());
    assert_eq!(error.remote_effect, RemoteEffect::None);
}

/// Il pool chiuso, invece, non e una contesa: resta definitivo.
#[test]
fn closed_pool_stays_non_retryable() {
    let error = pool_error(ErrorCategory::Internal, "pool SQL Server chiuso");
    assert_eq!(error.retry, RetryDisposition::Never);
    assert!(!error.is_retryable());
}
