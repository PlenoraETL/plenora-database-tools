use crate::config::OracleConfig;
use crate::error::{driver_error, interruption_error};
use oracle_rs::Connection;
use plenora_database_core::provider::SecretString;
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase, Result};
use std::time::Duration;

pub async fn connect(
    config: &OracleConfig,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Result<Connection> {
    // Il driver abilita aws-lc-rs mentre gli altri adapter fissano ring. Se i
    // due backend convivono rustls non sceglie da solo e va in panic: la
    // selezione e esplicita e idempotente prima di costruire il client.
    let _ = rustls::crypto::ring::default_provider().install_default();
    if cancellation.is_cancelled() {
        return Err(interruption_error(cancellation, ErrorPhase::Connect));
    }
    let driver_config = config.driver_config(secret)?.into_inner();
    let operation = Connection::connect_with_config(driver_config);
    tokio::select! {
        result = operation => result.map_err(|error| driver_error(ErrorPhase::Connect, &error)),
        _ = cancellation.cancelled() => Err(interruption_error(cancellation, ErrorPhase::Connect)),
    }
}

pub async fn with_timeout<T>(
    config: &OracleConfig,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
    operation: impl std::future::Future<Output = oracle_rs::Result<T>>,
) -> Result<T> {
    with_timeout_duration(config.operation_timeout(), phase, cancellation, operation).await
}

pub async fn with_timeout_duration<T>(
    timeout: Duration,
    phase: ErrorPhase,
    cancellation: &CancellationToken,
    operation: impl std::future::Future<Output = oracle_rs::Result<T>>,
) -> Result<T> {
    tokio::select! {
        result = tokio::time::timeout(timeout, operation) => result.map_or_else(
            |_| Err(plenora_database_core::DatabaseError::new(
                ErrorCategory::Timeout,
                phase,
                Some(plenora_database_core::plan::ProviderKind::Oracle),
                "operazione Oracle oltre il timeout configurato",
            )),
            |result| result.map_err(|error| driver_error(phase, &error)),
        ),
        _ = cancellation.cancelled() => Err(interruption_error(cancellation, phase)),
    }
}
