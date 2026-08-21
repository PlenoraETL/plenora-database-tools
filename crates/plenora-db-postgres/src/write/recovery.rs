use super::WriteRuntime;
use crate::{error::public_error_envelope, PostgresTlsMode};
use plenora_database_core::outcome::{
    CertainPhase, Recovery, RowCounts, WriteOutcome, WriteStatus,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{
    CancellationReason, CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect,
    RetryDisposition,
};
use tokio_postgres::{CancelToken, NoTls, Transaction};

pub(super) async fn cancel_backend(
    token: &CancelToken,
    tls_mode: PostgresTlsMode,
    tls_connector: &tokio_postgres_rustls::MakeRustlsConnect,
    timeout_ms: u64,
) {
    let cancellation = async {
        match tls_mode {
            PostgresTlsMode::Disabled => token.cancel_query(NoTls).await,
            PostgresTlsMode::Require => token.cancel_query(tls_connector.clone()).await,
        }
    };
    let _cancel_result =
        tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), cancellation).await;
}

pub(super) fn cancelled_write_error(
    cancellation: &CancellationToken,
    rollback_confirmed: bool,
) -> DatabaseError {
    public_error_envelope(
        interruption_category(cancellation),
        ErrorPhase::Write,
        if rollback_confirmed {
            RemoteEffect::RolledBack
        } else {
            RemoteEffect::Unknown
        },
        if rollback_confirmed {
            RetryDisposition::Never
        } else {
            RetryDisposition::RequiresRecovery
        },
        interruption_message(cancellation),
    )
}

pub(super) fn interruption_category(cancellation: &CancellationToken) -> ErrorCategory {
    if cancellation.reason() == Some(CancellationReason::Deadline) {
        ErrorCategory::Timeout
    } else {
        ErrorCategory::Cancelled
    }
}

pub(super) fn interruption_message(cancellation: &CancellationToken) -> &'static str {
    if cancellation.reason() == Some(CancellationReason::Deadline) {
        "durata massima write PostgreSQL esaurita"
    } else {
        "write PostgreSQL cancellata sul server"
    }
}

pub(super) fn commit_interruption_error(
    cancellation: &CancellationToken,
    execution_id: &str,
) -> DatabaseError {
    let mut error = public_error_envelope(
        interruption_category(cancellation),
        ErrorPhase::Commit,
        RemoteEffect::Unknown,
        RetryDisposition::RequiresRecovery,
        "deadline o cancellazione durante commit PostgreSQL: verificare lo stato remoto",
    );
    error.execution_id = Some(execution_id.to_owned());
    error
}

/// Recovery authority for the only state in which rollback is still meaningful.
///
/// Keeping these inputs together prevents a failure path from accidentally using
/// another execution id, cancellation source, TLS policy or backend cancel token.
pub(super) struct PreCommitRecovery<'a> {
    pub(super) cancellation: &'a CancellationToken,
    pub(super) runtime: &'a WriteRuntime,
    pub(super) cancel_token: &'a CancelToken,
    pub(super) execution_id: &'a str,
}

impl PreCommitRecovery<'_> {
    pub(super) async fn rollback_cancellation(
        &self,
        transaction: Transaction<'_>,
    ) -> DatabaseError {
        self.runtime.metrics.cancellation();
        cancel_backend(
            self.cancel_token,
            self.runtime.tls_mode,
            self.runtime.tls_config.connector(),
            self.runtime.network_options.connect_timeout_ms,
        )
        .await;
        let rollback_confirmed = transaction.rollback().await.is_ok();
        let mut error = cancelled_write_error(self.cancellation, rollback_confirmed);
        error.execution_id = Some(self.execution_id.to_owned());
        error
    }

    pub(super) async fn rollback_resource_error(
        &self,
        transaction: Transaction<'_>,
        cause: &DatabaseError,
    ) -> DatabaseError {
        let rollback_confirmed = transaction.rollback().await.is_ok();
        let mut error = resource_write_error(cause, rollback_confirmed);
        error.execution_id = Some(self.execution_id.to_owned());
        error
    }

    pub(super) async fn rollback_error(
        &self,
        transaction: Transaction<'_>,
        mut error: DatabaseError,
    ) -> DatabaseError {
        let rollback_confirmed = transaction.rollback().await.is_ok();
        error.provider = Some(ProviderKind::Postgres);
        error.execution_id = Some(self.execution_id.to_owned());
        if rollback_confirmed {
            error.remote_effect = RemoteEffect::RolledBack;
        } else {
            error.remote_effect = RemoteEffect::Unknown;
            error.retry = RetryDisposition::RequiresRecovery;
        }
        error
    }
}

pub(super) fn unknown_write_outcome(
    execution_id: String,
    received: u64,
    verification_action: &str,
) -> WriteOutcome {
    WriteOutcome {
        schema_version: 2,
        status: WriteStatus::OutcomeUnknown,
        execution_id,
        provider: ProviderKind::Postgres,
        rows: RowCounts {
            received,
            confirmed: 0,
            inserted: None,
            updated: None,
            deleted: None,
            failed: 0,
            skipped: 0,
        },
        recovery: Some(Recovery {
            last_certain_phase: CertainPhase::CommitRequested,
            automatic_retry_allowed: false,
            idempotency_key: None,
            staging_object: None,
            verification_action: Some(verification_action.to_owned()),
        }),
    }
}

pub(super) fn resource_write_error(
    error: &DatabaseError,
    rollback_confirmed: bool,
) -> DatabaseError {
    public_error_envelope(
        ErrorCategory::ResourceLimit,
        ErrorPhase::Write,
        if rollback_confirmed {
            RemoteEffect::RolledBack
        } else {
            RemoteEffect::Unknown
        },
        if rollback_confirmed {
            RetryDisposition::Never
        } else {
            RetryDisposition::RequiresRecovery
        },
        &error.message,
    )
}
