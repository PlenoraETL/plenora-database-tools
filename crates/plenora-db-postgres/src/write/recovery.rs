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
    let error = public_error_envelope(
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
    );
    if rollback_confirmed {
        error
    } else {
        error.after_unconfirmed_rollback()
    }
}

/// Classifica la cancellazione preservandone la causa.
/// Una deadline produce Timeout; una richiesta esplicita produce Cancelled.
pub(super) use crate::error::interruption_category;

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

/// Come e finita una transazione che non ha mai inviato il COMMIT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Rollback {
    /// Il server ha confermato il ROLLBACK.
    Confirmed,
    /// La sessione era gia chiusa: il ROLLBACK non e partito, e non serve.
    /// Una transazione esplicita rende visibili i propri effetti soltanto
    /// con il COMMIT, e il server scarta quella di una sessione che si
    /// chiude; chi costruisce questo valore e raggiungibile solo prima del
    /// COMMIT ([`PreCommitRecovery`], la diagnostica row-scoped), quindi
    /// nessun effetto e visibile.
    SessionClosed,
    /// Il ROLLBACK e fallito su una sessione ancora aperta: l'effetto non e
    /// provato (ERR-014).
    Unconfirmed,
}

impl Rollback {
    pub(super) async fn of(transaction: Transaction<'_>) -> Self {
        match transaction.rollback().await {
            Ok(()) => Self::Confirmed,
            Err(error) if error.is_closed() => Self::SessionClosed,
            Err(_) => Self::Unconfirmed,
        }
    }
}

/// L'errore di una scrittura la cui transazione non potra mai confermare un
/// effetto: la sessione si e chiusa prima del COMMIT, o il server ha
/// rifiutato uno statement e l'ha abortita. La fase resta quella in cui e
/// fallita, nessun effetto e visibile (`none`), e una recovery non ha niente
/// da verificare.
///
/// Fino al 25/10 usciva in fase `rollback` con effetto ignoto e
/// `requires_recovery`: dopo un `copy_in` rifiutato all'apertura (per
/// esempio Replace con DELETE e senza INSERT) `tokio-postgres` 0.7.18 riceve
/// fuori sequenza la risposta al `CopyFail` che invia da se, e il ROLLBACK
/// che seguiva falliva con `UnexpectedMessage`. Misurato sul fixture
/// PostgreSQL 16 (scenario `replace-senza-insert` del runner dei vettori): le
/// righe che il DELETE aveva cancellato sono ancora tutte nel target dopo il
/// rifiuto.
pub(super) fn never_committed(mut error: DatabaseError) -> DatabaseError {
    error.remote_effect = RemoteEffect::None;
    if error.retry == RetryDisposition::RequiresRecovery {
        error.retry = RetryDisposition::Never;
    }
    error
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
        let rollback = Rollback::of(transaction).await;
        let mut error = cancelled_write_error(self.cancellation, rollback == Rollback::Confirmed);
        if rollback == Rollback::SessionClosed {
            error = never_committed(error);
            error.phase = ErrorPhase::Write;
        }
        error.execution_id = Some(self.execution_id.to_owned());
        error
    }

    pub(super) async fn rollback_resource_error(
        &self,
        transaction: Transaction<'_>,
        cause: &DatabaseError,
    ) -> DatabaseError {
        let rollback = Rollback::of(transaction).await;
        let mut error = resource_write_error(cause, rollback == Rollback::Confirmed);
        if rollback == Rollback::SessionClosed {
            error = never_committed(error);
            error.phase = ErrorPhase::Write;
        }
        error.execution_id = Some(self.execution_id.to_owned());
        error
    }

    /// Il rifiuto di uno statement da parte del server, dentro la
    /// transazione esplicita: PostgreSQL la porta nello stato abortito, in
    /// cui ogni comando successivo e ignorato e un COMMIT diventa un
    /// ROLLBACK. Il ROLLBACK confermato dice `rolled_back`, come in
    /// [`Self::rollback_error`]; uno non confermato non toglie la prova,
    /// che viene dal rifiuto e non dalla risposta al ROLLBACK.
    pub(super) async fn rollback_aborted(
        &self,
        transaction: Transaction<'_>,
        mut error: DatabaseError,
    ) -> DatabaseError {
        let rollback = Rollback::of(transaction).await;
        error.provider = Some(ProviderKind::Postgres);
        error.execution_id = Some(self.execution_id.to_owned());
        if rollback == Rollback::Confirmed {
            error.remote_effect = RemoteEffect::RolledBack;
            error
        } else {
            never_committed(error)
        }
    }

    pub(super) async fn rollback_error(
        &self,
        transaction: Transaction<'_>,
        mut error: DatabaseError,
    ) -> DatabaseError {
        let rollback = Rollback::of(transaction).await;
        error.provider = Some(ProviderKind::Postgres);
        error.execution_id = Some(self.execution_id.to_owned());
        match rollback {
            Rollback::Confirmed => {
                error.remote_effect = RemoteEffect::RolledBack;
                error
            }
            Rollback::SessionClosed => never_committed(error),
            Rollback::Unconfirmed => error.after_unconfirmed_rollback(),
        }
    }
}

pub(super) fn unknown_write_outcome(
    execution_id: String,
    received: u64,
    verification_action: &str,
    cause: Option<ErrorCategory>,
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
            cause,
            last_certain_phase: CertainPhase::CommitRequested,
            automatic_retry_allowed: false,
            idempotency_key: None,
            staging_object: None,
            verification_action: Some(verification_action.to_owned()),
        }),
    }
}

/// L'errore di un batch rifiutato prima della scrittura, dopo il rollback.
///
/// La categoria resta quella della causa: dalla riserva delle risorse
/// arrivano anche i rifiuti di mappatura delle geometrie, e riclassificarli
/// `resource_limit` diceva al chiamante di alzare un budget per un dato che
/// non cambiera. Il rollback decide effetto remoto e retry, non che cosa e
/// fallito.
pub(super) fn resource_write_error(
    error: &DatabaseError,
    rollback_confirmed: bool,
) -> DatabaseError {
    let error = public_error_envelope(
        error.category,
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
    );
    if rollback_confirmed {
        error
    } else {
        error.after_unconfirmed_rollback()
    }
}
