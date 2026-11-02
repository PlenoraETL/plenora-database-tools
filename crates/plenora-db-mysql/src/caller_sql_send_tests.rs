use super::*;
use plenora_database_core::native_query_policy::NativeQueryPolicy;
use plenora_database_core::{RemoteEffect, RetryDisposition};

fn scope() -> MysqlTransaction {
    MysqlTransaction {
        session: MysqlSession::without_connection(&crate::profile::MYSQL_PROFILE),
        sent: SendMarker::default(),
        open: true,
        recovery_required: false,
        native_query_policy: NativeQueryPolicy::Deny,
    }
}

async fn wrapped() -> Box<dyn TransactionScope> {
    plenora_database_core::caller_sql::open_transaction(Box::pin(async {
        Ok(Box::new(scope()) as Box<dyn TransactionScope>)
    }))
    .await
    .unwrap()
}

#[tokio::test]
async fn denied_sql_before_send_has_no_effect() {
    let mut scope = wrapped().await;
    let cancel = CancellationToken::new();
    let statement = Statement::new("DROP TABLE t");
    let error = scope.execute(&statement, &cancel).await.unwrap_err();
    assert_eq!(error.phase, ErrorPhase::Validate);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
    let error = scope.query(&statement, &cancel).await.unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::None);
}

#[tokio::test]
async fn zero_batch_before_send_has_no_effect() {
    let mut scope = wrapped().await;
    let cancel = CancellationToken::new();
    let statement = Statement::new("SELECT 1");
    let Err(error) = scope.query_stream(&statement, 0, &cancel).await else {
        panic!("zero batch accepted");
    };
    assert_eq!(error.phase, ErrorPhase::Validate);
    assert_eq!(error.remote_effect, RemoteEffect::None);
    assert_eq!(error.retry, RetryDisposition::Never);
}

#[tokio::test]
async fn validation_after_driver_execution_requires_recovery() {
    let mut scope = scope();
    let marker = scope.begin_caller_sql();
    // Simula il driver: il test verifica il marcatore dell'adapter,
    // senza attribuire evidence live a una prova senza server.
    scope.sent.sending(async {}).await;
    let error = marker.failure(DatabaseError::invalid_plan("validazione risultato"));
    assert_eq!(error.phase, ErrorPhase::Validate);
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    let next = scope.begin_caller_sql();
    drop(scope);
    assert_eq!(
        next.failure(DatabaseError::invalid_plan("locale"))
            .remote_effect,
        RemoteEffect::None
    );
    assert_eq!(
        marker
            .failure(DatabaseError::invalid_plan("precedente"))
            .remote_effect,
        RemoteEffect::Unknown
    );
}

#[tokio::test]
async fn mariadb_preserves_local_errors_and_promotes_post_send_validation() {
    let mut transaction = scope();
    transaction.session = MysqlSession::without_connection(&crate::profile::MARIADB_PROFILE);
    let sent = transaction.begin_caller_sql();
    let cancel = CancellationToken::new();
    let error = transaction
        .execute(&Statement::new("DROP TABLE t"), &cancel)
        .await
        .unwrap_err();
    assert_eq!(sent.failure(error).remote_effect, RemoteEffect::None);
    let statement = Statement::new("SELECT 1");
    let Err(error) = transaction.query_stream(&statement, 0, &cancel).await else {
        panic!("zero batch accepted")
    };
    assert_eq!(sent.failure(error).remote_effect, RemoteEffect::None);
    transaction.sent.sending(async {}).await;
    assert_eq!(
        sent.failure(DatabaseError::invalid_plan("risultato"))
            .remote_effect,
        RemoteEffect::Unknown
    );
}

#[tokio::test]
async fn stream_warning_closes_commit_and_does_not_confirm_rollback() {
    let mut tx = scope();
    tx.open = false;
    tx.recovery_required = true;
    assert!(Box::new(tx)
        .commit(&CancellationToken::new())
        .await
        .is_err());
    let mut tx = scope();
    tx.open = false;
    tx.recovery_required = true;
    let error = Box::new(tx)
        .rollback(&CancellationToken::new())
        .await
        .unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
}

#[tokio::test]
async fn protected_set_is_rejected_before_send_even_under_allow() {
    let cancel = CancellationToken::new();
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        let mut tx = scope();
        tx.session = MysqlSession::without_connection(profile);
        tx.native_query_policy = NativeQueryPolicy::Allow;
        let statement = Statement::new("SET @x=1, @@session.sql_notes=0");
        let error = tx.execute(&statement, &cancel).await.unwrap_err();
        assert_eq!(error.phase, ErrorPhase::Validate);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        let error = tx.query(&statement, &cancel).await.unwrap_err();
        assert_eq!(error.phase, ErrorPhase::Validate);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert!(tx.query_stream(&statement, 1, &cancel).await.is_err());
        assert!(tx.open);
    }
}

#[tokio::test]
async fn unconfirmed_savepoint_rollback_closes_scope_but_local_validation_does_not() {
    let mut tx = scope();
    let cancel = CancellationToken::new();
    assert!(tx.rollback_to_savepoint("", &cancel).await.is_err());
    assert!(tx.open);
    let error = tx
        .rollback_to_savepoint("valid", &cancel)
        .await
        .unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    assert!(!tx.open);
    assert!(tx.recovery_required);
    let error = Box::new(tx).rollback(&cancel).await.unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
}

#[tokio::test]
async fn discarded_failure_is_not_a_confirmed_rollback() {
    for effect in [
        RemoteEffect::Unknown,
        RemoteEffect::None,
        RemoteEffect::RolledBack,
    ] {
        let mut tx = scope();
        tx.discard_after_failure(effect).await;
        let result = Box::new(tx).rollback(&CancellationToken::new()).await;
        if effect == RemoteEffect::RolledBack {
            result.unwrap();
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.phase, ErrorPhase::Rollback);
            assert_eq!(error.remote_effect, RemoteEffect::Unknown);
            assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
        }
    }
}

#[tokio::test]
async fn local_rejection_matrix_preserves_scope_and_unsent_marker() {
    let cancel = CancellationToken::new();
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        for (policy, sql) in [
            (NativeQueryPolicy::Deny, "DROP TABLE t"),
            (NativeQueryPolicy::Allow, "SELECT 'unterminated"),
            (NativeQueryPolicy::Allow, "SELECT 1; DO 0"),
            (NativeQueryPolicy::Allow, "SET sql_notes=0"),
            (NativeQueryPolicy::Allow, "SET NAMES gbk"),
            (NativeQueryPolicy::Allow, "SET autocommit=1"),
            (NativeQueryPolicy::Allow, "CREATE TABLE t(n INT)"),
            (NativeQueryPolicy::Allow, "LOCK TABLES t WRITE"),
        ] {
            let mut tx = scope();
            tx.session = MysqlSession::without_connection(profile);
            tx.native_query_policy = policy;
            let sent = tx.begin_caller_sql();
            let statement = Statement::new(sql);
            let execute = tx.execute(&statement, &cancel).await.unwrap_err();
            let query = tx.query(&statement, &cancel).await.unwrap_err();
            let Err(stream) = tx.query_stream(&statement, 1, &cancel).await else {
                panic!("SQL locale accettato")
            };
            let conditional = tx
                .execute_conditional_update(
                    ConditionalUpdate {
                        update: &statement,
                        key_probe: None,
                        expected_affected_rows: 1,
                    },
                    &cancel,
                )
                .await
                .unwrap_err();
            for error in [execute, query, stream, conditional] {
                let error = sent.failure(error);
                assert_eq!(error.phase, ErrorPhase::Validate);
                assert_eq!(error.remote_effect, RemoteEffect::None);
                assert_eq!(error.retry, RetryDisposition::Never);
                assert_eq!(error.provider, Some(profile.kind()));
            }
            assert!(tx.open);
        }
    }
}

/// Riproduce anche la delega MariaDB -> MySQL, senza connessione al server.
#[tokio::test]
async fn nested_wrappers_preserve_local_rejections_for_both_products() {
    let cancel = CancellationToken::new();
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        for depth in 1..=3 {
            let mut tx = scope();
            tx.session = MysqlSession::without_connection(profile);
            tx.native_query_policy = NativeQueryPolicy::Allow;
            let mut tx = Box::new(tx) as Box<dyn TransactionScope>;
            for _ in 0..depth {
                tx =
                    plenora_database_core::caller_sql::open_transaction(Box::pin(
                        async move { Ok(tx) },
                    ))
                    .await
                    .unwrap();
            }
            for sql in [
                "SELECT 1; DO 0",
                "SELECT 1 AS $a$; DO 0; SELECT 1 AS $a$",
                "SELECT q'[' FROM (SELECT 1 q) x; DO 0; SELECT ']' FROM DUAL",
                "SELECT 'unterminated",
                "SET SESSION sql_notes=0",
                "SET @x=1, @@session.sql_mode=''",
                "SET max_error_count=0",
            ] {
                let statement = Statement::new(sql);
                let execute = tx.execute(&statement, &cancel).await.unwrap_err();
                let query = tx.query(&statement, &cancel).await.unwrap_err();
                let Err(stream) = tx.query_stream(&statement, 1, &cancel).await else {
                    panic!("SQL locale accettato")
                };
                for error in [execute, query, stream] {
                    assert_eq!(error.category, ErrorCategory::InvalidPlan);
                    assert_eq!(error.phase, ErrorPhase::Validate);
                    assert_eq!(error.provider, Some(profile.kind()));
                    assert_eq!(error.remote_effect, RemoteEffect::None, "depth={depth}");
                    assert_eq!(error.retry, RetryDisposition::Never);
                }
            }
        }
    }
}

#[tokio::test]
async fn caller_write_warning_and_server_rollback_close_scope() {
    for effect in [RemoteEffect::Unknown, RemoteEffect::RolledBack] {
        let mut tx = scope();
        let mut error = crate::session::warning_error(tx.session.kind(), ErrorPhase::Write);
        error.remote_effect = effect;
        tx.finish_caller_write(Err(error)).await.unwrap_err();
        assert!(!tx.open);
        assert_eq!(tx.recovery_required, effect == RemoteEffect::Unknown);
        let error = Box::new(tx)
            .commit(&CancellationToken::new())
            .await
            .unwrap_err();
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Commit);
    }
}

#[tokio::test]
async fn unverified_stream_completion_prevents_commit() {
    for exhausted in [true, false] {
        let mut tx = scope();
        abandon_stream(exhausted, &mut tx.open, &mut tx.recovery_required);
        assert_eq!(tx.open, exhausted);
        assert_eq!(tx.recovery_required, !exhausted);
        if !exhausted {
            let error = Box::new(tx)
                .commit(&CancellationToken::new())
                .await
                .unwrap_err();
            assert_eq!(error.category, ErrorCategory::InvalidPlan);
            assert_eq!(error.phase, ErrorPhase::Commit);
        }
    }
}

#[tokio::test]
async fn failed_profile_read_closes_scope_and_commit_requires_recovery() {
    let cancel = CancellationToken::new();
    let mut tx = scope();
    let error = tx
        .verify_profile(ErrorPhase::Read, &cancel)
        .await
        .unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    assert!(!tx.open);
    assert!(tx.recovery_required);
    assert_eq!(tx.session.state(), crate::MysqlSessionState::Quarantined);
    let error = Box::new(tx).commit(&cancel).await.unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
    // Il controllo prima di COMMIT e obbligatorio anche senza altre operazioni.
    let error = Box::new(scope()).commit(&cancel).await.unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
}

#[tokio::test]
async fn stream_eof_waits_for_profile_validation() {
    for valid in [false, true] {
        let mut open = true;
        let mut failed = false;
        let (sender, batches) = tokio::sync::mpsc::channel(1);
        let pump = Box::pin(async move {
            send_stream_batch(&sender, Vec::new(), ProviderKind::Mysql).await?;
            if valid {
                Ok(())
            } else {
                Err(DatabaseError::new(
                    ErrorCategory::Execution,
                    ErrorPhase::Read,
                    Some(ProviderKind::Mysql),
                    "profilo di sessione diverso da quello configurato",
                ))
            }
        });
        let mut stream = MysqlRowStream {
            pump,
            batches,
            exhausted: false,
            pump_done: false,
            transaction_open: &mut open,
            transaction_failed: &mut failed,
            profile: &crate::profile::MYSQL_PROFILE,
        };
        let cancel = CancellationToken::new();
        assert!(stream.next_batch(&cancel).await.unwrap().is_some());
        let eof = stream.next_batch(&cancel).await;
        if valid {
            assert!(eof.unwrap().is_none());
        } else {
            let error = eof.unwrap_err();
            assert_eq!(error.remote_effect, RemoteEffect::Unknown);
            assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
        }
        drop(stream);
        assert_eq!(open, valid);
        assert_eq!(failed, !valid);
    }
}
