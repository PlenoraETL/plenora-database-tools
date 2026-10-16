use super::*;
use plenora_database_core::native_query_policy::NativeQueryPolicy;
use plenora_database_core::{RemoteEffect, RetryDisposition};

fn scope() -> MysqlTransaction {
    MysqlTransaction {
        session: MysqlSession::without_connection(&crate::profile::MYSQL_PROFILE),
        sent: SendMarker::default(),
        open: true,
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
