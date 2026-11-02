use super::*;
use plenora_database_core::native_query_policy::NativeQueryPolicy;
use plenora_database_core::{RemoteEffect, RetryDisposition};

fn scope() -> OracleTransaction {
    OracleTransaction {
        connection: PooledOracleConnection::without_connection(),
        operation_timeout: Duration::from_secs(1),
        lob_cache: LobCache::default(),
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
async fn empty_parameters_fail_before_connection_or_send() {
    for value in [
        ParameterValue::String(String::new()),
        ParameterValue::Bytes(vec![]),
    ] {
        let mut transaction = scope();
        transaction.native_query_policy = NativeQueryPolicy::Allow;
        let statement =
            Statement::new("INSERT INTO secret_table VALUES (:1)").with_params(vec![value.clone()]);
        let cancellation = CancellationToken::new();
        let error = transaction
            .execute_inner(&statement, &cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.message, "parametro vuoto Oracle non ammesso");
        let error = transaction
            .execute_atomic_dml(&statement, &cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.category, ErrorCategory::DataMapping);
        let error = transaction
            .execute_parameter_array(
                "INSERT INTO secret_table VALUES (:1)",
                vec![vec![ParameterValue::I32(1)], vec![value]],
                &cancellation,
            )
            .await
            .unwrap_err();
        drop(transaction);
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.remote_effect, RemoteEffect::None);
    }
}
