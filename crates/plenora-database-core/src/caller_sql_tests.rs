use super::*;
use crate::error::{ErrorCategory, RemoteEffect, RetryDisposition};
use crate::plan::ProviderKind;
use crate::provider::{Provider, SecretString};
use crate::resource::{ResourceBudget, ResourceLimits};
use crate::transaction::TransactionOptions;

/// Un errore `none` costruito come lo costruiscono gli adapter quando
/// sbagliano: dopo l'invio, nella fase dell'operazione.
fn none_error(category: ErrorCategory, phase: ErrorPhase) -> DatabaseError {
    DatabaseError::new(category, phase, Some(ProviderKind::Postgres), "rifiuto")
}

/// Test di instradamento del wrapper, non evidence dei provider.
/// I percorsi driver sono qualificati dalle rispettive prove live.
#[derive(Clone, Copy)]
enum Scenario {
    /// PostgreSQL: una procedura fa COMMIT e poi RAISE.
    ProcedureCommitThenRaise,
    /// Oracle: il fetch di una query ordinaria fallisce.
    FetchFails,
    /// SQL Server: lo stream gia aperto viene cancellato.
    StreamCancelled,
    /// Db2: il worker di una lettura fallisce.
    ReadWorkerFails,
    /// MySQL: la decodifica del result set fallisce.
    DecodeFails,
    /// Un errore di validazione locale, prima dell'invio.
    Validation,
    TrackedBeforeSend,
    TrackedAfterSend,
    GraphRowLimit,
    OracleProbePolicy,
    Db2ProbePolicy,
}

struct FakeScope(Scenario, SendMarker);

struct FakeStream(Scenario);

impl RowStream for FakeStream {
    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<Vec<Row>>> {
        let scenario = self.0;
        Box::pin(async move {
            Err(match scenario {
                Scenario::StreamCancelled => none_error(ErrorCategory::Cancelled, ErrorPhase::Read),
                _ => none_error(ErrorCategory::DataMapping, ErrorPhase::Read),
            })
        })
    }
}

impl TransactionScope for FakeScope {
    fn begin_caller_sql(&mut self) -> SendMarker {
        self.1 = if matches!(
            self.0,
            Scenario::TrackedBeforeSend | Scenario::TrackedAfterSend
        ) {
            SendMarker::before_send()
        } else {
            SendMarker::default()
        };
        self.1.clone()
    }

    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn execute<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        let scenario = self.0;
        let sent = self.1.clone();
        Box::pin(async move {
            if matches!(scenario, Scenario::TrackedAfterSend) {
                sent.sending(async {}).await;
            }
            Err(match scenario {
                Scenario::Validation | Scenario::TrackedBeforeSend | Scenario::TrackedAfterSend => {
                    DatabaseError::invalid_plan("policy nativa")
                }
                _ => none_error(ErrorCategory::Execution, ErrorPhase::Write),
            })
        })
    }

    fn query<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        let scenario = self.0;
        Box::pin(async move {
            Err(match scenario {
                Scenario::ReadWorkerFails => none_error(ErrorCategory::Protocol, ErrorPhase::Read),
                Scenario::DecodeFails => none_error(ErrorCategory::DataMapping, ErrorPhase::Read),
                _ => none_error(ErrorCategory::Execution, ErrorPhase::Read),
            })
        })
    }

    fn execute_graph<'a>(
        &'a mut self,
        statement: &'a GraphStatement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<GraphRow>> {
        Box::pin(async move {
            statement.validate()?;
            // Il server ha creato tre nodi prima del controllo locale.
            let returned_rows = 3;
            if returned_rows > statement.max_rows {
                return Err(DatabaseError::resource_limit(
                    "il risultato Cypher supera il limite righe dichiarato",
                ));
            }
            Ok(Vec::new())
        })
    }

    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        let scenario = self.0;
        Box::pin(async move { Ok(Box::new(FakeStream(scenario)) as Box<dyn RowStream + Send>) })
    }

    fn savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let scenario = self.0;
        Box::pin(async move {
            if matches!(
                scenario,
                Scenario::OracleProbePolicy | Scenario::Db2ProbePolicy
            ) {
                // UPDATE gia riuscito con conteggio diverso dall'atteso;
                // Oracle e Db2 validano la sonda soltanto a questo punto.
                assert_eq!(request.expected_affected_rows, 2);
                crate::native_query_policy::enforce_policy(
                    crate::native_query_policy::NativeQueryPolicy::Deny,
                    &request.key_probe.expect("sonda").sql,
                )?;
            }
            Err(none_error(ErrorCategory::Execution, ErrorPhase::Write))
        })
    }

    fn commit(
        self: Box<Self>,
        _cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async { Ok(CommitOutcome::Committed) })
    }

    fn rollback(self: Box<Self>, _cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

struct FakeProvider(Scenario);

impl Provider for FakeProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn test_connection<'a>(
        &'a self,
        _secret: &'a SecretString,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, crate::provider::ConnectionInfo> {
        Box::pin(async { Err(DatabaseError::invalid_plan("non usato")) })
    }

    fn probe_capabilities<'a>(
        &'a self,
        _secret: &'a SecretString,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, crate::capabilities::ProviderCapabilities> {
        Box::pin(async { Err(DatabaseError::invalid_plan("non usato")) })
    }

    fn inspect<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a crate::plan::Operation,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, crate::provider::Inspection> {
        Box::pin(async { Err(DatabaseError::invalid_plan("non usato")) })
    }

    fn read<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a crate::plan::ReadOperation,
        _parameters: &'a crate::provider::ParameterBag,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn crate::provider::BatchStream>> {
        Box::pin(async { Err(DatabaseError::invalid_plan("non usato")) })
    }

    fn prepare_write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a crate::plan::WriteOperation,
        _input_schema: crate::arrow::SchemaRef,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, crate::provider::PreparedWrite> {
        Box::pin(async { Err(DatabaseError::invalid_plan("non usato")) })
    }

    fn write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _prepared: crate::provider::PreparedWrite,
        _input: Box<dyn crate::provider::BatchStream>,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, crate::outcome::WriteOutcome> {
        Box::pin(async { Err(DatabaseError::invalid_plan("non usato")) })
    }

    fn open_transaction<'a>(
        &'a self,
        _secret: &'a SecretString,
        _options: &'a TransactionOptions,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn TransactionScope>> {
        crate::caller_sql::open_transaction({
            let scenario = self.0;
            Box::pin(async move {
                Ok(Box::new(FakeScope(scenario, SendMarker::default()))
                    as Box<dyn TransactionScope>)
            })
        })
    }

    fn run_ddl<'a>(
        &'a self,
        _secret: &'a SecretString,
        _sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        crate::caller_sql::run_ddl(self.kind(), cancellation, {
            // `CALL` fuori transazione: la procedura ha fatto COMMIT, poi RAISE.
            Box::pin(async { Err(none_error(ErrorCategory::Execution, ErrorPhase::Write)) })
        })
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("il finto e sincrono"),
    }
}

fn assert_unknown(error: &DatabaseError, what: &str) {
    assert_eq!(error.remote_effect, RemoteEffect::Unknown, "{what}");
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery, "{what}");
}

/// Ogni scenario segnalato, dietro il punto obbligato, esce ignoto: invio,
/// query, fetch e decodifica dello stream, cancellazione, update
/// condizionale, DDL fuori transazione.
#[test]
fn every_caller_sql_exit_after_sending_is_unknown() {
    let cancellation = CancellationToken::new();
    let secret = SecretString::new("segreto");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = TransactionOptions::default();
    let statement = Statement::new("CALL p()");
    for scenario in [
        Scenario::ProcedureCommitThenRaise,
        Scenario::FetchFails,
        Scenario::StreamCancelled,
        Scenario::ReadWorkerFails,
        Scenario::DecodeFails,
    ] {
        let provider = FakeProvider(scenario);
        let mut transaction =
            block_on(provider.begin_transaction(&secret, &options, &budget, &cancellation))
                .expect("begin");
        let error = block_on(transaction.execute(&statement, &cancellation)).expect_err("execute");
        assert_unknown(&error, "execute");
        let error = block_on(transaction.query(&statement, &cancellation)).expect_err("query");
        assert_unknown(&error, "query");
        let request = ConditionalUpdate {
            update: &statement,
            key_probe: None,
            expected_affected_rows: 1,
        };
        let error = block_on(transaction.execute_conditional_update(request, &cancellation))
            .expect_err("update condizionale");
        assert_unknown(&error, "update condizionale");
        {
            let mut stream =
                block_on(transaction.query_stream(&statement, 10, &cancellation)).expect("stream");
            let error = block_on(stream.next_batch(&cancellation)).expect_err("fetch");
            assert_unknown(&error, "fetch dello stream");
        }
        let error =
            block_on(provider.execute_ddl(&secret, "CALL p()", &cancellation)).expect_err("ddl");
        assert_unknown(&error, "execute_ddl");
    }
}

/// Il wrapper non puo distinguere una validazione locale da una dopo invio.
#[test]
fn adapter_validation_without_send_evidence_requires_recovery() {
    let cancellation = CancellationToken::new();
    let provider = FakeProvider(Scenario::Validation);
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut transaction = block_on(provider.begin_transaction(
        &SecretString::new("segreto"),
        &TransactionOptions::default(),
        &budget,
        &cancellation,
    ))
    .expect("begin");
    let error =
        block_on(transaction.execute(&Statement::new("DROP"), &cancellation)).expect_err("policy");
    assert_unknown(&error, "validazione adapter senza prova di mancato invio");
    assert_eq!(error.phase, ErrorPhase::Validate);
}

#[test]
fn postgres_graph_row_limit_after_send_requires_recovery() {
    let mut transaction = CallerSqlTransaction::new(Box::new(FakeScope(
        Scenario::GraphRowLimit,
        SendMarker::default(),
    )));
    let statement = GraphStatement::new(
        "graph",
        "UNWIND [1,2,3] AS i CREATE (n) RETURN n",
        vec!["n".into()],
    )
    .with_max_rows(2);
    let error = block_on(transaction.execute_graph(&statement, &CancellationToken::new()))
        .expect_err("limite dopo CREATE");
    assert_eq!(error.phase, ErrorPhase::Validate);
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    assert_unknown(&error, "Cypher dopo CREATE");
}

fn assert_probe_policy_after_update(scenario: Scenario) {
    let mut transaction =
        CallerSqlTransaction::new(Box::new(FakeScope(scenario, SendMarker::default())));
    let update = Statement::new("UPDATE items SET version = version + 1 WHERE id = 1");
    let probe = Statement::new("DROP TABLE items");
    let request = ConditionalUpdate {
        update: &update,
        key_probe: Some(&probe),
        expected_affected_rows: 2,
    };
    let error =
        block_on(transaction.execute_conditional_update(request, &CancellationToken::new()))
            .expect_err("policy sonda dopo UPDATE");
    assert_eq!(error.phase, ErrorPhase::Validate);
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_unknown(&error, "sonda dopo UPDATE");
}

#[test]
fn oracle_probe_policy_after_send_requires_recovery() {
    assert_probe_policy_after_update(Scenario::OracleProbePolicy);
}

#[test]
fn db2_probe_policy_after_send_requires_recovery() {
    assert_probe_policy_after_update(Scenario::Db2ProbePolicy);
}

#[test]
fn wrapper_uses_send_evidence_instead_of_validation_phase() {
    for (scenario, effect) in [
        (Scenario::TrackedBeforeSend, RemoteEffect::None),
        (Scenario::TrackedAfterSend, RemoteEffect::Unknown),
    ] {
        let mut tx =
            CallerSqlTransaction::new(Box::new(FakeScope(scenario, SendMarker::default())));
        let error =
            block_on(tx.execute(&Statement::new("DROP TABLE t"), &CancellationToken::new()))
                .unwrap_err();
        assert_eq!(error.phase, ErrorPhase::Validate);
        assert_eq!(error.remote_effect, effect);
    }
}

#[test]
fn unpolled_driver_future_is_not_a_send() {
    let sent = SendMarker::before_send();
    drop(sent.sending(async {}));
    assert_eq!(
        sent.failure(DatabaseError::invalid_plan("locale"))
            .remote_effect,
        RemoteEffect::None
    );
    assert_unknown(
        &SendMarker::default().failure(DatabaseError::invalid_plan("senza marcatore")),
        "default conservativo",
    );
}
