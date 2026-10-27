//! La campagna deve provare tutti i vicini anche se un confronto condanna la sessione.
use super::*;
use plenora_database_core::arrow::SchemaRef;
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ConnectionInfo, Inspection, PreparedWrite, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::transaction::TransactionOptions;
use plenora_database_core::transaction::{CommitOutcome, ConditionalUpdate, RowStream};
use plenora_database_core::{ErrorCategory, Row};
use std::future::Future;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Evidence {
    opened: usize,
    compared: usize,
    rolled_back: usize,
}
struct Fake {
    evidence: Arc<Mutex<Evidence>>,
    rollback_fails: bool,
}
struct Scope {
    evidence: Arc<Mutex<Evidence>>,
    poisoned: bool,
    rollback_fails: bool,
}
fn refusal() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Read,
        Some(ProviderKind::Sqlserver),
        "rifiuto simulato",
    )
}
fn row(value: V) -> Vec<Row> {
    vec![Row::try_new(Arc::from(["v".to_owned()]), vec![value]).expect("riga")]
}
impl Provider for Fake {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Sqlserver
    }
    fn test_connection<'a>(
        &'a self,
        _secret: &'a SecretString,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        Box::pin(async { Err(refusal()) })
    }
    fn probe_capabilities<'a>(
        &'a self,
        _secret: &'a SecretString,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        Box::pin(async { Err(refusal()) })
    }
    fn inspect<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a Operation,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        Box::pin(async { Err(refusal()) })
    }
    fn read<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a ReadOperation,
        _parameters: &'a ParameterBag,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async { Err(refusal()) })
    }
    fn prepare_write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a WriteOperation,
        _input_schema: SchemaRef,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        Box::pin(async { Err(refusal()) })
    }
    fn write<'a>(
        &'a self,
        _secret: &'a SecretString,
        _prepared: PreparedWrite,
        _input: Box<dyn BatchStream>,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome> {
        Box::pin(async { Err(refusal()) })
    }
    fn open_transaction<'a>(
        &'a self,
        _secret: &'a SecretString,
        _options: &'a TransactionOptions,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn TransactionScope>> {
        plenora_database_core::caller_sql::open_transaction(Box::pin(async move {
            self.evidence.lock().expect("evidence").opened += 1;
            Ok(Box::new(Scope {
                evidence: Arc::clone(&self.evidence),
                poisoned: false,
                rollback_fails: self.rollback_fails,
            }) as Box<dyn TransactionScope>)
        }))
    }
}
impl TransactionScope for Scope {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Sqlserver
    }
    fn execute<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _cancel: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        assert!(!self.poisoned);
        Box::pin(async { Ok(1) })
    }
    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        _cancel: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        assert!(
            !self.poisoned,
            "nessun SQL dopo il rifiuto nella stessa sessione"
        );
        let result = match statement.sql.as_str() {
            "raw" => Ok(row(V::String("1".to_owned()))),
            "Select" => Ok(row(V::I32(1))),
            "Count" => {
                self.evidence.lock().expect("evidence").compared += 1;
                self.poisoned = true;
                Err(refusal())
            }
            other => panic!("query inattesa: {other}"),
        };
        Box::pin(async move { result })
    }
    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async { Err(refusal()) })
    }
    fn savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        panic!("un savepoint non recupera una sessione condannata")
    }
    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        panic!("un savepoint non recupera una sessione condannata")
    }
    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        panic!("un savepoint non recupera una sessione condannata")
    }
    fn execute_conditional_update<'a>(
        &'a mut self,
        _request: ConditionalUpdate<'a>,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Err(refusal()) })
    }
    fn commit(
        self: Box<Self>,
        _cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async { Err(refusal()) })
    }
    fn rollback(self: Box<Self>, _cancel: &CancellationToken) -> ProviderFuture<'_, ()> {
        self.evidence.lock().expect("evidence").rolled_back += 1;
        Box::pin(async move {
            if self.rollback_fails {
                Err(refusal())
            } else {
                Ok(())
            }
        })
    }
}

#[test]
fn poisoned_comparisons_get_fresh_transactions_and_cleanup_is_required() {
    let kind = Kind::Integer {
        min: -100,
        max: 100,
    };
    let set = values(kind);
    let index = set
        .iter()
        .position(|v| *v == V::I64(1))
        .expect("uno generato");
    let expected = 1 + comparisons(&set, index).len();
    assert!(expected > 1);
    for rollback_fails in [false, true] {
        let evidence = Arc::new(Mutex::new(Evidence::default()));
        let provider = Fake {
            evidence: Arc::clone(&evidence),
            rollback_fails,
        };
        let secret = SecretString::new("unused");
        let sweep = SweepTarget {
            target: Target {
                provider: &provider,
                secret: &secret,
                limits: ResourceLimits::default(),
                table: ObjectRef {
                    catalog: None,
                    schema: None,
                    object: "t".to_owned(),
                },
                id_column: "id",
                native: &|op, _| format!("{op:?}"),
                native_cross_type_declared: false,
            },
            raw: &|_| "raw".to_owned(),
            facts: ServerFacts::default(),
            part: Some(index..index + 1),
        };
        let column = Column {
            name: "v",
            kind,
            render: Render::Decimal,
        };
        let mut outcome = SweepOutcome::default();
        let mut future = Box::pin(write_and_compare(
            &sweep,
            &column,
            FidelityPath::Native,
            1,
            &mut outcome,
        ));
        let result = match future
            .as_mut()
            .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
        {
            std::task::Poll::Ready(result) => result,
            std::task::Poll::Pending => panic!("finto sincrono"),
        };
        assert_eq!(result.is_err(), rollback_fails);
        let evidence = evidence.lock().expect("evidence");
        assert_eq!(evidence.compared, if rollback_fails { 1 } else { expected });
        assert_eq!(evidence.opened, evidence.compared);
        assert_eq!(evidence.rolled_back, evidence.compared);
    }
}
