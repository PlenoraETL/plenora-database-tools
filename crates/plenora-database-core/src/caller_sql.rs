//! Il punto obbligato dello SQL del chiamante.
//!
//! Ogni operazione pubblica che invia SQL scritto dal chiamante — `execute`,
//! `query`, `query_stream` con il suo stream, `execute_graph`, l'update
//! condizionale di una transazione, ed `execute_ddl` del provider — passa da
//! qui. Una volta inviato quell'SQL, il contesto non prova che sia rimasto
//! senza effetto: un DDL Oracle o MySQL fa commit implicito di cio che lo
//! precede, una procedura PostgreSQL chiamata in autocommit o un trigger
//! autonomo fa commit da solo, un batch T-SQL contiene piu istruzioni, una
//! funzione puo scrivere anche dentro un SELECT. Qualunque errore esca per
//! tutta la vita dell'operazione — invio, fetch, decodifica, cancellazione,
//! timeout, worker fallito — passa da [`DatabaseError::after_unproven_send`]:
//! mai `none`.
//!
//! Gli adapter non applicano la regola da soli: implementano
//! [`Provider::open_transaction`](crate::provider::Provider::open_transaction)
//! e [`Provider::run_ddl`](crate::provider::Provider::run_ddl), passando dai
//! rispettivi helper di questo modulo anche per le chiamate dirette. La prova
//! d'inventario copre tutti i metodi pubblici e verifica entrambi gli ingressi.
//!
//! # Limite dichiarato
//!
//! **Regola**: un errore di fase `validate` resta com'e, perche quella fase
//! e per definizione la validazione locale prima di qualunque I/O (policy
//! nativa, nomi, argomenti). **Ambito**: un errore locale dell'adapter in
//! un'altra fase prima dell'invio — un parametro non convertibile, una
//! transazione gia chiusa — esce come ignoto. **Hazard**: nessuno per la
//! correttezza (ignoto non afferma nulla di falso), ma il chiamante riceve
//! `requires_recovery` dove `none` sarebbe stato vero. **Rientro**: un
//! confine esplicito fra preparazione e invio nel trait degli adapter.

use crate::error::{DatabaseError, ErrorPhase};
use crate::graph::{GraphRow, GraphStatement};
use crate::provider::ProviderFuture;
use crate::row::Row;
use crate::transaction::{
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionScope,
};
use crate::CancellationToken;

/// La regola, in un punto solo: dopo l'invio di SQL del chiamante un errore
/// non e mai `none`. Un errore di fase `validate` precede l'invio.
#[must_use]
pub fn caller_sql_failure(error: DatabaseError) -> DatabaseError {
    if error.phase == ErrorPhase::Validate {
        error
    } else {
        error.after_unproven_send()
    }
}

/// Protegge anche l'ingresso pubblico diretto dell'adapter.
#[must_use]
pub fn open_transaction(
    call: ProviderFuture<'_, Box<dyn TransactionScope>>,
) -> ProviderFuture<'_, Box<dyn TransactionScope>> {
    Box::pin(async move {
        Ok(Box::new(CallerSqlTransaction::new(call.await?)) as Box<dyn TransactionScope>)
    })
}

/// Controlla l'annullamento prima di interrogare l'adapter; dopo l'invio
/// conserva l'incertezza anche quando il driver restituisce un rifiuto.
#[must_use]
pub fn run_ddl<'a>(
    provider: crate::plan::ProviderKind,
    cancellation: &'a CancellationToken,
    call: ProviderFuture<'a, ()>,
) -> ProviderFuture<'a, ()> {
    Box::pin(async move {
        if cancellation.is_cancelled() {
            return Err(DatabaseError::interrupted(
                cancellation,
                Some(provider),
                ErrorPhase::Write,
                "DDL interrotto prima dell'invio",
            ));
        }
        call.await.map_err(caller_sql_failure)
    })
}

/// Una transazione il cui SQL del chiamante passa dalla regola.
///
/// Si costruisce soltanto dal core, tramite [`open_transaction`].
pub struct CallerSqlTransaction {
    inner: Box<dyn TransactionScope>,
}

impl CallerSqlTransaction {
    pub(crate) fn new(inner: Box<dyn TransactionScope>) -> Self {
        Self { inner }
    }
}

impl TransactionScope for CallerSqlTransaction {
    fn provider_kind(&self) -> crate::plan::ProviderKind {
        self.inner.provider_kind()
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        let call = self.inner.execute(statement, cancellation);
        Box::pin(async move { call.await.map_err(caller_sql_failure) })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        let call = self.inner.query(statement, cancellation);
        Box::pin(async move { call.await.map_err(caller_sql_failure) })
    }

    fn execute_graph<'a>(
        &'a mut self,
        statement: &'a GraphStatement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<GraphRow>> {
        let call = self.inner.execute_graph(statement, cancellation);
        Box::pin(async move { call.await.map_err(caller_sql_failure) })
    }

    fn query_stream<'a>(
        &'a mut self,
        statement: &'a Statement,
        batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        let call = self.inner.query_stream(statement, batch_size, cancellation);
        Box::pin(async move {
            let stream = call.await.map_err(caller_sql_failure)?;
            Ok(Box::new(CallerSqlRowStream { inner: stream }) as Box<dyn RowStream + Send + 'a>)
        })
    }

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        self.inner.savepoint(name, cancellation)
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        self.inner.rollback_to_savepoint(name, cancellation)
    }

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        self.inner.release_savepoint(name, cancellation)
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let call = self.inner.execute_conditional_update(request, cancellation);
        Box::pin(async move { call.await.map_err(caller_sql_failure) })
    }

    fn commit(
        self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        self.inner.commit(cancellation)
    }

    fn rollback(self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        self.inner.rollback(cancellation)
    }
}

/// Lo stream di una query del chiamante: ogni fetch, decodifica,
/// cancellazione o timeout passa dalla regola.
struct CallerSqlRowStream<'a> {
    inner: Box<dyn RowStream + Send + 'a>,
}

impl RowStream for CallerSqlRowStream<'_> {
    fn next_batch<'b>(
        &'b mut self,
        cancellation: &'b CancellationToken,
    ) -> ProviderFuture<'b, Option<Vec<Row>>> {
        let call = self.inner.next_batch(cancellation);
        Box::pin(async move { call.await.map_err(caller_sql_failure) })
    }
}

#[cfg(test)]
#[path = "caller_sql_tests.rs"]
mod tests;
