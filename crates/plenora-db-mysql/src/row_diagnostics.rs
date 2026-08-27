//! Percorso di scrittura diagnostico `MySQL` riga per riga.
//!
//! Il percorso normale raggruppa più righe in un solo INSERT: è veloce, ma un
//! rifiuto del server non è attribuibile a una riga precisa, e il contratto
//! `plenora-row-diagnostics-v1` non ammette indici dedotti. Quando la sorgente
//! dichiara quante righe produrrà, la scrittura passa qui: **uno statement per
//! riga**, così l'indice sorgente del rifiuto è provato dallo statement stesso
//! e non da un messaggio vendor.
//!
//! La causa arriva dal codice del server; la colonna dal contratto dichiarato
//! della sorgente, verificato contro lo schema preparato prima della
//! transazione. La chiave viene pubblicata redatta, quindi nessun valore di
//! riga lascia il processo.

use crate::write::MysqlWritePlan;
use crate::MysqlSession;
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::provider::BatchStream;
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::row_diagnostics::{
    RowApplication, RowRejection, RowScopedWriter, RowWriteFuture,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition, RollbackEvidence,
};

/// Esecutore row-scoped sopra una transazione `MySQL` già aperta.
pub struct MysqlRowWriter<'a> {
    session: &'a mut MysqlSession,
    plan: &'a MysqlWritePlan,
    input: &'a mut dyn BatchStream,
    schema: &'a SchemaRef,
    budget: &'a ResourceBudget,
    cancellation: &'a CancellationToken,
    /// Colonna dichiarata dal piano, mai dedotta dal messaggio del server.
    constraint_column: Option<String>,
    /// Batch corrente e indice sorgente assoluto della sua prima riga.
    batch: Option<RecordBatch>,
    batch_start: u64,
    applied: u64,
}

impl<'a> MysqlRowWriter<'a> {
    pub const fn new(
        session: &'a mut MysqlSession,
        plan: &'a MysqlWritePlan,
        input: &'a mut dyn BatchStream,
        schema: &'a SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
        constraint_column: Option<String>,
    ) -> Self {
        Self {
            session,
            plan,
            input,
            schema,
            budget,
            cancellation,
            constraint_column,
            batch: None,
            batch_start: 0,
            applied: 0,
        }
    }

    /// Righe effettivamente applicate nella transazione aperta.
    pub const fn applied(&self) -> u64 {
        self.applied
    }

    /// Posiziona il cursore sulla riga sorgente richiesta.
    ///
    /// Consuma i batch dello stream finché l'indice assoluto non cade in
    /// quello corrente, e restituisce l'offset della riga dentro il batch.
    /// L'offset dentro il batch non lascia mai questo metodo: l'indice
    /// pubblicato resta quello della sorgente.
    async fn locate(&mut self, source_index: u64) -> Result<usize> {
        loop {
            if let Some(batch) = self.batch.as_ref() {
                let rows = batch_rows(batch)?;
                let end = checked_batch_end(self.batch_start, rows)?;
                if source_index < end {
                    let offset = source_index
                        .checked_sub(self.batch_start)
                        .ok_or_else(|| row_error("indice sorgente già superato"))?;
                    return usize::try_from(offset)
                        .map_err(|_| row_error("offset di batch non rappresentabile"));
                }
                self.batch_start = end;
                self.batch = None;
            }

            let batch = self
                .input
                .next_batch(self.cancellation)
                .await
                .map_err(|mut error| {
                    error.phase = ErrorPhase::Write;
                    error.provider = Some(crate::profile::PROVISIONAL_KIND);
                    error
                })?
                .ok_or_else(|| row_error("input esaurito prima delle righe dichiarate"))?;
            crate::write::validate_batch_schema(&batch, self.schema)?;
            if batch.num_rows() == 0 {
                continue;
            }
            let rows = batch_rows(&batch)?;
            self.plan.validate_spatial_batch(&batch, self.budget)?;
            // La quota viene consumata all'ingresso del batch invece che alla
            // sua fine: qui il batch resta vivo per molti statement, e un
            // rifiuto chiude comunque l'intera operazione, quindi non esiste
            // una finestra in cui restituire la quota abbia un significato.
            let lease = self.budget.try_lease(ResourceKind::Rows, rows)?;
            lease.commit(rows)?;
            self.batch = Some(batch);
        }
    }
}

impl RowScopedWriter for MysqlRowWriter<'_> {
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
        Box::pin(async move {
            let offset = self.locate(source_index).await?;
            let (sql, parameters) = {
                let batch = self
                    .batch
                    .as_ref()
                    .ok_or_else(|| row_error("batch assente dopo il posizionamento"))?;
                (
                    self.plan.render_insert(1)?,
                    self.plan.bind_chunk(batch, offset, 1)?,
                )
            };
            if let Some(cause) = self
                .session
                .exec_row_write(&sql, parameters, self.cancellation)
                .await?
            {
                Ok(RowApplication::Rejected(RowRejection {
                    cause: cause.to_owned(),
                    column: self.constraint_column.clone(),
                }))
            } else {
                self.applied = self
                    .applied
                    .checked_add(1)
                    .ok_or_else(|| row_error("overflow nelle righe applicate"))?;
                Ok(RowApplication::Applied)
            }
        })
    }

    fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>> {
        Box::pin(async move {
            if let Some(batch) = self.batch.take() {
                let end = checked_consumed_batch_end(
                    self.batch_start,
                    batch_rows(&batch)?,
                    self.applied,
                )?;
                self.batch_start = end;
            }
            loop {
                match self.input.next_batch(self.cancellation).await {
                    Ok(Some(batch)) if batch.num_rows() == 0 => {}
                    Ok(Some(_)) => {
                        return Err(row_error("input oltre il totale dichiarato"));
                    }
                    Ok(None) => return Ok(()),
                    Err(mut error) => {
                        error.phase = ErrorPhase::Write;
                        error.provider = Some(crate::profile::PROVISIONAL_KIND);
                        return Err(error);
                    }
                }
            }
        })
    }

    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
        Box::pin(async move {
            // L'annullamento non eredita la cancellazione dell'operazione: va
            // tentato comunque, ed è la sua conferma — non la sua richiesta —
            // a decidere che cosa il documento può dichiarare.
            let cleanup = CancellationToken::new();
            if self
                .session
                .exec_transaction(
                    crate::session::MysqlTransactionCommand::Rollback,
                    ErrorPhase::Rollback,
                    &cleanup,
                )
                .await
                .is_ok()
            {
                RollbackEvidence::Confirmed
            } else {
                self.session.discard().await;
                RollbackEvidence::Lost
            }
        })
    }
}

fn batch_rows(batch: &RecordBatch) -> Result<u64> {
    u64::try_from(batch.num_rows()).map_err(|_| row_error("righe batch non rappresentabili"))
}

fn checked_batch_end(batch_start: u64, rows: u64) -> Result<u64> {
    plenora_database_core::checked_source_row_end(batch_start, rows)
        .ok_or_else(|| row_error("overflow nell'offset sorgente"))
}

fn checked_consumed_batch_end(batch_start: u64, rows: u64, applied: u64) -> Result<u64> {
    let end = checked_batch_end(batch_start, rows)?;
    if end != applied {
        return Err(row_error("input oltre il totale dichiarato"));
    }
    Ok(end)
}

fn row_error(message: &'static str) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::InvalidPlan,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        provider: Some(crate::profile::PROVISIONAL_KIND),
        execution_id: None,
        message: message.to_owned(),
        diagnostics: None,
    }
}

#[cfg(test)]
#[path = "row_diagnostics_tests.rs"]
mod tests;
