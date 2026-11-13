//! Ledger posseduto dall'esecutore, mai dal codec. Nessuna conferma prima del commit.
use crate::execution_loss::{invalid, Basis, EffectiveEntry, EvaluationEntry, ExecutionLossReport};
use crate::loss::{LossConsent, MappingPolicy};
use crate::loss_registry::RuleId;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;

type Key = (u32, RuleId, Basis);
type Row = (u64, u64, Option<(u32, RuleId)>);
type Cell = (Row, u32, RuleId);

/// Identità sorgente, indipendente dai batch di trasporto e dai retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LossToken {
    owner: u64,
    batch: u64,
    row: u64,
    field: u32,
    rule: RuleId,
}
impl LossToken {
    const fn row(self) -> Row {
        (self.batch, self.row, Some((self.field, self.rule)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classification {
    Exact(Basis),
    WouldChange(Basis),
    HardReject,
}
impl Classification {
    /// Decisione distinta dalla classificazione e dai conteggi.
    /// # Errors
    /// Strict, chiavi e rifiuti indipendenti non vengono autorizzati dal consenso.
    pub fn authorize(self, policy: MappingPolicy, consent: LossConsent, key: bool) -> Result<()> {
        if matches!(self, Self::HardReject)
            || (matches!(self, Self::WouldChange(_))
                && (key || policy == MappingPolicy::Strict || !consent.accepted()))
        {
            return Err(DatabaseError::new(
                ErrorCategory::DataMapping,
                ErrorPhase::Write,
                None,
                "trasformazione del valore non autorizzata",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowState {
    Checked,
    Sent,
    Applied,
    Discarded,
}

#[derive(Clone)]
pub struct LossLedger {
    identity: u64,
    report: ExecutionLossReport,
    batch: Option<u64>,
    retired: bool,
    cells: BTreeMap<Cell, Classification>,
    rows: BTreeMap<Row, RowState>,
    pending: BTreeMap<Key, u64>,
    sent: BTreeMap<Key, u64>,
    max_cells: usize,
    persistent: bool,
}
impl LossLedger {
    /// # Errors
    /// Valida il contratto e il budget worst-case prima degli effetti.
    pub fn new(
        report: ExecutionLossReport,
        transport_bytes: usize,
        max_batch_cells: usize,
    ) -> Result<Self> {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let identity = NEXT_ID
            .fetch_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |id| id.checked_add(1),
            )
            .map_err(|_| limit())?;
        report.validate_transport_budget(transport_bytes)?;
        if report.evaluation != crate::execution_loss::Evaluation::default()
            || !report.effective.is_empty()
        {
            return Err(invalid());
        }
        Ok(Self {
            identity,
            report,
            batch: None,
            retired: false,
            cells: BTreeMap::new(),
            rows: BTreeMap::new(),
            pending: BTreeMap::new(),
            sent: BTreeMap::new(),
            max_cells: max_batch_cells,
            persistent: false,
        })
    }
    /// # Errors
    /// Un batch vecchio non puo essere ricontato; il batch corrente e idempotente.
    pub fn begin_batch(&mut self, batch: u64) -> Result<()> {
        if self.persistent {
            return Err(invalid());
        }
        if self.batch == Some(batch) {
            return if self.retired { Err(invalid()) } else { Ok(()) };
        }
        if self.batch.is_some_and(|last| batch < last) {
            return Err(invalid());
        }
        if self.rows.values().any(|s| *s == RowState::Sent) {
            return Err(invalid());
        }
        self.cells.clear();
        self.rows.clear();
        self.batch = Some(batch);
        self.retired = false;
        Ok(())
    }
    /// Una cella esterna per regola. Le foglie vengono combinate prima della chiamata.
    /// # Errors
    /// Rifiuta visite incoerenti, overflow, rischio assente e budget esaurito.
    pub fn observe(
        &mut self,
        row: u64,
        field: u32,
        rule: RuleId,
        classification: Classification,
    ) -> Result<()> {
        if self.persistent {
            return Err(invalid());
        }
        self.observe_cell(
            (self.batch.ok_or_else(invalid)?, row, None),
            field,
            rule,
            classification,
        )
    }
    /// Conserva le identità fino all'esito target, anche fra batch sorgente.
    /// # Errors
    /// Rifiuta batch regressivi e la commistione con l'adapter storico.
    pub fn begin_source_batch(&mut self, batch: u64) -> Result<()> {
        if (!self.persistent && self.batch.is_some())
            || self.retired
            || self.batch.is_some_and(|last| batch < last)
        {
            return Err(invalid());
        }
        self.persistent = true;
        self.batch = Some(batch);
        Ok(())
    }
    /// # Errors
    /// La stessa identità è idempotente solo con la stessa classificazione.
    pub fn observe_token(
        &mut self,
        row: u64,
        field: u32,
        rule: RuleId,
        classification: Classification,
    ) -> Result<LossToken> {
        if !self.persistent {
            return Err(invalid());
        }
        let token = LossToken {
            owner: self.identity,
            batch: self.batch.ok_or_else(invalid)?,
            row,
            field,
            rule,
        };
        self.observe_cell(token.row(), field, rule, classification)?;
        Ok(token)
    }
    fn observe_cell(
        &mut self,
        row: Row,
        field: u32,
        rule: RuleId,
        classification: Classification,
    ) -> Result<()> {
        if self.batch.is_none() || self.retired || self.report.evaluation.complete {
            return Err(invalid());
        }
        let cell = (row, field, rule);
        if let Some(old) = self.cells.get(&cell) {
            return if *old == classification {
                Ok(())
            } else {
                Err(invalid())
            };
        }
        if self
            .rows
            .get(&row)
            .is_some_and(|state| *state != RowState::Checked)
        {
            return Err(invalid());
        }
        if self.cells.len() >= self.max_cells {
            return Err(limit());
        }
        if !self
            .report
            .risk
            .iter()
            .any(|r| (r.field_id, r.rule_id) == (field, rule))
        {
            return Err(invalid());
        }
        let (basis, changes) = match classification {
            Classification::Exact(b) => (b, 0),
            Classification::WouldChange(b) => (b, 1),
            Classification::HardReject => return Err(invalid()),
        };
        if !rule.bases().contains(&basis) {
            return Err(invalid());
        }
        if let Some(entry) = self
            .report
            .evaluation
            .entries
            .iter_mut()
            .find(|r| (r.field_id, r.rule_id, r.basis) == (field, rule, basis))
        {
            let checked = entry.checked.checked_add(1).ok_or_else(limit)?;
            let total_changes = entry.would_change.checked_add(changes).ok_or_else(limit)?;
            entry.checked = checked;
            entry.would_change = total_changes;
        } else {
            self.report.evaluation.entries.push(EvaluationEntry {
                field_id: field,
                rule_id: rule,
                basis,
                checked: 1,
                would_change: changes,
            });
        }
        self.cells.insert(cell, classification);
        self.rows.insert(row, RowState::Checked);
        Ok(())
    }
    fn events(&self, row: Row) -> BTreeMap<Key, u64> {
        self.cells
            .iter()
            .filter_map(|(&(r, field, rule), c)| {
                if r == row {
                    if let Classification::WouldChange(basis) = c {
                        return Some(((field, rule, *basis), 1));
                    }
                }
                None
            })
            .collect()
    }
    /// Chiamare immediatamente prima dell'invio; solo questi eventi possono diventare incerti.
    /// # Errors
    /// Una riga non puo essere inviata due volte nel ledger.
    pub fn sent(&mut self, row: u64) -> Result<()> {
        self.mark_sent((self.batch.ok_or_else(invalid)?, row, None))
    }
    /// # Errors
    /// Separa l'invio al target dal semplice trasporto allo stage.
    pub fn sent_token(&mut self, token: LossToken) -> Result<()> {
        if token.owner != self.identity {
            return Err(invalid());
        }
        self.mark_sent(token.row())
    }
    fn mark_sent(&mut self, row: Row) -> Result<()> {
        if self.rows.get(&row) != Some(&RowState::Checked) {
            return Err(invalid());
        }
        let events = self.events(row);
        add(&mut self.sent, &events)?;
        self.rows.insert(row, RowState::Sent);
        Ok(())
    }
    /// # Errors
    /// Associa solo le righe realmente applicate, dopo successo dello statement.
    pub fn applied(&mut self, row: u64) -> Result<()> {
        self.mark_applied((self.batch.ok_or_else(invalid)?, row, None))
    }
    /// # Errors
    /// Conferma soltanto la cella attribuita dal chiamante.
    pub fn applied_token(&mut self, token: LossToken) -> Result<()> {
        if token.owner != self.identity {
            return Err(invalid());
        }
        self.mark_applied(token.row())
    }
    fn mark_applied(&mut self, row: Row) -> Result<()> {
        if self.rows.get(&row) != Some(&RowState::Sent) {
            return Err(invalid());
        }
        let events = self.events(row);
        add(&mut self.pending, &events)?;
        subtract(&mut self.sent, &events)?;
        self.rows.insert(row, RowState::Applied);
        Ok(())
    }
    /// Rifiuto di riga, no-match, DO NOTHING o rollback confermato al savepoint.
    /// # Errors
    /// Rifiuta righe sconosciute; non cancella evaluation.
    pub fn discard(&mut self, row: u64) -> Result<()> {
        self.mark_discarded((self.batch.ok_or_else(invalid)?, row, None))
    }
    /// # Errors
    /// Scarta un ramo escluso o annullato senza cancellare evaluation.
    pub fn discard_token(&mut self, token: LossToken) -> Result<()> {
        if token.owner != self.identity {
            return Err(invalid());
        }
        self.mark_discarded(token.row())
    }
    /// Riutilizza la conversione autorizzata dopo annullamento certo del tentativo.
    /// # Errors
    /// Solo Discarded può tornare Checked; evaluation non viene incrementata.
    pub fn retry_token(&mut self, token: LossToken) -> Result<()> {
        if token.owner != self.identity || self.rows.get(&token.row()) != Some(&RowState::Discarded)
        {
            return Err(invalid());
        }
        self.rows.insert(token.row(), RowState::Checked);
        Ok(())
    }
    fn mark_discarded(&mut self, row: Row) -> Result<()> {
        let events = self.events(row);
        match self.rows.get(&row).ok_or_else(invalid)? {
            RowState::Sent => subtract(&mut self.sent, &events)?,
            RowState::Applied => subtract(&mut self.pending, &events)?,
            RowState::Checked | RowState::Discarded => {}
        }
        self.rows.insert(row, RowState::Discarded);
        Ok(())
    }
    pub const fn complete(&mut self) {
        self.report.evaluation.complete = true;
    }
    /// Commit confermato di un'unita, utilizzabile anche da provider con commit parziale.
    /// # Errors
    /// Nessuna riga inviata puo restare senza esito; contatori sempre checked.
    pub fn commit(&mut self) -> Result<()> {
        if self.rows.values().any(|state| *state == RowState::Sent) {
            return Err(invalid());
        }
        self.publish(false)?;
        self.retire();
        Ok(())
    }
    /// Finalizza una singola transazione usando gli stessi assi per esiti ed errori.
    /// La preview validata prima del COMMIT non e mai pubblicata dopo un rollback.
    /// # Errors
    /// Rifiuta contatori incerti non rappresentabili.
    pub fn finalize(
        &mut self,
        result: &Result<crate::outcome::WriteOutcome>,
        confirmed: ExecutionLossReport,
    ) -> Result<ExecutionLossReport> {
        let effect = match result {
            Ok(outcome) => outcome.remote_effect(),
            Err(error) => error.remote_effect,
        };
        match effect {
            crate::RemoteEffect::Committed => Ok(confirmed),
            crate::RemoteEffect::None | crate::RemoteEffect::RolledBack => {
                self.rollback();
                Ok(self.report())
            }
            crate::RemoteEffect::Partial | crate::RemoteEffect::Unknown => {
                self.uncertain()?;
                Ok(self.report())
            }
        }
    }
    pub fn rollback(&mut self) {
        self.pending.clear();
        self.sent.clear();
        self.retire();
    }
    /// # Errors
    /// Pubblica separatamente soltanto eventi inviati e non annullati definitivamente.
    pub fn uncertain(&mut self) -> Result<()> {
        add(&mut self.pending, &self.sent)?;
        self.sent.clear();
        self.publish(true)?;
        self.retire();
        Ok(())
    }
    fn retire(&mut self) {
        self.retired = true;
        self.cells.clear();
        self.rows.clear();
    }
    fn publish(&mut self, uncertain: bool) -> Result<()> {
        let mut entries = self.report.effective.clone();
        for (&(field_id, rule_id, basis), &count) in &self.pending {
            if count == 0 {
                continue;
            }
            if let Some(entry) = entries
                .iter_mut()
                .find(|e| (e.field_id, e.rule_id, e.basis) == (field_id, rule_id, basis))
            {
                let target = if uncertain {
                    &mut entry.uncertain
                } else {
                    &mut entry.committed
                };
                *target = target.checked_add(count).ok_or_else(limit)?;
            } else {
                entries.push(EffectiveEntry {
                    field_id,
                    rule_id,
                    category: rule_id.definition().category,
                    basis,
                    committed: if uncertain { 0 } else { count },
                    uncertain: if uncertain { count } else { 0 },
                });
            }
        }
        self.report.effective = entries;
        self.pending.clear();
        Ok(())
    }
    #[must_use]
    pub fn report(&self) -> ExecutionLossReport {
        let mut report = self.report.clone();
        report.sort();
        report
    }
}
fn add(target: &mut BTreeMap<Key, u64>, source: &BTreeMap<Key, u64>) -> Result<()> {
    let mut next = target.clone();
    for (&key, &count) in source {
        let entry = next.entry(key).or_default();
        *entry = entry.checked_add(count).ok_or_else(limit)?;
    }
    *target = next;
    Ok(())
}
fn subtract(target: &mut BTreeMap<Key, u64>, source: &BTreeMap<Key, u64>) -> Result<()> {
    for (key, count) in source {
        let entry = target.get_mut(key).ok_or_else(invalid)?;
        *entry = entry.checked_sub(*count).ok_or_else(invalid)?;
    }
    target.retain(|_, count| *count != 0);
    Ok(())
}

fn limit() -> DatabaseError {
    let mut error = crate::execution_loss::limit();
    error.phase = ErrorPhase::Write;
    error
}
#[cfg(test)]
#[path = "loss_ledger_tests.rs"]
mod tests;
