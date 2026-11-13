//! Contesto comune agli adapter, senza SQL o decisioni del codec.
use crate::execution_loss::{limit, ExecutionLossReport};
use crate::loss::{LossConsent, MappingPolicy};
use crate::loss_ledger::{Classification, LossLedger, LossToken};
use crate::loss_registry::{compile_instances, RuleId, RuleInstance};
use crate::plan::ProviderKind;
use crate::resource::{ResourceBudget, ResourceKind, ResourceLease};
use crate::Result;

// Riserva conservativa dei nuovi percorsi: per cella copre due mappe BTree
// anche con un nodo allocato per elemento, token/maschere, temporanei e un
// clone completo alla volta. 16 KiB supera 4 * 12 slot * 256 byte su 64 bit.
// Gli aggregati per regola e le stringhe sono riservati separatamente.
const CELL_BYTES: u64 = 16_384;

// PG attribuisce righe intere e conserva il vettore booleano; gli altri adapter
// attribuiscono colonne. La transizione di stato del ledger resta unica.
type TrackedCell = (u64, usize);
enum Tracking {
    Rows(Vec<bool>),
    Cells(Vec<Vec<TrackedCell>>),
}
impl Tracking {
    fn new(rows_only: bool, rows: usize) -> Self {
        if rows_only {
            Self::Rows(vec![false; rows])
        } else {
            Self::Cells(vec![Vec::new(); rows])
        }
    }
    const fn is_rows(&self) -> bool {
        matches!(self, Self::Rows(_))
    }
    fn entries(&self, row: usize) -> Result<(Option<TrackedCell>, &[TrackedCell])> {
        match self {
            Self::Rows(flags) => {
                let used = *flags.get(row).ok_or_else(crate::execution_loss::invalid)?;
                Ok((used.then_some((row as u64, 0)), &[]))
            }
            Self::Cells(rows) => Ok((
                None,
                rows.get(row).ok_or_else(crate::execution_loss::invalid)?,
            )),
        }
    }
}

/// Possiede le riserve e la preview non pubblicata.
pub struct ExecutionLossContext {
    pub ledger: LossLedger,
    pub confirmed: ExecutionLossReport,
    pub consent: LossConsent,
    policy: MappingPolicy,
    sequence: u64,
    budget: ResourceBudget,
    pub lease: Option<ResourceLease>,
    _report_lease: Option<ResourceLease>,
    tracked: Tracking,
    sources: Vec<(u64, usize, usize, ResourceLease)>,
    instances: Vec<RuleInstance>,
    rule_count: usize,
    legacy_accounting: bool,
}
impl ExecutionLossContext {
    /// # Errors
    /// Compila il rischio e riserva il rapporto prima dei valori.
    pub fn new(
        provider: ProviderKind,
        policy: MappingPolicy,
        consent: LossConsent,
        rules: Vec<RuleInstance>,
        budget: &ResourceBudget,
    ) -> Result<(Self, Vec<RuleInstance>)> {
        let (rules, report) = compile_instances(provider, policy, rules)?;
        let legacy_accounting =
            matches!(provider, ProviderKind::Postgres | ProviderKind::Sqlserver);
        let report_bytes = if legacy_accounting {
            0
        } else {
            serde_json::to_vec(&report).map_err(|_| limit())?.len() as u64
        };
        // Include stringhe, istanze, aggregati, rapporti e preview.
        let bytes = report_bytes
            .checked_mul(8)
            .and_then(|n| n.checked_add((rules.len() as u64).checked_mul(8192)?))
            .ok_or_else(limit)?;
        let report_lease = if legacy_accounting {
            None
        } else {
            Some(budget.try_lease(ResourceKind::MemoryBytes, bytes)?)
        };
        let divisor = match provider {
            ProviderKind::Postgres => 128,
            ProviderKind::Sqlserver => 192,
            _ => CELL_BYTES,
        };
        let context = Self {
            ledger: LossLedger::new(
                report.clone(),
                usize::try_from(budget.limits().output_bytes).unwrap_or(usize::MAX),
                usize::try_from(budget.limits().memory_bytes / divisor).unwrap_or(usize::MAX),
            )?,
            confirmed: report,
            consent,
            policy,
            sequence: 0,
            budget: budget.clone(),
            lease: None,
            _report_lease: report_lease,
            tracked: Tracking::new(provider == ProviderKind::Postgres, 0),
            sources: Vec::new(),
            instances: if legacy_accounting {
                Vec::new()
            } else {
                rules.clone()
            },
            rule_count: rules.len(),
            legacy_accounting,
        };
        Ok((context, rules))
    }
    /// # Errors
    /// Riserva celle, indici, maschere e clone prima delle osservazioni.
    pub fn begin_batch(&mut self, rows: usize, rules: usize) -> Result<()> {
        if rules != self.rule_count {
            return Err(crate::execution_loss::invalid());
        }
        self.begin_tracked_batch(rows, rows.checked_mul(rules).ok_or_else(limit)?)
    }
    /// Metadata values are sparse: one cell per declared subject, not fields times rules.
    /// # Errors
    /// Refuses a budget overflow before observing or sending any metadata.
    pub fn begin_metadata_batch(&mut self, fields: usize) -> Result<()> {
        let cells = self
            .confirmed
            .risk
            .iter()
            .filter(|r| r.subject.is_some())
            .count();
        self.begin_tracked_batch(fields, cells)
    }
    fn begin_tracked_batch(&mut self, rows: usize, cells: usize) -> Result<()> {
        let cell_bytes = if self.legacy_accounting {
            192
        } else {
            CELL_BYTES
        };
        let row_bytes = if self.legacy_accounting { 0 } else { 64 };
        let bytes = (cells as u64)
            .checked_mul(cell_bytes)
            .and_then(|n| n.checked_add((rows as u64).checked_mul(row_bytes)?))
            .ok_or_else(limit)?;
        let sequence = self.sequence.checked_add(1).ok_or_else(limit)?;
        self.ledger.begin_batch(sequence)?;
        self.tracked = Tracking::new(self.tracked.is_rows(), 0);
        self.lease = None;
        self.lease = (bytes > 0)
            .then(|| self.budget.try_lease(ResourceKind::MemoryBytes, bytes))
            .transpose()?;
        self.tracked = Tracking::new(self.tracked.is_rows(), rows);
        self.sequence = sequence;
        Ok(())
    }
    /// Conserva la selezione delle assegnazioni determinata dall'adapter.
    /// # Errors
    /// Rifiuta indici e cardinalità oltre la prenotazione.
    pub fn track(&mut self, row: usize, id: u64, column: usize) -> Result<()> {
        match &mut self.tracked {
            Tracking::Rows(flags) => {
                if id != row as u64 {
                    return Err(crate::execution_loss::invalid());
                }
                *flags
                    .get_mut(row)
                    .ok_or_else(crate::execution_loss::invalid)? = true;
            }
            Tracking::Cells(rows) => {
                let tracked = rows
                    .get_mut(row)
                    .ok_or_else(crate::execution_loss::invalid)?;
                if !tracked.iter().any(|(old, _)| *old == id) {
                    if tracked.len() >= self.rule_count {
                        return Err(limit());
                    }
                    tracked.push((id, column));
                }
            }
        }
        Ok(())
    }

    /// # Errors
    /// L'invio riguarda soltanto le identità osservate.
    pub fn sent(&mut self, row: usize) -> Result<()> {
        let (single, cells) = self.tracked.entries(row)?;
        for (id, _) in single.into_iter().chain(cells.iter().copied()) {
            self.ledger.sent(id)?;
        }
        Ok(())
    }
    /// # Errors
    /// La maschera proviene dall'esito target, non da `affected_rows` aggregato.
    pub fn attributed(&mut self, row: usize, assigned: impl Fn(usize) -> bool) -> Result<()> {
        let (single, cells) = self.tracked.entries(row)?;
        for (id, column) in single.into_iter().chain(cells.iter().copied()) {
            if assigned(column) {
                self.ledger.applied(id)?;
            } else {
                self.ledger.discard(id)?;
            }
        }
        Ok(())
    }
    /// # Errors
    /// Riserva token persistenti, maschere e buffer trasformati prima della valutazione.
    pub fn begin_source_batch(
        &mut self,
        batch: u64,
        rows: usize,
        rules: usize,
        buffer_bytes: u64,
    ) -> Result<()> {
        if self.legacy_accounting
            || rules != self.instances.len()
            || self.sources.iter().any(|(id, _, _, _)| *id == batch)
        {
            return Err(crate::execution_loss::invalid());
        }
        let bytes = (rows as u64)
            .checked_mul(rules as u64)
            .and_then(|n| n.checked_mul(CELL_BYTES))
            .and_then(|n| n.checked_add((rows as u64).checked_mul(64)?))
            .and_then(|n| n.checked_add(buffer_bytes))
            // Anche un batch vuoto occupa l'indice e la lease; include capacità Vec.
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(limit)?;
        let lease = self.budget.try_lease(ResourceKind::MemoryBytes, bytes)?;
        self.ledger.begin_source_batch(batch)?;
        self.sources.push((batch, rows, rules, lease));
        Ok(())
    }
    /// # Errors
    /// Validazione indipendente, classificazione, consenso e trasformazione in ordine.
    /// Il valore restituito si riusa nel bind e nei retry senza nuova valutazione.
    pub fn evaluate<T>(
        &mut self,
        row: u64,
        instance: &RuleInstance,
        key: bool,
        validate_classify: impl FnOnce() -> Result<(Classification, T)>,
        transform: impl FnOnce(T) -> Result<T>,
    ) -> Result<(LossToken, T)> {
        let (_, rows, rules, _) = self
            .sources
            .last()
            .ok_or_else(crate::execution_loss::invalid)?;
        if row >= *rows as u64 || *rules == 0 || !self.instances.contains(instance) {
            return Err(crate::execution_loss::invalid());
        }
        let (classification, value) = validate_classify()?;
        if classification == Classification::HardReject {
            classification.authorize(self.policy, self.consent, key)?;
        }
        let token =
            self.ledger
                .observe_token(row, instance.field_id, instance.rule_id, classification)?;
        classification.authorize(self.policy, self.consent, key)?;
        let value = if matches!(classification, Classification::WouldChange(_)) {
            transform(value)?
        } else {
            value
        };
        Ok((token, value))
    }
    /// # Errors
    /// Osserva prima del rifiuto del consenso; `HardReject` non viene contato.
    pub fn observe(
        &mut self,
        row: u64,
        field: u32,
        rule: RuleId,
        classification: Classification,
        key: bool,
    ) -> Result<()> {
        if classification != Classification::HardReject {
            self.ledger.observe(row, field, rule, classification)?;
        }
        classification.authorize(self.policy, self.consent, key)
    }
    /// Checks one metadata value using the same authorization and ledger as rows.
    /// # Errors
    /// Refuses an unplanned key, strict loss, or missing actual-loss consent.
    pub fn observe_metadata(
        &mut self,
        row: u64,
        field: u32,
        subject: u32,
        classification: Classification,
    ) -> Result<()> {
        if classification != Classification::HardReject {
            self.ledger.observe_metadata(
                row,
                field,
                RuleId::PgMetadataKeyNotPreserved,
                subject,
                classification,
            )?;
        }
        classification.authorize(self.policy, self.consent, false)
    }
    /// # Errors
    /// Finalizza usando la preview posseduta dal contesto.
    pub fn finalize(
        &mut self,
        result: &Result<crate::outcome::WriteOutcome>,
    ) -> Result<ExecutionLossReport> {
        self.ledger.finalize(result, self.confirmed.clone())
    }
    /// # Errors
    /// Valida successo e incertezza senza pubblicare la preview.
    pub fn prepare_commit(&mut self, complete: bool) -> Result<()> {
        if complete {
            self.ledger.complete();
        }
        let mut preview = self.ledger.clone();
        preview.commit()?;
        let report = preview.report();
        report.validate()?;
        drop(preview);
        if !self.legacy_accounting {
            let mut uncertain = self.ledger.clone();
            uncertain.uncertain()?;
            uncertain.report().validate()?;
        }
        self.confirmed = report;
        Ok(())
    }
}
