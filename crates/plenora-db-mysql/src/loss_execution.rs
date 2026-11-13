//! Adapter MySQL: valori finali riusabili, identita sorgente e attribuzione target.
use crate::{loss_codec::RuleCodec, write::MysqlWritePlan};
use mysql_async::{Params, Value};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::execution_loss_context::ExecutionLossContext;
use plenora_database_core::loss::LossConsent;
use plenora_database_core::loss_ledger::LossToken;
use plenora_database_core::plan::{ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

/// Limite iniziale del percorso seriale; Strict non attraversa questo adapter.
pub const SERIAL_ROW_LIMIT: u64 = 10_000;
struct BoundRow {
    values: Vec<Value>,
    tokens: Vec<LossToken>,
    evaluated: bool,
}
pub struct Evaluation {
    pub context: ExecutionLossContext,
    codecs: Vec<(usize, bool, RuleCodec)>,
    rules: usize,
    sequence: u64,
    received: u64,
    serial: bool,
    keys: Vec<usize>,
    rows: Vec<Option<BoundRow>>,
    frozen: std::collections::VecDeque<Vec<Option<BoundRow>>>,
    replay: bool,
    row_leases: std::collections::VecDeque<plenora_database_core::resource::ResourceLease>,
    active_rows: Option<plenora_database_core::resource::ResourceLease>,
}
impl Evaluation {
    pub fn new(
        schema: &SchemaRef,
        plan: &MysqlWritePlan,
        operation: &WriteOperation,
        consent: LossConsent,
        budget: &ResourceBudget,
    ) -> Result<Self> {
        let mut codecs = Vec::new();
        for column in plan.rewrites() {
            let field = schema.fields().get(column.field).ok_or_else(invalid)?;
            let key = operation.keys.contains(field.name());
            let selected = operation.mode != WriteMode::Update
                || key
                || operation.update_columns.is_empty()
                || operation.update_columns.contains(field.name());
            if !selected {
                continue;
            }
            // RuleCodec compone tutte le rewrite di double(255,30) una sola volta.
            if let Some(rewrite) = column.rewrites.first() {
                let codec = RuleCodec::new(
                    ProviderKind::Mysql,
                    u32::try_from(column.field).map_err(|_| invalid())?,
                    field.data_type().clone(),
                    &column.target_type,
                    *rewrite,
                )?;
                if !codec.matches_rewrites(&column.rewrites) {
                    return Err(invalid());
                }
                codecs.push((column.field, key, codec));
            }
        }
        let instances = codecs
            .iter()
            .flat_map(|(_, _, c)| c.instances.clone())
            .collect();
        let (context, instances) = ExecutionLossContext::new(
            ProviderKind::Mysql,
            operation.mapping_policy,
            consent,
            instances,
            budget,
        )?;
        Ok(Self {
            context,
            codecs,
            rules: instances.len(),
            sequence: 0,
            received: 0,
            keys: operation
                .keys
                .iter()
                .map(|key| schema.index_of(key).map_err(|_| invalid()))
                .collect::<Result<Vec<_>>>()?,
            serial: matches!(operation.mode, WriteMode::Update | WriteMode::Upsert),
            rows: Vec::new(),
            frozen: std::collections::VecDeque::new(),
            replay: false,
            row_leases: std::collections::VecDeque::new(),
            active_rows: None,
        })
    }
    pub const fn is_replay(&self) -> bool {
        self.replay
    }
    pub fn reserve_rows(&mut self, budget: &ResourceBudget, rows: usize) -> Result<()> {
        self.row_leases.push_back(budget.try_lease(
            plenora_database_core::resource::ResourceKind::Rows,
            rows as u64,
        )?);
        Ok(())
    }
    pub fn commit_rows(&mut self, rows: u64) -> Result<()> {
        self.active_rows.take().ok_or_else(invalid)?.commit(rows)
    }
    pub fn freeze_batch(&mut self) {
        self.frozen.push_back(std::mem::take(&mut self.rows));
    }
    pub const fn replay(&mut self) {
        self.replay = true;
    }
    pub fn begin(&mut self, batch: &RecordBatch) -> Result<()> {
        if self.replay {
            self.active_rows = Some(self.row_leases.pop_front().ok_or_else(invalid)?);
            self.rows = self.frozen.pop_front().ok_or_else(invalid)?;
            if self.rows.len() != batch.num_rows() {
                return Err(invalid());
            }
            return Ok(());
        }
        let received = self
            .received
            .checked_add(batch.num_rows() as u64)
            .ok_or_else(limit)?;
        if self.serial && received > SERIAL_ROW_LIMIT {
            return Err(limit());
        }
        // Arrow, valori bind, clone di trasporto e vettori: riserva prima di allocare.
        let bytes = (batch.get_array_memory_size() as u64)
            .checked_mul(4)
            .and_then(|n| {
                n.checked_add(
                    (batch.num_rows() as u64)
                        .checked_mul(batch.num_columns() as u64)?
                        .checked_mul(256)?,
                )
            })
            .ok_or_else(limit)?;
        let sequence = self.sequence.checked_add(1).ok_or_else(limit)?;
        self.context
            .begin_source_batch(sequence, batch.num_rows(), self.rules, bytes)?;
        self.rows = (0..batch.num_rows()).map(|_| None).collect();
        self.sequence = sequence;
        self.received = received;
        Ok(())
    }
    fn prepare_row(
        &mut self,
        plan: &MysqlWritePlan,
        batch: &RecordBatch,
        row: usize,
        keys_only: bool,
    ) -> Result<()> {
        if row >= self.rows.len() {
            return Err(invalid());
        }
        if self.rows[row].is_none() {
            let Params::Positional(values) = plan.bind_chunk(batch, row, 1)? else {
                return Err(invalid());
            };
            let mut bound = BoundRow {
                values,
                tokens: Vec::new(),
                evaluated: false,
            };
            for (field, key, codec) in &self.codecs {
                if *key {
                    let (tokens, value) = codec.evaluate(
                        &mut self.context,
                        batch.column(*field).as_ref(),
                        row,
                        true,
                    )?;
                    bound.values[*field] = value;
                    bound.tokens.extend(tokens);
                }
            }
            self.rows[row] = Some(bound);
        }
        let bound = self.rows[row].as_mut().ok_or_else(invalid)?;
        if !keys_only && !bound.evaluated {
            for (field, key, codec) in &self.codecs {
                if !*key {
                    let (tokens, value) = codec.evaluate(
                        &mut self.context,
                        batch.column(*field).as_ref(),
                        row,
                        false,
                    )?;
                    bound.values[*field] = value;
                    bound.tokens.extend(tokens);
                }
            }
            bound.evaluated = true;
        }
        Ok(())
    }
    pub fn bind_keys(
        &mut self,
        plan: &MysqlWritePlan,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<Params> {
        self.prepare_row(plan, batch, row, true)?;
        let bound = self.rows[row].as_ref().ok_or_else(invalid)?;
        Ok(Params::Positional(
            self.keys.iter().map(|i| bound.values[*i].clone()).collect(),
        ))
    }
    /// La cache e l'unica origine dei parametri dopo l'autorizzazione.
    pub fn bind(
        &mut self,
        plan: &MysqlWritePlan,
        batch: &RecordBatch,
        start: usize,
        count: usize,
    ) -> Result<Params> {
        let end = start
            .checked_add(count)
            .filter(|n| *n <= self.rows.len())
            .ok_or_else(invalid)?;
        let mut parameters = Vec::new();
        for row in start..end {
            self.prepare_row(plan, batch, row, false)?;
            parameters.extend(self.rows[row].as_ref().ok_or_else(invalid)?.values.clone());
        }
        Ok(Params::Positional(parameters))
    }
    pub fn sent(&mut self, start: usize, count: usize) -> Result<()> {
        for row in &self.rows[start..start + count] {
            for token in &row.as_ref().ok_or_else(invalid)?.tokens {
                self.context.ledger.sent_token(*token)?;
            }
        }
        Ok(())
    }
    pub fn applied(&mut self, start: usize, count: usize, assigned: bool) -> Result<()> {
        for row in &self.rows[start..start + count] {
            for token in &row.as_ref().ok_or_else(invalid)?.tokens {
                if assigned {
                    self.context.ledger.applied_token(*token)?;
                } else {
                    self.context.ledger.discard_token(*token)?;
                }
            }
        }
        Ok(())
    }
    pub fn finish(
        &mut self,
        result: Result<plenora_database_core::outcome::WriteOutcome>,
    ) -> Result<plenora_database_core::outcome::WriteOutcome> {
        let report = self.context.finalize(&result)?;
        match result {
            Ok(mut outcome) => {
                outcome.schema_version = 3;
                outcome.loss_report = Some(report.into());
                Ok(outcome)
            }
            Err(mut error) => {
                error.loss_report = Some(Box::new(report.into()));
                Err(error)
            }
        }
    }
}
fn invalid() -> DatabaseError {
    crate::write::mapping_error("stato di attribuzione MySQL incoerente")
}
fn limit() -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::ResourceLimit,
        ErrorPhase::Write,
        Some(ProviderKind::Mysql),
        "budget del percorso permissivo MySQL esaurito",
    )
}

#[cfg(test)]
#[path = "loss_execution_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "loss_fault_tests.rs"]
pub mod faults;
