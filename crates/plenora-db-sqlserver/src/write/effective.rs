//! Classificazione pura e ledger SQL Server; il codec non incrementa contatori.
use super::{
    codec,
    plan::{self, WriteColumnPlan, WritePlan},
    MutationCounts,
};
use chrono::Timelike;
use plenora_database_core::arrow::{
    array::{Array, BinaryArray, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray},
    RecordBatch,
};
use plenora_database_core::execution_loss::{Basis, ExecutionLossReport};
use plenora_database_core::loss::LossConsent;
use plenora_database_core::loss_ledger::{Classification, LossLedger};
use plenora_database_core::loss_registry::{compile_instances, RuleId, RuleInstance};
use plenora_database_core::plan::{ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::store_rewrite::{StoreRewrite, TextLengthUnit};
use plenora_database_core::{DatabaseError, Result};

pub(super) struct Evaluation {
    pub ledger: LossLedger,
    pub confirmed: ExecutionLossReport,
    rules: Vec<RuleInstance>,
    pub consent: LossConsent,
    operation: WriteOperation,
    sequence: u64,
    tracked: Vec<Vec<(u64, usize)>>,
    budget: ResourceBudget,
    lease: Option<ResourceLease>,
}
impl Evaluation {
    pub fn new(
        plan: &WritePlan,
        operation: &WriteOperation,
        consent: LossConsent,
        budget: &ResourceBudget,
    ) -> Result<Self> {
        let (rules, report) = compile_instances(
            ProviderKind::Sqlserver,
            operation.mapping_policy,
            plan.loss_instances(operation)?,
        )?;
        Ok(Self {
            ledger: LossLedger::new(
                report.clone(),
                usize::try_from(budget.limits().output_bytes).unwrap_or(usize::MAX),
                usize::try_from(budget.limits().memory_bytes / 192).unwrap_or(usize::MAX),
            )?,
            confirmed: report,
            rules,
            consent,
            operation: operation.clone(),
            sequence: 0,
            tracked: Vec::new(),
            budget: budget.clone(),
            lease: None,
        })
    }
    pub fn batch(&mut self, batch: &RecordBatch, plan: &WritePlan) -> Result<()> {
        self.lease = None;
        let bytes = (batch.num_rows() as u64)
            .checked_mul(self.rules.len() as u64)
            .and_then(|n| n.checked_mul(192))
            .ok_or_else(invalid)?;
        self.lease = (bytes > 0)
            .then(|| self.budget.try_lease(ResourceKind::MemoryBytes, bytes))
            .transpose()?;
        self.sequence = self.sequence.checked_add(1).ok_or_else(invalid)?;
        self.ledger.begin_batch(self.sequence)?;
        self.tracked = vec![Vec::new(); batch.num_rows()];
        for row in 0..batch.num_rows() {
            // Codifica a secco: range, formato, chiavi e guardie indipendenti prima dell'invio.
            codec::bind_row(plan, batch, row)?;
            for rule in &self.rules {
                let index = rule.field_id as usize;
                let column = plan.columns.get(index).ok_or_else(invalid)?;
                let key = self.operation.keys.contains(&column.name);
                let used = match self.operation.mode {
                    WriteMode::DeleteByKeys => key,
                    WriteMode::Update => {
                        key || self.operation.update_columns.is_empty()
                            || self.operation.update_columns.contains(&column.name)
                    }
                    _ => true,
                };
                if !used {
                    continue;
                }
                let classification =
                    classify(rule.rule_id, column, batch.column(index).as_ref(), row)?;
                let id = (row as u64)
                    .checked_mul(plan.columns.len() as u64)
                    .and_then(|n| n.checked_add(index as u64))
                    .ok_or_else(invalid)?;
                if classification != Classification::HardReject {
                    self.ledger
                        .observe(id, rule.field_id, rule.rule_id, classification)?;
                    if !self.tracked[row].iter().any(|(old, _)| *old == id) {
                        self.tracked[row].push((id, index));
                    }
                }
                classification.authorize(self.operation.mapping_policy, self.consent, key)?;
            }
        }
        Ok(())
    }
    pub fn sent(&mut self, row: usize) -> Result<()> {
        for &(id, _) in &self.tracked[row] {
            self.ledger.sent(id)?;
        }
        Ok(())
    }
    pub fn applied(&mut self, row: usize, counts: MutationCounts, plan: &WritePlan) -> Result<()> {
        if counts.confirmed > 1 {
            return Err(invalid());
        }
        for &(id, index) in &self.tracked[row] {
            let column = &plan.columns[index];
            let assigned = counts.updated == 0
                || self.operation.update_columns.is_empty()
                || self.operation.update_columns.contains(&column.name);
            if counts.confirmed == 1 && counts.deleted == 0 && assigned {
                self.ledger.applied(id)?;
            } else {
                self.ledger.discard(id)?;
            }
        }
        Ok(())
    }
    pub fn discard(&mut self, row: usize) -> Result<()> {
        for &(id, _) in &self.tracked[row] {
            self.ledger.discard(id)?;
        }
        Ok(())
    }
    pub fn prepare_commit(&mut self) -> Result<()> {
        self.ledger.complete();
        let mut preview = self.ledger.clone();
        preview.commit()?;
        self.confirmed = preview.report();
        self.confirmed.validate()
    }
}
fn invalid() -> DatabaseError {
    DatabaseError::new(
        plenora_database_core::ErrorCategory::DataMapping,
        plenora_database_core::ErrorPhase::Write,
        Some(ProviderKind::Sqlserver),
        "valore SQL Server non classificabile",
    )
}
fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref::<T>().ok_or_else(invalid)
}
const fn verdict(changes: bool, basis: Basis) -> Classification {
    if changes {
        Classification::WouldChange(basis)
    } else {
        Classification::Exact(basis)
    }
}
pub(super) fn text(
    column: &WriteColumnPlan,
    value: &str,
    padding: Option<u64>,
) -> Result<Classification> {
    let limit = plan::text_limit(
        &column.native_type,
        &column.native_declaration,
        column.collation.as_deref(),
    )
    .ok_or_else(invalid)?;
    if !limit.code_page.represents(value) {
        return Ok(verdict(true, Basis::Conservative));
    }
    let length = limit.length_unit().count(value) as u64;
    Ok(verdict(
        padding.map_or_else(
            || limit.max.is_some_and(|max| length > max as u64),
            |n| length < n,
        ),
        Basis::Exact,
    ))
}
pub(super) fn binary(column: &WriteColumnPlan, length: usize) -> Classification {
    verdict(
        plan::binary_limit(&column.native_type, &column.native_declaration)
            .is_some_and(|n| length > n),
        Basis::Exact,
    )
}
pub(super) fn temporal(column: &WriteColumnPlan, value: i64) -> Classification {
    verdict(
        plan::temporal_unit(&column.native_type, &column.native_declaration)
            .is_some_and(|unit| value.rem_euclid(unit) != 0),
        Basis::Exact,
    )
}
pub(super) fn offset(column: &WriteColumnPlan, nanos: u32) -> Classification {
    verdict(
        plan::offset_scale(&column.native_type, &column.native_declaration)
            .is_some_and(|scale| scale < 9 && !nanos.is_multiple_of(10_u32.pow(9 - scale))),
        Basis::Exact,
    )
}
pub(super) fn encoding(column: &WriteColumnPlan, value: &str) -> Result<Classification> {
    let limit = plan::text_limit(
        &column.native_type,
        &column.native_declaration,
        column.collation.as_deref(),
    )
    .ok_or_else(invalid)?;
    Ok(verdict(
        !limit.code_page.represents(value),
        if limit.code_page == plan::CodePage::Ascii && !value.is_ascii() {
            Basis::Conservative
        } else {
            Basis::Exact
        },
    ))
}
pub(super) fn classify(
    rule: RuleId,
    column: &WriteColumnPlan,
    array: &dyn Array,
    row: usize,
) -> Result<Classification> {
    if row >= array.len() {
        return Err(invalid());
    }
    if array.is_null(row) {
        return Ok(Classification::Exact(rule.bases()[0]));
    }
    for rewrite in &column.rewrites {
        if rewrite.rule_id() == rule {
            if let StoreRewrite::PadText { chars } = rewrite {
                return text(
                    column,
                    downcast::<StringArray>(array)?.value(row),
                    Some(*chars),
                );
            }
            let unit = plan::text_limit(
                &column.native_type,
                &column.native_declaration,
                column.collation.as_deref(),
            )
            .map_or(TextLengthUnit::Characters, plan::TextLimit::length_unit);
            return rewrite.classify(array, row, unit);
        }
    }
    Ok(match rule {
        RuleId::SsTextLimit => text(column, downcast::<StringArray>(array)?.value(row), None)?,
        RuleId::SsEncoding => encoding(column, downcast::<StringArray>(array)?.value(row))?,
        RuleId::SsBinaryLimit => binary(column, downcast::<BinaryArray>(array)?.value(row).len()),
        RuleId::SsTemporal => {
            let value = if column.kind == crate::SqlServerColumnKind::Time {
                downcast::<Time64MicrosecondArray>(array)?.value(row)
            } else {
                downcast::<TimestampMicrosecondArray>(array)?.value(row)
            };
            temporal(column, value)
        }
        RuleId::SsOffset => offset(
            column,
            codec::datetimeoffset(downcast::<StringArray>(array)?.value(row))?.nanosecond(),
        ),
        RuleId::SsXml => {
            downcast::<StringArray>(array)?;
            verdict(true, Basis::Conservative)
        }
        RuleId::SsAddNullable => verdict(false, Basis::Exact),
        _ => return Err(invalid()),
    })
}

#[cfg(test)]
#[path = "effective_tests.rs"]
mod tests;
