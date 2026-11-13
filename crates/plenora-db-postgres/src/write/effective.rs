//! Classificazione pura e contabilità della scrittura PostgreSQL.
use super::canonical::{self, LeafClass, SessionLeaf, SessionShape};
use super::plan::WriteColumnPlan;
use arrow_array::{Array, ListArray, RecordBatch, StringArray};
use plenora_database_core::execution_loss::Basis;
use plenora_database_core::execution_loss_context::ExecutionLossContext;
use plenora_database_core::loss::{LossConsent, MappingPolicy};
use plenora_database_core::loss_ledger::Classification;
use plenora_database_core::loss_registry::{RuleId, RuleInstance};
use plenora_database_core::plan::{ProviderKind, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::{DatabaseError, ErrorPhase, Result};

pub struct Evaluation {
    context: ExecutionLossContext,
    rules: Vec<RuleInstance>,

    changes: bool,
}

impl std::ops::Deref for Evaluation {
    type Target = ExecutionLossContext;
    fn deref(&self) -> &Self::Target {
        &self.context
    }
}
impl std::ops::DerefMut for Evaluation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.context
    }
}
impl Evaluation {
    pub fn into_context(self) -> ExecutionLossContext {
        self.context
    }

    pub fn new(
        rules: Vec<RuleInstance>,
        operation: &WriteOperation,
        consent: LossConsent,
        budget: &plenora_database_core::resource::ResourceBudget,
    ) -> Result<Self> {
        let (context, rules) = ExecutionLossContext::new(
            ProviderKind::Postgres,
            operation.mapping_policy,
            consent,
            rules,
            budget,
        )?;
        Ok(Self {
            context,
            rules,
            changes: false,
        })
    }

    pub fn metadata(
        &mut self,
        plan: &crate::geometry_properties::SchemaPlan,
        schema: &arrow_schema::Schema,
        operation: &WriteOperation,
    ) -> Result<()> {
        if operation.mode == WriteMode::DeleteByKeys {
            return Ok(());
        }
        self.context.begin_metadata_batch(plan.columns.len())?;
        for rule in self.rules.iter().filter(|r| r.subject.is_some()) {
            let subject = rule.subject.as_ref().ok_or_else(invalid)?;
            let position = rule.field_id as usize;
            let column = plan.columns.get(position).ok_or_else(invalid)?;
            let (original, retained) = match subject.scope {
                plenora_database_core::metadata_subject::MetadataScope::Schema => {
                    (schema.metadata(), &column.schema)
                }
                plenora_database_core::metadata_subject::MetadataScope::Field => {
                    (schema.field(position).metadata(), &column.field)
                }
            };
            let key = original
                .keys()
                .find(|key| crate::column_metadata::digest(key.as_bytes()) == subject.key_sha256)
                .ok_or_else(invalid)?;
            let classification = plenora_database_core::loss_registry::classify_metadata(
                rule.rule_id,
                original.get(key).map(String::as_str),
                retained.get(key).map(String::as_str),
            )?;
            self.context.track(position, position as u64, position)?;
            self.context.observe_metadata(
                position as u64,
                rule.field_id,
                subject.index,
                classification,
            )?;
        }
        Ok(())
    }

    // Le istanze usano i typmod del target fissato dal lock, come i codec.
    pub(super) fn bind(&mut self, plans: &[WriteColumnPlan]) -> Result<()> {
        use plenora_database_core::loss_registry::ValueShape;
        for rule in &mut self.rules {
            let plan = plans.get(rule.field_id as usize).ok_or_else(invalid)?;
            let Some(session) = &plan.session_rule else {
                continue;
            };
            if let [leaf] = session.leaves.as_slice() {
                rule.parameters.shape = match leaf.shape {
                    SessionShape::Scalar => ValueShape::Scalar,
                    SessionShape::Array => ValueShape::Nested,
                    SessionShape::Range | SessionShape::Nested => ValueShape::Opaque,
                };
                if let LeafClass::Padded { chars } = leaf.class {
                    for rewrite in &plan.store.rewrites {
                        if let plenora_database_core::store_rewrite::StoreRewrite::PadText {
                            chars: measured,
                        } = rewrite
                        {
                            if *measured != chars as u64 {
                                return Err(invalid());
                            }
                        }
                    }
                }
                match leaf.class {
                    LeafClass::Text {
                        max_chars: Some(length),
                    }
                    | LeafClass::Padded { chars: length }
                        if matches!(rule.rule_id, RuleId::PgTextLimit | RuleId::PgPadText) =>
                    {
                        rule.parameters.length = Some(length as u64);
                        rule.parameters.text_unit =
                            Some(plenora_database_core::store_rewrite::TextLengthUnit::Characters);
                    }
                    LeafClass::Numeric { precision, scale }
                        if rule.rule_id == RuleId::PgNumeric =>
                    {
                        rule.parameters.precision = Some(precision);
                        rule.parameters.scale = Some(scale);
                    }
                    LeafClass::Session { fraction, .. } if rule.rule_id == RuleId::PgTemporal => {
                        rule.parameters.precision = Some(fraction);
                    }
                    _ => {}
                }
            } else {
                rule.parameters.shape = ValueShape::Opaque;
            }
        }
        Ok(())
    }

    pub(super) fn batch(
        &mut self,
        batch: &RecordBatch,
        plans: &[WriteColumnPlan],
        operation: &WriteOperation,
    ) -> Result<bool> {
        self.context
            .begin_batch(batch.num_rows(), self.rules.len())?;
        let mut changes = false;
        for row in 0..batch.num_rows() {
            for (index, plan) in plans.iter().enumerate() {
                let name = batch.schema_ref().field(index).name();
                let key = operation.keys.contains(name);
                let used = match operation.mode {
                    WriteMode::DeleteByKeys => key,
                    WriteMode::Update => key || operation.update_columns.contains(name),
                    _ => true,
                };
                if !used {
                    continue;
                }
                let array = batch.column(index).as_ref();
                canonical::check(plan, array, row)?;
                drop(super::prepared_codec::arrow_value(array, plan, row)?);
                for rule in self
                    .rules
                    .iter()
                    .filter(|r| r.field_id as usize == index && r.subject.is_none())
                {
                    let classification = classify(rule.rule_id, plan, array, row)?;
                    if classification == Classification::HardReject {
                        return Err(invalid());
                    }
                    self.context.track(row, row as u64, index)?;
                    self.context.observe(
                        row as u64,
                        rule.field_id,
                        rule.rule_id,
                        classification,
                        key,
                    )?;
                    changes |= matches!(classification, Classification::WouldChange(_));
                }
            }
        }
        self.changes = changes;
        Ok(changes)
    }

    pub fn prepare_commit(&mut self) -> Result<()> {
        self.context.prepare_commit(false)
    }

    pub fn profile(&self, operation: &WriteOperation) -> Result<()> {
        profile(operation, self.consent)
    }

    pub const fn has_changes(&self) -> bool {
        self.changes
    }

    pub fn sent(&mut self, row: usize) -> Result<()> {
        self.context.sent(row)
    }

    pub fn applied(&mut self, row: usize, count: u64) -> Result<()> {
        if count > 1 {
            return Err(invalid());
        }
        self.context.attributed(row, |_| count == 1)
    }
}

pub fn profile(operation: &WriteOperation, consent: LossConsent) -> Result<()> {
    if operation.mapping_policy != MappingPolicy::Strict
        && !consent.accepted()
        && matches!(
            operation.transaction_profile,
            TransactionProfile::ChunkCommitted | TransactionProfile::BestEffortDdl
        )
    {
        return Err(DatabaseError::unsupported(
            ProviderKind::Postgres,
            ErrorPhase::Prepare,
            "profilo PostgreSQL non atomico senza consenso alle perdite",
        ));
    }
    Ok(())
}

fn invalid() -> DatabaseError {
    DatabaseError::new(
        plenora_database_core::ErrorCategory::DataMapping,
        ErrorPhase::Write,
        Some(ProviderKind::Postgres),
        "valore PostgreSQL non classificabile o fuori dai limiti del target",
    )
}
const fn verdict(changes: bool, basis: Basis) -> Classification {
    if changes {
        Classification::WouldChange(basis)
    } else {
        Classification::Exact(basis)
    }
}

#[allow(clippy::too_many_lines)]
pub(super) fn classify(
    rule: RuleId,
    plan: &WriteColumnPlan,
    array: &dyn Array,
    row: usize,
) -> Result<Classification> {
    if row >= array.len() {
        return Err(invalid());
    }
    if array.is_null(row) {
        return Ok(Classification::Exact(rule.bases()[0]));
    }
    for rewrite in &plan.store.rewrites {
        if rewrite.rule_id().canonical(ProviderKind::Postgres) == rule
            && !(rule == RuleId::PgPadText
                && plan.session_rule.as_ref().is_some_and(|session| {
                    session
                        .leaves
                        .iter()
                        .any(|leaf| matches!(leaf.class, LeafClass::Padded { .. }))
                }))
        {
            if *rewrite == plenora_database_core::store_rewrite::StoreRewrite::JsonNumbers {
                let values = texts(array, row, SessionShape::Scalar)?;
                let mut changed = false;
                for text in values.iter().flatten() {
                    changed |= json_number_changes(text)?;
                }
                return Ok(verdict(changed, Basis::Exact));
            }
            if let plenora_database_core::store_rewrite::StoreRewrite::ExcessSpaces { bytes } =
                rewrite
            {
                let values = texts(array, row, SessionShape::Scalar)?;
                if values
                    .iter()
                    .flatten()
                    .any(|value| value.len() as u64 > *bytes)
                {
                    return Err(invalid());
                }
            }
            return rewrite.classify(
                array,
                row,
                plenora_database_core::store_rewrite::TextLengthUnit::Characters,
            );
        }
    }
    match rule {
        RuleId::PgGeometryRepresentation => {
            let array = array
                .as_any()
                .downcast_ref::<arrow_array::BinaryArray>()
                .ok_or_else(invalid)?;
            plan.classify_representation(array.value(row))
        }
        RuleId::PgNullability | RuleId::PgAddNullable => Ok(verdict(false, Basis::Exact)),
        RuleId::PgArrowMismatch | RuleId::PgMissingColumn | RuleId::PgSrid | RuleId::PgCrs => {
            Ok(verdict(true, Basis::Conservative))
        }
        RuleId::PgTextLimit
        | RuleId::PgPadText
        | RuleId::PgJsonb
        | RuleId::PgNumeric
        | RuleId::PgTemporal => {
            let session = plan.session_rule.as_ref().ok_or_else(invalid)?;
            let mut change = false;
            let mut basis = Basis::Exact;
            for leaf in &session.leaves {
                let relevant = matches!(
                    (rule, leaf.class),
                    (
                        RuleId::PgTextLimit,
                        LeafClass::Text { max_chars: Some(_) } | LeafClass::Padded { .. }
                    ) | (RuleId::PgPadText, LeafClass::Padded { .. })
                        | (RuleId::PgJsonb, LeafClass::Jsonb)
                        | (
                            RuleId::PgNumeric,
                            LeafClass::Rounding | LeafClass::Numeric { .. }
                        )
                        | (RuleId::PgTemporal, LeafClass::Session { reduced: true, .. })
                );
                if !relevant {
                    continue;
                }
                if matches!(leaf.shape, SessionShape::Nested | SessionShape::Range) {
                    if matches!(rule, RuleId::PgTextLimit | RuleId::PgPadText) {
                        return Err(invalid());
                    }
                    change = true;
                    basis = Basis::Conservative;
                    continue;
                }
                if rule == RuleId::PgTemporal && leaf.shape == SessionShape::Scalar {
                    if let LeafClass::Session {
                        leaf: kind,
                        fraction,
                        ..
                    } = leaf.class
                    {
                        if kind == SessionLeaf::Interval {
                            change = true;
                            basis = Basis::Conservative;
                            continue;
                        }
                        if let Some(value) = temporal(array, row, fraction)? {
                            change |= value;
                            continue;
                        }
                    }
                }
                let values = texts(array, row, leaf.shape)?;
                for text in values.iter().flatten() {
                    match (rule, leaf.class) {
                        (
                            RuleId::PgTextLimit,
                            LeafClass::Text { max_chars: Some(n) } | LeafClass::Padded { chars: n },
                        ) => {
                            if text.chars().count() > n {
                                return Err(invalid());
                            }
                        }
                        (RuleId::PgPadText, LeafClass::Padded { chars }) => {
                            change |= text.chars().count() < chars;
                        }
                        (RuleId::PgJsonb, _) => {
                            use serde::Deserialize;
                            let mut parser = serde_json::Deserializer::from_str(text);
                            serde::de::IgnoredAny::deserialize(&mut parser)
                                .map_err(|_| invalid())?;
                            parser.end().map_err(|_| invalid())?;
                            change |= canonical::json_duplicate_keys(text).ok_or_else(invalid)?;
                        }
                        (RuleId::PgNumeric, LeafClass::Numeric { precision, scale }) => {
                            let c = numeric(text, precision, scale)?;
                            change |= matches!(c, Classification::WouldChange(_));
                            if matches!(c, Classification::WouldChange(Basis::Conservative)) {
                                basis = Basis::Conservative;
                            }
                        }
                        (RuleId::PgNumeric, _)
                        | (
                            RuleId::PgTemporal,
                            LeafClass::Session {
                                leaf: SessionLeaf::Interval,
                                ..
                            },
                        ) => {
                            change = true;
                            basis = Basis::Conservative;
                        }
                        (RuleId::PgTemporal, LeafClass::Session { fraction, .. }) => {
                            change |= fractional_text(text, fraction);
                        }
                        _ => return Err(invalid()),
                    }
                }
            }
            Ok(verdict(change, basis))
        }
        _ => Err(invalid()),
    }
}

fn texts(array: &dyn Array, row: usize, shape: SessionShape) -> Result<Vec<Option<String>>> {
    if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let items = list.value(row);
        let items = items
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(invalid)?;
        return Ok(items.iter().map(|v| v.map(str::to_owned)).collect());
    }
    let text = array
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(invalid)?
        .value(row);
    if shape == SessionShape::Array {
        canonical::array_elements(text).ok_or_else(invalid)
    } else {
        Ok(vec![Some(text.to_owned())])
    }
}

// jsonb usa numeric: conserva tutte le cifre e la scala dei decimali,
// anche oltre f64. Riscrive gli esponenti e il segno dello zero. Non usare
// serde_json::Value, che passerebbe da f64 perdendo la prova di uguaglianza.
fn json_number_changes(text: &str) -> Result<bool> {
    serde_json::from_str::<serde::de::IgnoredAny>(text).map_err(|_| invalid())?;
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut changed = false;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                index += 1;
                while index < bytes.len() && bytes[index] != b'"' {
                    index += if bytes[index] == b'\\' { 2 } else { 1 };
                }
                index += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = index;
                while index < bytes.len()
                    && matches!(bytes[index], b'-' | b'+' | b'.' | b'e' | b'E' | b'0'..=b'9')
                {
                    index += 1;
                }
                let number = &bytes[start..index];
                changed |= number.contains(&b'e')
                    || number.contains(&b'E')
                    || (number[0] == b'-' && number[1..].iter().all(|b| matches!(b, b'0' | b'.')));
            }
            _ => index += 1,
        }
    }
    Ok(changed)
}

fn fractional_text(text: &str, fraction: u32) -> bool {
    text.split_once('.').is_some_and(|(_, tail)| {
        tail.bytes()
            .take_while(u8::is_ascii_digit)
            .skip(fraction as usize)
            .any(|b| b != b'0')
    })
}

fn temporal(array: &dyn Array, row: usize, precision: u32) -> Result<Option<bool>> {
    use arrow_array::{
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    if precision > 9 {
        return Err(invalid());
    }
    macro_rules! value {
        ($ty:ty, $scale:expr) => {
            if let Some(a) = array.as_any().downcast_ref::<$ty>() {
                let nanos = i128::from(a.value(row)) * $scale;
                return Ok(Some(nanos.rem_euclid(10_i128.pow(9 - precision)) != 0));
            }
        };
    }
    value!(TimestampSecondArray, 1_000_000_000);
    value!(TimestampMillisecondArray, 1_000_000);
    value!(TimestampMicrosecondArray, 1_000);
    value!(TimestampNanosecondArray, 1);
    value!(Time64MicrosecondArray, 1_000);
    value!(Time64NanosecondArray, 1);
    value!(Time32SecondArray, 1_000_000_000);
    value!(Time32MillisecondArray, 1_000_000);
    Ok(None)
}

/// Decimale esatto senza float né allocazioni proporzionali all'esponente.
fn numeric(text: &str, precision: u32, scale: i32) -> Result<Classification> {
    if precision == 0 || precision > 1000 || text.len() > 1_000_000 {
        return Err(invalid());
    }
    let trimmed = text.trim();
    let unsigned = trimmed.strip_prefix(['+', '-']).unwrap_or(trimmed);
    if matches!(
        unsigned.to_ascii_lowercase().as_str(),
        "nan" | "infinity" | "inf"
    ) {
        return Ok(verdict(true, Basis::Conservative));
    }
    let mut parts = unsigned.split(['e', 'E']);
    let mantissa = parts.next().ok_or_else(invalid)?;
    let exponent = parts
        .next()
        .map(str::parse::<i64>)
        .transpose()
        .map_err(|_| invalid())?
        .unwrap_or(0);
    if parts.next().is_some() {
        return Err(invalid());
    }
    let mut digits = Vec::with_capacity(mantissa.len());
    let mut dot = false;
    let mut fractional = 0_i64;
    for b in mantissa.bytes() {
        if b == b'.' && !dot {
            dot = true;
        } else if b.is_ascii_digit() {
            digits.push(b);
            fractional += i64::from(dot);
        } else {
            return Err(invalid());
        }
    }
    if digits.is_empty() {
        return Err(invalid());
    }
    let start = digits
        .iter()
        .position(|b| *b != b'0')
        .unwrap_or(digits.len());
    let digits = &digits[start..];
    if digits.is_empty() {
        return Ok(verdict(false, Basis::Exact));
    }
    let shift = exponent
        .checked_sub(fractional)
        .and_then(|e| e.checked_add(i64::from(scale)))
        .ok_or_else(invalid)?;
    let length = i64::try_from(digits.len()).map_err(|_| invalid())?;
    let kept = length.checked_add(shift).ok_or_else(invalid)?;
    if kept > i64::from(precision) {
        return Err(invalid());
    }
    if shift >= 0 {
        return Ok(verdict(false, Basis::Exact));
    }
    let cut = usize::try_from(kept.max(0)).map_err(|_| invalid())?;
    let changes = digits[cut..].iter().any(|b| *b != b'0');
    let round_up = kept >= 0 && digits.get(cut).is_some_and(|b| *b >= b'5');
    if round_up && kept == i64::from(precision) && digits[..cut].iter().all(|b| *b == b'9') {
        return Err(invalid());
    }
    Ok(verdict(changes, Basis::Exact))
}

#[cfg(test)]
#[path = "effective_tests.rs"]
mod tests;
