//! Conversioni pure preparatorie: nessun contatore, SQL o consenso locale.
//! La qualifica live resta separata; questo modulo non apre `validate_operation`.

use crate::write::{bind_value, mapping_error};
use crate::MysqlColumnKind;
use mysql_async::Value;
use plenora_database_core::arrow::array::{Array, Float64Array, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::DataType;
use plenora_database_core::execution_loss_context::ExecutionLossContext;
use plenora_database_core::loss_ledger::{Classification, LossToken};
use plenora_database_core::loss_registry::{
    classify_rewrite, resolve_rewrite, RuleInstance, RuleParameters,
};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::store_rewrite::{StoreRewrite, TextLengthUnit};
use plenora_database_core::Result;

/// Codec di colonna: le due regole double(255,30) sono inseparabili.
/// Una sola uscita impedisce che una regola esatta sovrascriva una conversione.
pub struct RuleCodec {
    pub(crate) instances: Vec<RuleInstance>,
    rules: Vec<RewriteCodec>,
}

impl RuleCodec {
    pub(crate) fn new(
        provider: ProviderKind,
        field: u32,
        arrow: DataType,
        native: &str,
        rewrite: StoreRewrite,
    ) -> Result<Self> {
        let first = RewriteCodec::new(provider, field, arrow, native, rewrite)?;
        let rules = if native == "double(255,30)" {
            [
                StoreRewrite::NegativeZero,
                StoreRewrite::Underflow { min_abs: 1e-10 },
            ]
            .into_iter()
            .map(|r| RewriteCodec::new(provider, field, first.arrow.clone(), native, r))
            .collect::<Result<Vec<_>>>()?
        } else {
            vec![first]
        };
        Ok(Self {
            instances: rules.iter().map(|r| r.instance.clone()).collect(),
            rules,
        })
    }

    /// Il catalogo ammesso deve conservare tutte le rewrite, parametri compresi.
    pub(crate) fn matches_rewrites(&self, rewrites: &[StoreRewrite]) -> bool {
        rewrites.len() == self.rules.len()
            && self
                .rules
                .iter()
                .all(|rule| rewrites.iter().filter(|r| **r == rule.rewrite).count() == 1)
    }

    /// Riservare token e buffer con `begin_source_batch`; conservare il risultato
    /// per stage/target/retry. Le guardie hard precedono ogni osservazione.
    pub(crate) fn evaluate(
        &self,
        context: &mut ExecutionLossContext,
        array: &dyn Array,
        row: usize,
        key: bool,
    ) -> Result<(Vec<LossToken>, Value)> {
        let candidates = self
            .rules
            .iter()
            .map(|r| r.candidate(array, row))
            .collect::<Result<Vec<_>>>()?;
        if let Some(index) = candidates
            .iter()
            .position(|(c, _)| *c == Classification::HardReject)
        {
            context.evaluate(
                row as u64,
                &self.rules[index].instance,
                key,
                || Ok((Classification::HardReject, ())),
                Ok,
            )?;
            return Err(mapping_error("rifiuto indipendente non applicato"));
        }
        let mut tokens = Vec::with_capacity(self.rules.len());
        let mut output = Value::NULL;
        for (index, (rule, (classification, candidate))) in
            self.rules.iter().zip(candidates).enumerate()
        {
            let (token, value) = context.evaluate(
                row as u64,
                &rule.instance,
                key,
                || Ok((classification, candidate)),
                Ok,
            )?;
            if index == 0 || matches!(classification, Classification::WouldChange(_)) {
                output = value;
            }
            tokens.push(token);
        }
        Ok((tokens, output))
    }
}

/// Una rewrite congelata dal planner dopo A; non sostituisce A o i gate di colonna.
struct RewriteCodec {
    pub(crate) instance: RuleInstance,
    native: String,
    rewrite: StoreRewrite,
    arrow: DataType,
}

impl RewriteCodec {
    pub(crate) fn new(
        provider: ProviderKind,
        field_id: u32,
        arrow: DataType,
        native: &str,
        rewrite: StoreRewrite,
    ) -> Result<Self> {
        if provider != ProviderKind::Mysql {
            return Err(mapping_error(
                "codec permissivo non disponibile per il provider",
            ));
        }
        let rule_id = resolve_rewrite(provider, &arrow, native, rewrite)?;
        Ok(Self {
            instance: RuleInstance {
                subject: None,
                field_id,
                rule_id,
                parameters: RuleParameters {
                    precision: match rewrite {
                        StoreRewrite::FractionalSeconds { digits } => Some(u32::from(digits)),
                        _ => None,
                    },
                    ..RuleParameters::default()
                },
                source_type: Some(arrow.to_string()),
                target_type: Some(native.to_owned()),
            },
            native: native.to_owned(),
            rewrite,
            arrow,
        })
    }

    fn candidate(&self, array: &dyn Array, row: usize) -> Result<(Classification, Value)> {
        if array.data_type() != &self.arrow || row >= array.len() {
            return Err(mapping_error("array incoerente con il codec congelato"));
        }
        let classification = classify_rewrite(
            ProviderKind::Mysql,
            &self.native,
            self.rewrite,
            array,
            row,
            TextLengthUnit::Characters,
        )?;
        if classification == Classification::HardReject || array.is_null(row) {
            return Ok((classification, Value::NULL));
        }
        let value = match self.rewrite {
            StoreRewrite::FractionalSeconds { digits } => {
                let micros = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| mapping_error("array temporale incoerente"))?
                    .value(row);
                let Some(rounded) =
                    temporal_candidate(micros, digits, self.native.starts_with("timestamp"))
                else {
                    return Ok((Classification::HardReject, Value::NULL));
                };
                bind_value(
                    &TimestampMicrosecondArray::from(vec![rounded]),
                    0,
                    &MysqlColumnKind::Timestamp,
                )?
            }
            StoreRewrite::NegativeZero | StoreRewrite::Underflow { .. } => {
                let value = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| mapping_error("array numerico incoerente"))?
                    .value(row);
                // 10^225 come f64 e inferiore al limite decimale M-D; il
                // successivo f64 lo supera. Nessun massimo del fixture riaperto.
                if value.abs() > 1e225 {
                    return Ok((Classification::HardReject, Value::NULL));
                }
                Value::Double(
                    if matches!(self.rewrite, StoreRewrite::NegativeZero) && value == 0.0 {
                        0.0
                    } else {
                        value
                    },
                )
            }
            // Every non nullo e gia HardReject nel resolver comune.
            _ => return Err(mapping_error("trasformazione non qualificabile")),
        };
        Ok((classification, value))
    }
}

/// Arrotondamento della frazione civile non negativa, pareggio verso il secondo
/// successivo come il bootstrap senza `TIME_TRUNCATE_FRACTIONAL`. Nessun clamp.
fn temporal_candidate(micros: i64, digits: u8, timestamp: bool) -> Option<i64> {
    if digits > 6 || !in_range(micros, timestamp) {
        return None;
    }
    let quantum = 10_i64.pow(u32::from(6 - digits));
    let fraction = micros.rem_euclid(1_000_000);
    let rounded_fraction = fraction.checked_add(quantum / 2)? / quantum * quantum;
    let rounded = micros
        .div_euclid(1_000_000)
        .checked_mul(1_000_000)?
        .checked_add(rounded_fraction)?;
    in_range(rounded, timestamp).then_some(rounded)
}

fn in_range(micros: i64, timestamp: bool) -> bool {
    // Intervalli MySQL del codec: DATETIME 0..9999 (fixture A); TIMESTAMP UTC 1970..2038.
    let (min, max) = if timestamp {
        (1_000_000, 2_147_483_647_999_999)
    } else {
        (-62_167_219_200_000_000, 253_402_300_799_999_999)
    };
    (min..=max).contains(&micros)
}

#[cfg(test)]
#[path = "loss_codec_tests.rs"]
mod tests;
