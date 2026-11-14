//! Rapporto esecutivo v3 e lettura separata del rapporto legacy non misurato.
use crate::loss::{
    LossCategory, LossReport, LossSeverity, MappingPolicy, MAX_LOSSES, MAX_REASON_CHARS,
    MAX_TYPE_CHARS,
};
use crate::loss_registry::RuleId;
use crate::{DatabaseError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Basis {
    Exact,
    Conservative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskEntry {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::metadata_subject::present"
    )]
    pub subject: Option<crate::metadata_subject::MetadataSubject>,
    pub field_id: u32,
    pub rule_id: RuleId,
    pub category: LossCategory,
    pub severity: LossSeverity,
    pub reason: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_type"
    )]
    pub source_type: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_type"
    )]
    pub target_type: Option<String>,
}
impl RiskEntry {
    #[must_use]
    pub fn new(
        field_id: u32,
        rule_id: RuleId,
        policy: MappingPolicy,
        source_type: Option<String>,
        target_type: Option<String>,
    ) -> Self {
        let rule = rule_id.definition();
        Self {
            subject: None,
            field_id,
            rule_id,
            category: rule.category,
            severity: if rule_id == RuleId::PgNullability && policy != MappingPolicy::Strict {
                LossSeverity::Information
            } else {
                rule.severity
            },
            reason: rule.reason.to_owned(),
            source_type,
            target_type,
        }
    }
    #[must_use]
    pub fn legacy(&self) -> crate::loss::MappingLoss {
        let mut loss = self.rule_id.legacy(
            self.field_id,
            self.source_type.clone(),
            self.target_type.clone(),
        );
        loss.severity = self.severity;
        loss
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationEntry {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::metadata_subject::present"
    )]
    pub subject: Option<u32>,
    pub field_id: u32,
    pub rule_id: RuleId,
    pub basis: Basis,
    pub checked: u64,
    pub would_change: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evaluation {
    pub complete: bool,
    #[serde(serialize_with = "sorted_evaluation")]
    pub entries: Vec<EvaluationEntry>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffectiveEntry {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::metadata_subject::present"
    )]
    pub subject: Option<u32>,
    pub field_id: u32,
    pub rule_id: RuleId,
    pub category: LossCategory,
    pub basis: Basis,
    pub committed: u64,
    pub uncertain: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionLossReport {
    pub schema_version: u32,
    pub policy: MappingPolicy,
    #[serde(serialize_with = "sorted_risk")]
    pub risk: Vec<RiskEntry>,
    pub evaluation: Evaluation,
    #[serde(serialize_with = "sorted_effective")]
    pub effective: Vec<EffectiveEntry>,
}

pub(crate) fn invalid() -> DatabaseError {
    DatabaseError::invalid_plan("rapporto esecutivo delle perdite incoerente")
}
pub(crate) fn limit() -> DatabaseError {
    let mut error =
        DatabaseError::resource_limit("rapporto esecutivo delle perdite oltre i limiti");
    error.phase = crate::ErrorPhase::Prepare;
    error
}

impl ExecutionLossReport {
    /// Piano non ancora valutato. Gli alias devono essere risolti dal piano.
    /// # Errors
    /// Rifiuta duplicati e limiti prima dell'ingresso.
    pub fn planned(policy: MappingPolicy, mut risk: Vec<RiskEntry>) -> Result<Self> {
        risk.sort_by_key(|r| (r.field_id, r.rule_id, r.subject.as_ref().map(|s| s.index)));
        let report = Self {
            schema_version: if risk.iter().any(|r| r.subject.is_some()) {
                4
            } else {
                3
            },
            policy,
            risk,
            evaluation: Evaluation::default(),
            effective: vec![],
        };
        report.validate()?;
        Ok(report)
    }

    /// # Errors
    /// Rifiuta campi fuori contratto e conteggi non sostenuti dalla valutazione.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version
            != if self.risk.iter().any(|r| r.subject.is_some()) {
                4
            } else {
                3
            }
        {
            return Err(invalid());
        }
        if self.risk.len() > MAX_LOSSES
            || self.evaluation.entries.len() > MAX_LOSSES
            || self.effective.len() > MAX_LOSSES
        {
            return Err(limit());
        }
        let mut risks = BTreeMap::new();
        let mut metadata_keys = std::collections::BTreeSet::new();
        let mut potential = 0_usize;
        for risk in &self.risk {
            if (risk.rule_id == RuleId::PgMetadataKeyNotPreserved) != risk.subject.is_some() {
                return Err(invalid());
            }
            if let Some(subject) = &risk.subject {
                subject.validate()?;
                let field = (subject.scope == crate::metadata_subject::MetadataScope::Field)
                    .then_some(risk.field_id);
                if !metadata_keys.insert((field, &subject.key_sha256)) {
                    return Err(invalid());
                }
                if risk.rule_id != RuleId::PgMetadataKeyNotPreserved {
                    return Err(invalid());
                }
            }
            if risk.reason.is_empty()
                || risk.reason.chars().count() > MAX_REASON_CHARS
                || [&risk.source_type, &risk.target_type]
                    .into_iter()
                    .flatten()
                    .any(|t| t.chars().count() > MAX_TYPE_CHARS)
            {
                return Err(limit());
            }
            if risk.category != risk.rule_id.definition().category
                || risks
                    .insert(
                        (
                            risk.field_id,
                            risk.rule_id,
                            risk.subject.as_ref().map(|s| s.index),
                        ),
                        risk,
                    )
                    .is_some()
            {
                return Err(invalid());
            }
            potential = potential
                .checked_add(risk.rule_id.bases().len())
                .ok_or_else(limit)?;
        }
        if potential > MAX_LOSSES {
            return Err(limit());
        }
        let mut evaluations = BTreeMap::new();
        for entry in &self.evaluation.entries {
            if !risks.contains_key(&(entry.field_id, entry.rule_id, entry.subject))
                || entry.would_change > entry.checked
                || (entry.subject.is_some() && entry.checked > 1)
                || !entry.rule_id.bases().contains(&entry.basis)
                || evaluations
                    .insert(
                        (entry.field_id, entry.rule_id, entry.basis, entry.subject),
                        entry,
                    )
                    .is_some()
            {
                return Err(invalid());
            }
        }
        let mut effective = BTreeMap::new();
        for entry in &self.effective {
            let key = (entry.field_id, entry.rule_id, entry.basis, entry.subject);
            let evaluated = evaluations.get(&key).ok_or_else(invalid)?;
            let total = entry
                .committed
                .checked_add(entry.uncertain)
                .ok_or_else(invalid)?;
            if total == 0
                || total > evaluated.would_change
                || entry.category != entry.rule_id.definition().category
                || effective.insert(key, entry).is_some()
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    /// Coerenza con gli effetti confermati dall'outcome. Le basi della stessa
    /// cella/regola sono disgiunte; regole o campi diversi non si sommano.
    /// # Errors
    /// Rifiuta perdite incompatibili con stato o righe confermate.
    pub(crate) fn validate_outcome(
        &self,
        status: crate::outcome::WriteStatus,
        confirmed: u64,
    ) -> Result<()> {
        use crate::outcome::WriteStatus;
        let mut committed = BTreeMap::new();
        for entry in &self.effective {
            if (status == WriteStatus::RolledBack && (entry.committed != 0 || entry.uncertain != 0))
                || (status == WriteStatus::Committed && entry.uncertain != 0)
            {
                return Err(invalid());
            }
            let count = committed
                .entry((entry.field_id, entry.rule_id, entry.subject))
                .or_insert(0_u64);
            *count = count.checked_add(entry.committed).ok_or_else(invalid)?;
            if entry.subject.map_or(*count > confirmed, |_| *count > 1) {
                return Err(invalid());
            }
        }
        Ok(())
    }

    /// Budget serializzato con escaping reale e contatori alla massima larghezza.
    /// # Errors
    /// Rifiuta il piano prima degli effetti se il peggior caso non entra nel trasporto.
    pub fn validate_transport_budget(&self, max_bytes: usize) -> Result<()> {
        self.validate()?;
        let mut worst = self.clone();
        worst.evaluation.complete = false;
        worst.evaluation.entries.clear();
        worst.effective.clear();
        for risk in &self.risk {
            for &basis in risk.rule_id.bases() {
                worst.evaluation.entries.push(EvaluationEntry {
                    subject: risk.subject.as_ref().map(|s| s.index),
                    field_id: risk.field_id,
                    rule_id: risk.rule_id,
                    basis,
                    checked: u64::MAX,
                    would_change: u64::MAX,
                });
                worst.effective.push(EffectiveEntry {
                    subject: risk.subject.as_ref().map(|s| s.index),
                    field_id: risk.field_id,
                    rule_id: risk.rule_id,
                    category: risk.category,
                    basis,
                    committed: u64::MAX,
                    uncertain: u64::MAX,
                });
            }
        }
        let bytes = serde_json::to_vec(&worst).map_err(|_| invalid())?;
        if bytes.len() > max_bytes {
            return Err(limit());
        }
        Ok(())
    }

    pub fn sort(&mut self) {
        self.risk
            .sort_by_key(|r| (r.field_id, r.rule_id, r.subject.as_ref().map(|s| s.index)));
        self.evaluation
            .entries
            .sort_by_key(|r| (r.field_id, r.rule_id, r.basis, r.subject));
        self.effective
            .sort_by_key(|r| (r.field_id, r.rule_id, r.basis, r.subject));
    }
}
impl RuleId {
    #[must_use]
    pub const fn bases(self) -> &'static [Basis] {
        use crate::loss_registry::ClassificationMode;
        match self.definition().mode {
            ClassificationMode::Mixed => &[Basis::Exact, Basis::Conservative],
            ClassificationMode::Conservative => &[Basis::Conservative],
            ClassificationMode::Exact
            | ClassificationMode::HardReject
            | ClassificationMode::NoChange => &[Basis::Exact],
        }
    }
}

/// Legacy resta rischio non misurato: nessuna conversione implicita in v3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum VersionedLossReport {
    Execution(ExecutionLossReport),
    Legacy(LossReport),
}
impl From<LossReport> for VersionedLossReport {
    fn from(report: LossReport) -> Self {
        Self::Legacy(report)
    }
}
impl From<ExecutionLossReport> for VersionedLossReport {
    fn from(report: ExecutionLossReport) -> Self {
        Self::Execution(report)
    }
}
impl VersionedLossReport {
    /// # Errors
    /// Applica il validatore della versione dichiarata.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Legacy(r) => r.validate(),
            Self::Execution(r) => r.validate(),
        }
    }
    #[must_use]
    pub const fn legacy(&self) -> Option<&LossReport> {
        match self {
            Self::Legacy(r) => Some(r),
            Self::Execution(_) => None,
        }
    }
}
fn present_type<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<String>, D::Error> {
    String::deserialize(d).map(Some)
}

fn sorted_risk<S: serde::Serializer>(
    entries: &[RiskEntry],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut entries: Vec<_> = entries.iter().collect();
    entries.sort_by_key(|r| (r.field_id, r.rule_id, r.subject.as_ref().map(|s| s.index)));
    entries.serialize(serializer)
}
fn sorted_evaluation<S: serde::Serializer>(
    entries: &[EvaluationEntry],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut entries: Vec<_> = entries.iter().collect();
    entries.sort_by_key(|r| (r.field_id, r.rule_id, r.basis, r.subject));
    entries.serialize(serializer)
}
fn sorted_effective<S: serde::Serializer>(
    entries: &[EffectiveEntry],
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut entries: Vec<_> = entries.iter().collect();
    entries.sort_by_key(|r| (r.field_id, r.rule_id, r.basis, r.subject));
    entries.serialize(serializer)
}
