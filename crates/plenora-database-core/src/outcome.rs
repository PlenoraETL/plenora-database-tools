use crate::plan::ProviderKind;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteStatus {
    Committed,
    RolledBack,
    PartiallyCommitted,
    OutcomeUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowCounts {
    pub received: u64,
    pub confirmed: u64,
    pub inserted: Option<u64>,
    pub updated: Option<u64>,
    pub deleted: Option<u64>,
    pub failed: u64,
    pub skipped: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CertainPhase {
    SessionReady,
    TransactionBegun,
    StagingPrepared,
    Writing,
    Finalizing,
    CommitOrEditRequested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recovery {
    pub last_certain_phase: CertainPhase,
    pub automatic_retry_allowed: bool,
    pub idempotency_key: Option<String>,
    pub staging_object: Option<String>,
    pub verification_action: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LayerStatus {
    Committed,
    RolledBack,
    Partial,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LayerOutcome {
    pub layer: String,
    pub status: LayerStatus,
    pub confirmed: Option<u64>,
    pub failed: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteOutcome {
    pub schema_version: u32,
    pub status: WriteStatus,
    pub execution_id: String,
    pub provider: ProviderKind,
    pub rows: RowCounts,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub layer_outcomes: Vec<LayerOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<Recovery>,
}

impl WriteOutcome {
    /// Verifica recovery e contabilità delle righe.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per combinazioni di stato/recovery incoerenti
    /// o conteggi superiori alle righe ricevute.
    pub fn validate(&self) -> crate::Result<()> {
        let uncertain = matches!(
            self.status,
            WriteStatus::PartiallyCommitted | WriteStatus::OutcomeUnknown
        );
        if uncertain != self.recovery.is_some() {
            return Err(crate::DatabaseError::invalid_plan(
                "recovery deve essere presente soltanto per outcome parziale o incerto",
            ));
        }
        if self.rows.confirmed + self.rows.failed + self.rows.skipped > self.rows.received {
            return Err(crate::DatabaseError::invalid_plan(
                "i conteggi di write superano le righe ricevute",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_arcgis_unknown_example() {
        let input = include_str!("../../../contracts/v1/examples/outcome-unknown.json");
        let outcome: WriteOutcome = serde_json::from_str(input).expect("outcome example");
        outcome.validate().expect("valid outcome");
        assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
        assert_eq!(outcome.layer_outcomes.len(), 1);
    }
}
