use crate::plan::ProviderKind;
use crate::RemoteEffect;
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
    CommitRequested,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteOutcome {
    pub schema_version: u32,
    pub status: WriteStatus,
    pub execution_id: String,
    pub provider: ProviderKind,
    pub rows: RowCounts,
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
        if matches!(self.status, WriteStatus::OutcomeUnknown)
            && self
                .recovery
                .as_ref()
                .is_some_and(|recovery| recovery.automatic_retry_allowed)
        {
            return Err(crate::DatabaseError::invalid_plan(
                "un outcome ignoto richiede recovery prima del retry",
            ));
        }
        if matches!(
            self.status,
            WriteStatus::RolledBack | WriteStatus::OutcomeUnknown
        ) && self.rows.confirmed != 0
        {
            return Err(crate::DatabaseError::invalid_plan(
                "un outcome rolled_back o unknown non può confermare righe",
            ));
        }
        let accounted = self
            .rows
            .confirmed
            .checked_add(self.rows.failed)
            .and_then(|value| value.checked_add(self.rows.skipped))
            .ok_or_else(|| {
                crate::DatabaseError::invalid_plan("overflow nella contabilità delle righe")
            })?;
        if accounted > self.rows.received {
            return Err(crate::DatabaseError::invalid_plan(
                "i conteggi di write superano le righe ricevute",
            ));
        }
        let mutations = [self.rows.inserted, self.rows.updated, self.rows.deleted]
            .into_iter()
            .flatten()
            .try_fold(0_u64, u64::checked_add)
            .ok_or_else(|| {
                crate::DatabaseError::invalid_plan("overflow nei conteggi delle mutazioni")
            })?;
        if mutations > self.rows.confirmed {
            return Err(crate::DatabaseError::invalid_plan(
                "le mutazioni confermate superano le righe confermate",
            ));
        }
        Ok(())
    }

    #[must_use]
    pub const fn remote_effect(&self) -> RemoteEffect {
        match self.status {
            WriteStatus::Committed => RemoteEffect::Committed,
            WriteStatus::RolledBack => RemoteEffect::RolledBack,
            WriteStatus::PartiallyCommitted => RemoteEffect::Partial,
            WriteStatus::OutcomeUnknown => RemoteEffect::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_unknown_outcome_example() {
        let input = include_str!("../../../contracts/v2/examples/outcome-unknown.json");
        let outcome: WriteOutcome = serde_json::from_str(input).expect("outcome example");
        outcome.validate().expect("valid outcome");
        assert_eq!(outcome.schema_version, 2);
        assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
        assert_eq!(
            outcome
                .recovery
                .as_ref()
                .expect("recovery")
                .last_certain_phase,
            CertainPhase::CommitRequested
        );
        assert_eq!(outcome.remote_effect(), RemoteEffect::Unknown);
    }

    #[test]
    fn row_accounting_overflow_is_rejected_without_panicking() {
        let outcome = WriteOutcome {
            schema_version: 2,
            status: WriteStatus::Committed,
            execution_id: "overflow".to_owned(),
            provider: ProviderKind::Postgres,
            rows: RowCounts {
                received: u64::MAX,
                confirmed: u64::MAX,
                inserted: None,
                updated: None,
                deleted: None,
                failed: 1,
                skipped: 0,
            },
            recovery: None,
        };
        assert!(outcome.validate().is_err());
    }

    #[test]
    fn unknown_outcome_cannot_authorize_automatic_retry() {
        let mut outcome: WriteOutcome = serde_json::from_str(include_str!(
            "../../../contracts/v2/examples/outcome-unknown.json"
        ))
        .expect("outcome example");
        outcome
            .recovery
            .as_mut()
            .expect("unknown recovery")
            .automatic_retry_allowed = true;
        assert!(outcome.validate().is_err());
    }
}
