//! Esiti portabili delle scritture e informazioni necessarie al recupero.

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

/// Le lunghezze massime che `contracts/v2/write-outcome.schema.json`
/// dichiara, in code point come `maxLength` di JSON Schema.
const MAX_EXECUTION_ID_CHARS: usize = 128;
const MAX_IDEMPOTENCY_KEY_CHARS: usize = 256;
const MAX_STAGING_OBJECT_CHARS: usize = 512;
const MAX_VERIFICATION_ACTION_CHARS: usize = 1024;

impl WriteOutcome {
    /// Verifica che l'esito stia dentro il contratto, e che sia coerente.
    ///
    /// Guardava soltanto recovery e contabilità: la major, la lunghezza di
    /// `execution_id` e quelle dei campi di recovery — tutte scritte nello
    /// schema — non le controllava nessuno. Un esito con `execution_id` vuoto
    /// o con un `verification_action` di diecimila caratteri passava di qui e
    /// arrivava al consumatore, che e l'unico posto dove si sarebbe scoperto.
    ///
    /// # Errors
    ///
    /// `InvalidPlan` per major non supportata, `execution_id` fuori dalle
    /// lunghezze del contratto, campi di recovery troppo lunghi, combinazioni
    /// di stato/recovery incoerenti o conteggi superiori alle righe ricevute.
    pub fn validate(&self) -> crate::Result<()> {
        if self.schema_version != 2 {
            return Err(crate::DatabaseError::invalid_plan(
                "esito di scrittura con schema_version non supportata",
            ));
        }
        // `minLength: 1` e `maxLength: 128` contano code point.
        let execution_id_chars = self.execution_id.chars().count();
        if execution_id_chars == 0 {
            return Err(crate::DatabaseError::invalid_plan(
                "esito di scrittura senza execution_id",
            ));
        }
        if execution_id_chars > MAX_EXECUTION_ID_CHARS {
            return Err(crate::DatabaseError::invalid_plan(
                "execution_id oltre la lunghezza del contratto",
            ));
        }
        if let Some(recovery) = &self.recovery {
            // I messaggi non riportano il campo: `staging_object` e un nome di
            // oggetto e `verification_action` una frase costruita su di esso.
            let too_long = [
                (&recovery.idempotency_key, MAX_IDEMPOTENCY_KEY_CHARS),
                (&recovery.staging_object, MAX_STAGING_OBJECT_CHARS),
                (&recovery.verification_action, MAX_VERIFICATION_ACTION_CHARS),
            ]
            .into_iter()
            .any(|(field, limit)| {
                field
                    .as_ref()
                    .is_some_and(|value| value.chars().count() > limit)
            });
            if too_long {
                return Err(crate::DatabaseError::invalid_plan(
                    "campo di recovery oltre la lunghezza del contratto",
                ));
            }
        }
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
#[path = "outcome_tests.rs"]
mod tests;
