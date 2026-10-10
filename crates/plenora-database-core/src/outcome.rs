//! Esiti portabili delle scritture e informazioni necessarie al recupero.

use crate::plan::ProviderKind;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};
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

impl WriteOutcome {
    /// La scrittura come la riporta una superficie pubblica: il documento se
    /// e un successo pieno, l'errore tipizzato altrimenti.
    ///
    /// Un esito `outcome_unknown`, `partially_committed` o `rolled_back` e un
    /// valore legittimo del tipo Rust, ma non e un successo: una superficie
    /// che lo consegnasse come risultato `ok` direbbe all'orchestratore di
    /// proseguire su uno stato remoto che nessuno ha verificato (SURF-014 dei
    /// contratti comuni). CLI, runtime e SDK Python passano tutti da qui, cosi
    /// la stessa classe di esito ha lo stesso trattamento su ogni strada.
    ///
    /// Il documento dell'esito non va perso: [`UnsettledWrite`] lo conserva,
    /// e [`UnsettledWrite::public_document`] lo mette in
    /// `details.write_outcome` dell'errore pubblico, dove `recovery` dice al
    /// chiamante che cosa verificare prima di un nuovo tentativo.
    ///
    /// Un documento che [`Self::validate`] rifiuta non e mai un successo e
    /// non si inoltra: un `committed` con conteggi incoerenti, o un esito
    /// ignoto che autorizza il retry automatico, diventano un errore con
    /// effetto remoto ignoto e senza il documento, che contraddirebbe gli
    /// assi.
    ///
    /// # Errors
    ///
    /// [`UnsettledWrite`] per ogni stato diverso da `committed` e per ogni
    /// documento fuori contratto.
    pub fn settle(self) -> std::result::Result<Self, Box<UnsettledWrite>> {
        if self.validate().is_err() {
            return Err(Box::new(UnsettledWrite {
                error: DatabaseError {
                    category: ErrorCategory::Internal,
                    phase: ErrorPhase::Finalize,
                    remote_effect: RemoteEffect::Unknown,
                    retry: RetryDisposition::RequiresRecovery,
                    provider: Some(self.provider),
                    execution_id: None,
                    message: "esito di scrittura fuori contratto: verificare lo stato remoto \
                              prima di ogni nuovo tentativo"
                        .to_owned(),
                    diagnostics: None,
                },
                outcome: None,
            }));
        }
        let (category, phase, retry, message) = match self.status {
            WriteStatus::Committed => return Ok(self),
            // Gli stessi assi del vettore `database-write-error` dei contratti
            // e dell'errore di commit dell'SDK: l'incertezza sta sul COMMIT.
            WriteStatus::OutcomeUnknown => (
                ErrorCategory::Internal,
                ErrorPhase::Commit,
                RetryDisposition::RequiresRecovery,
                "esito del commit ignoto: verificare lo stato remoto per execution_id \
                 prima di ogni nuovo tentativo",
            ),
            WriteStatus::PartiallyCommitted => (
                ErrorCategory::Execution,
                ErrorPhase::Write,
                RetryDisposition::RequiresRecovery,
                "scrittura confermata solo in parte: recovery richiesto prima di ogni \
                 nuovo tentativo",
            ),
            // Nessun adapter lo restituisce oggi come esito: lo stato esiste
            // nel contratto, e una superficie non puo leggerlo come successo.
            WriteStatus::RolledBack => (
                ErrorCategory::Execution,
                ErrorPhase::Write,
                RetryDisposition::Never,
                "scrittura annullata: nessuna riga confermata",
            ),
        };
        let error = DatabaseError {
            category,
            phase,
            remote_effect: self.remote_effect(),
            retry,
            provider: Some(self.provider),
            execution_id: Some(self.execution_id.clone()),
            message: message.to_owned(),
            diagnostics: None,
        };
        Err(Box::new(UnsettledWrite {
            error,
            outcome: Some(self),
        }))
    }
}

/// Una scrittura che non si e conclusa con un commit certo e completo.
///
/// Porta insieme l'errore pubblico e il documento dell'esito, che contiene la
/// `recovery`: separarli farebbe perdere al chiamante proprio l'informazione
/// che gli serve per decidere se e come riprendere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsettledWrite {
    error: DatabaseError,
    outcome: Option<WriteOutcome>,
}

impl UnsettledWrite {
    /// L'errore tipizzato con gli assi dell'esito.
    #[must_use]
    pub const fn error(&self) -> &DatabaseError {
        &self.error
    }

    /// Il documento `write-outcome` restituito dal provider; `None` se il
    /// contratto lo rifiuta.
    #[must_use]
    pub const fn outcome(&self) -> Option<&WriteOutcome> {
        self.outcome.as_ref()
    }

    /// L'errore `plenora-error-v1` con l'esito, se c'e, in
    /// `details.write_outcome`.
    ///
    /// `details` e l'unico oggetto aperto dello schema comune. L'esito non
    /// porta valori di riga: identificatore d'esecuzione, conteggi e, nella
    /// `recovery`, nomi di oggetto e la frase di verifica costruita su di essi,
    /// tutti limitati in lunghezza da `write-outcome.schema.json`.
    ///
    /// # Errors
    ///
    /// Se l'errore o l'esito non si serializzano.
    pub fn public_document(&self) -> serde_json::Result<serde_json::Value> {
        let mut document = serde_json::to_value(self.error.public_projection())?;
        let Some(outcome) = &self.outcome else {
            return Ok(document);
        };
        let outcome = serde_json::to_value(outcome)?;
        if let serde_json::Value::Object(fields) = &mut document {
            let details = fields
                .entry("details")
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            if let serde_json::Value::Object(details) = details {
                details.insert("write_outcome".to_owned(), outcome);
            }
        }
        Ok(document)
    }
}

#[cfg(test)]
#[path = "outcome_tests.rs"]
mod tests;
