//! Envelope di errore pubblico, classificazione del retry e redazione.
//!
//! I messaggi contengono solo contesto operativo: payload, DSN, bind e valori
//! di riga non attraversano questo confine.

use crate::plan::ProviderKind;
use crate::row_diagnostics::RowDiagnostics;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    InvalidPlan,
    InvalidConfiguration,
    Schema,
    DataMapping,
    Crs,
    Unsupported,
    NotFound,
    Conflict,
    /// Update ottimistico rifiutato: la versione attesa non corrisponde più
    /// allo stato corrente. Distinta da `NotFound` (chiave inesistente) e da
    /// `Conflict` (violazione integrità): il consumer deve poter reagire con
    /// una politica di retry/refresh dedicata.
    ConcurrentModification,
    Authentication,
    Authorization,
    Timeout,
    Cancelled,
    ResourceLimit,
    Io,
    Protocol,
    Transient,
    Execution,
    Internal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorPhase {
    Validate,
    Connect,
    Probe,
    Prepare,
    Read,
    Write,
    Finalize,
    Commit,
    Rollback,
    Cleanup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteEffect {
    None,
    RolledBack,
    Partial,
    Committed,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "delay_ms", rename_all = "snake_case")]
pub enum RetryDisposition {
    Never,
    /// L'operazione e la sessione che l'ha eseguita vanno messe da parte: né
    /// un retry automatico né un riuso della connessione sono autorizzati
    /// finché l'effetto remoto non è stato verificato fuori banda.
    Quarantine,
    Safe,
    RequiresIdempotencyKey,
    RequiresRecovery,
    After(u64),
}

/// Errore pubblico già redatto.
///
/// `message` deve contenere contesto operativo, mai DSN, token, SQL bindato o
/// payload. Il dettaglio vendor appartiene a un sink protetto esterno.
///
/// # Cosa conta come contesto operativo
///
/// La regola "niente payload" da sola non decide i casi di confine, e per un
/// po' e stata applicata a intuito: alcuni messaggi ricopiavano un argomento
/// qualsiasi della riga di comando, altri toglievano perfino il nome della
/// colonna che il chiamante aveva scritto nel proprio piano. Sono due cose
/// diverse, e la differenza e **da dove viene la stringa**.
///
/// Puo comparire nel messaggio cio che il chiamante ha dichiarato in uno slot
/// tipizzato: nomi di colonna, di chiave, di parametro, chiavi di session
/// context, identificatori di oggetto. Sono la sua struttura, li ha scritti
/// lui, e senza di essi l'errore non e azionabile — "una chiave non esiste nel
/// batch" non dice quale.
///
/// Non puo comparire cio che e arrivato in uno slot non ancora validato o che
/// trasporta dati: un argomento posizionale fuori posto (puo essere una DSN,
/// un token, dello SQL), una parola estratta da testo SQL libero, il valore di
/// una cella, il `Display` di un errore di libreria che nomina byte e offset
/// del dato. Li il messaggio dice **quale slot** e sbagliato e quali valori
/// sono ammessi, mai cosa e stato ricevuto.
///
/// La prova di un errore di parsing e la posizione — riga e colonna — non il
/// frammento.
///
/// `diagnostics` è il carrier row-scoped: quando l'esecuzione ha potuto
/// identificare le righe sorgente rifiutate, il documento
/// `plenora-row-diagnostics-v1` viaggia con l'errore invece di essere
/// ricostruito dal testo del messaggio.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{category:?} during {phase:?} (effect={remote_effect:?}, retry={retry:?}): {message}")]
#[serde(deny_unknown_fields)]
pub struct DatabaseError {
    pub category: ErrorCategory,
    pub phase: ErrorPhase,
    pub remote_effect: RemoteEffect,
    pub retry: RetryDisposition,
    pub provider: Option<ProviderKind>,
    pub execution_id: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diagnostics: Option<Box<RowDiagnostics>>,
}

/// La categoria pubblica di un'interruzione, secondo la causa che l'ha
/// prodotta.
///
/// Sta nel core perche provider e retry engine classifichino la stessa causa
/// nello stesso modo.
#[must_use]
pub fn interruption_category(cancellation: &crate::CancellationToken) -> ErrorCategory {
    if cancellation.reason() == Some(crate::CancellationReason::Deadline) {
        ErrorCategory::Timeout
    } else {
        ErrorCategory::Cancelled
    }
}

impl DatabaseError {
    /// Costruisce un errore locale non ritentabile e senza effetto remoto.
    ///
    /// `message` deve essere gia redatto secondo il contratto di
    /// [`DatabaseError`]: il costruttore centralizza l'inviluppo sicuro, non
    /// rende pubblicabile un dettaglio del driver o un payload.
    #[must_use]
    pub fn new(
        category: ErrorCategory,
        phase: ErrorPhase,
        provider: Option<ProviderKind>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category,
            phase,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider,
            execution_id: None,
            message: message.into(),
            diagnostics: None,
        }
    }

    #[must_use]
    pub fn invalid_plan(message: impl Into<String>) -> Self {
        Self::new(
            ErrorCategory::InvalidPlan,
            ErrorPhase::Validate,
            None,
            message,
        )
    }

    #[must_use]
    pub fn unsupported(
        provider: ProviderKind,
        phase: ErrorPhase,
        message: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCategory::Unsupported, phase, Some(provider), message)
    }

    #[must_use]
    /// L'errore di un'operazione interrotta, con la **causa** che l'ha
    /// interrotta.
    ///
    /// Una deadline scaduta e un `Timeout`, tutto il resto una `Cancelled`.
    /// La classificazione centralizzata impedisce che provider e retry engine
    /// attribuiscano categorie diverse allo stesso token.
    pub fn interrupted(
        cancellation: &crate::CancellationToken,
        provider: Option<ProviderKind>,
        phase: ErrorPhase,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category: interruption_category(cancellation),
            ..Self::cancelled(provider, phase, message)
        }
    }

    pub fn cancelled(
        provider: Option<ProviderKind>,
        phase: ErrorPhase,
        message: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCategory::Cancelled, phase, provider, message)
    }

    #[must_use]
    pub fn resource_limit(message: impl Into<String>) -> Self {
        Self::new(
            ErrorCategory::ResourceLimit,
            ErrorPhase::Validate,
            None,
            message,
        )
    }

    /// Un retry **automatico** e autorizzato, senza intervento fuori banda.
    ///
    /// Solo `Safe` e `After` lo sono. `RequiresRecovery` e
    /// `RequiresIdempotencyKey` descrivono un esito che un tentativo cieco
    /// duplicherebbe: il chiamante deve prima verificare l'effetto remoto o
    /// procurarsi una chiave di idempotenza. Il metodo diceva il contrario di
    /// [`crate::Result`]-consumer come `retry_with_policy`, che quelle due
    /// disposizioni le propaga invece di ritentarle — un consumer che si
    /// fidava di questa risposta poteva duplicare una scrittura.
    ///
    /// Chi deve distinguere «non ritentabile mai» da «ritentabile dopo
    /// recovery» usa [`Self::requires_manual_recovery`].
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self.retry,
            RetryDisposition::Safe | RetryDisposition::After(_)
        )
    }

    /// Il retry e possibile, ma soltanto dopo un intervento fuori banda.
    ///
    /// Vero per `RequiresRecovery` e `RequiresIdempotencyKey`: l'operazione
    /// non e persa, ma nessun automatismo puo riprenderla da solo.
    #[must_use]
    pub const fn requires_manual_recovery(&self) -> bool {
        matches!(
            self.retry,
            RetryDisposition::RequiresRecovery | RetryDisposition::RequiresIdempotencyKey
        )
    }

    /// Allega la diagnostica row-scoped dopo averne verificato le invarianti.
    ///
    /// # Errors
    ///
    /// Propaga `InvalidPlan` quando il documento non supera la validazione del
    /// contratto: un errore non può trasportare una diagnostica non valida.
    pub fn with_row_diagnostics(mut self, diagnostics: RowDiagnostics) -> Result<Self> {
        diagnostics.validate()?;
        self.diagnostics = Some(Box::new(diagnostics));
        Ok(self)
    }

    #[must_use]
    pub fn row_diagnostics(&self) -> Option<&RowDiagnostics> {
        self.diagnostics.as_deref()
    }
}

impl From<arrow_schema::ArrowError> for DatabaseError {
    fn from(_: arrow_schema::ArrowError) -> Self {
        Self::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            None,
            "schema Arrow non valido",
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_effect_is_not_a_cause_category() {
        let error = DatabaseError {
            category: ErrorCategory::Timeout,
            phase: ErrorPhase::Commit,
            remote_effect: RemoteEffect::Unknown,
            retry: RetryDisposition::RequiresRecovery,
            provider: Some(ProviderKind::Postgres),
            execution_id: Some("execution-1".to_owned()),
            message: "esito commit non verificabile".to_owned(),
            diagnostics: None,
        };

        assert_eq!(error.category, ErrorCategory::Timeout);
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
        // Un commit dall'esito ignoto non e ritentabile *da solo*: prima
        // qualcuno deve verificare cosa e successo davvero al remoto.
        assert!(!error.is_retryable());
        assert!(error.requires_manual_recovery());
    }

    /// Le due risposte partizionano le disposizioni, e nessuna disposizione
    /// e insieme automatica e da recuperare a mano.
    #[test]
    fn automatic_retry_and_manual_recovery_partition_the_dispositions() {
        let dispositions = [
            (RetryDisposition::Never, false, false),
            (RetryDisposition::Quarantine, false, false),
            (RetryDisposition::Safe, true, false),
            (RetryDisposition::After(10), true, false),
            (RetryDisposition::RequiresIdempotencyKey, false, true),
            (RetryDisposition::RequiresRecovery, false, true),
        ];
        for (retry, automatic, manual) in dispositions {
            let error = DatabaseError {
                category: ErrorCategory::Transient,
                phase: ErrorPhase::Write,
                remote_effect: RemoteEffect::None,
                retry,
                provider: None,
                execution_id: None,
                message: "classificazione".to_owned(),
                diagnostics: None,
            };
            assert_eq!(error.is_retryable(), automatic, "{retry:?}");
            assert_eq!(error.requires_manual_recovery(), manual, "{retry:?}");
            assert!(!(error.is_retryable() && error.requires_manual_recovery()));
        }
    }

    /// La quarantena non è un retry rimandato: nessun tentativo automatico è
    /// autorizzato finché l'effetto remoto non è stato verificato.
    #[test]
    fn quarantine_is_not_a_retryable_disposition() {
        let error = DatabaseError {
            category: ErrorCategory::DataMapping,
            phase: ErrorPhase::Rollback,
            remote_effect: RemoteEffect::Unknown,
            retry: RetryDisposition::Quarantine,
            provider: Some(ProviderKind::Mysql),
            execution_id: Some("execution-2".to_owned()),
            message: "annullamento non confermato".to_owned(),
            diagnostics: None,
        };
        assert!(!error.is_retryable());
        assert_eq!(
            serde_json::to_value(error.retry).expect("retry serializzabile"),
            serde_json::json!({"kind": "quarantine"})
        );
    }

    /// Il carrier non è un campo libero: una diagnostica che non supera il
    /// contratto non può viaggiare con l'errore.
    #[test]
    fn the_row_diagnostics_carrier_refuses_an_invalid_document() {
        let mut tracker = crate::row_diagnostics::WriteDiagnosticsTracker::new(
            5_200,
            crate::row_diagnostics::RowDiagnosticsPolicy {
                key_field: Some("parcel_id".to_owned()),
                constraint_column: None,
                examples_limit: 10,
            },
        )
        .expect("tracker");
        tracker.stage_rows(4_999).expect("righe messe in scena");
        let report = tracker
            .reject_row(
                &crate::row_diagnostics::RejectedRow {
                    source_index: 4_999,
                    cause: crate::row_diagnostics::CAUSE_CONSTRAINT_VIOLATION.to_owned(),
                    column: None,
                },
                crate::row_diagnostics::RollbackEvidence::Confirmed,
            )
            .expect("diagnostica");

        let carried = DatabaseError::invalid_plan("riga rifiutata")
            .with_row_diagnostics(report.clone())
            .expect("carrier valido");
        assert_eq!(carried.row_diagnostics(), Some(&report));
        let encoded = serde_json::to_value(&carried).expect("errore serializzabile");
        assert_eq!(encoded["diagnostics"]["scope"], "write");
        assert!(!encoded["diagnostics"].to_string().contains("4999.0"));

        let mut broken = report;
        broken.observed_total = 7;
        assert!(DatabaseError::invalid_plan("riga rifiutata")
            .with_row_diagnostics(broken)
            .is_err());

        let plain = DatabaseError::invalid_plan("nessuna diagnostica");
        let encoded = serde_json::to_value(&plain).expect("errore serializzabile");
        assert!(
            encoded.get("diagnostics").is_none(),
            "un errore senza diagnostica non pubblica il campo"
        );
    }

    #[test]
    fn cancelled_error_is_never_retryable_by_default() {
        let error = DatabaseError::cancelled(None, ErrorPhase::Read, "annullata");
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert!(!error.is_retryable());
    }

    #[test]
    fn new_builds_the_safe_local_envelope() {
        let error = DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            Some(ProviderKind::Sqlserver),
            "conversione redatta",
        );
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert_eq!(error.provider, Some(ProviderKind::Sqlserver));
        assert!(error.execution_id.is_none());
        assert!(error.diagnostics.is_none());
    }
}
