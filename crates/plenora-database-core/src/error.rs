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

impl DatabaseError {
    #[must_use]
    pub fn invalid_plan(message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::InvalidPlan,
            phase: ErrorPhase::Validate,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: None,
            execution_id: None,
            message: message.into(),
            diagnostics: None,
        }
    }

    #[must_use]
    pub fn unsupported(
        provider: ProviderKind,
        phase: ErrorPhase,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category: ErrorCategory::Unsupported,
            phase,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: Some(provider),
            execution_id: None,
            message: message.into(),
            diagnostics: None,
        }
    }

    #[must_use]
    pub fn cancelled(
        provider: Option<ProviderKind>,
        phase: ErrorPhase,
        message: impl Into<String>,
    ) -> Self {
        Self {
            category: ErrorCategory::Cancelled,
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
    pub fn resource_limit(message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::ResourceLimit,
            phase: ErrorPhase::Validate,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: None,
            execution_id: None,
            message: message.into(),
            diagnostics: None,
        }
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        !matches!(
            self.retry,
            RetryDisposition::Never | RetryDisposition::Quarantine
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
        Self {
            category: ErrorCategory::DataMapping,
            phase: ErrorPhase::Read,
            remote_effect: RemoteEffect::None,
            retry: RetryDisposition::Never,
            provider: None,
            execution_id: None,
            message: "schema Arrow non valido".to_owned(),
            diagnostics: None,
        }
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
        assert!(error.is_retryable());
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
}
