use crate::plan::ProviderKind;
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
    Safe,
    RequiresIdempotencyKey,
    RequiresRecovery,
    After(u64),
}

/// Errore pubblico già redatto.
///
/// `message` deve contenere contesto operativo, mai DSN, token, SQL bindato o
/// payload. Il dettaglio vendor appartiene a un sink protetto esterno.
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
        }
    }

    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        !matches!(self.retry, RetryDisposition::Never)
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
        };

        assert_eq!(error.category, ErrorCategory::Timeout);
        assert_eq!(error.remote_effect, RemoteEffect::Unknown);
        assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
        assert!(error.is_retryable());
    }

    #[test]
    fn cancelled_error_is_never_retryable_by_default() {
        let error = DatabaseError::cancelled(None, ErrorPhase::Read, "annullata");
        assert_eq!(error.remote_effect, RemoteEffect::None);
        assert_eq!(error.retry, RetryDisposition::Never);
        assert!(!error.is_retryable());
    }
}
