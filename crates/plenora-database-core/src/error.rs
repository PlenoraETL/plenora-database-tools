use crate::plan::ProviderKind;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, DatabaseError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCategory {
    InvalidPlan,
    InvalidConfiguration,
    Authentication,
    Authorization,
    NotFound,
    Conflict,
    Unsupported,
    Timeout,
    Cancelled,
    ResourceLimit,
    DataMapping,
    Protocol,
    Transient,
    OutcomeUnknown,
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

/// Errore pubblico già redatto.
///
/// `message` deve contenere contesto operativo, mai DSN, token, SQL bindato o
/// payload. Il dettaglio vendor appartiene a un sink protetto esterno.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("{category:?} during {phase:?}: {message}")]
#[serde(deny_unknown_fields)]
pub struct DatabaseError {
    pub category: ErrorCategory,
    pub phase: ErrorPhase,
    pub provider: Option<ProviderKind>,
    pub retryable: bool,
    pub execution_id: Option<String>,
    pub message: String,
}

impl DatabaseError {
    #[must_use]
    pub fn invalid_plan(message: impl Into<String>) -> Self {
        Self {
            category: ErrorCategory::InvalidPlan,
            phase: ErrorPhase::Validate,
            provider: None,
            retryable: false,
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
            provider: Some(provider),
            retryable: false,
            execution_id: None,
            message: message.into(),
        }
    }
}

impl From<arrow_schema::ArrowError> for DatabaseError {
    fn from(_: arrow_schema::ArrowError) -> Self {
        Self {
            category: ErrorCategory::DataMapping,
            phase: ErrorPhase::Read,
            provider: None,
            retryable: false,
            execution_id: None,
            message: "schema Arrow non valido".to_owned(),
        }
    }
}
