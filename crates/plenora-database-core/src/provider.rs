use crate::arrow::{RecordBatch, SchemaRef};
use crate::capabilities::ProviderCapabilities;
use crate::loss::LossReport;
use crate::outcome::WriteOutcome;
use crate::plan::{Operation, ProviderKind, ReadOperation, WriteOperation};
use crate::query::QueryOperation;
use crate::resource::{ResourceBudget, ResourceLease};
use crate::CancellationToken;
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;

pub type ProviderFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretString([REDACTED])")
    }
}

pub trait SecretResolver: Send + Sync {
    fn resolve<'a>(&'a self, connection_ref: &'a str) -> ProviderFuture<'a, SecretString>;
}

pub trait BatchStream: Send {
    fn schema(&self) -> SchemaRef;
    fn next_batch(&mut self) -> ProviderFuture<'_, Option<RecordBatch>>;

    /// Numero di righe sorgente che lo stream si impegna a produrre.
    ///
    /// Solo la sorgente conosce la dimensione dell'input: senza questa
    /// dichiarazione la scrittura non può partizionare l'input fra righe
    /// rifiutate, annullate e mai tentate, e non pubblica diagnostica
    /// row-scoped invece di stimarla.
    fn declared_input_rows(&self) -> Option<u64> {
        None
    }

    /// Politica di pubblicazione degli esempi row-scoped della sorgente.
    fn row_diagnostics_policy(&self) -> crate::row_diagnostics::RowDiagnosticsPolicy {
        crate::row_diagnostics::RowDiagnosticsPolicy::default()
    }

    fn next_batch_with_cancellation<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(crate::DatabaseError {
                    category: crate::ErrorCategory::Cancelled,
                    phase: crate::ErrorPhase::Read,
                    remote_effect: crate::RemoteEffect::None,
                    retry: crate::RetryDisposition::Never,
                    provider: None,
                    execution_id: None,
                    message: "lettura cancellata".to_owned(),
                    diagnostics: None,
                });
            }
            self.next_batch().await
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ParameterValue {
    Bool(bool),
    I32(i32),
    I64(i64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
    Timestamp(String),
    TimestampTz(String),
    Decimal(String),
    Uuid(String),
    Json(Value),
    Wkb {
        bytes: Vec<u8>,
        srid: Option<u32>,
        dimensions: crate::geometry::Dimensions,
        semantics: crate::geometry::SpatialSemantics,
    },
    Null {
        type_name: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ParameterBag(BTreeMap<String, ParameterValue>);

impl ParameterBag {
    #[must_use]
    pub const fn new(values: BTreeMap<String, ParameterValue>) -> Self {
        Self(values)
    }

    #[must_use]
    pub fn get(&self, name: &str) -> Option<&ParameterValue> {
        self.0.get(name)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &ParameterValue)> {
        self.0.iter()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionInfo {
    pub provider: ProviderKind,
    pub server_version: String,
    pub connection_identity: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Inspection {
    pub operation: String,
    pub document: Value,
}

pub struct PreparedWrite {
    pub operation: WriteOperation,
    pub input_schema: SchemaRef,
    pub loss_report: LossReport,
    pub budget: ResourceBudget,
    pub operation_lease: ResourceLease,
    pub columns_lease: ResourceLease,
}

pub trait Provider: Send + Sync {
    fn kind(&self) -> ProviderKind;

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo>;

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities>;

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection>;

    fn read<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>>;

    fn query<'a>(
        &'a self,
        _secret: &'a SecretString,
        _operation: &'a QueryOperation,
        _parameters: &'a ParameterBag,
        _budget: &'a ResourceBudget,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        Box::pin(async move {
            Err(crate::DatabaseError::unsupported(
                self.kind(),
                crate::ErrorPhase::Prepare,
                "query AST non supportata dal provider",
            ))
        })
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite>;

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, WriteOutcome>;
}
