//! Observability standard `db.*`.
//!
//! Il PFM richiede che la libreria emetta un set canonico di
//! segnali via un `sink` generico, senza imporre uno specifico backend
//! (tracing, prometheus, statsd, ecc.). Questo modulo definisce:
//!
//! - il trait `MetricsRecorder`,
//! - l'enum `MetricName` con i nomi standard,
//! - il tipo `MetricEvent` con valore + tags,
//! - un `NoopRecorder` usato quando nessun sink è configurato.
//!
//! **Policy di redazione**: nessuna implementazione della libreria deve
//! passare al recorder: password, token, connection string complete, SQL
//! interpolato con dati, geometrie massive, valori classificati non
//! necessari. Solo `execution_id`, `correlation_id`, `provider_family`,
//! `operation_kind` e valori numerici scalari.

use crate::plan::ProviderKind;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Nome canonico di un evento metrico.
///
/// La serializzazione (`snake_case`) produce direttamente il nome atteso
/// dai backend osservabilità (`db.operation.duration`, ecc.).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricName {
    /// Durata di una singola operazione (read/write/query/execute).
    DbOperationDuration,
    /// Attesa in checkout dal pool connessioni.
    DbPoolWait,
    /// Righe lette in un batch/stream.
    DbRowsRead,
    /// Righe scritte da una write operation.
    DbRowsWritten,
    /// Durata totale di una transazione (begin → commit/rollback).
    DbTransactionDuration,
    /// Counter incrementato per ogni rollback (esplicito o su errore).
    DbTransactionRollback,
    /// Counter incrementato per ogni commit ambiguo (`OutcomeUnknown`).
    DbTransactionOutcomeUnknown,
    /// Counter incrementato per ogni retry classificato dalla libreria.
    DbRetry,
    /// Counter incrementato per ogni timeout scattato.
    DbTimeout,
    /// Counter incrementato per ogni cancellation applicata.
    DbCancelled,
    /// Counter incrementato per ogni fallimento di capability probe.
    DbCapabilityFailure,
}

impl MetricName {
    /// Nome dot-separato canonico (utile per backend che accettano stringhe).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DbOperationDuration => "db.operation.duration",
            Self::DbPoolWait => "db.pool.wait",
            Self::DbRowsRead => "db.rows.read",
            Self::DbRowsWritten => "db.rows.written",
            Self::DbTransactionDuration => "db.transaction.duration",
            Self::DbTransactionRollback => "db.transaction.rollback",
            Self::DbTransactionOutcomeUnknown => "db.transaction.outcome_unknown",
            Self::DbRetry => "db.retry",
            Self::DbTimeout => "db.timeout",
            Self::DbCancelled => "db.cancelled",
            Self::DbCapabilityFailure => "db.capability.failure",
        }
    }
}

/// Valore associato a un evento.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MetricValue {
    /// Counter incrementale (tipicamente `+1`).
    Count(u64),
    /// Durata in millisecondi.
    DurationMs(u64),
    /// Numero di righe (per read/write).
    Rows(u64),
}

/// Categorizzazione dell'operazione che ha originato l'evento.
///
/// Usata come tag: aiuta ad aggregare metriche per pipeline
/// (es. `read` vs `write` vs `transaction`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Connect,
    Probe,
    Inspect,
    Read,
    Query,
    Write,
    Transaction,
    Execute,
    Facade,
    Cursor,
    ConformanceProbe,
}

/// Tag di contesto tecnico associati all'evento (**mai** payload di dominio).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricTags {
    /// Famiglia di provider (`postgres`, `mysql`, `sqlserver`, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_family: Option<ProviderKind>,
    /// Categoria dell'operazione che ha originato l'evento.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_kind: Option<OperationKind>,
    /// Identificativo dell'esecuzione (opaco al PFM).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    /// Correlazione end-to-end fornita dal chiamante.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
}

impl MetricTags {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            provider_family: None,
            operation_kind: None,
            execution_id: None,
            correlation_id: None,
        }
    }

    #[must_use]
    pub const fn with_provider(mut self, provider: ProviderKind) -> Self {
        self.provider_family = Some(provider);
        self
    }

    #[must_use]
    pub const fn with_operation(mut self, op: OperationKind) -> Self {
        self.operation_kind = Some(op);
        self
    }
}

/// Evento metrico emesso dalla libreria verso il sink configurato.
///
/// # Redazione
///
/// Nessun campo dell'evento deve contenere: password, token, DSN, SQL
/// interpolato con dati, geometrie/blob, valori classificati. Solo:
/// identificativi opachi, tag categoriali, contatori/durate numerici.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricEvent {
    pub name: MetricName,
    pub value: MetricValue,
    #[serde(default, skip_serializing_if = "MetricTags::is_default")]
    pub tags: MetricTags,
}

impl MetricTags {
    #[must_use]
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

impl MetricEvent {
    #[must_use]
    pub const fn new(name: MetricName, value: MetricValue) -> Self {
        Self {
            name,
            value,
            tags: MetricTags::new(),
        }
    }

    #[must_use]
    pub fn with_tags(mut self, tags: MetricTags) -> Self {
        self.tags = tags;
        self
    }
}

/// Sink al quale la libreria consegna gli eventi metrici.
///
/// Implementato dal consumer (tracing subscriber, prometheus registry,
/// statsd client, aggregatore in-memory, ...). L'implementazione DEVE
/// essere non-bloccante e failure-tolerant: un errore del sink non deve
/// mai propagare nell'operazione DB che l'ha originato.
pub trait MetricsRecorder: Send + Sync {
    fn record(&self, event: MetricEvent);
}

/// Sink no-op. È il default quando il consumer non configura observability.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopRecorder;

impl MetricsRecorder for NoopRecorder {
    fn record(&self, _event: MetricEvent) {}
}

/// Sink aggregatore in-memory utile ai test unit: raccoglie tutti gli
/// eventi in un `Vec` accessibile via `snapshot()`.
#[derive(Debug, Default)]
pub struct CollectRecorder {
    inner: std::sync::Mutex<Vec<MetricEvent>>,
}

impl CollectRecorder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<MetricEvent> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn clear(&self) {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

impl MetricsRecorder for CollectRecorder {
    fn record(&self, event: MetricEvent) {
        let mut guard = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.push(event);
    }
}

/// Shortcut type per un recorder condiviso tra provider e consumer.
pub type SharedRecorder = Arc<dyn MetricsRecorder>;

/// Factory: crea un `SharedRecorder` no-op.
#[must_use]
pub fn noop_recorder() -> SharedRecorder {
    Arc::new(NoopRecorder)
}

#[cfg(test)]
#[path = "metrics_recorder_tests.rs"]
mod tests;
