//! Contratti portabili di `plenora-database-tools`.
//!
//! Questo crate non contiene client, pool, runtime async o dialect SQL.

pub mod cancellation;
pub mod capabilities;
pub mod conformance;
pub mod error;
pub mod ewkb;
pub mod facade;
pub mod field_contract;
pub mod geometry;
pub mod limits;
pub mod loss;
pub mod metrics_recorder;
pub mod native_query_policy;
pub mod outcome;
pub mod plan;
pub mod portable;
pub mod protocol;
pub mod provider;
pub mod query;
pub mod resource;
pub mod row;
pub mod row_diagnostics;
pub mod session_context;
pub mod spatial_catalog;
pub mod spatial_predicate;
pub mod transaction;

pub use cancellation::{CancellationReason, CancellationToken};
pub use conformance::{
    check_profile, probe_application_oltp_v1, probe_pfm_core_v1, probe_pfm_gis_v1, Capability,
    CapabilityEvidence, ConformanceProfile, EvidenceKind, ProfileReport, ProfileStatus,
    APPLICATION_OLTP_V1, PFM_CORE_V1, PFM_GIS_V1,
};
pub use row::Row;
pub use facade::{
    execute_portable, execute_portable_returning, execute_portable_returning_one,
    execute_scalar_bool, execute_scalar_bytes, execute_scalar_date, execute_scalar_decimal,
    execute_scalar_f64, execute_scalar_i32, execute_scalar_i64, execute_scalar_json,
    execute_scalar_string, execute_scalar_timestamp, execute_scalar_timestamptz,
    execute_scalar_uuid, query_one, query_optional,
};
pub use metrics_recorder::{
    noop_recorder, CollectRecorder, MetricEvent, MetricName, MetricTags, MetricValue,
    MetricsRecorder, NoopRecorder, OperationKind, SharedRecorder,
};
pub use native_query_policy::{enforce_policy, NativeQueryPolicy};
pub use portable::{
    and, compile_portable, eq, or, select, select_all, spatial, DeleteStatement, Direction,
    Expression, InsertStatement, Nulls, OrderBy, PortableStatement, Predicate, Projection,
    SelectStatement, TableRef, UpdateStatement, UpsertStatement,
};
pub use spatial_predicate::{SpatialFilter, SpatialPredicate, SpatialReference};
pub use error::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition};
pub use resource::{ResourceBudget, ResourceKind, ResourceLease, ResourceLimits};
pub use session_context::{
    validate_context_key, validate_context_value, SessionClassification, SessionContext,
    SessionEntry, SessionValue,
};
pub use transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name, AccessMode,
    CommitOutcome, ConditionalUpdate, IsolationLevel, RowStream, Statement, TransactionOptions,
    TransactionScope,
};
pub use row_diagnostics::{
    diagnose_row_scoped_write, into_read_rejection, ReadDiagnosticsPolicy, ReadDiagnosticsTracker,
    RejectedRow, RollbackEvidence, RowApplication, RowDiagnostics, RowDiagnosticsPolicy,
    RowRejection, RowRejectionOutcome, RowScopedWriter, WriteDiagnosticsTracker,
};

/// Unico punto di versione Arrow del workspace.
pub mod arrow {
    pub use arrow_array as array;
    pub use arrow_array::RecordBatch;
    pub use arrow_schema as schema;
    pub use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
}
