//! Contratti portabili di `plenora-database-tools`.
//!
//! Questo crate non contiene client, pool, runtime async o dialect SQL.

pub mod cancellation;
pub mod capabilities;
pub mod error;
pub mod ewkb;
pub mod field_contract;
pub mod geometry;
pub mod limits;
pub mod loss;
pub mod outcome;
pub mod plan;
pub mod protocol;
pub mod provider;
pub mod query;
pub mod resource;
pub mod row_diagnostics;
pub mod spatial_catalog;

pub use cancellation::{CancellationReason, CancellationToken};
pub use error::{DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition};
pub use resource::{ResourceBudget, ResourceKind, ResourceLease, ResourceLimits};
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
