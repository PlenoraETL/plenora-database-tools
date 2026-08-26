//! Provider Microsoft SQL Server basato direttamente sul protocollo TDS.
//!
//! Le fasi offline espongono configurazione, renderer e lifecycle della
//! connessione. Read/write/Spatial diventano capability pubbliche solo dopo la
//! relativa campagna su server reale.

#![forbid(unsafe_code)]

mod arrow;
mod catalog;
mod config;
mod connection;
pub(crate) mod error;
#[cfg(test)]
mod live_tests;
mod parameter;
mod pool;
mod provider;
mod query;
mod read;
mod recovery;
mod session;
mod transaction;
mod types;
mod write;

pub use catalog::{
    describe_object, list_objects, list_schemas, probe_server, SqlServerColumn,
    SqlServerConstraint, SqlServerExternalTableMetadata, SqlServerGraphKind, SqlServerIndex,
    SqlServerObjectDescription, SqlServerObjectName, SqlServerObjectSummary, SqlServerPartitioning,
    SqlServerPermission, SqlServerProbe, SqlServerSchemaToken, SqlServerSecurityPredicate,
    SqlServerSpatialBoundingBox, SqlServerSpatialIndex, SqlServerTemporalMetadata,
    SqlServerViewMetadata,
};
pub use config::{CertificatePolicy, SqlServerConfig};
pub use connection::SqlServerSession;
pub use pool::{PooledSqlServerSession, SqlServerPool};
pub use provider::SqlServerProvider;
pub use read::{read_object, SqlServerBatchStream};
pub use recovery::{RecoveryAction, RecoveryDecision, TransactionEvent, TransactionState};
pub use session::{SessionState, SESSION_BOOTSTRAP_SQL};
pub use types::{
    SqlServerColumnKind, SqlServerColumnSpec, SqlServerReadPlan, SqlServerWireEncoding,
};
pub use write::{
    prepare_write, prepare_write_with_mode, write_prepared, PreparedSqlServerWrite,
    SqlServerInsertMode, SqlServerSchemaEvolution,
};

/// Limite documentato dal provider per una singola richiesta.
pub const MAX_BIND_PARAMETERS: usize = 2_100;

/// Limite T-SQL per ogni parte di un identificatore regolare.
pub const MAX_IDENTIFIER_CHARACTERS: usize = 128;
