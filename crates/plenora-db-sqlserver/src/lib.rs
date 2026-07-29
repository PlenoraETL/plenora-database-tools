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
mod pool;
mod read;
mod recovery;
mod session;
mod types;
mod write;

pub use catalog::{
    describe_object, list_objects, list_schemas, probe_server, SqlServerColumn,
    SqlServerConstraint, SqlServerIndex, SqlServerObjectDescription, SqlServerObjectSummary,
    SqlServerProbe, SqlServerSchemaToken,
};
pub use config::{CertificatePolicy, SqlServerConfig};
pub use connection::SqlServerSession;
pub use pool::{PooledSqlServerSession, SqlServerPool};
pub use read::{read_object, SqlServerBatchStream};
pub use recovery::{RecoveryAction, RecoveryDecision, TransactionEvent, TransactionState};
pub use session::{SessionState, SESSION_BOOTSTRAP_SQL};
pub use types::{SqlServerColumnKind, SqlServerColumnSpec, SqlServerReadPlan};
pub use write::{prepare_write, write_prepared, PreparedSqlServerWrite};

/// Limite documentato dal provider per una singola richiesta.
pub const MAX_BIND_PARAMETERS: usize = 2_100;

/// Limite T-SQL per ogni parte di un identificatore regolare.
pub const MAX_IDENTIFIER_CHARACTERS: usize = 128;
