//! Provider `MySQL` basato sul protocollo binario nativo.
//!
//! Il riferimento non e scritto qui: lo fissa `docker/mysql/references.json`
//! per digest, ed e il gate a verificare che il server misurato sia quello.
//!
//! Le capability restano fail-closed fino alla rispettiva prova live; in
//! particolare il DDL atomico di `MySQL` non viene dichiarato come DDL
//! transazionale.

#![forbid(unsafe_code)]

mod arrow;
mod catalog;
mod config;
mod error;
mod parameter;
mod pool;
mod profile;
mod provider;
mod query;
mod read;
mod row_diagnostics;
mod session;
mod transaction;
mod types;
mod write;

#[cfg(test)]
mod live_tests;

// Le misure comparative MariaDB esistono solo nei test; il binario pubblico
// non contiene l'harness ne i relativi bypass di identificazione.
#[cfg(test)]
mod evidence;

#[cfg(test)]
mod mariadb_evidence;

#[cfg(test)]
mod session_evidence;

pub use arrow::MysqlColumnBuffer;
pub use catalog::{
    describe_object, list_objects, list_schemas, probe_server, MysqlColumn, MysqlIndex,
    MysqlObjectDescription, MysqlObjectSummary, MysqlProbe, MysqlSchemaToken,
};
pub use config::{MysqlCertificatePolicy, MysqlConfig};
pub use parameter::bind_parameters;
pub use pool::MysqlPool;
pub use provider::{MariadbProvider, MysqlProvider};
pub use query::{query_result_columns, render_query};
pub use read::{
    query_operation, read_operation, MysqlBatchStream, DEFAULT_BATCH_ROWS, MAX_BATCH_ROWS,
};
pub use session::{MysqlSession, MysqlSessionState, SESSION_BOOTSTRAP_SQL};
pub use types::{MysqlColumnKind, MysqlColumnSpec, MysqlReadPlan};

/// Limite `MySQL` del numero di placeholder in un prepared statement.
pub const MAX_BIND_PARAMETERS: usize = u16::MAX as usize;

/// Limite `MySQL` per ogni componente di un identificatore.
pub const MAX_IDENTIFIER_CHARACTERS: usize = 64;
