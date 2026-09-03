//! Provider Oracle basato sul protocollo thin asincrono.
//!
//! Non richiede Oracle Instant Client. Ogni capability pubblica e aperta solo
//! dalla relativa prova live del repository; quelle non misurate restano chiuse.

mod catalog;
mod config;
mod connection;
mod decode;
mod error;
mod parameter;
mod pool;
mod provider;
mod read;
mod transaction;
mod types;
mod write;

pub use catalog::{OracleColumn, OracleIndex, OracleObjectDescription, OracleObjectSummary};
pub use config::{OracleConfig, OracleTlsMode};
pub use pool::{OraclePool, PooledOracleConnection};
pub use provider::OracleProvider;
pub use types::{OracleColumnKind, OracleColumnSpec, OracleReadPlan};

#[cfg(test)]
#[path = "config_tests.rs"]
mod config_tests;

#[cfg(test)]
#[path = "error_tests.rs"]
mod error_tests;

#[cfg(test)]
#[path = "parameter_tests.rs"]
mod parameter_tests;

#[cfg(test)]
#[path = "types_tests.rs"]
mod types_tests;

#[cfg(test)]
#[path = "live_tests.rs"]
mod live_tests;
