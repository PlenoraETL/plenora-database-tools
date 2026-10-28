//! Provider Oracle basato sul protocollo thin asincrono.
//!
//! Non richiede Oracle Instant Client. Ogni capability pubblica e aperta solo
//! dalla relativa prova live del repository; quelle non misurate restano chiuse.

// Il driver deve essere il fork in `vendor/oracle-rs`: il marcatore esiste
// solo li. Se un grafo risolvesse `oracle_rs` sull'upstream, qui la
// compilazione si ferma invece di consegnare un driver che non esegue la
// rinegoziazione TCPS.
const _: &str = oracle_rs::PLENORA_FORK;

mod catalog;
mod config;
mod connection;
mod decode;
mod error;
mod operand_catalog;
mod parameter;
mod pool;
mod provider;
mod read;
mod row_visibility;
mod transaction;
mod types;
mod write;
mod write_admission;
mod write_intermediate;

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
#[path = "operand_catalog_tests.rs"]
mod operand_catalog_tests;

#[cfg(test)]
#[path = "live_tests.rs"]
mod live_tests;

#[cfg(test)]
#[path = "write_rewrite_live_tests.rs"]
mod write_rewrite_live_tests;

#[cfg(test)]
#[path = "write_admission_live_tests.rs"]
mod write_admission_live_tests;

#[cfg(test)]
#[path = "number_oracle_tests.rs"]
mod number_oracle_tests;

#[cfg(test)]
#[path = "number_live_tests.rs"]
mod number_live_tests;

#[cfg(test)]
#[path = "driver_fidelity_live_tests.rs"]
mod driver_fidelity_live_tests;

#[cfg(test)]
#[path = "driver_codec_oracle_tests.rs"]
mod driver_codec_oracle_tests;
