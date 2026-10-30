//! Provider IBM Db2 LUW attraverso il driver CLI/ODBC ufficiale.
//!
//! Il crate collega dinamicamente un driver manager ODBC. Il client IBM non
//! viene incorporato nell'artefatto Rust e resta un prerequisito runtime.

mod catalog;
mod config;
mod connection;
mod error;
mod operand_catalog;
mod parameter;
mod parameter_catalog;
mod provider;
mod read;
mod row_diagnostics;
mod row_visibility;
mod transaction;
mod types;
mod write;
mod write_admission;

pub use catalog::{Db2Column, Db2Index, Db2ObjectDescription, Db2ObjectSummary};
pub use config::{Db2Config, Db2TlsMode};
pub use provider::Db2Provider;
pub use types::{Db2ColumnKind, Db2ColumnSpec, Db2ReadPlan};

#[cfg(test)]
#[path = "config_tests.rs"]
mod config_tests;

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod catalog_tests;

#[cfg(test)]
#[path = "error_tests.rs"]
mod error_tests;

#[cfg(test)]
#[path = "operand_catalog_tests.rs"]
mod operand_catalog_tests;

#[cfg(test)]
#[path = "provider_tests.rs"]
mod provider_tests;

#[cfg(test)]
#[path = "types_tests.rs"]
mod types_tests;

#[cfg(test)]
#[path = "read_tests.rs"]
mod read_tests;

#[cfg(test)]
#[path = "transaction_tests.rs"]
mod transaction_tests;

#[cfg(test)]
#[path = "write_tests.rs"]
mod write_tests;

#[cfg(test)]
#[path = "live_tests.rs"]
mod live_tests;

#[cfg(test)]
#[path = "spatial_live_tests.rs"]
mod spatial_live_tests;

#[cfg(test)]
#[path = "write_rewrite_live_tests.rs"]
mod write_rewrite_live_tests;

#[cfg(test)]
#[path = "write_rewrite_ora_tests.rs"]
mod write_rewrite_ora_tests;

#[cfg(test)]
#[path = "write_admission_live_tests.rs"]
mod write_admission_live_tests;

#[cfg(test)]
#[path = "write_keys_collation_tests.rs"]
mod write_keys_collation_tests;

#[cfg(test)]
#[path = "operand_live_tests.rs"]
mod operand_live_tests;

#[cfg(test)]
#[path = "parameter_fidelity_live_tests.rs"]
mod parameter_fidelity_live_tests;

#[cfg(test)]
#[path = "parameter_catalog_tests.rs"]
mod parameter_catalog_tests;
