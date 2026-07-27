//! Contratti portabili di `plenora-database-tools`.
//!
//! Questo crate non contiene client, pool, runtime async o dialect SQL.

pub mod capabilities;
pub mod error;
pub mod geometry;
pub mod limits;
pub mod loss;
pub mod outcome;
pub mod plan;
pub mod provider;
pub mod query;
pub mod spatial_catalog;

pub use error::{DatabaseError, ErrorCategory, ErrorPhase, Result};

/// Unico punto di versione Arrow del workspace.
pub mod arrow {
    pub use arrow_array as array;
    pub use arrow_array::RecordBatch;
    pub use arrow_schema as schema;
    pub use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
}
