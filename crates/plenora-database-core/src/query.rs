//! Facciata di compatibilita per il precedente nome del modulo query.
//!
//! Il modello canonico vive in [`crate::relational`]. Questo re-export mantiene
//! compatibili i consumer v2; nuovo codice deve importare `relational`.

pub use crate::relational::*;
