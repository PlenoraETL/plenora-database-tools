//! Preset di `ResourceBudget` condivisi tra sessioni e bulk write del binding.
//!
//! Centralizzarli impedisce che i percorsi sync e async applichino limiti
//! diversi alla stessa operazione.

use plenora_database_core::resource::{ResourceBudget, ResourceLimits};

/// Budget per sessioni interattive / query non bulk.
///
/// Usa `ResourceLimits::default()` che è calibrato per il consumer
/// tipico PFM (~100k rows, ~10 MiB payload). Bulk write deve usare
/// `write_bulk_budget` esplicitamente.
///
/// # Panics
///
/// `ResourceBudget::new(ResourceLimits::default())` è sicuro perché il preset
/// usa limiti positivi.
#[must_use]
pub fn session_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("session default budget")
}

/// Budget per bulk write (COPY / staging replace). Preset generoso
/// allineato al CLI `write-arrow`.
///
/// # Panics
///
/// Vedi `session_budget`.
#[must_use]
pub fn write_bulk_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits {
        rows: 10_000_000,
        memory_bytes: 128 * 1024 * 1024,
        output_bytes: 128 * 1024 * 1024,
        cell_bytes: 4 * 1024 * 1024,
        ..ResourceLimits::default()
    })
    .expect("write bulk budget")
}
