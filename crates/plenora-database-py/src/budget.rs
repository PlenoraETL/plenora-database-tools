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
/// `ResourceBudget::new` rifiuta solo limiti nulli; il preset li ha tutti
/// positivi, e `budget_tests` costruisce entrambi i preset.
#[must_use]
#[allow(
    clippy::expect_used,
    reason = "preset costante con limiti positivi: il rifiuto non e costruibile, e budget_tests lo prova"
)]
pub fn session_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("session default budget")
}

/// Budget per bulk write (COPY / staging replace). Preset generoso
/// allineato al CLI `write-arrow`.
///
/// Vedi `session_budget` per l'infallibilita del preset.
#[must_use]
#[allow(
    clippy::expect_used,
    reason = "preset costante con limiti positivi: il rifiuto non e costruibile, e budget_tests lo prova"
)]
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

#[cfg(test)]
#[path = "budget_tests.rs"]
mod budget_tests;
