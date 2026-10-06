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
/// `ResourceBudget::new` rifiuta tre cose: un limite nullo, `cell_bytes`
/// oltre `memory_bytes` e una deadline (`now + duration_ms`) che non sta in
/// un `Instant`. I preset hanno tutti i limiti positivi, `cell_bytes` (64 e
/// 4 MiB) sotto `memory_bytes` (512 e 128 MiB) e `duration_ms` di 30 s; i
/// valori sono costanti, e `budget_tests` costruisce entrambi i preset e
/// verifica le tre condizioni.
#[must_use]
#[allow(
    clippy::expect_used,
    reason = "preset costante: limiti positivi, cell_bytes <= memory_bytes, deadline di 30 s rappresentabile; budget_tests lo prova"
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
    reason = "preset costante: limiti positivi, cell_bytes <= memory_bytes, deadline di 30 s rappresentabile; budget_tests lo prova"
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
