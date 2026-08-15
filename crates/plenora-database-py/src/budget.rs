//! Preset di `ResourceBudget` condivisi tra i moduli SDK.
//!
//! Prima di Fase E, `default_budget()` era ridefinito 5 volte identico
//! (`session`/`async_session`/`mysql_session`/`async_mysql_session`/
//! `arrow_reader`) più una variante custom in `write.rs` per bulk write.
//! Il rischio era che i 5 identici divergessero silenziosamente al
//! prossimo tuning dei limiti default.
//!
//! Ora due preset nominati:
//! - [`session_budget`]: default lightweight per session/query — usa
//!   i limiti di `ResourceLimits::default()`.
//! - [`write_bulk_budget`]: generoso per bulk write via COPY — 10M
//!   righe, 128 MiB memoria/output, cell 4 MiB. Allineato al preset
//!   che il CLI usa per `write-arrow`.

use plenora_database_core::resource::{ResourceBudget, ResourceLimits};

/// Budget per sessioni interattive / query non bulk.
///
/// Usa `ResourceLimits::default()` che è calibrato per il consumer
/// tipico PFM (~100k rows, ~10 MiB payload). Bulk write deve usare
/// `write_bulk_budget` esplicitamente.
///
/// # Panics
///
/// `ResourceBudget::new(ResourceLimits::default())` non fallisce mai
/// nelle build attuali (limiti hardcoded > 0), ma il costruttore
/// ritorna `Result` per estensione futura.
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
