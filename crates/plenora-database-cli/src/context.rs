//! `PostgresCommandContext` — factory unica per l'ambiente d'esecuzione
//! di un sottocomando Postgres (secret, provider, budget, cancel).
//!
//! Prima di Fase C, ~30 call sites nel CLI ripetevano la sequenza:
//!
//! ```ignore
//! let secret = secret_from_env(&dsn_env)?;
//! let provider = postgres_provider_for_pfm();
//! let budget = pfm_budget()?;
//! let cancel = CancellationToken::new();
//! ```
//!
//! Rischi della duplicazione:
//! - default TLS `Disabled` sparso invece che centralizzato — se
//!   cambia default (review #1) va toccato in molti punti;
//! - budget e cancel scelti indipendentemente — un comando può usare
//!   limiti diversi senza motivo apparente;
//! - factory provider ripetuta con lievi variazioni (`PostgresProvider::default()`
//!   vs `postgres_provider_for_pfm()` vs `secure_pfm_probe_provider()`);
//! - test coverage si moltiplica invece che essere concentrata.
//!
//! Ora un tipo unico con costruttori nominali per modalità supportate.
//! **La migrazione è graduale**: le vecchie helper (`secret_from_env`,
//! `postgres_provider_for_pfm`, `pfm_budget`) restano come thin wrapper
//! e vengono progressivamente sostituite.

use crate::{secret_from_env, CliError, CliResult};
use plenora_database_core::provider::SecretString;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::CancellationToken;
use plenora_db_postgres::PostgresProvider;

/// Ambiente d'esecuzione di un sottocomando Postgres CLI.
///
/// Contiene tutte le dipendenze provider-side che i sottocomandi
/// PFM/ops/testing/diagnose reclamano. I costruttori nominali
/// documentano la modalità (TLS on/off, budget preset, ecc.).
pub(crate) struct PostgresCommandContext {
    pub secret: SecretString,
    pub provider: PostgresProvider,
    pub budget: ResourceBudget,
    pub cancel: CancellationToken,
}

impl PostgresCommandContext {
    /// Contesto per comandi PFM: TLS `Require` + `WebPKI` trust store
    /// (secure-by-default). ADR-011.
    ///
    /// Per test/dev locali contro Docker senza TLS usare
    /// [`Self::for_pfm_insecure_local`].
    ///
    /// # Errors
    ///
    /// - `secret_from_env` fallisce se `dsn_env` non è settata.
    /// - `ResourceBudget::new` non fallisce per limiti default ma
    ///   propaghiamo l'errore per coerenza col trait.
    pub(crate) fn for_pfm(dsn_env: &str) -> CliResult<Self> {
        Ok(Self {
            secret: secret_from_env(dsn_env)?,
            provider: PostgresProvider::default(),
            budget: ResourceBudget::new(ResourceLimits::default()).map_err(CliError::from)?,
            cancel: CancellationToken::new(),
        })
    }

    /// Contesto senza TLS per test/dev locali (`insecure_local()`).
    /// ADR-011: nome esplicito per non nascondere il rischio dietro
    /// un flag booleano opaco.
    ///
    /// # Errors
    ///
    /// Vedi `for_pfm`.
    #[allow(dead_code)] // usato da consumer dev/test, non da tutti i sub-command
    pub(crate) fn for_pfm_insecure_local(dsn_env: &str) -> CliResult<Self> {
        Ok(Self {
            secret: secret_from_env(dsn_env)?,
            provider: PostgresProvider::insecure_local(),
            budget: ResourceBudget::new(ResourceLimits::default()).map_err(CliError::from)?,
            cancel: CancellationToken::new(),
        })
    }
}
