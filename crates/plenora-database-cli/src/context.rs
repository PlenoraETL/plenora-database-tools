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
use plenora_db_postgres::{PostgresProvider, PostgresTlsMode};

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
    /// Contesto per comandi PFM tradizionali: TLS **disabled** (compat
    /// con setup dev/staging pre-review #1), budget PFM default.
    ///
    /// # TLS default — decisione review #1
    ///
    /// Il default resta `Disabled` per non rompere consumer esistenti
    /// che passano DSN senza TLS material. La review raccomanda TLS
    /// obbligatorio per produzione: per quel caso usare
    /// [`Self::for_pfm_secure`] esplicitamente. Il consumer PFM può
    /// migrare gradualmente al secure senza cambiare il default.
    ///
    /// # Errors
    ///
    /// - `secret_from_env` fallisce se `dsn_env` non è settata.
    /// - `ResourceBudget::new` non fallisce per limiti default ma
    ///   propaghiamo l'errore per coerenza col trait.
    pub(crate) fn for_pfm(dsn_env: &str) -> CliResult<Self> {
        Ok(Self {
            secret: secret_from_env(dsn_env)?,
            provider: PostgresProvider::default().with_tls_mode(PostgresTlsMode::Disabled),
            budget: ResourceBudget::new(ResourceLimits::default()).map_err(CliError::from)?,
            cancel: CancellationToken::new(),
        })
    }

    /// Come `for_pfm` ma con TLS **required** (`PostgresTlsMode::Require`).
    /// Usare per produzione o quando è richiesto attestation cert.
    ///
    /// # Errors
    ///
    /// Vedi `for_pfm`.
    #[allow(dead_code)] // API pubblica per migrazione TLS review #6; consumer non ancora migrato
    pub(crate) fn for_pfm_secure(dsn_env: &str) -> CliResult<Self> {
        Ok(Self {
            secret: secret_from_env(dsn_env)?,
            // Fix review #6: usa `default_secure` (Require + WebPKI)
            // invece di mutare manualmente il default.
            provider: PostgresProvider::default_secure(),
            budget: ResourceBudget::new(ResourceLimits::default()).map_err(CliError::from)?,
            cancel: CancellationToken::new(),
        })
    }
}
