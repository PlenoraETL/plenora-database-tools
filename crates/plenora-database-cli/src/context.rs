//! `PostgresCommandContext` — factory unica per l'ambiente d'esecuzione
//! di un sottocomando Postgres (secret, provider, budget, cancel).
//!
//! Centralizza secret, provider, budget e cancellazione perche ogni comando
//! applichi gli stessi default e le stesse policy operative.

use crate::{secret_from_env, CliError, CliResult};
use plenora_database_core::provider::SecretString;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::TransactionOptions;
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
    /// Contesto per comandi PFM: TLS `Require`, con radice di fiducia `WebPKI`
    /// oppure la CA privata indicata dalle variabili di
    /// [`crate::pfm::postgres_provider_for_pfm`]. ADR-011.
    ///
    /// Il provider arriva da quel factory e non da un `default()` locale:
    /// due sorgenti dello stesso provider divergono, e la divergenza si
    /// manifesta come un sottocomando che non riesce a connettersi mentre un
    /// altro ci riesce.
    ///
    /// # Errors
    ///
    /// - `secret_from_env` fallisce se `dsn_env` non è settata.
    /// - il materiale TLS indicato dalle variabili e illeggibile o non valido.
    /// - `ResourceBudget::new` non fallisce per limiti default ma
    ///   propaghiamo l'errore per coerenza col trait.
    pub(crate) fn for_pfm(dsn_env: &str) -> CliResult<Self> {
        Ok(Self {
            secret: secret_from_env(dsn_env)?,
            provider: crate::pfm::postgres_provider_for_pfm()?,
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

    /// Opzioni PFM baseline: `native_query_policy: Deny` (CHG-003).
    /// I comandi high-level (`transaction-test`, `execute-sql`) partono
    /// da qui. Aggiungere `.native_query_policy = Allow` esplicitamente
    /// solo per `execute-sql` con `--allow-raw` (opt-in dichiarato).
    #[must_use]
    #[allow(clippy::unused_self)] // mantiene la costruzione legata al contesto del comando
    pub(crate) fn pfm_transaction_options(&self) -> TransactionOptions {
        TransactionOptions {
            context: crate::session_ctx::active(),
            ..TransactionOptions::pfm_defaults()
        }
    }
}
