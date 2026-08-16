//! Safety flags globali: `--allow-write-tests`, `--ephemeral-schema NAME`.
//!
//! Sono opt-in per proteggere DB di produzione dai comandi che creano oggetti
//! temp o ordinari. Il flag `--allow-write-tests` è richiesto per:
//!   - `test-concurrency` (crea `_plenora_test_concurrency`)
//!   - `benchmark-write` (INSERT su temp table)
//!   - `benchmark-spatial` (temp table con GIST index)
//!
//! `--ephemeral-schema NAME` fa in modo che il session context contenga
//! `SET search_path = <name>` e che al termine dei test lo schema venga
//! droppato. Non è necessario per i test che usano solo `TEMP TABLE`
//! `ON COMMIT DROP`, ma è utile per audit.

use crate::pfm::{pfm_budget, postgres_provider_for_pfm};
use crate::{secret_from_env, CliResult};
use plenora_database_core::provider::{Provider, SecretString};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::CancellationToken;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone, Default)]
pub(crate) struct SafetyFlags {
    pub allow_write_tests: bool,
    pub ephemeral_schema: Option<String>,
}

static ACTIVE: OnceLock<Mutex<SafetyFlags>> = OnceLock::new();

fn store() -> &'static Mutex<SafetyFlags> {
    ACTIVE.get_or_init(|| Mutex::new(SafetyFlags::default()))
}

/// Estrae `--allow-write-tests` e `--ephemeral-schema NAME` dagli argomenti.
/// Imposta i flag attivi per la sessione e restituisce gli argomenti residui.
pub(crate) fn strip_safety_flags(args: Vec<String>) -> CliResult<Vec<String>> {
    let mut out = Vec::with_capacity(args.len());
    let mut iter = args.into_iter();
    let mut flags = SafetyFlags::default();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--allow-write-tests" => flags.allow_write_tests = true,
            "--ephemeral-schema" => {
                let name = iter
                    .next()
                    .ok_or("--ephemeral-schema richiede un nome schema")?;
                if !is_valid_identifier(&name) {
                    return Err(
                        "nome schema ephemeral non valido (usare [A-Za-z_][A-Za-z0-9_]{0,62})"
                            .into(),
                    );
                }
                flags.ephemeral_schema = Some(name);
            }
            _ => out.push(arg),
        }
    }
    *store().lock().expect("safety flags mutex poisoned") = flags;
    Ok(out)
}

pub(crate) fn active() -> SafetyFlags {
    store()
        .lock()
        .map_or_else(|_| SafetyFlags::default(), |g| g.clone())
}

/// Gate esplicito per comandi che creano oggetti persistenti sul DB target.
/// Restituisce errore se `--allow-write-tests` non è attivo.
pub(crate) fn require_write_tests(command: &str) -> CliResult<()> {
    if active().allow_write_tests {
        Ok(())
    } else {
        Err(format!(
            "{command} crea oggetti sul DB. Aggiungere --allow-write-tests \
             per acconsentire esplicitamente."
        )
        .into())
    }
}

/// Crea lo schema effimero se richiesto. È no-op quando `--ephemeral-schema`
/// non è impostato. Il chiamante deve poi chiamare `drop_ephemeral_schema`
/// al termine (best-effort).
pub(crate) async fn ensure_ephemeral_schema(dsn_env: &str) -> CliResult<Option<String>> {
    let Some(name) = active().ephemeral_schema else {
        return Ok(None);
    };
    let secret = secret_from_env(dsn_env)?;
    run_admin_stmt(&secret, &format!("CREATE SCHEMA IF NOT EXISTS \"{name}\"")).await?;
    Ok(Some(name))
}

pub(crate) async fn drop_ephemeral_schema(dsn_env: &str, name: &str) {
    if let Ok(secret) = secret_from_env(dsn_env) {
        let _ = run_admin_stmt(
            &secret,
            &format!("DROP SCHEMA IF EXISTS \"{name}\" CASCADE"),
        )
        .await;
    }
}

async fn run_admin_stmt(secret: &SecretString, sql: &str) -> CliResult<()> {
    let provider = postgres_provider_for_pfm();
    let budget = pfm_budget()?;
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), &budget, &cancel)
        .await?;
    tx.execute(&Statement::new(sql.to_owned()), &cancel).await?;
    tx.commit(&cancel).await?;
    Ok(())
}

fn is_valid_identifier(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }
    let mut chars = name.chars();
    let first = chars.next().expect("non-empty");
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // I test manipolano lo store globale; serializziamoli per non falsare le
    // assertion quando cargo test li esegue in parallelo.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn strip_extracts_allow_write_tests_flag() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let rest = strip_safety_flags(vec![
            "dsn".into(),
            "--allow-write-tests".into(),
            "iterations".into(),
        ])
        .expect("parse");
        assert_eq!(rest, vec!["dsn", "iterations"]);
        assert!(active().allow_write_tests);
        let _ = strip_safety_flags(vec![]);
    }

    #[test]
    fn strip_extracts_ephemeral_schema_name() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let rest = strip_safety_flags(vec![
            "--ephemeral-schema".into(),
            "probe_schema".into(),
            "dsn".into(),
        ])
        .expect("parse");
        assert_eq!(rest, vec!["dsn"]);
        assert_eq!(active().ephemeral_schema.as_deref(), Some("probe_schema"));
        let _ = strip_safety_flags(vec![]);
    }

    #[test]
    fn ephemeral_schema_requires_valid_identifier() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(strip_safety_flags(vec!["--ephemeral-schema".into(), "1invalid".into(),]).is_err());
        assert!(
            strip_safety_flags(vec!["--ephemeral-schema".into(), "has space".into(),]).is_err()
        );
        let _ = strip_safety_flags(vec![]);
    }

    #[test]
    fn require_write_tests_gates_correctly() {
        let _guard = TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _ = strip_safety_flags(vec![]);
        assert!(require_write_tests("cmd").is_err());
        let _ = strip_safety_flags(vec!["--allow-write-tests".into()]);
        assert!(require_write_tests("cmd").is_ok());
        let _ = strip_safety_flags(vec![]);
    }
}
