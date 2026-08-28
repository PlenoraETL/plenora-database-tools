//! La compilazione di un piano portabile, che non ha bisogno di un database.
//!
//! Il comando e puro e non dipende dalle feature dei provider: legge un AST,
//! lo compila per il dialetto richiesto e stampa l'SQL.

use crate::{ensure_end, print_json, CliResult};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::portable::{compile_portable, PortableStatement};
use serde_json::json;
use std::fs;

/// I dialetti che questo comando compila, con il nome che accetta dalla riga
/// di comando.
///
/// L'elenco e la sola fonte: il messaggio d'errore lo legge da qui invece di
/// ripeterlo a mano, cosi non puo nominare un insieme diverso da quello che il
/// match accetta.
///
/// Un elenco piu corto del vero nasconde; uno piu lungo promette. La prova qui
/// sotto chiude tutte e due, perche attraversa ogni nome compilando un piano.
const DIALECTS: &[(&str, ProviderKind)] = &[
    ("postgres", ProviderKind::Postgres),
    ("mysql", ProviderKind::Mysql),
    ("mariadb", ProviderKind::Mariadb),
    ("sqlserver", ProviderKind::Sqlserver),
    ("db2", ProviderKind::Db2),
];

/// `portable-compile <dialetto> PORTABLE.json`: compila e stampa l'SQL.
///
/// # Errors
///
/// Se il dialetto non e riconosciuto, se il file non e leggibile o non e un
/// AST valido, o se il compilatore rifiuta il piano per quel dialetto.
pub(crate) fn portable_compile(args: &mut impl Iterator<Item = String>) -> CliResult<()> {
    let names = DIALECTS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join("|");
    let provider = args
        .next()
        .ok_or_else(|| format!("manca il provider ({names})"))?;
    let portable_path = args.next().ok_or("manca il percorso PORTABLE.json")?;
    ensure_end(args)?;

    let kind = DIALECTS
        .iter()
        .find(|(name, _)| *name == provider)
        .map(|(_, kind)| *kind)
        .ok_or_else(|| format!("provider sconosciuto: ammessi {names}"))?;

    let contents = fs::read(&portable_path)
        .map_err(|_| format!("PORTABLE.json non leggibile: {portable_path}"))?;
    let ast: PortableStatement = serde_json::from_slice(&contents).map_err(|e| {
        format!(
            "PORTABLE.json non parsabile a riga {}, colonna {}",
            e.line(),
            e.column()
        )
    })?;
    let compiled = compile_portable(kind, &ast)?;

    print_json(&json!({
        "status": "ok",
        "provider": kind,
        "sql": compiled.sql,
        "param_count": compiled.params.len(),
        // Non stampiamo i params: possono contenere valori sensibili.
    }))
}

#[cfg(test)]
#[path = "portable_cmd_tests.rs"]
mod tests;
