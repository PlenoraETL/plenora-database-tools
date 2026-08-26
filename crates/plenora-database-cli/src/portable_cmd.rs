//! La compilazione di un piano portabile, che non ha bisogno di un database.
//!
//! # Perche un modulo suo
//!
//! `portable-compile` stava in `query_cmd`, che e compilato soltanto con la
//! feature `postgres`. Il comando pero non apre nessuna connessione: legge un
//! AST da un file, lo compila per il dialetto che gli si chiede, e stampa
//! l'SQL. E' puro.
//!
//! L'effetto era che un binario costruito con `--features mysql` non poteva
//! compilare un piano portabile **per `MySQL`**, e chi lo cercava trovava
//! «comando non compilato in questo binario» — una risposta vera e inutile,
//! perche il comando non aveva niente a che vedere con il provider assente.
//!
//! Qui non e gated, e resta puro: se un giorno gli servisse una connessione,
//! il posto giusto non sarebbe piu questo.

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
/// Prima ne offriva tre, e sbagliava in **entrambe** le direzioni. `mariadb`
/// mancava benche il compilatore la conoscesse, quindi un dialetto reale era
/// irraggiungibile dal CLI. E `sqlserver` c'era benche il compilatore lo
/// rifiuti, quindi l'aiuto proponeva un nome che rispondeva «non supportato».
///
/// Un elenco piu corto del vero nasconde; uno piu lungo promette. La prova qui
/// sotto chiude tutte e due, perche attraversa ogni nome compilando un piano.
const DIALECTS: &[(&str, ProviderKind)] = &[
    ("postgres", ProviderKind::Postgres),
    ("mysql", ProviderKind::Mysql),
    ("mariadb", ProviderKind::Mariadb),
    // `sqlserver` non c'e, e ci **era**: il CLI lo offriva da sempre, e
    // `compile_portable` rispondeva «non supportato per Sqlserver». Una
    // promessa che il compilatore non poteva mantenere, scritta anche
    // nell'aiuto del comando.
    //
    // Il giorno in cui il dialetto T-SQL entrera nel compilatore, la riga
    // torna qui — e la prova qui sotto se ne accorgera da sola, perche
    // attraversa ogni nome di questo elenco compilando un piano vero.
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
mod tests {
    use super::{portable_compile, DIALECTS};
    use plenora_database_core::portable::{compile_portable, PortableStatement};

    /// Ogni dialetto offerto dal comando viene davvero compilato.
    ///
    /// Il comando ne offriva quattro e il compilatore ne compila tre: chi
    /// chiedeva `sqlserver` riceveva «`compile_portable` non supportato», dopo
    /// che l'aiuto glielo aveva proposto. Un elenco di nomi accettati e una
    /// promessa, e questa prova la attraversa nome per nome invece di
    /// confrontarla con un secondo elenco scritto qui — due elenchi
    /// divergono, un elenco attraversato no.
    #[test]
    fn every_offered_dialect_compiles_a_plan() {
        let ast: PortableStatement = serde_json::from_str(
            r#"{"type":"select","table":{"name":"t"},
                "projection":{"kind":"columns","value":["a"]},
                "filter":null,"order_by":[],"limit":1}"#,
        )
        .expect("piano di prova");
        assert!(!DIALECTS.is_empty(), "nessun dialetto offerto");
        for (name, kind) in DIALECTS {
            let compiled = compile_portable(*kind, &ast);
            assert!(
                compiled.is_ok(),
                "il comando offre `{name}` ma il compilatore lo rifiuta: {:?}",
                compiled.err().map(|error| error.message),
            );
        }
    }

    /// Un dialetto sconosciuto viene rifiutato nominando quelli che ci sono.
    #[test]
    fn an_unknown_dialect_names_the_ones_that_exist() {
        let mut args = ["fantasia".to_owned(), "piano.json".to_owned()].into_iter();
        let error = portable_compile(&mut args).expect_err("dialetto inventato");
        // `CliError` non e `Display`: il messaggio sta dentro la variante
        // fatale, che e la sola che questo percorso puo produrre.
        let crate::CliError::Fatal(fatal) = error else {
            panic!("un dialetto sconosciuto non e un fallimento silenzioso");
        };
        let message = fatal.message;
        for (name, _) in DIALECTS {
            assert!(
                message.contains(name),
                "il rifiuto non nomina `{name}`: {message}"
            );
        }
    }
}
