//! Lo schema di `PostGIS`, letto da `pg_extension`, per la regola Q: lo SQL
//! della libreria nomina funzioni, tipi e relazioni di `PostGIS`
//! (`spatial_ref_sys`, `postgis_typmod_srid`, `ST_GeomFromEWKB`, `geography`)
//! qualificati con lo schema in cui l'estensione e installata, non risolti
//! dal `search_path` della sessione.
//!
//! Fino al 25/10 `spatial_ref_sys` e le funzioni `postgis_typmod_*` erano
//! nominati senza schema: un `search_path` con uno schema prima di quello di
//! `PostGIS` (anche lo schema `"$user"` del ruolo, che precede `public` nel
//! default) poteva sostituirli.
//!
//! # Limite dichiarato: le letture spatial
//!
//! - **Regola**: i nomi di `PostGIS` dello SQL della libreria si qualificano
//!   con lo schema dell'estensione.
//! - **Ambito**: scrittura (segnaposto, tipo delle colonne create), preflight,
//!   introspezione delle colonne, SRID ereditati delle query.
//! - **Hazard**: le letture con predicati spatial (`spatial`, `spatial_probe`,
//!   fuori da questo ciclo) e le proiezioni `ST_AsEWKB` delle letture
//!   (`types.rs`) nominano ancora le funzioni senza schema: uno schema prima
//!   di quello di `PostGIS` nel `search_path` con una `st_asewkb` propria
//!   cambierebbe cio che la lettura restituisce. La guardia del `search_path`
//!   non lo vede, perche `PostGIS` non sta in `pg_catalog`.
//! - **Rientro**: il prefisso di [`qualifier`] anche nel piano delle letture.

use crate::error::classify_error;
use plenora_database_core::{ErrorPhase, Result};
use tokio_postgres::GenericClient;

const POSTGIS_SCHEMA_SQL: &str = "SELECT pg_catalog.quote_ident(n.nspname) \
     FROM pg_catalog.pg_extension e \
     JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace \
     WHERE e.extname = 'postgis'";

/// I nomi di `PostGIS` che lo SQL della libreria usa e che [`qualify`]
/// qualifica: un elenco chiuso, controllato dalla prova della regola Q.
const POSTGIS_NAMES: [&str; 6] = [
    "spatial_ref_sys",
    "postgis_typmod_srid",
    "postgis_typmod_dims",
    "postgis_typmod_type",
    "ST_GeomFromEWKB",
    "ST_AsEWKB",
];

/// Il prefisso con cui qualificare i nomi di `PostGIS` (`"public".`), vuoto
/// se l'estensione non c'e: lo SQL che li nomina fallisce allora come prima,
/// con l'errore del server.
///
/// # Errors
///
/// L'errore classificato della lettura, in `phase`.
pub async fn qualifier<C: GenericClient + Sync>(client: &C, phase: ErrorPhase) -> Result<String> {
    let row = client
        .query_opt(POSTGIS_SCHEMA_SQL, &[])
        .await
        .map_err(|error| classify_error(phase, &error))?;
    Ok(row.map_or_else(String::new, |row| format!("{}.", row.get::<_, String>(0))))
}

/// `sql` con i nomi di [`POSTGIS_NAMES`] qualificati da `qualifier`, e i tipi
/// `geometry`/`geography` dopo `::`.
///
/// Si applica soltanto a SQL della libreria, dove questi nomi compaiono come
/// riferimenti all'estensione e mai dentro identificatori quotati o
/// letterali.
#[must_use]
pub fn qualify(sql: &str, qualifier: &str) -> String {
    if qualifier.is_empty() {
        return sql.to_owned();
    }
    let mut out = String::with_capacity(sql.len() + 64);
    let bytes = sql.as_bytes();
    let mut index = 0;
    'scan: while index < sql.len() {
        let boundary = index == 0 || {
            let previous = bytes[index - 1];
            !(previous.is_ascii_alphanumeric() || previous == b'_' || previous == b'.')
        };
        if boundary {
            for name in POSTGIS_NAMES {
                let end = index + name.len();
                let follows = bytes
                    .get(end)
                    .is_none_or(|next| !(next.is_ascii_alphanumeric() || *next == b'_'));
                if sql.get(index..end) == Some(name) && follows {
                    out.push_str(qualifier);
                    out.push_str(name);
                    index = end;
                    continue 'scan;
                }
            }
        }
        if sql[index..].starts_with("::") {
            for name in ["geometry", "geography"] {
                let start = index + 2;
                let end = start + name.len();
                let follows = bytes
                    .get(end)
                    .is_none_or(|next| !(next.is_ascii_alphanumeric() || *next == b'_'));
                if sql.get(start..end) == Some(name) && follows {
                    out.push_str("::");
                    out.push_str(qualifier);
                    out.push_str(name);
                    index = end;
                    continue 'scan;
                }
            }
        }
        let character = sql[index..].chars().next().unwrap_or_default();
        out.push(character);
        index += character.len_utf8().max(1);
    }
    out
}

#[cfg(test)]
#[path = "postgis_schema_tests.rs"]
mod tests;
