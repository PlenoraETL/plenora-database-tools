//! La regola Q2 su PostgreSQL: lo SQL della libreria nella transazione della
//! scrittura e nel dry run gira con `search_path = pg_catalog, pg_temp`.
//!
//! Fino al 25/10 la regola Q qualificava funzioni, relazioni e tipi di
//! `pg_catalog`, e una guardia rifiutava gli schemi **prima** di `pg_catalog`
//! con nomi omonimi. Restava aperto un operatore raggiunto con una
//! conversione implicita: con `CREATE OPERATOR public.<> (LEFTARG = oid,
//! RIGHTARG = integer, ...)` che risponde `false`, il confronto `y.typelem <>
//! 0` della regola V trovava in `public` la corrispondenza esatta, che
//! `pg_catalog` (`oid <> oid`) non ha, e saltava il ramo degli array: con
//! `DateStyle = DMY` `['01/02/2026']` verso `date[]` si committava come
//! `{2026-02-01}`. La guardia non lo vedeva, perche `public` sta dopo
//! `pg_catalog`.
//!
//! # La regola
//!
//! La transazione della scrittura chiude il percorso appena aperta
//! ([`SessionPath::close`], `set_config(..., true)`: locale alla
//! transazione, un `ROLLBACK` o un `COMMIT` lo tolgono). Da li ogni lettura
//! del catalogo, le guardie, la regola W2, le regole V e K, la regola R
//! dopo ogni lock, la registrazione degli id dei campi risolvono operatori,
//! funzioni e tipi soltanto in `pg_catalog`: nessuno schema li puo oscurare,
//! con o senza conversioni implicite. `pg_temp` in coda non vale per
//! funzioni e operatori, e per le relazioni viene dopo `pg_catalog`.
//!
//! Gli statement sul target (DDL di Create e dell'evoluzione, `DELETE`,
//! `TRUNCATE`, INSERT, UPDATE, DELETE preparati, `COPY`, `CREATE INDEX`)
//! girano invece con il `search_path` della sessione rimesso al suo posto
//! ([`SessionPath::on_target`]), perche trigger, funzioni e default del
//! target (codice del target, non della libreria) risolvono i loro nomi a
//! ogni chiamata con il percorso in vigore: chiuderlo farebbe fallire un
//! trigger che scrive in `audit_log` senza schema, e cambierebbe in silenzio
//! un confronto `citext` di un trigger (`citext = citext` sta in `public`).
//! Il testo di questi statement non dipende dal percorso: target qualificato
//! con lo schema (anche quando il piano non lo nomina, `public`), tipi dei
//! segnaposto e delle colonne create qualificati, confronto delle chiavi con
//! l'operatore esplicito dell'indice che prova l'unicita (regola K2). La
//! prova statica e in `sql_qualification_tests`.
//!
//! Il dry run con la DSN legge cio che la scrittura leggerebbe in una
//! transazione che si annulla, con il percorso chiuso nello stesso modo.
//!
//! # Limite dichiarato
//!
//! - **Regola**: lo SQL della libreria non dipende dal `search_path`.
//! - **Ambito**: transazione della scrittura e dry run. Restano con il
//!   percorso della sessione, e con la guardia della regola Q
//!   (`write::visibility::guard_search_path`) e la qualificazione, le
//!   letture e le query (i filtri confrontano colonne di tipi del target con
//!   i loro operatori, che stanno fuori da `pg_catalog`), l'introspezione e
//!   il preflight (`format_type` e la visibilita delle funzioni di `PostGIS`
//!   dipendono dal percorso per definizione), le transazioni applicative e
//!   `run_ddl` (SQL del chiamante).
//! - **Hazard**: un `COMMENT ON COLUMN` della registrazione degli id gira
//!   con il percorso chiuso: un event trigger che risolve nomi senza schema
//!   li cerca in `pg_catalog`. Un tipo di enum, dominio o composto che il
//!   piano dichiara senza schema si risolve con il percorso della sessione
//!   negli statement sul target.
//! - **Rientro**: operatori qualificati per tipo nei filtri delle letture,
//!   per chiudere il percorso anche li.

use crate::error::classify_error;
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use tokio_postgres::GenericClient;

/// Il percorso della libreria.
pub const CLOSED: &str = "pg_catalog, pg_temp";

const CURRENT_SQL: &str = "SELECT pg_catalog.current_setting('search_path')";
const SET_SQL: &str = "SELECT pg_catalog.set_config('search_path', $1::pg_catalog.text, true)";

/// Il `search_path` della sessione, letto prima di chiudere il percorso.
#[derive(Debug, Clone)]
pub struct SessionPath(String);

impl SessionPath {
    /// Legge il percorso della sessione e chiude il percorso per il resto
    /// della transazione aperta su `client`.
    ///
    /// # Errors
    ///
    /// L'errore classificato della lettura o dell'impostazione, in `phase`.
    pub async fn close<C: GenericClient + Sync>(client: &C, phase: ErrorPhase) -> Result<Self> {
        let session: String = client
            .query_one(CURRENT_SQL, &[])
            .await
            .map_err(|error| classify_error(phase, &error))?
            .get(0);
        set(client, CLOSED, phase).await?;
        Ok(Self(session))
    }

    /// Esegue `statement`, uno statement sul target, con il percorso della
    /// sessione, e richiude il percorso.
    ///
    /// Se lo statement fallisce l'errore e il suo: la transazione e gia
    /// interrotta o il chiamante la annulla, e la chiusura non serve piu.
    ///
    /// # Errors
    ///
    /// L'errore dello statement, o quello classificato dell'impostazione.
    pub async fn on_target<C, T, E, F>(&self, client: &C, statement: F) -> std::result::Result<T, E>
    where
        C: GenericClient + Sync,
        E: From<DatabaseError>,
        F: std::future::Future<Output = std::result::Result<T, E>>,
    {
        set(client, &self.0, ErrorPhase::Write).await?;
        let value = statement.await?;
        set(client, CLOSED, ErrorPhase::Write).await?;
        Ok(value)
    }

    /// Rimette il percorso della sessione per il resto della transazione:
    /// per il percorso row-scoped, dove dopo l'ultima lettura del catalogo
    /// restano soltanto gli INSERT sul target.
    ///
    /// # Errors
    ///
    /// L'errore classificato dell'impostazione.
    pub async fn restore<C: GenericClient + Sync>(&self, client: &C) -> Result<()> {
        set(client, &self.0, ErrorPhase::Write).await
    }
}

async fn set<C: GenericClient + Sync>(client: &C, value: &str, phase: ErrorPhase) -> Result<()> {
    client
        .query_one(SET_SQL, &[&value])
        .await
        .map_err(|error| classify_error(phase, &error))?;
    Ok(())
}
