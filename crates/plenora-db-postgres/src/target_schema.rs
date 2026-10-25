//! La regola T su PostgreSQL: un oggetto senza schema si risolve una volta
//! per operazione, con il `search_path` della sessione, e da li ogni passo lo
//! nomina con lo schema risolto.
//!
//! Fino al 25/10 (quarta rilettura indipendente, S1) un `ObjectRef` senza
//! schema aveva due letture. Con un ruolo `search_path = app, public` e due
//! tabelle `app.r4t` e `public.r4t`:
//!
//! - preflight, dry run, Append e Update andavano in `public.r4t`
//!   (`write::qualified_target` metteva `public` dove il piano taceva);
//! - la lettura rendeva le righe di `app.r4t` (lo statement `FROM "r4t"` lo
//!   risolveva la sessione) con le colonne di `public.r4t` (il catalogo letto
//!   con `public`);
//! - describe e la chiave della cache dello schema guardavano `public`.
//!
//! # La regola
//!
//! Prima di ogni altra lettura del catalogo dell'operazione, sulla sessione
//! della libreria e con il suo percorso (prima di
//! [`crate::search_path::SessionPath::close`]):
//!
//! - un oggetto senza schema che esiste si risolve nello schema in cui la
//!   sessione lo trova: `pg_catalog.to_regclass(pg_catalog.quote_ident($1))`,
//!   lo stesso cammino del `search_path` (e degli schemi senza `USAGE`, che la
//!   sessione salta) che uno statement senza schema farebbe;
//! - un oggetto senza schema che non esiste, e il target di Create, si
//!   risolvono in `pg_catalog.current_schema()`, lo schema in cui
//!   `CREATE TABLE` senza schema creerebbe; se nessuno schema del percorso
//!   esiste, l'operazione si rifiuta (`not_found`);
//! - un oggetto in uno schema temporaneo (`pg_temp`, `pg_temp_N`,
//!   `pg_toast_temp_N`), nominato o risolto, si rifiuta (`invalid_plan`):
//!   la preparazione e la scrittura girano su connessioni diverse del pool, e
//!   ogni sessione ha il proprio schema temporaneo.
//!
//! Il piano risolto e quello che preparazione, preflight, dry run, scrittura,
//! lettura, describe e cache usano, e quello che si pubblica: in
//! `PreparedWrite::operation` e nel `target` del dry run.
//!
//! # Limite dichiarato
//!
//! - **Regola**: un oggetto senza schema si risolve una volta per
//!   operazione, con il percorso della sessione.
//! - **Ambito**: scrittura (preparazione, preflight, dry run con la DSN,
//!   transazione), lettura, describe ed elenco degli oggetti di PostgreSQL.
//!   Lo schema risolto non entra in `WriteOutcome`, il cui contratto
//!   (`contracts/v2/write-outcome.schema.json`) e chiuso: chi lo vuole lo
//!   legge da `PreparedWrite::operation` o dal dry run.
//! - **Hazard**: la risoluzione precede la transazione della scrittura: un
//!   oggetto omonimo creato dopo in uno schema che viene prima nel percorso
//!   non sposta la scrittura, che resta sullo schema risolto (e il
//!   comportamento voluto: una sola lettura del nome). Un oggetto senza
//!   schema verso uno schema temporaneo si rifiuta anche quando esisterebbe
//!   soltanto in quella sessione.
//! - **Rientro**: lo schema risolto nel documento dell'esito, con una major
//!   del contratto.

use crate::error::{classify_error, public_error};
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use tokio_postgres::GenericClient;

/// Che cosa si risolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Purpose {
    /// Un oggetto che l'operazione legge o scrive.
    Existing,
    /// Il target di Create: lo schema in cui la sessione creerebbe.
    Create,
}

/// Gira con il percorso della sessione, che e cio che legge: gli operatori
/// sono qualificati e con operandi dello stesso tipo esatto, perche uno
/// schema del percorso non li sostituisca (un `public."="(oid, regclass)`
/// vincerebbe su `pg_catalog` per la conversione implicita).
const EXISTING_SQL: &str = "SELECT n.nspname::pg_catalog.text \
       FROM pg_catalog.pg_class c \
       JOIN pg_catalog.pg_namespace n ON n.oid OPERATOR(pg_catalog.=) c.relnamespace \
      WHERE c.oid OPERATOR(pg_catalog.=) \
            pg_catalog.to_regclass(pg_catalog.quote_ident($1::pg_catalog.text))::pg_catalog.oid";

const CURRENT_SQL: &str = "SELECT pg_catalog.current_schema()::pg_catalog.text";

/// Il motivo pubblico del rifiuto di uno schema temporaneo.
pub const TEMPORARY_SCHEMA: &str = "oggetto PostgreSQL in uno schema temporaneo: ogni sessione \
     ha il proprio, e l'operazione non resta su una sola sessione";

/// Il motivo pubblico di un percorso senza schemi.
pub const NO_CURRENT_SCHEMA: &str =
    "nessuno schema del search_path della sessione esiste: l'oggetto senza schema non si risolve";

/// Uno schema temporaneo, per nome.
#[must_use]
pub fn is_temporary(schema: &str) -> bool {
    schema == "pg_temp" || schema.starts_with("pg_temp_") || schema.starts_with("pg_toast_temp_")
}

fn temporary(phase: ErrorPhase) -> DatabaseError {
    public_error(ErrorCategory::InvalidPlan, phase, false, TEMPORARY_SCHEMA)
}

/// `object` con lo schema risolto con il percorso della sessione di
/// `client` ([la regola](self)).
///
/// # Errors
///
/// `invalid_plan` per uno schema temporaneo, `not_found` se il percorso non
/// ha schemi, l'errore classificato della lettura; tutti in `phase`.
pub async fn resolve<C: GenericClient + Sync>(
    client: &C,
    object: &ObjectRef,
    purpose: Purpose,
    phase: ErrorPhase,
) -> Result<ObjectRef> {
    if let Some(schema) = object.schema.as_deref() {
        if is_temporary(schema) {
            return Err(temporary(phase));
        }
        return Ok(object.clone());
    }
    let read = |error: tokio_postgres::Error| classify_error(phase, &error);
    let existing = if purpose == Purpose::Existing {
        client
            .query_opt(EXISTING_SQL, &[&object.object])
            .await
            .map_err(read)?
            .map(|row| row.get::<_, String>(0))
    } else {
        None
    };
    let schema = if let Some(schema) = existing {
        schema
    } else {
        client
            .query_one(CURRENT_SQL, &[])
            .await
            .map_err(read)?
            .get::<_, Option<String>>(0)
            .ok_or_else(|| public_error(ErrorCategory::NotFound, phase, false, NO_CURRENT_SCHEMA))?
    };
    if is_temporary(&schema) {
        return Err(temporary(phase));
    }
    Ok(ObjectRef {
        catalog: object.catalog.clone(),
        schema: Some(schema),
        object: object.object.clone(),
    })
}

/// Lo schema di un oggetto gia risolto da [`resolve`]: ogni lettura del
/// catalogo e ogni statement lo nominano.
///
/// # Errors
///
/// `invalid_plan` se l'oggetto non e stato risolto: nessun ripiego su uno
/// schema predefinito.
pub fn schema_of(object: &ObjectRef) -> Result<&str> {
    object.schema.as_deref().ok_or_else(|| {
        DatabaseError::invalid_plan("oggetto PostgreSQL senza lo schema risolto (regola T)")
    })
}
