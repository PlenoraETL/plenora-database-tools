//! La regola R: una scrittura distruttiva vede tutte le righe che dichiara di
//! toccare, o non parte.
//!
//! `Replace` svuota il target con un `DELETE` (PostgreSQL, `MySQL`, Oracle,
//! Db2) e promette che dopo il COMMIT il target contiene soltanto l'ingresso;
//! `DeleteByKeys` e `Update` pubblicano `skipped` per le chiavi che non trovano.
//! Entrambe le promesse valgono soltanto se lo statement vede **tutte** le
//! righe del target. Una policy di sicurezza di riga (RLS di PostgreSQL, VPD
//! di Oracle, RCAC di Db2), una regola di riscrittura (`CREATE RULE ... ON
//! DELETE DO INSTEAD NOTHING`), un trigger di riga che sopprime righe
//! (`BEFORE DELETE ... RETURN NULL`, `INSTEAD OF`), una vista o una relazione
//! remota possono restringere lo statement senza errore: il server risponde
//! con successo, `Replace` lascia righe vecchie accanto alle nuove e
//! `DeleteByKeys` pubblica come assente una chiave che c'e ma non si vede.
//!
//! Misurato il 25/10 su PostgreSQL 16 (rilettura indipendente): tabella
//! (1,a),(2,b),(3,a) con policy `USING (owner = 'a')` e Replace con le righe
//! 10 e 11 → `committed`, target 2,10,11; lo stesso con una policy
//! `RESTRICTIVE`; `ON DELETE DO INSTEAD NOTHING` → `committed`, nulla
//! sostituito; `BEFORE DELETE` che restituisce `NULL` per id=2 → 2,10,11.
//!
//! La regola: quando il catalogo dice che uno di questi meccanismi puo
//! filtrare lo statement distruttivo, la scrittura si rifiuta **prima di ogni
//! effetto** con `unsupported`, e il dry run riporta lo stesso rifiuto. La
//! decisione e una sola ([`refuse_filtered_rows`]); i provider leggono i fatti
//! ([`RowFilters`]) dal proprio catalogo e non hanno un'altra strada.
//!
//! # Limite dichiarato
//!
//! - **Regola.** Il rifiuto e conservativo: ogni trigger di riga `BEFORE` o
//!   `INSTEAD OF` sull'evento conta, anche uno che non sopprime mai righe (un
//!   `updated_at` aggiornato da un trigger rende `Update` non ammessa), perche
//!   il catalogo non dice che cosa restituisce la funzione.
//! - **Ambito.** `Replace`, `DeleteByKeys`, `Update` su ogni provider che
//!   chiama la regola; `TruncateInsert` ne e fuori dove usa `TRUNCATE`, che
//!   non e soggetto a policy, regole o trigger di riga.
//! - **Hazard.** Un DDL concorrente (una policy o un trigger creati fra la
//!   lettura del catalogo e lo statement) sfugge alla prima lettura: dove il
//!   provider lo puo fare la regola si rilegge con il lock dello statement
//!   preso (PostgreSQL), altrove resta la finestra. I privilegi che esentano
//!   da una policy (`EXEMPT ACCESS POLICY` di Oracle, `BYPASSRLS` di
//!   PostgreSQL a parte) e le etichette di Oracle Label Security non sono
//!   letti: la regola rifiuta comunque, o non vede (OLS).
//! - **Rientro.** Un conteggio delle righe del target prima e dopo lo
//!   statement, nella stessa transazione e con lo stesso snapshot, che provi
//!   che lo statement ha toccato tutte le righe viste da un ruolo senza
//!   filtri; oppure una misura per meccanismo che ne restringa il rifiuto.

use crate::plan::{ProviderKind, WriteMode};
use crate::{DatabaseError, ErrorPhase, Result};

/// L'evento DML le cui righe la mode deve vedere tutte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestructiveEvent {
    /// Il `DELETE` di `Replace` e di `DeleteByKeys`.
    Delete,
    /// L'`UPDATE` di `Update`.
    Update,
}

/// L'evento distruttivo della mode, se ne ha uno.
///
/// `Upsert` non c'e: una riga in conflitto nascosta da una policy di UPDATE
/// fa fallire lo statement (PostgreSQL non la salta in silenzio), e una riga
/// inserita si conta. `TruncateInsert` non c'e: dove la mode esiste e un
/// `TRUNCATE`, che non passa da policy, regole o trigger di riga.
#[must_use]
pub const fn destructive_event(mode: WriteMode) -> Option<DestructiveEvent> {
    match mode {
        WriteMode::Replace | WriteMode::DeleteByKeys => Some(DestructiveEvent::Delete),
        WriteMode::Update => Some(DestructiveEvent::Update),
        WriteMode::Create | WriteMode::Append | WriteMode::TruncateInsert | WriteMode::Upsert => {
            None
        }
    }
}

/// Cio che il catalogo dice del target, per l'evento della mode.
///
/// Un fatto per meccanismo, cosi ogni provider riempie soltanto cio che il
/// proprio catalogo sa e lascia `false` cio che il suo prodotto non ha.
///
/// Un provider che non sa leggere un fatto che il suo prodotto ha lo dichiara
/// con `unreadable`: la regola rifiuta.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowFilters {
    /// Una policy di sicurezza di riga si applica allo statement: RLS attiva
    /// per il ruolo sul target o su una relazione sotto di esso (PostgreSQL),
    /// una policy VPD per l'evento (Oracle), row permission attive (Db2).
    pub row_security: bool,
    /// Una regola di riscrittura per l'evento (PostgreSQL `pg_rewrite`).
    pub rewrite_rule: bool,
    /// Un trigger di riga che puo sopprimere o sostituire lo statement:
    /// `BEFORE ... FOR EACH ROW` o `INSTEAD OF` per l'evento (PostgreSQL);
    /// ogni trigger abilitato per l'evento dove il catalogo non distingue
    /// (Oracle, Db2).
    pub row_trigger: bool,
    /// Il target e una vista. `Replace` la rifiuta sempre: la definizione
    /// della vista e un filtro, e il catalogo non prova che non lo sia.
    pub view: bool,
    /// Una relazione il cui filtro il catalogo locale non dice: una tabella
    /// remota (foreign table, nickname), un sinonimo, una vista le cui
    /// relazioni di base il provider non risolve.
    pub opaque_relation: bool,
    /// Il catalogo che direbbe uno dei fatti non e leggibile dal ruolo.
    pub unreadable: bool,
}

/// I motivi, stabili, senza dati: uno per meccanismo.
pub const HIDDEN_BY_ROW_SECURITY: &str =
    "una policy di sicurezza di riga puo nascondere righe del target allo statement";
pub const HIDDEN_BY_RULE: &str = "una regola di riscrittura del target puo sostituire lo statement";
pub const HIDDEN_BY_TRIGGER: &str =
    "un trigger di riga BEFORE o INSTEAD OF del target puo sopprimere righe";
pub const HIDDEN_BY_VIEW: &str =
    "replace su una vista: la definizione della vista puo escludere righe";
pub const HIDDEN_BY_OPAQUE_RELATION: &str =
    "il target dipende da una relazione di cui il catalogo non dice i filtri";
pub const HIDDEN_UNREADABLE: &str =
    "il catalogo che dice se lo statement vede tutte le righe non e leggibile";

const fn mode_name(mode: WriteMode) -> &'static str {
    match mode {
        WriteMode::Replace => "replace",
        WriteMode::DeleteByKeys => "delete_by_keys",
        WriteMode::Update => "update",
        WriteMode::Create => "create",
        WriteMode::Append => "append",
        WriteMode::TruncateInsert => "truncate_insert",
        WriteMode::Upsert => "upsert",
    }
}

/// La regola R: il rifiuto della scrittura `mode` sul target descritto da
/// `filters`, o `Ok` se lo statement distruttivo vede tutte le righe.
///
/// Una mode senza evento distruttivo ([`destructive_event`]) passa sempre.
///
/// # Errors
///
/// `unsupported` nella fase `phase`, con il motivo del primo meccanismo
/// trovato nell'ordine: catalogo illeggibile, sicurezza di riga, regola,
/// trigger, relazione opaca, vista (solo `Replace`).
pub fn refuse_filtered_rows(
    provider: ProviderKind,
    mode: WriteMode,
    phase: ErrorPhase,
    filters: RowFilters,
) -> Result<()> {
    if destructive_event(mode).is_none() {
        return Ok(());
    }
    let reason = if filters.unreadable {
        HIDDEN_UNREADABLE
    } else if filters.row_security {
        HIDDEN_BY_ROW_SECURITY
    } else if filters.rewrite_rule {
        HIDDEN_BY_RULE
    } else if filters.row_trigger {
        HIDDEN_BY_TRIGGER
    } else if filters.opaque_relation {
        HIDDEN_BY_OPAQUE_RELATION
    } else if filters.view && mode == WriteMode::Replace {
        HIDDEN_BY_VIEW
    } else {
        return Ok(());
    };
    Err(DatabaseError::unsupported(
        provider,
        phase,
        format!("{} rifiutata: {reason}", mode_name(mode)),
    ))
}

#[cfg(test)]
#[path = "row_visibility_tests.rs"]
mod tests;
