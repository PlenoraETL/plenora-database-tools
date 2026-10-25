//! La regola R su Db2 (`plenora_database_core::row_visibility`): gli
//! statement di ogni mode su un target esistente arrivano al target cosi
//! come sono, e il `DELETE` di Replace e gli statement di Update e
//! `DeleteByKeys` ne vedono tutte le righe.
//!
//! I fatti, dal catalogo:
//!
//! - row permission attive (`SYSCAT.TABLES.CONTROL` `R` o `B`): il controllo
//!   di accesso per riga (RCAC) restringe lo statement distruttivo senza
//!   errore;
//! - un trigger per l'evento distruttivo (`SYSCAT.TRIGGERS.TRIGEVENT`, anche
//!   `M` per i trigger a piu eventi), di qualunque tipo: il catalogo non dice
//!   se un `INSTEAD OF` sopprime righe;
//! - un trigger `INSTEAD OF` (`TRIGTIME = 'I'`) per uno degli eventi della
//!   mode (INSERT di Append e Replace, INSERT e UPDATE del `MERGE` di
//!   Upsert): le righe vanno al trigger, non al target. Un trigger `BEFORE`
//!   Db2 non sopprime righe (puo cambiarle o fallire con `SIGNAL`);
//! - il tipo del target (`SYSCAT.TABLES.TYPE`): ogni tipo diverso da una
//!   tabella (vista, alias, nickname federato, MQT) e opaco, per ogni mode.
//!   La preflight lo rifiuta gia per le scritture; la regola non se ne fida.
//!
//! Se il catalogo non si legge (la lettura fallisce), la regola rifiuta.
//!
//! # Limite dichiarato
//!
//! - **Regola.** Non misurato live: nessun fixture Db2 acceso al momento
//!   della regola (25/10). Le colonne lette sono quelle documentate del
//!   catalogo; una lettura che fallisce rifiuta, non ammette.
//! - **Ambito.** Ogni mode Db2 su un target esistente.
//! - **Hazard.** Un DDL concorrente fra la lettura e lo statement resta una
//!   finestra.
//! - **Rientro.** La prova live sul fixture Db2 LUW (`live_tests.rs`).

use plenora_database_core::plan::{ProviderKind, WriteMode};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::row::Row;
use plenora_database_core::row_visibility::{
    destructive_event, mode_events, refuse_unfaithful_write, DestructiveEvent, RowFilters,
    WriteEvent,
};
use plenora_database_core::transaction::{Statement, TransactionScope};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase, Result};

/// La lettura dei fatti: tipo, controllo di accesso, trigger per l'evento
/// distruttivo, trigger `INSTEAD OF` per gli eventi della mode. Un target
/// assente rende `-` come tipo; un evento che la mode non ha si passa come
/// [`NO_EVENT`].
pub const FACTS_SQL: &str = "SELECT \
    COALESCE((SELECT TRIM(TYPE) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?), '-'), \
    COALESCE((SELECT TRIM(CONTROL) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?), ''), \
    CASE WHEN EXISTS (SELECT 1 FROM SYSCAT.TRIGGERS \
      WHERE TABSCHEMA = ? AND TABNAME = ? AND TRIGEVENT IN (?, 'M')) THEN 'Y' ELSE 'N' END, \
    CASE WHEN EXISTS (SELECT 1 FROM SYSCAT.TRIGGERS \
      WHERE TABSCHEMA = ? AND TABNAME = ? AND TRIGTIME = 'I' \
        AND TRIGEVENT IN (?, ?, 'M')) THEN 'Y' ELSE 'N' END \
    FROM SYSIBM.SYSDUMMY1";

/// Il segnaposto di un evento che la mode non ha: non e un valore di
/// `TRIGEVENT` (`I`, `U`, `D`, `M`).
pub const NO_EVENT: &str = "-";

const fn event_code(event: WriteEvent) -> &'static str {
    match event {
        WriteEvent::Insert => "I",
        WriteEvent::Update => "U",
        WriteEvent::Delete => "D",
    }
}

/// I parametri di [`FACTS_SQL`] per il target e gli eventi della mode.
#[must_use]
pub fn facts_parameters(schema: &str, object: &str, mode: WriteMode) -> Vec<ParameterValue> {
    let text = |value: &str| ParameterValue::String(value.to_owned());
    let destructive = match destructive_event(mode) {
        Some(DestructiveEvent::Delete) => event_code(WriteEvent::Delete),
        Some(DestructiveEvent::Update) => event_code(WriteEvent::Update),
        None => NO_EVENT,
    };
    let events = mode_events(mode);
    let event = |index: usize| {
        events
            .get(index)
            .map_or(NO_EVENT, |event| event_code(*event))
    };
    vec![
        text(schema),
        text(object),
        text(schema),
        text(object),
        text(schema),
        text(object),
        text(destructive),
        text(schema),
        text(object),
        text(event(0)),
        text(event(1)),
    ]
}

/// I fatti di una riga di [`FACTS_SQL`]: `None` se il target non c'e (lo
/// statement lo rifiutera), altrimenti i filtri per la regola. Una riga che
/// non ha la forma attesa e un catalogo non letto.
#[must_use]
pub fn filters_from_row(rows: &[Row]) -> Option<RowFilters> {
    let unreadable = RowFilters {
        unreadable: true,
        ..RowFilters::default()
    };
    let [row] = rows else {
        return Some(unreadable);
    };
    let text = |index: usize| match row.get_index(index) {
        Some(ParameterValue::String(value)) => Some(value.trim().to_owned()),
        _ => None,
    };
    let (Some(kind), Some(control), Some(trigger), Some(instead)) =
        (text(0), text(1), text(2), text(3))
    else {
        return Some(unreadable);
    };
    if kind == "-" {
        return None;
    }
    Some(RowFilters {
        row_security: matches!(control.as_str(), "R" | "B"),
        rewrite_rule: false,
        instead_trigger: instead == "Y",
        row_trigger: trigger == "Y",
        path_bypass: false,
        view: kind == "V",
        opaque_relation: kind != "T",
        unreadable: false,
    })
}

/// La regola R nella transazione della scrittura, prima del primo statement
/// della mode.
///
/// # Errors
///
/// Il rifiuto della regola (`unsupported`, fase `write`), anche quando il
/// catalogo non si legge; un'interruzione resta tale.
pub async fn enforce(
    transaction: &mut dyn TransactionScope,
    schema: &str,
    object: &str,
    mode: WriteMode,
    cancellation: &CancellationToken,
) -> Result<()> {
    if mode_events(mode).is_empty() {
        return Ok(());
    }
    let statement = Statement {
        sql: FACTS_SQL.to_owned(),
        params: facts_parameters(schema, object, mode),
    };
    let filters = match transaction.query(&statement, cancellation).await {
        Ok(rows) => filters_from_row(&rows),
        Err(error)
            if matches!(
                error.category,
                ErrorCategory::Cancelled | ErrorCategory::Timeout
            ) =>
        {
            return Err(error);
        }
        Err(_) => Some(RowFilters {
            unreadable: true,
            ..RowFilters::default()
        }),
    };
    filters.map_or(Ok(()), |filters| {
        refuse_unfaithful_write(ProviderKind::Db2, mode, ErrorPhase::Write, filters)
    })
}

#[cfg(test)]
#[path = "row_visibility_tests.rs"]
mod tests;
