//! La regola R su Oracle (`plenora_database_core::row_visibility`): gli
//! statement di ogni mode su un target esistente arrivano al target cosi
//! come sono, e il `DELETE` di Replace e gli statement di Update e
//! `DeleteByKeys` ne vedono tutte le righe.
//!
//! I fatti, dal dizionario e con i privilegi del ruolo:
//!
//! - una policy VPD abilitata per l'evento distruttivo (`ALL_POLICIES`, `DEL`
//!   o `UPD`): il predicato della policy restringe lo statement senza errore;
//! - un trigger abilitato per l'evento distruttivo (`ALL_TRIGGERS`), di
//!   qualunque tipo: il dizionario non dice se un `INSTEAD OF` o un compound
//!   trigger sopprimono righe;
//! - un trigger `INSTEAD OF` abilitato per uno degli eventi della mode
//!   (INSERT di Append e Replace, INSERT e UPDATE del `MERGE` di Upsert): le
//!   righe vanno alla funzione, non al target. Un trigger `BEFORE INSERT`
//!   Oracle non sopprime righe (puo soltanto cambiarle o fallire);
//! - il tipo del target (`ALL_OBJECTS`): una vista o un sinonimo hanno
//!   relazioni di base che qui non si risolvono, e si rifiutano per ogni
//!   mode.
//!
//! Se il dizionario non si legge (la lettura fallisce), la regola rifiuta:
//! l'assenza di policy non e provata.
//!
//! # Limite dichiarato
//!
//! - **Regola.** Non misurato live: il fixture Oracle non era acceso al
//!   momento della regola (25/10). Le colonne lette sono quelle documentate
//!   del dizionario (`ALL_POLICIES.ENABLE/DEL/UPD`, `ALL_TRIGGERS.STATUS/
//!   TRIGGERING_EVENT`); una lettura che fallisce rifiuta, non ammette.
//! - **Ambito.** Ogni mode Oracle su un target esistente.
//! - **Hazard.** `EXEMPT ACCESS POLICY` non si legge (la regola rifiuta
//!   comunque); Oracle Label Security e Real Application Security non si
//!   leggono (la regola non li vede); un DDL concorrente fra la lettura e lo
//!   statement resta una finestra.
//! - **Rientro.** La prova live sul fixture Oracle Free (`live_tests.rs`), e
//!   la lettura delle policy OLS/RAS.

use plenora_database_core::plan::{ProviderKind, WriteMode};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::row::Row;
use plenora_database_core::row_visibility::{
    destructive_event, mode_events, refuse_unfaithful_write, DestructiveEvent, RowFilters,
    WriteEvent,
};
use plenora_database_core::transaction::{Statement, TransactionScope};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase, Result};

/// La lettura dei fatti: tipo dell'oggetto, policy VPD per l'evento
/// distruttivo, trigger per l'evento distruttivo, trigger `INSTEAD OF` per gli
/// eventi della mode. I bind sono posizionali e ciascuno compare una volta;
/// un evento assente si passa come [`NO_EVENT`].
pub const FACTS_SQL: &str = "SELECT \
    (SELECT MAX(OBJECT_TYPE) FROM ALL_OBJECTS \
      WHERE OWNER = :1 AND OBJECT_NAME = :2 \
        AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'SYNONYM')), \
    CASE WHEN EXISTS (SELECT 1 FROM ALL_POLICIES \
      WHERE OBJECT_OWNER = :3 AND OBJECT_NAME = :4 AND ENABLE = 'YES' \
        AND DECODE(:5, 'DELETE', DEL, 'UPDATE', UPD, 'NO') = 'YES') THEN 'Y' ELSE 'N' END, \
    CASE WHEN EXISTS (SELECT 1 FROM ALL_TRIGGERS \
      WHERE TABLE_OWNER = :6 AND TABLE_NAME = :7 AND STATUS = 'ENABLED' \
        AND INSTR(TRIGGERING_EVENT, :8) > 0) THEN 'Y' ELSE 'N' END, \
    CASE WHEN EXISTS (SELECT 1 FROM ALL_TRIGGERS \
      WHERE TABLE_OWNER = :9 AND TABLE_NAME = :10 AND STATUS = 'ENABLED' \
        AND TRIGGER_TYPE = 'INSTEAD OF' \
        AND (INSTR(TRIGGERING_EVENT, :11) > 0 OR INSTR(TRIGGERING_EVENT, :12) > 0)) \
      THEN 'Y' ELSE 'N' END \
    FROM DUAL";

/// Il segnaposto di un evento che la mode non ha: non compare in
/// `TRIGGERING_EVENT` (`INSERT OR UPDATE OR DELETE`) e non e un valore di
/// `DECODE`. Non si usa la stringa vuota, che Oracle legge come `NULL`.
pub const NO_EVENT: &str = "#";

const fn event_name(event: WriteEvent) -> &'static str {
    match event {
        WriteEvent::Insert => "INSERT",
        WriteEvent::Delete => "DELETE",
        WriteEvent::Update => "UPDATE",
    }
}

/// I parametri di [`FACTS_SQL`] per il target e gli eventi della mode.
#[must_use]
pub fn facts_parameters(owner: &str, object: &str, mode: WriteMode) -> Vec<ParameterValue> {
    let text = |value: &str| ParameterValue::String(value.to_owned());
    let destructive = match destructive_event(mode) {
        Some(DestructiveEvent::Delete) => event_name(WriteEvent::Delete),
        Some(DestructiveEvent::Update) => event_name(WriteEvent::Update),
        None => NO_EVENT,
    };
    let events = mode_events(mode);
    let event = |index: usize| {
        events
            .get(index)
            .map_or(NO_EVENT, |event| event_name(*event))
    };
    vec![
        text(owner),
        text(object),
        text(owner),
        text(object),
        text(destructive),
        text(owner),
        text(object),
        text(destructive),
        text(owner),
        text(object),
        text(event(0)),
        text(event(1)),
    ]
}

/// I fatti di una riga di [`FACTS_SQL`]: `None` se il target non c'e (lo
/// statement lo rifiutera), altrimenti i filtri per la regola. Una riga che
/// non ha la forma attesa e un dizionario non letto.
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
    let flag = |index: usize| text(index).map(|value| value == "Y");
    let kind = match row.get_index(0) {
        Some(ParameterValue::Null { .. }) | None => return None,
        _ => text(0),
    };
    let (Some(kind), Some(policy), Some(trigger), Some(instead)) =
        (kind, flag(1), flag(2), flag(3))
    else {
        return Some(unreadable);
    };
    Some(RowFilters {
        row_security: policy,
        rewrite_rule: false,
        instead_trigger: instead,
        row_trigger: trigger,
        path_bypass: false,
        view: kind == "VIEW",
        opaque_relation: kind != "TABLE",
        unreadable: false,
    })
}

/// La regola R nella transazione della scrittura, prima del primo statement
/// della mode.
///
/// # Errors
///
/// Il rifiuto della regola (`unsupported`, fase `write`), anche quando il
/// dizionario non si legge.
pub async fn enforce(
    transaction: &mut dyn TransactionScope,
    owner: &str,
    object: &str,
    mode: WriteMode,
    cancellation: &CancellationToken,
) -> Result<()> {
    if mode_events(mode).is_empty() {
        return Ok(());
    }
    let statement = Statement {
        sql: FACTS_SQL.to_owned(),
        params: facts_parameters(owner, object, mode),
    };
    let filters = match transaction.query(&statement, cancellation).await {
        Ok(rows) => filters_from_row(&rows),
        // Un'interruzione resta un'interruzione: non dice niente dei filtri.
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
        refuse_unfaithful_write(ProviderKind::Oracle, mode, ErrorPhase::Write, filters)
    })
}

#[cfg(test)]
#[path = "row_visibility_tests.rs"]
mod tests;
