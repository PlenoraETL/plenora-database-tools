//! La regola R su Oracle (`plenora_database_core::row_visibility`): il
//! `DELETE` di Replace e gli statement di Update e `DeleteByKeys` devono
//! vedere tutte le righe del target.
//!
//! I fatti, dal dizionario e con i privilegi del ruolo:
//!
//! - una policy VPD abilitata per l'evento (`ALL_POLICIES`, `DEL` o `UPD`):
//!   il predicato della policy restringe lo statement senza errore;
//! - un trigger abilitato per l'evento (`ALL_TRIGGERS`), di qualunque tipo: il
//!   dizionario non dice se un `INSTEAD OF` o un compound trigger sopprimono
//!   righe;
//! - il tipo del target (`ALL_OBJECTS`): una vista o un sinonimo hanno
//!   relazioni di base che qui non si risolvono, e si rifiutano.
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
//! - **Ambito.** Replace, Update, `DeleteByKeys` Oracle.
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
    destructive_event, refuse_filtered_rows, DestructiveEvent, RowFilters,
};
use plenora_database_core::transaction::{Statement, TransactionScope};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase, Result};

/// La lettura dei fatti: tipo dell'oggetto, policy VPD per l'evento,
/// trigger per l'evento. I bind sono posizionali e ciascuno compare una
/// volta.
pub const FACTS_SQL: &str = "SELECT \
    (SELECT MAX(OBJECT_TYPE) FROM ALL_OBJECTS \
      WHERE OWNER = :1 AND OBJECT_NAME = :2 \
        AND OBJECT_TYPE IN ('TABLE', 'VIEW', 'SYNONYM')), \
    CASE WHEN EXISTS (SELECT 1 FROM ALL_POLICIES \
      WHERE OBJECT_OWNER = :3 AND OBJECT_NAME = :4 AND ENABLE = 'YES' \
        AND DECODE(:5, 'DELETE', DEL, UPD) = 'YES') THEN 'Y' ELSE 'N' END, \
    CASE WHEN EXISTS (SELECT 1 FROM ALL_TRIGGERS \
      WHERE TABLE_OWNER = :6 AND TABLE_NAME = :7 AND STATUS = 'ENABLED' \
        AND INSTR(TRIGGERING_EVENT, :8) > 0) THEN 'Y' ELSE 'N' END \
    FROM DUAL";

const fn event_name(event: DestructiveEvent) -> &'static str {
    match event {
        DestructiveEvent::Delete => "DELETE",
        DestructiveEvent::Update => "UPDATE",
    }
}

/// I parametri di [`FACTS_SQL`] per il target e l'evento.
#[must_use]
pub fn facts_parameters(owner: &str, object: &str, event: DestructiveEvent) -> Vec<ParameterValue> {
    let text = |value: &str| ParameterValue::String(value.to_owned());
    let event = event_name(event);
    vec![
        text(owner),
        text(object),
        text(owner),
        text(object),
        text(event),
        text(owner),
        text(object),
        text(event),
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
    let (Some(kind), Some(policy), Some(trigger)) = (kind, flag(1), flag(2)) else {
        return Some(unreadable);
    };
    Some(RowFilters {
        row_security: policy,
        rewrite_rule: false,
        row_trigger: trigger,
        view: kind == "VIEW",
        opaque_relation: kind != "TABLE",
        unreadable: false,
    })
}

/// La regola R nella transazione della scrittura, prima del primo statement
/// distruttivo.
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
    let Some(event) = destructive_event(mode) else {
        return Ok(());
    };
    let statement = Statement {
        sql: FACTS_SQL.to_owned(),
        params: facts_parameters(owner, object, event),
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
        refuse_filtered_rows(ProviderKind::Oracle, mode, ErrorPhase::Write, filters)
    })
}

#[cfg(test)]
#[path = "row_visibility_tests.rs"]
mod tests;
