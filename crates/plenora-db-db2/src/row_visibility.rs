//! La regola R su Db2 (`plenora_database_core::row_visibility`): il `DELETE`
//! di Replace e gli statement di Update e `DeleteByKeys` devono vedere tutte
//! le righe del target.
//!
//! I fatti, dal catalogo:
//!
//! - row permission attive (`SYSCAT.TABLES.CONTROL` `R` o `B`): il controllo
//!   di accesso per riga (RCAC) restringe lo statement senza errore;
//! - un trigger per l'evento (`SYSCAT.TRIGGERS.TRIGEVENT`, anche `M` per i
//!   trigger a piu eventi), di qualunque tipo: il catalogo non dice se un
//!   `INSTEAD OF` sopprime righe;
//! - il tipo del target (`SYSCAT.TABLES.TYPE`): ogni tipo diverso da una
//!   tabella (vista, alias, nickname federato, MQT) e opaco. La preflight lo
//!   rifiuta gia per le scritture; la regola non se ne fida.
//!
//! Se il catalogo non si legge (la lettura fallisce), la regola rifiuta.
//!
//! # Limite dichiarato
//!
//! - **Regola.** Non misurato live: nessun fixture Db2 acceso al momento
//!   della regola (25/10). Le colonne lette sono quelle documentate del
//!   catalogo; una lettura che fallisce rifiuta, non ammette.
//! - **Ambito.** Replace, Update, `DeleteByKeys` Db2.
//! - **Hazard.** Un DDL concorrente fra la lettura e lo statement resta una
//!   finestra.
//! - **Rientro.** La prova live sul fixture Db2 LUW (`live_tests.rs`).

use plenora_database_core::plan::{ProviderKind, WriteMode};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::row::Row;
use plenora_database_core::row_visibility::{
    destructive_event, refuse_filtered_rows, DestructiveEvent, RowFilters,
};
use plenora_database_core::transaction::{Statement, TransactionScope};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase, Result};

/// La lettura dei fatti: tipo, controllo di accesso, trigger per l'evento.
/// Un target assente rende `-` come tipo.
pub const FACTS_SQL: &str = "SELECT \
    COALESCE((SELECT TRIM(TYPE) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?), '-'), \
    COALESCE((SELECT TRIM(CONTROL) FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?), ''), \
    CASE WHEN EXISTS (SELECT 1 FROM SYSCAT.TRIGGERS \
      WHERE TABSCHEMA = ? AND TABNAME = ? AND TRIGEVENT IN (?, 'M')) THEN 'Y' ELSE 'N' END \
    FROM SYSIBM.SYSDUMMY1";

/// I parametri di [`FACTS_SQL`] per il target e l'evento.
#[must_use]
pub fn facts_parameters(
    schema: &str,
    object: &str,
    event: DestructiveEvent,
) -> Vec<ParameterValue> {
    let text = |value: &str| ParameterValue::String(value.to_owned());
    let event = match event {
        DestructiveEvent::Delete => "D",
        DestructiveEvent::Update => "U",
    };
    vec![
        text(schema),
        text(object),
        text(schema),
        text(object),
        text(schema),
        text(object),
        text(event),
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
    let (Some(kind), Some(control), Some(trigger)) = (text(0), text(1), text(2)) else {
        return Some(unreadable);
    };
    if kind == "-" {
        return None;
    }
    Some(RowFilters {
        row_security: matches!(control.as_str(), "R" | "B"),
        rewrite_rule: false,
        row_trigger: trigger == "Y",
        view: kind == "V",
        opaque_relation: kind != "T",
        unreadable: false,
    })
}

/// La regola R nella transazione della scrittura, prima del primo statement
/// distruttivo.
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
    let Some(event) = destructive_event(mode) else {
        return Ok(());
    };
    let statement = Statement {
        sql: FACTS_SQL.to_owned(),
        params: facts_parameters(schema, object, event),
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
        refuse_filtered_rows(ProviderKind::Db2, mode, ErrorPhase::Write, filters)
    })
}

#[cfg(test)]
#[path = "row_visibility_tests.rs"]
mod tests;
