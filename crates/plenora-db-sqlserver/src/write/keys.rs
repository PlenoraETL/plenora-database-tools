//! La regola K sotto la collation del target (`plenora_database_core::write_keys`,
//! «chiavi uguali per il target, diverse come valori»).
//!
//! La condizione 2 del core confronta i valori Arrow: due chiavi `'a'` e
//! `'A'` sono distinte per il core e la stessa voce di un indice sotto una
//! collation che ignora le maiuscole (`SQL_Latin1_General_CP1_CI_AS`, quella
//! del container). Fino al 25/10 l'Upsert `[('a','x'),('A','y')]` su una
//! chiave `nvarchar` dava `committed`, `confirmed 2`, e una riga sola: la
//! seconda aggiornava la prima. Lo stesso per due testi di `datetimeoffset`
//! con offset diversi per lo stesso istante, che il server confronta uguali.
//!
//! La libreria non emula le collation: fa decidere al **server**. Nella
//! transazione della scrittura, dopo il lock del target e prima del primo
//! statement sul target, crea una tabella temporanea `#plenora_keys` con le
//! colonne chiave dello stesso tipo e della stessa collation del target e un
//! indice unico; batch per batch, prima degli statement del batch, ci
//! inserisce le chiavi del batch. Una violazione dell'indice e una chiave
//! ripetuta sotto la semantica dell'indice del target: `data_mapping` con
//! `DUPLICATE_INPUT_KEYS`, e la transazione si annulla (`rolled_back`). La
//! tabella si elimina prima del COMMIT; con un errore la elimina il
//! ROLLBACK, che annulla anche la sua creazione.
//!
//! Una chiave testuale senza collation nel catalogo si rifiuta: lasciare
//! il default di tempdb non proverebbe il confronto del target.
//!
//! L'indice unico di `#plenora_keys` confronta come quello del target: lo
//! stesso tipo, la stessa collation, i `NULL` uguali (un indice unico non
//! filtrato, l'unico che prova le chiavi: `plan::validate_unique_key`).

use super::plan::{plan_error, sql_identifier, WriteColumnPlan, WritePlan};
use crate::{PooledSqlServerSession, SqlServerColumnKind};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::plan::{ProviderKind, WriteMode};
use plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Renderer};
use tiberius::Query;

/// La tabella temporanea delle chiavi.
const TABLE: &str = "#plenora_keys";

/// I parametri per statement: SQL Server ne ammette 2100.
const PARAMETERS: usize = 2_000;

/// Le righe di un `VALUES`: SQL Server ne ammette 1000.
const ROWS: usize = 1_000;

/// Se la mode confronta le chiavi.
#[must_use]
pub(super) const fn applies(mode: WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
    )
}

fn key_columns(plan: &WritePlan) -> Result<Vec<&WriteColumnPlan>> {
    plan.key_input_indices
        .iter()
        .map(|index| {
            plan.columns.get(*index).ok_or_else(|| {
                plan_error(
                    ErrorCategory::Internal,
                    "indice chiave SQL Server fuori dal piano compilato",
                )
            })
        })
        .collect()
}

const fn renderer() -> Renderer {
    Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
}

/// Lo SQL che crea `#plenora_keys`.
///
/// # Errors
///
/// `internal` per un indice di chiave fuori dal piano; `invalid_plan` per un
/// identificatore o una collation fuori dalla grammatica.
pub(super) fn create_sql(plan: &WritePlan) -> Result<String> {
    let renderer = renderer();
    let mut definitions = Vec::new();
    let mut names = Vec::new();
    for (ordinal, column) in key_columns(plan)?.into_iter().enumerate() {
        if matches!(
            column.kind,
            SqlServerColumnKind::Geometry | SqlServerColumnKind::Geography
        ) {
            return Err(plan_error(
                ErrorCategory::Unsupported,
                "geometry/geography non sono ammesse come chiavi write SQL Server",
            ));
        }
        let name = renderer.quote_identifier(&sql_identifier(&format!("k{ordinal}"))?)?;
        let declaration = super::sql::intermediate_declaration(column)?;
        if column.collation.is_none()
            && matches!(column.native_type.as_str(), "nchar" | "nvarchar" | "ntext")
        {
            return Err(plan_error(
                ErrorCategory::Unsupported,
                "collation della colonna chiave SQL Server non provata",
            ));
        }
        definitions.push(format!("{name} {declaration} NULL"));
        names.push(name);
    }
    Ok(format!(
        "DROP TABLE IF EXISTS {TABLE}; CREATE TABLE {TABLE} ({}); \
         CREATE UNIQUE INDEX [plenora_keys_unique] ON {TABLE} ({});",
        definitions.join(", "),
        names.join(", ")
    ))
}

/// Lo SQL che elimina `#plenora_keys`.
pub(super) const DROP_SQL: &str = "DROP TABLE IF EXISTS #plenora_keys;";

/// Inserisce in `#plenora_keys` le chiavi di `batch`, con lo stesso bind
/// degli statement della riga.
///
/// # Errors
///
/// `data_mapping` con [`DUPLICATE_INPUT_KEYS`] se l'indice unico della tabella
/// rifiuta una chiave: e ripetuta sotto il confronto del target. Gli errori
/// del bind e della sessione, com'erano.
pub(super) async fn register(
    pooled: &mut PooledSqlServerSession,
    plan: &WritePlan,
    batch: &RecordBatch,
    cancellation: &CancellationToken,
) -> Result<()> {
    let columns = key_columns(plan)?;
    if columns.is_empty() || batch.num_rows() == 0 {
        return Ok(());
    }
    let renderer = renderer();
    let names = (0..columns.len())
        .map(|ordinal| renderer.quote_identifier(&sql_identifier(&format!("k{ordinal}"))?))
        .collect::<Result<Vec<_>>>()?;
    let chunk = (PARAMETERS / columns.len()).clamp(1, ROWS);
    let mut start = 0;
    while start < batch.num_rows() {
        let end = (start + chunk).min(batch.num_rows());
        let mut ordinal = 1_usize;
        let mut tuples = Vec::with_capacity(end - start);
        for _ in start..end {
            let expressions = columns
                .iter()
                .map(|column| {
                    let expression = super::sql::placeholder_expression(column, ordinal);
                    ordinal += 1;
                    expression
                })
                .collect::<Result<Vec<_>>>()?;
            tuples.push(format!("({})", expressions.join(", ")));
        }
        let mut query = Query::new(format!(
            "INSERT INTO {TABLE} ({}) VALUES {};",
            names.join(", "),
            tuples.join(", ")
        ));
        for row in start..end {
            for column in &columns {
                super::codec::bind_column(&mut query, plan, column, batch, row)?;
            }
        }
        pooled
            .session_mut()?
            .execute_query(query, ErrorPhase::Write, cancellation)
            .await
            .map_err(duplicate)?;
        start = end;
    }
    Ok(())
}

/// Una violazione dell'indice unico di `#plenora_keys` (2601) e una chiave
/// ripetuta; ogni altro errore resta com'era.
fn duplicate(error: DatabaseError) -> DatabaseError {
    if error.category == ErrorCategory::Conflict {
        let mut mapped = DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Write,
            Some(ProviderKind::Sqlserver),
            format!("scrittura rifiutata: {DUPLICATE_INPUT_KEYS}"),
        );
        mapped.remote_effect = error.remote_effect;
        mapped
    } else {
        error
    }
}

#[cfg(test)]
#[path = "keys_tests.rs"]
mod tests;
