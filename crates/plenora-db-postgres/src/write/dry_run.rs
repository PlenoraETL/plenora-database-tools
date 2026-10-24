//! Il dry run della scrittura: cio che `execute` decide dell'ingresso, senza
//! sessione e senza effetti.
//!
//! Ogni batch passa dalle stesse funzioni della scrittura, nello stesso
//! ordine: schema del batch, prenotazione con la verifica comune dei valori
//! geometrici (`enforce_input_limits` -> `verify_value`), SRID di colonna sui
//! valori `wkb`, codifica dei valori nella forma che la scrittura invierebbe
//! (testo COPY o parametri dell'INSERT). Il risultato si scarta. Fino al 24/10
//! il dry run della CLI compilava soltanto il piano, e un valore che la
//! scrittura rifiuta con `data_mapping` o `crs` passava per buono.
//!
//! Cio che resta della scrittura e del server: vincoli del target (unique,
//! check, foreign key, trigger) e conversioni che il server applica alla
//! colonna.

use super::recovery::{interruption_category, interruption_message, resource_write_error};
use super::resources::reserve_batch_within;
use super::{
    arrow_value, copy_buffer, row_diagnostics, stamp_declared_srid, statement,
    validate_batch_schema, PreparedPostgresWrite,
};
use crate::error::public_error;
use crate::PostgresInsertMode;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use plenora_database_core::plan::{ProviderKind, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::BatchStream;
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

/// I limiti del provider che la scrittura applica a ogni batch.
#[derive(Debug, Clone, Copy)]
pub struct InputLimits {
    pub insert_mode: PostgresInsertMode,
    pub max_batch_bytes: u64,
    pub max_wkb_cell_bytes: u64,
}

/// Quanto ingresso il dry run ha giudicato.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VerifiedInput {
    pub batches: u64,
    pub rows: u64,
}

/// La forma in cui la scrittura codificherebbe i valori.
enum Encoding {
    CopyText,
    Parameters,
}

/// Giudica l'ingresso come `execute`, senza contattare il server.
///
/// # Errors
///
/// Gli errori che la scrittura darebbe sullo stesso ingresso prima di
/// inviarlo al server, con la stessa categoria e la stessa fase, e
/// `remote_effect: none`: un dry run non ha effetti da annullare.
pub async fn verify_input(
    state: &PreparedPostgresWrite,
    operation: &WriteOperation,
    input: &mut dyn BatchStream,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    limits: InputLimits,
) -> Result<VerifiedInput> {
    verify(state, operation, input, budget, cancellation, limits)
        .await
        .map_err(without_effect)
}

async fn verify(
    state: &PreparedPostgresWrite,
    operation: &WriteOperation,
    input: &mut dyn BatchStream,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    limits: InputLimits,
) -> Result<VerifiedInput> {
    let schema = input.schema();
    let plans = &state.column_plans;
    // Lo stesso bivio di `execute`: Append in una sola transazione con un
    // totale dichiarato passa dalla diagnostica row-scoped, che scrive riga
    // per riga con i parametri dell'INSERT.
    if operation.mode == WriteMode::Append
        && operation.transaction_profile == TransactionProfile::SingleTransaction
    {
        if let Some(total) = input.declared_input_rows() {
            row_diagnostics::validate_input(
                &schema,
                &schema,
                operation,
                total,
                input.row_diagnostics_policy(),
            )?;
            return verify_row_scoped(
                state,
                operation,
                &schema,
                input,
                budget,
                cancellation,
                limits,
                total,
            )
            .await;
        }
    }
    let bulk = matches!(
        operation.mode,
        WriteMode::Create | WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert
    );
    let encoding = match (bulk, limits.insert_mode) {
        (true, PostgresInsertMode::CopyText) => Encoding::CopyText,
        (true, PostgresInsertMode::CopyBinary) => {
            // La codifica binaria sceglie la forma di alcuni valori dal tipo
            // della colonna, che si conosce solo dal server: un dry run che la
            // approssimasse direbbe di piu di quanto sa.
            return Err(public_error(
                ErrorCategory::Unsupported,
                ErrorPhase::Validate,
                false,
                "dry run PostgreSQL non disponibile con insert mode CopyBinary: \
                 la codifica dei valori dipende dai tipi del target",
            ));
        }
        _ => Encoding::Parameters,
    };
    let mut verified = VerifiedInput::default();
    loop {
        if cancellation.is_cancelled() {
            return Err(interrupted(cancellation));
        }
        let Some(batch) = input.next_batch(cancellation).await? else {
            break;
        };
        validate_batch_schema(&batch, &schema)?;
        if cancellation.is_cancelled() {
            return Err(interrupted(cancellation));
        }
        // `execute` riporta i rifiuti della prenotazione in fase `write`
        // (`rollback_resource_error`).
        let resources = reserve_batch_within(
            &batch,
            plans,
            limits.max_batch_bytes,
            limits.max_wkb_cell_bytes,
            budget,
        )
        .map_err(|error| resource_write_error(&error, true))?;
        let batch = stamp_declared_srid(&batch, plans, budget.limits().nesting_depth)?;
        let rows = resources.rows;
        if rows == 0 {
            continue;
        }
        match encoding {
            Encoding::CopyText => {
                copy_buffer(&batch, plans)?;
            }
            Encoding::Parameters => {
                let (_, indexes) =
                    statement(operation, &operation.target, batch.schema_ref(), plans)?;
                encode_parameters(&batch, plans, &indexes, 0..batch.num_rows())?;
            }
        }
        resources.commit()?;
        count(&mut verified, rows)?;
    }
    Ok(verified)
}

/// Il percorso row-scoped: le righe nell'ordine della sorgente, ciascuna con
/// i parametri dell'INSERT diagnostico, e il totale dichiarato rispettato.
#[allow(clippy::too_many_arguments)]
async fn verify_row_scoped(
    state: &PreparedPostgresWrite,
    operation: &WriteOperation,
    schema: &SchemaRef,
    input: &mut dyn BatchStream,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    limits: InputLimits,
    declared: u64,
) -> Result<VerifiedInput> {
    let plans = &state.column_plans;
    let (_, indexes) = statement(operation, &operation.target, schema, plans)?;
    let mut verified = VerifiedInput::default();
    loop {
        let batch = input
            .next_batch(cancellation)
            .await
            .map_err(row_diagnostics::provider_write_error)?;
        let Some(batch) = batch else {
            break;
        };
        validate_batch_schema(&batch, schema)?;
        if batch.num_rows() == 0 {
            continue;
        }
        if verified.rows >= declared {
            return Err(row_diagnostics::input_beyond_declared_error());
        }
        let resources = reserve_batch_within(
            &batch,
            plans,
            limits.max_batch_bytes,
            limits.max_wkb_cell_bytes,
            budget,
        )?;
        resources.commit()?;
        let batch = stamp_declared_srid(&batch, plans, budget.limits().nesting_depth)?;
        let rows = u64::try_from(batch.num_rows())
            .map_err(|_| DatabaseError::resource_limit("batch oltre il conteggio supportato"))?;
        // Le righe oltre il totale dichiarato non si scrivono: la scrittura
        // si ferma al totale e rifiuta cio che resta.
        let applicable = usize::try_from(declared.saturating_sub(verified.rows).min(rows))
            .map_err(|_| DatabaseError::resource_limit("batch oltre il conteggio supportato"))?;
        encode_parameters(&batch, plans, &indexes, 0..applicable)?;
        if u64::try_from(applicable).unwrap_or(u64::MAX) < rows {
            return Err(row_diagnostics::input_beyond_declared_error());
        }
        count(&mut verified, rows)?;
    }
    if verified.rows < declared {
        return Err(row_diagnostics::short_input_error());
    }
    Ok(verified)
}

fn encode_parameters(
    batch: &RecordBatch,
    plans: &[super::WriteColumnPlan],
    indexes: &[usize],
    rows: std::ops::Range<usize>,
) -> Result<()> {
    for row in rows {
        for index in indexes {
            arrow_value(batch.column(*index).as_ref(), &plans[*index], row)?;
        }
    }
    Ok(())
}

fn count(verified: &mut VerifiedInput, rows: u64) -> Result<()> {
    let overflow = || DatabaseError::resource_limit("overflow nel conteggio del dry run");
    verified.batches = verified.batches.checked_add(1).ok_or_else(overflow)?;
    verified.rows = verified.rows.checked_add(rows).ok_or_else(overflow)?;
    Ok(())
}

fn interrupted(cancellation: &CancellationToken) -> DatabaseError {
    public_error(
        interruption_category(cancellation),
        ErrorPhase::Write,
        false,
        interruption_message(cancellation),
    )
}

/// La busta della scrittura, senza l'effetto: niente e stato scritto, quindi
/// niente e stato annullato, e il `retry` della classe resta quello. Un
/// errore che nella scrittura chiederebbe di verificare lo stato remoto
/// (`requires_recovery`) qui non ha stato remoto da verificare.
fn without_effect(mut error: DatabaseError) -> DatabaseError {
    error.provider = Some(ProviderKind::Postgres);
    error.remote_effect = RemoteEffect::None;
    if error.retry == RetryDisposition::RequiresRecovery {
        error.retry = RetryDisposition::Never;
    }
    error
}
