//! Nessun batch viene consegnato dopo un avviso non esatto. UTF-8 valido
//! non prova che il driver non abbia sostituito caratteri prima del decoder.

use odbc_api::handles::Diagnostics;
use odbc_api::{BlockCursor, Cursor, RowSetBuffer};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};

pub fn column_name(cursor: &mut impl Cursor, ordinal: u16) -> Result<String> {
    use odbc_api::handles::{slice_to_utf8, Statement};
    let mut statement = cursor.as_stmt_ref();
    let mut buffer = vec![0; 128];
    statement
        .col_name_strict(ordinal, &mut buffer)
        .into_result_strict(&statement, "SQLColAttribute")
        .map_err(|error| crate::error::driver_error(&error, ErrorPhase::Read))?;
    check(&statement, ErrorPhase::Read)?;
    slice_to_utf8(&buffer).map_err(|_| {
        DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Read,
            Some(ProviderKind::Db2),
            "nome colonna Db2 con codifica non valida",
        )
    })
}

pub fn check(diagnostics: &impl Diagnostics, phase: ErrorPhase) -> Result<()> {
    check_records(diagnostics, phase, false)
}

fn check_records(
    diagnostics: &impl Diagnostics,
    phase: ErrorPhase,
    end_of_cursor: bool,
) -> Result<()> {
    // Non chiediamo il testo diagnostico: puo contenere valori applicativi.
    for number in 1..=i16::MAX {
        let Some(record) = diagnostics.diagnostic_record(number, &mut []) else {
            break;
        };
        if !record.state.prevents_exact_result()
            || (end_of_cursor && record.state.as_str() == "02000")
        {
            continue;
        }
        return Err(DatabaseError::new(
            ErrorCategory::DataMapping,
            phase,
            Some(ProviderKind::Db2),
            format!(
                "lettura Db2 non esatta (SQLSTATE {}, codice {})",
                record.state.as_str(),
                record.native_error
            ),
        ));
    }
    Ok(())
}

pub fn fetch<C: Cursor, B: RowSetBuffer>(
    cursor: &mut BlockCursor<C, B>,
    phase: ErrorPhase,
) -> Result<Option<&B>> {
    let (batch, diagnostics) = cursor
        .fetch_with_diagnostics()
        .map_err(|error| crate::error::driver_error(&error, phase))?;
    checked_batch(batch, &diagnostics, phase)
}

/// `02000` descrive la fine cursore solo senza batch. Gli avvisi informativi
/// possono accompagnarlo; ogni diagnostico non esatto resta bloccante.
pub fn checked_batch<'a, B>(
    batch: Option<&'a B>,
    diagnostics: &impl Diagnostics,
    phase: ErrorPhase,
) -> Result<Option<&'a B>> {
    check_records(diagnostics, phase, batch.is_none())?;
    Ok(batch)
}
