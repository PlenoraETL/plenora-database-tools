//! Nessun batch viene consegnato dopo un avviso del driver. UTF-8 valido
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
        .col_name(ordinal, &mut buffer)
        .into_result(&statement)
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
    // Non chiediamo il testo diagnostico: puo contenere valori applicativi.
    if let Some(record) = diagnostics.diagnostic_record(1, &mut []) {
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

/// `SQL_NO_DATA` non consegna celle: il solo 02000 descrive la fine cursore,
/// non una conversione. Non tollerarlo su un batch, ne insieme ad altri record.
pub fn checked_batch<'a, B>(
    batch: Option<&'a B>,
    diagnostics: &impl Diagnostics,
    phase: ErrorPhase,
) -> Result<Option<&'a B>> {
    if batch.is_none()
        && diagnostics
            .diagnostic_record(1, &mut [])
            .is_some_and(|record| record.state.as_str() == "02000")
        && diagnostics.diagnostic_record(2, &mut []).is_none()
    {
        return Ok(None);
    }
    check(diagnostics, phase)?;
    Ok(batch)
}
