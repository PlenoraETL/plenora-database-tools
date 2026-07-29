mod probe;
mod schema;

pub use probe::{list_objects, list_schemas, probe_server, SqlServerObjectSummary, SqlServerProbe};
pub use schema::{
    describe_object, SqlServerColumn, SqlServerConstraint, SqlServerIndex,
    SqlServerObjectDescription, SqlServerSchemaToken,
};

use crate::error::driver_error;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use tiberius::{FromSql, Row};

fn one_result(mut results: Vec<Vec<Row>>) -> Result<Vec<Row>> {
    if results.len() != 1 {
        return Err(mapping_error(
            "risposta catalogo SQL Server con numero di result set inatteso",
        ));
    }
    results
        .pop()
        .ok_or_else(|| mapping_error("risposta catalogo SQL Server priva del result set atteso"))
}

fn required<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<T>
where
    T: FromSql<'a>,
{
    optional(row, index, field)?.ok_or_else(|| {
        mapping_error(format!(
            "campo catalogo SQL Server obbligatorio assente: {field}"
        ))
    })
}

fn optional<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<Option<T>>
where
    T: FromSql<'a>,
{
    row.try_get(index).map_err(|error| {
        let mut public = driver_error(&error, ErrorPhase::Probe, RemoteEffect::None);
        public.message = format!("tipo catalogo SQL Server incompatibile per il campo {field}");
        public
    })
}

fn text(row: &Row, index: usize, field: &'static str) -> Result<String> {
    required::<&str>(row, index, field).map(ToOwned::to_owned)
}

fn optional_text(row: &Row, index: usize, field: &'static str) -> Result<Option<String>> {
    optional::<&str>(row, index, field).map(|value| value.map(ToOwned::to_owned))
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Probe,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
    }
}
