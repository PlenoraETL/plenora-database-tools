use crate::config::Db2Config;
use crate::error::{driver_error, interruption_error, task_error};
use odbc_api::buffers::TextRowSet;
use odbc_api::{Connection, ConnectionOptions, Cursor, IntoParameter};
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::provider::SecretString;
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};

const PROBE_SQL: &str = "SELECT CURRENT SERVER, SERVICE_LEVEL \
    FROM TABLE (SYSPROC.ENV_GET_INST_INFO()) AS instance_info \
    FETCH FIRST 1 ROW ONLY";
const SPATIAL_PROBE_SQL: &str = "VALUES (\
    LENGTH(ST_ASBINARY(ST_GEOMETRY('POINT (1 2)', 4326))), \
    ST_SRID(ST_GEOMETRY('POINT (1 2)', 4326)), \
    ST_COORDDIM(ST_GEOMETRY('POINT Z (1 2 3)', 4326)), \
    ST_INTERSECTS(ST_GEOMETRY('POINT (1 2)', 4326), ST_GEOMETRY('POINT (1 2)', 4326)), \
    ST_CONTAINS(ST_GEOMETRY('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))', 4326), ST_GEOMETRY('POINT (1 2)', 4326)), \
    ST_WITHIN(ST_GEOMETRY('POINT (1 2)', 4326), ST_GEOMETRY('POLYGON ((0 0, 3 0, 3 3, 0 3, 0 0))', 4326))\
)";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Db2Probe {
    pub database: String,
    pub server_version: String,
    pub spatial: bool,
}

pub async fn probe(
    config: &Db2Config,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Result<Db2Probe> {
    if cancellation.is_cancelled() {
        return Err(interruption_error(cancellation, ErrorPhase::Connect));
    }
    let config = config.clone();
    let secret = secret.clone();
    let mut task = tokio::task::spawn_blocking(move || probe_blocking(&config, &secret));
    tokio::select! {
        outcome = &mut task => outcome.map_err(|_| task_error(ErrorPhase::Connect))?,
        _ = cancellation.cancelled() => Err(interruption_error(cancellation, ErrorPhase::Connect)),
    }
}

fn probe_blocking(config: &Db2Config, secret: &SecretString) -> Result<Db2Probe> {
    with_connection(config, secret, |connection, timeout| {
        let rows = query_text(connection, PROBE_SQL, &[], timeout, 2, 128)?;
        let row = exactly_one(rows, "probe Db2 senza una riga univoca")?;
        Ok(Db2Probe {
            database: required_text(&row, 0, "probe Db2 con database assente")?,
            server_version: required_text(&row, 1, "probe Db2 con versione assente")?,
            spatial: probe_spatial(connection, timeout).unwrap_or(false),
        })
    })
}

fn probe_spatial(connection: &Connection<'_>, timeout: usize) -> Result<bool> {
    let rows = query_text(connection, SPATIAL_PROBE_SQL, &[], timeout, 6, 32)?;
    let row = exactly_one(rows, "probe spatial Db2 senza una riga univoca")?;
    let expected = ["21", "4326", "3", "1", "1", "1"];
    Ok(expected.iter().enumerate().all(|(index, expected)| {
        row.get(index)
            .and_then(|value| value.as_deref())
            .is_some_and(|value| value.trim() == *expected)
    }))
}

pub fn with_connection<T>(
    config: &Db2Config,
    secret: &SecretString,
    operation: impl FnOnce(&Connection<'_>, usize) -> Result<T>,
) -> Result<T> {
    let (connection, timeout) = open_connection(config, secret)?;
    operation(&connection, timeout)
}

pub fn open_connection(
    config: &Db2Config,
    secret: &SecretString,
) -> Result<(Connection<'static>, usize)> {
    let environment =
        odbc_api::environment().map_err(|error| driver_error(&error, ErrorPhase::Connect))?;
    let connection_string = config.connection_string(secret)?;
    let options = ConnectionOptions {
        login_timeout_sec: Some(u32::try_from(config.connect_timeout().as_secs()).map_err(
            |_| {
                DatabaseError::new(
                    ErrorCategory::InvalidConfiguration,
                    ErrorPhase::Validate,
                    Some(ProviderKind::Db2),
                    "timeout connessione Db2 non rappresentabile",
                )
            },
        )?),
        packet_size: None,
    };
    let connection = environment
        .connect_with_connection_string(&connection_string, options)
        .map_err(|error| driver_error(&error, ErrorPhase::Connect))?;
    let timeout = usize::try_from(config.operation_timeout().as_secs()).map_err(|_| {
        DatabaseError::new(
            ErrorCategory::InvalidConfiguration,
            ErrorPhase::Validate,
            Some(ProviderKind::Db2),
            "timeout operazione Db2 non rappresentabile",
        )
    })?;
    Ok((connection, timeout))
}

pub fn query_text(
    connection: &Connection<'_>,
    sql: &str,
    parameters: &[&str],
    timeout: usize,
    expected_columns: usize,
    max_cell_bytes: usize,
) -> Result<Vec<Vec<Option<String>>>> {
    let parameters: Vec<_> = parameters
        .iter()
        .map(|parameter| parameter.into_parameter())
        .collect();
    let cursor = connection
        .execute(sql, parameters.as_slice(), Some(timeout))
        .map_err(|error| driver_error(&error, ErrorPhase::Probe))?
        .ok_or_else(|| malformed_probe("query catalogo Db2 senza result set"))?;
    // Il CLI IBM restituisce SQL_NO_TOTAL su SQLGetData anche per queste
    // colonne corte. Un rowset associato usa SQLBindCol, evita quel percorso
    // incompatibile e impone contemporaneamente un limite verificabile.
    let buffer =
        TextRowSet::from_max_str_lens(64, std::iter::repeat_n(max_cell_bytes, expected_columns))
            .map_err(|error| driver_error(&error, ErrorPhase::Probe))?;
    let mut cursor = cursor
        .bind_buffer(buffer)
        .map_err(|error| driver_error(&error, ErrorPhase::Probe))?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor
        .fetch()
        .map_err(|error| driver_error(&error, ErrorPhase::Probe))?
    {
        for row_index in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_cols());
            for column in 0..batch.num_cols() {
                if batch
                    .indicator_at(column, row_index)
                    .is_truncated(batch.max_len(column))
                {
                    return Err(malformed_probe(format!(
                        "query catalogo Db2 con testo troncato (colonna {}, capacita {}, indicatore {:?}, ricevuti {})",
                        column + 1,
                        batch.max_len(column),
                        batch.indicator_at(column, row_index),
                        batch.at(column, row_index).map_or(0, <[u8]>::len),
                    )));
                }
                let value = batch
                    .at(column, row_index)
                    .map(|value| {
                        std::str::from_utf8(value)
                            .map(str::to_owned)
                            .map_err(|_| malformed_probe("query catalogo Db2 con testo non UTF-8"))
                    })
                    .transpose()?;
                row.push(value);
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

pub fn exactly_one<T>(mut rows: Vec<T>, message: &'static str) -> Result<T> {
    if rows.len() != 1 {
        return Err(malformed_probe(message));
    }
    Ok(rows.remove(0))
}

pub fn required_text(
    row: &[Option<String>],
    column: usize,
    message: &'static str,
) -> Result<String> {
    row.get(column)
        .and_then(|value| value.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| malformed_probe(message))
}

fn malformed_probe(message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::Protocol,
        ErrorPhase::Probe,
        Some(ProviderKind::Db2),
        message,
    )
}
