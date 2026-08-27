use super::plan::{WriteColumnPlan, WritePlan};
use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveTime, Utc};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, UInt8Array,
};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::ewkb::inspect_ewkb_detailed;
use plenora_database_core::protocol;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use tiberius::{IntoSql, Query, TokenRow};

#[derive(Debug)]
pub(super) struct BatchInspection {
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) geometry_components: u64,
}

#[derive(Debug)]
pub(super) struct RowInspection {
    pub(super) bytes: u64,
    pub(super) geometry_components: u64,
}

pub(super) fn inspect_row(
    batch: &RecordBatch,
    row: usize,
    plan: &WritePlan,
    cell_limit: u64,
    component_limit: u64,
    nesting_depth: u64,
) -> Result<RowInspection> {
    if batch.schema().as_ref() != plan.input_schema.as_ref() || row >= batch.num_rows() {
        return Err(mapping_error(
            "riga Arrow incompatibile col piano preparato SQL Server",
        ));
    }
    let mut bytes = 0_u64;
    let mut geometry_components = 0_u64;
    for &column_index in &plan.key_input_indices {
        let column = plan
            .columns
            .get(column_index)
            .ok_or_else(|| mapping_error("indice chiave SQL Server fuori dal piano compilato"))?;
        if batch.column(column.input_index).is_null(row) {
            return Err(mapping_error(
                "NULL Arrow non ammesso in una chiave write SQL Server",
            ));
        }
    }
    for column in &plan.columns {
        let array = batch.column(column.input_index);
        if !column.nullable && array.is_null(row) {
            return Err(mapping_error(
                "NULL Arrow destinato a colonna SQL Server NOT NULL",
            ));
        }
        let value_bytes = match column.kind {
            crate::SqlServerColumnKind::Utf8 | crate::SqlServerColumnKind::TimestampTz => {
                let strings = downcast::<StringArray>(array.as_ref())?;
                (!strings.is_null(row)).then(|| strings.value(row).len())
            }
            crate::SqlServerColumnKind::Binary => {
                let binary = downcast::<BinaryArray>(array.as_ref())?;
                (!binary.is_null(row)).then(|| binary.value(row).len())
            }
            crate::SqlServerColumnKind::Geometry | crate::SqlServerColumnKind::Geography => {
                let binary = downcast::<BinaryArray>(array.as_ref())?;
                if binary.is_null(row) {
                    None
                } else {
                    let value = binary.value(row);
                    enforce_cell(value.len(), cell_limit)?;
                    let inspection = inspect_ewkb_detailed(value, component_limit, nesting_depth)?;
                    if inspection.root.srid.is_some() {
                        return Err(mapping_error(
                            "write SQL Server richiede WKB senza SRID embedded",
                        ));
                    }
                    let expected_dimensions = plan
                        .input_schema
                        .field(column.input_index)
                        .metadata()
                        .get(protocol::GEOMETRY_DIMENSIONS)
                        .map(String::as_str)
                        .ok_or_else(|| {
                            mapping_error("dimensioni spatial assenti dal piano Arrow")
                        })?;
                    if inspection.root.dimensions_label() != expected_dimensions {
                        return Err(mapping_error("dimensioni WKB diverse dal contratto Arrow"));
                    }
                    geometry_components = geometry_components
                        .checked_add(inspection.stats.components)
                        .ok_or_else(|| {
                            DatabaseError::resource_limit("componenti geometriche overflow")
                        })?;
                    Some(value.len())
                }
            }
            crate::SqlServerColumnKind::Bool | crate::SqlServerColumnKind::U8 => Some(1),
            crate::SqlServerColumnKind::I16 => Some(2),
            crate::SqlServerColumnKind::I32
            | crate::SqlServerColumnKind::F32
            | crate::SqlServerColumnKind::Date => Some(4),
            crate::SqlServerColumnKind::I64
            | crate::SqlServerColumnKind::F64
            | crate::SqlServerColumnKind::Time
            | crate::SqlServerColumnKind::Timestamp => Some(8),
            crate::SqlServerColumnKind::Decimal { .. } => Some(16),
        };
        if let Some(value_bytes) = value_bytes {
            enforce_cell(value_bytes, cell_limit)?;
            bytes = bytes
                .checked_add(u64::try_from(value_bytes).map_err(|_| {
                    DatabaseError::resource_limit("dimensione cella non rappresentabile")
                })?)
                .ok_or_else(|| DatabaseError::resource_limit("dimensione riga Arrow overflow"))?;
        }
    }
    Ok(RowInspection {
        bytes,
        geometry_components,
    })
}

pub(super) fn inspect_batch(
    batch: &RecordBatch,
    plan: &WritePlan,
    cell_limit: u64,
    component_limit: u64,
    nesting_depth: u64,
) -> Result<BatchInspection> {
    if batch.schema().as_ref() != plan.input_schema.as_ref() {
        return Err(mapping_error(
            "schema Arrow cambiato dopo prepare_write SQL Server",
        ));
    }
    let rows = u64::try_from(batch.num_rows())
        .map_err(|_| DatabaseError::resource_limit("numero righe non rappresentabile"))?;
    let bytes = batch.columns().iter().try_fold(0_u64, |total, array| {
        let bytes = u64::try_from(array.get_array_memory_size())
            .map_err(|_| DatabaseError::resource_limit("dimensione batch non rappresentabile"))?;
        total
            .checked_add(bytes)
            .ok_or_else(|| DatabaseError::resource_limit("dimensione batch Arrow overflow"))
    })?;
    let mut geometry_components = 0_u64;
    for &column_index in &plan.key_input_indices {
        let column = plan
            .columns
            .get(column_index)
            .ok_or_else(|| mapping_error("indice chiave SQL Server fuori dal piano compilato"))?;
        if batch.column(column.input_index).null_count() > 0 {
            return Err(mapping_error(
                "NULL Arrow non ammesso in una chiave write SQL Server",
            ));
        }
    }
    for column in &plan.columns {
        let array = batch.column(column.input_index);
        if !column.nullable && array.null_count() > 0 {
            return Err(mapping_error(
                "NULL Arrow destinato a colonna SQL Server NOT NULL",
            ));
        }
        match column.kind {
            crate::SqlServerColumnKind::Utf8 => {
                let strings = downcast::<StringArray>(array.as_ref())?;
                for value in strings.iter().flatten() {
                    enforce_cell(value.len(), cell_limit)?;
                }
            }
            crate::SqlServerColumnKind::Binary => {
                let binary = downcast::<BinaryArray>(array.as_ref())?;
                for value in binary.iter().flatten() {
                    enforce_cell(value.len(), cell_limit)?;
                }
            }
            crate::SqlServerColumnKind::Geometry | crate::SqlServerColumnKind::Geography => {
                let binary = downcast::<BinaryArray>(array.as_ref())?;
                for value in binary.iter().flatten() {
                    enforce_cell(value.len(), cell_limit)?;
                    let remaining = component_limit
                        .checked_sub(geometry_components)
                        .ok_or_else(|| {
                            DatabaseError::resource_limit("budget componenti geometriche esaurito")
                        })?;
                    if remaining == 0 {
                        return Err(DatabaseError::resource_limit(
                            "budget componenti geometriche esaurito",
                        ));
                    }
                    let inspection = inspect_ewkb_detailed(value, remaining, nesting_depth)?;
                    if inspection.root.srid.is_some() {
                        return Err(mapping_error(
                            "write SQL Server richiede WKB senza SRID embedded",
                        ));
                    }
                    let expected_dimensions = plan
                        .input_schema
                        .field(column.input_index)
                        .metadata()
                        .get(protocol::GEOMETRY_DIMENSIONS)
                        .map(String::as_str)
                        .ok_or_else(|| {
                            mapping_error("dimensioni spatial assenti dal piano Arrow")
                        })?;
                    if inspection.root.dimensions_label() != expected_dimensions {
                        return Err(mapping_error("dimensioni WKB diverse dal contratto Arrow"));
                    }
                    geometry_components = geometry_components
                        .checked_add(inspection.stats.components)
                        .ok_or_else(|| {
                            DatabaseError::resource_limit("componenti geometriche overflow")
                        })?;
                }
            }
            _ => {}
        }
    }
    Ok(BatchInspection {
        rows,
        bytes,
        geometry_components,
    })
}

#[allow(clippy::too_many_lines)]
pub(super) fn bind_row(
    plan: &WritePlan,
    batch: &RecordBatch,
    row: usize,
) -> Result<Query<'static>> {
    let mut query = Query::new(plan.row_sql.clone());
    for column in &plan.columns {
        let array = batch.column(column.input_index);
        match column.kind {
            crate::SqlServerColumnKind::Bool => {
                query.bind(optional_scalar::<BooleanArray, bool>(
                    array.as_ref(),
                    row,
                    BooleanArray::value,
                )?);
            }
            crate::SqlServerColumnKind::U8 => {
                query.bind(optional_scalar::<UInt8Array, u8>(
                    array.as_ref(),
                    row,
                    UInt8Array::value,
                )?);
            }
            crate::SqlServerColumnKind::I16 => {
                query.bind(optional_scalar::<Int16Array, i16>(
                    array.as_ref(),
                    row,
                    Int16Array::value,
                )?);
            }
            crate::SqlServerColumnKind::I32 => {
                query.bind(optional_scalar::<Int32Array, i32>(
                    array.as_ref(),
                    row,
                    Int32Array::value,
                )?);
            }
            crate::SqlServerColumnKind::I64 => {
                query.bind(optional_scalar::<Int64Array, i64>(
                    array.as_ref(),
                    row,
                    Int64Array::value,
                )?);
            }
            crate::SqlServerColumnKind::F32 => {
                query.bind(optional_scalar::<Float32Array, f32>(
                    array.as_ref(),
                    row,
                    Float32Array::value,
                )?);
            }
            crate::SqlServerColumnKind::F64 => {
                query.bind(optional_scalar::<Float64Array, f64>(
                    array.as_ref(),
                    row,
                    Float64Array::value,
                )?);
            }
            crate::SqlServerColumnKind::Utf8 => {
                let values = downcast::<StringArray>(array.as_ref())?;
                query.bind((!values.is_null(row)).then(|| values.value(row).to_owned()));
            }
            crate::SqlServerColumnKind::Binary => {
                let values = downcast::<BinaryArray>(array.as_ref())?;
                query.bind((!values.is_null(row)).then(|| values.value(row).to_vec()));
            }
            crate::SqlServerColumnKind::Date => {
                let values = downcast::<Date32Array>(array.as_ref())?;
                let value = (!values.is_null(row))
                    .then(|| date32(values.value(row)))
                    .transpose()?;
                query.bind(value);
            }
            crate::SqlServerColumnKind::Time => {
                let values = downcast::<Time64MicrosecondArray>(array.as_ref())?;
                let value = (!values.is_null(row))
                    .then(|| time64(values.value(row)))
                    .transpose()?;
                query.bind(value);
            }
            crate::SqlServerColumnKind::Timestamp => {
                let values = downcast::<TimestampMicrosecondArray>(array.as_ref())?;
                let value = (!values.is_null(row))
                    .then(|| timestamp(values.value(row)).map(|value| value.naive_utc()))
                    .transpose()?;
                query.bind(value);
            }
            crate::SqlServerColumnKind::TimestampTz => {
                let values = downcast::<StringArray>(array.as_ref())?;
                let value = (!values.is_null(row)).then(|| values.value(row).to_owned());
                query.bind(value);
            }
            crate::SqlServerColumnKind::Decimal { scale, .. } => {
                let values = downcast::<Decimal128Array>(array.as_ref())?;
                let value = (!values.is_null(row))
                    .then(|| decimal_string(values.value(row), scale))
                    .transpose()?;
                query.bind(value);
            }
            crate::SqlServerColumnKind::Geometry | crate::SqlServerColumnKind::Geography => {
                bind_spatial(&mut query, array.as_ref(), row, column)?;
            }
        }
    }
    Ok(query)
}

/// Materializza soltanto i descrittori TDS; stringhe e binari restano
/// borrowed dal batch Arrow. L'intera conversione avviene prima di aprire il
/// comando bulk, così un errore di mapping non lascia un flusso TDS parziale.
#[allow(clippy::too_many_lines)]
pub(super) fn bulk_rows<'a>(plan: &WritePlan, batch: &'a RecordBatch) -> Result<Vec<TokenRow<'a>>> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row_index in 0..batch.num_rows() {
        let mut row = TokenRow::with_capacity(plan.columns.len());
        for column in &plan.columns {
            let array = batch.column(column.input_index);
            match column.kind {
                crate::SqlServerColumnKind::Bool => row.push(
                    optional_scalar::<BooleanArray, bool>(
                        array.as_ref(),
                        row_index,
                        BooleanArray::value,
                    )?
                    .into_sql(),
                ),
                crate::SqlServerColumnKind::U8 => row.push(
                    optional_scalar::<UInt8Array, u8>(
                        array.as_ref(),
                        row_index,
                        UInt8Array::value,
                    )?
                    .into_sql(),
                ),
                crate::SqlServerColumnKind::I16 => row.push(
                    optional_scalar::<Int16Array, i16>(
                        array.as_ref(),
                        row_index,
                        Int16Array::value,
                    )?
                    .into_sql(),
                ),
                crate::SqlServerColumnKind::I32 => row.push(
                    optional_scalar::<Int32Array, i32>(
                        array.as_ref(),
                        row_index,
                        Int32Array::value,
                    )?
                    .into_sql(),
                ),
                crate::SqlServerColumnKind::I64 => row.push(
                    optional_scalar::<Int64Array, i64>(
                        array.as_ref(),
                        row_index,
                        Int64Array::value,
                    )?
                    .into_sql(),
                ),
                crate::SqlServerColumnKind::F32 => row.push(
                    optional_scalar::<Float32Array, f32>(
                        array.as_ref(),
                        row_index,
                        Float32Array::value,
                    )?
                    .into_sql(),
                ),
                crate::SqlServerColumnKind::F64 => row.push(
                    optional_scalar::<Float64Array, f64>(
                        array.as_ref(),
                        row_index,
                        Float64Array::value,
                    )?
                    .into_sql(),
                ),
                crate::SqlServerColumnKind::Utf8 if column.native_type == "uniqueidentifier" => {
                    let values = downcast::<StringArray>(array.as_ref())?;
                    let value = (!values.is_null(row_index))
                        .then(|| {
                            tiberius::Uuid::parse_str(values.value(row_index))
                                .map_err(|_| mapping_error("UUID SQL Server non valido"))
                        })
                        .transpose()?;
                    row.push(value.into_sql());
                }
                crate::SqlServerColumnKind::Utf8 => {
                    let values = downcast::<StringArray>(array.as_ref())?;
                    row.push(
                        (!values.is_null(row_index))
                            .then(|| values.value(row_index))
                            .into_sql(),
                    );
                }
                crate::SqlServerColumnKind::Binary => {
                    let values = downcast::<BinaryArray>(array.as_ref())?;
                    row.push(
                        (!values.is_null(row_index))
                            .then(|| values.value(row_index))
                            .into_sql(),
                    );
                }
                crate::SqlServerColumnKind::Date => {
                    return Err(mapping_error(
                        "date SQL Server non ammesso nel codec TDS bulk verificato",
                    ))
                }
                crate::SqlServerColumnKind::Time => {
                    let values = downcast::<Time64MicrosecondArray>(array.as_ref())?;
                    let value = (!values.is_null(row_index))
                        .then(|| time64(values.value(row_index)))
                        .transpose()?;
                    row.push(value.into_sql());
                }
                crate::SqlServerColumnKind::Timestamp => {
                    let values = downcast::<TimestampMicrosecondArray>(array.as_ref())?;
                    let value = (!values.is_null(row_index))
                        .then(|| timestamp(values.value(row_index)).map(|value| value.naive_utc()))
                        .transpose()?;
                    row.push(value.into_sql());
                }
                crate::SqlServerColumnKind::TimestampTz => {
                    let values = downcast::<StringArray>(array.as_ref())?;
                    let value = (!values.is_null(row_index))
                        .then(|| datetimeoffset(values.value(row_index)))
                        .transpose()?;
                    row.push(value.into_sql());
                }
                crate::SqlServerColumnKind::Decimal { scale, .. } => {
                    let values = downcast::<Decimal128Array>(array.as_ref())?;
                    let scale = u8::try_from(scale)
                        .map_err(|_| mapping_error("scala decimal TDS bulk non valida"))?;
                    if scale >= 38 {
                        return Err(mapping_error("scala decimal TDS bulk oltre 37"));
                    }
                    let value = (!values.is_null(row_index)).then(|| {
                        tiberius::numeric::Numeric::new_with_scale(values.value(row_index), scale)
                    });
                    row.push(value.into_sql());
                }
                crate::SqlServerColumnKind::Geometry | crate::SqlServerColumnKind::Geography => {
                    return Err(mapping_error(
                        "spatial SQL Server non ammesso nel codec TDS bulk",
                    ));
                }
            }
        }
        rows.push(row);
    }
    Ok(rows)
}

fn bind_spatial(
    query: &mut Query<'static>,
    array: &dyn Array,
    row: usize,
    column: &WriteColumnPlan,
) -> Result<()> {
    let values = downcast::<BinaryArray>(array)?;
    query.bind((!values.is_null(row)).then(|| values.value(row).to_vec()));
    let srid = column
        .spatial_srid
        .ok_or_else(|| mapping_error("SRID spatial SQL Server assente dal piano"))?;
    query.bind(
        i32::try_from(srid)
            .map_err(|_| mapping_error("SRID spatial SQL Server oltre il range int"))?,
    );
    Ok(())
}

fn optional_scalar<A, T>(
    array: &dyn Array,
    row: usize,
    value: impl FnOnce(&A, usize) -> T,
) -> Result<Option<T>>
where
    A: Array + 'static,
{
    let typed = downcast::<A>(array)?;
    Ok((!typed.is_null(row)).then(|| value(typed, row)))
}

fn downcast<A: Array + 'static>(array: &dyn Array) -> Result<&A> {
    array
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| mapping_error("array Arrow incompatibile col piano SQL Server"))
}

fn date32(days: i32) -> Result<NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .ok_or_else(|| mapping_error("epoch Date32 non rappresentabile"))?;
    epoch
        .checked_add_signed(Duration::days(i64::from(days)))
        .ok_or_else(|| mapping_error("Date32 oltre il range SQL Server/chrono"))
}

fn time64(microseconds: i64) -> Result<NaiveTime> {
    const DAY_MICROSECONDS: i64 = 86_400_000_000;
    if !(0..DAY_MICROSECONDS).contains(&microseconds) {
        return Err(mapping_error("Time64 oltre il giorno SQL Server"));
    }
    let seconds = u32::try_from(microseconds / 1_000_000)
        .map_err(|_| mapping_error("Time64 secondi non rappresentabili"))?;
    let nanos = u32::try_from((microseconds % 1_000_000) * 1_000)
        .map_err(|_| mapping_error("Time64 frazione non rappresentabile"))?;
    NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanos)
        .ok_or_else(|| mapping_error("Time64 SQL Server non valido"))
}

fn timestamp(microseconds: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp_micros(microseconds)
        .ok_or_else(|| mapping_error("timestamp Arrow oltre il range SQL Server/chrono"))
}

fn datetimeoffset(value: &str) -> Result<DateTime<FixedOffset>> {
    DateTime::parse_from_rfc3339(value)
        .map_err(|_| mapping_error("datetimeoffset SQL Server non valido"))
}

fn decimal_string(value: i128, scale: i8) -> Result<String> {
    let negative = value.is_negative();
    let digits = value.unsigned_abs().to_string();
    let scale = usize::try_from(scale)
        .map_err(|_| mapping_error("scala Decimal128 negativa non supportata"))?;
    let body = if scale == 0 {
        digits
    } else if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Ok(if negative { format!("-{body}") } else { body })
}

fn enforce_cell(length: usize, limit: u64) -> Result<()> {
    if u64::try_from(length)
        .map_err(|_| DatabaseError::resource_limit("dimensione cella non rappresentabile"))?
        > limit
    {
        return Err(DatabaseError::resource_limit(
            "cella Arrow oltre il limite write SQL Server",
        ));
    }
    Ok(())
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        ErrorCategory::DataMapping,
        ErrorPhase::Write,
        Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        message,
    )
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod tests;
