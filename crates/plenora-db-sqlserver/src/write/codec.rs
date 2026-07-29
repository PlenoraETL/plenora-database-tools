use super::plan::{WriteColumnPlan, WritePlan};
use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray, UInt8Array,
};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::ewkb::inspect_ewkb_detailed;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use tiberius::Query;

pub(super) struct BatchInspection {
    pub(super) rows: u64,
    pub(super) bytes: u64,
    pub(super) geometry_components: u64,
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
                    if inspection.root.srid.is_some()
                        || inspection.root.has_z
                        || inspection.root.has_m
                    {
                        return Err(mapping_error(
                            "write SQL Server richiede WKB XY puro senza header EWKB",
                        ));
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
    let mut query = Query::new(plan.insert_sql.clone());
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
    DatabaseError {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_formatter_handles_boundaries_without_abs_overflow() {
        assert_eq!(decimal_string(12_345, 2).expect("decimal"), "123.45");
        assert_eq!(decimal_string(-1, 3).expect("decimal"), "-0.001");
        assert_eq!(
            decimal_string(i128::MIN, 0).expect("minimum"),
            "-170141183460469231731687303715884105728"
        );
        assert!(decimal_string(1, -1).is_err());
    }

    #[test]
    fn temporal_extremes_fail_without_panicking() {
        assert!(time64(-1).is_err());
        assert!(time64(86_400_000_000).is_err());
        assert!(timestamp(i64::MAX).is_err());
    }
}
