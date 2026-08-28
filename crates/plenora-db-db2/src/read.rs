use crate::catalog;
use crate::connection::with_connection;
use crate::error::{driver_error, interruption_error};
use crate::{Db2ColumnKind, Db2Config, Db2ObjectDescription, Db2ReadPlan};
use chrono::{Datelike, NaiveDate, NaiveDateTime};
use odbc_api::buffers::TextRowSet;
use odbc_api::{Cursor, IntoParameter};
use plenora_database_core::arrow::array::builder::{BinaryBuilder, StringBuilder};
use plenora_database_core::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::plan::{Operation, ReadOperation};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceKind, ResourceLease};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_engine::{DeadlineGuard, ReadBatchReservation};
use std::collections::BTreeSet;
use std::sync::Arc;
use tokio::sync::mpsc;

const DEFAULT_BATCH_ROWS: usize = 1_024;
const CHANNEL_CAPACITY: usize = 1;

struct ReadDemand {
    rows: usize,
}

pub async fn read_operation(
    config: &Db2Config,
    secret: &SecretString,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<Box<dyn BatchStream>> {
    if cancellation.is_cancelled() {
        return Err(interruption_error(cancellation, ErrorPhase::Prepare));
    }
    budget.ensure_active()?;
    let inspection = catalog::inspect(
        config,
        secret,
        &Operation::DatabaseDescribeObject {
            source: operation.source.clone(),
        },
        cancellation,
    )
    .await?;
    let description: Db2ObjectDescription = serde_json::from_value(inspection.document)
        .map_err(|_| read_error(ErrorCategory::Protocol, "catalogo Db2 non deserializzabile"))?;
    let plan = Db2ReadPlan::compile(&description, operation, budget.limits().cell_bytes)?;
    let bound = bind_parameters(&plan.bind_names, parameters)?;
    let operation_lease = budget.try_lease(ResourceKind::ConcurrentOperations, 1)?;
    let column_count = u64::try_from(plan.columns.len())
        .map_err(|_| DatabaseError::resource_limit("numero colonne Db2 non rappresentabile"))?;
    let columns_lease = budget.try_lease(ResourceKind::Columns, column_count)?;
    let mut deadline = DeadlineGuard::new(cancellation, budget)?;
    let internal = deadline.token().clone();
    let deadline_task = deadline.take_deadline_task()?;
    let (demand_sender, demand_receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let (result_sender, result_receiver) = mpsc::channel(CHANNEL_CAPACITY);
    let worker_config = config.clone();
    let worker_secret = secret.clone();
    let worker_plan = plan.clone();
    let worker_cancellation = internal.clone();
    let error_sender = result_sender.clone();
    let worker_task = tokio::task::spawn_blocking(move || {
        if let Err(error) = pump_rows(
            &worker_config,
            &worker_secret,
            &worker_plan,
            &bound,
            &worker_cancellation,
            demand_receiver,
            &result_sender,
        ) {
            let _ = error_sender.blocking_send(Err(error));
        }
    });
    Ok(Box::new(Db2BatchStream {
        demand_sender,
        result_receiver,
        schema: Arc::clone(&plan.schema),
        plan,
        budget: budget.clone(),
        cancellation: internal,
        deadline_task,
        worker_task: Some(worker_task),
        _operation_lease: operation_lease,
        _columns_lease: columns_lease,
        finished: false,
    }))
}

pub fn bind_parameters(names: &[String], parameters: &ParameterBag) -> Result<Vec<String>> {
    let unique = names.iter().collect::<BTreeSet<_>>();
    if unique.len() != parameters.len() {
        return Err(read_error(
            ErrorCategory::InvalidPlan,
            "parametri Db2 mancanti o eccedenti",
        ));
    }
    names
        .iter()
        .map(|name| {
            let value = parameters.get(name).ok_or_else(|| {
                read_error(
                    ErrorCategory::InvalidPlan,
                    "parametro Db2 richiesto assente",
                )
            })?;
            match value {
                ParameterValue::Bool(value) => Ok(value.to_string()),
                ParameterValue::I32(value) => Ok(value.to_string()),
                ParameterValue::I64(value) => Ok(value.to_string()),
                ParameterValue::F64(value) if value.is_finite() => Ok(value.to_string()),
                ParameterValue::String(value)
                | ParameterValue::Date(value)
                | ParameterValue::Timestamp(value)
                | ParameterValue::Decimal(value)
                | ParameterValue::Uuid(value) => Ok(value.clone()),
                ParameterValue::Enum { label, .. } => Ok(label.clone()),
                _ => Err(DatabaseError::unsupported(
                    plenora_database_core::plan::ProviderKind::Db2,
                    ErrorPhase::Prepare,
                    "tipo parametro Db2 non ancora qualificato",
                )),
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn pump_rows(
    config: &Db2Config,
    secret: &SecretString,
    plan: &Db2ReadPlan,
    parameters: &[String],
    cancellation: &CancellationToken,
    mut demands: mpsc::Receiver<ReadDemand>,
    results: &mpsc::Sender<Result<Option<RecordBatch>>>,
) -> Result<()> {
    with_connection(config, secret, |connection, timeout| {
        let confirmed =
            catalog::describe_object(connection, timeout, &plan.schema_name, &plan.object_name)?;
        if confirmed.schema_token != plan.schema_token {
            return Err(read_error(
                ErrorCategory::Schema,
                "schema Db2 cambiato durante la preparazione",
            ));
        }
        let parameters: Vec<_> = parameters
            .iter()
            .map(|parameter| parameter.as_str().into_parameter())
            .collect();
        let mut cursor = Some(
            connection
                .execute(&plan.sql, parameters.as_slice(), Some(timeout))
                .map_err(|error| driver_error(&error, ErrorPhase::Read))?
                .ok_or_else(|| {
                    read_error(ErrorCategory::Protocol, "lettura Db2 senza result set")
                })?,
        );
        while let Some(demand) = demands.blocking_recv() {
            if cancellation.is_cancelled() {
                return Err(interruption_error(cancellation, ErrorPhase::Read));
            }
            let buffer = TextRowSet::from_max_str_lens(demand.rows, plan.wire_text_capacities())
                .map_err(|error| driver_error(&error, ErrorPhase::Read))?;
            let active = cursor.take().ok_or_else(|| {
                read_error(ErrorCategory::Internal, "cursore Db2 non disponibile")
            })?;
            let mut block = active
                .bind_buffer(buffer)
                .map_err(|error| driver_error(&error, ErrorPhase::Read))?;
            let batch = block
                .fetch()
                .map_err(|error| driver_error(&error, ErrorPhase::Read))?;
            let decoded = batch.map(|batch| decode_batch(batch, plan)).transpose()?;
            let (active, buffer) = block
                .unbind()
                .map_err(|error| driver_error(&error, ErrorPhase::Read))?;
            cursor = Some(active);
            drop(buffer);
            let drained = decoded.is_none();
            if results.blocking_send(Ok(decoded)).is_err() || drained {
                break;
            }
        }
        Ok(())
    })
}

enum Values {
    Bool(Vec<Option<bool>>),
    I16(Vec<Option<i16>>),
    I32(Vec<Option<i32>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Decimal(Vec<Option<i128>>, u8, i8),
    Utf8(Vec<Option<String>>),
    Geometry(Vec<Option<Vec<u8>>>),
    Date(Vec<Option<i32>>),
    Timestamp(Vec<Option<i64>>),
}

impl Values {
    fn new(kind: &Db2ColumnKind, rows: usize) -> Self {
        match *kind {
            Db2ColumnKind::Bool => Self::Bool(Vec::with_capacity(rows)),
            Db2ColumnKind::I16 => Self::I16(Vec::with_capacity(rows)),
            Db2ColumnKind::I32 => Self::I32(Vec::with_capacity(rows)),
            Db2ColumnKind::I64 => Self::I64(Vec::with_capacity(rows)),
            Db2ColumnKind::F32 => Self::F32(Vec::with_capacity(rows)),
            Db2ColumnKind::F64 => Self::F64(Vec::with_capacity(rows)),
            Db2ColumnKind::Decimal { precision, scale } => {
                Self::Decimal(Vec::with_capacity(rows), precision, scale)
            }
            Db2ColumnKind::Utf8 => Self::Utf8(Vec::with_capacity(rows)),
            Db2ColumnKind::Geometry => Self::Geometry(Vec::with_capacity(rows)),
            Db2ColumnKind::Date => Self::Date(Vec::with_capacity(rows)),
            Db2ColumnKind::Timestamp => Self::Timestamp(Vec::with_capacity(rows)),
        }
    }

    fn push(&mut self, value: Option<&[u8]>) -> Result<()> {
        let text = value
            .map(|value| {
                std::str::from_utf8(value)
                    .map(str::trim)
                    .map_err(|_| mapping("testo Db2 non UTF-8"))
            })
            .transpose()?;
        match self {
            Self::Bool(values) => values.push(text.map(parse_bool).transpose()?),
            Self::I16(values) => values.push(parse(text, "SMALLINT Db2 non rappresentabile")?),
            Self::I32(values) => values.push(parse(text, "INTEGER Db2 non rappresentabile")?),
            Self::I64(values) => values.push(parse(text, "BIGINT Db2 non rappresentabile")?),
            Self::F32(values) => values.push(text.map(parse_f32).transpose()?),
            Self::F64(values) => values.push(text.map(parse_f64).transpose()?),
            Self::Decimal(values, _, scale) => {
                values.push(text.map(|value| parse_decimal(value, *scale)).transpose()?);
            }
            Self::Utf8(values) => values.push(
                value
                    .map(|value| std::str::from_utf8(value).map(str::to_owned))
                    .transpose()
                    .map_err(|_| mapping("VARCHAR Db2 non UTF-8"))?,
            ),
            Self::Geometry(values) => values.push(value.map(decode_hex_wkb).transpose()?),
            Self::Date(values) => values.push(text.map(parse_date).transpose()?),
            Self::Timestamp(values) => values.push(text.map(parse_timestamp).transpose()?),
        }
        Ok(())
    }

    fn finish(self) -> Result<ArrayRef> {
        let array: ArrayRef = match self {
            Self::Bool(values) => Arc::new(BooleanArray::from(values)),
            Self::I16(values) => Arc::new(Int16Array::from(values)),
            Self::I32(values) => Arc::new(Int32Array::from(values)),
            Self::I64(values) => Arc::new(Int64Array::from(values)),
            Self::F32(values) => Arc::new(Float32Array::from(values)),
            Self::F64(values) => Arc::new(Float64Array::from(values)),
            Self::Decimal(values, precision, scale) => Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .map_err(DatabaseError::from)?,
            ),
            Self::Utf8(values) => {
                let mut builder = StringBuilder::with_capacity(
                    values.len(),
                    values.iter().flatten().map(String::len).sum(),
                );
                for value in values {
                    if let Some(value) = value {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            Self::Geometry(values) => {
                let byte_capacity = values.iter().flatten().map(Vec::len).sum::<usize>();
                let mut builder = BinaryBuilder::with_capacity(values.len(), byte_capacity);
                for value in values {
                    if let Some(value) = value {
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            Self::Date(values) => Arc::new(Date32Array::from(values)),
            Self::Timestamp(values) => Arc::new(TimestampMicrosecondArray::from(values)),
        };
        Ok(array)
    }
}

fn decode_batch(batch: &TextRowSet, plan: &Db2ReadPlan) -> Result<RecordBatch> {
    validate_spatial_checks(batch, plan)?;
    let mut values = plan
        .columns
        .iter()
        .map(|column| Values::new(&column.kind, batch.num_rows()))
        .collect::<Vec<_>>();
    for row in 0..batch.num_rows() {
        for (column, output) in values.iter_mut().enumerate() {
            if batch
                .indicator_at(column, row)
                .is_truncated(batch.max_len(column))
            {
                return Err(mapping("cella Db2 oltre il limite configurato"));
            }
            output.push(batch.at(column, row))?;
        }
    }
    let arrays = values
        .into_iter()
        .map(Values::finish)
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::clone(&plan.schema), arrays).map_err(DatabaseError::from)
}

fn validate_spatial_checks(batch: &TextRowSet, plan: &Db2ReadPlan) -> Result<()> {
    let hidden_start = plan.columns.len();
    for row in 0..batch.num_rows() {
        for (check_index, check) in plan.spatial_checks.iter().enumerate() {
            let geometry = batch.at(check.column_index, row);
            let srid = batch.at(hidden_start + check_index * 2, row);
            let dimensions = batch.at(hidden_start + check_index * 2 + 1, row);
            if geometry.is_none() {
                if srid.is_some() || dimensions.is_some() {
                    return Err(read_error(
                        ErrorCategory::Protocol,
                        "controlli spatial Db2 non nulli per una geometry nulla",
                    ));
                }
                continue;
            }
            let srid = parse_check(srid, "SRID spatial Db2 non rappresentabile")?;
            if srid != u64::from(check.expected_srid) {
                return Err(read_error(
                    ErrorCategory::Crs,
                    "SRID spatial Db2 diverso dal declared_crs",
                ));
            }
            let dimensions = parse_check(
                dimensions,
                "dimensione coordinata spatial Db2 non rappresentabile",
            )?;
            if !matches!(dimensions, 2 | 3) {
                return Err(read_error(
                    ErrorCategory::DataMapping,
                    "dimensione coordinata spatial Db2 non qualificata",
                ));
            }
        }
    }
    Ok(())
}

fn parse_check(value: Option<&[u8]>, message: &'static str) -> Result<u64> {
    let text = value
        .and_then(|value| std::str::from_utf8(value).ok())
        .map(str::trim)
        .ok_or_else(|| read_error(ErrorCategory::DataMapping, message))?;
    text.parse()
        .map_err(|_| read_error(ErrorCategory::DataMapping, message))
}

pub fn decode_hex_wkb(value: &[u8]) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(mapping("WKB esadecimale Db2 con lunghezza dispari"));
    }
    value
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = hex_nibble(pair[0])?;
            let low = hex_nibble(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_nibble(value: u8) -> Result<u8> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(mapping("WKB esadecimale Db2 non valido")),
    }
}

fn validate_spatial_batch(
    batch: &RecordBatch,
    plan: &Db2ReadPlan,
    budget: &ResourceBudget,
    component_limit: u64,
) -> Result<u64> {
    let mut components = 0_u64;
    for (index, column) in plan.columns.iter().enumerate() {
        if column.kind != Db2ColumnKind::Geometry {
            continue;
        }
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| read_error(ErrorCategory::Internal, "array spatial Db2 non binario"))?;
        for row in 0..batch.num_rows() {
            if values.is_null(row) {
                continue;
            }
            let bytes = values.value(row);
            if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > budget.limits().cell_bytes {
                return Err(DatabaseError::resource_limit(
                    "geometry Db2 oltre il limite cella",
                ));
            }
            let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(
                bytes,
                component_limit.saturating_sub(components),
                budget.limits().nesting_depth,
            )
            .map_err(|mut error| {
                error.phase = ErrorPhase::Read;
                error.provider = Some(plenora_database_core::plan::ProviderKind::Db2);
                error
            })?;
            if inspection.has_any_embedded_srid || inspection.has_any_m {
                return Err(mapping(
                    "ST_AsBinary Db2 ha prodotto WKB con SRID embedded o dimensione M",
                ));
            }
            components = components
                .checked_add(inspection.stats.components)
                .ok_or_else(|| DatabaseError::resource_limit("overflow componenti geometry Db2"))?;
        }
    }
    Ok(components)
}

fn parse<T>(value: Option<&str>, message: &'static str) -> Result<Option<T>>
where
    T: std::str::FromStr,
{
    value
        .map(|value| value.parse().map_err(|_| mapping(message)))
        .transpose()
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.to_ascii_uppercase().as_str() {
        "1" | "TRUE" => Ok(true),
        "0" | "FALSE" => Ok(false),
        _ => Err(mapping("BOOLEAN Db2 non rappresentabile")),
    }
}

pub fn parse_decimal(value: &str, scale: i8) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let (integer, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let scale = usize::try_from(scale).map_err(|_| mapping("scala DECIMAL Db2 negativa"))?;
    if integer.is_empty() && fraction.is_empty()
        || fraction.len() > scale
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(mapping("DECIMAL Db2 non rappresentabile senza perdita"));
    }
    let mut digits = String::with_capacity(integer.len() + scale);
    digits.push_str(integer);
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n('0', scale - fraction.len()));
    let magnitude = digits
        .parse::<i128>()
        .map_err(|_| mapping("DECIMAL Db2 oltre Decimal128"))?;
    Ok(if negative { -magnitude } else { magnitude })
}

fn parse_f32(value: &str) -> Result<f32> {
    let value = value
        .parse::<f32>()
        .map_err(|_| mapping("floating point Db2 non rappresentabile"))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(mapping("floating point Db2 non finito"))
    }
}

fn parse_f64(value: &str) -> Result<f64> {
    let value = value
        .parse::<f64>()
        .map_err(|_| mapping("floating point Db2 non rappresentabile"))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(mapping("floating point Db2 non finito"))
    }
}

pub fn parse_date(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d")
        .map_err(|_| mapping("DATE Db2 non rappresentabile"))?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch valida");
    Ok(date.num_days_from_ce() - epoch.num_days_from_ce())
}

pub fn parse_timestamp(value: &str) -> Result<i64> {
    let timestamp = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(value, "%Y-%m-%d-%H.%M.%S%.f"))
        .map_err(|_| mapping("TIMESTAMP Db2 non rappresentabile"))?;
    Ok(timestamp.and_utc().timestamp_micros())
}

fn mapping(message: &'static str) -> DatabaseError {
    read_error(ErrorCategory::DataMapping, message)
}

fn read_error(category: ErrorCategory, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Read,
        Some(plenora_database_core::plan::ProviderKind::Db2),
        message,
    )
}

struct Db2BatchStream {
    demand_sender: mpsc::Sender<ReadDemand>,
    result_receiver: mpsc::Receiver<Result<Option<RecordBatch>>>,
    schema: SchemaRef,
    plan: Db2ReadPlan,
    budget: ResourceBudget,
    cancellation: CancellationToken,
    deadline_task: tokio::task::JoinHandle<()>,
    worker_task: Option<tokio::task::JoinHandle<()>>,
    _operation_lease: ResourceLease,
    _columns_lease: ResourceLease,
    finished: bool,
}

impl BatchStream for Db2BatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move {
            if self.finished {
                return Ok(None);
            }
            if cancellation.is_cancelled() {
                self.cancellation.cancel();
                self.finished = true;
                return Err(interruption_error(cancellation, ErrorPhase::Read));
            }
            self.budget.ensure_active()?;
            let has_spatial = !self.plan.spatial_checks.is_empty();
            let reservation =
                ReadBatchReservation::acquire(&self.budget, DEFAULT_BATCH_ROWS, None, has_spatial)?;
            let per_row = self.plan.text_buffer_bytes(1).max(1);
            let memory_rows = reservation.byte_limit / per_row.saturating_mul(2);
            let rows = reservation
                .row_limit
                .min(usize::try_from(memory_rows).unwrap_or(usize::MAX));
            if rows == 0 {
                return Err(DatabaseError::resource_limit(
                    "budget Db2 insufficiente per un buffer di lettura",
                ));
            }
            self.demand_sender
                .send(ReadDemand { rows })
                .await
                .map_err(|_| read_error(ErrorCategory::Protocol, "worker Db2 non disponibile"))?;
            let outcome = tokio::select! {
                result = self.result_receiver.recv() => result,
                _ = cancellation.cancelled() => {
                    self.cancellation.cancel();
                    self.finished = true;
                    return Err(interruption_error(cancellation, ErrorPhase::Read));
                }
            };
            match outcome {
                Some(Ok(Some(batch))) => {
                    let components = validate_spatial_batch(
                        &batch,
                        &self.plan,
                        &self.budget,
                        reservation.component_limit,
                    )?;
                    let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
                    let peak = bytes.saturating_add(self.plan.text_buffer_bytes(rows));
                    if peak > reservation.byte_limit {
                        self.cancellation.cancel();
                        self.finished = true;
                        return Err(DatabaseError::resource_limit(
                            "batch Db2 oltre il budget memoria prenotato",
                        ));
                    }
                    reservation.commit(batch.num_rows() as u64, bytes.max(1), components)?;
                    Ok(Some(batch))
                }
                Some(Ok(None)) => {
                    self.finished = true;
                    Ok(None)
                }
                Some(Err(error)) => {
                    self.finished = true;
                    Err(error)
                }
                None => {
                    self.finished = true;
                    Err(read_error(
                        ErrorCategory::Protocol,
                        "worker Db2 terminato senza esito",
                    ))
                }
            }
        })
    }
}

impl Drop for Db2BatchStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.deadline_task.abort();
        if let Some(task) = self.worker_task.take() {
            task.abort();
        }
    }
}
