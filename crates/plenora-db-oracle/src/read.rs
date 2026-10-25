use crate::catalog;
use crate::connection::with_timeout_duration;
use crate::decode::{decode_columns, row_from_driver};
use crate::error::interruption_error;
use crate::parameter::{bind_parameters_with_lobs, LobCache};
use crate::{
    OracleColumnKind, OracleColumnSpec, OracleConfig, OracleObjectDescription, OraclePool,
    OracleReadPlan, PooledOracleConnection,
};
use chrono::{DateTime, NaiveDateTime};
use oracle_rs::ColumnInfo;
use plenora_database_core::arrow::array::builder::{BinaryBuilder, StringBuilder};
use plenora_database_core::arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
    TimestampMicrosecondArray,
};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::plan::{FilterExpression, Operation, ProviderKind, ReadOperation};
use plenora_database_core::provider::{BatchStream, ParameterBag, ParameterValue, ProviderFuture};
use plenora_database_core::relational::SpatialFunction;
use plenora_database_core::resource::{ResourceBudget, ResourceLease};
use plenora_database_core::row::Row;
use plenora_database_core::spatial_predicate::{SpatialPredicate, SpatialReference};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_engine::{ContractLeases, DeadlineGuard, ReadBatchReservation};
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

const BATCH_ROWS: usize = 512;

#[allow(clippy::significant_drop_tightening)]
pub async fn read_operation(
    config: &OracleConfig,
    pool: &Arc<OraclePool>,
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
        pool,
        &Operation::DatabaseDescribeObject {
            source: operation.source.clone(),
        },
        cancellation,
    )
    .await
    .map_err(|error| read_context(error, "ispezione iniziale read Oracle non completata"))?;
    let description: OracleObjectDescription = serde_json::from_value(inspection.document)
        .map_err(|_| {
            read_error(
                ErrorCategory::Protocol,
                "catalogo Oracle non deserializzabile",
            )
        })?;
    if let Some(filter) = &operation.filter {
        validate_spatial_filter(&description, filter, parameters)?;
    }
    let plan = OracleReadPlan::compile(&description, operation)?;
    let parameter_values = named_parameters(&plan.bind_names, parameters)?;
    let leases = ContractLeases::acquire(budget, plan.columns.len())?;
    let (operation_lease, columns_lease) = leases.into_parts();
    let mut deadline = DeadlineGuard::new(cancellation, budget)?;
    let internal = deadline.token().clone();
    let deadline_task = deadline.take_deadline_task()?;
    let mut connection = pool.checkout(&internal).await?;
    connection.disallow_reuse();
    let raw = connection.connection()?;
    raw.set_response_byte_limit(
        usize::try_from(budget.limits().memory_bytes).unwrap_or(usize::MAX),
    )
    .await;
    let mut lob_cache = LobCache::default();
    let params = bind_parameters_with_lobs(
        raw,
        &parameter_values,
        false,
        &mut lob_cache,
        config.operation_timeout(),
        ErrorPhase::Read,
        &internal,
    )
    .await?;
    let confirmed =
        catalog::describe_object(config, raw, &plan.schema_name, &plan.object_name, &internal)
            .await
            .map_err(|error| read_context(error, "conferma schema read Oracle non completata"))?;
    if confirmed.schema_token != plan.schema_token {
        return Err(read_error(
            ErrorCategory::Schema,
            "schema Oracle cambiato durante la preparazione",
        ));
    }
    let result = with_timeout_duration(
        config.operation_timeout(),
        ErrorPhase::Read,
        &internal,
        raw.query(&plan.sql, &params),
    )
    .await
    .map_err(|error| read_context(error, "query read Oracle non completata"))?;
    let decoded_columns = decode_columns(&result.columns)?;
    Ok(Box::new(OracleBatchStream {
        connection,
        timeout: config.operation_timeout(),
        wire_columns: result.columns,
        decoded_columns,
        pending: result.rows.into(),
        cursor_id: result.cursor_id,
        has_more: result.has_more_rows,
        plan,
        budget: budget.clone(),
        internal,
        deadline_task,
        _operation_lease: operation_lease,
        _columns_lease: columns_lease,
        finished: false,
    }))
}

#[allow(clippy::too_many_lines)]
fn validate_spatial_filter(
    description: &OracleObjectDescription,
    filter: &FilterExpression,
    parameters: &ParameterBag,
) -> Result<()> {
    match filter {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            for argument in args {
                validate_spatial_filter(description, argument, parameters)?;
            }
        }
        FilterExpression::Spatial {
            function,
            field,
            geometry_parameter: Some(geometry_parameter),
            distance_parameter,
        } => {
            let column = description
                .columns
                .iter()
                .find(|column| column.name == *field)
                .ok_or_else(|| {
                    read_error(
                        ErrorCategory::NotFound,
                        "colonna Spatial Oracle non trovata",
                    )
                })?;
            if !column.data_type.eq_ignore_ascii_case("SDO_GEOMETRY") {
                return Err(read_error(
                    ErrorCategory::DataMapping,
                    "filtro Spatial Oracle applicato a una colonna non geometrica",
                ));
            }
            let expected_srid = column.spatial_srid.ok_or_else(|| {
                read_error(
                    ErrorCategory::Crs,
                    "colonna Spatial Oracle senza SRID catalogato",
                )
            })?;
            let expected_dimensions = match column.spatial_dimensions {
                Some(2) => Dimensions::Xy,
                Some(3) => Dimensions::Xyz,
                _ => {
                    return Err(read_error(
                        ErrorCategory::Crs,
                        "dimensioni Spatial Oracle non qualificate",
                    ))
                }
            };
            let expected_semantics = column.spatial_semantics.ok_or_else(|| {
                read_error(
                    ErrorCategory::Crs,
                    "colonna Spatial Oracle senza semantica catalogata",
                )
            })?;
            let value = parameters.get(geometry_parameter).ok_or_else(|| {
                read_error(
                    ErrorCategory::InvalidPlan,
                    "parametro geometry Oracle richiesto assente",
                )
            })?;
            let ParameterValue::Wkb {
                bytes,
                srid,
                dimensions,
                semantics,
            } = value
            else {
                return Err(read_error(
                    ErrorCategory::DataMapping,
                    "filtro Spatial Oracle richiede un parametro WKB tipizzato",
                ));
            };
            let semantics_compatible = *semantics == expected_semantics
                || (*semantics == plenora_database_core::geometry::SpatialSemantics::Geometry
                    && expected_semantics
                        == plenora_database_core::geometry::SpatialSemantics::Geography);
            if *srid != Some(expected_srid)
                || *dimensions != expected_dimensions
                || !semantics_compatible
            {
                return Err(read_error(
                    ErrorCategory::Crs,
                    "frame WKB del filtro diverso dal catalogo Spatial Oracle",
                ));
            }
            let reference = SpatialReference::new_validated(
                bytes.clone(),
                expected_srid,
                expected_dimensions,
                *semantics,
            )
            .map_err(oracle_read_error)?;
            let predicate = match function {
                SpatialFunction::Intersects => SpatialPredicate::Intersects,
                SpatialFunction::Contains => SpatialPredicate::Contains,
                SpatialFunction::Within => SpatialPredicate::Within,
                SpatialFunction::DWithin => SpatialPredicate::DWithin {
                    distance_meters: distance_parameter
                        .as_deref()
                        .and_then(|name| parameters.get(name))
                        .and_then(numeric_distance)
                        .ok_or_else(|| {
                            read_error(
                                ErrorCategory::DataMapping,
                                "DWithin Oracle richiede una distanza numerica",
                            )
                        })?,
                },
                _ => {
                    return Err(read_error(
                        ErrorCategory::Unsupported,
                        "predicato Spatial Oracle non qualificato",
                    ))
                }
            };
            plenora_database_core::spatial_policy::validate_predicate(
                ProviderKind::Oracle,
                &predicate,
                &reference,
            )
            .map_err(oracle_read_error)?;
        }
        _ => {}
    }
    Ok(())
}

fn numeric_distance(value: &ParameterValue) -> Option<f64> {
    match value {
        ParameterValue::F64(value) => Some(*value),
        ParameterValue::I32(value) => Some(f64::from(*value)),
        ParameterValue::I64(value) => value.to_string().parse().ok(),
        ParameterValue::Decimal(value) => value.parse().ok(),
        _ => None,
    }
}

const fn oracle_read_error(mut error: DatabaseError) -> DatabaseError {
    error.phase = ErrorPhase::Prepare;
    error.provider = Some(ProviderKind::Oracle);
    error
}

fn named_parameters(names: &[String], parameters: &ParameterBag) -> Result<Vec<ParameterValue>> {
    let unique = names.iter().collect::<BTreeSet<_>>();
    if unique.len() != parameters.len() {
        return Err(read_error(
            ErrorCategory::InvalidPlan,
            "parametri Oracle mancanti o eccedenti",
        ));
    }
    let values = names
        .iter()
        .map(|name| {
            parameters.get(name).cloned().ok_or_else(|| {
                read_error(
                    ErrorCategory::InvalidPlan,
                    "parametro Oracle richiesto assente",
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(values)
}

struct OracleBatchStream {
    connection: PooledOracleConnection,
    timeout: Duration,
    wire_columns: Vec<ColumnInfo>,
    decoded_columns: Arc<crate::decode::DecodeColumns>,
    pending: VecDeque<oracle_rs::Row>,
    cursor_id: u16,
    has_more: bool,
    plan: OracleReadPlan,
    budget: ResourceBudget,
    internal: CancellationToken,
    deadline_task: tokio::task::JoinHandle<()>,
    _operation_lease: ResourceLease,
    _columns_lease: ResourceLease,
    finished: bool,
}

impl Drop for OracleBatchStream {
    fn drop(&mut self) {
        self.internal.cancel();
        self.deadline_task.abort();
    }
}

impl BatchStream for OracleBatchStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.plan.schema)
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
                self.finished = true;
                return Err(interruption_error(cancellation, ErrorPhase::Read));
            }
            let reservation = ReadBatchReservation::acquire(
                &self.budget,
                BATCH_ROWS,
                None,
                !self.plan.spatial_columns.is_empty(),
            )?;
            while self.pending.len() < reservation.row_limit && self.has_more {
                let fetch_rows =
                    u32::try_from(reservation.row_limit - self.pending.len()).unwrap_or(u32::MAX);
                let result = with_timeout_duration(
                    self.timeout,
                    ErrorPhase::Read,
                    &self.internal,
                    self.connection.connection()?.fetch_more(
                        self.cursor_id,
                        &self.wire_columns,
                        fetch_rows,
                    ),
                )
                .await?;
                self.cursor_id = result.cursor_id;
                self.has_more = result.has_more_rows;
                self.pending.extend(result.rows);
                if fetch_rows == 0 {
                    break;
                }
            }
            if self.pending.is_empty() {
                self.finished = true;
                self.connection.allow_reuse();
                return Ok(None);
            }
            let take = reservation.row_limit.min(self.pending.len());
            let mut rows = Vec::with_capacity(take);
            for _ in 0..take {
                let driver_row = self.pending.pop_front().ok_or_else(|| {
                    read_error(ErrorCategory::Internal, "buffer righe Oracle incoerente")
                })?;
                rows.push(
                    row_from_driver(
                        self.connection.connection()?,
                        Arc::clone(&self.decoded_columns),
                        driver_row,
                        self.timeout,
                        ErrorPhase::Read,
                        &self.internal,
                    )
                    .await?,
                );
            }
            let (batch, components) =
                rows_to_batch(&self.plan, &rows, &self.budget, reservation.component_limit)?;
            let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
            reservation.commit(
                u64::try_from(batch.num_rows()).unwrap_or(u64::MAX),
                bytes,
                components,
            )?;
            if self.pending.is_empty() && !self.has_more {
                self.finished = true;
                self.connection.allow_reuse();
            }
            Ok(Some(batch))
        })
    }
}

enum Values {
    Bool(Vec<Option<bool>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    /// `true` quando la sorgente e un NUMBER decimale (`FLOAT(b)`), non un
    /// `BINARY_DOUBLE`: ogni valore passa da f64 solo se ne e ricostruibile.
    F64(Vec<Option<f64>>, bool),
    Decimal(Vec<Option<i128>>, u8, i8),
    Utf8(Vec<Option<String>>),
    Binary(Vec<Option<Vec<u8>>>),
    Timestamp(Vec<Option<i64>>, bool),
}

impl Values {
    fn new(column: &OracleColumnSpec, capacity: usize) -> Self {
        match column.kind {
            OracleColumnKind::Bool => Self::Bool(Vec::with_capacity(capacity)),
            OracleColumnKind::I64 => Self::I64(Vec::with_capacity(capacity)),
            OracleColumnKind::F32 => Self::F32(Vec::with_capacity(capacity)),
            OracleColumnKind::F64 => Self::F64(
                Vec::with_capacity(capacity),
                column.native_type != "BINARY_DOUBLE",
            ),
            OracleColumnKind::Decimal { precision, scale } => {
                Self::Decimal(Vec::with_capacity(capacity), precision, scale)
            }
            OracleColumnKind::Utf8 => Self::Utf8(Vec::with_capacity(capacity)),
            OracleColumnKind::Binary | OracleColumnKind::Geometry => {
                Self::Binary(Vec::with_capacity(capacity))
            }
            OracleColumnKind::DateTime => Self::Timestamp(Vec::with_capacity(capacity), false),
            OracleColumnKind::TimestampTz => Self::Timestamp(Vec::with_capacity(capacity), true),
        }
    }

    fn push(&mut self, value: &ParameterValue) -> Result<()> {
        if matches!(value, ParameterValue::Null { .. }) {
            match self {
                Self::Bool(values) => values.push(None),
                Self::I64(values) | Self::Timestamp(values, _) => values.push(None),
                Self::F32(values) => values.push(None),
                Self::F64(values, _) => values.push(None),
                Self::Decimal(values, ..) => values.push(None),
                Self::Utf8(values) => values.push(None),
                Self::Binary(values) => values.push(None),
            }
            return Ok(());
        }
        let expected = self.kind_name();
        match (self, value) {
            (Self::Bool(values), ParameterValue::Bool(value)) => values.push(Some(*value)),
            (Self::Bool(values), ParameterValue::I64(value)) if matches!(*value, 0 | 1) => {
                values.push(Some(*value == 1));
            }
            (Self::Bool(values), ParameterValue::Decimal(value))
                if matches!(value.as_str(), "0" | "1") =>
            {
                values.push(Some(value == "1"));
            }
            (Self::I64(values), ParameterValue::I64(value)) => values.push(Some(*value)),
            (Self::I64(values), ParameterValue::I32(value)) => values.push(Some(i64::from(*value))),
            (Self::I64(values), ParameterValue::Decimal(value)) => values.push(Some(
                value
                    .parse()
                    .map_err(|_| mapping("NUMBER intero Oracle non rappresentabile"))?,
            )),
            // BINARY_FLOAT e BINARY_DOUBLE arrivano come `DUMP()` dei byte
            // salvati: passare da NUMBER azzerava in silenzio ogni
            // |x| < 1e-130, `UTL_RAW.CAST_FROM_BINARY_*` rendeva +0 per un
            // -0.0 non canonico.
            (Self::F32(values), ParameterValue::String(dump)) => {
                values.push(binary_float_from_dump(dump)?);
            }
            (Self::F32(values), ParameterValue::F64(value)) => {
                let narrowed = narrow_f32(*value);
                if !narrowed.is_finite() || f64::from(narrowed).to_bits() != value.to_bits() {
                    return Err(mapping("BINARY_FLOAT Oracle non rappresentabile"));
                }
                values.push(Some(narrowed));
            }
            (Self::F64(values, false), ParameterValue::String(dump)) => {
                values.push(binary_double_from_dump(dump)?);
            }
            (Self::F64(values, _), ParameterValue::F64(value)) => values.push(Some(*value)),
            // FLOAT(b) e un NUMBER decimale fino a 38 cifre: diventa f64 solo
            // se il valore se ne ricava di nuovo (9007199254740993 o 1/3 no).
            (Self::F64(values, true), ParameterValue::Decimal(value)) => {
                values.push(Some(decimal_as_f64(value)?));
            }
            (Self::F64(values, true), ParameterValue::I64(value)) => {
                values.push(Some(decimal_as_f64(&value.to_string())?));
            }
            (Self::Decimal(values, precision, scale), ParameterValue::Decimal(value)) => {
                values.push(Some(parse_decimal(value, *precision, *scale)?));
            }
            (Self::Decimal(values, precision, 0), ParameterValue::I64(value)) => {
                values.push(Some(parse_decimal(&value.to_string(), *precision, 0)?));
            }
            (Self::Utf8(values), ParameterValue::String(value)) => values.push(Some(value.clone())),
            (Self::Utf8(values), ParameterValue::Json(value)) => values.push(Some(
                serde_json::to_string(value)
                    .map_err(|_| mapping("JSON Oracle non serializzabile"))?,
            )),
            (Self::Binary(values), ParameterValue::Bytes(value)) => {
                values.push(Some(value.clone()));
            }
            (Self::Timestamp(values, false), ParameterValue::Timestamp(value)) => {
                values.push(Some(parse_naive_timestamp(value)?));
            }
            (Self::Timestamp(values, true), ParameterValue::TimestampTz(value)) => {
                values.push(Some(parse_tz_timestamp(value)?));
            }
            _ => {
                return Err(read_error(
                    ErrorCategory::DataMapping,
                    format!(
                        "tipo runtime Oracle {} diverso dal piano Arrow {expected}",
                        parameter_kind(value)
                    ),
                ));
            }
        }
        Ok(())
    }

    const fn kind_name(&self) -> &'static str {
        match self {
            Self::Bool(_) => "bool",
            Self::I64(_) => "int64",
            Self::F32(_) => "float32",
            Self::F64(..) => "float64",
            Self::Decimal(..) => "decimal128",
            Self::Utf8(_) => "utf8",
            Self::Binary(_) => "binary",
            Self::Timestamp(..) => "timestamp",
        }
    }

    fn finish(self) -> Result<ArrayRef> {
        let array: ArrayRef = match self {
            Self::Bool(values) => Arc::new(BooleanArray::from(values)),
            Self::I64(values) => Arc::new(Int64Array::from(values)),
            Self::F32(values) => Arc::new(Float32Array::from(values)),
            Self::F64(values, _) => Arc::new(Float64Array::from(values)),
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
            Self::Binary(values) => {
                let mut builder = BinaryBuilder::with_capacity(
                    values.len(),
                    values.iter().flatten().map(Vec::len).sum(),
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
            Self::Timestamp(values, _) => Arc::new(TimestampMicrosecondArray::from(values)),
        };
        Ok(array)
    }
}

const fn parameter_kind(value: &ParameterValue) -> &'static str {
    match value {
        ParameterValue::Null { .. } => "null",
        ParameterValue::Bool(_) => "bool",
        ParameterValue::I32(_) => "int32",
        ParameterValue::I64(_) => "int64",
        ParameterValue::F64(_) => "float64",
        ParameterValue::String(_) => "string",
        ParameterValue::Bytes(_) => "bytes",
        ParameterValue::Decimal(_) => "decimal",
        ParameterValue::Date(_) => "date",
        ParameterValue::Timestamp(_) => "timestamp",
        ParameterValue::TimestampTz(_) => "timestamptz",
        ParameterValue::Json(_) => "json",
        ParameterValue::Uuid(_) => "uuid",
        ParameterValue::Enum { .. } => "enum",
        ParameterValue::Wkb { .. } => "wkb",
    }
}

#[allow(clippy::cast_possible_truncation)]
const fn narrow_f32(value: f64) -> f32 {
    value as f32
}

/// Segno, cifre significative ed esponente decimale della prima cifra; lo
/// zero e `(false, "", 0)`. `None` se il testo non e un decimale finito.
fn canonical_decimal(text: &str) -> Option<(bool, String, i64)> {
    let (negative, unsigned) = match text.as_bytes().first()? {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    let (mantissa, exponent) = match unsigned.find(['e', 'E']) {
        Some(index) => (
            &unsigned[..index],
            unsigned[index + 1..].parse::<i64>().ok()?,
        ),
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    let trimmed = digits.trim_start_matches('0');
    let leading = digits.len() - trimmed.len();
    let significant = trimmed.trim_end_matches('0');
    if significant.is_empty() {
        return Some((false, String::new(), 0));
    }
    let point = i64::try_from(whole.len())
        .ok()?
        .checked_add(exponent)?
        .checked_sub(i64::try_from(leading).ok()?)?;
    Some((negative, significant.to_owned(), point))
}

/// Il decimale `D` di un NUMBER (`FLOAT(b)`) come f64.
///
/// Regola: `D` diventa `x = f64(D)` solo se `D` e, come valore decimale, la
/// forma piu breve che ricostruisce `x` (quella di `{x:e}`). Cosi la
/// conversione e iniettiva: due decimali distinti non diventano mai lo stesso
/// f64, e rileggere e riscrivere `x` riconsegna `D`.
///
/// Ambito: la read Arrow delle colonne `FLOAT(b)`, `REAL`, `DOUBLE
/// PRECISION`, il cui tipo Arrow dichiarato resta Float64. Hazard evitato:
/// `0.1` e `0.10000000000000001`, o `1.0000000000000002` e la sua espansione
/// a 38 cifre, avevano gli stessi bit; due chiavi distinte sembravano uguali
/// e una read + write riscriveva il valore salvato. Coerente con il golden
/// (`float_coercion: false`): un decimale che f64 non distingue non viene
/// convertito ma rifiutato (`DataMapping`).
///
/// Condizione di rientro: per questi valori serve un target esatto
/// (`Decimal128` con scala dichiarata) o una politica lossy esplicita e
/// dichiarata nel `LossReport`; la write Oracle oggi qualifica solo `Strict`.
///
/// # Errors
///
/// `DataMapping` se il testo non e un decimale o non e la forma breve del
/// proprio f64.
pub fn decimal_as_f64(text: &str) -> Result<f64> {
    let unrepresentable = || mapping("NUMBER Oracle non rappresentabile esattamente in Float64");
    let source = canonical_decimal(text).ok_or_else(unrepresentable)?;
    let value = text
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .ok_or_else(unrepresentable)?;
    if canonical_decimal(&format!("{value:e}")).as_ref() != Some(&source) {
        return Err(unrepresentable());
    }
    Ok(value)
}

/// Byte di `DUMP()` di un `BINARY_FLOAT` (`Typ=100`, 4 byte) o
/// `BINARY_DOUBLE` (`Typ=101`, 8 byte) nel valore IEEE.
///
/// I byte vengono dal valore salvato, non da una conversione: un -0.0 legato
/// dalle versioni precedenti del driver e salvato non canonico (`7f ff ...`,
/// mentre Oracle normalizza ogni zero a `80 00 ...`), non e uguale a 0 nei
/// confronti del server e `UTL_RAW.CAST_FROM_BINARY_DOUBLE` lo rende +0.
/// Un valore cosi, i non finiti e un `DUMP` malformato sono `DataMapping`.
fn oracle_binary_float(dump: &str, type_code: u8, length: usize) -> Result<Option<[u8; 8]>> {
    let invalid = || mapping("BINARY_FLOAT/BINARY_DOUBLE Oracle non rappresentabile");
    if dump == "NULL" {
        return Ok(None);
    }
    let bytes = dump
        .strip_prefix(&format!("Typ={type_code} Len={length}: "))
        .ok_or_else(invalid)?
        .split(',')
        .map(|byte| byte.trim().parse::<u8>().map_err(|_| invalid()))
        .collect::<Result<Vec<_>>>()?;
    if bytes.len() != length {
        return Err(invalid());
    }
    // Formato Oracle: positivi con il bit di segno acceso, negativi con
    // tutti i bit invertiti.
    let mut ieee = [0_u8; 8];
    for (target, byte) in ieee.iter_mut().zip(&bytes) {
        *target = if bytes[0] & 0x80 == 0 { !byte } else { *byte };
    }
    if bytes[0] & 0x80 != 0 {
        ieee[0] ^= 0x80;
    }
    Ok(Some(ieee))
}

pub fn binary_double_from_dump(dump: &str) -> Result<Option<f64>> {
    let Some(ieee) = oracle_binary_float(dump, 101, 8)? else {
        return Ok(None);
    };
    let value = f64::from_be_bytes(ieee);
    if !value.is_finite() || (value == 0.0 && value.is_sign_negative()) {
        return Err(mapping("BINARY_DOUBLE Oracle non rappresentabile"));
    }
    Ok(Some(value))
}

pub fn binary_float_from_dump(dump: &str) -> Result<Option<f32>> {
    let Some(ieee) = oracle_binary_float(dump, 100, 4)? else {
        return Ok(None);
    };
    let value = f32::from_be_bytes([ieee[0], ieee[1], ieee[2], ieee[3]]);
    if !value.is_finite() || (value == 0.0 && value.is_sign_negative()) {
        return Err(mapping("BINARY_FLOAT Oracle non rappresentabile"));
    }
    Ok(Some(value))
}

fn rows_to_batch(
    plan: &OracleReadPlan,
    rows: &[Row],
    budget: &ResourceBudget,
    component_limit: u64,
) -> Result<(RecordBatch, u64)> {
    let mut values = plan
        .columns
        .iter()
        .map(|column| Values::new(column, rows.len()))
        .collect::<Vec<_>>();
    let mut components = 0_u64;
    for row in rows {
        if row.len() != plan.columns.len() + plan.spatial_columns.len() * 2 {
            return Err(read_error(
                ErrorCategory::Protocol,
                "result set Oracle con arieta inattesa",
            ));
        }
        let mut normalized_spatial = vec![None; plan.columns.len()];
        for (check, column_index) in plan.spatial_columns.iter().enumerate() {
            let geometry = row
                .get_index(*column_index)
                .ok_or_else(|| mapping("geometry Oracle assente"))?;
            let srid = row
                .get_index(plan.columns.len() + check * 2)
                .ok_or_else(|| mapping("controllo SRID Oracle assente"))?;
            let dimensions = row
                .get_index(plan.columns.len() + check * 2 + 1)
                .ok_or_else(|| mapping("controllo dimensioni Oracle assente"))?;
            normalized_spatial[*column_index] = validate_geometry(
                geometry,
                srid,
                dimensions,
                &plan.columns[*column_index],
                budget,
                component_limit,
                &mut components,
            )?
            .map(ParameterValue::Bytes);
        }
        for (index, output) in values.iter_mut().enumerate() {
            let value = normalized_spatial[index].as_ref().unwrap_or(
                row.get_index(index)
                    .ok_or_else(|| mapping("riga Oracle incompleta"))?,
            );
            output.push(value)?;
        }
    }
    let arrays = values
        .into_iter()
        .map(Values::finish)
        .collect::<Result<Vec<_>>>()?;
    let batch =
        RecordBatch::try_new(Arc::clone(&plan.schema), arrays).map_err(DatabaseError::from)?;
    Ok((batch, components))
}

fn validate_geometry(
    geometry: &ParameterValue,
    srid: &ParameterValue,
    dimensions: &ParameterValue,
    column: &crate::OracleColumnSpec,
    budget: &ResourceBudget,
    component_limit: u64,
    components: &mut u64,
) -> Result<Option<Vec<u8>>> {
    if matches!(geometry, ParameterValue::Null { .. }) {
        if !matches!(srid, ParameterValue::Null { .. })
            || !matches!(dimensions, ParameterValue::Null { .. })
        {
            return Err(read_error(
                ErrorCategory::Protocol,
                "controlli spatial Oracle non nulli per geometry nulla",
            ));
        }
        return Ok(None);
    }
    let ParameterValue::Bytes(bytes) = geometry else {
        return Err(mapping("projection WKB Oracle non binaria"));
    };
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > budget.limits().cell_bytes {
        return Err(DatabaseError::resource_limit(
            "geometry Oracle oltre il limite cella",
        ));
    }
    let catalog_srid = column
        .spatial_srid
        .ok_or_else(|| mapping("geometry Oracle senza SRID nel catalogo"))?;
    let catalog_dimensions = column
        .spatial_dimensions
        .ok_or_else(|| mapping("geometry Oracle senza dimensioni nel catalogo"))?;
    if numeric_u32(srid)? != catalog_srid {
        return Err(read_error(
            ErrorCategory::Crs,
            "SRID geometry Oracle diverso dal catalogo",
        ));
    }
    if numeric_u32(dimensions)? != u32::from(catalog_dimensions) {
        return Err(mapping("dimensioni geometry Oracle diverse dal catalogo"));
    }
    let remaining = component_limit.saturating_sub(*components);
    let normalized = if column.spatial_dimensions == Some(3) {
        plenora_database_core::ewkb::normalize_unmarked_xyz(
            bytes,
            remaining,
            budget.limits().nesting_depth,
        )
        .map_err(|mut error| {
            error.phase = ErrorPhase::Read;
            error.provider = Some(ProviderKind::Oracle);
            error
        })?
    } else {
        bytes.clone()
    };
    let inspected = plenora_database_core::ewkb::inspect_ewkb_detailed(
        &normalized,
        remaining,
        budget.limits().nesting_depth,
    )
    .map_err(|mut error| {
        error.phase = ErrorPhase::Read;
        error.provider = Some(ProviderKind::Oracle);
        error
    })?;
    if inspected.has_any_embedded_srid || inspected.has_any_m {
        return Err(mapping(
            "Oracle TO_WKBGEOMETRY ha prodotto un frame non qualificato",
        ));
    }
    *components = components
        .checked_add(inspected.stats.components)
        .ok_or_else(|| DatabaseError::resource_limit("overflow componenti geometry Oracle"))?;
    Ok(Some(normalized))
}

fn numeric_u32(value: &ParameterValue) -> Result<u32> {
    match value {
        ParameterValue::I64(value) => {
            u32::try_from(*value).map_err(|_| mapping("intero spatial Oracle oltre u32"))
        }
        ParameterValue::Decimal(value) => value
            .parse()
            .map_err(|_| mapping("numero spatial Oracle non rappresentabile")),
        _ => Err(mapping("numero spatial Oracle assente")),
    }
}

fn read_context(mut error: DatabaseError, message: &'static str) -> DatabaseError {
    message.clone_into(&mut error.message);
    error
}

fn parse_naive_timestamp(value: &str) -> Result<i64> {
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%dT%H:%M:%S"]
        .iter()
        .find_map(|format| NaiveDateTime::parse_from_str(value, format).ok())
        .map(|value| value.and_utc().timestamp_micros())
        .ok_or_else(|| mapping("timestamp Oracle non rappresentabile"))
}

fn parse_tz_timestamp(value: &str) -> Result<i64> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.timestamp_micros())
        .map_err(|_| mapping("timestamp con fuso Oracle non rappresentabile"))
}

/// Testo decimale del driver nel valore intero di un `Decimal128(precision,
/// scale)`.
///
/// Un testo senza cifre (`""`, `"-"`, `"."`) era letto come 0: il decoder
/// rendeva `""` per -infinito. Ora e un errore, come le cifre oltre la
/// precisione dichiarata, che `with_precision_and_scale` non controlla sui
/// valori.
pub fn parse_decimal(value: &str, precision: u8, scale: i8) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if (whole.is_empty() && fraction.is_empty())
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > usize::try_from(scale).unwrap_or(0)
    {
        return Err(mapping("NUMBER Oracle non rappresentabile in Decimal128"));
    }
    // La frazione sta gia nella scala: la parte intera deve stare nel resto.
    let integer_digits = usize::from(precision).saturating_sub(usize::try_from(scale).unwrap_or(0));
    if whole.trim_start_matches('0').len() > integer_digits {
        return Err(mapping("NUMBER Oracle oltre la precisione Decimal128"));
    }
    let mut digits = String::with_capacity(whole.len() + usize::try_from(scale).unwrap_or(0));
    digits.push_str(if whole.is_empty() { "0" } else { whole });
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n(
        '0',
        usize::try_from(scale).unwrap_or(0) - fraction.len(),
    ));
    let parsed = digits
        .parse::<i128>()
        .map_err(|_| mapping("NUMBER Oracle oltre i128"))?;
    Ok(if negative { -parsed } else { parsed })
}

fn read_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Read,
        Some(ProviderKind::Oracle),
        message,
    )
}

fn mapping(message: &'static str) -> DatabaseError {
    read_error(ErrorCategory::DataMapping, message)
}
