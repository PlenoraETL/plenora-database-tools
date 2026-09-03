use crate::catalog;
use crate::connection::{connect, with_timeout_duration};
use crate::decode::{decode_columns, row_from_driver};
use crate::error::interruption_error;
use crate::parameter::{bind_parameters_with_lobs, LobCache};
use crate::{OracleColumnKind, OracleConfig, OracleObjectDescription, OracleReadPlan};
use chrono::{DateTime, NaiveDateTime};
use oracle_rs::{ColumnInfo, Connection};
use plenora_database_core::arrow::array::builder::{BinaryBuilder, StringBuilder};
use plenora_database_core::arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
    TimestampMicrosecondArray,
};
use plenora_database_core::arrow::{RecordBatch, SchemaRef};
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::{FilterExpression, Operation, ProviderKind, ReadOperation};
use plenora_database_core::provider::{
    BatchStream, ParameterBag, ParameterValue, ProviderFuture, SecretString,
};
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

pub async fn read_operation(
    config: &OracleConfig,
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
    let connection = connect(config, secret, &internal).await?;
    let mut lob_cache = LobCache::default();
    let params = bind_parameters_with_lobs(
        &connection,
        &parameter_values,
        false,
        &mut lob_cache,
        config.operation_timeout(),
        ErrorPhase::Read,
        &internal,
    )
    .await?;
    let confirmed = catalog::describe_object(
        config,
        &connection,
        &plan.schema_name,
        &plan.object_name,
        &internal,
    )
    .await?;
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
        connection.query(&plan.sql, &params),
    )
    .await?;
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
            if *srid != Some(expected_srid)
                || *dimensions != expected_dimensions
                || *semantics != SpatialSemantics::Geometry
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
                SpatialSemantics::Geometry,
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
    connection: Connection,
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
                    self.connection
                        .fetch_more(self.cursor_id, &self.wire_columns, fetch_rows),
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
                        &self.connection,
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
            Ok(Some(batch))
        })
    }
}

enum Values {
    Bool(Vec<Option<bool>>),
    I64(Vec<Option<i64>>),
    F32(Vec<Option<f32>>),
    F64(Vec<Option<f64>>),
    Decimal(Vec<Option<i128>>, u8, i8),
    Utf8(Vec<Option<String>>),
    Binary(Vec<Option<Vec<u8>>>),
    Timestamp(Vec<Option<i64>>, bool),
}

impl Values {
    fn new(kind: &OracleColumnKind, capacity: usize) -> Self {
        match *kind {
            OracleColumnKind::Bool => Self::Bool(Vec::with_capacity(capacity)),
            OracleColumnKind::I64 => Self::I64(Vec::with_capacity(capacity)),
            OracleColumnKind::F32 => Self::F32(Vec::with_capacity(capacity)),
            OracleColumnKind::F64 => Self::F64(Vec::with_capacity(capacity)),
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
                Self::F64(values) => values.push(None),
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
            (Self::F32(values), ParameterValue::F64(value)) => {
                let narrowed = narrow_f32(*value);
                if !narrowed.is_finite() {
                    return Err(mapping("BINARY_FLOAT Oracle non rappresentabile"));
                }
                values.push(Some(narrowed));
            }
            (Self::F32(values), ParameterValue::Decimal(value)) => {
                let parsed = value
                    .parse::<f64>()
                    .map_err(|_| mapping("BINARY_FLOAT Oracle non rappresentabile"))?;
                let narrowed = narrow_f32(parsed);
                if !narrowed.is_finite() {
                    return Err(mapping("BINARY_FLOAT Oracle non rappresentabile"));
                }
                values.push(Some(narrowed));
            }
            (Self::F32(values), ParameterValue::I64(value)) => {
                values.push(Some(narrow_f32(i64_to_f64(*value))));
            }
            (Self::F64(values), ParameterValue::F64(value)) => values.push(Some(*value)),
            (Self::F64(values), ParameterValue::Decimal(value)) => values.push(Some(
                value
                    .parse()
                    .ok()
                    .filter(|value: &f64| value.is_finite())
                    .ok_or_else(|| mapping("BINARY_DOUBLE Oracle non rappresentabile"))?,
            )),
            (Self::F64(values), ParameterValue::I64(value)) => {
                values.push(Some(i64_to_f64(*value)));
            }
            (Self::Decimal(values, _, scale), ParameterValue::Decimal(value)) => {
                values.push(Some(parse_decimal(value, *scale)?));
            }
            (Self::Decimal(values, _, 0), ParameterValue::I64(value)) => {
                values.push(Some(i128::from(*value)));
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
            Self::F64(_) => "float64",
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

#[allow(clippy::cast_precision_loss)]
const fn i64_to_f64(value: i64) -> f64 {
    value as f64
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
        .map(|column| Values::new(&column.kind, rows.len()))
        .collect::<Vec<_>>();
    let mut components = 0_u64;
    for row in rows {
        if row.len() != plan.columns.len() + plan.spatial_columns.len() * 2 {
            return Err(read_error(
                ErrorCategory::Protocol,
                "result set Oracle con arieta inattesa",
            ));
        }
        for (index, output) in values.iter_mut().enumerate() {
            let value = row
                .get_index(index)
                .ok_or_else(|| mapping("riga Oracle incompleta"))?;
            output.push(value)?;
        }
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
            validate_geometry(
                geometry,
                srid,
                dimensions,
                &plan.columns[*column_index],
                budget,
                component_limit,
                &mut components,
            )?;
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
) -> Result<()> {
    if matches!(geometry, ParameterValue::Null { .. }) {
        if !matches!(srid, ParameterValue::Null { .. })
            || !matches!(dimensions, ParameterValue::Null { .. })
        {
            return Err(read_error(
                ErrorCategory::Protocol,
                "controlli spatial Oracle non nulli per geometry nulla",
            ));
        }
        return Ok(());
    }
    let ParameterValue::Bytes(bytes) = geometry else {
        return Err(mapping("projection WKB Oracle non binaria"));
    };
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > budget.limits().cell_bytes {
        return Err(DatabaseError::resource_limit(
            "geometry Oracle oltre il limite cella",
        ));
    }
    if numeric_u32(srid)? != column.spatial_srid.expect("geometry con SRID") {
        return Err(read_error(
            ErrorCategory::Crs,
            "SRID geometry Oracle diverso dal catalogo",
        ));
    }
    if numeric_u32(dimensions)?
        != u32::from(column.spatial_dimensions.expect("geometry con dimensioni"))
    {
        return Err(mapping("dimensioni geometry Oracle diverse dal catalogo"));
    }
    let remaining = component_limit.saturating_sub(*components);
    let inspected = plenora_database_core::ewkb::inspect_ewkb_detailed(
        bytes,
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
    Ok(())
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

fn parse_decimal(value: &str, scale: i8) -> Result<i128> {
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['+', '-']).unwrap_or(value);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > usize::try_from(scale).unwrap_or(0)
    {
        return Err(mapping("NUMBER Oracle non rappresentabile in Decimal128"));
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
