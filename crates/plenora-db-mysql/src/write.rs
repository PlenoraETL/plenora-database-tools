//! Prima tranche offline del path write `MySQL`.

use crate::types::{mysql_identifier, mysql_renderer};
use crate::{MysqlColumnKind, MysqlColumnSpec, MysqlObjectDescription};
use chrono::{Datelike, NaiveDate, Timelike};
use mysql_async::{Params, Value};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::field_contract::FieldContract;
use plenora_database_core::loss::{LossReport, MappingPolicy};
use plenora_database_core::outcome::{
    CertainPhase, Recovery, RowCounts, WriteOutcome, WriteStatus,
};
use plenora_database_core::plan::{TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use plenora_database_sql::ObjectName;

#[derive(Debug, Clone)]
struct MysqlWriteColumn {
    name: String,
    kind: MysqlColumnKind,
    nullable: bool,
    quoted: String,
    spatial_srid: Option<u32>,
    exact_geometry_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MysqlWritePlan {
    quoted_target: String,
    columns: Vec<MysqlWriteColumn>,
}

fn compile_write_column(
    field: &Field,
    renderer: &plenora_database_sql::Renderer,
) -> Result<MysqlWriteColumn> {
    let contract = FieldContract::parse(field)?;
    let (kind, spatial_srid, exact_geometry_type) = if contract.spatial {
        if !contract.is_geometry()
            || contract.encoding != Some("wkb")
            || field.data_type() != &DataType::Binary
        {
            return Err(unsupported(
                "write spatial MySQL richiede geometry GeoArrow WKB Binary",
            ));
        }
        if contract.dimensions != Some("xy") {
            return Err(unsupported("MySQL 8.4 qualifica soltanto geometrie XY"));
        }
        let srid = contract
            .srid
            .ok_or_else(|| crs_error("write spatial MySQL richiede SRID dichiarato"))?;
        let exact = match contract.types_declaration {
            Some("mixed") => None,
            Some("exact") => {
                let geometry_type = contract
                    .geometry_types
                    .ok_or_else(|| mapping_error("tipo geometrico exact assente dal contratto"))?;
                if geometry_type.contains(',') || !mysql_geometry_type_is_supported(geometry_type) {
                    return Err(unsupported(
                        "insieme di tipi geometrici non qualificato per MySQL",
                    ));
                }
                Some(geometry_type.to_ascii_lowercase())
            }
            _ => {
                return Err(unsupported(
                    "dichiarazione tipi geometrici non qualificata per MySQL",
                ));
            }
        };
        (MysqlColumnKind::Geometry, Some(srid), exact)
    } else {
        (write_column_kind(field)?, None, None)
    };
    Ok(MysqlWriteColumn {
        name: field.name().clone(),
        kind,
        nullable: field.is_nullable(),
        quoted: renderer.quote_identifier(&mysql_identifier(field.name())?),
        spatial_srid,
        exact_geometry_type,
    })
}

fn validate_spatial_policy(operation: &WriteOperation, columns: &[MysqlWriteColumn]) -> Result<()> {
    let spatial = columns
        .iter()
        .any(|column| column.kind == MysqlColumnKind::Geometry);
    match (spatial, operation.srid_policy) {
        (true, Some(plenora_database_core::plan::SridPolicy::RequireMatch)) | (false, None) => {
            Ok(())
        }
        (true, _) => Err(unsupported(
            "write spatial MySQL richiede SridPolicy::RequireMatch",
        )),
        (false, Some(_)) => Err(unsupported(
            "srid_policy non appartiene a una append MySQL scalare",
        )),
    }
}

fn mysql_geometry_type_is_supported(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    )
}

impl MysqlWritePlan {
    /// Compila il piano di scrittura di una `WriteOperation`.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso fuori dal sottoinsieme qualificato.
    pub(super) fn compile(
        schema: &SchemaRef,
        operation: &WriteOperation,
        database: &str,
    ) -> Result<Self> {
        plenora_database_core::field_contract::validate_schema_contract(schema.as_ref())?;
        validate_operation(operation, database)?;
        if schema.fields().is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "schema Arrow vuoto per append MySQL",
            ));
        }
        let renderer = mysql_renderer();
        let target_schema = operation.target.schema.as_deref().unwrap_or(database);
        let target = ObjectName {
            catalog: None,
            schema: Some(mysql_identifier(target_schema)?),
            object: mysql_identifier(&operation.target.object)?,
        };
        let columns = schema
            .fields()
            .iter()
            .map(|field| compile_write_column(field, &renderer))
            .collect::<Result<Vec<_>>>()?;
        validate_spatial_policy(operation, &columns)?;
        Ok(Self {
            quoted_target: renderer.quote_object(&target),
            columns,
        })
    }

    /// Renderizza un INSERT multi-riga con soli placeholder.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso fuori dai limiti di binding di `MySQL`.
    pub(super) fn render_insert(&self, rows: usize) -> Result<String> {
        if rows == 0 {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "INSERT MySQL richiede almeno una riga",
            ));
        }
        let placeholder_count = rows.checked_mul(self.columns.len()).ok_or_else(|| {
            prepare_error(
                ErrorCategory::ResourceLimit,
                "overflow nel conteggio dei placeholder MySQL",
            )
        })?;
        if placeholder_count > crate::MAX_BIND_PARAMETERS {
            return Err(prepare_error(
                ErrorCategory::ResourceLimit,
                format!(
                    "INSERT MySQL con {placeholder_count} placeholder oltre il limite di {}",
                    crate::MAX_BIND_PARAMETERS
                ),
            ));
        }

        let quoted_columns = self
            .columns
            .iter()
            .map(|column| column.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = self
            .columns
            .iter()
            .map(|column| {
                column.spatial_srid.map_or_else(
                    || "?".to_owned(),
                    |srid| format!("ST_GeomFromWKB(?, {srid})"),
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!(
            "INSERT INTO {} ({quoted_columns}) VALUES ",
            self.quoted_target
        );
        for row in 0..rows {
            if row > 0 {
                sql.push_str(", ");
            }
            sql.push('(');
            sql.push_str(&row_placeholders);
            sql.push(')');
        }
        sql.push(';');
        Ok(sql)
    }

    #[must_use]
    pub(super) const fn rows_per_statement(&self) -> usize {
        crate::MAX_BIND_PARAMETERS / self.columns.len()
    }

    pub(super) fn bind_chunk(
        &self,
        batch: &RecordBatch,
        start: usize,
        rows: usize,
    ) -> Result<Params> {
        if rows == 0
            || start
                .checked_add(rows)
                .is_none_or(|end| end > batch.num_rows())
            || batch.num_columns() != self.columns.len()
        {
            return Err(write_error(
                ErrorCategory::InvalidPlan,
                "intervallo batch MySQL non valido",
            ));
        }
        let capacity = rows.checked_mul(self.columns.len()).ok_or_else(|| {
            write_error(
                ErrorCategory::ResourceLimit,
                "overflow nel conteggio dei bind MySQL",
            )
        })?;
        let mut values = Vec::with_capacity(capacity);
        for row in start..start + rows {
            for (index, column) in self.columns.iter().enumerate() {
                let array = batch.column(index);
                if array.is_null(row) {
                    if !column.nullable {
                        return Err(prepare_error(
                            ErrorCategory::DataMapping,
                            format!("NULL nella colonna MySQL non nullable `{}`", column.name),
                        ));
                    }
                    values.push(Value::NULL);
                } else {
                    values.push(bind_value(array.as_ref(), row, &column.kind)?);
                }
            }
        }
        Ok(Params::Positional(values))
    }

    pub(super) fn validate_spatial_batch(
        &self,
        batch: &RecordBatch,
        budget: &ResourceBudget,
    ) -> Result<plenora_database_core::ewkb::EwkbStats> {
        let mut components = 0_u64;
        let mut max_depth = 0_u64;
        for (index, column) in self.columns.iter().enumerate() {
            if column.kind != MysqlColumnKind::Geometry {
                continue;
            }
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    write_error(
                        ErrorCategory::DataMapping,
                        "array geometry incoerente con il piano MySQL",
                    )
                })?;
            for row in 0..batch.num_rows() {
                if values.is_null(row) {
                    continue;
                }
                let remaining = budget.remaining(ResourceKind::GeometryComponents);
                let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(
                    values.value(row),
                    remaining,
                    budget.limits().nesting_depth,
                )
                .map_err(|mut error| {
                    error.phase = ErrorPhase::Write;
                    error.provider = Some(plenora_database_core::plan::ProviderKind::Mysql);
                    error
                })?;
                if inspection.has_any_z || inspection.has_any_m {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "MySQL 8.4 qualifica soltanto payload WKB XY",
                    ));
                }
                if inspection.has_any_embedded_srid {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "SRID embedded nel payload EWKB MySQL non qualificato",
                    ));
                }
                let geometry_type = inspection
                    .root
                    .geometry_type_name()
                    .filter(|value| mysql_geometry_type_is_supported(value))
                    .ok_or_else(|| {
                        write_error(
                            ErrorCategory::DataMapping,
                            "tipo geometry WKB non qualificato per MySQL",
                        )
                    })?;
                if column
                    .exact_geometry_type
                    .as_deref()
                    .is_some_and(|expected| !geometry_type.eq_ignore_ascii_case(expected))
                {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "tipo geometry WKB diverso dal contratto Arrow",
                    ));
                }
                components = components
                    .checked_add(inspection.stats.components)
                    .ok_or_else(|| {
                        write_error(
                            ErrorCategory::ResourceLimit,
                            "overflow componenti geometry MySQL",
                        )
                    })?;
                max_depth = max_depth.max(inspection.stats.max_depth);
            }
        }
        if components > 0 {
            budget
                .try_lease(ResourceKind::GeometryComponents, components)?
                .commit(components)?;
        }
        Ok(plenora_database_core::ewkb::EwkbStats {
            components,
            max_depth,
        })
    }

    pub(super) fn preflight(&self, target: &MysqlObjectDescription) -> Result<LossReport> {
        if target.kind != "BASE TABLE" {
            return Err(unsupported("append MySQL richiede una BASE TABLE"));
        }
        if target.engine.as_deref() != Some("InnoDB") {
            return Err(unsupported(
                "append SingleTransaction MySQL richiede una tabella InnoDB",
            ));
        }
        for column in &self.columns {
            let server = target
                .columns
                .iter()
                .find(|candidate| candidate.name == column.name)
                .ok_or_else(|| mapping_error("colonna target MySQL mancante"))?;
            if !server.generation_expression.is_empty() {
                return Err(mapping_error(
                    "append MySQL non puo scrivere una colonna generata",
                ));
            }
            let spec = MysqlColumnSpec::from_catalog(server)?;
            if column.kind == MysqlColumnKind::Geometry {
                if spec.kind != MysqlColumnKind::Geometry {
                    return Err(mapping_error(
                        "campo geometry Arrow diretto a una colonna MySQL non spatial",
                    ));
                }
                if server.spatial_srid != column.spatial_srid {
                    return Err(crs_error("SRID target MySQL diverso dal contratto Arrow"));
                }
                let native = server.data_type.to_ascii_lowercase();
                if native != "geometry"
                    && column.exact_geometry_type.as_deref() != Some(native.as_str())
                {
                    return Err(mapping_error(
                        "tipo geometry target MySQL incompatibile col contratto Arrow",
                    ));
                }
                if column.exact_geometry_type.is_none() && native != "geometry" {
                    return Err(mapping_error(
                        "geometrie mixed richiedono una colonna MySQL GEOMETRY",
                    ));
                }
            } else if spec.kind != column.kind
                || !write_native_type_is_qualified(server, &column.kind)
            {
                return Err(mapping_error(
                    "schema Arrow incompatibile con la colonna target MySQL",
                ));
            }
            if column.nullable && !server.nullable {
                return Err(mapping_error(
                    "nullability Arrow incompatibile con la colonna target MySQL",
                ));
            }
        }
        for server in &target.columns {
            if self.columns.iter().any(|column| column.name == server.name) {
                continue;
            }
            let generated = !server.generation_expression.is_empty();
            let automatic = server
                .extra
                .split_ascii_whitespace()
                .any(|part| part.eq_ignore_ascii_case("auto_increment"));
            if !server.nullable && server.default_expression.is_none() && !generated && !automatic {
                return Err(mapping_error(
                    "colonna target MySQL obbligatoria assente dallo schema Arrow",
                ));
            }
        }
        Ok(LossReport {
            schema_version: 1,
            policy: MappingPolicy::Strict,
            losses: Vec::new(),
        })
    }
}

pub fn validate_batch_schema(batch: &RecordBatch, declared: &SchemaRef) -> Result<()> {
    if batch.schema().as_ref() == declared.as_ref() {
        Ok(())
    } else {
        Err(write_error(
            ErrorCategory::InvalidPlan,
            "schema del batch MySQL diverso dallo schema dichiarato",
        ))
    }
}

pub fn committed_outcome(
    execution_id: String,
    received: u64,
    inserted: u64,
) -> Result<WriteOutcome> {
    let outcome = WriteOutcome {
        schema_version: 1,
        status: WriteStatus::Committed,
        execution_id,
        provider: plenora_database_core::plan::ProviderKind::Mysql,
        rows: RowCounts {
            received,
            confirmed: inserted,
            inserted: Some(inserted),
            updated: Some(0),
            deleted: Some(0),
            failed: 0,
            skipped: 0,
        },
        layer_outcomes: Vec::new(),
        recovery: None,
    };
    outcome.validate().map_err(|mut error| {
        error.category = ErrorCategory::Internal;
        error.phase = ErrorPhase::Write;
        error.provider = Some(plenora_database_core::plan::ProviderKind::Mysql);
        error.execution_id = Some(outcome.execution_id.clone());
        error
    })?;
    Ok(outcome)
}

pub fn commit_failure(
    mut error: DatabaseError,
    execution_id: String,
    received: u64,
) -> Result<WriteOutcome> {
    error.execution_id = Some(execution_id.clone());
    if error.remote_effect == RemoteEffect::RolledBack {
        return Err(error);
    }
    let outcome = WriteOutcome {
        schema_version: 1,
        status: WriteStatus::OutcomeUnknown,
        execution_id,
        provider: plenora_database_core::plan::ProviderKind::Mysql,
        rows: RowCounts {
            received,
            confirmed: 0,
            inserted: None,
            updated: None,
            deleted: None,
            failed: 0,
            skipped: 0,
        },
        layer_outcomes: Vec::new(),
        recovery: Some(Recovery {
            last_certain_phase: CertainPhase::CommitOrEditRequested,
            automatic_retry_allowed: false,
            idempotency_key: None,
            staging_object: None,
            verification_action: Some(
                "verificare la tabella target MySQL prima di qualsiasi retry".to_owned(),
            ),
        }),
    };
    outcome.validate()?;
    Ok(outcome)
}

pub fn rolled_back_error(
    mut error: DatabaseError,
    rollback_confirmed: bool,
    execution_id: &str,
) -> DatabaseError {
    error.execution_id = Some(execution_id.to_owned());
    if rollback_confirmed || error.remote_effect == RemoteEffect::RolledBack {
        error.remote_effect = RemoteEffect::RolledBack;
    } else {
        error.remote_effect = RemoteEffect::Unknown;
        if error.retry != RetryDisposition::Quarantine {
            error.retry = RetryDisposition::RequiresRecovery;
        }
    }
    error
}

fn bind_value(array: &dyn Array, row: usize, kind: &MysqlColumnKind) -> Result<Value> {
    macro_rules! primitive {
        ($array:ty, $value:expr) => {{
            let values = array
                .as_any()
                .downcast_ref::<$array>()
                .ok_or_else(|| mapping_error("array Arrow incoerente con il piano MySQL"))?;
            $value(values.value(row))
        }};
    }
    Ok(match kind {
        MysqlColumnKind::Bool => primitive!(BooleanArray, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::I8 => primitive!(Int8Array, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::U8 => primitive!(UInt8Array, |value| Value::UInt(u64::from(value))),
        MysqlColumnKind::I16 => primitive!(Int16Array, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::U16 => primitive!(UInt16Array, |value| Value::UInt(u64::from(value))),
        MysqlColumnKind::I32 => primitive!(Int32Array, |value| Value::Int(i64::from(value))),
        MysqlColumnKind::U32 => primitive!(UInt32Array, |value| Value::UInt(u64::from(value))),
        MysqlColumnKind::I64 => primitive!(Int64Array, Value::Int),
        MysqlColumnKind::U64 => primitive!(UInt64Array, Value::UInt),
        MysqlColumnKind::F32 => primitive!(Float32Array, Value::Float),
        MysqlColumnKind::F64 => primitive!(Float64Array, Value::Double),
        MysqlColumnKind::Utf8 => primitive!(StringArray, |value: &str| Value::Bytes(
            value.as_bytes().to_vec()
        )),
        MysqlColumnKind::Binary | MysqlColumnKind::Geometry => {
            primitive!(BinaryArray, |value: &[u8]| Value::Bytes(value.to_vec()))
        }
        MysqlColumnKind::Date => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| mapping_error("array Date32 incoerente con il piano MySQL"))?
                .value(row);
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(i64::from(days))))
                .ok_or_else(|| mapping_error("Date32 fuori intervallo MySQL"))?;
            Value::Date(
                u16::try_from(date.year())
                    .map_err(|_| mapping_error("anno fuori intervallo MySQL"))?,
                u8::try_from(date.month())
                    .map_err(|_| mapping_error("mese data fuori intervallo MySQL"))?,
                u8::try_from(date.day())
                    .map_err(|_| mapping_error("giorno data fuori intervallo MySQL"))?,
                0,
                0,
                0,
                0,
            )
        }
        MysqlColumnKind::Timestamp => {
            let micros = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| mapping_error("array timestamp incoerente con il piano MySQL"))?
                .value(row);
            let instant = chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(|| mapping_error("timestamp fuori intervallo MySQL"))?
                .naive_utc();
            Value::Date(
                u16::try_from(instant.year())
                    .map_err(|_| mapping_error("anno timestamp fuori intervallo MySQL"))?,
                u8::try_from(instant.month())
                    .map_err(|_| mapping_error("mese timestamp fuori intervallo MySQL"))?,
                u8::try_from(instant.day())
                    .map_err(|_| mapping_error("giorno timestamp fuori intervallo MySQL"))?,
                u8::try_from(instant.hour())
                    .map_err(|_| mapping_error("ora timestamp fuori intervallo MySQL"))?,
                u8::try_from(instant.minute())
                    .map_err(|_| mapping_error("minuto timestamp fuori intervallo MySQL"))?,
                u8::try_from(instant.second())
                    .map_err(|_| mapping_error("secondo timestamp fuori intervallo MySQL"))?,
                instant.nanosecond() / 1_000,
            )
        }
        MysqlColumnKind::Decimal { scale, .. } => {
            let value = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| mapping_error("array decimal incoerente con il piano MySQL"))?
                .value(row);
            Value::Bytes(decimal_text(value, *scale)?.into_bytes())
        }
        MysqlColumnKind::Time => {
            return Err(mapping_error("tipo MySQL non qualificato per append"));
        }
    })
}

fn decimal_text(value: i128, scale: i8) -> Result<String> {
    let scale = usize::try_from(scale).map_err(|_| mapping_error("scala decimal negativa"))?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let text = value.to_string();
    let (sign, digits) = text
        .strip_prefix('-')
        .map_or(("", text.as_str()), |digits| ("-", digits));
    let body = if digits.len() <= scale {
        format!("0.{}{digits}", "0".repeat(scale - digits.len()))
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    Ok(format!("{sign}{body}"))
}

fn write_native_type_is_qualified(column: &crate::MysqlColumn, kind: &MysqlColumnKind) -> bool {
    let native = column.data_type.to_ascii_lowercase();
    match kind {
        MysqlColumnKind::Bool | MysqlColumnKind::I8 | MysqlColumnKind::U8 => native == "tinyint",
        MysqlColumnKind::I16 | MysqlColumnKind::U16 => native == "smallint",
        MysqlColumnKind::I32 | MysqlColumnKind::U32 => {
            matches!(native.as_str(), "mediumint" | "int" | "integer")
        }
        MysqlColumnKind::I64 | MysqlColumnKind::U64 => native == "bigint",
        MysqlColumnKind::F32 => native == "float",
        MysqlColumnKind::F64 => matches!(native.as_str(), "double" | "real"),
        MysqlColumnKind::Utf8 => matches!(
            native.as_str(),
            "varchar" | "tinytext" | "text" | "mediumtext" | "longtext"
        ),
        MysqlColumnKind::Binary => matches!(
            native.as_str(),
            "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob"
        ),
        MysqlColumnKind::Date => native == "date",
        MysqlColumnKind::Timestamp => {
            matches!(native.as_str(), "datetime" | "timestamp")
                && column.datetime_precision == Some(6)
        }
        MysqlColumnKind::Decimal { .. } => matches!(native.as_str(), "decimal" | "numeric"),
        MysqlColumnKind::Time | MysqlColumnKind::Geometry => false,
    }
}

fn mapping_error(message: impl Into<String>) -> DatabaseError {
    prepare_error(ErrorCategory::DataMapping, message)
}

fn crs_error(message: impl Into<String>) -> DatabaseError {
    prepare_error(ErrorCategory::Crs, message)
}

fn write_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    let mut error = prepare_error(category, message);
    error.phase = ErrorPhase::Write;
    error
}

fn validate_operation(operation: &WriteOperation, database: &str) -> Result<()> {
    if operation.mode != WriteMode::Append {
        return Err(unsupported(
            "solo Append MySQL e qualificata in questa tranche",
        ));
    }
    if operation.transaction_profile != TransactionProfile::SingleTransaction {
        return Err(unsupported(
            "append MySQL richiede il profilo SingleTransaction",
        ));
    }
    if operation.mapping_policy != MappingPolicy::Strict {
        return Err(unsupported(
            "append MySQL richiede MappingPolicy::Strict finche il loss preflight non e qualificato",
        ));
    }
    if operation.allow_partial {
        return Err(unsupported("append MySQL parziale non qualificata"));
    }
    if !operation.keys.is_empty() || !operation.update_columns.is_empty() {
        return Err(unsupported(
            "keys e update_columns non appartengono ad Append MySQL",
        ));
    }
    if operation.create_spatial_index {
        return Err(unsupported(
            "creazione indice spatial MySQL non ancora qualificata",
        ));
    }
    if operation.target.layer_id.is_some() {
        return Err(unsupported("layer_id non appartiene al provider MySQL"));
    }
    if operation
        .target
        .catalog
        .as_deref()
        .is_some_and(|catalog| catalog != database)
        || operation
            .target
            .schema
            .as_deref()
            .is_some_and(|schema| schema != database)
    {
        return Err(unsupported(
            "target cross-database MySQL non supportato dal provider",
        ));
    }
    Ok(())
}

fn write_column_kind(field: &Field) -> Result<MysqlColumnKind> {
    let kind = match field.data_type() {
        DataType::Boolean => MysqlColumnKind::Bool,
        DataType::Int8 => MysqlColumnKind::I8,
        DataType::UInt8 => MysqlColumnKind::U8,
        DataType::Int16 => MysqlColumnKind::I16,
        DataType::UInt16 => MysqlColumnKind::U16,
        DataType::Int32 => MysqlColumnKind::I32,
        DataType::UInt32 => MysqlColumnKind::U32,
        DataType::Int64 => MysqlColumnKind::I64,
        DataType::UInt64 => MysqlColumnKind::U64,
        DataType::Float32 => MysqlColumnKind::F32,
        DataType::Float64 => MysqlColumnKind::F64,
        DataType::Utf8 => MysqlColumnKind::Utf8,
        DataType::Binary => MysqlColumnKind::Binary,
        DataType::Date32 => MysqlColumnKind::Date,
        DataType::Timestamp(TimeUnit::Microsecond, None) => MysqlColumnKind::Timestamp,
        DataType::Decimal128(precision, scale)
            if *precision > 0
                && *precision <= 38
                && *scale >= 0
                && *scale <= precision.cast_signed() =>
        {
            MysqlColumnKind::Decimal {
                precision: *precision,
                scale: *scale,
            }
        }
        other => {
            return Err(unsupported(format!(
                "tipo Arrow non qualificato per append MySQL: {other:?}"
            )));
        }
    };
    Ok(kind)
}

fn unsupported(message: impl Into<String>) -> DatabaseError {
    prepare_error(ErrorCategory::Unsupported, message)
}

fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MysqlColumn, MysqlObjectDescription, MysqlSchemaToken, MAX_BIND_PARAMETERS};
    use chrono::NaiveDate;
    use mysql_async::{Params, Value};
    use plenora_database_core::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array,
        Int64Array, StringArray, TimestampMicrosecondArray, UInt32Array,
    };
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    use plenora_database_core::arrow::RecordBatch;
    use plenora_database_core::loss::MappingPolicy;
    use plenora_database_core::outcome::{CertainPhase, WriteStatus};
    use plenora_database_core::plan::{ObjectRef, ProviderKind, TransactionProfile, WriteMode};
    use plenora_database_core::protocol;
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            fields,
            HashMap::from([(
                protocol::CONTRACT_VERSION_KEY.to_owned(),
                protocol::CONTRACT_VERSION.to_owned(),
            )]),
        ))
    }

    fn append_operation() -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("warehouse".to_owned()),
                object: "events".to_owned(),
                layer_id: None,
            },
            mode: WriteMode::Append,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    fn append_plan(fields: Vec<Field>) -> MysqlWritePlan {
        MysqlWritePlan::compile(&schema(fields), &append_operation(), "warehouse")
            .expect("piano append qualificato")
    }

    /// L'ordine delle colonne e quello dello schema Arrow, non un ordine
    /// ricavato dal nome: e l'unico che resta allineato ai buffer di riga.
    #[test]
    fn insert_renders_qualified_quoted_columns_in_schema_order() {
        let plan = append_plan(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        assert_eq!(
            plan.render_insert(2).expect("insert di due righe"),
            "INSERT INTO `warehouse`.`events` (`id`, `label`) VALUES (?, ?), (?, ?);"
        );

        let escaped = append_plan(vec![
            Field::new("zeta", DataType::Int64, false),
            Field::new("al`pha", DataType::Utf8, false),
        ]);
        assert_eq!(
            escaped.render_insert(1).expect("insert di una riga"),
            "INSERT INTO `warehouse`.`events` (`zeta`, `al``pha`) VALUES (?, ?);"
        );
    }

    /// Un INSERT senza righe non e una scrittura vuota: e una VALUES list
    /// sintatticamente invalida che il server rifiuterebbe dopo la rete.
    #[test]
    fn insert_requires_at_least_one_row() {
        let error = append_plan(vec![Field::new("id", DataType::Int64, false)])
            .render_insert(0)
            .expect_err("insert senza righe");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il tetto di 65.535 placeholder e del protocollo: superarlo va visto
    /// prima del `COM_STMT_PREPARE`, non nell'errore del server.
    #[test]
    fn insert_stops_at_the_placeholder_ceiling_before_the_network() {
        let plan = append_plan(vec![Field::new("id", DataType::Int64, false)]);
        let sql = plan
            .render_insert(MAX_BIND_PARAMETERS)
            .expect("insert al tetto dei placeholder");
        assert_eq!(sql.matches('?').count(), MAX_BIND_PARAMETERS);
        let error = plan
            .render_insert(MAX_BIND_PARAMETERS + 1)
            .expect_err("insert oltre il tetto dei placeholder");
        assert_eq!(error.category, ErrorCategory::ResourceLimit);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il conteggio dei placeholder e un prodotto: senza controllo esplicito
    /// un overflow lo riporterebbe dentro il tetto invece di rifiutarlo.
    #[test]
    fn insert_row_count_overflow_is_checked() {
        let plan = append_plan(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, false),
        ]);
        let error = plan
            .render_insert(usize::MAX / 2 + 1)
            .expect_err("prodotto righe per colonne in overflow");
        assert_eq!(error.category, ErrorCategory::ResourceLimit);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    #[test]
    fn compile_accepts_supported_arrow_types_in_schema_order() {
        let plan = append_plan(vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new(
                "created_at",
                DataType::Timestamp(
                    plenora_database_core::arrow::schema::TimeUnit::Microsecond,
                    None,
                ),
                false,
            ),
        ]);
        assert_eq!(plan.columns[0].kind, MysqlColumnKind::Bool);
        assert_eq!(plan.columns[1].kind, MysqlColumnKind::I64);
        assert_eq!(
            plan.columns[2].kind,
            MysqlColumnKind::Decimal {
                precision: 12,
                scale: 2,
            }
        );
        assert_eq!(plan.columns[3].kind, MysqlColumnKind::Timestamp);
        assert_eq!(plan.columns[0].name, "flag");
        assert!(!plan.columns[0].nullable);
        assert_eq!(plan.columns[2].quoted, "`amount`");
    }

    #[test]
    fn compile_rejects_unqualified_operation_shapes_before_the_network() {
        let input = schema(vec![Field::new("id", DataType::Int64, false)]);
        let mut cases = Vec::new();

        let mut operation = append_operation();
        operation.mode = WriteMode::Update;
        cases.push(operation);

        let mut operation = append_operation();
        operation.transaction_profile = TransactionProfile::ChunkCommitted;
        cases.push(operation);

        let mut operation = append_operation();
        operation.allow_partial = true;
        cases.push(operation);

        let mut operation = append_operation();
        operation.keys.push("id".to_owned());
        cases.push(operation);

        let mut operation = append_operation();
        operation.update_columns.push("label".to_owned());
        cases.push(operation);

        let mut operation = append_operation();
        operation.create_spatial_index = true;
        cases.push(operation);

        let mut operation = append_operation();
        operation.mapping_policy = MappingPolicy::Lossy;
        cases.push(operation);

        for operation in cases {
            let error = MysqlWritePlan::compile(&input, &operation, "warehouse")
                .expect_err("forma write non qualificata");
            assert_eq!(error.category, ErrorCategory::Unsupported);
            assert_eq!(error.phase, ErrorPhase::Prepare);
        }
    }

    #[test]
    fn compile_rejects_cross_database_and_layer_targets() {
        let input = schema(vec![Field::new("id", DataType::Int64, false)]);

        let mut cross_database = append_operation();
        cross_database.target.schema = Some("other_database".to_owned());
        let error = MysqlWritePlan::compile(&input, &cross_database, "warehouse")
            .expect_err("target cross-database");
        assert_eq!(error.category, ErrorCategory::Unsupported);

        let mut layer = append_operation();
        layer.target.layer_id = Some(plenora_database_core::plan::LayerId::Number(1));
        let error = MysqlWritePlan::compile(&input, &layer, "warehouse").expect_err("layer MySQL");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    #[test]
    fn compile_rejects_empty_or_unqualified_arrow_schemas() {
        let error = MysqlWritePlan::compile(&schema(Vec::new()), &append_operation(), "warehouse")
            .expect_err("schema vuoto");
        assert_eq!(error.category, ErrorCategory::Schema);

        let unsupported = schema(vec![Field::new(
            "created_at",
            DataType::Timestamp(
                plenora_database_core::arrow::schema::TimeUnit::Nanosecond,
                Some("UTC".into()),
            ),
            false,
        )]);
        let error = MysqlWritePlan::compile(&unsupported, &append_operation(), "warehouse")
            .expect_err("timestamp con timezone non qualificato");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    /// Il contratto Arrow e parte del piano: una versione estranea non puo
    /// essere interpretata e non deve arrivare al server.
    #[test]
    fn compile_rejects_a_foreign_contract_version() {
        let foreign = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            HashMap::from([(
                protocol::CONTRACT_VERSION_KEY.to_owned(),
                "999.0".to_owned(),
            )]),
        ));
        let error = MysqlWritePlan::compile(&foreign, &append_operation(), "warehouse")
            .expect_err("contratto Arrow estraneo");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }

    fn server_column(
        name: &str,
        data_type: &str,
        declaration: &str,
        nullable: bool,
    ) -> MysqlColumn {
        MysqlColumn {
            name: name.to_owned(),
            ordinal: 1,
            data_type: data_type.to_owned(),
            native_declaration: declaration.to_owned(),
            nullable,
            default_expression: None,
            character_set: None,
            collation: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            spatial_srid: None,
            extra: String::new(),
            generation_expression: String::new(),
        }
    }

    fn base_table(columns: Vec<MysqlColumn>) -> MysqlObjectDescription {
        MysqlObjectDescription {
            schema: "warehouse".to_owned(),
            name: "events".to_owned(),
            kind: "BASE TABLE".to_owned(),
            engine: Some("InnoDB".to_owned()),
            columns,
            token: MysqlSchemaToken("token".to_owned()),
        }
    }

    fn identity_plan() -> MysqlWritePlan {
        append_plan(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ])
    }

    fn identity_target() -> Vec<MysqlColumn> {
        vec![
            server_column("id", "bigint", "bigint", false),
            server_column("label", "varchar", "varchar(32)", true),
        ]
    }

    fn server_error(code: u16, message: &str) -> mysql_async::Error {
        mysql_async::Error::Server(mysql_async::ServerError {
            code,
            message: message.to_owned(),
            state: "HY000".to_owned(),
        })
    }

    /// Il chunk non dipende dai dati ma dal numero di colonne: due esecuzioni
    /// della stessa append devono produrre esattamente gli stessi INSERT.
    #[test]
    fn chunk_size_is_deterministic_and_fits_the_placeholder_ceiling() {
        let single = append_plan(vec![Field::new("id", DataType::Int64, false)]);
        assert_eq!(single.rows_per_statement(), MAX_BIND_PARAMETERS);
        let pair = identity_plan();
        assert_eq!(pair.rows_per_statement(), MAX_BIND_PARAMETERS / 2);
        assert_eq!(
            pair.rows_per_statement(),
            identity_plan().rows_per_statement()
        );
        assert_eq!(
            pair.render_insert(pair.rows_per_statement())
                .expect("chunk al tetto")
                .matches('?')
                .count(),
            pair.rows_per_statement() * 2
        );
    }

    /// I valori viaggiano come bind del protocollo binario: il testo SQL resta
    /// fatto di soli placeholder anche per testo, decimal e NULL.
    #[test]
    fn chunk_binding_is_positional_and_never_interpolates_values() {
        let fields = vec![
            Field::new("flag", DataType::Boolean, false),
            Field::new("id", DataType::Int64, false),
            Field::new("count", DataType::UInt32, false),
            Field::new("ratio", DataType::Float64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
            Field::new("day", DataType::Date32, false),
            Field::new(
                "moment",
                DataType::Timestamp(
                    plenora_database_core::arrow::schema::TimeUnit::Microsecond,
                    None,
                ),
                false,
            ),
            Field::new("amount", DataType::Decimal128(12, 2), true),
        ];
        let plan = append_plan(fields.clone());
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
        let day = NaiveDate::from_ymd_opt(2026, 1, 2).expect("giorno");
        let days = i32::try_from(day.signed_duration_since(epoch).num_days()).expect("date32");
        let micros = day
            .and_hms_micro_opt(3, 4, 5, 123_456)
            .expect("istante")
            .and_utc()
            .timestamp_micros();
        let columns: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![true, false])),
            Arc::new(Int64Array::from(vec![7, -7])),
            Arc::new(UInt32Array::from(vec![4_000_000_000, 0])),
            Arc::new(Float64Array::from(vec![1.5, -2.25])),
            Arc::new(StringArray::from(vec![Some("reference"), None])),
            Arc::new(BinaryArray::from_opt_vec(vec![Some(&[1_u8, 2][..]), None])),
            Arc::new(Date32Array::from(vec![days, days])),
            Arc::new(TimestampMicrosecondArray::from(vec![micros, micros])),
            Arc::new(
                Decimal128Array::from(vec![Some(-105_i128), None])
                    .with_precision_and_scale(12, 2)
                    .expect("decimal"),
            ),
        ];
        let batch = RecordBatch::try_new(schema(fields), columns).expect("batch append");
        let Params::Positional(values) = plan.bind_chunk(&batch, 0, 2).expect("bind del chunk")
        else {
            panic!("bind MySQL non posizionale");
        };
        assert_eq!(values.len(), 18);
        assert_eq!(values[0], Value::Int(1));
        assert_eq!(values[1], Value::Int(7));
        assert_eq!(values[2], Value::UInt(4_000_000_000));
        assert_eq!(values[3], Value::Double(1.5));
        assert_eq!(values[4], Value::Bytes(b"reference".to_vec()));
        assert_eq!(values[5], Value::Bytes(vec![1, 2]));
        assert_eq!(values[6], Value::Date(2026, 1, 2, 0, 0, 0, 0));
        assert_eq!(values[7], Value::Date(2026, 1, 2, 3, 4, 5, 123_456));
        assert_eq!(values[8], Value::Bytes(b"-1.05".to_vec()));
        assert_eq!(values[9], Value::Int(0));
        assert_eq!(values[13], Value::NULL);
        assert_eq!(values[14], Value::NULL);
        assert_eq!(values[17], Value::NULL);

        let sql = plan.render_insert(2).expect("insert del chunk");
        assert!(!sql.contains("reference"), "{sql}");
        assert!(!sql.contains("1.05"), "{sql}");
        assert!(!sql.to_ascii_uppercase().contains("INFILE"), "{sql}");
    }

    /// Una cella NULL in una colonna dichiarata non nullable e un errore di
    /// mapping locale: va vista prima di aprire la transazione.
    #[test]
    fn null_cells_in_non_nullable_columns_fail_before_the_network() {
        let plan = append_plan(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            schema(vec![Field::new("id", DataType::Int64, true)]),
            vec![Arc::new(Int64Array::from(vec![None, Some(2)])) as ArrayRef],
        )
        .expect("batch con NULL");
        let error = plan
            .bind_chunk(&batch, 0, 2)
            .expect_err("NULL in colonna non nullable");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }

    /// Il chunk deve restare dentro il batch: un intervallo fuori misura e un
    /// errore esplicito, non una lettura oltre la fine dell'array.
    #[test]
    fn chunk_bounds_are_checked_against_the_batch() {
        let fields = vec![Field::new("id", DataType::Int64, false)];
        let plan = append_plan(fields.clone());
        let batch = RecordBatch::try_new(
            schema(fields),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef],
        )
        .expect("batch");
        assert_eq!(
            plan.bind_chunk(&batch, 1, 2)
                .expect_err("chunk oltre il batch")
                .category,
            ErrorCategory::InvalidPlan
        );
        assert_eq!(
            plan.bind_chunk(&batch, 0, 0)
                .expect_err("chunk vuoto")
                .category,
            ErrorCategory::InvalidPlan
        );
    }

    /// Lo schema del batch e quello dichiarato dallo stream: una deriva va
    /// vista prima di convertire i valori.
    #[test]
    fn batch_schema_drift_is_rejected_before_binding() {
        let declared = schema(vec![Field::new("id", DataType::Int64, false)]);
        let stable = RecordBatch::try_new(
            Arc::clone(&declared),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("batch stabile");
        validate_batch_schema(&stable, &declared).expect("schema stabile");

        let drifted = RecordBatch::try_new(
            schema(vec![Field::new("renamed", DataType::Int64, false)]),
            vec![Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef],
        )
        .expect("batch deviato");
        let error = validate_batch_schema(&drifted, &declared).expect_err("schema deviato");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(error.phase, ErrorPhase::Write);
    }

    /// Strict puo dichiarare zero perdite solo dopo aver visto lo schema del
    /// server: e il preflight, non il piano offline, a stabilirlo.
    #[test]
    fn server_preflight_reports_no_losses_only_for_a_compatible_table() {
        let report = identity_plan()
            .preflight(&base_table(vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "varchar", "varchar(32)", true),
                server_column("noted_at", "datetime", "datetime(6)", true),
            ]))
            .expect("preflight compatibile");
        assert_eq!(report.schema_version, 1);
        assert_eq!(report.policy, MappingPolicy::Strict);
        assert!(report.losses.is_empty());
        assert!(report.permits_execution());
    }

    /// Ogni divergenza fra schema Arrow e schema server e una perdita che
    /// Strict non ammette: nessuna transazione deve essere aperta.
    #[test]
    fn server_preflight_rejects_targets_that_strict_cannot_write() {
        let plan = identity_plan();
        let cases = vec![
            vec![server_column("id", "bigint", "bigint", false)],
            vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "int", "int", true),
            ],
            vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "varchar", "varchar(32)", false),
            ],
            vec![
                server_column("id", "bigint", "bigint", false),
                server_column("label", "varchar", "varchar(32)", true),
                server_column("mandatory", "int", "int", false),
            ],
        ];
        for columns in cases {
            let error = plan
                .preflight(&base_table(columns))
                .expect_err("target incompatibile");
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert_eq!(error.phase, ErrorPhase::Prepare);
        }

        let mut generated = base_table(identity_target());
        generated.columns[1].generation_expression = "concat('x')".to_owned();
        assert_eq!(
            plan.preflight(&generated)
                .expect_err("colonna generata")
                .category,
            ErrorCategory::DataMapping
        );

        let mut view = base_table(identity_target());
        view.kind = "VIEW".to_owned();
        assert_eq!(
            plan.preflight(&view)
                .expect_err("target non tabella")
                .category,
            ErrorCategory::Unsupported
        );
    }

    /// Una colonna JSON, ENUM o BIT non e ancora un target di scrittura
    /// qualificato anche se in lettura collassa su Utf8 o Binary.
    #[test]
    fn server_preflight_keeps_unqualified_write_targets_closed() {
        let plan = identity_plan();
        for (data_type, declaration) in [
            ("json", "json"),
            ("enum", "enum('alpha','beta')"),
            ("set", "set('read','write')"),
            ("char", "char(8)"),
        ] {
            let error = plan
                .preflight(&base_table(vec![
                    server_column("id", "bigint", "bigint", false),
                    server_column("label", data_type, declaration, true),
                ]))
                .expect_err("target non qualificato");
            assert_eq!(error.category, ErrorCategory::DataMapping);
        }

        let year = append_plan(vec![Field::new("year_value", DataType::Int16, false)]);
        assert_eq!(
            year.preflight(&base_table(vec![server_column(
                "year_value",
                "year",
                "year",
                false,
            )]))
            .expect_err("YEAR reinterpreta Int16")
            .category,
            ErrorCategory::DataMapping
        );

        let binary = append_plan(vec![Field::new("payload", DataType::Binary, true)]);
        for (data_type, declaration) in [("bit", "bit(16)"), ("binary", "binary(16)")] {
            assert_eq!(
                binary
                    .preflight(&base_table(vec![server_column(
                        "payload",
                        data_type,
                        declaration,
                        true,
                    )]))
                    .expect_err("target binary non qualificato")
                    .category,
                ErrorCategory::DataMapping
            );
        }
    }

    #[test]
    fn server_preflight_requires_microsecond_temporal_precision() {
        let plan = append_plan(vec![Field::new(
            "moment",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]);
        for precision in [None, Some(0), Some(3)] {
            let mut column = server_column("moment", "datetime", "datetime", true);
            column.datetime_precision = precision;
            assert_eq!(
                plan.preflight(&base_table(vec![column]))
                    .expect_err("precisione temporale lossy")
                    .category,
                ErrorCategory::DataMapping
            );
        }
        let mut exact = server_column("moment", "datetime", "datetime(6)", true);
        exact.datetime_precision = Some(6);
        assert!(plan.preflight(&base_table(vec![exact])).is_ok());
    }

    /// Un COMMIT interrotto non e un rollback: l'esito resta ignoto e non
    /// autorizza retry automatico.
    #[test]
    fn commit_interruption_produces_an_unknown_outcome_without_automatic_retry() {
        let interrupted = crate::error::timeout_error(ErrorPhase::Commit, RemoteEffect::None);
        let outcome = commit_failure(interrupted, "mysql-test-1".to_owned(), 7)
            .expect("esito ignoto pubblicabile");
        outcome.validate().expect("outcome valido");
        assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
        assert_eq!(outcome.provider, ProviderKind::Mysql);
        assert_eq!(outcome.rows.received, 7);
        assert_eq!(outcome.rows.confirmed, 0);
        let recovery = outcome.recovery.expect("recovery obbligatoria");
        assert!(!recovery.automatic_retry_allowed);
        assert_eq!(
            recovery.last_certain_phase,
            CertainPhase::CommitOrEditRequested
        );
    }

    /// Il deadlock e l'unico esito che il server dichiara annullato: resta
    /// `RolledBack` anche quando emerge al commit o senza rollback confermato.
    #[test]
    fn a_declared_deadlock_stays_rolled_back_instead_of_unknown() {
        let deadlock = crate::error::driver_error(
            &server_error(1_213, "Deadlock found when trying to get lock"),
            ErrorPhase::Write,
            RemoteEffect::None,
        );
        assert_eq!(deadlock.remote_effect, RemoteEffect::RolledBack);

        let error = commit_failure(deadlock.clone(), "mysql-test-2".to_owned(), 3)
            .expect_err("deadlock dichiarato dal server");
        assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
        assert_eq!(error.execution_id.as_deref(), Some("mysql-test-2"));

        let shaped = rolled_back_error(deadlock, false, "mysql-test-2");
        assert_eq!(shaped.remote_effect, RemoteEffect::RolledBack);
        assert_eq!(shaped.execution_id.as_deref(), Some("mysql-test-2"));
    }

    /// Un errore pre-commit puo dichiarare `RolledBack` solo dopo un ROLLBACK
    /// confermato: altrimenti l'effetto remoto resta ignoto.
    #[test]
    fn pre_commit_errors_claim_rollback_only_when_it_is_confirmed() {
        let failure = crate::error::driver_error(
            &server_error(1_062, "Duplicate entry"),
            ErrorPhase::Write,
            RemoteEffect::None,
        );
        let confirmed = rolled_back_error(failure.clone(), true, "mysql-test-3");
        assert_eq!(confirmed.category, ErrorCategory::Conflict);
        assert_eq!(confirmed.remote_effect, RemoteEffect::RolledBack);
        assert_eq!(confirmed.retry, RetryDisposition::Never);

        let ambiguous = rolled_back_error(failure, false, "mysql-test-3");
        assert_eq!(ambiguous.remote_effect, RemoteEffect::Unknown);
        assert_eq!(ambiguous.retry, RetryDisposition::RequiresRecovery);
    }

    #[test]
    fn an_already_quarantined_error_stays_non_retryable_when_rollback_is_unobservable() {
        let quarantined = DatabaseError {
            category: ErrorCategory::Protocol,
            phase: ErrorPhase::Write,
            remote_effect: RemoteEffect::Unknown,
            retry: RetryDisposition::Quarantine,
            provider: Some(ProviderKind::Mysql),
            execution_id: None,
            message: "conteggio righe MySQL incoerente".to_owned(),
            diagnostics: None,
        };

        let shaped = rolled_back_error(quarantined, false, "mysql-test-quarantine");
        assert_eq!(shaped.remote_effect, RemoteEffect::Unknown);
        assert_eq!(shaped.retry, RetryDisposition::Quarantine);
        assert!(!shaped.is_retryable());
        assert_eq!(
            shaped.execution_id.as_deref(),
            Some("mysql-test-quarantine")
        );
    }

    /// Il conteggio pubblicato deve superare la validazione del contratto e
    /// non puo confermare piu righe di quante ne siano state ricevute.
    #[test]
    fn committed_outcome_row_counts_are_contract_valid() {
        let outcome =
            committed_outcome("mysql-test-4".to_owned(), 5, 5).expect("outcome committed");
        outcome.validate().expect("outcome valido");
        assert_eq!(outcome.status, WriteStatus::Committed);
        assert_eq!(outcome.provider, ProviderKind::Mysql);
        assert_eq!(outcome.rows.inserted, Some(5));
        assert_eq!(outcome.rows.updated, Some(0));
        assert_eq!(outcome.rows.deleted, Some(0));
        assert_eq!(outcome.rows.failed, 0);
        assert_eq!(outcome.rows.skipped, 0);
        assert!(outcome.recovery.is_none());

        assert_eq!(
            committed_outcome("mysql-test-5".to_owned(), 2, 3)
                .expect_err("conferme oltre le righe ricevute")
                .category,
            ErrorCategory::Internal
        );
    }

    fn spatial_column(native_type: &str, srid: u32) -> MysqlColumn {
        let mut column = server_column("geom", native_type, native_type, true);
        column.spatial_srid = Some(srid);
        column
    }

    fn spatial_field(native_type: &str, srid: u32) -> Field {
        MysqlColumnSpec::from_catalog(&spatial_column(native_type, srid))
            .expect("colonna spatial qualificata")
            .arrow_field()
    }

    fn spatial_operation() -> WriteOperation {
        let mut operation = append_operation();
        operation.srid_policy = Some(plenora_database_core::plan::SridPolicy::RequireMatch);
        operation
    }

    fn point_wkb(type_word: u32, srid: Option<u32>, ordinates: &[f64]) -> Vec<u8> {
        let mut bytes = vec![1_u8];
        bytes.extend_from_slice(&type_word.to_le_bytes());
        if let Some(srid) = srid {
            bytes.extend_from_slice(&srid.to_le_bytes());
        }
        for ordinate in ordinates {
            bytes.extend_from_slice(&ordinate.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn compile_and_preflight_qualify_only_xy_wkb_with_matching_srid() {
        let input = schema(vec![spatial_field("geometry", 4_326)]);
        let plan = MysqlWritePlan::compile(&input, &spatial_operation(), "warehouse")
            .expect("piano spatial XY");
        assert_eq!(plan.columns[0].kind, MysqlColumnKind::Geometry);
        assert_eq!(plan.columns[0].spatial_srid, Some(4_326));
        assert_eq!(
            plan.render_insert(1).expect("insert geometry"),
            "INSERT INTO `warehouse`.`events` (`geom`) VALUES (ST_GeomFromWKB(?, 4326));"
        );
        assert!(plan
            .preflight(&base_table(vec![spatial_column("geometry", 4_326)]))
            .is_ok());
        assert_eq!(
            plan.preflight(&base_table(vec![spatial_column("geometry", 3_857)]))
                .expect_err("SRID target diverso")
                .category,
            ErrorCategory::Crs
        );
    }

    #[test]
    fn compile_rejects_dimensions_the_mysql_server_cannot_represent() {
        for dimensions in ["xyz", "xym", "xyzm"] {
            let mut metadata = spatial_field("geometry", 4_326).metadata().clone();
            metadata.insert(
                protocol::GEOMETRY_DIMENSIONS.to_owned(),
                dimensions.to_owned(),
            );
            let field = Field::new("geom", DataType::Binary, true).with_metadata(metadata);
            let error =
                MysqlWritePlan::compile(&schema(vec![field]), &spatial_operation(), "warehouse")
                    .expect_err("dimensione non rappresentabile da MySQL");
            assert_eq!(error.category, ErrorCategory::Unsupported);
        }
    }

    #[test]
    fn spatial_batch_rejects_ewkb_srid_and_z_before_binding() {
        let input = schema(vec![spatial_field("geometry", 4_326)]);
        let plan = MysqlWritePlan::compile(&input, &spatial_operation(), "warehouse")
            .expect("piano spatial XY");
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget spatial");
        for payload in [
            point_wkb(0x2000_0001, Some(4_326), &[1.0, 2.0]),
            point_wkb(1_001, None, &[1.0, 2.0, 3.0]),
            point_wkb(2_001, None, &[1.0, 2.0, 3.0]),
            point_wkb(3_001, None, &[1.0, 2.0, 3.0, 4.0]),
        ] {
            let batch = RecordBatch::try_new(
                Arc::clone(&input),
                vec![Arc::new(BinaryArray::from(vec![payload.as_slice()])) as ArrayRef],
            )
            .expect("batch spatial non qualificato");
            assert_eq!(
                plan.validate_spatial_batch(&batch, &budget)
                    .expect_err("payload spatial non qualificato")
                    .category,
                ErrorCategory::DataMapping
            );
        }

        let xy = point_wkb(1, None, &[1.0, 2.0]);
        let batch = RecordBatch::try_new(
            input,
            vec![Arc::new(BinaryArray::from(vec![xy.as_slice()])) as ArrayRef],
        )
        .expect("batch spatial XY");
        assert_eq!(
            plan.validate_spatial_batch(&batch, &budget)
                .expect("WKB XY")
                .components,
            2
        );
        let Params::Positional(values) = plan
            .bind_chunk(&batch, 0, 1)
            .expect("bind WKB XY posizionale")
        else {
            panic!("bind MySQL non posizionale");
        };
        assert_eq!(values, vec![Value::Bytes(xy)]);
    }

    #[test]
    fn spatial_batch_enforces_exact_type_and_cumulative_component_budget() {
        let input = schema(vec![spatial_field("linestring", 4_326)]);
        let plan = MysqlWritePlan::compile(&input, &spatial_operation(), "warehouse")
            .expect("piano spatial exact");
        let point = point_wkb(1, None, &[1.0, 2.0]);
        let wrong_type = RecordBatch::try_new(
            Arc::clone(&input),
            vec![Arc::new(BinaryArray::from(vec![point.as_slice()])) as ArrayRef],
        )
        .expect("batch con tipo geometry errato");
        assert_eq!(
            plan.validate_spatial_batch(
                &wrong_type,
                &ResourceBudget::new(ResourceLimits::default()).expect("budget exact"),
            )
            .expect_err("tipo geometry diverso dal contratto")
            .category,
            ErrorCategory::DataMapping
        );

        let input = schema(vec![spatial_field("point", 4_326)]);
        let plan = MysqlWritePlan::compile(&input, &spatial_operation(), "warehouse")
            .expect("piano point");
        let limits = ResourceLimits {
            geometry_components: 3,
            ..ResourceLimits::default()
        };
        let budget = ResourceBudget::new(limits).expect("budget componenti");
        let two_points = RecordBatch::try_new(
            Arc::clone(&input),
            vec![Arc::new(BinaryArray::from(vec![point.as_slice(), point.as_slice()])) as ArrayRef],
        )
        .expect("batch due point");
        assert_eq!(
            plan.validate_spatial_batch(&two_points, &budget)
                .expect_err("quattro componenti oltre il budget tre")
                .category,
            ErrorCategory::ResourceLimit
        );
        assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 3);

        let one_point = RecordBatch::try_new(
            input,
            vec![Arc::new(BinaryArray::from(vec![point.as_slice()])) as ArrayRef],
        )
        .expect("batch un point");
        plan.validate_spatial_batch(&one_point, &budget)
            .expect("due componenti consumati");
        assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 1);
        assert_eq!(
            plan.validate_spatial_batch(&one_point, &budget)
                .expect_err("budget cumulativo esaurito")
                .category,
            ErrorCategory::ResourceLimit
        );
        assert_eq!(budget.remaining(ResourceKind::GeometryComponents), 1);
    }
}
