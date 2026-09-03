use crate::catalog::{describe_object, OracleObjectDescription};
use crate::config::OracleConfig;
use crate::connection::with_timeout;
use crate::transaction::OracleTransaction;
use crate::OraclePool;
use plenora_database_core::arrow::array::Array;
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::field_contract::{validate_schema_contract, FieldContract};
use plenora_database_core::loss::{LossReport, MappingPolicy};
use plenora_database_core::outcome::{RowCounts, WriteOutcome, WriteStatus};
use plenora_database_core::plan::{
    ProviderKind, SridPolicy, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::primary_key::validate_create_primary_key;
use plenora_database_core::provider::{BatchStream, ParameterValue, PreparedWrite};
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::transaction::{CommitOutcome, Statement, TransactionScope};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use plenora_database_engine::{
    arrow_binary_value, arrow_parameter_value, validate_prepared_budget, ContractLeases,
    WriteResourceReservation,
};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct OracleWritePlan {
    mode: WriteMode,
    target: String,
    schema_name: String,
    object_name: String,
    columns: Vec<WriteColumn>,
    keys: Vec<usize>,
    updates: Vec<usize>,
    create_sql: Option<String>,
    create_spatial_index: bool,
}

#[derive(Debug, Clone)]
struct WriteColumn {
    name: String,
    quoted: String,
    data_type: DataType,
    nullable: bool,
    spatial: Option<SpatialWriteColumn>,
}

#[derive(Debug, Clone)]
struct SpatialWriteColumn {
    srid: u32,
    dimensions: &'static str,
    semantics: plenora_database_core::geometry::SpatialSemantics,
    exact_geometry_type: Option<String>,
}

impl OracleWritePlan {
    fn compile(
        config: &OracleConfig,
        schema: &SchemaRef,
        operation: &WriteOperation,
    ) -> Result<Self> {
        validate_operation(config, schema, operation)?;
        let renderer = renderer();
        let schema_name = operation
            .target
            .schema
            .clone()
            .unwrap_or_else(|| config.username().to_ascii_uppercase());
        let object_name = operation.target.object.clone();
        let target = renderer.quote_object(&ObjectName {
            catalog: None,
            schema: Some(Identifier::new(schema_name.clone())?),
            object: Identifier::new(object_name.clone())?,
        })?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| compile_column(field, &renderer))
            .collect::<Result<Vec<_>>>()?;
        let positions = columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let keys = operation
            .keys
            .iter()
            .map(|name| {
                positions.get(name.as_str()).copied().ok_or_else(|| {
                    prepare_error(
                        ErrorCategory::InvalidPlan,
                        "chiave Oracle assente dallo schema Arrow",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let updates = if operation.update_columns.is_empty() && operation.mode == WriteMode::Upsert
        {
            (0..columns.len())
                .filter(|index| !keys.contains(index))
                .collect()
        } else {
            operation
                .update_columns
                .iter()
                .map(|name| {
                    positions.get(name.as_str()).copied().ok_or_else(|| {
                        prepare_error(
                            ErrorCategory::InvalidPlan,
                            "colonna update Oracle assente dallo schema Arrow",
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };
        if updates.iter().any(|index| keys.contains(index)) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "una chiave Oracle non puo essere anche colonna di aggiornamento",
            ));
        }
        let create_sql = (operation.mode == WriteMode::Create)
            .then(|| create_table_sql(&target, &columns, &keys))
            .transpose()?;
        Ok(Self {
            mode: operation.mode,
            target,
            schema_name,
            object_name,
            columns,
            keys,
            updates,
            create_sql,
            create_spatial_index: operation.create_spatial_index,
        })
    }

    fn preflight(&self, target: &OracleObjectDescription) -> Result<LossReport> {
        if target.kind != "TABLE" {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "scrittura Oracle richiede una tabella base",
            ));
        }
        for column in &self.columns {
            let server = target
                .columns
                .iter()
                .find(|candidate| candidate.name == column.name)
                .ok_or_else(|| {
                    prepare_error(ErrorCategory::DataMapping, "colonna target Oracle mancante")
                })?;
            if server.identity || server.virtual_column {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "scrittura esplicita su colonna Oracle generata o identity",
                ));
            }
            if column.nullable && !server.nullable {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "nullability Arrow incompatibile con il target Oracle",
                ));
            }
            if !native_type_matches(column, server) {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "tipo Arrow incompatibile con il target Oracle",
                ));
            }
            if let Some(spatial) = &column.spatial {
                if server.spatial_srid != Some(spatial.srid)
                    || server.spatial_dimensions
                        != Some(if spatial.dimensions == "xyz" { 3 } else { 2 })
                    || !matches!(
                        (server.spatial_semantics, spatial.semantics),
                        (Some(server), requested)
                            if server == requested
                                || (server
                                    == plenora_database_core::geometry::SpatialSemantics::Geography
                                    && requested
                                        == plenora_database_core::geometry::SpatialSemantics::Geometry)
                    )
                {
                    return Err(prepare_error(
                        ErrorCategory::Crs,
                        "contratto spatial Arrow diverso dai metadati Oracle",
                    ));
                }
            }
        }
        if matches!(
            self.mode,
            WriteMode::Append | WriteMode::Replace | WriteMode::Upsert
        ) {
            for server in &target.columns {
                if self.columns.iter().any(|column| column.name == server.name) {
                    continue;
                }
                if !server.nullable
                    && server.default_expression.is_none()
                    && !server.identity
                    && !server.virtual_column
                {
                    return Err(prepare_error(
                        ErrorCategory::DataMapping,
                        "colonna target Oracle obbligatoria assente dall'input",
                    ));
                }
            }
        }
        if matches!(
            self.mode,
            WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
        ) && !target_has_unique_key(target, &self.keys, &self.columns)
        {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "le chiavi Oracle devono corrispondere a un indice univoco",
            ));
        }
        Ok(LossReport {
            schema_version: 2,
            policy: MappingPolicy::Strict,
            losses: Vec::new(),
        })
    }

    fn insert_statement(&self, row: &[ParameterValue]) -> Statement {
        let columns = self
            .columns
            .iter()
            .map(|column| column.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = self
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| column.placeholder(index + 1))
            .collect::<Vec<_>>()
            .join(", ");
        Statement::new(format!(
            "INSERT INTO {} ({columns}) VALUES ({placeholders})",
            self.target
        ))
        .with_params(row.to_vec())
    }

    fn update_statement(&self, row: &[ParameterValue]) -> Statement {
        let assignments = self
            .updates
            .iter()
            .enumerate()
            .map(|(position, index)| {
                format!(
                    "{} = {}",
                    self.columns[*index].quoted,
                    self.columns[*index].placeholder(position + 1)
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = self
            .keys
            .iter()
            .enumerate()
            .map(|(position, index)| {
                format!(
                    "{} = :{}",
                    self.columns[*index].quoted,
                    self.updates.len() + position + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let params = self
            .updates
            .iter()
            .chain(&self.keys)
            .map(|index| row[*index].clone())
            .collect();
        Statement::new(format!(
            "UPDATE {} SET {assignments} WHERE {predicate}",
            self.target
        ))
        .with_params(params)
    }

    fn delete_statement(&self, row: &[ParameterValue]) -> Statement {
        let predicate = self
            .keys
            .iter()
            .enumerate()
            .map(|(position, index)| format!("{} = :{}", self.columns[*index].quoted, position + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        Statement::new(format!("DELETE FROM {} WHERE {predicate}", self.target))
            .with_params(self.keys.iter().map(|index| row[*index].clone()).collect())
    }

    fn upsert_statement(&self, row: &[ParameterValue]) -> Statement {
        let source = self
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| {
                format!("{} AS {}", column.placeholder(position + 1), column.quoted)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = self
            .keys
            .iter()
            .map(|index| {
                let column = &self.columns[*index].quoted;
                format!("T.{column} = S.{column}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let updates = if self.updates.is_empty() {
            let key = &self.columns[self.keys[0]].quoted;
            format!("T.{key} = S.{key}")
        } else {
            self.updates
                .iter()
                .map(|index| {
                    let column = &self.columns[*index].quoted;
                    format!("T.{column} = S.{column}")
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        let names = self
            .columns
            .iter()
            .map(|column| column.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let values = self
            .columns
            .iter()
            .map(|column| format!("S.{}", column.quoted))
            .collect::<Vec<_>>()
            .join(", ");
        Statement::new(format!(
            "MERGE INTO {} T USING (SELECT {source} FROM DUAL) S ON ({predicate}) \
             WHEN MATCHED THEN UPDATE SET {updates} \
             WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})",
            self.target
        ))
        .with_params(row.to_vec())
    }

    const fn mutation_kind(&self) -> &'static str {
        match self.mode {
            WriteMode::Update => "updated",
            WriteMode::DeleteByKeys => "deleted",
            WriteMode::Append | WriteMode::Create | WriteMode::Replace => "inserted",
            WriteMode::Upsert | WriteMode::TruncateInsert => "mixed",
        }
    }
}

impl WriteColumn {
    fn placeholder(&self, position: usize) -> String {
        self.spatial.as_ref().map_or_else(
            || {
                if self.data_type == DataType::Boolean {
                    format!("TO_BOOLEAN(:{position})")
                } else if matches!(self.data_type, DataType::Timestamp(_, Some(_))) {
                    format!(
                        "TO_TIMESTAMP_TZ(:{position}, '{}')",
                        plenora_database_core::provider::ORACLE_TIMESTAMP_TZ_FORMAT_MODEL
                    )
                } else {
                    format!(":{position}")
                }
            },
            |spatial| {
                format!(
                    "MDSYS.SDO_UTIL.FROM_WKBGEOMETRY(:{position}, {})",
                    spatial.srid
                )
            },
        )
    }

    fn native_type(&self) -> Result<String> {
        if self.spatial.is_some() {
            Ok("MDSYS.SDO_GEOMETRY".to_owned())
        } else {
            native_type(&self.data_type)
        }
    }
}

pub async fn prepare_write(
    config: &OracleConfig,
    pool: &Arc<OraclePool>,
    operation: &WriteOperation,
    input_schema: SchemaRef,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<PreparedWrite> {
    budget.ensure_active()?;
    if cancellation.is_cancelled() {
        return Err(crate::error::interruption_error(
            cancellation,
            ErrorPhase::Prepare,
        ));
    }
    let plan = OracleWritePlan::compile(config, &input_schema, operation)?;
    let (operation_lease, columns_lease) =
        ContractLeases::acquire(budget, input_schema.fields().len())?.into_parts();
    let loss_report = if operation.mode == WriteMode::Create {
        LossReport {
            schema_version: 2,
            policy: MappingPolicy::Strict,
            losses: Vec::new(),
        }
    } else {
        let connection = pool.checkout(cancellation).await?;
        let target = describe_object(
            config,
            connection.connection()?,
            &plan.schema_name,
            &plan.object_name,
            cancellation,
        )
        .await?;
        drop(connection);
        plan.preflight(&target)?
    };
    loss_report.validate()?;
    Ok(PreparedWrite::new(
        operation.clone(),
        input_schema,
        loss_report,
        budget.clone(),
        operation_lease,
        columns_lease,
    )
    .with_driver_state(plan))
}

#[allow(clippy::significant_drop_tightening)]
pub async fn execute_write(
    config: &OracleConfig,
    pool: &Arc<OraclePool>,
    mut prepared: PreparedWrite,
    mut input: Box<dyn BatchStream>,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<WriteOutcome> {
    validate_prepared_budget(&prepared.budget, budget)?;
    if input.schema().as_ref() != prepared.input_schema.as_ref() {
        return Err(write_error(
            ErrorCategory::InvalidPlan,
            "schema dello stream diverso dallo schema preparato",
        ));
    }
    let declared_rows = input.declared_input_rows().ok_or_else(|| {
        write_error(
            ErrorCategory::InvalidPlan,
            "stream Oracle senza conteggio righe dichiarato",
        )
    })?;
    if input.row_diagnostics_policy()
        != plenora_database_core::row_diagnostics::RowDiagnosticsPolicy::default()
    {
        return Err(write_error(
            ErrorCategory::Unsupported,
            "diagnostica row-scoped Oracle non ancora qualificata",
        ));
    }
    let plan = prepared
        .take_driver_state::<OracleWritePlan>()
        .ok_or_else(|| write_error(ErrorCategory::InvalidPlan, "piano preparato Oracle assente"))?;
    let created = plan.create_sql.is_some();
    if created {
        setup_created_target(config, pool, &plan, cancellation)
            .await
            .map_err(shape_create_setup_error)?;
    }
    let mut transaction = OracleTransaction::begin(
        config,
        pool,
        &plenora_database_core::transaction::TransactionOptions::default(),
        cancellation,
    )
    .await?;
    let execution = execute_input(
        &mut transaction,
        &plan,
        &prepared.input_schema,
        input.as_mut(),
        budget,
        cancellation,
        declared_rows,
    )
    .await;
    let (received, affected) = match execution {
        Ok(counts) => counts,
        Err(error) => return Err(rollback_and_shape(Box::new(transaction), error, created).await),
    };
    let execution_id = format!(
        "oracle-write-{}",
        EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    match Box::new(transaction).commit(cancellation).await? {
        CommitOutcome::Committed => committed_outcome(execution_id, &plan, received, affected),
        CommitOutcome::OutcomeUnknown { recovery } => {
            let outcome = WriteOutcome {
                schema_version: 2,
                status: WriteStatus::OutcomeUnknown,
                execution_id,
                provider: ProviderKind::Oracle,
                rows: RowCounts {
                    received,
                    confirmed: 0,
                    inserted: None,
                    updated: None,
                    deleted: None,
                    failed: 0,
                    skipped: received,
                },
                recovery: Some(recovery),
            };
            outcome.validate()?;
            Ok(outcome)
        }
    }
}

async fn setup_created_target(
    config: &OracleConfig,
    pool: &Arc<OraclePool>,
    plan: &OracleWritePlan,
    cancellation: &CancellationToken,
) -> Result<()> {
    let connection = pool.checkout(cancellation).await?;
    let raw = connection.connection()?;
    let create_sql = plan
        .create_sql
        .as_ref()
        .expect("setup richiesto soltanto per create");
    with_timeout(
        config,
        ErrorPhase::Write,
        cancellation,
        raw.execute(create_sql, &[]),
    )
    .await?;
    for column in plan
        .columns
        .iter()
        .filter(|column| column.spatial.is_some())
    {
        let spatial = column.spatial.as_ref().expect("filtrato");
        let geographic = plenora_database_core::spatial_policy::is_geographic_srid(spatial.srid);
        let xy = if geographic {
            "MDSYS.SDO_DIM_ELEMENT('LONGITUDE', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('LATITUDE', -90, 90, 0.005)"
        } else {
            "MDSYS.SDO_DIM_ELEMENT('X', -1000000000000000, 1000000000000000, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -1000000000000000, 1000000000000000, 0.005)"
        };
        let axes = if spatial.dimensions == "xyz" {
            format!("MDSYS.SDO_DIM_ARRAY({xy}, MDSYS.SDO_DIM_ELEMENT('Z', -1000000000000000, 1000000000000000, 0.005))")
        } else {
            format!("MDSYS.SDO_DIM_ARRAY({xy})")
        };
        let sql = format!("INSERT INTO USER_SDO_GEOM_METADATA (TABLE_NAME, COLUMN_NAME, DIMINFO, SRID) VALUES (:1, :2, {axes}, :3)");
        let params = crate::parameter::bind_parameters(&[
            ParameterValue::String(plan.object_name.clone()),
            ParameterValue::String(column.name.clone()),
            ParameterValue::I64(i64::from(spatial.srid)),
        ])?;
        with_timeout(
            config,
            ErrorPhase::Write,
            cancellation,
            raw.execute(&sql, &params),
        )
        .await?;
        if plan.create_spatial_index {
            let index_name = spatial_index_name(&plan.object_name, &column.name);
            let index = renderer().quote_identifier(&Identifier::new(index_name)?)?;
            let sql = format!(
                "CREATE INDEX {index} ON {} ({}) INDEXTYPE IS MDSYS.SPATIAL_INDEX_V2",
                plan.target, column.quoted
            );
            with_timeout(
                config,
                ErrorPhase::Write,
                cancellation,
                raw.execute(&sql, &[]),
            )
            .await?;
        }
    }
    raw.commit()
        .await
        .map_err(|error| crate::error::driver_error(ErrorPhase::Commit, &error))?;
    drop(connection);
    Ok(())
}

async fn execute_input(
    transaction: &mut OracleTransaction,
    plan: &OracleWritePlan,
    schema: &SchemaRef,
    input: &mut dyn BatchStream,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    declared_rows: u64,
) -> Result<(u64, u64)> {
    if plan.mode == WriteMode::Replace {
        transaction
            .execute(
                &Statement::new(format!("DELETE FROM {}", plan.target)),
                cancellation,
            )
            .await?;
    }
    let mut received = 0_u64;
    let mut affected = 0_u64;
    while let Some(batch) = input.next_batch(cancellation).await? {
        validate_batch_schema(&batch, schema)?;
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
        let components = plan.validate_spatial_batch(&batch, budget)?;
        let reservation =
            WriteResourceReservation::acquire(budget, rows, bytes, bytes, components)?;
        for (row_index, row) in batch_values(&batch, plan)?.into_iter().enumerate() {
            let statement = match plan.mode {
                WriteMode::Append | WriteMode::Create | WriteMode::Replace => {
                    plan.insert_statement(&row)
                }
                WriteMode::Update => plan.update_statement(&row),
                WriteMode::Upsert => plan.upsert_statement(&row),
                WriteMode::DeleteByKeys => plan.delete_statement(&row),
                WriteMode::TruncateInsert => {
                    return Err(write_error(
                        ErrorCategory::Unsupported,
                        "truncate_insert Oracle non qualificato",
                    ))
                }
            };
            let changed = if plan.mode != WriteMode::Replace && received == 0 && row_index == 0 {
                transaction
                    .execute_write_dml(&statement, cancellation)
                    .await?
            } else {
                transaction
                    .execute_atomic_dml(&statement, cancellation)
                    .await?
            };
            affected = affected.checked_add(changed).ok_or_else(|| {
                write_error(
                    ErrorCategory::ResourceLimit,
                    "conteggio DML Oracle in overflow",
                )
            })?;
        }
        received = received.checked_add(rows).ok_or_else(|| {
            write_error(
                ErrorCategory::ResourceLimit,
                "conteggio input Oracle in overflow",
            )
        })?;
        reservation.commit()?;
    }
    if received != declared_rows {
        return Err(write_error(
            ErrorCategory::InvalidPlan,
            "righe prodotte dallo stream Oracle diverse dal conteggio dichiarato",
        ));
    }
    Ok((received, affected))
}

impl OracleWritePlan {
    fn validate_spatial_batch(&self, batch: &RecordBatch, budget: &ResourceBudget) -> Result<u64> {
        let mut components = 0_u64;
        for (column_index, column) in self.columns.iter().enumerate() {
            let Some(contract) = &column.spatial else {
                continue;
            };
            let array = batch.column(column_index);
            for row in 0..batch.num_rows() {
                if array.is_null(row) {
                    continue;
                }
                let bytes = arrow_binary_value(array.as_ref(), &column.data_type, row)
                    .map_err(oracle_write_error)?;
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > budget.limits().cell_bytes {
                    return Err(DatabaseError::resource_limit(
                        "geometry Oracle oltre il limite cella",
                    ));
                }
                let remaining = budget
                    .remaining(ResourceKind::GeometryComponents)
                    .saturating_sub(components);
                let inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(
                    bytes,
                    remaining,
                    budget.limits().nesting_depth,
                )
                .map_err(|mut error| {
                    error.phase = ErrorPhase::Write;
                    error.provider = Some(ProviderKind::Oracle);
                    error
                })?;
                if inspection.has_any_embedded_srid || inspection.has_any_m {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "write Oracle accetta WKB puro senza SRID embedded o dimensione M",
                    ));
                }
                if inspection.root.dimensions_label() != contract.dimensions {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "dimensioni WKB diverse dal contratto Arrow",
                    ));
                }
                let geometry_type = inspection.root.geometry_type_name().ok_or_else(|| {
                    write_error(
                        ErrorCategory::Unsupported,
                        "tipo geometry WKB non qualificato per Oracle",
                    )
                })?;
                if !geometry_type_is_writable(geometry_type)
                    || contract
                        .exact_geometry_type
                        .as_deref()
                        .is_some_and(|expected| !expected.eq_ignore_ascii_case(geometry_type))
                {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "tipo WKB diverso dal contratto Arrow",
                    ));
                }
                components = components
                    .checked_add(inspection.stats.components)
                    .ok_or_else(|| {
                        DatabaseError::resource_limit("componenti geometry Oracle in overflow")
                    })?;
            }
        }
        Ok(components)
    }
}

fn validate_batch_schema(batch: &RecordBatch, declared: &SchemaRef) -> Result<()> {
    if batch.schema().as_ref() == declared.as_ref() {
        Ok(())
    } else {
        Err(write_error(
            ErrorCategory::InvalidPlan,
            "schema del batch diverso dallo schema dichiarato",
        ))
    }
}

fn batch_values(batch: &RecordBatch, plan: &OracleWritePlan) -> Result<Vec<Vec<ParameterValue>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .zip(batch.schema().fields().iter().zip(&plan.columns))
                .map(|(array, (field, _column))| {
                    arrow_parameter_value(array.as_ref(), field, row, 'T')
                        .map_err(oracle_write_error)
                })
                .collect()
        })
        .collect()
}

const fn oracle_write_error(mut error: DatabaseError) -> DatabaseError {
    error.phase = ErrorPhase::Write;
    error.provider = Some(ProviderKind::Oracle);
    error
}

#[allow(clippy::too_many_lines)]
fn validate_operation(
    config: &OracleConfig,
    schema: &SchemaRef,
    operation: &WriteOperation,
) -> Result<()> {
    if operation
        .target
        .catalog
        .as_deref()
        .is_some_and(|catalog| !catalog.eq_ignore_ascii_case(config.service_name()))
    {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "write cross-database Oracle non supportata",
        ));
    }
    if operation.mapping_policy != MappingPolicy::Strict {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "write Oracle qualifica soltanto mapping strict",
        ));
    }
    let expected_profile = if operation.mode == WriteMode::Create {
        TransactionProfile::BestEffortDdl
    } else {
        TransactionProfile::SingleTransaction
    };
    if operation.transaction_profile != expected_profile {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "profilo transazionale Oracle incompatibile con la modalita write",
        ));
    }
    if operation.mode == WriteMode::TruncateInsert || operation.allow_partial {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "truncate_insert o write parziale Oracle non qualificati",
        ));
    }
    if operation.create_spatial_index && operation.mode != WriteMode::Create {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "indice spatial Oracle creabile soltanto con mode=create",
        ));
    }
    let has_spatial = schema.fields().iter().try_fold(false, |found, field| {
        FieldContract::parse(field).map(|contract| found || contract.spatial)
    })?;
    if operation.create_spatial_index && !has_spatial {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "create_spatial_index Oracle senza colonne spatial",
        ));
    }
    if operation.mode == WriteMode::Create && has_spatial {
        let canonical = |name: &str| name == name.to_ascii_uppercase();
        let identifiers_are_canonical = canonical(&operation.target.object)
            && operation.target.schema.as_deref().is_none_or(canonical)
            && schema.fields().iter().try_fold(true, |valid, field| {
                FieldContract::parse(field)
                    .map(|contract| valid && (!contract.spatial || canonical(field.name())))
            })?;
        if !identifiers_are_canonical {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "create spatial Oracle richiede identificatori canonici maiuscoli per i metadata SDO",
            ));
        }
    }
    if schema.fields().is_empty() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "schema Arrow vuoto per write Oracle",
        ));
    }
    validate_write_spatial_policy(schema, operation.srid_policy)?;
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<BTreeSet<_>>();
    if names.len() != schema.fields().len() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "schema Arrow Oracle con colonne duplicate",
        ));
    }
    let keys = operation.keys.iter().collect::<BTreeSet<_>>();
    if keys.len() != operation.keys.len() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "write Oracle con chiavi duplicate",
        ));
    }
    match operation.mode {
        WriteMode::Create => {
            validate_create_primary_key(schema, &operation.keys).map_err(|violation| {
                prepare_error(ErrorCategory::InvalidPlan, violation.message("Oracle"))
            })?;
        }
        WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
            if operation.keys.is_empty() =>
        {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "modalita Oracle key-based senza chiavi",
            ))
        }
        WriteMode::Append | WriteMode::Replace if !operation.keys.is_empty() => {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "append/replace Oracle non usano chiavi",
            ))
        }
        _ => {}
    }
    if operation.mode == WriteMode::DeleteByKeys
        && (schema.fields().len() != operation.keys.len() || !operation.update_columns.is_empty())
    {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "delete_by_keys Oracle richiede uno schema composto dalle sole chiavi",
        ));
    }
    Ok(())
}

fn validate_write_spatial_policy(schema: &SchemaRef, policy: Option<SridPolicy>) -> Result<()> {
    validate_schema_contract(schema.as_ref())?;
    let has_spatial = schema.fields().iter().try_fold(false, |found, field| {
        FieldContract::parse(field).map(|contract| found || contract.spatial)
    })?;
    match (has_spatial, policy) {
        (true, Some(SridPolicy::RequireMatch)) | (false, None) => Ok(()),
        (true, _) => Err(prepare_error(
            ErrorCategory::Unsupported,
            "write spatial Oracle richiede SridPolicy::RequireMatch",
        )),
        (false, Some(_)) => Err(prepare_error(
            ErrorCategory::Unsupported,
            "srid_policy Oracle senza colonne spatial",
        )),
    }
}

fn compile_column(field: &Field, renderer: &Renderer) -> Result<WriteColumn> {
    let contract = FieldContract::parse(field)?;
    let spatial = if contract.spatial {
        if contract.encoding != Some("wkb") {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "write spatial Oracle richiede WKB puro",
            ));
        }
        let dimensions = match contract.dimensions {
            Some("xy") => "xy",
            Some("xyz") => "xyz",
            _ => {
                return Err(prepare_error(
                    ErrorCategory::Unsupported,
                    "write spatial Oracle qualifica soltanto XY o XYZ",
                ))
            }
        };
        let srid = contract.srid.ok_or_else(|| {
            prepare_error(
                ErrorCategory::Crs,
                "write spatial Oracle richiede SRID dichiarato",
            )
        })?;
        if srid == 0 || i32::try_from(srid).is_err() {
            return Err(prepare_error(
                ErrorCategory::Crs,
                "SRID spatial Oracle fuori intervallo",
            ));
        }
        let semantics = if contract.is_geography() {
            if !plenora_database_core::spatial_policy::is_geographic_srid(srid) {
                return Err(prepare_error(
                    ErrorCategory::Crs,
                    "geography Oracle richiede un SRID geografico qualificato",
                ));
            }
            plenora_database_core::geometry::SpatialSemantics::Geography
        } else {
            plenora_database_core::geometry::SpatialSemantics::Geometry
        };
        if !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Err(prepare_error(
                ErrorCategory::DataMapping,
                "write spatial Oracle richiede Arrow Binary",
            ));
        }
        let exact_geometry_type = match contract.types_declaration {
            Some("mixed") => None,
            Some("exact") => {
                let kind = contract.geometry_types.ok_or_else(|| {
                    prepare_error(
                        ErrorCategory::DataMapping,
                        "tipo geometry exact Oracle assente",
                    )
                })?;
                if kind.contains(',') || !geometry_type_is_writable(kind) {
                    return Err(prepare_error(
                        ErrorCategory::Unsupported,
                        "tipo geometry Oracle non qualificato",
                    ));
                }
                Some(kind.to_owned())
            }
            _ => {
                return Err(prepare_error(
                    ErrorCategory::Unsupported,
                    "dichiarazione tipi geometry Oracle non qualificata",
                ))
            }
        };
        Some(SpatialWriteColumn {
            srid,
            dimensions,
            semantics,
            exact_geometry_type,
        })
    } else {
        native_type(field.data_type())?;
        None
    };
    Ok(WriteColumn {
        name: field.name().clone(),
        quoted: renderer.quote_identifier(&Identifier::new(field.name().clone())?)?,
        data_type: field.data_type().clone(),
        nullable: field.is_nullable(),
        spatial,
    })
}

fn geometry_type_is_writable(value: &str) -> bool {
    [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection",
    ]
    .iter()
    .any(|candidate| candidate.eq_ignore_ascii_case(value))
}

fn create_table_sql(target: &str, columns: &[WriteColumn], keys: &[usize]) -> Result<String> {
    let mut definitions = columns
        .iter()
        .map(|column| {
            Ok(format!(
                "{} {}{}",
                column.quoted,
                column.native_type()?,
                if column.nullable { "" } else { " NOT NULL" }
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if !keys.is_empty() {
        definitions.push(format!(
            "PRIMARY KEY ({})",
            keys.iter()
                .map(|index| columns[*index].quoted.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(format!(
        "CREATE TABLE {target} ({})",
        definitions.join(", ")
    ))
}

fn native_type(data_type: &DataType) -> Result<String> {
    match data_type {
        DataType::Boolean => Ok("BOOLEAN".to_owned()),
        DataType::Int16 => Ok("NUMBER(5)".to_owned()),
        DataType::Int32 => Ok("NUMBER(10)".to_owned()),
        DataType::Int64 => Ok("NUMBER(19)".to_owned()),
        DataType::Float32 => Ok("BINARY_FLOAT".to_owned()),
        DataType::Float64 => Ok("BINARY_DOUBLE".to_owned()),
        DataType::Decimal128(precision, scale)
            if *precision > 0
                && *precision <= 38
                && *scale >= 0
                && *scale <= i8::try_from(*precision).unwrap_or(i8::MAX) =>
        {
            Ok(format!("NUMBER({precision}, {scale})"))
        }
        DataType::Utf8 => Ok("CLOB".to_owned()),
        DataType::Binary | DataType::LargeBinary => Ok("BLOB".to_owned()),
        DataType::Date32 => Ok("DATE".to_owned()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok("TIMESTAMP(6)".to_owned()),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            Ok("TIMESTAMP(6) WITH TIME ZONE".to_owned())
        }
        _ => Err(prepare_error(
            ErrorCategory::Unsupported,
            "tipo Arrow non qualificato per write Oracle",
        )),
    }
}

fn native_type_matches(column: &WriteColumn, server: &crate::OracleColumn) -> bool {
    let native = server.data_type.to_ascii_uppercase();
    if column.spatial.is_some() {
        return native == "SDO_GEOMETRY";
    }
    match &column.data_type {
        DataType::Boolean => native == "BOOLEAN",
        DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            native == "NUMBER" && server.scale.unwrap_or(0) == 0
        }
        DataType::Float32 => native == "BINARY_FLOAT",
        DataType::Float64 => matches!(native.as_str(), "BINARY_DOUBLE" | "FLOAT"),
        DataType::Decimal128(precision, scale) => {
            native == "NUMBER"
                && server.precision == Some(u64::from(*precision))
                && server.scale == Some(i64::from(*scale))
        }
        DataType::Utf8 => matches!(
            native.as_str(),
            "CHAR" | "NCHAR" | "VARCHAR2" | "NVARCHAR2" | "CLOB" | "NCLOB" | "JSON"
        ),
        DataType::Binary | DataType::LargeBinary => {
            matches!(native.as_str(), "RAW" | "LONG RAW" | "BLOB")
        }
        DataType::Date32 => native == "DATE",
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            native.starts_with("TIMESTAMP") && !native.contains("TIME ZONE")
        }
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            native.starts_with("TIMESTAMP") && native.contains("TIME ZONE")
        }
        _ => false,
    }
}

fn target_has_unique_key(
    target: &OracleObjectDescription,
    keys: &[usize],
    columns: &[WriteColumn],
) -> bool {
    let expected = keys
        .iter()
        .map(|index| columns[*index].name.as_str())
        .collect::<BTreeSet<_>>();
    target.indexes.iter().any(|index| {
        index.unique
            && index
                .columns
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                == expected
    })
}

fn spatial_index_name(table: &str, column: &str) -> String {
    let mut base = format!("PLN_{table}_{column}_SIDX").to_ascii_uppercase();
    if base.len() > 118 {
        base.truncate(118);
    }
    base
}

const fn shape_create_setup_error(mut error: DatabaseError) -> DatabaseError {
    error.remote_effect = RemoteEffect::Partial;
    error.retry = RetryDisposition::RequiresRecovery;
    error
}

async fn rollback_and_shape(
    transaction: Box<OracleTransaction>,
    mut original: DatabaseError,
    created: bool,
) -> DatabaseError {
    let cleanup = CancellationToken::new();
    if transaction.rollback(&cleanup).await.is_ok() {
        if created {
            original.remote_effect = RemoteEffect::Partial;
            original.retry = RetryDisposition::RequiresRecovery;
        } else {
            original.remote_effect = RemoteEffect::RolledBack;
            original.retry = RetryDisposition::Never;
        }
    } else {
        original.remote_effect = RemoteEffect::Unknown;
        original.retry = RetryDisposition::RequiresRecovery;
    }
    original
}

fn committed_outcome(
    execution_id: String,
    plan: &OracleWritePlan,
    received: u64,
    affected: u64,
) -> Result<WriteOutcome> {
    let skipped = received.saturating_sub(affected.min(received));
    let (confirmed, inserted, updated, deleted) = match plan.mutation_kind() {
        "inserted" => (received, Some(received), Some(0), Some(0)),
        "updated" => (affected, Some(0), Some(affected), Some(0)),
        "deleted" => (affected, Some(0), Some(0), Some(affected)),
        _ => (received, None, None, None),
    };
    let outcome = WriteOutcome {
        schema_version: 2,
        status: WriteStatus::Committed,
        execution_id,
        provider: ProviderKind::Oracle,
        rows: RowCounts {
            received,
            confirmed,
            inserted,
            updated,
            deleted,
            failed: 0,
            skipped,
        },
        recovery: None,
    };
    outcome.validate()?;
    Ok(outcome)
}

const fn renderer() -> Renderer {
    Renderer::new(
        Dialect::Oracle,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
}

fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(ProviderKind::Oracle),
        message,
    )
}

fn write_error(category: ErrorCategory, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Write,
        Some(ProviderKind::Oracle),
        message,
    )
}

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;
