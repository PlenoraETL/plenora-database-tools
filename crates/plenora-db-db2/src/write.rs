use crate::catalog::{describe_object, Db2ObjectDescription};
use crate::connection::open_connection;
use crate::error::{interruption_error, task_error};
use crate::transaction::Db2Transaction;
use crate::Db2Config;
use chrono::{Duration, NaiveDate};
use plenora_database_core::arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::field_contract::{validate_schema_contract, FieldContract};
use plenora_database_core::loss::{LossReport, MappingPolicy};
use plenora_database_core::outcome::{RowCounts, WriteOutcome, WriteStatus};
use plenora_database_core::plan::{
    ProviderKind, SridPolicy, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::primary_key::validate_create_primary_key;
use plenora_database_core::provider::{BatchStream, ParameterValue, PreparedWrite, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceKind};
use plenora_database_core::transaction::{
    CommitOutcome, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use plenora_database_engine::{validate_prepared_budget, ContractLeases, WriteResourceReservation};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

const ARRAY_BIND_ROWS: usize = 4_096;
static EXECUTION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct Db2WritePlan {
    mode: WriteMode,
    target: String,
    columns: Vec<WriteColumn>,
    keys: Vec<usize>,
    updates: Vec<usize>,
    create_sql: Option<String>,
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
    exact_geometry_type: Option<String>,
}

impl Db2WritePlan {
    pub fn compile(
        config: &Db2Config,
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
        let target = renderer.quote_object(&ObjectName {
            catalog: None,
            schema: Some(Identifier::new(schema_name)?),
            object: Identifier::new(operation.target.object.clone())?,
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
            .map(|key| {
                positions.get(key.as_str()).copied().ok_or_else(|| {
                    prepare_error(
                        ErrorCategory::InvalidPlan,
                        "chiave Db2 assente dallo schema Arrow",
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let updates = if operation.update_columns.is_empty()
            && matches!(operation.mode, WriteMode::Update | WriteMode::Upsert)
        {
            (0..columns.len())
                .filter(|index| !keys.contains(index))
                .collect()
        } else {
            operation
                .update_columns
                .iter()
                .map(|column| {
                    positions.get(column.as_str()).copied().ok_or_else(|| {
                        prepare_error(
                            ErrorCategory::InvalidPlan,
                            "colonna update Db2 assente dallo schema Arrow",
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };
        if updates.iter().any(|index| keys.contains(index)) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "una chiave Db2 non puo essere anche colonna di aggiornamento",
            ));
        }
        let create_sql = (operation.mode == WriteMode::Create)
            .then(|| create_table_sql(&target, &columns, &keys))
            .transpose()?;
        Ok(Self {
            mode: operation.mode,
            target,
            columns,
            keys,
            updates,
            create_sql,
        })
    }

    fn preflight(&self, target: &Db2ObjectDescription) -> Result<LossReport> {
        if target.kind != "TABLE" {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "scrittura Db2 richiede una tabella base",
            ));
        }
        for column in &self.columns {
            let server = target
                .columns
                .iter()
                .find(|candidate| candidate.name == column.name)
                .ok_or_else(|| {
                    prepare_error(ErrorCategory::DataMapping, "colonna target Db2 mancante")
                })?;
            if server.generated || server.identity {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "scrittura esplicita su colonna Db2 generata o identity",
                ));
            }
            if column.nullable && !server.nullable {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "nullability Arrow incompatibile con il target Db2",
                ));
            }
            if !native_type_matches(column, server) {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "tipo Arrow incompatibile con il target Db2",
                ));
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
                    && !server.generated
                    && !server.identity
                {
                    return Err(prepare_error(
                        ErrorCategory::DataMapping,
                        "colonna target Db2 obbligatoria assente dall'input",
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
                "le chiavi Db2 devono corrispondere a un indice univoco",
            ));
        }
        Ok(LossReport {
            schema_version: 2,
            policy: MappingPolicy::Strict,
            losses: Vec::new(),
        })
    }

    pub(crate) fn validate_spatial_batch(
        &self,
        batch: &RecordBatch,
        budget: &ResourceBudget,
    ) -> Result<()> {
        let mut components = 0_u64;
        for (index, column) in self.columns.iter().enumerate() {
            let Some(contract) = &column.spatial else {
                continue;
            };
            let array = batch.column(index);
            for row in 0..batch.num_rows() {
                if array.is_null(row) {
                    continue;
                }
                let bytes = binary_value(array.as_ref(), &column.data_type, row)?;
                if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > budget.limits().cell_bytes {
                    return Err(DatabaseError::resource_limit(
                        "geometry Db2 oltre il limite cella",
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
                    error.provider = Some(ProviderKind::Db2);
                    error
                })?;
                if inspection.has_any_embedded_srid || inspection.has_any_m {
                    return Err(write_error(
                        ErrorCategory::DataMapping,
                        "write Db2 accetta WKB puro senza SRID embedded o dimensione M",
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
                        "tipo geometry WKB non qualificato per Db2",
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
                        DatabaseError::resource_limit("overflow componenti geometry Db2")
                    })?;
            }
        }
        if components > 0 {
            budget
                .try_lease(ResourceKind::GeometryComponents, components)?
                .commit(components)?;
        }
        Ok(())
    }

    pub(crate) fn insert_statement(&self, rows: &[Vec<ParameterValue>]) -> Statement {
        let columns = self
            .columns
            .iter()
            .map(|column| column.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let row_placeholders = self
            .columns
            .iter()
            .map(WriteColumn::placeholder)
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = std::iter::repeat_n(format!("({row_placeholders})"), rows.len())
            .collect::<Vec<_>>()
            .join(", ");
        Statement::new(format!(
            "INSERT INTO {} ({columns}) VALUES {placeholders}",
            self.target
        ))
        .with_params(rows.iter().flatten().cloned().collect())
    }

    pub(crate) fn array_insert_sql(&self) -> String {
        let columns = self
            .columns
            .iter()
            .map(|column| column.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = self
            .columns
            .iter()
            .map(WriteColumn::placeholder)
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "INSERT INTO {} ({columns}) VALUES ({placeholders})",
            self.target
        )
    }

    fn update_statement(&self, row: &[ParameterValue]) -> Statement {
        let assignments = self
            .updates
            .iter()
            .map(|index| {
                format!(
                    "{} = {}",
                    self.columns[*index].quoted,
                    self.columns[*index].placeholder()
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let predicate = self.key_predicate();
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
        let params = self.keys.iter().map(|index| row[*index].clone()).collect();
        Statement::new(format!(
            "DELETE FROM {} WHERE {}",
            self.target,
            self.key_predicate()
        ))
        .with_params(params)
    }

    fn upsert_statement(&self, row: &[ParameterValue]) -> Statement {
        let source_columns = self
            .columns
            .iter()
            .map(|column| column.quoted.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = self
            .columns
            .iter()
            .map(WriteColumn::placeholder)
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
        let insert_values = self
            .columns
            .iter()
            .map(|column| format!("S.{}", column.quoted))
            .collect::<Vec<_>>()
            .join(", ");
        Statement::new(format!(
            "MERGE INTO {} AS T USING (VALUES ({placeholders})) AS S ({source_columns}) \
             ON {predicate} WHEN MATCHED THEN UPDATE SET {updates} \
             WHEN NOT MATCHED THEN INSERT ({source_columns}) VALUES ({insert_values})",
            self.target
        ))
        .with_params(row.to_vec())
    }

    fn key_predicate(&self) -> String {
        self.keys
            .iter()
            .map(|index| format!("{} = ?", self.columns[*index].quoted))
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    const fn mutation_columns(&self) -> &'static str {
        match self.mode {
            WriteMode::Update => "updated",
            WriteMode::DeleteByKeys => "deleted",
            WriteMode::Append | WriteMode::Create | WriteMode::Replace => "inserted",
            WriteMode::Upsert | WriteMode::TruncateInsert => "mixed",
        }
    }
}

impl WriteColumn {
    fn placeholder(&self) -> String {
        self.spatial.as_ref().map_or_else(
            || "?".to_owned(),
            |spatial| format!("ST_GEOMETRY(BLOB(HEXTORAW(?)), {})", spatial.srid),
        )
    }

    fn native_type(&self) -> Result<String> {
        if self.spatial.is_some() {
            Ok("ST_GEOMETRY".to_owned())
        } else {
            native_type(&self.data_type)
        }
    }
}

pub async fn prepare_write(
    config: &Db2Config,
    secret: &SecretString,
    operation: &WriteOperation,
    input_schema: SchemaRef,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<PreparedWrite> {
    budget.ensure_active()?;
    if cancellation.is_cancelled() {
        return Err(interruption_error(cancellation, ErrorPhase::Prepare));
    }
    let plan = Db2WritePlan::compile(config, &input_schema, operation)?;
    let (operation_lease, columns_lease) =
        ContractLeases::acquire(budget, input_schema.fields().len())?.into_parts();
    let loss_report = if operation.mode == WriteMode::Create {
        LossReport {
            schema_version: 2,
            policy: MappingPolicy::Strict,
            losses: Vec::new(),
        }
    } else {
        let config = config.clone();
        let secret = secret.clone();
        let schema = operation
            .target
            .schema
            .clone()
            .unwrap_or_else(|| config.username().to_ascii_uppercase());
        let object = operation.target.object.clone();
        let plan_for_probe = plan.clone();
        let task = tokio::task::spawn_blocking(move || {
            let (connection, timeout) = open_connection(&config, &secret)?;
            let target = describe_object(&connection, timeout, &schema, &object)?;
            plan_for_probe.preflight(&target)
        });
        task.await.map_err(|_| task_error(ErrorPhase::Prepare))??
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

pub async fn execute_write(
    config: &Db2Config,
    secret: &SecretString,
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
            "stream Db2 senza conteggio righe dichiarato",
        )
    })?;
    let plan = prepared
        .take_driver_state::<Db2WritePlan>()
        .ok_or_else(|| write_error(ErrorCategory::InvalidPlan, "piano preparato Db2 assente"))?;
    let diagnostic_policy = input.row_diagnostics_policy();
    let diagnostic = (plan.mode == WriteMode::Append
        && diagnostic_policy
            != plenora_database_core::row_diagnostics::RowDiagnosticsPolicy::default())
    .then(|| validate_diagnostic_input(&prepared.input_schema, declared_rows, diagnostic_policy))
    .transpose()?;
    let mut transaction = Db2Transaction::begin(
        config,
        secret,
        &TransactionOptions::default(),
        budget,
        cancellation,
    )
    .await?;
    let execution_id = format!(
        "db2-write-{}",
        EXECUTION_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    );
    if let Some(policy) = diagnostic {
        return execute_diagnostic_write(
            transaction,
            &plan,
            &prepared.input_schema,
            input.as_mut(),
            budget,
            cancellation,
            declared_rows,
            policy,
            execution_id,
        )
        .await;
    }
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
        Err(error) => return Err(rollback_and_shape(Box::new(transaction), error).await),
    };
    finish_commit(
        transaction,
        cancellation,
        execution_id,
        &plan,
        received,
        affected,
    )
    .await
}

async fn finish_commit(
    transaction: Db2Transaction,
    cancellation: &CancellationToken,
    execution_id: String,
    plan: &Db2WritePlan,
    received: u64,
    affected: u64,
) -> Result<WriteOutcome> {
    match Box::new(transaction).commit(cancellation).await? {
        CommitOutcome::Committed => committed_outcome(execution_id, plan, received, affected),
        CommitOutcome::OutcomeUnknown { recovery } => {
            let outcome = WriteOutcome {
                schema_version: 2,
                status: WriteStatus::OutcomeUnknown,
                execution_id,
                provider: ProviderKind::Db2,
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

fn validate_diagnostic_input(
    schema: &SchemaRef,
    declared_rows: u64,
    policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
) -> Result<plenora_database_core::row_diagnostics::RowDiagnosticsPolicy> {
    plenora_database_core::row_diagnostics::WriteDiagnosticsTracker::new(
        declared_rows,
        policy.clone(),
    )?;
    for field in [
        policy.key_field.as_deref(),
        policy.constraint_column.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if schema.field_with_name(field).is_err() {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "policy row-scoped Db2 riferita a un campo assente dallo schema preparato",
            ));
        }
    }
    Ok(policy)
}

#[allow(clippy::too_many_arguments)]
async fn execute_diagnostic_write(
    mut transaction: Db2Transaction,
    plan: &Db2WritePlan,
    schema: &SchemaRef,
    input: &mut dyn BatchStream,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    declared_rows: u64,
    policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
    execution_id: String,
) -> Result<WriteOutcome> {
    let constraint_column = policy.constraint_column.clone();
    let mut tracker = plenora_database_core::row_diagnostics::WriteDiagnosticsTracker::new(
        declared_rows,
        policy,
    )?;
    let applied;
    let diagnosed = {
        let mut writer = crate::row_diagnostics::Db2RowWriter::new(
            &mut transaction,
            plan,
            input,
            schema,
            budget,
            cancellation,
            constraint_column,
        );
        let diagnosed = plenora_database_core::row_diagnostics::diagnose_row_scoped_write(
            &mut writer,
            &mut tracker,
        )
        .await;
        applied = writer.applied();
        diagnosed
    };
    match diagnosed {
        Ok(Some(outcome)) => Err(outcome.into_error(Some(ProviderKind::Db2), Some(execution_id))?),
        Ok(None) => {
            finish_commit(
                transaction,
                cancellation,
                execution_id,
                plan,
                declared_rows,
                applied,
            )
            .await
        }
        Err(error) => Err(rollback_and_shape(Box::new(transaction), error).await),
    }
}

async fn execute_input(
    transaction: &mut Db2Transaction,
    plan: &Db2WritePlan,
    input_schema: &SchemaRef,
    input: &mut dyn BatchStream,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
    declared_rows: u64,
) -> Result<(u64, u64)> {
    if let Some(sql) = &plan.create_sql {
        transaction
            .execute_control_statement(sql.clone(), cancellation)
            .await?;
    } else if plan.mode == WriteMode::Replace {
        execute_statement(
            transaction,
            Statement::new(format!("DELETE FROM {}", plan.target)),
            cancellation,
        )
        .await?;
    }
    let mut received = 0_u64;
    let mut affected = 0_u64;
    while let Some(batch) = input.next_batch(cancellation).await? {
        validate_batch_schema(&batch, input_schema)?;
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
        let reservation = WriteResourceReservation::acquire(budget, rows, bytes, bytes, 0)?;
        plan.validate_spatial_batch(&batch, budget)?;
        let values = batch_values(&batch, plan)?;
        affected = affected
            .checked_add(execute_batch(transaction, plan, &values, cancellation).await?)
            .ok_or_else(|| {
                write_error(
                    ErrorCategory::ResourceLimit,
                    "conteggio righe Db2 in overflow",
                )
            })?;
        received = received.checked_add(rows).ok_or_else(|| {
            write_error(
                ErrorCategory::ResourceLimit,
                "conteggio input Db2 in overflow",
            )
        })?;
        reservation.commit()?;
    }
    if received != declared_rows {
        return Err(write_error(
            ErrorCategory::InvalidPlan,
            "righe prodotte dallo stream Db2 diverse dal conteggio dichiarato",
        ));
    }
    Ok((received, affected))
}

async fn execute_batch(
    transaction: &mut Db2Transaction,
    plan: &Db2WritePlan,
    rows: &[Vec<ParameterValue>],
    cancellation: &CancellationToken,
) -> Result<u64> {
    match plan.mode {
        WriteMode::Append | WriteMode::Create | WriteMode::Replace => {
            let mut affected = 0_u64;
            let sql = plan.array_insert_sql();
            for chunk in rows.chunks(ARRAY_BIND_ROWS) {
                affected = affected
                    .checked_add(
                        transaction
                            .execute_parameter_array(sql.clone(), chunk.to_vec(), cancellation)
                            .await?,
                    )
                    .ok_or_else(|| {
                        write_error(
                            ErrorCategory::ResourceLimit,
                            "conteggio insert Db2 in overflow",
                        )
                    })?;
            }
            Ok(affected)
        }
        WriteMode::Update => {
            execute_rows(transaction, rows, cancellation, |row| {
                plan.update_statement(row)
            })
            .await
        }
        WriteMode::Upsert => {
            execute_rows(transaction, rows, cancellation, |row| {
                plan.upsert_statement(row)
            })
            .await
        }
        WriteMode::DeleteByKeys => {
            execute_rows(transaction, rows, cancellation, |row| {
                plan.delete_statement(row)
            })
            .await
        }
        WriteMode::TruncateInsert => Err(write_error(
            ErrorCategory::Unsupported,
            "truncate_insert Db2 non qualificato",
        )),
    }
}

async fn execute_rows<F>(
    transaction: &mut Db2Transaction,
    rows: &[Vec<ParameterValue>],
    cancellation: &CancellationToken,
    statement: F,
) -> Result<u64>
where
    F: Fn(&[ParameterValue]) -> Statement,
{
    let mut affected = 0_u64;
    for row in rows {
        affected = affected
            .checked_add(execute_statement(transaction, statement(row), cancellation).await?)
            .ok_or_else(|| {
                write_error(
                    ErrorCategory::ResourceLimit,
                    "conteggio DML Db2 in overflow",
                )
            })?;
    }
    Ok(affected)
}

async fn execute_statement(
    transaction: &mut Db2Transaction,
    statement: Statement,
    cancellation: &CancellationToken,
) -> Result<u64> {
    transaction.execute(&statement, cancellation).await
}

async fn rollback_and_shape(
    transaction: Box<Db2Transaction>,
    mut original: DatabaseError,
) -> DatabaseError {
    let cleanup = CancellationToken::new();
    if transaction.rollback(&cleanup).await.is_ok() {
        original.remote_effect = RemoteEffect::RolledBack;
        original.retry = RetryDisposition::Never;
    } else {
        original.remote_effect = RemoteEffect::Unknown;
        original.retry = RetryDisposition::RequiresRecovery;
    }
    original
}

fn committed_outcome(
    execution_id: String,
    plan: &Db2WritePlan,
    received: u64,
    affected: u64,
) -> Result<WriteOutcome> {
    let skipped = received.saturating_sub(affected.min(received));
    let (confirmed, inserted, updated, deleted) = match plan.mutation_columns() {
        "inserted" => (received, Some(received), Some(0), Some(0)),
        "updated" => (affected, Some(0), Some(affected), Some(0)),
        "deleted" => (affected, Some(0), Some(0), Some(affected)),
        _ => (received, None, None, None),
    };
    let outcome = WriteOutcome {
        schema_version: 2,
        status: WriteStatus::Committed,
        execution_id,
        provider: ProviderKind::Db2,
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

pub fn validate_batch_schema(batch: &RecordBatch, declared: &SchemaRef) -> Result<()> {
    if batch.schema().as_ref() == declared.as_ref() {
        Ok(())
    } else {
        Err(write_error(
            ErrorCategory::InvalidPlan,
            "schema del batch diverso dallo schema dichiarato",
        ))
    }
}

pub fn batch_values(batch: &RecordBatch, plan: &Db2WritePlan) -> Result<Vec<Vec<ParameterValue>>> {
    (0..batch.num_rows())
        .map(|row| {
            batch
                .columns()
                .iter()
                .zip(batch.schema().fields().iter().zip(&plan.columns))
                .map(|(array, (field, column))| array_value(array.as_ref(), field, column, row))
                .collect()
        })
        .collect()
}

fn array_value(
    array: &dyn Array,
    field: &Field,
    column: &WriteColumn,
    row: usize,
) -> Result<ParameterValue> {
    if array.is_null(row) {
        return Ok(ParameterValue::Null {
            type_name: format!("{:?}", field.data_type()),
        });
    }
    if column.spatial.is_some() {
        return Ok(ParameterValue::Bytes(
            binary_value(array, field.data_type(), row)?.to_vec(),
        ));
    }
    let value = match field.data_type() {
        DataType::Boolean => ParameterValue::Bool(downcast::<BooleanArray>(array)?.value(row)),
        DataType::Int16 => {
            ParameterValue::I32(i32::from(downcast::<Int16Array>(array)?.value(row)))
        }
        DataType::Int32 => ParameterValue::I32(downcast::<Int32Array>(array)?.value(row)),
        DataType::Int64 => ParameterValue::I64(downcast::<Int64Array>(array)?.value(row)),
        DataType::Float32 => {
            let value = f64::from(downcast::<Float32Array>(array)?.value(row));
            finite(value)?;
            ParameterValue::F64(value)
        }
        DataType::Float64 => {
            let value = downcast::<Float64Array>(array)?.value(row);
            finite(value)?;
            ParameterValue::F64(value)
        }
        DataType::Decimal128(_, scale) => ParameterValue::Decimal(decimal_text(
            downcast::<Decimal128Array>(array)?.value(row),
            *scale,
        )?),
        DataType::Utf8 => {
            ParameterValue::String(downcast::<StringArray>(array)?.value(row).to_owned())
        }
        DataType::Date32 => {
            ParameterValue::Date(date_text(downcast::<Date32Array>(array)?.value(row))?)
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => ParameterValue::Timestamp(
            timestamp_text(downcast::<TimestampMicrosecondArray>(array)?.value(row))?,
        ),
        _ => {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "tipo Arrow non qualificato per write Db2",
            ));
        }
    };
    Ok(value)
}

fn binary_value<'a>(array: &'a dyn Array, data_type: &DataType, row: usize) -> Result<&'a [u8]> {
    match data_type {
        DataType::Binary => Ok(downcast::<BinaryArray>(array)?.value(row)),
        DataType::LargeBinary => Ok(downcast::<LargeBinaryArray>(array)?.value(row)),
        _ => Err(write_error(
            ErrorCategory::DataMapping,
            "campo spatial Db2 non binario",
        )),
    }
}

fn downcast<T: 'static>(array: &dyn Array) -> Result<&T> {
    array.as_any().downcast_ref().ok_or_else(|| {
        write_error(
            ErrorCategory::DataMapping,
            "array Arrow incoerente con il tipo dichiarato",
        )
    })
}

fn finite(value: f64) -> Result<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(write_error(
            ErrorCategory::DataMapping,
            "float non finito non qualificato per write Db2",
        ))
    }
}

fn date_text(days: i32) -> Result<String> {
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .and_then(|epoch| epoch.checked_add_signed(Duration::days(i64::from(days))))
        .map(|value| value.format("%Y-%m-%d").to_string())
        .ok_or_else(|| write_error(ErrorCategory::DataMapping, "data Arrow fuori range Db2"))
}

fn timestamp_text(micros: i64) -> Result<String> {
    chrono::DateTime::from_timestamp_micros(micros)
        .map(|value| {
            value
                .naive_utc()
                .format("%Y-%m-%d %H:%M:%S%.6f")
                .to_string()
        })
        .ok_or_else(|| {
            write_error(
                ErrorCategory::DataMapping,
                "timestamp Arrow fuori range Db2",
            )
        })
}

fn decimal_text(value: i128, scale: i8) -> Result<String> {
    if scale < 0 {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "scala DECIMAL negativa non qualificata per write Db2",
        ));
    }
    let scale = u32::try_from(scale).map_err(|_| {
        prepare_error(
            ErrorCategory::DataMapping,
            "scala DECIMAL Db2 non rappresentabile",
        )
    })?;
    if scale == 0 {
        return Ok(value.to_string());
    }
    let negative = value.is_negative();
    let digits = value.unsigned_abs().to_string();
    let scale = usize::try_from(scale).unwrap_or(usize::MAX);
    let padded = if digits.len() <= scale {
        format!("{}{}", "0".repeat(scale + 1 - digits.len()), digits)
    } else {
        digits
    };
    let split = padded.len() - scale;
    Ok(format!(
        "{}{}.{}",
        if negative { "-" } else { "" },
        &padded[..split],
        &padded[split..]
    ))
}

fn validate_operation(
    config: &Db2Config,
    schema: &SchemaRef,
    operation: &WriteOperation,
) -> Result<()> {
    if operation
        .target
        .catalog
        .as_deref()
        .is_some_and(|catalog| !catalog.eq_ignore_ascii_case(config.database()))
    {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "write cross-database Db2 non supportata",
        ));
    }
    if operation.mapping_policy != MappingPolicy::Strict {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "write Db2 qualifica soltanto mapping strict",
        ));
    }
    if operation.transaction_profile != TransactionProfile::SingleTransaction {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "write Db2 richiede il profilo single_transaction",
        ));
    }
    if operation.mode == WriteMode::TruncateInsert {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "truncate_insert Db2 non qualificato",
        ));
    }
    if operation.allow_partial || operation.create_spatial_index {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "write parziale o indice spatial Db2 non qualificato",
        ));
    }
    if schema.fields().is_empty() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "schema Arrow vuoto per write Db2",
        ));
    }
    validate_write_spatial_policy(schema, operation.srid_policy)?;
    let unique_names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<BTreeSet<_>>();
    if unique_names.len() != schema.fields().len() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "schema Arrow Db2 con colonne duplicate",
        ));
    }
    let unique_keys = operation.keys.iter().collect::<BTreeSet<_>>();
    if unique_keys.len() != operation.keys.len() {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "write Db2 con chiavi duplicate",
        ));
    }
    match operation.mode {
        WriteMode::Create => {
            validate_create_primary_key(schema, &operation.keys).map_err(|violation| {
                prepare_error(ErrorCategory::InvalidPlan, violation.message("Db2"))
            })?;
        }
        WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
            if operation.keys.is_empty() =>
        {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "modalita Db2 key-based senza chiavi",
            ));
        }
        WriteMode::Append | WriteMode::Replace if !operation.keys.is_empty() => {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "append/replace Db2 non usano chiavi",
            ));
        }
        _ => {}
    }
    if operation.mode == WriteMode::DeleteByKeys
        && (schema.fields().len() != operation.keys.len() || !operation.update_columns.is_empty())
    {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "delete_by_keys Db2 richiede uno schema composto dalle sole chiavi",
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
            "write spatial Db2 richiede SridPolicy::RequireMatch",
        )),
        (false, Some(_)) => Err(prepare_error(
            ErrorCategory::Unsupported,
            "srid_policy Db2 senza colonne spatial",
        )),
    }
}

fn compile_column(field: &Field, renderer: &Renderer) -> Result<WriteColumn> {
    let contract = FieldContract::parse(field)?;
    let spatial = if contract.spatial {
        if contract.is_geography() {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "semantica geography Db2 non qualificata",
            ));
        }
        if contract.encoding != Some("wkb") {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "write spatial Db2 richiede encoding WKB puro",
            ));
        }
        let dimensions = match contract.dimensions {
            Some("xy") => "xy",
            Some("xyz") => "xyz",
            _ => {
                return Err(prepare_error(
                    ErrorCategory::Unsupported,
                    "write spatial Db2 qualifica soltanto dimensioni XY o XYZ",
                ));
            }
        };
        let srid = contract.srid.ok_or_else(|| {
            prepare_error(
                ErrorCategory::Crs,
                "write spatial Db2 richiede SRID dichiarato",
            )
        })?;
        if srid == 0 || i32::try_from(srid).is_err() {
            return Err(prepare_error(
                ErrorCategory::Crs,
                "SRID spatial Db2 fuori intervallo",
            ));
        }
        if !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary) {
            return Err(prepare_error(
                ErrorCategory::DataMapping,
                "write spatial Db2 richiede Arrow Binary",
            ));
        }
        let exact_geometry_type = match contract.types_declaration {
            Some("mixed") => None,
            Some("exact") => {
                let geometry_type = contract.geometry_types.ok_or_else(|| {
                    prepare_error(
                        ErrorCategory::DataMapping,
                        "tipo geometry exact Db2 assente",
                    )
                })?;
                if geometry_type.contains(',') || !geometry_type_is_writable(geometry_type) {
                    return Err(prepare_error(
                        ErrorCategory::Unsupported,
                        "tipo geometry Db2 non qualificato",
                    ));
                }
                Some(geometry_type.to_owned())
            }
            _ => {
                return Err(prepare_error(
                    ErrorCategory::Unsupported,
                    "dichiarazione tipi geometry Db2 non qualificata",
                ));
            }
        };
        Some(SpatialWriteColumn {
            srid,
            dimensions,
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
    let native = match data_type {
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int16 => "SMALLINT".to_owned(),
        DataType::Int32 => "INTEGER".to_owned(),
        DataType::Int64 => "BIGINT".to_owned(),
        DataType::Float32 => "REAL".to_owned(),
        DataType::Float64 => "DOUBLE".to_owned(),
        DataType::Decimal128(precision, scale)
            if *precision > 0
                && *precision <= 31
                && *scale >= 0
                && *scale <= i8::try_from(*precision).unwrap_or(i8::MAX) =>
        {
            format!("DECIMAL({precision}, {scale})")
        }
        DataType::Utf8 => "CLOB(2G)".to_owned(),
        DataType::Date32 => "DATE".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP(6)".to_owned(),
        _ => {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "tipo Arrow non qualificato per write Db2",
            ));
        }
    };
    Ok(native)
}

fn native_type_matches(column: &WriteColumn, server: &crate::Db2Column) -> bool {
    let native = server.data_type.to_ascii_uppercase();
    if column.spatial.is_some() {
        return crate::types::is_spatial_type(&native);
    }
    match &column.data_type {
        DataType::Boolean => native == "BOOLEAN",
        DataType::Int16 => native == "SMALLINT",
        DataType::Int32 => native == "INTEGER",
        DataType::Int64 => native == "BIGINT",
        DataType::Float32 => native == "REAL",
        DataType::Float64 => native == "DOUBLE",
        DataType::Decimal128(precision, scale) => {
            matches!(native.as_str(), "DECIMAL" | "NUMERIC")
                && server.length == u64::from(*precision)
                && server.scale == i64::from(*scale)
        }
        DataType::Utf8 => matches!(native.as_str(), "CHARACTER" | "CHAR" | "VARCHAR" | "CLOB"),
        DataType::Date32 => native == "DATE",
        DataType::Timestamp(TimeUnit::Microsecond, None) => native == "TIMESTAMP",
        _ => false,
    }
}

fn target_has_unique_key(
    target: &Db2ObjectDescription,
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

const fn renderer() -> Renderer {
    Renderer::new(
        Dialect::Db2,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
}

fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(ProviderKind::Db2),
        message,
    )
}

fn write_error(category: ErrorCategory, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Write,
        Some(ProviderKind::Db2),
        message,
    )
}
