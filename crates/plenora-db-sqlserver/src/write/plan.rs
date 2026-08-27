use crate::{
    SqlServerColumn, SqlServerColumnKind, SqlServerColumnSpec, SqlServerObjectDescription,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::field_contract::{
    validate_schema_contract, FieldContract as CanonicalFieldContract,
};
use plenora_database_core::geometry::Dimensions;
use plenora_database_core::loss::{
    LossCategory, LossReport, LossSeverity, MappingLoss, MappingPolicy,
};
use plenora_database_core::plan::{TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::protocol;
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Identifier, ObjectName, Renderer};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub(super) struct WriteColumnPlan {
    pub(super) input_index: usize,
    pub(super) name: String,
    pub(super) kind: SqlServerColumnKind,
    pub(super) native_type: String,
    pub(super) native_declaration: String,
    pub(super) nullable: bool,
    pub(super) collation: Option<String>,
    pub(super) spatial_srid: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ObservedSpatialContract {
    pub(super) srid: Option<u32>,
    pub(super) dimensions: Option<Dimensions>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SpatialIndexKind {
    Geometry,
    Geography,
}

#[derive(Debug, Clone)]
pub(super) struct SpatialIndexPlan {
    pub(super) quoted_name: String,
    pub(super) quoted_column: String,
    pub(super) kind: SpatialIndexKind,
}

#[derive(Debug, Clone)]
pub(super) enum TargetLifecycle {
    Existing {
        lock_sql: String,
        truncate_sql: Option<String>,
        add_columns_sql: Vec<String>,
        schema_fingerprint: String,
    },
    Create {
        create_sql: String,
    },
    Replace {
        lock_sql: String,
        schema_fingerprint: String,
        create_sql: String,
        staging_object: String,
        backup_object: String,
    },
}

#[derive(Debug, Clone)]
pub(super) struct WritePlan {
    pub(super) input_schema: SchemaRef,
    pub(super) columns: Vec<WriteColumnPlan>,
    pub(super) mode: WriteMode,
    pub(super) row_sql: String,
    pub(super) key_input_indices: Vec<usize>,
    pub(super) bulk_table: String,
    pub(super) bulk_columns_aligned: bool,
    pub(super) lifecycle: TargetLifecycle,
    pub(super) schema: String,
    pub(super) object: String,
    pub(super) added_columns: Vec<AddedColumn>,
    pub(super) spatial_indexes: Vec<SpatialIndexPlan>,
}

#[derive(Debug, Clone)]
pub(super) struct AddedColumn {
    pub(super) input_index: usize,
    pub(super) source_type: String,
    pub(super) native_declaration: String,
}

impl WritePlan {
    #[allow(clippy::too_many_lines)]
    pub(super) fn compile_existing(
        description: &SqlServerObjectDescription,
        operation: &WriteOperation,
        input_schema: SchemaRef,
        observed_spatial: &HashMap<String, ObservedSpatialContract>,
        schema_evolution: super::SqlServerSchemaEvolution,
    ) -> Result<Self> {
        validate_operation(operation)?;
        if input_schema.fields().is_empty() {
            return Err(plan_error(
                ErrorCategory::InvalidPlan,
                "write SQL Server richiede almeno una colonna",
            ));
        }
        validate_schema_contract(&input_schema)?;
        let renderer = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        );
        let object_name = ObjectName {
            catalog: None,
            schema: Some(sql_identifier(&description.schema)?),
            object: sql_identifier(&description.name)?,
        };
        let quoted_object = renderer.quote_object(&object_name)?;
        let mut columns = Vec::with_capacity(input_schema.fields().len());
        let mut added_columns = Vec::new();
        let mut add_columns_sql = Vec::new();
        for (input_index, field) in input_schema.fields().iter().enumerate() {
            let target = description
                .columns
                .iter()
                .find(|column| column.name == field.name().as_str());
            let Some(target) = target else {
                let evolution_allowed = schema_evolution
                    == super::SqlServerSchemaEvolution::AddNullableColumns
                    && supports_additive_evolution(operation.mode);
                if !evolution_allowed {
                    return Err(plan_error(
                        ErrorCategory::Schema,
                        format!(
                            "colonna Arrow assente nel target SQL Server: {}",
                            field.name()
                        ),
                    ));
                }
                if !field.is_nullable() {
                    return Err(plan_error(
                        ErrorCategory::Schema,
                        format!(
                            "schema evolution SQL Server ammette solo colonne nullable: {}",
                            field.name()
                        ),
                    ));
                }
                if operation.keys.iter().any(|key| key == field.name()) {
                    return Err(plan_error(
                        ErrorCategory::InvalidPlan,
                        "schema evolution SQL Server non puo aggiungere una chiave",
                    ));
                }
                let target_spec = SqlServerColumnSpec::from_create_field(field)?;
                validate_arrow_type(field, &target_spec)?;
                let spatial_srid = validate_spatial_contract(field, &target_spec, None)?;
                if let Some(collation) = &target_spec.collation {
                    validate_collation_name(collation)?;
                }
                let quoted_column = renderer.quote_identifier(&sql_identifier(field.name())?)?;
                let mut definition = format!("{quoted_column} {}", target_spec.native_declaration);
                if let Some(collation) = &target_spec.collation {
                    definition.push_str(" COLLATE ");
                    definition.push_str(collation);
                }
                definition.push_str(" NULL");
                add_columns_sql.push(format!("ALTER TABLE {quoted_object} ADD {definition};"));
                added_columns.push(AddedColumn {
                    input_index,
                    source_type: field.data_type().to_string(),
                    native_declaration: target_spec.native_declaration.clone(),
                });
                columns.push(WriteColumnPlan {
                    input_index,
                    name: field.name().clone(),
                    kind: target_spec.kind,
                    native_type: target_spec.native_type,
                    native_declaration: target_spec.native_declaration,
                    nullable: true,
                    collation: target_spec.collation,
                    spatial_srid,
                });
                continue;
            };
            if !is_writable(target) {
                return Err(plan_error(
                    ErrorCategory::Schema,
                    format!("colonna SQL Server non scrivibile: {}", target.name),
                ));
            }
            let target_spec = SqlServerColumnSpec::from_catalog(target)?;
            validate_arrow_type(field, &target_spec)?;
            let spatial_srid =
                validate_spatial_contract(field, &target_spec, observed_spatial.get(&target.name))?;
            columns.push(WriteColumnPlan {
                input_index,
                name: target.name.clone(),
                kind: target_spec.kind,
                native_type: target_spec.native_type,
                native_declaration: target_spec.native_declaration,
                nullable: target.nullable,
                collation: target.collation.clone(),
                spatial_srid,
            });
        }
        if matches!(
            operation.mode,
            WriteMode::Append | WriteMode::TruncateInsert | WriteMode::Upsert
        ) {
            validate_required_target_columns(description, &columns)?;
        }
        let parameter_count = columns.iter().try_fold(0_usize, |count, column| {
            count
                .checked_add(if column.spatial_srid.is_some() { 2 } else { 1 })
                .ok_or_else(|| {
                    plan_error(
                        ErrorCategory::ResourceLimit,
                        "overflow parametri write SQL Server",
                    )
                })
        })?;
        if parameter_count > crate::MAX_BIND_PARAMETERS {
            return Err(plan_error(
                ErrorCategory::ResourceLimit,
                "write SQL Server oltre 2100 parametri per riga",
            ));
        }
        validate_mutation_columns(operation, &columns)?;
        if matches!(
            operation.mode,
            WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
        ) {
            validate_unique_key(description, &operation.keys)?;
        }
        let row_statement =
            super::sql::compile_row_statement(operation, &columns, &renderer, &quoted_object)?;
        let bulk_columns_aligned = bulk_columns_are_aligned(description, &columns);
        Ok(Self {
            input_schema,
            columns,
            mode: operation.mode,
            row_sql: row_statement.sql,
            key_input_indices: row_statement.key_input_indices,
            bulk_table: quoted_object.clone(),
            bulk_columns_aligned,
            lifecycle: TargetLifecycle::Existing {
                lock_sql: lock_sql(&quoted_object),
                truncate_sql: (operation.mode == WriteMode::TruncateInsert)
                    .then(|| format!("TRUNCATE TABLE {quoted_object};")),
                add_columns_sql,
                schema_fingerprint: description.token.structural_fingerprint.clone(),
            },
            schema: description.schema.clone(),
            object: description.name.clone(),
            added_columns,
            spatial_indexes: Vec::new(),
        })
    }

    pub(super) fn compile_create(
        operation: &WriteOperation,
        input_schema: SchemaRef,
    ) -> Result<Self> {
        compile_new_target(operation, input_schema, &operation.target.object, None)
    }

    pub(super) fn compile_replace(
        description: &SqlServerObjectDescription,
        operation: &WriteOperation,
        input_schema: SchemaRef,
        staging_object: &str,
        backup_object: &str,
    ) -> Result<Self> {
        validate_replace_description(description, operation)?;
        compile_new_target(
            operation,
            input_schema,
            staging_object,
            Some((description, staging_object, backup_object)),
        )
    }

    pub(super) fn loss_report(&self, policy: MappingPolicy) -> Result<LossReport> {
        let losses = self
            .added_columns
            .iter()
            .map(|column| {
                Ok(MappingLoss {
                    field_id: u32::try_from(column.input_index).map_err(|_| {
                        DatabaseError::invalid_plan("numero colonne oltre il contratto LossReport")
                    })?,
                    category: LossCategory::NativeType,
                    severity: LossSeverity::Information,
                    reason: "colonna aggiunta al target SQL Server come nullable".to_owned(),
                    source_type: Some(column.source_type.clone()),
                    target_type: Some(column.native_declaration.clone()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let report = LossReport {
            schema_version: 2,
            policy,
            losses,
        };
        // Stesso motivo del preflight PostgreSQL: i tetti del contratto si
        // verificano dove il report nasce.
        report.validate()?;
        Ok(report)
    }
}

const fn supports_additive_evolution(mode: WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::Append | WriteMode::TruncateInsert | WriteMode::Update | WriteMode::Upsert
    )
}

fn compile_new_target(
    operation: &WriteOperation,
    input_schema: SchemaRef,
    write_object: &str,
    replacement: Option<(&SqlServerObjectDescription, &str, &str)>,
) -> Result<WritePlan> {
    validate_operation(operation)?;
    if input_schema.fields().is_empty() {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "create/replace SQL Server richiede almeno una colonna",
        ));
    }
    validate_schema_contract(&input_schema)?;
    let renderer = Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    );
    let schema = operation.target.schema.as_deref().ok_or_else(|| {
        plan_error(
            ErrorCategory::InvalidPlan,
            "create/replace SQL Server richiede schema",
        )
    })?;
    let quoted_write_object = renderer.quote_object(&ObjectName {
        catalog: None,
        schema: Some(sql_identifier(schema)?),
        object: sql_identifier(write_object)?,
    })?;
    let mut columns = Vec::with_capacity(input_schema.fields().len());
    for (input_index, field) in input_schema.fields().iter().enumerate() {
        let spec = SqlServerColumnSpec::from_create_field(field)?;
        validate_arrow_type(field, &spec)?;
        let spatial_srid = validate_spatial_contract(field, &spec, None)?;
        if let Some(collation) = &spec.collation {
            validate_collation_name(collation)?;
            if !matches!(
                spec.native_type.as_str(),
                "char" | "varchar" | "nchar" | "nvarchar" | "text" | "ntext"
            ) {
                return Err(plan_error(
                    ErrorCategory::DataMapping,
                    "collation SQL Server ammessa soltanto sui tipi testuali",
                ));
            }
        }
        columns.push(WriteColumnPlan {
            input_index,
            name: field.name().clone(),
            kind: spec.kind,
            native_type: spec.native_type,
            native_declaration: spec.native_declaration,
            nullable: field.is_nullable(),
            collation: spec.collation,
            spatial_srid,
        });
    }
    validate_mutation_columns(operation, &columns)?;
    validate_parameter_count(&columns)?;
    let row_statement =
        super::sql::compile_row_statement(operation, &columns, &renderer, &quoted_write_object)?;
    let create_sql = create_table_sql(&renderer, &quoted_write_object, &columns, &operation.keys)?;
    let spatial_indexes = compile_spatial_indexes(operation, &columns, &renderer)?;
    let lifecycle = if let Some((description, staging_object, backup_object)) = replacement {
        let quoted_original = renderer.quote_object(&ObjectName {
            catalog: None,
            schema: Some(sql_identifier(&description.schema)?),
            object: sql_identifier(&description.name)?,
        })?;
        TargetLifecycle::Replace {
            lock_sql: lock_sql(&quoted_original),
            schema_fingerprint: description.token.structural_fingerprint.clone(),
            create_sql,
            staging_object: staging_object.to_owned(),
            backup_object: backup_object.to_owned(),
        }
    } else {
        TargetLifecycle::Create { create_sql }
    };
    Ok(WritePlan {
        input_schema,
        columns,
        mode: operation.mode,
        row_sql: row_statement.sql,
        key_input_indices: row_statement.key_input_indices,
        bulk_table: quoted_write_object,
        bulk_columns_aligned: true,
        lifecycle,
        schema: schema.to_owned(),
        object: operation.target.object.clone(),
        added_columns: Vec::new(),
        spatial_indexes,
    })
}

fn compile_spatial_indexes(
    operation: &WriteOperation,
    columns: &[WriteColumnPlan],
    renderer: &Renderer,
) -> Result<Vec<SpatialIndexPlan>> {
    if !operation.create_spatial_index {
        return Ok(Vec::new());
    }
    let indexes = columns
        .iter()
        .enumerate()
        .filter_map(|(ordinal, column)| {
            let kind = match column.kind {
                SqlServerColumnKind::Geometry => SpatialIndexKind::Geometry,
                SqlServerColumnKind::Geography => SpatialIndexKind::Geography,
                _ => return None,
            };
            Some((ordinal, column, kind))
        })
        .map(|(ordinal, column, kind)| {
            let index_ordinal = ordinal.checked_add(1).ok_or_else(|| {
                plan_error(
                    ErrorCategory::ResourceLimit,
                    "overflow ordinale indice spatial SQL Server",
                )
            })?;
            let name = format!("IX_pln_spatial_{index_ordinal}");
            Ok(SpatialIndexPlan {
                quoted_name: renderer.quote_identifier(&sql_identifier(&name)?)?,
                quoted_column: renderer.quote_identifier(&sql_identifier(&column.name)?)?,
                kind,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if indexes.is_empty() {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "creazione indice spatial richiesta senza colonne geometry/geography",
        ));
    }
    Ok(indexes)
}

fn validate_parameter_count(columns: &[WriteColumnPlan]) -> Result<()> {
    let count = columns.iter().try_fold(0_usize, |count, column| {
        count
            .checked_add(if column.spatial_srid.is_some() { 2 } else { 1 })
            .ok_or_else(|| {
                plan_error(
                    ErrorCategory::ResourceLimit,
                    "overflow parametri write SQL Server",
                )
            })
    })?;
    if count > crate::MAX_BIND_PARAMETERS {
        return Err(plan_error(
            ErrorCategory::ResourceLimit,
            "write SQL Server oltre 2100 parametri per riga",
        ));
    }
    Ok(())
}

fn lock_sql(quoted_object: &str) -> String {
    format!(
        "SELECT TOP (1) 1 AS [plenora_lock] FROM {quoted_object} \
         WITH (TABLOCKX, HOLDLOCK);"
    )
}

fn create_table_sql(
    renderer: &Renderer,
    quoted_object: &str,
    columns: &[WriteColumnPlan],
    keys: &[String],
) -> Result<String> {
    let mut definitions = columns
        .iter()
        .map(|column| {
            let name = renderer.quote_identifier(&sql_identifier(&column.name)?)?;
            let collation = if let Some(value) = &column.collation {
                validate_collation_name(value)?;
                format!(" COLLATE {value}")
            } else {
                String::new()
            };
            let nullable = if column.nullable {
                " NULL"
            } else {
                " NOT NULL"
            };
            Ok(format!(
                "{name} {}{collation}{nullable}",
                column.native_declaration
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    if !keys.is_empty() {
        let mut quoted_keys = Vec::with_capacity(keys.len());
        for key in keys {
            let column = columns
                .iter()
                .find(|column| column.name == *key)
                .ok_or_else(|| {
                    plan_error(
                        ErrorCategory::InvalidPlan,
                        format!("chiave create SQL Server assente: {key}"),
                    )
                })?;
            if column.nullable {
                return Err(plan_error(
                    ErrorCategory::InvalidPlan,
                    "chiave primaria create SQL Server non puo essere nullable",
                ));
            }
            quoted_keys.push(renderer.quote_identifier(&sql_identifier(key)?)?);
        }
        definitions.push(format!("PRIMARY KEY ({})", quoted_keys.join(", ")));
    }
    Ok(format!(
        "CREATE TABLE {quoted_object} ({});",
        definitions.join(", ")
    ))
}

fn validate_collation_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.chars().count() > crate::MAX_IDENTIFIER_CHARACTERS
        || !value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphabetic)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        return Err(plan_error(
            ErrorCategory::DataMapping,
            "collation SQL Server fuori dalla grammatica ammessa",
        ));
    }
    Ok(())
}

fn validate_replace_description(
    description: &SqlServerObjectDescription,
    operation: &WriteOperation,
) -> Result<()> {
    if description.kind != "USER_TABLE"
        || description.temporal_type != 0
        || description.memory_optimized
    {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "replace SQL Server limitato a USER_TABLE non temporal e non memory-optimized",
        ));
    }
    if description.columns.iter().any(|column| {
        !is_writable(column)
            || column.default_definition.is_some()
            || column.computed_definition.is_some()
    }) {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "replace SQL Server non preserva identity/computed/generated/default",
        ));
    }
    let unsupported_constraint = description.constraints.iter().any(|constraint| {
        if constraint.kind != "PRIMARY_KEY_CONSTRAINT" {
            return true;
        }
        constraint.columns.as_deref().is_none_or(|columns| {
            columns
                .split(',')
                .ne(operation.keys.iter().map(String::as_str))
        })
    });
    if unsupported_constraint
        || description.indexes.iter().any(|index| {
            let represented_primary_key = index.primary_key
                && index.kind == "CLUSTERED"
                && index.unique
                && !index.disabled
                && !index.filtered
                && index.spatial.is_none();
            let represented_spatial = operation.create_spatial_index
                && is_canonical_spatial_index(index, &description.columns);
            !represented_primary_key && !represented_spatial
        })
        || description
            .columns
            .iter()
            .any(|column| column.type_schema != "sys")
    {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "replace SQL Server rifiuta UDT, vincoli o indici non rappresentati dal piano",
        ));
    }
    Ok(())
}

fn is_canonical_spatial_index(
    index: &crate::SqlServerIndex,
    columns: &[crate::SqlServerColumn],
) -> bool {
    if index.kind != "SPATIAL"
        || index.unique
        || index.primary_key
        || index.unique_constraint
        || index.disabled
        || index.filtered
    {
        return false;
    }
    let Some(spatial) = index.spatial.as_ref() else {
        return false;
    };
    columns.iter().any(|column| {
        let expected_name = format!("IX_pln_spatial_{}", column.ordinal);
        let expected_columns = format!("{}:0:0:0", column.name);
        let type_and_tessellation_match = match spatial.spatial_type.as_str() {
            "GEOMETRY" => {
                column.native_type.eq_ignore_ascii_case("geometry")
                    && spatial.tessellation_scheme == "GEOMETRY_AUTO_GRID"
                    && spatial.bounding_box.is_some()
            }
            "GEOGRAPHY" => {
                column.native_type.eq_ignore_ascii_case("geography")
                    && spatial.tessellation_scheme == "GEOGRAPHY_AUTO_GRID"
                    && spatial.bounding_box.is_none()
            }
            _ => false,
        };
        type_and_tessellation_match
            && index.name.as_deref() == Some(expected_name.as_str())
            && index.columns.as_deref() == Some(expected_columns.as_str())
    })
}

pub(super) fn validate_bulk_profile(plan: &WritePlan) -> Result<()> {
    if !matches!(plan.mode, WriteMode::Append | WriteMode::TruncateInsert) {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "TDS bulk supporta soltanto append e truncate_insert",
        ));
    }
    if !plan.bulk_columns_aligned {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "TDS bulk richiede tutte le colonne scrivibili nell'ordine del catalogo",
        ));
    }
    for column in &plan.columns {
        let supported = match column.kind {
            SqlServerColumnKind::Bool
            | SqlServerColumnKind::U8
            | SqlServerColumnKind::I16
            | SqlServerColumnKind::I32
            | SqlServerColumnKind::I64
            | SqlServerColumnKind::F32
            | SqlServerColumnKind::F64 => true,
            SqlServerColumnKind::Utf8 => {
                matches!(column.native_type.as_str(), "nvarchar" | "uniqueidentifier")
            }
            SqlServerColumnKind::Binary => column.native_type == "varbinary",
            SqlServerColumnKind::Decimal { scale, .. } => {
                column.native_type == "decimal" && (0..38).contains(&scale)
            }
            SqlServerColumnKind::Time => column.native_type == "time",
            SqlServerColumnKind::Timestamp => column.native_type == "datetime2",
            SqlServerColumnKind::TimestampTz => column.native_type == "datetimeoffset",
            SqlServerColumnKind::Date
            | SqlServerColumnKind::Geometry
            | SqlServerColumnKind::Geography => false,
        };
        if !supported {
            return Err(plan_error(
                ErrorCategory::Unsupported,
                format!(
                    "tipo {} non ammesso dal profilo TDS bulk verificato",
                    column.native_declaration
                ),
            ));
        }
    }
    Ok(())
}

fn bulk_columns_are_aligned(
    description: &SqlServerObjectDescription,
    columns: &[WriteColumnPlan],
) -> bool {
    let writable = description
        .columns
        .iter()
        .filter(|column| is_writable(column))
        .collect::<Vec<_>>();
    writable.len() == columns.len()
        && writable
            .iter()
            .zip(columns)
            .all(|(target, input)| target.name == input.name)
}

pub(super) fn validate_operation(operation: &WriteOperation) -> Result<()> {
    let profile_supported = match operation.mode {
        WriteMode::Replace => matches!(
            operation.transaction_profile,
            TransactionProfile::SingleTransaction | TransactionProfile::StagedSwap
        ),
        _ => operation.transaction_profile == TransactionProfile::SingleTransaction,
    };
    if !profile_supported {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "transaction_profile incompatibile col lifecycle SQL Server",
        ));
    }
    if operation.allow_partial {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "write SQL Server richiede esecuzione atomica",
        ));
    }
    if operation.create_spatial_index
        && !matches!(operation.mode, WriteMode::Create | WriteMode::Replace)
    {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "indice spatial SQL Server supportato soltanto in create/replace",
        ));
    }
    if operation.create_spatial_index && operation.keys.is_empty() {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "indice spatial SQL Server richiede una primary key clustered",
        ));
    }
    validate_mutation_shape(operation)?;
    if operation.target.catalog.is_some() || operation.target.schema.is_none() {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "target SQL Server richiede schema, senza catalog",
        ));
    }
    Ok(())
}

fn validate_mutation_shape(operation: &WriteOperation) -> Result<()> {
    let duplicate_keys = operation
        .keys
        .iter()
        .enumerate()
        .any(|(index, key)| operation.keys[..index].contains(key));
    let duplicate_updates = operation
        .update_columns
        .iter()
        .enumerate()
        .any(|(index, name)| operation.update_columns[..index].contains(name));
    if duplicate_keys || duplicate_updates {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "chiavi e colonne update SQL Server devono essere univoche",
        ));
    }
    if operation
        .keys
        .iter()
        .any(|key| operation.update_columns.contains(key))
    {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "una chiave SQL Server non puo essere anche colonna update",
        ));
    }
    match operation.mode {
        WriteMode::Append | WriteMode::TruncateInsert => {
            if !operation.keys.is_empty() || !operation.update_columns.is_empty() {
                return Err(plan_error(
                    ErrorCategory::InvalidPlan,
                    "append/truncate_insert non accettano keys o update_columns",
                ));
            }
        }
        WriteMode::Update => {
            if operation.keys.is_empty() || operation.update_columns.is_empty() {
                return Err(plan_error(
                    ErrorCategory::InvalidPlan,
                    "update SQL Server richiede keys e update_columns",
                ));
            }
        }
        WriteMode::Upsert => {
            if operation.keys.is_empty() {
                return Err(plan_error(
                    ErrorCategory::InvalidPlan,
                    "upsert SQL Server richiede almeno una chiave",
                ));
            }
        }
        WriteMode::DeleteByKeys => {
            if operation.keys.is_empty() || !operation.update_columns.is_empty() {
                return Err(plan_error(
                    ErrorCategory::InvalidPlan,
                    "delete_by_keys richiede keys e non accetta update_columns",
                ));
            }
        }
        WriteMode::Create | WriteMode::Replace => {
            if !operation.update_columns.is_empty() {
                return Err(plan_error(
                    ErrorCategory::InvalidPlan,
                    "create/replace non accettano update_columns",
                ));
            }
        }
    }
    Ok(())
}

fn validate_mutation_columns(
    operation: &WriteOperation,
    columns: &[WriteColumnPlan],
) -> Result<()> {
    for name in operation.keys.iter().chain(&operation.update_columns) {
        let column = columns
            .iter()
            .find(|column| column.name == *name)
            .ok_or_else(|| {
                plan_error(
                    ErrorCategory::InvalidPlan,
                    format!("chiave o colonna update assente dallo schema Arrow: {name}"),
                )
            })?;
        if operation.keys.contains(name)
            && matches!(
                column.kind,
                SqlServerColumnKind::Geometry | SqlServerColumnKind::Geography
            )
        {
            return Err(plan_error(
                ErrorCategory::Unsupported,
                "geometry/geography non sono ammesse come chiavi write SQL Server",
            ));
        }
    }
    Ok(())
}

fn validate_unique_key(description: &SqlServerObjectDescription, keys: &[String]) -> Result<()> {
    if keys
        .iter()
        .any(|key| key.contains(',') || key.contains(':'))
    {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "chiavi SQL Server con ',' o ':' non verificabili dal catalogo corrente",
        ));
    }
    let expected = keys.iter().map(String::as_str).collect::<HashSet<_>>();
    let unique = description.indexes.iter().any(|index| {
        if !index.unique || index.disabled || index.filtered {
            return false;
        }
        let Some(columns) = index.columns.as_deref().and_then(index_key_columns) else {
            return false;
        };
        columns.len() == expected.len()
            && columns.iter().map(String::as_str).collect::<HashSet<_>>() == expected
    });
    if !unique {
        return Err(plan_error(
            ErrorCategory::Schema,
            "keys write SQL Server prive di indice univoco non filtrato equivalente",
        ));
    }
    Ok(())
}

fn index_key_columns(encoded: &str) -> Option<Vec<String>> {
    let mut columns = Vec::new();
    for item in encoded.split(',') {
        let (prefix, included) = item.rsplit_once(':')?;
        let (prefix, _descending) = prefix.rsplit_once(':')?;
        let (name, ordinal) = prefix.rsplit_once(':')?;
        let ordinal = ordinal.parse::<usize>().ok()?;
        let included = included.parse::<u8>().ok()?;
        if included == 0 && ordinal > 0 {
            columns.push((ordinal, name.to_owned()));
        }
    }
    columns.sort_by_key(|(ordinal, _)| *ordinal);
    Some(columns.into_iter().map(|(_, name)| name).collect())
}

fn validate_arrow_type(field: &Field, target: &SqlServerColumnSpec) -> Result<()> {
    let expected = match target.kind {
        SqlServerColumnKind::Bool => DataType::Boolean,
        SqlServerColumnKind::U8 => DataType::UInt8,
        SqlServerColumnKind::I16 => DataType::Int16,
        SqlServerColumnKind::I32 => DataType::Int32,
        SqlServerColumnKind::I64 => DataType::Int64,
        SqlServerColumnKind::F32 => DataType::Float32,
        SqlServerColumnKind::F64 => DataType::Float64,
        SqlServerColumnKind::Utf8 | SqlServerColumnKind::TimestampTz => DataType::Utf8,
        SqlServerColumnKind::Binary
        | SqlServerColumnKind::Geometry
        | SqlServerColumnKind::Geography => DataType::Binary,
        SqlServerColumnKind::Date => DataType::Date32,
        SqlServerColumnKind::Time => DataType::Time64(TimeUnit::Microsecond),
        SqlServerColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        SqlServerColumnKind::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
    };
    if field.data_type() != &expected {
        return Err(plan_error(
            ErrorCategory::DataMapping,
            format!(
                "tipo Arrow incompatibile con {} SQL Server",
                target.native_declaration
            ),
        ));
    }
    Ok(())
}

fn validate_spatial_contract(
    field: &Field,
    target: &SqlServerColumnSpec,
    observed: Option<&ObservedSpatialContract>,
) -> Result<Option<u32>> {
    let Some(semantics) = target.kind.spatial_semantics() else {
        return Ok(None);
    };
    let contract = CanonicalFieldContract::parse(field)?;
    if field
        .metadata()
        .get(protocol::GEOARROW_EXTENSION_NAME)
        .map(String::as_str)
        != Some("geoarrow.wkb")
        || contract.encoding != Some("wkb")
    {
        return Err(plan_error(
            ErrorCategory::DataMapping,
            "colonna spatial Arrow priva del contratto GeoArrow WKB",
        ));
    }
    let expected_semantics = match semantics {
        plenora_database_core::geometry::SpatialSemantics::Geometry => "geometry",
        plenora_database_core::geometry::SpatialSemantics::Geography => "geography",
    };
    if contract.spatial_semantics != Some(expected_semantics) {
        return Err(plan_error(
            ErrorCategory::DataMapping,
            "semantica geometry/geography Arrow incompatibile col target",
        ));
    }
    let source_dimensions = match contract.dimensions {
        Some("xy") => Dimensions::Xy,
        Some("xyz") => Dimensions::Xyz,
        Some("xym") => Dimensions::Xym,
        Some("xyzm") => Dimensions::Xyzm,
        _ => {
            return Err(plan_error(
                ErrorCategory::DataMapping,
                "write spatial SQL Server richiede dimensioni WKB esplicite",
            ));
        }
    };
    if observed
        .and_then(|value| value.dimensions)
        .is_some_and(|target_dimensions| target_dimensions != source_dimensions)
    {
        return Err(plan_error(
            ErrorCategory::DataMapping,
            "dimensioni Arrow diverse dai valori esistenti nel target SQL Server",
        ));
    }
    let source_srid = contract.srid.ok_or_else(|| {
        plan_error(
            ErrorCategory::DataMapping,
            "SRID Arrow obbligatorio per write spatial SQL Server",
        )
    })?;
    if observed
        .and_then(|value| value.srid)
        .is_some_and(|target_srid| target_srid != source_srid)
    {
        return Err(plan_error(
            ErrorCategory::DataMapping,
            "SRID Arrow diverso dai valori esistenti nel target SQL Server",
        ));
    }
    i32::try_from(source_srid).map_err(|_| {
        plan_error(
            ErrorCategory::DataMapping,
            "SRID Arrow oltre il range int SQL Server",
        )
    })?;
    Ok(Some(source_srid))
}

fn validate_required_target_columns(
    description: &SqlServerObjectDescription,
    columns: &[WriteColumnPlan],
) -> Result<()> {
    for target in &description.columns {
        if is_writable(target)
            && !target.nullable
            && target.default_definition.is_none()
            && !columns.iter().any(|column| column.name == target.name)
        {
            return Err(plan_error(
                ErrorCategory::Schema,
                format!(
                    "colonna target obbligatoria assente dall'input: {}",
                    target.name
                ),
            ));
        }
    }
    Ok(())
}

fn is_writable(column: &SqlServerColumn) -> bool {
    !column.identity
        && !column.computed
        && column.generated_always_type == 0
        && !matches!(column.native_type.as_str(), "timestamp" | "rowversion")
}

pub(super) fn sql_identifier(value: &str) -> Result<Identifier> {
    if value.chars().count() > crate::MAX_IDENTIFIER_CHARACTERS {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "identificatore oltre 128 caratteri SQL Server",
        ));
    }
    Identifier::new(value.to_owned())
}

pub(super) fn plan_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        message,
    )
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;
