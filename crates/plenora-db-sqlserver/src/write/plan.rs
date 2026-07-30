use crate::{
    SqlServerColumn, SqlServerColumnKind, SqlServerColumnSpec, SqlServerObjectDescription,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::field_contract::{
    validate_schema_contract, FieldContract as CanonicalFieldContract,
};
use plenora_database_core::plan::{TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::protocol;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
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
    pub(super) spatial_srid: Option<u32>,
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
    pub(super) lock_sql: String,
    pub(super) truncate_sql: Option<String>,
    pub(super) schema_fingerprint: String,
    pub(super) schema: String,
    pub(super) object: String,
}

impl WritePlan {
    #[allow(clippy::too_many_lines)]
    pub(super) fn compile(
        description: &SqlServerObjectDescription,
        operation: &WriteOperation,
        input_schema: SchemaRef,
        observed_srids: &HashMap<String, Option<u32>>,
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
        let quoted_object = renderer.quote_object(&object_name);
        let mut columns = Vec::with_capacity(input_schema.fields().len());
        for (input_index, field) in input_schema.fields().iter().enumerate() {
            let target = description
                .columns
                .iter()
                .find(|column| column.name == field.name().as_str())
                .ok_or_else(|| {
                    plan_error(
                        ErrorCategory::Schema,
                        format!(
                            "colonna Arrow assente nel target SQL Server: {}",
                            field.name()
                        ),
                    )
                })?;
            if !is_writable(target) {
                return Err(plan_error(
                    ErrorCategory::Schema,
                    format!("colonna SQL Server non scrivibile: {}", target.name),
                ));
            }
            let target_spec = SqlServerColumnSpec::from_catalog(target)?;
            validate_arrow_type(field, &target_spec)?;
            let spatial_srid = validate_spatial_contract(
                field,
                &target_spec,
                observed_srids.get(&target.name).copied().flatten(),
            )?;
            columns.push(WriteColumnPlan {
                input_index,
                name: target.name.clone(),
                kind: target_spec.kind,
                native_type: target_spec.native_type,
                native_declaration: target_spec.native_declaration,
                nullable: target.nullable,
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
            lock_sql: format!(
                "SELECT TOP (0) 1 AS [plenora_lock] FROM {quoted_object} \
                 WITH (TABLOCKX, HOLDLOCK);"
            ),
            truncate_sql: (operation.mode == WriteMode::TruncateInsert)
                .then(|| format!("TRUNCATE TABLE {quoted_object};")),
            schema_fingerprint: description.token.structural_fingerprint.clone(),
            schema: description.schema.clone(),
            object: description.name.clone(),
        })
    }
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
            SqlServerColumnKind::Utf8 => column.native_type == "nvarchar",
            SqlServerColumnKind::Binary => column.native_type == "varbinary",
            SqlServerColumnKind::Decimal { scale, .. } => {
                column.native_type == "decimal" && (0..38).contains(&scale)
            }
            SqlServerColumnKind::Date
            | SqlServerColumnKind::Time
            | SqlServerColumnKind::Timestamp
            | SqlServerColumnKind::TimestampTz
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
    if !matches!(
        operation.mode,
        WriteMode::Append
            | WriteMode::TruncateInsert
            | WriteMode::Update
            | WriteMode::Upsert
            | WriteMode::DeleteByKeys
    ) {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "write SQL Server non supporta ancora create/replace",
        ));
    }
    if operation.transaction_profile != TransactionProfile::SingleTransaction {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "write SQL Server richiede single_transaction",
        ));
    }
    if operation.allow_partial || operation.create_spatial_index {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "write SQL Server richiede esecuzione atomica senza creazione indice",
        ));
    }
    validate_mutation_shape(operation)?;
    if operation.target.catalog.is_some()
        || operation.target.schema.is_none()
        || operation.target.layer_id.is_some()
    {
        return Err(plan_error(
            ErrorCategory::InvalidPlan,
            "target SQL Server richiede schema, senza catalog/layer",
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
        WriteMode::Create | WriteMode::Replace => {}
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
    observed_srid: Option<u32>,
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
    if contract
        .dimensions
        .is_some_and(|dimensions| dimensions != "xy")
    {
        return Err(plan_error(
            ErrorCategory::Unsupported,
            "write spatial SQL Server iniziale supporta soltanto XY",
        ));
    }
    let source_srid = contract.srid.ok_or_else(|| {
        plan_error(
            ErrorCategory::DataMapping,
            "SRID Arrow obbligatorio per write spatial SQL Server",
        )
    })?;
    if observed_srid.is_some_and(|target_srid| target_srid != source_srid) {
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
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
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
    use plenora_database_core::loss::MappingPolicy;
    use plenora_database_core::plan::{ObjectRef, SridPolicy};
    use std::sync::Arc;

    fn operation(mode: WriteMode) -> WriteOperation {
        WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some("dbo".to_owned()),
                object: "target".to_owned(),
                layer_id: None,
            },
            mode,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys: Vec::new(),
            update_columns: Vec::new(),
            srid_policy: Some(SridPolicy::RequireMatch),
            create_spatial_index: false,
            allow_partial: false,
        }
    }

    #[test]
    fn write_modes_and_required_key_shapes_are_explicit_before_io() {
        assert!(validate_operation(&operation(WriteMode::Append)).is_ok());
        assert!(validate_operation(&operation(WriteMode::TruncateInsert)).is_ok());
        for mode in [WriteMode::Create, WriteMode::Replace] {
            let error = validate_operation(&operation(mode)).expect_err("unsupported mode");
            assert_eq!(error.category, ErrorCategory::Unsupported);
            assert_eq!(error.phase, ErrorPhase::Prepare);
        }
        for mode in [
            WriteMode::Update,
            WriteMode::Upsert,
            WriteMode::DeleteByKeys,
        ] {
            let error = validate_operation(&operation(mode)).expect_err("keys required");
            assert_eq!(error.category, ErrorCategory::InvalidPlan);
            assert_eq!(error.phase, ErrorPhase::Prepare);
        }
        let mut update = operation(WriteMode::Update);
        update.keys = vec!["id".to_owned()];
        update.update_columns = vec!["label".to_owned()];
        validate_operation(&update).expect("valid update shape");
        let mut upsert = operation(WriteMode::Upsert);
        upsert.keys = vec!["id".to_owned()];
        validate_operation(&upsert).expect("valid upsert shape");
        let mut delete = operation(WriteMode::DeleteByKeys);
        delete.keys = vec!["id".to_owned()];
        validate_operation(&delete).expect("valid delete shape");
    }

    #[test]
    fn partial_or_non_atomic_write_is_rejected() {
        let mut candidate = operation(WriteMode::Append);
        candidate.allow_partial = true;
        assert!(validate_operation(&candidate).is_err());
        candidate.allow_partial = false;
        candidate.transaction_profile = TransactionProfile::ChunkCommitted;
        assert!(validate_operation(&candidate).is_err());
    }

    #[test]
    fn bulk_profile_is_explicit_and_rejects_ambiguous_columns_or_spatial() {
        let mut plan =
            WritePlan {
                input_schema: Arc::new(plenora_database_core::arrow::Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                ])),
                columns: vec![WriteColumnPlan {
                    input_index: 0,
                    name: "id".to_owned(),
                    kind: SqlServerColumnKind::I32,
                    native_type: "int".to_owned(),
                    native_declaration: "int".to_owned(),
                    nullable: false,
                    spatial_srid: None,
                }],
                mode: WriteMode::Append,
                row_sql: String::new(),
                key_input_indices: Vec::new(),
                bulk_table: "[dbo].[target]".to_owned(),
                bulk_columns_aligned: false,
                lock_sql: String::new(),
                truncate_sql: None,
                schema_fingerprint: "fingerprint".to_owned(),
                schema: "dbo".to_owned(),
                object: "target".to_owned(),
            };
        assert_eq!(
            validate_bulk_profile(&plan)
                .expect_err("partial columns")
                .category,
            ErrorCategory::Unsupported
        );
        plan.bulk_columns_aligned = true;
        validate_bulk_profile(&plan).expect("verified scalar profile");
        plan.columns[0].kind = SqlServerColumnKind::Geometry;
        plan.columns[0].native_type = "geometry".to_owned();
        assert_eq!(
            validate_bulk_profile(&plan)
                .expect_err("spatial bulk")
                .category,
            ErrorCategory::Unsupported
        );
        plan.columns[0].kind = SqlServerColumnKind::Date;
        plan.columns[0].native_type = "date".to_owned();
        assert!(validate_bulk_profile(&plan).is_err());
        plan.columns[0].kind = SqlServerColumnKind::Decimal {
            precision: 19,
            scale: 4,
        };
        plan.columns[0].native_type = "money".to_owned();
        assert!(validate_bulk_profile(&plan).is_err());
    }
}
