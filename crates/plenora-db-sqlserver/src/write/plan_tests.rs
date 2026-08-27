use super::*;
use crate::{
    SqlServerColumn, SqlServerConstraint, SqlServerIndex, SqlServerSchemaEvolution,
    SqlServerSchemaToken, SqlServerSpatialBoundingBox, SqlServerSpatialIndex,
};
use plenora_database_core::loss::{LossSeverity, MappingPolicy};
use plenora_database_core::plan::{ObjectRef, SridPolicy};
use std::collections::HashMap;
use std::sync::Arc;

fn operation(mode: WriteMode) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("dbo".to_owned()),
            object: "target".to_owned(),
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

fn spatial_create_schema() -> SchemaRef {
    fn spatial_field(name: &str, semantics: &str, native_type: &str) -> Field {
        Field::new(name, DataType::Binary, true).with_metadata(HashMap::from([
            (
                protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                "geoarrow.wkb".to_owned(),
            ),
            (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
            (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
            (
                protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                "mixed".to_owned(),
            ),
            (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
            (
                protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                semantics.to_owned(),
            ),
            (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
            (
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "declared_unresolved".to_owned(),
            ),
            (
                protocol::SQLSERVER_NATIVE_TYPE.to_owned(),
                native_type.to_owned(),
            ),
            (
                protocol::SQLSERVER_NATIVE_DECLARATION.to_owned(),
                native_type.to_owned(),
            ),
        ]))
    }
    Arc::new(plenora_database_core::arrow::Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int32, false),
            spatial_field("shape", "geometry", "geometry"),
            spatial_field("position", "geography", "geography"),
        ],
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

#[test]
fn write_modes_and_required_key_shapes_are_explicit_before_io() {
    assert!(validate_operation(&operation(WriteMode::Append)).is_ok());
    assert!(validate_operation(&operation(WriteMode::TruncateInsert)).is_ok());
    assert!(validate_operation(&operation(WriteMode::Create)).is_ok());
    assert!(validate_operation(&operation(WriteMode::Replace)).is_ok());
    let mut staged_replace = operation(WriteMode::Replace);
    staged_replace.transaction_profile = TransactionProfile::StagedSwap;
    assert!(validate_operation(&staged_replace).is_ok());
    let mut staged_append = operation(WriteMode::Append);
    staged_append.transaction_profile = TransactionProfile::StagedSwap;
    assert!(validate_operation(&staged_append).is_err());
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
fn spatial_indexes_are_planned_only_for_atomic_create_or_replace() {
    let mut create = operation(WriteMode::Create);
    create.keys = vec!["id".to_owned()];
    create.create_spatial_index = true;
    let plan = WritePlan::compile_create(&create, spatial_create_schema()).expect("spatial create");
    assert_eq!(plan.spatial_indexes.len(), 2);
    assert_eq!(plan.spatial_indexes[0].kind, SpatialIndexKind::Geometry);
    assert_eq!(plan.spatial_indexes[1].kind, SpatialIndexKind::Geography);
    assert_eq!(plan.spatial_indexes[0].quoted_name, "[IX_pln_spatial_2]");
    assert_eq!(plan.spatial_indexes[0].quoted_column, "[shape]");
    assert_eq!(plan.spatial_indexes[1].quoted_name, "[IX_pln_spatial_3]");
    assert_eq!(plan.spatial_indexes[1].quoted_column, "[position]");

    let mut append = operation(WriteMode::Append);
    append.create_spatial_index = true;
    assert_eq!(
        validate_operation(&append)
            .expect_err("index on append")
            .category,
        ErrorCategory::Unsupported
    );
    let mut no_key = operation(WriteMode::Create);
    no_key.create_spatial_index = true;
    assert_eq!(
        validate_operation(&no_key)
            .expect_err("index without clustered key")
            .category,
        ErrorCategory::InvalidPlan
    );
    let mut no_spatial = operation(WriteMode::Create);
    no_spatial.keys = vec!["id".to_owned()];
    no_spatial.create_spatial_index = true;
    assert_eq!(
        WritePlan::compile_create(
            &no_spatial,
            Arc::new(plenora_database_core::arrow::Schema::new(vec![Field::new(
                "id",
                DataType::Int32,
                false,
            )])),
        )
        .expect_err("index without spatial columns")
        .category,
        ErrorCategory::InvalidPlan
    );
}

#[test]
fn only_canonical_spatial_indexes_are_representable_on_replace() {
    let columns = vec![SqlServerColumn {
        ordinal: 2,
        name: "shape".to_owned(),
        type_schema: "sys".to_owned(),
        native_type: "geometry".to_owned(),
        max_length: -1,
        precision: 0,
        scale: 0,
        nullable: true,
        identity: false,
        computed: false,
        generated_always_type: 0,
        collation: None,
        default_definition: None,
        computed_definition: None,
        computed_persisted: false,
    }];
    let canonical = SqlServerIndex {
        index_id: 2,
        name: Some("IX_pln_spatial_2".to_owned()),
        kind: "SPATIAL".to_owned(),
        unique: false,
        primary_key: false,
        unique_constraint: false,
        disabled: false,
        filtered: false,
        filter_definition: None,
        columns: Some("shape:0:0:0".to_owned()),
        spatial: Some(SqlServerSpatialIndex {
            spatial_type: "GEOMETRY".to_owned(),
            tessellation_scheme: "GEOMETRY_AUTO_GRID".to_owned(),
            bounding_box: Some(SqlServerSpatialBoundingBox {
                xmin: -1.0,
                ymin: -2.0,
                xmax: 3.0,
                ymax: 4.0,
            }),
        }),
    };
    assert!(is_canonical_spatial_index(&canonical, &columns));
    let mut custom = canonical;
    custom.name = Some("custom_index".to_owned());
    assert!(!is_canonical_spatial_index(&custom, &columns));
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
    let mut plan = WritePlan {
        input_schema: Arc::new(plenora_database_core::arrow::Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )])),
        columns: vec![WriteColumnPlan {
            input_index: 0,
            name: "id".to_owned(),
            kind: SqlServerColumnKind::I32,
            native_type: "int".to_owned(),
            native_declaration: "int".to_owned(),
            nullable: false,
            collation: None,
            spatial_srid: None,
        }],
        mode: WriteMode::Append,
        row_sql: String::new(),
        key_input_indices: Vec::new(),
        bulk_table: "[dbo].[target]".to_owned(),
        bulk_columns_aligned: false,
        lifecycle: TargetLifecycle::Existing {
            lock_sql: String::new(),
            truncate_sql: None,
            add_columns_sql: Vec::new(),
            schema_fingerprint: "fingerprint".to_owned(),
        },
        schema: "dbo".to_owned(),
        object: "target".to_owned(),
        added_columns: Vec::new(),
        spatial_indexes: Vec::new(),
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
    plan.columns[0].kind = SqlServerColumnKind::Time;
    plan.columns[0].native_type = "time".to_owned();
    validate_bulk_profile(&plan).expect("native time bulk");
    plan.columns[0].kind = SqlServerColumnKind::Timestamp;
    plan.columns[0].native_type = "datetime2".to_owned();
    validate_bulk_profile(&plan).expect("native datetime2 bulk");
    plan.columns[0].kind = SqlServerColumnKind::TimestampTz;
    plan.columns[0].native_type = "datetimeoffset".to_owned();
    validate_bulk_profile(&plan).expect("native datetimeoffset bulk");
    plan.columns[0].kind = SqlServerColumnKind::Utf8;
    plan.columns[0].native_type = "uniqueidentifier".to_owned();
    validate_bulk_profile(&plan).expect("native UUID bulk");
    plan.columns[0].native_type = "xml".to_owned();
    assert!(validate_bulk_profile(&plan).is_err());
    plan.columns[0].kind = SqlServerColumnKind::Timestamp;
    plan.columns[0].native_type = "datetime".to_owned();
    assert!(validate_bulk_profile(&plan).is_err());
    plan.columns[0].kind = SqlServerColumnKind::Decimal {
        precision: 19,
        scale: 4,
    };
    plan.columns[0].native_type = "money".to_owned();
    assert!(validate_bulk_profile(&plan).is_err());
}

#[test]
fn create_plan_compiles_quoted_atomic_ddl_and_primary_key() {
    let mut text_metadata = HashMap::new();
    text_metadata.insert(
        protocol::SQLSERVER_COLLATION.to_owned(),
        "Latin1_General_100_BIN2".to_owned(),
    );
    let schema = Arc::new(plenora_database_core::arrow::Schema::new(vec![
        Field::new("asset id", DataType::Int32, false),
        Field::new("label", DataType::Utf8, true).with_metadata(text_metadata),
    ]));
    let mut create = operation(WriteMode::Create);
    create.target.object = "asset registry".to_owned();
    create.keys = vec!["asset id".to_owned()];
    let plan = WritePlan::compile_create(&create, schema).expect("create plan");
    let TargetLifecycle::Create { create_sql } = plan.lifecycle else {
        panic!("create lifecycle")
    };
    assert!(create_sql.starts_with("CREATE TABLE [dbo].[asset registry]"));
    assert!(create_sql.contains("[asset id] int NOT NULL"));
    assert!(create_sql.contains("[label] nvarchar(max) COLLATE Latin1_General_100_BIN2 NULL"));
    assert!(create_sql.contains("PRIMARY KEY ([asset id])"));
    assert!(plan.row_sql.contains("INSERT INTO [dbo].[asset registry]"));
}

#[test]
fn create_plan_rejects_nullable_or_missing_primary_keys() {
    let nullable = Arc::new(plenora_database_core::arrow::Schema::new(vec![Field::new(
        "id",
        DataType::Int32,
        true,
    )]));
    let mut create = operation(WriteMode::Create);
    create.keys = vec!["id".to_owned()];
    assert!(WritePlan::compile_create(&create, nullable).is_err());

    let schema = Arc::new(plenora_database_core::arrow::Schema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )]));
    create.keys = vec!["missing".to_owned()];
    assert!(WritePlan::compile_create(&create, schema).is_err());

    let mut malicious_metadata = HashMap::new();
    malicious_metadata.insert(
        protocol::SQLSERVER_COLLATION.to_owned(),
        "Latin1_General_100_BIN2;DROP_TABLE".to_owned(),
    );
    let malicious = Arc::new(plenora_database_core::arrow::Schema::new(vec![Field::new(
        "id",
        DataType::Int32,
        false,
    )
    .with_metadata(malicious_metadata)]));
    create.keys = vec!["id".to_owned()];
    assert!(WritePlan::compile_create(&create, malicious).is_err());
}

#[test]
fn additive_schema_evolution_is_explicit_nullable_and_transaction_planned() {
    let description = SqlServerObjectDescription {
        database_id: 1,
        object_id: 2,
        catalog: "db".to_owned(),
        schema: "dbo".to_owned(),
        name: "target".to_owned(),
        kind: "USER_TABLE".to_owned(),
        temporal_type: 0,
        temporal: None,
        graph_kind: None,
        external: None,
        partitioning: None,
        owner: "dbo".to_owned(),
        security_predicates: Vec::new(),
        permissions: Vec::new(),
        view: None,
        memory_optimized: false,
        durability: None,
        columns: vec![SqlServerColumn {
            ordinal: 1,
            name: "id".to_owned(),
            type_schema: "sys".to_owned(),
            native_type: "int".to_owned(),
            max_length: 4,
            precision: 10,
            scale: 0,
            nullable: false,
            identity: false,
            computed: false,
            generated_always_type: 0,
            collation: None,
            default_definition: None,
            computed_definition: None,
            computed_persisted: false,
        }],
        constraints: Vec::new(),
        indexes: Vec::new(),
        token: SqlServerSchemaToken {
            schema_version: 1,
            database_id: 1,
            object_id: 2,
            structural_fingerprint: "fingerprint".to_owned(),
        },
    };
    let schema = Arc::new(plenora_database_core::arrow::Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("note", DataType::Utf8, true),
    ]));
    let append = operation(WriteMode::Append);
    assert!(WritePlan::compile_existing(
        &description,
        &append,
        Arc::clone(&schema),
        &HashMap::new(),
        SqlServerSchemaEvolution::Disabled,
    )
    .is_err());
    let plan = WritePlan::compile_existing(
        &description,
        &append,
        Arc::clone(&schema),
        &HashMap::new(),
        SqlServerSchemaEvolution::AddNullableColumns,
    )
    .expect("add nullable column");
    let TargetLifecycle::Existing {
        add_columns_sql, ..
    } = &plan.lifecycle
    else {
        panic!("existing lifecycle");
    };
    assert_eq!(
        add_columns_sql,
        &["ALTER TABLE [dbo].[target] ADD [note] nvarchar(max) NULL;"]
    );
    let report = plan
        .loss_report(MappingPolicy::Strict)
        .expect("loss report");
    assert_eq!(report.losses.len(), 1);
    assert_eq!(report.losses[0].severity, LossSeverity::Information);

    let non_nullable = Arc::new(plenora_database_core::arrow::Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("required", DataType::Utf8, false),
    ]));
    assert!(WritePlan::compile_existing(
        &description,
        &append,
        non_nullable,
        &HashMap::new(),
        SqlServerSchemaEvolution::AddNullableColumns,
    )
    .is_err());
}

#[test]
fn replace_preserves_composite_primary_key_order() {
    let column = |ordinal, name: &str| SqlServerColumn {
        ordinal,
        name: name.to_owned(),
        type_schema: "sys".to_owned(),
        native_type: "int".to_owned(),
        max_length: 4,
        precision: 10,
        scale: 0,
        nullable: false,
        identity: false,
        computed: false,
        generated_always_type: 0,
        collation: None,
        default_definition: None,
        computed_definition: None,
        computed_persisted: false,
    };
    let description = SqlServerObjectDescription {
        database_id: 1,
        object_id: 2,
        catalog: "db".to_owned(),
        schema: "dbo".to_owned(),
        name: "target".to_owned(),
        kind: "USER_TABLE".to_owned(),
        temporal_type: 0,
        temporal: None,
        graph_kind: None,
        external: None,
        partitioning: None,
        owner: "dbo".to_owned(),
        security_predicates: Vec::new(),
        permissions: Vec::new(),
        view: None,
        memory_optimized: false,
        durability: None,
        columns: vec![column(1, "tenant_id"), column(2, "asset_id")],
        constraints: vec![SqlServerConstraint {
            name: "PK_target".to_owned(),
            kind: "PRIMARY_KEY_CONSTRAINT".to_owned(),
            definition: None,
            columns: Some("tenant_id,asset_id".to_owned()),
            referenced_object: None,
            disabled: false,
            not_trusted: false,
        }],
        indexes: vec![SqlServerIndex {
            index_id: 1,
            name: Some("PK_target".to_owned()),
            kind: "CLUSTERED".to_owned(),
            unique: true,
            primary_key: true,
            unique_constraint: false,
            disabled: false,
            filtered: false,
            filter_definition: None,
            columns: Some("tenant_id:1:0:0,asset_id:2:0:0".to_owned()),
            spatial: None,
        }],
        token: SqlServerSchemaToken {
            schema_version: 1,
            database_id: 1,
            object_id: 2,
            structural_fingerprint: "fingerprint".to_owned(),
        },
    };
    let mut replace = operation(WriteMode::Replace);
    replace.keys = vec!["tenant_id".to_owned(), "asset_id".to_owned()];
    validate_replace_description(&description, &replace).expect("same PK order");
    replace.keys.reverse();
    let error = validate_replace_description(&description, &replace).expect_err("reordered PK");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}
