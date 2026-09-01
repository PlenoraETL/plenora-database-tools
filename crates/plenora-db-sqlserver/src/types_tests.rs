use super::*;
use crate::SqlServerSchemaToken;
use plenora_database_core::plan::{ObjectRef, OrderBy, ProviderKind, ReadOperation, SortDirection};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::ReadCheckpoint;
use std::collections::HashMap;

fn description(native_type: &str, precision: u8, scale: u8) -> SqlServerObjectDescription {
    SqlServerObjectDescription {
        database_id: 1,
        object_id: 2,
        catalog: "db".to_owned(),
        schema: "dbo".to_owned(),
        name: "fixture".to_owned(),
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
            name: "value".to_owned(),
            type_schema: "sys".to_owned(),
            native_type: native_type.to_owned(),
            max_length: 8,
            precision,
            scale,
            nullable: true,
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
            structural_fingerprint: "abc".to_owned(),
        },
    }
}

/// `TOP` e `OFFSET` non convivono, e il dialetto lo impone.
///
/// SQL Server rifiuta `TOP` insieme a `OFFSET ... FETCH` nella stessa
/// espressione: e un errore di sintassi, non una preferenza. Il tetto
/// cambia percio forma a seconda che il piano chieda una finestra, e
/// questo test fissa le due — perche una sottostringa `OFFSET` sarebbe
/// verde anche sul SQL che il server rifiuta.
#[test]
fn the_ceiling_changes_shape_when_a_window_is_asked() {
    let target = description("int", 10, 0);
    let source = plenora_database_core::plan::ObjectRef {
        catalog: None,
        schema: Some("dbo".to_owned()),
        object: "fixture".to_owned(),
    };
    let base = ReadOperation {
        source,
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: Some(5),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let capped = SqlServerReadPlan::compile_operation(&target, &base).expect("solo tetto");
    assert!(
        capped.sql.contains("TOP (5)") && !capped.sql.contains("OFFSET"),
        "senza finestra il tetto e TOP: {}",
        capped.sql
    );

    let windowed = ReadOperation {
        row_offset: Some(20),
        ..base
    };
    let plan = SqlServerReadPlan::compile_operation(&target, &windowed).expect("finestra");
    assert!(
        !plan.sql.contains("TOP ("),
        "TOP non puo convivere con la finestra: {}",
        plan.sql
    );
    assert!(
        plan.sql.contains("OFFSET 20 ROWS FETCH NEXT 5 ROWS ONLY"),
        "il tetto deve viaggiare come FETCH NEXT: {}",
        plan.sql
    );
}

#[test]
fn qualified_checkpoint_renders_as_a_bound_sql_server_keyset() {
    let target = description("int", 10, 0);
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dbo".to_owned()),
            object: "fixture".to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "value".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(100),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let checkpoint = ReadCheckpoint::new(
        ProviderKind::Sqlserver,
        &operation,
        &ParameterBag::default(),
        vec![ParameterValue::I32(41)],
    )
    .expect("checkpoint");
    let (resumed, _) = checkpoint
        .resume(
            ProviderKind::Sqlserver,
            &operation,
            &ParameterBag::default(),
        )
        .expect("resume");
    let plan = SqlServerReadPlan::compile_operation(&target, &resumed).expect("piano ripreso");
    assert_eq!(
        plan.sql,
        "SELECT TOP (100) [value] AS [value] FROM [dbo].[fixture] WHERE [value] > @p1 ORDER BY [value] ASC;"
    );
    assert_eq!(plan.bind_names, ["__plenora_resume_0"]);
}

#[test]
fn decimal_projection_and_type_are_exact() {
    let plan = SqlServerReadPlan::compile(&description("decimal", 38, 12)).expect("plan");
    assert!(plan.sql.contains("CONVERT(varchar(50), [value])"));
    assert_eq!(
        plan.schema.field(0).data_type(),
        &DataType::Decimal128(38, 12)
    );
}

#[test]
fn unsupported_type_fails_before_io() {
    let error =
        SqlServerReadPlan::compile(&description("sql_variant", 0, 0)).expect_err("unsupported");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

#[test]
fn query_metadata_is_native_exact_and_spatial_remains_fail_closed() {
    let timestamp = SqlServerColumnSpec::from_query_metadata(
        "measured_at".to_owned(),
        "datetime2(7)".to_owned(),
        true,
        None,
    )
    .expect("datetime2");
    assert_eq!(timestamp.kind, SqlServerColumnKind::Timestamp);
    assert_eq!(timestamp.wire_encoding, SqlServerWireEncoding::Native);
    assert_eq!(timestamp.native_scale(), Some(7));
    assert!(timestamp.accepts_tds_column_type(tiberius::ColumnType::Datetime2));
    assert!(!timestamp.accepts_tds_column_type(tiberius::ColumnType::NVarchar));

    let error = SqlServerColumnSpec::from_query_metadata(
        "shape".to_owned(),
        "geometry".to_owned(),
        true,
        None,
    )
    .expect_err("spatial output");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn encoded_query_spatial_output_requires_and_applies_an_explicit_contract() {
    let binary = SqlServerColumnSpec::from_query_metadata(
        "start_point".to_owned(),
        "varbinary(max)".to_owned(),
        true,
        None,
    )
    .expect("encoded WKB metadata");
    let mut plan =
        SqlServerReadPlan::from_query_result("SELECT 1".to_owned(), Vec::new(), vec![binary])
            .expect("query plan");
    plan.apply_query_spatial_contract(
        0,
        SpatialSemantics::Geography,
        Some(4_326),
        Dimensions::Xyzm,
    )
    .expect("spatial contract");
    let field = plan.schema.field(0);
    assert_eq!(field.data_type(), &DataType::Binary);
    assert_eq!(
        field.metadata()[protocol::GEOMETRY_SPATIAL_SEMANTICS],
        "geography"
    );
    assert_eq!(field.metadata()[protocol::GEOMETRY_DIMENSIONS], "xyzm");
    assert_eq!(field.metadata()[protocol::GEOMETRY_SRID], "4326");
    assert_eq!(
        plan.columns[0].wire_encoding,
        SqlServerWireEncoding::Projected
    );
    assert!(plan.columns[0].accepts_tds_column_type(tiberius::ColumnType::BigVarBin));
}

#[test]
fn malformed_or_ambiguous_query_native_types_fail_closed() {
    for declaration in ["decimal", "decimal(39,0)", "decimal(10,11)", "dbo.custom"] {
        assert!(
            SqlServerColumnSpec::from_query_metadata(
                "value".to_owned(),
                declaration.to_owned(),
                true,
                None,
            )
            .is_err(),
            "{declaration}"
        );
    }
    assert!(SqlServerColumnSpec::from_query_metadata(
        "amount".to_owned(),
        "money".to_owned(),
        true,
        None,
    )
    .is_err());
}

#[test]
fn create_type_compilation_uses_safe_defaults_and_exact_metadata() {
    let default_text = Field::new("label", DataType::Utf8, false);
    let spec = SqlServerColumnSpec::from_create_field(&default_text).expect("text default");
    assert_eq!(spec.native_type, "nvarchar");
    assert_eq!(spec.native_declaration, "nvarchar(max)");

    let mut metadata = HashMap::new();
    metadata.insert(
        protocol::SQLSERVER_NATIVE_TYPE.to_owned(),
        "nvarchar".to_owned(),
    );
    metadata.insert(
        protocol::SQLSERVER_NATIVE_DECLARATION.to_owned(),
        "nvarchar(100)".to_owned(),
    );
    metadata.insert(
        protocol::SQLSERVER_COLLATION.to_owned(),
        "Latin1_General_100_BIN2".to_owned(),
    );
    let exact = Field::new("label", DataType::Utf8, false).with_metadata(metadata);
    let spec = SqlServerColumnSpec::from_create_field(&exact).expect("exact metadata");
    assert_eq!(spec.native_declaration, "nvarchar(100)");
    assert_eq!(spec.collation.as_deref(), Some("Latin1_General_100_BIN2"));
}

#[test]
fn create_type_compilation_rejects_sql_and_invalid_native_contracts() {
    for declaration in [
        "nvarchar(max));drop table dbo.asset;--",
        "nvarchar(4001)",
        "decimal(10,11)",
        "dbo.custom_type",
    ] {
        let mut metadata = HashMap::new();
        metadata.insert(
            protocol::SQLSERVER_NATIVE_DECLARATION.to_owned(),
            declaration.to_owned(),
        );
        let field = Field::new("value", DataType::Utf8, true).with_metadata(metadata);
        assert!(
            SqlServerColumnSpec::from_create_field(&field).is_err(),
            "{declaration}"
        );
    }

    let mut metadata = HashMap::new();
    metadata.insert(
        protocol::SQLSERVER_NATIVE_TYPE.to_owned(),
        "varchar".to_owned(),
    );
    metadata.insert(
        protocol::SQLSERVER_NATIVE_DECLARATION.to_owned(),
        "nvarchar(100)".to_owned(),
    );
    let mismatch = Field::new("value", DataType::Utf8, true).with_metadata(metadata);
    let error = SqlServerColumnSpec::from_create_field(&mismatch).expect_err("native mismatch");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}
