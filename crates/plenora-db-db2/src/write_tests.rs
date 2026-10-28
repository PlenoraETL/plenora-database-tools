use crate::write::Db2WritePlan;
use crate::{Db2Config, Db2TlsMode};
use plenora_database_core::arrow::array::{ArrayRef, BinaryArray, Int32Array};
use plenora_database_core::arrow::schema::{DataType, Field};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    ObjectRef, SridPolicy, TransactionProfile, WriteMode, WriteOperation,
};
use plenora_database_core::protocol::{self, contract_schema};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::ErrorCategory;
use std::collections::HashMap;
use std::sync::Arc;

fn config() -> Db2Config {
    Db2Config::new("db2.example.test", "warehouse", "loader").with_tls_mode(Db2TlsMode::Disable)
}

fn schema() -> plenora_database_core::arrow::schema::SchemaRef {
    contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("VALUE", DataType::Utf8, true),
    ])
    .expect("schema")
}

fn operation(mode: WriteMode) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: Some("warehouse".to_owned()),
            schema: Some("APP".to_owned()),
            object: "TARGET".to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn spatial_schema(
    dimensions: &str,
    geometry_type: &str,
) -> plenora_database_core::arrow::schema::SchemaRef {
    contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        Field::new("SHAPE", DataType::Binary, true).with_metadata(HashMap::from([
            (
                protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                "geoarrow.wkb".to_owned(),
            ),
            (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
            (
                protocol::GEOMETRY_DIMENSIONS.to_owned(),
                dimensions.to_owned(),
            ),
            (
                protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                "exact".to_owned(),
            ),
            (
                protocol::GEOMETRY_TYPES.to_owned(),
                geometry_type.to_owned(),
            ),
            (protocol::GEOMETRY_SRID.to_owned(), "4326".to_owned()),
            (
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "declared_unresolved".to_owned(),
            ),
            // ARROW-VOCABULARY §4: un CRS dichiarato ha identita e ordine
            // degli assi.
            (protocol::GEOMETRY_CRS_ID.to_owned(), "DB2:4326".to_owned()),
            (
                protocol::GEOMETRY_AXIS_ORDER.to_owned(),
                "unknown".to_owned(),
            ),
            (
                protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                "geometry".to_owned(),
            ),
            (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
        ])),
    ])
    .expect("schema")
}

fn point_xy() -> Vec<u8> {
    let mut bytes = vec![1_u8];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&1_f64.to_le_bytes());
    bytes.extend_from_slice(&2_f64.to_le_bytes());
    bytes
}

#[test]
fn append_plan_accepts_the_qualified_scalar_subset() {
    let mut plan = Db2WritePlan::compile(&config(), &schema(), &operation(WriteMode::Append))
        .expect("piano append Db2");
    assert!(plan.array_insert_sql().is_err());
    plan.test_catalog();
    assert_eq!(
        plan.array_insert_sql().unwrap(),
        "INSERT INTO \"APP\".\"TARGET\" (\"ID\", \"VALUE\") VALUES (CAST(? AS INTEGER), CAST(? AS CLOB(193 OCTETS)))"
    );
    assert_eq!(
        plan.array_insert_sql().unwrap().matches('?').count(),
        2,
        "il numero di placeholder resta quello delle colonne, non delle righe"
    );
}

#[test]
fn key_based_modes_fail_before_the_network_without_keys() {
    for mode in [
        WriteMode::Update,
        WriteMode::Upsert,
        WriteMode::DeleteByKeys,
    ] {
        let error = Db2WritePlan::compile(&config(), &schema(), &operation(mode))
            .expect_err("modalita key-based senza chiavi");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
    }
}

#[test]
fn unsupported_write_profiles_and_mapping_policies_fail_closed() {
    let mut write = operation(WriteMode::Append);
    write.mapping_policy = MappingPolicy::Lossy;
    assert_eq!(
        Db2WritePlan::compile(&config(), &schema(), &write)
            .expect_err("mapping lossy")
            .category,
        ErrorCategory::Unsupported
    );

    write.mapping_policy = MappingPolicy::Strict;
    write.transaction_profile = TransactionProfile::ChunkCommitted;
    assert_eq!(
        Db2WritePlan::compile(&config(), &schema(), &write)
            .expect_err("profilo chunked")
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn cross_database_targets_are_rejected_before_the_driver() {
    let mut write = operation(WriteMode::Append);
    write.target.catalog = Some("another_database".to_owned());
    let error =
        Db2WritePlan::compile(&config(), &schema(), &write).expect_err("target cross-database");

    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn spatial_append_uses_integrated_constructor_and_declared_srid() {
    let schema = spatial_schema("xy", "point");
    let mut write = operation(WriteMode::Append);
    write.srid_policy = Some(SridPolicy::RequireMatch);
    let plan = Db2WritePlan::compile(&config(), &schema, &write).expect("piano spatial Db2");
    let mut plan = plan;
    plan.test_catalog();
    let statement = plan.insert_statement(&[vec![
        plenora_database_core::provider::ParameterValue::I32(1),
        plenora_database_core::provider::ParameterValue::Bytes(point_xy()),
    ]]);

    assert!(statement
        .unwrap()
        .sql
        .contains("ST_GEOMETRY(BLOB(HEXTORAW(?)), 4326)"));
}

#[test]
fn spatial_write_fails_closed_without_require_match() {
    let schema = spatial_schema("xy", "point");
    let error = Db2WritePlan::compile(&config(), &schema, &operation(WriteMode::Append))
        .expect_err("spatial senza RequireMatch");

    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn spatial_batch_is_checked_before_any_database_mutation() {
    let schema = spatial_schema("xy", "point");
    let mut write = operation(WriteMode::Append);
    write.srid_policy = Some(SridPolicy::RequireMatch);
    let plan = Db2WritePlan::compile(&config(), &schema, &write).expect("piano spatial Db2");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let valid = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![Some(point_xy().as_slice())])) as ArrayRef,
        ],
    )
    .expect("batch valido");
    plan.validate_spatial_batch(&valid, &budget)
        .expect("WKB conforme");

    let wrong_dimensions = spatial_schema("xyz", "point");
    let wrong_plan =
        Db2WritePlan::compile(&config(), &wrong_dimensions, &write).expect("piano XYZ");
    let wrong_batch = RecordBatch::try_new(wrong_dimensions, valid.columns().to_vec())
        .expect("batch non conforme al contratto");
    assert_eq!(
        wrong_plan
            .validate_spatial_batch(&wrong_batch, &budget)
            .expect_err("dimensioni diverse")
            .category,
        ErrorCategory::DataMapping
    );
}

/// `spatial_schema("xy", "point")` con i metadati di `SHAPE` cambiati.
fn spatial_schema_with(
    changes: &[(&str, Option<&str>)],
) -> plenora_database_core::arrow::schema::SchemaRef {
    let base = spatial_schema("xy", "point");
    let fields = base
        .fields()
        .iter()
        .map(|field| {
            if field.name() != "SHAPE" {
                return field.as_ref().clone();
            }
            let mut metadata = field.metadata().clone();
            for &(key, value) in changes {
                match value {
                    Some(value) => metadata.insert(key.to_owned(), value.to_owned()),
                    None => metadata.remove(key),
                };
            }
            field.as_ref().clone().with_metadata(metadata)
        })
        .collect::<Vec<_>>();
    Arc::new(
        plenora_database_core::arrow::schema::Schema::new_with_metadata(
            fields,
            base.metadata().clone(),
        ),
    )
}

/// GEO-001 e GEO-008 su Db2, in compilazione e quindi prima di ogni effetto:
/// un campo `lat_lon` e `unsupported` (Db2 memorizza la longitudine in `x`);
/// uno SRID derivato da `EPSG:<n>` e `crs`, perche gli id SRS di Db2 sono
/// locali all'installazione e la derivazione non si verifica. Fino al 24/10
/// entrambi passavano.
#[test]
fn spatial_plan_refuses_north_first_axes_and_derived_srids() {
    let mut write = operation(WriteMode::Append);
    write.srid_policy = Some(SridPolicy::RequireMatch);
    let lat_lon = spatial_schema_with(&[(protocol::GEOMETRY_AXIS_ORDER, Some("lat_lon"))]);
    assert_eq!(
        Db2WritePlan::compile(&config(), &lat_lon, &write)
            .expect_err("lat_lon")
            .category,
        ErrorCategory::Unsupported
    );
    let derived = spatial_schema_with(&[
        (protocol::GEOMETRY_SRID, None),
        (protocol::GEOMETRY_CRS_ID, Some("EPSG:4326")),
        (protocol::GEOMETRY_AXIS_ORDER, Some("lon_lat")),
    ]);
    let error = Db2WritePlan::compile(&config(), &derived, &write).expect_err("SRID derivato");
    assert_eq!(error.category, ErrorCategory::Crs);
    assert_eq!(
        error.provider,
        Some(plenora_database_core::plan::ProviderKind::Db2)
    );
}

#[test]
fn varchar2_compat_si_legge_o_la_regola_s_non_e_provata() {
    use crate::write::parse_varchar2_compat;
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::row::Row;
    let row = |value: ParameterValue| {
        Row::try_new(Arc::from(vec!["VALUE".to_owned()]), vec![value]).expect("riga")
    };
    let text = |value: &str| row(ParameterValue::String(value.to_owned()));
    assert!(!parse_varchar2_compat(&[text("OFF")]).expect("OFF"));
    assert!(parse_varchar2_compat(&[text("ON ")]).expect("ON"));
    assert!(parse_varchar2_compat(&[text("OFF"), text("ON")]).expect("un membro ON"));
    for unproven in [
        Vec::new(),
        vec![text("")],
        vec![text("YES")],
        vec![row(ParameterValue::Null {
            type_name: "VARCHAR".to_owned(),
        })],
        vec![text("ON"), row(ParameterValue::I32(1))],
    ] {
        let error = parse_varchar2_compat(&unproven).expect_err("non provata");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }
}
