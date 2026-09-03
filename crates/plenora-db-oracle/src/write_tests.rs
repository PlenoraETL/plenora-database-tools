use super::*;
use crate::types::{OracleColumnKind, OracleColumnSpec};
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::protocol::contract_schema;

fn spatial_schema() -> SchemaRef {
    contract_schema(vec![
        Field::new("ID", DataType::Int32, false),
        OracleColumnSpec {
            name: "shape".to_owned(),
            native_type: "SDO_GEOMETRY".to_owned(),
            nullable: false,
            kind: OracleColumnKind::Geometry,
            spatial_srid: Some(4326),
            spatial_dimensions: Some(2),
            spatial_semantics: Some(plenora_database_core::geometry::SpatialSemantics::Geometry),
        }
        .arrow_field(),
    ])
}

#[test]
fn spatial_create_rejects_identifiers_oracle_will_normalize() {
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: "lowercase_table".to_owned(),
        },
        mode: WriteMode::Create,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::BestEffortDdl,
        keys: vec!["ID".to_owned()],
        update_columns: Vec::new(),
        srid_policy: Some(SridPolicy::RequireMatch),
        create_spatial_index: true,
        allow_partial: false,
    };
    let error = OracleWritePlan::compile(
        &OracleConfig::new("oracle", "FREEPDB1", "plenora"),
        &spatial_schema(),
        &operation,
    )
    .expect_err("identificatori Spatial Oracle non canonici");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn create_setup_failure_is_always_partial_and_requires_recovery() {
    let error = shape_create_setup_error(write_error(
        ErrorCategory::Execution,
        "setup create Oracle fallito",
    ));
    assert_eq!(error.remote_effect, RemoteEffect::Partial);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
}
