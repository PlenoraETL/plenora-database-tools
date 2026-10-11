use super::*;
use crate::types::{OracleColumnKind, OracleColumnSpec};
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::protocol::contract_schema;

#[test]
fn spatial_index_names_preserve_utf8_and_distinguish_long_identifiers() {
    let table = format!("{}é", "A".repeat(113));
    let name = spatial_index_name(&table, "SHAPE");
    assert!(name.len() <= 118);
    plenora_database_core::identifier::validate_identifier(
        plenora_database_core::identifier::IdentifierDialect::Oracle,
        &name,
    )
    .unwrap();
    let table = "A".repeat(114);
    assert_ne!(
        spatial_index_name(&table, "SHAPE_A"),
        spatial_index_name(&table, "SHAPE_B")
    );
    assert_eq!(spatial_index_name("PLACE", "SHAPE"), "PLN_PLACE_SHAPE_SIDX");
    assert_eq!(
        spatial_index_name(&table, "SHAPE_A"),
        spatial_index_name(&table, "SHAPE_A")
    );
}

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
        .arrow_field()
        .expect("campo Arrow geometry"),
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
fn a_known_setup_failure_after_the_create_is_partial_and_requires_recovery() {
    let error = shape_create_setup_error(write_error(
        ErrorCategory::Execution,
        "setup create Oracle fallito",
    ));
    assert_eq!(error.remote_effect, RemoteEffect::Partial);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
}

/// Un passo del setup con effetto ignoto (un comando interrotto in volo, il
/// COMMIT senza conferma) resta ignoto: non si appiattisce su `partial`.
#[test]
fn a_setup_failure_with_an_unknown_effect_stays_unknown() {
    let error = shape_create_setup_error(
        write_error(ErrorCategory::Timeout, "setup create Oracle interrotto")
            .after_interrupted_send(),
    );
    assert_eq!(error.remote_effect, RemoteEffect::Unknown);
    assert_eq!(error.retry, RetryDisposition::RequiresRecovery);
}

/// Il COMMIT del setup passa dalla regola unica: senza un rifiuto certo
/// l'esito e ignoto; ORA-02091 prova il rollback; un canale perso e `io`.
#[test]
fn the_setup_commit_follows_the_single_commit_rule() {
    let unknown = shape_create_setup_error(setup_commit_error(&oracle_rs::Error::Protocol(
        "x".to_owned(),
    )));
    assert_eq!(unknown.remote_effect, RemoteEffect::Unknown);
    assert_eq!(unknown.phase, ErrorPhase::Commit);
    let lost = setup_commit_error(&oracle_rs::Error::OracleError {
        code: 3_113,
        message: "canale".to_owned(),
    });
    assert_eq!(lost.category, ErrorCategory::Io);
    assert_eq!(lost.remote_effect, RemoteEffect::Unknown);
    let rejected = setup_commit_error(&oracle_rs::Error::OracleError {
        code: 2_091,
        message: "annullata".to_owned(),
    });
    assert_eq!(rejected.remote_effect, RemoteEffect::RolledBack);
}
