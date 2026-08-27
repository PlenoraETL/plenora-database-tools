use super::*;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ObjectRef, SridPolicy, TransactionProfile, WriteOperation};
use plenora_database_sql::{Dialect, DialectCapabilities};

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
        keys: vec!["id".to_owned()],
        update_columns: Vec::new(),
        srid_policy: Some(SridPolicy::RequireMatch),
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn columns() -> Vec<WriteColumnPlan> {
    vec![
        WriteColumnPlan {
            input_index: 0,
            name: "id".to_owned(),
            kind: SqlServerColumnKind::I32,
            native_type: "int".to_owned(),
            native_declaration: "int".to_owned(),
            nullable: false,
            collation: None,
            spatial_srid: None,
        },
        WriteColumnPlan {
            input_index: 1,
            name: "label".to_owned(),
            kind: SqlServerColumnKind::Utf8,
            native_type: "nvarchar".to_owned(),
            native_declaration: "nvarchar(100)".to_owned(),
            nullable: false,
            collation: None,
            spatial_srid: None,
        },
    ]
}

fn renderer() -> Renderer {
    Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
}

#[test]
fn keyed_statements_are_bound_and_never_use_merge() {
    let renderer = renderer();
    let mut update = operation(WriteMode::Update);
    update.update_columns = vec!["label".to_owned()];
    let compiled = compile_row_statement(&update, &columns(), &renderer, "[dbo].[target]")
        .expect("update SQL");
    assert_eq!(compiled.key_input_indices, vec![0]);
    assert!(compiled.sql.contains("SET [label] = @P2"));
    assert!(compiled.sql.contains("WHERE [id] = @P1"));

    let upsert = compile_row_statement(&operation(WriteMode::Upsert), &columns(), &renderer, "[t]")
        .expect("upsert SQL");
    assert!(upsert.sql.contains("UPDLOCK, HOLDLOCK"));
    assert!(upsert.sql.contains("IF @@ROWCOUNT = 0"));
    assert!(!upsert.sql.to_uppercase().contains("MERGE"));

    let delete = compile_row_statement(
        &operation(WriteMode::DeleteByKeys),
        &columns()[..1],
        &renderer,
        "[t]",
    )
    .expect("delete SQL");
    assert!(delete.sql.contains("OUTPUT 3"));
    assert!(delete.sql.contains("WHERE [id] = @P1"));
}
