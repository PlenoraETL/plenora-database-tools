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
            rewrites: Vec::new(),
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
            rewrites: Vec::new(),
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
    assert!(compiled
        .sql
        .contains("SET [label] = CONVERT(nvarchar(100), @P2)"));
    assert!(compiled.sql.contains("WHERE [id] = CONVERT(int, @P1)"));

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
    assert!(delete.sql.contains("WHERE [id] = CONVERT(int, @P1)"));
}

/// Upsert lascia un'azione per ogni riga: con le sole chiavi, una chiave gia
/// presente lascia 0, cosi una riga senza azione resta soltanto l'INSERT che
/// il server ha scartato (`IGNORE_DUP_KEY`), e `row_mutation` la rifiuta.
#[test]
fn upsert_lascia_un_azione_per_ogni_riga() {
    let renderer = renderer();
    let keys_only = compile_row_statement(
        &operation(WriteMode::Upsert),
        &columns()[..1],
        &renderer,
        "[t]",
    )
    .expect("upsert sole chiavi");
    assert!(keys_only
        .sql
        .contains("END; ELSE INSERT INTO @plenora_actions ([action]) VALUES (0);"));
    let full = compile_row_statement(&operation(WriteMode::Upsert), &columns(), &renderer, "[t]")
        .expect("upsert");
    assert!(!full.sql.contains("VALUES (0)"));
    assert!(full.sql.contains("OUTPUT 2 INTO @plenora_actions"));
    assert!(full.sql.contains("OUTPUT 1 INTO @plenora_actions"));
}

/// Lo stesso parametro tipizzato serve a valori, predicati e registrazione chiavi.
#[test]
fn varchar_predicates_use_catalog_type_and_code_page() {
    let mut columns = columns();
    columns[0].kind = SqlServerColumnKind::Utf8;
    columns[0].native_type = "varchar".to_owned();
    columns[0].native_declaration = "varchar(10)".to_owned();
    columns[0].collation = Some("SQL_Latin1_General_CP1_CI_AS".to_owned());
    let expression = "CONVERT(varchar(10), @P1 COLLATE SQL_Latin1_General_CP1_CI_AS) COLLATE SQL_Latin1_General_CP1_CI_AS";
    for mode in [
        WriteMode::Append,
        WriteMode::Upsert,
        WriteMode::Update,
        WriteMode::DeleteByKeys,
    ] {
        let sql = compile_row_statement(&operation(mode), &columns, &renderer(), "[t]")
            .expect("SQL")
            .sql;
        assert!(sql.contains(expression), "{mode:?}: {sql}");
        assert!(!sql.contains("[id] = @P1"));
    }
    columns[0].collation = None;
    assert_eq!(
        placeholder_expression(&columns[0], 1)
            .expect_err("collation necessaria")
            .category,
        ErrorCategory::Unsupported
    );
}

/// Ogni forma misurata usa il tipo del target in tutti i DML preparati.
#[test]
fn prepared_paths_preserve_every_measured_catalog_type() {
    let fixture: serde_json::Value =
        serde_json::from_slice(super::super::admission::FIXTURE).expect("fixture");
    let natives = fixture["misure"]
        .as_array()
        .expect("misure")
        .iter()
        .flat_map(|measurement| measurement["chiavi"].as_array().expect("chiavi"))
        .map(|key| key["nativo"].as_str().expect("nativo"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(natives.len(), 1644);
    for native in natives {
        let declaration = native
            .split(';')
            .next()
            .expect("tipo")
            .replace("(n)", "(137)")
            .replace("real(24)", "real");
        let family = declaration.split('(').next().expect("famiglia");
        let mut columns = columns();
        let column = &mut columns[0];
        column.native_type = family.to_owned();
        column.native_declaration.clone_from(&declaration);
        column.kind = SqlServerColumnKind::Utf8;
        column.collation = if matches!(
            family,
            "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext"
        ) {
            Some(
                if native.contains("cp=65001") {
                    "Latin1_General_100_CI_AS_SC_UTF8"
                } else {
                    "SQL_Latin1_General_CP1_CI_AS"
                }
                .to_owned(),
            )
        } else {
            None
        };
        let collation = column.collation.clone();
        for mode in [
            WriteMode::Append,
            WriteMode::Upsert,
            WriteMode::Update,
            WriteMode::DeleteByKeys,
        ] {
            let result = compile_row_statement(&operation(mode), &columns, &renderer(), "[t]");
            if matches!(family, "text" | "ntext" | "image") {
                assert_eq!(
                    result.err().expect("tipo non convertibile").category,
                    ErrorCategory::Unsupported
                );
                continue;
            }
            let sql = result.expect("tipo esatto").sql;
            assert!(
                sql.contains(&format!("CONVERT({declaration}, @P1")),
                "{native}: {sql}"
            );
            if let Some(collation) = &collation {
                assert!(
                    sql.contains(&format!(") COLLATE {collation}")),
                    "{native}: {sql}"
                );
            }
        }
    }
}
