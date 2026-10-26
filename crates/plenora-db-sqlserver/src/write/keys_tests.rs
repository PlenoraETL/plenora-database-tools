use super::*;
use crate::write::plan::TargetLifecycle;
use plenora_database_core::arrow::schema::{DataType, Field, Schema};
use std::sync::Arc;

fn column(
    name: &str,
    kind: SqlServerColumnKind,
    declaration: &str,
    collation: Option<&str>,
) -> WriteColumnPlan {
    WriteColumnPlan {
        input_index: 0,
        name: name.to_owned(),
        kind,
        native_type: declaration
            .split_once('(')
            .map_or(declaration, |(native, _)| native)
            .to_owned(),
        native_declaration: declaration.to_owned(),
        nullable: false,
        collation: collation.map(ToOwned::to_owned),
        spatial_srid: None,
        rewrites: Vec::new(),
    }
}

fn plan(columns: Vec<WriteColumnPlan>, keys: Vec<usize>) -> WritePlan {
    WritePlan {
        input_schema: Arc::new(Schema::new(vec![Field::new("k", DataType::Utf8, false)])),
        columns,
        mode: WriteMode::Upsert,
        row_sql: String::new(),
        key_input_indices: keys,
        bulk_table: String::new(),
        bulk_columns_aligned: false,
        lifecycle: TargetLifecycle::Create {
            create_sql: String::new(),
        },
        schema: "dbo".to_owned(),
        object: "t".to_owned(),
        added_columns: Vec::new(),
        spatial_indexes: Vec::new(),
        strict: true,
        precision_losses: Vec::new(),
    }
}

#[test]
fn la_tabella_delle_chiavi_ha_il_tipo_e_la_collation_del_target() {
    let plan = plan(
        vec![
            column(
                "k",
                SqlServerColumnKind::Utf8,
                "nvarchar(10)",
                Some("SQL_Latin1_General_CP1_CI_AS"),
            ),
            column("v", SqlServerColumnKind::I32, "int", None),
            column(
                "o",
                SqlServerColumnKind::TimestampTz,
                "datetimeoffset(7)",
                None,
            ),
        ],
        vec![0, 2],
    );
    assert_eq!(
        create_sql(&plan).expect("sql"),
        "DROP TABLE IF EXISTS #plenora_keys; CREATE TABLE #plenora_keys ([k0] nvarchar(10) \
         COLLATE SQL_Latin1_General_CP1_CI_AS NULL, [k1] datetimeoffset(7) NULL); \
         CREATE UNIQUE INDEX [plenora_keys_unique] ON #plenora_keys ([k0], [k1]);"
    );
}

#[test]
fn una_collation_fuori_dalla_grammatica_si_rifiuta() {
    let plan = plan(
        vec![column(
            "k",
            SqlServerColumnKind::Utf8,
            "nvarchar(10)",
            Some("Latin1; DROP"),
        )],
        vec![0],
    );
    assert!(create_sql(&plan).is_err());
}

#[test]
fn una_violazione_dell_indice_e_una_chiave_ripetuta() {
    let conflict = DatabaseError::new(
        ErrorCategory::Conflict,
        ErrorPhase::Write,
        Some(ProviderKind::Sqlserver),
        "vincolo SQL Server violato (codice 2601)",
    );
    let mapped = duplicate(conflict);
    assert_eq!(mapped.category, ErrorCategory::DataMapping);
    assert!(mapped.message.contains(DUPLICATE_INPUT_KEYS));
    let other = DatabaseError::new(
        ErrorCategory::Execution,
        ErrorPhase::Write,
        Some(ProviderKind::Sqlserver),
        "altro",
    );
    assert_eq!(duplicate(other).category, ErrorCategory::Execution);
    assert!(applies(WriteMode::Upsert));
    assert!(applies(WriteMode::Update));
    assert!(applies(WriteMode::DeleteByKeys));
    assert!(!applies(WriteMode::Append));
}

#[test]
fn una_chiave_testuale_senza_collation_non_eredita_quella_di_tempdb() {
    for native in ["char(10)", "varchar(10)", "nchar(10)", "nvarchar(10)"] {
        let plan = plan(
            vec![column("k", SqlServerColumnKind::Utf8, native, None)],
            vec![0],
        );
        let error = create_sql(&plan).expect_err("collation non provata");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }
}
