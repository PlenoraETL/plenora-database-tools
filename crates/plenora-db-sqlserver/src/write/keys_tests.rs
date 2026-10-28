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
fn fixture_matrix_preserves_key_tables_and_checks_bulk_admission() {
    let fixture: serde_json::Value =
        serde_json::from_slice(crate::write::admission::FIXTURE).unwrap();
    let forms = fixture["misure"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|m| m["chiavi"].as_array().unwrap())
        .map(|k| k["nativo"].as_str().unwrap())
        .collect::<std::collections::BTreeSet<_>>();
    let mut bulk_accepted = 0;
    let mut bulk_refused = 0;
    for native in &forms {
        let declaration = native
            .split(';')
            .next()
            .unwrap()
            .replace("(n)", "(137)")
            .replace("real(24)", "real");
        let family = declaration.split('(').next().unwrap();
        let collation = matches!(
            family,
            "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext"
        )
        .then_some(if native.contains("cp=65001") {
            "Latin1_General_100_CI_AS_SC_UTF8"
        } else {
            "SQL_Latin1_General_CP1_CI_AS"
        });
        let kind = match family {
            "bigint" => SqlServerColumnKind::I64,
            "int" => SqlServerColumnKind::I32,
            "smallint" => SqlServerColumnKind::I16,
            "tinyint" => SqlServerColumnKind::U8,
            "bit" => SqlServerColumnKind::Bool,
            "real" => SqlServerColumnKind::F32,
            "float" => SqlServerColumnKind::F64,
            "date" => SqlServerColumnKind::Date,
            "time" => SqlServerColumnKind::Time,
            "datetimeoffset" => SqlServerColumnKind::TimestampTz,
            "datetime" | "datetime2" | "smalldatetime" => SqlServerColumnKind::Timestamp,
            "binary" | "varbinary" | "image" => SqlServerColumnKind::Binary,
            "decimal" | "numeric" => {
                let (p, s) = declaration
                    .split_once('(')
                    .unwrap()
                    .1
                    .trim_end_matches(')')
                    .split_once(',')
                    .unwrap();
                SqlServerColumnKind::Decimal {
                    precision: p.trim().parse().unwrap(),
                    scale: s.trim().parse().unwrap(),
                }
            }
            "money" => SqlServerColumnKind::Decimal {
                precision: 19,
                scale: 4,
            },
            "smallmoney" => SqlServerColumnKind::Decimal {
                precision: 10,
                scale: 4,
            },
            "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" | "xml"
            | "uniqueidentifier" => SqlServerColumnKind::Utf8,
            _ => panic!("unmapped fixture type: {native}"),
        };
        let mut plan = plan(vec![column("k", kind, &declaration, collation)], vec![0]);
        let keys = create_sql(&plan);
        if matches!(family, "text" | "ntext" | "image") {
            assert_eq!(keys.unwrap_err().category, ErrorCategory::Unsupported);
        } else {
            let expected = collation.map_or_else(
                || declaration.clone(),
                |c| format!("{declaration} COLLATE {c}"),
            );
            assert!(keys.unwrap().contains(&format!("[k0] {expected} NULL")));
        }
        plan.mode = WriteMode::Append;
        plan.bulk_columns_aligned = true;
        match crate::write::plan::validate_bulk_profile(&plan) {
            Ok(()) => {
                assert_eq!(plan.columns[0].native_declaration, declaration);
                assert_eq!(plan.columns[0].collation.as_deref(), collation);
                bulk_accepted += 1;
            }
            Err(error) => {
                assert_eq!(error.category, ErrorCategory::Unsupported);
                assert_eq!(error.phase, ErrorPhase::Prepare);
                bulk_refused += 1;
            }
        }
    }
    eprintln!(
        "SQL Server: {} key-table cases; bulk {} admitted, {} refused",
        forms.len(),
        bulk_accepted,
        bulk_refused
    );
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
