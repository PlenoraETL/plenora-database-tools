use super::*;
use crate::write_admission::ColumnFacts;
use crate::write_intermediate::TargetType;

// Inverso indipendente della notazione della matrice: n viene istanziato
// con lunghezze diverse per scoprire default e perdita dell'unita.
fn catalog(native: &str, length: u64) -> (ColumnFacts, String) {
    let declaration = native
        .split(';')
        .next()
        .unwrap()
        .replace('n', &length.to_string());
    let mut facts = ColumnFacts {
        name: "K".to_owned(),
        data_type: declaration.clone(),
        data_length: length,
        char_length: length,
        char_used: None,
        precision: None,
        scale: None,
        collation: None,
    };
    if let Some(args) = declaration
        .strip_prefix("NUMBER(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (p, s) = args.split_once(',').unwrap();
        facts.data_type = "NUMBER".to_owned();
        facts.precision = p.parse().ok();
        facts.scale = Some(s.parse().unwrap());
    } else if declaration == "FLOAT(126)" {
        facts.data_type = "FLOAT".to_owned();
        facts.precision = Some(126);
    } else if native.contains("(n") {
        facts.data_type = native.split('(').next().unwrap().to_owned();
        if facts.data_type != "RAW" {
            facts.char_used = Some(if native.contains("BYTE") { "B" } else { "C" }.to_owned());
            facts.collation = Some("BINARY".to_owned());
            if !native.contains("BYTE") {
                facts.data_length = length * 4;
            }
        }
    }
    (facts, declaration)
}

fn plan() -> OracleWritePlan {
    let mut result = OracleWritePlan {
        mode: WriteMode::Upsert,
        target: "T".to_owned(),
        schema_name: "S".to_owned(),
        object_name: "T".to_owned(),
        columns: vec![WriteColumn {
            name: "K".to_owned(),
            quoted: "K".to_owned(),
            data_type: DataType::Int32,
            nullable: false,
            spatial: None,
            float_digits: None,
            target_type: None,
        }],
        keys: vec![0],
        updates: vec![1],
        create_sql: None,
        create_spatial_index: false,
        rewrites: vec![],
    };
    let mut value = result.columns[0].clone();
    value.name = "V".to_owned();
    value.quoted = "V".to_owned();
    result.columns.push(value);
    result
}

#[test]
fn fixture_native_forms_cross_every_sql_path_have_identity_or_refusal() {
    let fixture: serde_json::Value =
        serde_json::from_slice(crate::write_admission::FIXTURE).unwrap();
    let forms = fixture["misure"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|m| m["chiavi"].as_array().unwrap())
        .map(|k| k["nativo"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    let mut accepted = 0;
    let mut refused = 0;
    for native in &forms {
        for length in [7, 193] {
            let (facts, expected) = catalog(native, length);
            let mut plan = plan();
            let descriptor = TargetType::from_catalog(&facts);
            let reject = matches!(facts.data_type.as_str(), "NCLOB" | "JSON")
                || facts.data_type.contains("LOCAL TIME ZONE");
            if reject {
                let error = descriptor.unwrap_err();
                assert_eq!(error.category, ErrorCategory::Unsupported);
                assert_eq!(error.phase, ErrorPhase::Prepare);
            } else {
                let descriptor = descriptor.unwrap_or_else(|e| panic!("{native}: {e:?}"));
                assert_eq!(descriptor.declaration, expected);
                assert_eq!(descriptor.collation, facts.collation);
                for column in &mut plan.columns {
                    column.target_type = Some(descriptor.clone());
                }
            }
            let row = [ParameterValue::I64(1), ParameterValue::I64(2)];
            // Array DML consumes these same statements, without rewriting SQL.
            for statement in [
                plan.insert_statement(&row),
                plan.upsert_statement(&row),
                plan.update_statement(&row),
                plan.delete_statement(&row),
                plan.insert_statement(&row),
            ] {
                if reject {
                    assert!(statement.is_err());
                    refused += 1;
                } else {
                    let statement = statement.unwrap();
                    if matches!(facts.data_type.as_str(), "BLOB" | "CLOB") {
                        assert!(!statement.sql.contains("CAST(:"));
                        accepted += 1;
                        continue;
                    }
                    for ordinal in 1..=statement.params.len() {
                        assert!(
                            statement
                                .sql
                                .contains(&format!("CAST(:{ordinal} AS {expected})")),
                            "{}",
                            statement.sql
                        );
                    }
                    assert!(
                        statement.sql.contains(&format!("CAST(:1 AS {expected})")),
                        "{}",
                        statement.sql
                    );
                    if facts.collation.is_some() {
                        assert!(statement.sql.contains("COLLATE BINARY"));
                    }
                    assert!(array_dml_compatible(&statement));
                    accepted += 1;
                }
            }
        }
    }
    assert_eq!(accepted + refused, forms.len() * 2 * 5);
    eprintln!(
        "Oracle: {} forms x 2 lengths x 5 paths = {} identical, {} refused",
        forms.len(),
        accepted,
        refused
    );
}

#[test]
fn missing_or_untrusted_catalog_never_falls_back_to_arrow() {
    let row = [ParameterValue::I64(1), ParameterValue::I64(2)];
    assert!(plan().insert_statement(&row).is_err());
    for collation in [None, Some("BINARY_CI"), Some("BINARY); payload")] {
        let (mut facts, _) = catalog("VARCHAR2(n BYTE);AL32UTF8", 12);
        facts.collation = collation.map(str::to_owned);
        let error = TargetType::from_catalog(&facts).unwrap_err();
        assert!(!error.message.contains("payload"));
    }
    let (mut facts, _) = catalog("VARCHAR2(n BYTE);AL32UTF8", 12);
    facts.char_used = None;
    assert!(TargetType::from_catalog(&facts).is_err());
    assert!(TargetType::from_catalog(&catalog("LONG RAW", 12).0).is_err());
}

#[test]
fn closed_pairs_keep_the_admission_diagnosis_before_intermediate_checks() {
    let mut plan = plan();
    plan.mode = WriteMode::Append;
    plan.keys.clear();
    plan.columns.truncate(1);
    plan.columns[0].data_type = DataType::Timestamp(
        plenora_database_core::arrow::schema::TimeUnit::Microsecond,
        Some("UTC".into()),
    );
    let server = crate::write_admission::ServerFacts {
        major: "23".to_owned(),
        version: "23.0.0.0.0".to_owned(),
        charset: "AL32UTF8".to_owned(),
        national_charset: "AL16UTF16".to_owned(),
        nls_comp: "BINARY".to_owned(),
    };
    let error = plan
        .admit(
            &[catalog("TIMESTAMP(0) WITH LOCAL TIME ZONE", 1).0],
            &server,
        )
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::None
    );
    assert_eq!(
        error.message,
        plenora_database_core::write_admission::REFUSED_PAIR
    );
    assert!(plan.columns[0].target_type.is_none());
    assert!(plan
        .insert_statement(&[ParameterValue::String("private".to_owned())])
        .is_err());
}

#[test]
fn measured_json_remains_refused_without_a_qualified_intermediate_type() {
    let mut plan = plan();
    plan.mode = WriteMode::Append;
    plan.keys.clear();
    plan.columns.truncate(1);
    plan.columns[0].data_type = DataType::Utf8;
    let server = crate::write_admission::ServerFacts {
        major: "23".to_owned(),
        version: "23.0.0.0.0".to_owned(),
        charset: "AL32UTF8".to_owned(),
        national_charset: "AL16UTF16".to_owned(),
        nls_comp: "BINARY".to_owned(),
    };
    let error = plan.admit(&[catalog("JSON", 1).0], &server).unwrap_err();
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::None
    );
    assert_eq!(
        error.message,
        plenora_database_core::write_admission::REFUSED_PAIR
    );
    assert!(plan.columns[0].target_type.is_none());
    assert!(plan
        .insert_statement(&[ParameterValue::String("private".to_owned())])
        .is_err());
}

#[test]
fn raw_and_spatial_bytes_choose_distinct_bind_representations() {
    let mut column = plan().columns.remove(0);
    let value = ParameterValue::Bytes(vec![1, 2, 3]);
    assert!(matches!(
        column.parameter(value.clone()).unwrap(),
        ParameterValue::Bytes(_)
    ));
    column.spatial = Some(SpatialWriteColumn {
        srid: 4326,
        dimensions: "xyz",
        semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
        exact_geometry_type: None,
    });
    assert!(matches!(
        column.parameter(value).unwrap(),
        ParameterValue::Wkb {
            srid: Some(4326),
            dimensions: plenora_database_core::geometry::Dimensions::Xyz,
            ..
        }
    ));
    assert_eq!(
        column.placeholder(1).unwrap(),
        "MDSYS.SDO_UTIL.FROM_WKBGEOMETRY(:1, 4326)"
    );
}

#[test]
fn lob_write_uses_typed_locators_and_reads_target_inside_transaction() {
    for native in ["BLOB", "CLOB"] {
        for mode in [
            WriteMode::Append,
            WriteMode::Create,
            WriteMode::Replace,
            WriteMode::Update,
            WriteMode::Upsert,
        ] {
            let mut plan = plan();
            plan.mode = mode;
            plan.columns[0].target_type =
                Some(TargetType::from_catalog(&catalog("NUMBER(10,0)", 7).0).unwrap());
            plan.columns[1].target_type =
                Some(TargetType::from_catalog(&catalog(native, 7).0).unwrap());
            let row = [
                ParameterValue::I64(1),
                ParameterValue::String(String::new()),
            ];
            let statement = match mode {
                WriteMode::Update => plan.update_statement(&row),
                WriteMode::Upsert => plan.upsert_statement(&row),
                _ => plan.insert_statement(&row),
            }
            .unwrap();
            let (statement, verification) = lob::verified_statement(&plan, &statement).unwrap();
            assert!(!statement.sql.contains("VARCHAR2"));
            assert!(verification.declarations.contains(native));
            assert!(verification.after.contains("SELECT V INTO plenora_lob_"));
            assert!(verification.after.contains("DBMS_LOB.GETLENGTH"));
            assert!(verification.after.contains("DBMS_LOB.COMPARE"));
            assert!(verification.after.contains("IS NULL"));
            assert!(verification.after.contains("RAISE_APPLICATION_ERROR"));
            let index = usize::from(mode != WriteMode::Update);
            assert_eq!(
                verification.types[index],
                plan.columns[1].target_type.as_ref().unwrap().lob_type()
            );
            assert_eq!(
                statement.sql.contains("RETURNING ROWID"),
                mode != WriteMode::Upsert
            );
        }
    }
}

#[test]
fn production_profile_and_rewrite_guard_do_not_replace_cast_length_checks() {
    use plenora_database_core::arrow::array::StringArray;
    use plenora_database_core::protocol::contract_schema;
    let server = crate::write_admission::ServerFacts {
        major: "23".into(),
        version: "23.0.0.0.0".into(),
        charset: "AL32UTF8".into(),
        national_charset: "AL16UTF16".into(),
        nls_comp: "BINARY".into(),
    };
    for (native, length, value) in [
        ("CHAR(n BYTE);AL32UTF8", 1, "\u{e9}".to_owned()),
        ("CHAR(n BYTE);AL32UTF8", 1, "ab".to_owned()),
        ("CHAR(n CHAR);AL32UTF8", 1, "ab".to_owned()),
        ("VARCHAR2(n BYTE);AL32UTF8", 1, "\u{e9}".to_owned()),
        ("VARCHAR2(n CHAR);AL32UTF8", 1, "ab".to_owned()),
        ("NCHAR(n);AL16UTF16", 1, "\u{1f600}".to_owned()),
        ("CHAR(n BYTE);AL32UTF8", 2000, "\u{e9}".repeat(2000)),
        ("VARCHAR2(n CHAR);AL32UTF8", 4000, "\u{e9}".repeat(4000)),
        ("NCHAR(n);AL16UTF16", 1000, "\u{1f600}".repeat(1000)),
    ] {
        for mode in [
            WriteMode::Append,
            WriteMode::Replace,
            WriteMode::Update,
            WriteMode::Upsert,
            WriteMode::DeleteByKeys,
        ] {
            let schema = contract_schema(vec![Field::new("K", DataType::Utf8, false)]).unwrap();
            let operation = WriteOperation {
                target: plenora_database_core::plan::ObjectRef {
                    catalog: None,
                    schema: Some("S".into()),
                    object: "T".into(),
                },
                mode,
                mapping_policy: MappingPolicy::Strict,
                transaction_profile: TransactionProfile::SingleTransaction,
                keys: if matches!(
                    mode,
                    WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
                ) {
                    vec!["K".into()]
                } else {
                    vec![]
                },
                update_columns: vec![],
                srid_policy: None,
                create_spatial_index: false,
                allow_partial: false,
            };
            let mut plan = plan();
            plan.columns.truncate(1);
            plan.columns[0].data_type = DataType::Utf8;
            plan.mode = mode;
            plan.keys = if operation.keys.is_empty() {
                vec![]
            } else {
                vec![0]
            };
            let (mut facts, _) = catalog(native, length);
            // DATA_LENGTH is capped by STANDARD, even for CHAR semantics.
            facts.data_length = if native.starts_with("NCHAR") {
                length * 2
            } else if native.contains("BYTE") || native.starts_with("VARCHAR2") {
                length
            } else {
                length * 4
            };
            plan.admit(&[facts], &server).unwrap();
            let guard = RewriteGuard::new(
                ProviderKind::Oracle,
                &schema,
                &operation,
                plan.rewrites.clone(),
            )
            .unwrap();
            for rows in [1, 3] {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(StringArray::from(vec![value.as_str(); rows]))],
                )
                .unwrap();
                // This is the production A + S path, without the probe bypass.
                guard
                    .check(&batch)
                    .expect("A/S alone admit this oversized value");
                let error = batch_values(&batch, &plan)
                    .expect_err("must refuse before scalar or array execution");
                assert_eq!(error.category, ErrorCategory::DataMapping);
                assert_eq!(error.message, "testo oltre la lunghezza del target Oracle");
            }
        }
    }
}

#[test]
fn cast_length_guard_preserves_valid_boundaries_and_nulls() {
    for (native, value) in [
        ("CHAR(n BYTE);AL32UTF8", "a"),
        ("CHAR(n CHAR);AL32UTF8", "\u{e9}"),
        ("VARCHAR2(n CHAR);AL32UTF8", "\u{1f600}"),
        ("NCHAR(n);AL16UTF16", "\u{e9}"),
    ] {
        let (facts, _) = catalog(native, 1);
        let target = TargetType::from_catalog(&facts).unwrap();
        target
            .check_value(&ParameterValue::String(value.into()))
            .unwrap();
        target
            .check_value(&ParameterValue::Null {
                type_name: String::new(),
            })
            .unwrap();
    }
}

#[test]
fn scalar_sdk_admission_already_refuses_small_oracle_text_overflow() {
    use plenora_database_core::operand_admission::{
        operand_type, CatalogType, Charset, LengthUnit,
    };
    for (name, unit, value) in [
        ("CHAR", LengthUnit::Bytes, "\u{e9}"),
        ("CHAR", LengthUnit::Bytes, "ab"),
        ("VARCHAR2", LengthUnit::Bytes, "\u{e9}"),
        ("CHAR", LengthUnit::Characters, "ab"),
        ("VARCHAR2", LengthUnit::Characters, "ab"),
        ("NCHAR", LengthUnit::Utf16, "\u{1f600}"),
    ] {
        // The descriptor returned by operand_catalog::column_types.
        let catalog = CatalogType {
            name: name.into(),
            length: Some(1),
            length_unit: unit,
            charset: Charset::Unicode,
            ..CatalogType::default()
        };
        let operand = operand_type(ProviderKind::Oracle, &catalog);
        let error = plenora_database_core::admission_gate::described(
            ProviderKind::Oracle,
            &ParameterValue::String(value.into()),
            &operand,
        )
        .unwrap_err();
        assert_eq!(error.category, ErrorCategory::DataMapping);
    }
}

#[test]
fn production_profile_does_not_replace_raw_cast_length_checks() {
    use plenora_database_core::arrow::array::{ArrayRef, BinaryArray, LargeBinaryArray};
    use plenora_database_core::protocol::contract_schema;
    let server = crate::write_admission::ServerFacts {
        major: "23".into(),
        version: "23.0.0.0.0".into(),
        charset: "AL32UTF8".into(),
        national_charset: "AL16UTF16".into(),
        nls_comp: "BINARY".into(),
    };
    for data_type in [DataType::Binary, DataType::LargeBinary] {
        for length in [1, 10] {
            let value = vec![0xAB; usize::try_from(length).unwrap() + 1];
            for mode in [
                WriteMode::Append,
                WriteMode::Replace,
                WriteMode::Update,
                WriteMode::Upsert,
                WriteMode::DeleteByKeys,
            ] {
                let schema =
                    contract_schema(vec![Field::new("K", data_type.clone(), false)]).unwrap();
                let operation = WriteOperation {
                    target: plenora_database_core::plan::ObjectRef {
                        catalog: None,
                        schema: Some("S".into()),
                        object: "T".into(),
                    },
                    mode,
                    mapping_policy: MappingPolicy::Strict,
                    transaction_profile: TransactionProfile::SingleTransaction,
                    keys: if matches!(
                        mode,
                        WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
                    ) {
                        vec!["K".into()]
                    } else {
                        vec![]
                    },
                    update_columns: vec![],
                    srid_policy: None,
                    create_spatial_index: false,
                    allow_partial: false,
                };
                let mut plan = plan();
                plan.columns.truncate(1);
                plan.columns[0].data_type = data_type.clone();
                plan.mode = mode;
                plan.keys = if operation.keys.is_empty() {
                    vec![]
                } else {
                    vec![0]
                };
                let (mut facts, _) = catalog("RAW(n)", length);
                facts.char_length = 0;
                plan.admit(&[facts], &server).unwrap();
                let guard = RewriteGuard::new(
                    ProviderKind::Oracle,
                    &schema,
                    &operation,
                    plan.rewrites.clone(),
                )
                .unwrap();
                for rows in [1, 3] {
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![match data_type {
                            DataType::Binary => {
                                Arc::new(BinaryArray::from(vec![value.as_slice(); rows]))
                                    as ArrayRef
                            }
                            _ => Arc::new(LargeBinaryArray::from(vec![value.as_slice(); rows]))
                                as ArrayRef,
                        }],
                    )
                    .unwrap();
                    // This is the production A + S path, without the probe bypass.
                    guard
                        .check(&batch)
                        .expect("A/S alone admit this oversized value");
                    let error = batch_values(&batch, &plan)
                        .expect_err("must refuse before scalar or array execution");
                    assert_eq!(error.category, ErrorCategory::DataMapping);
                    assert_eq!(
                        error.message,
                        "binario oltre la lunghezza del target Oracle"
                    );
                }
            }
        }
    }
}

#[test]
fn raw_cast_guard_uses_only_data_length_and_preserves_nulls_and_lobs() {
    for length in [1, 10] {
        let (mut facts, _) = catalog("RAW(n)", length);
        facts.char_length = 0;
        let target = TargetType::from_catalog(&facts).unwrap();
        target
            .check_value(&ParameterValue::Bytes(vec![
                0xAB;
                usize::try_from(length).unwrap()
            ]))
            .unwrap();
        target
            .check_value(&ParameterValue::Null {
                type_name: String::new(),
            })
            .unwrap();
        assert!(target
            .check_value(&ParameterValue::Bytes(vec![
                0xAB;
                usize::try_from(length).unwrap()
                    + 1
            ]))
            .is_err());
    }
    let target = TargetType::from_catalog(&catalog("BLOB", 1).0).unwrap();
    target
        .check_value(&ParameterValue::Bytes(vec![0xAB; 11]))
        .unwrap();
}
