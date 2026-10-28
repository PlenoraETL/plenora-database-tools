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
            let reject = matches!(facts.data_type.as_str(), "BLOB" | "CLOB" | "NCLOB" | "JSON")
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
