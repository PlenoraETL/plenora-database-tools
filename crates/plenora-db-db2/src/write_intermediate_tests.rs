use super::*;
use crate::write_admission::ColumnFacts;

fn catalog(native: &str, length: u64) -> (ColumnFacts, String) {
    let declaration = native
        .split(';')
        .next()
        .unwrap()
        .replace('n', &length.to_string());
    let name = declaration.split('(').next().unwrap();
    let mut facts = ColumnFacts {
        name: "K".into(),
        type_schema: "SYSIBM".into(),
        type_name: name.into(),
        length,
        scale: 0,
        codepage: 0,
        string_units: String::new(),
        string_units_length: 0,
        collation: String::new(),
    };
    if name == "DECIMAL" {
        let (p, s) = declaration[8..declaration.len() - 1]
            .split_once(',')
            .unwrap();
        facts.length = p.parse().unwrap();
        facts.scale = s.parse().unwrap();
    } else if name == "TIMESTAMP" {
        facts.scale = declaration[10..declaration.len() - 1].parse().unwrap();
    } else if native.contains(";cp=") {
        facts.codepage = 1208;
        facts.string_units = if native.contains("OCTETS") {
            "OCTETS"
        } else {
            "CODEUNITS32"
        }
        .into();
        facts.string_units_length = length;
        facts.length = if facts.string_units == "OCTETS" {
            length
        } else {
            length * 4
        };
        facts.collation = "SYSIBM.IDENTITY".into();
    }
    (facts, declaration)
}

impl Db2WritePlan {
    pub(crate) fn test_catalog(&mut self) {
        for column in &mut self.columns {
            let native = if column.data_type == DataType::Utf8 {
                "CLOB(n OCTETS);cp=1208"
            } else {
                "INTEGER"
            };
            column.target_type =
                Some(intermediate::TargetType::from_catalog(&catalog(native, 193).0).unwrap());
        }
    }
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
    let mut count = 0;
    for native in &forms {
        for length in [7, 193] {
            let (facts, expected) = catalog(native, length);
            let descriptor = intermediate::TargetType::from_catalog(&facts).unwrap();
            let column = WriteColumn {
                name: "K".into(),
                quoted: "K".into(),
                data_type: DataType::Int32,
                nullable: false,
                spatial: None,
                target_type: Some(descriptor),
            };
            let mut value = column.clone();
            value.name = "V".into();
            value.quoted = "V".into();
            let mut plan = Db2WritePlan {
                mode: WriteMode::Upsert,
                target: "T".into(),
                schema_name: "S".into(),
                object_name: "T".into(),
                columns: vec![column, value],
                keys: vec![0],
                updates: vec![1],
                create_sql: None,
                rewrites: vec![],
                varchar2_compat: Some(false),
                policy: MappingPolicy::Strict,
            };
            let row = vec![ParameterValue::I32(1), ParameterValue::I32(2)];
            let sql = [
                plan.insert_statement(std::slice::from_ref(&row))
                    .unwrap()
                    .sql,
                plan.upsert_statement(&row).unwrap().sql,
                plan.update_statement(&row).unwrap().sql,
                plan.delete_statement(&row).unwrap().sql,
                plan.array_insert_sql().unwrap(),
            ];
            for statement in sql {
                assert_eq!(
                    statement.matches('?').count(),
                    statement.matches(&format!("CAST(? AS {expected})")).count(),
                    "{statement}"
                );
                count += 1;
            }
            plan.columns[0].target_type = None;
            assert!(plan.insert_statement(std::slice::from_ref(&row)).is_err());
            assert!(plan.upsert_statement(&row).is_err());
            assert!(plan.update_statement(&row).is_err());
            assert!(plan.delete_statement(&row).is_err());
            assert!(plan.array_insert_sql().is_err());
        }
    }
    assert_eq!(count, forms.len() * 2 * 5);
    eprintln!("Db2: {} forms x 2 lengths x 5 paths = {count} identical; {count} missing descriptors refused", forms.len());
}

#[test]
fn unproven_catalog_is_refused_without_payload() {
    for change in 0..4 {
        let mut facts = catalog("VARCHAR(n OCTETS);cp=1208", 7).0;
        match change {
            0 => facts.collation.clear(),
            1 => facts.codepage = 1252,
            2 => facts.type_schema = "payload".into(),
            _ => facts.string_units.clear(),
        }
        let error = intermediate::TargetType::from_catalog(&facts).unwrap_err();
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert!(!error.message.contains("payload"));
    }
}
