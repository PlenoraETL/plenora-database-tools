use super::*;
use arrow_schema::DataType;
use plenora_database_core::protocol;
use serde_json::Value;

pub fn fixture_field(value: &Value) -> Field {
    let kind = match value["type"].as_str().unwrap() {
        "binary" => DataType::Binary,
        "int64" => DataType::Int64,
        "uint64" => DataType::UInt64,
        "string" => DataType::Utf8,
        other => panic!("fixture type {other}"),
    };
    Field::new(
        value["name"].as_str().unwrap(),
        kind,
        value["nullable"].as_bool().unwrap(),
    )
    .with_metadata(
        serde_json::from_value::<std::collections::HashMap<String, String>>(
            value["metadata"].clone(),
        )
        .unwrap(),
    )
}

#[test]
fn qualification_registry_and_every_property_fixture() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/postgres_geometry_qualification.json"
    ))
    .unwrap();
    let expected: Vec<&str> = source["properties"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p.as_str().unwrap())
        .collect();
    let names: Vec<_> = crate::geometry_properties::PROPERTY_REGISTRY
        .iter()
        .map(|p| p.name)
        .collect();
    assert_eq!(names, expected);
    assert_ne!(
        &names[1..],
        expected,
        "la rimozione di una voce deve fallire"
    );
    let fixtures = source["fixtures"].as_array().unwrap();
    assert_eq!(fixtures.len(), 14 * 2);
    for fixture in fixtures {
        let fields = fixture["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(fixture_field)
            .collect::<Vec<_>>();
        let schema = Schema::new_with_metadata(
            fields,
            serde_json::from_value::<std::collections::HashMap<String, String>>(
                fixture["schema"].clone(),
            )
            .unwrap(),
        );
        for field in schema.fields() {
            let planned = ColumnMetadata::plan(field, &schema);
            if fixture["name"] == "dimensions_unknown" && field.data_type() == &DataType::Binary {
                let error = planned.unwrap_err();
                assert_eq!(error.category, ErrorCategory::Crs);
                assert_eq!(error.phase, ErrorPhase::Validate);
                assert_eq!(
                    error.remote_effect,
                    plenora_database_core::RemoteEffect::None
                );
                continue;
            }
            // Conflitti vietati dal contratto non diventano round-trip validi.
            if fixture["name"] == "conflicting_crs" && planned.is_err() {
                continue;
            }
            let planned = planned.unwrap();
            let comment = plan_registration(Some("owner's \\\ncommento città"), &planned)
                .unwrap()
                .unwrap();
            let decoded = decode(Some(&comment)).unwrap().unwrap();
            for property in &crate::geometry_properties::PROPERTY_REGISTRY {
                assert_eq!(
                    decoded.field.get(property.key),
                    planned.field.get(property.key),
                    "{} / {}",
                    fixture["name"],
                    property.name
                );
                assert_eq!(
                    decoded.schema.get(property.key),
                    planned.schema.get(property.key)
                );
            }
            assert_eq!(
                decoded, planned,
                "metadata opachi e presenza sono significativi"
            );
            assert!(comment.ends_with("owner's \\\ncommento città"));
            assert!(plan_registration(Some(&comment), &planned)
                .unwrap()
                .is_none());
        }
    }
}

fn scalar() -> ColumnMetadata {
    ColumnMetadata {
        version: 1,
        physical: None,
        id: "42".to_owned(),
        data_type: "Utf8".to_owned(),
        field: BTreeMap::from([(protocol::FIELD_ID.to_owned(), "42".to_owned())]),
        schema: BTreeMap::from([(protocol::CONTRACT_VERSION_KEY.to_owned(), "1".to_owned())]),
    }
}

#[test]
fn codec_corruption_duplicates_versions_and_legacy_are_fail_closed() {
    let saved = scalar();
    let text = plan_registration(Some("human"), &saved).unwrap().unwrap();
    assert_eq!(
        crate::field_identity::registered_id(Some(&text))
            .unwrap()
            .as_deref(),
        Some("42")
    );
    assert!(decode(Some("plenora.field_id.v1:0042\nhuman"))
        .unwrap()
        .is_none());
    assert!(decode(Some("human")).unwrap().is_none());
    for corrupted in [
        text.replace("\"42\"", "\"43\""),
        text.replace("\"version\":1", "\"version\":2"),
        text.replace("\"version\":1", "\"version\":1,\"version\":1"),
        format!("{text}\nplenora.arrow.v1:{{}}\n"),
        text.replace("plenora.arrow.v1:", "plenora.arrow.v2:"),
        text.replace("plenora.field_id.v1:42", "plenora.field_id.v1:SECRET"),
    ] {
        let error = decode(Some(&corrupted)).unwrap_err();
        assert!(!error.message.contains("SECRET"));
    }
    let mut changed = saved;
    changed
        .field
        .insert("opaque".to_owned(), "different".to_owned());
    assert_eq!(
        plan_registration(Some(&text), &changed)
            .unwrap_err()
            .category,
        ErrorCategory::Conflict
    );
}

#[test]
fn utf8_limits_are_serialized_bytes_without_truncation() {
    let mut saved = scalar();
    saved.field.insert("opaque".to_owned(), String::new());
    let overhead = saved.encode().unwrap().len();
    for delta in [-1_i32, 0, 1] {
        let size =
            usize::try_from(i64::try_from(COLUMN_LIMIT - overhead).unwrap() + i64::from(delta))
                .unwrap();
        let value = format!("{}{}", "é".repeat(size / 2), "x".repeat(size % 2));
        saved.field.insert("opaque".to_owned(), value.clone());
        let encoded = saved.encode();
        if delta > 0 {
            assert_eq!(encoded.unwrap_err().category, ErrorCategory::ResourceLimit);
        } else {
            assert_eq!(encoded.unwrap().len(), overhead + value.len());
        }
    }
    let field = Field::new("v", DataType::Utf8, true).with_metadata(
        std::collections::HashMap::from([("opaque".to_owned(), "NUL\0SECRET".to_owned())]),
    );
    assert!(ColumnMetadata::plan(&field, &Schema::empty()).is_err());
}
