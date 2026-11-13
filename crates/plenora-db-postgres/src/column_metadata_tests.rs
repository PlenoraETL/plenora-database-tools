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
            if fixture["name"] == "axis_lat_lon" && field.data_type() == &DataType::Binary {
                assert_eq!(planned.unwrap_err().category, ErrorCategory::Unsupported);
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
        omitted: Vec::new(),
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

#[test]
fn b07_audit_enumerates_the_original_60_cases_and_165_overlapping_extras() {
    use crate::geometry_properties::{validate_registry, SchemaPlan, PROPERTY_REGISTRY};
    let audit: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/postgres_b07_audit.json"
    ))
    .unwrap();
    let source: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/postgres_geometry_qualification.json"
    ))
    .unwrap();
    assert_eq!(
        audit["sha256"],
        "4db1549fea5a05822c1f60a5e354cbe5c69dca761e5a51fd42d4f5d45da6d2d0"
    );
    validate_registry(&PROPERTY_REGISTRY).unwrap();
    assert!(validate_registry(&PROPERTY_REGISTRY[1..]).is_err());
    let mut incomplete = PROPERTY_REGISTRY.to_vec();
    incomplete.last_mut().unwrap().metadata_rule = None;
    assert!(
        validate_registry(&incomplete).is_err(),
        "optional property without loss rule must fail compilation"
    );
    let cases = audit["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 60);
    let mut identities = std::collections::BTreeSet::new();
    let mut extras = 0;
    for case in cases {
        assert!(identities.insert(case["index"].as_u64().unwrap()));
        let fixture = source["fixtures"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| {
                f["name"] == case["axes"]["fixture"] && f["variant"] == case["axes"]["variante"]
            })
            .unwrap();
        let schema = Schema::new_with_metadata(
            fixture["fields"]
                .as_array()
                .unwrap()
                .iter()
                .map(fixture_field)
                .collect::<Vec<_>>(),
            serde_json::from_value::<std::collections::HashMap<String, String>>(
                fixture["schema"].clone(),
            )
            .unwrap(),
        );
        let plan = SchemaPlan::compile(&schema).unwrap();
        for extra in case["extra"].as_array().unwrap() {
            extras += 1;
            let property = PROPERTY_REGISTRY
                .iter()
                .find(|p| p.name == extra["property"].as_str().unwrap())
                .unwrap();
            for (field, saved) in schema.fields().iter().zip(&plan.columns) {
                let text = plan_registration(None, saved).unwrap().unwrap();
                let read = decode(Some(&text)).unwrap().unwrap();
                // These historical cases fit the comment: prove preservation,
                // rather than treating an empty report as evidence of it.
                assert!(read.omitted.is_empty());
                if property.key == "*" {
                    assert_eq!(
                        read.field,
                        field
                            .metadata()
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()
                    );
                    assert_eq!(
                        read.schema,
                        schema
                            .metadata()
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect()
                    );
                } else {
                    assert_eq!(
                        read.field.get(property.key),
                        field.metadata().get(property.key)
                    );
                    assert_eq!(
                        read.schema.get(property.key),
                        schema.metadata().get(property.key)
                    );
                }
            }
        }
    }
    assert_eq!(extras, 165);
}

#[test]
fn metadata_plan_limits_preserve_whole_values_and_localize_every_omission() {
    use crate::geometry_properties::SchemaPlan;
    use plenora_database_core::metadata_subject::{MetadataReason, MetadataScope};
    let field =
        Field::new("v", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            ("SECRET_KEY".to_owned(), "é".repeat(COLUMN_LIMIT)),
            ("small".to_owned(), "unchanged".to_owned()),
        ]));
    let schema = Schema::new(vec![field]);
    let plan = SchemaPlan::compile(&schema).unwrap();
    let saved = &plan.columns[0];
    assert_eq!(saved.field.get("small").unwrap(), "unchanged");
    assert!(!saved.field.contains_key("SECRET_KEY"));
    assert_eq!(saved.omitted.len(), 1);
    assert_eq!(saved.omitted[0].scope, MetadataScope::Field);
    assert_eq!(saved.omitted[0].reason, MetadataReason::ColumnLimit);
    assert_eq!(saved.omitted[0].key_sha256, digest(b"SECRET_KEY"));
    let rules = plan.rules().unwrap();
    let (_, risk) = plenora_database_core::loss_registry::compile_instances(
        plenora_database_core::plan::ProviderKind::Postgres,
        plenora_database_core::loss::MappingPolicy::Compatible,
        rules,
    )
    .unwrap();
    assert_eq!(risk.schema_version, 4);
    let json = serde_json::to_string(&risk).unwrap();
    assert!(!json.contains("SECRET_KEY"));
    assert!(risk.effective.is_empty());
    assert!(risk.evaluation.entries.is_empty());
    let text = plan_registration(Some("human"), saved).unwrap().unwrap();
    assert_eq!(decode(Some(&text)).unwrap().unwrap(), *saved);
    let mut read_back = saved.clone();
    read_back.omitted.clear();
    assert!(
        plan_registration(Some(&text), &read_back)
            .unwrap()
            .is_none(),
        "append with the schema just read preserves historical annotations"
    );
    let mut invalid_subject = saved.clone();
    invalid_subject.omitted[0].field_identity = Some("999".to_owned());
    let corrupt = plan_registration(None, &invalid_subject).unwrap().unwrap();
    assert!(
        decode(Some(&corrupt)).is_err(),
        "recomputed digest does not bypass semantic validation"
    );
}

#[test]
fn schema_metadata_replicas_share_one_aggregate_decision() {
    use crate::geometry_properties::SchemaPlan;
    use plenora_database_core::metadata_subject::{MetadataReason, MetadataScope};
    let schema = Schema::new_with_metadata(
        (0..20)
            .map(|i| Field::new(format!("v{i}"), DataType::Utf8, true))
            .collect::<Vec<_>>(),
        std::collections::HashMap::from([("large".to_owned(), "x".repeat(COLUMN_LIMIT - 1024))]),
    );
    let plan = SchemaPlan::compile(&schema).unwrap();
    assert_eq!(
        plan.rules().unwrap().len(),
        1,
        "one schema key, not twenty losses"
    );
    for column in &plan.columns {
        assert!(column.schema.is_empty());
        assert_eq!(column.omitted.len(), 1);
        assert_eq!(column.omitted[0].scope, MetadataScope::Schema);
        assert_eq!(column.omitted[0].reason, MetadataReason::OperationLimit);
    }
    assert!(
        plan.columns
            .iter()
            .map(|c| c.encode().unwrap().len())
            .sum::<usize>()
            <= OPERATION_LIMIT
    );
}

#[test]
fn metadata_decisions_have_one_production_owner() {
    for (name, source) in [
        ("writer executor", include_str!("write.rs")),
        ("writer", include_str!("write/plan.rs")),
        ("preflight", include_str!("preflight.rs")),
        ("reader", include_str!("read_stream.rs")),
        ("types", include_str!("types.rs")),
        ("registration", include_str!("field_identity.rs")),
        ("value report", include_str!("write/effective.rs")),
    ] {
        assert!(
            source.contains("geometry_properties::"),
            "{name} must consume the registry plan"
        );
        for forbidden in [
            "GEOMETRY_",
            "\"plenora.geometry.",
            "encoding.as_deref()",
            "dimensions.as_deref()",
        ] {
            assert!(
                !source.contains(forbidden),
                "property decision outside registry: {name}/{forbidden}"
            );
        }
    }
}

#[test]
fn comment_depth_limit_counts_structure_not_escaped_metadata_text() {
    for depth in [15, 16, 17] {
        let json = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
        assert_eq!(bounded_json(&json).is_ok(), depth <= 16);
    }
    let json = serde_json::to_string(&"[{\"\\".repeat(100)).unwrap();
    bounded_json(&json).unwrap();
}

#[test]
fn registration_plan_replicates_schema_history_to_added_columns_before_effects() {
    use crate::geometry_properties::SchemaPlan;
    let base = protocol::contract_schema(vec![Field::new("a", DataType::Utf8, true)]).unwrap();
    let mut metadata = base.metadata().clone();
    metadata.insert("opaque", "x".repeat(COLUMN_LIMIT));
    let original = Schema::new_with_metadata(base.fields().clone(), metadata);
    let old = SchemaPlan::compile(&original).unwrap();
    let comment = plan_registration(None, &old.columns[0]).unwrap().unwrap();
    let extended = protocol::contract_schema(vec![
        base.field(0).clone(),
        Field::new("b", DataType::Utf8, true),
    ])
    .unwrap();
    let plan = SchemaPlan::compile(&extended).unwrap();
    assert!(
        plan.rules().unwrap().is_empty(),
        "history is not a new write loss"
    );
    let registrations = plan
        .registrations(&extended, &[("a".to_owned(), Some(comment))])
        .unwrap();
    assert!(registrations[0].comment.is_none());
    let added = decode(registrations[1].comment.as_deref())
        .unwrap()
        .unwrap();
    assert_eq!(added.omitted, old.columns[0].omitted);
    // The same key can exceed a different limit on the next operation.
    let fields = std::iter::once(base.field(0).clone())
        .chain((0..24).map(|i| Field::new(format!("new{i}"), DataType::Utf8, true)))
        .collect();
    let expanded = protocol::contract_schema(fields).unwrap();
    let mut metadata = expanded.metadata().clone();
    metadata.insert("opaque", "x".repeat(COLUMN_LIMIT - 1024));
    let expanded = Schema::new_with_metadata(expanded.fields().clone(), metadata);
    let next = SchemaPlan::compile(&expanded).unwrap();
    assert_eq!(
        next.columns[0].omitted[0].reason,
        plenora_database_core::metadata_subject::MetadataReason::OperationLimit
    );
    let comment = plan_registration(None, &old.columns[0]).unwrap();
    let registrations = next
        .registrations(&expanded, &[("a".to_owned(), comment)])
        .unwrap();
    for registration in &registrations[1..] {
        let saved = decode(registration.comment.as_deref()).unwrap().unwrap();
        assert_eq!(saved.omitted, old.columns[0].omitted);
    }
}

#[test]
fn final_registration_limit_includes_catalog_history() {
    use crate::geometry_properties::SchemaPlan;
    use plenora_database_core::metadata_subject::{
        MetadataHop, MetadataReason, MetadataScope, MetadataSubject,
    };
    let base = protocol::contract_schema(
        (0..32)
            .map(|i| Field::new(format!("v{i}"), DataType::Utf8, true))
            .collect(),
    )
    .unwrap();
    let prior = SchemaPlan::compile(&base).unwrap();
    let history: Vec<_> = (0..3000)
        .map(|index| MetadataSubject {
            index,
            scope: MetadataScope::Schema,
            field_identity: None,
            property: "native_metadata".to_owned(),
            key_sha256: format!("{index:064x}"),
            hop: MetadataHop::ColumnComment,
            reason: MetadataReason::ColumnLimit,
        })
        .collect();
    let comments = base
        .fields()
        .iter()
        .zip(prior.columns)
        .map(|(field, mut saved)| {
            saved.omitted.clone_from(&history);
            assert!(saved.encode().unwrap().len() <= COLUMN_LIMIT);
            (
                field.name().clone(),
                plan_registration(None, &saved).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let mut metadata = base.metadata().clone();
    metadata.insert("new_opaque", "x".repeat(COLUMN_LIMIT));
    let incoming = Schema::new_with_metadata(base.fields().clone(), metadata);
    let plan = SchemaPlan::compile(&incoming).unwrap();
    assert!(
        plan.columns
            .iter()
            .map(|c| c.encode().unwrap().len())
            .sum::<usize>()
            < OPERATION_LIMIT
    );
    assert_eq!(
        plan.registrations(&incoming, &comments)
            .err()
            .unwrap()
            .category,
        ErrorCategory::ResourceLimit
    );
}
