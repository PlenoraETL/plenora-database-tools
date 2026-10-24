use super::*;
use std::collections::HashMap;

fn spatial_field(extra: &[(&str, &str)]) -> Field {
    let mut metadata = HashMap::from([
        (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
        (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
        (
            protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
            "exact".to_owned(),
        ),
        (protocol::GEOMETRY_TYPES.to_owned(), "point".to_owned()),
        (
            protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
            "missing".to_owned(),
        ),
        (
            protocol::GEOARROW_EXTENSION_NAME.to_owned(),
            GEOARROW_WKB_EXTENSION_NAME.to_owned(),
        ),
        (protocol::FIELD_ID.to_owned(), "0".to_owned()),
        (
            protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
            "geometry".to_owned(),
        ),
        (
            protocol::GEOMETRY_PRECISION.to_owned(),
            "float64".to_owned(),
        ),
    ]);
    metadata.extend(
        extra
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned())),
    );
    Field::new("geometry", DataType::Binary, true).with_metadata(metadata)
}

fn current_schema(field: Field) -> Schema {
    Schema::new_with_metadata(
        vec![field],
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    )
}

#[test]
fn current_contract_requires_schema_version() {
    let schema = Schema::new(vec![spatial_field(&[])]);
    let error = validate_schema_contract(&schema).expect_err("versione assente");
    // REJ-001: un difetto dei metadati dello schema, non di un valore.
    assert_eq!(error.category, ErrorCategory::Schema);
    assert!(validate_schema_contract(&current_schema(spatial_field(&[]))).is_ok());
}

#[test]
fn malformed_contract_version_is_a_schema_defect() {
    for malformata in ["", "01", "0", "1.0", "v1", " 1", "-1"] {
        let mut schema = current_schema(spatial_field(&[]));
        schema.metadata.insert(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            malformata.to_owned(),
        );
        let error = validate_schema_contract(&schema).expect_err("versione malformata");
        assert_eq!(error.category, ErrorCategory::Schema, "{malformata:?}");
    }
}

#[test]
fn future_contract_version_fails_closed() {
    let mut schema = current_schema(spatial_field(&[]));
    schema
        .metadata
        .insert(protocol::CONTRACT_VERSION_KEY.to_owned(), "2".to_owned());
    let error = validate_schema_contract(&schema).expect_err("future version");
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[test]
fn conflicting_crs_representations_are_rejected() {
    let field = spatial_field(&[
        (protocol::GEOMETRY_CRS_RESOLUTION, "resolved"),
        (protocol::GEOMETRY_CRS_ID, "EPSG:4326"),
        (protocol::GEOMETRY_SRID, "3003"),
        (protocol::GEOMETRY_AXIS_ORDER, "lat_lon"),
    ]);
    let error = FieldContract::parse(&field).expect_err("conflicting CRS");
    assert_eq!(error.category, ErrorCategory::Crs);
}

#[test]
fn geometry_type_order_and_unresolved_state_are_closed() {
    let reversed = spatial_field(&[(protocol::GEOMETRY_TYPES, "polygon,point")]);
    assert!(FieldContract::parse(&reversed).is_err());
    let unresolved = spatial_field(&[(protocol::GEOMETRY_TYPES_DECLARATION, "unresolved")]);
    assert!(FieldContract::parse(&unresolved).is_err());
}

#[test]
fn canonical_and_legacy_values_must_agree() {
    let field = spatial_field(&[(LEGACY_DIMENSIONS, "xyz")]);
    assert!(FieldContract::parse(&field).is_err());
}

/// Un campo geometrico pubblicabile: tutte le chiavi canoniche di §4.
fn published_geometry() -> std::collections::HashMap<String, String> {
    [
        ("ARROW:extension:name", "geoarrow.wkb"),
        ("plenora.field_id", "1"),
        ("plenora.geometry.encoding", "ewkb"),
        ("plenora.geometry.dimensions", "xy"),
        ("plenora.geometry.spatial_semantics", "geometry"),
        ("plenora.geometry.precision", "float64"),
        ("plenora.geometry.types_declaration", "exact"),
        ("plenora.geometry.types", "point"),
        ("plenora.geometry.crs_resolution", "resolved"),
        ("plenora.geometry.crs_id", "EPSG:4326"),
        ("plenora.geometry.srid", "4326"),
        ("plenora.geometry.axis_order", "lat_lon"),
    ]
    .into_iter()
    .map(|(key, value)| (key.to_owned(), value.to_owned()))
    .collect()
}

fn published(metadata: std::collections::HashMap<String, String>) -> Result<()> {
    let field = Field::new("geom", DataType::Binary, true).with_metadata(metadata);
    let schema = Schema::new_with_metadata(
        vec![field],
        std::collections::HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    );
    validate_published_schema(&schema)
}

#[test]
fn a_complete_geometry_field_is_publishable() {
    published(published_geometry()).expect("campo completo");
}

/// §4: un campo con metadati geometrici dichiara l'estensione.
#[test]
fn a_geometry_without_the_extension_is_not_publishable() {
    let mut metadata = published_geometry();
    metadata.remove("ARROW:extension:name");
    assert!(published(metadata).is_err());
}

/// §4: `resolved` e `declared_unresolved` chiedono un CRS (id o
/// definizione) e l'ordine degli assi.
#[test]
fn a_resolved_crs_needs_an_identity_and_an_axis_order() {
    for resolution in ["resolved", "declared_unresolved"] {
        let mut without_crs = published_geometry();
        without_crs.insert(
            "plenora.geometry.crs_resolution".to_owned(),
            resolution.to_owned(),
        );
        without_crs.remove("plenora.geometry.crs_id");
        without_crs.remove("plenora.geometry.srid");
        assert!(published(without_crs).is_err(), "{resolution} senza CRS");
        let mut without_axis = published_geometry();
        without_axis.insert(
            "plenora.geometry.crs_resolution".to_owned(),
            resolution.to_owned(),
        );
        without_axis.remove("plenora.geometry.axis_order");
        assert!(published(without_axis).is_err(), "{resolution} senza assi");
    }
}

/// §3: il vocabolario e case-sensitive.
#[test]
fn geometry_types_are_case_sensitive_when_published() {
    let mut metadata = published_geometry();
    metadata.insert("plenora.geometry.types".to_owned(), "POINT".to_owned());
    assert!(published(metadata).is_err());
}

/// Una chiave legacy non soddisfa un requisito di pubblicazione.
#[test]
fn legacy_keys_do_not_satisfy_publication() {
    for (canonical, legacy, value) in [
        ("plenora.geometry.dimensions", "plenora.dimensions", "xy"),
        (
            "plenora.geometry.spatial_semantics",
            "plenora.spatial_semantics",
            "geometry",
        ),
    ] {
        let mut metadata = published_geometry();
        metadata.remove(canonical);
        metadata.insert(legacy.to_owned(), value.to_owned());
        assert!(published(metadata).is_err(), "{legacy}");
    }
}

/// Due field id numericamente uguali con zeri iniziali diversi sono lo
/// stesso id, come in `contract_schema`.
#[test]
fn equal_field_ids_with_leading_zeros_are_duplicates_when_published() {
    let mut first = published_geometry();
    first.insert("plenora.field_id".to_owned(), "7".to_owned());
    let mut second = published_geometry();
    second.insert("plenora.field_id".to_owned(), "007".to_owned());
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("a", DataType::Binary, true).with_metadata(first),
            Field::new("b", DataType::Binary, true).with_metadata(second),
        ],
        std::collections::HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    );
    assert!(validate_published_schema(&schema).is_err());
}

/// §3: lo SRID e un intero decimale signed 32-bit. 2147483648 e fuori dal
/// dominio e non si pubblica (ne si accetta), anche con un CRS dichiarato.
#[test]
fn an_srid_outside_the_signed_32_bit_domain_is_rejected() {
    let mut metadata = published_geometry();
    for (key, value) in [
        ("plenora.geometry.srid", "2147483648"),
        ("plenora.geometry.crs_resolution", "declared_unresolved"),
        ("plenora.geometry.crs_id", "MYSQL:2147483648"),
        ("plenora.geometry.axis_order", "unknown"),
    ] {
        metadata.insert(key.to_owned(), value.to_owned());
    }
    let error = published(metadata).expect_err("fuori dal dominio i32");
    // REJ-001: un valore fuori dalla grammatica del vocabolario e `schema`.
    assert_eq!(error.category, ErrorCategory::Schema);
    let mut boundary = published_geometry();
    for (key, value) in [
        ("plenora.geometry.srid", "2147483647"),
        ("plenora.geometry.crs_resolution", "declared_unresolved"),
        ("plenora.geometry.crs_id", "MYSQL:2147483647"),
        ("plenora.geometry.axis_order", "unknown"),
    ] {
        boundary.insert(key.to_owned(), value.to_owned());
    }
    published(boundary).expect("i32::MAX e nel dominio");
}

/// §3: un negativo e nel dominio del contratto, ma nessun provider lo
/// rappresenta: si rifiuta come non supportato, non come fuori formato.
#[test]
fn a_negative_srid_is_in_the_contract_but_unsupported() {
    for raw in ["-1", "-2147483648"] {
        let mut metadata = published_geometry();
        metadata.insert("plenora.geometry.srid".to_owned(), raw.to_owned());
        let error = published(metadata).expect_err(raw);
        assert_eq!(error.category, ErrorCategory::Unsupported, "{raw}");
    }
}

/// §3: definizione e identificatore del CRS, se presenti, non sono vuoti;
/// una definizione vuota non soddisfa il requisito di identita.
#[test]
fn an_empty_crs_definition_or_id_is_not_an_identity() {
    for resolution in ["resolved", "declared_unresolved"] {
        let mut metadata = published_geometry();
        metadata.remove("plenora.geometry.crs_id");
        metadata.remove("plenora.geometry.srid");
        for (key, value) in [
            ("plenora.geometry.crs_resolution", resolution),
            ("plenora.geometry.crs_definition", ""),
            ("plenora.geometry.crs_definition_format", "wkt"),
            ("plenora.geometry.axis_order", "unknown"),
        ] {
            metadata.insert(key.to_owned(), value.to_owned());
        }
        assert!(
            published(metadata).is_err(),
            "{resolution} con definizione vuota"
        );
    }
    let mut empty_id = published_geometry();
    empty_id.insert("plenora.geometry.crs_id".to_owned(), String::new());
    assert!(published(empty_id).is_err(), "identificatore vuoto");
}

fn ingress(metadata: std::collections::HashMap<String, String>) -> Result<()> {
    let field = Field::new("geom", DataType::Binary, true).with_metadata(metadata);
    validate_schema_contract(&Schema::new_with_metadata(
        vec![field],
        std::collections::HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn with(changes: &[(&str, Option<&str>)]) -> std::collections::HashMap<String, String> {
    let mut metadata = published_geometry();
    for (key, value) in changes {
        match value {
            Some(value) => metadata.insert((*key).to_owned(), (*value).to_owned()),
            None => metadata.remove(*key),
        };
    }
    metadata
}

/// REJ-001, classe vocabolario: ogni difetto e `schema`. Fino al 24/10 erano
/// `data_mapping`, la categoria di un valore, e alcuni passavano: `Point`
/// (il catalogo dei tipi e case-sensitive), un field id `007`, una chiave
/// `plenora.geometry.` sconosciuta, la precisione assente.
#[test]
fn every_vocabulary_defect_is_a_schema_defect() {
    for (name, changes) in [
        (
            "tipo non canonico",
            vec![("plenora.geometry.types", Some("Point"))],
        ),
        (
            "tipi fuori ordine",
            vec![("plenora.geometry.types", Some("polygon,point"))],
        ),
        ("lista vuota", vec![("plenora.geometry.types", Some(""))]),
        ("field id con zeri", vec![("plenora.field_id", Some("007"))]),
        ("field id assente", vec![("plenora.field_id", None)]),
        (
            "precisione assente",
            vec![("plenora.geometry.precision", None)],
        ),
        (
            "chiave sconosciuta",
            vec![("plenora.geometry.colour", Some("red"))],
        ),
        (
            "encoding fuori insieme",
            vec![("plenora.geometry.encoding", Some("twkb"))],
        ),
        (
            "srid con zeri",
            vec![("plenora.geometry.srid", Some("04326"))],
        ),
        (
            "srid con a capo",
            vec![("plenora.geometry.srid", Some("4326\n"))],
        ),
        ("estensione assente", vec![("ARROW:extension:name", None)]),
    ] {
        let error = ingress(with(&changes)).expect_err(name);
        assert_eq!(error.category, ErrorCategory::Schema, "{name}");
    }
    // Le chiavi native restano libere (ARROW-VOCABULARY §5).
    ingress(with(&[("plenora.geometry.native.postgres", Some("x"))])).expect("chiave nativa");
}

/// REJ-001, classe CRS: ogni difetto di una chiave CRS e `crs`, GEO-005,
/// GEO-016 e GEO-020 compresi.
#[test]
fn every_crs_defect_is_a_crs_defect() {
    let wkt2 = r#"GEOGCRS["WGS 84",ID["EPSG",4326]]"#;
    for (name, changes) in [
        (
            "crs_id senza autorita",
            vec![("plenora.geometry.crs_id", Some(":4326"))],
        ),
        (
            "crs_id con spazio",
            vec![("plenora.geometry.crs_id", Some("EPSG:43 26"))],
        ),
        (
            "risoluzione assente",
            vec![("plenora.geometry.crs_resolution", None)],
        ),
        (
            "assi fuori insieme",
            vec![("plenora.geometry.axis_order", Some("xy"))],
        ),
        (
            "catalogo locale risolto",
            vec![("plenora.geometry.crs_id", Some("POSTGIS:4326"))],
        ),
        (
            "definizione contraddittoria",
            vec![
                ("plenora.geometry.crs_id", Some("EPSG:3857")),
                ("plenora.geometry.srid", None),
                ("plenora.geometry.crs_definition", Some(wkt2)),
                ("plenora.geometry.crs_definition_format", Some("wkt2")),
            ],
        ),
        (
            "definizione malformata",
            vec![
                ("plenora.geometry.crs_definition", Some("GEOGCRS[\"x\")")),
                ("plenora.geometry.crs_definition_format", Some("wkt2")),
            ],
        ),
    ] {
        let error = ingress(with(&changes)).expect_err(name);
        assert_eq!(error.category, ErrorCategory::Crs, "{name}");
    }
    // Un catalogo locale non risolto e conforme (GEO-020).
    ingress(with(&[
        (
            "plenora.geometry.crs_resolution",
            Some("declared_unresolved"),
        ),
        ("plenora.geometry.crs_id", Some("POSTGIS:4326")),
    ]))
    .expect("catalogo locale dichiarato");
    // GEO-004: chi registra l'identita non rifiuta una definizione che non
    // calcola, purche coerente.
    ingress(with(&[
        ("plenora.geometry.crs_definition", Some(wkt2)),
        ("plenora.geometry.crs_definition_format", Some("wkt2")),
    ]))
    .expect("definizione coerente");
}

/// REJ-002: il vocabolario di ogni campo prima del CRS di qualunque campo.
#[test]
fn vocabulary_of_a_later_field_precedes_crs_of_an_earlier_one() {
    let bad_crs = Field::new("a", DataType::Binary, true)
        .with_metadata(with(&[("plenora.geometry.crs_id", Some(":x"))]));
    let bad_vocabulary = Field::new("b", DataType::Binary, true).with_metadata(with(&[
        ("plenora.field_id", Some("2")),
        ("plenora.geometry.types", Some("Point")),
    ]));
    let error = validate_schema_contract(&Schema::new_with_metadata(
        vec![bad_crs, bad_vocabulary],
        std::collections::HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
    .expect_err("due difetti");
    assert_eq!(error.category, ErrorCategory::Schema);
}

/// ARROW-VOCABULARY §4: `missing` vieta identita e assi, non lo SRID.
#[test]
fn a_missing_crs_may_carry_a_numeric_srid() {
    ingress(with(&[
        ("plenora.geometry.crs_resolution", Some("missing")),
        ("plenora.geometry.crs_id", None),
        ("plenora.geometry.axis_order", None),
    ]))
    .expect("missing con SRID");
}

/// GEO-008: lo SRID del bersaglio e il dichiarato, o quello di `EPSG:<n>`;
/// un catalogo locale non si deriva, e un CRS dichiarato senza SRID
/// derivabile e `crs` invece di finire con SRID 0.
#[test]
fn the_target_srid_is_declared_or_derived_from_epsg_only() {
    let target = |changes: &[(&str, Option<&str>)]| {
        let field = Field::new("geom", DataType::Binary, true).with_metadata(with(changes));
        FieldContract::parse(&field).and_then(FieldContract::target_srid)
    };
    assert_eq!(target(&[]).expect("dichiarato"), Some(4326));
    assert_eq!(
        target(&[("plenora.geometry.srid", None)]).expect("derivato"),
        Some(4326)
    );
    for local in ["POSTGIS:4326", "MSSQL:4326"] {
        let error = target(&[
            ("plenora.geometry.srid", None),
            (
                "plenora.geometry.crs_resolution",
                Some("declared_unresolved"),
            ),
            ("plenora.geometry.crs_id", Some(local)),
        ])
        .expect_err(local);
        assert_eq!(error.category, ErrorCategory::Crs, "{local}");
    }
    assert_eq!(
        target(&[
            ("plenora.geometry.srid", None),
            ("plenora.geometry.crs_resolution", Some("missing")),
            ("plenora.geometry.crs_id", None),
            ("plenora.geometry.axis_order", None),
        ])
        .expect("senza CRS"),
        None
    );
}

/// GEO-008: uno SRID derivato porta la propria provenienza, e un bersaglio
/// senza catalogo in cui verificarlo lo rifiuta `crs`; uno dichiarato passa.
#[test]
fn a_derived_srid_is_marked_and_refused_where_unverifiable() {
    let contract = |changes: &[(&str, Option<&str>)]| {
        Field::new("geom", DataType::Binary, true).with_metadata(with(changes))
    };
    let declared = contract(&[]);
    let declared = FieldContract::parse(&declared).expect("dichiarato");
    assert_eq!(
        declared.target_srid_source().expect("dichiarato"),
        Some(TargetSrid {
            srid: 4326,
            derived: false
        })
    );
    assert_eq!(
        declared.declared_target_srid("Db2").expect("Db2"),
        Some(4326)
    );
    let derived = contract(&[("plenora.geometry.srid", None)]);
    let derived = FieldContract::parse(&derived).expect("derivato");
    assert_eq!(
        derived.target_srid_source().expect("derivato"),
        Some(TargetSrid {
            srid: 4326,
            derived: true
        })
    );
    let error = derived
        .declared_target_srid("Db2")
        .expect_err("derivato senza catalogo");
    assert_eq!(error.category, ErrorCategory::Crs);
}

/// GEO-001: solo `lat_lon` e `northing_easting` sono irrappresentabili su un
/// bersaglio est-primo; `unknown` e `other` passano con l'hazard dichiarato.
#[test]
fn only_north_first_axis_orders_are_unrepresentable() {
    for (axis, representable) in [
        ("lon_lat", true),
        ("easting_northing", true),
        ("unknown", true),
        ("other", true),
        ("lat_lon", false),
        ("northing_easting", false),
    ] {
        let field = Field::new("geom", DataType::Binary, true)
            .with_metadata(with(&[("plenora.geometry.axis_order", Some(axis))]));
        let result = FieldContract::parse(&field)
            .expect("campo conforme")
            .ensure_east_first_axis("PostgreSQL");
        assert_eq!(result.is_ok(), representable, "{axis}");
        if let Err(error) = result {
            assert_eq!(error.category, ErrorCategory::Unsupported);
        }
    }
}
