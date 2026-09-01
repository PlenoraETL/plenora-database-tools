use super::*;
use crate::profile::ProductProfile;
use plenora_database_core::field_contract::validate_schema_contract;
use plenora_database_core::plan::{ObjectRef, OrderBy, ProviderKind};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::ReadCheckpoint;

fn column(name: &str, data_type: &str, declaration: &str) -> MysqlColumn {
    MysqlColumn {
        name: name.to_owned(),
        ordinal: 1,
        data_type: data_type.to_owned(),
        native_declaration: declaration.to_owned(),
        nullable: false,
        default_expression: None,
        character_set: None,
        collation: None,
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        spatial_srid: None,
        extra: String::new(),
        generation_expression: String::new(),
    }
}

#[test]
fn mapping_preserves_signedness_and_rejects_wide_decimal() {
    assert_eq!(
        MysqlColumnSpec::from_catalog(&column("id", "bigint", "bigint unsigned"))
            .expect("unsigned bigint")
            .kind,
        MysqlColumnKind::U64
    );
    let mut decimal = column("amount", "decimal", "decimal(39,0)");
    decimal.numeric_precision = Some(39);
    decimal.numeric_scale = Some(0);
    assert_eq!(
        MysqlColumnSpec::from_catalog(&decimal)
            .expect_err("wide decimal")
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn a_geometry_without_a_declared_srid_is_refused_by_both_profiles() {
    // La regola e la stessa per i due prodotti; a divergere e cio che il
    // catalogo puo dire. Su MySQL `SRS_ID` esiste, quindi una geometry con
    // SRID dichiarato si descrive; su MariaDB la colonna non esiste e la
    // query la dichiara nulla, percio **ogni** geometry finisce in questo
    // ramo. Non e "questa colonna non ha un CRS": e "non c'e modo di
    // saperlo", e il contratto GeoArrow pubblicato un CRS lo dichiara.
    let unknown = column("geom", "geometry", "geometry");
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        let error = MysqlColumnSpec::from_catalog_with_profile(&unknown, profile)
            .expect_err("una geometry senza SRID dichiarato si rifiuta");
        assert_eq!(error.category, ErrorCategory::Crs);
        assert!(
            error.message.contains(profile.product()),
            "{}: il rifiuto non nomina chi ha rifiutato — {}",
            profile.product(),
            error.message
        );
    }

    // Con l'SRID dichiarato la colonna si descrive, e questo e cio che
    // rende il rifiuto sopra una conseguenza della query e non della
    // regola: su MariaDB questo caso non si presenta, perche `srs_id`
    // arriva sempre nullo.
    let declared = MysqlColumn {
        spatial_srid: Some(4_326),
        ..column("geom", "geometry", "geometry")
    };
    for profile in [
        &crate::profile::MYSQL_PROFILE as &dyn crate::profile::ProductProfile,
        &crate::profile::MARIADB_PROFILE,
    ] {
        let spec = MysqlColumnSpec::from_catalog_with_profile(&declared, profile)
            .expect("geometry con SRID");
        assert_eq!(spec.kind, MysqlColumnKind::Geometry);
        assert_eq!(spec.spatial_srid, Some(4_326));
    }
    assert!(crate::profile::MARIADB_PROFILE
        .object_columns_query()
        .contains("NULL AS srs_id"));
}

#[test]
fn spatial_projection_is_wkb_xy_with_declared_srid() {
    let mut geometry = column("geom", "geometry", "geometry");
    geometry.spatial_srid = Some(4_326);
    let spec = MysqlColumnSpec::from_catalog(&geometry).expect("geometry");
    let field = spec.arrow_field();
    assert_eq!(field.data_type(), &DataType::Binary);
    assert_eq!(
        field.metadata().get(protocol::GEOMETRY_DIMENSIONS),
        Some(&"xy".to_owned())
    );
    assert_eq!(
        spec.projection(&mysql_renderer(), &crate::profile::MYSQL_PROFILE)
            .expect("projection"),
        "ST_AsBinary(`geom`) AS `geom`"
    );
}

#[test]
fn concrete_spatial_type_produces_an_exact_valid_contract() {
    let mut point = column("geom", "point", "point");
    point.spatial_srid = Some(4_326);
    let field = MysqlColumnSpec::from_catalog(&point)
        .expect("point MySQL")
        .arrow_field();
    assert_eq!(
        field.metadata().get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"exact".to_owned())
    );
    validate_schema_contract(contract_schema(vec![field]).as_ref())
        .expect("contratto geometrico MySQL canonico");
}

#[test]
fn mysql_geomcollection_alias_produces_the_canonical_exact_type() {
    let mut collection = column("geom", "geomcollection", "geomcollection");
    collection.spatial_srid = Some(4_326);
    let field = MysqlColumnSpec::from_catalog(&collection)
        .expect("geomcollection MySQL")
        .arrow_field();
    assert_eq!(
        field.metadata().get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"exact".to_owned())
    );
    assert_eq!(
        field.metadata().get(protocol::GEOMETRY_TYPES),
        Some(&"geometrycollection".to_owned())
    );
    validate_schema_contract(contract_schema(vec![field]).as_ref())
        .expect("contratto geometrycollection canonico");
}

#[test]
fn limit_without_order_is_rejected_fail_closed() {
    let description = MysqlObjectDescription {
        schema: "data".to_owned(),
        name: "items".to_owned(),
        kind: "BASE TABLE".to_owned(),
        engine: Some("InnoDB".to_owned()),
        columns: vec![column("id", "bigint", "bigint")],
        indexes: Vec::new(),
        token: crate::MysqlSchemaToken("token".to_owned()),
    };
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("data".to_owned()),
            object: "items".to_owned(),
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: Some(1),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    assert_eq!(
        MysqlReadPlan::compile(&description, &operation)
            .expect_err("nondeterministic limit")
            .category,
        ErrorCategory::InvalidPlan
    );

    let ordered = ReadOperation {
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        ..operation
    };
    let plan = MysqlReadPlan::compile(&description, &ordered).expect("ordered plan");
    assert_eq!(
        plan.sql,
        "SELECT `id` FROM `data`.`items` ORDER BY `id` ASC LIMIT 1;"
    );
}

#[test]
fn qualified_checkpoint_renders_as_a_bound_mysql_keyset() {
    let description = MysqlObjectDescription {
        schema: "data".to_owned(),
        name: "items".to_owned(),
        kind: "BASE TABLE".to_owned(),
        engine: Some("InnoDB".to_owned()),
        columns: vec![column("id", "bigint", "bigint")],
        indexes: Vec::new(),
        token: crate::MysqlSchemaToken("token".to_owned()),
    };
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("data".to_owned()),
            object: "items".to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(100),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    for provider in [ProviderKind::Mysql, ProviderKind::Mariadb] {
        let checkpoint = ReadCheckpoint::new(
            provider,
            &operation,
            &ParameterBag::default(),
            vec![ParameterValue::I64(41)],
        )
        .expect("checkpoint");
        let (resumed, _) = checkpoint
            .resume(provider, &operation, &ParameterBag::default())
            .expect("resume");
        let plan = MysqlReadPlan::compile(&description, &resumed).expect("piano ripreso");
        assert_eq!(
            plan.sql,
            "SELECT `id` FROM `data`.`items` WHERE `id` > ? ORDER BY `id` ASC LIMIT 100;"
        );
        assert_eq!(plan.bind_names, ["__plenora_resume_0"]);
    }
}

/// La finestra si rende, e da sola porta con se il tetto del tipo.
///
/// `OFFSET n` senza `LIMIT` non e sintassi valida su questi motori, e il
/// massimo di `BIGINT UNSIGNED` e la forma con cui il dialetto dice «da
/// qui in poi, tutto». Il valore non arriva dal piano, ed e la ragione per
/// cui questo test scrive il SQL atteso per intero invece di cercare la
/// sottostringa `OFFSET`: un tetto inventato in silenzio sarebbe
/// esattamente il genere di cosa che una sottostringa non vede.
#[test]
fn the_window_renders_with_and_without_a_ceiling() {
    let description = MysqlObjectDescription {
        schema: "data".to_owned(),
        name: "items".to_owned(),
        kind: "BASE TABLE".to_owned(),
        engine: Some("InnoDB".to_owned()),
        columns: vec![column("id", "bigint", "bigint")],
        indexes: Vec::new(),
        token: crate::MysqlSchemaToken("token".to_owned()),
    };
    let ordered = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("data".to_owned()),
            object: "items".to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: Some(20),
        filter: None,
        declared_crs: Vec::new(),
    };
    assert_eq!(
        MysqlReadPlan::compile(&description, &ordered)
            .expect("finestra senza tetto")
            .sql,
        "SELECT `id` FROM `data`.`items` ORDER BY `id` ASC LIMIT 18446744073709551615 OFFSET 20;"
    );

    let bounded = ReadOperation {
        row_limit: Some(5),
        ..ordered.clone()
    };
    assert_eq!(
        MysqlReadPlan::compile(&description, &bounded)
            .expect("finestra con tetto")
            .sql,
        "SELECT `id` FROM `data`.`items` ORDER BY `id` ASC LIMIT 5 OFFSET 20;"
    );

    // E senza ordinamento la finestra e rifiutata, come il tetto.
    let unordered = ReadOperation {
        order_by: Vec::new(),
        ..ordered
    };
    assert_eq!(
        MysqlReadPlan::compile(&description, &unordered)
            .expect_err("finestra non riproducibile")
            .category,
        ErrorCategory::InvalidPlan
    );
}
use plenora_database_core::plan::DeclaredCrs;

/// Una tabella con una geometria di cui il catalogo non sa l'SRID.
fn spatial_description() -> MysqlObjectDescription {
    let mut shape = column("shape", "geometry", "geometry");
    shape.nullable = true;
    MysqlObjectDescription {
        schema: "data".to_owned(),
        name: "places".to_owned(),
        kind: "BASE TABLE".to_owned(),
        engine: Some("InnoDB".to_owned()),
        columns: vec![column("id", "bigint", "bigint"), shape],
        indexes: Vec::new(),
        token: crate::MysqlSchemaToken("token".to_owned()),
    }
}

fn crs_read(declared: Vec<DeclaredCrs>) -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("data".to_owned()),
            object: "places".to_owned(),
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: declared,
    }
}

fn crs_declaration(column: &str, srid: u32) -> DeclaredCrs {
    DeclaredCrs {
        column: column.to_owned(),
        srid,
    }
}

/// Senza dichiarazione la colonna resta rifiutata, come prima.
///
/// E' il caso che non deve cambiare: la dichiarazione apre una porta, non
/// ne toglie una chiusa. Un profilo il cui catalogo l'SRID non ce l'ha
/// continua a rifiutare chi non gliene da uno, perche il contratto
/// `GeoArrow` pubblica un CRS e pubblicarlo senza saperlo resta peggio del
/// rifiuto.
#[test]
fn a_geometry_without_catalog_or_plan_srid_is_still_refused() {
    let error = MysqlReadPlan::compile_with_profile(
        &spatial_description(),
        &crs_read(Vec::new()),
        &crate::profile::MARIADB_PROFILE,
    )
    .expect_err("nessuna delle due fonti parla");
    assert_eq!(error.category, ErrorCategory::Crs);
}

/// Con la dichiarazione il piano compila, e porta con se il controllo.
///
/// Le due asserzioni non sono ridondanti. La prima dice che la lettura
/// esiste; la seconda che non e stata creduta sulla parola — se il piano
/// compilasse senza il controllo, il CRS pubblicato sarebbe
/// un'affermazione del chiamante ripetuta dal provider, che e esattamente
/// cio che la regola 1 vieta.
#[test]
fn a_declared_crs_compiles_and_carries_its_verification() {
    let plan = MysqlReadPlan::compile_with_profile(
        &spatial_description(),
        &crs_read(vec![crs_declaration("shape", 4326)]),
        &crate::profile::MARIADB_PROFILE,
    )
    .expect("il piano dichiara il CRS");
    assert_eq!(plan.crs_checks.len(), 1);
    assert_eq!(plan.crs_checks[0].expected, 4326);
    assert_eq!(plan.crs_checks[0].column, "shape");
    // La colonna del controllo sta **dopo** le due visibili: gli indici di
    // cio che il decoder legge non cambiano.
    assert_eq!(plan.crs_checks[0].result_index, 2);
    assert!(
        plan.sql.contains("ST_SRID(`shape`)"),
        "il controllo non arriva al server: {}",
        plan.sql
    );
    // E lo schema pubblicato ha due campi, non tre: la colonna del
    // controllo non e un dato, e nessun consumatore deve vederla.
    assert_eq!(plan.schema.fields().len(), 2);
}

/// Il CRS dichiarato arriva ai metadata del campo Arrow.
#[test]
fn a_declared_crs_reaches_the_published_field() {
    let plan = MysqlReadPlan::compile_with_profile(
        &spatial_description(),
        &crs_read(vec![crs_declaration("shape", 3003)]),
        &crate::profile::MARIADB_PROFILE,
    )
    .expect("il piano dichiara il CRS");
    let field = plan.schema.field(1);
    assert_eq!(field.name(), "shape");
    assert_eq!(
        field
            .metadata()
            .get("plenora.geometry.srid")
            .map(String::as_str),
        Some("3003"),
        "il CRS dichiarato non compare fra i metadata: {:?}",
        field.metadata()
    );
}

/// Tre dichiarazioni sbagliate, tre rifiuti che nominano cose diverse.
///
/// Un rifiuto solo per tutti e tre sarebbe piu corto e direbbe meno: chi
/// sbaglia il nome di una colonna e chi dichiara un CRS su un `BIGINT`
/// hanno due problemi diversi, e il secondo probabilmente crede che quella
/// tabella contenga qualcosa che non contiene.
#[test]
fn a_declaration_that_does_not_apply_is_refused_by_its_own_reason() {
    let missing = MysqlReadPlan::compile_with_profile(
        &spatial_description(),
        &crs_read(vec![crs_declaration("assente", 4326)]),
        &crate::profile::MARIADB_PROFILE,
    )
    .expect_err("colonna inesistente");
    assert_eq!(missing.category, ErrorCategory::NotFound);

    let scalar = MysqlReadPlan::compile_with_profile(
        &spatial_description(),
        &crs_read(vec![crs_declaration("id", 4326)]),
        &crate::profile::MARIADB_PROFILE,
    )
    .expect_err("colonna non geometrica");
    assert_eq!(scalar.category, ErrorCategory::InvalidPlan);

    // E la terza: il catalogo lo sa gia. Due fonti per lo stesso fatto
    // sono una fonte di troppo, e quando divergono nessuna delle due e
    // piu quella giusta.
    let mut known = spatial_description();
    known.columns[1].spatial_srid = Some(4326);
    let duplicated = MysqlReadPlan::compile_with_profile(
        &known,
        &crs_read(vec![crs_declaration("shape", 4326)]),
        &crate::profile::MARIADB_PROFILE,
    )
    .expect_err("il catalogo la descrive gia");
    assert_eq!(duplicated.category, ErrorCategory::InvalidPlan);
}

/// Dove il catalogo parla, il piano non deve nemmeno provarci.
///
/// Il profilo `MySQL` legge `SRS_ID` e non pretende dichiarazioni: una
/// colonna con SRID di catalogo compila senza controlli, e il piano non
/// porta niente da verificare riga per riga.
#[test]
fn a_catalog_srid_needs_no_verification() {
    let mut known = spatial_description();
    known.columns[1].spatial_srid = Some(4326);
    let plan = MysqlReadPlan::compile_with_profile(
        &known,
        &crs_read(Vec::new()),
        &crate::profile::MYSQL_PROFILE,
    )
    .expect("il catalogo basta");
    assert!(plan.crs_checks.is_empty());
    assert!(
        !plan.sql.contains("ST_SRID"),
        "una lettura senza dichiarazioni non deve chiedere SRID: {}",
        plan.sql
    );
}
