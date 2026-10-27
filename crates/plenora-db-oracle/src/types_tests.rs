use crate::{OracleColumn, OracleColumnKind, OracleColumnSpec};
use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::protocol::GEOMETRY_SPATIAL_SEMANTICS;

fn column(data_type: &str) -> OracleColumn {
    OracleColumn {
        name: "TS".to_owned(),
        ordinal: 1,
        data_type: data_type.to_owned(),
        data_type_owner: (data_type == "SDO_GEOMETRY").then(|| "MDSYS".to_owned()),
        data_length: 11,
        char_length: 0,
        precision: None,
        scale: None,
        nullable: false,
        default_expression: None,
        identity: false,
        virtual_column: false,
        spatial_srid: None,
        spatial_dimensions: None,
        spatial_semantics: None,
    }
}

#[test]
fn geography_catalog_semantics_reaches_the_arrow_contract() {
    let mut spatial = column("SDO_GEOMETRY");
    spatial.spatial_srid = Some(4326);
    spatial.spatial_dimensions = Some(2);
    spatial.spatial_semantics = Some(SpatialSemantics::Geography);
    let field = OracleColumnSpec::from_catalog(&spatial)
        .expect("geography Oracle catalogata")
        .arrow_field()
        .expect("campo Arrow geography");
    assert_eq!(
        field.metadata().get(GEOMETRY_SPATIAL_SEMANTICS),
        Some(&"geography".to_owned())
    );
}

#[test]
fn timestamp_precision_is_generic_but_local_time_zone_stays_closed() {
    let timestamp = OracleColumnSpec::from_catalog(&column("TIMESTAMP(9)"))
        .expect("TIMESTAMP con precisione Oracle");
    assert_eq!(timestamp.kind, OracleColumnKind::DateTime);

    let timestamp_tz = OracleColumnSpec::from_catalog(&column("TIMESTAMP(3) WITH TIME ZONE"))
        .expect("TIMESTAMP WITH TIME ZONE Oracle");
    assert_eq!(timestamp_tz.kind, OracleColumnKind::TimestampTz);

    OracleColumnSpec::from_catalog(&column("TIMESTAMP(6) WITH LOCAL TIME ZONE"))
        .expect_err("TIMESTAMP WITH LOCAL TIME ZONE non qualificato");
}

#[test]
fn geometry_spec_without_srid_is_an_error_not_a_panic() {
    // Una spec costruita a mano, fuori da `from_catalog`, puo mancare dello
    // SRID: prima `arrow_field` andava in panic, ora rifiuta con `Crs`.
    let spec = OracleColumnSpec {
        name: "SHAPE".to_owned(),
        native_type: "SDO_GEOMETRY".to_owned(),
        nullable: true,
        kind: OracleColumnKind::Geometry,
        spatial_srid: None,
        spatial_dimensions: Some(2),
        spatial_semantics: Some(SpatialSemantics::Geometry),
    };
    let error = spec.arrow_field().expect_err("geometry senza SRID");
    assert_eq!(error.category, plenora_database_core::ErrorCategory::Crs);
}

/// Un tipo `SDO_GEOMETRY` di un altro schema non e `MDSYS.SDO_GEOMETRY`:
/// potrebbe avere un metodo `MAP` o `ORDER`, e la guardia del server sugli
/// operandi scalari e misurata soltanto su quello di `MDSYS`. Non e una
/// geometria del provider, e il tipo non e qualificato.
#[test]
fn sdo_geometry_of_another_owner_is_not_a_geometry() {
    let mut omonimo = column("SDO_GEOMETRY");
    omonimo.data_type_owner = Some("PLENORA".to_owned());
    omonimo.spatial_srid = Some(4326);
    omonimo.spatial_dimensions = Some(2);
    omonimo.spatial_semantics = Some(SpatialSemantics::Geometry);
    assert!(!omonimo.is_sdo_geometry());
    let error = OracleColumnSpec::from_catalog(&omonimo).expect_err("tipo omonimo");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Unsupported
    );
    omonimo.data_type_owner = None;
    assert!(!omonimo.is_sdo_geometry());
    omonimo.data_type_owner = Some("MDSYS".to_owned());
    assert!(omonimo.is_sdo_geometry());
    assert_eq!(
        OracleColumnSpec::from_catalog(&omonimo)
            .expect("MDSYS.SDO_GEOMETRY")
            .kind,
        OracleColumnKind::Geometry
    );
}

/// Il bind di main 8.0.2 e testo UTF-8, non un TIMESTAMP TZ TTC a 13 byte.
/// La lettura deve accompagnarlo con lo stesso modello SQL esplicito.
#[test]
fn filter_timestamptz_keeps_main_wire_bytes_and_explicit_conversion() {
    use crate::{OracleObjectDescription, OracleReadPlan};
    use oracle_rs::capabilities::Capabilities;
    use oracle_rs::messages::{ExecuteMessage, ExecuteOptions};
    use oracle_rs::statement::Statement;
    use oracle_rs::Value;
    use plenora_database_core::cast_table::OperandType;
    use plenora_database_core::plan::{FilterExpression, ObjectRef, ProviderKind, ReadOperation};
    use plenora_database_core::provider::{ParameterBag, ParameterValue};

    let description = OracleObjectDescription {
        schema: "PLENORA".to_owned(),
        name: "EVENTS".to_owned(),
        kind: "TABLE".to_owned(),
        columns: vec![column("TIMESTAMP(6) WITH TIME ZONE")],
        indexes: vec![],
        schema_token: "fixture".to_owned(),
    };
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".to_owned()),
            object: "EVENTS".to_owned(),
        },
        projection: vec!["TS".to_owned()],
        order_by: vec![],
        row_limit: None,
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: "TS".to_owned(),
            parameter: "p".to_owned(),
        }),
        declared_crs: vec![],
    };
    let expected_sql = "SELECT T.\"TS\" AS \"TS\" FROM \"PLENORA\".\"EVENTS\" T WHERE \"T\".\"TS\" = TO_TIMESTAMP_TZ(:1, 'YYYY-MM-DD\"T\"HH24:MI:SS.FFTZH:TZM')";
    let packet = |sql: &str, values| {
        let statement = Statement::new(sql);
        let mut message = ExecuteMessage::new(&statement, ExecuteOptions::for_query(1));
        message.set_bind_values(values);
        message
            .build_request(&Capabilities::new())
            .expect("pacchetto TTC")
    };
    for offset in ["+02:00", "-05:30", "+00:00"] {
        for (fraction, normalized) in [
            ("", ".000000"),
            (".000001", ".000001"),
            (".123", ".123000"),
            (".123001", ".123001"),
        ] {
            let value =
                ParameterValue::TimestampTz(format!("2024-01-01T10:20:30{fraction}{offset}"));
            let parameters = ParameterBag::new([("p".to_owned(), value.clone())].into());
            let admitted = plenora_database_core::admission_gate::read_filter(
                ProviderKind::Oracle,
                operation.filter.as_ref(),
                &parameters,
                |_| Some(OperandType::TimestampTz { fraction_digits: 6 }),
            )
            .expect("ammissione");
            assert_eq!(admitted.get("p"), Some(&value));
            crate::parameter::reject_empty(admitted.get("p").unwrap()).expect("filtro");
            let bound = crate::parameter::bind_parameters(&[admitted.get("p").unwrap().clone()])
                .expect("bind filtro");
            // Oracolo del percorso origin/main: parse_timestamp_tz formatta
            // sei decimali e bind_parameter costruisce Value::String.
            let main_bound = vec![Value::String(format!(
                "2024-01-01T10:20:30{normalized}{offset}"
            ))];
            assert_eq!(
                packet(expected_sql, bound.clone()),
                packet(expected_sql, main_bound)
            );
            let plan = OracleReadPlan::compile_with_parameters(&description, &operation, &admitted)
                .expect("piano");
            assert_eq!(plan.bind_names, ["p"]);
            assert_eq!(plan.sql, expected_sql);
            assert_eq!(
                packet(&plan.sql, bound.clone()),
                packet(expected_sql, bound)
            );
        }
    }
}
