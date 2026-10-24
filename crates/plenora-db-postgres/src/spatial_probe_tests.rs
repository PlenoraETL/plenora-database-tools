use super::*;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::relational::{
    CommonTableExpression, QueryDerivedSource, QueryProjection, QuerySource, SpatialOperator,
};

fn wkb_point() -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes
}

fn ewkb_point(srid: u32) -> Vec<u8> {
    plenora_database_core::ewkb::with_root_srid(&wkb_point(), srid, 32).expect("ewkb")
}

fn bag(entries: &[(&str, ParameterValue)]) -> ParameterBag {
    ParameterBag::new(
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
            .collect(),
    )
}

fn column(relation: Option<&str>, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: relation.map(str::to_owned),
            field: field.to_owned(),
        },
    }
}

fn parameter(name: &str) -> QueryExpression {
    QueryExpression::Parameter {
        name: name.to_owned(),
    }
}

fn bbox(left: QueryExpression, right: QueryExpression) -> QueryExpression {
    QueryExpression::SpatialOperator {
        operator: SpatialOperator::BoundingBoxIntersects,
        left: Box::new(left),
        right: Box::new(right),
    }
}

fn operation(source: Option<&str>, filter: QueryExpression) -> QueryOperation {
    QueryOperation {
        common_table_expressions: Vec::new(),
        source: source.map(|object| QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("public".to_owned()),
                object: object.to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column(None, "id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(filter),
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
        declared_crs: Vec::new(),
    }
}

fn category(result: Result<QueryProbes>) -> ErrorCategory {
    result.expect_err("rifiuto atteso").category
}

#[test]
fn a_column_probe_is_checked_against_the_catalog_and_bound_with_its_srid() {
    let parameters = bag(&[(
        "p",
        ParameterValue::Wkb {
            bytes: wkb_point(),
            srid: Some(4326),
            dimensions: plenora_database_core::geometry::Dimensions::Xy,
            semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
        },
    )]);
    let probes = plan_query_probes(
        &operation(Some("t"), bbox(column(None, "g"), parameter("p"))),
        &parameters,
        32,
    )
    .expect("sonda 4326");
    assert_eq!(
        probes.columns,
        vec![ColumnFrame {
            schema: Some("public".to_owned()),
            object: "t".to_owned(),
            column: "g".to_owned(),
            srid: 4326,
        }]
    );
    assert_eq!(
        probes.parameters.get("p"),
        Some(&ParameterValue::Bytes(ewkb_point(4326)))
    );
}

#[test]
fn a_probe_without_srid_or_against_an_unresolved_term_is_refused() {
    let plain = bag(&[("p", ParameterValue::Bytes(wkb_point()))]);
    assert_eq!(
        category(plan_query_probes(
            &operation(Some("t"), bbox(column(None, "g"), parameter("p"))),
            &plain,
            32
        )),
        ErrorCategory::Crs
    );
    let srid = bag(&[("p", ParameterValue::Bytes(ewkb_point(4326)))]);
    // Una sorgente derivata puo ridefinire la colonna: non si indovina.
    let mut derived = operation(None, bbox(column(Some("d"), "g"), parameter("p")));
    derived.derived_source = Some(QueryDerivedSource {
        query: Box::new(operation(Some("t"), column(None, "ok"))),
        alias: "d".to_owned(),
    });
    assert_eq!(
        category(plan_query_probes(&derived, &srid, 32)),
        ErrorCategory::Crs
    );
    // Un nome senza schema che e una CTE non e la tabella del catalogo.
    let mut cte = operation(Some("t"), bbox(column(None, "g"), parameter("p")));
    if let Some(source) = cte.source.as_mut() {
        source.object.schema = None;
    }
    cte.common_table_expressions = vec![CommonTableExpression {
        name: "t".to_owned(),
        recursive: false,
        query: Box::new(operation(Some("u"), column(None, "ok"))),
    }];
    assert_eq!(
        category(plan_query_probes(&cte, &srid, 32)),
        ErrorCategory::Crs
    );
    // Due sonde in sistemi diversi.
    let two = bag(&[
        ("p", ParameterValue::Bytes(ewkb_point(4326))),
        ("q", ParameterValue::Bytes(ewkb_point(3857))),
    ]);
    assert_eq!(
        category(plan_query_probes(
            &operation(Some("t"), bbox(parameter("p"), parameter("q"))),
            &two,
            32
        )),
        ErrorCategory::Crs
    );
}

#[test]
fn a_spatial_value_with_another_embedded_srid_is_refused() {
    let value = QueryExpression::SpatialValue {
        expression: Box::new(parameter("p")),
        srid: 4326,
        semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
    };
    let parameters = bag(&[("p", ParameterValue::Bytes(ewkb_point(3857)))]);
    assert_eq!(
        category(plan_query_probes(
            &operation(
                Some("t"),
                QueryExpression::Spatial {
                    function: SpatialFunction::Intersects,
                    arguments: vec![column(None, "g"), value],
                }
            ),
            &parameters,
            32
        )),
        ErrorCategory::Crs
    );
}

#[test]
fn a_probe_also_used_as_another_value_is_not_rewritten() {
    let parameters = bag(&[(
        "p",
        ParameterValue::Wkb {
            bytes: wkb_point(),
            srid: Some(4326),
            dimensions: plenora_database_core::geometry::Dimensions::Xy,
            semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
        },
    )]);
    let filter = QueryExpression::And {
        arguments: vec![
            bbox(column(None, "g"), parameter("p")),
            QueryExpression::Compare {
                left: Box::new(column(None, "raw")),
                operator: plenora_database_core::plan::ComparisonOperator::Eq,
                right: Box::new(parameter("p")),
            },
        ],
    };
    assert_eq!(
        category(plan_query_probes(
            &operation(Some("t"), filter),
            &parameters,
            32
        )),
        ErrorCategory::InvalidPlan
    );
}

#[test]
fn column_against_column_stays_with_the_server() {
    let probes = plan_query_probes(
        &operation(Some("t"), bbox(column(None, "g"), column(None, "h"))),
        &ParameterBag::default(),
        32,
    )
    .expect("nessuna sonda");
    assert!(probes.columns.is_empty());
}

#[test]
fn a_read_filter_probe_is_compared_with_the_column_typmod() {
    let filter = FilterExpression::Spatial {
        function: SpatialFunction::Intersects,
        field: "g".to_owned(),
        geometry_parameter: Some("p".to_owned()),
        distance_parameter: None,
    };
    let columns = [ColumnSpec {
        name: "g".to_owned(),
        native_type: "geometry".to_owned(),
        nullable: true,
        numeric_precision: None,
        numeric_scale: None,
        spatial_srid: Some(4326),
        spatial_dimensions: None,
        spatial_type: None,
        spatial_crs_id: None,
        default_expression: None,
        identity_kind: None,
        generated_kind: None,
        native_declaration: None,
        type_kind: None,
        composite_fields: Vec::new(),
        enum_labels: Vec::new(),
        domain_base_type: None,
        domain_constraints: Vec::new(),
        collation: None,
        source: None,
        registered_field_id: None,
        kind: crate::types::ColumnKind::Geometry,
    }];
    let accepted = verify_read_filter(
        Some(&filter),
        &columns,
        &bag(&[("p", ParameterValue::Bytes(ewkb_point(4326)))]),
        32,
    )
    .expect("stesso SRID");
    assert_eq!(
        accepted.get("p"),
        Some(&ParameterValue::Bytes(ewkb_point(4326)))
    );
    for value in [
        ParameterValue::Bytes(ewkb_point(3857)),
        ParameterValue::Bytes(wkb_point()),
    ] {
        let error =
            verify_read_filter(Some(&filter), &columns, &bag(&[("p", value)]), 32).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Crs);
    }
}
