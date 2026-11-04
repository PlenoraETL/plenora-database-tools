use plenora_database_core::{
    admission_gate::{self, described as admit},
    operand_admission::{operand_type, CatalogType, OperandType},
    plan::ProviderKind as P,
    provider::{ParameterBag, ParameterValue as V},
    relational::QueryOperation,
};
use serde_json::json;
fn query(expr: &serde_json::Value) -> QueryOperation {
    serde_json::from_value(json!({"source":{"object":{"catalog":null,"schema":null,"object":"t"},"alias":null},"projection":[{"expression":expr,"alias":"v"}],"filter":null,"having":null,"row_limit":null})).unwrap()
}
#[test]
fn mysql_typed_json_reaches_binder() {
    let q = query(&json!({"kind":"typed_parameter","name":"p","parameter_type":"json"}));
    let a = admission_gate::query(
        P::Mysql,
        &q,
        "SELECT CAST(? AS JSON) FROM t".into(),
        &["p".into()],
        &[V::Json(json!({"a":1}))],
    )
    .unwrap();
    let bag = ParameterBag::new([("p".into(), a.statement().params[0].clone())].into());
    plenora_db_mysql::bind_parameters(&["p".into()], &bag, P::Mysql).unwrap();
    assert!(plenora_db_mysql::bind_parameters(
        &["p".into()],
        &ParameterBag::new([("p".into(), V::Json(json!({"a":1})))].into()),
        P::Mysql
    )
    .is_err());
}
#[test]
fn timestamp_invalid_suffix_is_refused() {
    for p in [P::Postgres, P::Mysql, P::Mariadb, P::Sqlserver, P::Db2] {
        let v = V::Timestamp("2024-01-01T00:00:00.000000PAYLOAD".into());
        assert!(admit(p, &v, &OperandType::Date).is_err());
    }
}
#[test]
fn postgres_name_is_bounded_in_bytes() {
    let ty = operand_type(
        P::Postgres,
        &CatalogType {
            name: "name".into(),
            ..Default::default()
        },
    );
    for text in ["x".repeat(64), "x".repeat(65), "\u{e9}".repeat(32)] {
        assert!(admit(P::Postgres, &V::String(text), &ty).is_err());
    }
    for text in ["x".repeat(63), "\u{e9}".repeat(31)] {
        assert!(admit(P::Postgres, &V::String(text), &ty).is_ok());
    }
}
#[test]
fn spatial_typed_wkb_passes_both_gates() {
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    let q = query(
        &json!({"kind":"spatial_value","expression":{"kind":"typed_parameter","name":"p","parameter_type":"binary"},"srid":4326,"semantics":"geometry"}),
    );
    let mut bytes = vec![1, 1, 0, 0, 0];
    bytes.extend(1f64.to_le_bytes());
    bytes.extend(2f64.to_le_bytes());
    let v = V::Wkb {
        bytes: bytes.clone(),
        srid: Some(4326),
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    };
    let mut b = plenora_database_core::typed_parameter::BoundValues::default();
    b.add("p", &v);
    b.judge_query(P::Postgres, &q).unwrap();
    for p in [
        P::Postgres,
        P::Mysql,
        P::Mariadb,
        P::Sqlserver,
        P::Oracle,
        P::Db2,
    ] {
        let a = admission_gate::query(
            p,
            &q,
            "unused".into(),
            &["p".into()],
            std::slice::from_ref(&v),
        )
        .unwrap();
        let expected = if p == P::Sqlserver {
            v.clone()
        } else {
            V::Bytes(bytes.clone())
        };
        assert_eq!(a.statement().params, vec![expected]);
    }
}
#[test]
fn enumerate_typed_parameter_pipeline() {
    for provider in [P::Mysql, P::Mariadb] {
        plenora_database_testkit::bind_matrix900::check(provider, |_, value| {
            let bag = ParameterBag::new([("p".into(), value.clone())].into());
            plenora_db_mysql::bind_parameters(&["p".into()], &bag, provider).is_ok()
        });
    }
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "enumerazione esplicita delle famiglie malformate"
)]
fn malformed_source_cross_every_target() {
    let providers = [
        P::Postgres,
        P::Mysql,
        P::Mariadb,
        P::Sqlserver,
        P::Oracle,
        P::Db2,
    ];
    let names = [
        "boolean",
        "integer",
        "big_integer",
        "float",
        "decimal",
        "string",
        "binary",
        "date",
        "timestamp",
        "timestamp_tz",
        "uuid",
        "json",
    ];
    let mut invalid = Vec::new();
    for text in [
        "",
        "payload",
        "2024-02-30",
        "2024-01-01x",
        "2024-1-01",
        "2024-01-01\0",
    ] {
        invalid.push(V::Date(text.into()));
    }
    for text in [
        "",
        "not-a-timestamp",
        "2024-01-01T00:00:00.000000PAYLOAD",
        "2024-01-01T00:00:00.",
        "2024-01-01T00:00:00Zjunk",
        "2024-01-01T24:00:00",
        "2024-02-30T00:00:00",
        "2024-01-01T00:00:00.0000001",
        "2024-01-01T00:00:00+99:99",
        "2024-01-01T00:00:00.??",
    ] {
        invalid.push(V::Timestamp(text.into()));
        invalid.push(V::TimestampTz(text.into()));
    }
    for text in ["", "+", "1.2x", "1..0", "NaN", "1e0", "1\0"] {
        invalid.push(V::Decimal(text.into()));
    }
    for text in [
        "",
        "00000000000000000000000000000000",
        "00000000-0000-0000-0000-00000000000x",
    ] {
        invalid.push(V::Uuid(text.into()));
    }
    for bytes in [vec![], vec![1, 1, 0, 0, 0], vec![3, 1, 0, 0, 0]] {
        invalid.push(V::Wkb {
            bytes,
            srid: Some(4326),
            dimensions: plenora_database_core::geometry::Dimensions::Xy,
            semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
        });
    }
    invalid.extend([
        V::Null {
            type_name: String::new(),
        },
        V::Null {
            type_name: "bad\0type".into(),
        },
    ]);
    for p in providers {
        for name in names {
            let kind = serde_json::from_value(json!(name)).unwrap();
            let target = plenora_database_core::cast_table::rendered_target(p, kind);
            for value in &invalid {
                assert!(admit(p, value, &target).is_err(), "{p:?}/{name}");
                let q = query(&json!({"kind":"typed_parameter","name":"p","parameter_type":name}));
                let mut bound = plenora_database_core::typed_parameter::BoundValues::default();
                bound.add("p", value);
                assert!(
                    bound.judge_query(p, &q).is_err(),
                    "{p:?}/{name}: origine malformata in T"
                );
                assert!(admission_gate::query(
                    p,
                    &q,
                    "unused".into(),
                    &["p".into()],
                    std::slice::from_ref(value)
                )
                .is_err());
            }
        }
    }
    // Json, Bytes, integer and bool carry structured Rust values; Time and
    // Interval are not ParameterValue variants and cannot enter this gate.
}

#[test]
fn mariadb_json_is_refused_before_io() {
    for document in [json!({"b":2,"a":1}), json!([1, 2]), json!(1), json!(null)] {
        let value = V::Json(document);
        let bag = ParameterBag::new([("p".into(), value.clone())].into());
        let error = plenora_db_mysql::bind_parameters(&["p".into()], &bag, P::Mariadb).unwrap_err();
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported
        );
        for expr in [
            json!({"kind":"typed_parameter","name":"p","parameter_type":"json"}),
            json!({"kind":"cast","expression":{"kind":"parameter","name":"p"},"target_type":"json"}),
        ] {
            let q = query(&expr);
            assert!(admission_gate::query(
                P::Mariadb,
                &q,
                "unused".into(),
                &["p".into()],
                std::slice::from_ref(&value)
            )
            .is_err());
            assert!(plenora_database_sql::Renderer::new(
                plenora_database_sql::Dialect::Mysql,
                plenora_database_sql::DialectCapabilities {
                    spatial_intersects: false
                }
            )
            .with_provider(P::Mariadb)
            .render_query(&q)
            .is_err());
        }
    }
}
