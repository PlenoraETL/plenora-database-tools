//! Matrice finita dei tipi dichiarati e delle famiglie di bind, senza server.
use plenora_database_core::{
    admission_gate, plan::ProviderKind as P, provider::ParameterValue as V,
    relational::QueryOperation,
};
use serde_json::json;
/// Visita tutte le celle; il binder riceve soltanto i valori ammessi.
///
/// # Panics
/// Una decisione incoerente o un AST della matrice non valido fallisce la prova.
pub fn check(p: P, binder: impl Fn(&str, &V) -> bool) {
    let kinds = [
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
    let values = [
        (
            "null",
            V::Null {
                type_name: "text".into(),
            },
        ),
        ("bool", V::Bool(true)),
        ("i32", V::I32(1)),
        ("i64", V::I64(1)),
        ("f64", V::F64(1.5)),
        ("decimal", V::Decimal("1.5".into())),
        ("string", V::String("abc".into())),
        ("binary", V::Bytes(vec![1, 2])),
        ("date", V::Date("2024-01-01".into())),
        ("timestamp", V::Timestamp("2024-01-01T12:00:00".into())),
        (
            "timestamp_tz",
            V::TimestampTz("2024-01-01T12:00:00+00:00".into()),
        ),
        (
            "uuid",
            V::Uuid("00000000-0000-0000-0000-000000000001".into()),
        ),
        ("json", V::Json(json!({"a":1}))),
        (
            "enum",
            V::Enum {
                type_name: "mood".into(),
                label: "ok".into(),
            },
        ),
    ];

    for kind in kinds {
        for (label, value) in &values {
            let q: QueryOperation = serde_json::from_value(json!({"source":{"object":{"catalog":null,"schema":null,"object":"t"},"alias":null},"projection":[{"expression":{"kind":"typed_parameter","name":"p","parameter_type":kind},"alias":"v"}]})).unwrap();
            let mut bound = plenora_database_core::typed_parameter::BoundValues::default();
            bound.add("p", value);
            let typed = bound.judge_query(p, &q);
            let admitted = admission_gate::query(
                p,
                &q,
                "unused".into(),
                &["p".into()],
                std::slice::from_ref(value),
            );
            assert_eq!(
                typed.is_ok(),
                admitted.is_ok(),
                "{p:?}/{kind}/{label}: T/cancello"
            );
            if let Ok(admitted) = admitted {
                let bound = binder(kind, &admitted.statement().params[0]);
                if p == P::Db2 && kind == "uuid" && label == &"uuid" {
                    // SQLDescribeParam non prova OCTETS/CODEUNITS32: CHAR(36)
                    // si chiude conservativamente a 9 caratteri senza catalogo.
                    assert!(
                        !bound,
                        "Db2 UUID: il descrittore ambiguo deve restare chiuso"
                    );
                } else {
                    assert!(bound, "{p:?}/{kind}/{label}: cancello/binder");
                }
            }
        }
    }
    check_spatial(p, &binder);
}

fn check_spatial(p: P, binder: &impl Fn(&str, &V) -> bool) {
    // Il contesto spatial deve arrivare fino al binder scalare, incluso il
    // percorso transazionale usato dallo SDK: Wkb diventa Bytes validati.
    let query: QueryOperation = serde_json::from_value(json!({"source":null,"projection":[{"expression":{"kind":"spatial_value","expression":{"kind":"typed_parameter","name":"p","parameter_type":"binary"},"srid":4326,"semantics":"geometry"},"alias":"v"}]})).unwrap();
    let bytes = [
        vec![1, 1, 0, 0, 0],
        1_f64.to_le_bytes().to_vec(),
        2_f64.to_le_bytes().to_vec(),
    ]
    .concat();
    for data in [bytes, vec![1, 1, 0, 0, 0]] {
        let valid = data.len() == 21;
        let value = V::Wkb {
            bytes: data,
            srid: Some(4326),
            dimensions: plenora_database_core::geometry::Dimensions::Xy,
            semantics: plenora_database_core::geometry::SpatialSemantics::Geometry,
        };
        let mut bound = plenora_database_core::typed_parameter::BoundValues::default();
        bound.add("p", &value);
        assert_eq!(bound.judge_query(p, &query).is_ok(), valid);
        let admitted = admission_gate::query(p, &query, "unused".into(), &["p".into()], &[value]);
        assert_eq!(admitted.is_ok(), valid);
        if let Ok(admitted) = admitted {
            assert!(
                binder("binary", &admitted.statement().params[0]),
                "{p:?}: WKB spatial/binder"
            );
        }
    }
}
