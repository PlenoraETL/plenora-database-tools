use super::*;
use crate::geometry::{Dimensions, SpatialSemantics};

const PG: ProviderKind = ProviderKind::Postgres;

fn wkb_point() -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&1_u32.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes
}

fn ewkb_point(srid: u32) -> Vec<u8> {
    let mut bytes = vec![0x01];
    bytes.extend_from_slice(&0x2000_0001_u32.to_le_bytes());
    bytes.extend_from_slice(&srid.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes.extend_from_slice(&1.0_f64.to_le_bytes());
    bytes
}

fn wkb(bytes: Vec<u8>, srid: Option<u32>) -> ParameterValue {
    ParameterValue::Wkb {
        bytes,
        srid,
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    }
}

fn category(result: Result<impl std::fmt::Debug>) -> ErrorCategory {
    result.expect_err("rifiuto atteso").category
}

/// Una sonda senza SRID non si confronta con niente: era il WKB nudo che
/// `PostGIS` riceveva con SRID 0 e confrontava senza errore.
#[test]
fn a_probe_without_srid_is_refused() {
    assert_eq!(
        category(probe_srid(PG, &ParameterValue::Bytes(wkb_point()), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(
        category(probe_srid(PG, &wkb(wkb_point(), None), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(
        category(probe_srid(PG, &wkb(wkb_point(), Some(0)), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(
        category(probe_srid(PG, &ParameterValue::Bytes(ewkb_point(0)), 32)),
        ErrorCategory::Crs
    );
}

#[test]
fn the_probe_srid_comes_from_the_value_or_the_ewkb() {
    assert_eq!(
        probe_srid(PG, &ParameterValue::Bytes(ewkb_point(3857)), 32).unwrap(),
        Some(3857)
    );
    assert_eq!(
        probe_srid(PG, &wkb(wkb_point(), Some(4326)), 32).unwrap(),
        Some(4326)
    );
    assert_eq!(
        probe_srid(PG, &wkb(ewkb_point(4326), Some(4326)), 32).unwrap(),
        Some(4326)
    );
    assert_eq!(
        category(probe_srid(PG, &wkb(ewkb_point(3857), Some(4326)), 32)),
        ErrorCategory::Crs
    );
    assert_eq!(probe_srid(PG, &ParameterValue::I64(1), 32).unwrap(), None);
}

/// I byte legati portano lo SRID: il server non lo deve indovinare.
#[test]
fn the_bound_probe_carries_its_srid() {
    let (srid, bytes) = stamped_probe(PG, &wkb(wkb_point(), Some(4326)), 32)
        .unwrap()
        .unwrap();
    assert_eq!(srid, 4326);
    assert_eq!(bytes, ewkb_point(4326));
    let (_, untouched) = stamped_probe(PG, &ParameterValue::Bytes(ewkb_point(3857)), 32)
        .unwrap()
        .unwrap();
    assert_eq!(untouched, ewkb_point(3857));
}

/// `ST_SetSRID(ST_GeomFromEWKB(..), 4326)` riscriveva uno SRID 3857
/// incorporato: un valore che ne porta uno diverso si rifiuta.
#[test]
fn a_spatial_value_with_another_srid_is_refused() {
    for value in [
        ParameterValue::Bytes(ewkb_point(3857)),
        wkb(wkb_point(), Some(3857)),
        wkb(ewkb_point(3857), None),
    ] {
        assert_eq!(
            category(check_spatial_value(PG, &value, 4326, 32)),
            ErrorCategory::Crs
        );
    }
    for value in [
        ParameterValue::Bytes(wkb_point()),
        ParameterValue::Bytes(ewkb_point(4326)),
        wkb(wkb_point(), Some(4326)),
        ParameterValue::I64(0),
    ] {
        check_spatial_value(PG, &value, 4326, 32).expect("valore coerente");
    }
}

#[test]
fn frames_must_be_known_and_equal() {
    compare_frames(PG, 4326, Some(4326)).expect("stesso SRID");
    assert_eq!(
        category(compare_frames(PG, 3857, Some(4326))),
        ErrorCategory::Crs
    );
    assert_eq!(category(compare_frames(PG, 4326, None)), ErrorCategory::Crs);
    assert_eq!(
        category(compare_frames(PG, 4326, Some(0))),
        ErrorCategory::Crs
    );
}

/// La regola delle sonde per prodotto, prova, uso e frame, scritta per
/// esteso: la tabella attesa non si ricava dalla regola. Senza catalogo
/// passano soltanto le guardie misurate (`PostgreSQL`, `MySQL`), e la
/// scrittura su SQL Server, che non ha SRID di colonna; un bind nudo non
/// passa mai; una sonda senza SRID e `crs` anche col catalogo.
#[test]
fn the_probe_rule_admits_only_proven_srids() {
    use Admission::{Catalog, Server, ValueCarriesFrame};
    use ProbeEvidence::{Catalog as Cat, CompileOnly as Co};
    use ProbeFrame::{Declared, Missing, Unbound};
    use ProbeUse::{BoundingBox, Function, WriteValue};
    use ProviderKind::{Db2, Mariadb, Mysql, Oracle, Postgres, Sqlserver};
    let refused = Err(ErrorCategory::Unsupported);
    let crs = Err(ErrorCategory::Crs);
    #[allow(clippy::type_complexity)]
    let table: &[(
        ProviderKind,
        ProbeEvidence,
        ProbeUse,
        ProbeFrame,
        std::result::Result<Admission, ErrorCategory>,
    )] = &[
        (
            Postgres,
            Co,
            Function,
            Declared(3857),
            Ok(Server(ServerGuard::PostgresColumnTypmod)),
        ),
        (
            Postgres,
            Co,
            BoundingBox,
            Declared(4326),
            Ok(Server(ServerGuard::PostgresColumnTypmod)),
        ),
        (
            Postgres,
            Co,
            WriteValue,
            Declared(4326),
            Ok(Server(ServerGuard::PostgresTypmodWrite)),
        ),
        (Postgres, Co, BoundingBox, Unbound, refused),
        (Postgres, Cat, BoundingBox, Unbound, Ok(Catalog)),
        (
            Mysql,
            Co,
            Function,
            Declared(3857),
            Ok(Server(ServerGuard::MysqlSrid)),
        ),
        (
            Mysql,
            Co,
            BoundingBox,
            Declared(3857),
            Ok(Server(ServerGuard::MysqlSrid)),
        ),
        (
            Mysql,
            Co,
            WriteValue,
            Declared(3857),
            Ok(Server(ServerGuard::MysqlSrid)),
        ),
        (Mysql, Co, Function, Unbound, refused),
        (Mariadb, Co, Function, Declared(4326), refused),
        (Mariadb, Co, BoundingBox, Declared(4326), refused),
        (Mariadb, Co, WriteValue, Declared(4326), refused),
        (Sqlserver, Co, Function, Declared(4326), refused),
        (Sqlserver, Co, BoundingBox, Declared(4326), refused),
        (
            Sqlserver,
            Co,
            WriteValue,
            Declared(4326),
            Ok(ValueCarriesFrame),
        ),
        (Sqlserver, Cat, Function, Unbound, Ok(Catalog)),
        (Oracle, Co, Function, Declared(4326), refused),
        (Oracle, Co, WriteValue, Declared(4326), refused),
        (Oracle, Cat, Function, Unbound, Ok(Catalog)),
        (Db2, Co, Function, Declared(4326), refused),
        (Db2, Co, WriteValue, Declared(4326), refused),
        (Postgres, Cat, Function, Missing, crs),
        (Mysql, Co, Function, Missing, crs),
        (Sqlserver, Co, WriteValue, Missing, crs),
    ];
    for (provider, evidence, probe_use, frame, expected) in table {
        let observed =
            admit_probe(*provider, *evidence, *probe_use, *frame).map_err(|error| error.category);
        assert_eq!(
            &observed, expected,
            "{provider:?} {evidence:?} {probe_use:?} {frame:?}"
        );
    }
    // Il rifiuto dice perche, senza valori.
    let error = admit_probe(Mariadb, Co, WriteValue, Declared(4326)).expect_err("MariaDB");
    assert_eq!(error.message, VALUE_SRID_UNPROVEN);
    assert_eq!(error.provider, Some(Mariadb));
}

/// La colonna di una sonda si risolve senza catalogo soltanto se nomina una
/// tabella di base senza ambiguita: non una sorgente derivata, non una CTE,
/// non un alias che non c'e.
#[test]
fn a_probe_peer_resolves_only_to_an_unambiguous_base_table() {
    use crate::plan::ObjectRef;
    use crate::relational::{QueryDerivedSource, QueryJoin, QuerySource};
    let source = |object: &str, alias: Option<&str>| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: object.to_owned(),
        },
        alias: alias.map(str::to_owned),
    };
    let column = |relation: Option<&str>| ColumnRef {
        relation: relation.map(str::to_owned),
        field: "g".to_owned(),
    };
    let mut operation: QueryOperation = serde_json::from_value(serde_json::json!({
        "source": null, "projection": [{"expression": {"kind": "wildcard", "relation": null}, "alias": null}],
        "filter": null, "having": null, "row_limit": null, "row_offset": null, "locking": null
    }))
    .expect("operazione minima");
    operation.source = Some(source("strade", Some("s")));
    let scope = RelationScope::of(&operation, &BTreeSet::new());
    assert_eq!(
        scope.base_column(&column(Some("s"))),
        Some(BaseColumn {
            schema: Some("public".to_owned()),
            object: "strade".to_owned(),
            column: "g".to_owned(),
        })
    );
    assert_eq!(scope.base_column(&column(Some("altrove"))), None);
    assert!(scope.base_column(&column(None)).is_some());
    // Un join: senza relazione la colonna e ambigua.
    operation.joins.push(QueryJoin {
        kind: crate::relational::JoinKind::Inner,
        source: Some(source("fiumi", Some("f"))),
        derived_source: None,
        lateral: false,
        on: None,
    });
    let scope = RelationScope::of(&operation, &BTreeSet::new());
    assert_eq!(scope.base_column(&column(None)), None);
    assert_eq!(
        scope.base_column(&column(Some("f"))).map(|c| c.object),
        Some("fiumi".to_owned())
    );
    // Una sorgente derivata con quel nome non e una tabella di base.
    operation.joins[0].source = None;
    operation.joins[0].derived_source = Some(QueryDerivedSource {
        query: Box::new(operation.clone()),
        alias: "f".to_owned(),
    });
    let scope = RelationScope::of(&operation, &BTreeSet::new());
    assert_eq!(scope.base_column(&column(Some("f"))), None);
    // Un nome senza schema che e una CTE della query.
    let mut cte = operation.clone();
    cte.joins.clear();
    cte.source = Some(QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: None,
            object: "vicine".to_owned(),
        },
        alias: None,
    });
    let scope = RelationScope::of(&cte, &BTreeSet::from(["vicine".to_owned()]));
    assert_eq!(scope.base_column(&column(None)), None);
}
