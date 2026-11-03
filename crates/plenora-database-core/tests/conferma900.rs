//! Lettore di conferma PR #145: matrice A900-001 via facade pubblica e sonde JSON.
#![allow(clippy::all, clippy::pedantic)]
use plenora_database_core::admission_gate;
use plenora_database_core::facade::{execute_portable, query_judged};
use plenora_database_core::operand_admission::{CatalogType, RelationTypes};
use plenora_database_core::plan::{ObjectRef, ProviderKind as P};
use plenora_database_core::portable::{
    Expression, InsertStatement, PortableStatement, Predicate, TableRef, UpdateStatement,
    UpsertStatement,
};
use plenora_database_core::provider::{ParameterValue as V, ProviderFuture};
use plenora_database_core::relational::{InsertOperation, MutationOperation, QueryExpression};
use plenora_database_core::row::Row;
use plenora_database_core::spatial_operand::{
    CatalogColumn, ColumnOrigin, OperandCatalog, OperandFamily, OperandPlan, RelationColumns,
};
use plenora_database_core::transaction::{
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase};
use serde_json::json;

struct Finta {
    provider: P,
    types: RelationTypes,
    catalog: Vec<CatalogColumn>,
    eseguiti: Vec<Statement>,
}

fn rifiuto(p: P) -> DatabaseError {
    DatabaseError::unsupported(p, ErrorPhase::Prepare, "finto")
}

impl TransactionScope for Finta {
    fn provider_kind(&self) -> P {
        self.provider
    }
    fn execute<'a>(
        &'a mut self,
        s: &'a Statement,
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        self.eseguiti.push(s.clone());
        Box::pin(async { Ok(1) })
    }
    fn query<'a>(
        &'a mut self,
        s: &'a Statement,
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        self.eseguiti.push(s.clone());
        Box::pin(async { Ok(Vec::new()) })
    }
    fn query_stream<'a>(
        &'a mut self,
        _s: &'a Statement,
        _b: u32,
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        let p = self.provider;
        Box::pin(async move { Err(rifiuto(p)) })
    }
    fn savepoint<'a>(
        &'a mut self,
        _n: &'a str,
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let p = self.provider;
        Box::pin(async move { Err(rifiuto(p)) })
    }
    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _n: &'a str,
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let p = self.provider;
        Box::pin(async move { Err(rifiuto(p)) })
    }
    fn release_savepoint<'a>(
        &'a mut self,
        _n: &'a str,
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let p = self.provider;
        Box::pin(async move { Err(rifiuto(p)) })
    }
    fn execute_conditional_update<'a>(
        &'a mut self,
        _r: ConditionalUpdate<'a>,
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        let p = self.provider;
        Box::pin(async move { Err(rifiuto(p)) })
    }
    fn column_types<'a>(
        &'a mut self,
        _r: &'a (Option<String>, String),
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RelationTypes>> {
        let t = self.types.clone();
        Box::pin(async move { Ok(Some(t)) })
    }
    fn operand_catalog<'a>(
        &'a mut self,
        _r: &'a [(Option<String>, String)],
        _n: &'a [String],
        _c: &'a CancellationToken,
    ) -> ProviderFuture<'a, OperandCatalog> {
        let mut cat = OperandCatalog::new(self.provider);
        cat.insert(
            Some("s".into()),
            "t".into(),
            RelationColumns::Known(self.catalog.clone()),
        );
        Box::pin(async move { Ok(cat) })
    }
    fn commit(self: Box<Self>, _c: &CancellationToken) -> ProviderFuture<'_, CommitOutcome> {
        let p = self.provider;
        Box::pin(async move { Err(rifiuto(p)) })
    }
    fn rollback(self: Box<Self>, _c: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    let mut f = Box::pin(f);
    match f
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(o) => o,
        std::task::Poll::Pending => panic!("sincrono"),
    }
}

fn ct(
    name: &str,
    length: Option<u64>,
    precision: Option<u64>,
    scale: Option<i64>,
    dp: Option<u64>,
) -> CatalogType {
    CatalogType {
        name: name.into(),
        length,
        precision,
        scale,
        datetime_precision: dp,
        ..CatalogType::default()
    }
}

/// Tipi di colonna per provider: (nome colonna, tipo catalogo).
fn columns(p: P) -> Vec<(&'static str, CatalogType)> {
    let mut v = vec![(
        "id",
        match p {
            P::Postgres => ct("int4", None, None, None, None),
            P::Oracle => ct("number", None, Some(10), Some(0), None),
            P::Sqlserver => ct("int", None, None, None, None),
            _ => ct("integer", None, None, None, None),
        },
    )];
    let more: Vec<(&'static str, CatalogType)> = match p {
        P::Postgres => vec![
            ("c_text", ct("text", None, None, None, None)),
            ("c_varchar", ct("varchar", Some(100), None, None, None)),
            ("c_bpchar", ct("bpchar", Some(100), None, None, None)),
            ("c_name", ct("name", None, None, None, None)),
            ("c_date", ct("date", None, None, None, None)),
            ("c_ts", ct("timestamp", None, None, None, Some(6))),
            ("c_tstz", ct("timestamptz", None, None, None, Some(6))),
            ("c_uuid", ct("uuid", None, None, None, None)),
            ("c_num", ct("numeric", None, Some(38), Some(10), None)),
            ("c_numu", ct("numeric", None, None, None, None)),
            ("c_int8", ct("int8", None, None, None, None)),
            ("c_f8", ct("float8", None, None, None, None)),
            ("c_json", ct("jsonb", None, None, None, None)),
            ("c_enum", ct("enum", None, None, None, None)),
        ],
        P::Mysql | P::Mariadb => vec![
            ("c_text", ct("longtext", None, None, None, None)),
            ("c_varchar", ct("varchar", Some(100), None, None, None)),
            ("c_char", ct("char", Some(100), None, None, None)),
            ("c_date", ct("date", None, None, None, None)),
            ("c_ts", ct("datetime", None, None, None, Some(6))),
            ("c_ts2", ct("timestamp", None, None, None, Some(6))),
            ("c_num", ct("decimal", None, Some(38), Some(10), None)),
            ("c_int8", ct("bigint", None, None, None, None)),
            ("c_f8", ct("double", None, None, None, None)),
            ("c_json", ct("json", None, None, None, None)),
        ],
        P::Sqlserver => vec![
            ("c_text", ct("nvarchar", None, None, None, None)),
            ("c_varchar", ct("varchar", Some(100), None, None, None)),
            ("c_char", ct("nchar", Some(100), None, None, None)),
            ("c_date", ct("date", None, None, None, None)),
            ("c_ts", ct("datetime2", None, None, Some(7), None)),
            ("c_tstz", ct("datetimeoffset", None, None, Some(7), None)),
            ("c_uuid", ct("uniqueidentifier", None, None, None, None)),
            ("c_num", ct("decimal", None, Some(38), Some(10), None)),
            ("c_int8", ct("bigint", None, None, None, None)),
            ("c_f8", ct("float", None, Some(53), None, None)),
        ],
        P::Oracle => vec![
            ("c_varchar", ct("varchar2", Some(100), None, None, None)),
            ("c_char", ct("char", Some(100), None, None, None)),
            ("c_date", ct("date", None, None, None, None)),
            ("c_ts", ct("timestamp(6)", None, None, Some(6), None)),
            (
                "c_tstz",
                ct("timestamp(6) with time zone", None, None, Some(6), None),
            ),
            ("c_num", ct("number", None, Some(38), Some(10), None)),
            ("c_numu", ct("number", None, None, None, None)),
            ("c_f8", ct("binary_double", None, None, None, None)),
            ("c_json", ct("json", None, None, None, None)),
        ],
        P::Db2 => vec![
            ("c_varchar", ct("varchar", Some(100), None, None, None)),
            ("c_clob", ct("clob", Some(1000), None, None, None)),
            ("c_char", ct("character", Some(100), None, None, None)),
            ("c_date", ct("date", None, None, None, None)),
            ("c_ts", ct("timestamp", None, None, Some(6), None)),
            ("c_num", ct("decimal", None, Some(31), Some(10), None)),
            ("c_df", ct("decfloat", None, Some(34), None, None)),
            ("c_int8", ct("bigint", None, None, None, None)),
            ("c_f8", ct("double", None, None, None, None)),
        ],
        _ => vec![],
    };
    v.extend(more);
    v
}

fn finta(p: P) -> Finta {
    let mut types = RelationTypes::new();
    for (n, t) in columns(p) {
        types.insert(n.into(), t);
    }
    Finta {
        provider: p,
        types,
        catalog: Vec::new(),
        eseguiti: Vec::new(),
    }
}

fn malformed() -> Vec<V> {
    let mut out = Vec::new();
    for t in [
        "",
        "payload",
        "2024-02-30",
        "2023-02-29",
        "2024-13-01",
        "2024-00-10",
        "2024-01-00",
        "2024-01-01x",
        "2024-1-01",
        "2024-01-01\0",
        "2024-01-01T00:00:00",
        " 2024-01-01",
        "2024-01-01 ",
        "+2024-01-01",
        "20240101",
        "2024/01/01",
        "\u{ff12}024-01-01",
        "2024-01-01Z",
    ] {
        out.push(V::Date(t.into()));
    }
    for t in [
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
        "2024-01-01T00:00",
        "2024-01-01T00:00:60",
        "2024-01-01T00:60:00",
        "2024-01-01T0:00:00",
        "2024-01-01T00:00:00 ",
        " 2024-01-01T00:00:00",
        "2024-01-01T00:00:00\n",
        "2024-01-01X00:00:00",
        "2024-01-01T00:00:00.123456789",
        "2024-01-01T00:00:00+0100",
        "2024-01-01T00:00:00+01",
        "2024-01-01T00:00:00-24:00",
        "2024-01-01T00:00:00+01:60",
        "2024-01-01T00:00:00.1.2",
        "2024-01-01T00:00:00,5",
        "2024-01-01",
        "2024-01-01T",
        "2024-01-01T00:00:00\u{a0}",
        "2024-01-01T00:00:00UTC",
        "2024-01-01T00:00:00+01:00:00",
        "0000-01-01T00:00:00Q",
    ] {
        out.push(V::Timestamp(t.into()));
        out.push(V::TimestampTz(t.into()));
    }
    // Forme valide per un tipo e non per l'altro.
    out.push(V::Timestamp("2024-01-01T00:00:00Z".into()));
    out.push(V::Timestamp("2024-01-01T00:00:00+01:00".into()));
    for t in [
        "", "+", "-", ".", "1.2x", "1..0", "NaN", "inf", "1e0", "1E5", "1\0", " 1", "1 ", "1,0",
        "0x10", "\u{661}", "--1", "+-1", "1_000", "١.٥",
    ] {
        out.push(V::Decimal(t.into()));
    }
    for t in [
        "",
        "00000000000000000000000000000000",
        "00000000-0000-0000-0000-00000000000x",
        "{00000000-0000-0000-0000-000000000000}",
        "00000000-0000-0000-0000-0000000000000",
        " 00000000-0000-0000-0000-000000000000",
        "urn:uuid:00000000-0000-0000-0000-000000000000",
        "0000000-00000-0000-0000-000000000000",
    ] {
        out.push(V::Uuid(t.into()));
    }
    out.extend([
        V::String("bad\0text".into()),
        V::Enum {
            type_name: "mood".into(),
            label: "bad\0label".into(),
        },
        V::Null {
            type_name: String::new(),
        },
        V::Null {
            type_name: "bad\0type".into(),
        },
    ]);
    out
}

fn insert(col: &str, value: V) -> PortableStatement {
    PortableStatement::Insert(InsertStatement {
        table: TableRef {
            schema: Some("s".into()),
            name: "t".into(),
        },
        columns: vec!["id".into(), col.into()],
        values: vec![vec![
            Expression::Literal(V::I32(1)),
            Expression::Literal(value),
        ]],
        returning: Vec::new(),
    })
}

fn update(col: &str, value: V) -> PortableStatement {
    PortableStatement::Update(UpdateStatement {
        table: TableRef {
            schema: Some("s".into()),
            name: "t".into(),
        },
        assignments: vec![(col.into(), Expression::Literal(value))],
        filter: None,
        returning: Vec::new(),
    })
}

fn upsert(col: &str, value: V) -> PortableStatement {
    PortableStatement::Upsert(UpsertStatement {
        table: TableRef {
            schema: Some("s".into()),
            name: "t".into(),
        },
        columns: vec!["id".into(), col.into()],
        values: vec![vec![
            Expression::Literal(V::I32(1)),
            Expression::Literal(value.clone()),
        ]],
        conflict_target: vec![],
        update_on_conflict: vec![(col.into(), Expression::Literal(value))],
        returning: Vec::new(),
    })
}

fn mutation(col: &str) -> MutationOperation {
    MutationOperation::Insert(InsertOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("s".into()),
            object: "t".into(),
        },
        columns: vec!["id".into(), col.into()],
        rows: vec![vec![
            QueryExpression::Parameter { name: "id".into() },
            QueryExpression::Parameter {
                name: "value".into(),
            },
        ]],
        returning: Vec::new(),
    })
}

const PROVIDERS: [P; 6] = [
    P::Postgres,
    P::Mysql,
    P::Mariadb,
    P::Sqlserver,
    P::Oracle,
    P::Db2,
];

#[test]
fn a900_001_matrice_facade_malformati() {
    let c = CancellationToken::new();
    let mut accettati = Vec::new();
    let mut celle = 0usize;
    for p in PROVIDERS {
        for (col, _) in columns(p).into_iter().skip(1) {
            for value in malformed() {
                for (shape, st) in [
                    ("insert", insert(col, value.clone())),
                    ("update", update(col, value.clone())),
                    ("upsert", upsert(col, value.clone())),
                ] {
                    celle += 1;
                    let mut f = finta(p);
                    let out = block_on(execute_portable(&mut f, &st, &c));
                    if out.is_ok() || !f.eseguiti.is_empty() {
                        accettati.push(format!(
                            "{p:?}/{shape}/{col}/{value:?} -> {:?}",
                            f.eseguiti.first().map(|s| s.params.clone())
                        ));
                    }
                }
                celle += 1;
                let mut f = finta(p);
                let out = block_on(admission_gate::mutation(
                    &mut f,
                    &mutation(col),
                    &[V::I32(1), value.clone()],
                    &c,
                ));
                if let Ok(a) = out {
                    accettati.push(format!(
                        "{p:?}/mutation/{col}/{value:?} -> {:?}",
                        a.statement().params
                    ));
                }
            }
        }
    }
    assert_eq!(celle, 28_272);
    assert!(
        accettati.is_empty(),
        "malformati ammessi:\n{}",
        accettati.join("\n")
    );
}

/// Controllo positivo: forme valide restano ammesse (nessuna perdita per rifiuto).
#[test]
fn a900_001_valide_ammesse() {
    let c = CancellationToken::new();
    let mut esiti = Vec::new();
    let cases: Vec<(&str, V)> = vec![
        ("c_date", V::Date("2024-02-29".into())),
        ("c_ts", V::Timestamp("2024-01-01T10:20:30.123456".into())),
        ("c_ts", V::Timestamp("2024-01-01 10:20:30".into())),
        ("c_date", V::Timestamp("2024-01-01T00:00:00".into())),
        ("c_tstz", V::TimestampTz("2024-01-01T10:20:30+01:00".into())),
        ("c_tstz", V::TimestampTz("2024-01-01T10:20:30Z".into())),
        ("c_varchar", V::Timestamp("2024-01-01T10:20:30".into())),
        (
            "c_varchar",
            V::Uuid("123E4567-E89B-12D3-A456-426614174000".into()),
        ),
        ("c_varchar", V::Decimal("+1.50".into())),
        ("c_num", V::Decimal(".5".into())),
        ("c_num", V::Decimal("5.".into())),
        ("c_num", V::Decimal("-0".into())),
        (
            "c_uuid",
            V::Uuid("123e4567-e89b-12d3-a456-426614174000".into()),
        ),
    ];
    for p in PROVIDERS {
        let cols: Vec<_> = columns(p).into_iter().map(|(n, _)| n).collect();
        for (col, value) in &cases {
            if !cols.contains(col) {
                continue;
            }
            let mut f = finta(p);
            let out = block_on(execute_portable(&mut f, &insert(col, value.clone()), &c));
            if matches!(*col, "c_date" | "c_ts" | "c_uuid") {
                assert!(out.is_ok(), "{p:?}/{col}: {out:?}");
            }
            esiti.push(format!(
                "{p:?}/{col}/{value:?} -> {}",
                match out {
                    Ok(_) => format!("OK {:?}", f.eseguiti[0].params[1]),
                    Err(e) => format!("ERR {:?} {}", e.category, e.message),
                }
            ));
        }
    }
    eprintln!("{}", esiti.join("\n"));
}

fn json_catalog(p: P, identity: &str) -> Vec<CatalogColumn> {
    vec![
        CatalogColumn::new("id", ColumnOrigin::Stored, OperandFamily::Numeric)
            .with_type(ct("int", None, None, None, None), "int"),
        CatalogColumn::new(
            "payload",
            ColumnOrigin::Stored,
            if identity == "json" {
                OperandFamily::Other
            } else {
                OperandFamily::Text
            },
        )
        .with_type(ct(identity, None, None, None, None), identity),
    ]
    .into_iter()
    .map(|c| {
        let _ = p;
        c
    })
    .collect()
}

/// JSON MySQL/MariaDB: confronti portable e relazionali, IS NULL e proiezione.
#[test]
fn json_superfici_composte() {
    let c = CancellationToken::new();
    let mut righe = Vec::new();
    for p in [P::Mysql, P::Mariadb] {
        for value in [
            V::String("{\"b\":2,\"a\":1}".into()),
            V::Json(json!({"b":2,"a":1})),
            V::Null {
                type_name: "unknown".into(),
            },
            V::I64(1),
        ] {
            // portable WHERE payload = value (UPDATE e DELETE) con colonna JSON
            for (label, st) in [
                (
                    "portable.update",
                    PortableStatement::Update(UpdateStatement {
                        table: TableRef {
                            schema: Some("s".into()),
                            name: "t".into(),
                        },
                        assignments: vec![("id".into(), Expression::Literal(V::I32(2)))],
                        filter: Some(Predicate::Eq {
                            column: "payload".into(),
                            value: Expression::Literal(value.clone()),
                        }),
                        returning: Vec::new(),
                    }),
                ),
                (
                    "portable.in",
                    PortableStatement::Update(UpdateStatement {
                        table: TableRef {
                            schema: Some("s".into()),
                            name: "t".into(),
                        },
                        assignments: vec![("id".into(), Expression::Literal(V::I32(2)))],
                        filter: Some(Predicate::In {
                            column: "payload".into(),
                            values: vec![Expression::Literal(value.clone())],
                        }),
                        returning: Vec::new(),
                    }),
                ),
            ] {
                let mut f = finta(p);
                // MariaDB: column_types dice longtext; il catalogo operandi dice json.
                if p == P::Mariadb {
                    f.types
                        .insert("payload".into(), ct("longtext", None, None, None, None));
                } else {
                    f.types
                        .insert("payload".into(), ct("json", None, None, None, None));
                }
                f.catalog = json_catalog(p, "json");
                let out = block_on(execute_portable(&mut f, &st, &c));
                assert!(out.is_err());
                assert!(f.eseguiti.is_empty());
                righe.push(format!(
                    "{p:?}/{label}/{value:?} -> {}",
                    match out {
                        Ok(_) => format!("ESEGUITO {:?}", f.eseguiti[0].params),
                        Err(e) => format!("rifiuto {:?}", e.category),
                    }
                ));
            }
        }
        // Relazionale: payload IS NULL, proiezione, confronto.
        for (label, q) in [
            (
                "rel.is_null",
                json!({"kind":"is_null","expression":{"kind":"column","column":{"relation":null,"field":"payload"}},"negated":false}),
            ),
            (
                "rel.eq_param",
                json!({"kind":"compare","left":{"kind":"column","column":{"relation":null,"field":"payload"}},"operator":"eq","right":{"kind":"parameter","name":"p"}}),
            ),
            (
                "rel.eq_typed_json",
                json!({"kind":"compare","left":{"kind":"column","column":{"relation":null,"field":"payload"}},"operator":"eq","right":{"kind":"typed_parameter","name":"p","parameter_type":"json"}}),
            ),
            (
                "rel.eq_typed_string",
                json!({"kind":"compare","left":{"kind":"column","column":{"relation":null,"field":"payload"}},"operator":"eq","right":{"kind":"typed_parameter","name":"p","parameter_type":"string"}}),
            ),
        ] {
            let query: Result<plenora_database_core::relational::QueryOperation, _> =
                serde_json::from_value(json!({
                    "source": {"object": {"catalog": null, "schema": "s", "object": "t"}, "alias": null},
                    "projection": [{"expression": {"kind": "column", "column": {"relation": null, "field": "payload"}}, "alias": null}],
                    "filter": q, "having": null, "row_limit": null
                }));
            let query = query.expect("piano di regressione valido");
            let mut f = finta(p);
            f.catalog = json_catalog(p, "json");
            let needs = label != "rel.is_null";
            let names: Vec<String> = if needs { vec!["p".into()] } else { vec![] };
            let params = if needs {
                vec![if label == "rel.eq_typed_json" {
                    V::Json(json!({"a":1}))
                } else {
                    V::String("{\"a\":1}".into())
                }]
            } else {
                vec![]
            };
            let st = Statement::new("SQL").with_params(params);
            let out = block_on(query_judged(
                &mut f,
                &st,
                Some(&OperandPlan::Query(&query)),
                &names,
                &c,
            ));
            if needs {
                assert!(out.is_err());
                assert!(f.eseguiti.is_empty());
            } else {
                assert!(out.is_ok());
            }
            righe.push(format!(
                "{p:?}/{label} -> {}",
                match out {
                    Ok(_) => format!("ESEGUITO {:?}", f.eseguiti[0].params),
                    Err(e) => format!("rifiuto {:?} {}", e.category, e.message),
                }
            ));
        }
        // Solo proiezione.
        let query: plenora_database_core::relational::QueryOperation = serde_json::from_value(json!({
            "source": {"object": {"catalog": null, "schema": "s", "object": "t"}, "alias": null},
            "projection": [{"expression": {"kind": "column", "column": {"relation": null, "field": "payload"}}, "alias": null}],
            "filter": null, "having": null, "row_limit": null
        })).unwrap();
        let mut f = finta(p);
        f.catalog = json_catalog(p, "json");
        let out = block_on(query_judged(
            &mut f,
            &Statement::new("SQL"),
            Some(&OperandPlan::Query(&query)),
            &[],
            &c,
        ));
        assert!(out.is_ok());
        righe.push(format!(
            "{p:?}/rel.proiezione -> {}",
            match out {
                Ok(_) => "ESEGUITO".to_string(),
                Err(e) => format!("rifiuto {:?} {}", e.category, e.message),
            }
        ));
    }
    eprintln!("{}", righe.join("\n"));
    for r in &righe {
        if r.contains("portable") || r.contains("rel.eq") {
            assert!(
                !r.contains("ESEGUITO") || r.contains("Null"),
                "confronto JSON eseguito: {r}"
            );
        }
    }
}

/// MySQL: un parametro nudo con lo stesso nome di un parametro tipizzato JSON.
#[test]
fn mysql_parametro_nudo_condivide_nome_con_json_tipizzato() {
    let q: plenora_database_core::relational::QueryOperation = serde_json::from_value(json!({
        "source": {"object": {"catalog": null, "schema": null, "object": "t"}, "alias": null},
        "projection": [
            {"expression": {"kind":"typed_parameter","name":"p","parameter_type":"json"}, "alias": "a"},
            {"expression": {"kind":"parameter","name":"p"}, "alias": "b"}
        ],
        "filter": null, "having": null, "row_limit": null
    })).unwrap();
    let out = admission_gate::query(
        P::Mysql,
        &q,
        "SELECT CAST(? AS JSON), ? FROM t".into(),
        &["p".into(), "p".into()],
        &[V::Json(json!({"a":1})), V::Json(json!({"a":1}))],
    );
    assert!(out.is_err());
    eprintln!(
        "nudo+tipizzato: {:?}",
        out.as_ref()
            .map(|a| a.statement().params.clone())
            .map_err(|e| e.message.clone())
    );
}

#[test]
fn wkb_tipizzato_srid_diverso_dal_piano() {
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    let c = CancellationToken::new();
    let q: plenora_database_core::relational::QueryOperation = serde_json::from_value(json!({
        "source": {"object": {"catalog": null, "schema": "s", "object": "t"}, "alias": null},
        "projection": [{"expression": {"kind":"spatial_value","expression":{"kind":"typed_parameter","name":"p","parameter_type":"binary"},"srid":4326,"semantics":"geometry"}, "alias": "v"}],
        "filter": null, "having": null, "row_limit": null
    })).unwrap();
    let mut bytes = vec![1, 1, 0, 0, 0];
    bytes.extend(1f64.to_le_bytes());
    bytes.extend(2f64.to_le_bytes());
    for (srid, dims) in [
        (Some(3857), Dimensions::Xy),
        (None, Dimensions::Xy),
        (Some(4326), Dimensions::Xyz),
    ] {
        let v = V::Wkb {
            bytes: bytes.clone(),
            srid,
            dimensions: dims,
            semantics: SpatialSemantics::Geometry,
        };
        for p in PROVIDERS {
            let a =
                admission_gate::query(p, &q, "SQL".into(), &["p".into()], std::slice::from_ref(&v));
            let mut f = finta(p);
            let st = Statement::new("SQL").with_params(vec![v.clone()]);
            let j = block_on(query_judged(
                &mut f,
                &st,
                Some(&OperandPlan::Query(&q)),
                &["p".into()],
                &c,
            ));
            if srid == Some(3857) || dims == Dimensions::Xyz {
                assert!(a.is_err());
                assert!(j.is_err());
                assert!(f.eseguiti.is_empty());
                if srid == Some(3857) {
                    assert_eq!(
                        a.as_ref().unwrap_err().category,
                        plenora_database_core::ErrorCategory::Crs
                    );
                    assert_eq!(
                        j.as_ref().unwrap_err().category,
                        plenora_database_core::ErrorCategory::Crs
                    );
                }
            } else {
                assert!(a.is_ok());
            }
            eprintln!(
                "WKB srid={srid:?} {dims:?} {p:?}: gate={:?} judged={:?} eseguiti={:?}",
                a.as_ref()
                    .map(|a| a.statement().params.clone())
                    .map_err(|e| e.message.clone()),
                j.as_ref().map(|_| ()).map_err(|e| e.message.clone()),
                f.eseguiti.first().map(|s| s.params.clone())
            );
        }
    }
}

#[test]
fn wkb_tipizzato_srid_diverso_in_intersects() {
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    let c = CancellationToken::new();
    let q: plenora_database_core::relational::QueryOperation = serde_json::from_value(json!({
        "source": {"object": {"catalog": null, "schema": "s", "object": "t"}, "alias": null},
        "projection": [{"expression": {"kind": "column", "column": {"relation": null, "field": "id"}}, "alias": null}],
        "filter": {"kind":"spatial","function":"intersects","arguments":[
            {"kind": "column", "column": {"relation": null, "field": "g"}},
            {"kind":"spatial_value","expression":{"kind":"typed_parameter","name":"p","parameter_type":"binary"},"srid":4326,"semantics":"geometry"}
        ]},
        "having": null, "row_limit": null
    })).unwrap();
    let mut bytes = vec![1, 1, 0, 0, 0];
    bytes.extend(1f64.to_le_bytes());
    bytes.extend(2f64.to_le_bytes());
    let v = V::Wkb {
        bytes,
        srid: Some(3857),
        dimensions: Dimensions::Xy,
        semantics: SpatialSemantics::Geometry,
    };
    for p in PROVIDERS {
        let mut f = finta(p);
        f.catalog = vec![
            CatalogColumn::new("id", ColumnOrigin::Stored, OperandFamily::Numeric)
                .with_type(ct("int", None, None, None, None), "int"),
            CatalogColumn::new("g", ColumnOrigin::Stored, OperandFamily::Geometry)
                .with_type(ct("geometry", None, None, None, None), "geometry"),
        ];
        let st = Statement::new("SQL").with_params(vec![v.clone()]);
        let j = block_on(query_judged(
            &mut f,
            &st,
            Some(&OperandPlan::Query(&q)),
            &["p".into()],
            &c,
        ));
        assert_eq!(
            j.as_ref().unwrap_err().category,
            plenora_database_core::ErrorCategory::Crs
        );
        assert!(f.eseguiti.is_empty());
        eprintln!(
            "INTERSECTS {p:?}: judged={:?} eseguiti={:?}",
            j.as_ref()
                .map(|_| ())
                .map_err(|e| (e.category, e.message.clone())),
            f.eseguiti.first().map(|s| s.params.clone())
        );
    }
}
