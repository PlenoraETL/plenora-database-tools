//! Il cancello dell'ammissione (`admission_gate`) su ogni percorso che esegue
//! SQL compilato: la facade portable (SDK `execute_portable_*`, CLI) e il DML
//! relazionale dello SDK (`admission_gate::mutation`).
//!
//! Il finto della transazione ha il catalogo dei tipi del server
//! (`numeric(12, 4)`, `timestamp(0)`, `double precision`) e registra cio che
//! esegue: un valore che il server arrotonderebbe si rifiuta prima
//! dell'esecuzione, un valore esatto si esegue riscritto nel tipo della
//! colonna. Fino al terzo giro della #135 la CLI `portable-execute` e il DML
//! dello SDK eseguivano il compilato senza ammissione.

use plenora_database_core::admission_gate;
use plenora_database_core::facade::execute_portable;
use plenora_database_core::operand_admission::{CatalogType, RelationTypes};
use plenora_database_core::plan::{ObjectRef, ProviderKind};
use plenora_database_core::portable::{Expression, InsertStatement, PortableStatement, TableRef};
use plenora_database_core::provider::{ParameterValue as V, ProviderFuture};
use plenora_database_core::relational::{InsertOperation, MutationOperation, QueryExpression};
use plenora_database_core::row::Row;
use plenora_database_core::transaction::{
    CommitOutcome, ConditionalUpdate, RowStream, Statement, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory, ErrorPhase};

struct Finta {
    eseguiti: Vec<Statement>,
}

fn rifiuto() -> DatabaseError {
    DatabaseError::unsupported(
        ProviderKind::Postgres,
        ErrorPhase::Prepare,
        "non usato dal finto",
    )
}

fn tipo(
    name: &str,
    precision: Option<u64>,
    scale: Option<i64>,
    fraction: Option<u64>,
) -> CatalogType {
    CatalogType {
        name: name.to_owned(),
        precision,
        scale,
        datetime_precision: fraction,
        ..CatalogType::default()
    }
}

impl TransactionScope for Finta {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Postgres
    }

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64> {
        self.eseguiti.push(statement.clone());
        Box::pin(async { Ok(1) })
    }

    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>> {
        self.eseguiti.push(statement.clone());
        Box::pin(async { Ok(Vec::new()) })
    }

    fn query_stream<'a>(
        &'a mut self,
        _statement: &'a Statement,
        _batch_size: u32,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>> {
        Box::pin(async { Err(rifiuto()) })
    }

    fn savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Err(rifiuto()) })
    }

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Err(rifiuto()) })
    }

    fn release_savepoint<'a>(
        &'a mut self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Err(rifiuto()) })
    }

    fn execute_conditional_update<'a>(
        &'a mut self,
        _request: ConditionalUpdate<'a>,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        Box::pin(async { Err(rifiuto()) })
    }

    fn column_types<'a>(
        &'a mut self,
        _relation: &'a (Option<String>, String),
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RelationTypes>> {
        let mut types = RelationTypes::new();
        types.insert("id".to_owned(), tipo("int4", None, None, None));
        types.insert("d".to_owned(), tipo("numeric", Some(12), Some(4), None));
        types.insert("ts".to_owned(), tipo("timestamp", None, None, Some(0)));
        types.insert("f".to_owned(), tipo("float8", None, None, None));
        Box::pin(async move { Ok(Some(types)) })
    }

    fn commit(
        self: Box<Self>,
        _cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome> {
        Box::pin(async { Err(rifiuto()) })
    }

    fn rollback(self: Box<Self>, _cancellation: &CancellationToken) -> ProviderFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    {
        std::task::Poll::Ready(output) => output,
        std::task::Poll::Pending => panic!("il finto e sincrono"),
    }
}

fn portable(column: &str, value: V) -> PortableStatement {
    PortableStatement::Insert(InsertStatement {
        table: TableRef {
            schema: Some("public".to_owned()),
            name: "t".to_owned(),
        },
        columns: vec!["id".to_owned(), column.to_owned()],
        values: vec![vec![
            Expression::Literal(V::I32(1)),
            Expression::Literal(value),
        ]],
        returning: Vec::new(),
    })
}

fn mutation(column: &str) -> MutationOperation {
    MutationOperation::Insert(InsertOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("public".to_owned()),
            object: "t".to_owned(),
        },
        columns: vec!["id".to_owned(), column.to_owned()],
        rows: vec![vec![
            QueryExpression::Parameter {
                name: "id".to_owned(),
            },
            QueryExpression::Parameter {
                name: "value".to_owned(),
            },
        ]],
        returning: Vec::new(),
    })
}

/// I valori che il server arrotonderebbe in silenzio, e quelli esatti con la
/// forma riscritta nel tipo della colonna.
fn casi() -> Vec<(&'static str, V, Option<V>)> {
    vec![
        ("d", V::Decimal("1.23456".to_owned()), None),
        ("d", V::F64(0.1), None),
        ("ts", V::Timestamp("2024-01-01T10:20:30.6".to_owned()), None),
        (
            "d",
            V::Decimal("1.2345".to_owned()),
            Some(V::Decimal("1.2345".to_owned())),
        ),
        ("d", V::F64(0.5), Some(V::Decimal("0.5".to_owned()))),
        (
            "ts",
            V::Timestamp("2024-01-01T10:20:30".to_owned()),
            Some(V::Timestamp("2024-01-01T10:20:30".to_owned())),
        ),
        ("f", V::Decimal("0.5".to_owned()), Some(V::F64(0.5))),
        ("f", V::I64((1 << 53) + 1), None),
    ]
}

#[test]
fn the_portable_facade_admits_every_literal_on_the_column_type() {
    let cancellation = CancellationToken::new();
    for (column, value, admitted) in casi() {
        let mut finta = Finta {
            eseguiti: Vec::new(),
        };
        let outcome = block_on(execute_portable(
            &mut finta,
            &portable(column, value.clone()),
            &cancellation,
        ));
        match admitted {
            None => {
                let error = outcome.expect_err("valore arrotondato dal server");
                assert_eq!(error.category, ErrorCategory::DataMapping, "{value:?}");
                assert!(finta.eseguiti.is_empty(), "{value:?} eseguito");
            }
            Some(admitted) => {
                outcome.expect("valore esatto");
                assert_eq!(finta.eseguiti.len(), 1);
                assert_eq!(finta.eseguiti[0].params[1], admitted, "{value:?}");
            }
        }
    }
}

#[test]
fn the_sdk_mutation_admits_every_value_on_the_column_type() {
    let cancellation = CancellationToken::new();
    for (column, value, admitted) in casi() {
        let mut finta = Finta {
            eseguiti: Vec::new(),
        };
        let outcome = block_on(admission_gate::mutation(
            &mut finta,
            &mutation(column),
            &[V::I32(1), value.clone()],
            &cancellation,
        ));
        match admitted {
            None => {
                let error = outcome.expect_err("valore arrotondato dal server");
                assert_eq!(error.category, ErrorCategory::DataMapping, "{value:?}");
            }
            Some(admitted) => {
                let statement = outcome.expect("valore esatto");
                assert_eq!(
                    statement.statement().params,
                    vec![V::I32(1), admitted],
                    "{value:?}"
                );
            }
        }
    }
    let mut finta = Finta {
        eseguiti: Vec::new(),
    };
    let error = block_on(admission_gate::mutation(
        &mut finta,
        &mutation("d"),
        &[V::I32(1)],
        &cancellation,
    ))
    .expect_err("valori meno dei segnaposto");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
}

/// La seconda lettura del terzo giro: `execute_judged` con un piano
/// `Mutation` eseguiva lo statement ricevuto senza ammissione.
#[test]
fn the_judged_entry_points_admit_every_plan() {
    use plenora_database_core::facade::{execute_judged, query_judged};
    use plenora_database_core::spatial_operand::OperandPlan;
    let cancellation = CancellationToken::new();
    for (column, value, admitted) in casi() {
        let mut finta = Finta {
            eseguiti: Vec::new(),
        };
        let operation = mutation(column);
        let statement = Statement::new("INSERT ...").with_params(vec![V::I32(1), value.clone()]);
        let names =
            plenora_database_core::compile_relational_mutation(ProviderKind::Postgres, &operation)
                .expect("lowering")
                .bind_names;
        let outcome = block_on(execute_judged(
            &mut finta,
            &statement,
            Some(&OperandPlan::Mutation(&operation)),
            &names,
            &cancellation,
        ));
        match admitted {
            None => {
                let error = outcome.expect_err("valore arrotondato dal server");
                assert_eq!(error.category, ErrorCategory::DataMapping, "{value:?}");
                assert!(finta.eseguiti.is_empty(), "{value:?} eseguito");
            }
            Some(admitted) => {
                outcome.expect("valore esatto");
                assert_eq!(finta.eseguiti[0].params, vec![V::I32(1), admitted]);
            }
        }
    }
    // Una query con parametri si ammette con i nomi dei segnaposto che il
    // renderer ha reso (`admission_gate::query`): un valore senza nome non
    // si lega, e la query non si esegue.
    let query: plenora_database_core::relational::QueryOperation = serde_json::from_value(
        serde_json::json!({
            "source": {"object": {"catalog": null, "schema": "public", "object": "t"}, "alias": null},
            "projection": [{"expression": {"kind": "column", "column": {"relation": null, "field": "id"}}, "alias": null}],
            "filter": null,
            "having": null,
            "row_limit": null
        }),
    )
    .expect("query");
    let mut finta = Finta {
        eseguiti: Vec::new(),
    };
    let error = block_on(query_judged(
        &mut finta,
        &Statement::new("SELECT id FROM t WHERE id = $1").with_params(vec![V::I32(1)]),
        Some(&OperandPlan::Query(&query)),
        &[],
        &cancellation,
    ))
    .expect_err("un valore senza segnaposto");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(
        error.message,
        plenora_database_core::spatial_operand::BIND_LAYOUT_MISMATCH
    );
    assert!(finta.eseguiti.is_empty());
}

/// La query relazionale ammette un parametro tipizzato sul tipo del suo
/// `CAST` ([`plenora_database_core::cast_table::rendered_target`]), dopo la
/// regola T sul valore del chiamante: `DECIMAL(38, 10)` arrotonderebbe
/// undici cifre frazionarie.
#[test]
fn a_typed_query_parameter_is_admitted_on_its_cast() {
    let query: plenora_database_core::relational::QueryOperation = serde_json::from_value(
        serde_json::json!({
            "source": {"object": {"catalog": null, "schema": "public", "object": "t"}, "alias": null},
            "projection": [{"expression": {"kind": "column", "column": {"relation": null, "field": "id"}}, "alias": null}],
            "filter": {
                "kind": "compare",
                "left": {"kind": "column", "column": {"relation": null, "field": "id"}},
                "operator": "eq",
                "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "decimal"}
            },
            "having": null,
            "row_limit": null
        }),
    )
    .expect("query");
    let names = ["p".to_owned()];
    let error = admission_gate::query(
        ProviderKind::Postgres,
        &query,
        "SQL".to_owned(),
        &names,
        &[V::Decimal("0.12345678901".to_owned())],
    )
    .expect_err("undici cifre frazionarie in DECIMAL(38, 10)");
    // La regola T (`typed_parameter`) giudica il valore del chiamante prima
    // della riscrittura, e lo rifiuta gia lei.
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(
        error.message,
        plenora_database_core::typed_parameter::TYPED_PARAMETER_MISMATCH
    );
    let admitted = admission_gate::query(
        ProviderKind::Postgres,
        &query,
        "SQL".to_owned(),
        &names,
        &[V::Decimal("0.1234567891".to_owned())],
    )
    .expect("dieci cifre frazionarie");
    assert_eq!(
        admitted.statement().params,
        vec![V::Decimal("0.1234567891".to_owned())]
    );
    // Entrambe le facade devono eseguire il valore ammesso e rifiutare
    // quello inesatto prima che il finto registri uno statement.
    let mut query = query;
    query.filter = None;
    query.projection[0].expression = QueryExpression::TypedParameter {
        name: "p".to_owned(),
        parameter_type: plenora_database_core::relational::QueryParameterType::Decimal,
    };
    for execute in [false, true] {
        for (text, accepted) in [("0.1234567891", true), ("0.12345678901", false)] {
            let mut finta = Finta {
                eseguiti: Vec::new(),
            };
            let statement = Statement::new("SQL").with_params(vec![V::Decimal(text.to_owned())]);
            let plan = plenora_database_core::spatial_operand::OperandPlan::Query(&query);
            let cancellation = CancellationToken::new();
            let result = if execute {
                block_on(plenora_database_core::facade::execute_judged(
                    &mut finta,
                    &statement,
                    Some(&plan),
                    &names,
                    &cancellation,
                ))
                .map(|_| ())
            } else {
                block_on(plenora_database_core::facade::query_judged(
                    &mut finta,
                    &statement,
                    Some(&plan),
                    &names,
                    &cancellation,
                ))
                .map(|_| ())
            };
            assert_eq!(result.is_ok(), accepted, "{result:?}");
            assert_eq!(finta.eseguiti.len(), usize::from(accepted));
            if accepted {
                assert_eq!(finta.eseguiti[0].params, statement.params);
            }
        }
    }
}

#[test]
fn repeated_query_binds_do_not_collapse_signed_zero() {
    let query = serde_json::from_value(serde_json::json!({
        "source": {"object": {"catalog": null, "schema": "public", "object": "t"}, "alias": null},
        "projection": [{"expression": {"kind": "parameter", "name": "p"}, "alias": null}],
        "filter": null, "having": null, "row_limit": null
    }))
    .expect("query");
    for values in [
        vec![V::F64(-0.0), V::F64(0.0)],
        vec![V::F64(0.0), V::F64(-0.0)],
        vec![
            V::Json(serde_json::json!({"nested": [-0.0]})),
            V::Json(serde_json::json!({"nested": [0.0]})),
        ],
    ] {
        let error = admission_gate::query(
            ProviderKind::Postgres,
            &query,
            "SQL".to_owned(),
            &["p".to_owned(), "p".to_owned()],
            &values,
        )
        .expect_err("zeri con segno diverso");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
        assert_eq!(
            error.message,
            "lo stesso parametro della query con valori diversi"
        );
    }
}

/// Un parametro tipizzato confrontato in un DML relazionale si rende
/// `CAST(? AS DECIMAL(38, 10))` con una sentinella (regola T della #133): il
/// cancello vi lega il valore del chiamante ammesso sul tipo del `CAST`.
/// Undici cifre frazionarie il `CAST` le arrotonderebbe: si rifiuta; un
/// valore esatto si lega al suo segnaposto, non la sentinella.
#[test]
fn a_compared_typed_parameter_is_admitted_on_its_cast() {
    let operation: MutationOperation = serde_json::from_value(serde_json::json!({
        "type": "delete",
        "target": {"catalog": null, "schema": "public", "object": "t"},
        "filter": {
            "kind": "compare",
            "left": {"kind": "column", "column": {"relation": null, "field": "dec"}},
            "operator": "eq",
            "right": {"kind": "typed_parameter", "name": "p", "parameter_type": "decimal"}
        }
    }))
    .expect("delete");
    let cancellation = CancellationToken::new();
    let mut finta = Finta {
        eseguiti: Vec::new(),
    };
    let error = block_on(admission_gate::mutation(
        &mut finta,
        &operation,
        &[V::Decimal("0.12345678901".to_owned())],
        &cancellation,
    ))
    .expect_err("undici cifre frazionarie in DECIMAL(38, 10)");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    let admitted = block_on(admission_gate::mutation(
        &mut finta,
        &operation,
        &[V::Decimal("0.5".to_owned())],
        &cancellation,
    ))
    .expect("valore esatto");
    assert_eq!(
        admitted.statement().params,
        vec![V::Decimal("0.5".to_owned())]
    );
    assert!(
        admitted.statement().sql.contains("CAST("),
        "{}",
        admitted.statement().sql
    );
}
