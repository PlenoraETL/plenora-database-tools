#![allow(warnings, clippy::all, clippy::pedantic, clippy::nursery)]
// =====================================================================
// MISURA TEMPORANEA 8.0.4 — solo nel ramo `misura/804`, mai in main.
// Stampa gli esiti con prefisso `MISURA`: valore inviato, osservato per
// percorso (TransactionScope::query, query_stream, lettura Arrow) e oracolo
// del server. Non asserisce nulla: misura.
// =====================================================================

use plenora_database_core::arrow::array::{
    Array, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use plenora_database_core::plan::{ComparisonOperator, ObjectRef, OrderBy, ReadOperation, SortDirection};
use plenora_database_core::provider::{BatchStream, ParameterBag, Provider, SecretString};
use plenora_database_core::query::{
    ColumnRef, JoinKind, QueryExpression, QueryJoin, QueryOperation, QueryProjection, QuerySource,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, DatabaseError};

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget misura")
}

fn errore(error: &DatabaseError) -> String {
    format!(
        "ERRORE categoria={:?} fase={:?} messaggio={}",
        error.category, error.phase, error.message
    )
}

async fn ddl<P: Provider>(provider: &P, secret: &SecretString, label: &str, sql: &str) {
    let cancellation = CancellationToken::new();
    match provider.execute_ddl(secret, sql, &cancellation).await {
        Ok(()) => println!("MISURA {label} ddl ok: {sql}"),
        Err(error) => println!("MISURA {label} ddl {}: {sql}", errore(&error)),
    }
}

async fn dml<P: Provider>(provider: &P, secret: &SecretString, label: &str, statements: &[String]) {
    let cancellation = CancellationToken::new();
    let budget = budget();
    let mut transaction = match provider
        .begin_transaction(secret, &TransactionOptions::default(), &budget, &cancellation)
        .await
    {
        Ok(transaction) => transaction,
        Err(error) => {
            println!("MISURA {label} dml begin {}", errore(&error));
            return;
        }
    };
    for sql in statements {
        match transaction.execute(&Statement::new(sql.clone()), &cancellation).await {
            Ok(rows) => println!("MISURA {label} dml ok righe={rows}: {sql}"),
            Err(error) => println!("MISURA {label} dml {}: {sql}", errore(&error)),
        }
    }
    match transaction.commit(&cancellation).await {
        Ok(outcome) => println!("MISURA {label} dml commit {outcome:?}"),
        Err(error) => println!("MISURA {label} dml commit {}", errore(&error)),
    }
}

fn dump_row(label: &str, path: &str, index: usize, row: &plenora_database_core::row::Row) {
    println!(
        "MISURA {label} {path} riga[{index}] colonne={:?} valori={:?}",
        row.columns(),
        row.values()
    );
    let mut seen: Vec<String> = Vec::new();
    for name in row.columns() {
        if !seen.contains(name) {
            seen.push(name.clone());
            println!(
                "MISURA {label} {path} riga[{index}] get({name:?})={:?}",
                row.get(name)
            );
        }
    }
}

/// Query transazionale: `query` e `query_stream`, ciascuna in una
/// transazione propria perche un errore dell'una non avveleni l'altra.
async fn tx_query<P: Provider>(provider: &P, secret: &SecretString, label: &str, sql: &str) {
    println!("MISURA {label} sql inviato: {sql}");
    let cancellation = CancellationToken::new();
    let budget = budget();
    let statement = Statement::new(sql.to_owned());
    match provider
        .begin_transaction(secret, &TransactionOptions::default(), &budget, &cancellation)
        .await
    {
        Err(error) => println!("MISURA {label} tx.query begin {}", errore(&error)),
        Ok(mut transaction) => {
            match transaction.query(&statement, &cancellation).await {
                Ok(rows) => {
                    println!("MISURA {label} tx.query ok righe={}", rows.len());
                    for (index, row) in rows.iter().enumerate() {
                        dump_row(label, "tx.query", index, row);
                    }
                }
                Err(error) => println!("MISURA {label} tx.query {}", errore(&error)),
            }
            let _ = transaction.rollback(&cancellation).await;
        }
    }
    match provider
        .begin_transaction(secret, &TransactionOptions::default(), &budget, &cancellation)
        .await
    {
        Err(error) => println!("MISURA {label} tx.query_stream begin {}", errore(&error)),
        Ok(mut transaction) => {
            {
                match transaction.query_stream(&statement, 64, &cancellation).await {
                    Err(error) => {
                        println!("MISURA {label} tx.query_stream apertura {}", errore(&error));
                    }
                    Ok(mut stream) => {
                        let mut index = 0;
                        loop {
                            match stream.next_batch(&cancellation).await {
                                Ok(Some(rows)) => {
                                    for row in &rows {
                                        dump_row(label, "tx.query_stream", index, row);
                                        index += 1;
                                    }
                                }
                                Ok(None) => {
                                    println!("MISURA {label} tx.query_stream fine righe={index}");
                                    break;
                                }
                                Err(error) => {
                                    println!(
                                        "MISURA {label} tx.query_stream batch {}",
                                        errore(&error)
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            let _ = transaction.rollback(&cancellation).await;
        }
    }
}

async fn arrow_dump(
    label: &str,
    result: plenora_database_core::Result<Box<dyn BatchStream>>,
) {
    let cancellation = CancellationToken::new();
    match result {
        Err(error) => println!("MISURA {label} arrow apertura {}", errore(&error)),
        Ok(mut stream) => {
            let schema = stream.schema();
            let fields: Vec<String> = schema
                .fields()
                .iter()
                .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
                .collect();
            println!("MISURA {label} arrow schema={fields:?}");
            loop {
                match stream.next_batch(&cancellation).await {
                    Ok(Some(batch)) => {
                        println!("MISURA {label} arrow batch righe={}", batch.num_rows());
                        let batch_schema = batch.schema();
                        for (index, column) in batch.columns().iter().enumerate() {
                            let raw = if let Some(values) =
                                column.as_any().downcast_ref::<TimestampMicrosecondArray>()
                            {
                                format!(" grezzi_us={:?}", values.values().to_vec())
                            } else if let Some(values) =
                                column.as_any().downcast_ref::<TimestampNanosecondArray>()
                            {
                                format!(" grezzi_ns={:?}", values.values().to_vec())
                            } else {
                                String::new()
                            };
                            println!(
                                "MISURA {label} arrow col[{index}] nome={} tipo={:?} valori={}{raw}",
                                batch_schema.field(index).name(),
                                column.data_type(),
                                format!("{column:?}").replace('\n', " ")
                            );
                        }
                        let mut seen: Vec<String> = Vec::new();
                        for field in batch_schema.fields() {
                            let name = field.name().clone();
                            if !seen.contains(&name) {
                                seen.push(name.clone());
                                let by_name = batch
                                    .column_by_name(&name)
                                    .map(|column| format!("{column:?}").replace('\n', " "));
                                println!("MISURA {label} arrow column_by_name({name:?})={by_name:?}");
                            }
                        }
                    }
                    Ok(None) => {
                        println!("MISURA {label} arrow fine");
                        break;
                    }
                    Err(error) => {
                        println!("MISURA {label} arrow batch {}", errore(&error));
                        break;
                    }
                }
            }
        }
    }
}

async fn arrow_read<P: Provider>(
    provider: &P,
    secret: &SecretString,
    label: &str,
    source: ObjectRef,
    projection: &[&str],
    order: Option<&str>,
) {
    let operation = ReadOperation {
        source,
        projection: projection.iter().map(|name| (*name).to_owned()).collect(),
        order_by: order
            .map(|field| {
                vec![OrderBy {
                    field: field.to_owned(),
                    direction: SortDirection::Asc,
                }]
            })
            .unwrap_or_default(),
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    println!("MISURA {label} arrow read projection={projection:?}");
    let cancellation = CancellationToken::new();
    let budget = budget();
    let parameters = ParameterBag::default();
    let result = provider
        .read(secret, &operation, &parameters, &budget, &cancellation)
        .await;
    arrow_dump(label, result).await;
}

fn column(relation: &str, field: &str) -> QueryExpression {
    QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    }
}

fn join_query(a: &ObjectRef, b: &ObjectRef, id: &str, projection: Vec<QueryProjection>) -> QueryOperation {
    QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: a.clone(),
            alias: Some("a".to_owned()),
        }),
        derived_source: None,
        projection,
        joins: vec![QueryJoin {
            kind: JoinKind::Inner,
            source: Some(QuerySource {
                object: b.clone(),
                alias: Some("b".to_owned()),
            }),
            derived_source: None,
            lateral: false,
            on: Some(QueryExpression::Compare {
                left: Box::new(column("a", id)),
                operator: ComparisonOperator::Lt,
                right: Box::new(column("b", id)),
            }),
        }],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: Vec::new(),
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    }
}

async fn arrow_query<P: Provider>(
    provider: &P,
    secret: &SecretString,
    label: &str,
    operation: &QueryOperation,
) {
    println!("MISURA {label} arrow query projection={:?}", operation.projection);
    let cancellation = CancellationToken::new();
    let budget = budget();
    let parameters = ParameterBag::default();
    let result = provider
        .query(secret, operation, &parameters, &budget, &cancellation)
        .await;
    arrow_dump(label, result).await;
}

struct Dialetto {
    label: &'static str,
    dual: &'static str,
    drop_table: &'static str,
    drop_view: &'static str,
    a_sql: String,
    b_sql: String,
    v_sql: String,
    v2_sql: String,
    a_ref: ObjectRef,
    b_ref: ObjectRef,
    v_ref: ObjectRef,
    v2_ref: ObjectRef,
    id: &'static str,
    label_col: &'static str,
}

/// Ipotesi B: colonne omonime.
async fn misura_omonime<P: Provider>(provider: &P, secret: &SecretString, d: &Dialetto) {
    let l = d.label;
    println!("MISURA ===== {l} ipotesi B: colonne omonime =====");
    for view in [&d.v_sql, &d.v2_sql] {
        ddl(provider, secret, l, &d.drop_view.replace("{}", view)).await;
    }
    for table in [&d.a_sql, &d.b_sql] {
        ddl(provider, secret, l, &d.drop_table.replace("{}", table)).await;
        ddl(
            provider,
            secret,
            l,
            &format!("CREATE TABLE {table} (id INTEGER NOT NULL, label VARCHAR(20) NOT NULL)"),
        )
        .await;
    }
    dml(
        provider,
        secret,
        l,
        &[
            format!("INSERT INTO {} (id, label) VALUES (1, 'a-uno')", d.a_sql),
            format!("INSERT INTO {} (id, label) VALUES (2, 'b-due')", d.b_sql),
        ],
    )
    .await;

    tx_query(provider, secret, &format!("{l} B1-letterali"), &format!("SELECT 1 AS x, 2 AS x{}", d.dual)).await;
    tx_query(
        provider,
        secret,
        &format!("{l} B1b-letterali-tipi-diversi"),
        &format!("SELECT 1 AS x, 'due' AS x{}", d.dual),
    )
    .await;
    tx_query(
        provider,
        secret,
        &format!("{l} B2-join"),
        &format!("SELECT a.id, b.id FROM {} a JOIN {} b ON a.id < b.id", d.a_sql, d.b_sql),
    )
    .await;
    tx_query(
        provider,
        secret,
        &format!("{l} B3-join-star"),
        &format!("SELECT * FROM {} a JOIN {} b ON a.id < b.id", d.a_sql, d.b_sql),
    )
    .await;
    tx_query(
        provider,
        secret,
        &format!("{l} B4-oracolo-nomi-distinti"),
        &format!(
            "SELECT a.id AS id_a, b.id AS id_b, a.label AS label_a, b.label AS label_b FROM {} a JOIN {} b ON a.id < b.id",
            d.a_sql, d.b_sql
        ),
    )
    .await;

    let plain = join_query(
        &d.a_ref,
        &d.b_ref,
        d.id,
        vec![
            QueryProjection { expression: column("a", d.id), alias: None },
            QueryProjection { expression: column("b", d.id), alias: None },
        ],
    );
    arrow_query(provider, secret, &format!("{l} B5-arrow-join-senza-alias"), &plain).await;
    let aliased = join_query(
        &d.a_ref,
        &d.b_ref,
        d.id,
        vec![
            QueryProjection { expression: column("a", d.id), alias: Some("x".to_owned()) },
            QueryProjection { expression: column("b", d.id), alias: Some("x".to_owned()) },
        ],
    );
    arrow_query(provider, secret, &format!("{l} B6-arrow-join-alias-x-x"), &aliased).await;
    let wildcard = join_query(
        &d.a_ref,
        &d.b_ref,
        d.id,
        vec![QueryProjection {
            expression: QueryExpression::Wildcard { relation: None },
            alias: None,
        }],
    );
    arrow_query(provider, secret, &format!("{l} B7-arrow-join-wildcard"), &wildcard).await;

    // Lettura Arrow di un oggetto: l'unico modo di portarvi nomi omonimi e una
    // vista; il server dice se l'ammette.
    ddl(
        provider,
        secret,
        l,
        &format!(
            "CREATE VIEW {} AS SELECT a.id, b.id FROM {} a JOIN {} b ON a.id < b.id",
            d.v_sql, d.a_sql, d.b_sql
        ),
    )
    .await;
    arrow_read(provider, secret, &format!("{l} B8-arrow-read-vista-join"), d.v_ref.clone(), &[], None).await;
    ddl(
        provider,
        secret,
        l,
        &format!("CREATE VIEW {} AS SELECT 1 AS x, 2 AS x{}", d.v2_sql, d.dual),
    )
    .await;
    arrow_read(provider, secret, &format!("{l} B9-arrow-read-vista-letterali"), d.v2_ref.clone(), &[], None).await;
    arrow_read(
        provider,
        secret,
        &format!("{l} B10-arrow-read-controllo"),
        d.a_ref.clone(),
        &[d.id, d.label_col],
        Some(d.id),
    )
    .await;
    println!("MISURA ===== {l} ipotesi B: fine =====");
}

use crate::{MariadbProvider, MysqlConfig, MysqlProvider};

fn env(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn misura_secret() -> SecretString {
    SecretString::new(env("PLENORA_MYSQL_PASSWORD", "DataFlow_Test_2026!"))
}

fn misura_config() -> MysqlConfig {
    MysqlConfig::new(
        env("PLENORA_MYSQL_HOST", "127.0.0.1"),
        env("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        env("PLENORA_MYSQL_USER", "dataflow"),
        misura_secret(),
    )
    .with_port(env("PLENORA_MYSQL_PORT", "3306").parse().expect("porta"))
    .with_private_ca_certificate(std::env::var("PLENORA_MYSQL_CA").expect("CA della misura"))
}

fn dialetto_mysql(label: &'static str) -> Dialetto {
    let schema = env("PLENORA_MYSQL_DATABASE", "dataflow_test");
    let r = |object: &str| ObjectRef {
        catalog: None,
        schema: Some(schema.clone()),
        object: object.to_owned(),
    };
    Dialetto {
        label,
        dual: "",
        drop_table: "DROP TABLE IF EXISTS {}",
        drop_view: "DROP VIEW IF EXISTS {}",
        a_sql: "mis804_a".to_owned(),
        b_sql: "mis804_b".to_owned(),
        v_sql: "mis804_v".to_owned(),
        v2_sql: "mis804_v2".to_owned(),
        a_ref: r("mis804_a"),
        b_ref: r("mis804_b"),
        v_ref: r("mis804_v"),
        v2_ref: r("mis804_v2"),
        id: "id",
        label_col: "label",
    }
}

#[tokio::test]
#[ignore = "misura 8.0.4 live"]
async fn misura_804_omonime() {
    let label = env("MISURA_LABEL", "mysql");
    let label: &'static str = Box::leak(label.into_boxed_str());
    if env("MISURA_PRODUCT", "mysql") == "mariadb" {
        let provider = MariadbProvider::new(misura_config(), 2).expect("provider MariaDB misura");
        misura_omonime(&provider, &misura_secret(), &dialetto_mysql(label)).await;
    } else {
        let provider = MysqlProvider::new(misura_config(), 2).expect("provider MySQL misura");
        misura_omonime(&provider, &misura_secret(), &dialetto_mysql(label)).await;
    }
}
