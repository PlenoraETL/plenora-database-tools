//! Prove opt-in, incluse nei test PostgreSQL del workflow Rust.
use super::*;
use arrow_array::{Float64Array, Int64Array, RecordBatch, StringArray};
use futures_util::{stream, TryStreamExt};
use plenora_database_core::plan::WriteMode;
use plenora_database_core::resource::ResourceLimits;
use std::time::Duration;

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connessione fixture");
    tokio::spawn(async move {
        connection.await.expect("driver fixture");
    });
    client
}

fn operation(table: &str) -> WriteOperation {
    let mut operation = write_visibility_live_tests::operation(table, WriteMode::Append);
    operation.target.schema = Some("pln_borrowed".into());
    operation
}

fn batch() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![Some(1), Some(2), None])) as arrow_array::ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(vec![Some("a\t\\\nè"), None, Some("")]))
                as arrow_array::ArrayRef,
        ),
        (
            "number",
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                Some(f64::INFINITY),
                None,
            ])) as arrow_array::ArrayRef,
        ),
    ])
    .expect("batch")
}

fn read_plan(table: &str) -> ReadPlan {
    ReadPlan::new(
        ReadOperation {
            source: operation(table).target,
            projection: vec![],
            order_by: vec![],
            row_limit: None,
            row_offset: None,
            filter: None,
            declared_crs: vec![],
        },
        ParameterBag::default(),
    )
}

async fn settings<C: tokio_postgres::GenericClient + Sync>(client: &C) -> Vec<String> {
    client
        .query(
            "SELECT name || '=' || setting FROM pg_catalog.pg_settings ORDER BY name",
            &[],
        )
        .await
        .expect("settings")
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn reset(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "DROP SCHEMA IF EXISTS pln_borrowed CASCADE; CREATE SCHEMA pln_borrowed;
        CREATE TABLE pln_borrowed.borrowed (id bigint, value text, number double precision);
        CREATE TABLE pln_borrowed.owned (LIKE pln_borrowed.borrowed)",
        )
        .await
        .expect("fixture");
}

struct One(Option<RecordBatch>, SchemaRef);
impl BatchStream for One {
    fn schema(&self) -> SchemaRef {
        self.1.clone()
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.0.take()) })
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_borrowed_commit_rollback_and_owned_parity() {
    let Some(dsn) = write_visibility_live_tests::live_dsn() else {
        return;
    };
    let _serial = write_visibility_live_tests::SERIAL.lock().await;
    let mut client = connect(&dsn).await;
    let observer = connect(&dsn).await;
    for mode in [PostgresInsertMode::CopyText, PostgresInsertMode::CopyBinary] {
        reset(&client).await;
        let before = settings(&client).await;
        let input = batch();
        let mut plan = WritePlan::new(operation("borrowed"), input.schema()).expect("plan");
        plan.insert_mode = mode;
        let cancellation = CancellationToken::new();
        let mut provisional = None;
        for commit in [false, true] {
            let tx = client.transaction().await.expect("begin");
            tx.batch_execute("INSERT INTO pln_borrowed.owned (id) VALUES (99)")
                .await
                .expect("lavoro precedente del chiamante");
            let result = scrivi_arrow(
                &tx,
                &plan,
                stream::once(async {
                    assert_eq!(settings(&tx).await, before);
                    input.clone()
                }),
                &budget(),
                &cancellation,
            )
            .await
            .expect("scrittura prestata");
            assert_eq!((result.received, result.applied), (3, 3));
            assert!(result.loss_report.effective.is_empty());
            result.loss_report.validate().expect("rapporto provvisorio");
            assert_eq!(settings(&tx).await, before);
            let marker: i64 = tx
                .query_one("SELECT count(*) FROM pln_borrowed.owned WHERE id = 99", &[])
                .await
                .expect("lavoro del chiamante preservato")
                .get(0);
            assert_eq!(marker, 1);
            let invisible: i64 = observer
                .query_one("SELECT count(*) FROM pln_borrowed.owned", &[])
                .await
                .expect("nessun commit implicito")
                .get(0);
            assert_eq!(invisible, 0);
            let count: i64 = observer
                .query_one("SELECT count(*) FROM pln_borrowed.borrowed", &[])
                .await
                .expect("isolamento")
                .get(0);
            assert_eq!(count, 0);
            if commit {
                provisional = Some(result.loss_report);
                tx.commit().await.expect("commit chiamante");
            } else {
                tx.rollback().await.expect("rollback chiamante");
            }
        }
        assert_eq!(settings(&client).await, before);
        client
            .batch_execute("DELETE FROM pln_borrowed.owned WHERE id = 99")
            .await
            .expect("rimuovi marker committed");
        let provider = PostgresProvider::insecure_local().with_insert_mode(mode);
        let owned = write_visibility_live_tests::write_stream(
            &provider,
            &SecretString::new(dsn.clone()),
            &operation("owned"),
            Box::new(One(Some(input.clone()), input.schema())),
        )
        .await
        .expect("scrittura posseduta");
        assert_eq!(owned.rows.confirmed, 3);
        let plenora_database_core::execution_loss::VersionedLossReport::Execution(committed) =
            owned.loss_report.expect("report")
        else {
            panic!("execution report");
        };
        let provisional = provisional.expect("pending report");
        assert_eq!(provisional.risk, committed.risk);
        assert_eq!(provisional.evaluation, committed.evaluation);

        // Confronto binario lato PostgreSQL, inclusi NULL e segno dello zero.
        let equal: bool = observer.query_one("SELECT NOT EXISTS (
            (SELECT id, value, pg_catalog.float8send(number) FROM pln_borrowed.borrowed
             EXCEPT ALL SELECT id, value, pg_catalog.float8send(number) FROM pln_borrowed.owned)
            UNION ALL
            (SELECT id, value, pg_catalog.float8send(number) FROM pln_borrowed.owned
             EXCEPT ALL SELECT id, value, pg_catalog.float8send(number) FROM pln_borrowed.borrowed))", &[])
            .await.expect("parita binaria").get(0);
        assert!(equal);
        let read = read_plan("borrowed");
        let borrowed: Vec<_> = leggi_arrow(&client, &read, &budget(), &cancellation)
            .await
            .expect("read")
            .try_collect()
            .await
            .expect("batch prestati");
        let mut owned = provider
            .read(
                &SecretString::new(dsn.clone()),
                &read.operation,
                &read.parameters,
                &budget(),
                &cancellation,
            )
            .await
            .expect("read posseduta");
        let mut expected = vec![];
        while let Some(batch) = owned
            .next_batch(&cancellation)
            .await
            .expect("batch posseduti")
        {
            expected.push(batch);
        }
        assert_eq!(borrowed, expected);
        assert_eq!(settings(&client).await, before);
    }
    client
        .batch_execute("DROP SCHEMA pln_borrowed CASCADE")
        .await
        .expect("cleanup");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_borrowed_budget_and_cancelled_input_leave_caller_in_control() {
    let Some(dsn) = write_visibility_live_tests::live_dsn() else {
        return;
    };
    let _serial = write_visibility_live_tests::SERIAL.lock().await;
    let mut client = connect(&dsn).await;
    reset(&client).await;
    let before = settings(&client).await;
    let input = batch();
    let plan = WritePlan::new(operation("borrowed"), input.schema()).expect("plan");
    let limited = ResourceBudget::new(ResourceLimits {
        rows: 1,
        ..ResourceLimits::default()
    })
    .expect("budget");
    let tx = client.transaction().await.expect("begin");
    let error = scrivi_arrow(
        &tx,
        &plan,
        stream::iter([input.clone()]),
        &limited,
        &CancellationToken::new(),
    )
    .await
    .expect_err("budget deve fallire");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    assert_eq!(settings(&tx).await, before);
    tx.rollback().await.expect("rollback chiamante");

    let partial_budget = ResourceBudget::new(ResourceLimits {
        rows: 4,
        ..ResourceLimits::default()
    })
    .expect("budget");
    let tx = client.transaction().await.expect("begin");
    let error = scrivi_arrow(
        &tx,
        &plan,
        stream::iter([input.clone(), input.clone()]),
        &partial_budget,
        &CancellationToken::new(),
    )
    .await
    .expect_err("secondo batch oltre budget");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
    let count: i64 = tx
        .query_one("SELECT count(*) FROM pln_borrowed.borrowed", &[])
        .await
        .expect("prima COPY applicata")
        .get(0);
    assert_eq!(count, 3);
    tx.rollback().await.expect("rollback chiamante");
    let tx = client.transaction().await.expect("begin");
    scrivi_arrow(
        &tx,
        &plan,
        stream::iter([input]),
        &budget(),
        &CancellationToken::new(),
    )
    .await
    .expect("seed read");
    tx.commit().await.expect("commit chiamante");
    let read = read_plan("borrowed");
    let read_budget = ResourceBudget::new(ResourceLimits {
        rows: 1,
        ..ResourceLimits::default()
    })
    .expect("budget");
    let result: Result<Vec<_>> =
        leggi_arrow(&client, &read, &read_budget, &CancellationToken::new())
            .await
            .expect("read stream")
            .try_collect()
            .await;
    assert_eq!(
        result.expect_err("read oltre budget").category,
        ErrorCategory::ResourceLimit
    );
    // Un produttore in attesa non deve impedire l'annullamento.
    let token = CancellationToken::new();
    let tx = client.transaction().await.expect("begin");
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        token.cancel();
    };
    let resources = budget();
    let write = scrivi_arrow(&tx, &plan, stream::pending(), &resources, &token);
    let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(write, cancel)
    })
    .await
    .expect("annullamento bounded");
    assert_eq!(
        result.expect_err("cancelled").category,
        ErrorCategory::Cancelled
    );
    tx.rollback().await.expect("rollback chiamante");
    let read = read_plan("borrowed");
    let error = leggi_arrow(&client, &read, &budget(), &token)
        .await
        .err()
        .expect("read cancellata");
    assert_eq!(error.category, ErrorCategory::Cancelled);
    client
        .batch_execute("DROP SCHEMA pln_borrowed CASCADE")
        .await
        .expect("cleanup");
}

#[tokio::test]
async fn live_borrowed_cancel_in_copy() {
    let Some(dsn) = write_visibility_live_tests::live_dsn() else {
        return;
    };
    let _serial = write_visibility_live_tests::SERIAL.lock().await;
    let mut client = connect(&dsn).await;
    let observer = connect(&dsn).await;
    reset(&client).await;
    client
        .batch_execute(
            "CREATE FUNCTION pln_borrowed.slow() RETURNS trigger LANGUAGE plpgsql AS
        $$ BEGIN PERFORM pg_catalog.pg_sleep(0.01); RETURN NEW; END $$;
        CREATE TRIGGER slow BEFORE INSERT ON pln_borrowed.borrowed
        FOR EACH ROW EXECUTE FUNCTION pln_borrowed.slow()",
        )
        .await
        .expect("trigger COPY");
    let pid: i32 = client
        .query_one("SELECT pg_catalog.pg_backend_pid()", &[])
        .await
        .expect("pid")
        .get(0);
    for mode in [PostgresInsertMode::CopyText, PostgresInsertMode::CopyBinary] {
        let input = RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from(vec![1; 128])) as arrow_array::ArrayRef,
        )])
        .expect("batch COPY lento");
        let mut plan = WritePlan::new(operation("borrowed"), input.schema()).expect("plan");
        plan.insert_mode = mode;
        let tx = client.transaction().await.expect("begin");
        let token = CancellationToken::new();
        let cancel = async {
            loop {
                let copying: bool = observer.query_one("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity
                    WHERE pid = $1 AND state = 'active' AND query LIKE 'COPY %' AND wait_event = 'PgSleep')", &[&pid])
                    .await.expect("osservazione COPY").get(0);
                if copying {
                    token.cancel();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        let resources = budget();
        let write = scrivi_arrow(&tx, &plan, stream::iter([input]), &resources, &token);
        let (result, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(write, cancel)
        })
        .await
        .expect("COPY osservata e annullata");
        assert_eq!(
            result.expect_err("cancelled").category,
            ErrorCategory::Cancelled
        );
        tx.rollback()
            .await
            .expect("rollback chiamante dopo CopyFail");
        let count: i64 = client
            .query_one("SELECT count(*) FROM pln_borrowed.borrowed", &[])
            .await
            .expect("nessuna riga committed")
            .get(0);
        assert_eq!(count, 0);
        client
            .simple_query("SELECT 1")
            .await
            .expect("connessione riusabile");
    }
    client
        .batch_execute("DROP SCHEMA pln_borrowed CASCADE")
        .await
        .expect("cleanup");
}

#[test]
fn borrowed_preflight_does_not_control_the_callers_transaction() {
    // Queste funzioni generiche sono raggiunte sia dal Client sia dalla Transaction.
    for source in [
        include_str!("write/canonical.rs"),
        include_str!("write/borrowed.rs"),
        include_str!("write.rs")
            .split("pub async fn leaf_losses")
            .nth(1)
            .unwrap()
            .split("pub async fn assign_dry_run_key_equality")
            .next()
            .unwrap(),
        include_str!("write/admission.rs")
            .split("pub async fn preflight")
            .nth(1)
            .unwrap(),
    ] {
        for forbidden in [
            "batch_execute(\"BEGIN\")",
            "batch_execute(\"ROLLBACK\")",
            "SessionPath::close",
            "set_config(",
        ] {
            assert!(
                !source.contains(forbidden),
                "controllo della sessione nel preflight: {forbidden}"
            );
        }
    }
}

/// La DSN del `PgBouncer` del riferimento: modo transazione, `max_prepared_statements > 0`.
fn pgbouncer_dsn() -> Option<String> {
    let declared = std::env::var("PLENORA_TEST_PGBOUNCER_DSN").ok();
    if declared.is_none() {
        assert!(
            std::env::var("PLENORA_REQUIRE_LIVE_PGBOUNCER").is_err(),
            "PLENORA_REQUIRE_LIVE_PGBOUNCER e acceso ma PLENORA_TEST_PGBOUNCER_DSN manca"
        );
    }
    declared
}

/// Il contratto L-DB1: `PgBouncer` >= 1.21 in modo transazione con
/// `max_prepared_statements > 0`. Quattro client condividono due connessioni
/// server, quindi le istruzioni nominate di `tokio-postgres` passano fra
/// backend diversi; letture anche fuori transazione.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_borrowed_through_pgbouncer_transaction_mode() {
    let Some(dsn) = write_visibility_live_tests::live_dsn() else {
        return;
    };
    let Some(pooled) = pgbouncer_dsn() else {
        return;
    };
    let _serial = write_visibility_live_tests::SERIAL.lock().await;
    let mut admin: tokio_postgres::Config = pooled.parse().expect("dsn pgbouncer");
    admin.dbname("pgbouncer");
    let (console, connection) = admin.connect(tokio_postgres::NoTls).await.expect("console");
    tokio::spawn(async move {
        connection.await.expect("driver console");
    });
    let mut config = std::collections::BTreeMap::new();
    for message in console
        .simple_query("SHOW CONFIG")
        .await
        .expect("show config")
    {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            config.insert(
                row.get(0).unwrap_or_default().to_owned(),
                row.get(1).unwrap_or_default().to_owned(),
            );
        }
    }
    assert_eq!(
        config.get("pool_mode").map(String::as_str),
        Some("transaction")
    );
    let prepared: u64 = config
        .get("max_prepared_statements")
        .and_then(|value| value.parse().ok())
        .expect("max_prepared_statements");
    assert!(prepared > 0);
    let mut version = None;
    for message in console
        .simple_query("SHOW VERSION")
        .await
        .expect("show version")
    {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = message {
            version = row.get(0).map(str::to_owned);
        }
    }
    let version = version.expect("versione pgbouncer");
    let numbers: Vec<u32> = version
        .split(|c: char| !c.is_ascii_digit())
        .filter(|part| !part.is_empty())
        .take(2)
        .map(|part| part.parse().expect("versione"))
        .collect();
    assert!(numbers >= vec![1, 21], "PgBouncer {version} sotto 1.21");

    let direct = connect(&dsn).await;
    reset(&direct).await;
    for table in ["p0", "p1", "p2", "p3"] {
        direct
            .batch_execute(&format!(
                "CREATE TABLE pln_borrowed.{table} (LIKE pln_borrowed.borrowed)"
            ))
            .await
            .expect("tabelle per client");
    }
    let input = batch();
    let cancellation = CancellationToken::new();
    let work = ["p0", "p1", "p2", "p3"].map(|table| {
        let pooled = pooled.clone();
        let input = input.clone();
        let cancellation = cancellation.clone();
        async move {
            let mut client = connect(&pooled).await;
            for round in 0..3 {
                let mut plan = WritePlan::new(operation(table), input.schema()).expect("plan");
                plan.insert_mode = if round % 2 == 0 {
                    PostgresInsertMode::CopyBinary
                } else {
                    PostgresInsertMode::CopyText
                };
                let tx = client.transaction().await.expect("begin");
                let before = settings(&tx).await;
                let result = scrivi_arrow(
                    &tx,
                    &plan,
                    stream::iter([input.clone()]),
                    &budget(),
                    &cancellation,
                )
                .await
                .expect("scrittura prestata tramite pgbouncer");
                assert_eq!((result.received, result.applied), (3, 3));
                result.loss_report.validate().expect("rapporto provvisorio");
                assert_eq!(settings(&tx).await, before);
                if round == 1 {
                    tx.rollback().await.expect("rollback chiamante");
                } else {
                    tx.commit().await.expect("commit chiamante");
                }
            }
            // Fuori transazione: ogni istruzione puo cambiare backend.
            let read: Vec<_> = leggi_arrow(&client, &read_plan(table), &budget(), &cancellation)
                .await
                .expect("lettura prestata tramite pgbouncer")
                .try_collect()
                .await
                .expect("batch tramite pgbouncer");
            (table, read)
        }
    });
    for (table, read) in futures_util::future::join_all(work).await {
        let rows: usize = read.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, 6, "{table}: due commit da tre righe");
        let expected: Vec<_> = leggi_arrow(&direct, &read_plan(table), &budget(), &cancellation)
            .await
            .expect("lettura diretta")
            .try_collect()
            .await
            .expect("batch diretti");
        assert_eq!(
            read, expected,
            "{table}: stessi batch con e senza pgbouncer"
        );
    }
    direct
        .batch_execute("DROP SCHEMA pln_borrowed CASCADE")
        .await
        .expect("cleanup");
}
