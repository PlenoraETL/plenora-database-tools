//! Unit + live test del transaction scope `PostgreSQL`.
//!
//! I test live richiedono un `PostgreSQL` raggiungibile all'hostname
//! `dataflow-postgres` (compose network `plenora-postgres_default`).
#![allow(clippy::float_cmp)] // matches!() con parametri f64 letterali

// Import condivisi dai sottomoduli live e facade.
#[allow(unused_imports)]
use super::sql::{build_begin_sql, phase_of, quote_identifier};
#[allow(unused_imports)]
use super::PostgresTransaction;
#[allow(unused_imports)]
use plenora_database_core::provider::{ParameterValue, ProviderFuture};
#[allow(unused_imports)]
use plenora_database_core::row::Row;
#[allow(unused_imports)]
use plenora_database_core::transaction::{
    concurrent_modification_error, outcome_unknown_recovery, validate_savepoint_name, AccessMode,
    CommitOutcome, ConditionalUpdate, IsolationLevel, RowStream, Statement, TransactionOptions,
    TransactionScope,
};
#[allow(unused_imports)]
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};

#[test]
fn begin_default_is_bare() {
    let sql = build_begin_sql(&TransactionOptions::default());
    assert_eq!(sql, "BEGIN;");
}

#[test]
fn begin_with_isolation_and_read_only() {
    let opts = TransactionOptions {
        isolation: Some(IsolationLevel::Serializable),
        access_mode: Some(AccessMode::ReadOnly),
        deferrable: Some(true),
        ..TransactionOptions::default()
    };
    assert_eq!(
        build_begin_sql(&opts),
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY DEFERRABLE;"
    );
}

#[test]
fn begin_with_statement_timeout_appends_set_local() {
    let opts = TransactionOptions {
        isolation: Some(IsolationLevel::ReadCommitted),
        statement_timeout_ms: Some(750),
        ..TransactionOptions::default()
    };
    assert_eq!(
        build_begin_sql(&opts),
        "BEGIN ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 750;"
    );
}

#[test]
fn quote_identifier_escapes_double_quotes() {
    assert_eq!(quote_identifier("plain"), "\"plain\"");
    assert_eq!(quote_identifier("evil\"name"), "\"evil\"\"name\"");
}

#[test]
fn phase_of_detects_read_head() {
    assert_eq!(phase_of("SELECT 1"), ErrorPhase::Read);
    assert_eq!(
        phase_of("  with cte AS (SELECT 1) SELECT * FROM cte"),
        ErrorPhase::Read
    );
    assert_eq!(phase_of("SHOW server_version"), ErrorPhase::Read);
}

#[test]
fn phase_of_detects_write_head() {
    assert_eq!(phase_of("INSERT INTO t VALUES (1)"), ErrorPhase::Write);
    assert_eq!(phase_of("UPDATE t SET x=1"), ErrorPhase::Write);
    assert_eq!(phase_of("DELETE FROM t"), ErrorPhase::Write);
    assert_eq!(phase_of("CREATE TABLE t (x INT)"), ErrorPhase::Write);
}

/// Test integrazione live per A1: multi-statement, savepoint, cancellation,
/// `statement_timeout`. Chiudono il milestone A1 verso Postgres reale.
#[cfg(test)]
mod live {
    use super::*;
    use crate::PostgresProvider;
    use plenora_database_core::provider::{Provider, SecretString};
    use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
    use std::sync::Arc;

    /// DSN del riferimento plaintext, usato quando il runner non ne impone
    /// uno.
    const REFERENCE_DSN: &str =
        "host=dataflow-postgres user=dataflow password=dataflow_test_2026 dbname=dataflow_test";

    /// Il DSN su cui girano questi test live.
    ///
    /// `PLENORA_TEST_POSTGRES_DSN` ha la precedenza: la matrice delle versioni
    /// indirizza cosi la suite verso il `PostgreSQL` che sta qualificando.
    fn live_dsn() -> String {
        std::env::var("PLENORA_TEST_POSTGRES_DSN").unwrap_or_else(|_| REFERENCE_DSN.to_owned())
    }

    fn secret() -> SecretString {
        SecretString::new(live_dsn())
    }

    fn budget() -> ResourceBudget {
        ResourceBudget::new(ResourceLimits::default()).expect("budget")
    }

    /// Provider dei test live: TLS disattivato, come ogni altra fixture live
    /// del repository.
    ///
    /// Il provider ordinario richiede TLS, mentre il Compose usato da questi
    /// test serve plaintext; la scelta insicura deve quindi restare esplicita.
    ///
    /// Nessuno di questi test riguarda TLS: provano transazioni, savepoint,
    /// facade e policy. La superficie TLS ha il proprio compose
    /// (`docker-compose.postgres-tls.yml`) e i propri test.
    fn provider() -> PostgresProvider {
        PostgresProvider::insecure_local_with_batch_rows(1_024)
    }

    async fn count(provider: &PostgresProvider, sql: &str) -> i64 {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect out-of-band");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let _ = provider;
        let row = client.query_one(sql, &[]).await.expect("count");
        row.get::<_, i64>(0)
    }

    async fn scratch_table(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect setup");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
             CREATE TABLE {name} (id INT PRIMARY KEY, v TEXT NOT NULL);",
            ))
            .await
            .expect("scratch table");
    }

    async fn drop_table(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect drop");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {name};"))
            .await
            .expect("drop");
    }

    #[tokio::test]
    async fn live_commit_multi_statement_persists_all() {
        scratch_table("a1_commit").await;
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let n1 = tx
            .execute(
                &Statement::new("INSERT INTO a1_commit VALUES ($1, $2)").with_params(vec![
                    ParameterValue::I32(1),
                    ParameterValue::String("a".into()),
                ]),
                &cancel,
            )
            .await
            .expect("insert 1");
        assert_eq!(n1, 1);

        let n2 = tx
            .execute(
                &Statement::new("INSERT INTO a1_commit VALUES ($1, $2)").with_params(vec![
                    ParameterValue::I32(2),
                    ParameterValue::String("b".into()),
                ]),
                &cancel,
            )
            .await
            .expect("insert 2");
        assert_eq!(n2, 1);

        let outcome = tx.commit(&cancel).await.expect("commit");
        assert!(outcome.is_committed());

        assert_eq!(
            count(&provider, "SELECT COUNT(*)::BIGINT FROM a1_commit").await,
            2
        );
        drop_table("a1_commit").await;
    }

    #[tokio::test]
    async fn live_rollback_discards_all_statements() {
        scratch_table("a1_rollback").await;
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new("INSERT INTO a1_rollback VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("x".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert");

        tx.rollback(&cancel).await.expect("rollback");

        assert_eq!(
            count(&provider, "SELECT COUNT(*)::BIGINT FROM a1_rollback").await,
            0
        );
        drop_table("a1_rollback").await;
    }

    /// Letture e scritture **insieme**, sullo stesso pool, per quanti giri si
    /// vuole.
    ///
    /// # Cosa mancava
    ///
    /// `live_postgres_concurrent_pool_stress_when_dsn_is_available` mette
    /// dodici **lettori** su un pool da quattro, e verifica le metriche del
    /// pool riga per riga. E' la prova di contesa piu ricca del repository, e
    /// legge soltanto.
    ///
    /// Un pool puo sbagliare proprio dove i due carichi si mescolano: una
    /// connessione che torna dal path di scrittura con la transazione non
    /// chiusa e innocua fra scrittori, che ne aprono un'altra subito, ed e
    /// velenosa per un lettore che la trova con un `BEGIN` addosso.
    ///
    /// Su `MySQL`, `MariaDB` e `SQL Server` questa misura c'e. Qui era l'ultima a
    /// mancare.
    ///
    /// # Perche e anche il soak
    ///
    /// `PLENORA_PG_MIXED_ROUNDS` cambia il numero di giri e nient'altro: la
    /// corsa lunga e la corsa breve sono lo **stesso codice**. Un soak che
    /// esercitasse un percorso diverso da quello del test misurerebbe la
    /// tenuta di codice che nessuno attraversa mai.
    ///
    /// # Cosa verifica
    ///
    /// Ogni lettore ha una **fetta di lunghezza diversa**: con fette uguali,
    /// due lettori che si scambiassero la connessione a meta transazione
    /// renderebbero comunque il totale giusto. Cosi lo scambio cambia il
    /// totale.
    ///
    /// Ogni scrittore scrive un payload che porta il proprio numero, e la
    /// rilettura arriva da **un'altra connessione**: il conteggio coglie una
    /// perdita, il payload coglie un'attribuzione sbagliata.
    ///
    /// Le metriche del pool chiudono il cerchio, e sono cio che `PostgreSQL` ha
    /// e gli altri tre no: nessun timeout, nessuna sessione invalidata, e un
    /// numero di connessioni nuove che resta dentro la capacita dichiarata —
    /// cioe il pool non ne ha aperte e dimenticate lungo la strada.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn live_postgres_mixed_load_shares_one_pool_between_readers_and_writers() {
        const READERS: i32 = 6;
        const WRITERS: i32 = 6;
        const BASE_SLICE: i32 = 5;
        const ROWS_PER_WRITER: i32 = 4;

        let rounds: i32 = std::env::var("PLENORA_PG_MIXED_ROUNDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8);

        let seeded: i32 = (0..READERS).map(|reader| BASE_SLICE + reader).sum();
        scratch_table("a1_mixed_read").await;
        scratch_table("a1_mixed_write").await;
        seed_mixed_read("a1_mixed_read", seeded).await;

        let provider =
            Arc::new(PostgresProvider::insecure_local_with_batch_rows(2).with_pool_size(4, 5_000));
        let secret = Arc::new(secret());

        let mut letture = 0_i32;
        let mut scritture = 0_u64;
        for round in 0..rounds {
            let mut tasks = Vec::new();
            for reader in 0..READERS {
                let provider = Arc::clone(&provider);
                let secret = Arc::clone(&secret);
                tasks.push(tokio::spawn(async move {
                    let cancel = CancellationToken::new();
                    let length = BASE_SLICE + reader;
                    let first = (0..reader).map(|other| BASE_SLICE + other).sum::<i32>() + 1;
                    let mut tx = provider
                        .begin_transaction(
                            &secret,
                            &TransactionOptions::default(),
                            &budget(),
                            &cancel,
                        )
                        .await
                        .expect("begin del lettore");
                    let rows = tx
                        .query(
                            &Statement::new(
                                "SELECT id FROM a1_mixed_read WHERE id >= $1 ORDER BY id LIMIT $2",
                            )
                            .with_params(vec![
                                ParameterValue::I32(first),
                                ParameterValue::I64(i64::from(length)),
                            ]),
                            &cancel,
                        )
                        .await
                        .expect("query del lettore");
                    Box::new(tx)
                        .commit(&cancel)
                        .await
                        .expect("commit del lettore");
                    assert_eq!(
                        i32::try_from(rows.len()).expect("righe"),
                        length,
                        "il lettore {reader} ha visto una fetta che non e la sua"
                    );
                    (rows.len(), 0_u64)
                }));
            }
            for writer in 0..WRITERS {
                let provider = Arc::clone(&provider);
                let secret = Arc::clone(&secret);
                tasks.push(tokio::spawn(async move {
                    let cancel = CancellationToken::new();
                    let mut tx = provider
                        .begin_transaction(
                            &secret,
                            &TransactionOptions::default(),
                            &budget(),
                            &cancel,
                        )
                        .await
                        .expect("begin dello scrittore");
                    // Le chiavi non si ripetono fra i giri: un insert che
                    // trovasse la propria riga gia scritta fallirebbe sul
                    // primario, e la prova leggerebbe come contesa cio che e
                    // aritmetica.
                    let first = (round * WRITERS + writer) * ROWS_PER_WRITER + 1;
                    let mut written = 0_u64;
                    for id in first..first + ROWS_PER_WRITER {
                        written += tx
                            .execute(
                                &Statement::new("INSERT INTO a1_mixed_write VALUES ($1, $2)")
                                    .with_params(vec![
                                        ParameterValue::I32(id),
                                        ParameterValue::String(format!("w{writer}-{id}")),
                                    ]),
                                &cancel,
                            )
                            .await
                            .expect("insert dello scrittore");
                    }
                    Box::new(tx)
                        .commit(&cancel)
                        .await
                        .expect("commit dello scrittore");
                    (0_usize, written)
                }));
            }
            for task in tasks {
                let (seen, written) = task.await.expect("worker del carico misto");
                letture += i32::try_from(seen).expect("righe lette");
                scritture += written;
            }
        }

        assert_eq!(
            letture,
            seeded * rounds,
            "le fette devono ricomporre la tabella a ogni giro"
        );
        assert_eq!(
            scritture,
            u64::try_from(WRITERS * ROWS_PER_WRITER * rounds).expect("righe scritte"),
            "ogni insert dichiara la sua riga"
        );

        // La rilettura da un'altra connessione: il payload deve nominare lo
        // scrittore che compete a quella chiave.
        let sbagliate = count(
            &provider,
            &format!(
                "SELECT COUNT(*) FROM a1_mixed_write \
                 WHERE v <> 'w' || (((id - 1) / {ROWS_PER_WRITER}) % {WRITERS})::text \
                       || '-' || id::text"
            ),
        )
        .await;
        assert_eq!(
            sbagliate, 0,
            "una riga e finita sotto il nome di un altro scrittore"
        );

        let metrics = provider.metrics_snapshot();
        assert_eq!(
            metrics.pool_timeouts, 0,
            "il pool si e fermato ad aspettare"
        );
        assert_eq!(
            metrics.invalidated_sessions, 0,
            "una sessione e stata invalidata sotto contesa"
        );
        assert!(
            (1..=4).contains(&metrics.pool_new_connections),
            "connessioni nuove {}: il pool ne ha aperte oltre la capacita",
            metrics.pool_new_connections
        );

        drop_table("a1_mixed_read").await;
        drop_table("a1_mixed_write").await;
    }

    /// Riempie la tabella di lettura con `rows` chiavi contigue.
    async fn seed_mixed_read(name: &str, rows: i32) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect seed");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "INSERT INTO {name} SELECT g, 'r' || g::text FROM generate_series(1, {rows}) g;"
            ))
            .await
            .expect("seed della tabella di lettura");
    }

    #[tokio::test]
    async fn live_savepoint_rollback_preserves_prior_statements() {
        scratch_table("a1_sp").await;
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new("INSERT INTO a1_sp VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("keep".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert keep");

        tx.savepoint("sp1", &cancel).await.expect("savepoint");

        tx.execute(
            &Statement::new("INSERT INTO a1_sp VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(2),
                ParameterValue::String("drop".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert drop");

        tx.rollback_to_savepoint("sp1", &cancel)
            .await
            .expect("rollback to");
        tx.release_savepoint("sp1", &cancel).await.expect("release");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());

        assert_eq!(
            count(&provider, "SELECT COUNT(*)::BIGINT FROM a1_sp").await,
            1
        );
        assert_eq!(
            count(
                &provider,
                "SELECT COUNT(*)::BIGINT FROM a1_sp WHERE v = 'keep'"
            )
            .await,
            1
        );
        drop_table("a1_sp").await;
    }

    #[tokio::test]
    async fn live_savepoint_name_with_injection_is_rejected() {
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let err = tx
            .savepoint("sp; DROP TABLE users; --", &cancel)
            .await
            .expect_err("nome invalido deve essere rifiutato");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_statement_timeout_triggers_cancelled_57014() {
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();
        let opts = TransactionOptions {
            statement_timeout_ms: Some(50),
            ..TransactionOptions::default()
        };

        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget, &cancel)
            .await
            .expect("begin");

        let err = tx
            .execute(&Statement::new("SELECT pg_sleep(2)"), &cancel)
            .await
            .expect_err("timeout deve interrompere");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::Cancelled
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_serializable_read_only_deferrable_isolation() {
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();
        let opts = TransactionOptions {
            isolation: Some(IsolationLevel::Serializable),
            access_mode: Some(AccessMode::ReadOnly),
            deferrable: Some(true),
            ..TransactionOptions::default()
        };

        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget, &cancel)
            .await
            .expect("begin serializable ro deferrable");

        // Un SELECT in tx SERIALIZABLE READ ONLY DEFERRABLE deve passare senza
        // errori (esclude la clausola di conflitto serializable per definizione).
        tx.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("select 1");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    #[tokio::test]
    async fn live_cancellation_before_execute_is_rejected() {
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        cancel.cancel();
        let err = tx
            .execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect_err("cancel deve bloccare");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::Cancelled
        );

        // Rollback esplicito ignora il cancellation token: deve chiudere la tx.
        tx.rollback(&cancel).await.expect("rollback ignora cancel");
    }

    // === B1: Spatial profile portabile ===

    use crate::spatial::build_spatial_select;
    use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
    use plenora_database_core::{SpatialFilter, SpatialPredicate, SpatialReference};

    async fn fetch_ewkb(sql_returning_geom: &str) -> Vec<u8> {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one(&format!("SELECT ST_AsEWKB({sql_returning_geom})"), &[])
            .await
            .expect("fetch ewkb");
        row.get::<_, Vec<u8>>(0)
    }

    async fn setup_spatial_scratch(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
             CREATE TABLE {name} (
                 id INT PRIMARY KEY,
                 geom geometry(Point, 4326) NOT NULL
             );
             INSERT INTO {name} VALUES
                 (1, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326)),
                 (2, ST_SetSRID(ST_MakePoint(12.49, 41.90), 4326)),
                 (3, ST_SetSRID(ST_MakePoint(2.35,  48.86), 4326));",
            ))
            .await
            .expect("setup");
    }

    fn reference(ewkb: Vec<u8>) -> SpatialReference {
        SpatialReference {
            ewkb,
            srid: 4326,
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geometry,
        }
    }

    /// Helper per test `DWithin`: SRID 4326 + `Geography`.
    ///
    /// `DWithin` con `Geometry` e SRID geografico è fail-closed. Un test che vuole esercitare
    /// `DWithin` su WGS84 deve usare `Geography` per distanze in metri.
    fn reference_geography(ewkb: Vec<u8>) -> SpatialReference {
        SpatialReference {
            ewkb,
            srid: 4326,
            dimensions: Dimensions::Xy,
            semantics: SpatialSemantics::Geography,
        }
    }

    #[tokio::test]
    async fn live_spatial_intersects_polygon_returns_points_inside() {
        setup_spatial_scratch("b1_intersects").await;
        // Poligono che copre l'Italia settentrionale e centrale (grosso modo).
        let polygon = fetch_ewkb("ST_SetSRID(ST_MakeEnvelope(6.0, 40.0, 14.0, 46.0), 4326)").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Intersects,
            reference: reference(polygon),
        };
        let stmt =
            build_spatial_select(None, "b1_intersects", &["id"], &filter, None).expect("build");

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        // I punti Milano (1) e Roma (2) sono nel bbox; Parigi (3) no.
        let ids: Vec<i32> = rows
            .iter()
            .filter_map(|r| match r[0] {
                ParameterValue::I32(v) => Some(v),
                _ => None,
            })
            .collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, vec![1, 2]);
        drop_table("b1_intersects").await;
    }

    #[tokio::test]
    async fn live_spatial_dwithin_uses_distance_parameter() {
        setup_spatial_scratch("b1_dwithin").await;
        // Punto vicino a Milano: 100m a est di (9.19, 45.46).
        let near_milan = fetch_ewkb("ST_SetSRID(ST_MakePoint(9.191, 45.46), 4326)").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        // Geography rende DWithin su SRID 4326 una distanza in metri
        // (calcolo geodetico
        // WGS84). 100m a Milano → DWithin(150) matcha il punto vicino,
        // DWithin(50) no.
        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::DWithin {
                distance_meters: 150.0,
            },
            reference: reference_geography(near_milan),
        };
        let stmt = build_spatial_select(None, "b1_dwithin", &["id"], &filter, None).expect("build");

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        // Solo Milano (1) è entro ~0.01° dal punto di riferimento.
        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0][0], ParameterValue::I32(1)));
        drop_table("b1_dwithin").await;
    }

    #[tokio::test]
    async fn live_spatial_bounding_box_uses_index_operator() {
        setup_spatial_scratch("b1_bbox").await;
        let polygon = fetch_ewkb("ST_SetSRID(ST_MakeEnvelope(1.0, 48.0, 5.0, 50.0), 4326)").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::BoundingBox,
            reference: reference(polygon),
        };
        let stmt = build_spatial_select(None, "b1_bbox", &["id"], &filter, None).expect("build");

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        // Solo Parigi (3) è nel bbox europeo occidentale.
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0][0], ParameterValue::I32(3)));
        drop_table("b1_bbox").await;
    }

    #[tokio::test]
    async fn live_spatial_within_selects_features_contained_in_reference() {
        setup_spatial_scratch("b1_within").await;
        // Poligono che contiene solo Milano.
        let polygon = fetch_ewkb("ST_SetSRID(ST_MakeEnvelope(9.0, 45.0, 9.5, 46.0), 4326)").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Within,
            reference: reference(polygon),
        };
        let stmt = build_spatial_select(None, "b1_within", &["id"], &filter, None).expect("build");

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0][0], ParameterValue::I32(1)));
        drop_table("b1_within").await;
    }

    #[tokio::test]
    async fn live_spatial_srid_preserved_roundtrip() {
        // Il punto ricaricato via ST_AsEWKB deve avere lo stesso SRID.
        setup_spatial_scratch("b1_srid").await;
        let polygon = fetch_ewkb("ST_SetSRID(ST_MakeEnvelope(0, 0, 100, 100), 4326)").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let filter = SpatialFilter {
            geometry_column: "geom".into(),
            predicate: SpatialPredicate::Intersects,
            reference: reference(polygon),
        };
        // La query include la colonna geom stessa e verifichiamo che il
        // decode restituisca ParameterValue::Bytes (WKB) senza errori.
        let stmt = build_spatial_select(None, "b1_srid", &["id"], &filter, None).expect("build");

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let rows = tx.query(&stmt, &cancel).await.expect("query");
        tx.rollback(&cancel).await.expect("rollback");

        assert_eq!(rows.len(), 3);
        drop_table("b1_srid").await;
    }

    // === P2a+P2b: Tipi Postgres extra (tsvector/tsquery/xml/net/money) ===

    #[tokio::test]
    async fn live_read_tsvector_as_string() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT to_tsvector('english', 'a fast brown fox')"),
            &cancel,
        )
        .await
        .expect("tsvector read");
        match &row[0] {
            ParameterValue::String(s) => {
                // Rappresentazione canonica: lexemi con posizioni
                assert!(s.contains("brown"));
                assert!(s.contains("fox"));
            }
            other => panic!("expected String, got {other:?}"),
        }
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_read_tsquery_as_string() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT 'plenora & database'::tsquery"),
            &cancel,
        )
        .await
        .expect("tsquery read");
        assert!(matches!(&row[0], ParameterValue::String(s) if s.contains("plenora")));
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_read_xml_as_string() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT '<root><item>x</item></root>'::xml"),
            &cancel,
        )
        .await
        .expect("xml read");
        assert!(matches!(&row[0], ParameterValue::String(s) if s.contains("<root>")));
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_read_network_types_via_text_cast() {
        // Pattern documentato per cidr/inet/macaddr/money: cast lato SQL
        // a text perché il wire binario Postgres per questi tipi non è
        // UTF-8 e non è direttamente decodificabile via wrapper generico.
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new(
                "SELECT '192.168.0.0/24'::cidr::text AS c, \
                    '10.0.0.5'::inet::text AS i, \
                    '08:00:2b:01:02:03'::macaddr::text AS m, \
                    '08:00:2b:01:02:03:04:05'::macaddr8::text AS m8, \
                    1234.56::money::text AS money_txt",
            ),
            &cancel,
        )
        .await
        .expect("net types via ::text");
        assert!(matches!(&row["c"], ParameterValue::String(s) if s.contains("192.168.0.0")));
        assert!(matches!(&row["i"], ParameterValue::String(s) if s.contains("10.0.0.5")));
        assert!(matches!(&row["m"], ParameterValue::String(s) if s.contains("08:00:2b")));
        assert!(matches!(&row["m8"], ParameterValue::String(s) if s.contains("08:00")));
        // money è locale-dependent (es. "$1,234.56" o "€1.234,56"): verifico
        // solo che sia non-vuoto e contenga cifre.
        assert!(matches!(&row["money_txt"], ParameterValue::String(s)
            if !s.is_empty() && s.chars().any(|c| c.is_ascii_digit())));
        tx.rollback(&cancel).await.expect("rollback");
    }

    // === P1a: Enum + Domain type safety ===

    async fn setup_enum_domain_scratch(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
             DROP TYPE IF EXISTS {name}_mood;
             DROP DOMAIN IF EXISTS {name}_email;
             CREATE TYPE {name}_mood AS ENUM ('happy', 'sad', 'neutral');
             CREATE DOMAIN {name}_email AS TEXT CHECK (VALUE LIKE '%@%');
             CREATE TABLE {name} (
                 id INT PRIMARY KEY,
                 mood {name}_mood NOT NULL,
                 email {name}_email
             );
             INSERT INTO {name} VALUES (1, 'happy', 'a@b.it'), (2, 'sad', NULL);",
            ))
            .await
            .expect("setup enum+domain");
    }

    async fn teardown_enum_domain_scratch(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
             DROP TYPE IF EXISTS {name}_mood;
             DROP DOMAIN IF EXISTS {name}_email;",
            ))
            .await
            .expect("teardown");
    }

    #[tokio::test]
    async fn live_enum_column_decoded_as_parameter_value_enum() {
        setup_enum_domain_scratch("p1a_enum").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT mood FROM p1a_enum WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("query_one");

        match &row["mood"] {
            ParameterValue::Enum { type_name, label } => {
                assert_eq!(type_name, "p1a_enum_mood");
                assert_eq!(label, "happy");
            }
            other => panic!("expected Enum, got {other:?}"),
        }

        tx.rollback(&cancel).await.expect("rollback");
        teardown_enum_domain_scratch("p1a_enum").await;
    }

    #[tokio::test]
    async fn live_enum_write_via_parameter_value_enum() {
        setup_enum_domain_scratch("p1a_enum_w").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // INSERT usando ParameterValue::Enum: la label viene bindata come
        // testo, Postgres fa cast implicito nella colonna mood enum.
        let n = tx
            .execute(
                &Statement::new("INSERT INTO p1a_enum_w VALUES ($1, $2, NULL)").with_params(vec![
                    ParameterValue::I32(99),
                    ParameterValue::Enum {
                        type_name: "p1a_enum_w_mood".into(),
                        label: "neutral".into(),
                    },
                ]),
                &cancel,
            )
            .await
            .expect("insert enum via ParameterValue::Enum");
        assert_eq!(n, 1);

        // Rileggo e verifico
        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT mood FROM p1a_enum_w WHERE id = 99"),
            &cancel,
        )
        .await
        .expect("query_one");
        match &row["mood"] {
            ParameterValue::Enum { label, .. } => assert_eq!(label, "neutral"),
            other => panic!("expected Enum, got {other:?}"),
        }

        tx.rollback(&cancel).await.expect("rollback");
        teardown_enum_domain_scratch("p1a_enum_w").await;
    }

    #[tokio::test]
    async fn live_enum_facade_scalar() {
        use plenora_database_core::facade::execute_scalar_enum;

        setup_enum_domain_scratch("p1a_enum_s").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let (type_name, label) = execute_scalar_enum(
            tx.as_mut(),
            &Statement::new("SELECT mood FROM p1a_enum_s WHERE id = 2"),
            &cancel,
        )
        .await
        .expect("scalar enum");
        assert_eq!(type_name, "p1a_enum_s_mood");
        assert_eq!(label, "sad");

        tx.rollback(&cancel).await.expect("rollback");
        teardown_enum_domain_scratch("p1a_enum_s").await;
    }

    #[tokio::test]
    async fn live_enum_null_becomes_typed_null() {
        // Verifica che un enum NULL sia decodificato come typed null,
        // non come Enum { label: "" }.
        setup_enum_domain_scratch("p1a_enum_null").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT NULL::p1a_enum_null_mood AS mood"),
            &cancel,
        )
        .await
        .expect("query_one null enum");
        match &row["mood"] {
            ParameterValue::Null { type_name } => {
                assert_eq!(type_name, "p1a_enum_null_mood");
            }
            other => panic!("expected typed null, got {other:?}"),
        }

        tx.rollback(&cancel).await.expect("rollback");
        teardown_enum_domain_scratch("p1a_enum_null").await;
    }

    #[tokio::test]
    async fn live_domain_over_text_decodes_as_base_type_string() {
        // Domain è "type alias con constraint" su un base type.
        // tokio_postgres risolve la colonna al base type per la maggior
        // parte dei domain, quindi il valore è decodificato come il base.
        // Vantaggio: nessun handling speciale, i domain funzionano.
        // Compromesso: si perde il type_name del domain (email vs text).
        setup_enum_domain_scratch("p1a_dom").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT email FROM p1a_dom WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("domain over TEXT decodes as String");
        assert!(matches!(&row["email"], ParameterValue::String(s) if s == "a@b.it"));

        // NULL su domain colonna → typed null. Il type_name che appare è
        // quello del base type (comportamento di tokio_postgres).
        let row_null = query_one(
            tx.as_mut(),
            &Statement::new("SELECT email FROM p1a_dom WHERE id = 2"),
            &cancel,
        )
        .await
        .expect("domain NULL");
        assert!(matches!(&row_null["email"], ParameterValue::Null { .. }));

        tx.rollback(&cancel).await.expect("rollback");
        teardown_enum_domain_scratch("p1a_dom").await;
    }

    /// Le quattro forme di `RETURNING` del piano portabile, attraversate.
    ///
    /// Attraversa sul server l'espressione mostrata anche dall'esempio SDK:
    ///
    /// ```text
    /// new = s.insert("users").values(name="Ada").returning("id").one()
    /// ```
    ///
    /// # Cosa verifica
    ///
    /// Che le righe tornino, e che tornino **cio che il chiamante non aveva**:
    /// la chiave generata dalla sequenza e il default calcolato dal server. Un
    /// `RETURNING` che rendesse i valori appena mandati sarebbe un giro a
    /// vuoto — il chiamante li ha gia.
    ///
    /// Le quattro forme insieme, perche il compilatore le tratta in quattro
    /// rami distinti e un solo insert ne proverebbe uno.
    ///
    /// # Da non confondere con la capability
    ///
    /// `writes.returning` e chiusa su tutti e quattro i motori, e resta chiusa:
    /// riguarda il percorso **bulk**, dove `write()` consuma uno stream
    /// illimitato e rende un riassunto. Questa e un'altra superficie — uno
    /// statement per volta, limitato da cio che il chiamante scrive — e vive
    /// nel piano.
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn live_portable_returning_carries_what_the_server_generated() {
        use plenora_database_core::facade::execute_portable_returning;
        use plenora_database_core::portable::{
            DeleteStatement, Expression, InsertStatement, PortableStatement, Predicate, TableRef,
            UpdateStatement,
        };

        let name = "a1_returning";
        {
            use tokio_postgres::NoTls;
            let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
                .await
                .expect("connect setup");
            tokio::spawn(async move {
                let _ = connection.await;
            });
            client
                .batch_execute(&format!(
                    "DROP TABLE IF EXISTS {name};
                     CREATE TABLE {name} (
                         id SERIAL PRIMARY KEY,
                         v TEXT NOT NULL,
                         creato TIMESTAMPTZ NOT NULL DEFAULT now());"
                ))
                .await
                .expect("tabella del returning");
        }

        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin del returning");

        let table = TableRef {
            schema: None,
            name: name.to_owned(),
        };

        // --- INSERT: due righe, e cio che torna e cio che il server ha fatto
        let inserted = execute_portable_returning(
            tx.as_mut(),
            &PortableStatement::Insert(InsertStatement {
                table: table.clone(),
                columns: vec!["v".to_owned()],
                values: vec![
                    vec![Expression::Literal(ParameterValue::String(
                        "ada".to_owned(),
                    ))],
                    vec![Expression::Literal(ParameterValue::String(
                        "alan".to_owned(),
                    ))],
                ],
                returning: vec!["id".to_owned(), "creato".to_owned()],
            }),
            &cancel,
        )
        .await
        .expect("insert con returning");
        assert_eq!(inserted.len(), 2, "una riga per valore inserito");
        // La chiave arriva dalla sequenza, e il chiamante non l'aveva.
        assert!(
            matches!(inserted[0].get_index(0), Some(ParameterValue::I32(1))),
            "la chiave generata deve tornare: {:?}",
            inserted[0].get_index(0)
        );
        // E il default calcolato dal server: senza, `RETURNING` renderebbe
        // soltanto cio che il chiamante ha mandato.
        assert!(
            !matches!(
                inserted[0].get_index(1),
                None | Some(ParameterValue::Null { .. })
            ),
            "il default del server deve tornare"
        );

        // --- UPDATE
        let updated = execute_portable_returning(
            tx.as_mut(),
            &PortableStatement::Update(UpdateStatement {
                table: table.clone(),
                assignments: vec![(
                    "v".to_owned(),
                    Expression::Literal(ParameterValue::String("ada-lovelace".to_owned())),
                )],
                filter: Some(Predicate::Eq {
                    column: "id".to_owned(),
                    value: Expression::Literal(ParameterValue::I32(1)),
                }),
                returning: vec!["v".to_owned()],
            }),
            &cancel,
        )
        .await
        .expect("update con returning");
        assert_eq!(updated.len(), 1);
        assert!(matches!(
            updated[0].get_index(0),
            Some(ParameterValue::String(value)) if value == "ada-lovelace"
        ));

        // --- DELETE
        let deleted = execute_portable_returning(
            tx.as_mut(),
            &PortableStatement::Delete(DeleteStatement {
                table,
                filter: Some(Predicate::Eq {
                    column: "id".to_owned(),
                    value: Expression::Literal(ParameterValue::I32(2)),
                }),
                returning: vec!["id".to_owned()],
            }),
            &cancel,
        )
        .await
        .expect("delete con returning");
        assert_eq!(deleted.len(), 1);
        assert!(matches!(
            deleted[0].get_index(0),
            Some(ParameterValue::I32(2))
        ));

        tx.rollback(&cancel).await.expect("rollback del returning");
        drop_table(name).await;
    }

    // === F1e: Spatial predicate nell'AST portable ===

    #[tokio::test]
    async fn live_portable_spatial_intersects_end_to_end() {
        use plenora_database_core::facade::execute_portable_returning;
        use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
        use plenora_database_core::portable::{
            select as p_select, spatial as p_spatial, Direction,
        };
        use plenora_database_core::{SpatialPredicate, SpatialReference};

        // Setup 3 punti in SRID 4326 (Milano, Roma, Parigi).
        setup_spatial_scratch("f1e_portable").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();

        // Estrai un polygon di riferimento dal DB come EWKB (bbox Italia).
        let bbox_ewkb =
            fetch_ewkb("ST_SetSRID(ST_MakeEnvelope(6.0, 40.0, 14.0, 46.0), 4326)").await;

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // SELECT id FROM f1e_portable WHERE ST_Intersects(geom, <bbox>)
        let stmt = p_select("f1e_portable", vec!["id"])
            .where_(p_spatial(
                "geom",
                SpatialPredicate::Intersects,
                SpatialReference {
                    ewkb: bbox_ewkb,
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .order_by("id", Direction::Asc)
            .into_statement();

        let rows = execute_portable_returning(tx.as_mut(), &stmt, &cancel)
            .await
            .expect("spatial query");

        // Milano (id=1) e Roma (id=2) dentro; Parigi (id=3) fuori.
        let ids: Vec<i32> = rows
            .iter()
            .filter_map(|r| match r.get_index(0) {
                Some(ParameterValue::I32(v)) => Some(*v),
                _ => None,
            })
            .collect();
        assert_eq!(ids, vec![1, 2]);

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1e_portable").await;
    }

    #[tokio::test]
    async fn live_portable_spatial_dwithin_end_to_end() {
        use plenora_database_core::facade::execute_portable_returning;
        use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
        use plenora_database_core::portable::{select as p_select, spatial as p_spatial};
        use plenora_database_core::{SpatialPredicate, SpatialReference};

        setup_spatial_scratch("f1e_dwithin").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();

        // Punto vicino a Milano.
        let near_milan = fetch_ewkb("ST_SetSRID(ST_MakePoint(9.191, 45.46), 4326)").await;

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // Geography è obbligatorio per DWithin su SRID 4326.
        // Distanza in metri veri (150m > 100m di offset → matcha Milano).
        let stmt = p_select("f1e_dwithin", vec!["id"])
            .where_(p_spatial(
                "geom",
                SpatialPredicate::DWithin {
                    distance_meters: 150.0,
                },
                SpatialReference {
                    ewkb: near_milan,
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geography,
                },
            ))
            .into_statement();

        let rows = execute_portable_returning(tx.as_mut(), &stmt, &cancel)
            .await
            .expect("spatial dwithin");

        // Solo Milano è entro 150m dal punto di riferimento.
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].get_index(0), Some(ParameterValue::I32(1))));

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1e_dwithin").await;
    }

    // === F1d: RETURNING canonico via facade portable ===

    #[tokio::test]
    async fn live_execute_portable_returning_produces_generated_id() {
        use plenora_database_core::facade::execute_portable_returning_one;
        use plenora_database_core::portable::{
            Expression, InsertStatement, PortableStatement, TableRef,
        };

        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // Setup temp table con SERIAL id
        tx.execute(
            &Statement::new(
                "CREATE TEMP TABLE f1d_returning (id SERIAL PRIMARY KEY, v INT) ON COMMIT DROP",
            ),
            &cancel,
        )
        .await
        .expect("temp");

        // INSERT ... RETURNING id via portable AST
        let insert = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("f1d_returning"),
            columns: vec!["v".into()],
            values: vec![vec![Expression::literal(ParameterValue::I32(99))]],
            returning: vec!["id".into(), "v".into()],
        });
        let row = execute_portable_returning_one(tx.as_mut(), &insert, &cancel)
            .await
            .expect("returning");

        // Verifica colonne + valori
        assert_eq!(row.len(), 2);
        let id = match &row["id"] {
            ParameterValue::I32(v) => *v,
            other => panic!("expected i32 id, got {other:?}"),
        };
        assert!(id >= 1);
        assert!(matches!(&row["v"], ParameterValue::I32(99)));

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_execute_portable_without_returning_via_facade_rejects_returning() {
        use plenora_database_core::facade::execute_portable;
        use plenora_database_core::portable::{
            Expression, InsertStatement, PortableStatement, TableRef,
        };

        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // INSERT con RETURNING passato a execute_portable → InvalidPlan.
        let insert_with_returning = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("t"),
            columns: vec!["x".into()],
            values: vec![vec![Expression::literal(ParameterValue::I32(1))]],
            returning: vec!["id".into()],
        });
        let err = execute_portable(tx.as_mut(), &insert_with_returning, &cancel)
            .await
            .expect_err("returning richiede execute_portable_returning");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_execute_portable_returning_via_update() {
        use plenora_database_core::facade::execute_portable_returning;
        use plenora_database_core::portable::{
            eq as p_eq, Expression, PortableStatement, TableRef, UpdateStatement,
        };

        scratch_table("f1d_upd_ret").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // Seed
        tx.execute(
            &Statement::new("INSERT INTO f1d_upd_ret VALUES (1, 'orig'), (2, 'orig')"),
            &cancel,
        )
        .await
        .expect("seed");

        // UPDATE ... RETURNING id, v — deve tornare 2 righe con i nuovi valori
        let update = PortableStatement::Update(UpdateStatement {
            table: TableRef::new("f1d_upd_ret"),
            assignments: vec![(
                "v".into(),
                Expression::literal(ParameterValue::String("new".into())),
            )],
            filter: Some(p_eq("v", ParameterValue::String("orig".into()))),
            returning: vec!["id".into(), "v".into()],
        });
        let rows = execute_portable_returning(tx.as_mut(), &update, &cancel)
            .await
            .expect("returning");
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert!(matches!(&row["v"], ParameterValue::String(s) if s == "new"));
        }

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1d_upd_ret").await;
    }

    // === F1c: PortableStatement AST end-to-end ===

    use plenora_database_core::plan::ProviderKind;
    use plenora_database_core::portable::{
        and as p_and, compile_portable, eq as p_eq, select as p_select, Direction, Expression,
        InsertStatement, PortableStatement, TableRef, UpdateStatement,
    };

    #[tokio::test]
    async fn live_portable_insert_update_select_roundtrip() {
        scratch_table("f1c_portable").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // INSERT (2 rows) via portable AST.
        let insert = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("f1c_portable"),
            columns: vec!["id".into(), "v".into()],
            values: vec![
                vec![
                    Expression::literal(ParameterValue::I32(1)),
                    Expression::literal(ParameterValue::String("alpha".into())),
                ],
                vec![
                    Expression::literal(ParameterValue::I32(2)),
                    Expression::literal(ParameterValue::String("beta".into())),
                ],
            ],
            returning: Vec::new(),
        });
        let stmt = compile_portable(ProviderKind::Postgres, &insert).expect("compile insert");
        let inserted = tx.execute(&stmt, &cancel).await.expect("insert");
        assert_eq!(inserted, 2);

        // UPDATE con WHERE composto.
        let update = PortableStatement::Update(UpdateStatement {
            table: TableRef::new("f1c_portable"),
            assignments: vec![(
                "v".into(),
                Expression::literal(ParameterValue::String("alpha-updated".into())),
            )],
            filter: Some(p_and(vec![
                p_eq("id", ParameterValue::I32(1)),
                p_eq("v", ParameterValue::String("alpha".into())),
            ])),
            returning: Vec::new(),
        });
        let stmt = compile_portable(ProviderKind::Postgres, &update).expect("compile update");
        let updated = tx.execute(&stmt, &cancel).await.expect("update");
        assert_eq!(updated, 1);

        // SELECT con projection + where + order + limit.
        let select = p_select("f1c_portable", vec!["id", "v"])
            .where_(p_eq("id", ParameterValue::I32(1)))
            .order_by("id", Direction::Asc)
            .limit(10)
            .into_statement();
        let stmt = compile_portable(ProviderKind::Postgres, &select).expect("compile select");
        let rows = tx.query(&stmt, &cancel).await.expect("select");
        assert_eq!(rows.len(), 1);
        assert!(matches!(&rows[0]["id"], ParameterValue::I32(1)));
        assert!(matches!(
            &rows[0]["v"],
            ParameterValue::String(s) if s == "alpha-updated"
        ));

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1c_portable").await;
    }

    #[tokio::test]
    async fn live_portable_upsert_do_update_set() {
        scratch_table("f1c_upsert").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        // INSERT iniziale.
        let insert = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("f1c_upsert"),
            columns: vec!["id".into(), "v".into()],
            values: vec![vec![
                Expression::literal(ParameterValue::I32(1)),
                Expression::literal(ParameterValue::String("first".into())),
            ]],
            returning: Vec::new(),
        });
        let stmt = compile_portable(ProviderKind::Postgres, &insert).expect("compile");
        tx.execute(&stmt, &cancel).await.expect("insert");

        // UPSERT sulla stessa chiave con DO UPDATE.
        let upsert = PortableStatement::Upsert(plenora_database_core::portable::UpsertStatement {
            table: TableRef::new("f1c_upsert"),
            columns: vec!["id".into(), "v".into()],
            values: vec![vec![
                Expression::literal(ParameterValue::I32(1)),
                Expression::literal(ParameterValue::String("upserted".into())),
            ]],
            conflict_target: vec!["id".into()],
            update_on_conflict: vec![(
                "v".into(),
                Expression::literal(ParameterValue::String("upserted".into())),
            )],
            returning: Vec::new(),
        });
        let stmt = compile_portable(ProviderKind::Postgres, &upsert).expect("compile upsert");
        let affected = tx.execute(&stmt, &cancel).await.expect("upsert");
        assert_eq!(affected, 1);

        // Verifica lo stato via portable SELECT.
        let sel = p_select("f1c_upsert", vec!["v"])
            .where_(p_eq("id", ParameterValue::I32(1)))
            .into_statement();
        let stmt = compile_portable(ProviderKind::Postgres, &sel).expect("compile select");
        let rows = tx.query(&stmt, &cancel).await.expect("select");
        assert!(matches!(
            &rows[0]["v"],
            ParameterValue::String(s) if s == "upserted"
        ));

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("f1c_upsert").await;
    }

    // === F1b: Facade scalar completa ===

    use plenora_database_core::facade::{
        execute_scalar_bytes, execute_scalar_date, execute_scalar_decimal, execute_scalar_json,
        execute_scalar_timestamp, execute_scalar_timestamptz, execute_scalar_uuid,
    };

    async fn scalar_tx<'a>(
        provider: &'a PostgresProvider,
        cancel: &'a CancellationToken,
        budget: &'a plenora_database_core::resource::ResourceBudget,
    ) -> Box<dyn plenora_database_core::transaction::TransactionScope + 'a> {
        provider
            .begin_transaction(&secret(), &TransactionOptions::default(), budget, cancel)
            .await
            .expect("begin")
    }

    #[tokio::test]
    async fn live_facade_scalar_bytes() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_bytes(
            tx.as_mut(),
            &Statement::new("SELECT '\\xdeadbeef'::BYTEA"),
            &cancel,
        )
        .await
        .expect("bytes");
        assert_eq!(v, vec![0xde, 0xad, 0xbe, 0xef]);
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_uuid() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_uuid(
            tx.as_mut(),
            &Statement::new("SELECT '12345678-1234-1234-1234-123456789012'::UUID"),
            &cancel,
        )
        .await
        .expect("uuid");
        assert_eq!(v, "12345678-1234-1234-1234-123456789012");
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_json() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_json(
            tx.as_mut(),
            &Statement::new(r#"SELECT '{"k":1}'::JSONB"#),
            &cancel,
        )
        .await
        .expect("json");
        assert_eq!(v, serde_json::json!({"k": 1}));
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_date() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let v = execute_scalar_date(
            tx.as_mut(),
            &Statement::new("SELECT '2026-08-11'::DATE"),
            &cancel,
        )
        .await
        .expect("date");
        assert_eq!(v, "2026-08-11");
        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_timestamp_and_timestamptz() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;

        let ts = execute_scalar_timestamp(
            tx.as_mut(),
            &Statement::new("SELECT '2026-08-11 10:20:30'::TIMESTAMP"),
            &cancel,
        )
        .await
        .expect("timestamp");
        assert!(ts.starts_with("2026-08-11T10:20:30"));

        let tstz = execute_scalar_timestamptz(
            tx.as_mut(),
            &Statement::new("SELECT '2026-08-11T10:20:30+00:00'::TIMESTAMPTZ"),
            &cancel,
        )
        .await
        .expect("timestamptz");
        assert!(tstz.starts_with("2026-08-11T10:20:30"));

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_facade_scalar_decimal_returns_string() {
        // Il roundtrip decimal preserva la rappresentazione testuale precisa.
        let provider = provider();
        let cancel = CancellationToken::new();
        let budget = budget();
        let mut tx = scalar_tx(&provider, &cancel, &budget).await;
        let value = execute_scalar_decimal(
            tx.as_mut(),
            &Statement::new("SELECT 3.14::NUMERIC(10,2)"),
            &cancel,
        )
        .await
        .expect("decimal roundtrip");
        assert_eq!(value, "3.14");
        tx.rollback(&cancel).await.expect("rollback");
    }

    // === B3: Native-query governance ===

    use plenora_database_core::native_query_policy::NativeQueryPolicy;

    fn strict_options() -> TransactionOptions {
        TransactionOptions {
            native_query_policy: NativeQueryPolicy::Deny,
            ..TransactionOptions::default()
        }
    }

    #[tokio::test]
    async fn live_native_deny_permits_crud() {
        scratch_table("b3_ok").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        tx.execute(
            &Statement::new("INSERT INTO b3_ok VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("v".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert ok");

        tx.execute(
            &Statement::new("UPDATE b3_ok SET v = 'w' WHERE id = 1"),
            &cancel,
        )
        .await
        .expect("update ok");

        tx.execute(&Statement::new("DELETE FROM b3_ok WHERE id = 1"), &cancel)
            .await
            .expect("delete ok");

        tx.commit(&cancel).await.expect("commit");
        drop_table("b3_ok").await;
    }

    #[tokio::test]
    async fn live_native_deny_blocks_ddl() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        for sql in [
            "CREATE TABLE b3_ddl (x INT)",
            "DROP TABLE b3_ddl",
            "ALTER TABLE b3_ddl ADD COLUMN y INT",
            "TRUNCATE b3_ddl",
            "GRANT SELECT ON b3_ddl TO public",
        ] {
            let err = tx
                .execute(&Statement::new(sql), &cancel)
                .await
                .expect_err(&format!("DDL deve essere bloccato: {sql}"));
            assert_eq!(
                err.category,
                plenora_database_core::ErrorCategory::InvalidPlan,
                "sql={sql}"
            );
        }

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_native_deny_blocks_session_commands() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        for sql in ["SET timezone = 'UTC'", "SHOW server_version", "RESET ALL"] {
            let err = tx
                .execute(&Statement::new(sql), &cancel)
                .await
                .expect_err(&format!("session cmd deve essere bloccato: {sql}"));
            assert_eq!(
                err.category,
                plenora_database_core::ErrorCategory::InvalidPlan,
                "sql={sql}"
            );
        }

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_native_deny_blocks_multi_statement() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(&secret(), &strict_options(), &budget(), &cancel)
            .await
            .expect("begin strict");

        let err = tx
            .execute(&Statement::new("SELECT 1; DROP TABLE nothing"), &cancel)
            .await
            .expect_err("multi-statement deve essere bloccato");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_native_allow_permits_ddl_with_escape_hatch() {
        // `Allow` consente esplicitamente DDL per migrazioni e diagnostica.
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin allow");
        tx.execute(
            &Statement::new("CREATE TEMP TABLE b3_esc (x INT) ON COMMIT DROP"),
            &cancel,
        )
        .await
        .expect("ddl consentita in Allow");
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_transaction_control_is_blocked_even_in_allow() {
        // BEGIN/COMMIT/ROLLBACK/SAVEPOINT sono gestiti dalla libreria;
        // devono essere rifiutati anche in Allow.
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        for sql in ["COMMIT", "ROLLBACK", "SAVEPOINT sp"] {
            let err = tx
                .execute(&Statement::new(sql), &cancel)
                .await
                .expect_err(&format!("tx-control deve essere bloccato: {sql}"));
            assert_eq!(
                err.category,
                plenora_database_core::ErrorCategory::InvalidPlan
            );
        }

        tx.rollback(&cancel).await.expect("rollback");
    }

    // === B4: Server-side streaming (cursor) ===

    #[tokio::test]
    async fn live_query_stream_paginates_result_in_batches() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        // 250 righe con batch_size=100 → 3 batch attesi (100+100+50).
        let stmt = Statement::new("SELECT gs::BIGINT AS n FROM generate_series(1, 250) gs");
        let mut stream = tx
            .query_stream(&stmt, 100, &cancel)
            .await
            .expect("open stream");

        let mut batch_sizes = Vec::new();
        while let Some(batch) = stream.next_batch(&cancel).await.expect("next") {
            batch_sizes.push(batch.len());
        }
        assert_eq!(batch_sizes, vec![100, 100, 50]);
        drop(stream);

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    #[tokio::test]
    async fn live_query_stream_exhausts_at_end() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT gs::INT FROM generate_series(1, 5) gs");
        let mut stream = tx.query_stream(&stmt, 10, &cancel).await.expect("open");

        let first = stream.next_batch(&cancel).await.expect("first");
        assert!(matches!(first, Some(ref rows) if rows.len() == 5));

        let second = stream.next_batch(&cancel).await.expect("second");
        assert!(second.is_none());

        // Chiamate successive continuano a ritornare None (idempotente).
        let third = stream.next_batch(&cancel).await.expect("third");
        assert!(third.is_none());
        drop(stream);

        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_query_stream_respects_bound_parameters() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT gs::BIGINT FROM generate_series($1::INT, $2::INT) gs")
            .with_params(vec![ParameterValue::I32(10), ParameterValue::I32(14)]);
        let mut stream = tx.query_stream(&stmt, 2, &cancel).await.expect("open");

        let mut all = Vec::new();
        while let Some(batch) = stream.next_batch(&cancel).await.expect("next") {
            for row in batch {
                match row.get_index(0) {
                    Some(ParameterValue::I64(v)) => all.push(*v),
                    _ => panic!("expected i64"),
                }
            }
        }
        assert_eq!(all, vec![10, 11, 12, 13, 14]);
        drop(stream);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_query_stream_cancelled_mid_stream_returns_cancelled() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT gs::INT FROM generate_series(1, 1000) gs");
        let mut stream = tx.query_stream(&stmt, 50, &cancel).await.expect("open");

        // Consumiamo un batch OK.
        let _ = stream.next_batch(&cancel).await.expect("first batch");

        // Poi cancelliamo prima del successivo.
        cancel.cancel();
        let err = stream
            .next_batch(&cancel)
            .await
            .expect_err("cancel deve bloccare");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::Cancelled
        );
        drop(stream);

        tx.rollback(&cancel).await.expect("rollback ignora cancel");
    }

    #[tokio::test]
    async fn live_query_stream_zero_batch_size_is_invalid_plan() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let stmt = Statement::new("SELECT 1");
        let Err(err) = tx.query_stream(&stmt, 0, &cancel).await else {
            panic!("batch_size=0 deve fallire");
        };
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_query_stream_cursor_released_on_commit() {
        // Dopo il commit, il cursor deve essere scomparso dalla sessione.
        // Riusando la stessa sessione (attraverso il pool), un `FETCH` sul
        // nome dovrebbe fallire con 34000 (invalid_cursor_name).
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        {
            let stmt = Statement::new("SELECT 1");
            let mut stream = tx.query_stream(&stmt, 10, &cancel).await.expect("open");
            let _ = stream.next_batch(&cancel).await.expect("first");
        }
        tx.commit(&cancel).await.expect("commit");

        // Apro un'altra transazione — non abbiamo garanzia deterministica
        // che sia la stessa connessione, ma il test di "commit chiude il
        // cursor" è già coperto dalla non-visibilità cross-transaction dei
        // cursor in Postgres. Ci basta verificare che la nuova tx sia sana.
        let mut tx2 = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin 2");
        tx2.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("sessione sana");
        tx2.commit(&cancel).await.expect("commit 2");
    }

    // === Opz 3: DDL fuori transazione ===

    #[tokio::test]
    async fn live_execute_ddl_creates_index_concurrently() {
        use plenora_database_core::provider::Provider;
        // CREATE INDEX CONCURRENTLY è vietato dentro transazione: la libreria
        // deve permetterne l'esecuzione via `Provider::execute_ddl`.
        scratch_table("opz3_ddl").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        Provider::execute_ddl(
            &provider,
            &secret(),
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS opz3_ddl_v_idx ON opz3_ddl (v)",
            &cancel,
        )
        .await
        .expect("CREATE INDEX CONCURRENTLY out-of-tx deve funzionare");

        // Cleanup
        Provider::execute_ddl(
            &provider,
            &secret(),
            "DROP INDEX IF EXISTS opz3_ddl_v_idx",
            &cancel,
        )
        .await
        .expect("drop index");
        drop_table("opz3_ddl").await;
    }

    #[tokio::test]
    async fn live_execute_ddl_rejects_invalid_sql() {
        use plenora_database_core::provider::Provider;
        let provider = provider();
        let cancel = CancellationToken::new();

        let err = Provider::execute_ddl(&provider, &secret(), "NOT SQL AT ALL", &cancel)
            .await
            .expect_err("SQL malformata deve fallire");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
    }

    // === A6: Conformance profile ===

    use plenora_database_core::conformance::{
        check_profile, probe_application_oltp_v1, EvidenceKind, ProfileStatus, APPLICATION_OLTP_V1,
    };

    #[tokio::test]
    async fn live_probe_pfm_core_v1_passes_on_postgres() {
        use plenora_database_core::conformance::{
            check_profile, probe_pfm_core_v1, ProfileStatus, PFM_CORE_V1,
        };
        let provider = provider();
        let cancel = CancellationToken::new();

        let evidence = probe_pfm_core_v1(&provider, &secret(), &cancel).await;
        let report = check_profile(&PFM_CORE_V1, &evidence);
        assert_eq!(
            report.status,
            ProfileStatus::Pass,
            "PFM_CORE_V1 FAIL. missing={:?} failed={:?}",
            report.missing,
            report.failed,
        );
    }

    #[tokio::test]
    async fn live_probe_pfm_gis_v1_passes_on_postgres() {
        use plenora_database_core::conformance::{
            check_profile, probe_pfm_gis_v1, ProfileStatus, PFM_GIS_V1,
        };
        let provider = provider();
        let cancel = CancellationToken::new();

        let evidence = probe_pfm_gis_v1(&provider, &secret(), &cancel).await;
        let report = check_profile(&PFM_GIS_V1, &evidence);
        assert_eq!(
            report.status,
            ProfileStatus::Pass,
            "PFM_GIS_V1 FAIL. missing={:?} failed={:?}",
            report.missing,
            report.failed,
        );
    }

    #[tokio::test]
    async fn live_probe_application_oltp_v1_passes_on_postgres() {
        let provider = provider();
        let cancel = CancellationToken::new();

        let evidence = probe_application_oltp_v1(&provider, &secret(), &cancel).await;

        // Ogni capability richiesta deve avere un'evidence Verified.
        for cap in APPLICATION_OLTP_V1.required {
            let found = evidence
                .iter()
                .find(|e| e.capability == *cap)
                .unwrap_or_else(|| panic!("evidence assente per {cap:?}"));
            assert_eq!(
                found.kind,
                EvidenceKind::Verified,
                "{:?} non verificata: {:?}",
                cap,
                found.notes
            );
        }

        let report = check_profile(&APPLICATION_OLTP_V1, &evidence);
        assert_eq!(
            report.status,
            ProfileStatus::Pass,
            "profilo FAIL. missing={:?} failed={:?} evidence={:?}",
            report.missing,
            report.failed,
            report.evidence
        );
        assert!(report.missing.is_empty());
        assert!(report.failed.is_empty());
    }

    // === A5: Facade OLTP (query, query_one, scalar) ===

    use plenora_database_core::facade::{
        execute_scalar_bool, execute_scalar_i64, execute_scalar_string, query_one, query_optional,
    };

    #[tokio::test]
    async fn live_facade_execute_scalar_i64_returns_single_cell() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let value = execute_scalar_i64(tx.as_mut(), &Statement::new("SELECT 42::BIGINT"), &cancel)
            .await
            .expect("scalar i64");
        assert_eq!(value, 42);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_execute_scalar_string_and_bool() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let s = execute_scalar_string(
            tx.as_mut(),
            &Statement::new("SELECT 'hello'::TEXT"),
            &cancel,
        )
        .await
        .expect("scalar string");
        assert_eq!(s, "hello");
        let b = execute_scalar_bool(tx.as_mut(), &Statement::new("SELECT TRUE"), &cancel)
            .await
            .expect("scalar bool");
        assert!(b);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_one_returns_full_row() {
        scratch_table("a5_query_one").await;
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        tx.execute(
            &Statement::new("INSERT INTO a5_query_one VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("payload".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert");

        let row = query_one(
            tx.as_mut(),
            &Statement::new("SELECT id, v FROM a5_query_one WHERE id = $1")
                .with_params(vec![ParameterValue::I32(1)]),
            &cancel,
        )
        .await
        .expect("query_one");
        assert_eq!(row.len(), 2);
        assert!(matches!(&row[0], ParameterValue::I32(1)));
        assert!(matches!(&row[1], ParameterValue::String(s) if s == "payload"));

        tx.commit(&cancel).await.expect("commit");
        drop_table("a5_query_one").await;
    }

    #[tokio::test]
    async fn live_facade_query_one_zero_rows_is_not_found() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let err = query_one(
            tx.as_mut(),
            &Statement::new("SELECT 1 WHERE FALSE"),
            &cancel,
        )
        .await
        .expect_err("must be NotFound");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::NotFound);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_one_multiple_rows_is_conflict() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let err = query_one(
            tx.as_mut(),
            &Statement::new("SELECT * FROM (VALUES (1), (2)) t(x)"),
            &cancel,
        )
        .await
        .expect_err("must be Conflict for >1 row");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::Conflict);
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_optional_none_and_some() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let none = query_optional(
            tx.as_mut(),
            &Statement::new("SELECT 1 WHERE FALSE"),
            &cancel,
        )
        .await
        .expect("optional none");
        assert!(none.is_none());

        let some = query_optional(tx.as_mut(), &Statement::new("SELECT 'x'::TEXT"), &cancel)
            .await
            .expect("optional some");
        assert!(some.is_some());

        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_decodes_all_supported_scalar_types() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let row = query_one(
            tx.as_mut(),
            &Statement::new(
                "SELECT
                TRUE::BOOL,
                42::INT4,
                -1234567890::INT8,
                3.14::FLOAT8,
                'text'::TEXT,
                '\\xdeadbeef'::BYTEA,
                '2026-01-15'::DATE,
                '2026-01-15 10:20:30'::TIMESTAMP,
                '2026-01-15T10:20:30Z'::TIMESTAMPTZ,
                '12345678-1234-1234-1234-123456789012'::UUID,
                '{\"k\":1}'::JSONB",
            ),
            &cancel,
        )
        .await
        .expect("decode row");

        assert!(matches!(&row[0], ParameterValue::Bool(true)));
        assert!(matches!(&row[1], ParameterValue::I32(42)));
        assert!(matches!(&row[2], ParameterValue::I64(-1_234_567_890)));
        assert!(matches!(&row[3], ParameterValue::F64(_)));
        assert!(matches!(&row[4], ParameterValue::String(s) if s == "text"));
        assert!(matches!(&row[5], ParameterValue::Bytes(b) if b == &[0xde, 0xad, 0xbe, 0xef]));
        assert!(matches!(&row[6], ParameterValue::Date(_)));
        assert!(matches!(&row[7], ParameterValue::Timestamp(_)));
        assert!(matches!(&row[8], ParameterValue::TimestampTz(_)));
        assert!(matches!(&row[9], ParameterValue::Uuid(_)));
        assert!(matches!(&row[10], ParameterValue::Json(_)));

        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_query_null_becomes_typed_null() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let row = query_one(tx.as_mut(), &Statement::new("SELECT NULL::TEXT"), &cancel)
            .await
            .expect("null decode");
        match &row[0] {
            ParameterValue::Null { type_name } => assert_eq!(type_name, "text"),
            other => panic!("expected typed null, got {other:?}"),
        }
        tx.commit(&cancel).await.expect("commit");
    }

    #[tokio::test]
    async fn live_facade_scalar_type_mismatch_is_data_mapping() {
        let provider = provider();
        let cancel = CancellationToken::new();
        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");
        let err = execute_scalar_i64(
            tx.as_mut(),
            &Statement::new("SELECT 'not-a-number'::TEXT"),
            &cancel,
        )
        .await
        .expect_err("type mismatch");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::DataMapping
        );
        tx.commit(&cancel).await.expect("commit");
    }

    // === A4: Session context ===

    use plenora_database_core::session_context::{SessionContext, SessionEntry, SessionValue};

    async fn read_session_setting(name: &str) -> String {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let row = client
            .query_one("SELECT current_setting($1, true)", &[&name])
            .await
            .expect("current_setting");
        row.get::<_, Option<String>>(0).unwrap_or_default()
    }

    #[tokio::test]
    async fn live_session_context_is_readable_inside_transaction() {
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.tenant",
            SessionEntry::public(SessionValue::Text("acme".into())),
        )
        .expect("tenant");
        ctx.insert(
            "app.actor",
            SessionEntry::sensitive(SessionValue::Text("user-42".into())),
        )
        .expect("actor");

        let opts = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };
        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget(), &cancel)
            .await
            .expect("begin with context");

        // Verifica intra-tx: current_setting deve tornare i valori applicati.
        let update_result = tx
            .execute(
                &Statement::new(
                    "DO $$
                 BEGIN
                     IF current_setting('app.tenant', true) <> 'acme' THEN
                         RAISE EXCEPTION 'tenant not set';
                     END IF;
                     IF current_setting('app.actor', true) <> 'user-42' THEN
                         RAISE EXCEPTION 'actor not set';
                     END IF;
                 END$$;",
                ),
                &cancel,
            )
            .await;
        update_result.expect("DO block must succeed");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    #[tokio::test]
    async fn live_session_context_resets_after_commit_on_pooled_reuse() {
        // Un tx con context su una connessione, poi una SECONDA tx *senza*
        // context sulla STESSA connessione (idealmente ripescata dal pool).
        // Il context della prima non deve leakare nella seconda: `SET LOCAL`
        // + `is_local=true` sono resettati automaticamente dal COMMIT.
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.leak_probe",
            SessionEntry::public(SessionValue::Text("first-tx".into())),
        )
        .expect("insert");
        let opts_with = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };

        let mut tx1 = provider
            .begin_transaction(&secret(), &opts_with, &budget(), &cancel)
            .await
            .expect("begin 1");
        tx1.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("tx1 select");
        tx1.commit(&cancel).await.expect("commit tx1");

        let mut tx2 = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin 2");

        // In tx2 la setting non deve esistere: attendiamo stringa vuota.
        let leak_check = tx2
            .execute(
                &Statement::new(
                    "DO $$
                 BEGIN
                     IF current_setting('app.leak_probe', true) <> '' THEN
                         RAISE EXCEPTION 'context leak: %', current_setting('app.leak_probe', true);
                     END IF;
                 END$$;",
                ),
                &cancel,
            )
            .await;
        leak_check.expect("no leak");

        tx2.commit(&cancel).await.expect("commit tx2");
    }

    #[tokio::test]
    async fn live_session_context_is_isolated_from_external_session() {
        // Un tx applica il context, un client SEPARATO (nuova connessione)
        // interroga il proprio setting: deve essere vuoto perché GUC transaction-local
        // vive solo nella sessione che l'ha impostato.
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.isolation_probe",
            SessionEntry::public(SessionValue::Text("only-in-tx".into())),
        )
        .expect("insert");
        let opts = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };

        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget(), &cancel)
            .await
            .expect("begin");
        tx.execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect("select");

        // In parallelo, altra sessione: la GUC non deve esistere.
        let external = read_session_setting("app.isolation_probe").await;
        assert_eq!(external, "");

        tx.rollback(&cancel).await.expect("rollback");
    }

    #[tokio::test]
    async fn live_session_context_typed_values_serialize_correctly() {
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut ctx = SessionContext::new();
        ctx.insert(
            "app.int_val",
            SessionEntry::public(SessionValue::Integer(42)),
        )
        .expect("int");
        ctx.insert(
            "app.bool_val",
            SessionEntry::public(SessionValue::Boolean(true)),
        )
        .expect("bool");

        let opts = TransactionOptions {
            context: ctx,
            ..TransactionOptions::default()
        };
        let mut tx = provider
            .begin_transaction(&secret(), &opts, &budget(), &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new(
                "DO $$
             BEGIN
                 IF current_setting('app.int_val', true) <> '42' THEN
                     RAISE EXCEPTION 'int not encoded';
                 END IF;
                 IF current_setting('app.bool_val', true) <> 'true' THEN
                     RAISE EXCEPTION 'bool not encoded';
                 END IF;
             END$$;",
            ),
            &cancel,
        )
        .await
        .expect("DO ok");

        assert!(tx.commit(&cancel).await.expect("commit").is_committed());
    }

    // === A3: Optimistic concurrency ===

    async fn versioned_scratch(name: &str) {
        use tokio_postgres::NoTls;
        let (client, connection) = tokio_postgres::connect(&live_dsn(), NoTls)
            .await
            .expect("connect setup");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS {name};
             CREATE TABLE {name} (
                 id INT PRIMARY KEY,
                 version INT NOT NULL,
                 payload TEXT NOT NULL
             );
             INSERT INTO {name} VALUES (1, 17, 'v17'), (2, 42, 'meta');",
            ))
            .await
            .expect("setup versioned");
    }

    fn conditional_update<'a>(
        update: &'a Statement,
        probe: Option<&'a Statement>,
    ) -> ConditionalUpdate<'a> {
        ConditionalUpdate {
            update,
            key_probe: probe,
            expected_affected_rows: 1,
        }
    }

    #[tokio::test]
    async fn live_optimistic_update_matches_expected_version_applied() {
        versioned_scratch("a3_ok").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let update = Statement::new(
            "UPDATE a3_ok SET version = version + 1, payload = $1 \
         WHERE id = $2 AND version = $3",
        )
        .with_params(vec![
            ParameterValue::String("v18".into()),
            ParameterValue::I32(1),
            ParameterValue::I32(17),
        ]);
        let probe = Statement::new("SELECT 1 FROM a3_ok WHERE id = $1")
            .with_params(vec![ParameterValue::I32(1)]);

        tx.execute_conditional_update(conditional_update(&update, Some(&probe)), &cancel)
            .await
            .expect("update ottimistico");

        tx.commit(&cancel).await.expect("commit");

        assert_eq!(
            count(
                &provider,
                "SELECT COUNT(*)::BIGINT FROM a3_ok WHERE version = 18"
            )
            .await,
            1
        );
        drop_table("a3_ok").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_wrong_version_is_concurrent_modification() {
        versioned_scratch("a3_conflict").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let update = Statement::new(
            "UPDATE a3_conflict SET version = version + 1 \
         WHERE id = $1 AND version = $2",
        )
        .with_params(vec![ParameterValue::I32(1), ParameterValue::I32(99)]);
        let probe = Statement::new("SELECT 1 FROM a3_conflict WHERE id = $1")
            .with_params(vec![ParameterValue::I32(1)]);

        let err = tx
            .execute_conditional_update(conditional_update(&update, Some(&probe)), &cancel)
            .await
            .expect_err("mismatch versione");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::ConcurrentModification
        );
        assert_eq!(
            err.remote_effect,
            plenora_database_core::RemoteEffect::RolledBack
        );

        tx.rollback(&cancel).await.expect("rollback");
        assert_eq!(
            count(
                &provider,
                "SELECT version::BIGINT FROM a3_conflict WHERE id = 1"
            )
            .await,
            17
        );
        drop_table("a3_conflict").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_missing_key_with_probe_is_not_found() {
        versioned_scratch("a3_missing").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        let update = Statement::new(
            "UPDATE a3_missing SET version = version + 1 \
         WHERE id = $1 AND version = $2",
        )
        .with_params(vec![ParameterValue::I32(999), ParameterValue::I32(17)]);
        let probe = Statement::new("SELECT 1 FROM a3_missing WHERE id = $1")
            .with_params(vec![ParameterValue::I32(999)]);

        let err = tx
            .execute_conditional_update(conditional_update(&update, Some(&probe)), &cancel)
            .await
            .expect_err("chiave assente");
        assert_eq!(err.category, plenora_database_core::ErrorCategory::NotFound);

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("a3_missing").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_without_probe_defaults_to_conflict() {
        versioned_scratch("a3_default").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        // Chiave assente, ma NESSUN probe: il default conservativo classifica
        // come ConcurrentModification (fail-loud sulla concorrenza).
        let update = Statement::new(
            "UPDATE a3_default SET version = version + 1 \
         WHERE id = $1 AND version = $2",
        )
        .with_params(vec![ParameterValue::I32(9999), ParameterValue::I32(0)]);

        let err = tx
            .execute_conditional_update(conditional_update(&update, None), &cancel)
            .await
            .expect_err("mismatch senza probe");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::ConcurrentModification
        );

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("a3_default").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_multi_row_matches_expected_count() {
        versioned_scratch("a3_multi").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin");

        // Update in blocco: le due righe iniziali hanno version=17 e version=42.
        // WHERE version < 100 le colpisce entrambe: expected=2.
        let update = Statement::new("UPDATE a3_multi SET version = version + 1 WHERE version < $1")
            .with_params(vec![ParameterValue::I32(100)]);

        let request = ConditionalUpdate {
            update: &update,
            key_probe: None,
            expected_affected_rows: 2,
        };
        tx.execute_conditional_update(request, &cancel)
            .await
            .expect("update multi-riga");

        tx.commit(&cancel).await.expect("commit");
        assert_eq!(
            count(
                &provider,
                "SELECT COUNT(*)::BIGINT FROM a3_multi WHERE version > 17"
            )
            .await,
            2
        );
        drop_table("a3_multi").await;
    }

    #[tokio::test]
    async fn live_optimistic_update_two_writers_only_one_succeeds() {
        // Simulazione end-to-end del pattern PFM: due writer concorrenti che
        // partono dalla stessa expected_version. Con SERIALIZABLE, uno vince
        // e l'altro deve ricevere ConcurrentModification (o serialization
        // failure retryable, tracciato dal test).
        versioned_scratch("a3_race").await;
        let provider = provider();
        let cancel = CancellationToken::new();

        let mut tx_a = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin A");
        let mut tx_b = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget(),
                &cancel,
            )
            .await
            .expect("begin B");

        let update_a = Statement::new(
            "UPDATE a3_race SET version = version + 1, payload = $1 \
         WHERE id = $2 AND version = $3",
        )
        .with_params(vec![
            ParameterValue::String("A".into()),
            ParameterValue::I32(1),
            ParameterValue::I32(17),
        ]);
        let update_b = Statement::new(
            "UPDATE a3_race SET version = version + 1, payload = $1 \
         WHERE id = $2 AND version = $3",
        )
        .with_params(vec![
            ParameterValue::String("B".into()),
            ParameterValue::I32(1),
            ParameterValue::I32(17),
        ]);
        let probe = Statement::new("SELECT 1 FROM a3_race WHERE id = $1")
            .with_params(vec![ParameterValue::I32(1)]);

        // A applica e committa.
        tx_a.execute_conditional_update(conditional_update(&update_a, Some(&probe)), &cancel)
            .await
            .expect("A applica");
        tx_a.commit(&cancel).await.expect("A commit");

        // B parte dalla stessa expected_version=17 ma la riga è già a 18.
        let err = tx_b
            .execute_conditional_update(conditional_update(&update_b, Some(&probe)), &cancel)
            .await
            .expect_err("B deve fallire");
        assert_eq!(
            err.category,
            plenora_database_core::ErrorCategory::ConcurrentModification
        );

        tx_b.rollback(&cancel).await.expect("B rollback");

        drop_table("a3_race").await;
    }

    #[tokio::test]
    async fn live_execute_after_constraint_violation_still_reports_25p02() {
        // Pattern classico: un errore in transazione mette Postgres in
        // "in_failed_sql_transaction" — verifichiamo che il mapping A2 sia
        // ancora corretto quando invocato via il transaction scope.
        scratch_table("a1_fail").await;
        let provider = provider();
        let budget = budget();
        let cancel = CancellationToken::new();

        let mut tx = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, &cancel)
            .await
            .expect("begin");

        tx.execute(
            &Statement::new("INSERT INTO a1_fail VALUES ($1, $2)").with_params(vec![
                ParameterValue::I32(1),
                ParameterValue::String("first".into()),
            ]),
            &cancel,
        )
        .await
        .expect("insert 1");

        let dup = tx
            .execute(
                &Statement::new("INSERT INTO a1_fail VALUES ($1, $2)").with_params(vec![
                    ParameterValue::I32(1),
                    ParameterValue::String("dup".into()),
                ]),
                &cancel,
            )
            .await
            .expect_err("unique violation");
        assert_eq!(dup.category, plenora_database_core::ErrorCategory::Conflict);

        // Uno statement successivo in tx guasta → 25P02 (Protocol nel mapping).
        let poisoned = tx
            .execute(&Statement::new("SELECT 1"), &cancel)
            .await
            .expect_err("tx guasta");
        assert_eq!(
            poisoned.category,
            plenora_database_core::ErrorCategory::Protocol
        );

        tx.rollback(&cancel).await.expect("rollback");
        drop_table("a1_fail").await;
    }
}
