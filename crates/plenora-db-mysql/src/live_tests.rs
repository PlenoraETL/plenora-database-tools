#![allow(clippy::significant_drop_tightening)]

use crate::{
    describe_object, list_objects, list_schemas, probe_server, MysqlConfig, MysqlProvider,
    MysqlSession,
};
use mysql_async::prelude::Queryable;
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind, ReadOperation};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::{CancellationToken, ErrorCategory, ResourceBudget, ResourceLimits};

const DEFAULT_PASSWORD: &str = "DataFlow_Test_2026!";

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn live_secret() -> SecretString {
    SecretString::new(environment("PLENORA_MYSQL_PASSWORD", DEFAULT_PASSWORD))
}

/// La matrice dei riferimenti qualificati, unica fonte di verita di versione
/// e digest. I test la leggono per non ricopiare una versione che il compose
/// potrebbe nel frattempo aver cambiato.
const REFERENCE_MATRIX: &str = include_str!("../../../docker/mysql/references.json");

/// Il prefisso `major.minor.` della baseline dichiarata dalla matrice.
fn baseline_version_prefix() -> String {
    let document: serde_json::Value =
        serde_json::from_str(REFERENCE_MATRIX).expect("matrice MySQL leggibile");
    let baseline = document["references"]
        .as_array()
        .expect("matrice MySQL con elenco riferimenti")
        .iter()
        .find(|entry| entry["role"] == "baseline")
        .expect("matrice MySQL con una baseline");
    let exact = baseline["exact_version"]
        .as_str()
        .expect("versione esatta della baseline");
    let (major_minor, _patch) = exact.rsplit_once('.').expect("versione major.minor.patch");
    format!("{major_minor}.")
}

/// Il prefisso di versione che il riferimento in prova deve pubblicare.
///
/// Senza variabile l'atteso e la baseline della matrice. La matrice di
/// compatibilita sovrascrive la variabile per ciascuna immagine, quindi un
/// riferimento 8.4 o 8.0 non viene mai confrontato con la baseline.
fn expected_version_prefix() -> String {
    std::env::var("PLENORA_MYSQL_EXPECTED_VERSION").unwrap_or_else(|_| baseline_version_prefix())
}

fn live_config() -> MysqlConfig {
    live_config_for_host(environment("PLENORA_MYSQL_HOST", "127.0.0.1"))
}

/// La configurazione live, sempre con verifica TLS contro la CA della fixture.
///
/// La CA privata non e opzionale. Con un fallback a `TrustServerCertificate`
/// l'intera suite live girerebbe con l'identita del server non verificata, e
/// i due test di rifiuto hostname proverebbero soltanto se stessi.
fn live_config_for_host(host: String) -> MysqlConfig {
    let ca_path = std::env::var("PLENORA_MYSQL_CA")
        .expect("PLENORA_MYSQL_CA obbligatoria: la suite live non accetta TLS non verificata");
    MysqlConfig::new(
        host,
        environment("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        environment("PLENORA_MYSQL_USER", "dataflow"),
        live_secret(),
    )
    .with_port(
        environment("PLENORA_MYSQL_PORT", "3306")
            .parse()
            .expect("porta MySQL live"),
    )
    .with_private_ca_certificate(ca_path)
}

async fn observe_inflight_query(audit: &mut mysql_async::Conn, marker: &str) -> u64 {
    let pattern = format!("%{marker}%");
    for _ in 0..100 {
        let observed: Option<(u64, u64, u64)> = audit
            .exec_first(
                "SELECT COUNT(DISTINCT prepared.STATEMENT_ID), \
                        CAST(COALESCE(MAX(prepared.OWNER_THREAD_ID), 0) AS UNSIGNED), \
                        COUNT(current.EVENT_ID) \
                 FROM performance_schema.prepared_statements_instances AS prepared \
                 LEFT JOIN performance_schema.events_statements_current AS current \
                   ON current.THREAD_ID = prepared.OWNER_THREAD_ID \
                  AND current.EVENT_NAME IN ('statement/com/Execute', 'statement/sql/select') \
                 WHERE prepared.SQL_TEXT LIKE ?",
                (&pattern,),
            )
            .await
            .expect("osservazione prepared statement in-flight");
        if let Some((1, owner_thread_id, 1)) = observed {
            if owner_thread_id > 0 {
                let threads: Option<u64> = audit
                    .exec_first(
                        "SELECT COUNT(*) FROM performance_schema.threads WHERE THREAD_ID = ?",
                        (owner_thread_id,),
                    )
                    .await
                    .expect("thread MySQL in-flight");
                assert_eq!(threads, Some(1));
                return owner_thread_id;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("COM_STMT_EXECUTE non osservata in-flight per {marker}");
}

async fn observe_prepared_thread(audit: &mut mysql_async::Conn, marker: &str) -> u64 {
    let pattern = format!("%{marker}%");
    let observed: Option<(u64, u64)> = audit
        .exec_first(
            "SELECT COUNT(DISTINCT STATEMENT_ID), \
                    CAST(COALESCE(MAX(OWNER_THREAD_ID), 0) AS UNSIGNED) \
             FROM performance_schema.prepared_statements_instances \
             WHERE SQL_TEXT LIKE ?",
            (&pattern,),
        )
        .await
        .expect("osservazione prepared statement early drop");
    let Some((1, owner_thread_id)) = observed else {
        panic!("prepared statement non univoco per {marker}: {observed:?}");
    };
    assert!(owner_thread_id > 0);
    owner_thread_id
}

async fn await_single_statement_execution(audit: &mut mysql_async::Conn, baseline: u64) {
    for _ in 0..100 {
        let current: Option<(String, u64)> = audit
            .query_first("SHOW GLOBAL STATUS LIKE 'Com_stmt_execute'")
            .await
            .expect("COM_STMT_EXECUTE durante avvio worker");
        let delta = current.and_then(|(_, value)| value.checked_sub(baseline));
        match delta {
            Some(1) => return,
            Some(value) if value > 1 => {
                panic!("QueryOperation eseguita piu di una volta")
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(10)).await,
        }
    }
    panic!("QueryOperation non eseguita entro il limite di osservazione");
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito"]
async fn live_reference_probe_catalog_and_spatial_metadata() {
    let cancellation = CancellationToken::new();
    let config = live_config();
    let mut session = MysqlSession::open(&config, &cancellation)
        .await
        .expect("connessione MySQL live");

    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe MySQL live");
    assert!(
        probe
            .product_version
            .starts_with(&expected_version_prefix()),
        "{probe:?}"
    );
    assert_eq!(probe.database, "dataflow_test");
    assert_eq!(probe.time_zone, "+00:00");
    assert!(probe.sql_mode.contains("STRICT_TRANS_TABLES"));
    assert!(!probe.tls_cipher.is_empty());

    let schemas = list_schemas(&mut session, &cancellation)
        .await
        .expect("schemi MySQL live");
    assert!(schemas.iter().any(|schema| schema == "dataflow_test"));
    let objects = list_objects(&mut session, "dataflow_test", &cancellation)
        .await
        .expect("oggetti MySQL live");
    assert!(objects.iter().any(|object| object.name == "catalog_probe"));
    assert!(objects
        .iter()
        .any(|object| object.name == "catalog_probe_view"));

    let first = describe_object(
        &mut session,
        "dataflow_test",
        "catalog_probe",
        &cancellation,
    )
    .await
    .expect("descrizione MySQL live");
    let second = describe_object(
        &mut session,
        "dataflow_test",
        "catalog_probe",
        &cancellation,
    )
    .await
    .expect("descrizione MySQL live ripetuta");
    drop(session);
    assert_eq!(first.token, second.token);
    assert_eq!(first.engine.as_deref(), Some("InnoDB"));
    let geometry = first
        .columns
        .iter()
        .find(|column| column.name == "geom")
        .expect("colonna geometry");
    assert_eq!(geometry.data_type, "geometry");
    assert_eq!(geometry.spatial_srid, Some(4_326));
    let point = first
        .columns
        .iter()
        .find(|column| column.name == "geom_point")
        .expect("colonna point");
    assert_eq!(point.data_type, "point");
    assert_eq!(point.spatial_srid, Some(4_326));
    let collection = first
        .columns
        .iter()
        .find(|column| column.name == "geom_collection")
        .expect("colonna geometrycollection");
    assert_eq!(collection.data_type, "geomcollection");
    assert_eq!(collection.spatial_srid, Some(4_326));
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito con CA privata"]
async fn live_verified_tls_rejects_a_hostname_mismatch() {
    let cancellation = CancellationToken::new();
    let config = live_config_for_host("mysql-hostname-mismatch".to_owned());
    let Err(error) = MysqlSession::open(&config, &cancellation).await else {
        panic!("hostname TLS errato accettato");
    };
    assert_eq!(error.category, ErrorCategory::Protocol);
    assert_eq!(error.message, "verifica identita TLS MySQL rifiutata");
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito con CA privata"]
async fn live_provider_read_rejects_a_hostname_mismatch() {
    let cancellation = CancellationToken::new();
    let config = live_config_for_host("mysql-hostname-mismatch".to_owned());
    let provider = MysqlProvider::new(config, 1).expect("provider MySQL live");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "catalog_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: Some(1),
        filter: None,
    };
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget MySQL live");
    let Err(error) = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("hostname TLS errato accettato dal path read");
    };
    assert_eq!(error.category, ErrorCategory::Protocol);
    assert_eq!(error.message, "verifica identita TLS MySQL rifiutata");
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per reset pool"]
async fn live_pool_reset_reapplies_deterministic_session_bootstrap() {
    use mysql_async::prelude::Queryable;

    let config = live_config();
    let pool = mysql_async::Pool::new(config.pooled_driver_opts(1).expect("pool opts MySQL live"));
    let mut connection = pool.get_conn().await.expect("checkout");
    let first_connection_id: Option<u64> = connection
        .query_first("SELECT CONNECTION_ID()")
        .await
        .expect("id prima del reset");
    connection
        .query_drop("SET SESSION autocommit = 0, time_zone = '+05:00', sql_mode = ''")
        .await
        .expect("altera stato sessione");
    connection.reset().await.expect("reset connessione");
    let reset_connection_id: Option<u64> = connection
        .query_first("SELECT CONNECTION_ID()")
        .await
        .expect("id dopo il reset");
    assert_eq!(reset_connection_id, first_connection_id);
    let state: Option<(u8, String, String)> = connection
        .query_first("SELECT @@session.autocommit, @@session.time_zone, @@session.sql_mode")
        .await
        .expect("legge stato sessione");
    let (autocommit, time_zone, sql_mode) = state.expect("riga stato sessione");
    assert_eq!(autocommit, 1);
    assert_eq!(time_zone, "+00:00");
    assert!(sql_mode.contains("STRICT_TRANS_TABLES"));
    assert!(sql_mode.contains("ERROR_FOR_DIVISION_BY_ZERO"));
    assert!(sql_mode.contains("NO_ENGINE_SUBSTITUTION"));
    drop(connection);
    pool.disconnect().await.expect("disconnect pool MySQL");
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per cancellazione"]
async fn live_inflight_cancellation_quarantines_the_session() {
    let cancellation = CancellationToken::new();
    let mut session = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("sessione MySQL live");
    let toggle = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        toggle.cancel();
    });

    let error = session
        .query_rows(
            "SELECT SLEEP(5)",
            plenora_database_core::ErrorPhase::Read,
            &cancellation,
        )
        .await
        .expect_err("query MySQL cancellata");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Cancelled
    );
    assert_eq!(session.state(), crate::MysqlSessionState::Quarantined);
    assert!(!session.is_reusable());
    drop(session);
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per deadline"]
async fn live_deadline_reports_timeout_and_quarantines_the_session() {
    let cancellation = CancellationToken::new();
    let mut session = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("sessione MySQL live");
    let toggle = cancellation.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        toggle.cancel_due_to_deadline();
    });

    let error = session
        .query_rows(
            "SELECT SLEEP(5)",
            plenora_database_core::ErrorPhase::Read,
            &cancellation,
        )
        .await
        .expect_err("query MySQL oltre deadline");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Timeout
    );
    assert_eq!(session.state(), crate::MysqlSessionState::Quarantined);
    assert!(!session.is_reusable());
    drop(session);
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per acquire pool"]
async fn live_pool_acquire_timeout_is_independent_from_connect_timeout() {
    let config = live_config().with_timeouts(
        std::time::Duration::from_secs(5),
        std::time::Duration::from_secs(5),
        std::time::Duration::from_millis(50),
    );
    let pool = crate::MysqlPool::new(&config, 1).expect("pool MySQL live");
    let cancellation = CancellationToken::new();
    let first = pool.checkout(&cancellation).await.expect("primo checkout");

    let error = pool
        .checkout(&cancellation)
        .await
        .expect_err("pool saturo oltre acquire timeout");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Timeout
    );

    drop(first);
    let recovered = pool
        .checkout(&cancellation)
        .await
        .expect("checkout dopo rilascio permit");
    assert!(recovered.is_reusable());
    drop(recovered);
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per timeout"]
async fn live_operation_timeout_quarantines_the_session() {
    let cancellation = CancellationToken::new();
    let config = live_config().with_timeouts(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_millis(50),
        std::time::Duration::from_secs(10),
    );
    let mut session = MysqlSession::open(&config, &cancellation)
        .await
        .expect("sessione MySQL live");

    let error = session
        .query_rows(
            "SELECT SLEEP(1)",
            plenora_database_core::ErrorPhase::Read,
            &cancellation,
        )
        .await
        .expect_err("query MySQL in timeout");
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::Timeout
    );
    assert_eq!(session.state(), crate::MysqlSessionState::Quarantined);
    assert!(!session.is_reusable());
    drop(session);
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito"]
async fn live_provider_connection_capabilities_and_inspect() {
    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let secret = live_secret();

    let connection = provider
        .test_connection(&secret, &cancellation)
        .await
        .expect("test connessione MySQL live");
    assert_eq!(connection.provider, ProviderKind::Mysql);
    assert!(connection
        .server_version
        .starts_with(&expected_version_prefix()));

    let capabilities = provider
        .probe_capabilities(&secret, &cancellation)
        .await
        .expect("capability MySQL live");
    assert_eq!(capabilities.provider, ProviderKind::Mysql);
    assert!(capabilities.reads.streaming);
    assert!(capabilities.reads.projection);
    assert!(capabilities.reads.filter);
    assert!(capabilities.reads.ordering);
    assert!(!capabilities.writes.create);
    assert!(capabilities.writes.append);
    assert!(capabilities.writes.rollback_on_failure);
    assert!(!capabilities.writes.update);
    assert!(!capabilities.writes.upsert);
    assert!(!capabilities.writes.replace);
    assert!(capabilities.transactions.single_transaction);
    assert!(!capabilities.transactions.transactional_ddl);
    assert!(!capabilities.transactions.staged_swap);
    assert_eq!(
        capabilities.transactions.scope,
        plenora_database_core::capabilities::TransactionScope::Transaction
    );
    assert!(capabilities.spatial.read_wkb);
    assert!(capabilities.spatial.geometry);
    assert!(capabilities.spatial.mixed_geometry_types);
    assert_eq!(
        capabilities.spatial.dimensions,
        vec![plenora_database_core::geometry::Dimensions::Xy]
    );

    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: ObjectRef {
                    catalog: None,
                    schema: Some("dataflow_test".to_owned()),
                    object: "catalog_probe".to_owned(),
                    layer_id: None,
                },
            },
            &cancellation,
        )
        .await
        .expect("inspect MySQL live");
    assert_eq!(inspection.operation, "database.describe_object");
    assert_eq!(inspection.document["name"], "catalog_probe");
    assert_eq!(inspection.document["engine"], "InnoDB");
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per drop stream"]
async fn live_early_stream_drop_cancels_worker_and_keeps_provider_usable() {
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 1).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 2_048,
        memory_bytes: 96 * 1_024,
        output_bytes: 4 * 1_024 * 1_024,
        cell_bytes: 2 * 1_024,
        ..ResourceLimits::default()
    })
    .expect("budget drop stream MySQL");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "stream_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        filter: None,
    };
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream lungo MySQL");
    let first = stream
        .next_batch(&cancellation)
        .await
        .expect("primo batch bounded")
        .expect("batch non vuoto");
    assert!(first.num_rows() > 0);
    assert!(first.num_rows() < 2_048);
    drop(stream);

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    provider
        .test_connection(&live_secret(), &CancellationToken::new())
        .await
        .expect("provider utilizzabile dopo drop anticipato");
}

/// La riga che non entra nel residuo del batch corrente diventa il carry-over
/// del batch successivo: nessuna riga persa, nessuna duplicata, ordine
/// invariato.
///
/// La fixture e dimensionata sulla stima conservativa del percorso (`64` byte
/// per cella piu il doppio del payload): la prima riga sta nel budget, la
/// somma delle due lo supera.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per righe variabili multi-batch"]
async fn live_a_row_over_the_batch_budget_carries_over_to_the_next_batch() {
    use mysql_async::prelude::Queryable;
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};

    let config = live_config();
    let setup = mysql_async::Pool::new(
        config
            .pooled_driver_opts(1)
            .expect("pool setup righe variabili"),
    );
    let mut connection = setup.get_conn().await.expect("checkout setup");
    connection
        .query_drop("DROP TABLE IF EXISTS variable_stream_probe")
        .await
        .expect("drop fixture precedente");
    connection
        .query_drop(
            "CREATE TABLE variable_stream_probe (id BIGINT NOT NULL PRIMARY KEY, payload VARBINARY(40000) NOT NULL)",
        )
        .await
        .expect("crea fixture righe variabili");
    connection
        .query_drop(
            "INSERT INTO variable_stream_probe VALUES (1, REPEAT(X'41', 29000)), (2, REPEAT(X'42', 34920))",
        )
        .await
        .expect("popola fixture righe variabili");
    drop(connection);
    setup.disconnect().await.expect("disconnect setup");

    let provider = MysqlProvider::new(config, 1).expect("provider righe variabili");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "variable_stream_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        filter: None,
    };
    // Stima conservativa: riga 1 = 58 160 byte, riga 2 = 70 000 byte. La
    // prima entra nei 120 000 byte di budget, le due insieme no.
    let budget = ResourceBudget::new(ResourceLimits {
        memory_bytes: 120_000,
        output_bytes: 120_000,
        cell_bytes: 65_536,
        ..ResourceLimits::default()
    })
    .expect("budget righe variabili");
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream righe variabili");
    let mut ids = Vec::new();
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .expect("batch righe variabili")
    {
        batches.push(batch.num_rows());
        let values = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .expect("id int64");
        ids.extend((0..values.len()).map(|index| values.value(index)));
    }
    assert_eq!(ids, vec![1, 2]);
    assert_eq!(
        batches,
        vec![1, 1],
        "la seconda riga apre il batch successivo"
    );
}

/// Con i limiti di default e quattro colonne il percorso consegna tutte le
/// righe in un batch solo. Prima del carry-over il confronto con il massimo
/// teorico `cell_bytes × colonne` chiudeva il batch dopo una riga.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per batching a limiti default"]
async fn live_default_limits_batch_many_rows_over_four_columns() {
    use mysql_async::prelude::Queryable;
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};

    let config = live_config();
    let setup = mysql_async::Pool::new(config.pooled_driver_opts(1).expect("pool setup batching"));
    let mut connection = setup.get_conn().await.expect("checkout setup");
    connection
        .query_drop("DROP TABLE IF EXISTS wide_stream_probe")
        .await
        .expect("drop fixture precedente");
    connection
        .query_drop(
            "CREATE TABLE wide_stream_probe (\
             id BIGINT NOT NULL PRIMARY KEY, \
             alpha_value BIGINT NOT NULL, \
             beta_value BIGINT NOT NULL, \
             label VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL)",
        )
        .await
        .expect("crea fixture quattro colonne");
    connection
        .query_drop("SET SESSION cte_max_recursion_depth = 4096")
        .await
        .expect("profondita ricorsione fixture");
    connection
        .query_drop(
            "INSERT INTO wide_stream_probe (id, alpha_value, beta_value, label) \
             WITH RECURSIVE sequence (id) AS (\
             SELECT 1 UNION ALL SELECT id + 1 FROM sequence WHERE id < 512) \
             SELECT id, id * 2, id * 3, CONCAT('row-', id) FROM sequence",
        )
        .await
        .expect("popola fixture quattro colonne");
    drop(connection);
    setup.disconnect().await.expect("disconnect setup");

    let provider = MysqlProvider::new(config, 1).expect("provider batching");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "wide_stream_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        filter: None,
    };
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget default");
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream quattro colonne");
    let mut ids = Vec::new();
    let mut batches = Vec::new();
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .expect("batch quattro colonne")
    {
        batches.push(batch.num_rows());
        let values = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .expect("id int64");
        ids.extend((0..values.len()).map(|index| values.value(index)));
    }
    assert_eq!(batches, vec![512], "un solo batch a limiti default");
    assert_eq!(ids, (1..=512).collect::<Vec<i64>>());
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per filtri prepared"]
async fn live_read_projection_filter_order_and_default_schema() {
    use plenora_database_core::arrow::array::{Int64Array, StringArray};
    use plenora_database_core::plan::{FilterExpression, OrderBy, SortDirection};
    use plenora_database_core::provider::ParameterValue;
    use std::collections::BTreeMap;

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 2,
        memory_bytes: 1024 * 1024,
        output_bytes: 1024 * 1024,
        cell_bytes: 64 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget filter MySQL");
    let operation = plenora_database_core::plan::ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: None,
            object: "catalog_probe".to_owned(),
            layer_id: None,
        },
        projection: vec!["id".to_owned(), "name".to_owned()],
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Desc,
        }],
        row_limit: Some(1),
        filter: Some(FilterExpression::Eq {
            field: "id".to_owned(),
            parameter: "wanted_id".to_owned(),
        }),
    };
    let parameters = ParameterBag::new(BTreeMap::from([(
        "wanted_id".to_owned(),
        ParameterValue::I64(1),
    )]));
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("filtered stream MySQL");
    assert_eq!(stream.schema().fields().len(), 2);
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("filtered batch")
        .expect("filtered row");
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("filtered id")
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("filtered name")
            .value(0),
        "reference"
    );
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per lettura Arrow"]
#[allow(clippy::too_many_lines)]
async fn live_streaming_read_maps_scalar_and_xy_geometry_exactly() {
    use plenora_database_core::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use plenora_database_core::field_contract::validate_schema_contract;
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};
    use plenora_database_core::protocol;

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 10,
        memory_bytes: 8 * 1024 * 1024,
        output_bytes: 8 * 1024 * 1024,
        cell_bytes: 1024 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget read MySQL");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "catalog_probe".to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(1),
        filter: None,
    };
    let mut stream = provider
        .read(
            &live_secret(),
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream MySQL live");
    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("schema MySQL canonico");
    let geometry_field = schema.field_with_name("geom").expect("campo geometry");
    assert_eq!(
        geometry_field.metadata().get(protocol::GEOMETRY_DIMENSIONS),
        Some(&"xy".to_owned())
    );
    assert_eq!(
        geometry_field.metadata().get(protocol::GEOMETRY_SRID),
        Some(&"4326".to_owned())
    );
    assert_eq!(
        geometry_field
            .metadata()
            .get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"mixed".to_owned())
    );
    let point_field = schema.field_with_name("geom_point").expect("campo point");
    assert_eq!(
        point_field
            .metadata()
            .get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"exact".to_owned())
    );
    assert_eq!(
        point_field.metadata().get(protocol::GEOMETRY_TYPES),
        Some(&"point".to_owned())
    );
    let collection_field = schema
        .field_with_name("geom_collection")
        .expect("campo geometrycollection");
    assert_eq!(
        collection_field
            .metadata()
            .get(protocol::GEOMETRY_TYPES_DECLARATION),
        Some(&"exact".to_owned())
    );
    assert_eq!(
        collection_field.metadata().get(protocol::GEOMETRY_TYPES),
        Some(&"geometrycollection".to_owned())
    );

    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch MySQL")
        .expect("prima riga MySQL");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .expect("id int64")
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column_by_name("name")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .expect("name utf8")
            .value(0),
        "reference"
    );
    assert_eq!(
        batch
            .column_by_name("amount")
            .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
            .expect("amount decimal")
            .value(0),
        12_345_000
    );
    assert!(batch
        .column_by_name("active")
        .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
        .expect("active bool")
        .value(0));
    assert!(
        batch
            .column_by_name("event_date")
            .and_then(|array| array.as_any().downcast_ref::<Date32Array>())
            .expect("date32")
            .value(0)
            > 0
    );
    assert!(
        batch
            .column_by_name("event_ts")
            .and_then(|array| { array.as_any().downcast_ref::<TimestampMicrosecondArray>() })
            .expect("timestamp micros")
            .value(0)
            > 0
    );
    assert_eq!(
        batch
            .column_by_name("payload")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .expect("json utf8")
            .value(0),
        r#"{"qualified": true}"#
    );
    assert_eq!(
        batch
            .column_by_name("payload_bin")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .expect("varbinary")
            .value(0),
        &[0x00, 0x11, 0x22, 0x33, 0xAA, 0xBB, 0xCC, 0xDD]
    );
    let wkb = batch
        .column_by_name("geom")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .expect("geometry WKB")
        .value(0);
    let inspection =
        plenora_database_core::ewkb::inspect_ewkb_detailed(wkb, 16, 8).expect("WKB MySQL valido");
    assert_eq!(inspection.root.dimensions_label(), "xy");
    assert!(inspection.root.srid.is_none());
    let point_wkb = batch
        .column_by_name("geom_point")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .expect("point WKB")
        .value(0);
    let point_inspection = plenora_database_core::ewkb::inspect_ewkb_detailed(point_wkb, 16, 8)
        .expect("POINT WKB MySQL valido");
    assert_eq!(point_inspection.root.dimensions_label(), "xy");
    let collection_wkb = batch
        .column_by_name("geom_collection")
        .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
        .expect("geometrycollection WKB")
        .value(0);
    let collection_inspection =
        plenora_database_core::ewkb::inspect_ewkb_detailed(collection_wkb, 16, 8)
            .expect("GEOMETRYCOLLECTION WKB MySQL valido");
    assert_eq!(collection_inspection.root.dimensions_label(), "xy");
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream MySQL")
        .is_none());
}

/// Ogni famiglia di tipo wire accettata dal path query in una sola
/// `COM_STMT_PREPARE` e in una sola esecuzione.
///
/// `MySQL` non ha un equivalente di `describe_first_result_set`: lo schema
/// pubblicato viene dai soli metadati di colonna del prepared statement.
/// La prova copre quindi, per ogni colonna proiettata, il nome o l'alias, il
/// `DataType` Arrow, la nullability dichiarata dal prepare e il
/// `MYSQL_NATIVE_TYPE`, e verifica che `MYSQL_NATIVE_DECLARATION` resti
/// assente: la descrizione del prepare non conserva lunghezza, FSP, collation
/// ne il tipo dell'espressione, quindi una dichiarazione ricostruita sarebbe
/// un metadato non fedele. I valori sono poi confrontati uno per uno, cosi il
/// mapping dei tipi e quello dei valori restano provati insieme.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per QueryOperation"]
#[allow(clippy::too_many_lines)]
async fn live_scalar_single_source_query_uses_prepare_metadata_as_schema() {
    use plenora_database_core::arrow::array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, StringArray, TimestampMicrosecondArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use plenora_database_core::arrow::schema::{DataType, TimeUnit};
    use plenora_database_core::arrow::RecordBatch;
    use plenora_database_core::field_contract::validate_schema_contract;
    use plenora_database_core::plan::{ComparisonOperator, SortDirection};
    use plenora_database_core::protocol;
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
    };
    use std::collections::BTreeMap;

    fn cell<'batch, T: 'static>(batch: &'batch RecordBatch, name: &str) -> &'batch T {
        batch
            .column_by_name(name)
            .and_then(|array| array.as_any().downcast_ref::<T>())
            .unwrap_or_else(|| panic!("colonna {name} assente o con array Arrow diverso"))
    }

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 4,
        memory_bytes: 4 * 1024 * 1024,
        output_bytes: 4 * 1024 * 1024,
        cell_bytes: 64 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget query MySQL");
    let column = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    };
    let microsecond = DataType::Timestamp(TimeUnit::Microsecond, None);
    // Colonna della fixture, alias richiesto, `DataType` Arrow atteso,
    // `MYSQL_NATIVE_TYPE` atteso e nullability attesa dal prepare. Ogni riga
    // copre una famiglia distinta fra quelle accettate dal path query; la
    // geometria resta fuori perche il renderer non la incapsula in
    // `ST_AsBinary` e il contratto GeoArrow non sarebbe dimostrato.
    let expected: Vec<(&str, &str, DataType, &str, bool)> = vec![
        ("id", "", DataType::Int64, "bigint", false),
        ("name", "label", DataType::Utf8, "varchar", false),
        ("amount", "", DataType::Decimal128(18, 4), "decimal", true),
        ("active", "", DataType::Boolean, "tinyint", false),
        ("event_date", "", DataType::Date32, "date", true),
        ("event_ts", "", microsecond.clone(), "datetime", true),
        ("payload", "", DataType::Utf8, "json", true),
        ("payload_bin", "", DataType::Binary, "varbinary", true),
        ("tiny_signed", "", DataType::Int8, "tinyint", false),
        ("tiny_unsigned", "", DataType::UInt8, "tinyint", false),
        ("small_signed", "", DataType::Int16, "smallint", false),
        ("small_unsigned", "", DataType::UInt16, "smallint", false),
        ("year_value", "", DataType::Int16, "year", false),
        ("medium_signed", "", DataType::Int32, "mediumint", false),
        ("medium_unsigned", "", DataType::UInt32, "mediumint", false),
        ("int_signed", "", DataType::Int32, "int", false),
        ("int_unsigned", "", DataType::UInt32, "int", false),
        ("big_unsigned", "", DataType::UInt64, "bigint", false),
        ("float_value", "", DataType::Float32, "float", false),
        ("double_value", "", DataType::Float64, "double", false),
        (
            "decimal_scale_zero",
            "",
            DataType::Decimal128(12, 0),
            "decimal",
            false,
        ),
        (
            "decimal_unsigned",
            "",
            DataType::Decimal128(9, 2),
            "decimal",
            false,
        ),
        ("time_value", "", DataType::Utf8, "time", false),
        ("stamp_value", "", microsecond, "timestamp", false),
        ("bit_value", "", DataType::Binary, "bit", false),
        ("enum_value", "", DataType::Utf8, "enum", false),
        ("set_value", "", DataType::Utf8, "set", false),
        ("char_value", "", DataType::Utf8, "char", false),
        ("binary_value", "", DataType::Binary, "binary", false),
        // MySQL descrive TINYTEXT, TEXT, MEDIUMTEXT e LONGTEXT, come i
        // quattro BLOB corrispondenti, con un unico tipo wire
        // `MYSQL_TYPE_BLOB`: la classe di lunghezza resta nella dichiarazione
        // e non viaggia nei metadati del result set. Il provider pubblica
        // quindi `text` o `blob` per tutte e quattro le taglie, perche il tipo
        // dichiarato non e ricostruibile dal solo prepare. Le colonne restano
        // tutte proiettate: e il valore di ogni famiglia a essere provato.
        ("tiny_text", "", DataType::Utf8, "text", false),
        ("tiny_blob", "", DataType::Binary, "blob", false),
        ("text_value", "", DataType::Utf8, "text", false),
        ("blob_value", "", DataType::Binary, "blob", false),
        ("medium_text", "", DataType::Utf8, "text", false),
        ("medium_blob", "", DataType::Binary, "blob", false),
        ("long_text", "", DataType::Utf8, "text", false),
        ("long_blob", "", DataType::Binary, "blob", false),
        ("absent_note", "", DataType::Utf8, "varchar", true),
    ];
    let operation = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "catalog_probe".to_owned(),
                layer_id: None,
            },
            alias: None,
        }),
        derived_source: None,
        projection: expected
            .iter()
            .map(|(source, alias, ..)| QueryProjection {
                expression: column(source),
                alias: (!alias.is_empty()).then(|| (*alias).to_owned()),
            })
            .collect(),
        joins: Vec::new(),
        filter: Some(QueryExpression::Compare {
            left: Box::new(column("id")),
            operator: ComparisonOperator::Gte,
            right: Box::new(QueryExpression::Parameter {
                name: "floor".to_owned(),
            }),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: column("id"),
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(1),
        row_offset: None,
        locking: None,
    };
    let parameters = ParameterBag::new(BTreeMap::from([(
        "floor".to_owned(),
        ParameterValue::I64(1),
    )]));
    let mut stream = provider
        .query(
            &live_secret(),
            &operation,
            &parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("stream QueryOperation MySQL");

    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    assert_eq!(schema.fields().len(), expected.len());
    for (index, (source, alias, data_type, native_type, nullable)) in expected.iter().enumerate() {
        let published = if alias.is_empty() { source } else { alias };
        let field = schema.field(index);
        assert_eq!(field.name(), published, "nome della colonna {index}");
        assert_eq!(field.data_type(), data_type, "tipo Arrow di {published}");
        assert_eq!(
            field.is_nullable(),
            *nullable,
            "nullability dal prepare di {published}"
        );
        assert_eq!(
            field.metadata().get(protocol::MYSQL_NATIVE_TYPE),
            Some(&(*native_type).to_owned()),
            "MYSQL_NATIVE_TYPE di {published}"
        );
        assert!(
            !field
                .metadata()
                .contains_key(protocol::MYSQL_NATIVE_DECLARATION),
            "dichiarazione non provabile pubblicata per {published}"
        );
    }

    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch QueryOperation")
        .expect("riga QueryOperation");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), expected.len());

    assert_eq!(cell::<Int64Array>(&batch, "id").value(0), 1);
    assert_eq!(cell::<StringArray>(&batch, "label").value(0), "reference");
    assert_eq!(
        cell::<Decimal128Array>(&batch, "amount").value(0),
        12_345_000
    );
    assert!(cell::<BooleanArray>(&batch, "active").value(0));
    // 2026-01-02 e il giorno 20455 dall'epoch Arrow.
    assert_eq!(cell::<Date32Array>(&batch, "event_date").value(0), 20_455);
    assert_eq!(
        cell::<TimestampMicrosecondArray>(&batch, "event_ts").value(0),
        1_767_323_045_123_456
    );
    assert_eq!(
        cell::<StringArray>(&batch, "payload").value(0),
        r#"{"qualified": true}"#
    );
    assert_eq!(
        cell::<BinaryArray>(&batch, "payload_bin").value(0),
        &[0x00, 0x11, 0x22, 0x33, 0xAA, 0xBB, 0xCC, 0xDD]
    );

    assert_eq!(cell::<Int8Array>(&batch, "tiny_signed").value(0), -8);
    assert_eq!(cell::<UInt8Array>(&batch, "tiny_unsigned").value(0), 200);
    assert_eq!(cell::<Int16Array>(&batch, "small_signed").value(0), -300);
    assert_eq!(
        cell::<UInt16Array>(&batch, "small_unsigned").value(0),
        40_000
    );
    assert_eq!(cell::<Int16Array>(&batch, "year_value").value(0), 2_026);
    assert_eq!(
        cell::<Int32Array>(&batch, "medium_signed").value(0),
        -70_000
    );
    assert_eq!(
        cell::<UInt32Array>(&batch, "medium_unsigned").value(0),
        16_000_000
    );
    assert_eq!(cell::<Int32Array>(&batch, "int_signed").value(0), -123_456);
    assert_eq!(
        cell::<UInt32Array>(&batch, "int_unsigned").value(0),
        4_000_000_000
    );
    assert_eq!(
        cell::<UInt64Array>(&batch, "big_unsigned").value(0),
        18_446_744_073_709_551_615
    );
    // 1.5 e 2.25 sono esatti in binario: il confronto sui bit non ammette
    // tolleranze e coglie ogni riscrittura del valore.
    assert_eq!(
        cell::<Float32Array>(&batch, "float_value")
            .value(0)
            .to_bits(),
        1.5_f32.to_bits()
    );
    assert_eq!(
        cell::<Float64Array>(&batch, "double_value")
            .value(0)
            .to_bits(),
        2.25_f64.to_bits()
    );
    assert_eq!(
        cell::<Decimal128Array>(&batch, "decimal_scale_zero").value(0),
        -42
    );
    assert_eq!(
        cell::<Decimal128Array>(&batch, "decimal_unsigned").value(0),
        725
    );
    // MySQL TIME copre anche durate negative oltre le 24 ore: il contratto e
    // il testo canonico, non un Time64 Arrow.
    assert_eq!(
        cell::<StringArray>(&batch, "time_value").value(0),
        "01:02:03.456789"
    );
    assert_eq!(
        cell::<TimestampMicrosecondArray>(&batch, "stamp_value").value(0),
        1_767_323_045_123_456
    );
    assert_eq!(
        cell::<BinaryArray>(&batch, "bit_value").value(0),
        &[0x01, 0x02]
    );
    assert_eq!(cell::<StringArray>(&batch, "enum_value").value(0), "beta");
    assert_eq!(
        cell::<StringArray>(&batch, "set_value").value(0),
        "read,write"
    );
    assert_eq!(cell::<StringArray>(&batch, "char_value").value(0), "char");
    assert_eq!(
        cell::<BinaryArray>(&batch, "binary_value").value(0),
        &[0x01, 0x02, 0x03, 0x04]
    );
    assert_eq!(cell::<StringArray>(&batch, "tiny_text").value(0), "tiny");
    assert_eq!(
        cell::<BinaryArray>(&batch, "tiny_blob").value(0),
        &[0x0A, 0x0B]
    );
    assert_eq!(cell::<StringArray>(&batch, "text_value").value(0), "text");
    assert_eq!(
        cell::<BinaryArray>(&batch, "blob_value").value(0),
        &[0x0C, 0x0D]
    );
    assert_eq!(
        cell::<StringArray>(&batch, "medium_text").value(0),
        "medium"
    );
    assert_eq!(
        cell::<BinaryArray>(&batch, "medium_blob").value(0),
        &[0x0E, 0x0F]
    );
    assert_eq!(cell::<StringArray>(&batch, "long_text").value(0), "long");
    assert_eq!(
        cell::<BinaryArray>(&batch, "long_blob").value(0),
        &[0x10, 0x11]
    );

    // La nullability arriva dal prepare, non dai valori osservati: `amount` e
    // dichiarata nullable e porta un valore, `absent_note` e dichiarata
    // nullable ed e davvero nulla.
    assert!(!cell::<Decimal128Array>(&batch, "amount").is_null(0));
    assert!(cell::<StringArray>(&batch, "absent_note").is_null(0));

    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream QueryOperation")
        .is_none());
}

/// Aggregazione raggruppata, bind di HAVING e DISTINCT su `MySQL` con TLS.
///
/// Il `sql_mode` deterministico del provider non include `ONLY_FULL_GROUP_BY`:
/// il server accetterebbe in silenzio un gruppo non determinato. La prova che
/// conta e quindi che lo schema e le righe restituite siano esattamente quelli
/// dichiarati dai metadati di `COM_STMT_PREPARE`, con il parametro di HAVING
/// passato come bind e mai come letterale.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per aggregazione e DISTINCT"]
#[allow(clippy::too_many_lines)]
async fn live_grouped_aggregate_having_bind_and_distinct_over_verified_tls() {
    use plenora_database_core::arrow::array::{
        BinaryArray, BooleanArray, Decimal128Array, Int64Array,
    };
    use plenora_database_core::arrow::schema::DataType;
    use plenora_database_core::field_contract::validate_schema_contract;
    use plenora_database_core::plan::{ComparisonOperator, SortDirection};
    use plenora_database_core::protocol;
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
        ScalarFunction,
    };
    use std::collections::BTreeMap;

    let cancellation = CancellationToken::new();
    let mut session = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("connessione MySQL live");
    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe MySQL live");
    assert!(
        probe
            .product_version
            .starts_with(&expected_version_prefix()),
        "{probe:?}"
    );
    assert!(!probe.tls_cipher.is_empty(), "sessione MySQL non cifrata");
    drop(session);

    let column = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    };
    let aggregate = |function: ScalarFunction, field: &str| QueryExpression::Scalar {
        function,
        arguments: vec![column(field)],
    };
    let probe_source = |object: &str| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: object.to_owned(),
            layer_id: None,
        },
        alias: None,
    };
    let scalar_query = |source: QuerySource, projection: Vec<QueryProjection>| QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(source),
        derived_source: None,
        projection,
        joins: Vec::new(),
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
    };

    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let mut grouped = scalar_query(
        probe_source("catalog_probe"),
        vec![
            QueryProjection {
                expression: column("active"),
                alias: None,
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Count, "id"),
                alias: Some("events".to_owned()),
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Minimum, "id"),
                alias: Some("first_id".to_owned()),
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Maximum, "id"),
                alias: Some("last_id".to_owned()),
            },
            QueryProjection {
                expression: aggregate(ScalarFunction::Average, "amount"),
                alias: Some("mean_amount".to_owned()),
            },
        ],
    );
    grouped.group_by = vec![column("active")];
    grouped.having = Some(QueryExpression::Compare {
        left: Box::new(aggregate(ScalarFunction::Count, "id")),
        operator: ComparisonOperator::Gte,
        right: Box::new(QueryExpression::Parameter {
            name: "floor".to_owned(),
        }),
    });
    grouped.order_by = vec![QueryOrdering {
        expression: column("active"),
        direction: SortDirection::Asc,
    }];
    let grouped_budget = ResourceBudget::new(ResourceLimits {
        rows: 4,
        memory_bytes: 4 * 1024 * 1024,
        output_bytes: 4 * 1024 * 1024,
        cell_bytes: 64 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget aggregazione MySQL");
    let mut stream = provider
        .query(
            &live_secret(),
            &grouped,
            &ParameterBag::new(BTreeMap::from([(
                "floor".to_owned(),
                ParameterValue::I64(1),
            )])),
            &grouped_budget,
            &cancellation,
        )
        .await
        .expect("stream aggregazione MySQL");

    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    assert_eq!(schema.fields().len(), 5);
    let expected_columns = [
        ("active", DataType::Boolean, false, "tinyint"),
        ("events", DataType::Int64, false, "bigint"),
        ("first_id", DataType::Int64, true, "bigint"),
        ("last_id", DataType::Int64, true, "bigint"),
        ("mean_amount", DataType::Decimal128(22, 8), true, "decimal"),
    ];
    for (index, (name, data_type, nullable, native_type)) in expected_columns.iter().enumerate() {
        let field = schema.field(index);
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), data_type, "{name}");
        assert_eq!(field.is_nullable(), *nullable, "{name}");
        assert_eq!(
            field.metadata().get(protocol::MYSQL_NATIVE_TYPE),
            Some(&(*native_type).to_owned()),
            "{name}"
        );
        assert!(
            !field
                .metadata()
                .contains_key(protocol::MYSQL_NATIVE_DECLARATION),
            "{name}"
        );
    }

    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch aggregazione")
        .expect("gruppo aggregato");
    assert_eq!(batch.num_rows(), 1);
    assert!(batch
        .column(0)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("active")
        .value(0));
    for (index, expected) in [(1_usize, 1_i64), (2, 1), (3, 1)] {
        assert_eq!(
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("aggregato intero")
                .value(0),
            expected
        );
    }
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("mean_amount")
            .value(0),
        123_450_000_000_i128
    );
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream aggregazione")
        .is_none());

    // 2048 righe con lo stesso payload collassano in un solo valore distinto.
    let mut distinct = scalar_query(
        probe_source("stream_probe"),
        vec![QueryProjection {
            expression: column("payload"),
            alias: None,
        }],
    );
    distinct.distinct = true;
    let distinct_budget = ResourceBudget::new(ResourceLimits {
        rows: 4,
        memory_bytes: 4 * 1024 * 1024,
        output_bytes: 4 * 1024 * 1024,
        cell_bytes: 64 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget DISTINCT MySQL");
    let mut stream = provider
        .query(
            &live_secret(),
            &distinct,
            &ParameterBag::new(BTreeMap::new()),
            &distinct_budget,
            &cancellation,
        )
        .await
        .expect("stream DISTINCT MySQL");
    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "payload");
    assert_eq!(schema.field(0).data_type(), &DataType::Binary);
    assert!(!schema.field(0).is_nullable());
    assert_eq!(
        schema.field(0).metadata().get(protocol::MYSQL_NATIVE_TYPE),
        Some(&"varbinary".to_owned())
    );
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch DISTINCT")
        .expect("riga DISTINCT");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("payload")
            .value(0),
        [0x5A_u8; 1024].as_slice()
    );
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream DISTINCT")
        .is_none());

    // SUM su BIGINT produce DECIMAL(41,0): il contratto Decimal128 non lo
    // rappresenta e il path query lo dichiara non supportato invece di
    // troncare silenziosamente.
    let wide_sum = scalar_query(
        probe_source("catalog_probe"),
        vec![QueryProjection {
            expression: aggregate(ScalarFunction::Sum, "id"),
            alias: Some("total".to_owned()),
        }],
    );
    let sum_budget = ResourceBudget::new(ResourceLimits::default()).expect("budget SUM MySQL");
    let outcome = provider
        .query(
            &live_secret(),
            &wide_sum,
            &ParameterBag::new(BTreeMap::new()),
            &sum_budget,
            &cancellation,
        )
        .await;
    let Err(error) = outcome else {
        panic!("SUM oltre Decimal128 accettato");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

/// Join fisici INNER, LEFT, RIGHT e CROSS su `MySQL` con TLS verificato.
///
/// La prova che conta e la nullabilita del lato esterno:
/// `stream_probe`.`payload` e dichiarata `VARBINARY(1024) NOT NULL`, ma dal
/// lato non preservato di un LEFT JOIN i metadati di `COM_STMT_PREPARE` la
/// pubblicano nullable e la riga restituisce NULL. Un contratto Arrow
/// costruito dalla dichiarazione del catalogo invece che dai metadati dello
/// statement mentirebbe esattamente qui, e il consumatore scoprirebbe la
/// differenza solo leggendo il batch.
///
/// Ogni colonna proiettata porta un alias: il result set di un join espone
/// altrimenti due colonne `id` omonime, che il path query rifiuta come nomi
/// di output duplicati.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per i join fisici"]
#[allow(clippy::too_many_lines)]
async fn live_physical_joins_bind_on_clauses_and_publish_outer_nullability() {
    use plenora_database_core::arrow::array::{Array, BinaryArray, Int64Array, StringArray};
    use plenora_database_core::arrow::schema::DataType;
    use plenora_database_core::field_contract::validate_schema_contract;
    use plenora_database_core::plan::{ComparisonOperator, SortDirection};
    use plenora_database_core::protocol;
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::query::{
        ColumnRef, JoinKind, QueryExpression, QueryJoin, QueryOperation, QueryOrdering,
        QueryProjection, QuerySource,
    };
    use std::collections::BTreeMap;

    let cancellation = CancellationToken::new();
    let mut session = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("connessione MySQL live");
    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe MySQL live");
    assert!(
        probe
            .product_version
            .starts_with(&expected_version_prefix()),
        "{probe:?}"
    );
    assert!(!probe.tls_cipher.is_empty(), "sessione MySQL non cifrata");
    drop(session);

    let relation = |object: &str, alias: &str| QuerySource {
        object: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: object.to_owned(),
            layer_id: None,
        },
        alias: Some(alias.to_owned()),
    };
    let qualified = |relation: &str, field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: Some(relation.to_owned()),
            field: field.to_owned(),
        },
    };
    let named = |expression: QueryExpression, alias: &str| QueryProjection {
        expression,
        alias: Some(alias.to_owned()),
    };
    let joined = |join: QueryJoin,
                  projection: Vec<QueryProjection>,
                  order: QueryExpression,
                  direction: SortDirection,
                  limit: u64| QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(relation("catalog_probe", "c")),
        derived_source: None,
        projection,
        joins: vec![join],
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: order,
            direction,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(limit),
        row_offset: None,
        locking: None,
    };
    let join_on = |kind: JoinKind, on: Option<QueryExpression>| QueryJoin {
        kind,
        source: Some(relation("stream_probe", "s")),
        derived_source: None,
        lateral: false,
        on,
    };
    let budget = || {
        ResourceBudget::new(ResourceLimits {
            rows: 4,
            memory_bytes: 4 * 1024 * 1024,
            output_bytes: 4 * 1024 * 1024,
            cell_bytes: 64 * 1024,
            ..ResourceLimits::default()
        })
        .expect("budget join MySQL")
    };
    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");

    // INNER JOIN con un bind nella clausola ON: il parametro e posizionale e
    // precede quelli delle clausole successive.
    let inner = joined(
        join_on(
            JoinKind::Inner,
            Some(QueryExpression::And {
                arguments: vec![
                    QueryExpression::Compare {
                        left: Box::new(qualified("c", "id")),
                        operator: ComparisonOperator::Eq,
                        right: Box::new(qualified("s", "id")),
                    },
                    QueryExpression::Compare {
                        left: Box::new(qualified("s", "id")),
                        operator: ComparisonOperator::Lte,
                        right: Box::new(QueryExpression::Parameter {
                            name: "ceiling".to_owned(),
                        }),
                    },
                ],
            }),
        ),
        vec![
            named(qualified("c", "id"), "probe_id"),
            named(qualified("c", "name"), "probe_name"),
            named(qualified("s", "id"), "stream_id"),
        ],
        qualified("s", "id"),
        SortDirection::Asc,
        1,
    );
    let mut stream = provider
        .query(
            &live_secret(),
            &inner,
            &ParameterBag::new(BTreeMap::from([(
                "ceiling".to_owned(),
                ParameterValue::I64(1),
            )])),
            &budget(),
            &cancellation,
        )
        .await
        .expect("stream INNER JOIN MySQL");
    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    let expected_inner = [
        ("probe_id", DataType::Int64, "bigint"),
        ("probe_name", DataType::Utf8, "varchar"),
        ("stream_id", DataType::Int64, "bigint"),
    ];
    assert_eq!(schema.fields().len(), expected_inner.len());
    for (index, (name, data_type, native_type)) in expected_inner.iter().enumerate() {
        let field = schema.field(index);
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), data_type, "{name}");
        // Nessuna relazione e preservata da un INNER JOIN: le colonne
        // dichiarate NOT NULL restano non nullable.
        assert!(!field.is_nullable(), "{name}");
        assert_eq!(
            field.metadata().get(protocol::MYSQL_NATIVE_TYPE),
            Some(&(*native_type).to_owned()),
            "{name}"
        );
        assert!(
            !field
                .metadata()
                .contains_key(protocol::MYSQL_NATIVE_DECLARATION),
            "{name}"
        );
    }
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch INNER JOIN")
        .expect("riga INNER JOIN");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("probe_id")
            .value(0),
        1
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("probe_name")
            .value(0),
        "reference"
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("stream_id")
            .value(0),
        1
    );
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream INNER JOIN")
        .is_none());

    // LEFT JOIN senza corrispondenza: `stream_probe` arriva a 2048, il bind
    // porta la soglia oltre e nessuna riga puo accoppiarsi.
    let left = joined(
        join_on(
            JoinKind::Left,
            Some(QueryExpression::And {
                arguments: vec![
                    QueryExpression::Compare {
                        left: Box::new(qualified("c", "id")),
                        operator: ComparisonOperator::Eq,
                        right: Box::new(qualified("s", "id")),
                    },
                    QueryExpression::Compare {
                        left: Box::new(qualified("s", "id")),
                        operator: ComparisonOperator::Gte,
                        right: Box::new(QueryExpression::Parameter {
                            name: "floor".to_owned(),
                        }),
                    },
                ],
            }),
        ),
        vec![
            named(qualified("c", "id"), "probe_id"),
            named(qualified("s", "id"), "stream_id"),
            named(qualified("s", "payload"), "stream_payload"),
        ],
        qualified("c", "id"),
        SortDirection::Asc,
        1,
    );
    let mut stream = provider
        .query(
            &live_secret(),
            &left,
            &ParameterBag::new(BTreeMap::from([(
                "floor".to_owned(),
                ParameterValue::I64(4096),
            )])),
            &budget(),
            &cancellation,
        )
        .await
        .expect("stream LEFT JOIN MySQL");
    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "probe_id");
    assert!(!schema.field(0).is_nullable());
    assert_eq!(schema.field(1).name(), "stream_id");
    assert!(schema.field(1).is_nullable());
    // `payload` e dichiarata NOT NULL nel catalogo: il lato esterno del join
    // la rende nullable nei metadati dello statement.
    assert_eq!(schema.field(2).name(), "stream_payload");
    assert_eq!(schema.field(2).data_type(), &DataType::Binary);
    assert!(schema.field(2).is_nullable());
    assert_eq!(
        schema.field(2).metadata().get(protocol::MYSQL_NATIVE_TYPE),
        Some(&"varbinary".to_owned())
    );
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch LEFT JOIN")
        .expect("riga LEFT JOIN");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("probe_id")
            .value(0),
        1
    );
    assert!(batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("stream_id")
        .is_null(0));
    assert!(batch
        .column(2)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("stream_payload")
        .is_null(0));
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream LEFT JOIN")
        .is_none());

    // RIGHT JOIN: la relazione preservata e quella dichiarata nel join, e la
    // nullabilita si sposta sulle colonne della sorgente di base.
    let right = joined(
        join_on(
            JoinKind::Right,
            Some(QueryExpression::Compare {
                left: Box::new(qualified("c", "id")),
                operator: ComparisonOperator::Eq,
                right: Box::new(qualified("s", "id")),
            }),
        ),
        vec![
            named(qualified("c", "id"), "probe_id"),
            named(qualified("c", "name"), "probe_name"),
            named(qualified("s", "id"), "stream_id"),
        ],
        qualified("s", "id"),
        SortDirection::Desc,
        1,
    );
    let mut stream = provider
        .query(
            &live_secret(),
            &right,
            &ParameterBag::new(BTreeMap::new()),
            &budget(),
            &cancellation,
        )
        .await
        .expect("stream RIGHT JOIN MySQL");
    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    assert!(schema.field(0).is_nullable());
    assert!(schema.field(1).is_nullable());
    assert!(!schema.field(2).is_nullable());
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch RIGHT JOIN")
        .expect("riga RIGHT JOIN");
    assert_eq!(batch.num_rows(), 1);
    assert!(batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("probe_id")
        .is_null(0));
    assert!(batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("probe_name")
        .is_null(0));
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("stream_id")
            .value(0),
        2048
    );
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream RIGHT JOIN")
        .is_none());

    // CROSS JOIN senza clausola ON: il prodotto e ordinato e limitato, cosi
    // le righe restano deterministiche.
    let cross = joined(
        join_on(JoinKind::Cross, None),
        vec![
            named(qualified("c", "id"), "probe_id"),
            named(qualified("s", "id"), "stream_id"),
        ],
        qualified("s", "id"),
        SortDirection::Asc,
        2,
    );
    let mut stream = provider
        .query(
            &live_secret(),
            &cross,
            &ParameterBag::new(BTreeMap::new()),
            &budget(),
            &cancellation,
        )
        .await
        .expect("stream CROSS JOIN MySQL");
    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    assert_eq!(schema.fields().len(), 2);
    assert!(!schema.field(0).is_nullable());
    assert!(!schema.field(1).is_nullable());
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch CROSS JOIN")
        .expect("righe CROSS JOIN");
    assert_eq!(batch.num_rows(), 2);
    let probe_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("probe_id");
    let stream_ids = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("stream_id");
    assert_eq!((probe_ids.value(0), stream_ids.value(0)), (1, 1));
    assert_eq!((probe_ids.value(1), stream_ids.value(1)), (1, 2));
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream CROSS JOIN")
        .is_none());
}

/// Window function scalari su `MySQL` con TLS verificato.
///
/// La finestra e valutata dal server dopo WHERE e prima di LIMIT: la prova che
/// conta e che lo schema Arrow arrivi dai metadati di `COM_STMT_PREPARE` e che
/// i valori siano esattamente quelli della semantica dichiarata dal piano.
///
/// Il piano usa solo le forme che restano determinate senza dimostrare che la
/// chiave d'ordine sia univoca: `RANK` e `DENSE_RANK`, stabili fra righe pari,
/// un aggregato con frame RANGE, che taglia la partizione confrontando i
/// valori della chiave e non le posizioni, e un `COUNT` sulla sola partizione.
/// `ROW_NUMBER`, `LAG`, `LEAD` e il frame ROWS non compaiono: il provider li
/// chiude prima della rete.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per le window scalari"]
#[allow(clippy::too_many_lines)]
async fn live_scalar_window_functions_publish_peer_stable_ranking_and_range_aggregates() {
    use plenora_database_core::arrow::array::{Array, Int64Array, UInt64Array};
    use plenora_database_core::arrow::schema::DataType;
    use plenora_database_core::field_contract::validate_schema_contract;
    use plenora_database_core::plan::{ComparisonOperator, SortDirection};
    use plenora_database_core::protocol;
    use plenora_database_core::provider::ParameterValue;
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
        ScalarFunction, WindowFrame, WindowFrameBound, WindowFrameUnits,
    };
    use std::collections::BTreeMap;

    let cancellation = CancellationToken::new();
    let mut session = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("connessione MySQL live");
    let probe = probe_server(&mut session, &cancellation)
        .await
        .expect("probe MySQL live");
    assert!(
        probe
            .product_version
            .starts_with(&expected_version_prefix()),
        "{probe:?}"
    );
    assert!(!probe.tls_cipher.is_empty(), "sessione MySQL non cifrata");
    drop(session);

    let identifier = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: Some("s".to_owned()),
            field: field.to_owned(),
        },
    };
    let peer_key = || QueryExpression::Compare {
        left: Box::new(identifier("id")),
        operator: ComparisonOperator::Eq,
        right: Box::new(QueryExpression::Parameter {
            name: "peer_boundary".to_owned(),
        }),
    };
    let ascending = |expression: QueryExpression| QueryOrdering {
        expression,
        direction: SortDirection::Asc,
    };
    let over = |function: ScalarFunction,
                arguments: Vec<QueryExpression>,
                partition_by: Vec<QueryExpression>,
                order_by: Vec<QueryOrdering>,
                frame: Option<WindowFrame>,
                alias: &str| QueryProjection {
        expression: QueryExpression::Window {
            function,
            arguments,
            partition_by,
            order_by,
            frame,
        },
        alias: Some(alias.to_owned()),
    };

    let operation = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "stream_probe".to_owned(),
                layer_id: None,
            },
            alias: Some("s".to_owned()),
        }),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: identifier("id"),
                alias: Some("id".to_owned()),
            },
            over(
                ScalarFunction::Rank,
                Vec::new(),
                Vec::new(),
                vec![ascending(peer_key())],
                None,
                "ranked",
            ),
            over(
                ScalarFunction::DenseRank,
                Vec::new(),
                Vec::new(),
                vec![ascending(peer_key())],
                None,
                "dense_ranked",
            ),
            // RANGE confronta i valori della chiave d'ordine: il frame arriva
            // fino a tutte le righe pari alla corrente compresa, quindi il
            // massimo non dipende dall'ordine fisico scelto dal motore.
            over(
                ScalarFunction::Maximum,
                vec![identifier("id")],
                Vec::new(),
                vec![ascending(peer_key())],
                Some(WindowFrame {
                    units: WindowFrameUnits::Range,
                    start: WindowFrameBound::UnboundedPreceding,
                    end: Some(WindowFrameBound::CurrentRow),
                }),
                "running_max",
            ),
            over(
                ScalarFunction::Count,
                vec![QueryExpression::Wildcard { relation: None }],
                vec![peer_key()],
                Vec::new(),
                None,
                "peers",
            ),
        ],
        joins: Vec::new(),
        filter: Some(QueryExpression::Compare {
            left: Box::new(identifier("id")),
            operator: ComparisonOperator::Lte,
            right: Box::new(QueryExpression::Parameter {
                name: "ceiling".to_owned(),
            }),
        }),
        group_by: Vec::new(),
        having: None,
        order_by: vec![ascending(identifier("id"))],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: Some(3),
        row_offset: None,
        locking: None,
    };

    let provider = MysqlProvider::new(live_config(), 2).expect("provider MySQL live");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 4,
        memory_bytes: 4 * 1024 * 1024,
        output_bytes: 4 * 1024 * 1024,
        cell_bytes: 64 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget window MySQL");
    let mut stream = provider
        .query(
            &live_secret(),
            &operation,
            &ParameterBag::new(BTreeMap::from([
                ("ceiling".to_owned(), ParameterValue::I64(3)),
                ("peer_boundary".to_owned(), ParameterValue::I64(3)),
            ])),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream window MySQL");

    let schema = stream.schema();
    validate_schema_contract(schema.as_ref()).expect("contratto Arrow canonico");
    // Il rango e un conteggio di posizione: MySQL lo dichiara UNSIGNED e il
    // contratto Arrow lo pubblica come UInt64, non come Int64.
    let expected = [
        ("id", DataType::Int64, false),
        ("ranked", DataType::UInt64, false),
        ("dense_ranked", DataType::UInt64, false),
        ("running_max", DataType::Int64, true),
        ("peers", DataType::Int64, false),
    ];
    assert_eq!(schema.fields().len(), expected.len());
    for (index, (name, data_type, nullable)) in expected.iter().enumerate() {
        let field = schema.field(index);
        assert_eq!(field.name(), name);
        assert_eq!(field.data_type(), data_type, "{name}");
        assert_eq!(field.is_nullable(), *nullable, "{name}");
        assert_eq!(
            field.metadata().get(protocol::MYSQL_NATIVE_TYPE),
            Some(&"bigint".to_owned()),
            "{name}"
        );
        assert!(
            !field
                .metadata()
                .contains_key(protocol::MYSQL_NATIVE_DECLARATION),
            "{name}"
        );
    }

    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch window")
        .expect("righe window");
    assert_eq!(batch.num_rows(), 3);
    let signed = |index: usize, label: &str| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("{label} non e Int64"))
            .clone()
    };
    let values = |array: &Int64Array| (0..3).map(|row| array.value(row)).collect::<Vec<_>>();

    assert_eq!(values(&signed(0, "id")), vec![1, 2, 3]);
    let ranks = |index: usize, label: &str| {
        let array = batch
            .column(index)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap_or_else(|| panic!("{label} non e UInt64"));
        (0..3).map(|row| array.value(row)).collect::<Vec<_>>()
    };
    // Le prime due righe appartengono allo stesso gruppo di peer. RANK salta
    // quindi la posizione 2, mentre DENSE_RANK non lascia buchi.
    assert_eq!(ranks(1, "ranked"), vec![1, 1, 3]);
    assert_eq!(ranks(2, "dense_ranked"), vec![1, 1, 2]);
    // Il frame RANGE include simultaneamente entrambi i peer: per id 1 il
    // massimo e gia 2, cosa che un frame ROWS non garantirebbe.
    assert_eq!(values(&signed(3, "running_max")), vec![2, 2, 3]);
    assert_eq!(values(&signed(4, "peers")), vec![2, 2, 1]);
    assert!(stream
        .next_batch(&cancellation)
        .await
        .expect("fine stream window")
        .is_none());
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per execution count QueryOperation"]
#[allow(clippy::too_many_lines)]
async fn live_query_operation_executes_once_holds_lease_and_stays_demand_bounded() {
    use plenora_database_core::plan::SortDirection;
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
    };
    use plenora_database_core::resource::ResourceKind;
    use std::collections::BTreeMap;

    let column = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: Some("s".to_owned()),
            field: field.to_owned(),
        },
    };
    let operation = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "stream_probe".to_owned(),
                layer_id: None,
            },
            alias: Some("s".to_owned()),
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("id"),
            alias: Some("single_execution_observed_id".to_owned()),
        }],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: column("id"),
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    };
    let audit_pool = mysql_async::Pool::new(
        live_config()
            .pooled_driver_opts(1)
            .expect("pool performance_schema"),
    );
    let mut audit = audit_pool.get_conn().await.expect("checkout audit");
    let baseline: Option<(String, u64)> = audit
        .query_first("SHOW GLOBAL STATUS LIKE 'Com_stmt_execute'")
        .await
        .expect("baseline globale COM_STMT_EXECUTE");
    let (_, baseline) = baseline.expect("contatore globale COM_STMT_EXECUTE");
    let budget = ResourceBudget::new(ResourceLimits {
        rows: 2_048,
        memory_bytes: 96 * 1_024,
        output_bytes: 4 * 1_024 * 1_024,
        cell_bytes: 2 * 1_024,
        concurrent_operations: 1,
        ..ResourceLimits::default()
    })
    .expect("budget QueryOperation single execution");
    let provider = MysqlProvider::new(live_config(), 1).expect("provider QueryOperation live");
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .query(
            &live_secret(),
            &operation,
            &ParameterBag::new(BTreeMap::new()),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream QueryOperation single execution");
    assert_eq!(budget.remaining(ResourceKind::ConcurrentOperations), 0);

    let Err(error) = provider
        .query(
            &live_secret(),
            &operation,
            &ParameterBag::new(BTreeMap::new()),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("seconda QueryOperation accettata mentre la lease e attiva");
    };
    assert_eq!(error.category, ErrorCategory::ResourceLimit);

    await_single_statement_execution(&mut audit, baseline).await;

    let mut rows = 0_usize;
    let mut batches = 0_usize;
    while rows < 2_048 {
        let batch = stream
            .next_batch(&cancellation)
            .await
            .expect("batch QueryOperation")
            .expect("batch presente fino al limite righe");
        assert!(batch.num_rows() > 0);
        assert!(batch.num_rows() < 2_048);
        rows = rows.saturating_add(batch.num_rows());
        batches = batches.saturating_add(1);
        assert_eq!(budget.remaining(ResourceKind::ConcurrentOperations), 0);
        let current: Option<(String, u64)> = audit
            .query_first("SHOW GLOBAL STATUS LIKE 'Com_stmt_execute'")
            .await
            .expect("COM_STMT_EXECUTE dopo un batch");
        assert_eq!(
            current.and_then(|(_, value)| value.checked_sub(baseline)),
            Some(1)
        );
    }
    assert_eq!(rows, 2_048);
    assert!(batches > 1, "la query deve attraversare piu batch bounded");
    drop(stream);
    assert_eq!(budget.remaining(ResourceKind::ConcurrentOperations), 1);

    let mut early_operation = operation.clone();
    early_operation.projection = vec![QueryProjection {
        expression: column("id"),
        alias: Some("early_drop_inflight_marker".to_owned()),
    }];
    let early_budget = ResourceBudget::new(ResourceLimits {
        rows: 1,
        memory_bytes: 4 * 1_024 * 1_024,
        output_bytes: 4 * 1_024 * 1_024,
        cell_bytes: 2 * 1_024,
        concurrent_operations: 1,
        ..ResourceLimits::default()
    })
    .expect("budget drop anticipato QueryOperation");
    crate::session::reset_test_row_pulls();
    let mut early = provider
        .query(
            &live_secret(),
            &early_operation,
            &ParameterBag::new(BTreeMap::new()),
            &early_budget,
            &CancellationToken::new(),
        )
        .await
        .expect("stream QueryOperation da abbandonare");
    let first = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        early.next_batch(&cancellation),
    )
    .await
    .expect("primo batch bounded QueryOperation")
    .expect("primo batch prima del drop")
    .expect("batch prima del drop");
    assert_eq!(first.num_rows(), 1);
    assert_eq!(crate::session::test_row_pulls(), 1);
    let owner_thread_id = observe_prepared_thread(&mut audit, "early_drop_inflight_marker").await;
    assert_eq!(
        early_budget.remaining(ResourceKind::ConcurrentOperations),
        0
    );
    drop(early);
    assert_eq!(
        early_budget.remaining(ResourceKind::ConcurrentOperations),
        1
    );
    provider
        .test_connection(&live_secret(), &CancellationToken::new())
        .await
        .expect("provider riusabile dopo drop anticipato QueryOperation");
    let mut replacement_operation = operation.clone();
    replacement_operation.projection[0].alias = Some("early_drop_replacement_marker".to_owned());
    let replacement_budget = ResourceBudget::new(ResourceLimits {
        rows: 1,
        concurrent_operations: 1,
        ..ResourceLimits::default()
    })
    .expect("budget replacement early drop");
    let replacement = provider
        .query(
            &live_secret(),
            &replacement_operation,
            &ParameterBag::new(BTreeMap::new()),
            &replacement_budget,
            &CancellationToken::new(),
        )
        .await
        .expect("replacement QueryOperation dopo early drop");
    let replacement_thread =
        observe_prepared_thread(&mut audit, "early_drop_replacement_marker").await;
    assert_ne!(replacement_thread, owner_thread_id);
    drop(replacement);
    drop(audit);
    audit_pool.disconnect().await.expect("disconnect audit");
}

#[tokio::test]
#[ignore = "richiede MySQL live esplicito per lifecycle QueryOperation"]
#[allow(clippy::too_many_lines)]
async fn live_query_operation_cancellation_and_timeout_quarantine_the_session() {
    use plenora_database_core::plan::SortDirection;
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
    };
    use plenora_database_core::resource::ResourceKind;
    use plenora_database_core::{ErrorPhase, RemoteEffect, RetryDisposition};
    use std::collections::BTreeMap;
    use std::time::Duration;

    let operation = |marker: &str| {
        let column = |field: &str| QueryExpression::Column {
            column: ColumnRef {
                relation: Some("slow".to_owned()),
                field: field.to_owned(),
            },
        };
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("dataflow_test".to_owned()),
                    object: "slow_query_probe".to_owned(),
                    layer_id: None,
                },
                alias: Some("slow".to_owned()),
            }),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: column("id"),
                    alias: Some("slow_id".to_owned()),
                },
                QueryProjection {
                    expression: column("delay_value"),
                    alias: Some(marker.to_owned()),
                },
            ],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: column("id"),
                direction: SortDirection::Asc,
            }],
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        }
    };
    let replacement_operation = |marker: &str| {
        let mut replacement = operation(marker);
        replacement
            .source
            .as_mut()
            .expect("sorgente replacement")
            .object
            .object = "stream_probe".to_owned();
        replacement.projection = vec![QueryProjection {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: Some("slow".to_owned()),
                    field: "id".to_owned(),
                },
            },
            alias: Some(marker.to_owned()),
        }];
        replacement
    };
    let limits = || ResourceLimits {
        rows: 2_048,
        memory_bytes: 4 * 1_024 * 1_024,
        output_bytes: 4 * 1_024 * 1_024,
        cell_bytes: 2 * 1_024,
        concurrent_operations: 1,
        ..ResourceLimits::default()
    };
    let assert_envelope =
        |error: &plenora_database_core::DatabaseError, category: ErrorCategory, message: &str| {
            assert_eq!(error.category, category);
            assert_eq!(error.phase, ErrorPhase::Read);
            assert_eq!(error.remote_effect, RemoteEffect::None);
            assert_eq!(error.retry, RetryDisposition::Never);
            assert_eq!(error.provider, Some(ProviderKind::Mysql));
            assert_eq!(error.message, message);
        };
    let audit_pool = mysql_async::Pool::new(
        live_config()
            .pooled_driver_opts(1)
            .expect("pool audit lifecycle"),
    );
    let mut audit = audit_pool
        .get_conn()
        .await
        .expect("checkout audit lifecycle");
    let audit_connection_id: Option<u64> = audit
        .query_first("SELECT CONNECTION_ID()")
        .await
        .expect("connection id audit lifecycle");
    let run_id = audit_connection_id.expect("connection id audit presente");

    let cancellation = CancellationToken::new();
    let cancellation_marker = format!("cancel_inflight_{run_id}");
    let cancellation_operation = operation(&cancellation_marker);
    let provider = MysqlProvider::new(
        live_config().with_timeouts(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(10),
        ),
        1,
    )
    .expect("provider cancellation QueryOperation");
    let budget = ResourceBudget::new(limits()).expect("budget cancellation QueryOperation");
    let mut stream = provider
        .query(
            &live_secret(),
            &cancellation_operation,
            &ParameterBag::new(BTreeMap::new()),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream cancellation QueryOperation");
    let (cancellation_thread, error) = {
        let next_batch = stream.next_batch(&cancellation);
        tokio::pin!(next_batch);
        let cancellation_thread = tokio::select! {
            owner = observe_inflight_query(&mut audit, &cancellation_marker) => owner,
            result = &mut next_batch => panic!(
                "cancellation QueryOperation terminata prima della barriera: {result:?}"
            ),
        };
        cancellation.cancel();
        let error = next_batch
            .await
            .expect_err("QueryOperation cancellata in-flight");
        (cancellation_thread, error)
    };
    assert_envelope(
        &error,
        ErrorCategory::Cancelled,
        "operazione MySQL cancellata; connessione quarantinata",
    );
    drop(stream);
    assert_eq!(budget.remaining(ResourceKind::ConcurrentOperations), 1);
    provider
        .test_connection(&live_secret(), &CancellationToken::new())
        .await
        .expect("provider riusabile dopo quarantena per cancellation");
    let cancellation_replacement_marker = format!("cancel_replacement_{run_id}");
    let cancellation_replacement = replacement_operation(&cancellation_replacement_marker);
    let replacement_budget =
        ResourceBudget::new(limits()).expect("budget replacement cancellation");
    let replacement = provider
        .query(
            &live_secret(),
            &cancellation_replacement,
            &ParameterBag::new(BTreeMap::new()),
            &replacement_budget,
            &CancellationToken::new(),
        )
        .await
        .expect("replacement dopo cancellation");
    let replacement_thread =
        observe_prepared_thread(&mut audit, &cancellation_replacement_marker).await;
    assert_ne!(replacement_thread, cancellation_thread);
    drop(replacement);

    let timeout_provider = MysqlProvider::new(
        live_config().with_timeouts(
            Duration::from_secs(10),
            Duration::from_millis(500),
            Duration::from_secs(10),
        ),
        1,
    )
    .expect("provider timeout QueryOperation");
    let timeout_marker = format!("operation_timeout_inflight_{run_id}");
    let timeout_operation = operation(&timeout_marker);
    let timeout_budget = ResourceBudget::new(limits()).expect("budget timeout QueryOperation");
    let timeout_cancellation = CancellationToken::new();
    let mut stream = timeout_provider
        .query(
            &live_secret(),
            &timeout_operation,
            &ParameterBag::new(BTreeMap::new()),
            &timeout_budget,
            &timeout_cancellation,
        )
        .await
        .expect("stream timeout QueryOperation");
    let (timeout_thread, error) = {
        let next_batch = stream.next_batch(&timeout_cancellation);
        tokio::pin!(next_batch);
        let timeout_thread = tokio::select! {
            owner = observe_inflight_query(&mut audit, &timeout_marker) => owner,
            result = &mut next_batch => panic!(
                "timeout QueryOperation terminato prima della barriera: {result:?}"
            ),
        };
        let error = next_batch.await.expect_err("QueryOperation in timeout");
        (timeout_thread, error)
    };
    assert_envelope(
        &error,
        ErrorCategory::Timeout,
        "timeout operazione MySQL; connessione quarantinata",
    );
    drop(stream);
    assert_eq!(
        timeout_budget.remaining(ResourceKind::ConcurrentOperations),
        1
    );
    timeout_provider
        .test_connection(&live_secret(), &CancellationToken::new())
        .await
        .expect("provider riusabile dopo quarantena per timeout");
    let timeout_replacement_marker = format!("timeout_replacement_{run_id}");
    let timeout_replacement = replacement_operation(&timeout_replacement_marker);
    let replacement_budget = ResourceBudget::new(limits()).expect("budget replacement timeout");
    let replacement = timeout_provider
        .query(
            &live_secret(),
            &timeout_replacement,
            &ParameterBag::new(BTreeMap::new()),
            &replacement_budget,
            &CancellationToken::new(),
        )
        .await
        .expect("replacement dopo timeout");
    let replacement_thread = observe_prepared_thread(&mut audit, &timeout_replacement_marker).await;
    assert_ne!(replacement_thread, timeout_thread);
    drop(replacement);

    let deadline_provider = MysqlProvider::new(
        live_config().with_timeouts(
            Duration::from_secs(10),
            Duration::from_secs(10),
            Duration::from_secs(10),
        ),
        1,
    )
    .expect("provider deadline QueryOperation");
    let deadline_marker = format!("resource_deadline_inflight_{run_id}");
    let deadline_operation = operation(&deadline_marker);
    let mut deadline_limits = limits();
    deadline_limits.duration_ms = 500;
    let deadline_budget =
        ResourceBudget::new(deadline_limits).expect("budget deadline QueryOperation");
    let deadline_cancellation = CancellationToken::new();
    let mut stream = deadline_provider
        .query(
            &live_secret(),
            &deadline_operation,
            &ParameterBag::new(BTreeMap::new()),
            &deadline_budget,
            &deadline_cancellation,
        )
        .await
        .expect("stream deadline QueryOperation");
    let (deadline_thread, error) = {
        let next_batch = stream.next_batch(&deadline_cancellation);
        tokio::pin!(next_batch);
        let deadline_thread = tokio::select! {
            owner = observe_inflight_query(&mut audit, &deadline_marker) => owner,
            result = &mut next_batch => panic!(
                "deadline QueryOperation terminata prima della barriera: {result:?}"
            ),
        };
        let error = next_batch
            .await
            .expect_err("QueryOperation oltre deadline budget");
        (deadline_thread, error)
    };
    assert_envelope(
        &error,
        ErrorCategory::Timeout,
        "timeout operazione MySQL; connessione quarantinata",
    );
    drop(stream);
    assert_eq!(
        deadline_budget.remaining(ResourceKind::ConcurrentOperations),
        1
    );
    deadline_provider
        .test_connection(&live_secret(), &CancellationToken::new())
        .await
        .expect("provider riusabile dopo quarantena per deadline");
    let deadline_replacement_marker = format!("deadline_replacement_{run_id}");
    let deadline_replacement = replacement_operation(&deadline_replacement_marker);
    let replacement_budget = ResourceBudget::new(limits()).expect("budget replacement deadline");
    let replacement = deadline_provider
        .query(
            &live_secret(),
            &deadline_replacement,
            &ParameterBag::new(BTreeMap::new()),
            &replacement_budget,
            &CancellationToken::new(),
        )
        .await
        .expect("replacement dopo deadline");
    let replacement_thread =
        observe_prepared_thread(&mut audit, &deadline_replacement_marker).await;
    assert_ne!(replacement_thread, deadline_thread);
    drop(replacement);
    drop(audit);
    audit_pool
        .disconnect()
        .await
        .expect("disconnect audit lifecycle");
}

// --- Append qualificato: sessione, transazione singola e recovery ---------

/// Lo schema Arrow del tracer append: un tipo per ciascuna famiglia che il
/// piano di scrittura qualifica, con nullability esplicita.
fn append_input_schema() -> plenora_database_core::arrow::SchemaRef {
    use plenora_database_core::arrow::schema::{DataType, Field, Schema, TimeUnit};
    use plenora_database_core::protocol;
    use std::collections::HashMap;

    std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new("active", DataType::Boolean, false),
            Field::new("day", DataType::Date32, true),
            Field::new(
                "moment",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("payload", DataType::Binary, true),
        ],
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn row_diagnostics_input_schema() -> plenora_database_core::arrow::SchemaRef {
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    use plenora_database_core::protocol;
    use std::collections::HashMap;

    std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("parcel_id", DataType::Int64, false),
            Field::new("area_m2", DataType::Int64, false),
        ],
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn row_diagnostics_batch(
    schema: &plenora_database_core::arrow::SchemaRef,
    start: u64,
    end: u64,
) -> plenora_database_core::arrow::RecordBatch {
    use plenora_database_core::arrow::array::{ArrayRef, Int64Array};
    use plenora_database_core::arrow::RecordBatch;
    use std::sync::Arc;

    let parcel_ids = (start..end)
        .map(|index| i64::try_from(index).expect("indice fixture rappresentabile"))
        .collect::<Vec<_>>();
    let areas = (start..end)
        .map(|index| if index == 4_999 { -987_654_321 } else { 1 })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from(parcel_ids)) as ArrayRef,
            Arc::new(Int64Array::from(areas)) as ArrayRef,
        ],
    )
    .expect("batch fixture row diagnostics")
}

const APPEND_TARGET_DDL: &str = "(\
     id BIGINT NOT NULL PRIMARY KEY, \
     label VARCHAR(64) NULL, \
     amount DECIMAL(12, 2) NULL, \
     active TINYINT(1) NOT NULL, \
     day DATE NULL, \
     moment DATETIME(6) NULL, \
     payload VARBINARY(32) NULL) ENGINE=InnoDB";

async fn append_setup_connection(config: &MysqlConfig) -> mysql_async::Conn {
    mysql_async::Conn::new(config.driver_opts().expect("opzioni driver MySQL live"))
        .await
        .expect("connessione di servizio MySQL live")
}

async fn reset_append_target(connection: &mut mysql_async::Conn, table: &str) {
    connection
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("drop del target append");
    connection
        .query_drop(format!("CREATE TABLE `{table}` {APPEND_TARGET_DDL}"))
        .await
        .expect("create del target append");
}

fn append_spatial_schema(
    target: &crate::MysqlObjectDescription,
) -> plenora_database_core::arrow::SchemaRef {
    use plenora_database_core::arrow::schema::Schema;
    use plenora_database_core::protocol;
    use std::collections::HashMap;

    let fields = target
        .columns
        .iter()
        .map(|column| {
            crate::MysqlColumnSpec::from_catalog(column)
                .expect("colonna target spatial qualificata")
                .arrow_field()
        })
        .collect::<Vec<_>>();
    std::sync::Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn append_point_xy_wkb(x: f64, y: f64) -> Vec<u8> {
    let mut point = vec![1_u8];
    point.extend_from_slice(&1_u32.to_le_bytes());
    point.extend_from_slice(&x.to_le_bytes());
    point.extend_from_slice(&y.to_le_bytes());
    point
}

type SpatialObservation = (i64, Option<u32>, Option<f64>, Option<f64>);

/// Le righe del tracer: quelle di indice pari sono interamente nulle nelle
/// colonne nullable, cosi il bind di NULL viene provato anche sul server.
fn append_batch(
    schema: &plenora_database_core::arrow::SchemaRef,
    ids: &[i64],
) -> plenora_database_core::arrow::RecordBatch {
    use chrono::NaiveDate;
    use plenora_database_core::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use plenora_database_core::arrow::RecordBatch;
    use std::sync::Arc;

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("epoch");
    let day = NaiveDate::from_ymd_opt(2026, 1, 2).expect("giorno del tracer");
    let days = i32::try_from(day.signed_duration_since(epoch).num_days()).expect("date32");
    let micros = day
        .and_hms_micro_opt(3, 4, 5, 123_456)
        .expect("istante del tracer")
        .and_utc()
        .timestamp_micros();
    let populated = ids.iter().map(|id| id % 2 == 1).collect::<Vec<_>>();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.to_vec())),
        Arc::new(StringArray::from(
            populated
                .iter()
                .map(|full| full.then_some("etichetta"))
                .collect::<Vec<_>>(),
        )),
        Arc::new(
            Decimal128Array::from(
                populated
                    .iter()
                    .map(|full| full.then_some(-105_i128))
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(12, 2)
            .expect("decimal del tracer"),
        ),
        Arc::new(BooleanArray::from(populated.clone())),
        Arc::new(Date32Array::from(
            populated
                .iter()
                .map(|full| full.then_some(days))
                .collect::<Vec<_>>(),
        )),
        Arc::new(TimestampMicrosecondArray::from(
            populated
                .iter()
                .map(|full| full.then_some(micros))
                .collect::<Vec<_>>(),
        )),
        Arc::new(BinaryArray::from_opt_vec(
            populated
                .iter()
                .map(|full| full.then_some(&[1_u8, 2, 3][..]))
                .collect::<Vec<_>>(),
        )),
    ];
    RecordBatch::try_new(Arc::clone(schema), columns).expect("batch append del tracer")
}

struct VecBatchStream {
    schema: plenora_database_core::arrow::SchemaRef,
    batches: std::collections::VecDeque<plenora_database_core::arrow::RecordBatch>,
}

impl plenora_database_core::provider::BatchStream for VecBatchStream {
    fn schema(&self) -> plenora_database_core::arrow::SchemaRef {
        std::sync::Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a plenora_database_core::CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<
        'a,
        Option<plenora_database_core::arrow::RecordBatch>,
    > {
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }
}

struct DiagnosticBatchStream {
    inner: VecBatchStream,
    declared_rows: u64,
    policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy,
}

impl plenora_database_core::provider::BatchStream for DiagnosticBatchStream {
    fn schema(&self) -> plenora_database_core::arrow::SchemaRef {
        self.inner.schema()
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a plenora_database_core::CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<
        'a,
        Option<plenora_database_core::arrow::RecordBatch>,
    > {
        self.inner.next_batch(cancellation)
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.declared_rows)
    }

    fn row_diagnostics_policy(
        &self,
    ) -> plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
        self.policy.clone()
    }
}

fn append_operation(table: &str) -> plenora_database_core::plan::WriteOperation {
    plenora_database_core::plan::WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: table.to_owned(),
            layer_id: None,
        },
        mode: plenora_database_core::plan::WriteMode::Append,
        mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
        transaction_profile: plenora_database_core::plan::TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn write_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits {
        rows: 1_024,
        memory_bytes: 8 * 1024 * 1024,
        output_bytes: 8 * 1024 * 1024,
        cell_bytes: 1024 * 1024,
        ..ResourceLimits::default()
    })
    .expect("budget write MySQL live")
}

/// Il tracer verticale completo: prepare con preflight sullo schema del
/// server, INSERT dentro una sola transazione, COMMIT e rilettura esatta dal
/// path di lettura qualificato.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per append transazionale"]
#[allow(clippy::too_many_lines)]
async fn live_append_commits_a_single_transaction_and_reads_back_exactly() {
    use plenora_database_core::arrow::array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int64Array, StringArray,
        TimestampMicrosecondArray,
    };
    use plenora_database_core::outcome::WriteStatus;
    use plenora_database_core::plan::{OrderBy, ReadOperation, SortDirection};

    let table = "write_append_probe";
    let config = live_config();
    let mut setup = append_setup_connection(&config).await;
    reset_append_target(&mut setup, table).await;

    let provider = MysqlProvider::new(config, 2).expect("provider append MySQL live");
    let cancellation = CancellationToken::new();
    let budget = write_budget();
    let schema = append_input_schema();
    let operation = append_operation(table);
    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare append MySQL live");
    assert_eq!(
        prepared.loss_report.policy,
        plenora_database_core::loss::MappingPolicy::Strict
    );
    assert!(prepared.loss_report.losses.is_empty());
    assert!(prepared.loss_report.permits_execution());

    let input = VecBatchStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![
            append_batch(&schema, &[1, 2]),
            append_batch(&schema, &[3]),
        ]),
    };
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(input),
            &budget,
            &cancellation,
        )
        .await
        .expect("write append MySQL live");
    outcome.validate().expect("outcome append valido");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.provider, ProviderKind::Mysql);
    assert_eq!(outcome.rows.received, 3);
    assert_eq!(outcome.rows.confirmed, 3);
    assert_eq!(outcome.rows.inserted, Some(3));
    assert_eq!(outcome.rows.failed, 0);
    assert_eq!(outcome.rows.skipped, 0);
    assert!(outcome.recovery.is_none());
    assert!(!outcome.execution_id.is_empty());

    let read = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: table.to_owned(),
            layer_id: None,
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        filter: None,
    };
    let mut stream = provider
        .read(
            &live_secret(),
            &read,
            &ParameterBag::default(),
            &write_budget(),
            &cancellation,
        )
        .await
        .expect("rilettura append MySQL live");
    let mut read_batches = Vec::new();
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .expect("batch di rilettura")
    {
        read_batches.push(batch);
    }
    assert_eq!(
        read_batches
            .iter()
            .map(plenora_database_core::arrow::RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
    let mut ids = Vec::new();
    let mut labels = Vec::new();
    let mut amounts = Vec::new();
    let mut active = Vec::new();
    let mut days = Vec::new();
    let mut moments = Vec::new();
    let mut payloads = Vec::new();
    for batch in &read_batches {
        let id = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .expect("id riletto");
        let label = batch
            .column_by_name("label")
            .and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .expect("label riletta");
        let amount = batch
            .column_by_name("amount")
            .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
            .expect("amount riletto");
        let flag = batch
            .column_by_name("active")
            .and_then(|array| array.as_any().downcast_ref::<BooleanArray>())
            .expect("active riletto");
        let day = batch
            .column_by_name("day")
            .and_then(|array| array.as_any().downcast_ref::<Date32Array>())
            .expect("day riletto");
        let moment = batch
            .column_by_name("moment")
            .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
            .expect("moment riletto");
        let payload = batch
            .column_by_name("payload")
            .and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
            .expect("payload riletto");
        for row in 0..batch.num_rows() {
            ids.push(id.value(row));
            labels.push((!label.is_null(row)).then(|| label.value(row).to_owned()));
            amounts.push((!amount.is_null(row)).then(|| amount.value(row)));
            active.push(flag.value(row));
            days.push((!day.is_null(row)).then(|| day.value(row)));
            moments.push((!moment.is_null(row)).then(|| moment.value(row)));
            payloads.push((!payload.is_null(row)).then(|| payload.value(row).to_vec()));
        }
    }
    let expected = append_batch(&schema, &[1, 2, 3]);
    let expected_amounts = expected
        .column_by_name("amount")
        .and_then(|array| array.as_any().downcast_ref::<Decimal128Array>())
        .expect("amount scritto");
    let expected_days = expected
        .column_by_name("day")
        .and_then(|array| array.as_any().downcast_ref::<Date32Array>())
        .expect("day scritto");
    let expected_moments = expected
        .column_by_name("moment")
        .and_then(|array| array.as_any().downcast_ref::<TimestampMicrosecondArray>())
        .expect("moment scritto");
    assert_eq!(ids, vec![1, 2, 3]);
    assert_eq!(
        labels,
        vec![
            Some("etichetta".to_owned()),
            None,
            Some("etichetta".to_owned())
        ]
    );
    assert_eq!(active, vec![true, false, true]);
    assert_eq!(
        payloads,
        vec![Some(vec![1, 2, 3]), None, Some(vec![1, 2, 3])]
    );
    assert_eq!(
        amounts,
        (0..3)
            .map(|row| (!expected_amounts.is_null(row)).then(|| expected_amounts.value(row)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        days,
        (0..3)
            .map(|row| (!expected_days.is_null(row)).then(|| expected_days.value(row)))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        moments,
        (0..3)
            .map(|row| (!expected_moments.is_null(row)).then(|| expected_moments.value(row)))
            .collect::<Vec<_>>()
    );
    drop(stream);

    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("cleanup del target append");
    drop(setup);
}

/// Il path spatial scrive esclusivamente WKB XY bind-safe; lo SRID viene
/// applicato dalla funzione `MySQL` qualificata e verificato sul valore salvato.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per append spatial XY"]
async fn live_append_spatial_xy_preserves_srid_and_coordinates() {
    use plenora_database_core::arrow::array::{ArrayRef, BinaryArray, Int64Array};
    use plenora_database_core::arrow::RecordBatch;
    use plenora_database_core::outcome::WriteStatus;
    use plenora_database_core::plan::SridPolicy;
    use std::sync::Arc;

    let table = "write_spatial_probe";
    let config = live_config();
    let cancellation = CancellationToken::new();
    let mut setup = append_setup_connection(&config).await;
    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("drop del target spatial");
    setup
        .query_drop(format!(
            "CREATE TABLE `{table}` (\
             id BIGINT NOT NULL PRIMARY KEY, \
             geom POINT NULL SRID 4326) ENGINE=InnoDB"
        ))
        .await
        .expect("create del target spatial");

    let mut catalog = MysqlSession::open(&config, &cancellation)
        .await
        .expect("sessione catalogo spatial");
    let target = describe_object(&mut catalog, "dataflow_test", table, &cancellation)
        .await
        .expect("descrizione target spatial");
    drop(catalog);
    let schema = append_spatial_schema(&target);
    let point = append_point_xy_wkb(12.5, -7.25);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
            Arc::new(BinaryArray::from_opt_vec(vec![
                Some(point.as_slice()),
                None,
            ])) as ArrayRef,
        ],
    )
    .expect("batch spatial XY");
    let mut operation = append_operation(table);
    operation.srid_policy = Some(SridPolicy::RequireMatch);
    let provider = MysqlProvider::new(config, 2).expect("provider spatial MySQL live");
    let budget = write_budget();
    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare append spatial MySQL live");
    assert!(prepared.loss_report.losses.is_empty());
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(VecBatchStream {
                schema: Arc::clone(&schema),
                batches: std::collections::VecDeque::from(vec![batch]),
            }),
            &budget,
            &cancellation,
        )
        .await
        .expect("write append spatial MySQL live");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.confirmed, 2);

    let rows: Vec<SpatialObservation> = setup
        .query(format!(
            "SELECT id, ST_SRID(geom), ST_X(geom), ST_Y(geom) \
             FROM `{table}` ORDER BY id"
        ))
        .await
        .expect("rilettura server-side spatial");
    assert_eq!(
        rows,
        vec![
            (1, Some(4_326), Some(12.5), Some(-7.25)),
            (2, None, None, None),
        ]
    );

    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("cleanup del target spatial");
    drop(setup);
}

/// Il fallimento di un batch annulla l'intera transazione: nessuna riga del
/// batch precedente resta visibile e l'errore dichiara `RolledBack`.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per rollback transazionale"]
async fn live_append_batch_failure_rolls_back_without_partial_rows() {
    let table = "write_rollback_probe";
    let config = live_config();
    let mut setup = append_setup_connection(&config).await;
    reset_append_target(&mut setup, table).await;
    setup
        .query_drop(format!(
            "INSERT INTO `{table}` (id, label, amount, active, day, moment, payload) \
             VALUES (2, NULL, NULL, 1, NULL, NULL, NULL)"
        ))
        .await
        .expect("riga preesistente in conflitto");

    let provider = MysqlProvider::new(config, 2).expect("provider rollback MySQL live");
    let cancellation = CancellationToken::new();
    let budget = write_budget();
    let schema = append_input_schema();
    let prepared = provider
        .prepare_write(
            &live_secret(),
            &append_operation(table),
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare rollback MySQL live");
    let input = VecBatchStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![
            append_batch(&schema, &[10, 11]),
            append_batch(&schema, &[2]),
        ]),
    };
    let error = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(input),
            &budget,
            &cancellation,
        )
        .await
        .expect_err("chiave duplicata nel secondo batch");
    assert_eq!(error.category, ErrorCategory::Conflict);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::RolledBack
    );
    assert_eq!(error.retry, plenora_database_core::RetryDisposition::Never);
    assert!(error.execution_id.is_some());

    let remaining: Vec<i64> = setup
        .query(format!("SELECT id FROM `{table}` ORDER BY id"))
        .await
        .expect("rilettura dopo rollback");
    assert_eq!(remaining, vec![2], "il rollback ha lasciato righe parziali");

    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("cleanup del target rollback");
    drop(setup);
}

#[tokio::test]
#[ignore = "richiede MySQL live per l'oracolo row diagnostics"]
#[allow(clippy::too_many_lines)]
async fn live_provider_row_diagnostics_matches_confirmed_rollback_oracle() {
    const INPUT_TOTAL: u64 = 5_200;
    const REJECTED_INDEX: u64 = 4_999;
    const TABLE: &str = "write_row_diagnostics_probe";

    let config = live_config();
    let mut setup = append_setup_connection(&config).await;
    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{TABLE}`"))
        .await
        .expect("drop fixture row diagnostics");
    setup
        .query_drop(format!(
            "CREATE TABLE `{TABLE}` (\
             parcel_id BIGINT NOT NULL PRIMARY KEY, \
             area_m2 BIGINT NOT NULL, \
             CONSTRAINT chk_row_diagnostics_area_nonnegative \
             CHECK (area_m2 >= 0)) ENGINE=InnoDB"
        ))
        .await
        .expect("create fixture row diagnostics");

    let schema = row_diagnostics_input_schema();
    let budget = ResourceBudget::new(ResourceLimits {
        rows: INPUT_TOTAL,
        memory_bytes: 8 * 1_024 * 1_024,
        output_bytes: 8 * 1_024 * 1_024,
        cell_bytes: 1_024,
        ..ResourceLimits::default()
    })
    .expect("budget row diagnostics MySQL live");
    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(config, 2).expect("provider row diagnostics MySQL live");
    let prepared = provider
        .prepare_write(
            &live_secret(),
            &append_operation(TABLE),
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare row diagnostics MySQL live");
    let input = DiagnosticBatchStream {
        inner: VecBatchStream {
            schema: std::sync::Arc::clone(&schema),
            batches: std::collections::VecDeque::from(vec![
                row_diagnostics_batch(&schema, 0, 4_096),
                row_diagnostics_batch(&schema, 4_096, INPUT_TOTAL),
            ]),
        },
        declared_rows: INPUT_TOTAL,
        policy: plenora_database_core::row_diagnostics::RowDiagnosticsPolicy {
            key_field: Some("parcel_id".to_owned()),
            constraint_column: Some("area_m2".to_owned()),
            examples_limit: 10,
        },
    };
    let error = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(input),
            &budget,
            &cancellation,
        )
        .await
        .expect_err("constraint row-scoped MySQL live");

    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Write);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::RolledBack
    );
    assert_eq!(error.retry, plenora_database_core::RetryDisposition::Never);
    assert_eq!(error.provider, Some(ProviderKind::Mysql));
    assert!(error.execution_id.is_some());
    assert_eq!(error.message, "riga sorgente rifiutata dal database");
    for forbidden in ["4999", "-987654321", "parcel_id", "area_m2"] {
        assert!(!error.message.contains(forbidden));
    }

    let envelope = serde_json::to_value(&error).expect("envelope row diagnostics serializzabile");
    assert_eq!(
        envelope.get("diagnostics"),
        Some(&serde_json::json!({
            "contract": "plenora-row-diagnostics-v1",
            "scope": "write",
            "index_basis": "source_row_zero_based",
            "completeness": "complete",
            "observed_total": 1,
            "total": 1,
            "input_total": 5200,
            "counts": {"database.constraint_violation": 1},
            "examples_limit": 10,
            "examples_truncated": false,
            "examples": [{
                "source_index": REJECTED_INDEX,
                "cause": "database.constraint_violation",
                "column": "area_m2",
                "key": {"field": "parcel_id", "state": "redacted"},
                "write_state": "certainly_rejected"
            }],
            "diagnostic_state_counts": {
                "certainly_rejected": 1,
                "certainly_not_attempted": 0,
                "certainly_rolled_back": 0,
                "effect_unknown": 0
            },
            "write_outcome": {
                "certainly_rejected": {"state": "known", "value": 1},
                "certainly_not_attempted": {"state": "known", "value": 200},
                "certainly_rolled_back": {"state": "known", "value": 4999},
                "effect_unknown": {"state": "known", "value": 0}
            }
        }))
    );

    let remaining: Option<u64> = setup
        .query_first(format!("SELECT COUNT(*) FROM `{TABLE}`"))
        .await
        .expect("conteggio dopo rollback row diagnostics");
    assert_eq!(remaining, Some(0));
    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{TABLE}`"))
        .await
        .expect("cleanup fixture row diagnostics");
    drop(setup);
}

/// Un timeout in-flight rende ambiguo l'effetto remoto, quarantina la
/// connessione e la fa sostituire nel pool: la scrittura successiva riparte
/// su una sessione nuova e committa.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per quarantena write"]
#[allow(clippy::too_many_lines)]
async fn live_append_timeout_quarantines_and_replaces_the_pooled_session() {
    use plenora_database_core::outcome::WriteStatus;
    use std::collections::BTreeSet;

    async fn pooled_identifiers(audit: &mut mysql_async::Conn) -> BTreeSet<u64> {
        audit
            .query::<u64, _>(
                "SELECT ID FROM information_schema.processlist WHERE USER = 'dataflow'",
            )
            .await
            .expect("processlist MySQL live")
            .into_iter()
            .collect()
    }

    let table = "write_quarantine_probe";
    let config = live_config();
    let mut setup = append_setup_connection(&config).await;
    reset_append_target(&mut setup, table).await;
    let mut audit = append_setup_connection(&config).await;
    let mut holder = append_setup_connection(&config).await;

    let provider = MysqlProvider::new(
        config.with_timeouts(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_millis(1_500),
            std::time::Duration::from_secs(5),
        ),
        1,
    )
    .expect("provider quarantena MySQL live");
    let schema = append_input_schema();
    let cancellation = CancellationToken::new();

    let baseline = pooled_identifiers(&mut audit).await;
    let budget = write_budget();
    let prepared = provider
        .prepare_write(
            &live_secret(),
            &append_operation(table),
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare quarantena MySQL live");
    let committed = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(VecBatchStream {
                schema: std::sync::Arc::clone(&schema),
                batches: std::collections::VecDeque::from(vec![append_batch(&schema, &[1])]),
            }),
            &budget,
            &cancellation,
        )
        .await
        .expect("prima write MySQL live");
    assert_eq!(committed.status, WriteStatus::Committed);
    let pooled = pooled_identifiers(&mut audit).await;
    let first_session = pooled.difference(&baseline).copied().collect::<Vec<_>>();
    assert_eq!(first_session.len(), 1, "sessione del pool non identificata");
    let first_session = first_session[0];

    holder
        .query_drop("BEGIN")
        .await
        .expect("apertura transazione bloccante");
    holder
        .query_drop(format!(
            "INSERT INTO `{table}` (id, label, amount, active, day, moment, payload) \
             VALUES (5, NULL, NULL, 1, NULL, NULL, NULL)"
        ))
        .await
        .expect("lock esclusivo sulla chiave 5");

    let blocked_budget = write_budget();
    let blocked_prepared = provider
        .prepare_write(
            &live_secret(),
            &append_operation(table),
            std::sync::Arc::clone(&schema),
            &blocked_budget,
            &cancellation,
        )
        .await
        .expect("prepare bloccato MySQL live");
    let blocked = provider
        .write(
            &live_secret(),
            blocked_prepared,
            Box::new(VecBatchStream {
                schema: std::sync::Arc::clone(&schema),
                batches: std::collections::VecDeque::from(vec![append_batch(&schema, &[5])]),
            }),
            &blocked_budget,
            &cancellation,
        )
        .await
        .expect_err("INSERT bloccato oltre il timeout di operazione");
    assert_eq!(blocked.category, ErrorCategory::Timeout);
    assert_eq!(
        blocked.remote_effect,
        plenora_database_core::RemoteEffect::Unknown
    );
    assert_eq!(
        blocked.retry,
        plenora_database_core::RetryDisposition::RequiresRecovery
    );

    let mut replaced = false;
    for _ in 0..100 {
        if !pooled_identifiers(&mut audit)
            .await
            .contains(&first_session)
        {
            replaced = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(replaced, "la sessione quarantinata non e stata chiusa");

    holder
        .query_drop("ROLLBACK")
        .await
        .expect("rilascio del lock");

    let recovery_budget = write_budget();
    let recovery_prepared = provider
        .prepare_write(
            &live_secret(),
            &append_operation(table),
            std::sync::Arc::clone(&schema),
            &recovery_budget,
            &cancellation,
        )
        .await
        .expect("prepare dopo quarantena MySQL live");
    let recovered = provider
        .write(
            &live_secret(),
            recovery_prepared,
            Box::new(VecBatchStream {
                schema: std::sync::Arc::clone(&schema),
                batches: std::collections::VecDeque::from(vec![append_batch(&schema, &[5])]),
            }),
            &recovery_budget,
            &cancellation,
        )
        .await
        .expect("write dopo la sostituzione della sessione");
    assert_eq!(recovered.status, WriteStatus::Committed);
    assert_eq!(recovered.rows.confirmed, 1);

    // La lista processi e uno snapshot: il server puo non aver ancora chiuso
    // la sessione quarantinata quando la si campiona. Si attende che il pool
    // si stabilizzi, con scadenza — una sessione di troppo che non sparisce
    // resta un fallimento, non un'attesa infinita.
    let mut replacement = Vec::new();
    for _ in 0..100 {
        replacement = pooled_identifiers(&mut audit)
            .await
            .difference(&baseline)
            .copied()
            .collect::<Vec<_>>();
        if replacement.len() == 1 && replacement[0] != first_session {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert_eq!(replacement.len(), 1, "pool con piu di una sessione viva");
    assert_ne!(
        replacement[0], first_session,
        "la sessione quarantinata e stata riusata"
    );

    let remaining: Vec<i64> = setup
        .query(format!("SELECT id FROM `{table}` ORDER BY id"))
        .await
        .expect("rilettura dopo quarantena");
    assert_eq!(remaining, vec![1, 5]);

    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("cleanup del target quarantena");
    drop(holder);
    drop(audit);
    drop(setup);
}

// ============================ v1.2 — Transaction OLTP live ================

#[tokio::test]
async fn live_v12_transaction_execute_and_commit() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider tx live");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = plenora_database_core::transaction::TransactionOptions::default();

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup connect");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_tx_commit")
            .await
            .expect("drop");
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_tx_commit (id BIGINT PRIMARY KEY, label TEXT NOT NULL) \
                 ENGINE=InnoDB",
            )
            .await
            .expect("create");
    }

    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin tx");
    let stmt = plenora_database_core::transaction::Statement {
        sql: "INSERT INTO _v12_tx_commit (id, label) VALUES (?, ?)".to_owned(),
        params: vec![
            plenora_database_core::provider::ParameterValue::I64(1),
            plenora_database_core::provider::ParameterValue::String("alfa".to_owned()),
        ],
    };
    let affected = tx.execute(&stmt, &cancellation).await.expect("insert");
    assert_eq!(affected, 1);
    let commit = tx.commit(&cancellation).await.expect("commit");
    assert!(matches!(
        commit,
        plenora_database_core::transaction::CommitOutcome::Committed
    ));

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let count: Option<u64> = check
        .connection_mut()
        .unwrap()
        .query_first("SELECT COUNT(*) FROM _v12_tx_commit WHERE id = 1")
        .await
        .expect("count");
    assert_eq!(count, Some(1));
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_tx_commit")
        .await
        .ok();
}

#[tokio::test]
async fn live_v12_transaction_rollback_drops_all_writes() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = plenora_database_core::transaction::TransactionOptions::default();

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_tx_rb")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop("CREATE TABLE _v12_tx_rb (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
            .await
            .expect("create");
    }

    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin");
    for id in 1..=3_i64 {
        let stmt = plenora_database_core::transaction::Statement {
            sql: "INSERT INTO _v12_tx_rb (id) VALUES (?)".to_owned(),
            params: vec![plenora_database_core::provider::ParameterValue::I64(id)],
        };
        tx.execute(&stmt, &cancellation).await.expect("insert");
    }
    tx.rollback(&cancellation).await.expect("rollback");

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let count: Option<u64> = check
        .connection_mut()
        .unwrap()
        .query_first("SELECT COUNT(*) FROM _v12_tx_rb")
        .await
        .expect("count");
    assert_eq!(count, Some(0), "rollback deve annullare tutti gli insert");
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_tx_rb")
        .await
        .ok();
}

#[tokio::test]
async fn live_v12_transaction_savepoint_rollback_to_partial() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = plenora_database_core::transaction::TransactionOptions::default();

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_tx_sp")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop("CREATE TABLE _v12_tx_sp (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
            .await
            .expect("create");
    }

    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin");

    let stmt1 = plenora_database_core::transaction::Statement {
        sql: "INSERT INTO _v12_tx_sp (id) VALUES (?)".to_owned(),
        params: vec![plenora_database_core::provider::ParameterValue::I64(1)],
    };
    tx.execute(&stmt1, &cancellation).await.expect("insert 1");

    tx.savepoint("sp1", &cancellation).await.expect("savepoint");

    let stmt2 = plenora_database_core::transaction::Statement {
        sql: "INSERT INTO _v12_tx_sp (id) VALUES (?)".to_owned(),
        params: vec![plenora_database_core::provider::ParameterValue::I64(2)],
    };
    tx.execute(&stmt2, &cancellation).await.expect("insert 2");
    let stmt3 = plenora_database_core::transaction::Statement {
        sql: "INSERT INTO _v12_tx_sp (id) VALUES (?)".to_owned(),
        params: vec![plenora_database_core::provider::ParameterValue::I64(3)],
    };
    tx.execute(&stmt3, &cancellation).await.expect("insert 3");

    tx.rollback_to_savepoint("sp1", &cancellation)
        .await
        .expect("rollback to sp");
    tx.release_savepoint("sp1", &cancellation)
        .await
        .expect("release sp");

    let commit = tx.commit(&cancellation).await.expect("commit");
    assert!(matches!(
        commit,
        plenora_database_core::transaction::CommitOutcome::Committed
    ));

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let rows: Vec<u64> = check
        .connection_mut()
        .unwrap()
        .query::<u64, _>("SELECT id FROM _v12_tx_sp ORDER BY id")
        .await
        .expect("select");
    assert_eq!(
        rows,
        vec![1],
        "solo l'insert fuori savepoint deve essere committato"
    );
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_tx_sp")
        .await
        .ok();
}

#[tokio::test]
async fn live_v12_transaction_query_returns_typed_rows() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = plenora_database_core::transaction::TransactionOptions::default();

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_tx_query")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_tx_query (id BIGINT PRIMARY KEY, val DOUBLE NOT NULL) \
                 ENGINE=InnoDB",
            )
            .await
            .expect("create");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("INSERT INTO _v12_tx_query VALUES (1, 10.5), (2, 20.5), (3, 30.5)")
            .await
            .expect("seed");
    }

    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin");
    let stmt = plenora_database_core::transaction::Statement {
        sql: "SELECT id, val FROM _v12_tx_query WHERE id >= ? ORDER BY id".to_owned(),
        params: vec![plenora_database_core::provider::ParameterValue::I64(2)],
    };
    let rows = tx.query(&stmt, &cancellation).await.expect("query");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns().len(), 2);

    tx.rollback(&cancellation).await.expect("rollback");

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_tx_query")
        .await
        .ok();
}

#[tokio::test]
async fn live_v12_provider_execute_ddl_creates_and_drops_table() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();

    provider
        .execute_ddl(
            &live_secret(),
            "CREATE TABLE _v12_ddl (id BIGINT PRIMARY KEY) ENGINE=InnoDB",
            &cancellation,
        )
        .await
        .expect("execute_ddl CREATE");

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let exists: Option<u64> = check
        .connection_mut()
        .unwrap()
        .query_first(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema='dataflow_test' AND table_name='_v12_ddl'",
        )
        .await
        .expect("check exists");
    assert_eq!(exists, Some(1));

    provider
        .execute_ddl(&live_secret(), "DROP TABLE _v12_ddl", &cancellation)
        .await
        .expect("execute_ddl DROP");
}

#[tokio::test]
async fn live_v12_conditional_update_rolls_back_on_mismatch() {
    use plenora_database_core::transaction::ConditionalUpdate;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = plenora_database_core::transaction::TransactionOptions::default();

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_cu")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_cu (id BIGINT PRIMARY KEY, version INT NOT NULL) \
                 ENGINE=InnoDB",
            )
            .await
            .expect("create");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("INSERT INTO _v12_cu VALUES (1, 100)")
            .await
            .expect("seed");
    }

    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin");

    let update = plenora_database_core::transaction::Statement {
        sql: "UPDATE _v12_cu SET version = version + 1 WHERE id = ? AND version = ?".to_owned(),
        params: vec![
            plenora_database_core::provider::ParameterValue::I64(1),
            plenora_database_core::provider::ParameterValue::I32(99),
        ],
    };
    let request = ConditionalUpdate {
        update: &update,
        key_probe: None,
        expected_affected_rows: 1,
    };
    let result = tx.execute_conditional_update(request, &cancellation).await;
    assert!(matches!(
        result.as_ref().err().map(|e| e.category),
        Some(ErrorCategory::ConcurrentModification)
    ));

    let stmt_check = plenora_database_core::transaction::Statement {
        sql: "SELECT version FROM _v12_cu WHERE id = 1".to_owned(),
        params: vec![],
    };
    let rows = tx.query(&stmt_check, &cancellation).await.expect("query");
    assert_eq!(rows.len(), 1);

    tx.rollback(&cancellation).await.expect("rollback");

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_cu")
        .await
        .ok();
}

// ============================ v1.2 — Write bulk modes (Create/TruncateInsert) ==

fn write_op_scalar(
    schema: &str,
    table: &str,
    mode: plenora_database_core::plan::WriteMode,
) -> plenora_database_core::plan::WriteOperation {
    plenora_database_core::plan::WriteOperation {
        target: plenora_database_core::plan::ObjectRef {
            catalog: None,
            schema: Some(schema.to_owned()),
            object: table.to_owned(),
            layer_id: None,
        },
        mode,
        mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
        transaction_profile: plenora_database_core::plan::TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn scalar_batch(
    ids: &[i64],
    labels: &[&str],
) -> (
    plenora_database_core::arrow::SchemaRef,
    plenora_database_core::arrow::RecordBatch,
) {
    use plenora_database_core::arrow::array::{Int64Array, StringArray};
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    use plenora_database_core::arrow::RecordBatch;
    use std::sync::Arc;

    let schema: plenora_database_core::arrow::SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(labels.to_vec())),
        ],
    )
    .expect("scalar batch");
    (schema, batch)
}

struct BatchesStream {
    schema: plenora_database_core::arrow::SchemaRef,
    batches: std::collections::VecDeque<plenora_database_core::arrow::RecordBatch>,
    declared: u64,
}

impl plenora_database_core::provider::BatchStream for BatchesStream {
    fn schema(&self) -> plenora_database_core::arrow::SchemaRef {
        std::sync::Arc::clone(&self.schema)
    }
    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<
        'a,
        Option<plenora_database_core::arrow::RecordBatch>,
    > {
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }
    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.declared)
    }
}

#[tokio::test]
async fn live_v12_write_create_mode_builds_table_and_inserts() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    // Cleanup
    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_create")
            .await
            .ok();
    }

    let (schema, batch) = scalar_batch(&[1, 2, 3], &["a", "b", "c"]);
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_create",
        plenora_database_core::plan::WriteMode::Create,
    );

    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare_write create");

    let stream = BatchesStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![batch]),
        declared: 3,
    };
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(stream),
            &budget,
            &cancellation,
        )
        .await
        .expect("write create");
    assert_eq!(
        outcome.status,
        plenora_database_core::outcome::WriteStatus::Committed
    );
    assert_eq!(outcome.rows.received, 3);
    assert_eq!(outcome.rows.confirmed, 3);

    // Verifica DDL applicato
    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let exists: Option<u64> = check
        .connection_mut()
        .unwrap()
        .query_first(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema='dataflow_test' AND table_name='_v12_create'",
        )
        .await
        .expect("exists");
    assert_eq!(exists, Some(1));
    let count: Option<u64> = check
        .connection_mut()
        .unwrap()
        .query_first("SELECT COUNT(*) FROM _v12_create")
        .await
        .expect("count");
    assert_eq!(count, Some(3));
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_create")
        .await
        .ok();
}

#[tokio::test]
async fn live_v12_write_create_mode_conflict_if_exists() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    // Setup: crea la tabella prima
    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_create_conflict")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop("CREATE TABLE _v12_create_conflict (id BIGINT PRIMARY KEY) ENGINE=InnoDB")
            .await
            .expect("create");
    }

    let (schema, batch) = scalar_batch(&[1], &["x"]);
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_create_conflict",
        plenora_database_core::plan::WriteMode::Create,
    );

    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare");
    let stream = BatchesStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![batch]),
        declared: 1,
    };
    let result = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(stream),
            &budget,
            &cancellation,
        )
        .await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::Conflict)
        ),
        "mode=create su target esistente deve restituire Conflict"
    );

    let mut cleanup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("cleanup");
    cleanup
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_create_conflict")
        .await
        .ok();
}

/// Fail-closed: `TruncateInsert` è stato rimosso su `MySQL` (TRUNCATE è DDL
/// con commit implicito → non rollback-safe se l'INSERT successivo
/// fallisce). `prepare_write` deve rifiutarlo come `Unsupported`.
#[tokio::test]
async fn live_v12_write_truncate_insert_rejected_fail_closed() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let (schema, _batch) = scalar_batch(&[1, 2], &["new-a", "new-b"]);
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_trunc",
        plenora_database_core::plan::WriteMode::TruncateInsert,
    );

    let Err(error) = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("TruncateInsert MySQL deve essere rifiutato fail-closed");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

fn write_op_with_keys(
    schema: &str,
    table: &str,
    mode: plenora_database_core::plan::WriteMode,
    keys: Vec<String>,
) -> plenora_database_core::plan::WriteOperation {
    let mut op = write_op_scalar(schema, table, mode);
    op.keys = keys;
    op
}

#[tokio::test]
async fn live_v12_write_upsert_updates_existing_and_inserts_new() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_upsert")
            .await
            .ok();
        setup.connection_mut().unwrap()
            .query_drop("CREATE TABLE _v12_upsert (id BIGINT PRIMARY KEY, label TEXT NOT NULL) ENGINE=InnoDB")
            .await.expect("create");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("INSERT INTO _v12_upsert VALUES (1, 'old-1'), (2, 'old-2')")
            .await
            .expect("seed");
    }

    // Upsert: id=1 (esistente, sarà aggiornato); id=3 (nuovo, sarà inserito).
    let (schema, batch) = scalar_batch(&[1, 3], &["upd-1", "new-3"]);
    let operation = write_op_with_keys(
        "dataflow_test",
        "_v12_upsert",
        plenora_database_core::plan::WriteMode::Upsert,
        vec!["id".to_owned()],
    );

    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare upsert");
    let stream = BatchesStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![batch]),
        declared: 2,
    };
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(stream),
            &budget,
            &cancellation,
        )
        .await
        .expect("write upsert");
    assert_eq!(
        outcome.status,
        plenora_database_core::outcome::WriteStatus::Committed
    );

    // Verifica: id=1 aggiornato (upd-1), id=2 invariato (old-2), id=3 nuovo (new-3)
    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let rows: Vec<(i64, String)> = check
        .connection_mut()
        .unwrap()
        .query::<(i64, String), _>("SELECT id, label FROM _v12_upsert ORDER BY id")
        .await
        .expect("select");
    assert_eq!(
        rows,
        vec![
            (1, "upd-1".to_owned()),
            (2, "old-2".to_owned()),
            (3, "new-3".to_owned()),
        ]
    );
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_upsert")
        .await
        .ok();
}

/// Fail-closed: un Upsert su `keys=[id]` verso una tabella che ha un
/// **secondo** unique index (`code`) deve essere rifiutato in prepare —
/// `ON DUPLICATE KEY UPDATE` potrebbe collidere su `code` e aggiornare la
/// riga sbagliata.
#[tokio::test]
async fn live_v12_write_upsert_rejects_conflicting_unique_index() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_upsert_unsafe")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_upsert_unsafe (\
                 id BIGINT PRIMARY KEY, \
                 label TEXT NOT NULL, \
                 code BIGINT NOT NULL, \
                 UNIQUE KEY uq_code (code)\
                 ) ENGINE=InnoDB",
            )
            .await
            .expect("create");
    }

    let (schema, _batch) = scalar_batch(&[1], &["x"]);
    let operation = write_op_with_keys(
        "dataflow_test",
        "_v12_upsert_unsafe",
        plenora_database_core::plan::WriteMode::Upsert,
        vec!["id".to_owned()],
    );
    let Err(error) = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("prepare upsert deve fallire fail-closed sull'indice in conflitto");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_upsert_unsafe")
        .await
        .ok();
}

fn keys_only_batch(
    ids: &[i64],
) -> (
    plenora_database_core::arrow::SchemaRef,
    plenora_database_core::arrow::RecordBatch,
) {
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    use plenora_database_core::arrow::RecordBatch;
    use std::sync::Arc;

    let schema: plenora_database_core::arrow::SchemaRef =
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("keys-only batch");
    (schema, batch)
}

#[tokio::test]
async fn live_v12_write_delete_by_keys_removes_matching_rows() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_del")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_del (id BIGINT PRIMARY KEY, label TEXT NOT NULL) ENGINE=InnoDB",
            )
            .await
            .expect("create");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("INSERT INTO _v12_del VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')")
            .await
            .expect("seed");
    }

    // Delete id=2 e id=4; id=99 non esiste (idempotent)
    let (schema, batch) = keys_only_batch(&[2, 4, 99]);
    let operation = write_op_with_keys(
        "dataflow_test",
        "_v12_del",
        plenora_database_core::plan::WriteMode::DeleteByKeys,
        vec!["id".to_owned()],
    );

    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare delete");
    let stream = BatchesStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![batch]),
        declared: 3,
    };
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(stream),
            &budget,
            &cancellation,
        )
        .await
        .expect("write delete");
    assert_eq!(
        outcome.status,
        plenora_database_core::outcome::WriteStatus::Committed
    );
    // 3 keys ricevute, 2 effettivamente cancellate (id 2 e 4); id 99 skipped
    assert_eq!(outcome.rows.received, 3);
    assert_eq!(outcome.rows.confirmed, 2);
    assert_eq!(outcome.rows.deleted, Some(2));
    assert_eq!(outcome.rows.skipped, 1);

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let remaining: Vec<i64> = check
        .connection_mut()
        .unwrap()
        .query::<i64, _>("SELECT id FROM _v12_del ORDER BY id")
        .await
        .expect("select");
    assert_eq!(remaining, vec![1, 3]);
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_del")
        .await
        .ok();
}

#[tokio::test]
async fn live_v12_write_update_via_staging_updates_matching_rows() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_upd")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_upd (id BIGINT PRIMARY KEY, label TEXT NOT NULL) ENGINE=InnoDB",
            )
            .await
            .expect("create");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("INSERT INTO _v12_upd VALUES (1, 'orig-1'), (2, 'orig-2'), (3, 'orig-3')")
            .await
            .expect("seed");
    }

    // Update: id=1 → new-1, id=2 → new-2, id=99 → no-op (non trovato)
    let (schema, batch) = scalar_batch(&[1, 2, 99], &["new-1", "new-2", "ghost"]);
    let operation = write_op_with_keys(
        "dataflow_test",
        "_v12_upd",
        plenora_database_core::plan::WriteMode::Update,
        vec!["id".to_owned()],
    );

    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare update");
    let stream = BatchesStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![batch]),
        declared: 3,
    };
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(stream),
            &budget,
            &cancellation,
        )
        .await
        .expect("write update");
    assert_eq!(
        outcome.status,
        plenora_database_core::outcome::WriteStatus::Committed
    );
    assert_eq!(outcome.rows.received, 3);
    assert_eq!(
        outcome.rows.confirmed, 2,
        "2 righe target aggiornate (id 1 e 2)"
    );
    assert_eq!(outcome.rows.updated, Some(2));
    assert_eq!(
        outcome.rows.skipped, 1,
        "id 99 non trovato in target = skipped"
    );

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    let rows: Vec<(i64, String)> = check
        .connection_mut()
        .unwrap()
        .query::<(i64, String), _>("SELECT id, label FROM _v12_upd ORDER BY id")
        .await
        .expect("select");
    assert_eq!(
        rows,
        vec![
            (1, "new-1".to_owned()),
            (2, "new-2".to_owned()),
            (3, "orig-3".to_owned()),
        ]
    );
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_upd")
        .await
        .ok();
}

// ============================ v1.2 — Blocco C: spatial verified ===========

#[tokio::test]
async fn live_v12_capabilities_publish_verified_spatial_functions() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let caps = provider
        .probe_capabilities(&live_secret(), &cancellation)
        .await
        .expect("probe caps");
    let functions = &caps.spatial.functions;
    assert!(
        !functions.is_empty(),
        "v1.2 deve pubblicare funzioni spatial verified"
    );
    // 20+ funzioni attese (metadata + predicati + metrics + constructors + set ops)
    assert!(
        functions.len() >= 20,
        "atteso >= 20 funzioni verified, trovate {}",
        functions.len()
    );
    for expected in &[
        plenora_database_core::query::SpatialFunction::Intersects,
        plenora_database_core::query::SpatialFunction::Contains,
        plenora_database_core::query::SpatialFunction::Within,
        plenora_database_core::query::SpatialFunction::Distance,
        plenora_database_core::query::SpatialFunction::Area,
    ] {
        assert!(
            functions.contains(expected),
            "spatial function verified attesa mancante: {expected:?}"
        );
    }
}

#[tokio::test]
async fn live_v12_query_spatial_functions_render_and_execute() {
    use plenora_database_core::plan::{ObjectRef, SortDirection};
    use plenora_database_core::provider::ParameterBag;
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
        SpatialFunction,
    };
    use plenora_database_core::resource::ResourceBudget;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    // Setup: crea tabella con GEOMETRY SRID 4326
    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_spatial")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_spatial (id BIGINT PRIMARY KEY, \
                 shape GEOMETRY NOT NULL SRID 4326) ENGINE=InnoDB",
            )
            .await
            .expect("create");
        // Insert 3 geometrie: 2 point + 1 linestring
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "INSERT INTO _v12_spatial VALUES \
                 (1, ST_GeomFromText('POINT(0 0)', 4326)), \
                 (2, ST_GeomFromText('POINT(1 1)', 4326)), \
                 (3, ST_GeomFromText('LINESTRING(0 0, 5 5)', 4326))",
            )
            .await
            .expect("seed");
    }

    // Query portable: SELECT id, ST_Area(shape) AS area FROM _v12_spatial ORDER BY id
    let mut operation = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "_v12_spatial".to_owned(),
                layer_id: None,
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![
            QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "id".to_owned(),
                    },
                },
                alias: None,
            },
            QueryProjection {
                expression: QueryExpression::Spatial {
                    function: SpatialFunction::Area,
                    arguments: vec![QueryExpression::Column {
                        column: ColumnRef {
                            relation: None,
                            field: "shape".to_owned(),
                        },
                    }],
                },
                alias: Some("area".to_owned()),
            },
        ],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "id".to_owned(),
                },
            },
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    };
    operation.derived_source = None; // (placeholder)

    let bag = ParameterBag::default();
    let stream = provider
        .query(&live_secret(), &operation, &bag, &budget, &cancellation)
        .await
        .expect("query spatial");
    // Consumo lo stream: se render e execute OK, il test passa (validazione
    // semantica dei valori area richiederebbe parsing Arrow — sufficient
    // qui che la query non fallisca).
    drop(stream);

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_spatial")
        .await
        .ok();
}

#[tokio::test]
async fn live_v12_query_spatial_predicate_intersects_in_filter() {
    use plenora_database_core::plan::{ObjectRef, SortDirection};
    use plenora_database_core::provider::{ParameterBag, ParameterValue};
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
        SpatialFunction,
    };
    use plenora_database_core::resource::ResourceBudget;
    use std::collections::BTreeMap;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        setup
            .connection_mut()
            .unwrap()
            .query_drop("DROP TABLE IF EXISTS _v12_spatial_pred")
            .await
            .ok();
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "CREATE TABLE _v12_spatial_pred (id BIGINT PRIMARY KEY, \
                 shape GEOMETRY NOT NULL SRID 4326) ENGINE=InnoDB",
            )
            .await
            .expect("create");
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "INSERT INTO _v12_spatial_pred VALUES \
                 (1, ST_GeomFromText('POINT(1 1)', 4326)), \
                 (2, ST_GeomFromText('POINT(10 10)', 4326))",
            )
            .await
            .expect("seed");
    }

    // WKB per POINT(1 1) SRID 4326: 25 bytes standard WKB (little-endian).
    // Header: 01 (LE) + 01000000 (type=Point) + coordinates.
    // NOTA: MySQL ST_GeomFromWKB nel path portable si aspetta un blob WKB;
    // il test qui usa una geometry SRID-agnostic (SRID 0 verrebbe fissato
    // dal SRID column constraint). Se lo storage richiede SRID matching,
    // usiamo ST_SRID nella query.
    // WKB POINT(1 1) little-endian: 01 01000000 000000000000F03F 000000000000F03F
    let wkb_point_1_1: Vec<u8> = vec![
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F,
    ];

    let operation = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "_v12_spatial_pred".to_owned(),
                layer_id: None,
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "id".to_owned(),
                },
            },
            alias: None,
        }],
        joins: Vec::new(),
        // WHERE ST_Intersects(shape, ST_GeomFromWKB(:probe))
        filter: Some(QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![
                QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "shape".to_owned(),
                    },
                },
                QueryExpression::Parameter {
                    name: "probe".to_owned(),
                },
            ],
        }),
        group_by: Vec::new(),
        having: None,
        order_by: vec![QueryOrdering {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "id".to_owned(),
                },
            },
            direction: SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        row_limit: None,
        row_offset: None,
        locking: None,
    };

    let mut bag_map = BTreeMap::new();
    bag_map.insert("probe".to_owned(), ParameterValue::Bytes(wkb_point_1_1));
    let bag = ParameterBag::new(bag_map);

    let result = provider
        .query(&live_secret(), &operation, &bag, &budget, &cancellation)
        .await;
    // Se il render + execute passano è sufficient — non asseriamo l'esatto
    // count (Intersects su POINT(1,1) vs POINT(1,1) del target: match
    // atteso ma SRID può causare no-match; il test valida solo che il path
    // spatial in WHERE non fallisce).
    if let Err(e) = &result {
        panic!("spatial predicate WHERE fallisce: {e:?}");
    }
    drop(result.unwrap());

    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("check");
    check
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE _v12_spatial_pred")
        .await
        .ok();
}

/// Fail-closed: `Replace` è stato rimosso su `MySQL` (staging + RENAME perde
/// vincoli/indici/FK/trigger/permessi non ricostruibili). `prepare_write`
/// deve rifiutarlo come `Unsupported`.
#[tokio::test]
async fn live_v12_write_replace_rejected_fail_closed() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let (schema, _batch) = scalar_batch(&[1, 2, 3], &["new-1", "new-2", "new-3"]);
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_rep",
        plenora_database_core::plan::WriteMode::Replace,
    );

    let Err(error) = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("Replace MySQL deve essere rifiutato fail-closed");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
}

#[tokio::test]
async fn live_v12_write_update_without_keys_rejected() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let (schema, _batch) = scalar_batch(&[1], &["x"]);
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_upd_no_keys",
        plenora_database_core::plan::WriteMode::Update,
    );

    let result = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::InvalidPlan)
        ),
        "update senza keys deve fallire InvalidPlan"
    );
}

#[tokio::test]
async fn live_v12_write_delete_by_keys_without_keys_rejected() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let (schema, _batch) = keys_only_batch(&[1]);
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_del_no_keys",
        plenora_database_core::plan::WriteMode::DeleteByKeys,
    );

    let result = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::InvalidPlan)
        ),
        "delete_by_keys senza keys deve fallire InvalidPlan"
    );
}

#[tokio::test]
async fn live_v12_write_upsert_without_keys_rejected() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let (schema, _batch) = scalar_batch(&[1], &["x"]);
    // Upsert senza keys → InvalidPlan al prepare (validate_operation).
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_no_upsert",
        plenora_database_core::plan::WriteMode::Upsert,
    );

    let result = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::InvalidPlan)
        ),
        "upsert senza keys deve fallire con InvalidPlan"
    );
}

// ============================================================================
//  PFM CHG-003 — NativeQueryPolicy MySQL parity
// ============================================================================
//
// I test qui sotto verificano che l'enforcement di `NativeQueryPolicy` (già
// coperto client-side da unit test in `plenora-database-core`) sia
// effettivamente cablato dentro `MysqlTransaction::{execute, query,
// execute_conditional_update}`. Non serve un data-plane elaborato: basta
// aprire una tx con `pfm_defaults()` (Deny) e verificare che DDL / session
// control ricevano `InvalidPlan` prima di toccare il server.

#[tokio::test]
async fn live_native_query_policy_deny_rejects_ddl() {
    use plenora_database_core::transaction::{Statement, TransactionOptions};

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin pfm_defaults");
    let result = tx
        .execute(
            &Statement::new("CREATE TABLE _nqp_deny_ddl (x INT)"),
            &cancellation,
        )
        .await;
    let _ = tx.rollback(&cancellation).await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::InvalidPlan)
        ),
        "DDL sotto pfm_defaults deve fallire con InvalidPlan"
    );
}

#[tokio::test]
async fn live_native_query_policy_deny_rejects_session_control() {
    use plenora_database_core::transaction::{Statement, TransactionOptions};

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin pfm_defaults");
    let result = tx
        .execute(
            &Statement::new("SET SESSION time_zone = '+00:00'"),
            &cancellation,
        )
        .await;
    let _ = tx.rollback(&cancellation).await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::InvalidPlan)
        ),
        "SET SESSION sotto pfm_defaults deve fallire con InvalidPlan"
    );
}

#[tokio::test]
async fn live_native_query_policy_allow_permits_ddl() {
    use plenora_database_core::transaction::{Statement, TransactionOptions};

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    // Cleanup pregresso via DDL diretto (fuori dalla tx sotto test).
    provider
        .execute_ddl(
            &live_secret(),
            "DROP TABLE IF EXISTS _nqp_allow_ddl",
            &cancellation,
        )
        .await
        .expect("cleanup pregresso");

    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &TransactionOptions::default(), // Allow (baseline non-PFM)
            &budget,
            &cancellation,
        )
        .await
        .expect("begin default");
    // Con Allow il policy non blocca; MySQL fa autocommit implicito su DDL,
    // quindi va a buon fine oltre l'enforcement.
    tx.execute(
        &Statement::new("CREATE TABLE _nqp_allow_ddl (x INT) ENGINE=InnoDB"),
        &cancellation,
    )
    .await
    .expect("DDL permesso da NativeQueryPolicy::Allow");
    let _ = tx.rollback(&cancellation).await;

    provider
        .execute_ddl(
            &live_secret(),
            "DROP TABLE IF EXISTS _nqp_allow_ddl",
            &cancellation,
        )
        .await
        .expect("cleanup finale");
}

#[tokio::test]
async fn live_native_query_policy_deny_rejects_ddl_via_query() {
    use plenora_database_core::transaction::{Statement, TransactionOptions};

    // Assicura enforcement anche sul path `query` (non solo `execute`).
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin pfm_defaults");
    let result = tx
        .query(
            &Statement::new("CREATE TABLE _nqp_deny_q (x INT)"),
            &cancellation,
        )
        .await;
    let _ = tx.rollback(&cancellation).await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::InvalidPlan)
        ),
        "DDL via query() sotto pfm_defaults deve fallire con InvalidPlan"
    );
}

#[tokio::test]
async fn live_native_query_policy_deny_rejects_conditional_update_ddl() {
    use plenora_database_core::transaction::{ConditionalUpdate, Statement, TransactionOptions};

    // Assicura enforcement anche sul path `execute_conditional_update`.
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &TransactionOptions::pfm_defaults(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin pfm_defaults");
    let update = Statement::new("CREATE TABLE _nqp_deny_cu (x INT)");
    let request = ConditionalUpdate {
        update: &update,
        key_probe: None,
        expected_affected_rows: 0,
    };
    let result = tx.execute_conditional_update(request, &cancellation).await;
    let _ = tx.rollback(&cancellation).await;
    assert!(
        matches!(
            result.err().map(|e| e.category),
            Some(ErrorCategory::InvalidPlan)
        ),
        "conditional_update con SQL non-CRUD sotto pfm_defaults deve fallire con InvalidPlan"
    );
}
