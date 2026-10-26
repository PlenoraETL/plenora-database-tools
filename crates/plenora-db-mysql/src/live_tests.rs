#![allow(clippy::significant_drop_tightening)]

use crate::MariadbProvider;
use crate::{
    describe_object, list_objects, list_schemas, probe_server, MysqlConfig, MysqlProvider,
    MysqlSession,
};
use mysql_async::prelude::Queryable;
use plenora_database_core::arrow::array::{
    ArrayRef, Float32Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::{WriteOutcome, WriteStatus};
use plenora_database_core::plan::{ObjectRef, Operation, ProviderKind, ReadOperation};
use plenora_database_core::plan::{TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::protocol::contract_schema;
use plenora_database_core::provider::{BatchStream, ProviderFuture};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::write_admission::{AdmissionKey, TargetDimensions};
use plenora_database_core::{
    CancellationToken, ErrorCategory, ReadCheckpoint, ResourceBudget, ResourceLimits,
};
use plenora_database_core::{DatabaseError, RemoteEffect};
use plenora_database_engine::{Engine, Observation};
use plenora_database_testkit::write_probe::{self, ProbeHooks, ProbeSpec, PROBE_TABLE};
use std::collections::VecDeque;
use std::sync::Arc;

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
        plenora_database_core::strict_json::from_slice(REFERENCE_MATRIX.as_bytes())
            .expect("matrice MySQL leggibile");
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
        },
        projection: Vec::new(),
        order_by: Vec::new(),
        row_limit: Some(1),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
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
    let pool = mysql_async::Pool::new(
        config
            .pooled_driver_opts(1, "MySQL")
            .expect("pool opts MySQL live"),
    );
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
    assert!(sql_mode.contains("NO_AUTO_VALUE_ON_ZERO"));
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
    // Sei mode qualificate; `TruncateInsert` resta fail-closed e non ha un
    // flag proprio nel contratto — la sua prova e il rifiuto in prepare,
    // in `live_v12_write_truncate_insert_rejected_without_remote_effects`.
    assert!(capabilities.writes.create);
    assert!(capabilities.writes.append);
    assert!(capabilities.writes.rollback_on_failure);
    assert!(capabilities.writes.update);
    assert!(capabilities.writes.upsert);
    assert!(capabilities.writes.replace);
    assert!(capabilities.writes.delete_by_keys);
    assert!(capabilities.writes.bulk);
    // Nessuna delle tre e implementata: il contratto non le promette.
    assert!(!capabilities.writes.array_binding);
    assert!(!capabilities.writes.returning);
    assert!(capabilities.transactions.single_transaction);
    assert!(!capabilities.transactions.transactional_ddl);
    // Lo swap staged non esiste piu su MySQL: Replace e DELETE + insert
    // nella stessa transazione, non una tabella pubblicata al posto di
    // un'altra.
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

    let source = ObjectRef {
        catalog: None,
        schema: Some("dataflow_test".to_owned()),
        object: "catalog_probe".to_owned(),
    };
    let inspection = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: source.clone(),
            },
            &cancellation,
        )
        .await
        .expect("inspect MySQL live");
    assert_eq!(inspection.operation, "database.describe_object");
    assert_eq!(inspection.document["name"], "catalog_probe");
    assert_eq!(inspection.document["engine"], "InnoDB");
    let engine = Engine::new(
        std::sync::Arc::new(
            MysqlProvider::new(live_config(), 1).expect("typed reflection provider"),
        ),
        live_secret(),
    );
    let typed = engine
        .reflect_table(&source, false, &cancellation)
        .await
        .expect("typed MySQL reflection");
    let typed_table = typed.one_table().expect("one reflected table");
    assert_eq!(typed_table.name(), "catalog_probe");
    assert_eq!(typed_table.constraints(), Observation::NotMeasured);
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
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
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
            .pooled_driver_opts(1, "MySQL")
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
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
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
    let setup = mysql_async::Pool::new(
        config
            .pooled_driver_opts(1, "MySQL")
            .expect("pool setup batching"),
    );
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
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
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
        },
        projection: vec!["id".to_owned(), "name".to_owned()],
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Desc,
        }],
        row_limit: Some(1),
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: "id".to_owned(),
            parameter: "wanted_id".to_owned(),
        }),
        declared_crs: Vec::new(),
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
#[ignore = "richiede MySQL live esplicito e prova persistenza/ripresa"]
async fn live_keyset_checkpoint_persists_reopens_without_duplicates_or_gaps() {
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::plan::{OrderBy, SortDirection};
    use plenora_database_core::provider::ParameterValue;

    let cancellation = CancellationToken::new();
    let provider = MysqlProvider::new(live_config(), 2).expect("provider checkpoint MySQL");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget checkpoint MySQL");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: "stream_probe".to_owned(),
        },
        projection: vec!["id".to_owned()],
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(3),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let parameters = ParameterBag::default();
    let secret = live_secret();
    let mut first = provider
        .read(&secret, &operation, &parameters, &budget, &cancellation)
        .await
        .expect("prima pagina checkpoint MySQL");
    let batch = first
        .next_batch(&cancellation)
        .await
        .expect("batch checkpoint MySQL")
        .expect("righe checkpoint MySQL");
    let ids = batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .expect("id checkpoint MySQL");
    assert_eq!(ids.values(), &[1, 2, 3]);
    let token = ReadCheckpoint::new(
        ProviderKind::Mysql,
        &operation,
        &parameters,
        vec![ParameterValue::I64(ids.value(ids.len() - 1))],
    )
    .expect("token checkpoint MySQL")
    .to_json()
    .expect("persistenza checkpoint MySQL");
    drop(first);

    let checkpoint = ReadCheckpoint::from_json(&token).expect("riapertura checkpoint MySQL");
    let mut next_page = operation.clone();
    next_page.row_limit = Some(4);
    let (resumed, resumed_parameters) = checkpoint
        .resume(ProviderKind::Mysql, &next_page, &parameters)
        .expect("piano ripreso MySQL");
    let mut second = provider
        .read(
            &secret,
            &resumed,
            &resumed_parameters,
            &budget,
            &cancellation,
        )
        .await
        .expect("seconda pagina checkpoint MySQL");
    let batch = second
        .next_batch(&cancellation)
        .await
        .expect("batch ripreso MySQL")
        .expect("righe riprese MySQL");
    let ids = batch
        .column_by_name("id")
        .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
        .expect("id ripreso MySQL");
    assert_eq!(ids.values(), &[4, 5, 6, 7]);
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
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: Some(1),
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
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
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "catalog_probe".to_owned(),
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
        },
        alias: None,
    };
    let scalar_query = |source: QuerySource, projection: Vec<QueryProjection>| QueryOperation {
        declared_crs: Vec::new(),
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
        declared_crs: Vec::new(),
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
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "stream_probe".to_owned(),
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
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "stream_probe".to_owned(),
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
            .pooled_driver_opts(1, "MySQL")
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
            declared_crs: Vec::new(),
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("dataflow_test".to_owned()),
                    object: "slow_query_probe".to_owned(),
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
            .pooled_driver_opts(1, "MySQL")
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
    mysql_async::Conn::new(
        config
            .driver_opts("MySQL")
            .expect("opzioni driver MySQL live"),
    )
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

    // ARROW-VOCABULARY §4: un campo geometria ha il proprio field id, che la
    // descrizione di catalogo non assegna (lo fa chi pubblica lo schema).
    let fields = target
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let field = crate::MysqlColumnSpec::from_catalog(column)
                .expect("colonna target spatial qualificata")
                .arrow_field();
            let mut metadata = field.metadata().clone();
            metadata.insert(protocol::FIELD_ID.to_owned(), index.to_string());
            field.with_metadata(metadata)
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
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
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

    // GEO-001: il WKB scritto ha la longitudine per prima (12.5) e la
    // latitudine dopo (-7.25). Su un SRS geografico `ST_X`/`ST_Y` seguono
    // l'ordine del registro (latitudine prima per EPSG:4326), quindi la
    // verifica legge `ST_Longitude`/`ST_Latitude`, che non dipendono
    // dall'ordine. Fino al 24/10 questo test leggeva `ST_X`/`ST_Y` e fissava
    // come giusto il valore memorizzato con gli assi scambiati (latitudine
    // 12.5).
    let rows: Vec<SpatialObservation> = setup
        .query(format!(
            "SELECT id, ST_SRID(geom), ST_Longitude(geom), ST_Latitude(geom) \
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

/// GEO-001 e GEO-008 su un SRS geografico, end to end. Un campo `lon_lat` in
/// EPSG:4326 senza `plenora.geometry.srid` (SRID derivato, verificato nel
/// catalogo) si memorizza con la longitudine come longitudine, e la lettura
/// rende lo stesso WKB, longitudine per prima. Fino al 24/10 la scrittura
/// memorizzava `POINT(12.5 60)` con latitudine 12.5, e la lettura rendeva il
/// WKB del registro, latitudine per prima. Uno SRID derivato che il catalogo
/// non registra come `EPSG` si rifiuta `crs` prima di ogni effetto.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito per l'ordine degli assi geografici"]
#[allow(clippy::too_many_lines)]
async fn live_geographic_axis_order_round_trips_and_derived_srid_is_verified() {
    use plenora_database_core::arrow::array::{ArrayRef, BinaryArray, Int64Array};
    use plenora_database_core::arrow::schema::Schema;
    use plenora_database_core::arrow::RecordBatch;
    use plenora_database_core::outcome::WriteStatus;
    use plenora_database_core::plan::SridPolicy;
    use plenora_database_core::protocol;
    use std::sync::Arc;

    let table = "write_axis_probe";
    let config = live_config();
    let cancellation = CancellationToken::new();
    let mut setup = append_setup_connection(&config).await;
    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("drop del target");
    setup
        .query_drop(format!(
            "CREATE TABLE `{table}` (\
             id BIGINT NOT NULL PRIMARY KEY, \
             geom POINT NULL SRID 4326) ENGINE=InnoDB"
        ))
        .await
        .expect("create del target");
    let mut catalog = MysqlSession::open(&config, &cancellation)
        .await
        .expect("sessione catalogo");
    let target = describe_object(&mut catalog, "dataflow_test", table, &cancellation)
        .await
        .expect("descrizione target");
    drop(catalog);
    let with_crs = |crs_id: &str| {
        let base = append_spatial_schema(&target);
        let fields = base
            .fields()
            .iter()
            .map(|field| {
                if field.name() != "geom" {
                    return field.as_ref().clone();
                }
                let mut metadata = field.metadata().clone();
                metadata.remove(protocol::GEOMETRY_SRID);
                metadata.insert(protocol::GEOMETRY_CRS_ID.to_owned(), crs_id.to_owned());
                metadata.insert(
                    protocol::GEOMETRY_AXIS_ORDER.to_owned(),
                    "lon_lat".to_owned(),
                );
                field.as_ref().clone().with_metadata(metadata)
            })
            .collect::<Vec<_>>();
        Arc::new(Schema::new_with_metadata(fields, base.metadata().clone()))
    };
    let mut operation = append_operation(table);
    operation.srid_policy = Some(SridPolicy::RequireMatch);
    let provider = MysqlProvider::new(config, 2).expect("provider MySQL live");
    let budget = write_budget();

    // Uno SRID derivato che il catalogo non registra: `crs`, niente scritto.
    let error = provider
        .prepare_write(
            &live_secret(),
            &operation,
            with_crs("EPSG:999999"),
            &budget,
            &cancellation,
        )
        .await
        .err()
        .expect("SRID derivato assente dal catalogo");
    assert_eq!(error.category, ErrorCategory::Crs);

    let schema = with_crs("EPSG:4326");
    let point = append_point_xy_wkb(12.5, 60.0);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])) as ArrayRef,
            Arc::new(BinaryArray::from_opt_vec(vec![Some(point.as_slice())])) as ArrayRef,
        ],
    )
    .expect("batch");
    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare con SRID derivato da EPSG:4326");
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
        .expect("write");
    assert_eq!(outcome.status, WriteStatus::Committed);
    let rows: Vec<SpatialObservation> = setup
        .query(format!(
            "SELECT id, ST_SRID(geom), ST_Longitude(geom), ST_Latitude(geom) \
             FROM `{table}` ORDER BY id"
        ))
        .await
        .expect("rilettura server-side");
    assert_eq!(rows, vec![(1, Some(4_326), Some(12.5), Some(60.0))]);

    let read = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: table.to_owned(),
        },
        projection: vec!["geom".to_owned()],
        order_by: Vec::new(),
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let mut stream = provider
        .read(
            &live_secret(),
            &read,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("stream");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch")
        .expect("riga");
    let wkb = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("WKB")
        .value(0)
        .to_vec();
    assert_eq!(wkb, point, "la lettura rende la longitudine per prima");
    drop(stream);

    // `x` e `y` del contratto sono la prima e la seconda ordinata del WKB:
    // longitudine e latitudine, come su PostGIS, anche se `ST_X` di `MySQL`
    // su EPSG:4326 renderebbe la latitudine.
    let coordinate = |function| plenora_database_core::query::QueryProjection {
        expression: plenora_database_core::query::QueryExpression::Spatial {
            function,
            arguments: vec![plenora_database_core::query::QueryExpression::Column {
                column: plenora_database_core::query::ColumnRef {
                    relation: None,
                    field: "geom".to_owned(),
                },
            }],
        },
        alias: None,
    };
    let query = plenora_database_core::query::QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(plenora_database_core::query::QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: table.to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![
            coordinate(plenora_database_core::query::SpatialFunction::X),
            coordinate(plenora_database_core::query::SpatialFunction::Y),
        ],
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
    let mut stream = provider
        .query(
            &live_secret(),
            &query,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("query X/Y");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("batch X/Y")
        .expect("riga X/Y");
    let value = |index: usize| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<plenora_database_core::arrow::array::Float64Array>()
            .expect("double")
            .value(0)
    };
    assert_eq!((value(0), value(1)), (12.5, 60.0));
    drop(stream);

    setup
        .query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("cleanup");
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

// ============================ Transaction OLTP live =======================

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

/// Il riuso di una connessione ripulisce il `SessionContext`.
///
/// Le variabili utente `MySQL` sono legate alla **connessione**, non alla
/// transazione: `COMMIT` e `ROLLBACK` non le toccano. Se il pool riciclasse
/// una connessione senza ripulirla, il context scritto da un chiamante
/// resterebbe leggibile dal successivo — una fuga fra tenant, non un
/// dettaglio di igiene.
///
/// Il pool di produzione tiene `min = 0`: nessuna connessione resta idle,
/// quindi ogni checkout ne apre una nuova e `CONNECTION_ID()` cambia
/// sempre. Il riuso non e osservabile li, e un test che lo aspettasse
/// passerebbe per la ragione sbagliata. Con un pool che ne trattiene una —
/// le stesse `Opts` del provider, solo `min = 1` — il riuso diventa
/// osservabile e si vede che la variabile e sparita **sulla stessa
/// connessione**. E cio che protegge il giorno in cui `min` cambiera:
/// `with_reset_connection(true)`, che il provider imposta.
#[tokio::test]
async fn live_v12_session_context_is_cleared_when_a_connection_is_reused() {
    use mysql_async::prelude::Queryable as _;

    let retaining = mysql_async::Pool::new(mysql_async::Opts::from(
        mysql_async::OptsBuilder::from_opts(
            live_config()
                .pooled_driver_opts(1, "MySQL")
                .expect("opts pooled"),
        )
        .pool_opts(Some(
            mysql_async::PoolOpts::default()
                .with_constraints(mysql_async::PoolConstraints::new(1, 1).expect("vincoli 1..1"))
                .with_reset_connection(true),
        )),
    ));

    let first_id: Option<u64> = {
        let mut connection = retaining.get_conn().await.expect("checkout iniziale");
        connection
            .query_drop("SET @`plenora_ctx_app.tenant` = 'acme'")
            .await
            .expect("scrittura context");
        let written: Option<String> = connection
            .query_first("SELECT @`plenora_ctx_app.tenant`")
            .await
            .expect("rilettura context");
        assert_eq!(
            written.as_deref(),
            Some("acme"),
            "il context non e stato scritto: il test non proverebbe nulla"
        );
        connection
            .query_first("SELECT CONNECTION_ID()")
            .await
            .expect("id")
    };

    let mut reused = retaining.get_conn().await.expect("checkout riusato");
    let reused_id: Option<u64> = reused
        .query_first("SELECT CONNECTION_ID()")
        .await
        .expect("id riusato");
    assert_eq!(
        reused_id, first_id,
        "il pool non ha riusato la connessione: la prova non sarebbe valida"
    );
    let survived: Option<Option<String>> = reused
        .query_first("SELECT @`plenora_ctx_app.tenant`")
        .await
        .expect("rilettura dopo il riuso");
    assert_eq!(
        survived,
        Some(None),
        "il context del chiamante precedente e sopravvissuto al riuso"
    );
    drop(reused);
    retaining
        .disconnect()
        .await
        .expect("chiusura pool di prova");
}

/// Sul percorso reale, il chiamante senza context non vede quello di prima.
///
/// Tre transazioni su un pool da uno, chiuse in entrambi i modi: `COMMIT` e
/// `ROLLBACK` restituiscono la connessione per strade diverse, e nessuna
/// delle due deve lasciare il context visibile.
#[tokio::test]
async fn live_v12_session_context_does_not_reach_the_next_transaction() {
    use plenora_database_core::session_context::{SessionEntry, SessionValue};

    let provider = MysqlProvider::new(live_config(), 1).expect("provider pool=1");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let mut with_context = plenora_database_core::transaction::TransactionOptions::default();
    with_context
        .context
        .insert(
            "app.tenant",
            SessionEntry::public(SessionValue::Text("acme".to_owned())),
        )
        .expect("chiave valida");
    let leaked = plenora_database_core::transaction::Statement {
        sql: "SELECT 1 WHERE @`plenora_ctx_app.tenant` IS NOT NULL".to_owned(),
        params: Vec::new(),
    };

    let mut tx = provider
        .begin_transaction(&live_secret(), &with_context, &budget, &cancellation)
        .await
        .expect("begin con context");
    assert_eq!(
        tx.query(&leaked, &cancellation)
            .await
            .expect("lettura")
            .len(),
        1,
        "il context non e arrivato: la fase successiva non proverebbe nulla"
    );
    tx.commit(&cancellation).await.expect("commit");

    let tx = provider
        .begin_transaction(&live_secret(), &with_context, &budget, &cancellation)
        .await
        .expect("begin con context");
    tx.rollback(&cancellation).await.expect("rollback");

    let options = plenora_database_core::transaction::TransactionOptions::default();
    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin senza context");
    assert_eq!(
        tx.query(&leaked, &cancellation)
            .await
            .expect("lettura")
            .len(),
        0,
        "il context del chiamante precedente e ancora leggibile"
    );
    tx.rollback(&cancellation).await.expect("rollback finale");
}

/// Il `SessionContext` raggiunge il server, con la chiave che il core produce.
///
/// Il core impone `namespace.name`, quindi il provider deve accettare il punto.
/// Il server accetta queste variabili, quotate o no.
#[tokio::test]
async fn live_v12_transaction_session_context_reaches_the_server() {
    use plenora_database_core::session_context::{SessionEntry, SessionValue};

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");

    let mut options = plenora_database_core::transaction::TransactionOptions::default();
    options
        .context
        .insert(
            "app.tenant",
            SessionEntry::public(SessionValue::Text("acme".to_owned())),
        )
        .expect("chiave valida per il core");
    options
        .context
        .insert(
            "app.request_id",
            SessionEntry::internal(SessionValue::Integer(42)),
        )
        .expect("chiave valida per il core");

    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin con session context");

    // La riga esiste solo se **entrambe** le variabili hanno il valore
    // atteso: un context non arrivato le lascia NULL, il confronto e falso e
    // il result set e vuoto. Cosi l'assert dipende dai valori, non dal fatto
    // che la query giri.
    let statement = plenora_database_core::transaction::Statement {
        sql: "SELECT 1 WHERE @`plenora_ctx_app.tenant` = 'acme' \
             AND @`plenora_ctx_app.request_id` = '42'"
            .to_owned(),
        params: Vec::new(),
    };
    let rows = tx
        .query(&statement, &cancellation)
        .await
        .expect("lettura del context");
    assert_eq!(rows.len(), 1, "il session context non e arrivato al server");

    tx.rollback(&cancellation).await.expect("rollback");
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

// ============================ Write bulk: Create/TruncateInsert ============

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

/// Il produttore che, finiti i batch, non risponde piu.
struct StallingBatchesStream(BatchesStream);

impl plenora_database_core::provider::BatchStream for StallingBatchesStream {
    fn schema(&self) -> plenora_database_core::arrow::SchemaRef {
        self.0.schema()
    }
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> plenora_database_core::provider::ProviderFuture<
        'a,
        Option<plenora_database_core::arrow::RecordBatch>,
    > {
        if self.0.batches.is_empty() {
            return Box::pin(std::future::pending());
        }
        self.0.next_batch(cancellation)
    }
    fn declared_input_rows(&self) -> Option<u64> {
        self.0.declared_input_rows()
    }
}

/// Una scrittura `create` abbandonata dopo il DDL confermato (il chiamante
/// lascia cadere la future mentre il produttore non risponde) non arriva a
/// un esito: il target resta osservabile nel registro dei residui.
#[tokio::test]
async fn live_v12_an_abandoned_create_is_recorded_as_a_possible_residue() {
    use plenora_database_core::provider::BatchStream as _;
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut setup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("setup");
    setup
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE IF EXISTS _v12_abandoned_create")
        .await
        .ok();
    let (schema, batch) = scalar_batch(&[1, 2, 3], &["a", "b", "c"]);
    let operation = write_op_scalar(
        "dataflow_test",
        "_v12_abandoned_create",
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
    // Il batch dichiara piu righe di quante ne arrivino: la scrittura resta
    // in attesa del produttore.
    let stream = StallingBatchesStream(BatchesStream {
        schema: std::sync::Arc::clone(&schema),
        batches: std::collections::VecDeque::from(vec![batch]),
        declared: 6,
    });
    assert!(stream.declared_input_rows().is_some());
    assert_eq!(provider.ddl_residues().total, 0);
    let abandoned = tokio::time::timeout(
        std::time::Duration::from_secs(20),
        provider.write(
            &live_secret(),
            prepared,
            Box::new(stream),
            &budget,
            &cancellation,
        ),
    )
    .await;
    assert!(
        abandoned.is_err(),
        "la scrittura doveva attendere il produttore"
    );
    let residues = provider.ddl_residues();
    assert_eq!(residues.total, 1);
    assert_eq!(residues.recent[0].object, "_v12_abandoned_create");
    setup
        .connection_mut()
        .unwrap()
        .query_drop("DROP TABLE IF EXISTS _v12_abandoned_create")
        .await
        .ok();
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
    assert_eq!(error.category, ErrorCategory::Schema);
    assert!(error
        .message
        .ends_with(plenora_database_core::write_keys::UPSERT_WITH_OTHER_UNIQUE_INDEX));

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

// ============================ Spatial verificato ==========================

/// La probe pubblica esattamente la lista delle funzioni spatial qualificate.
#[tokio::test]
async fn live_v12_capabilities_publish_verified_spatial_functions() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let caps = provider
        .probe_capabilities(&live_secret(), &cancellation)
        .await
        .expect("probe caps");
    assert!(
        !caps.spatial.functions.is_empty(),
        "il provider deve pubblicare funzioni spatial verificate"
    );
    assert_eq!(
        caps.spatial.functions,
        crate::query::VERIFIED_SPATIAL_FUNCTIONS,
        "le capability pubblicate divergono dalla lista verified"
    );
}

/// Ogni funzione di `VERIFIED_SPATIAL_FUNCTIONS`, eseguita contro il
/// riferimento.
///
/// La sonda le prova tutte, costruendo gli argomenti da
/// `accepts_argument_count` e `takes_geometry_at`: dove il contratto vuole una
/// geometria arriva la colonna, altrove un intero.
///
/// # Due geometrie, non una
///
/// Ogni funzione viene provata su una `LINESTRING` **e** su un `POLYGON`, e
/// conta se ne attraversa almeno una. Alcune funzioni non sono definite per
/// entrambe le forme; usarne una sola produrrebbe una falsa assenza.
///
/// # I rifiuti si raccolgono
///
/// Panicare sulla prima funzione rotta trasforma una lista sbagliata in una
/// sequenza di gate live, uno per difetto. Il primo giro avrebbe trovato solo
/// `Dimensions`; il secondo solo `NPoints`; per vedere le dodici sarebbero
/// serviti dodici giri con una fixture in piedi. Chi accorcia la lista deve
/// vederla intera in un colpo.
///
/// # Cosa dimostra, e cosa no
///
/// Il SRID e 0, cartesiano, per le funzioni che rendono uno scalare. Cio che si
/// dimostra e che il renderer produce SQL che `MySQL` esegue; se una funzione
/// valga anche su un sistema geografico e un'altra domanda, e questa lista non
/// la pone.
///
/// Per le funzioni che rendono **geometria** il sistema di riferimento non e
/// un dettaglio della fixture: senza un SRID sull'ingresso non c'e un frame da
/// ereditare, e il provider rifiuterebbe il piano prima di scrivere SQL. Quelle
/// girano percio su tre colonne in 3003 — proiettato — con il CRS dichiarato
/// dal piano, che e la sola forma in cui un chiamante puo chiederle.
///
/// Se una funzione non esegue, questo test diventa rosso e la lista va
/// accorciata: e il modo in cui una capability si chiude con una prova in mano
/// invece di restare aperta per analogia.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_v12_every_verified_spatial_function_executes() {
    use plenora_database_core::plan::ObjectRef;
    use plenora_database_core::provider::{ParameterBag, ParameterValue};
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource,
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
        let connection = setup.connection_mut().expect("connessione di setup");
        connection
            .query_drop("DROP TABLE IF EXISTS _v12_spatial_all")
            .await
            .ok();
        connection
            .query_drop(
                "CREATE TABLE _v12_spatial_all (id BIGINT PRIMARY KEY, point GEOMETRY NOT NULL, line GEOMETRY NOT NULL, poly GEOMETRY NOT NULL, framed_point GEOMETRY NOT NULL, framed_line GEOMETRY NOT NULL, framed_poly GEOMETRY NOT NULL) ENGINE=InnoDB",
            )
            .await
            .expect("create della tabella della sonda");
        // Tre geometrie senza sistema di riferimento e le stesse tre in
        // **3003**, che e un sistema proiettato.
        //
        // La coppia serve perche le funzioni che rendono geometria si chiedono
        // in un modo diverso dalle altre: portano un CRS dichiarato, e senza un
        // SRID sull'ingresso non ci sarebbe niente da ereditare — il piano
        // verrebbe rifiutato dal provider invece che dal server, e la sonda
        // misurerebbe se stessa.
        //
        // Proiettato e non geografico perche e la condizione in cui la
        // superficie esiste su entrambi i prodotti: `raw.crs_rule_check` ha
        // trovato che su `MySQL` queste funzioni rispondono 3618 in un sistema
        // geografico, e girano in uno proiettato.
        connection
            .query_drop(
                "INSERT INTO _v12_spatial_all VALUES (1, ST_GeomFromText('POINT(2 3)'), ST_GeomFromText('LINESTRING(0 0, 5 5, 10 0)'), ST_GeomFromText('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))'), ST_GeomFromText('POINT(1550000 4950000)', 3003), ST_GeomFromText('LINESTRING(1500000 4900000, 1550000 4950000, 1600000 4900000)', 3003), ST_GeomFromText('POLYGON((1500000 4900000, 1500000 5000000, 1600000 5000000, 1600000 4900000, 1500000 4900000))', 3003))",
            )
            .await
            .expect("seed della sonda");
    }

    let shape = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    };

    // Le funzioni rotte si **raccolgono**, non si segnalano una per volta.
    // Panicare sulla prima trasforma una lista sbagliata in una sequenza di
    // gate: il primo giro ha trovato `Dimensions`, e solo il secondo avrebbe
    // trovato `NPoints`. Chi accorcia la lista deve vederla intera.
    let mut executed = 0_usize;
    let mut broken: Vec<String> = Vec::new();
    for function in crate::query::VERIFIED_SPATIAL_FUNCTIONS {
        // Ogni funzione prova entrambe le geometrie: basta che una riesca.
        // Un tipo di ingresso incompatibile non dimostra l'assenza della funzione.
        let mut refusals: Vec<String> = Vec::new();
        // Una funzione che **rende** geometria si chiede sulle colonne con un
        // sistema di riferimento, e dichiarandolo: e la sola forma in cui il
        // provider la ammette, quindi e la sola che questa sonda deve
        // attraversare.
        let framed = function.returns_geometry();
        // Tre geometrie, non due: `ST_X` vuole un punto, e su una linea
        // fallirebbe per il dato invece che per il motore.
        for field in if framed {
            ["framed_point", "framed_line", "framed_poly"]
        } else {
            ["point", "line", "poly"]
        } {
            // L'arieta dichiarata dal contratto, non una tabella scritta a mano
            // qui: se il core la cambia, la sonda la segue.
            let arity = (1..=4)
                .find(|count| function.accepts_argument_count(*count))
                .unwrap_or_else(|| panic!("arieta sconosciuta per {function:?}"));
            let arguments: Vec<QueryExpression> = (0..arity)
                .map(|index| {
                    if function.takes_geometry_at(index) {
                        shape(field)
                    } else {
                        // Il renderer parametrizza soltanto `Parameter`: un
                        // letterale nell'AST non esiste, e non deve esistere.
                        // `PointN` vuole un indice di vertice, `Buffer` una
                        // distanza: 1 e valido per entrambi.
                        QueryExpression::Parameter {
                            name: "scalare".to_owned(),
                        }
                    }
                })
                .collect();
            // Prima che `arguments` entri nell'operazione: dopo e stato spostato,
            // e la domanda «questa funzione usa uno scalare?» non si potrebbe piu
            // porre alla forma che l'ha decisa.
            let uses_scalar = arguments
                .iter()
                .any(|argument| matches!(argument, QueryExpression::Parameter { .. }));

            let operation = QueryOperation {
                declared_crs: if framed {
                    vec![plenora_database_core::plan::DeclaredCrs {
                        column: field.to_owned(),
                        srid: 3_003,
                    }]
                } else {
                    Vec::new()
                },
                common_table_expressions: Vec::new(),
                source: Some(QuerySource {
                    object: ObjectRef {
                        catalog: None,
                        schema: Some("dataflow_test".to_owned()),
                        object: "_v12_spatial_all".to_owned(),
                    },
                    alias: None,
                }),
                derived_source: None,
                projection: vec![QueryProjection {
                    expression: QueryExpression::Spatial {
                        function: *function,
                        arguments,
                    },
                    alias: Some("probe".to_owned()),
                }],
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

            // Il bag contiene il parametro scalare solo se la funzione lo usa:
            // il preflight rifiuta parametri legati ma non referenziati.
            let bag = if uses_scalar {
                ParameterBag::new(BTreeMap::from([(
                    "scalare".to_owned(),
                    ParameterValue::I32(1),
                )]))
            } else {
                ParameterBag::default()
            };
            let opened = provider
                .query(&live_secret(), &operation, &bag, &budget, &cancellation)
                .await;
            let mut stream = match opened {
                Ok(stream) => stream,
                Err(error) => {
                    refusals.push(format!(
                        "su {field} il prepare fallisce ({})",
                        error.message
                    ));
                    continue;
                }
            };
            // Il prepare non e l'esecuzione. Ottenere lo stream e lasciarlo cadere
            // proverebbe che il server ha accettato lo statement, non che lo abbia
            // eseguito: l'errore del worker arriva al receiver **dopo**, e un
            // `drop` lo butterebbe via insieme allo stream. Qui si chiede il primo
            // batch e poi la fine dello stream, cioe si attraversa il risultato.
            match stream.next_batch(&cancellation).await {
                Err(error) => {
                    refusals.push(format!("su {field} non esegue ({})", error.message));
                    continue;
                }
                Ok(first) => {
                    if first.is_none_or(|batch| batch.num_rows() != 1) {
                        refusals.push(format!("su {field} non produce la riga della sonda"));
                        continue;
                    }
                }
            }
            match stream.next_batch(&cancellation).await {
                Err(error) => {
                    refusals.push(format!(
                        "su {field} non chiude lo stream ({})",
                        error.message
                    ));
                    continue;
                }
                Ok(Some(_)) => {
                    refusals.push(format!("su {field} non chiude lo stream dopo l'unica riga"));
                    continue;
                }
                Ok(None) => {}
            }
            // Una geometria che la attraversa basta: la domanda e se il renderer
            // produce SQL che il server esegue, non se ogni funzione valga su ogni
            // forma.
            refusals.clear();
            break;
        }
        if refusals.is_empty() {
            executed += 1;
        } else {
            broken.push(format!("{function:?}: {}", refusals.join(", e ")));
        }
    }

    assert!(
        broken.is_empty(),
        "verified che il riferimento non esegue ({} su {}): {}",
        broken.len(),
        crate::query::VERIFIED_SPATIAL_FUNCTIONS.len(),
        broken.join("; ")
    );
    assert_eq!(
        executed,
        crate::query::VERIFIED_SPATIAL_FUNCTIONS.len(),
        "la sonda deve attraversare l'intera lista pubblicata"
    );

    let mut cleanup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("cleanup");
    cleanup
        .connection_mut()
        .expect("connessione di cleanup")
        .query_drop("DROP TABLE IF EXISTS _v12_spatial_all")
        .await
        .ok();
}

/// Il CRS pubblicato per una geometria calcolata, e cosa succede se e falso.
///
/// # La domanda
///
/// Il contratto `GeoArrow` pubblica un sistema di riferimento. Per una
/// geometria **calcolata** il motore spesso non lo dice — `raw.crs_rule_check`
/// ha misurato `ST_Buffer` su `MariaDB` rendere SRID 0 partendo da 3003 — e cio
/// che il provider pubblica non e quell'etichetta ma il CRS dichiarato per la
/// colonna d'ingresso, propagato dalla regola della funzione.
///
/// Una regola e una dichiarazione, e una dichiarazione senza verifica e una
/// promessa. Questo test chiede le due meta.
///
/// # Le due meta
///
/// La prima: su una colonna che porta davvero 3003, il campo Arrow del
/// risultato dichiara 3003 e i byte sono WKB leggibile. Non e scontato che sia
/// 3003 — il motore ne rende un altro per il buffer, e se il provider
/// ripubblicasse cio che il motore dice, questo assert cadrebbe.
///
/// La seconda, che e quella che conta: sulla stessa colonna con **una riga in
/// 3857**, la lettura fallisce. Non fallisce al prepare — li nessuno puo
/// saperlo, perche una colonna geometrica che nessuna DDL vincola puo contenere
/// qualunque cosa — ma alla riga che smentisce la dichiarazione. Un provider
/// che si fidasse della parola del piano pubblicherebbe 3003 su una geometria
/// che sta altrove, ed e esattamente il difetto che il rifiuto di ieri
/// preveniva rifiutando tutto.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_v12_a_computed_geometry_carries_the_declared_frame_and_refuses_a_lie() {
    use plenora_database_core::arrow::array::Array;
    use plenora_database_core::plan::{DeclaredCrs, ObjectRef};
    use plenora_database_core::provider::ParameterBag;
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource,
    };
    use plenora_database_core::resource::ResourceBudget;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let table = "_v12_computed_geometry_crs";

    {
        let mut setup = MysqlSession::open(&live_config(), &cancellation)
            .await
            .expect("setup");
        let connection = setup.connection_mut().expect("connessione di setup");
        connection
            .query_drop(format!("DROP TABLE IF EXISTS {table}"))
            .await
            .ok();
        connection
            .query_drop(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, area GEOMETRY NOT NULL) \
                 ENGINE=InnoDB"
            ))
            .await
            .expect("create");
        // Due righe oneste e una che mente. La terza non e un caso di
        // laboratorio: una colonna geometrica che nessuna DDL vincola e
        // esattamente quella che puo contenerla, ed e la ragione per cui il
        // contratto chiede una dichiarazione **e** la verifica.
        for (id, srid) in [(1, 3_003), (2, 3_003), (3, 3_857)] {
            connection
                .query_drop(format!(
                    "INSERT INTO {table} VALUES ({id}, ST_GeomFromText('POLYGON((1500000 \
                     4900000, 1500000 5000000, 1600000 5000000, 1600000 4900000, 1500000 \
                     4900000))', {srid}))"
                ))
                .await
                .expect("seed");
        }
    }

    let plan = |limit: Option<u64>| QueryOperation {
        declared_crs: vec![DeclaredCrs {
            column: "area".to_owned(),
            srid: 3_003,
        }],
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: table.to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: QueryExpression::Spatial {
                function: plenora_database_core::query::SpatialFunction::Envelope,
                arguments: vec![QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "area".to_owned(),
                    },
                }],
            },
            alias: Some("frame".to_owned()),
        }],
        joins: Vec::new(),
        filter: None,
        group_by: Vec::new(),
        having: None,
        order_by: vec![plenora_database_core::query::QueryOrdering {
            expression: QueryExpression::Column {
                column: ColumnRef {
                    relation: None,
                    field: "id".to_owned(),
                },
            },
            direction: plenora_database_core::plan::SortDirection::Asc,
        }],
        distinct: false,
        distinct_on: Vec::new(),
        set_operations: Vec::new(),
        // Le due righe oneste soltanto: la stessa query senza limite incontra
        // la terza, ed e la seconda meta di questo test.
        row_limit: limit,
        row_offset: None,
        locking: None,
    };

    // --- La prima meta: il frame dichiarato arriva fino ad Arrow ------------
    let mut stream = provider
        .query(
            &live_secret(),
            &plan(Some(2)),
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("una geometria calcolata con un CRS dichiarato deve aprirsi");
    let batch = stream
        .next_batch(&cancellation)
        .await
        .expect("la lettura deve attraversare le due righe oneste")
        .expect("un batch");
    assert_eq!(batch.num_rows(), 2);
    // Una colonna sola: quella di conferma sta oltre la projection del
    // chiamante, e nessuno schema pubblicato la nomina.
    assert_eq!(
        batch.num_columns(),
        1,
        "la colonna di conferma non deve comparire nello schema"
    );
    let field = batch.schema().field(0).clone();
    assert_eq!(field.name(), "frame");
    let metadata = field.metadata();
    assert_eq!(
        metadata
            .get(plenora_database_core::protocol::GEOMETRY_SRID)
            .map(String::as_str),
        Some("3003"),
        "il CRS pubblicato deve essere quello dichiarato per l'ingresso, non \
         quello che il motore rende: {metadata:?}"
    );
    assert_eq!(
        metadata
            .get(plenora_database_core::protocol::GEOARROW_EXTENSION_NAME)
            .map(String::as_str),
        Some("geoarrow.wkb"),
        "{metadata:?}"
    );
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<plenora_database_core::arrow::array::BinaryArray>()
        .expect("una geometria e WKB binario");
    for row in 0..values.len() {
        let wkb = values.value(row);
        assert!(!wkb.is_empty(), "WKB vuoto alla riga {row}");
        // Il primo byte e l'ordine dei byte del WKB: 0 big endian, 1 little.
        // Se qui ci fosse il formato interno del prodotto, i primi quattro
        // byte sarebbero l'SRID e questo assert cadrebbe.
        assert!(wkb[0] <= 1, "byte iniziale {} alla riga {row}", wkb[0]);
    }
    while stream
        .next_batch(&cancellation)
        .await
        .expect("chiusura dello stream")
        .is_some()
    {}

    // --- La seconda meta: la riga che smentisce ferma la lettura -----------
    let mut stream = provider
        .query(
            &live_secret(),
            &plan(None),
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("il prepare non puo sapere cosa contengono le righe");
    let mut refusal = None;
    loop {
        match stream.next_batch(&cancellation).await {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(error) => {
                refusal = Some(error);
                break;
            }
        }
    }
    let refusal = refusal.expect(
        "una riga in 3857 sotto una dichiarazione 3003 deve fermare la lettura: \
         pubblicare 3003 su quella geometria sarebbe un CRS falso",
    );
    assert_eq!(refusal.category, plenora_database_core::ErrorCategory::Crs);
    assert!(
        refusal.message.contains("3003") && refusal.message.contains("3857"),
        "il messaggio deve nominare i due SRID: {}",
        refusal.message
    );
    // Un SRID e un identificatore di registro, non una cella: la geometria che
    // l'ha portato non compare nel messaggio.
    assert!(
        !refusal.message.contains("POLYGON") && !refusal.message.contains("1500000"),
        "il messaggio pubblico porta il payload: {}",
        refusal.message
    );

    let mut cleanup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("cleanup");
    cleanup
        .connection_mut()
        .expect("connessione di cleanup")
        .query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .await
        .ok();
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
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
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "_v12_spatial".to_owned(),
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

/// Il predicato spatial in WHERE, letto fino in fondo.
///
/// Fino al 24/10 il test chiedeva `ST_Intersects(shape, ST_GeomFromWKB(?))`
/// e controllava soltanto che `query` restituisse uno stream: il parametro
/// nudo ha SRID 0, e `MySQL` rifiuta il confronto con la colonna SRID 4326
/// (3033) solo leggendo, quindi il test passava su una query che non
/// funziona. Ora: il parametro nudo si rifiuta compilando (`unsupported`,
/// prima di ogni effetto, vedi `MYSQL_SPATIAL_PARAMETER_REFUSAL`); il
/// predicato fra due colonne si legge per intero, con un punto asimmetrico
/// (1 2) e il suo scambio (2 1), cosi che una riga con le coordinate invertite
/// non possa passare per buona.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_v12_query_spatial_predicate_intersects_in_filter() {
    use plenora_database_core::arrow::array::Int64Array;
    use plenora_database_core::plan::{ObjectRef, SortDirection};
    use plenora_database_core::provider::{ParameterBag, ParameterValue};
    use plenora_database_core::query::{
        ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
        SpatialFunction,
    };
    use plenora_database_core::resource::ResourceBudget;
    use plenora_database_core::{ErrorPhase, RemoteEffect};
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
                 shape GEOMETRY NOT NULL SRID 4326, \
                 probe GEOMETRY NOT NULL SRID 4326) ENGINE=InnoDB",
            )
            .await
            .expect("create");
        // Le coordinate in ordine longitudine-latitudine, come il prodotto le
        // scambia (GEO-001): senza l'opzione `MySQL` leggerebbe 4326 in
        // ordine latitudine-longitudine. La riga 2 ha la forma con gli assi
        // scambiati rispetto alla sonda e non deve intersecarla.
        setup
            .connection_mut()
            .unwrap()
            .query_drop(
                "INSERT INTO _v12_spatial_pred VALUES \
                 (1, ST_GeomFromText('POINT(1 2)', 4326, 'axis-order=long-lat'), \
                     ST_GeomFromText('POINT(1 2)', 4326, 'axis-order=long-lat')), \
                 (2, ST_GeomFromText('POINT(2 1)', 4326, 'axis-order=long-lat'), \
                     ST_GeomFromText('POINT(1 2)', 4326, 'axis-order=long-lat'))",
            )
            .await
            .expect("seed");
    }

    let column = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    };
    let query = |second: QueryExpression| QueryOperation {
        declared_crs: Vec::new(),
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dataflow_test".to_owned()),
                object: "_v12_spatial_pred".to_owned(),
            },
            alias: None,
        }),
        derived_source: None,
        projection: vec![QueryProjection {
            expression: column("id"),
            alias: None,
        }],
        joins: Vec::new(),
        filter: Some(QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![column("shape"), second],
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
        row_limit: None,
        row_offset: None,
        locking: None,
    };

    // Il parametro nudo: WKB POINT(1 2) senza CRS. Rifiutato prima di ogni
    // effetto, non al primo batch.
    let wkb_point_1_2: Vec<u8> = vec![
        0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
    ];
    let bag = ParameterBag::new(BTreeMap::from([(
        "probe".to_owned(),
        ParameterValue::Bytes(wkb_point_1_2),
    )]));
    let refused = provider
        .query(
            &live_secret(),
            &query(QueryExpression::Parameter {
                name: "probe".to_owned(),
            }),
            &bag,
            &budget,
            &cancellation,
        )
        .await;
    let Err(error) = refused else {
        panic!("parametro geometry senza SRID accettato");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported, "{error:?}");
    assert_eq!(error.phase, ErrorPhase::Prepare, "{error:?}");
    assert_eq!(error.remote_effect, RemoteEffect::None, "{error:?}");
    assert!(
        error
            .message
            .contains(plenora_database_sql::MYSQL_SPATIAL_PARAMETER_REFUSAL),
        "{error:?}"
    );

    // Colonna contro colonna: lo stream si legge fino alla fine e porta la
    // sola riga 1.
    let mut stream = provider
        .query(
            &live_secret(),
            &query(column("probe")),
            &ParameterBag::new(BTreeMap::new()),
            &budget,
            &cancellation,
        )
        .await
        .expect("predicato spatial fra colonne");
    let mut ids = Vec::new();
    while let Some(batch) = stream
        .next_batch(&cancellation)
        .await
        .expect("batch del predicato spatial")
    {
        let column = batch
            .column_by_name("id")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
            .expect("id Int64");
        ids.extend(column.iter().map(|id| id.expect("id non nullo")));
    }
    assert_eq!(ids, vec![1]);

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
//  NativeQueryPolicy
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
    for sql in ["SELECT 1--1; COMMIT", "SELECT '--'; COMMIT"] {
        let error = tx
            .execute(&Statement::new(sql), &cancellation)
            .await
            .expect_err("statement nascosto deve essere rifiutato prima dell'I/O");
        assert_eq!(error.category, ErrorCategory::InvalidPlan);
    }
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

// ==================== Contratto Replace / TruncateInsert ==================
//
// Replace su MySQL e DELETE FROM + bulk insert nella stessa transazione
// InnoDB. Questi test provano le due meta del contratto: cosa sopravvive
// (identita della tabella, indici, FK, trigger, check, default,
// AUTO_INCREMENT) e cosa succede quando la scrittura si rompe a meta.

/// Una PRIMARY KEY su un tipo non indicizzabile si ferma prima della rete.
///
/// `Utf8` diventa `TEXT`, e `TEXT` in chiave senza lunghezza di prefisso e
/// l'errore 1170 del server. Il piano lo rifiuta prima: il test verifica le
/// due meta, che il provider non emetta nulla e che il motore — su ogni
/// versione della matrice — rifiuti davvero la stessa DDL.
#[tokio::test]
async fn live_v12_write_create_primary_key_on_text_is_refused_before_the_network() {
    let table = "_v12_create_pk_text";
    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, _batch) = scalar_batch(&[1], &["prima"]);
    let mut operation = write_op_scalar(
        "dataflow_test",
        table,
        plenora_database_core::plan::WriteMode::Create,
    );
    operation.keys = vec!["label".to_owned()];

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
        panic!("chiave primaria su TEXT accettata");
    };
    assert_eq!(
        error.category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::None
    );
    assert_eq!(
        error.retry,
        plenora_database_core::RetryDisposition::Never,
        "un piano invalido non migliora con un retry"
    );

    assert_eq!(
        replace_fixture_scalar(&format!(
            "SELECT IFNULL(MAX(TABLE_NAME), '-') FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'"
        ))
        .await,
        "-",
        "il rifiuto ha comunque creato la tabella"
    );

    // Il vincolo non e nostro: e del motore, su questa versione.
    let server = fixture_exec_error(&format!(
        "CREATE TABLE dataflow_test.{table} (label TEXT NOT NULL, PRIMARY KEY (label))"
    ))
    .await;
    assert!(
        server.contains("1170"),
        "atteso 1170 dal server, ricevuto: {server}"
    );

    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;
}

/// Esegue una lista di statement sulla connessione di servizio.
async fn replace_fixture_exec(statements: &[String]) {
    let cancellation = CancellationToken::new();
    let mut setup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("setup fixture replace");
    for statement in statements {
        setup
            .connection_mut()
            .expect("connessione fixture")
            .query_drop(statement.as_str())
            .await
            .unwrap_or_else(|error| panic!("statement fixture fallito: {statement} -> {error}"));
    }
}

/// L'errore che il server restituisce per uno statement che deve fallire.
///
/// Serve a provare che un rifiuto del piano corrisponde a un rifiuto reale
/// del motore, e non a un vincolo inventato dal provider.
async fn fixture_exec_error(statement: &str) -> String {
    let cancellation = CancellationToken::new();
    let mut setup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("setup fixture errore");
    let error = setup
        .connection_mut()
        .expect("connessione fixture")
        .query_drop(statement)
        .await
        .expect_err("statement accettato dal server");
    error.to_string()
}

/// Un singolo valore testuale letto dalla connessione di servizio.
async fn replace_fixture_scalar(sql: &str) -> String {
    let cancellation = CancellationToken::new();
    let mut setup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("setup query replace");
    setup
        .connection_mut()
        .expect("connessione query")
        .query_first::<String, _>(sql)
        .await
        .unwrap_or_else(|error| panic!("query fixture fallita: {sql} -> {error}"))
        .unwrap_or_default()
}

/// Impronta di tutto cio che Replace deve conservare.
///
/// `SHOW CREATE TABLE` copre colonne, default, indici, unique, foreign key,
/// check, engine e charset; `CREATE_TIME` e il contatore `AUTO_INCREMENT`
/// distinguono una tabella conservata da una ricreata — una tabella nuova
/// riparte da `AUTO_INCREMENT = 1` e da un altro istante di creazione.
async fn replace_metadata_digest(table: &str) -> String {
    let create = replace_fixture_scalar(&format!(
        "SELECT CONCAT_WS('|', \
            (SELECT CREATE_TIME FROM information_schema.TABLES \
              WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'), \
            (SELECT IFNULL(AUTO_INCREMENT, 0) FROM information_schema.TABLES \
              WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'), \
            (SELECT IFNULL(GROUP_CONCAT(TRIGGER_NAME ORDER BY TRIGGER_NAME), '-') \
               FROM information_schema.TRIGGERS \
              WHERE EVENT_OBJECT_SCHEMA = 'dataflow_test' \
                AND EVENT_OBJECT_TABLE = '{table}'), \
            (SELECT IFNULL(GROUP_CONCAT(CONCAT(CONSTRAINT_NAME, ':', CONSTRAINT_TYPE) \
                                        ORDER BY CONSTRAINT_NAME), '-') \
               FROM information_schema.TABLE_CONSTRAINTS \
              WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'))"
    ))
    .await;
    let ddl = replace_fixture_scalar(&format!(
        "SELECT GROUP_CONCAT(CONCAT_WS(':', COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, \
                                       IFNULL(COLUMN_DEFAULT, '-'), EXTRA) \
                             ORDER BY ORDINAL_POSITION) \
           FROM information_schema.COLUMNS \
          WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'"
    ))
    .await;
    let indexes = replace_fixture_scalar(&format!(
        "SELECT IFNULL(GROUP_CONCAT(CONCAT_WS(':', INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, \
                                              NON_UNIQUE) \
                                    ORDER BY INDEX_NAME, SEQ_IN_INDEX), '-') \
           FROM information_schema.STATISTICS \
          WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'"
    ))
    .await;
    format!("{create}||{ddl}||{indexes}")
}

async fn replace_rows_digest(table: &str) -> String {
    replace_fixture_scalar(&format!(
        "SELECT IFNULL(GROUP_CONCAT(CONCAT_WS(':', id, label) ORDER BY id), '') FROM {table}"
    ))
    .await
}

/// Il target del contratto Replace nasce in `docker/mysql/init`, non qui: il
/// trigger richiede privilegi che l'utente della fixture non ha con il binlog
/// attivo. I test resettano le righe, mai la definizione.
const REPLACE_TARGET: &str = "replace_target";
const REPLACE_AUDIT: &str = "replace_audit";
/// Nome che la fixture non crea mai: serve alla prova del target assente.
const REPLACE_MISSING: &str = "replace_target_assente";

/// Riporta la fixture allo stato noto: tre righe nel target e contatore del
/// trigger azzerato. Nessun DDL — la definizione della tabella e il soggetto
/// del test, non un suo effetto collaterale.
async fn replace_fixture_reset() {
    replace_fixture_exec(&[
        format!("DELETE FROM {REPLACE_TARGET}"),
        format!(
            "INSERT INTO {REPLACE_TARGET} (id, label, parent_id) \
             VALUES (1, 'prima', 1), (2, 'seconda', 2), (3, 'terza', 1)"
        ),
        format!("UPDATE {REPLACE_AUDIT} SET n = 0"),
    ])
    .await;
}

/// Schema Arrow allineato al target della fixture Replace.
fn replace_batch(
    rows: &[(i64, &str, i64)],
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
        Field::new("parent_id", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch replace");
    (schema, batch)
}

/// Stream che consegna un batch e poi fallisce: il DELETE e gia passato e
/// alcune righe nuove sono gia scritte quando arriva l'errore.
struct FailingBatchesStream {
    schema: plenora_database_core::arrow::SchemaRef,
    first: Option<plenora_database_core::arrow::RecordBatch>,
}

impl plenora_database_core::provider::BatchStream for FailingBatchesStream {
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
        let next = self.first.take().map_or_else(
            || {
                Err(plenora_database_core::DatabaseError::invalid_plan(
                    "sorgente interrotta a meta stream",
                ))
            },
            |batch| Ok(Some(batch)),
        );
        Box::pin(async move { next })
    }
}

/// Stream che cancella il token mentre consegna il batch: la scrittura si
/// trova cancellata con il target gia svuotato dentro la transazione.
struct CancellingBatchesStream {
    schema: plenora_database_core::arrow::SchemaRef,
    batch: Option<plenora_database_core::arrow::RecordBatch>,
    token: CancellationToken,
}

impl plenora_database_core::provider::BatchStream for CancellingBatchesStream {
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
        let next = self.batch.take();
        if next.is_some() {
            self.token.cancel();
        }
        Box::pin(async move { Ok(next) })
    }
}

/// Replace scrive nel target esistente: la tabella e la stessa, con gli
/// stessi indici, unique, foreign key, check, default, trigger e contatore
/// `AUTO_INCREMENT`. Con `staging + RENAME` l'impronta cambierebbe in ogni
/// sua parte.
#[tokio::test]
async fn live_v12_write_replace_preserves_table_identity_and_metadata() {
    replace_fixture_reset().await;
    let before = replace_metadata_digest(REPLACE_TARGET).await;
    assert!(
        before.contains("replace_target_label_uk"),
        "fixture senza unique index: {before}"
    );
    assert!(
        before.contains("replace_target_audit"),
        "fixture senza trigger: {before}"
    );
    assert!(
        before.contains("etichetta-default"),
        "fixture senza default: {before}"
    );
    assert!(
        before.contains("replace_target_fk:FOREIGN KEY"),
        "fixture senza foreign key: {before}"
    );

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, batch) = replace_batch(&[(1, "nuova-a", 1), (2, "nuova-b", 2)]);
    let operation = write_op_scalar(
        "dataflow_test",
        REPLACE_TARGET,
        plenora_database_core::plan::WriteMode::Replace,
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
        .expect("prepare replace");
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(BatchesStream {
                schema,
                batches: std::collections::VecDeque::from(vec![batch]),
                declared: 2,
            }),
            &budget,
            &cancellation,
        )
        .await
        .expect("write replace");

    assert_eq!(
        outcome.status,
        plenora_database_core::outcome::WriteStatus::Committed
    );
    assert_eq!(outcome.rows.confirmed, 2);
    assert_eq!(
        replace_metadata_digest(REPLACE_TARGET).await,
        before,
        "metadata del target mutato"
    );
    assert_eq!(
        replace_rows_digest(REPLACE_TARGET).await,
        "1:nuova-a,2:nuova-b"
    );
    assert_eq!(
        replace_fixture_scalar(&format!("SELECT CAST(n AS CHAR) FROM {REPLACE_AUDIT}")).await,
        "2",
        "il trigger non ha visto le righe nuove"
    );
}

/// Il target di Replace deve esistere: non e un `Create` mascherato.
#[tokio::test]
async fn live_v12_write_replace_on_a_missing_target_is_not_found() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, _batch) = replace_batch(&[(1, "nuova-a", 1)]);
    let operation = write_op_scalar(
        "dataflow_test",
        REPLACE_MISSING,
        plenora_database_core::plan::WriteMode::Replace,
    );

    let Err(error) = provider
        .prepare_write(&live_secret(), &operation, schema, &budget, &cancellation)
        .await
    else {
        panic!("Replace su target assente accettato");
    };
    assert_eq!(error.category, ErrorCategory::NotFound);
}

/// Un errore a meta stream arriva quando il DELETE e gia passato: il rollback
/// deve riportare esattamente le righe di prima, non un target vuoto.
#[tokio::test]
async fn live_v12_write_replace_restores_the_previous_rows_when_the_stream_fails() {
    replace_fixture_reset().await;
    let before = replace_rows_digest(REPLACE_TARGET).await;
    assert_eq!(before, "1:prima,2:seconda,3:terza");

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, batch) = replace_batch(&[(10, "nuova-a", 1)]);
    let operation = write_op_scalar(
        "dataflow_test",
        REPLACE_TARGET,
        plenora_database_core::plan::WriteMode::Replace,
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
        .expect("prepare replace");
    let Err(error) = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(FailingBatchesStream {
                schema,
                first: Some(batch),
            }),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("stream interrotto accettato come successo");
    };
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(
        replace_rows_digest(REPLACE_TARGET).await,
        before,
        "righe precedenti non ripristinate"
    );
    assert_eq!(
        replace_fixture_scalar(&format!("SELECT CAST(n AS CHAR) FROM {REPLACE_AUDIT}")).await,
        "0",
        "effetto del trigger sopravvissuto al rollback"
    );
}

/// La cancellazione arriva dopo il DELETE: dentro la transazione il target e
/// gia vuoto, e solo il rollback lo riporta allo stato precedente.
#[tokio::test]
async fn live_v12_write_replace_restores_the_previous_rows_on_cancellation() {
    replace_fixture_reset().await;
    let before = replace_rows_digest(REPLACE_TARGET).await;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, batch) = replace_batch(&[(10, "nuova-a", 1)]);
    let operation = write_op_scalar(
        "dataflow_test",
        REPLACE_TARGET,
        plenora_database_core::plan::WriteMode::Replace,
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
        .expect("prepare replace");
    let Err(error) = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(CancellingBatchesStream {
                schema,
                batch: Some(batch),
                token: cancellation.clone(),
            }),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("cancellazione accettata come successo");
    };
    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert_eq!(
        replace_rows_digest(REPLACE_TARGET).await,
        before,
        "righe precedenti non ripristinate dopo cancellazione"
    );
}

/// Fail-closed: `TruncateInsert` resta non qualificata su `MySQL` — `TRUNCATE`
/// e DDL con commit implicito — e il rifiuto arriva in compile, prima del
/// checkout dal pool e quindi prima di qualunque effetto remoto. Il test lo
/// prova due volte: nessuna riga toccata e nessuna sessione aperta.
#[tokio::test]
async fn live_v12_write_truncate_insert_rejected_without_remote_effects() {
    replace_fixture_reset().await;
    let rows_before = replace_rows_digest(REPLACE_TARGET).await;
    let metadata_before = replace_metadata_digest(REPLACE_TARGET).await;
    let probe_cancellation = CancellationToken::new();
    let mut probe = MysqlSession::open(&live_config(), &probe_cancellation)
        .await
        .expect("sessione di misura");
    let connections_before: Option<(String, u64)> = probe
        .connection_mut()
        .expect("connessione di misura")
        .query_first("SHOW GLOBAL STATUS LIKE 'Connections'")
        .await
        .expect("contatore connessioni");

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, _batch) = replace_batch(&[(10, "nuova-a", 1)]);
    let operation = write_op_scalar(
        "dataflow_test",
        REPLACE_TARGET,
        plenora_database_core::plan::WriteMode::TruncateInsert,
    );

    let Err(error) = provider
        .prepare_write(&live_secret(), &operation, schema, &budget, &cancellation)
        .await
    else {
        panic!("TruncateInsert MySQL deve essere rifiutato fail-closed");
    };
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Prepare);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::None
    );
    assert!(
        error.message.contains("WriteMode::Replace"),
        "il rifiuto deve indicare l'alternativa qualificata: {}",
        error.message
    );

    // Il contatore globale delle connessioni non si e mosso: il rifiuto e
    // arrivato prima del checkout dal pool, quindi prima di qualunque
    // effetto remoto. La lettura usa la stessa sessione della misura
    // iniziale, cosi non e la misura stessa a spostare il contatore.
    let connections_after: Option<(String, u64)> = probe
        .connection_mut()
        .expect("connessione di misura")
        .query_first("SHOW GLOBAL STATUS LIKE 'Connections'")
        .await
        .expect("contatore connessioni");
    assert_eq!(
        connections_after, connections_before,
        "il rifiuto ha aperto una connessione al server"
    );
    assert_eq!(replace_rows_digest(REPLACE_TARGET).await, rows_before);
    assert_eq!(
        replace_metadata_digest(REPLACE_TARGET).await,
        metadata_before
    );
}

/// Un `Create` che fallisce dopo la DDL lascia la tabella sul server: il DDL
/// `MySQL` fa commit implicito e il `ROLLBACK` annulla solo le righe. L'errore
/// deve dichiarare l'effetto parziale invece di affermare che nulla e
/// successo — un retry cieco troverebbe `Conflict` su un target che il
/// chiamante crede assente.
#[tokio::test]
async fn live_v12_write_create_failure_leaves_the_table_and_reports_partial() {
    let table = "_v12_create_residue";
    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, batch) = scalar_batch(&[1], &["prima"]);
    let operation = write_op_scalar(
        "dataflow_test",
        table,
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
        .expect("prepare create");
    let Err(error) = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(FailingBatchesStream {
                schema,
                first: Some(batch),
            }),
            &budget,
            &cancellation,
        )
        .await
    else {
        panic!("stream interrotto accettato come successo");
    };

    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::Partial,
        "un Create fallito non e RolledBack: la tabella resta"
    );
    assert_eq!(
        error.retry,
        plenora_database_core::RetryDisposition::RequiresRecovery
    );
    assert!(
        error.message.contains("commit implicito"),
        "il messaggio deve nominare il residuo: {}",
        error.message
    );

    // La tabella esiste davvero ed e vuota: le righe sono tornate indietro,
    // lo schema no.
    assert_eq!(
        replace_fixture_scalar(&format!(
            "SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.TABLES \n WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'"
        ))
        .await,
        "1",
        "la tabella creata dalla DDL doveva sopravvivere al rollback"
    );
    assert_eq!(
        replace_fixture_scalar(&format!("SELECT CAST(COUNT(*) AS CHAR) FROM {table}")).await,
        "0",
        "le righe non sono state annullate"
    );

    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;
}

/// `execute_ddl` usa il text protocol: il prepared protocol rifiuta parte del
/// DDL con l'errore 1295, e uno statement che il server accetta non deve
/// fallire per la scelta del canale.
#[tokio::test]
async fn live_v12_execute_ddl_accepts_statements_the_prepared_protocol_refuses() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let table = "_v12_ddl_text_protocol";
    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;

    provider
        .execute_ddl(
            &live_secret(),
            &format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
            &cancellation,
        )
        .await
        .expect("CREATE via text protocol");

    // `ANALYZE TABLE` produce un result set: `exec_drop` del prepared
    // protocol lo tollera, ma la coppia CREATE/ANALYZE prova che il canale
    // regge sia DDL puro sia DDL con output.
    provider
        .execute_ddl(
            &live_secret(),
            &format!("ANALYZE TABLE {table}"),
            &cancellation,
        )
        .await
        .expect("ANALYZE via text protocol");

    assert_eq!(
        replace_fixture_scalar(&format!(
            "SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.TABLES \n WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}'"
        ))
        .await,
        "1"
    );

    provider
        .execute_ddl(
            &live_secret(),
            &format!("DROP TABLE {table}"),
            &cancellation,
        )
        .await
        .expect("DROP via text protocol");
}

/// Un DDL interrotto **in volo** non puo dichiarare `RemoteEffect::None`: lo
/// statement puo essersi gia committato e nessun rollback lo annullerebbe.
///
/// Il blocco e deterministico: una transazione aperta su un `SELECT` tiene il
/// metadata lock della tabella, quindi l'`ALTER TABLE` attende invece di
/// completarsi, e il timeout di operazione scatta mentre il DDL e sul server.
/// `exec_control` risponde `Unknown`; `exec_write`, che il percorso usava
/// prima, avrebbe risposto `None`.
#[tokio::test]
async fn live_v12_execute_ddl_in_flight_interruption_reports_an_unknown_remote_effect() {
    let table = "_v12_ddl_mdl_lock";
    replace_fixture_exec(&[
        format!("DROP TABLE IF EXISTS {table}"),
        format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
        format!("INSERT INTO {table} VALUES (1)"),
    ])
    .await;

    // Il lock: una transazione che legge la tabella e resta aperta.
    let cancellation = CancellationToken::new();
    let mut holder = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("sessione che tiene il metadata lock");
    holder
        .connection_mut()
        .expect("connessione holder")
        .query_drop("BEGIN")
        .await
        .expect("apertura transazione bloccante");
    let _: Vec<i64> = holder
        .connection_mut()
        .expect("connessione holder")
        .query(format!("SELECT id FROM {table}"))
        .await
        .expect("metadata lock acquisito");

    let provider = MysqlProvider::new(
        live_config().with_timeouts(
            std::time::Duration::from_secs(10),
            std::time::Duration::from_millis(500),
            std::time::Duration::from_secs(10),
        ),
        1,
    )
    .expect("provider timeout DDL");
    let Err(error) = provider
        .execute_ddl(
            &live_secret(),
            &format!("ALTER TABLE {table} ADD COLUMN label VARCHAR(16) NULL"),
            &CancellationToken::new(),
        )
        .await
    else {
        panic!("ALTER completata nonostante il metadata lock");
    };

    assert_eq!(error.category, ErrorCategory::Timeout);
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Write);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::Unknown,
        "un DDL autocommit interrotto in volo non e 'nessun effetto'"
    );

    holder
        .connection_mut()
        .expect("connessione holder")
        .query_drop("ROLLBACK")
        .await
        .expect("rilascio del metadata lock");
    drop(holder);
    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;
}

/// Il caso opposto: un token gia cancellato chiude al checkout, prima che una
/// connessione esista. Li `RemoteEffect::None` e la verita — nessuno
/// statement ha raggiunto il server — e il percorso non deve inventare
/// incertezza.
#[tokio::test]
async fn live_v12_execute_ddl_pre_cancellation_reports_no_remote_effect() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    cancellation.cancel();

    let Err(error) = provider
        .execute_ddl(
            &live_secret(),
            "CREATE TABLE _v12_ddl_mai_creata (id BIGINT PRIMARY KEY)",
            &cancellation,
        )
        .await
    else {
        panic!("DDL eseguita con token gia cancellato");
    };
    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Connect);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::None
    );
    assert_eq!(
        replace_fixture_scalar(
            "SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.TABLES \n WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '_v12_ddl_mai_creata'"
        )
        .await,
        "0"
    );
}

/// Verifica che Create con keys dichiari la PRIMARY KEY sulla tabella costruita.
#[tokio::test]
async fn live_v12_write_create_with_keys_declares_the_primary_key() {
    let table = "_v12_create_pk";
    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;

    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let (schema, batch) = scalar_batch(&[1, 2], &["prima", "seconda"]);
    let mut operation = write_op_scalar(
        "dataflow_test",
        table,
        plenora_database_core::plan::WriteMode::Create,
    );
    operation.keys = vec!["id".to_owned()];

    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await
        .expect("prepare create con keys");
    let outcome = provider
        .write(
            &live_secret(),
            prepared,
            Box::new(BatchesStream {
                schema,
                batches: std::collections::VecDeque::from(vec![batch]),
                declared: 2,
            }),
            &budget,
            &cancellation,
        )
        .await
        .expect("write create con keys");
    assert_eq!(
        outcome.status,
        plenora_database_core::outcome::WriteStatus::Committed
    );

    assert_eq!(
        replace_fixture_scalar(&format!(
            "SELECT IFNULL(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX), '-') \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = 'dataflow_test' AND TABLE_NAME = '{table}' \
             AND INDEX_NAME = 'PRIMARY'"
        ))
        .await,
        "id",
        "la tabella creata non ha la PRIMARY KEY dichiarata"
    );

    replace_fixture_exec(&[format!("DROP TABLE IF EXISTS {table}")]).await;
}

// === query_stream: lo streaming di una query dentro la transazione ===
//
// Sei prove, le stesse che il provider PostgreSQL ha da tempo: paginazione,
// esaurimento, parametri, cancellazione a meta, `batch_size` zero, e la
// connessione riusabile dopo un consumo completo.
//
// La settima e la differenza fra i due prodotti, e non ha controparte:
// `PostgreSQL` dichiara un cursore nominato, quindi abbandonare uno stream a
// meta non costa niente — lo chiude il commit. `MySQL` fa scorrere il result
// set sul filo, e un consumo parziale lascia pacchetti in coda: la
// connessione non e piu utilizzabile, e la transazione deve dirlo invece di
// lasciarlo scoprire allo statement successivo.

/// Prepara una tabella con `rows` righe numerate da 1.
async fn seed_stream_table(table: &str, rows: u32) {
    let cancellation = CancellationToken::new();
    let mut setup = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("setup connect");
    let connection = setup.connection_mut().expect("connessione di setup");
    connection
        .query_drop(format!("DROP TABLE IF EXISTS {table}"))
        .await
        .expect("drop");
    connection
        .query_drop(format!(
            "CREATE TABLE {table} (n BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB"
        ))
        .await
        .expect("create");
    // Un `INSERT` solo: mille round-trip renderebbero il test lento senza
    // renderlo piu vero.
    let values = (1..=rows)
        .map(|n| format!("({n})"))
        .collect::<Vec<_>>()
        .join(",");
    connection
        .query_drop(format!("INSERT INTO {table} (n) VALUES {values}"))
        .await
        .expect("seed");
}

fn stream_statement(sql: &str) -> plenora_database_core::transaction::Statement {
    plenora_database_core::transaction::Statement {
        sql: sql.to_owned(),
        params: Vec::new(),
    }
}

#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_query_stream_paginates_result_in_batches() {
    let table = "_stream_paginates";
    seed_stream_table(table, 250).await;
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");

    let statement = stream_statement(&format!("SELECT n FROM {table} ORDER BY n"));
    let mut sizes = Vec::new();
    {
        let mut stream = tx
            .query_stream(&statement, 100, &cancellation)
            .await
            .expect("apre lo stream");
        while let Some(batch) = stream.next_batch(&cancellation).await.expect("batch") {
            sizes.push(batch.len());
        }
    }
    // 250 righe con batch da 100: tre batch, e l'ultimo corto. Il conteggio
    // per batch e non il totale, perche un totale giusto uscirebbe anche da
    // uno stream che consegna tutto in un colpo — cioe da uno stream che non
    // strema.
    assert_eq!(sizes, vec![100, 100, 50]);

    // La connessione e pulita: il result set e stato consumato fino in fondo,
    // quindi la transazione puo ancora committare.
    assert!(tx
        .commit(&cancellation)
        .await
        .expect("commit")
        .is_committed());
}

#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_query_stream_exhausts_at_end() {
    let table = "_stream_exhausts";
    seed_stream_table(table, 3).await;
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");

    let statement = stream_statement(&format!("SELECT n FROM {table} ORDER BY n"));
    {
        let mut stream = tx
            .query_stream(&statement, 10, &cancellation)
            .await
            .expect("apre lo stream");
        let first = stream.next_batch(&cancellation).await.expect("primo batch");
        assert_eq!(first.map(|rows| rows.len()), Some(3));
        // E poi `None`, non un batch vuoto: sono due cose diverse per chi
        // scrive un ciclo, e la seconda lo farebbe girare per sempre.
        assert!(stream
            .next_batch(&cancellation)
            .await
            .expect("fine")
            .is_none());
        assert!(stream
            .next_batch(&cancellation)
            .await
            .expect("fine, di nuovo")
            .is_none());
    }
    assert!(tx
        .commit(&cancellation)
        .await
        .expect("commit")
        .is_committed());
}

#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_query_stream_respects_bound_parameters() {
    let table = "_stream_params";
    seed_stream_table(table, 20).await;
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");

    // Il valore resta **fuori** dal testo: e la stessa regola di ogni altro
    // percorso di questo provider, e uno stream non e un'eccezione.
    let statement = plenora_database_core::transaction::Statement {
        sql: format!("SELECT n FROM {table} WHERE n > ? ORDER BY n"),
        params: vec![plenora_database_core::provider::ParameterValue::I64(17)],
    };
    let mut seen = Vec::new();
    {
        let mut stream = tx
            .query_stream(&statement, 10, &cancellation)
            .await
            .expect("apre lo stream");
        while let Some(batch) = stream.next_batch(&cancellation).await.expect("batch") {
            for row in batch {
                seen.push(format!("{:?}", row.values()[0]));
            }
        }
    }
    assert_eq!(seen.len(), 3, "atteso n in (18, 19, 20): {seen:?}");
    assert!(tx
        .commit(&cancellation)
        .await
        .expect("commit")
        .is_committed());
}

#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_query_stream_zero_batch_size_is_invalid_plan() {
    let table = "_stream_zero";
    seed_stream_table(table, 1).await;
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");

    let statement = stream_statement(&format!("SELECT n FROM {table}"));
    // `expect_err` non si puo usare: il ramo `Ok` porta uno stream, che non
    // e `Debug`. Il `match` dice la stessa cosa e nomina il caso che non deve
    // succedere.
    let Err(error) = tx.query_stream(&statement, 0, &cancellation).await else {
        panic!("un batch da zero righe non avanza mai, e va rifiutato");
    };
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    // Il rifiuto arriva **prima** di aprire il result set, quindi la
    // connessione e intatta e la transazione resta usabile.
    assert!(tx
        .commit(&cancellation)
        .await
        .expect("commit")
        .is_committed());
}

#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_query_stream_cancelled_mid_stream_returns_cancelled() {
    let table = "_stream_cancel";
    seed_stream_table(table, 500).await;
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");

    let statement = stream_statement(&format!("SELECT n FROM {table} ORDER BY n"));
    let interrupted = CancellationToken::new();
    {
        let mut stream = tx
            .query_stream(&statement, 10, &interrupted)
            .await
            .expect("apre lo stream");
        assert!(stream
            .next_batch(&interrupted)
            .await
            .expect("primo batch")
            .is_some());
        interrupted.cancel();
        let error = stream
            .next_batch(&interrupted)
            .await
            .expect_err("dopo la cancellazione il batch successivo e un rifiuto");
        assert_eq!(error.category, ErrorCategory::Cancelled);
        assert_eq!(error.phase, plenora_database_core::ErrorPhase::Read);
    }

    // Il rifiuto non e la fine del test, e la meta. `RemoteEffect::None` dice
    // che la cancellazione di una lettura non ha lasciato niente dietro di se,
    // ed e un'affermazione sulla **transazione**, non sullo stream: se fosse
    // falsa, questa scrittura fallirebbe o non arriverebbe.
    tx.execute(
        &plenora_database_core::transaction::Statement {
            sql: format!("INSERT INTO {table} (n) VALUES (10001)"),
            params: Vec::new(),
        },
        &cancellation,
    )
    .await
    .expect("dopo una lettura cancellata la transazione scrive ancora");
    assert!(tx
        .commit(&cancellation)
        .await
        .expect("commit")
        .is_committed());

    // La rilettura arriva da **un'altra connessione**: che il commit dica
    // `Committed` e cio che il provider crede, e la riga sul server e cio che
    // e successo. Le due si confondono proprio nel caso in cui si vuole
    // distinguerle.
    assert_eq!(stream_row_count(table, 10001).await, 1);
}

/// Uno stream lasciato a meta **non** rompe la transazione.
///
/// `mysql_async` drena il result set pendente prima dello statement
/// successivo. Il pericolo esisteva sul filo e non esisteva nel driver, e la
/// differenza fra le due cose e esattamente quello che una misura serve a
/// stabilire — anche quando cio che si sta deducendo e un guasto e non una
/// capability.
///
/// Il caso non e esotico: un `break` dentro un ciclo e il modo piu comune di
/// consumare uno stream a meta. Con la bandiera, ogni `break` costava la
/// transazione.
#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_query_stream_abandoned_mid_way_leaves_the_transaction_usable() {
    let table = "_stream_abandoned";
    seed_stream_table(table, 500).await;
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .begin_transaction(
            &live_secret(),
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin");

    let statement = stream_statement(&format!("SELECT n FROM {table} ORDER BY n"));
    {
        let mut stream = tx
            .query_stream(&statement, 10, &cancellation)
            .await
            .expect("apre lo stream");
        // Un batch solo su cinquanta, e poi lo stream viene lasciato andare
        // senza che nessuno abbia cancellato niente. E' il caso piu comune —
        // un `break` dentro un ciclo — ed e quello che un test sulla sola
        // cancellazione non coprirebbe.
        assert!(stream
            .next_batch(&cancellation)
            .await
            .expect("primo batch")
            .is_some());
    }

    // La scrittura viene **dopo** l'abbandono e sulla stessa connessione: se i
    // pacchetti non letti fossero rimasti in coda, sarebbe questa a leggerli
    // al posto della propria risposta.
    tx.execute(
        &plenora_database_core::transaction::Statement {
            sql: format!("INSERT INTO {table} (n) VALUES (10002)"),
            params: Vec::new(),
        },
        &cancellation,
    )
    .await
    .expect("dopo uno stream abbandonato la transazione scrive ancora");
    assert!(tx
        .commit(&cancellation)
        .await
        .expect("commit")
        .is_committed());
    assert_eq!(stream_row_count(table, 10002).await, 1);
}

/// Quante righe con quel valore, da una connessione che non e quella del test.
async fn stream_row_count(table: &str, value: i64) -> i64 {
    let cancellation = CancellationToken::new();
    let mut check = MysqlSession::open(&live_config(), &cancellation)
        .await
        .expect("connessione di verifica");
    check
        .connection_mut()
        .expect("connessione di verifica")
        .query_first::<i64, _>(format!("SELECT COUNT(*) FROM {table} WHERE n = {value}"))
        .await
        .expect("conteggio")
        .expect("una riga di conteggio")
}

// Concorrenza e recupero del pool.

/// Misura lettori concorrenti sul pool, verifica i risultati e il
/// rilascio delle connessioni dopo il completamento dei task.
#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_concurrent_readers_share_the_pool_without_mixing_rows() {
    use std::sync::Arc;

    const WORKERS: i64 = 12;
    const ROUNDS: usize = 8;
    const ROWS_PER_WORKER: i64 = 5;

    let table = "_stress_concurrent";
    seed_stream_table(
        table,
        u32::try_from(WORKERS * ROWS_PER_WORKER).expect("righe"),
    )
    .await;

    let provider = Arc::new(MysqlProvider::new(live_config(), 4).expect("provider"));
    let mut tasks = Vec::new();
    for worker in 0..WORKERS {
        let provider = Arc::clone(&provider);
        let table = table.to_owned();
        tasks.push(tokio::spawn(async move {
            let cancellation = CancellationToken::new();
            let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
            // Ogni worker ha la **propria** fetta di id, contigua e disgiunta
            // da quella degli altri: se il pool consegnasse a due lettori la
            // stessa connessione a meta stream, le righe di uno finirebbero
            // nell'altro e l'ordinamento lo direbbe.
            let first = worker * ROWS_PER_WORKER + 1;
            let last = first + ROWS_PER_WORKER - 1;
            let statement = plenora_database_core::transaction::Statement {
                sql: format!("SELECT n FROM {table} WHERE n BETWEEN ? AND ? ORDER BY n"),
                params: vec![
                    plenora_database_core::provider::ParameterValue::I64(first),
                    plenora_database_core::provider::ParameterValue::I64(last),
                ],
            };
            let mut seen = Vec::new();
            for _ in 0..ROUNDS {
                let mut transaction = provider
                    .begin_transaction(
                        &live_secret(),
                        &plenora_database_core::transaction::TransactionOptions::default(),
                        &budget,
                        &cancellation,
                    )
                    .await
                    .expect("begin concorrente");
                {
                    let mut stream = transaction
                        .query_stream(&statement, 2, &cancellation)
                        .await
                        .expect("stream concorrente");
                    seen.clear();
                    while let Some(rows) = stream
                        .next_batch(&cancellation)
                        .await
                        .expect("batch concorrente")
                    {
                        for row in rows {
                            seen.push(format!("{:?}", row.values()[0]));
                        }
                    }
                }
                transaction
                    .commit(&cancellation)
                    .await
                    .expect("commit concorrente");
                assert_eq!(
                    seen.len(),
                    usize::try_from(ROWS_PER_WORKER).expect("righe"),
                    "il worker {worker} ha visto righe che non ha chiesto"
                );
            }
            seen.len()
        }));
    }

    let mut total = 0_usize;
    for task in tasks {
        total += task.await.expect("worker dello stress");
    }
    assert_eq!(
        total,
        usize::try_from(WORKERS * ROWS_PER_WORKER).expect("righe"),
        "l'ultimo giro di ogni worker deve rendere la sua fetta intera"
    );
}

/// Una cancellazione fra dodici lettori non porta via gli altri.
///
/// La prova gemella della precedente, e misura la cosa che un pool sbaglia piu
/// facilmente: una connessione interrotta a meta e restituita al pool senza
/// essere ripulita: il lettore successivo la trova con i pacchetti di qualcun
/// altro in coda.
///
/// Meta dei worker cancella a meta stream, l'altra meta legge fino in fondo.
/// Quelli che leggono devono vedere **la propria fetta intera** — non un
/// batch parziale, non righe altrui — e il provider deve restare usabile dopo.
#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_concurrent_cancellation_does_not_disturb_the_other_readers() {
    use std::sync::Arc;

    const WORKERS: i64 = 12;
    const ROWS_PER_WORKER: i64 = 6;

    let table = "_stress_cancel_mix";
    seed_stream_table(
        table,
        u32::try_from(WORKERS * ROWS_PER_WORKER).expect("righe"),
    )
    .await;

    let provider = Arc::new(MysqlProvider::new(live_config(), 4).expect("provider"));
    let mut tasks = Vec::new();
    for worker in 0..WORKERS {
        let provider = Arc::clone(&provider);
        let table = table.to_owned();
        let interrupts = worker % 2 == 0;
        tasks.push(tokio::spawn(async move {
            let cancellation = CancellationToken::new();
            let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
            let first = worker * ROWS_PER_WORKER + 1;
            let last = first + ROWS_PER_WORKER - 1;
            let statement = plenora_database_core::transaction::Statement {
                sql: format!("SELECT n FROM {table} WHERE n BETWEEN ? AND ? ORDER BY n"),
                params: vec![
                    plenora_database_core::provider::ParameterValue::I64(first),
                    plenora_database_core::provider::ParameterValue::I64(last),
                ],
            };
            let mut transaction = provider
                .begin_transaction(
                    &live_secret(),
                    &plenora_database_core::transaction::TransactionOptions::default(),
                    &budget,
                    &cancellation,
                )
                .await
                .expect("begin concorrente");
            let interrupted = CancellationToken::new();
            let mut seen = 0_usize;
            {
                let token = if interrupts {
                    &interrupted
                } else {
                    &cancellation
                };
                let mut stream = transaction
                    .query_stream(&statement, 2, token)
                    .await
                    .expect("stream concorrente");
                while let Some(rows) = stream.next_batch(token).await.transpose() {
                    match rows {
                        Ok(rows) => {
                            seen += rows.len();
                            if interrupts && seen >= 2 {
                                interrupted.cancel();
                            }
                        }
                        Err(error) => {
                            assert!(interrupts, "il worker {worker} non ha cancellato niente");
                            assert_eq!(error.category, ErrorCategory::Cancelled);
                            break;
                        }
                    }
                }
            }
            // La transazione resta usabile anche dopo una lettura cancellata:
            // il test la verifica anche sotto contesa.
            transaction
                .commit(&cancellation)
                .await
                .expect("commit dopo la cancellazione concorrente");
            (interrupts, seen)
        }));
    }

    for task in tasks {
        let (interrupts, seen) = task.await.expect("worker dello stress");
        if interrupts {
            // Ha smesso presto, ma non prima di aver letto qualcosa.
            assert!(seen >= 2, "un lettore cancellato non ha visto nulla");
        } else {
            assert_eq!(
                seen,
                usize::try_from(ROWS_PER_WORKER).expect("righe"),
                "un lettore non cancellato ha perso righe mentre un altro cancellava"
            );
        }
    }
}

#[tokio::test]
#[ignore = "live: richiede il riferimento MySQL"]
async fn live_caller_query_decode_failure_requires_recovery() {
    let provider = MysqlProvider::new(live_config(), 2).expect("provider");
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut tx = provider
        .open_transaction(
            &live_secret(),
            &plenora_database_core::transaction::TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("begin reale");
    let error = tx
        .query(
            &stream_statement("SELECT CAST(18446744073709551615 AS UNSIGNED) AS n"),
            &cancellation,
        )
        .await
        .expect_err("UInt64 oltre i64");
    assert_eq!(error.category, ErrorCategory::DataMapping);
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Read);
    assert_eq!(
        error.remote_effect,
        plenora_database_core::RemoteEffect::Unknown
    );
    assert_eq!(
        error.retry,
        plenora_database_core::RetryDisposition::RequiresRecovery
    );
    tx.rollback(&cancellation).await.expect("rollback");
}

// --- Le regole W1/W2 e la regola R della scrittura ----------------------
//
// Le regole W1/W2 e la regola R della scrittura su `MySQL`, contro il
// fixture live: la sessione applica le foreign key anche con il valore
// globale spento, Update conta le righe trovate e non soltanto quelle
// cambiate, e un trigger `BEFORE` sull'evento distruttivo rifiuta la mode
// prima di ogni effetto.
//
// Gli ingressi sono quelli della rilettura del 25/10 su 15b57f8 (`MySQL`
// 9.7): con `SET GLOBAL foreign_key_checks = 0` Append committava un orfano;
// Update `(1,'a'),(2,'z')` su `(1,a),(2,b),(3,a)` pubblicava `confirmed 1
// skipped 1` (la chiave 1, presente e invariata, come assente); con un
// `BEFORE UPDATE` che fa `SET NEW.label = OLD.label` pubblicava `confirmed
// 0 skipped 1`.
/// Le prove condividono le tabelle e, per W2, il valore globale.
static SEMANTICA_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SEMANTICA_TABLE: &str = "write_semantica";
const SEMANTICA_PARENT: &str = "write_semantica_padre";

fn semantica_schema(
    mode: plenora_database_core::plan::WriteMode,
) -> plenora_database_core::arrow::SchemaRef {
    use plenora_database_core::protocol;
    let mut fields = vec![plenora_database_core::arrow::schema::Field::new(
        "id",
        plenora_database_core::arrow::schema::DataType::Int64,
        false,
    )];
    if mode != plenora_database_core::plan::WriteMode::DeleteByKeys {
        fields.push(plenora_database_core::arrow::schema::Field::new(
            "label",
            plenora_database_core::arrow::schema::DataType::Utf8,
            true,
        ));
    }
    std::sync::Arc::new(
        plenora_database_core::arrow::schema::Schema::new_with_metadata(
            fields,
            std::collections::HashMap::from([(
                protocol::CONTRACT_VERSION_KEY.to_owned(),
                protocol::CONTRACT_VERSION.to_owned(),
            )]),
        ),
    )
}

fn semantica_batch(
    mode: plenora_database_core::plan::WriteMode,
    values: &[(i64, &str)],
) -> plenora_database_core::arrow::RecordBatch {
    let mut columns: Vec<std::sync::Arc<dyn plenora_database_core::arrow::array::Array>> =
        vec![std::sync::Arc::new(
            plenora_database_core::arrow::array::Int64Array::from(
                values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            ),
        )];
    if mode != plenora_database_core::plan::WriteMode::DeleteByKeys {
        columns.push(std::sync::Arc::new(
            plenora_database_core::arrow::array::StringArray::from(
                values
                    .iter()
                    .map(|(_, label)| Some(*label))
                    .collect::<Vec<_>>(),
            ),
        ));
    }
    plenora_database_core::arrow::RecordBatch::try_new(semantica_schema(mode), columns)
        .expect("batch")
}

fn semantica_operation(
    mode: plenora_database_core::plan::WriteMode,
) -> plenora_database_core::plan::WriteOperation {
    let keyed = matches!(
        mode,
        plenora_database_core::plan::WriteMode::Update
            | plenora_database_core::plan::WriteMode::DeleteByKeys
            | plenora_database_core::plan::WriteMode::Upsert
    );
    plenora_database_core::plan::WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("dataflow_test".to_owned()),
            object: SEMANTICA_TABLE.to_owned(),
        },
        mode,
        mapping_policy: plenora_database_core::loss::MappingPolicy::Strict,
        transaction_profile: plenora_database_core::plan::TransactionProfile::SingleTransaction,
        keys: if keyed {
            vec!["id".to_owned()]
        } else {
            Vec::new()
        },
        update_columns: if mode == plenora_database_core::plan::WriteMode::Update {
            vec!["label".to_owned()]
        } else {
            Vec::new()
        },
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

/// Il prodotto in prova: `MySQL`, o `MariaDB` con `PLENORA_MYSQL_PRODUCT=mariadb`
/// (lo stesso fixture TLS, un'altra porta). Le stesse prove valgono per
/// entrambi: la regola R e il bootstrap sono condivisi.
fn semantica_mariadb() -> bool {
    std::env::var("PLENORA_MYSQL_PRODUCT").is_ok_and(|product| product == "mariadb")
}

async fn semantica_write(
    mode: plenora_database_core::plan::WriteMode,
    values: &[(i64, &str)],
) -> plenora_database_core::Result<plenora_database_core::outcome::WriteOutcome> {
    if semantica_mariadb() {
        let provider = crate::MariadbProvider::new(live_config(), 1).expect("provider");
        return semantica_write_with(&provider, mode, values).await;
    }
    let provider = MysqlProvider::new(live_config(), 1).expect("provider");
    semantica_write_with(&provider, mode, values).await
}

async fn semantica_write_with(
    provider: &dyn Provider,
    mode: plenora_database_core::plan::WriteMode,
    values: &[(i64, &str)],
) -> plenora_database_core::Result<plenora_database_core::outcome::WriteOutcome> {
    let cancellation = CancellationToken::new();
    let budget = write_budget();
    let operation = semantica_operation(mode);
    let prepared = provider
        .prepare_write(
            &live_secret(),
            &operation,
            semantica_schema(mode),
            &budget,
            &cancellation,
        )
        .await?;
    let input = VecBatchStream {
        schema: semantica_schema(mode),
        batches: std::collections::VecDeque::from(vec![semantica_batch(mode, values)]),
    };
    provider
        .write(
            &live_secret(),
            prepared,
            Box::new(input),
            &budget,
            &cancellation,
        )
        .await
}

async fn semantica_admin() -> mysql_async::Conn {
    mysql_async::Conn::new(live_config().driver_opts("MySQL").expect("opzioni"))
        .await
        .expect("connessione di servizio")
}

/// Il target `(1,a),(2,b),(3,a)` e la preparazione dello scenario.
async fn semantica_prepare(connection: &mut mysql_async::Conn, setup: &[&str]) {
    for sql in [
        format!("DROP TABLE IF EXISTS `{SEMANTICA_TABLE}`"),
        format!("DROP TABLE IF EXISTS `{SEMANTICA_PARENT}`"),
        format!("CREATE TABLE `{SEMANTICA_PARENT}` (id BIGINT NOT NULL PRIMARY KEY) ENGINE=InnoDB"),
        format!("INSERT INTO `{SEMANTICA_PARENT}` VALUES (1), (2), (3), (10)"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(64) NULL) \
             ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES (1, 'a'), (2, 'b'), (3, 'a')"),
    ] {
        connection.query_drop(sql).await.expect("preparazione");
    }
    for sql in setup {
        connection
            .query_drop(*sql)
            .await
            .expect("preparazione dello scenario");
    }
}

async fn semantica_rows(connection: &mut mysql_async::Conn) -> Vec<String> {
    connection
        .query::<String, _>(format!(
            "SELECT CONCAT(id, ':', IFNULL(label, '<null>')) FROM `{SEMANTICA_TABLE}` ORDER BY id"
        ))
        .await
        .expect("righe")
}

async fn semantica_cleanup(connection: &mut mysql_async::Conn) {
    for sql in [
        format!("DROP TABLE IF EXISTS `{SEMANTICA_TABLE}`"),
        format!("DROP TABLE IF EXISTS `{SEMANTICA_PARENT}`"),
    ] {
        connection.query_drop(sql).await.expect("pulizia");
    }
}

/// S5: Update conta le righe che trova (`CLIENT_FOUND_ROWS`), anche quando
/// il valore non cambia.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_update_conta_le_righe_trovate_non_soltanto_quelle_cambiate() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    semantica_prepare(&mut connection, &[]).await;
    let written = semantica_write(
        plenora_database_core::plan::WriteMode::Update,
        &[(1, "a"), (2, "z"), (99, "z")],
    )
    .await;
    let after = semantica_rows(&mut connection).await;
    semantica_cleanup(&mut connection).await;
    let counted = written.as_ref().is_ok_and(|outcome| {
        (
            outcome.rows.confirmed,
            outcome.rows.updated,
            outcome.rows.skipped,
        ) == (2, Some(2), 1)
    });
    assert!(
        counted && after == ["1:a", "2:z", "3:a"],
        "{:?}, righe {after:?}",
        written.map(|outcome| outcome.rows)
    );
}

/// La regola R su `MySQL`: un trigger `BEFORE` sull'evento distruttivo della
/// mode (`BEFORE UPDATE` per Update, `BEFORE DELETE` per Replace e
/// `DeleteByKeys`) puo neutralizzare la riga senza un errore (`SET NEW.c =
/// OLD.c`): la mode si rifiuta prima di ogni effetto, come in PostgreSQL. Un
/// `BEFORE INSERT` (la riga e scritta, con i valori che il trigger decide) e
/// i trigger `AFTER` restano ammessi, con i conteggi delle righe scritte.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_regola_r_before_sull_evento_distruttivo_rifiuta() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let neutralize = format!(
        "CREATE TRIGGER `{SEMANTICA_TABLE}_neutro` BEFORE UPDATE ON `{SEMANTICA_TABLE}` \
         FOR EACH ROW SET NEW.label = OLD.label"
    );
    let before_delete = format!(
        "CREATE TRIGGER `{SEMANTICA_TABLE}_nota` BEFORE DELETE ON `{SEMANTICA_TABLE}` \
         FOR EACH ROW SET @plenora_nota = OLD.id"
    );
    let before_insert = format!(
        "CREATE TRIGGER `{SEMANTICA_TABLE}_maiuscolo` BEFORE INSERT ON `{SEMANTICA_TABLE}` \
         FOR EACH ROW SET NEW.label = UPPER(NEW.label)"
    );
    let after_update = format!(
        "CREATE TRIGGER `{SEMANTICA_TABLE}_dopo` AFTER UPDATE ON `{SEMANTICA_TABLE}` \
         FOR EACH ROW SET @plenora_nota = NEW.id"
    );
    let mut difformi = Vec::new();
    for (name, setup, mode, values) in [
        (
            "before-update",
            &neutralize,
            plenora_database_core::plan::WriteMode::Update,
            &[(2, "z")][..],
        ),
        (
            "before-delete-delete",
            &before_delete,
            plenora_database_core::plan::WriteMode::DeleteByKeys,
            &[(2, "z")][..],
        ),
        (
            "before-delete-replace",
            &before_delete,
            plenora_database_core::plan::WriteMode::Replace,
            &[(10, "z")][..],
        ),
    ] {
        semantica_prepare(&mut connection, &[setup.as_str()]).await;
        let before = semantica_rows(&mut connection).await;
        let written = semantica_write(mode, values).await;
        let after = semantica_rows(&mut connection).await;
        let refused = written.as_ref().err().is_some_and(|error| {
            error.category == ErrorCategory::Unsupported
                && error
                    .message
                    .ends_with(plenora_database_core::row_visibility::HIDDEN_BY_TRIGGER)
        });
        if !refused || after != before {
            difformi.push(format!(
                "{name}: {:?}, {before:?} -> {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    for (name, setup, mode, values, expected, confirmed) in [
        (
            "before-insert",
            &before_insert,
            plenora_database_core::plan::WriteMode::Append,
            &[(10, "y"), (11, "z")][..],
            &["1:a", "2:b", "3:a", "10:Y", "11:Z"][..],
            2,
        ),
        (
            "after-update",
            &after_update,
            plenora_database_core::plan::WriteMode::Update,
            &[(2, "z")][..],
            &["1:a", "2:z", "3:a"][..],
            1,
        ),
    ] {
        semantica_prepare(&mut connection, &[setup.as_str()]).await;
        let written = semantica_write(mode, values).await;
        let after = semantica_rows(&mut connection).await;
        if !written
            .as_ref()
            .is_ok_and(|outcome| outcome.rows.confirmed == confirmed)
            || after != expected
        {
            difformi.push(format!(
                "{name}: {:?}, righe {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    semantica_cleanup(&mut connection).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// W2: la sessione della scrittura fissa `foreign_key_checks = 1` e
/// `unique_checks = 1` qualunque sia il valore globale, e su `MariaDB`
/// `check_constraint_checks = 1`. L'account della fixture `MySQL` ha
/// `SYSTEM_VARIABLES_ADMIN` (`docker/mysql/init`) per spegnerli; la prova li
/// riaccende prima di ogni verifica.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e cambia un valore globale"]
async fn live_w2_foreign_key_checks_fissato_anche_con_il_globale_spento() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    semantica_prepare(
        &mut connection,
        &[&format!(
            "ALTER TABLE `{SEMANTICA_TABLE}` ADD CONSTRAINT `{SEMANTICA_TABLE}_padre` FOREIGN KEY (id) \
             REFERENCES `{SEMANTICA_PARENT}` (id)"
        )],
    )
    .await;
    connection
        .query_drop("SET GLOBAL foreign_key_checks = 0, GLOBAL unique_checks = 0")
        .await
        .expect("valore globale spento");
    // 99 non ha un padre.
    let written = semantica_write(
        plenora_database_core::plan::WriteMode::Append,
        &[(10, "y"), (99, "z")],
    )
    .await;
    connection
        .query_drop("SET GLOBAL foreign_key_checks = 1, GLOBAL unique_checks = 1")
        .await
        .expect("valore globale ripristinato");
    let after = semantica_rows(&mut connection).await;
    let mut difformi = Vec::new();
    if written.is_ok() || after != ["1:a", "2:b", "3:a"] {
        difformi.push(format!(
            "foreign_key_checks: {:?}, righe {after:?}",
            written.map(|outcome| outcome.rows)
        ));
    }
    // `sql_select_limit` globale: la sessione del pool lo riporta al massimo.
    connection
        .query_drop("SET GLOBAL sql_select_limit = 1")
        .await
        .expect("limite globale");
    let pool = mysql_async::Pool::new(
        live_config()
            .pooled_driver_opts(1, "MySQL")
            .expect("opzioni del pool"),
    );
    let read = match pool.get_conn().await {
        Ok(mut pooled) => pooled
            .query::<i64, _>(format!("SELECT id FROM `{SEMANTICA_TABLE}`"))
            .await
            .map(|ids| ids.len()),
        Err(error) => Err(error),
    };
    connection
        .query_drop("SET GLOBAL sql_select_limit = DEFAULT")
        .await
        .expect("limite globale ripristinato");
    pool.disconnect().await.expect("pool chiuso");
    if !matches!(read, Ok(3)) {
        difformi.push(format!("sql_select_limit: {read:?}"));
    }
    if semantica_mariadb() {
        semantica_prepare(
            &mut connection,
            &[&format!(
                "ALTER TABLE `{SEMANTICA_TABLE}` ADD CONSTRAINT `{SEMANTICA_TABLE}_corto` CHECK (id < 50)"
            )],
        )
        .await;
        connection
            .query_drop("SET GLOBAL check_constraint_checks = 0")
            .await
            .expect("valore globale spento");
        let written = semantica_write(
            plenora_database_core::plan::WriteMode::Append,
            &[(10, "y"), (99, "z")],
        )
        .await;
        connection
            .query_drop("SET GLOBAL check_constraint_checks = 1")
            .await
            .expect("valore globale ripristinato");
        let after = semantica_rows(&mut connection).await;
        if written.is_ok() || after != ["1:a", "2:b", "3:a"] {
            difformi.push(format!(
                "check_constraint_checks: {:?}, righe {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    semantica_cleanup(&mut connection).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

// ---- Seconda rilettura del 25/10 (su 675e322) ----
//
// Gli ingressi sono quelli verificati live dalla rilettura: `MySQL` 9.7 e
// `MariaDB` 12.3; ciascun esito qui sotto era un `committed` con un
// conteggio o un valore falso.

fn semantica_provider() -> Box<dyn Provider> {
    if semantica_mariadb() {
        Box::new(crate::MariadbProvider::new(live_config(), 1).expect("provider"))
    } else {
        Box::new(MysqlProvider::new(live_config(), 1).expect("provider"))
    }
}

/// Una scrittura con uno schema e un batch qualunque, sul prodotto in prova.
async fn semantica_write_batch(
    operation: &plenora_database_core::plan::WriteOperation,
    batch: plenora_database_core::arrow::RecordBatch,
) -> plenora_database_core::Result<plenora_database_core::outcome::WriteOutcome> {
    let provider = semantica_provider();
    let cancellation = CancellationToken::new();
    let budget = write_budget();
    let schema = batch.schema();
    let prepared = provider
        .prepare_write(
            &live_secret(),
            operation,
            std::sync::Arc::clone(&schema),
            &budget,
            &cancellation,
        )
        .await?;
    provider
        .write(
            &live_secret(),
            prepared,
            Box::new(VecBatchStream {
                schema,
                batches: std::collections::VecDeque::from(vec![batch]),
            }),
            &budget,
            &cancellation,
        )
        .await
}

/// S6: senza `NO_AUTO_VALUE_ON_ZERO` uno 0 scritto in una colonna
/// `AUTO_INCREMENT` diventava il prossimo valore della sequenza, con Append e
/// con Upsert `committed`. Ora resta 0.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_s6_zero_in_auto_increment_resta_zero() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let autoinc = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, \
             label VARCHAR(64) NULL) ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES (1, 'a'), (2, 'b'), (3, 'a')"),
    ];
    let autoinc: Vec<&str> = autoinc.iter().map(String::as_str).collect();
    let mut difformi = Vec::new();
    for mode in [
        plenora_database_core::plan::WriteMode::Append,
        plenora_database_core::plan::WriteMode::Upsert,
    ] {
        semantica_prepare(&mut connection, &autoinc).await;
        let written = semantica_write(mode, &[(0, "zero")]).await;
        let after = semantica_rows(&mut connection).await;
        if written.is_err() || after != ["0:zero", "1:a", "2:b", "3:a"] {
            difformi.push(format!(
                "{mode:?}: {:?}, righe {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    semantica_cleanup(&mut connection).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La regola K: Update e `DeleteByKeys` su chiavi senza un indice unico del
/// target. Con `(1,a),(1,b),(2,c)` e `KEY (id)` non unico, Update di
/// `(1,z),(99,q)` dava `confirmed 2 skipped 0` (due righe toccate dalla 1,
/// la 99 assente), `DeleteByKeys` di `1, 99` lo stesso. Ora si rifiutano
/// prima di ogni effetto (`schema`, come SQL Server).
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_regola_k_chiavi_senza_indice_unico_rifiutano() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let nonunique = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (id BIGINT NOT NULL, label VARCHAR(64) NULL, \
             KEY (id)) ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES (1, 'a'), (1, 'b'), (2, 'c')"),
    ];
    let nonunique: Vec<&str> = nonunique.iter().map(String::as_str).collect();
    let mut difformi = Vec::new();
    for mode in [
        plenora_database_core::plan::WriteMode::Update,
        plenora_database_core::plan::WriteMode::DeleteByKeys,
    ] {
        semantica_prepare(&mut connection, &nonunique).await;
        let written = semantica_write(mode, &[(1, "z"), (99, "q")]).await;
        let after = semantica_rows(&mut connection).await;
        let refused = written.as_ref().err().is_some_and(|error| {
            error.category == ErrorCategory::Schema
                && error
                    .message
                    .ends_with(plenora_database_core::write_keys::KEYS_WITHOUT_UNIQUE_INDEX)
        });
        if !refused || after != ["1:a", "1:b", "2:c"] {
            difformi.push(format!(
                "{mode:?}: {:?}, righe {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    semantica_cleanup(&mut connection).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S7: una chiave ripetuta nell'ingresso. Update di `(1,x),(1,y)` dava
/// `confirmed 1 skipped 1` con il target `1:x` (lo staging e lo `UPDATE ...
/// JOIN` applicano una riga sola), dove PostgreSQL e SQL Server davano
/// `confirmed 2` e `1:y`. La regola ora e una, in ogni provider: si rifiuta.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_chiavi_ripetute_nell_ingresso_rifiutano() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let mut difformi = Vec::new();
    for mode in [
        plenora_database_core::plan::WriteMode::Update,
        plenora_database_core::plan::WriteMode::Upsert,
        plenora_database_core::plan::WriteMode::DeleteByKeys,
    ] {
        semantica_prepare(&mut connection, &[]).await;
        let written = semantica_write(mode, &[(1, "x"), (1, "y")]).await;
        let after = semantica_rows(&mut connection).await;
        let refused = written.as_ref().err().is_some_and(|error| {
            error.category == ErrorCategory::DataMapping
                && error
                    .message
                    .ends_with(plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS)
        });
        if !refused || after != ["1:a", "2:b", "3:a"] {
            difformi.push(format!(
                "{mode:?}: {:?}, righe {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    semantica_cleanup(&mut connection).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Due chiavi diverse come valori e uguali per la collation della colonna
/// (`'a'` e `'A'` sotto una collation `_ci`): il core non le vede ripetute,
/// lo `UPDATE ... JOIN` ne applicava una e contava l'altra `skipped`. Ora le
/// coppie dello staging contro le righe trovate lo scoprono, e la scrittura
/// si rifiuta come per una chiave ripetuta.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_chiavi_uguali_per_la_collation_rifiutano() {
    use plenora_database_core::arrow::array::{Array, StringArray};
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let keyed = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (code VARCHAR(16) NOT NULL PRIMARY KEY, \
             label VARCHAR(64) NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES ('a', 'prima'), ('b', 'seconda')"),
    ];
    let keyed: Vec<&str> = keyed.iter().map(String::as_str).collect();
    semantica_prepare(&mut connection, &keyed).await;
    let schema = std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ],
        std::collections::HashMap::from([(
            plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
            plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ));
    let batch = plenora_database_core::arrow::RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(StringArray::from(vec!["a", "A"])) as std::sync::Arc<dyn Array>,
            std::sync::Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
        ],
    )
    .expect("batch");
    let mut operation = semantica_operation(plenora_database_core::plan::WriteMode::Update);
    operation.keys = vec!["code".to_owned()];
    let written = semantica_write_batch(&operation, batch).await;
    let after = connection
        .query::<String, _>(format!(
            "SELECT CONCAT(code, ':', label) FROM `{SEMANTICA_TABLE}` ORDER BY code"
        ))
        .await
        .expect("righe");
    semantica_cleanup(&mut connection).await;
    let refused = written.as_ref().err().is_some_and(|error| {
        error.category == ErrorCategory::DataMapping
            && error
                .message
                .ends_with(plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS)
    });
    assert!(
        refused && after == ["a:prima", "b:seconda"],
        "{:?}, righe {after:?}",
        written.map(|outcome| outcome.rows)
    );
}

/// La sessione che vede un trigger `BEFORE INSERT` della scrittura: fuso,
/// charset di connessione, `autocommit`, `foreign_key_checks`, `sql_mode` e
/// i byte del valore, con la sessione fresca, con i valori globali cambiati
/// e su una connessione del pool riusata dopo un `SET SESSION` del chiamante
/// (sonda M4 della rilettura).
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e cambia valori globali"]
async fn live_w2_sessione_della_scrittura_fissa_fuso_e_charset() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut admin = semantica_admin().await;
    let capture = [
        format!("ALTER TABLE `{SEMANTICA_TABLE}` MODIFY label VARCHAR(1000) NULL"),
        format!(
            "CREATE TRIGGER `{SEMANTICA_TABLE}_cattura` BEFORE INSERT ON `{SEMANTICA_TABLE}` \
             FOR EACH ROW SET NEW.label = CONCAT_WS('|', @@session.time_zone, \
             @@session.character_set_client, @@session.character_set_connection, \
             @@session.character_set_results, @@session.autocommit + 0, \
             @@session.foreign_key_checks + 0, \
             FIND_IN_SET('STRICT_TRANS_TABLES', @@session.sql_mode) > 0, \
             FIND_IN_SET('NO_AUTO_VALUE_ON_ZERO', @@session.sql_mode) > 0, HEX(NEW.label))"
        ),
    ];
    let capture: Vec<&str> = capture.iter().map(String::as_str).collect();
    let expected = "10:+00:00|utf8mb4|utf8mb4|utf8mb4|1|1|1|1|C3A8";
    let mut difformi = Vec::new();
    let mut check = |name: &str, written: &plenora_database_core::Result<u64>, rows: &[String]| {
        let row = rows.iter().find(|row| row.starts_with("10:"));
        if written.is_err() || row.map(String::as_str) != Some(expected) {
            difformi.push(format!("{name}: {written:?} {row:?}"));
        }
    };
    let append = plenora_database_core::plan::WriteMode::Append;
    semantica_prepare(&mut admin, &capture).await;
    let written = semantica_write(append, &[(10, "\u{e8}")])
        .await
        .map(|outcome| outcome.rows.confirmed);
    check("fresca", &written, &semantica_rows(&mut admin).await);
    admin
        .query_drop(
            "SET GLOBAL time_zone = '+05:00', GLOBAL character_set_client = 'latin1', \
             GLOBAL character_set_connection = 'latin1', GLOBAL character_set_results = 'latin1', \
             GLOBAL collation_connection = 'latin1_swedish_ci'",
        )
        .await
        .expect("globali cambiati");
    semantica_prepare(&mut admin, &capture).await;
    let written = semantica_write(append, &[(10, "\u{e8}")])
        .await
        .map(|outcome| outcome.rows.confirmed);
    check("globali", &written, &semantica_rows(&mut admin).await);
    semantica_prepare(&mut admin, &capture).await;
    let provider = semantica_provider();
    let ddl = provider
        .execute_ddl(
            &live_secret(),
            "SET SESSION time_zone = '+03:00', SESSION sql_mode = '', \
             SESSION foreign_key_checks = 0, NAMES latin1",
            &CancellationToken::new(),
        )
        .await;
    let written = semantica_write_with(provider.as_ref(), append, &[(10, "\u{e8}")])
        .await
        .map(|outcome| outcome.rows.confirmed);
    check(
        &format!("riuso dopo {ddl:?}"),
        &written,
        &semantica_rows(&mut admin).await,
    );
    admin
        .query_drop(
            "SET GLOBAL time_zone = 'SYSTEM', GLOBAL character_set_client = 'utf8mb4', \
             GLOBAL character_set_connection = 'utf8mb4', GLOBAL character_set_results = 'utf8mb4', \
             GLOBAL collation_connection = DEFAULT",
        )
        .await
        .expect("globali ripristinati");
    semantica_cleanup(&mut admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// L'isolamento chiesto a `begin_transaction` e quello della transazione,
/// riletto dentro la transazione.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito"]
async fn live_isolamento_richiesto_e_quello_effettivo() {
    use plenora_database_core::transaction::IsolationLevel;
    let provider = semantica_provider();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let variable = if semantica_mariadb() {
        "@@session.tx_isolation"
    } else {
        "@@session.transaction_isolation"
    };
    let mut difformi = Vec::new();
    for (level, expected) in [
        (IsolationLevel::Serializable, "SERIALIZABLE"),
        (IsolationLevel::ReadCommitted, "READ-COMMITTED"),
        (IsolationLevel::RepeatableRead, "REPEATABLE-READ"),
        (IsolationLevel::ReadUncommitted, "READ-UNCOMMITTED"),
    ] {
        let options = plenora_database_core::transaction::TransactionOptions {
            isolation: Some(level),
            ..plenora_database_core::transaction::TransactionOptions::default()
        };
        let mut tx = provider
            .begin_transaction(&live_secret(), &options, &budget, &cancellation)
            .await
            .expect("begin");
        let rows = tx
            .query(
                &plenora_database_core::transaction::Statement {
                    sql: format!("SELECT {variable}"),
                    params: Vec::new(),
                },
                &cancellation,
            )
            .await
            .expect("lettura");
        let observed = format!("{rows:?}");
        if !observed.contains(expected) {
            difformi.push(format!("{level:?}: {observed}"));
        }
        tx.rollback(&cancellation).await.expect("rollback");
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// `CLIENT_FOUND_ROWS` e lo SQL del chiamante: `execute_conditional_update`
/// confronta le righe che il `WHERE` trova, cambiate o no. Un UPDATE che
/// riscrive lo stesso valore riesce (fino al 25/10 era una modifica
/// concorrente); uno che non trova la versione attesa resta una modifica
/// concorrente.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_update_condizionale_conta_le_righe_trovate() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    semantica_prepare(&mut connection, &[]).await;
    let provider = semantica_provider();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let options = plenora_database_core::transaction::TransactionOptions::default();
    let statement = |label: &str, version: &str| plenora_database_core::transaction::Statement {
        sql: format!("UPDATE `{SEMANTICA_TABLE}` SET label = ? WHERE id = 1 AND label = ?"),
        params: vec![
            plenora_database_core::provider::ParameterValue::String(label.to_owned()),
            plenora_database_core::provider::ParameterValue::String(version.to_owned()),
        ],
    };
    let mut tx = provider
        .begin_transaction(&live_secret(), &options, &budget, &cancellation)
        .await
        .expect("begin");
    let same = statement("a", "a");
    let unchanged = tx
        .execute_conditional_update(
            plenora_database_core::transaction::ConditionalUpdate {
                update: &same,
                key_probe: None,
                expected_affected_rows: 1,
            },
            &cancellation,
        )
        .await;
    let stale = statement("z", "vecchia");
    let concurrent = tx
        .execute_conditional_update(
            plenora_database_core::transaction::ConditionalUpdate {
                update: &stale,
                key_probe: None,
                expected_affected_rows: 1,
            },
            &cancellation,
        )
        .await;
    let affected = tx.execute(&same, &cancellation).await;
    tx.rollback(&cancellation).await.expect("rollback");
    semantica_cleanup(&mut connection).await;
    assert!(unchanged.is_ok(), "{unchanged:?}");
    assert_eq!(
        concurrent.map_err(|error| error.category),
        Err(ErrorCategory::ConcurrentModification)
    );
    assert_eq!(
        affected.ok(),
        Some(1),
        "le righe trovate, non soltanto le cambiate"
    );
}

/// Lo staging di Update confronta le chiavi con la collation del target: con
/// una chiave sotto una collation che distingue le maiuscole, `'a'` e `'A'`
/// sono due righe e Update le aggiorna entrambe, ciascuna con il proprio
/// valore. Con lo staging nella collation predefinita del server il
/// confronto mescolava due collation diverse.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_staging_di_update_confronta_con_la_collation_del_target() {
    use plenora_database_core::arrow::array::{Array, StringArray};
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let keyed = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (code VARCHAR(16) CHARACTER SET utf8mb4 \
             COLLATE {} NOT NULL PRIMARY KEY, label VARCHAR(64) NULL) ENGINE=InnoDB",
            if semantica_mariadb() {
                "utf8mb4_uca1400_as_cs"
            } else {
                "utf8mb4_0900_as_cs"
            }
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES ('a', 'prima'), ('A', 'seconda')"),
    ];
    let keyed: Vec<&str> = keyed.iter().map(String::as_str).collect();
    semantica_prepare(&mut connection, &keyed).await;
    let schema = std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ],
        std::collections::HashMap::from([(
            plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
            plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ));
    let batch = plenora_database_core::arrow::RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(StringArray::from(vec!["a", "A"])) as std::sync::Arc<dyn Array>,
            std::sync::Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
        ],
    )
    .expect("batch");
    let mut operation = semantica_operation(plenora_database_core::plan::WriteMode::Update);
    operation.keys = vec!["code".to_owned()];
    let written = semantica_write_batch(&operation, batch).await;
    let after = connection
        .query::<String, _>(format!(
            "SELECT CONCAT(code, ':', label) FROM `{SEMANTICA_TABLE}` ORDER BY code"
        ))
        .await
        .expect("righe");
    semantica_cleanup(&mut connection).await;
    assert!(
        written
            .as_ref()
            .is_ok_and(|outcome| (outcome.rows.confirmed, outcome.rows.skipped) == (2, 0))
            && after == ["a:x", "A:y"],
        "{:?}, righe {after:?}",
        written.map(|outcome| outcome.rows)
    );
}

// ---- Terza rilettura del 25/10 (su 1b02559) ----

/// La regola P: `abc` con tre spazi verso `VARCHAR(3)` diventava `abc` con
/// il COMMIT, anche con `STRICT_TRANS_TABLES` (il server tronca gli spazi
/// finali con una nota). Ora un testo piu lungo della colonna si rifiuta
/// prima di ogni effetto, in ogni mode che scrive valori.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_p_spazi_finali_oltre_la_lunghezza_rifiutano() {
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let short = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (id BIGINT NOT NULL PRIMARY KEY, \
             label VARCHAR(3) NULL) ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES (1, 'a'), (2, 'b'), (3, 'a')"),
    ];
    let short: Vec<&str> = short.iter().map(String::as_str).collect();
    let mut difformi = Vec::new();
    for (mode, id) in [
        (plenora_database_core::plan::WriteMode::Append, 10),
        (plenora_database_core::plan::WriteMode::Upsert, 1),
        (plenora_database_core::plan::WriteMode::Update, 1),
    ] {
        semantica_prepare(&mut connection, &short).await;
        let written = semantica_write(mode, &[(id, "abc   ")]).await;
        let after = semantica_rows(&mut connection).await;
        let refused = written.as_ref().err().is_some_and(|error| {
            error.category == ErrorCategory::DataMapping
                && error.message.ends_with(crate::write::LOSSY_VALUE)
        });
        if !refused || after != ["1:a", "2:b", "3:a"] {
            difformi.push(format!(
                "{mode:?}: {:?}, righe {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
        // Entro la lunghezza il valore si scrive com'e.
        let written = semantica_write(mode, &[(id, "ab ")]).await;
        if written.is_err() {
            difformi.push(format!("{mode:?} entro la lunghezza: {written:?}"));
        }
    }
    semantica_cleanup(&mut connection).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La regola K2 su `MySQL` e `MariaDB`: un indice `UNIQUE` su un prefisso
/// della chiave prova l'unicita della colonna intera (prefissi unici, valori
/// unici, con la collation della colonna), e Update conferma la riga sola.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_k2_indice_unico_su_un_prefisso_prova_la_chiave() {
    use plenora_database_core::arrow::array::{Array, StringArray};
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let keyed = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (code VARCHAR(10) NOT NULL, label VARCHAR(64) NULL, \
             UNIQUE KEY prefisso (code(2))) ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES ('ab1', 'prima'), ('cd1', 'seconda')"),
    ];
    let keyed: Vec<&str> = keyed.iter().map(String::as_str).collect();
    semantica_prepare(&mut connection, &keyed).await;
    let schema = std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ],
        std::collections::HashMap::from([(
            plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
            plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ));
    let batch = plenora_database_core::arrow::RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(StringArray::from(vec!["ab1", "zz9"])) as std::sync::Arc<dyn Array>,
            std::sync::Arc::new(StringArray::from(vec![Some("x"), Some("y")])),
        ],
    )
    .expect("batch");
    let mut operation = semantica_operation(plenora_database_core::plan::WriteMode::Update);
    operation.keys = vec!["code".to_owned()];
    let written = semantica_write_batch(&operation, batch).await;
    let after = connection
        .query::<String, _>(format!(
            "SELECT CONCAT(code, ':', label) FROM `{SEMANTICA_TABLE}` ORDER BY code"
        ))
        .await
        .expect("righe");
    semantica_cleanup(&mut connection).await;
    assert!(
        written
            .as_ref()
            .is_ok_and(|outcome| (outcome.rows.confirmed, outcome.rows.skipped) == (1, 1))
            && after == ["ab1:x", "cd1:seconda"],
        "{:?}, righe {after:?}",
        written.map(|outcome| outcome.rows)
    );
}

// ---- Quarta rilettura del 25/10 (su 04029d4) ----

/// La regola K2 nell'Upsert: `ON DUPLICATE KEY UPDATE` scatta su un indice
/// `UNIQUE` di un **prefisso** della chiave quando due valori diversi hanno
/// lo stesso prefisso. Con `UNIQUE (code(2))` e la riga `('ab1', 'prima')`,
/// l'Upsert di `('ab2', 'nuovo')` aggiornava `ab1` in `nuovo` e non scriveva
/// `ab2`, con il COMMIT. Ora l'Upsert si rifiuta prima di ogni effetto, come
/// con un secondo indice unico (`id` PRIMARY KEY e `UNIQUE (email)`).
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
#[allow(clippy::too_many_lines)]
async fn live_s6_upsert_rifiuta_un_indice_unico_che_non_e_la_chiave() {
    use plenora_database_core::arrow::array::{Array, Int64Array, StringArray};
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let metadata = std::collections::HashMap::from([(
        plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
        plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
    )]);
    let mut difformi = Vec::new();

    // Un prefisso della chiave.
    let prefisso = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (code VARCHAR(10) NOT NULL, label VARCHAR(64) NULL, \
             UNIQUE KEY prefisso (code(2))) ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES ('ab1', 'prima')"),
    ];
    let prefisso: Vec<&str> = prefisso.iter().map(String::as_str).collect();
    semantica_prepare(&mut connection, &prefisso).await;
    let schema = std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ],
        metadata.clone(),
    ));
    let batch = plenora_database_core::arrow::RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(StringArray::from(vec!["ab2"])) as std::sync::Arc<dyn Array>,
            std::sync::Arc::new(StringArray::from(vec![Some("nuovo")])),
        ],
    )
    .expect("batch");
    let mut operation = semantica_operation(plenora_database_core::plan::WriteMode::Upsert);
    operation.keys = vec!["code".to_owned()];
    let written = semantica_write_batch(&operation, batch).await;
    let after = connection
        .query::<String, _>(format!(
            "SELECT CONCAT(code, ':', label) FROM `{SEMANTICA_TABLE}` ORDER BY code"
        ))
        .await
        .expect("righe");
    if !written.as_ref().err().is_some_and(|error| {
        error.category == ErrorCategory::Schema
            && error
                .message
                .ends_with(plenora_database_core::write_keys::UPSERT_WITH_OTHER_UNIQUE_INDEX)
    }) || after != ["ab1:prima"]
    {
        difformi.push(format!(
            "prefisso: {:?}, righe {after:?}",
            written.map(|outcome| outcome.rows)
        ));
    }

    // Un secondo indice unico, su un'altra colonna.
    let secondo = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (id BIGINT NOT NULL PRIMARY KEY, \
             email VARCHAR(64) NOT NULL, UNIQUE KEY posta (email)) ENGINE=InnoDB"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES (1, 'a@x')"),
    ];
    let secondo: Vec<&str> = secondo.iter().map(String::as_str).collect();
    semantica_prepare(&mut connection, &secondo).await;
    let schema = std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("email", DataType::Utf8, false),
        ],
        metadata,
    ));
    let batch = plenora_database_core::arrow::RecordBatch::try_new(
        schema,
        vec![
            std::sync::Arc::new(Int64Array::from(vec![2])) as std::sync::Arc<dyn Array>,
            std::sync::Arc::new(StringArray::from(vec!["a@x"])),
        ],
    )
    .expect("batch");
    let operation = semantica_operation(plenora_database_core::plan::WriteMode::Upsert);
    let written = semantica_write_batch(&operation, batch).await;
    let after = connection
        .query::<String, _>(format!(
            "SELECT CONCAT(id, ':', email) FROM `{SEMANTICA_TABLE}` ORDER BY id"
        ))
        .await
        .expect("righe");
    if !written.as_ref().err().is_some_and(|error| {
        error.category == ErrorCategory::Schema
            && error
                .message
                .ends_with(plenora_database_core::write_keys::UPSERT_WITH_OTHER_UNIQUE_INDEX)
    }) || after != ["1:a@x"]
    {
        difformi.push(format!(
            "secondo indice: {:?}, righe {after:?}",
            written.map(|outcome| outcome.rows)
        ));
    }
    semantica_cleanup(&mut connection).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La regola T su `MySQL` e `MariaDB`: un `ObjectRef` senza schema si
/// risolve una volta, nel database della configurazione, che e quello della
/// sessione (`DATABASE()`, `db_name` all'apertura); describe, lettura e
/// scrittura nominano la stessa tabella, e ogni statement la qualifica.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
async fn live_t_target_senza_schema_si_risolve_nel_database_della_sessione() {
    use plenora_database_core::arrow::array::Int64Array;
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    semantica_prepare(&mut connection, &[]).await;
    let session_database = connection
        .query_first::<Option<String>, _>("SELECT DATABASE()")
        .await
        .expect("DATABASE()")
        .flatten();
    let provider = semantica_provider();
    let cancellation = CancellationToken::new();
    let source = ObjectRef {
        catalog: None,
        schema: None,
        object: SEMANTICA_TABLE.to_owned(),
    };
    let described = provider
        .inspect(
            &live_secret(),
            &Operation::DatabaseDescribeObject {
                source: source.clone(),
            },
            &cancellation,
        )
        .await
        .expect("describe senza schema");
    let mut operation = semantica_operation(plenora_database_core::plan::WriteMode::Append);
    operation.target = source.clone();
    let written = semantica_write_batch(
        &operation,
        semantica_batch(plenora_database_core::plan::WriteMode::Append, &[(7, "t")]),
    )
    .await;
    let budget = write_budget();
    let mut stream = provider
        .read(
            &live_secret(),
            &ReadOperation {
                source,
                projection: vec!["id".to_owned()],
                order_by: Vec::new(),
                row_limit: None,
                row_offset: None,
                filter: None,
                declared_crs: Vec::new(),
            },
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("lettura senza schema");
    let mut ids = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await.expect("batch") {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id");
        ids.extend(column.values().iter().copied());
    }
    drop(stream);
    ids.sort_unstable();
    let after = semantica_rows(&mut connection).await;
    semantica_cleanup(&mut connection).await;
    assert_eq!(session_database.as_deref(), Some("dataflow_test"));
    assert_eq!(described.document["schema"], "dataflow_test");
    assert!(written.is_ok(), "{written:?}");
    assert_eq!(after, ["1:a", "2:b", "3:a", "7:t"]);
    assert_eq!(ids, [1, 2, 3, 7]);
}

/// La regola S e gli spazi finali delle chiavi (sesta rilettura del 25/10):
/// sotto una collation `PAD SPACE` (`utf8mb4_general_ci`) `'x'` e `'x '` sono
/// la stessa voce dell'indice, e l'Upsert di entrambe confermava 2 con una
/// sola riga; il core confronta ora le chiavi senza gli spazi finali e
/// rifiuta. Il `DOUBLE` di `MySQL` conserva `-0.0` (quello di `MariaDB` lo
/// salva come `0`, e la regola S lo rifiuta) e il testo conserva il carattere
/// NUL; `CHAR`, `BINARY`, `ENUM` e `SET` non sono tipi scrivibili.
#[tokio::test]
#[ignore = "richiede MySQL live esplicito e muta tabelle di prova"]
#[allow(clippy::too_many_lines)]
async fn live_s_spazi_finali_delle_chiavi_e_meno_zero() {
    use plenora_database_core::arrow::array::{Array, Float64Array, StringArray};
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    let _serial = SEMANTICA_SERIAL.lock().await;
    let mut connection = semantica_admin().await;
    let setup = [
        format!("DROP TABLE `{SEMANTICA_TABLE}`"),
        format!(
            "CREATE TABLE `{SEMANTICA_TABLE}` (code VARCHAR(16) NOT NULL PRIMARY KEY, \
             label VARCHAR(64) NULL, d DOUBLE NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 \
             COLLATE utf8mb4_general_ci"
        ),
        format!("INSERT INTO `{SEMANTICA_TABLE}` VALUES ('x', 'prima', NULL)"),
    ];
    let setup: Vec<&str> = setup.iter().map(String::as_str).collect();
    semantica_prepare(&mut connection, &setup).await;
    let metadata = std::collections::HashMap::from([(
        plenora_database_core::protocol::CONTRACT_VERSION_KEY.to_owned(),
        plenora_database_core::protocol::CONTRACT_VERSION.to_owned(),
    )]);
    let schema = std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ],
        metadata.clone(),
    ));
    let batch = |codes: Vec<&str>, labels: Vec<&str>| {
        plenora_database_core::arrow::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![
                std::sync::Arc::new(StringArray::from(codes)) as std::sync::Arc<dyn Array>,
                std::sync::Arc::new(StringArray::from(labels)),
            ],
        )
        .expect("batch")
    };
    let mut difformi = Vec::new();
    for mode in [
        plenora_database_core::plan::WriteMode::Upsert,
        plenora_database_core::plan::WriteMode::Update,
    ] {
        let mut operation = semantica_operation(mode);
        operation.keys = vec!["code".to_owned()];
        operation.update_columns = Vec::new();
        let written =
            semantica_write_batch(&operation, batch(vec!["x", "x "], vec!["uno", "due"])).await;
        let refused = written.as_ref().err().is_some_and(|error| {
            error.category == ErrorCategory::DataMapping
                && error
                    .message
                    .ends_with(plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS)
        });
        if !refused {
            difformi.push(format!(
                "{mode:?}: {:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    // Il NUL e -0.0 si conservano.
    let append = semantica_operation(plenora_database_core::plan::WriteMode::Append);
    if let Err(error) = semantica_write_batch(&append, batch(vec!["n"], vec!["a\0b"])).await {
        difformi.push(format!("NUL: {error:?}"));
    }
    let floats = std::sync::Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("d", DataType::Float64, true),
        ],
        metadata,
    ));
    let zero = plenora_database_core::arrow::RecordBatch::try_new(
        floats,
        vec![
            std::sync::Arc::new(StringArray::from(vec!["z"])) as std::sync::Arc<dyn Array>,
            std::sync::Arc::new(Float64Array::from(vec![-0.0])),
        ],
    )
    .expect("batch");
    // `MariaDB` salva -0.0 come 0 (anche da uno statement preparato): la
    // regola S lo rifiuta. `MySQL` lo conserva.
    let written = semantica_write_batch(&append, zero).await;
    let zero_row = if semantica_mariadb() {
        let refused = written.as_ref().is_err_and(|error| {
            error.category == ErrorCategory::DataMapping
                && error
                    .message
                    .contains(plenora_database_core::store_rewrite::REWRITTEN_VALUE)
        });
        if !refused {
            difformi.push(format!(
                "-0.0 MariaDB: {:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
        None
    } else {
        if let Err(error) = written {
            difformi.push(format!("-0.0: {error:?}"));
        }
        Some("z:-:-0")
    };
    let after = connection
        .query::<String, _>(format!(
            "SELECT CONCAT(code, ':', IFNULL(label, '-'), ':', IFNULL(CONCAT(d), '-')) \
             FROM `{SEMANTICA_TABLE}` ORDER BY code"
        ))
        .await
        .expect("righe");
    semantica_cleanup(&mut connection).await;
    let expected: Vec<&str> = ["n:a\0b:-", "x:prima:-"]
        .into_iter()
        .chain(zero_row)
        .collect();
    if after != expected {
        difformi.push(format!("righe {after:?}"));
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

// ---------------------------------------------------------------------------
// La regola A su `MySQL` e `MariaDB`, live
// (`plenora_database_core::write_admission`), e la regola K sotto la
// collation della colonna (`crate::write::KeyCollationGuard`).
//
// - [`live_sonde_scritture_server`]: la sonda. Misura ogni chiave
//   d'ammissione di [`specs`] con il percorso di scrittura della libreria,
//   scrive il referto (`PLENORA_SONDE_SCRITTURE_REFERTO`) e verifica che il
//   server dia ancora il fixture: e l'oracolo di regressione. La esegue
//   `scripts/sonde_scritture_server.py`, per `MySQL` e (con
//   `PLENORA_MYSQL_PRODUCT=mariadb`) per `MariaDB`.
// - I difetti della settima rilettura indipendente del 25/10 (su 092c859,
//   `Strict`, `committed` e `LossReport` vuoto): `DOUBLE(M,D)` e
//   `FLOAT(M,D)` che arrotondano e perdono `-0`; l'Upsert di `'a'` e `'A'`
//   su una chiave `varchar` sotto una collation `_ai_ci`, che contava 2.
//
// La connessione e quella della suite live (la stessa configurazione), con la CA
// privata obbligatoria: in CI la CA la genera, per ogni container usa e getta,
// `scripts/sonde_scritture_server.py`.

enum RegolaAProduct {
    Mysql(MysqlProvider),
    Mariadb(MariadbProvider),
}

struct RegolaAHooks {
    product: RegolaAProduct,
    secret: SecretString,
    database: String,
}

impl RegolaAHooks {
    fn new() -> Self {
        // La sonda apre centinaia di sessioni: tempi larghi, perche un host
        // carico non diventi una misura mancata.
        let config = live_config().with_timeouts(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(120),
            std::time::Duration::from_secs(60),
        );
        let database = config.database().to_owned();
        let product = if semantica_mariadb() {
            RegolaAProduct::Mariadb(MariadbProvider::new(config, 2).expect("provider MariaDB"))
        } else {
            RegolaAProduct::Mysql(MysqlProvider::new(config, 2).expect("provider MySQL"))
        };
        Self {
            product,
            secret: live_secret(),
            database,
        }
    }

    const fn inner(&self) -> &MysqlProvider {
        match &self.product {
            RegolaAProduct::Mysql(provider) => provider,
            RegolaAProduct::Mariadb(provider) => &provider.0,
        }
    }

    fn kind(&self) -> ProviderKind {
        self.provider().kind()
    }

    fn profile(&self) -> &'static dyn crate::profile::ProductProfile {
        if self.kind() == ProviderKind::Mariadb {
            &crate::profile::MARIADB_PROFILE
        } else {
            &crate::profile::MYSQL_PROFILE
        }
    }

    async fn server(&self) -> crate::write_admission::ServerFacts {
        let cancellation = CancellationToken::new();
        let pool = self.inner().pool_for(&self.secret).expect("pool");
        let mut session = pool.checkout(&cancellation).await.expect("sessione");
        crate::write_admission::server_facts(&mut session, &cancellation)
            .await
            .expect("versione del server")
    }

    async fn admin(&self) -> mysql_async::Conn {
        mysql_async::Conn::new(
            live_config()
                .driver_opts(self.profile().product())
                .expect("opzioni"),
        )
        .await
        .expect("connessione di servizio")
    }
}

impl ProbeHooks for RegolaAHooks {
    fn provider(&self) -> &dyn Provider {
        match &self.product {
            RegolaAProduct::Mysql(provider) => provider,
            RegolaAProduct::Mariadb(provider) => provider,
        }
    }

    fn secret(&self) -> &SecretString {
        &self.secret
    }

    fn schema(&self) -> Option<String> {
        Some(self.database.clone())
    }

    fn columns(&self) -> (&'static str, &'static str) {
        ("ID", "V")
    }

    fn create_sql(&self, natives: &[String]) -> Vec<String> {
        let columns = natives
            .iter()
            .enumerate()
            .map(|(index, native)| format!("V{index} {native}"))
            .collect::<Vec<_>>()
            .join(", ");
        vec![format!(
            "CREATE TABLE {PROBE_TABLE} (ID BIGINT NOT NULL PRIMARY KEY, {columns}) \
             ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        )]
    }

    fn drop_sql(&self) -> String {
        format!("DROP TABLE IF EXISTS {PROBE_TABLE}")
    }

    fn truncate_sql(&self) -> String {
        format!("TRUNCATE TABLE {PROBE_TABLE}")
    }

    fn admission_keys<'a>(
        &'a self,
        columns: &'a [(String, DataType)],
    ) -> ProviderFuture<'a, Vec<(AdmissionKey, TargetDimensions)>> {
        Box::pin(async move {
            let cancellation = CancellationToken::new();
            let pool = self.inner().pool_for(&self.secret)?;
            let mut session = pool.checkout(&cancellation).await?;
            let target = crate::catalog::describe_object_with_profile(
                &mut session,
                &self.database,
                PROBE_TABLE,
                self.profile(),
                &cancellation,
            )
            .await?;
            let json = crate::write_admission::json_valid_columns(
                &mut session,
                &self.database,
                PROBE_TABLE,
                &cancellation,
            )
            .await?;
            drop(session);
            let facts = crate::write_admission::target_facts(&target, &json);
            Ok(columns
                .iter()
                .map(|(name, arrow)| {
                    let value = facts
                        .iter()
                        .find(|column| &column.name == name)
                        .expect("colonna della sonda");
                    crate::write_admission::admission_key(arrow, value)
                })
                .collect())
        })
    }
}

/// Le lunghezze massime misurate: la tabella della sonda ha anche `ID
/// BIGINT`, e la riga di `InnoDB` sta in 65535 byte.
const VARCHAR_MAX_UTF8MB4: u64 = 16_380;
const VARCHAR_MAX_LATIN1: u64 = 65_000;
const VARBINARY_MAX: u64 = 65_000;

/// Le chiavi che `MySQL` e `MariaDB` ammettono in scrittura: ogni altra coppia
/// resta rifiutata (mondo chiuso). L'ordine tiene separate le colonne lunghe
/// (due `varchar` lunghi nella stessa tabella supererebbero la riga).
#[allow(clippy::too_many_lines)]
fn regola_a_specs(kind: ProviderKind) -> Vec<ProbeSpec> {
    let mut specs = vec![ProbeSpec::new("TINYINT(1)", DataType::Boolean)];
    specs.push(ProbeSpec::new("TINYINT", DataType::Int8));
    for native in ["TINYINT UNSIGNED", "TINYINT ZEROFILL"] {
        specs.push(ProbeSpec::new(native, DataType::UInt8));
    }
    specs.push(ProbeSpec::new("SMALLINT", DataType::Int16));
    for native in ["SMALLINT UNSIGNED", "SMALLINT ZEROFILL"] {
        specs.push(ProbeSpec::new(native, DataType::UInt16));
    }
    for native in ["MEDIUMINT", "INT"] {
        specs.push(ProbeSpec::new(native, DataType::Int32));
    }
    for native in [
        "MEDIUMINT UNSIGNED",
        "INT UNSIGNED",
        "MEDIUMINT ZEROFILL",
        "INT ZEROFILL",
    ] {
        specs.push(ProbeSpec::new(native, DataType::UInt32));
    }
    specs.push(ProbeSpec::new("BIGINT", DataType::Int64));
    for native in ["BIGINT UNSIGNED", "BIGINT ZEROFILL"] {
        specs.push(ProbeSpec::new(native, DataType::UInt64));
    }
    // `FLOAT(M,D)` e `DOUBLE(M,D)` su rappresentanti: ogni altra coppia
    // `(M,D)` resta non misurata, quindi rifiutata.
    for native in [
        "FLOAT",
        "FLOAT UNSIGNED",
        "FLOAT(7,3)",
        "FLOAT(10,2)",
        "FLOAT(12,0)",
    ] {
        specs.push(ProbeSpec::new(native, DataType::Float32));
    }
    for native in [
        "DOUBLE",
        "DOUBLE UNSIGNED",
        "DOUBLE(10,2)",
        "DOUBLE(16,4)",
        "DOUBLE(22,0)",
        "DOUBLE(255,30)",
    ] {
        specs.push(ProbeSpec::new(native, DataType::Float64));
    }
    for precision in 1..=38_u8 {
        for scale in 0..=precision.min(30) {
            specs.push(ProbeSpec::new(
                format!("DECIMAL({precision},{scale})"),
                DataType::Decimal128(precision, i8::try_from(scale).expect("scala")),
            ));
        }
    }
    specs.push(ProbeSpec::new("DATE", DataType::Date32));
    for precision in 0..=6 {
        specs.push(ProbeSpec::new(
            format!("DATETIME({precision})"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ));
    }
    for precision in 0..=6 {
        specs.push(ProbeSpec::new(
            format!("TIMESTAMP({precision}) NULL"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ));
    }
    let varchar = |length: u64, charset: &str| {
        ProbeSpec::new(
            format!("VARCHAR({length}) CHARACTER SET {charset}"),
            DataType::Utf8,
        )
        .with_dimension(length)
    };
    specs.push(varchar(1, "utf8mb4"));
    specs.push(varchar(1, "latin1"));
    specs.push(varchar(10, "utf8mb4"));
    specs.push(varchar(10, "latin1"));
    specs.push(ProbeSpec::new(
        "TINYTEXT CHARACTER SET utf8mb4",
        DataType::Utf8,
    ));
    specs.push(varchar(VARCHAR_MAX_UTF8MB4, "utf8mb4"));
    specs.push(ProbeSpec::new("TEXT CHARACTER SET utf8mb4", DataType::Utf8));
    specs.push(varchar(VARCHAR_MAX_LATIN1, "latin1"));
    for native in [
        "MEDIUMTEXT CHARACTER SET utf8mb4",
        "LONGTEXT CHARACTER SET utf8mb4",
        "TINYTEXT CHARACTER SET latin1",
        "TEXT CHARACTER SET latin1",
        "MEDIUMTEXT CHARACTER SET latin1",
        "LONGTEXT CHARACTER SET latin1",
    ] {
        specs.push(ProbeSpec::new(native, DataType::Utf8));
    }
    if kind == ProviderKind::Mariadb {
        // Il tipo `JSON` di `MariaDB`: un `LONGTEXT` con `CHECK (json_valid)`.
        specs.push(ProbeSpec::new("JSON", DataType::Utf8).document());
    }
    for length in [1_u64, 10] {
        specs.push(
            ProbeSpec::new(format!("VARBINARY({length})"), DataType::Binary).with_dimension(length),
        );
    }
    specs.push(ProbeSpec::new("TINYBLOB", DataType::Binary));
    specs.push(
        ProbeSpec::new(format!("VARBINARY({VARBINARY_MAX})"), DataType::Binary)
            .with_dimension(VARBINARY_MAX),
    );
    for native in ["BLOB", "MEDIUMBLOB", "LONGBLOB"] {
        specs.push(ProbeSpec::new(native, DataType::Binary));
    }
    specs
}

#[tokio::test]
#[ignore = "richiede MySQL o MariaDB live esplicito"]
async fn live_sonde_scritture_server() {
    let hooks = RegolaAHooks::new();
    let server = hooks.server().await;
    let kind = hooks.kind();
    let measured = write_probe::measure_server(
        &hooks,
        &regola_a_specs(kind),
        &server.major,
        &server.version,
    )
    .await;
    write_probe::report_and_check(&measured, crate::write_admission::fixture(kind));
}

struct RegolaABatches {
    schema: SchemaRef,
    queue: VecDeque<RecordBatch>,
    rows: u64,
}

impl BatchStream for RegolaABatches {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(async move { Ok(self.queue.pop_front()) })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.rows)
    }
}

async fn regola_a_ddl(hooks: &RegolaAHooks, sql: &str) {
    hooks
        .provider()
        .execute_ddl(&hooks.secret, sql, &CancellationToken::new())
        .await
        .unwrap_or_else(|error| panic!("DDL: {error:?}"));
}

async fn regola_a_drop_table(hooks: &RegolaAHooks, table: &str) {
    let _ = hooks
        .provider()
        .execute_ddl(
            &hooks.secret,
            &format!("DROP TABLE IF EXISTS {table}"),
            &CancellationToken::new(),
        )
        .await;
}

fn regola_a_operation(
    hooks: &RegolaAHooks,
    table: &str,
    mode: WriteMode,
    keys: &[&str],
    policy: MappingPolicy,
) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(hooks.database.clone()),
            object: table.to_owned(),
        },
        mode,
        mapping_policy: policy,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn regola_a_write(
    hooks: &RegolaAHooks,
    operation: &WriteOperation,
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
) -> Result<WriteOutcome, DatabaseError> {
    let schema = contract_schema(fields).expect("schema");
    let batch = RecordBatch::try_new(schema, columns).expect("batch");
    let rows = batch.num_rows() as u64;
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = hooks
        .provider()
        .prepare_write(
            &hooks.secret,
            operation,
            batch.schema(),
            &budget,
            &cancellation,
        )
        .await?;
    assert!(
        prepared.loss_report.losses.is_empty(),
        "Strict: nessuna perdita dichiarata"
    );
    hooks
        .provider()
        .write(
            &hooks.secret,
            prepared,
            Box::new(RegolaABatches {
                schema: batch.schema(),
                rows,
                queue: VecDeque::from([batch]),
            }),
            &budget,
            &cancellation,
        )
        .await
}

/// Append `Strict` di `(ID, V)`.
async fn regola_a_append(
    hooks: &RegolaAHooks,
    table: &str,
    value: ArrayRef,
) -> Result<WriteOutcome, DatabaseError> {
    let rows = value.len();
    let ids = Int64Array::from((0..i64::try_from(rows).expect("righe")).collect::<Vec<_>>());
    regola_a_write(
        hooks,
        &regola_a_operation(hooks, table, WriteMode::Append, &[], MappingPolicy::Strict),
        vec![
            Field::new("ID", DataType::Int64, false),
            Field::new("V", value.data_type().clone(), true),
        ],
        vec![Arc::new(ids), value],
    )
    .await
}

fn regola_a_assert_rewritten(result: Result<WriteOutcome, DatabaseError>) {
    let error = result.expect_err("un valore riscritto al salvataggio non si scrive con Strict");
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert!(
        error
            .message
            .contains(plenora_database_core::store_rewrite::REWRITTEN_VALUE),
        "{error:?}"
    );
}

async fn regola_a_count(hooks: &RegolaAHooks, table: &str) -> u64 {
    let mut connection = hooks.admin().await;
    let rows: Option<u64> = connection
        .query_first(format!("SELECT COUNT(*) FROM {table}"))
        .await
        .expect("conteggio");
    drop(connection);
    rows.expect("una riga")
}

#[tokio::test]
#[ignore = "richiede MySQL o MariaDB live esplicito"]
async fn live_a_double_m_d_non_arrotonda_in_silenzio() {
    let hooks = RegolaAHooks::new();
    let table = "PLN_A_DOUBLE_MD";
    regola_a_drop_table(&hooks, table).await;
    regola_a_ddl(
        &hooks,
        &format!(
            "CREATE TABLE {table} (ID BIGINT NOT NULL PRIMARY KEY, V DOUBLE(10,2) NULL) \
             ENGINE=InnoDB"
        ),
    )
    .await;
    // Su 092c859: `committed`, `LossReport` vuoto, `1.234` riletto `1.23` e
    // `-0` riletto `0`.
    for value in [1.234_f64, -0.0, 1.0 / 3.0] {
        regola_a_assert_rewritten(
            regola_a_append(&hooks, table, Arc::new(Float64Array::from(vec![value]))).await,
        );
    }
    // Con le altre politiche la scrittura non e qualificata su questo
    // provider (`validate_operation`): rifiutata, mai `committed`.
    let compatible = regola_a_write(
        &hooks,
        &regola_a_operation(
            &hooks,
            table,
            WriteMode::Append,
            &[],
            MappingPolicy::Compatible,
        ),
        vec![
            Field::new("ID", DataType::Int64, false),
            Field::new("V", DataType::Float64, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![9])),
            Arc::new(Float64Array::from(vec![1.234])),
        ],
    )
    .await
    .expect_err("Compatible non e qualificata");
    assert_eq!(compatible.category, ErrorCategory::Unsupported);
    assert_eq!(
        regola_a_count(&hooks, table).await,
        0,
        "nessuna riga scritta"
    );
    regola_a_drop_table(&hooks, table).await;

    let table = "PLN_A_FLOAT_MD";
    regola_a_drop_table(&hooks, table).await;
    regola_a_ddl(
        &hooks,
        &format!(
            "CREATE TABLE {table} (ID BIGINT NOT NULL PRIMARY KEY, V FLOAT(7,3) NULL) \
             ENGINE=InnoDB"
        ),
    )
    .await;
    for value in [1.234_567_f32, -0.0] {
        regola_a_assert_rewritten(
            regola_a_append(&hooks, table, Arc::new(Float32Array::from(vec![value]))).await,
        );
    }
    assert_eq!(
        regola_a_count(&hooks, table).await,
        0,
        "nessuna riga scritta"
    );
    regola_a_drop_table(&hooks, table).await;

    // Una scala non misurata non si ammette: la coppia non e provata.
    let table = "PLN_A_DOUBLE_NM";
    regola_a_drop_table(&hooks, table).await;
    regola_a_ddl(
        &hooks,
        &format!(
            "CREATE TABLE {table} (ID BIGINT NOT NULL PRIMARY KEY, V DOUBLE(11,3) NULL) \
             ENGINE=InnoDB"
        ),
    )
    .await;
    let error = regola_a_append(&hooks, table, Arc::new(Float64Array::from(vec![1.5])))
        .await
        .expect_err("coppia non misurata");
    assert_eq!(error.category, ErrorCategory::Unsupported, "{error:?}");
    assert_eq!(error.phase, plenora_database_core::ErrorPhase::Prepare);
    regola_a_drop_table(&hooks, table).await;
}

#[tokio::test]
#[ignore = "richiede MySQL o MariaDB live esplicito"]
async fn live_a_datetime_3_non_arrotonda_in_silenzio() {
    let hooks = RegolaAHooks::new();
    let table = "PLN_A_DATETIME3";
    regola_a_drop_table(&hooks, table).await;
    regola_a_ddl(
        &hooks,
        &format!(
            "CREATE TABLE {table} (ID BIGINT NOT NULL PRIMARY KEY, V DATETIME(3) NULL) \
             ENGINE=InnoDB"
        ),
    )
    .await;
    // 2026-01-01T00:00:00.123456: `DATETIME(3)` lo salverebbe come .123.
    let micro = 1_767_225_600_123_456_i64;
    regola_a_assert_rewritten(
        regola_a_append(
            &hooks,
            table,
            Arc::new(TimestampMicrosecondArray::from(vec![micro])),
        )
        .await,
    );
    let outcome = regola_a_append(
        &hooks,
        table,
        Arc::new(TimestampMicrosecondArray::from(vec![micro - 456])),
    )
    .await
    .expect("un istante al millisecondo si scrive");
    assert_eq!(outcome.status, WriteStatus::Committed);
    regola_a_drop_table(&hooks, table).await;
}

/// L'Upsert e `DeleteByKeys` di chiavi che il target confronta uguali.
#[allow(clippy::too_many_lines)]
async fn regola_a_collation_case(hooks: &RegolaAHooks, collation: Option<&str>) -> Vec<String> {
    let table = "PLN_K_COLLATION";
    regola_a_drop_table(hooks, table).await;
    let collate = collation.map_or_else(String::new, |name| format!(" COLLATE {name}"));
    regola_a_ddl(
        hooks,
        &format!(
            "CREATE TABLE {table} (code VARCHAR(16) CHARACTER SET utf8mb4{collate} NOT NULL \
             PRIMARY KEY, label VARCHAR(16) NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
        ),
    )
    .await;
    regola_a_ddl(hooks, &format!("INSERT INTO {table} VALUES ('z', 'prima')")).await;
    let mut difformi = Vec::new();
    let fields = || {
        vec![
            Field::new("code", DataType::Utf8, false),
            Field::new("label", DataType::Utf8, true),
        ]
    };
    for codes in [["a", "A"], ["a", "\u{e0}"]] {
        let written = regola_a_write(
            hooks,
            &regola_a_operation(
                hooks,
                table,
                WriteMode::Upsert,
                &["code"],
                MappingPolicy::Strict,
            ),
            fields(),
            vec![
                Arc::new(StringArray::from(codes.to_vec())),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .await;
        let refused = written.as_ref().err().is_some_and(|error| {
            error.category == ErrorCategory::DataMapping
                && error.remote_effect == RemoteEffect::RolledBack
                && error
                    .message
                    .ends_with(plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS)
        });
        // `'a'` e `'à'` sono la stessa voce sotto `_ai_ci` e `general_ci`.
        if !refused {
            difformi.push(format!(
                "Upsert {codes:?} {collation:?}: {:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    let deleted = regola_a_write(
        hooks,
        &regola_a_operation(
            hooks,
            table,
            WriteMode::DeleteByKeys,
            &["code"],
            MappingPolicy::Strict,
        ),
        vec![Field::new("code", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["z", "Z"]))],
    )
    .await;
    let refused = deleted.as_ref().err().is_some_and(|error| {
        error.category == ErrorCategory::DataMapping
            && error.remote_effect == RemoteEffect::RolledBack
            && error
                .message
                .ends_with(plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS)
    });
    if !refused {
        difformi.push(format!(
            "DeleteByKeys {collation:?}: {:?}",
            deleted.map(|outcome| outcome.rows)
        ));
    }
    if regola_a_count(hooks, table).await != 1 {
        difformi.push(format!("{collation:?}: il target e cambiato"));
    }
    // Chiavi distinte anche per la collation: si scrivono.
    let written = regola_a_write(
        hooks,
        &regola_a_operation(
            hooks,
            table,
            WriteMode::Upsert,
            &["code"],
            MappingPolicy::Strict,
        ),
        fields(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "Z"])),
            Arc::new(StringArray::from(vec!["x", "y", "dopo"])),
        ],
    )
    .await;
    match written {
        Ok(outcome) if outcome.status == WriteStatus::Committed && outcome.rows.confirmed == 3 => {}
        other => difformi.push(format!(
            "Upsert distinte {collation:?}: {:?}",
            other.map(|outcome| outcome.rows)
        )),
    }
    regola_a_drop_table(hooks, table).await;
    difformi
}

#[tokio::test]
#[ignore = "richiede MySQL o MariaDB live esplicito"]
async fn live_k_chiavi_uguali_per_la_collation_si_rifiutano() {
    let hooks = RegolaAHooks::new();
    let mut difformi = Vec::new();
    // La collation predefinita di `utf8mb4` (`utf8mb4_0900_ai_ci` su `MySQL`
    // 8+, `utf8mb4_uca1400_ai_ci` su `MariaDB` 11.8+, `utf8mb4_general_ci`
    // su `MariaDB` 10.11) e `utf8mb4_general_ci`.
    difformi.extend(regola_a_collation_case(&hooks, None).await);
    difformi.extend(regola_a_collation_case(&hooks, Some("utf8mb4_general_ci")).await);
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

#[tokio::test]
#[ignore = "richiede MySQL o MariaDB live esplicito"]
async fn live_k_una_chiave_text_sotto_collation_non_binaria_si_rifiuta() {
    let hooks = RegolaAHooks::new();
    let table = "PLN_K_TEXT";
    regola_a_drop_table(&hooks, table).await;
    regola_a_ddl(
        &hooks,
        &format!(
            "CREATE TABLE {table} (code TEXT CHARACTER SET utf8mb4 NOT NULL, \
             UNIQUE KEY (code(8))) ENGINE=InnoDB"
        ),
    )
    .await;
    let deleted = regola_a_write(
        &hooks,
        &regola_a_operation(
            &hooks,
            table,
            WriteMode::DeleteByKeys,
            &["code"],
            MappingPolicy::Strict,
        ),
        vec![Field::new("code", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["a"]))],
    )
    .await
    .expect_err("la chiave non si confronta sotto la collation");
    assert_eq!(deleted.category, ErrorCategory::Unsupported, "{deleted:?}");
    assert_eq!(deleted.message, crate::write::UNCOMPARABLE_COLLATION_KEY);
    regola_a_drop_table(&hooks, table).await;
}

#[tokio::test]
#[ignore = "richiede MySQL o MariaDB live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_a_i_tipi_di_create_sono_quelli_del_catalogo() {
    use plenora_database_core::arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Int16Array, Int32Array, Int8Array,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    let hooks = RegolaAHooks::new();
    let table = "PLN_A_CREATE";
    regola_a_drop_table(&hooks, table).await;
    let fields = vec![
        Field::new("ID", DataType::Int64, false),
        Field::new("b", DataType::Boolean, true),
        Field::new("i8", DataType::Int8, true),
        Field::new("u8", DataType::UInt8, true),
        Field::new("i16", DataType::Int16, true),
        Field::new("u16", DataType::UInt16, true),
        Field::new("i32", DataType::Int32, true),
        Field::new("u32", DataType::UInt32, true),
        Field::new("u64", DataType::UInt64, true),
        Field::new("f32", DataType::Float32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("t", DataType::Utf8, true),
        Field::new("bin", DataType::Binary, true),
        Field::new("d", DataType::Date32, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        Field::new("dec", DataType::Decimal128(10, 2), true),
    ];
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(Int8Array::from(vec![-1])),
        Arc::new(UInt8Array::from(vec![1])),
        Arc::new(Int16Array::from(vec![-1])),
        Arc::new(UInt16Array::from(vec![1])),
        Arc::new(Int32Array::from(vec![-1])),
        Arc::new(UInt32Array::from(vec![1])),
        Arc::new(UInt64Array::from(vec![1])),
        Arc::new(Float32Array::from(vec![1.5])),
        Arc::new(Float64Array::from(vec![1.5])),
        Arc::new(StringArray::from(vec!["a"])),
        Arc::new(BinaryArray::from(vec![&b"a"[..]])),
        Arc::new(Date32Array::from(vec![1])),
        Arc::new(TimestampMicrosecondArray::from(vec![1_000_000])),
        Arc::new(
            Decimal128Array::from(vec![150])
                .with_precision_and_scale(10, 2)
                .expect("decimal"),
        ),
    ];
    let outcome = regola_a_write(
        &hooks,
        &regola_a_operation(
            &hooks,
            table,
            WriteMode::Create,
            &["ID"],
            MappingPolicy::Strict,
        ),
        fields.clone(),
        columns,
    )
    .await
    .expect("Create con ogni tipo scrivibile");
    assert_eq!(outcome.status, WriteStatus::Committed);
    // La scrittura ha gia confrontato le riscritture dei tipi di Create con
    // quelle del catalogo; qui si confrontano i fatti, tipo per tipo.
    let cancellation = CancellationToken::new();
    let pool = hooks.inner().pool_for(&hooks.secret).expect("pool");
    let mut session = pool.checkout(&cancellation).await.expect("sessione");
    let target = crate::catalog::describe_object_with_profile(
        &mut session,
        &hooks.database,
        table,
        hooks.profile(),
        &cancellation,
    )
    .await
    .expect("catalogo");
    drop(session);
    let catalog = crate::write_admission::target_facts(&target, &std::collections::BTreeSet::new());
    let schema = contract_schema(fields).expect("schema");
    let mut difformi = Vec::new();
    for field in schema.fields() {
        let column = crate::types::MysqlColumnSpec::from_catalog_declaring(
            target
                .columns
                .iter()
                .find(|column| &column.name == field.name())
                .expect("colonna"),
            hooks.profile(),
            None,
        )
        .expect("spec");
        let expected =
            crate::write_admission::create_facts(field.name(), &column.kind, hooks.kind())
                .expect("tipo di Create");
        let found = catalog
            .iter()
            .find(|facts| &facts.name == field.name())
            .expect("fatti");
        if &expected != found {
            difformi.push(format!("{}: {expected:?} contro {found:?}", field.name()));
        }
    }
    regola_a_drop_table(&hooks, table).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
