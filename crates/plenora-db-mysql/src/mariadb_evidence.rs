//! Misura di evidenza `MariaDB`: cosa fa il driver, e cosa fa il provider.
//!
//! ADR 0014 chiede evidenza prima di scegliere fra provider dedicato e
//! qualificazione. La prima tranche l'ha raccolta dal **client**, con SQL
//! eseguito da `mariadb`/`mysql`: ha smentito tre delle cinque divergenze
//! dichiarate e ne ha trovate due che nessuno aveva nominato. Quello che il
//! client non puo vedere e il resto: il protocollo dei prepared statement, i
//! tipi wire che il server dichiara nei metadata, e cosa succede quando e il
//! **provider** ad attraversare quelle superfici.
//!
//! Il modulo osserva due famiglie, e le tiene separate nel verdetto perche
//! rispondono a due domande diverse:
//!
//! * `raw` — il driver `mysql_async` contro il server, senza provider in
//!   mezzo. Dice cosa il protocollo offre;
//! * `provider` — il provider `MySQL` corrente, attraversato con il bypass di
//!   solo test sul rifiuto iniziale. Dice cosa succede a **questo** codice.
//!
//! Le due possono divergere, ed e proprio il caso interessante: una
//! superficie che il protocollo offre ma che il provider non raggiunge —
//! perche emette `MAX_EXECUTION_TIME`, o perche legge
//! `information_schema.statistics.EXPRESSION` — e una superficie che
//! chiederebbe codice nuovo, non una che manca al motore.
//!
//! **Il bypass supera solo il rifiuto.** Non tocca SQL, mapping, timeout,
//! transazioni ne classificazione degli errori: cio che si osserva dopo e
//! esattamente cio che il provider fa oggi. Una sonda che fallisce **e** il
//! risultato: qui non si aggiungono rami `MariaDB` per proseguire, perche
//! sarebbero la risposta alla domanda che si sta ancora misurando.
//!
//! Nessuna sonda fa `panic` su un errore del server: l'errore e la misura.
//! Il test fallisce solo se l'harness non riesce a misurare — un server
//! irraggiungibile, una fixture assente — che e un problema suo e va chiuso
//! prima, non registrato come divergenza.

use crate::{MysqlConfig, MysqlProvider, MysqlSession};
use mysql_async::prelude::Queryable;
use plenora_database_core::plan::{ObjectRef, OrderBy, ReadOperation, SortDirection};
use plenora_database_core::provider::{ParameterBag, Provider, SecretString};
use plenora_database_core::query::{
    ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
};
use plenora_database_core::transaction::TransactionOptions;
use plenora_database_core::{CancellationToken, ResourceBudget, ResourceLimits};
use serde_json::json;

/// Il marcatore che il runner cerca nell'output di `cargo test --nocapture`.
const MARKER: &str = "PLENORA_MARIADB_EVIDENCE ";

/// Tabella di lavoro delle sonde: la creano loro, e la droppano.
const SCRATCH: &str = "plenora_driver_evidence";

/// La geometry sta in una tabella sua, e non e una comodita: il provider
/// deriva lo schema dall'intero oggetto prima di applicare la proiezione,
/// quindi una colonna spatial senza SRID dichiarato fa rifiutare **qualsiasi**
/// lettura di quella tabella. Tenendole insieme, la sonda sul mapping wire
/// misurava quella regola invece dei tipi.
const SCRATCH_GEO: &str = "plenora_driver_evidence_geo";

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn secret() -> SecretString {
    SecretString::new(environment("PLENORA_MYSQL_PASSWORD", "DataFlow_Test_2026!"))
}

fn config() -> MysqlConfig {
    let ca = std::env::var("PLENORA_MYSQL_CA")
        .expect("PLENORA_MYSQL_CA obbligatoria: la misura non accetta TLS non verificata");
    MysqlConfig::new(
        environment("PLENORA_MYSQL_HOST", "127.0.0.1"),
        environment("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        environment("PLENORA_MYSQL_USER", "dataflow"),
        secret(),
    )
    .with_port(
        environment("PLENORA_MYSQL_PORT", "3306")
            .parse()
            .expect("porta MySQL della misura"),
    )
    .with_private_ca_certificate(ca)
}

/// Una riga del verdetto.
struct Observation {
    probe: &'static str,
    family: &'static str,
    surface: &'static str,
    question: &'static str,
    outcome: String,
    detail: String,
    server_code: Option<u16>,
}

impl Observation {
    fn to_json(&self) -> serde_json::Value {
        json!({
            "probe": self.probe,
            "family": self.family,
            "surface": self.surface,
            "question": self.question,
            "outcome": self.outcome,
            "detail": self.detail,
            "server_code": self.server_code,
        })
    }
}

struct Recorder(Vec<Observation>);

/// Se una sonda gia registrata e stata accettata.
///
/// Serve alle sonde che dipendono da un'altra: quando la dipendenza fallisce,
/// il loro errore e la stessa cosa vista due volte. Registrarlo come rifiuto
/// autonomo gonfierebbe il conto delle divergenze con una sola causa, e
/// nasconderebbe che la superficie non e mai stata raggiunta.
impl Recorder {
    fn accepted_probe(&self, probe: &str) -> bool {
        self.0
            .iter()
            .any(|entry| entry.probe == probe && entry.outcome == "accepted")
    }
}

impl Recorder {
    fn accepted(
        &mut self,
        probe: &'static str,
        family: &'static str,
        surface: &'static str,
        question: &'static str,
        detail: String,
    ) {
        self.0.push(Observation {
            probe,
            family,
            surface,
            question,
            outcome: "accepted".to_owned(),
            detail,
            server_code: None,
        });
    }

    fn rejected(
        &mut self,
        probe: &'static str,
        family: &'static str,
        surface: &'static str,
        question: &'static str,
        detail: String,
        server_code: Option<u16>,
    ) {
        self.0.push(Observation {
            probe,
            family,
            surface,
            question,
            outcome: "rejected".to_owned(),
            detail,
            server_code,
        });
    }

    /// Una superficie che questa tranche non misura, e perche.
    ///
    /// Dichiararlo e piu onesto che dedurlo: un esito assente non e un esito
    /// negativo, e un verdetto che li confondesse porterebbe a decidere su
    /// una prova che non e stata fatta.
    fn not_measured(
        &mut self,
        probe: &'static str,
        family: &'static str,
        surface: &'static str,
        question: &'static str,
        reason: &str,
    ) {
        self.0.push(Observation {
            probe,
            family,
            surface,
            question,
            outcome: "not_measured".to_owned(),
            detail: reason.to_owned(),
            server_code: None,
        });
    }
}

/// Il codice d'errore che il server ha mandato, se l'errore viene da li.
fn server_code(error: &mysql_async::Error) -> Option<u16> {
    match error {
        mysql_async::Error::Server(server) => Some(server.code),
        _ => None,
    }
}

fn truncate(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        text.to_owned()
    } else {
        text.chars().take(limit).collect::<String>() + "…"
    }
}

fn condense(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

// --------------------------------------------------------------------------
// Famiglia `raw`: il driver contro il server, senza provider in mezzo.
// --------------------------------------------------------------------------

async fn raw_probes(recorder: &mut Recorder, connection: &mut mysql_async::Conn) {
    raw_protocol_probes(recorder, connection).await;
    raw_type_probes(recorder, connection).await;
}

async fn raw_protocol_probes(recorder: &mut Recorder, connection: &mut mysql_async::Conn) {
    // TLS: la misura gira su una connessione verificata, e il verdetto deve
    // poterlo dimostrare invece di affermarlo.
    match connection
        .query_first::<(String, String), _>("SHOW STATUS LIKE 'Ssl_cipher'")
        .await
    {
        Ok(Some((_, cipher))) if !cipher.is_empty() => recorder.accepted(
            "raw.tls_cipher",
            "raw",
            "protocollo",
            "quale cifrario TLS negozia la connessione della misura",
            cipher,
        ),
        Ok(_) => recorder.rejected(
            "raw.tls_cipher",
            "raw",
            "protocollo",
            "quale cifrario TLS negozia la connessione della misura",
            "connessione senza cifratura negoziata".to_owned(),
            None,
        ),
        Err(error) => recorder.rejected(
            "raw.tls_cipher",
            "raw",
            "protocollo",
            "quale cifrario TLS negozia la connessione della misura",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }
}

// Un catalogo di sonde e lineare per costruzione: spezzarlo ancora
// separerebbe una sonda dalla domanda che pone, che e cio che lo rende
// leggibile.
#[allow(clippy::too_many_lines)]
async fn raw_type_probes(recorder: &mut Recorder, connection: &mut mysql_async::Conn) {
    // La tabella delle famiglie di tipo: una colonna per ognuna di quelle che
    // il provider mappa. Se il DDL viene rifiutato, il rifiuto e la misura.
    let ddl = format!(
        "CREATE TABLE {SCRATCH} (\
         id BIGINT NOT NULL PRIMARY KEY, \
         small_signed SMALLINT NOT NULL, \
         big_unsigned BIGINT UNSIGNED NOT NULL, \
         exact_decimal DECIMAL(18, 4) NOT NULL, \
         approx_double DOUBLE NOT NULL, \
         moment_date DATE NULL, \
         moment_datetime DATETIME(6) NULL, \
         moment_timestamp TIMESTAMP NULL, \
         moment_time TIME(3) NULL, \
         text_utf8 VARCHAR(64) NULL, \
         blob_binary VARBINARY(32) NULL, \
         document JSON NULL, \
         choice ENUM('alfa', 'beta') NULL, \
         flags SET('x', 'y') NULL\
         ) ENGINE = InnoDB"
    );
    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH}"))
        .await;
    match connection.query_drop(&ddl).await {
        Ok(()) => recorder.accepted(
            "raw.type_table",
            "raw",
            "wire",
            "accetta una tabella con tutte le famiglie di tipo mappate",
            "creata".to_owned(),
        ),
        Err(error) => {
            recorder.rejected(
                "raw.type_table",
                "raw",
                "wire",
                "accetta una tabella con tutte le famiglie di tipo mappate",
                condense(&error.to_string()),
                server_code(&error),
            );
            return;
        }
    }

    // Una riga di dati: senza, la lettura del provider misurerebbe solo lo
    // schema derivato dai metadata, e il mapping dei valori — dove un fork
    // puo divergere sul filo — resterebbe fuori dalla misura.
    let seeded = connection
        .query_drop(format!(
            "INSERT INTO {SCRATCH} VALUES (1, -7, 18446744073709551615, 1234.5678,              2.5, '2026-08-18', '2026-08-18 06:00:00.000000',              '2026-08-18 06:00:00', '01:02:03.400', 'testo', 0x0102,              '{{\"k\": 1}}', 'alfa', 'x,y')"
        ))
        .await;
    match seeded {
        Ok(()) => recorder.accepted(
            "raw.type_row",
            "raw",
            "wire",
            "accetta un valore per ogni famiglia di tipo",
            "riga inserita".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "raw.type_row",
            "raw",
            "wire",
            "accetta un valore per ogni famiglia di tipo",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_GEO}"))
        .await;
    match connection
        .query_drop(format!(
            "CREATE TABLE {SCRATCH_GEO} (id BIGINT NOT NULL PRIMARY KEY, \n             shape GEOMETRY NULL) ENGINE = InnoDB"
        ))
        .await
    {
        Ok(()) => recorder.accepted(
            "raw.geometry_table",
            "raw",
            "spatial",
            "accetta una tabella con una colonna geometry",
            "creata".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "raw.geometry_table",
            "raw",
            "spatial",
            "accetta una tabella con una colonna geometry",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    match connection
        .prep(format!("SELECT shape FROM {SCRATCH_GEO} WHERE id = ?"))
        .await
    {
        Ok(statement) => recorder.accepted(
            "raw.prepare_metadata_geometry",
            "raw",
            "spatial",
            "quale tipo wire dichiara il server per una geometry",
            statement
                .columns()
                .iter()
                .map(|column| format!("{}:{:?}", column.name_str(), column.column_type()))
                .collect::<Vec<_>>()
                .join(", "),
        ),
        Err(error) => recorder.rejected(
            "raw.prepare_metadata_geometry",
            "raw",
            "spatial",
            "quale tipo wire dichiara il server per una geometry",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    // Prepared statement reale: i metadata di `COM_STMT_PREPARE` sono cio da
    // cui il path query deriva lo schema, e non si osservano da un client.
    match connection
        .prep(format!("SELECT * FROM {SCRATCH} WHERE id = ?"))
        .await
    {
        Ok(statement) => {
            let columns = statement
                .columns()
                .iter()
                .map(|column| {
                    format!(
                        "{}:{:?}{}",
                        column.name_str(),
                        column.column_type(),
                        if column
                            .flags()
                            .contains(mysql_async::consts::ColumnFlags::UNSIGNED_FLAG)
                        {
                            "/unsigned"
                        } else {
                            ""
                        }
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            recorder.accepted(
                "raw.prepare_metadata",
                "raw",
                "wire",
                "quali tipi wire dichiara il server nei metadata del prepare",
                columns,
            );
            recorder.accepted(
                "raw.prepare_parameters",
                "raw",
                "protocollo",
                "quanti parametri dichiara il prepare",
                statement.num_params().to_string(),
            );
        }
        Err(error) => recorder.rejected(
            "raw.prepare_metadata",
            "raw",
            "wire",
            "quali tipi wire dichiara il server nei metadata del prepare",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    // SRID dichiarato dalla colonna: MySQL lo espone in
    // `information_schema.columns.SRS_ID`, ed e da li che si sa se una
    // geometry ha un sistema di riferimento vincolato.
    match connection
        .query_first::<Option<u32>, _>(format!(
            "SELECT SRS_ID FROM information_schema.columns \
             WHERE TABLE_NAME = '{SCRATCH_GEO}' AND COLUMN_NAME = 'shape'"
        ))
        .await
    {
        Ok(value) => recorder.accepted(
            "raw.column_srid",
            "raw",
            "spatial",
            "espone SRS_ID di colonna in information_schema.columns",
            format!("{value:?}"),
        ),
        Err(error) => recorder.rejected(
            "raw.column_srid",
            "raw",
            "spatial",
            "espone SRS_ID di colonna in information_schema.columns",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    // Le funzioni spatial che il renderer condiviso emette per MySQL.
    match connection
        .query_first::<(String, u32, Vec<u8>), _>(
            "SELECT ST_GeometryType(g), ST_SRID(g), ST_AsWKB(g) \
             FROM (SELECT ST_GeomFromText('POINT(9.19 45.46)', 4326) AS g) AS probe",
        )
        .await
    {
        Ok(Some((kind, srid, wkb))) => recorder.accepted(
            "raw.spatial_functions",
            "raw",
            "spatial",
            "esegue le funzioni ST_* che il renderer emette per MySQL",
            format!("{kind} srid={srid} wkb={} byte", wkb.len()),
        ),
        Ok(None) => recorder.rejected(
            "raw.spatial_functions",
            "raw",
            "spatial",
            "esegue le funzioni ST_* che il renderer emette per MySQL",
            "nessuna riga".to_owned(),
            None,
        ),
        Err(error) => recorder.rejected(
            "raw.spatial_functions",
            "raw",
            "spatial",
            "esegue le funzioni ST_* che il renderer emette per MySQL",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    // Il timeout di statement che il provider imposta a ogni transazione.
    match connection
        .query_drop("SET SESSION MAX_EXECUTION_TIME = 1000")
        .await
    {
        Ok(()) => recorder.accepted(
            "raw.max_execution_time",
            "raw",
            "sessione",
            "accetta il timeout di statement che il provider emette",
            "accettato".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "raw.max_execution_time",
            "raw",
            "sessione",
            "accetta il timeout di statement che il provider emette",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    // La colonna che il preflight Upsert legge per riconoscere gli indici
    // funzionali, che non sa confrontare e deve rifiutare.
    match connection
        .query_first::<i64, _>(
            "SELECT COUNT(EXPRESSION) FROM information_schema.statistics \
             WHERE TABLE_SCHEMA = DATABASE()",
        )
        .await
    {
        Ok(_) => recorder.accepted(
            "raw.statistics_expression",
            "raw",
            "catalogo",
            "espone EXPRESSION in information_schema.statistics",
            "presente".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "raw.statistics_expression",
            "raw",
            "catalogo",
            "espone EXPRESSION in information_schema.statistics",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }
}

// --------------------------------------------------------------------------
// Famiglia `provider`: il provider MySQL corrente, attraversato dal bypass.
// --------------------------------------------------------------------------

async fn provider_probes(recorder: &mut Recorder) {
    let cancellation = CancellationToken::new();
    let provider =
        MysqlProvider::new(config(), 2).expect("provider della misura: harness, non divergenza");
    provider_protocol_probes(recorder, &provider, &cancellation).await;
    provider_surface_probes(recorder, &provider, &cancellation).await;
}

async fn provider_protocol_probes(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    cancellation: &CancellationToken,
) {
    match provider.test_connection(&secret(), cancellation).await {
        Ok(connection) => recorder.accepted(
            "provider.test_connection",
            "provider",
            "protocollo",
            "supera la probe del provider (con il bypass sul solo rifiuto)",
            format!("server_version={}", connection.server_version),
        ),
        Err(error) => recorder.rejected(
            "provider.test_connection",
            "provider",
            "protocollo",
            "supera la probe del provider (con il bypass sul solo rifiuto)",
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }

    match provider.probe_capabilities(&secret(), cancellation).await {
        Ok(capabilities) => recorder.accepted(
            "provider.capabilities",
            "provider",
            "protocollo",
            "pubblica le capability senza errori",
            format!(
                "create={} append={} upsert={} replace={} bulk={} spatial={}",
                capabilities.writes.create,
                capabilities.writes.append,
                capabilities.writes.upsert,
                capabilities.writes.replace,
                capabilities.writes.bulk,
                capabilities.spatial.functions.len()
            ),
        ),
        Err(error) => recorder.rejected(
            "provider.capabilities",
            "provider",
            "protocollo",
            "pubblica le capability senza errori",
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }
}

#[allow(clippy::too_many_lines)]
async fn provider_surface_probes(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    cancellation: &CancellationToken,
) {
    let schema_name = environment("PLENORA_MYSQL_DATABASE", "dataflow_test");

    // `describe_object` legge gli indici da `information_schema.statistics`,
    // colonna `EXPRESSION` inclusa: e la superficie che la misura dal client
    // aveva indicato come divergente, qui attraversata dal provider vero.
    match MysqlSession::open(&config(), cancellation).await {
        Ok(mut session) => {
            match crate::describe_object(&mut session, &schema_name, SCRATCH, cancellation).await {
                Ok(description) => recorder.accepted(
                    "provider.describe_object",
                    "provider",
                    "catalogo",
                    "descrive un oggetto, indici compresi",
                    format!(
                        "colonne={} indici={}",
                        description.columns.len(),
                        description.indexes.len()
                    ),
                ),
                Err(error) => recorder.rejected(
                    "provider.describe_object",
                    "provider",
                    "catalogo",
                    "descrive un oggetto, indici compresi",
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    None,
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.describe_object",
            "provider",
            "catalogo",
            "descrive un oggetto, indici compresi",
            condense(&format!("sessione non aperta: {:?}", error.category)),
            None,
        ),
    }

    let budget = ResourceBudget::new(ResourceLimits {
        rows: 1_024,
        memory_bytes: 96 * 1_024,
        output_bytes: 4 * 1_024 * 1_024,
        cell_bytes: 2 * 1_024,
        ..ResourceLimits::default()
    })
    .expect("budget della misura: harness, non divergenza");
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(schema_name.clone()),
            object: SCRATCH.to_owned(),
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

    // La query non passa dal catalogo: lo schema esce dai metadata del
    // prepare. Va eseguita comunque, anche quando `describe_object` fallisce,
    // perche e l'unica strada che raggiunge il mapper del provider su un
    // motore il cui catalogo non risponde.
    query_probes(recorder, provider, &schema_name, &budget, cancellation).await;

    // La lettura dipende dalla descrizione: il provider deriva lo schema
    // dallo stesso catalogo. Quando quella fallisce, l'errore qui e la stessa
    // causa vista una seconda volta — registrarlo come rifiuto autonomo
    // conterebbe due divergenze dove ce n'e una, e direbbe che la superficie
    // e stata provata quando non e mai stata raggiunta.
    let catalog_reachable = recorder.accepted_probe("provider.describe_object");
    if catalog_reachable {
        read_probes(recorder, provider, &operation, &budget, cancellation).await;
    } else {
        for (probe, surface, question) in [
            (
                "provider.read",
                "wire",
                "apre uno stream Arrow, ne deriva lo schema e decodifica i valori",
            ),
            (
                "provider.read_geometry",
                "spatial",
                "legge e decodifica almeno un batch con una colonna geometry",
            ),
        ] {
            recorder.not_measured(
                probe,
                "provider",
                surface,
                question,
                "dipende da provider.describe_object, che non ha raggiunto il \
                 catalogo: la superficie non e stata provata, non e stata \
                 rifiutata",
            );
        }
    }

    // Il timeout di statement e la ragione per cui il provider emette
    // `MAX_EXECUTION_TIME`: con le opzioni di default resta `None`, e la
    // sonda passerebbe senza mai toccare l'istruzione che si sta misurando.
    let timed = TransactionOptions {
        statement_timeout_ms: Some(5_000),
        ..TransactionOptions::default()
    };
    match provider
        .begin_transaction(&secret(), &timed, &budget, cancellation)
        .await
    {
        Ok(transaction) => match transaction.commit(cancellation).await {
            Ok(outcome) => recorder.accepted(
                "provider.transaction",
                "provider",
                "sessione",
                "apre e committa una transazione con statement timeout",
                format!("commit {outcome:?}"),
            ),
            Err(error) => recorder.rejected(
                "provider.transaction",
                "provider",
                "sessione",
                "apre e committa una transazione con statement timeout",
                condense(&format!("{:?}: {}", error.category, error.message)),
                None,
            ),
        },
        Err(error) => recorder.rejected(
            "provider.transaction",
            "provider",
            "sessione",
            "apre e committa una transazione con statement timeout",
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }

    cancellation_probes(recorder, provider, cancellation).await;

    recorder.not_measured(
        "provider.ambiguous_commit",
        "provider",
        "commit",
        "come classifica un commit di esito ignoto",
        "richiede fault injection deterministica sul COMMIT — uccidere la \
         connessione a meta commit da una seconda sessione e una corsa, non \
         un esperimento ripetibile, e un esito ottenuto cosi non distingue \
         il comportamento del provider dal momento in cui e arrivato il \
         colpo. Nessuna inferenza da qui.",
    );
}

/// Il mapper del provider, raggiunto **senza** passare dal catalogo.
///
/// `read` deriva lo schema da `describe_object`, quindi su `MariaDB` si ferma
/// prima di mappare qualsiasi cosa: i tipi wire divergenti — `JSON` come
/// `MYSQL_TYPE_BLOB`, `TIMESTAMP` con il flag unsigned — finora erano stati
/// osservati solo dal driver diretto, mai attraverso il codice che li
/// converte in Arrow. `QueryOperation` prende quella strada: lo schema esce
/// dai metadata del prepare, non da `information_schema`, quindi la sonda
/// misura il mapper anche dove il catalogo non e raggiungibile.
///
/// Consuma il batch: schema, nullability e valori decodificati insieme, che e
/// l'unico modo di accorgersi che due `DataType` uguali portano contenuti
/// diversi.
#[allow(clippy::too_many_lines)]
async fn query_probes(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    schema_name: &str,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    let column = |field: &str| QueryExpression::Column {
        column: ColumnRef {
            relation: None,
            field: field.to_owned(),
        },
    };
    let projected = [
        "id",
        "small_signed",
        "big_unsigned",
        "exact_decimal",
        "approx_double",
        "moment_date",
        "moment_datetime",
        "moment_timestamp",
        "moment_time",
        "text_utf8",
        "blob_binary",
        "document",
        "choice",
        "flags",
    ];
    let operation = QueryOperation {
        common_table_expressions: Vec::new(),
        source: Some(QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some(schema_name.to_owned()),
                object: SCRATCH.to_owned(),
                layer_id: None,
            },
            alias: None,
        }),
        derived_source: None,
        projection: projected
            .iter()
            .map(|field| QueryProjection {
                expression: column(field),
                alias: None,
            })
            .collect(),
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
        row_limit: Some(1),
        row_offset: None,
        locking: None,
    };

    match provider
        .query(
            &secret(),
            &operation,
            &ParameterBag::default(),
            budget,
            cancellation,
        )
        .await
    {
        Ok(mut stream) => match stream.next_batch(cancellation).await {
            Ok(Some(batch)) => {
                recorder.accepted(
                    "provider.query_schema",
                    "provider",
                    "wire",
                    "deriva lo schema Arrow dai metadata del prepare, senza catalogo",
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| {
                            format!(
                                "{}:{:?}/{}",
                                field.name(),
                                field.data_type(),
                                field.is_nullable()
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                recorder.accepted(
                    "provider.query_values",
                    "provider",
                    "wire",
                    "decodifica i valori di ogni famiglia di tipo",
                    truncate(
                        &batch
                            .columns()
                            .iter()
                            .map(|column| condense(&format!("{column:?}")))
                            .collect::<Vec<_>>()
                            .join(" | "),
                        900,
                    ),
                );
            }
            Ok(None) => {
                for probe in ["provider.query_schema", "provider.query_values"] {
                    recorder.rejected(
                        probe,
                        "provider",
                        "wire",
                        "esegue una QueryOperation e ne consuma il batch",
                        "stream aperto ma senza righe: la fixture non ha dati".to_owned(),
                        None,
                    );
                }
            }
            Err(error) => {
                for probe in ["provider.query_schema", "provider.query_values"] {
                    recorder.rejected(
                        probe,
                        "provider",
                        "wire",
                        "esegue una QueryOperation e ne consuma il batch",
                        condense(&format!("{:?}: {}", error.category, error.message)),
                        None,
                    );
                }
            }
        },
        Err(error) => {
            for probe in ["provider.query_schema", "provider.query_values"] {
                recorder.rejected(
                    probe,
                    "provider",
                    "wire",
                    "esegue una QueryOperation e ne consuma il batch",
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    None,
                );
            }
        }
    }
}

/// Lettura Arrow: schema, metadata e **valori decodificati**.
///
/// Lo schema da solo non basta a dire che il mapping funzioni: un tipo wire
/// che il server dichiara diversamente — `JSON` come `BLOB`, `TIMESTAMP` con
/// il flag unsigned — produce lo stesso `DataType` e poi decodifica un altro
/// valore. Le tre cose vanno confrontate insieme, ed e per questo che la
/// sonda registra i campi Arrow **e** il contenuto della prima riga.
#[allow(clippy::too_many_lines)]
async fn read_probes(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    operation: &ReadOperation,
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) {
    match provider
        .read(
            &secret(),
            operation,
            &ParameterBag::default(),
            budget,
            cancellation,
        )
        .await
    {
        Ok(mut stream) => match stream.next_batch(cancellation).await {
            Ok(Some(batch)) => {
                let schema = batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| {
                        format!(
                            "{}:{:?}/{}",
                            field.name(),
                            field.data_type(),
                            field.is_nullable()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                // I valori si leggono dal `Debug` degli array: e verboso ma
                // mostra cio che e stato decodificato, che e il punto — una
                // formattazione piu bella richiederebbe una dipendenza in piu
                // per una misura che si legge una volta.
                let values = batch
                    .columns()
                    .iter()
                    .map(|column| condense(&format!("{column:?}")))
                    .collect::<Vec<_>>()
                    .join(" | ");
                recorder.accepted(
                    "provider.read",
                    "provider",
                    "wire",
                    "apre uno stream Arrow, ne deriva lo schema e decodifica i valori",
                    format!("schema=[{schema}] valori=[{}]", truncate(&values, 600)),
                );
            }
            Ok(None) => recorder.rejected(
                "provider.read",
                "provider",
                "wire",
                "apre uno stream Arrow, ne deriva lo schema e decodifica i valori",
                "stream aperto ma senza righe: la fixture non ha dati".to_owned(),
                None,
            ),
            Err(error) => recorder.rejected(
                "provider.read",
                "provider",
                "wire",
                "apre uno stream Arrow, ne deriva lo schema e decodifica i valori",
                condense(&format!("{:?}: {}", error.category, error.message)),
                None,
            ),
        },
        Err(error) => recorder.rejected(
            "provider.read",
            "provider",
            "wire",
            "apre uno stream Arrow, ne deriva lo schema e decodifica i valori",
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }

    // La geometry attraverso il provider, fino al **contenuto** del batch:
    // aprire lo stream non prova la decodifica, ed e proprio li che una
    // geometry senza SRID dichiarato o un WKB diverso si farebbero sentire.
    let spatial = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: operation.source.schema.clone(),
            object: SCRATCH_GEO.to_owned(),
            layer_id: None,
        },
        ..operation.clone()
    };
    match provider
        .read(
            &secret(),
            &spatial,
            &ParameterBag::default(),
            budget,
            cancellation,
        )
        .await
    {
        Ok(mut stream) => match stream.next_batch(cancellation).await {
            Ok(Some(batch)) => recorder.accepted(
                "provider.read_geometry",
                "provider",
                "spatial",
                "legge e decodifica almeno un batch con una colonna geometry",
                format!(
                    "schema=[{}] valori=[{}]",
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| format!("{}:{:?}", field.name(), field.data_type()))
                        .collect::<Vec<_>>()
                        .join(", "),
                    truncate(
                        &batch
                            .columns()
                            .iter()
                            .map(|column| condense(&format!("{column:?}")))
                            .collect::<Vec<_>>()
                            .join(" | "),
                        300,
                    )
                ),
            ),
            Ok(None) => recorder.rejected(
                "provider.read_geometry",
                "provider",
                "spatial",
                "legge e decodifica almeno un batch con una colonna geometry",
                "stream aperto ma senza righe".to_owned(),
                None,
            ),
            Err(error) => recorder.rejected(
                "provider.read_geometry",
                "provider",
                "spatial",
                "legge e decodifica almeno un batch con una colonna geometry",
                condense(&format!("{:?}: {}", error.category, error.message)),
                None,
            ),
        },
        Err(error) => recorder.rejected(
            "provider.read_geometry",
            "provider",
            "spatial",
            "legge e decodifica almeno un batch con una colonna geometry",
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }
}

/// Cancellazione **in volo**, quarantena e riuso, sulla stessa connessione.
///
/// Cancellare prima di partire misura un `if`, non il protocollo: la domanda
/// vera e cosa succede quando il token cade mentre il server sta rispondendo
/// — come viene classificato l'esito, se la sessione resta utilizzabile, e se
/// il provider sa rimpiazzarla. Le tre cose si osservano sulla stessa
/// sessione, in quest'ordine, perche e l'ordine in cui un consumer le incontra.
async fn cancellation_probes(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    cancellation: &CancellationToken,
) {
    let inflight = CancellationToken::new();
    let mut session = match MysqlSession::open(&config(), cancellation).await {
        Ok(session) => session,
        Err(error) => {
            for probe in [
                "provider.cancellation_inflight",
                "provider.session_quarantine",
            ] {
                recorder.not_measured(
                    probe,
                    "provider",
                    "sessione",
                    "cancellazione in volo, quarantena e riuso",
                    &format!(
                        "sessione non aperta ({:?}): la superficie non e stata provata",
                        error.category
                    ),
                );
            }
            return;
        }
    };

    let toggle = inflight.clone();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        toggle.cancel();
    });
    let cancelled = session
        .query_rows(
            "SELECT SLEEP(5)",
            plenora_database_core::ErrorPhase::Read,
            &inflight,
        )
        .await;
    match cancelled {
        Ok(_) => recorder.rejected(
            "provider.cancellation_inflight",
            "provider",
            "sessione",
            "classifica una query cancellata mentre il server risponde",
            "la query e tornata senza errore: la cancellazione non ha morso".to_owned(),
            None,
        ),
        Err(error) => recorder.accepted(
            "provider.cancellation_inflight",
            "provider",
            "sessione",
            "classifica una query cancellata mentre il server risponde",
            format!(
                "{:?}/{:?}/retry={:?}",
                error.category, error.remote_effect, error.retry
            ),
        ),
    }

    recorder.accepted(
        "provider.session_quarantine",
        "provider",
        "sessione",
        "mette in quarantena la sessione cancellata e non la riusa",
        format!(
            "stato={:?} riusabile={}",
            session.state(),
            session.is_reusable()
        ),
    );
    drop(session);

    // Riuso: il provider deve saper rimpiazzare la sessione bruciata.
    match provider.test_connection(&secret(), cancellation).await {
        Ok(_) => recorder.accepted(
            "provider.session_reuse",
            "provider",
            "sessione",
            "il provider resta usabile dopo una cancellazione in volo",
            "connessione rimpiazzata".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "provider.session_reuse",
            "provider",
            "sessione",
            "il provider resta usabile dopo una cancellazione in volo",
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }
}

// --------------------------------------------------------------------------
// Punto d'ingresso
// --------------------------------------------------------------------------

/// Il punto d'ingresso della misura.
///
/// Il nome **non** ha il prefisso `live_` di proposito: quel prefisso e cio
/// che i tre runner della qualifica `MySQL` filtrano, e questa non e una prova
/// di qualifica — e una misura con un runner suo,
/// `scripts/check_mariadb_driver.py`. Con il prefisso finirebbe negli
/// inventari del gate `MySQL`, che dichiarano cosa quel provider ha dimostrato:
/// un'affermazione che una misura su `MariaDB` non puo sostenere.
///
/// `#[ignore]` la tiene fuori anche dal runner offline, che pretende un
/// server che qui non c'e.
#[tokio::test]
#[ignore = "misura di evidenza MariaDB: richiede un riferimento live esplicito"]
async fn mariadb_driver_evidence() {
    run_driver_evidence().await;
}

/// Esegue la misura e stampa il verdetto sul marcatore.
///
/// Il punto d'ingresso `#[test]` sta poco sopra, in questo stesso modulo, e
/// **non** in `live_tests`: quel file raccoglie i test della qualifica
/// `MySQL`, i cui inventari dichiarano cosa quel provider ha dimostrato, e
/// una misura su un motore non qualificato non puo sostenere
/// quell'affermazione. Per lo stesso motivo il nome non porta il prefisso
/// `live_`, che e cio che i tre runner del gate filtrano.
///
/// # Panics
///
/// Se la misura non riesce: server irraggiungibile, TLS non configurata,
/// provider non costruibile. Sono guasti dell'harness, non divergenze, e
/// vanno chiusi prima di leggere i numeri.
async fn run_driver_evidence() {
    // La guardia spegne il bypass quando la misura finisce: nessun test
    // eseguito dopo, in questo binario, deve trovarlo acceso.
    let _bypass = crate::catalog::MariadbRejectionBypass::engage();

    let mut connection = mysql_async::Conn::new(
        config()
            .driver_opts()
            .expect("opzioni driver della misura: harness, non divergenza"),
    )
    .await
    .expect("connessione della misura: harness, non divergenza");

    let identity: (String, String) = connection
        .query_first("SELECT VERSION(), @@version_comment")
        .await
        .expect("identita del server: harness, non divergenza")
        .expect("identita del server presente");

    let mut recorder = Recorder(Vec::new());
    raw_probes(&mut recorder, &mut connection).await;
    provider_probes(&mut recorder).await;

    for table in [SCRATCH, SCRATCH_GEO] {
        let _ = connection
            .query_drop(format!("DROP TABLE IF EXISTS {table}"))
            .await;
    }

    let document = json!({
        "schema_version": 1,
        "server": {
            "label": environment("PLENORA_EVIDENCE_LABEL", "sconosciuto"),
            "host": environment("PLENORA_MYSQL_HOST", "127.0.0.1"),
            "product_version": identity.0,
            "version_comment": identity.1,
            "digest": environment("PLENORA_EVIDENCE_DIGEST", ""),
        },
        "observations": recorder
            .0
            .iter()
            .map(Observation::to_json)
            .collect::<Vec<_>>(),
    });
    println!(
        "{MARKER}{}",
        serde_json::to_string(&document).expect("verdetto serializzabile")
    );
}
