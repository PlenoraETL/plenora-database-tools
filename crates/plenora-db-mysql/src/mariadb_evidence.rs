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

use crate::evidence::{
    condense, config, environment, secret, server_code, truncate, Observation, Recorder,
};
use crate::profile::{ProductProfile, MARIADB_PROFILE, MYSQL_PROFILE};
use crate::{MysqlConfig, MysqlProvider, MysqlSession};
use mysql_async::prelude::Queryable;
use plenora_database_core::plan::{ObjectRef, OrderBy, ReadOperation, SortDirection};
use plenora_database_core::provider::{ParameterBag, Provider};
use plenora_database_core::query::{
    ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
};
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
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

/// La tabella delle sonde sugli errori: vincoli da violare e righe da bloccare.
const SCRATCH_LOCK: &str = "plenora_driver_evidence_lock";

/// Una query che il timer del server interrompe davvero.
///
/// `SELECT SLEEP(1)` non va bene, e non e una supposizione: la prima corsa di
/// questa tranche l'ha misurato. Su `MySQL` 9.7 `MAX_EXECUTION_TIME` era
/// applicato e la `SLEEP` finiva indisturbata — "nessun errore" dove ci si
/// aspettava un codice — perche il controllo del tempo non passa di li. Una
/// scansione incrociata ci passa, e i due motori la interrompono entrambi.
const INTERRUPTIBLE_QUERY: &str = "SELECT COUNT(*) FROM information_schema.columns a,      information_schema.columns b, information_schema.columns c";

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
                            // I metadata fanno parte dello schema Arrow: il
                            // mapper vi pubblica il tipo nativo, e due campi
                            // con lo stesso `DataType` possono portare
                            // annotazioni diverse. Ordinati per chiave,
                            // altrimenti l'ordine della mappa renderebbe
                            // diversi due schemi uguali.
                            let mut metadata = field
                                .metadata()
                                .iter()
                                .map(|(key, value)| format!("{key}={value}"))
                                .collect::<Vec<_>>();
                            metadata.sort();
                            format!(
                                "{}:{:?}/{}/{{{}}}",
                                field.name(),
                                field.data_type(),
                                field.is_nullable(),
                                metadata.join(",")
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
                    // Nessun troncamento: una rappresentazione tagliata non
                    // puo dimostrare che quattordici colonne coincidano, e il
                    // confronto fra server e proprio su questa stringa. Il
                    // riepilogo leggibile lo produce il runner, che ha anche
                    // il digest per dire a colpo d'occhio se due server hanno
                    // decodificato lo stesso contenuto.
                    batch
                        .columns()
                        .iter()
                        .map(|column| condense(&format!("{column:?}")))
                        .collect::<Vec<_>>()
                        .join(" | "),
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

// --------------------------------------------------------------------------
// Famiglia `raw`, superficie `errori`: quale codice manda il server dove la
// classificazione del profilo deve riconoscerlo.
// --------------------------------------------------------------------------

/// Il profilo del prodotto che sta rispondendo, scelto come lo sceglierebbe
/// un provider: chiedendo al profilo `MySQL` se quel server e suo.
///
/// Non e una comodita dell'harness. Le sonde di questa tranche misurano cio
/// che **il profilo** emette — l'istruzione di timeout, le query di catalogo —
/// e sceglierlo con un `if` sulla stringa qui dentro vorrebbe dire misurare
/// una decisione che l'harness ha preso al posto del codice.
fn profile_for(product_version: &str, version_comment: &str) -> &'static dyn ProductProfile {
    if MYSQL_PROFILE
        .foreign_product_rejection(product_version, version_comment)
        .is_some()
    {
        &MARIADB_PROFILE
    } else {
        &MYSQL_PROFILE
    }
}

/// Una connessione in piu per le sonde che ne pretendono due, o che sporcano
/// le variabili di sessione.
///
/// # Panics
///
/// Se non si apre: e un guasto dell'harness, non una divergenza.
async fn open_connection() -> mysql_async::Conn {
    mysql_async::Conn::new(
        config()
            .driver_opts("MySQL")
            .expect("opzioni driver della misura: harness, non divergenza"),
    )
    .await
    .expect("connessione della misura: harness, non divergenza")
}

/// Esegue lo statement e registra **il codice**, non l'esito che ci si
/// aspetta.
///
/// Il verso conta: queste sonde chiedono "cosa manda il server quando questa
/// cosa va storta", quindi l'errore e la misura riuscita e il successo e la
/// notizia. Registrare il contrario — `accepted` quando il server rifiuta —
/// renderebbe illeggibile la colonna del confronto.
async fn record_server_code(
    recorder: &mut Recorder,
    connection: &mut mysql_async::Conn,
    probe: &'static str,
    question: &'static str,
    statement: String,
) {
    match connection.query_drop(statement).await {
        Ok(()) => recorder.accepted(
            probe,
            "raw",
            "errori",
            question,
            "nessun errore: il server ha accettato".to_owned(),
        ),
        Err(error) => recorder.rejected(
            probe,
            "raw",
            "errori",
            question,
            condense(&error.to_string()),
            server_code(&error),
        ),
    }
}

/// I codici che la tabella di classificazione traduce in categoria, retry ed
/// effetto remoto.
///
/// Sono la superficie che il profilo di un secondo prodotto non puo
/// ereditare: la categoria decide cosa il chiamante legge, il retry decide se
/// ritenta, e l'effetto remoto decide se deve ripulire. Un codice che
/// significasse altro sull'altro motore trasformerebbe una di queste tre
/// risposte nel suo contrario, in silenzio.
#[allow(clippy::too_many_lines)]
async fn error_code_probes(recorder: &mut Recorder, profile: &'static dyn ProductProfile) {
    let mut connection = open_connection().await;
    // La tabella porta con se tutti i vincoli che le sonde violano: NOT NULL,
    // chiave primaria, CHECK e una chiave esterna su se stessa. Una tabella
    // per vincolo avrebbe reso le sonde indipendenti e la preparazione tre
    // volte piu lunga, senza misurare nulla di piu.
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_LOCK}"),
        format!(
            "CREATE TABLE {SCRATCH_LOCK} (\
             id INT PRIMARY KEY, \
             v INT NOT NULL, \
             positive INT NULL CHECK (positive > 0), \
             parent INT NULL, \
             CONSTRAINT fk_parent FOREIGN KEY (parent) REFERENCES {SCRATCH_LOCK}(id)\
             ) ENGINE=InnoDB"
        ),
        format!("INSERT INTO {SCRATCH_LOCK} (id, v) VALUES (1, 0), (2, 0)"),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("preparazione delle sonde sugli errori: harness, non divergenza");
    }

    for (probe, question, statement) in [
        (
            "raw.error_unknown_column",
            "quale codice manda una colonna che non esiste",
            format!("SELECT plenora_missing_column FROM {SCRATCH_LOCK}"),
        ),
        (
            "raw.error_unknown_table",
            "quale codice manda una tabella che non esiste",
            "SELECT 1 FROM plenora_missing_table".to_owned(),
        ),
        (
            "raw.error_unknown_database",
            "cosa risponde una tabella in uno schema che non esiste",
            "SELECT 1 FROM plenora_missing_schema.plenora_missing_table".to_owned(),
        ),
        (
            "raw.error_duplicate_key",
            "quale codice manda una chiave duplicata",
            format!("INSERT INTO {SCRATCH_LOCK} (id, v) VALUES (1, 0)"),
        ),
        (
            "raw.error_not_null",
            "quale codice manda un NULL in colonna non nullable",
            format!("INSERT INTO {SCRATCH_LOCK} (id, v) VALUES (10, NULL)"),
        ),
        (
            "raw.error_foreign_key",
            "quale codice manda una chiave esterna violata",
            format!("INSERT INTO {SCRATCH_LOCK} (id, v, parent) VALUES (11, 0, 999)"),
        ),
        (
            "raw.error_check_violation",
            "quale codice manda un CHECK violato",
            format!("INSERT INTO {SCRATCH_LOCK} (id, v, positive) VALUES (12, 0, -1)"),
        ),
        (
            "raw.error_privilege",
            "quale codice manda una lettura senza privilegio",
            "SELECT 1 FROM mysql.user".to_owned(),
        ),
    ] {
        record_server_code(recorder, &mut connection, probe, question, statement).await;
    }

    // Il timeout: prima si applica cio che **il profilo** emette, poi si
    // esegue qualcosa che lo supera. Le due meta stanno insieme, perche un
    // timeout applicato e mai fatto scattare non dice quale codice il
    // chiamante vedra — ed e quel codice a decidere se legge "timeout" o
    // "errore del server redatto".
    let applied = connection
        .query_drop(profile.statement_timeout_statement(200))
        .await;
    match applied {
        Ok(()) => {
            record_server_code(
                recorder,
                &mut connection,
                "raw.error_statement_timeout",
                "quale codice manda lo statement che supera il timeout del profilo",
                INTERRUPTIBLE_QUERY.to_owned(),
            )
            .await;
        }
        Err(error) => recorder.rejected(
            "raw.error_statement_timeout",
            "raw",
            "errori",
            "quale codice manda lo statement che supera il timeout del profilo",
            condense(&format!(
                "timeout non applicato dal profilo {}: {error}",
                profile.product()
            )),
            server_code(&error),
        ),
    }
    // La connessione muore qui, ma il timeout resta scritto nella sessione
    // finche vive: spegnerlo evita che una sonda aggiunta domani sotto questa
    // riga misuri il timeout invece di cio che voleva.
    let _ = connection
        .query_drop(profile.statement_timeout_statement(0))
        .await;

    lock_probes(recorder).await;

    // Autenticazione: l'unico codice che si osserva senza una sessione
    // aperta, e quindi con una connessione sua.
    let wrong = MysqlConfig::new(
        environment("PLENORA_MYSQL_HOST", "127.0.0.1"),
        environment("PLENORA_MYSQL_DATABASE", "dataflow_test"),
        environment("PLENORA_MYSQL_USER", "dataflow"),
        plenora_database_core::provider::SecretString::new("password_sbagliata_della_misura"),
    )
    .with_port(
        environment("PLENORA_MYSQL_PORT", "3306")
            .parse()
            .expect("porta MySQL della misura"),
    )
    .with_private_ca_certificate(
        std::env::var("PLENORA_MYSQL_CA").expect("PLENORA_MYSQL_CA obbligatoria"),
    );
    match mysql_async::Conn::new(
        wrong
            .driver_opts("MySQL")
            .expect("opzioni driver della misura: harness, non divergenza"),
    )
    .await
    {
        Ok(_) => recorder.accepted(
            "raw.error_access_denied",
            "raw",
            "errori",
            "quale codice manda una password sbagliata",
            "nessun errore: il server ha accettato".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "raw.error_access_denied",
            "raw",
            "errori",
            "quale codice manda una password sbagliata",
            condense(&error.to_string()),
            server_code(&error),
        ),
    }

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_LOCK}"))
        .await;
}

/// I due codici che nascono dalla contesa, e che nessuna sessione sola puo
/// produrre.
///
/// Il deadlock e il piu importante dei due: e l'unico codice della tabella
/// che dichiara `retry: Safe` e `remote_effect: RolledBack`, cioe l'unico che
/// autorizza il chiamante a rifare l'operazione. Ereditarlo senza misura
/// significherebbe autorizzare un ritentativo su un motore dove quel codice
/// potrebbe non voler dire "la transazione vittima e gia annullata".
async fn lock_probes(recorder: &mut Recorder) {
    let mut holder = open_connection().await;
    let mut waiter = open_connection().await;

    holder
        .query_drop("BEGIN")
        .await
        .expect("transazione della misura: harness, non divergenza");
    holder
        .query_drop(format!(
            "SELECT v FROM {SCRATCH_LOCK} WHERE id = 1 FOR UPDATE"
        ))
        .await
        .expect("lock della misura: harness, non divergenza");
    // Un secondo, non il default: il default e cinquanta, e una sonda che
    // aspetta cinquanta secondi non e una sonda.
    waiter
        .query_drop("SET SESSION innodb_lock_wait_timeout = 1")
        .await
        .expect("timeout di lock della misura: harness, non divergenza");
    waiter
        .query_drop("BEGIN")
        .await
        .expect("transazione della misura: harness, non divergenza");
    record_server_code(
        recorder,
        &mut waiter,
        "raw.error_lock_wait",
        "quale codice manda l'attesa di un lock che scade",
        format!("UPDATE {SCRATCH_LOCK} SET v = v + 1 WHERE id = 1"),
    )
    .await;
    for connection in [&mut holder, &mut waiter] {
        let _ = connection.query_drop("ROLLBACK").await;
    }

    // Il deadlock pretende che le due UPDATE incrociate siano in volo
    // insieme: eseguite in sequenza, la prima aspetta e basta. E l'unica
    // sonda concorrente della misura, e la concorrenza e la sonda.
    // Le due righe ripartono da zero: e il valore finale a dire se la vittima
    // e stata annullata, e una somma che porta i resti delle sonde precedenti
    // non direbbe niente.
    holder
        .query_drop(format!("UPDATE {SCRATCH_LOCK} SET v = 0"))
        .await
        .expect("azzeramento del deadlock: harness, non divergenza");
    for (connection, id) in [(&mut holder, 1), (&mut waiter, 2)] {
        connection
            .query_drop("BEGIN")
            .await
            .expect("transazione della misura: harness, non divergenza");
        connection
            .query_drop(format!(
                "UPDATE {SCRATCH_LOCK} SET v = v + 1 WHERE id = {id}"
            ))
            .await
            .expect("prologo del deadlock: harness, non divergenza");
    }
    let (first, second) = tokio::join!(
        holder.query_drop(format!("UPDATE {SCRATCH_LOCK} SET v = v + 1 WHERE id = 2")),
        waiter.query_drop(format!("UPDATE {SCRATCH_LOCK} SET v = v + 1 WHERE id = 1")),
    );
    // Il codice da solo non basta, ed e il punto della sonda. `1213` porta con
    // se le uniche due promesse della tabella che cambiano cosa il chiamante
    // **fa**: che possa ritentare, e che non abbia nulla da ripulire. Reggono
    // solo se la transazione della vittima e sparita davvero, e questo si
    // osserva dopo il commit del superstite: il prologo ha applicato due
    // incrementi, la coppia incrociata ne aggiunge uno solo — l'altra meta e
    // la vittima — quindi due e la somma di una vittima annullata per intero.
    for connection in [&mut holder, &mut waiter] {
        let _ = connection.query_drop("COMMIT").await;
    }
    let effect = match holder
        .query_first::<i64, _>(format!("SELECT SUM(v) FROM {SCRATCH_LOCK}"))
        .await
    {
        Ok(Some(2)) => "vittima annullata".to_owned(),
        Ok(Some(other)) => format!("vittima conservata in parte (somma={other})"),
        Ok(None) | Err(_) => "somma non leggibile".to_owned(),
    };
    let question =
        "quale codice manda la vittima di un deadlock, e cosa resta della sua transazione";
    match (first, second) {
        (Err(error), _) | (Ok(()), Err(error)) => recorder.rejected(
            "raw.error_deadlock",
            "raw",
            "errori",
            question,
            condense(&format!("{error} effetto={effect}")),
            server_code(&error),
        ),
        (Ok(()), Ok(())) => recorder.accepted(
            "raw.error_deadlock",
            "raw",
            "errori",
            question,
            format!("nessun errore: le due transazioni incrociate sono passate ({effect})"),
        ),
    }
}

// --------------------------------------------------------------------------
// Famiglia `provider`, superficie `profilo`: le stesse superfici, attraversate
// con il profilo del prodotto che sta rispondendo.
// --------------------------------------------------------------------------

/// Cio che un `MariadbProvider` farebbe, senza che esista.
///
/// Le sonde `provider` di sopra attraversano il provider `MySQL` con il suo
/// profilo: su `MariaDB` si fermano dove il profilo `MySQL` le porta a
/// fermarsi — `SRS_ID` che non esiste, `MAX_EXECUTION_TIME` nemmeno — e quindi
/// misurano il vecchio codice, non il nuovo. Queste attraversano le stesse
/// superfici con `MARIADB_PROFILE`, che e l'unico modo di sapere se le query
/// che dichiarano `NULL AS srs_id` e `NULL AS expression` **girano** invece
/// di limitarsi a compilare.
///
/// Il provider pubblico non entra: il pool si costruisce con il profilo, che
/// e il seam gia esistente, e nessun percorso di produzione cambia.
#[allow(clippy::too_many_lines)]
async fn profile_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    cancellation: &CancellationToken,
) {
    let schema_name = environment("PLENORA_MYSQL_DATABASE", "dataflow_test");
    let pool = crate::MysqlPool::new_with_profile(&config(), 2, profile)
        .expect("pool della misura: harness, non divergenza");

    // Il catalogo, con le query che il profilo decide. Su MariaDB e la prima
    // volta che `NULL AS srs_id` e `NULL AS expression` arrivano al server.
    match pool.checkout(cancellation).await {
        Ok(mut session) => {
            match crate::catalog::describe_object_with_profile(
                &mut session,
                &schema_name,
                SCRATCH,
                profile,
                cancellation,
            )
            .await
            {
                Ok(description) => recorder.accepted(
                    "provider.profile_describe_object",
                    "provider",
                    "profilo",
                    "descrive un oggetto con le query del profilo del prodotto",
                    format!(
                        "colonne={} indici={}",
                        description.columns.len(),
                        description.indexes.len()
                    ),
                ),
                Err(error) => recorder.rejected(
                    "provider.profile_describe_object",
                    "provider",
                    "profilo",
                    "descrive un oggetto con le query del profilo del prodotto",
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    crate::evidence::server_code_in_message(&error.message),
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_describe_object",
            "provider",
            "profilo",
            "descrive un oggetto con le query del profilo del prodotto",
            condense(&format!("sessione non aperta: {:?}", error.category)),
            None,
        ),
    }

    // La geometria: la regola dell'SRID dichiarato e la stessa sui due
    // profili, ma su MariaDB `srs_id` arriva sempre nullo, quindi il rifiuto
    // e la sola risposta possibile. Qui si osserva quale rifiuto sia.
    match pool.checkout(cancellation).await {
        Ok(mut session) => {
            match crate::catalog::describe_object_with_profile(
                &mut session,
                &schema_name,
                SCRATCH_GEO,
                profile,
                cancellation,
            )
            .await
            {
                // Descrivere non basta: la regola dell'SRID vive nel
                // mapping, non nel catalogo, e una sonda che si fermasse alla
                // descrizione direbbe "accettata" di una tabella che il
                // provider non sa leggere. La prima stesura si fermava li, e
                // registrava "colonne=2" su tutti e tre.
                Ok(description) => {
                    let mapped = description
                        .columns
                        .iter()
                        .map(|column| {
                            crate::MysqlColumnSpec::from_catalog_with_profile(column, profile)
                        })
                        .collect::<Result<Vec<_>, _>>();
                    match mapped {
                        Ok(specs) => recorder.accepted(
                            "provider.profile_describe_geometry",
                            "provider",
                            "profilo",
                            "descrive e mappa una tabella con colonna geometry, con il profilo del prodotto",
                            format!("colonne={}", specs.len()),
                        ),
                        Err(error) => recorder.rejected(
                            "provider.profile_describe_geometry",
                            "provider",
                            "profilo",
                            "descrive e mappa una tabella con colonna geometry, con il profilo del prodotto",
                            condense(&format!("{:?}: {}", error.category, error.message)),
                            crate::evidence::server_code_in_message(&error.message),
                        ),
                    }
                }
                Err(error) => recorder.rejected(
                    "provider.profile_describe_geometry",
                    "provider",
                    "profilo",
                    "descrive e mappa una tabella con colonna geometry, con il profilo del prodotto",
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    crate::evidence::server_code_in_message(&error.message),
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_describe_geometry",
            "provider",
            "profilo",
            "descrive e mappa una tabella con colonna geometry, con il profilo del prodotto",
            condense(&format!("sessione non aperta: {:?}", error.category)),
            None,
        ),
    }

    functional_index_probes(recorder, profile, &pool, &schema_name, cancellation).await;
    profile_timeout_probe(recorder, profile, &pool, cancellation).await;
}

/// L'indice funzionale: prima si prova a crearlo, poi si guarda come il
/// catalogo lo descrive.
///
/// Le due sonde stanno insieme perche la seconda non si legge senza la prima.
/// Il profilo di `MariaDB` dichiara di non pubblicare le parti funzionali, e
/// da li discende un rifiuto; se pero su quel motore un indice su espressione
/// non si puo nemmeno creare, quel rifiuto e una difesa contro un caso che il
/// server non produce — che e una cosa diversa da un difetto, e va scritta.
async fn functional_index_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    pool: &crate::MysqlPool,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let mut connection = open_connection().await;
    let _ = connection
        .query_drop(format!("DROP INDEX plenora_idx_expression ON {SCRATCH}"))
        .await;
    record_server_code(
        recorder,
        &mut connection,
        "raw.functional_index_ddl",
        "accetta un indice su espressione, che e cio che popola EXPRESSION",
        format!("CREATE INDEX plenora_idx_expression ON {SCRATCH} ((LOWER(text_utf8)))"),
    )
    .await;

    match pool.checkout(cancellation).await {
        Ok(mut session) => {
            match crate::catalog::describe_object_with_profile(
                &mut session,
                schema_name,
                SCRATCH,
                profile,
                cancellation,
            )
            .await
            {
                Ok(description) => {
                    // Cio che conta non e il numero: e se esista una parte che
                    // il catalogo non sa attribuire a una colonna, e come
                    // l'indice che la contiene venga descritto.
                    let described = description
                        .indexes
                        .iter()
                        .map(|index| {
                            format!(
                                "{}:colonne={} confrontabile={}",
                                index.name,
                                index.columns.len(),
                                index.column_backed
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    recorder.accepted(
                        "provider.profile_functional_index",
                        "provider",
                        "profilo",
                        "come il catalogo descrive gli indici dopo il tentativo su espressione",
                        truncate(&described, 160),
                    );
                }
                Err(error) => recorder.rejected(
                    "provider.profile_functional_index",
                    "provider",
                    "profilo",
                    "come il catalogo descrive gli indici dopo il tentativo su espressione",
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    crate::evidence::server_code_in_message(&error.message),
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_functional_index",
            "provider",
            "profilo",
            "come il catalogo descrive gli indici dopo il tentativo su espressione",
            condense(&format!("sessione non aperta: {:?}", error.category)),
            None,
        ),
    }

    let _ = connection
        .query_drop(format!("DROP INDEX plenora_idx_expression ON {SCRATCH}"))
        .await;
}

/// Il timeout attraversato per intero: applicato dal profilo dentro una
/// transazione vera, e fatto scattare.
///
/// E la sonda che chiude la distanza fra "il profilo emette la variabile
/// giusta" e "il chiamante legge la cosa giusta quando scatta". Le due
/// affermazioni si erano gia separate una volta: l'istruzione era corretta e
/// il codice che ne usciva finiva nel ramo generico della classificazione.
// La sessione vive quanto la sonda per costruzione: e la transazione che si
// sta misurando, e rilasciarla prima renderebbe la misura piu corta della
// domanda.
#[allow(clippy::significant_drop_tightening)]
async fn profile_timeout_probe(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    pool: &crate::MysqlPool,
    cancellation: &CancellationToken,
) {
    let options = TransactionOptions {
        statement_timeout_ms: Some(200),
        ..TransactionOptions::default()
    };
    let question = "cosa legge il chiamante quando scatta il timeout applicato dal profilo";
    let session = match pool.checkout(cancellation).await {
        Ok(session) => session,
        Err(error) => {
            recorder.rejected(
                "provider.profile_timeout",
                "provider",
                "profilo",
                question,
                condense(&format!("sessione non aperta: {:?}", error.category)),
                None,
            );
            return;
        }
    };
    let mut transaction =
        match crate::transaction::MysqlTransaction::begin(session, &options, cancellation).await {
            Ok(transaction) => transaction,
            Err(error) => {
                recorder.rejected(
                    "provider.profile_timeout",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "timeout non applicato dal profilo {}: {:?}: {}",
                        profile.product(),
                        error.category,
                        error.message
                    )),
                    crate::evidence::server_code_in_message(&error.message),
                );
                return;
            }
        };
    match transaction
        .query(&Statement::new(INTERRUPTIBLE_QUERY), cancellation)
        .await
    {
        Ok(_) => recorder.accepted(
            "provider.profile_timeout",
            "provider",
            "profilo",
            question,
            "nessun errore: il timeout applicato non ha interrotto".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "provider.profile_timeout",
            "provider",
            "profilo",
            question,
            // La quaterna, non il solo messaggio: categoria, retry ed effetto
            // remoto sono cio che il chiamante usa per decidere, e sono le tre
            // cose che una classificazione ereditata sbaglia insieme.
            condense(&format!(
                "{:?}/{:?}/{:?}: {}",
                error.category, error.retry, error.remote_effect, error.message
            )),
            crate::evidence::server_code_in_message(&error.message),
        ),
    }
    let _ = Box::new(transaction).rollback(cancellation).await;
}

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
            .driver_opts("MySQL")
            .expect("opzioni driver della misura: harness, non divergenza"),
    )
    .await
    .expect("connessione della misura: harness, non divergenza");

    let identity: (String, String) = connection
        .query_first("SELECT VERSION(), @@version_comment")
        .await
        .expect("identita del server: harness, non divergenza")
        .expect("identita del server presente");

    // Il profilo del prodotto che risponde: da qui in poi le sonde misurano
    // cio che **quel** profilo emette, non cio che l'harness ha deciso.
    let profile = profile_for(&identity.0, &identity.1);

    let mut recorder = Recorder(Vec::new());
    raw_probes(&mut recorder, &mut connection).await;
    provider_probes(&mut recorder).await;
    error_code_probes(&mut recorder, profile).await;
    profile_probes(&mut recorder, profile, &CancellationToken::new()).await;

    for table in [SCRATCH, SCRATCH_GEO, SCRATCH_LOCK] {
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
