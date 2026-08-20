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
use crate::evidence::{
    qualified_filter_forms, read_mismatch, refusal_mismatch, ReadContract, ReadOutcome,
    RefusalContract, STREAMING_ROWS, STREAMING_ROWS_I64,
};
use crate::profile::{ProductProfile, MARIADB_PROFILE, MYSQL_PROFILE};
use crate::{MysqlConfig, MysqlProvider, MysqlSession};
use mysql_async::prelude::Queryable;
use plenora_database_core::arrow::array::{Array, Int32Array, Int64Array};
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, OrderBy, ReadOperation, SortDirection,
};
use plenora_database_core::protocol;
use plenora_database_core::provider::{ParameterBag, ParameterValue, Provider};
use plenora_database_core::query::{
    ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QueryProjection, QuerySource,
};
use plenora_database_core::transaction::{Statement, TransactionOptions, TransactionScope};
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, RemoteEffect, ResourceBudget, ResourceLimits,
    RetryDisposition,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

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

/// La tabella delle sonde di lettura: righe abbastanza da spezzare un batch.
const SCRATCH_ROWS: &str = "plenora_driver_evidence_rows";

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

/// Un budget che non interferisce: le sonde di lettura misurano il lettore,
/// non il limite di risorse, e un budget condiviso fra piu letture si
/// esaurirebbe a meta perche le prenotazioni si accumulano.
#[allow(clippy::cast_possible_truncation)]
fn read_budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits {
        rows: 4 * STREAMING_ROWS as u64,
        memory_bytes: 64 * 1_024 * 1_024,
        output_bytes: 128 * 1_024 * 1_024,
        cell_bytes: 2 * 1_024,
        ..ResourceLimits::default()
    })
    .expect("budget della misura: harness, non divergenza")
}

/// Consuma uno stream fino in fondo e ne descrive il contenuto.
async fn drain_read(
    provider: &MysqlProvider,
    profile: &'static dyn ProductProfile,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    cancellation: &CancellationToken,
) -> Result<ReadOutcome, plenora_database_core::DatabaseError> {
    let budget = read_budget();
    let mut stream = provider
        .read(&secret(), operation, parameters, &budget, cancellation)
        .await?;
    let keys = profile.metadata_keys();
    let foreign = if keys.native_type == protocol::MYSQL_NATIVE_TYPE {
        protocol::MARIADB_NATIVE_TYPE
    } else {
        protocol::MYSQL_NATIVE_TYPE
    };
    let mut outcome = ReadOutcome::default();
    let mut hasher = Sha256::new();
    while let Some(batch) = stream.next_batch(cancellation).await? {
        // Il digest copre **ogni** batch: confrontare il solo primo direbbe
        // "identici" di due stream che divergono alla riga successiva.
        let rendered = batch
            .columns()
            .iter()
            .map(|column| format!("{column:?}"))
            .collect::<Vec<_>>()
            .join(" | ");
        hasher.update(rendered.as_bytes());
        if outcome.batches == 0 {
            outcome.names = batch
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect();
            outcome.schema = batch
                .schema()
                .fields()
                .iter()
                .map(|field| {
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
                .join(", ");
            outcome.first_batch = condense(&rendered);
            for field in batch.schema().fields() {
                outcome.own_namespace +=
                    usize::from(field.metadata().contains_key(keys.native_type));
                outcome.foreign_namespace += usize::from(field.metadata().contains_key(foreign));
            }
            // Il primo intero della prima colonna: e cio che distingue un
            // ordinamento ascendente da uno discendente, e lo si prende
            // dall'array tipizzato invece che dal `Debug`, che e una
            // rappresentazione e potrebbe cambiare forma.
            outcome.first_integer = batch.columns().first().and_then(|column| {
                column
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .filter(|array| !array.is_empty())
                    .map(|array| i64::from(array.value(0)))
                    .or_else(|| {
                        column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .filter(|array| !array.is_empty())
                            .map(|array| array.value(0))
                    })
            });
        }
        outcome.batches += 1;
        outcome.rows += batch.num_rows();
    }
    outcome.digest = format!("{:x}", hasher.finalize());
    Ok(outcome)
}

/// Esegue una lettura, la confronta con il contratto, e registra l'esito.
///
/// La differenza con la prima stesura sta tutta qui: `accepted` non significa
/// piu "la chiamata non ha dato errore" ma "il risultato e quello dichiarato".
/// Una projection ignorata, un filtro che non filtra o uno stream consegnato
/// in un colpo solo restituiscono `Ok`, e senza questo confronto sarebbero
/// finiti verdi su tutti e tre i server — con l'aria di una convergenza.
#[allow(clippy::too_many_arguments)]
async fn record_read(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    profile: &'static dyn ProductProfile,
    probe: &'static str,
    question: &'static str,
    operation: &ReadOperation,
    parameters: &ParameterBag,
    contract: &ReadContract,
    detail: impl FnOnce(&ReadOutcome) -> String,
    cancellation: &CancellationToken,
) -> Option<ReadOutcome> {
    match drain_read(provider, profile, operation, parameters, cancellation).await {
        Ok(outcome) => match read_mismatch(contract, &outcome) {
            None => {
                recorder.accepted(probe, "provider", "profilo", question, detail(&outcome));
                Some(outcome)
            }
            Some(mismatch) => {
                recorder.rejected(
                    probe,
                    "provider",
                    "profilo",
                    question,
                    // Il contratto violato **e** la misura: non "la lettura e
                    // fallita", ma "ha risposto un'altra cosa".
                    condense(&format!("contratto non soddisfatto: {mismatch}")),
                    None,
                );
                None
            }
        },
        Err(error) => {
            recorder.rejected(
                probe,
                "provider",
                "profilo",
                question,
                condense(&format!("{:?}: {}", error.category, error.message)),
                crate::evidence::server_code_in_message(&error.message),
            );
            None
        }
    }
}

/// I quattordici campi della tabella dei tipi, nell'ordine in cui la lettura
/// li pubblica. Scritti qui perche il contratto della sonda sia un'attesa e
/// non un riflesso di cio che e arrivato.
const TYPE_TABLE_COLUMNS: &[&str] = &[
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

/// `provider.read` attraversato con il profilo del prodotto, fino al contenuto.
///
/// E il punto 1 della fase 3: schema, valori e namespace sui due riferimenti
/// qualificati. La sonda storica `provider.read` resta dov'e e continua a
/// misurare il provider `MySQL`; questa misura cosa succede quando a leggere e
/// il profilo che il prodotto merita, ed e l'unica che su `MariaDB` arriva in
/// fondo — l'altra si ferma al catalogo che non risponde.
///
/// Le sonde sono separate perche rispondono a domande separate, e una sola
/// riga verde su tutte non direbbe **quale** parte regge.
#[allow(clippy::too_many_lines)]
async fn profile_read_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let provider = MysqlProvider::with_profile(config(), 2, profile)
        .expect("provider della misura: harness, non divergenza");
    let source = |object: &str| ObjectRef {
        catalog: None,
        schema: Some(schema_name.to_owned()),
        object: object.to_owned(),
        layer_id: None,
    };
    let ordered = |object: &str| ReadOperation {
        source: source(object),
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        filter: None,
    };

    // Schema, valori e namespace dalla stessa lettura, con tre osservazioni:
    // una riga sola direbbe "la lettura funziona" senza dire se a coincidere
    // sia il tipo, il valore o l'annotazione.
    let full = ReadContract {
        columns: TYPE_TABLE_COLUMNS,
        rows: 1,
        batches: Some(1),
        first_integer: Some(1),
    };
    match drain_read(
        &provider,
        profile,
        &ordered(SCRATCH),
        &ParameterBag::default(),
        cancellation,
    )
    .await
    {
        Ok(outcome) => {
            if let Some(mismatch) = read_mismatch(&full, &outcome) {
                for probe in [
                    "provider.profile_read_schema",
                    "provider.profile_read_values",
                    "provider.profile_read_namespace",
                ] {
                    recorder.rejected(
                        probe,
                        "provider",
                        "profilo",
                        "legge la tabella dei tipi con il profilo del prodotto",
                        condense(&format!("contratto non soddisfatto: {mismatch}")),
                        None,
                    );
                }
            } else {
                recorder.accepted(
                    "provider.profile_read_schema",
                    "provider",
                    "profilo",
                    "lo schema Arrow che la lettura pubblica, metadata compresi",
                    outcome.schema.clone(),
                );
                recorder.accepted(
                    "provider.profile_read_values",
                    "provider",
                    "profilo",
                    "i valori decodificati dalla lettura, con il digest di tutti i batch",
                    format!(
                        "digest={} primo_batch={}",
                        outcome.digest, outcome.first_batch
                    ),
                );
                recorder.accepted(
                    "provider.profile_read_namespace",
                    "provider",
                    "profilo",
                    "sotto quale namespace la lettura annota le colonne",
                    format!(
                        "chiave={} annotate={} estranee={} su {} campi",
                        profile.metadata_keys().native_type,
                        outcome.own_namespace,
                        outcome.foreign_namespace,
                        outcome.names.len()
                    ),
                );
            }
        }
        Err(error) => {
            for probe in [
                "provider.profile_read_schema",
                "provider.profile_read_values",
                "provider.profile_read_namespace",
            ] {
                recorder.rejected(
                    probe,
                    "provider",
                    "profilo",
                    "legge la tabella dei tipi con il profilo del prodotto",
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    crate::evidence::server_code_in_message(&error.message),
                );
            }
        }
    }

    // Proiezione: tre colonne dichiarate in un ordine che **non** e quello
    // della tabella. Senza l'ordine atteso, una projection ignorata — che
    // restituisce tutte e quattordici le colonne — sarebbe passata per buona.
    let projected = ReadOperation {
        projection: vec![
            "text_utf8".to_owned(),
            "id".to_owned(),
            "exact_decimal".to_owned(),
        ],
        ..ordered(SCRATCH)
    };
    record_read(
        recorder,
        &provider,
        profile,
        "provider.profile_read_projection",
        "una projection dichiarata decide quali colonne escono, e in quale ordine",
        &projected,
        &ParameterBag::default(),
        &ReadContract {
            columns: &["text_utf8", "id", "exact_decimal"],
            rows: 1,
            batches: Some(1),
            first_integer: None,
        },
        |outcome| outcome.schema.clone(),
        cancellation,
    )
    .await;

    streaming_read_probes(recorder, &provider, profile, schema_name, cancellation).await;
}

/// Filtro, ordinamento e streaming, su una tabella fatta per questo.
///
/// Le sonde vogliono piu di una riga: un filtro che non esclude niente, un
/// ordinamento su un elemento solo e uno stream che finisce al primo batch non
/// distinguono un provider che le implementa da uno che le ignora.
#[allow(clippy::too_many_lines)]
async fn streaming_read_probes(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let mut connection = open_connection().await;
    let mut rows = String::new();
    for id in 1..=STREAMING_ROWS_I64 {
        if id > 1 {
            rows.push(',');
        }
        // Un payload di lunghezza fissa, e una `label` nulla ogni tre righe:
        // senza quella colonna `IS NULL` e `IS NOT NULL` sarebbero due modi di
        // dire "tutte".
        if id % 3 == 0 {
            let _ = write!(&mut rows, "({id},'{id:064}',NULL)");
        } else {
            let _ = write!(&mut rows, "({id},'{id:064}','eti-{id}')");
        }
    }
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_ROWS}"),
        format!(
            "CREATE TABLE {SCRATCH_ROWS} (id INT NOT NULL PRIMARY KEY, \
             payload VARCHAR(64) NOT NULL, label VARCHAR(32) NULL) ENGINE = InnoDB"
        ),
        format!("INSERT INTO {SCRATCH_ROWS} (id, payload, label) VALUES {rows}"),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("tabella delle sonde di lettura: harness, non divergenza");
    }

    let ascending = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(schema_name.to_owned()),
            object: SCRATCH_ROWS.to_owned(),
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

    // Le tredici forme che il renderer qualifica, ciascuna con il proprio
    // conteggio e la propria prima riga. Una sonda sola con tredici attese, e
    // non tredici sonde, perche la domanda e una: "il filtro decide quali
    // righe escono". Il rifiuto nomina la forma che non ha risposto.
    let mut observed = Vec::new();
    let mut refused: Option<String> = None;
    for case in qualified_filter_forms() {
        let operation = ReadOperation {
            filter: Some(case.expression),
            ..ascending.clone()
        };
        match drain_read(
            provider,
            profile,
            &operation,
            &case.parameters,
            cancellation,
        )
        .await
        {
            Ok(outcome) => {
                let contract = ReadContract {
                    columns: &["id", "payload", "label"],
                    rows: case.rows,
                    batches: None,
                    first_integer: Some(case.first),
                };
                if let Some(mismatch) = read_mismatch(&contract, &outcome) {
                    refused = Some(format!("{}: {mismatch}", case.name));
                    break;
                }
                observed.push(format!("{}={}/{}", case.name, outcome.rows, case.first));
            }
            Err(error) => {
                refused = Some(format!(
                    "{}: {:?}: {}",
                    case.name, error.category, error.message
                ));
                break;
            }
        }
    }
    let question = "ogni forma di filtro qualificata decide quali righe escono";
    match refused {
        None => recorder.accepted(
            "provider.profile_read_filter_forms",
            "provider",
            "profilo",
            question,
            observed.join(" "),
        ),
        Some(mismatch) => recorder.rejected(
            "provider.profile_read_filter_forms",
            "provider",
            "profilo",
            question,
            condense(&mismatch),
            None,
        ),
    }

    // Le due forme che il renderer rifiuta per scelta. Sono nel contratto
    // pubblico e non nella superficie qualificata: senza queste sonde,
    // `filter = true` si leggerebbe come "tutte le forme", che e la lettura
    // che il flag non sostiene.
    //
    // Il rifiuto si verifica per intero — categoria, fase, effetto remoto,
    // retry e causa — e non come "ha dato errore". Un `Err` arriva anche
    // quando la colonna non esiste o il parametro e del tipo sbagliato: il
    // giorno in cui il renderer spatial venisse abilitato, una sonda che si
    // accontenta di `Err` resterebbe verde per la ragione sbagliata, e il
    // fail-close sembrerebbe ancora verificato.
    for (name, probe, expression, parameters, contract) in [
        (
            "like case-insensitive",
            "provider.profile_read_filter_closed_like",
            FilterExpression::Like {
                field: "label".to_owned(),
                parameter: "testo".to_owned(),
                case_insensitive: true,
            },
            ParameterBag::new(std::collections::BTreeMap::from([(
                "testo".to_owned(),
                ParameterValue::String("eti-%".to_owned()),
            )])),
            RefusalContract {
                category: ErrorCategory::Unsupported,
                phase: ErrorPhase::Prepare,
                remote_effect: RemoteEffect::None,
                retry: RetryDisposition::Never,
                message_contains: "LIKE case-insensitive richiede collation esplicita",
            },
        ),
        (
            "filtro spatial",
            "provider.profile_read_filter_closed_spatial",
            FilterExpression::Spatial {
                function: plenora_database_core::query::SpatialFunction::Intersects,
                field: "label".to_owned(),
                geometry_parameter: Some("punto".to_owned()),
                distance_parameter: None,
            },
            // Un WKB vero — `POINT(0 0)`, little endian, ventuno byte — e un
            // campo che **esiste**: cosi il rifiuto non puo venire ne dal
            // parametro ne dalla colonna, e resta solo la regola che la sonda
            // sorveglia. Il renderer rifiuta `Spatial` prima di guardare il
            // tipo della colonna, quindi `label` va bene quanto una geometry —
            // e leggere la tabella spatial vera non andrebbe: il provider la
            // rifiuta prima, per SRID non dichiarato, mascherando la causa.
            //
            // La prima stesura nominava `geom`, che quella tabella non ha, e
            // il rifiuto arrivava da `NotFound`: il fail-close sembrava
            // verificato e non lo era. E il contratto ad averlo scoperto.
            ParameterBag::new(std::collections::BTreeMap::from([(
                "punto".to_owned(),
                ParameterValue::Bytes(vec![
                    0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                ]),
            )])),
            RefusalContract {
                category: ErrorCategory::Unsupported,
                phase: ErrorPhase::Prepare,
                remote_effect: RemoteEffect::None,
                retry: RetryDisposition::Never,
                message_contains: "filtro spatial richiede validazione WKB e SRID",
            },
        ),
    ] {
        let operation = ReadOperation {
            filter: Some(expression),
            ..ascending.clone()
        };
        let question =
            "le forme di filtro non qualificate restano rifiutate, e per la loro ragione";
        match drain_read(provider, profile, &operation, &parameters, cancellation).await {
            // Il fail-close non c'e piu: la forma e passata.
            Ok(outcome) => recorder.accepted(
                probe,
                "provider",
                "profilo",
                question,
                format!("{name}: accettata, righe={}", outcome.rows),
            ),
            Err(error) => match refusal_mismatch(&contract, &error) {
                None => recorder.rejected(
                    probe,
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{name}: {:?}/{:?}/{:?}/{:?}: {}",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        error.message
                    )),
                    None,
                ),
                // Rifiutata, ma non da cio che la sonda sorveglia: la
                // superficie non e stata provata, non e stata verificata.
                Some(mismatch) => recorder.not_measured(
                    probe,
                    "provider",
                    "profilo",
                    question,
                    &format!("{name}: rifiuto per un'altra ragione — {mismatch}"),
                ),
            },
        }
    }

    // Ordinamento: la prima riga dei due versi. Con l'attesa esatta, un
    // ordinamento ignorato — che rende `1` in entrambi i casi — non passa.
    for (probe, direction, first) in [
        (
            "provider.profile_read_ordering_asc",
            SortDirection::Asc,
            1_i64,
        ),
        (
            "provider.profile_read_ordering_desc",
            SortDirection::Desc,
            STREAMING_ROWS_I64,
        ),
    ] {
        let operation = ReadOperation {
            order_by: vec![OrderBy {
                field: "id".to_owned(),
                direction,
            }],
            row_limit: Some(1),
            ..ascending.clone()
        };
        record_read(
            recorder,
            provider,
            profile,
            probe,
            "l'ordinamento dichiarato decide quale riga arriva per prima",
            &operation,
            &ParameterBag::default(),
            &ReadContract {
                columns: &["id", "payload", "label"],
                rows: 1,
                batches: Some(1),
                first_integer: Some(first),
            },
            |outcome| format!("primo={:?}", outcome.first_integer),
            cancellation,
        )
        .await;
    }

    // Streaming: la tabella e piu lunga di un batch di esattamente una riga,
    // quindi due batch e l'unico esito che dimostra il taglio.
    record_read(
        recorder,
        provider,
        profile,
        "provider.profile_read_streaming",
        "una lettura piu lunga di un batch ne consegna piu di uno",
        &ascending,
        &ParameterBag::default(),
        &ReadContract {
            columns: &["id", "payload", "label"],
            rows: STREAMING_ROWS,
            batches: Some(2),
            first_integer: Some(1),
        },
        |outcome| {
            format!(
                "batch={} righe={} digest={}",
                outcome.batches, outcome.rows, outcome.digest
            )
        },
        cancellation,
    )
    .await;

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_ROWS}"))
        .await;
}

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

    // La probe, attraversata con il profilo del prodotto che risponde. E
    // l'unico punto in cui il riconoscimento e la qualifica della versione
    // vengono davvero eseguiti: tutte le altre sonde partono da una sessione
    // gia aperta, e quelle due decisioni le avrebbero saltate.
    //
    // Il bypass e acceso — la misura lo accende sempre — e salta il **rifiuto
    // del prodotto**, non la qualifica: se la versione di questo server non
    // fosse fra quelle dichiarate dal profilo, la sonda lo direbbe qui.
    match pool.checkout(cancellation).await {
        Ok(mut session) => {
            match crate::catalog::probe_server_with_profile(&mut session, profile, cancellation)
                .await
            {
                Ok(probe) => recorder.accepted(
                    "provider.profile_probe",
                    "provider",
                    "profilo",
                    "supera la probe con il profilo del prodotto, qualifica della versione inclusa",
                    format!(
                        "versione={} qualificata={}",
                        probe.product_version,
                        profile.qualified_versions().map_or_else(
                            || "nessun elenco dichiarato".to_owned(),
                            |versions| {
                                versions
                                    .iter()
                                    .map(|(major, minor)| format!("{major}.{minor}"))
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            }
                        ),
                    ),
                ),
                Err(error) => recorder.rejected(
                    "provider.profile_probe",
                    "provider",
                    "profilo",
                    "supera la probe con il profilo del prodotto, qualifica della versione inclusa",
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    crate::evidence::server_code_in_message(&error.message),
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_probe",
            "provider",
            "profilo",
            "supera la probe con il profilo del prodotto, qualifica della versione inclusa",
            condense(&format!("sessione non aperta: {:?}", error.category)),
            None,
        ),
    }

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
    profile_read_probes(recorder, profile, &schema_name, cancellation).await;
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
        // La quaterna, non il solo messaggio: categoria, fase, retry ed
        // effetto remoto sono cio che il chiamante usa per decidere, e sono le
        // cose che una classificazione ereditata sbaglia insieme. Verificarle
        // qui e cio che rende il rifiuto una prova: un 1969 che tornasse nel
        // ramo generico darebbe ancora un `Err`, e senza contratto la sonda
        // resterebbe verde.
        Err(error) => {
            let contract = RefusalContract {
                category: ErrorCategory::Timeout,
                phase: ErrorPhase::Read,
                remote_effect: RemoteEffect::None,
                retry: RetryDisposition::Never,
                message_contains: "timeout",
            };
            match refusal_mismatch(&contract, &error) {
                None => recorder.rejected(
                    "provider.profile_timeout",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{:?}/{:?}/{:?}/{:?}: {}",
                        error.category,
                        error.phase,
                        error.retry,
                        error.remote_effect,
                        error.message
                    )),
                    crate::evidence::server_code_in_message(&error.message),
                ),
                Some(mismatch) => recorder.not_measured(
                    "provider.profile_timeout",
                    "provider",
                    "profilo",
                    question,
                    &format!("il timeout non e stato classificato come tale — {mismatch}"),
                ),
            }
        }
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

    for table in [SCRATCH, SCRATCH_GEO, SCRATCH_LOCK, SCRATCH_ROWS] {
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
