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
    qualified_filter_forms, read_mismatch, refusal_mismatch, ExpressionIndexDdl, ReadContract,
    ReadOutcome, RefusalContract, STREAMING_ROWS, STREAMING_ROWS_I64,
};
use crate::profile::{ProductProfile, MARIADB_PROFILE, MYSQL_PROFILE};
use crate::{MysqlConfig, MysqlProvider, MysqlSession};
use mysql_async::prelude::Queryable;
use plenora_database_core::arrow::array::{Array, Int32Array, Int64Array};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, OrderBy, ReadOperation, SortDirection, TransactionProfile,
    WriteMode, WriteOperation,
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

/// La tabella su cui si prova a **vincolare** un SRID di colonna.
///
/// Separata da [`SCRATCH_GEO`], che serve alle sonde del wire e non deve
/// cambiare forma: qui la DDL viene rifatta due volte, una per sintassi.
const SCRATCH_SRID: &str = "plenora_driver_evidence_srid";

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
const INTERRUPTIBLE_QUERY: &str = "SELECT COUNT(*) FROM information_schema.columns a, \
     information_schema.columns b, information_schema.columns c";

// --------------------------------------------------------------------------
// Famiglia `raw`: il driver contro il server, senza provider in mezzo.
// --------------------------------------------------------------------------

async fn raw_probes(recorder: &mut Recorder, connection: &mut mysql_async::Conn) {
    raw_protocol_probes(recorder, connection).await;
    raw_type_probes(recorder, connection).await;
    returning_form_probe(recorder, connection).await;
}

/// La tabella su cui si misura `RETURNING`.
const SCRATCH_RETURNING: &str = "plenora_driver_evidence_returning";

/// Quali forme di `RETURNING` il server accetta.
///
/// Il compilatore portable rifiuta `RETURNING` su tutto il dialetto `Mysql`, e
/// il commento con cui lo rifiuta dice «`MySQL` non ha `RETURNING` universale
/// (solo 8.0.20+ per `INSERT`)». La prima meta e vera, la seconda no: `MySQL`
/// non ha `RETURNING` a nessuna versione, e la 8.0.20 che il commento cita non
/// c'entra. A confondere le acque e che `MariaDB` **ce l'ha** — da 10.5 su
/// `INSERT`, da molto prima su `DELETE` — e i due prodotti condividono un solo
/// `DialectKind`.
///
/// Il rifiuto e quindi giusto per `MySQL` e troppo largo per `MariaDB`, e la
/// differenza fra le due affermazioni non e deducibile da un commento: questa
/// sonda la misura. Non pretende un esito — e la prima volta che qualcuno pone
/// la domanda a questi riferimenti — ma lo registra per forma, con il codice
/// del server accanto a ogni rifiuto. Una divergenza fra `MySQL` e `MariaDB`
/// qui non e un difetto: e il fatto che serve per decidere se il compilatore
/// possa smettere di trattarli come lo stesso prodotto.
async fn returning_form_probe(recorder: &mut Recorder, connection: &mut mysql_async::Conn) {
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_RETURNING}"),
        format!(
            "CREATE TABLE {SCRATCH_RETURNING} (id INT NOT NULL PRIMARY KEY, \
             payload VARCHAR(32) NOT NULL) ENGINE = InnoDB"
        ),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("tabella di RETURNING: harness, non divergenza");
    }

    // Le quattro forme, ciascuna sulla propria riga: una forma che fallisse
    // per la riga di un'altra misurerebbe l'ordine delle sonde invece del
    // server.
    let forms = [
        (
            "insert",
            format!("INSERT INTO {SCRATCH_RETURNING} (id, payload) VALUES (1, 'a') RETURNING id"),
        ),
        (
            "replace",
            format!("REPLACE INTO {SCRATCH_RETURNING} (id, payload) VALUES (2, 'b') RETURNING id"),
        ),
        (
            "update",
            format!("UPDATE {SCRATCH_RETURNING} SET payload = 'c' WHERE id = 1 RETURNING id"),
        ),
        (
            "delete",
            format!("DELETE FROM {SCRATCH_RETURNING} WHERE id = 2 RETURNING id"),
        ),
    ];
    let mut measured = Vec::with_capacity(forms.len());
    for (name, sql) in forms {
        // `query_drop` basta: la domanda e se il server accetta la forma, non
        // quali valori renda. Le righe rese sono la domanda successiva, e ha
        // senso porla solo dove la prima ha risposto di si.
        let verdict = match connection.query_drop(sql).await {
            Ok(()) => "ok".to_owned(),
            Err(error) => {
                server_code(&error).map_or_else(|| "rifiutato".to_owned(), |code| code.to_string())
            }
        };
        measured.push(format!("{name}={verdict}"));
    }

    recorder.accepted(
        "raw.returning_forms",
        "raw",
        "scrittura",
        "quali forme di RETURNING il server accetta",
        measured.join(" "),
    );

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_RETURNING}"))
        .await;
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
            "INSERT INTO {SCRATCH} VALUES (1, -7, 18446744073709551615, 1234.5678, \
             2.5, '2026-08-18', '2026-08-18 06:00:00.000000', \
             '2026-08-18 06:00:00', '01:02:03.400', 'testo', 0x0102, \
             '{{\"k\": 1}}', 'alfa', 'x,y')"
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
            "CREATE TABLE {SCRATCH_GEO} (id BIGINT NOT NULL PRIMARY KEY, \n shape GEOMETRY NULL) ENGINE = InnoDB"
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

    // Se l'SRID di colonna esista **altrove**, prima di concludere che non
    // esista affatto.
    //
    // Che `SRS_ID` manchi da `information_schema.columns` e misurato, e da
    // solo dice che quella strada non c'e — non che non ce ne siano altre. Il
    // registro OGC e l'altra: `GEOMETRY_COLUMNS` e la tabella che `MySQL` 5.7
    // aveva e la 8.0 ha sostituito con `ST_GEOMETRY_COLUMNS`.
    //
    // # Il predicato si ricava dalla forma, non si indovina
    //
    // La prima stesura di questa sonda interrogava `WHERE TABLE_NAME`, e ha
    // preso 1054 da entrambe le `MariaDB`. Stava per registrare un'assenza
    // che non c'era: nel registro OGC la colonna si chiama `F_TABLE_NAME`, e
    // l'errore riguardava il predicato, non l'SRID. Chiedere prima **quali
    // colonne** il registro abbia, e costruire la query da quelle, e l'unico
    // modo in cui la risposta parla dell'SRID invece che del nome che la sonda
    // ha immaginato.
    let mut geometry_columns = Vec::new();
    for table in ["GEOMETRY_COLUMNS", "ST_GEOMETRY_COLUMNS"] {
        let shape = connection
            .query_first::<Option<String>, _>(format!(
                "SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'information_schema' AND TABLE_NAME = '{table}'"
            ))
            .await;
        let Ok(Some(Some(columns))) = shape else {
            geometry_columns.push(format!("{table}=assente"));
            continue;
        };
        let names: Vec<&str> = columns.split(',').collect();
        let pick = |candidates: &[&str]| -> Option<String> {
            candidates
                .iter()
                .find(|wanted| names.iter().any(|name| name.eq_ignore_ascii_case(wanted)))
                .map(|found| (*found).to_owned())
        };
        let (Some(name_column), Some(srid_column)) = (
            pick(&["F_TABLE_NAME", "TABLE_NAME"]),
            pick(&["SRID", "SRS_ID"]),
        ) else {
            geometry_columns.push(format!("{table}=senza SRID [{columns}]"));
            continue;
        };
        let value = connection
            .query_first::<Option<u32>, _>(format!(
                "SELECT {srid_column} FROM information_schema.{table} WHERE {name_column} = '{SCRATCH_GEO}'"
            ))
            .await;
        geometry_columns.push(match value {
            Ok(Some(srid)) => format!("{table}.{srid_column}={srid:?}"),
            Ok(None) => format!("{table}.{srid_column}=nessuna riga"),
            Err(error) => format!("{table}.{srid_column}=no({})", condense(&error.to_string())),
        });
    }
    let detail = geometry_columns.join(" ");
    // Cio che la sonda cerca e un SRID: un registro che ne rende uno la
    // accetta, uno che non ce l'ha o non c'e la rifiuta. Sono due misure
    // diverse, e il dettaglio le distingue per nome.
    if detail.contains("=Some(") {
        recorder.accepted(
            "raw.geometry_columns_registry",
            "raw",
            "spatial",
            "espone l'SRID di colonna in un registro OGC",
            detail,
        );
    } else {
        recorder.rejected(
            "raw.geometry_columns_registry",
            "raw",
            "spatial",
            "espone l'SRID di colonna in un registro OGC",
            detail,
            None,
        );
    }

    // Un SRID **dichiarato**, e da quale sintassi.
    //
    // Il registro rende `0` su `MariaDB` e `NULL` su `MySQL` per la stessa
    // fixture, che non ne dichiara nessuno: due modi di dire «non vincolata».
    // La domanda che conta e un'altra — se una colonna possa essere vincolata,
    // e con quale attributo — perche da li dipende se il CRS si possa sapere.
    //
    // La prima tranche ha misurato che `SRID` nella DDL e rifiutato da
    // `MariaDB` con un errore di sintassi, e si era fermata li. `REF_SYSTEM_ID`
    // e l'attributo che quel prodotto documenta al suo posto: la sonda prova
    // entrambe le forme su tutti e tre i server e registra quale sia accettata
    // e cosa il registro renda dopo. Provarne una sola direbbe «non si puo»
    // avendo chiesto in una lingua sola.
    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_SRID}"))
        .await;
    let mut declared = Vec::new();
    for (syntax, ddl) in [
        (
            "SRID",
            format!("CREATE TABLE {SCRATCH_SRID} (shape GEOMETRY NOT NULL SRID 4326)"),
        ),
        (
            "REF_SYSTEM_ID",
            format!("CREATE TABLE {SCRATCH_SRID} (shape GEOMETRY NOT NULL REF_SYSTEM_ID=4326)"),
        ),
    ] {
        let _ = connection
            .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_SRID}"))
            .await;
        if let Err(error) = connection.query_drop(ddl).await {
            declared.push(format!(
                "{syntax}=rifiutato({})",
                server_code(&error).map_or(0, |code| code)
            ));
        } else {
            // Il registro si rilegge con la stessa regola di prima: il nome
            // della colonna viene dalla forma, non da un'ipotesi.
            let mut seen = "registro assente".to_owned();
            for (table, name_column, srid_column) in [
                ("GEOMETRY_COLUMNS", "F_TABLE_NAME", "SRID"),
                ("ST_GEOMETRY_COLUMNS", "TABLE_NAME", "SRS_ID"),
            ] {
                if let Ok(Some(value)) = connection
                        .query_first::<Option<u32>, _>(format!(
                            "SELECT {srid_column} FROM information_schema.{table} WHERE {name_column} = '{SCRATCH_SRID}'"
                        ))
                        .await
                    {
                        seen = format!("{table}.{srid_column}={value:?}");
                        break;
                    }
            }
            declared.push(format!("{syntax}=accettato {seen}"));
        }
    }
    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_SRID}"))
        .await;
    let detail = declared.join(" ");
    if detail.contains("=accettato") {
        recorder.accepted(
            "raw.declared_column_srid",
            "raw",
            "spatial",
            "una colonna geometrica puo essere vincolata a un SRID, e il registro lo rende",
            detail,
        );
    } else {
        recorder.rejected(
            "raw.declared_column_srid",
            "raw",
            "spatial",
            "una colonna geometrica puo essere vincolata a un SRID, e il registro lo rende",
            detail,
            None,
        );
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
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
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

    ambiguous_commit_probe(recorder, provider, cancellation).await;
}

/// La tabella su cui si misura un commit di esito ignoto.
const SCRATCH_COMMIT: &str = "plenora_driver_evidence_commit";

/// Un commit **atterrato** di cui il chiamante non sa l'esito.
///
/// Era l'ultima superficie `not_measured` di questo documento, e la ragione
/// dichiarata era buona: uccidere la connessione a meta `COMMIT` da una
/// seconda sessione e una corsa, e un esito ottenuto cosi non distingue il
/// comportamento del provider dal momento in cui e arrivato il colpo.
///
/// La ragione escludeva **quel** metodo, non la misura. Il provider SQL Server
/// di questo repository usa da tempo la forma deterministica — `COMMIT
/// TRANSACTION; WAITFOR DELAY` — e qui vale la stessa: `COMMIT; DO SLEEP(5)`
/// fa atterrare il commit e **poi** trattiene la risposta, quindi la finestra
/// in cui cancellare e larga, ripetibile e sempre nello stesso punto. Il
/// percorso attraversato resta quello di produzione: l'interruttore cambia il
/// testo dello statement, non la logica che ne classifica l'esito.
///
/// # Cosa verifica, e perche la rilettura e il punto
///
/// Che il provider dichiari `OutcomeUnknown` e meta della prova. L'altra meta
/// e che quella dichiarazione sia **onesta**: `Unknown` non vuol dire «non e
/// successo niente», vuol dire «non lo so», e le due si distinguono solo
/// guardando il server da un'altra connessione. Se la riga c'e, il provider ha
/// detto la verita su una scrittura andata a buon fine senza che lui potesse
/// saperlo — che e il caso per cui `OutcomeUnknown` esiste, e il piu
/// pericoloso da sbagliare: un `RolledBack` qui autorizzerebbe un retry che
/// raddoppia la riga.
async fn ambiguous_commit_probe(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    outer: &CancellationToken,
) {
    let mut connection = open_connection().await;
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_COMMIT}"),
        format!("CREATE TABLE {SCRATCH_COMMIT} (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB"),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("tabella del commit: harness, non divergenza");
    }

    let question = "un commit atterrato ma non confermato e dichiarato ignoto, e la riga c'e";
    let budget = read_budget();
    let interrupted = CancellationToken::new();
    let outcome = async {
        let mut transaction = provider
            .begin_transaction(
                &secret(),
                &plenora_database_core::transaction::TransactionOptions::default(),
                &budget,
                outer,
            )
            .await?;
        transaction
            .execute(
                &plenora_database_core::transaction::Statement {
                    sql: format!("INSERT INTO {SCRATCH_COMMIT} (id) VALUES (7)"),
                    params: Vec::new(),
                },
                outer,
            )
            .await?;
        // L'interruttore vale da qui, e si spegne quando la guardia muore: il
        // commit successivo di questo binario non deve trovare cinque secondi
        // di attesa che nessuno ha chiesto.
        let _delayed = crate::session::DelayedCommitResponse::engage();
        let cancel_at = interrupted.clone();
        // La cancellazione arriva **dentro** la finestra, non al suo bordo: un
        // secondo su cinque lascia margine a una macchina lenta senza
        // avvicinarsi alla fine del ritardo.
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            cancel_at.cancel();
        });
        transaction.commit(&interrupted).await
    }
    .await;

    let contents = commit_contents(&mut connection).await;
    match outcome {
        Ok(plenora_database_core::transaction::CommitOutcome::OutcomeUnknown { recovery }) => {
            // La riga **deve** esserci: il commit era atterrato prima che la
            // risposta fosse trattenuta. Se non ci fosse, `Unknown` sarebbe
            // ugualmente lecito ma la sonda avrebbe misurato un altro caso, e
            // registrarlo qui direbbe una cosa per un'altra.
            let mismatch = (contents != "righe=1").then(|| {
                format!("il commit non era atterrato: {contents}; misurato un altro caso")
            });
            match mismatch {
                None => recorder.accepted(
                    "provider.ambiguous_commit",
                    "provider",
                    "commit",
                    question,
                    condense(&format!(
                        "OutcomeUnknown fase_certa={:?} verifica={:?} — {contents}",
                        recovery.last_certain_phase, recovery.verification_action
                    )),
                ),
                Some(reason) => recorder.not_measured(
                    "provider.ambiguous_commit",
                    "provider",
                    "commit",
                    question,
                    &condense(&reason),
                ),
            }
        }
        Ok(other) => recorder.rejected(
            "provider.ambiguous_commit",
            "provider",
            "commit",
            question,
            condense(&format!(
                "il commit ha dichiarato {other:?} invece di un esito ignoto — {contents}"
            )),
            None,
        ),
        Err(error) => recorder.not_measured(
            "provider.ambiguous_commit",
            "provider",
            "commit",
            question,
            &condense(&format!(
                "il commit non e arrivato a dichiarare un esito: {:?}/{:?}: {} — {contents}",
                error.category, error.phase, error.message
            )),
        ),
    }

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_COMMIT}"))
        .await;
}

/// Quante righe la tabella del commit contiene, da un'altra connessione.
async fn commit_contents(connection: &mut mysql_async::Conn) -> String {
    connection
        .query_first::<i64, _>(format!("SELECT COUNT(*) FROM {SCRATCH_COMMIT}"))
        .await
        .map_or_else(
            |error| format!("rilettura non riuscita: {}", condense(&error.to_string())),
            |row| {
                row.map_or_else(
                    || "rilettura senza righe".to_owned(),
                    |n| format!("righe={n}"),
                )
            },
        )
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
    };
    let ordered = |object: &str| ReadOperation {
        source: source(object),
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
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
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
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

    transaction_stream_probes(recorder, provider, &mut connection, cancellation).await;

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_ROWS}"))
        .await;
}

/// Lo stream di righe **dentro la transazione**, misurato su questo prodotto.
///
/// `provider.profile_read_streaming`, qui sopra, misura il percorso Arrow: il
/// lettore consegna piu di un batch. Non e la stessa superficie di
/// `TransactionScope::query_stream`, che apre un result set sul filo e lo fa
/// scorrere mentre la transazione e aperta — l'implementazione e condivisa con
/// `MySQL`, la misura no, e «condivide il codice» e un argomento che questo
/// documento non accetta per nessun'altra bandiera.
///
/// La seconda sonda e quella che conta di piu, e per una ragione che riguarda
/// la storia di questo percorso. La prima stesura di `query_stream`
/// dichiarava che abbandonare un result set a meta rende la connessione
/// inservibile, e faceva fallire con `RequiresRecovery` ogni operazione
/// successiva della transazione. Il riferimento `MySQL` ha smentito:
/// `mysql_async` drena i pacchetti pendenti, e la transazione committa. Se
/// `MariaDB` si comportasse diversamente sarebbe una divergenza vera fra i due
/// prodotti — e sarebbe il tipo di divergenza che non si scopre finche un
/// chiamante non esce da un ciclo con un `break` in produzione.
async fn transaction_stream_probes(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    connection: &mut mysql_async::Conn,
    cancellation: &CancellationToken,
) {
    paginating_stream_probe(recorder, provider, cancellation).await;
    abandoned_stream_probe(recorder, provider, connection, cancellation).await;
}

/// Lo stream consegna i batch della misura dichiarata, e poi la transazione
/// committa.
async fn paginating_stream_probe(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    cancellation: &CancellationToken,
) {
    use plenora_database_core::transaction::{Statement, TransactionOptions};

    let budget = read_budget();
    let batch = 4_096_u32;
    let question = "uno stream dentro la transazione consegna i batch della misura chiesta";

    let paginated = async {
        let mut transaction = provider
            .begin_transaction(
                &secret(),
                &TransactionOptions::default(),
                &budget,
                cancellation,
            )
            .await?;
        let statement = Statement {
            sql: format!("SELECT id FROM {SCRATCH_ROWS} ORDER BY id"),
            params: Vec::new(),
        };
        let mut sizes = Vec::new();
        {
            let mut stream = transaction
                .query_stream(&statement, batch, cancellation)
                .await?;
            while let Some(rows) = stream.next_batch(cancellation).await? {
                sizes.push(rows.len());
            }
        }
        let outcome = transaction.commit(cancellation).await?;
        Ok::<_, plenora_database_core::DatabaseError>((sizes, outcome))
    }
    .await;

    // Il conteggio **per batch**, non il totale: un totale giusto uscirebbe
    // anche da uno stream che consegna tutto in un colpo, cioe da uno stream
    // che non strema.
    let expected_sizes = {
        let mut sizes = vec![batch as usize; STREAMING_ROWS / batch as usize];
        let remainder = STREAMING_ROWS % batch as usize;
        if remainder > 0 {
            sizes.push(remainder);
        }
        sizes
    };
    match paginated {
        Ok((sizes, outcome)) if sizes == expected_sizes && outcome.is_committed() => recorder
            .accepted(
                "provider.transaction_row_stream",
                "provider",
                "profilo",
                question,
                format!("batch={sizes:?} commit=Committed"),
            ),
        Ok((sizes, outcome)) => recorder.rejected(
            "provider.transaction_row_stream",
            "provider",
            "profilo",
            question,
            condense(&format!(
                "atteso {expected_sizes:?} e un commit, misurato {sizes:?} e {outcome:?}"
            )),
            None,
        ),
        Err(error) => recorder.rejected(
            "provider.transaction_row_stream",
            "provider",
            "profilo",
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }
}

/// Uno stream lasciato a meta non impedisce alla transazione di scrivere e
/// committare.
async fn abandoned_stream_probe(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    connection: &mut mysql_async::Conn,
    cancellation: &CancellationToken,
) {
    use plenora_database_core::transaction::{Statement, TransactionOptions};

    let budget = read_budget();
    let marker = STREAMING_ROWS_I64 + 7;
    let abandoned = async {
        let mut transaction = provider
            .begin_transaction(&secret(), &TransactionOptions::default(), &budget, cancellation)
            .await?;
        let statement = Statement {
            sql: format!("SELECT id FROM {SCRATCH_ROWS} ORDER BY id"),
            params: Vec::new(),
        };
        {
            let mut stream = transaction.query_stream(&statement, 8, cancellation).await?;
            // Un batch su mille, e poi lo stream viene lasciato andare senza
            // che nessuno abbia cancellato niente.
            stream.next_batch(cancellation).await?;
        }
        // La scrittura viaggia sulla **stessa** connessione: se i pacchetti
        // non letti fossero rimasti in coda, sarebbe questa a leggerli al
        // posto della propria risposta.
        transaction
            .execute(
                &Statement {
                    sql: format!(
                        "INSERT INTO {SCRATCH_ROWS} (id, payload, label) VALUES ({marker}, 'x', NULL)"
                    ),
                    params: Vec::new(),
                },
                cancellation,
            )
            .await?;
        transaction.commit(cancellation).await
    }
    .await;

    // La rilettura arriva da un'altra connessione: che il commit dica
    // `Committed` e cio che il provider crede, e la riga sul server e cio che
    // e successo.
    let landed = connection
        .query_first::<i64, _>(format!(
            "SELECT COUNT(*) FROM {SCRATCH_ROWS} WHERE id = {marker}"
        ))
        .await
        .map_or_else(
            |error| format!("rilettura non riuscita: {}", condense(&error.to_string())),
            |count| format!("righe={}", count.unwrap_or_default()),
        );
    let question = "una transazione che abbandona uno stream a meta scrive e committa lo stesso";
    match abandoned {
        Ok(outcome) if outcome.is_committed() && landed == "righe=1" => recorder.accepted(
            "provider.transaction_row_stream_abandoned",
            "provider",
            "profilo",
            question,
            format!("commit=Committed — {landed}"),
        ),
        Ok(outcome) => recorder.rejected(
            "provider.transaction_row_stream_abandoned",
            "provider",
            "profilo",
            question,
            condense(&format!("il commit ha dichiarato {outcome:?} — {landed}")),
            None,
        ),
        Err(error) => recorder.rejected(
            "provider.transaction_row_stream_abandoned",
            "provider",
            "profilo",
            question,
            condense(&format!(
                "{:?}/{:?}: {} — {landed}",
                error.category, error.phase, error.message
            )),
            None,
        ),
    }
}

/// La tabella con una colonna generata e un unique index sopra.
///
/// E l'unico modo in cui `MariaDB` indicizza un'espressione: la sintassi
/// dell'indice funzionale non esiste (1064, misurato), quindi chi vuole
/// indicizzare `LOWER(name)` dichiara una colonna generata e indicizza quella.
const SCRATCH_GENERATED: &str = "plenora_driver_evidence_generated";

/// La stessa tabella **senza** chiave primaria: l'unico indice unico e quello
/// sulla colonna generata.
///
/// Serve alla sola forma in cui il preflight dell'Upsert non ha niente da
/// obiettare — l'indice coincide con le keys ed e confrontabile per colonne —
/// e a rendere visibile cosa la fermi allora: una colonna generata non si
/// scrive, e a dirlo e una guardia diversa, piu avanti.
const SCRATCH_GENERATED_ONLY: &str = "plenora_driver_evidence_generated_only";

/// Come il catalogo pubblica una colonna generata e l'indice che la usa, e
/// cosa ne segue per il preflight dell'Upsert.
///
/// E il punto 2 della fase 3. La domanda non e "`MariaDB` sa indicizzare
/// un'espressione" — sa farlo, in un modo solo — ma **come si presenta** al
/// catalogo, perche da li discendono due decisioni: se la colonna sia
/// scrivibile e se l'indice sia confrontabile con le keys di un Upsert.
///
/// La prima ha un rischio che questa tranche esiste per escludere. Su `MariaDB`
/// `GENERATION_EXPRESSION` e NULL per le colonne **non** generate, e il
/// profilo la normalizza con `COALESCE(..., '')` perche il lettore pretende
/// una stringa. Se fosse NULL anche per quelle generate, quella normalizzazione
/// trasformerebbe una colonna non scrivibile in una scrivibile — un fail-open
/// introdotto da una correzione. La sonda lo misura invece di fidarsi.
#[allow(clippy::too_many_lines)]
async fn generated_column_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let mut connection = open_connection().await;
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_GENERATED}"),
        format!(
            "CREATE TABLE {SCRATCH_GENERATED} (id INT NOT NULL PRIMARY KEY, \
             name VARCHAR(32) NOT NULL, \
             lname VARCHAR(32) AS (LOWER(name)) VIRTUAL, \
             UNIQUE KEY uq_lname (lname)) ENGINE = InnoDB"
        ),
        format!("DROP TABLE IF EXISTS {SCRATCH_GENERATED_ONLY}"),
        format!(
            "CREATE TABLE {SCRATCH_GENERATED_ONLY} (name VARCHAR(32) NOT NULL, \
             lname VARCHAR(32) AS (LOWER(name)) VIRTUAL, \
             UNIQUE KEY uq_lname (lname)) ENGINE = InnoDB"
        ),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("tabella della colonna generata: harness, non divergenza");
    }

    // Cosa il catalogo dice, letto senza profilo: e l'ingresso da cui tutto il
    // resto discende.
    let catalogued: Result<Option<(String, String, String, i64)>, _> = connection
        .query_first(format!(
            "SELECT c.EXTRA, IFNULL(c.GENERATION_EXPRESSION, '<NULL>'), \
             IFNULL(s.COLUMN_NAME, '<NULL>'), s.NON_UNIQUE \
             FROM information_schema.columns c \
             JOIN information_schema.statistics s \
             ON s.TABLE_SCHEMA = c.TABLE_SCHEMA AND s.TABLE_NAME = c.TABLE_NAME \
             WHERE c.TABLE_SCHEMA = '{schema_name}' \
             AND c.TABLE_NAME = '{SCRATCH_GENERATED}' \
             AND c.COLUMN_NAME = 'lname' AND s.INDEX_NAME = 'uq_lname'"
        ))
        .await;
    // Tre esiti, non due: la riga c'e, la riga non c'e, oppure la domanda non
    // e arrivata. Il terzo si confondeva con il secondo — `.ok().flatten()`
    // trasformava un privilegio mancante o una query incompatibile in
    // "assente" — e un'assenza inventata e un fatto registrato che non e mai
    // stato osservato.
    match catalogued {
        Ok(Some((extra, expression, indexed, non_unique))) => recorder.accepted(
            "raw.generated_column_catalog",
            "raw",
            "catalogo",
            "come il catalogo pubblica una colonna generata e l'indice che la usa",
            format!(
                "extra={extra} espressione={expression} indice_su={indexed} non_unique={non_unique}"
            ),
        ),
        Ok(None) => recorder.rejected(
            "raw.generated_column_catalog",
            "raw",
            "catalogo",
            "come il catalogo pubblica una colonna generata e l'indice che la usa",
            "il catalogo non ha reso la riga attesa".to_owned(),
            None,
        ),
        Err(error) => recorder.not_measured(
            "raw.generated_column_catalog",
            "raw",
            "catalogo",
            "come il catalogo pubblica una colonna generata e l'indice che la usa",
            &format!(
                "la domanda non e arrivata: {}{}",
                condense(&error.to_string()),
                server_code(&error).map_or_else(String::new, |code| format!(" (codice {code})"))
            ),
        ),
    }

    let pool = crate::MysqlPool::new_with_profile(&config(), 2, profile)
        .expect("pool della misura: harness, non divergenza");
    let question = "come il profilo descrive la colonna generata e il suo indice";
    match pool.checkout(cancellation).await {
        Ok(mut session) => {
            match crate::catalog::describe_object_with_profile(
                &mut session,
                schema_name,
                SCRATCH_GENERATED,
                profile,
                cancellation,
            )
            .await
            {
                Ok(description) => {
                    let indexes = description
                        .indexes
                        .iter()
                        .map(|index| {
                            format!(
                                "{}:{}/unico={}/confrontabile={}",
                                index.name,
                                index.columns.join("+"),
                                index.unique,
                                index.column_backed
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    // La struttura attesa, per intero. Registrare cio che si
                    // vede e diverso dal verificare che sia cio che serve: da
                    // questa forma dipendono due decisioni — se la colonna sia
                    // scrivibile e se l'indice sia confrontabile con le keys —
                    // e una descrizione che perdesse `lname`, o rendesse
                    // l'indice non unico, le cambierebbe entrambe restando
                    // verde.
                    let mismatch = crate::evidence::generated_index_mismatch(
                        &description,
                        "lname",
                        "uq_lname",
                    );
                    match mismatch {
                        None => recorder.accepted(
                            "provider.profile_generated_index",
                            "provider",
                            "profilo",
                            question,
                            format!("espressione_non_vuota=true {indexes}"),
                        ),
                        Some(reason) => recorder.rejected(
                            "provider.profile_generated_index",
                            "provider",
                            "profilo",
                            question,
                            condense(&format!("contratto non soddisfatto: {reason} — {indexes}")),
                            None,
                        ),
                    }
                }
                Err(error) => recorder.rejected(
                    "provider.profile_generated_index",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!("{:?}: {}", error.category, error.message)),
                    crate::evidence::server_code_in_message(&error.message),
                ),
            }
        }
        Err(error) => recorder.not_measured(
            "provider.profile_generated_index",
            "provider",
            "profilo",
            question,
            &format!(
                "sessione non aperta: {:?}: {}",
                error.category, error.message
            ),
        ),
    }

    upsert_preflight_probes(recorder, profile, schema_name, cancellation).await;

    for table in [SCRATCH_GENERATED, SCRATCH_GENERATED_ONLY] {
        let _ = connection
            .query_drop(format!("DROP TABLE IF EXISTS {table}"))
            .await;
    }
}

/// Cosa il preflight dell'Upsert decide su una tabella con un unique index su
/// colonna generata.
///
/// Il preflight non e una formalita: `ON DUPLICATE KEY UPDATE` scatta su
/// **qualsiasi** unique index in conflitto, non solo sulle keys dichiarate,
/// quindi un secondo indice unico su colonne diverse aggiornerebbe in silenzio
/// la riga sbagliata. Le due forme qui sotto sono le sole che una tabella cosi
/// permette, e devono essere **entrambe** rifiutate — per ragioni diverse, che
/// e cio che le sonde verificano.
#[allow(clippy::too_many_lines)]
async fn upsert_preflight_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let provider = MysqlProvider::with_profile(config(), 2, profile)
        .expect("provider della misura: harness, non divergenza");
    let budget = read_budget();
    let field = |name: &str, kind: plenora_database_core::arrow::schema::DataType| {
        plenora_database_core::arrow::schema::Field::new(name, kind, false)
    };
    let schema = |fields: Vec<plenora_database_core::arrow::schema::Field>| {
        std::sync::Arc::new(plenora_database_core::arrow::schema::Schema::new(fields))
    };
    let text = plenora_database_core::arrow::schema::DataType::Utf8;
    let integer = plenora_database_core::arrow::schema::DataType::Int32;

    // Le tre forme che una tabella con un indice unico su colonna generata
    // permette, e cosa deve fermarle. Nessuna e sicura, e nessuna deve
    // arrivare al server: cambia **chi** le ferma, ed e questo che la sonda
    // registra.
    for (probe, object, keys, columns, question, category, expected) in [
        (
            "provider.profile_upsert_on_primary_key",
            SCRATCH_GENERATED,
            vec!["id".to_owned()],
            vec![field("id", integer.clone()), field("name", text.clone())],
            "l'Upsert sulla chiave primaria si ferma davanti al secondo indice unico",
            ErrorCategory::Unsupported,
            "altro PK/UNIQUE index",
        ),
        (
            "provider.profile_upsert_on_generated_key",
            SCRATCH_GENERATED,
            vec!["lname".to_owned()],
            vec![
                field("id", integer.clone()),
                field("name", text.clone()),
                field("lname", text.clone()),
            ],
            "l'Upsert sulla colonna generata si ferma davanti alla chiave primaria",
            ErrorCategory::Unsupported,
            "altro PK/UNIQUE index",
        ),
        (
            // La forma in cui il preflight sugli indici non ha niente da
            // obiettare: l'indice unico coincide con le keys ed e
            // confrontabile per colonne. A fermare l'Upsert e allora un'altra
            // guardia — la colonna generata non si scrive — e sapere **quale**
            // delle due regge e il punto: se domani cadesse questa, il
            // preflight direbbe ancora di si.
            "provider.profile_upsert_generated_anchor",
            SCRATCH_GENERATED_ONLY,
            vec!["lname".to_owned()],
            vec![field("name", text.clone()), field("lname", text.clone())],
            "l'Upsert ancorato alla colonna generata si ferma perche quella colonna non si scrive",
            ErrorCategory::DataMapping,
            "colonna generata",
        ),
    ] {
        let operation = WriteOperation {
            target: ObjectRef {
                catalog: None,
                schema: Some(schema_name.to_owned()),
                object: object.to_owned(),
            },
            mode: WriteMode::Upsert,
            mapping_policy: MappingPolicy::Strict,
            transaction_profile: TransactionProfile::SingleTransaction,
            keys,
            // Vuote: le colonne da aggiornare si dichiarano solo per
            // `WriteMode::Update`, e la prima stesura le passava anche
            // all'Upsert — il piano rifiutava prima, e il preflight sugli
            // indici non veniva mai raggiunto. La sonda lo ha detto invece di
            // registrare quel rifiuto come se fosse il suo.
            update_columns: Vec::new(),
            srid_policy: None,
            create_spatial_index: false,
            allow_partial: false,
        };
        match provider
            .prepare_write(
                &secret(),
                &operation,
                schema(columns),
                &budget,
                cancellation,
            )
            .await
        {
            // Un Upsert preparato qui sarebbe la notizia: significherebbe che
            // nessuna delle due guardie ha visto quello che c'era da vedere.
            Ok(_) => recorder.accepted(
                probe,
                "provider",
                "profilo",
                question,
                "preparato: nessuna guardia ha rifiutato".to_owned(),
            ),
            Err(error) => {
                let contract = RefusalContract {
                    category,
                    phase: ErrorPhase::Prepare,
                    remote_effect: RemoteEffect::None,
                    retry: RetryDisposition::Never,
                    message_contains: expected,
                };
                match refusal_mismatch(&contract, &error) {
                    None => recorder.rejected(
                        probe,
                        "provider",
                        "profilo",
                        question,
                        condense(&format!(
                            "{:?}/{:?}/{:?}/{:?}: {}",
                            error.category,
                            error.phase,
                            error.remote_effect,
                            error.retry,
                            error.message
                        )),
                        None,
                    ),
                    Some(mismatch) => recorder.not_measured(
                        probe,
                        "provider",
                        "profilo",
                        question,
                        &format!("rifiuto per un'altra ragione — {mismatch}"),
                    ),
                }
            }
        }
    }
}

/// La tabella su cui si misura la scrittura in Append.
const SCRATCH_APPEND: &str = "plenora_driver_evidence_append";

/// Uno stream di batch che sa anche interrompere la corsa.
///
/// La cancellazione ha bisogno di una **barriera osservabile**, non di un
/// timeout: un `cancel()` dopo N millisecondi cade in un punto diverso a ogni
/// corsa, e una sonda che misura un punto diverso ogni volta non misura
/// niente. Qui il punto e dichiarato — la richiesta del batch numero `at` — e
/// a quel momento il primo batch e gia sul server, dentro la transazione
/// ancora aperta.
struct ScriptedBatches {
    schema: plenora_database_core::arrow::SchemaRef,
    batches: std::collections::VecDeque<plenora_database_core::arrow::RecordBatch>,
    declared: u64,
    /// Quale richiesta cancella la corsa, e quale token cancellare.
    cancel_at: Option<(usize, CancellationToken)>,
    served: usize,
}

impl plenora_database_core::provider::BatchStream for ScriptedBatches {
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
        self.served += 1;
        if let Some((at, token)) = &self.cancel_at {
            if self.served == *at {
                token.cancel();
            }
        }
        Box::pin(async move { Ok(self.batches.pop_front()) })
    }

    fn declared_input_rows(&self) -> Option<u64> {
        Some(self.declared)
    }
}

/// Lo schema di ingresso della scrittura: due colonne, nessuna generata.
fn scratch_schema() -> plenora_database_core::arrow::SchemaRef {
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]))
}

/// Un batch con gli id dati, e un payload derivato da ciascuno.
fn scratch_batch(ids: &[i32]) -> plenora_database_core::arrow::RecordBatch {
    scratch_batch_labelled("riga", ids)
}

/// Un batch il cui payload porta il prefisso dato.
///
/// `Update` ha bisogno di distinguere il valore di prima da quello di dopo:
/// una sonda che riscrivesse lo stesso payload non misurerebbe niente, perche
/// su questi motori `affected_rows` conta le righe **cambiate** e un
/// aggiornamento che non cambia nulla dichiara zero.
/// Lo schema di ingresso di `DeleteByKeys`: la sola colonna chiave.
///
/// Non e una semplificazione dell'harness, e il contratto della mode: il
/// piano rifiuta uno schema che porti colonne non-key — «`DeleteByKeys`:
/// colonna 'payload' non e una key» — e ha ragione, perche una cancellazione
/// non ha nulla da fare dei valori. La prima stesura di queste tre sonde
/// mandava lo schema comune a due colonne e veniva respinta in `prepare`: un
/// rifiuto legittimo, ma di una domanda diversa da quella che la sonda
/// poneva.
fn key_schema() -> plenora_database_core::arrow::SchemaRef {
    use plenora_database_core::arrow::schema::{DataType, Field, Schema};
    std::sync::Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
}

/// Un batch di sole chiavi, per `DeleteByKeys`.
fn key_batch(ids: &[i32]) -> plenora_database_core::arrow::RecordBatch {
    use plenora_database_core::arrow::array::Int32Array;
    plenora_database_core::arrow::RecordBatch::try_new(
        key_schema(),
        vec![std::sync::Arc::new(Int32Array::from(ids.to_vec()))],
    )
    .expect("batch della misura: harness, non divergenza")
}

fn scratch_batch_labelled(prefix: &str, ids: &[i32]) -> plenora_database_core::arrow::RecordBatch {
    let rows: Vec<(i32, String)> = ids
        .iter()
        .map(|id| (*id, format!("{prefix}-{id}")))
        .collect();
    scratch_batch_pairs(&rows)
}

/// Un batch in cui ogni riga porta il payload che la sonda le assegna.
///
/// Il prefisso non basta piu da quando una sonda deve costruire un conflitto:
/// far collidere due righe vuol dire dare a un id il payload **di un'altra**, e
/// un valore derivato dall'id non puo esprimerlo. La prima stesura della sonda
/// di rollback dell'Update ci e cascata — assegnava all'id 4 il valore che
/// aveva gia, quindi un no-op invece di un duplicato, e la scrittura riusciva
/// dove doveva fallire. La misura l'ha detto, ed e per questo che esiste
/// questa forma.
fn scratch_batch_pairs(rows: &[(i32, String)]) -> plenora_database_core::arrow::RecordBatch {
    use plenora_database_core::arrow::array::{Int32Array, StringArray};
    let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
    let payloads: Vec<String> = rows.iter().map(|(_, payload)| payload.clone()).collect();
    plenora_database_core::arrow::RecordBatch::try_new(
        scratch_schema(),
        vec![
            std::sync::Arc::new(Int32Array::from(ids)),
            std::sync::Arc::new(StringArray::from(payloads)),
        ],
    )
    .expect("batch della misura: harness, non divergenza")
}

/// Cosa non torna nell'esito pubblicato da una scrittura riuscita.
///
/// L'outcome e contratto: chi lo riceve ci legge lo stato, il prodotto che ha
/// scritto e la contabilita delle righe. Verificarne due campi lascerebbe
/// passare un `Committed` che dichiara zero righe ricevute, o un esito
/// attribuito al prodotto sbagliato — e sono proprio le cose su cui il
/// chiamante costruisce la propria.
/// La contabilita che una mode deve pubblicare, dichiarata dalla sonda.
///
/// Era un numero solo, e bastava finche le mode misurate scrivevano righe
/// nuove: `Append` e `Create` inseriscono cio che ricevono, quindi «sei» le
/// descriveva per intero. `Update` no — aggiorna invece di inserire, e una
/// chiave che non trova riscontro non e un errore ma una riga **saltata** —
/// e con un numero solo l'attesa sarebbe stata scritta dentro il
/// confronto invece che dalla sonda che sa cosa ha chiesto.
///
/// `inserted`, `updated` e `deleted` sono `Option` come nel contratto, dove
/// `None` non e zero: significa «non pertinente». La sonda dichiara quale
/// dei due intende, perche `None` passato per buono nasconde un esito che
/// non sa cosa abbia fatto.
struct ExpectedCounts {
    received: u64,
    confirmed: u64,
    inserted: Option<u64>,
    updated: Option<u64>,
    deleted: Option<u64>,
    skipped: u64,
}

impl ExpectedCounts {
    /// La forma di una mode che inserisce cio che riceve: `Append`, `Create`.
    const fn inserting(rows: u64) -> Self {
        Self {
            received: rows,
            confirmed: rows,
            inserted: Some(rows),
            updated: Some(0),
            deleted: Some(0),
            skipped: 0,
        }
    }
}

fn outcome_mismatch(
    outcome: &plenora_database_core::outcome::WriteOutcome,
    profile: &dyn ProductProfile,
    expected: &ExpectedCounts,
) -> Option<String> {
    use plenora_database_core::outcome::WriteStatus;
    if outcome.status != WriteStatus::Committed {
        return Some(format!("stato {:?} invece di Committed", outcome.status));
    }
    if outcome.provider != profile.kind() {
        return Some(format!(
            "esito attribuito a {:?} invece che a {:?}",
            outcome.provider,
            profile.kind()
        ));
    }
    let rows = &outcome.rows;
    if rows.received != expected.received || rows.confirmed != expected.confirmed {
        return Some(format!(
            "ricevute {} e confermate {}, attese {} e {}",
            rows.received, rows.confirmed, expected.received, expected.confirmed
        ));
    }
    if rows.inserted != expected.inserted
        || rows.updated != expected.updated
        || rows.deleted != expected.deleted
    {
        return Some(format!(
            "inserite {:?}, aggiornate {:?}, cancellate {:?}; attese {:?}, {:?} e {:?}",
            rows.inserted,
            rows.updated,
            rows.deleted,
            expected.inserted,
            expected.updated,
            expected.deleted
        ));
    }
    if rows.failed != 0 || rows.skipped != expected.skipped {
        return Some(format!(
            "fallite {} e saltate {}, attese 0 e {}",
            rows.failed, rows.skipped, expected.skipped
        ));
    }
    if outcome.recovery.is_some() {
        return Some("l'esito porta un recovery che una scrittura riuscita non ha".to_owned());
    }
    if outcome.schema_version != 2 {
        return Some(format!(
            "schema_version {} invece di 2",
            outcome.schema_version
        ));
    }
    if outcome.execution_id.is_empty() {
        return Some("execution_id vuoto: l'esito non e rintracciabile".to_owned());
    }
    // E l'esito deve essere coerente con se stesso, secondo il core: e la
    // stessa verifica che fa chi lo riceve.
    if let Err(error) = outcome.validate() {
        return Some(format!(
            "esito non valido per il contratto: {}",
            error.message
        ));
    }
    None
}

/// Cosa c'e nella tabella, letto da **un'altra** connessione.
///
/// La rilettura non passa dalla sessione che ha scritto, e non e un dettaglio:
/// una scrittura che dichiara sei righe e una tabella che ne contiene sei sono
/// due affermazioni diverse, e la seconda si verifica solo da fuori — dopo il
/// commit, con una connessione che non ha visto la transazione.
async fn table_contents(connection: &mut mysql_async::Conn, table: &str) -> String {
    connection
        .query_first::<(i64, Option<String>), _>(format!(
            "SELECT COUNT(*), GROUP_CONCAT(CONCAT(id, ':', payload) ORDER BY id) \
             FROM {table}"
        ))
        .await
        .map_or_else(
            |error| format!("rilettura non riuscita: {}", condense(&error.to_string())),
            |row| {
                row.map_or_else(
                    || "rilettura senza righe".to_owned(),
                    |(count, joined)| {
                        format!("righe={count} contenuto={}", joined.unwrap_or_default())
                    },
                )
            },
        )
}

/// Esegue una scrittura Append con lo stream dato e restituisce l'esito.
async fn scripted_write(
    provider: &MysqlProvider,
    operation: &WriteOperation,
    schema: plenora_database_core::arrow::SchemaRef,
    batches: Vec<plenora_database_core::arrow::RecordBatch>,
    declared: u64,
    cancel_at: Option<(usize, CancellationToken)>,
    cancellation: &CancellationToken,
) -> Result<plenora_database_core::outcome::WriteOutcome, plenora_database_core::DatabaseError> {
    let budget = read_budget();
    let prepared = provider
        .prepare_write(&secret(), operation, schema.clone(), &budget, cancellation)
        .await?;
    let stream = ScriptedBatches {
        schema,
        batches: batches.into_iter().collect(),
        declared,
        cancel_at,
        served: 0,
    };
    provider
        .write(&secret(), prepared, Box::new(stream), &budget, cancellation)
        .await
}

/// Riporta la tabella dell'append allo stato noto.
async fn reset_append(connection: &mut mysql_async::Conn) {
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_APPEND}"),
        format!(
            "CREATE TABLE {SCRATCH_APPEND} (id INT NOT NULL PRIMARY KEY, \
             payload VARCHAR(32) NOT NULL) ENGINE = InnoDB"
        ),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("tabella dell'append: harness, non divergenza");
    }
}

/// La mode `Append` attraversata con il profilo del prodotto, per intero.
///
/// Punto 3 della fase 3, una mode alla volta. `Append` e la piu semplice —
/// nessun DDL, nessuna keys — e proprio per questo e quella su cui si decide
/// **come** si misura una scrittura: la riuscita si verifica rileggendo da
/// un'altra sessione, il rollback pretende due batch di cui il primo arrivato
/// davvero al server, e la cancellazione una barriera dichiarata invece di un
/// timeout.
///
/// `writes.append` resta chiusa finche le tre sonde non sono verdi su
/// entrambi i riferimenti: la capability si apre nel commit che le vede
/// passare, non in quello che le scrive.
#[allow(clippy::too_many_lines)]
async fn append_write_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let provider = MysqlProvider::with_profile(config(), 2, profile)
        .expect("provider della misura: harness, non divergenza");
    let mut connection = open_connection().await;
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(schema_name.to_owned()),
            object: SCRATCH_APPEND.to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };

    // 1. La scrittura riuscita. Due batch, sei righe, e la verifica da
    //    un'altra connessione **dopo** il commit: cio che il provider dichiara
    //    di aver scritto e cio che la tabella contiene sono due affermazioni
    //    diverse, e la seconda si legge solo da fuori.
    reset_append(&mut connection).await;
    let question = "scrive in Append, e le righe si rileggono da un'altra sessione";
    let expected = "righe=6 contenuto=1:riga-1,2:riga-2,3:riga-3,4:riga-4,5:riga-5,6:riga-6";
    match scripted_write(
        &provider,
        &operation,
        scratch_schema(),
        vec![scratch_batch(&[1, 2, 3]), scratch_batch(&[4, 5, 6])],
        6,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => {
            let contents = table_contents(&mut connection, SCRATCH_APPEND).await;
            // L'outcome e contratto pubblico: chi lo riceve ci legge lo stato,
            // il prodotto che ha scritto e la contabilita delle righe. Due
            // campi su otto non lo presidiano — un `Committed` che dichiarasse
            // zero righe ricevute, o il prodotto sbagliato, sarebbe passato.
            let accounting = outcome_mismatch(&outcome, profile, &ExpectedCounts::inserting(6));
            if accounting.is_none() && contents == expected {
                recorder.accepted(
                    "provider.profile_write_append",
                    "provider",
                    "profilo",
                    question,
                    format!("dichiarate={} {contents}", outcome.rows.confirmed),
                );
            } else {
                recorder.rejected(
                    "provider.profile_write_append",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "contratto non soddisfatto: {}; atteso [{expected}], \
                         osservato [{contents}]",
                        accounting.unwrap_or_else(|| "contabilita in ordine".to_owned())
                    )),
                    None,
                );
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_write_append",
            "provider",
            "profilo",
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            crate::evidence::server_code_in_message(&error.message),
        ),
    }

    // 2. Il rollback. Due batch: il primo arriva al server e ci resta finche
    //    la transazione e aperta, il secondo duplica una chiave primaria e la
    //    fa abortire. Un errore di mapping o di preflight non proverebbe
    //    niente — non avrebbe mai scritto nulla da annullare.
    reset_append(&mut connection).await;
    let question = "un secondo batch rifiutato dal server annulla anche il primo";
    // La quaterna misurata, non quella attesa a tavolino: la chiave duplicata
    // non arriva come conflitto ma come **rifiuto di riga** — il piano di
    // scrittura la classifica cosi — e l'effetto dichiarato e `RolledBack`,
    // che e cio che la rilettura conferma.
    let contract = RefusalContract {
        category: ErrorCategory::DataMapping,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::RolledBack,
        retry: RetryDisposition::Never,
        message_contains: "riga sorgente rifiutata",
    };
    match scripted_write(
        &provider,
        &operation,
        scratch_schema(),
        vec![scratch_batch(&[10, 11, 12]), scratch_batch(&[10])],
        4,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => recorder.accepted(
            "provider.profile_write_append_rollback",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la chiave duplicata e passata, righe={}",
                outcome.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(&mut connection, SCRATCH_APPEND).await;
            let mismatch = refusal_mismatch(&contract, &error).or_else(|| {
                (contents != "righe=0 contenuto=")
                    .then(|| format!("il primo batch non e stato annullato: {contents}"))
            });
            match mismatch {
                None => recorder.rejected(
                    "provider.profile_write_append_rollback",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{:?}/{:?}/{:?}/{:?}: {} — {contents}",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        error.message
                    )),
                    crate::evidence::server_code_in_message(&error.message),
                ),
                // Il quadro osservato entra nel dettaglio: senza, il verdetto
                // dice cosa non torna ma non cosa e successo, e la correzione
                // del contratto diventa un'altra corsa.
                Some(reason) => recorder.not_measured(
                    "provider.profile_write_append_rollback",
                    "provider",
                    "profilo",
                    question,
                    &format!(
                        "il rollback non e stato osservato — {reason}; osservato \
                          {:?}/{:?}/{:?}/{:?}: {} — {contents}",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        condense(&error.message)
                    ),
                ),
            }
        }
    }

    append_cancellation_probe(
        recorder,
        &provider,
        profile,
        &operation,
        &mut connection,
        cancellation,
    )
    .await;

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_APPEND}"))
        .await;
}

/// La cancellazione a meta scrittura, su una barriera dichiarata.
///
/// Il token si annulla quando il provider chiede il **secondo** batch: a quel
/// punto il primo e gia sul server, dentro la transazione ancora aperta. Un
/// timeout cadrebbe ogni volta in un punto diverso, e una sonda che misura un
/// punto diverso ogni volta non misura niente.
///
/// Cio che si verifica non e solo l'errore: le righe residue lette da
/// un'altra connessione, e il fatto che il provider **resti usabile** — la
/// scrittura successiva scrive le sue righe, e la tabella contiene quelle e
/// nient'altro. Una sessione riusata con una transazione residua committerebbe
/// anche le righe di prima, e il solo conteggio dichiarato non lo direbbe.
///
/// Che la connessione sia stata **chiusa e sostituita** questa sonda non lo
/// osserva: lo dichiara il messaggio del provider, che e cosa afferma e non
/// cosa e successo. Osservarlo vorrebbe dire guardare l'identita della
/// sessione in `information_schema.processlist`, come fa il test live sulla
/// quarantena del pool `MySQL`. Finche non lo fa, la sonda dice quello che
/// vede.
///
/// Lunga come la gemella di `Create`, e per la stessa ragione: cio che
/// verifica sono cinque cose in fila — la quaterna del rifiuto, le righe
/// residue, la ripresa e cosa la tabella contiene dopo — e spezzarla
/// separerebbe l'attesa dal confronto.
#[allow(clippy::too_many_lines)]
async fn append_cancellation_probe(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    profile: &'static dyn ProductProfile,
    operation: &WriteOperation,
    connection: &mut mysql_async::Conn,
    outer: &CancellationToken,
) {
    reset_append(connection).await;
    let question =
        "una cancellazione a meta scrittura non lascia righe, e il provider resta usabile";
    let interrupted = CancellationToken::new();
    // L'effetto e **ignoto**, e non e una lacuna: da quel lato il provider non
    // puo sapere se il server avesse applicato, e dichiararlo `RolledBack`
    // sarebbe una promessa che non e in grado di mantenere. Cosa sia successo
    // davvero lo dice la rilettura da un'altra connessione, ed e per questo
    // che la sonda la fa. `RequiresRecovery` e la conseguenza: il chiamante
    // deve ripulire, non ritentare.
    let contract = RefusalContract {
        category: ErrorCategory::Cancelled,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        // Il frammento non nomina il prodotto: il messaggio lo porta gia, e
        // ciascun profilo ci mette il proprio.
        message_contains: "quarantinata",
    };
    let outcome = scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![scratch_batch(&[20, 21, 22]), scratch_batch(&[23, 24, 25])],
        6,
        Some((2, interrupted.clone())),
        &interrupted,
    )
    .await;
    match outcome {
        Ok(written) => recorder.accepted(
            "provider.profile_write_append_cancellation",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la cancellazione non ha interrotto, righe={}",
                written.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_APPEND).await;
            // Il provider si riprende? La scrittura successiva usa lo stesso
            // pool: se la sessione cancellata fosse tornata dentro senza
            // quarantena, questa erediterebbe una transazione aperta.
            let recovered = scripted_write(
                provider,
                operation,
                scratch_schema(),
                vec![scratch_batch(&[30, 31])],
                2,
                None,
                outer,
            )
            .await;
            let after = table_contents(connection, SCRATCH_APPEND).await;
            let mismatch = refusal_mismatch(&contract, &error)
                .or_else(|| {
                    (contents != "righe=0 contenuto=")
                        .then(|| format!("la cancellazione ha lasciato righe: {contents}"))
                })
                .or_else(|| match &recovered {
                    Ok(written) => {
                        outcome_mismatch(written, profile, &ExpectedCounts::inserting(2))
                            .or_else(|| {
                                // La tabella, non il conteggio dichiarato: una
                                // sessione riusata con una transazione residua
                                // committerebbe le tre righe di prima **piu** le
                                // due nuove, e il conteggio direbbe comunque due.
                                (after != "righe=2 contenuto=30:riga-30,31:riga-31").then(|| {
                                    format!("dopo la ripresa la tabella contiene [{after}]")
                                })
                            })
                            .map(|reason| format!("la ripresa non e quella attesa: {reason}"))
                    }
                    Err(next) => Some(format!(
                        "il provider non e piu utilizzabile: {:?}: {}",
                        next.category, next.message
                    )),
                });
            match mismatch {
                None => recorder.rejected(
                    "provider.profile_write_append_cancellation",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{:?}/{:?}/{:?}/{:?}: {} — dopo la cancellazione [{contents}], \
                         dopo la ripresa [{after}]",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        error.message
                    )),
                    None,
                ),
                Some(reason) => recorder.not_measured(
                    "provider.profile_write_append_cancellation",
                    "provider",
                    "profilo",
                    question,
                    &format!(
                        "la cancellazione non e stata osservata per intero — {reason}; \
                         osservato {:?}/{:?}/{:?}/{:?}: {} — dopo \
                         [{contents}], ripresa [{after}]",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        condense(&error.message)
                    ),
                ),
            }
        }
    }
}

/// La tabella che `Create` deve costruire da sola.
///
/// A differenza di quella dell'Append, l'harness non la crea: crearla e cio
/// che la mode fa, e trovarla gia li misurerebbe un'altra cosa.
const SCRATCH_CREATE: &str = "plenora_driver_evidence_create";

/// Cosa il catalogo dice della tabella creata: il contratto, e la resa.
///
/// Due stringhe e non una, perche dicono due cose diverse. Nomi, ordine,
/// nullability e chiave primaria sono cio che `Create` promette, e devono
/// coincidere sui tre server. I tipi nativi no: sono la resa del catalogo, e
/// `INT` esce `int` da `MySQL` e `int(11)` da `MariaDB` — la stessa divergenza
/// che la quinta tranche ha gia registrato su `bigint(20)`. Metterli nel
/// contratto renderebbe rossa una sonda per una differenza gia misurata e
/// capita; tenerli fuori dal dettaglio la nasconderebbe.
///
/// La tabella assente non e una stringa vuota: e detto, altrimenti
/// «nessuna colonna» e «nessuna tabella» si leggerebbero uguale, e sono i due
/// esiti che le sonde del residuo devono distinguere.
async fn create_shape(connection: &mut mysql_async::Conn) -> (String, String) {
    let columns = connection
        .query_first::<(Option<String>, Option<String>), _>(format!(
            "SELECT GROUP_CONCAT(CONCAT(COLUMN_NAME, '/', IS_NULLABLE) \
                      ORDER BY ORDINAL_POSITION), \
                    GROUP_CONCAT(CONCAT(COLUMN_NAME, ':', COLUMN_TYPE) \
                      ORDER BY ORDINAL_POSITION) \
             FROM information_schema.COLUMNS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{SCRATCH_CREATE}'"
        ))
        .await
        .ok()
        .flatten();
    let primary = connection
        .query_first::<Option<String>, _>(format!(
            "SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) \
             FROM information_schema.STATISTICS \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{SCRATCH_CREATE}' \
               AND INDEX_NAME = 'PRIMARY'"
        ))
        .await
        .ok()
        .flatten()
        .flatten();
    match columns {
        Some((Some(shape), types)) => (
            format!("colonne={shape} pk={}", primary.unwrap_or_default()),
            format!("tipi={}", types.unwrap_or_default()),
        ),
        _ => ("tabella assente".to_owned(), "tipi=".to_owned()),
    }
}

/// Toglie di mezzo la tabella di `Create`, che nessuna sonda deve trovare.
async fn drop_create(connection: &mut mysql_async::Conn) {
    connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_CREATE}"))
        .await
        .expect("pulizia della tabella di Create: harness, non divergenza");
}

/// La mode `Create` attraversata con il profilo del prodotto.
///
/// Ottava tranche, seconda write mode. `Create` aggiunge ad `Append` una
/// superficie sola, e da quella discende tutto il resto: **il DDL**. Su
/// `MySQL` e su `MariaDB` il DDL fa commit implicito, quindi la tabella creata
/// nella preparazione non appartiene alla transazione che segue e nessun
/// `ROLLBACK` la annulla.
///
/// Ne segue che il fallimento di una riga qui non e il fallimento di un
/// Append. Le righe tornano indietro, lo schema no, e cio che il chiamante
/// riceve non e `RolledBack` ma `Partial` con recupero richiesto — che e la
/// differenza fra «il server e come prima» e «il server ha una tabella vuota
/// in piu». Le tre sonde verificano percio, ciascuna, anche **cosa e
/// rimasto**.
#[allow(clippy::too_many_lines)]
async fn create_write_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let provider = MysqlProvider::with_profile(config(), 2, profile)
        .expect("provider della misura: harness, non divergenza");
    let mut connection = open_connection().await;
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(schema_name.to_owned()),
            object: SCRATCH_CREATE.to_owned(),
        },
        mode: WriteMode::Create,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        // Le keys di `Create` diventano la PRIMARY KEY. Servono: senza, la
        // tabella non ha vincoli e la sonda del rollback non avrebbe modo di
        // far rifiutare una riga dal server.
        keys: vec!["id".to_owned()],
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };
    // Cio che il DDL deve produrre, e che deve coincidere sui tre server.
    let expected_shape = "colonne=id/NO,payload/NO pk=id";

    // 1. La creazione riuscita. La tabella non c'e, la mode la costruisce e ci
    //    scrive; si rilegge da un'altra connessione dopo il commit, e si
    //    guarda anche la **forma** — una `Create` che scrivesse le righe
    //    giuste in una tabella sbagliata sarebbe verde su un conteggio.
    drop_create(&mut connection).await;
    let question = "crea la tabella dal piano e ci scrive, e la si rilegge da un'altra sessione";
    let expected = "righe=6 contenuto=1:riga-1,2:riga-2,3:riga-3,4:riga-4,5:riga-5,6:riga-6";
    match scripted_write(
        &provider,
        &operation,
        scratch_schema(),
        vec![scratch_batch(&[1, 2, 3]), scratch_batch(&[4, 5, 6])],
        6,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => {
            let contents = table_contents(&mut connection, SCRATCH_CREATE).await;
            let (shape, types) = create_shape(&mut connection).await;
            let accounting = outcome_mismatch(&outcome, profile, &ExpectedCounts::inserting(6))
                .or_else(|| (contents != expected).then(|| format!("contenuto [{contents}]")))
                .or_else(|| {
                    (shape != expected_shape)
                        .then(|| format!("forma attesa [{expected_shape}], osservata [{shape}]"))
                });
            match accounting {
                None => recorder.accepted(
                    "provider.profile_write_create",
                    "provider",
                    "profilo",
                    question,
                    format!(
                        "dichiarate={} {contents} {shape} {types}",
                        outcome.rows.confirmed
                    ),
                ),
                Some(reason) => recorder.rejected(
                    "provider.profile_write_create",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!("contratto non soddisfatto: {reason}")),
                    None,
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_write_create",
            "provider",
            "profilo",
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            crate::evidence::server_code_in_message(&error.message),
        ),
    }

    // 2. Il rollback, che qui annulla le righe e **non** la tabella. La
    //    quaterna e diversa da quella dell'Append per una ragione sola, e sta
    //    nel motore: la `CREATE TABLE` ha gia committato per conto suo, quindi
    //    dichiarare `RolledBack` direbbe al chiamante che il server e come
    //    prima mentre una tabella vuota e rimasta. `Partial` con
    //    `RequiresRecovery` e cio che il provider dichiara, e la rilettura e
    //    cio che lo verifica: righe zero, tabella presente.
    drop_create(&mut connection).await;
    let question = "un secondo batch rifiutato annulla le righe e lascia la tabella";
    // `Conflict`, non `DataMapping` — ed e la differenza piu istruttiva fra
    // questa mode e l'Append, misurata e non prevista: la prima stesura si
    // aspettava la stessa categoria della settima tranche e la sonda ha detto
    // di no.
    //
    // La ragione non e del motore ma di questo crate, ed e scritta nel punto
    // in cui si decide: la diagnostica per riga si attiva **solo** per
    // `Append` (`provider.rs`, «row-scoped diagnostics ha semantica valida
    // SOLO per Append»). Fuori di li la scrittura e un bulk INSERT, e il 1062
    // torna come il verdetto del codice server — `Conflict`, «vincolo univoco
    // violato». Lo stesso duplicato, quindi, arriva al chiamante in due
    // categorie diverse a seconda della mode, su tutti e tre i server.
    //
    // Gli altri tre assi sono quelli attesi, e il terzo e la superficie nuova:
    // l'effetto non e `RolledBack` ma `Partial`, perche il `ROLLBACK` ha
    // annullato le righe e non la tabella.
    let contract = RefusalContract {
        category: ErrorCategory::Conflict,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Partial,
        retry: RetryDisposition::RequiresRecovery,
        message_contains: "la tabella creata da mode='create' e rimasta",
    };
    match scripted_write(
        &provider,
        &operation,
        scratch_schema(),
        vec![scratch_batch(&[10, 11, 12]), scratch_batch(&[10])],
        4,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => recorder.accepted(
            "provider.profile_write_create_rollback",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la chiave duplicata e passata, righe={}",
                outcome.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(&mut connection, SCRATCH_CREATE).await;
            let (shape, _) = create_shape(&mut connection).await;
            let mismatch = refusal_mismatch(&contract, &error)
                .or_else(|| {
                    (contents != "righe=0 contenuto=")
                        .then(|| format!("le righe non sono state annullate: {contents}"))
                })
                // Il residuo si osserva, non si deduce dal messaggio: e il
                // messaggio ad affermare che la tabella e rimasta, e questa e
                // la riga che lo verifica sul server.
                .or_else(|| {
                    (shape != expected_shape)
                        .then(|| format!("la tabella non e rimasta come creata: [{shape}]"))
                });
            match mismatch {
                None => recorder.rejected(
                    "provider.profile_write_create_rollback",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{:?}/{:?}/{:?}/{:?}: {} — {contents}, {shape}",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        error.message
                    )),
                    crate::evidence::server_code_in_message(&error.message),
                ),
                Some(reason) => recorder.not_measured(
                    "provider.profile_write_create_rollback",
                    "provider",
                    "profilo",
                    question,
                    &format!(
                        "il rollback non e stato osservato — {reason}; osservato \
                          {:?}/{:?}/{:?}/{:?}: {} — {contents}, {shape}",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        condense(&error.message)
                    ),
                ),
            }
        }
    }

    create_cancellation_probe(
        recorder,
        &provider,
        profile,
        &operation,
        &mut connection,
        expected_shape,
        cancellation,
    )
    .await;

    drop_create(&mut connection).await;
}

/// La cancellazione a meta `Create`, sulla stessa barriera dichiarata.
///
/// Il token si annulla quando il provider chiede il secondo batch. Rispetto
/// all'Append cambia cosa resta: la tabella, che il DDL ha gia committato.
/// L'effetto remoto resta `Unknown` — non sapere se le righe siano state
/// applicate e piu grave che sapere che lo schema e rimasto, e il provider non
/// declassa la prima incertezza per annunciare la seconda.
///
/// La ripresa non puo essere un secondo `Create`: la tabella c'e, e la mode
/// fallirebbe per una ragione che non riguarda la quarantena della sessione.
/// Si toglie di mezzo dall'altra connessione, come farebbe chi recupera, e poi
/// si rifa — che e anche il modo in cui si verifica che il residuo fosse
/// davvero solo la tabella.
///
/// Lunga come la sua gemella dell'Append, e per la stessa ragione: cio che
/// verifica sono sei cose in fila — la quaterna del rifiuto, le righe, il
/// residuo, la ripresa e cosa la tabella contiene dopo — e spezzarla
/// separerebbe l'attesa dal confronto.
#[allow(clippy::too_many_lines)]
async fn create_cancellation_probe(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    profile: &'static dyn ProductProfile,
    operation: &WriteOperation,
    connection: &mut mysql_async::Conn,
    expected_shape: &str,
    outer: &CancellationToken,
) {
    drop_create(connection).await;
    let question =
        "una cancellazione a meta Create non lascia righe, lascia la tabella, e il provider resta usabile";
    let interrupted = CancellationToken::new();
    let contract = RefusalContract {
        category: ErrorCategory::Cancelled,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        message_contains: "la tabella creata da mode='create' e rimasta",
    };
    let outcome = scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![scratch_batch(&[20, 21, 22]), scratch_batch(&[23, 24, 25])],
        6,
        Some((2, interrupted.clone())),
        &interrupted,
    )
    .await;
    match outcome {
        Ok(written) => recorder.accepted(
            "provider.profile_write_create_cancellation",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la cancellazione non ha interrotto, righe={}",
                written.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_CREATE).await;
            let (shape, _) = create_shape(connection).await;
            // Il recupero che il contratto chiede al chiamante, fatto da fuori:
            // la tabella residua se ne va, e la mode puo ricominciare.
            drop_create(connection).await;
            let recovered = scripted_write(
                provider,
                operation,
                scratch_schema(),
                vec![scratch_batch(&[30, 31])],
                2,
                None,
                outer,
            )
            .await;
            let after = table_contents(connection, SCRATCH_CREATE).await;
            let mismatch = refusal_mismatch(&contract, &error)
                .or_else(|| {
                    (contents != "righe=0 contenuto=")
                        .then(|| format!("la cancellazione ha lasciato righe: {contents}"))
                })
                .or_else(|| {
                    (shape != expected_shape)
                        .then(|| format!("il residuo non e la tabella creata: [{shape}]"))
                })
                .or_else(|| match &recovered {
                    Ok(written) => {
                        outcome_mismatch(written, profile, &ExpectedCounts::inserting(2))
                            .or_else(|| {
                                (after != "righe=2 contenuto=30:riga-30,31:riga-31").then(|| {
                                    format!("dopo la ripresa la tabella contiene [{after}]")
                                })
                            })
                            .map(|reason| format!("la ripresa non e quella attesa: {reason}"))
                    }
                    Err(next) => Some(format!(
                        "il provider non e piu utilizzabile: {:?}: {}",
                        next.category, next.message
                    )),
                });
            match mismatch {
                None => recorder.rejected(
                    "provider.profile_write_create_cancellation",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{:?}/{:?}/{:?}/{:?}: {} — dopo la cancellazione [{contents}], \
                         residuo [{shape}], dopo la ripresa [{after}]",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        error.message
                    )),
                    None,
                ),
                Some(reason) => recorder.not_measured(
                    "provider.profile_write_create_cancellation",
                    "provider",
                    "profilo",
                    question,
                    &format!(
                        "la cancellazione non e stata osservata per intero — {reason}; \
                         osservato {:?}/{:?}/{:?}/{:?}: {} — dopo [{contents}], \
                         residuo [{shape}], ripresa [{after}]",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        condense(&error.message)
                    ),
                ),
            }
        }
    }
}

/// La tabella su cui `Update` lavora: esiste gia, e ha righe da cambiare.
const SCRATCH_UPDATE: &str = "plenora_driver_evidence_update";

/// Il contenuto di partenza, che ogni sonda di `Update` ripristina.
///
/// Sei righe con un payload riconoscibile: cio che la sonda verifica non e
/// «quante righe ci sono» ma **quali valori** ci sono dopo, e per dirlo serve
/// sapere quali c'erano prima.
///
/// Il vincolo univoco su `payload` non e decorativo: e cio che rende
/// possibile far fallire un `Update` dal server invece che dal mapping. Un
/// rollback provocato dal preflight non proverebbe niente, perche non avrebbe
/// mai aggiornato nulla da annullare.
const UPDATE_INITIAL: &str =
    "righe=6 contenuto=1:vecchio-1,2:vecchio-2,3:vecchio-3,4:vecchio-4,5:vecchio-5,6:vecchio-6";

async fn reset_update(connection: &mut mysql_async::Conn) {
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_UPDATE}"),
        format!(
            "CREATE TABLE {SCRATCH_UPDATE} (id INT NOT NULL PRIMARY KEY, \
             payload VARCHAR(32) NOT NULL, UNIQUE KEY uq_payload (payload)) \
             ENGINE = InnoDB"
        ),
        format!(
            "INSERT INTO {SCRATCH_UPDATE} (id, payload) VALUES \
             (1,'vecchio-1'),(2,'vecchio-2'),(3,'vecchio-3'),\
             (4,'vecchio-4'),(5,'vecchio-5'),(6,'vecchio-6')"
        ),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("tabella dell'update: harness, non divergenza");
    }
}

/// La mode `Update` attraversata con il profilo del prodotto.
///
/// Nona tranche, terza write mode. `Update` porta la superficie che nessuna
/// delle due precedenti aveva: **le keys**. Non scrive righe nuove — le
/// confronta con quelle che ci sono, e da quel confronto discendono due
/// promesse che ne `Append` ne `Create` fanno.
///
/// La prima e la contabilita: una chiave che non trova riscontro non e un
/// errore ma una riga **saltata**, e l'esito deve dirlo separando `updated`
/// da `skipped`. La seconda e il rollback, che qui non annulla degli inserti
/// ma **rimette i valori di prima**: e una cosa diversa, e si vede solo
/// sapendo quali fossero.
///
/// Il piano accumula le righe in una `CREATE TEMPORARY TABLE` di staging e
/// poi esegue un `UPDATE ... JOIN`. La temporary non e un residuo: su questi
/// motori non provoca commit implicito, quindi qui — a differenza di `Create`
/// — l'effetto remoto di un fallimento e `RolledBack` pieno.
#[allow(clippy::too_many_lines)]
async fn update_write_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let provider = MysqlProvider::with_profile(config(), 2, profile)
        .expect("provider della misura: harness, non divergenza");
    let mut connection = open_connection().await;
    let operation = WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(schema_name.to_owned()),
            object: SCRATCH_UPDATE.to_owned(),
        },
        mode: WriteMode::Update,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: vec!["id".to_owned()],
        // Vuoto significa «tutte le colonne non-key», che qui e `payload`.
        // Dichiararla esplicitamente misurerebbe la stessa cosa da una strada
        // che il piano tratta a parte, e il default e la strada comune.
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };

    // 1. L'aggiornamento riuscito, con una chiave che non c'e. Sei righe in
    //    ingresso, cinque che trovano riscontro e una — l'id 99 — che no: il
    //    contratto dice che quella e saltata, non fallita, e che non viene
    //    inserita. Sono tre affermazioni diverse, e la sonda le verifica
    //    tutte: la contabilita, il contenuto, e il fatto che la tabella abbia
    //    ancora sei righe.
    reset_update(&mut connection).await;
    let question =
        "aggiorna le righe che trovano riscontro, salta quelle che non c'e, e non ne inserisce";
    let expected =
        "righe=6 contenuto=1:nuovo-1,2:nuovo-2,3:nuovo-3,4:nuovo-4,5:nuovo-5,6:vecchio-6";
    let accounting = ExpectedCounts {
        received: 6,
        confirmed: 5,
        inserted: Some(0),
        updated: Some(5),
        deleted: Some(0),
        skipped: 1,
    };
    match scripted_write(
        &provider,
        &operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[1, 2, 3]),
            scratch_batch_labelled("nuovo", &[4, 5, 99]),
        ],
        6,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => {
            let contents = table_contents(&mut connection, SCRATCH_UPDATE).await;
            let mismatch = outcome_mismatch(&outcome, profile, &accounting)
                .or_else(|| (contents != expected).then(|| format!("contenuto [{contents}]")));
            match mismatch {
                None => recorder.accepted(
                    "provider.profile_write_update",
                    "provider",
                    "profilo",
                    question,
                    format!(
                        "aggiornate={:?} saltate={} {contents}",
                        outcome.rows.updated, outcome.rows.skipped
                    ),
                ),
                Some(reason) => recorder.rejected(
                    "provider.profile_write_update",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!("contratto non soddisfatto: {reason}")),
                    None,
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_write_update",
            "provider",
            "profilo",
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            crate::evidence::server_code_in_message(&error.message),
        ),
    }

    // 2. Il rollback, che qui rimette i valori di prima. Il primo batch
    //    aggiorna davvero tre righe; il secondo prova a dare a una quarta il
    //    payload di una riga che non ha toccato, e il vincolo univoco lo
    //    rifiuta. Cio che si verifica non e «la tabella e vuota» — non lo era
    //    mai — ma che le sei righe siano tornate esattamente quelle di
    //    partenza.
    reset_update(&mut connection).await;
    let question = "un secondo batch rifiutato rimette i valori che il primo aveva cambiato";
    // Nessun residuo DDL: la staging e una `TEMPORARY`, che su questi motori
    // non fa commit implicito. L'effetto e percio `RolledBack` pieno, non il
    // `Partial` di `Create` — ed e la differenza che questa sonda misura.
    let contract = RefusalContract {
        category: ErrorCategory::Conflict,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::RolledBack,
        retry: RetryDisposition::Never,
        // Il frammento non nomina il prodotto: il messaggio lo porta gia.
        message_contains: "vincolo univoco",
    };
    match scripted_write(
        &provider,
        &operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[1, 2, 3]),
            // All'id 4 il payload **della riga 5**, che il primo batch non ha
            // toccato: e l'unico modo di far scattare il vincolo univoco. Dare
            // a una riga il proprio valore sarebbe un no-op, e la scrittura
            // riuscirebbe.
            scratch_batch_pairs(&[(4, "vecchio-5".to_owned())]),
        ],
        4,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => recorder.accepted(
            "provider.profile_write_update_rollback",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: il payload duplicato e passato, aggiornate={:?}",
                outcome.rows.updated
            ),
        ),
        Err(error) => {
            let contents = table_contents(&mut connection, SCRATCH_UPDATE).await;
            let mismatch = refusal_mismatch(&contract, &error).or_else(|| {
                (contents != UPDATE_INITIAL)
                    .then(|| format!("i valori di prima non sono tornati: [{contents}]"))
            });
            match mismatch {
                None => recorder.rejected(
                    "provider.profile_write_update_rollback",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{:?}/{:?}/{:?}/{:?}: {} — {contents}",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        error.message
                    )),
                    crate::evidence::server_code_in_message(&error.message),
                ),
                Some(reason) => recorder.not_measured(
                    "provider.profile_write_update_rollback",
                    "provider",
                    "profilo",
                    question,
                    &format!(
                        "il rollback non e stato osservato — {reason}; osservato \
                          {:?}/{:?}/{:?}/{:?}: {} — {contents}",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        condense(&error.message)
                    ),
                ),
            }
        }
    }

    update_cancellation_probe(
        recorder,
        &provider,
        profile,
        &operation,
        &mut connection,
        cancellation,
    )
    .await;

    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_UPDATE}"))
        .await;
}

/// La cancellazione a meta `Update`, sulla stessa barriera dichiarata.
///
/// Cio che cambia rispetto alle due mode precedenti e cosa vuol dire «non ha
/// lasciato niente»: qui la tabella era piena prima e resta piena dopo, quindi
/// la prova non e un conteggio a zero ma il **contenuto di partenza**, riga
/// per riga. Un aggiornamento a meta che avesse committato il primo batch
/// darebbe lo stesso numero di righe e un contenuto diverso.
#[allow(clippy::too_many_lines)]
async fn update_cancellation_probe(
    recorder: &mut Recorder,
    provider: &MysqlProvider,
    profile: &'static dyn ProductProfile,
    operation: &WriteOperation,
    connection: &mut mysql_async::Conn,
    outer: &CancellationToken,
) {
    reset_update(connection).await;
    let question =
        "una cancellazione a meta Update lascia i valori di prima, e il provider resta usabile";
    let interrupted = CancellationToken::new();
    let contract = RefusalContract {
        category: ErrorCategory::Cancelled,
        phase: ErrorPhase::Write,
        remote_effect: RemoteEffect::Unknown,
        retry: RetryDisposition::RequiresRecovery,
        message_contains: "quarantinata",
    };
    let outcome = scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[1, 2, 3]),
            scratch_batch_labelled("nuovo", &[4, 5, 6]),
        ],
        6,
        Some((2, interrupted.clone())),
        &interrupted,
    )
    .await;
    match outcome {
        Ok(written) => recorder.accepted(
            "provider.profile_write_update_cancellation",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la cancellazione non ha interrotto, aggiornate={:?}",
                written.rows.updated
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_UPDATE).await;
            let recovered = scripted_write(
                provider,
                operation,
                scratch_schema(),
                vec![scratch_batch_labelled("ripreso", &[1, 2])],
                2,
                None,
                outer,
            )
            .await;
            let after = table_contents(connection, SCRATCH_UPDATE).await;
            let resumed = ExpectedCounts {
                received: 2,
                confirmed: 2,
                inserted: Some(0),
                updated: Some(2),
                deleted: Some(0),
                skipped: 0,
            };
            let expected_after = "righe=6 contenuto=1:ripreso-1,2:ripreso-2,3:vecchio-3,\
                                  4:vecchio-4,5:vecchio-5,6:vecchio-6";
            let mismatch = refusal_mismatch(&contract, &error)
                .or_else(|| {
                    (contents != UPDATE_INITIAL)
                        .then(|| format!("la cancellazione ha lasciato valori nuovi: [{contents}]"))
                })
                .or_else(|| match &recovered {
                    Ok(written) => outcome_mismatch(written, profile, &resumed)
                        .or_else(|| {
                            // Il contenuto, non il conteggio: una sessione
                            // riusata con una transazione residua
                            // committerebbe anche le righe di prima, e
                            // «aggiornate=2» lo direbbe lo stesso.
                            (after != expected_after)
                                .then(|| format!("dopo la ripresa la tabella contiene [{after}]"))
                        })
                        .map(|reason| format!("la ripresa non e quella attesa: {reason}")),
                    Err(next) => Some(format!(
                        "il provider non e piu utilizzabile: {:?}: {}",
                        next.category, next.message
                    )),
                });
            match mismatch {
                None => recorder.rejected(
                    "provider.profile_write_update_cancellation",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!(
                        "{:?}/{:?}/{:?}/{:?}: {} — dopo la cancellazione [{contents}], \
                         dopo la ripresa [{after}]",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        error.message
                    )),
                    None,
                ),
                Some(reason) => recorder.not_measured(
                    "provider.profile_write_update_cancellation",
                    "provider",
                    "profilo",
                    question,
                    &format!(
                        "la cancellazione non e stata osservata per intero — {reason}; \
                         osservato {:?}/{:?}/{:?}/{:?}: {} — dopo [{contents}], \
                         ripresa [{after}]",
                        error.category,
                        error.phase,
                        error.remote_effect,
                        error.retry,
                        condense(&error.message)
                    ),
                ),
            }
        }
    }
}

const SCRATCH_UPSERT: &str = "plenora_driver_evidence_upsert";
const SCRATCH_REPLACE: &str = "plenora_driver_evidence_replace";
const SCRATCH_DELETE: &str = "plenora_driver_evidence_delete";
/// La figlia che tiene per il braccio una riga della tabella di `DeleteByKeys`.
const SCRATCH_DELETE_CHILD: &str = "plenora_driver_evidence_delete_child";

/// Un payload piu lungo della colonna, che il server rifiuta.
///
/// Serve dove il conflitto non e disponibile: `Upsert` non puo fallire su una
/// chiave duplicata — e cio che la mode fa per mestiere — e il preflight
/// rifiuta a monte una tabella con un secondo indice unico, quindi non esiste
/// un'altra chiave su cui collidere. Un valore fuori misura e la via piu
/// diretta a un rifiuto del **server**, che e cio che le sonde di rollback
/// devono provocare: un errore di mapping o di preflight non proverebbe
/// niente, perche non avrebbe mai scritto nulla da annullare.
///
/// `STRICT_TRANS_TABLES` e attivo su tutti e tre i riferimenti — misurato
/// dalla prima tranche — quindi il troppo-lungo e un errore e non un
/// troncamento silenzioso.
const TOO_LONG: &str = "fuorimisura-fuorimisura-fuorimisura-fuorimisura";

/// Ricrea una tabella di lavoro con lo schema comune e le righe date.
async fn reset_scratch(
    connection: &mut mysql_async::Conn,
    table: &str,
    extra: &str,
    seed: &[(i32, &str)],
) {
    let mut statements = vec![
        format!("DROP TABLE IF EXISTS {table}"),
        format!(
            "CREATE TABLE {table} (id INT NOT NULL PRIMARY KEY, \
             payload VARCHAR(32) NOT NULL{extra}) ENGINE = InnoDB"
        ),
    ];
    if !seed.is_empty() {
        let values = seed
            .iter()
            .map(|(id, payload)| format!("({id},'{payload}')"))
            .collect::<Vec<_>>()
            .join(",");
        statements.push(format!("INSERT INTO {table} (id, payload) VALUES {values}"));
    }
    for statement in statements {
        connection
            .query_drop(statement)
            .await
            .expect("tabella di lavoro: harness, non divergenza");
    }
}

/// Le tre write mode che restavano, con le proprie tre sonde ciascuna.
///
/// Stanno in una funzione sola perche condividono l'unica cosa che le lega —
/// il modo di provocare un rifiuto del server — e non perche siano la stessa
/// misura: ognuna ha la propria tabella, la propria contabilita attesa e la
/// propria domanda.
///
/// * `Upsert` non ha un rollback «che rimette i valori»: mescola inserimenti e
///   aggiornamenti, e l'esito lo dichiara con `inserted` e `updated` a `None`
///   — «non pertinente» — perche su questi motori `affected_rows` vale 1 per
///   un inserimento e 2 per un aggiornamento, e il totale non si scompone
///   senza una seconda interrogazione. La sonda pretende quel `None`: un
///   numero inventato sarebbe peggio di un'assenza dichiarata.
/// * `Replace` svuota il target e lo riempie **nella stessa transazione**. Il
///   suo rollback e il piu importante di tutta la fase: un fallimento a meta
///   non deve lasciare la tabella vuota.
/// * `DeleteByKeys` cancella cio che trova e salta cio che non trova, come
///   `Update`, ma dal lato opposto.
#[allow(clippy::too_many_lines)]
async fn remaining_write_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    schema_name: &str,
    cancellation: &CancellationToken,
) {
    let provider = MysqlProvider::with_profile(config(), 2, profile)
        .expect("provider della misura: harness, non divergenza");
    let mut connection = open_connection().await;
    let target = |object: &str| ObjectRef {
        catalog: None,
        schema: Some(schema_name.to_owned()),
        object: object.to_owned(),
    };
    let plan = |object: &str, mode: WriteMode, keys: Vec<String>| WriteOperation {
        target: target(object),
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys,
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    };

    upsert_probes(
        recorder,
        profile,
        &provider,
        &plan(SCRATCH_UPSERT, WriteMode::Upsert, vec!["id".to_owned()]),
        &mut connection,
        cancellation,
    )
    .await;
    replace_probes(
        recorder,
        profile,
        &provider,
        &plan(SCRATCH_REPLACE, WriteMode::Replace, Vec::new()),
        &mut connection,
        cancellation,
    )
    .await;
    delete_probes(
        recorder,
        profile,
        &provider,
        &plan(
            SCRATCH_DELETE,
            WriteMode::DeleteByKeys,
            vec!["id".to_owned()],
        ),
        &mut connection,
        cancellation,
    )
    .await;

    for table in [
        SCRATCH_DELETE_CHILD,
        SCRATCH_DELETE,
        SCRATCH_REPLACE,
        SCRATCH_UPSERT,
    ] {
        let _ = connection
            .query_drop(format!("DROP TABLE IF EXISTS {table}"))
            .await;
    }
}

/// Il rifiuto di un valore che non entra nella colonna.
///
/// `DataMapping`/`Never`: il dato in ingresso e sbagliato e lo corregge chi
/// chiama. Fino alla nona tranche questo codice — 1406 — arrivava come guasto
/// generico `Execution`, che e vero ma non dice cosa fare. Cio che la sonda
/// verifica oltre alla categoria e che l'effetto sia `RolledBack`: nessuna di
/// queste mode esegue DDL, quindi il rollback e pieno e non lascia residui.
const TOO_LONG_REFUSAL: RefusalContract = RefusalContract {
    category: ErrorCategory::DataMapping,
    phase: ErrorPhase::Write,
    remote_effect: RemoteEffect::RolledBack,
    retry: RetryDisposition::Never,
    message_contains: "oltre la larghezza della colonna",
};

/// Il rifiuto di una cancellazione trattenuta da un vincolo referenziale.
///
/// `Conflict` e non `DataMapping`: non e la riga a essere malformata, e lo
/// stato del database a non ammettere l'operazione — una figlia che trattiene
/// la madre. Il chiamante che riceve i due verdetti fa due cose diverse:
/// corregge il dato, oppure guarda cos'altro dipende da quella riga.
const FOREIGN_KEY_REFUSAL: RefusalContract = RefusalContract {
    category: ErrorCategory::Conflict,
    phase: ErrorPhase::Write,
    remote_effect: RemoteEffect::RolledBack,
    retry: RetryDisposition::Never,
    message_contains: "integrita referenziale",
};

/// Il rifiuto di una cancellazione, uguale per le tre mode.
const CANCELLED_REFUSAL: RefusalContract = RefusalContract {
    category: ErrorCategory::Cancelled,
    phase: ErrorPhase::Write,
    remote_effect: RemoteEffect::Unknown,
    retry: RetryDisposition::RequiresRecovery,
    message_contains: "quarantinata",
};

/// Registra l'esito di una sonda il cui **rifiuto** e la prova.
fn record_refusal(
    recorder: &mut Recorder,
    probe: &'static str,
    // `'static` come il nome della sonda: la domanda che una misura pone e
    // scritta nel sorgente, non composta a runtime, e il registro la conserva
    // oltre la vita di questa chiamata.
    question: &'static str,
    error: &plenora_database_core::DatabaseError,
    mismatch: Option<String>,
    observed: &str,
) {
    let quartet = format!(
        "{:?}/{:?}/{:?}/{:?}: {}",
        error.category, error.phase, error.remote_effect, error.retry, error.message
    );
    match mismatch {
        None => recorder.rejected(
            probe,
            "provider",
            "profilo",
            question,
            condense(&format!("{quartet} — {observed}")),
            crate::evidence::server_code_in_message(&error.message),
        ),
        Some(reason) => recorder.not_measured(
            probe,
            "provider",
            "profilo",
            question,
            &condense(&format!(
                "il rifiuto atteso non e stato osservato — {reason}; osservato \
                 {quartet} — {observed}"
            )),
        ),
    }
}

/// Tre sonde in fila, e restano insieme: condividono il seme della
/// tabella, il contenuto di partenza con cui ciascuna si confronta e la
/// forma della contabilita attesa. Spezzarle vorrebbe dire passare quei
/// tre valori in giro, cioe separare l'attesa dal confronto.
#[allow(clippy::too_many_lines)]
async fn upsert_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    provider: &MysqlProvider,
    operation: &WriteOperation,
    connection: &mut mysql_async::Conn,
    cancellation: &CancellationToken,
) {
    // La tabella non ha altri indici unici oltre alla chiave primaria: il
    // preflight della sesta tranche rifiuta il caso contrario, e rifiutarlo e
    // gia una prova sua.
    let seed = [(1, "vecchio-1"), (2, "vecchio-2"), (3, "vecchio-3")];
    let initial = "righe=3 contenuto=1:vecchio-1,2:vecchio-2,3:vecchio-3";

    // 1. Tre righe che esistono e tre che no, in due batch: la mode aggiorna
    //    le prime e inserisce le seconde, e l'esito dichiara `inserted` e
    //    `updated` come non pertinenti invece di inventarne la scomposizione.
    reset_scratch(connection, SCRATCH_UPSERT, "", &seed).await;
    let question = "aggiorna cio che c'e, inserisce cio che non c'e, e non scompone cio che non sa";
    let expected = "righe=6 contenuto=1:nuovo-1,2:nuovo-2,3:nuovo-3,4:nuovo-4,5:nuovo-5,6:nuovo-6";
    let accounting = ExpectedCounts {
        received: 6,
        confirmed: 6,
        inserted: None,
        updated: None,
        deleted: Some(0),
        skipped: 0,
    };
    match scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[1, 2, 3]),
            scratch_batch_labelled("nuovo", &[4, 5, 6]),
        ],
        6,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => {
            let contents = table_contents(connection, SCRATCH_UPSERT).await;
            let mismatch = outcome_mismatch(&outcome, profile, &accounting)
                .or_else(|| (contents != expected).then(|| format!("contenuto [{contents}]")));
            match mismatch {
                None => recorder.accepted(
                    "provider.profile_write_upsert",
                    "provider",
                    "profilo",
                    question,
                    format!(
                        "confermate={} inserite={:?} aggiornate={:?} {contents}",
                        outcome.rows.confirmed, outcome.rows.inserted, outcome.rows.updated
                    ),
                ),
                Some(reason) => recorder.rejected(
                    "provider.profile_write_upsert",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!("contratto non soddisfatto: {reason}")),
                    None,
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_write_upsert",
            "provider",
            "profilo",
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            crate::evidence::server_code_in_message(&error.message),
        ),
    }

    // 2. Il rollback. Il primo batch aggiorna davvero, il secondo porta un
    //    valore fuori misura e il server lo rifiuta: le tre righe di partenza
    //    devono tornare com'erano, e non deve esserci traccia delle nuove.
    reset_scratch(connection, SCRATCH_UPSERT, "", &seed).await;
    let question = "un secondo batch rifiutato annulla anche gli aggiornamenti del primo";
    match scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[1, 2, 3]),
            scratch_batch_pairs(&[(4, TOO_LONG.to_owned())]),
        ],
        4,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => recorder.accepted(
            "provider.profile_write_upsert_rollback",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: il valore fuori misura e passato, confermate={}",
                outcome.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_UPSERT).await;
            let mismatch = refusal_mismatch(&TOO_LONG_REFUSAL, &error).or_else(|| {
                (contents != initial)
                    .then(|| format!("le righe di partenza non sono tornate: [{contents}]"))
            });
            record_refusal(
                recorder,
                "provider.profile_write_upsert_rollback",
                question,
                &error,
                mismatch,
                &contents,
            );
        }
    }

    // 3. La cancellazione: niente di applicato, e il provider ancora usabile.
    reset_scratch(connection, SCRATCH_UPSERT, "", &seed).await;
    let question = "una cancellazione a meta Upsert non applica nulla, e il provider resta usabile";
    let interrupted = CancellationToken::new();
    match scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[1, 2, 3]),
            scratch_batch_labelled("nuovo", &[4, 5, 6]),
        ],
        6,
        Some((2, interrupted.clone())),
        &interrupted,
    )
    .await
    {
        Ok(written) => recorder.accepted(
            "provider.profile_write_upsert_cancellation",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la cancellazione non ha interrotto, confermate={}",
                written.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_UPSERT).await;
            let resumed = ExpectedCounts {
                received: 1,
                confirmed: 1,
                inserted: None,
                updated: None,
                deleted: Some(0),
                skipped: 0,
            };
            let recovered = scripted_write(
                provider,
                operation,
                scratch_schema(),
                vec![scratch_batch_labelled("ripreso", &[9])],
                1,
                None,
                cancellation,
            )
            .await;
            let after = table_contents(connection, SCRATCH_UPSERT).await;
            let expected_after =
                "righe=4 contenuto=1:vecchio-1,2:vecchio-2,3:vecchio-3,9:ripreso-9";
            let mismatch = refusal_mismatch(&CANCELLED_REFUSAL, &error)
                .or_else(|| {
                    (contents != initial)
                        .then(|| format!("la cancellazione ha lasciato righe: [{contents}]"))
                })
                .or_else(|| match &recovered {
                    Ok(written) => outcome_mismatch(written, profile, &resumed)
                        .or_else(|| {
                            (after != expected_after)
                                .then(|| format!("dopo la ripresa la tabella contiene [{after}]"))
                        })
                        .map(|reason| format!("la ripresa non e quella attesa: {reason}")),
                    Err(next) => Some(format!(
                        "il provider non e piu utilizzabile: {:?}: {}",
                        next.category, next.message
                    )),
                });
            record_refusal(
                recorder,
                "provider.profile_write_upsert_cancellation",
                question,
                &error,
                mismatch,
                &format!("dopo [{contents}], ripresa [{after}]"),
            );
        }
    }
}

/// Tre sonde in fila, e restano insieme: condividono il seme della
/// tabella, il contenuto di partenza con cui ciascuna si confronta e la
/// forma della contabilita attesa. Spezzarle vorrebbe dire passare quei
/// tre valori in giro, cioe separare l'attesa dal confronto.
#[allow(clippy::too_many_lines)]
async fn replace_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    provider: &MysqlProvider,
    operation: &WriteOperation,
    connection: &mut mysql_async::Conn,
    cancellation: &CancellationToken,
) {
    let seed = [(1, "vecchio-1"), (2, "vecchio-2"), (3, "vecchio-3")];
    let initial = "righe=3 contenuto=1:vecchio-1,2:vecchio-2,3:vecchio-3";

    // 1. Il target viene svuotato e riempito nella stessa transazione: cio che
    //    resta sono **solo** le righe in ingresso. Il conteggio delle
    //    cancellate resta zero, e la sonda lo pretende invece di ignorarlo: e
    //    cio che il contratto dichiara, e un valore diverso vorrebbe dire che
    //    l'esito ha cambiato significato.
    reset_scratch(connection, SCRATCH_REPLACE, "", &seed).await;
    let question = "svuota il target e ci mette le righe in ingresso, nella stessa transazione";
    let expected = "righe=2 contenuto=7:nuovo-7,8:nuovo-8";
    let accounting = ExpectedCounts {
        received: 2,
        confirmed: 2,
        inserted: Some(2),
        updated: Some(0),
        deleted: Some(0),
        skipped: 0,
    };
    match scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![scratch_batch_labelled("nuovo", &[7, 8])],
        2,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => {
            let contents = table_contents(connection, SCRATCH_REPLACE).await;
            let mismatch = outcome_mismatch(&outcome, profile, &accounting)
                .or_else(|| (contents != expected).then(|| format!("contenuto [{contents}]")));
            match mismatch {
                None => recorder.accepted(
                    "provider.profile_write_replace",
                    "provider",
                    "profilo",
                    question,
                    format!(
                        "inserite={:?} cancellate={:?} {contents}",
                        outcome.rows.inserted, outcome.rows.deleted
                    ),
                ),
                Some(reason) => recorder.rejected(
                    "provider.profile_write_replace",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!("contratto non soddisfatto: {reason}")),
                    None,
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_write_replace",
            "provider",
            "profilo",
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            crate::evidence::server_code_in_message(&error.message),
        ),
    }

    // 2. Il rollback che conta piu di tutti. Il DELETE e gia passato quando la
    //    scrittura fallisce: se non tornasse indietro, un `Replace` fallito
    //    lascerebbe il target **vuoto** — cioe distruggerebbe i dati che
    //    doveva sostituire. La sonda verifica che le tre righe di partenza
    //    siano tutte li.
    reset_scratch(connection, SCRATCH_REPLACE, "", &seed).await;
    let question = "un Replace fallito non lascia il target vuoto: le righe di prima tornano";
    match scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[7, 8]),
            scratch_batch_pairs(&[(9, TOO_LONG.to_owned())]),
        ],
        3,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => recorder.accepted(
            "provider.profile_write_replace_rollback",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: il valore fuori misura e passato, confermate={}",
                outcome.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_REPLACE).await;
            let mismatch = refusal_mismatch(&TOO_LONG_REFUSAL, &error).or_else(|| {
                (contents != initial)
                    .then(|| format!("il DELETE non e stato annullato: [{contents}]"))
            });
            record_refusal(
                recorder,
                "provider.profile_write_replace_rollback",
                question,
                &error,
                mismatch,
                &contents,
            );
        }
    }

    // 3. La cancellazione, con la stessa posta in gioco del rollback.
    reset_scratch(connection, SCRATCH_REPLACE, "", &seed).await;
    let question =
        "una cancellazione a meta Replace non lascia il target vuoto, e il provider resta usabile";
    let interrupted = CancellationToken::new();
    match scripted_write(
        provider,
        operation,
        scratch_schema(),
        vec![
            scratch_batch_labelled("nuovo", &[7, 8]),
            scratch_batch_labelled("nuovo", &[9]),
        ],
        3,
        Some((2, interrupted.clone())),
        &interrupted,
    )
    .await
    {
        Ok(written) => recorder.accepted(
            "provider.profile_write_replace_cancellation",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la cancellazione non ha interrotto, confermate={}",
                written.rows.confirmed
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_REPLACE).await;
            let resumed = ExpectedCounts {
                received: 1,
                confirmed: 1,
                inserted: Some(1),
                updated: Some(0),
                deleted: Some(0),
                skipped: 0,
            };
            let recovered = scripted_write(
                provider,
                operation,
                scratch_schema(),
                vec![scratch_batch_labelled("ripreso", &[4])],
                1,
                None,
                cancellation,
            )
            .await;
            let after = table_contents(connection, SCRATCH_REPLACE).await;
            let mismatch = refusal_mismatch(&CANCELLED_REFUSAL, &error)
                .or_else(|| {
                    (contents != initial)
                        .then(|| format!("la cancellazione ha toccato il target: [{contents}]"))
                })
                .or_else(|| match &recovered {
                    Ok(written) => outcome_mismatch(written, profile, &resumed)
                        .or_else(|| {
                            (after != "righe=1 contenuto=4:ripreso-4")
                                .then(|| format!("dopo la ripresa la tabella contiene [{after}]"))
                        })
                        .map(|reason| format!("la ripresa non e quella attesa: {reason}")),
                    Err(next) => Some(format!(
                        "il provider non e piu utilizzabile: {:?}: {}",
                        next.category, next.message
                    )),
                });
            record_refusal(
                recorder,
                "provider.profile_write_replace_cancellation",
                question,
                &error,
                mismatch,
                &format!("dopo [{contents}], ripresa [{after}]"),
            );
        }
    }
}

/// Tre sonde in fila, e restano insieme: condividono il seme della
/// tabella, il contenuto di partenza con cui ciascuna si confronta e la
/// forma della contabilita attesa. Spezzarle vorrebbe dire passare quei
/// tre valori in giro, cioe separare l'attesa dal confronto.
#[allow(clippy::too_many_lines)]
async fn delete_probes(
    recorder: &mut Recorder,
    profile: &'static dyn ProductProfile,
    provider: &MysqlProvider,
    operation: &WriteOperation,
    connection: &mut mysql_async::Conn,
    cancellation: &CancellationToken,
) {
    let seed = [
        (1, "vecchio-1"),
        (2, "vecchio-2"),
        (3, "vecchio-3"),
        (4, "vecchio-4"),
    ];
    let initial = "righe=4 contenuto=1:vecchio-1,2:vecchio-2,3:vecchio-3,4:vecchio-4";

    // 1. Cancella cio che trova e salta cio che non trova. E la simmetrica
    //    dell'Update: una chiave assente non e un errore, e la contabilita
    //    deve distinguerla da una cancellata.
    reset_scratch(connection, SCRATCH_DELETE, "", &seed).await;
    let question = "cancella le chiavi che trova e salta quelle che non ci sono";
    let expected = "righe=2 contenuto=3:vecchio-3,4:vecchio-4";
    let accounting = ExpectedCounts {
        received: 3,
        confirmed: 2,
        inserted: Some(0),
        updated: Some(0),
        deleted: Some(2),
        skipped: 1,
    };
    match scripted_write(
        provider,
        operation,
        key_schema(),
        vec![key_batch(&[1, 2, 99])],
        3,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => {
            let contents = table_contents(connection, SCRATCH_DELETE).await;
            let mismatch = outcome_mismatch(&outcome, profile, &accounting)
                .or_else(|| (contents != expected).then(|| format!("contenuto [{contents}]")));
            match mismatch {
                None => recorder.accepted(
                    "provider.profile_write_delete_by_keys",
                    "provider",
                    "profilo",
                    question,
                    format!(
                        "cancellate={:?} saltate={} {contents}",
                        outcome.rows.deleted, outcome.rows.skipped
                    ),
                ),
                Some(reason) => recorder.rejected(
                    "provider.profile_write_delete_by_keys",
                    "provider",
                    "profilo",
                    question,
                    condense(&format!("contratto non soddisfatto: {reason}")),
                    None,
                ),
            }
        }
        Err(error) => recorder.rejected(
            "provider.profile_write_delete_by_keys",
            "provider",
            "profilo",
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            crate::evidence::server_code_in_message(&error.message),
        ),
    }

    // 2. Il rollback. Una figlia con vincolo di integrita referenziale tiene
    //    per il braccio la riga 2: il server rifiuta la cancellazione, e
    //    nessuna delle chiavi dello stesso batch deve sparire — nemmeno la 1,
    //    che di per se sarebbe cancellabile.
    reset_scratch(connection, SCRATCH_DELETE, "", &seed).await;
    for statement in [
        format!("DROP TABLE IF EXISTS {SCRATCH_DELETE_CHILD}"),
        format!(
            "CREATE TABLE {SCRATCH_DELETE_CHILD} (id INT NOT NULL PRIMARY KEY, \
             parent INT NOT NULL, CONSTRAINT fk_parent FOREIGN KEY (parent) \
             REFERENCES {SCRATCH_DELETE} (id)) ENGINE = InnoDB"
        ),
        format!("INSERT INTO {SCRATCH_DELETE_CHILD} (id, parent) VALUES (1, 2)"),
    ] {
        connection
            .query_drop(statement)
            .await
            .expect("tabella figlia: harness, non divergenza");
    }
    let question = "una chiave trattenuta da un vincolo fa tornare indietro l'intero batch";
    match scripted_write(
        provider,
        operation,
        key_schema(),
        vec![key_batch(&[1, 2])],
        2,
        None,
        cancellation,
    )
    .await
    {
        Ok(outcome) => recorder.accepted(
            "provider.profile_write_delete_by_keys_rollback",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: il vincolo non ha trattenuto, cancellate={:?}",
                outcome.rows.deleted
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_DELETE).await;
            let mismatch = refusal_mismatch(&FOREIGN_KEY_REFUSAL, &error).or_else(|| {
                (contents != initial)
                    .then(|| format!("il batch non e tornato indietro per intero: [{contents}]"))
            });
            record_refusal(
                recorder,
                "provider.profile_write_delete_by_keys_rollback",
                question,
                &error,
                mismatch,
                &contents,
            );
        }
    }
    let _ = connection
        .query_drop(format!("DROP TABLE IF EXISTS {SCRATCH_DELETE_CHILD}"))
        .await;

    // 3. La cancellazione a meta.
    reset_scratch(connection, SCRATCH_DELETE, "", &seed).await;
    let question =
        "una cancellazione a meta DeleteByKeys non toglie righe, e il provider resta usabile";
    let interrupted = CancellationToken::new();
    match scripted_write(
        provider,
        operation,
        key_schema(),
        vec![key_batch(&[1, 2]), key_batch(&[3])],
        3,
        Some((2, interrupted.clone())),
        &interrupted,
    )
    .await
    {
        Ok(written) => recorder.accepted(
            "provider.profile_write_delete_by_keys_cancellation",
            "provider",
            "profilo",
            question,
            format!(
                "nessun errore: la cancellazione non ha interrotto, cancellate={:?}",
                written.rows.deleted
            ),
        ),
        Err(error) => {
            let contents = table_contents(connection, SCRATCH_DELETE).await;
            let resumed = ExpectedCounts {
                received: 1,
                confirmed: 1,
                inserted: Some(0),
                updated: Some(0),
                deleted: Some(1),
                skipped: 0,
            };
            let recovered = scripted_write(
                provider,
                operation,
                key_schema(),
                vec![key_batch(&[4])],
                1,
                None,
                cancellation,
            )
            .await;
            let after = table_contents(connection, SCRATCH_DELETE).await;
            let mismatch = refusal_mismatch(&CANCELLED_REFUSAL, &error)
                .or_else(|| {
                    (contents != initial)
                        .then(|| format!("la cancellazione ha tolto righe: [{contents}]"))
                })
                .or_else(|| match &recovered {
                    Ok(written) => outcome_mismatch(written, profile, &resumed)
                        .or_else(|| {
                            (after != "righe=3 contenuto=1:vecchio-1,2:vecchio-2,3:vecchio-3")
                                .then(|| format!("dopo la ripresa la tabella contiene [{after}]"))
                        })
                        .map(|reason| format!("la ripresa non e quella attesa: {reason}")),
                    Err(next) => Some(format!(
                        "il provider non e piu utilizzabile: {:?}: {}",
                        next.category, next.message
                    )),
                });
            record_refusal(
                recorder,
                "provider.profile_write_delete_by_keys_cancellation",
                question,
                &error,
                mismatch,
                &format!("dopo [{contents}], ripresa [{after}]"),
            );
        }
    }
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
    // e la sola risposta possibile.
    //
    // E il rifiuto vale come prova **solo** se viene da quella regola. Una
    // sessione che non si apre, un catalogo che non risponde, una colonna che
    // manca: sono tutti `Err`, e registrarli come `rejected` direbbe che
    // `spatial.read_wkb` resta chiusa per la ragione sorvegliata quando invece
    // la sonda non ci e mai arrivata. Diventano `not_measured`, che e cio che
    // sono, e il gate li conta come prova mancante.
    let geometry_question =
        "descrive e mappa una tabella con colonna geometry, con il profilo del prodotto";
    let unreached = |what: &str, error: &plenora_database_core::DatabaseError| {
        format!(
            "{what}: {:?}: {} — la regola sull'SRID non e stata raggiunta",
            error.category, error.message
        )
    };
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
                            geometry_question,
                            format!("colonne={}", specs.len()),
                        ),
                        Err(error) => {
                            // Il frammento non nomina il prodotto perche il
                            // messaggio lo porta gia, e il controllo su quello
                            // e separato: cosi un rifiuto giusto attribuito al
                            // prodotto sbagliato non passa per buono.
                            let contract = RefusalContract {
                                category: ErrorCategory::Crs,
                                phase: ErrorPhase::Prepare,
                                remote_effect: RemoteEffect::None,
                                retry: RetryDisposition::Never,
                                message_contains: "senza SRID dichiarato",
                            };
                            match refusal_mismatch(&contract, &error) {
                                Some(mismatch) => recorder.not_measured(
                                    "provider.profile_describe_geometry",
                                    "provider",
                                    "profilo",
                                    geometry_question,
                                    &format!("rifiuto per un'altra ragione — {mismatch}"),
                                ),
                                None if !error.message.contains(profile.product()) => recorder
                                    .not_measured(
                                        "provider.profile_describe_geometry",
                                        "provider",
                                        "profilo",
                                        geometry_question,
                                        &format!(
                                            "il rifiuto non nomina {}: {}",
                                            profile.product(),
                                            error.message
                                        ),
                                    ),
                                None => recorder.rejected(
                                    "provider.profile_describe_geometry",
                                    "provider",
                                    "profilo",
                                    geometry_question,
                                    condense(&format!(
                                        "{:?}/{:?}/{:?}/{:?}: {}",
                                        error.category,
                                        error.phase,
                                        error.remote_effect,
                                        error.retry,
                                        error.message
                                    )),
                                    crate::evidence::server_code_in_message(&error.message),
                                ),
                            }
                        }
                    }
                }
                Err(error) => recorder.not_measured(
                    "provider.profile_describe_geometry",
                    "provider",
                    "profilo",
                    geometry_question,
                    &unreached("catalogo", &error),
                ),
            }
        }
        Err(error) => recorder.not_measured(
            "provider.profile_describe_geometry",
            "provider",
            "profilo",
            geometry_question,
            &unreached("sessione non aperta", &error),
        ),
    }

    functional_index_probes(recorder, profile, &pool, &schema_name, cancellation).await;
    profile_read_probes(recorder, profile, &schema_name, cancellation).await;
    generated_column_probes(recorder, profile, &schema_name, cancellation).await;
    append_write_probes(recorder, profile, &schema_name, cancellation).await;
    create_write_probes(recorder, profile, &schema_name, cancellation).await;
    update_write_probes(recorder, profile, &schema_name, cancellation).await;
    remaining_write_probes(recorder, profile, &schema_name, cancellation).await;
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
#[allow(clippy::too_many_lines)]
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
    // La DDL si esegue qui invece che dentro `record_server_code` perche il
    // suo esito serve due volte: come misura, e come premessa di cio che il
    // catalogo dovra mostrare dopo.
    let ddl = connection
        .query_drop(format!(
            "CREATE INDEX plenora_idx_expression ON {SCRATCH} ((LOWER(text_utf8)))"
        ))
        .await;
    let ddl_question = "accetta un indice su espressione, che e cio che popola EXPRESSION";
    match &ddl {
        Ok(()) => recorder.accepted(
            "raw.functional_index_ddl",
            "raw",
            "errori",
            ddl_question,
            "nessun errore: il server ha accettato".to_owned(),
        ),
        Err(error) => recorder.rejected(
            "raw.functional_index_ddl",
            "raw",
            "errori",
            ddl_question,
            condense(&error.to_string()),
            server_code(error),
        ),
    }
    // L'esito della DDL decide cosa il catalogo **deve** dire dopo, e per
    // deciderlo deve essere **quello misurato**: MySQL accetta, MariaDB
    // rifiuta con 1064. Un errore qualunque al suo posto — un privilegio
    // mancante, un timeout — diventava "l'indice non c'e", e un catalogo
    // senza indice passava per la conferma di un rifiuto mai avvenuto.
    let ddl_mismatch = ExpressionIndexDdl::of(profile)
        .mismatch(ddl.as_ref().copied().map_err(crate::evidence::server_code));
    let expression_index_exists = ddl.is_ok();

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
                    let functional = description
                        .indexes
                        .iter()
                        .find(|index| index.name == "plenora_idx_expression");
                    let question =
                        "come il catalogo descrive gli indici dopo il tentativo su espressione";
                    // Se la DDL non ha dato l'esito misurato, il catalogo non
                    // ha una forma attesa da confrontare: la superficie non e
                    // stata provata, e dirlo e diverso dal dichiararla sana.
                    let mismatch = ddl_mismatch.clone().map_or_else(
                        || match (expression_index_exists, functional) {
                            (true, None) => Some(
                                "la DDL e stata accettata ma l'indice non compare nel catalogo"
                                    .to_owned(),
                            ),
                            (true, Some(index)) if index.column_backed => Some(format!(
                            "l'indice su espressione risulta confrontabile per colonne ({:?}): \
                             la regola che rifiuta un Upsert su un indice non \
                             confrontabile non sarebbe raggiungibile",
                            index.columns
                        )),
                            (false, Some(_)) => Some(
                                "la DDL e stata rifiutata ma l'indice compare lo stesso".to_owned(),
                            ),
                            _ => None,
                        },
                        Some,
                    );
                    match (mismatch, ddl_mismatch.is_some()) {
                        (None, _) => recorder.accepted(
                            "provider.profile_functional_index",
                            "provider",
                            "profilo",
                            question,
                            truncate(&described, 160),
                        ),
                        (Some(reason), true) => recorder.not_measured(
                            "provider.profile_functional_index",
                            "provider",
                            "profilo",
                            question,
                            &format!("premessa mancante: {reason}"),
                        ),
                        (Some(reason), false) => recorder.rejected(
                            "provider.profile_functional_index",
                            "provider",
                            "profilo",
                            question,
                            condense(&format!("contratto non soddisfatto: {reason}")),
                            None,
                        ),
                    }
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
    // Sessione e transazione sono il **preambolo** della sonda, non cio che
    // misura. Un checkout che non riesce o un `begin` che fallisce sono `Err`
    // come lo e il timeout, e registrarli come `rejected` direbbe che il
    // limite ha fatto il suo lavoro quando la query non e mai partita.
    let session = match pool.checkout(cancellation).await {
        Ok(session) => session,
        Err(error) => {
            recorder.not_measured(
                "provider.profile_timeout",
                "provider",
                "profilo",
                question,
                &format!(
                    "sessione non aperta: {:?}: {} — il timeout non e stato applicato",
                    error.category, error.message
                ),
            );
            return;
        }
    };
    let mut transaction =
        match crate::transaction::MysqlTransaction::begin(session, &options, cancellation).await {
            Ok(transaction) => transaction,
            Err(error) => {
                recorder.not_measured(
                    "provider.profile_timeout",
                    "provider",
                    "profilo",
                    question,
                    &format!(
                        "timeout non applicato dal profilo {}: {:?}: {} — la \
                         transazione non e nemmeno cominciata",
                        profile.product(),
                        error.category,
                        error.message
                    ),
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
