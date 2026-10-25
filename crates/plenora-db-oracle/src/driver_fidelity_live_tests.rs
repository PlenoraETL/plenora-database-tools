//! Prove live del fork `oracle-rs`: ogni valore scritto dal driver si rilegge
//! dal server con una resa indipendente dal decoder (`TO_CHAR`, `DUMP`,
//! `LENGTHB`, `RAWTOHEX`, `JSON_SERIALIZE`, `JSON_VALUE`,
//! `DBMS_LOB.GETLENGTH`, `VALIDATE_CONVERSION`), e ogni valore letto dal
//! driver si confronta con quella resa. Ciascun test fallisce sul fork di
//! d978250 (base della PR) per il difetto che nomina.

use crate::driver_codec_oracle_tests::{oracle_calendar_date, reference_date_bytes, Outcome};
use oracle_rs::serde_json::{self, json, Value as Json};
use oracle_rs::types::{LobData, LobValue, OracleDate, OracleTimestamp};
use oracle_rs::{Connection, QueryResult, Value};
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::{CancellationToken, ErrorPhase};
use std::time::Duration;

fn connect_string() -> (String, String, String) {
    let host = std::env::var("PLENORA_ORACLE_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let port = std::env::var("PLENORA_ORACLE_PORT").unwrap_or_else(|_| "1521".to_owned());
    let service = std::env::var("PLENORA_ORACLE_SERVICE").unwrap_or_else(|_| "FREEPDB1".to_owned());
    let user = std::env::var("PLENORA_ORACLE_USER").unwrap_or_else(|_| "plenora".to_owned());
    let password =
        std::env::var("PLENORA_ORACLE_PASSWORD").unwrap_or_else(|_| "Plenora_Test_2026".to_owned());
    (format!("{host}:{port}/{service}"), user, password)
}

async fn driver() -> Connection {
    let (target, user, password) = connect_string();
    Connection::connect(&target, &user, &password)
        .await
        .expect("connessione Oracle della fixture")
}

async fn drop_table(connection: &Connection, table: &str) {
    let _ = connection
        .execute(&format!("DROP TABLE {table} PURGE"), &[])
        .await;
}

async fn create(connection: &Connection, table: &str, columns: &str) {
    drop_table(connection, table).await;
    connection
        .execute(&format!("CREATE TABLE {table} ({columns})"), &[])
        .await
        .unwrap();
}

/// Tutte le righe di una query, attraverso ogni pagina di fetch.
async fn all_rows(connection: &Connection, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    let mut result: QueryResult = connection.query(sql, params).await.unwrap();
    let columns = result.columns.clone();
    let mut rows: Vec<Vec<Value>> = std::mem::take(&mut result.rows)
        .into_iter()
        .map(|row| row.values().to_vec())
        .collect();
    let mut more = result.has_more_rows;
    while more {
        let page = connection
            .fetch_more(result.cursor_id, &columns, 50)
            .await
            .unwrap();
        rows.extend(page.rows.into_iter().map(|row| row.values().to_vec()));
        more = page.has_more_rows;
    }
    rows
}

async fn single(connection: &Connection, sql: &str, params: &[Value]) -> Vec<Value> {
    let mut rows = all_rows(connection, sql, params).await;
    assert_eq!(rows.len(), 1, "{sql}");
    rows.remove(0)
}

/// `Value` non implementa `PartialEq`: il confronto passa dalla resa `Debug`,
/// esatta per testo, byte, interi e `f64` (la resa piu corta che rilegge lo
/// stesso valore).
#[track_caller]
fn assert_value(actual: &Value, expected: &Value, context: impl std::fmt::Display) {
    assert_eq!(format!("{actual:?}"), format!("{expected:?}"), "{context}");
}

fn text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Integer(value) => value.to_string(),
        Value::Number(value) => value.as_str().to_owned(),
        other => panic!("valore testuale atteso, trovato {other:?}"),
    }
}

fn integer(value: &Value) -> i64 {
    text(value).parse().unwrap()
}

/// Le colonne uguali alla riga precedente arrivano come duplicate nel bit
/// vector anche fra una pagina di fetch e la successiva: la prima riga di
/// ogni pagina deve avere i valori dell'ultima riga della pagina prima.
/// Base: quelle colonne tornavano NULL, in silenzio.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_duplicate_columns_keep_their_value_across_fetch_pages() {
    let connection = driver().await;
    let table = "PLENORA_ORS_DUPLICATES";
    create(
        &connection,
        table,
        "ID NUMBER(10) PRIMARY KEY, A VARCHAR2(20), B NUMBER",
    )
    .await;
    connection
        .execute(
            &format!(
                "INSERT INTO {table} SELECT LEVEL, 'costante', 7 FROM DUAL CONNECT BY LEVEL <= 500"
            ),
            &[],
        )
        .await
        .unwrap();
    connection.commit().await.unwrap();
    let mut result = connection
        .query(&format!("SELECT ID, A, B FROM {table} ORDER BY ID"), &[])
        .await
        .unwrap();
    let columns = result.columns.clone();
    let mut rows = std::mem::take(&mut result.rows);
    let mut more = result.has_more_rows;
    while more {
        let mut page = connection
            .fetch_more(result.cursor_id, &columns, 7)
            .await
            .unwrap();
        rows.append(&mut page.rows);
        more = page.has_more_rows;
    }
    assert_eq!(rows.len(), 500);
    for (index, row) in rows.iter().enumerate() {
        let values = row.values();
        assert_eq!(text(&values[0]), (index + 1).to_string());
        assert_eq!(text(&values[1]), "costante", "riga {}", index + 1);
        assert_eq!(text(&values[2]), "7", "riga {}", index + 1);
    }
    drop_table(&connection, table).await;
}

fn signed_year(year: i32) -> String {
    if year < 0 {
        format!("-{:04}", year.unsigned_abs())
    } else {
        format!(" {year:04}")
    }
}

fn date_label(date: &OracleDate) -> String {
    format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}",
        signed_year(date.year),
        date.month,
        date.day,
        date.hour,
        date.minute,
        date.second
    )
}

fn dump_bytes(dump: &str) -> Vec<u8> {
    let (_, list) = dump.split_once(": ").expect("DUMP con i byte");
    list.split(',')
        .map(|byte| byte.trim().parse().unwrap())
        .collect()
}

/// A1: ogni data legata dal driver si rilegge uguale dal server (etichetta
/// `TO_CHAR` e byte `DUMP`) e dal driver; ogni data che il driver rifiuta e
/// una che Oracle non conserverebbe con la stessa etichetta
/// (`VALIDATE_CONVERSION` falso, o `TO_DATE` che la normalizza altrove, come
/// i giorni del salto del 1582). Base: gli anni negativi andavano in panic,
/// 10000 e 25601 diventavano altri anni.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_dates_bind_exactly_on_every_year_boundary_of_the_server_calendar() {
    let mut connection = driver().await;
    let table = "PLENORA_ORS_DATES";
    create(&connection, table, "ID NUMBER(10) PRIMARY KEY, D DATE").await;
    let mut years: Vec<i32> = Vec::new();
    for range in [
        -4713..=-4700,
        -110..=-90,
        -10..=10,
        95..=105,
        1575..=1590,
        1895..=1905,
        1995..=2005,
        9990..=10_001,
    ] {
        years.extend(range);
    }
    years.push(25_601);
    let days = [
        (1, 1),
        (2, 28),
        (2, 29),
        (10, 4),
        (10, 5),
        (10, 14),
        (10, 15),
        (12, 31),
    ];
    let mut id = 0_i64;
    for year in years {
        // Una connessione per anno: il fork non chiude i cursori delle query
        // gia lette e ORA-01000 arriverebbe prima della fine (limite noto,
        // esplicito, fuori da questa prova).
        connection.commit().await.unwrap();
        connection = driver().await;
        for (month, day) in days {
            id += 1;
            let date = OracleDate::new(year, month, day, 23, 59, 59);
            let label = date_label(&date);
            let bound = std::panic::catch_unwind(|| date.to_oracle_bytes().outcome())
                .unwrap_or_else(|_| panic!("{label}: panic nell'encoder"));
            let server = single(
                &connection,
                "SELECT VALIDATE_CONVERSION(:1 AS DATE, 'SYYYY-MM-DD HH24:MI:SS') FROM DUAL",
                &[Value::String(label.trim().to_owned())],
            )
            .await;
            let server_valid = integer(&server[0]) == 1;
            let normalized = if server_valid {
                let row = single(
                    &connection,
                    "SELECT TO_CHAR(TO_DATE(:1, 'SYYYY-MM-DD HH24:MI:SS'), 'SYYYY-MM-DD HH24:MI:SS') FROM DUAL",
                    &[Value::String(label.trim().to_owned())],
                )
                .await;
                text(&row[0]) == label
            } else {
                false
            };
            assert_eq!(
                bound.is_some(),
                normalized,
                "{label}: il driver e il server non concordano"
            );
            assert_eq!(
                bound.is_some(),
                oracle_calendar_date(year, month, day),
                "{label}"
            );
            let inserted = connection
                .execute(
                    &format!("INSERT INTO {table} (ID, D) VALUES (:1, :2)"),
                    &[Value::Integer(id), Value::Date(date)],
                )
                .await;
            let Some(bytes) = bound else {
                assert!(inserted.is_err(), "{label}: data rifiutata ma inserita");
                continue;
            };
            inserted.unwrap();
            let row = single(
                &connection,
                &format!(
                    "SELECT TO_CHAR(D, 'SYYYY-MM-DD HH24:MI:SS'), DUMP(D), D FROM {table} WHERE ID = :1"
                ),
                &[Value::Integer(id)],
            )
            .await;
            assert_eq!(text(&row[0]), label);
            assert_eq!(dump_bytes(&text(&row[1])), bytes, "{label}");
            assert_eq!(
                bytes,
                reference_date_bytes(year, month, day, 23, 59, 59).to_vec()
            );
            assert_value(&row[2], &Value::Date(date), &label);
        }
    }
    connection.commit().await.unwrap();
    let count = single(&connection, &format!("SELECT COUNT(*) FROM {table}"), &[]).await;
    assert!(integer(&count[0]) > 0);
    drop_table(&connection, table).await;
}

/// A2: un TIMESTAMP legato con ogni precisione frazionaria si rilegge dal
/// server (`TO_CHAR ... FF9`) e dal driver con le stesse cifre in ogni
/// colonna TIMESTAMP(0..=9); un valore con nanosecondi si rifiuta in
/// lettura invece di troncarsi (base: troncato al microsecondo).
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_timestamps_bind_and_read_exactly_at_every_fractional_precision() {
    let connection = driver().await;
    let table = "PLENORA_ORS_TIMESTAMPS";
    let columns: Vec<String> = (0..=9).map(|p| format!("T{p} TIMESTAMP({p})")).collect();
    create(
        &connection,
        table,
        &format!("ID NUMBER(10) PRIMARY KEY, {}", columns.join(", ")),
    )
    .await;
    let placeholders: Vec<String> = (0..=9).map(|p| format!(":{}", p + 2)).collect();
    let mut micros = vec![0_u32, 1, 9, 10, 99, 100, 999_999, 500_000, 123_456, 654_321];
    for digits in 1..=6_u32 {
        let unit = 10_u32.pow(6 - digits);
        micros.extend([unit, unit * 5, unit * 9]);
    }
    micros.sort_unstable();
    micros.dedup();
    for (index, micro) in micros.iter().enumerate() {
        let id = i64::try_from(index).unwrap();
        let timestamp = OracleTimestamp::new(2024, 2, 29, 23, 59, 58, *micro);
        let mut params = vec![Value::Integer(id)];
        params.extend((0..=9).map(|_| Value::Timestamp(timestamp)));
        connection
            .execute(
                &format!(
                    "INSERT INTO {table} VALUES (:1, {})",
                    placeholders.join(", ")
                ),
                &params,
            )
            .await
            .unwrap();
        let rendered: Vec<String> = (0..=9)
            .map(|p| format!("TO_CHAR(T{p}, 'YYYY-MM-DD HH24:MI:SS.FF9')"))
            .collect();
        let read: Vec<String> = (6..=9).map(|p| format!("T{p}")).collect();
        let row = single(
            &connection,
            &format!(
                "SELECT {}, {} FROM {table} WHERE ID = :1",
                rendered.join(", "),
                read.join(", ")
            ),
            &[Value::Integer(id)],
        )
        .await;
        let exact = format!("2024-02-29 23:59:58.{micro:06}000");
        for precision in 6..=9 {
            assert_eq!(text(&row[precision]), exact, "T{precision}");
            assert_value(
                &row[10 + precision - 6],
                &Value::Timestamp(timestamp),
                format!("T{precision}"),
            );
        }
        for precision in 0..6 {
            // Il server arrotonda alla precisione della colonna: la resa del
            // server ha zeri oltre la precisione.
            let server = text(&row[precision]);
            assert!(
                server[20 + precision..].bytes().all(|b| b == b'0'),
                "{server}"
            );
        }
    }
    connection.rollback().await.unwrap();
    drop_table(&connection, table).await;
    // Lettura di nanosecondi: errore, non troncamento. Una connessione
    // nuova per ogni lettura rifiutata: un errore di decodifica a meta
    // risposta chiude la connessione che lo incontra.
    let read = driver()
        .await
        .query(
            "SELECT TIMESTAMP '2024-01-01 00:00:00.123456789' FROM DUAL",
            &[],
        )
        .await;
    assert!(read.is_err(), "nanosecondi letti come {read:?}");
}

fn pattern(length: usize) -> String {
    (0..length)
        .map(|i| char::from(b"abcdefghij"[i % 10]))
        .collect()
}

/// A11: VARCHAR2 e RAW legati con una lunghezza attorno a ogni soglia del
/// prefisso (252 corto, 253, 254, 255) si rileggono uguali dal server; il
/// testo oltre 4000 byte va in un CLOB con pezzi da 32767 e il terminatore.
/// Base: oltre 252 byte un solo pezzo senza terminatore.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_text_and_raw_binds_cross_every_length_prefix() {
    let connection = driver().await;
    let table = "PLENORA_ORS_TEXT_BINDS";
    create(
        &connection,
        table,
        "ID NUMBER(10) PRIMARY KEY, S VARCHAR2(4000 BYTE), R RAW(2000), C CLOB",
    )
    .await;
    let lengths = [1_usize, 251, 252, 253, 254, 255, 256, 1_000, 2_000, 4_000];
    for (index, length) in lengths.iter().enumerate() {
        let id = i64::try_from(index).unwrap();
        let text_value = pattern(*length);
        let raw_length = (*length).min(2_000);
        let raw: Vec<u8> = (0..raw_length)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        connection
            .execute(
                &format!("INSERT INTO {table} (ID, S, R) VALUES (:1, :2, :3)"),
                &[
                    Value::Integer(id),
                    Value::String(text_value.clone()),
                    Value::Bytes(raw.clone()),
                ],
            )
            .await
            .unwrap();
        let row = single(
            &connection,
            &format!(
                "SELECT LENGTHB(S), CASE WHEN S = RPAD('abcdefghij', :1, 'abcdefghij') THEN 1 ELSE 0 END, \
                 UTL_RAW.LENGTH(R), RAWTOHEX(R), S, R FROM {table} WHERE ID = :2"
            ),
            // Il driver lega per posizione, nell'ordine dei segnaposto nel testo.
            &[Value::Integer(i64::try_from(*length).unwrap()), Value::Integer(id)],
        )
        .await;
        assert_eq!(
            integer(&row[0]),
            i64::try_from(*length).unwrap(),
            "S di {length}"
        );
        assert_eq!(
            integer(&row[1]),
            1,
            "contenuto di S di {length}: {:?}",
            row[4]
        );
        assert_eq!(integer(&row[2]), i64::try_from(raw_length).unwrap());
        let hex: String = raw.iter().fold(String::new(), |mut hex, byte| {
            use std::fmt::Write;
            write!(hex, "{byte:02X}").unwrap();
            hex
        });
        assert_eq!(text(&row[3]), hex, "R di {raw_length}");
        assert_value(&row[4], &Value::String(text_value), "valore");
        assert_value(&row[5], &Value::Bytes(raw), "valore");
    }
    for (index, length) in [4_001_usize, 32_767, 32_768, 65_535, 65_536, 100_000]
        .iter()
        .enumerate()
    {
        let id = 100 + i64::try_from(index).unwrap();
        let value = pattern(*length);
        let inserted = connection
            .execute(
                &format!("INSERT INTO {table} (ID, C) VALUES (:1, :2)"),
                &[Value::Integer(id), Value::String(value.clone())],
            )
            .await;
        let Ok(_) = inserted else {
            // Un rifiuto del server e esplicito: ammesso, ma non per i valori
            // che stanno in un solo pezzo.
            assert!(*length > 32_767, "CLOB di {length} rifiutato: {inserted:?}");
            continue;
        };
        let row = single(
            &connection,
            &format!("SELECT DBMS_LOB.GETLENGTH(C), DBMS_LOB.SUBSTR(C, 20, :1), C FROM {table} WHERE ID = :2"),
            &[Value::Integer(i64::try_from(*length - 19).unwrap()), Value::Integer(id)],
        )
        .await;
        assert_eq!(
            integer(&row[0]),
            i64::try_from(*length).unwrap(),
            "CLOB di {length}"
        );
        assert_eq!(
            text(&row[1]),
            value[*length - 20..],
            "coda del CLOB di {length}"
        );
        let Value::Lob(LobValue::Locator(locator)) = &row[2] else {
            panic!("locator atteso per il CLOB di {length}");
        };
        let LobData::String(read) = connection.read_lob(locator).await.unwrap() else {
            panic!("testo atteso");
        };
        assert_eq!(read, value, "CLOB di {length}");
    }
    connection.rollback().await.unwrap();
    drop_table(&connection, table).await;
}

/// Lettura: testo di ogni lunghezza 1..=300 e 4000, attorno alle soglie del
/// prefisso di lunghezza del server, uguale a quello generato qui.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_text_of_every_length_reads_exactly() {
    let connection = driver().await;
    let rows = all_rows(
        &connection,
        "SELECT LEVEL, RPAD('abcdefghij', LEVEL, 'abcdefghij') FROM DUAL CONNECT BY LEVEL <= 300 \
         UNION ALL SELECT 4000, RPAD('abcdefghij', 4000, 'abcdefghij') FROM DUAL",
        &[],
    )
    .await;
    assert_eq!(rows.len(), 301);
    for row in rows {
        let length = usize::try_from(integer(&row[0])).unwrap();
        assert_eq!(text(&row[1]), pattern(length), "{length}");
    }
}

fn keys_of_every_length() -> Json {
    let object: serde_json::Map<String, Json> = (0..=255_usize)
        .map(|length| {
            let key: String = (0..length)
                .map(|i| char::from(b'a' + u8::try_from((i + length) % 26).unwrap()))
                .collect();
            (key, json!(length))
        })
        .collect();
    Json::Object(object)
}

async fn insert_json(connection: &Connection, table: &str, id: i64, value: Value) -> bool {
    connection
        .execute(
            &format!("INSERT INTO {table} (ID, J) VALUES (:1, :2)"),
            &[Value::Integer(id), value],
        )
        .await
        .is_ok()
}

/// A3/A4/A12: documenti JSON legati come OSON si rileggono uguali dal server
/// (`JSON_SERIALIZE`, e `JSON_VALUE` per ogni chiave) e dal driver: chiavi di
/// ogni lunghezza 0..=255, piu di 255 nomi (id a due byte), annidamento fino
/// al limite, stringhe e documenti attorno a 252/253/254 e 65535/65536 byte.
/// Chiavi oltre 255 byte e numeri che OSON non codifica sono errori, non
/// valori diversi o NULL (A5). SQL NULL e JSON null restano distinti.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
#[allow(clippy::too_many_lines)]
async fn live_json_binds_round_trip_through_json_serialize() {
    let mut connection = driver().await;
    let table = "PLENORA_ORS_JSON";
    create(&connection, table, "ID NUMBER(10) PRIMARY KEY, J JSON").await;
    let mut documents: Vec<Json> = vec![keys_of_every_length()];
    for count in [255_usize, 256, 300, 1_000, 5_000] {
        let object: serde_json::Map<String, Json> =
            (0..count).map(|i| (format!("k{i}"), json!(i))).collect();
        documents.push(Json::Object(object));
    }
    for depth in [1_usize, 50, 127] {
        let mut value = json!("foglia");
        for level in 0..depth {
            value = if level % 2 == 0 {
                json!({ "a": value })
            } else {
                json!([value])
            };
        }
        documents.push(value);
    }
    for length in (200..=280).chain(65_480..=65_560).step_by(1) {
        documents.push(json!({ "s": "x".repeat(length) }));
    }
    for (index, document) in documents.iter().enumerate() {
        let id = i64::try_from(index).unwrap();
        // Una connessione per documento (vedi la prova delle date: ORA-01000).
        connection.commit().await.unwrap();
        connection = driver().await;
        let inserted = connection
            .execute(
                &format!("INSERT INTO {table} (ID, J) VALUES (:1, :2)"),
                &[Value::Integer(id), Value::Json(document.clone())],
            )
            .await;
        assert!(inserted.is_ok(), "documento {index}: {inserted:?}");
        connection.commit().await.unwrap();
        let row = single(
            &connection,
            &format!("SELECT JSON_SERIALIZE(J RETURNING CLOB), J FROM {table} WHERE ID = :1"),
            &[Value::Integer(id)],
        )
        .await;
        let serialized = match &row[0] {
            Value::Lob(LobValue::Locator(locator)) => {
                match connection.read_lob(locator).await.unwrap() {
                    LobData::String(text) => text,
                    LobData::Bytes(_) => panic!("testo atteso"),
                }
            }
            other => text(other),
        };
        let server: Json = serde_json::from_str(&serialized).unwrap();
        assert_eq!(&server, document, "JSON_SERIALIZE del documento {index}");
        assert_value(
            &row[1],
            &Value::Json(document.clone()),
            format!("driver, documento {index}"),
        );
        if let Json::Object(map) = document {
            if index < 4 {
                for (position, (key, expected)) in
                    map.iter().filter(|(key, _)| !key.is_empty()).enumerate()
                {
                    if position % 100 == 99 {
                        connection = driver().await;
                    }
                    let path = format!("$.\"{key}\"");
                    let value = single(
                        &connection,
                        &format!(
                            "SELECT JSON_VALUE(J, '{path}' RETURNING NUMBER NULL ON ERROR) FROM {table} WHERE ID = :1"
                        ),
                        &[Value::Integer(id)],
                    )
                    .await;
                    assert_eq!(
                        integer(&value[0]),
                        expected.as_i64().unwrap(),
                        "JSON_VALUE {path} nel documento {index}"
                    );
                }
            }
        }
    }
    let refused = [
        json!({ "x".repeat(256): 1 }),
        json!({ "y".repeat(300): 1 }),
        json!({ "n": 1e300 }),
        json!({ "n": -1e-200 }),
    ];
    for (index, document) in refused.iter().enumerate() {
        let id = 10_000 + i64::try_from(index).unwrap();
        assert!(
            !insert_json(&connection, table, id, Value::Json(document.clone())).await,
            "documento non codificabile {index} inserito"
        );
    }
    let stored = single(
        &connection,
        &format!("SELECT COUNT(*) FROM {table} WHERE ID >= 10000"),
        &[],
    )
    .await;
    assert_eq!(
        integer(&stored[0]),
        0,
        "un documento rifiutato e stato scritto"
    );
    // SQL NULL e JSON null.
    assert!(insert_json(&connection, table, 20_000, Value::Null).await);
    assert!(insert_json(&connection, table, 20_001, Value::Json(Json::Null)).await);
    let rows = all_rows(
        &connection,
        &format!(
            "SELECT ID, CASE WHEN J IS NULL THEN 1 ELSE 0 END, J FROM {table} WHERE ID >= 20000 ORDER BY ID"
        ),
        &[],
    )
    .await;
    assert_eq!(integer(&rows[0][1]), 1);
    assert_value(&rows[0][2], &Value::Null, "SQL NULL letto come JSON null");
    assert_eq!(integer(&rows[1][1]), 0);
    assert_value(&rows[1][2], &Value::Json(Json::Null), "valore");
    connection.rollback().await.unwrap();
    drop_table(&connection, table).await;
}

/// A6/A7: JSON costruito dal server si legge come lo rende `JSON_SERIALIZE`
/// (date, timestamp con nanosecondi, offset, binari, id) o si rifiuta: un
/// numero con piu cifre di un f64 non diventa un altro numero.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_json_reads_match_json_serialize_or_are_refused() {
    let connection = driver().await;
    let documents = [
        "JSON_OBJECT('d' VALUE DATE '2024-03-15', 'bc' VALUE DATE '-0044-03-15', \
         't' VALUE TIMESTAMP '2024-03-15 14:30:45.123456789', \
         't6' VALUE TIMESTAMP '2024-03-15 14:30:45.5', \
         'tz' VALUE TIMESTAMP '2024-03-15 14:30:45.5 +02:00', \
         'tzn' VALUE TIMESTAMP '2024-03-15 23:30:45 -05:30', \
         'b' VALUE HEXTORAW('0A0BFF'), 'f' VALUE TO_BINARY_DOUBLE(0.1), \
         'n' VALUE 0.1, 'i' VALUE 12345678901234, 'm' VALUE -1.5e-100 RETURNING JSON)",
        "JSON('{\"a\":[1,2,{\"b\":null,\"c\":true}],\"s\":\"\u{e8}\u{1F600}\"}')",
    ];
    for document in documents {
        let row = single(
            &connection,
            &format!("SELECT JSON_SERIALIZE({document}), {document} FROM DUAL"),
            &[],
        )
        .await;
        let server: Json = serde_json::from_str(&text(&row[0])).unwrap();
        assert_value(&row[1], &Value::Json(server), document);
    }
    // Una JSON collection: l'`_id` e un id OSON, reso in esadecimale minuscolo.
    let _ = connection
        .execute("DROP TABLE PLENORA_ORS_COLLECTION PURGE", &[])
        .await;
    connection
        .execute("CREATE JSON COLLECTION TABLE PLENORA_ORS_COLLECTION", &[])
        .await
        .unwrap();
    connection
        .execute(
            "INSERT INTO PLENORA_ORS_COLLECTION VALUES ('{\"nome\":\"x\",\"v\":[1,2]}')",
            &[],
        )
        .await
        .unwrap();
    let row = single(
        &connection,
        "SELECT JSON_SERIALIZE(DATA), DATA FROM PLENORA_ORS_COLLECTION",
        &[],
    )
    .await;
    let server: Json = serde_json::from_str(&text(&row[0])).unwrap();
    assert!(server["_id"].is_string());
    assert_value(&row[1], &Value::Json(server), "JSON collection");
    connection.rollback().await.unwrap();
    let _ = connection
        .execute("DROP TABLE PLENORA_ORS_COLLECTION PURGE", &[])
        .await;
    for document in [
        "JSON('{\"n\":0.30000000000000001}')",
        "JSON('{\"n\":123456789012345678901234567890}')",
        "JSON_OBJECT('i' VALUE INTERVAL '1-2' YEAR TO MONTH RETURNING JSON)",
        // Base: NaN e infiniti dentro un JSON diventavano `null`.
        "JSON_OBJECT('a' VALUE BINARY_DOUBLE_INFINITY RETURNING JSON)",
        "JSON_OBJECT('a' VALUE -BINARY_DOUBLE_INFINITY RETURNING JSON)",
        "JSON_OBJECT('a' VALUE TO_BINARY_DOUBLE('NaN') RETURNING JSON)",
        "JSON_ARRAY(BINARY_FLOAT_NAN RETURNING JSON)",
        "JSON_SCALAR(BINARY_DOUBLE_INFINITY)",
    ] {
        let read = driver()
            .await
            .query(&format!("SELECT {document} FROM DUAL"), &[])
            .await;
        assert!(read.is_err(), "{document} letto come {read:?}");
    }
}

/// A8: `BINARY_DOUBLE`, `BOOLEAN` e il testo nazionale si leggono esatti;
/// INTERVAL e un tipo senza decoder si rifiutano. Base: testo lossy dei byte.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_scalar_types_read_exactly_or_are_refused() {
    let connection = driver().await;
    for literal in [
        "'0.1'",
        "'-2.5'",
        "'1.7976931348623157E308'",
        "'4.9E-324'",
        "'123456.789'",
    ] {
        let row = single(
            &connection,
            &format!(
                "SELECT TO_BINARY_DOUBLE({literal}), TO_CHAR(TO_BINARY_DOUBLE({literal}), 'TM9') FROM DUAL"
            ),
            &[],
        )
        .await;
        let expected: f64 = text(&row[1]).parse().unwrap();
        assert_value(&row[0], &Value::Float(expected), literal);
    }
    let row = single(&connection, "SELECT TRUE, FALSE FROM DUAL", &[]).await;
    assert_value(&row[0], &Value::Boolean(true), "TRUE");
    assert_value(&row[1], &Value::Boolean(false), "FALSE");
    let row = single(
        &connection,
        "SELECT N'ab\u{e8}', TO_NCHAR(UNISTR('\\D83D\\DE00x')), LENGTHB(N'ab\u{e8}') FROM DUAL",
        &[],
    )
    .await;
    assert_value(&row[0], &Value::String("ab\u{e8}".to_owned()), "valore");
    assert_value(&row[1], &Value::String("\u{1F600}x".to_owned()), "valore");
    assert_eq!(integer(&row[2]), 6, "N'ab\u{e8}' in AL16UTF16");
    for query in [
        "SELECT INTERVAL '1-2' YEAR TO MONTH FROM DUAL",
        "SELECT INTERVAL '1 02:03:04' DAY TO SECOND FROM DUAL",
    ] {
        let read = driver().await.query(query, &[]).await;
        assert!(read.is_err(), "{query} letto come {read:?}");
    }
}

/// D1/D2: il describe porta la scala con il segno (NUMBER senza scala e
/// -127, NUMBER(5,-2) e -2; base: 129 e 254) e un tipo senza decoder (REF)
/// si rifiuta invece di essere descritto come VARCHAR2.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_describe_keeps_signed_scale_and_refuses_unknown_types() {
    let connection = driver().await;
    let table = "PLENORA_ORS_DESCRIBE";
    create(
        &connection,
        table,
        "A NUMBER, B NUMBER(5,-2), C NUMBER(10,2), D FLOAT",
    )
    .await;
    let result = connection
        .query(&format!("SELECT A, B, C, D FROM {table}"), &[])
        .await
        .unwrap();
    let scales: Vec<i16> = result.columns.iter().map(|column| column.scale).collect();
    let precisions: Vec<i16> = result
        .columns
        .iter()
        .map(|column| column.precision)
        .collect();
    assert_eq!(scales, vec![-127, -2, 2, -127]);
    assert_eq!(precisions, vec![0, 5, 10, 126]);
    drop_table(&connection, table).await;
    let _ = connection
        .execute("DROP TABLE PLENORA_ORS_REFS PURGE", &[])
        .await;
    let _ = connection
        .execute("DROP TYPE PLENORA_ORS_POINT FORCE", &[])
        .await;
    connection
        .execute("CREATE TYPE PLENORA_ORS_POINT AS OBJECT (X NUMBER)", &[])
        .await
        .unwrap();
    connection
        .execute("CREATE TABLE PLENORA_ORS_REFS OF PLENORA_ORS_POINT", &[])
        .await
        .unwrap();
    connection
        .execute(
            "INSERT INTO PLENORA_ORS_REFS VALUES (PLENORA_ORS_POINT(1))",
            &[],
        )
        .await
        .unwrap();
    connection.commit().await.unwrap();
    let read = driver()
        .await
        .query("SELECT REF(P) FROM PLENORA_ORS_REFS P", &[])
        .await;
    assert!(read.is_err(), "REF letto come {read:?}");
    let _ = connection
        .execute("DROP TABLE PLENORA_ORS_REFS PURGE", &[])
        .await;
    let _ = connection
        .execute("DROP TYPE PLENORA_ORS_POINT FORCE", &[])
        .await;
}

/// A10: il provider scrive un testo lungo in un CLOB temporaneo a pezzi da
/// 4000 byte; con caratteri fuori dal BMP l'offset deve contare le unita
/// UTF-16 del server. Base: contava i caratteri e ogni pezzo sovrascriveva
/// la coda del precedente.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_clob_chunks_with_supplementary_characters_are_contiguous() {
    let connection = driver().await;
    let table = "PLENORA_ORS_CLOB";
    create(&connection, table, "ID NUMBER(10) PRIMARY KEY, C CLOB").await;
    let texts = [
        "\u{1F600}".repeat(3_000),
        format!("{}{}", "a".repeat(3_999), "\u{1F600}".repeat(2_000)),
        "\u{e8}\u{1F600}x".repeat(2_500),
    ];
    let cancellation = CancellationToken::new();
    for (index, value) in texts.iter().enumerate() {
        let id = i64::try_from(index).unwrap();
        let mut cache = crate::parameter::LobCache::default();
        let mut bound = crate::parameter::bind_parameters_with_lobs(
            &connection,
            &[ParameterValue::String(value.clone())],
            true,
            &mut cache,
            Duration::from_secs(30),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .unwrap();
        connection
            .execute(
                &format!("INSERT INTO {table} (ID, C) VALUES (:1, :2)"),
                &[Value::Integer(id), bound.remove(0)],
            )
            .await
            .unwrap();
        let utf16 = value.encode_utf16().count();
        let row = single(
            &connection,
            &format!("SELECT DBMS_LOB.GETLENGTH(C), C FROM {table} WHERE ID = :1"),
            &[Value::Integer(id)],
        )
        .await;
        assert_eq!(
            integer(&row[0]),
            i64::try_from(utf16).unwrap(),
            "testo {index}"
        );
        let Value::Lob(LobValue::Locator(locator)) = &row[1] else {
            panic!("locator atteso");
        };
        let LobData::String(read) = connection.read_lob(locator).await.unwrap() else {
            panic!("testo atteso");
        };
        assert_eq!(&read, value, "testo {index}");
    }
    connection.rollback().await.unwrap();
    drop_table(&connection, table).await;
}

/// Un LOB inline o vuoto senza locator non si lega come NULL (base: il
/// contenuto spariva e la riga aveva NULL).
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_inline_lob_binds_are_refused_not_nulled() {
    let connection = driver().await;
    let table = "PLENORA_ORS_INLINE_LOB";
    create(&connection, table, "ID NUMBER(10), B BLOB").await;
    for (id, lob) in [
        (1, LobValue::Inline(b"contenuto".to_vec().into())),
        (2, LobValue::Empty),
    ] {
        let inserted = connection
            .execute(
                &format!("INSERT INTO {table} (ID, B) VALUES (:1, :2)"),
                &[Value::Integer(id), Value::Lob(lob)],
            )
            .await;
        assert!(inserted.is_err(), "LOB {id} legato come {inserted:?}");
    }
    let count = single(&connection, &format!("SELECT COUNT(*) FROM {table}"), &[]).await;
    assert_eq!(integer(&count[0]), 0);
    connection.rollback().await.unwrap();
    drop_table(&connection, table).await;
}

/// Calendario: prima del 1582-10-15 Oracle conta i giorni col calendario
/// giuliano, chrono col gregoriano prolettico; il provider rifiuta la data in
/// lettura (base: l'etichetta passava, un altro giorno per Arrow) e accetta
/// il primo giorno gregoriano.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_pre_gregorian_dates_are_refused_by_the_provider() {
    let connection = driver().await;
    let cancellation = CancellationToken::new();
    for (query, accepted) in [
        ("SELECT DATE '1500-03-01' FROM DUAL", false),
        ("SELECT DATE '-0044-03-15' FROM DUAL", false),
        ("SELECT TIMESTAMP '1582-10-04 12:00:00' FROM DUAL", false),
        ("SELECT DATE '1582-10-15' FROM DUAL", true),
    ] {
        let result = connection.query(query, &[]).await.unwrap();
        let rows = crate::decode::rows_from_result(
            &connection,
            Duration::from_secs(30),
            ErrorPhase::Read,
            &cancellation,
            result,
        )
        .await;
        assert_eq!(rows.is_ok(), accepted, "{query}: {rows:?}");
    }
}
