//! Fedelta dei parametri Oracle: ogni variante di `ParameterValue` o arriva
//! al server esatta, o e rifiutata in `Prepare`
//! (`plenora_database_testkit::parameter_fidelity`, con l'oracolo della resa
//! del server e i vicini generati per famiglia).

use super::fixture;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::provider::{ParameterValue as V, Provider};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_database_testkit::parameter_fidelity::{
    report, run, Case, Expect, FidelityPath, NativeSql, Target,
};

const TABLE: &str = "PARAM_FIDELITY";
const NATIVE_PORTABLE: &[FidelityPath] = &[FidelityPath::Native, FidelityPath::Portable];

/// La resa canonica del server, indipendente dai decoder del repository; il
/// testo come byte UTF-8 (`UTL_I18N`), anche per le forme nazionali.
fn raw_sql(column: &str) -> String {
    match column {
        "B" => "RAWTOHEX(B)".to_owned(),
        "S" | "N" | "NC" => format!("RAWTOHEX(UTL_I18N.STRING_TO_RAW({column}, 'AL32UTF8'))"),
        "CL" | "NCL" => format!(
            "RAWTOHEX(UTL_I18N.STRING_TO_RAW(DBMS_LOB.SUBSTR({column}, 4000, 1), 'AL32UTF8'))"
        ),
        "F" => "RAWTOHEX(UTL_RAW.CAST_FROM_BINARY_DOUBLE(F))".to_owned(),
        "DT" => "TO_CHAR(DT, 'YYYY-MM-DD\"T\"HH24:MI:SS')".to_owned(),
        "TS" | "TS0" => format!("TO_CHAR({column}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6')"),
        "TS9" => "TO_CHAR(TS9, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')".to_owned(),
        "TZ" => "TO_CHAR(TZ, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM')".to_owned(),
        "J" => "JSON_SERIALIZE(J)".to_owned(),
        _ => format!("TO_CHAR({column})"),
    }
}

/// Il segnaposto del valore: un `TimestampTz` si binda come testo nel modello
/// del provider e lo statement lo converte esplicitamente (limite dichiarato
/// su `crate::parameter::parse_timestamp_tz`).
fn value(column: &str) -> String {
    if column == "TZ" {
        format!(
            "TO_TIMESTAMP_TZ(:2, '{}')",
            plenora_database_core::provider::ORACLE_TIMESTAMP_TZ_FORMAT_MODEL
        )
    } else {
        ":2".to_owned()
    }
}

fn native(kind: NativeSql, column: &str) -> String {
    match kind {
        NativeSql::Insert => format!(
            "INSERT INTO {TABLE} (ID, {column}) VALUES (:1, {})",
            value(column)
        ),
        NativeSql::Select => format!("SELECT {column} FROM {TABLE} WHERE ID = :1"),
        NativeSql::Count => format!(
            "SELECT COUNT(*) FROM {TABLE} WHERE ID = :1 AND {column} = {}",
            value(column)
        ),
        NativeSql::Raw => format!(
            "SELECT NVL({}, 'NULL') FROM {TABLE} WHERE ID = :1",
            raw_sql(column)
        ),
        NativeSql::Delete => format!("DELETE FROM {TABLE} WHERE ID = :1"),
    }
}

fn text(value: &str) -> V {
    V::String(value.to_owned())
}

fn utf8_hex(value: &str) -> String {
    value
        .bytes()
        .fold(String::new(), |out, byte| out + &format!("{byte:02X}"))
}

fn double_hex(value: f64) -> String {
    // `CAST_FROM_BINARY_DOUBLE` rende i bit IEEE in big-endian.
    format!("{:016X}", value.to_bits())
}

fn exact(value: &V, raw: impl Into<String>) -> Expect {
    Expect::exact(value.clone(), raw)
}

#[allow(clippy::too_many_lines)]
fn cases() -> Vec<Case> {
    let bytes = V::Bytes(vec![0xAB, 0x01]);
    let nonbmp = text("\u{e8}\u{1F600}");
    let decimal = V::Decimal("12345.6789".to_owned());
    let two_53 = 1_i64 << 53;
    #[allow(clippy::cast_precision_loss)]
    let float_two_53 = two_53 as f64;
    let timestamp = V::Timestamp("2024-01-01T10:20:30.123456".to_owned());
    let mapping = Expect::Refused(ErrorCategory::DataMapping);
    vec![
        Case::new(
            "bytes 0xAB01 in RAW",
            "B",
            bytes.clone(),
            exact(&bytes, "AB01"),
        ),
        Case::new("bytes vuoti", "B", V::Bytes(Vec::new()), mapping.clone()),
        Case::new(
            "testo non-BMP",
            "S",
            nonbmp.clone(),
            exact(&nonbmp, utf8_hex("\u{e8}\u{1F600}")),
        ),
        Case::new("testo vuoto", "S", text(""), mapping.clone()),
        Case::new(
            "testo con NUL",
            "S",
            text("a\0b"),
            exact(&text("a\0b"), "610062"),
        ),
        // Il difetto 5 del secondo giro: NVARCHAR2 e NCHAR in AL16UTF16 si
        // leggevano come UTF-8 ("\0a\0b"), in query e in Arrow.
        Case::new(
            "testo in NVARCHAR2",
            "N",
            nonbmp.clone(),
            exact(&nonbmp, utf8_hex("\u{e8}\u{1F600}")),
        ),
        Case::new(
            "testo in NCHAR(4)",
            "NC",
            text("abcd"),
            exact(&text("abcd"), "61626364"),
        ),
        // Un LOB non si confronta con `=`: conservato e riletto.
        Case::new(
            "testo in NCLOB",
            "NCL",
            nonbmp.clone(),
            Expect::Assigned {
                read: nonbmp.clone(),
                raw: utf8_hex("\u{e8}\u{1F600}"),
            },
        )
        .only(&[FidelityPath::Native]),
        Case::new(
            "testo in CLOB",
            "CL",
            nonbmp.clone(),
            Expect::Assigned {
                read: nonbmp,
                raw: utf8_hex("\u{e8}\u{1F600}"),
            },
        )
        .only(&[FidelityPath::Native]),
        Case::new(
            "decimal esatto",
            "D",
            decimal.clone(),
            exact(&decimal, "12345.6789"),
        ),
        Case::new(
            "decimal oltre la scala della colonna",
            "D",
            V::Decimal("12345.67890001".to_owned()),
            Expect::Assigned {
                read: decimal,
                raw: "12345.6789".to_owned(),
            },
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
        Case::new(
            "decimal con esponente",
            "D",
            V::Decimal("1.5E2".to_owned()),
            exact(&V::Decimal("150".to_owned()), "150"),
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
        Case::new(
            "double 0.1 in BINARY_DOUBLE",
            "F",
            V::F64(0.1),
            exact(&V::F64(0.1), double_hex(0.1)),
        ),
        Case::new(
            "double 2^53",
            "F",
            V::F64(float_two_53),
            exact(&V::F64(float_two_53), double_hex(float_two_53)),
        ),
        // Misurato: Oracle conserva -0.0 come +0.0.
        Case::new("double -0.0", "F", V::F64(-0.0), mapping.clone()),
        Case::new("double NaN", "F", V::F64(f64::NAN), mapping.clone()),
        Case::new(
            "bigint 2^53 + 1",
            "I",
            V::I64(two_53 + 1),
            exact(&V::I64(two_53 + 1), (two_53 + 1).to_string()),
        ),
        Case::new(
            "bigint minimo",
            "I",
            V::I64(i64::MIN),
            exact(&V::I64(i64::MIN), i64::MIN.to_string()),
        ),
        Case::new("boolean", "BO", V::Bool(true), exact(&V::I64(1), "1")),
        Case::new(
            "date",
            "DT",
            V::Date("2024-02-29".to_owned()),
            exact(
                &V::Timestamp("2024-02-29T00:00:00".to_owned()),
                "2024-02-29T00:00:00",
            ),
        ),
        Case::new(
            "timestamp in DATE",
            "DT",
            V::Timestamp("2024-02-29T10:20:30".to_owned()),
            exact(
                &V::Timestamp("2024-02-29T10:20:30".to_owned()),
                "2024-02-29T10:20:30",
            ),
        ),
        Case::new(
            "timestamp senza frazione",
            "TS",
            V::Timestamp("2024-01-01T10:20:30".to_owned()),
            exact(
                &V::Timestamp("2024-01-01T10:20:30".to_owned()),
                "2024-01-01T10:20:30.000000",
            ),
        ),
        Case::new(
            "timestamp al microsecondo",
            "TS",
            timestamp.clone(),
            exact(&timestamp, "2024-01-01T10:20:30.123456"),
        ),
        Case::new(
            "timestamp oltre la precisione della colonna",
            "TS0",
            V::Timestamp("2024-01-01T10:20:30.4".to_owned()),
            Expect::Assigned {
                read: V::Timestamp("2024-01-01T10:20:30.000000".to_owned()),
                raw: "2024-01-01T10:20:30.000000".to_owned(),
            },
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
        Case::new(
            "timestamp sotto il microsecondo",
            "TS9",
            V::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            mapping.clone(),
        ),
        Case::new(
            "timestamptz",
            "TZ",
            V::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            exact(
                &V::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
                "2024-01-01T10:20:30.123456+02:00",
            ),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "timestamptz sotto il microsecondo",
            "TZ",
            V::TimestampTz("2024-01-01T10:20:30.1234567+02:00".to_owned()),
            mapping,
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "uuid",
            "U",
            V::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            exact(
                &text("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60"),
                "6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60",
            ),
        ),
        Case::new(
            "json",
            "J",
            V::Json(serde_json::json!({"a": 1})),
            Expect::Assigned {
                read: V::Json(serde_json::json!({"a": 1})),
                raw: "{\"a\":1}".to_owned(),
            },
        )
        .only(&[FidelityPath::Native]),
        Case::new(
            "enum",
            "S",
            V::Enum {
                type_name: "mood".to_owned(),
                label: "happy".to_owned(),
            },
            exact(&text("happy"), "6861707079"),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "NULL number",
            "I",
            V::Null {
                type_name: "number".to_owned(),
            },
            exact(
                &V::Null {
                    type_name: "decimal".to_owned(),
                },
                "NULL",
            ),
        )
        .only(&[FidelityPath::Native]),
    ]
}

const CREATE: &str = "CREATE TABLE PARAM_FIDELITY (ID NUMBER(10) PRIMARY KEY, B RAW(64), \
     S VARCHAR2(64 CHAR), N NVARCHAR2(64), NC NCHAR(4), NCL NCLOB, CL CLOB, \
     D NUMBER(12, 4), F BINARY_DOUBLE, I NUMBER(19), BO NUMBER(1), DT DATE, \
     TS TIMESTAMP(6), TS0 TIMESTAMP(0), TS9 TIMESTAMP(9), TZ TIMESTAMP(6) WITH TIME ZONE, \
     U VARCHAR2(36), J JSON)";

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_parameter_values_bind_exactly_or_are_refused() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {TABLE} PURGE"), &cancellation)
        .await;
    provider
        .execute_ddl(&secret, CREATE, &cancellation)
        .await
        .expect("DDL tabella sonda Oracle");
    // Un provider nuovo, cioe connessioni nuove, ogni pochi casi: il fork di
    // `oracle-rs` non chiude sul server i cursori delle istruzioni che
    // escono dalla cache (limite dichiarato in `statement_cache.rs`), e una
    // sessione con centinaia di SQL distinti arriva a ORA-01000.
    let schema = std::env::var("PLENORA_ORACLE_USER")
        .unwrap_or_else(|_| "plenora".to_owned())
        .to_ascii_uppercase();
    let mut outcomes = Vec::new();
    for chunk in cases().chunks(3) {
        let (chunk_provider, chunk_secret) = fixture();
        let target = Target {
            provider: &chunk_provider,
            secret: &chunk_secret,
            limits: ResourceLimits::default(),
            table: ObjectRef {
                catalog: None,
                schema: Some(schema.clone()),
                object: TABLE.to_owned(),
            },
            id_column: "ID",
            native: &native,
            native_cross_type_declared: true,
        };
        outcomes.extend(run(&target, chunk).await);
    }
    provider
        .execute_ddl(&secret, &format!("DROP TABLE {TABLE} PURGE"), &cancellation)
        .await
        .expect("pulizia tabella sonda Oracle");
    let (text, failures) = report(&outcomes);
    println!("{text}");
    assert!(
        failures.is_empty(),
        "binding Oracle non esatti: {failures:#?}"
    );
}

/// Il difetto 6 del secondo giro, sul testo: byte che UTF-8 non ammette si
/// leggevano con un sostituto U+FFFD. Ora la lettura si rifiuta. (Un CLOB
/// non si puo costruire con quei byte: `TO_CLOB` li sostituisce sul server,
/// misurato `61EFBFBD62`; la decodifica stretta del LOB e provata nei test del
/// fork, `text_decoding_tests`.)
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_invalid_text_bytes_are_refused_on_read() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Oracle");
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione Oracle");
    let outcome = transaction
        .query(
            &Statement::new("SELECT UTL_RAW.CAST_TO_VARCHAR2(HEXTORAW('61FF62')) AS T FROM DUAL"),
            &cancellation,
        )
        .await
        .map(|rows| rows.first().and_then(|row| row.values().first().cloned()));
    let _ = transaction.rollback(&cancellation).await;
    match outcome {
        Err(error) => assert_eq!(error.category, ErrorCategory::DataMapping),
        Ok(value) => panic!("byte non UTF-8 letti come {value:?}"),
    }
}
