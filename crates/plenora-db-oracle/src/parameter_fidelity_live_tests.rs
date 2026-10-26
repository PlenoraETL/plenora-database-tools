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

// ---------------------------------------------------------------------------
// Censimento generato (`plenora_database_testkit::parameter_sweep`)
// ---------------------------------------------------------------------------

const SWEEP_TABLE: &str = "PARAM_SWEEP";

fn sweep_value(column: &str) -> String {
    if column.starts_with("C_TZ") {
        format!(
            "TO_TIMESTAMP_TZ(:2, '{}')",
            plenora_database_core::provider::ORACLE_TIMESTAMP_TZ_FORMAT_MODEL
        )
    } else {
        ":2".to_owned()
    }
}

fn sweep_native(kind: NativeSql, column: &str) -> String {
    match kind {
        NativeSql::Insert => format!(
            "INSERT INTO {SWEEP_TABLE} (ID, {column}) VALUES (:1, {})",
            sweep_value(column)
        ),
        NativeSql::Select => format!("SELECT {column} FROM {SWEEP_TABLE} WHERE ID = :1"),
        NativeSql::Count => format!(
            "SELECT COUNT(*) FROM {SWEEP_TABLE} WHERE ID = :1 AND {column} = {}",
            sweep_value(column)
        ),
        NativeSql::Raw => format!("SELECT TO_CHAR({column}) FROM {SWEEP_TABLE} WHERE ID = :1"),
        NativeSql::Delete => format!("DELETE FROM {SWEEP_TABLE} WHERE ID = :1"),
    }
}

/// Le colonne del censimento: ogni parametrizzazione che `ALL_TAB_COLUMNS`
/// riporta (precisione, scala anche negativa, `BYTE` e `CHAR`, forme
/// nazionali).
#[allow(clippy::too_many_lines)]
fn sweep_columns() -> Vec<(
    plenora_database_testkit::parameter_sweep::Column,
    &'static str,
)> {
    use plenora_database_testkit::parameter_sweep::{Column, Kind, Render, Set, Unit};
    let decimal = |precision, scale| Kind::Decimal { precision, scale };
    let text = |length, padded, unit| Kind::Text {
        length,
        padded,
        unit,
        set: Set::Unicode,
    };
    let column = |name, kind, render| Column { name, kind, render };
    let utf8 = Render::Utf8Hex;
    vec![
        (
            column(
                "C_N",
                Kind::UnboundedDecimal { digits: 38 },
                Render::Decimal,
            ),
            "NUMBER",
        ),
        (
            column("C_N124", decimal(12, 4), Render::Decimal),
            "NUMBER(12, 4)",
        ),
        (
            column("C_N55", decimal(5, 5), Render::Decimal),
            "NUMBER(5, 5)",
        ),
        (
            column("C_NM2", decimal(6, -2), Render::Decimal),
            "NUMBER(6, -2)",
        ),
        (
            column("C_N10", decimal(10, 0), Render::Decimal),
            "NUMBER(10)",
        ),
        (
            column(
                "C_BF",
                Kind::Float { bits: 24 },
                Render::FloatHex {
                    bits: 24,
                    little_endian: false,
                },
            ),
            "BINARY_FLOAT",
        ),
        (
            column(
                "C_BD",
                Kind::Float { bits: 53 },
                Render::FloatHex {
                    bits: 53,
                    little_endian: false,
                },
            ),
            "BINARY_DOUBLE",
        ),
        (
            column("C_V3B", text(Some(3), false, Unit::Bytes), utf8),
            "VARCHAR2(3 BYTE)",
        ),
        (
            column("C_V3C", text(Some(3), false, Unit::Characters), utf8),
            "VARCHAR2(3 CHAR)",
        ),
        (
            column("C_C4", text(Some(4), true, Unit::Characters), utf8),
            "CHAR(4 CHAR)",
        ),
        (
            column("C_NV3", text(Some(3), false, Unit::Utf16), utf8),
            "NVARCHAR2(3)",
        ),
        (
            column("C_NC4", text(Some(4), true, Unit::Utf16), utf8),
            "NCHAR(4)",
        ),
        (
            column(
                "C_R4",
                Kind::Binary {
                    length: Some(4),
                    padded: false,
                },
                Render::BytesHex,
            ),
            "RAW(4)",
        ),
        (
            column("C_DT", Kind::Timestamp { fraction: 0 }, Render::Timestamp),
            "DATE",
        ),
        (
            column("C_TS0", Kind::Timestamp { fraction: 0 }, Render::Timestamp),
            "TIMESTAMP(0)",
        ),
        (
            column("C_TS3", Kind::Timestamp { fraction: 3 }, Render::Timestamp),
            "TIMESTAMP(3)",
        ),
        (
            column("C_TS9", Kind::Timestamp { fraction: 9 }, Render::Timestamp),
            "TIMESTAMP(9)",
        ),
        (
            column("C_TZ6", Kind::TimestampTz { fraction: 6 }, Render::Instant),
            "TIMESTAMP(6) WITH TIME ZONE",
        ),
    ]
}

/// La resa del server: i numeri con `TM9` (esatti), i floating point come i
/// loro bit, il testo come i suoi byte UTF-8 (`UTL_I18N`, anche le forme
/// nazionali), i temporali a nove cifre e gli istanti in UTC.
fn sweep_raw(column: &str) -> String {
    let expression = match column {
        // `CAST_FROM_BINARY_*` rende `-0` come `+0` (misurato): il segno di
        // zero si legge da `1 / x`.
        "C_BF" => "CASE WHEN C_BF = 0 AND 1 / C_BF < 0 THEN '80000000'                    ELSE RAWTOHEX(UTL_RAW.CAST_FROM_BINARY_FLOAT(C_BF)) END"
            .to_owned(),
        "C_BD" => "CASE WHEN C_BD = 0 AND 1 / C_BD < 0 THEN '8000000000000000'                    ELSE RAWTOHEX(UTL_RAW.CAST_FROM_BINARY_DOUBLE(C_BD)) END"
            .to_owned(),
        "C_V3B" | "C_V3C" | "C_C4" | "C_NV3" | "C_NC4" => {
            format!("RAWTOHEX(UTL_I18N.STRING_TO_RAW({column}, 'AL32UTF8'))")
        }
        "C_R4" => "RAWTOHEX(C_R4)".to_owned(),
        "C_DT" => "TO_CHAR(C_DT, 'YYYY-MM-DD\"T\"HH24:MI:SS')".to_owned(),
        "C_TS0" | "C_TS3" | "C_TS9" => {
            format!("TO_CHAR({column}, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')")
        }
        "C_TZ6" => "TO_CHAR(SYS_EXTRACT_UTC(C_TZ6), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')".to_owned(),
        _ => format!("TO_CHAR({column}, 'TM9')"),
    };
    format!("SELECT {expression} FROM {SWEEP_TABLE} WHERE ID = :1")
}

/// Il censimento generato: per ogni tipo di colonna, ogni valore generato
/// per famiglia, su ogni percorso, contro la resa del server e il modello
/// indipendente del tipo. Prova anche i decoder dei `NUMBER` con |x| < 1 e
/// un numero dispari di zeri iniziali (il difetto 1 del terzo giro della
/// #135, corretto su main nel fork da `fix/oracle-number-modulo`): fino a
/// quell'unione la lettura di quei valori qui fallisce.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_generated_values_bind_exactly_or_are_refused() {
    use plenora_database_testkit::parameter_sweep::{
        sweep, sweep_report, ServerFacts, SweepTarget,
    };
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let columns = sweep_columns();
    let definition = columns
        .iter()
        .map(|(column, sql)| format!("{} {sql}", column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let _ = provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {SWEEP_TABLE} PURGE"),
            &cancellation,
        )
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!("CREATE TABLE {SWEEP_TABLE} (ID NUMBER(10) PRIMARY KEY, {definition})"),
            &cancellation,
        )
        .await
        .expect("DDL tabella del censimento generato Oracle");
    let schema = std::env::var("PLENORA_ORACLE_USER")
        .unwrap_or_else(|_| "plenora".to_owned())
        .to_ascii_uppercase();
    let mut outcomes = Vec::new();
    // Una sessione nuova ogni trenta valori di una colonna e di un percorso:
    // il fork non chiude sul server i cursori delle istruzioni (il limite
    // dichiarato del censimento a casi), e una sessione lunga arriva a
    // ORA-01000. Le parti si riuniscono con `merge`.
    let paths = [
        FidelityPath::Native,
        FidelityPath::Portable,
        FidelityPath::Read,
    ];
    for (column, _) in &columns {
        let count = plenora_database_testkit::parameter_sweep::values(column.kind).len();
        for path in &paths {
            // La lettura apre una sessione e i cataloghi per ogni filtro:
            // parti piu piccole.
            let size = if *path == FidelityPath::Read { 10 } else { 30 };
            for start in (0..count).step_by(size) {
                let (part_provider, part_secret) = fixture();
                let target = SweepTarget {
                    target: Target {
                        provider: &part_provider,
                        secret: &part_secret,
                        limits: ResourceLimits::default(),
                        table: ObjectRef {
                            catalog: None,
                            schema: Some(schema.clone()),
                            object: SWEEP_TABLE.to_owned(),
                        },
                        id_column: "ID",
                        native: &sweep_native,
                        native_cross_type_declared: true,
                    },
                    raw: &sweep_raw,
                    part: Some(start..(start + size).min(count)),
                    facts: ServerFacts {
                        empty_is_null: true,
                        native_declared: true,
                        ..ServerFacts::default()
                    },
                };
                outcomes.extend(
                    sweep(
                        &target,
                        std::slice::from_ref(column),
                        std::slice::from_ref(path),
                    )
                    .await,
                );
            }
        }
    }
    let outcomes = plenora_database_testkit::parameter_sweep::merge(outcomes);
    provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {SWEEP_TABLE} PURGE"),
            &cancellation,
        )
        .await
        .expect("pulizia tabella del censimento generato Oracle");
    let (text, failures) = sweep_report(&outcomes);
    println!("{text}");
    assert!(
        failures.is_empty(),
        "valori generati non esatti su Oracle: {failures:#?}"
    );
}

/// `-0.0` in `BINARY_DOUBLE` e `BINARY_FLOAT`, misurato: con un letterale,
/// con l'aritmetica IEEE del server e con il bind del fork. La #134
/// (`store_rewrite`) scrive che `BINARY_DOUBLE` lo conserva; il provider lo
/// rifiutava come normalizzato. La misura decide quale vale, e la prova la
/// tiene ferma.
#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_binary_double_negative_zero_is_measured() {
    let (config, secret) = super::fixture_config();
    let driver = config
        .driver_config(&secret)
        .expect("driver config")
        .into_inner();
    let connection = oracle_rs::Connection::connect_with_config(driver)
        .await
        .expect("connessione della misura -0.0");
    let _ = connection
        .execute("DROP TABLE PARAM_NEG_ZERO PURGE", &[])
        .await;
    connection
        .execute(
            "CREATE TABLE PARAM_NEG_ZERO (ID NUMBER(2) PRIMARY KEY, BD BINARY_DOUBLE, \
             BF BINARY_FLOAT)",
            &[],
        )
        .await
        .expect("tabella della misura");
    for (id, double, float) in [
        (1, "TO_BINARY_DOUBLE('-0')", "TO_BINARY_FLOAT('-0')"),
        (2, "-0d", "-0f"),
        (3, "TO_BINARY_DOUBLE(0) * -1", "TO_BINARY_FLOAT(0) * -1"),
        (4, ":1", ":2"),
    ] {
        let params = if id == 4 {
            vec![oracle_rs::Value::Float(-0.0), oracle_rs::Value::Float(-0.0)]
        } else {
            Vec::new()
        };
        connection
            .execute(
                &format!("INSERT INTO PARAM_NEG_ZERO VALUES ({id}, {double}, {float})"),
                &params,
            )
            .await
            .expect("scrittura della misura");
    }
    let rows = connection
        .query(
            "SELECT ID, RAWTOHEX(UTL_RAW.CAST_FROM_BINARY_DOUBLE(BD)) || ' ' || TO_CHAR(BD) \
             || ' ' || TO_CHAR(1 / BD), RAWTOHEX(UTL_RAW.CAST_FROM_BINARY_FLOAT(BF)) || ' ' \
             || TO_CHAR(1 / BF) FROM PARAM_NEG_ZERO ORDER BY ID",
            &[],
        )
        .await
        .expect("lettura della misura");
    let measured = rows
        .rows
        .iter()
        .map(|row| {
            (
                row.get(0)
                    .and_then(oracle_rs::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                row.get(1)
                    .and_then(oracle_rs::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                row.get(2)
                    .and_then(oracle_rs::Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let _ = connection
        .execute("DROP TABLE PARAM_NEG_ZERO PURGE", &[])
        .await;
    let _ = connection.close().await;
    println!("misura -0.0 (id, BINARY_DOUBLE, BINARY_FLOAT): {measured:?}");
    // Misurato il 25/10 (Oracle Free 23ai): i letterali e l'aritmetica del
    // server danno `+0`; il bind del fork (`BINARY_DOUBLE`) conserva `-0.0`
    // in un `BINARY_DOUBLE` (1 / x = -Inf) e lo perde in un `BINARY_FLOAT`.
    // `UTL_RAW.CAST_FROM_BINARY_DOUBLE` e `TO_CHAR` rendono `-0` come `+0`:
    // la resa dei bit non vede il segno, `1 / x` si.
    let expected = [
        ("1", "0000000000000000 0 Inf", "00000000 Inf"),
        ("2", "0000000000000000 0 Inf", "00000000 Inf"),
        ("3", "0000000000000000 0 Inf", "00000000 Inf"),
        ("4", "0000000000000000 0 -Inf", "00000000 Inf"),
    ]
    .iter()
    .map(|(id, double, float)| ((*id).to_owned(), (*double).to_owned(), (*float).to_owned()))
    .collect::<Vec<_>>();
    assert_eq!(measured, expected, "la misura di -0.0 su Oracle e cambiata");
}
