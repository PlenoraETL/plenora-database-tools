//! Fedelta dei parametri Db2: ogni variante di `ParameterValue` o arriva al
//! server esatta, o e rifiutata in `Prepare` (`plenora_database_testkit::
//! parameter_fidelity`). I casi coprono lo statement nativo, il portable e il
//! filtro della lettura.

use crate::{Db2Config, Db2Provider, Db2TlsMode};
use plenora_database_core::arrow::array::{Array, Float32Array, Float64Array};
use plenora_database_core::arrow::RecordBatch;
use plenora_database_core::plan::{ObjectRef, OrderBy, ReadOperation, SortDirection};
use plenora_database_core::provider::{ParameterBag, ParameterValue, Provider, SecretString};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{CommitOutcome, Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_database_testkit::parameter_fidelity::{
    report, run, Case, Expect, FidelityPath, NativeSql, Target,
};

const SCHEMA: &str = "PLENORA_PARAM";
const TABLE: &str = "FIDELITY";

fn environment(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

fn live_provider() -> Db2Provider {
    Db2Provider::new(
        Db2Config::new(
            environment("PLENORA_DB2_HOST", "db2"),
            environment("PLENORA_DB2_DATABASE", "plenora"),
            environment("PLENORA_DB2_USER", "db2inst1"),
        )
        .with_port(
            environment("PLENORA_DB2_PORT", "50000")
                .parse()
                .expect("porta Db2 live"),
        )
        .with_tls_mode(Db2TlsMode::Disable),
    )
    .expect("provider Db2 live")
}

/// La resa canonica del server, indipendente dai decoder del repository:
/// esadecimale di byte, testo e double (i bit come li conserva Db2, in
/// little-endian), `CHAR` di decimali e temporali.
fn raw_sql(column: &str) -> String {
    match column {
        "B" | "BF" | "S" | "F" | "R" => format!("HEX({column})"),
        "BO" => "CASE WHEN BO THEN 'true' ELSE 'false' END".to_owned(),
        "DT" => "CHAR(DT, ISO)".to_owned(),
        "U" => "U".to_owned(),
        _ => format!("VARCHAR({column})"),
    }
}

fn native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (ID, {column}) VALUES (?, ?)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE ID = ?"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE ID = ? AND {column} = ?")
        }
        NativeSql::Raw => format!(
            "SELECT COALESCE({}, '') FROM {table} WHERE ID = ?",
            raw_sql(column)
        ),
        NativeSql::Delete => format!("DELETE FROM {table} WHERE ID = ?"),
    }
}

fn text(value: &str) -> ParameterValue {
    ParameterValue::String(value.to_owned())
}

fn utf8_hex(value: &str) -> String {
    value
        .bytes()
        .fold(String::new(), |out, byte| out + &format!("{byte:02X}"))
}

/// I bit di un double come `HEX` li rende: i byte nell'ordine della
/// macchina del server (`x86_64`, little-endian).
fn double_hex(value: f64) -> String {
    value
        .to_le_bytes()
        .iter()
        .fold(String::new(), |out, byte| out + &format!("{byte:02X}"))
}

fn exact(value: &ParameterValue, raw: impl Into<String>) -> Expect {
    Expect::exact(value.clone(), raw)
}

fn refused() -> Expect {
    Expect::Refused(ErrorCategory::Unsupported)
}

fn mapping() -> Expect {
    Expect::Refused(ErrorCategory::DataMapping)
}

const NATIVE_PORTABLE: &[FidelityPath] = &[FidelityPath::Native, FidelityPath::Portable];

#[allow(clippy::too_many_lines)]
fn cases() -> Vec<Case> {
    let bytes = ParameterValue::Bytes(vec![0xAB, 0x01]);
    let all_bytes = ParameterValue::Bytes(vec![0x00, 0xFF, 0x41, 0x7F, 0x80]);
    let nonbmp = text("\u{e8}\u{1F600}");
    let decimal = ParameterValue::Decimal("12345.6789".to_owned());
    let next = ParameterValue::F64(f64::from_bits(0.1_f64.to_bits() + 1));
    let two_53 = 1_i64 << 53;
    #[allow(clippy::cast_precision_loss)]
    let float_two_53 = two_53 as f64;
    let timestamp = ParameterValue::Timestamp("2024-01-01T10:20:30.123456".to_owned());
    vec![
        // Il difetto segnalato: i byte arrivavano come testo esadecimale, e
        // in una colonna FOR BIT DATA restavano i codici ASCII delle cifre.
        Case::new(
            "bytes 0xAB01 in VARCHAR FOR BIT DATA",
            "BF",
            bytes.clone(),
            exact(&bytes, "AB01"),
        )
        // La lettura Arrow dei binari (FOR BIT DATA, VARBINARY) non e
        // qualificata: si rifiuta prima dello stream.
        .on(FidelityPath::Read, refused()),
        Case::new(
            "bytes 0xAB01 in VARBINARY",
            "B",
            bytes.clone(),
            exact(&bytes, "AB01"),
        )
        // La lettura Arrow dei binari (FOR BIT DATA, VARBINARY) non e
        // qualificata: si rifiuta prima dello stream.
        .on(FidelityPath::Read, refused()),
        Case::new(
            "bytes con 0x00 e 0xFF",
            "B",
            all_bytes.clone(),
            exact(&all_bytes, "00FF417F80"),
        )
        // La lettura Arrow dei binari (FOR BIT DATA, VARBINARY) non e
        // qualificata: si rifiuta prima dello stream.
        .on(FidelityPath::Read, refused()),
        Case::new(
            "bytes vuoti",
            "B",
            ParameterValue::Bytes(Vec::new()),
            exact(&ParameterValue::Bytes(Vec::new()), ""),
        )
        .on(FidelityPath::Read, refused()),
        Case::new("bytes in una colonna testuale", "S", bytes, mapping()),
        // Il testo resta testo: in una colonna binaria vanno i suoi byte UTF-8.
        Case::new(
            "testo in una colonna binaria",
            "BF",
            text("AB01"),
            exact(&ParameterValue::Bytes(b"AB01".to_vec()), "41423031"),
        )
        .on(FidelityPath::Portable, mapping())
        .on(FidelityPath::Read, mapping()),
        Case::new(
            "testo non-BMP",
            "S",
            nonbmp.clone(),
            exact(&nonbmp, utf8_hex("\u{e8}\u{1F600}")),
        ),
        Case::new("testo vuoto", "S", text(""), exact(&text(""), "")),
        Case::new(
            "testo con NUL",
            "S",
            text("a\0b"),
            exact(&text("a\0b"), "610062"),
        ),
        Case::new(
            "decimal esatto",
            "D",
            decimal.clone(),
            exact(&decimal, "12345.6789"),
        ),
        Case::new(
            "decimal oltre la scala",
            "D",
            ParameterValue::Decimal("12345.67890001".to_owned()),
            mapping(),
        ),
        Case::new(
            "decimal con esponente",
            "D",
            ParameterValue::Decimal("1.5E2".to_owned()),
            mapping(),
        ),
        Case::new(
            "double 0.1",
            "F",
            ParameterValue::F64(0.1),
            exact(&ParameterValue::F64(0.1), double_hex(0.1)),
        ),
        Case::new(
            "double 0.1 + ulp",
            "F",
            next.clone(),
            exact(&next, double_hex(f64::from_bits(0.1_f64.to_bits() + 1))),
        ),
        Case::new(
            "double 2^53",
            "F",
            ParameterValue::F64(float_two_53),
            exact(&ParameterValue::F64(float_two_53), double_hex(float_two_53)),
        ),
        Case::new("double -0.0", "F", ParameterValue::F64(-0.0), mapping()),
        Case::new(
            "double subnormale",
            "F",
            ParameterValue::F64(5e-324),
            mapping(),
        ),
        Case::new(
            "double massimo",
            "F",
            ParameterValue::F64(f64::MAX),
            exact(&ParameterValue::F64(f64::MAX), double_hex(f64::MAX)),
        ),
        Case::new("double NaN", "F", ParameterValue::F64(f64::NAN), refused()),
        Case::new(
            "double +inf",
            "F",
            ParameterValue::F64(f64::INFINITY),
            refused(),
        ),
        Case::new(
            "double 0.1 in REAL",
            "R",
            ParameterValue::F64(0.1),
            mapping(),
        ),
        Case::new(
            "double 0.5 in REAL",
            "R",
            ParameterValue::F64(0.5),
            exact(&ParameterValue::F64(0.5), "0000003F"),
        ),
        Case::new(
            "double in DECIMAL",
            "D",
            ParameterValue::F64(0.1),
            mapping(),
        ),
        Case::new(
            "bigint 2^53 + 1 in DOUBLE",
            "F",
            ParameterValue::I64(two_53 + 1),
            mapping(),
        ),
        Case::new(
            "bigint 2^53 + 1",
            "I",
            ParameterValue::I64(two_53 + 1),
            exact(&ParameterValue::I64(two_53 + 1), (two_53 + 1).to_string()),
        ),
        Case::new(
            "bigint minimo",
            "I",
            ParameterValue::I64(i64::MIN),
            exact(&ParameterValue::I64(i64::MIN), i64::MIN.to_string()),
        ),
        Case::new(
            "integer minimo",
            "N",
            ParameterValue::I32(i32::MIN),
            exact(&ParameterValue::I32(i32::MIN), i32::MIN.to_string()),
        ),
        // Il caso non silenzioso del secondo giro: `0` in DECIMAL(5, 5).
        Case::new(
            "zero in DECIMAL(5, 5)",
            "D55",
            ParameterValue::I64(0),
            exact(&ParameterValue::Decimal("0.00000".to_owned()), ".00000"),
        ),
        Case::new(
            "boolean true",
            "BO",
            ParameterValue::Bool(true),
            exact(&ParameterValue::Bool(true), "true"),
        ),
        Case::new(
            "date bisestile",
            "DT",
            ParameterValue::Date("2024-02-29".to_owned()),
            exact(&ParameterValue::Date("2024-02-29".to_owned()), "2024-02-29"),
        ),
        Case::new(
            "timestamp al microsecondo",
            "TS",
            timestamp.clone(),
            exact(&timestamp, "2024-01-01-10.20.30.123456"),
        ),
        Case::new(
            "timestamp oltre la precisione della colonna",
            "TS0",
            timestamp,
            mapping(),
        ),
        Case::new(
            "timestamp sotto il microsecondo",
            "TS12",
            ParameterValue::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            mapping(),
        ),
        Case::new(
            "timestamptz",
            "TS",
            ParameterValue::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            refused(),
        )
        .on(FidelityPath::Portable, mapping())
        .on(FidelityPath::Read, mapping()),
        Case::new(
            "uuid",
            "U",
            ParameterValue::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            exact(
                &text("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60"),
                "6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60",
            ),
        ),
        Case::new(
            "uuid non canonico",
            "U",
            ParameterValue::Uuid("6f1c2a2e7-d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            mapping(),
        ),
        Case::new("testo in una colonna numerica", "N", text("12"), mapping()),
        Case::new(
            "enum",
            "S",
            ParameterValue::Enum {
                type_name: "mood".to_owned(),
                label: "happy".to_owned(),
            },
            exact(&text("happy"), utf8_hex("happy")),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "json",
            "S",
            ParameterValue::Json(serde_json::json!({"a": 1})),
            refused(),
        )
        // Sul percorso portabile il JSON in una colonna testuale e il suo
        // testo (regola `operand_admission`).
        .on(
            FidelityPath::Portable,
            exact(&text("{\"a\":1}"), utf8_hex("{\"a\":1}")),
        )
        .on(
            FidelityPath::Read,
            exact(&text("{\"a\":1}"), utf8_hex("{\"a\":1}")),
        ),
        Case::new(
            "NULL integer",
            "N",
            ParameterValue::Null {
                type_name: "integer".to_owned(),
            },
            exact(
                &ParameterValue::Null {
                    type_name: "integer".to_owned(),
                },
                "",
            ),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "NULL binary",
            "B",
            ParameterValue::Null {
                type_name: "binary".to_owned(),
            },
            exact(
                &ParameterValue::Null {
                    type_name: "binary".to_owned(),
                },
                "",
            ),
        )
        .only(NATIVE_PORTABLE),
    ]
}

async fn reset_table(provider: &Db2Provider, secret: &SecretString) {
    let cancellation = CancellationToken::new();
    for sql in [
        format!("DROP TABLE IF EXISTS {SCHEMA}.{TABLE}"),
        format!(
            "CREATE TABLE {SCHEMA}.{TABLE} (ID INTEGER NOT NULL PRIMARY KEY, \
             B VARBINARY(64), BF VARCHAR(64) FOR BIT DATA, S VARCHAR(64), \
             D DECIMAL(12, 4), D55 DECIMAL(5, 5), F DOUBLE, R REAL, I BIGINT, N INTEGER, BO BOOLEAN, \
             DT DATE, TS TIMESTAMP(6), TS0 TIMESTAMP(0), TS12 TIMESTAMP(12), U CHAR(36))"
        ),
    ] {
        provider
            .execute_ddl(secret, &sql, &cancellation)
            .await
            .expect("DDL tabella sonda Db2");
    }
}

#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_parameter_values_bind_exactly_or_are_refused() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    reset_table(&provider, &secret).await;
    let target = Target {
        provider: &provider,
        secret: &secret,
        limits: ResourceLimits::default(),
        table: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: TABLE.to_owned(),
        },
        id_column: "ID",
        native: &native,
        native_cross_type_declared: false,
    };
    let outcomes = run(&target, &cases()).await;
    provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {SCHEMA}.{TABLE}"),
            &CancellationToken::new(),
        )
        .await
        .expect("pulizia tabella sonda Db2");
    let (text, failures) = report(&outcomes);
    println!("{text}");
    assert!(failures.is_empty(), "binding Db2 non esatti: {failures:#?}");
}

async fn ddl(provider: &Db2Provider, secret: &SecretString, statements: &[String]) {
    for sql in statements {
        provider
            .execute_ddl(secret, sql, &CancellationToken::new())
            .await
            .expect("DDL tabella sonda Db2");
    }
}

async fn read_all(
    provider: &Db2Provider,
    secret: &SecretString,
    object: &str,
) -> plenora_database_core::Result<Vec<RecordBatch>> {
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: object.to_owned(),
        },
        projection: Vec::new(),
        order_by: vec![OrderBy {
            field: "ID".to_owned(),
            direction: SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    let cancellation = CancellationToken::new();
    let mut stream = provider
        .read(
            secret,
            &operation,
            &ParameterBag::default(),
            &budget,
            &cancellation,
        )
        .await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        batches.push(batch);
    }
    Ok(batches)
}

/// La lettura Arrow rendeva i DOUBLE e i REAL dal testo del driver, con 15 e
/// 7 cifre significative: `0.1 + ulp` tornava `0.1` e `f64::MAX` infinito.
#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_arrow_read_returns_floating_point_values_exactly() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let doubles = [
        f64::from_bits(0.1_f64.to_bits() + 1),
        f64::MAX,
        1.234_567_890_123_456_7,
        -1.234_567_890_123_456_7e-300,
    ];
    let reals = [0.1_f32, f32::MAX, -1.234_567_9e-30, 3.402_823_5e30];
    ddl(
        &provider,
        &secret,
        &[
            format!("DROP TABLE IF EXISTS {SCHEMA}.FLOATS"),
            format!("CREATE TABLE {SCHEMA}.FLOATS (ID INTEGER NOT NULL, F DOUBLE, R REAL)"),
        ],
    )
    .await;
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione Db2");
    for (index, (double, real)) in doubles.iter().zip(reals).enumerate() {
        transaction
            .execute(
                &Statement::new(format!(
                    "INSERT INTO {SCHEMA}.FLOATS (ID, F, R) VALUES (?, ?, ?)"
                ))
                .with_params(vec![
                    ParameterValue::I32(i32::try_from(index).expect("id")),
                    ParameterValue::F64(*double),
                    ParameterValue::F64(f64::from(real)),
                ]),
                &cancellation,
            )
            .await
            .expect("insert floating point Db2");
    }
    assert_eq!(
        transaction.commit(&cancellation).await.expect("commit"),
        CommitOutcome::Committed
    );
    let batches = read_all(&provider, &secret, "FLOATS")
        .await
        .expect("read Db2");
    let read_doubles = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("F Float64")
                .values()
                .to_vec()
        })
        .map(f64::to_bits)
        .collect::<Vec<_>>();
    let read_reals = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("R Float32")
                .values()
                .to_vec()
        })
        .map(f32::to_bits)
        .collect::<Vec<_>>();
    ddl(&provider, &secret, &[format!("DROP TABLE {SCHEMA}.FLOATS")]).await;
    assert_eq!(read_doubles, doubles.map(f64::to_bits));
    assert_eq!(read_reals, reals.map(f32::to_bits));
}

/// Un TIMESTAMP(12) con cifre oltre il microsecondo: i decoder le troncavano
/// formattando a sei cifre. Ora rifiutano, sul percorso transazionale e su
/// quello Arrow; il microsecondo esatto resta leggibile.
#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_sub_microsecond_timestamps_are_refused_on_read() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    ddl(
        &provider,
        &secret,
        &[
            format!("DROP TABLE IF EXISTS {SCHEMA}.STAMPS"),
            format!("CREATE TABLE {SCHEMA}.STAMPS (ID INTEGER NOT NULL, TS TIMESTAMP(12))"),
            format!("INSERT INTO {SCHEMA}.STAMPS VALUES (1, '2024-01-01-10.20.30.123456000000')"),
        ],
    )
    .await;
    let exact = read_all(&provider, &secret, "STAMPS").await;
    ddl(
        &provider,
        &secret,
        &[format!(
            "INSERT INTO {SCHEMA}.STAMPS VALUES (2, '2024-01-01-10.20.30.123456700000')"
        )],
    )
    .await;
    let finer = read_all(&provider, &secret, "STAMPS").await;
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget Db2 live");
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .expect("transazione Db2");
    let queried = transaction
        .query(
            &Statement::new(format!("SELECT TS FROM {SCHEMA}.STAMPS WHERE ID = ?"))
                .with_params(vec![ParameterValue::I32(2)]),
            &cancellation,
        )
        .await;
    transaction.rollback(&cancellation).await.expect("rollback");
    ddl(&provider, &secret, &[format!("DROP TABLE {SCHEMA}.STAMPS")]).await;
    assert_eq!(exact.expect("microsecondo esatto").len(), 1);
    assert_eq!(
        finer.expect_err("Arrow oltre il microsecondo").category,
        ErrorCategory::DataMapping
    );
    assert_eq!(
        queried.expect_err("query oltre il microsecondo").category,
        ErrorCategory::DataMapping
    );
}

// ---------------------------------------------------------------------------
// Censimento generato (`plenora_database_testkit::parameter_sweep`)
// ---------------------------------------------------------------------------

const SWEEP_TABLE: &str = "PARAM_SWEEP";

fn sweep_native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{SWEEP_TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (ID, {column}) VALUES (?, ?)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE ID = ?"),
        NativeSql::Count => format!("SELECT COUNT(*) FROM {table} WHERE ID = ? AND {column} = ?"),
        NativeSql::Raw => format!("SELECT VARCHAR({column}) FROM {table} WHERE ID = ?"),
        NativeSql::Delete => format!("DELETE FROM {table} WHERE ID = ?"),
    }
}

/// Le colonne del censimento: ogni parametrizzazione che `SYSCAT.COLUMNS`
/// riporta (precisione, scala, `DECFLOAT` 16 e 34, `OCTETS` e
/// `CODEUNITS32`, binari).
#[allow(clippy::too_many_lines)]
fn sweep_columns() -> Vec<(
    plenora_database_testkit::parameter_sweep::Column,
    &'static str,
)> {
    use plenora_database_testkit::parameter_sweep::{Column, Kind, Render, Set, Unit};
    let integer = |min: i128, max: i128| Kind::Integer { min, max };
    let text = |length, padded, unit| Kind::Text {
        length,
        padded,
        unit,
        set: Set::Unicode,
    };
    let column = |name, kind, render| Column { name, kind, render };
    vec![
        (
            column("C_SI", integer(-32_768, 32_767), Render::Decimal),
            "SMALLINT",
        ),
        (
            column(
                "C_I",
                integer(i128::from(i32::MIN), i128::from(i32::MAX)),
                Render::Decimal,
            ),
            "INTEGER",
        ),
        (
            column(
                "C_BI",
                integer(i128::from(i64::MIN), i128::from(i64::MAX)),
                Render::Decimal,
            ),
            "BIGINT",
        ),
        (
            column(
                "C_R",
                Kind::Float { bits: 24 },
                Render::FloatHex {
                    bits: 24,
                    little_endian: true,
                },
            ),
            "REAL",
        ),
        (
            column(
                "C_D",
                Kind::Float { bits: 53 },
                Render::FloatHex {
                    bits: 53,
                    little_endian: true,
                },
            ),
            "DOUBLE",
        ),
        (
            column(
                "C_N124",
                Kind::Decimal {
                    precision: 12,
                    scale: 4,
                },
                Render::Decimal,
            ),
            "DECIMAL(12, 4)",
        ),
        (
            column(
                "C_N55",
                Kind::Decimal {
                    precision: 5,
                    scale: 5,
                },
                Render::Decimal,
            ),
            "DECIMAL(5, 5)",
        ),
        (
            column(
                "C_DF16",
                Kind::UnboundedDecimal { digits: 16 },
                Render::Decimal,
            ),
            "DECFLOAT(16)",
        ),
        (
            column(
                "C_DF34",
                Kind::UnboundedDecimal { digits: 34 },
                Render::Decimal,
            ),
            "DECFLOAT(34)",
        ),
        (
            column("C_V3", text(Some(3), false, Unit::Bytes), Render::Utf8Hex),
            "VARCHAR(3)",
        ),
        (
            column("C_C4", text(Some(4), true, Unit::Bytes), Render::Utf8Hex),
            "CHAR(4)",
        ),
        (
            column(
                "C_V3C",
                text(Some(3), false, Unit::Characters),
                Render::Utf8Hex,
            ),
            "VARCHAR(3 CODEUNITS32)",
        ),
        (
            column(
                "C_VB4",
                Kind::Binary {
                    length: Some(4),
                    padded: false,
                },
                Render::BytesHex,
            ),
            "VARBINARY(4)",
        ),
        (
            column(
                "C_B4",
                Kind::Binary {
                    length: Some(4),
                    padded: true,
                },
                Render::BytesHex,
            ),
            "BINARY(4)",
        ),
        (column("C_DT", Kind::Date, Render::Date), "DATE"),
        (
            column("C_TS0", Kind::Timestamp { fraction: 0 }, Render::Timestamp),
            "TIMESTAMP(0)",
        ),
        (
            column("C_TS3", Kind::Timestamp { fraction: 3 }, Render::Timestamp),
            "TIMESTAMP(3)",
        ),
        (
            column("C_TS6", Kind::Timestamp { fraction: 6 }, Render::Timestamp),
            "TIMESTAMP(6)",
        ),
        (
            column(
                "C_TS12",
                Kind::Timestamp { fraction: 12 },
                Render::Timestamp,
            ),
            "TIMESTAMP(12)",
        ),
        (column("C_BO", Kind::Boolean, Render::Boolean), "BOOLEAN"),
    ]
}

/// La resa del server: i bit dei floating point (`HEX`, little-endian), i
/// byte UTF-8 del testo (codepage 1208), i decimali e i temporali come
/// `VARCHAR` a precisione piena.
fn sweep_raw(column: &str) -> String {
    let expression = match column {
        "C_R" | "C_D" | "C_V3" | "C_C4" | "C_V3C" | "C_VB4" | "C_B4" => format!("HEX({column})"),
        "C_DT" => "CHAR(C_DT, ISO)".to_owned(),
        "C_BO" => "CASE WHEN C_BO THEN 'true' WHEN NOT C_BO THEN 'false' END".to_owned(),
        _ => format!("VARCHAR({column})"),
    };
    format!("SELECT {expression} FROM {SCHEMA}.{SWEEP_TABLE} WHERE ID = ?")
}

/// Il censimento generato: per ogni tipo di colonna, ogni valore generato
/// per famiglia, su ogni percorso, contro la resa del server e il modello
/// indipendente del tipo. Su Db2 lo SQL del chiamante e esatto (il tipo
/// descritto del segnaposto): niente limite dichiarato sul percorso nativo.
#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_generated_values_bind_exactly_or_are_refused() {
    use plenora_database_testkit::parameter_sweep::{
        sweep, sweep_report, ServerFacts, SweepTarget,
    };
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let columns = sweep_columns();
    let definition = columns
        .iter()
        .map(|(column, sql)| format!("{} {sql}", column.name))
        .collect::<Vec<_>>()
        .join(", ");
    ddl(
        &provider,
        &secret,
        &[
            format!("DROP TABLE IF EXISTS {SCHEMA}.{SWEEP_TABLE}"),
            format!(
                "CREATE TABLE {SCHEMA}.{SWEEP_TABLE} (ID INTEGER NOT NULL PRIMARY KEY, {definition})"
            ),
        ],
    )
    .await;
    let target = SweepTarget {
        target: Target {
            provider: &provider,
            secret: &secret,
            limits: ResourceLimits::default(),
            table: ObjectRef {
                catalog: None,
                schema: Some(SCHEMA.to_owned()),
                object: SWEEP_TABLE.to_owned(),
            },
            id_column: "ID",
            native: &sweep_native,
            native_cross_type_declared: false,
        },
        raw: &sweep_raw,
        part: None,
        facts: ServerFacts {
            normalizes_negative_zero: true,
            refuses_subnormals: true,
            ..ServerFacts::default()
        },
    };
    let columns = columns
        .into_iter()
        .map(|(column, _)| column)
        .collect::<Vec<_>>();
    let mut outcomes = Vec::new();
    for column in &columns {
        for path in [
            FidelityPath::Native,
            FidelityPath::Portable,
            FidelityPath::Read,
        ] {
            // Solo contesto operativo: il timeout deve localizzare la prova
            // anche quando non si arriva al referto finale.
            eprintln!("censimento parametri: {} {path:?}: inizio", column.name);
            outcomes.extend(sweep(&target, std::slice::from_ref(column), &[path]).await);
            eprintln!("censimento parametri: {} {path:?}: fine", column.name);
        }
    }
    provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {SCHEMA}.{SWEEP_TABLE}"),
            &CancellationToken::new(),
        )
        .await
        .expect("pulizia tabella del censimento generato Db2");
    let (text, failures) = sweep_report(&outcomes);
    println!("{text}");
    assert!(
        failures.is_empty(),
        "valori generati non esatti su Db2: {failures:#?}"
    );
}

/// Stessa capacita CLI, diverse unita: sia l'ammissione al limite sia il
/// rifiuto dello spazio finale oltre limite sono obbligatori.
#[tokio::test]
#[ignore = "richiede Db2 LUW live esplicito"]
async fn live_text_catalog_units_preserve_boundaries_without_false_refusals() {
    let provider = live_provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cases = [
        ("CHAR(12 OCTETS)", "abc         "),
        ("VARCHAR(12 OCTETS)", "abc         "),
        ("CLOB(12 OCTETS)", "abc         "),
        ("CHAR(3 CODEUNITS32)", "a\u{1f600}b"),
        ("VARCHAR(3 CODEUNITS32)", "a\u{1f600}b"),
        ("CLOB(3 CODEUNITS32)", "a\u{1f600}b"),
        ("GRAPHIC(3 CODEUNITS16)", "a\u{1f600}"),
        ("VARGRAPHIC(3 CODEUNITS16)", "a\u{1f600}"),
        ("DBCLOB(3 CODEUNITS16)", "a\u{1f600}"),
        ("GRAPHIC(3 CODEUNITS32)", "a\u{1f600}b"),
        ("VARGRAPHIC(3 CODEUNITS32)", "a\u{1f600}b"),
        ("DBCLOB(3 CODEUNITS32)", "a\u{1f600}b"),
    ];
    let definition = cases
        .iter()
        .enumerate()
        .map(|(i, (kind, _))| format!("C{i} {kind}"))
        .collect::<Vec<_>>()
        .join(", ");
    ddl(
        &provider,
        &secret,
        &[
            format!("DROP TABLE IF EXISTS {SCHEMA}.TEXT_UNITS"),
            format!("CREATE TABLE {SCHEMA}.TEXT_UNITS ({definition})"),
        ],
    )
    .await;
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    for (i, (kind, value)) in cases.iter().enumerate() {
        let mut tx = provider
            .begin_transaction(
                &secret,
                &TransactionOptions::default(),
                &budget,
                &cancellation,
            )
            .await
            .expect("transazione");
        let sql = format!("INSERT INTO {SCHEMA}.TEXT_UNITS (C{i}) VALUES (?)");
        tx.execute(
            &Statement::new(&sql).with_params(vec![text(value)]),
            &cancellation,
        )
        .await
        .unwrap_or_else(|error| panic!("{kind}: {error}"));
        let rows = tx
            .query(
                &Statement::new(format!(
                    "SELECT HEX(VARCHAR(C{i})) FROM {SCHEMA}.TEXT_UNITS"
                )),
                &cancellation,
            )
            .await
            .expect("byte conservati");
        assert_eq!(rows.len(), 1, "{kind}");
        assert_eq!(rows[0].values(), &[text(&utf8_hex(value))], "{kind}");
        let error = tx
            .execute(
                &Statement::new(sql).with_params(vec![text(&format!("{value} "))]),
                &cancellation,
            )
            .await
            .expect_err("spazio oltre limite");
        assert_eq!(error.category, ErrorCategory::DataMapping, "{kind}");
        assert_eq!(
            error.phase,
            plenora_database_core::ErrorPhase::Prepare,
            "{kind}"
        );
        tx.rollback(&cancellation).await.expect("rollback");
    }
    ddl(
        &provider,
        &secret,
        &[format!("DROP TABLE {SCHEMA}.TEXT_UNITS")],
    )
    .await;
}
