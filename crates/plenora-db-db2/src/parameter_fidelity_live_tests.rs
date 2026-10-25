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

fn native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (ID, {column}) VALUES (?, ?)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE ID = ?"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE ID = ? AND {column} = ?")
        }
        NativeSql::Raw => format!("SELECT HEX({column}) FROM {table} WHERE ID = ?"),
        NativeSql::Delete => format!("DELETE FROM {table} WHERE ID = ?"),
    }
}

fn text(value: &str) -> ParameterValue {
    ParameterValue::String(value.to_owned())
}

fn exact(value: &ParameterValue) -> Expect {
    Expect::exact(value.clone())
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
    let float = ParameterValue::F64(0.1);
    let next = ParameterValue::F64(f64::from_bits(0.1_f64.to_bits() + 1));
    let timestamp = ParameterValue::Timestamp("2024-01-01T10:20:30.123456".to_owned());
    vec![
        // Il difetto segnalato: i byte arrivavano come testo esadecimale, e
        // in una colonna FOR BIT DATA restavano i codici ASCII delle cifre.
        Case::new(
            "bytes 0xAB01 in VARCHAR FOR BIT DATA",
            "BF",
            bytes.clone(),
            exact(&bytes)
                .with_raw(text("AB01"))
                .distinct_from(ParameterValue::Bytes(b"AB01".to_vec())),
        ),
        Case::new(
            "bytes 0xAB01 in VARBINARY",
            "B",
            bytes.clone(),
            exact(&bytes)
                .with_raw(text("AB01"))
                .distinct_from(ParameterValue::Bytes(b"AB01".to_vec())),
        ),
        Case::new(
            "bytes con 0x00 e 0xFF",
            "B",
            all_bytes.clone(),
            exact(&all_bytes).with_raw(text("00FF417F80")),
        ),
        Case::new(
            "bytes vuoti",
            "B",
            ParameterValue::Bytes(Vec::new()),
            exact(&ParameterValue::Bytes(Vec::new())).with_raw(text("")),
        ),
        Case::new("bytes in una colonna testuale", "S", bytes, mapping()),
        // Il testo resta testo: in una colonna binaria vanno i suoi byte UTF-8.
        Case::new(
            "testo in una colonna binaria",
            "BF",
            text("AB01"),
            exact(&ParameterValue::Bytes(b"AB01".to_vec())).with_raw(text("41423031")),
        ),
        Case::new(
            "testo non-BMP",
            "S",
            nonbmp.clone(),
            exact(&nonbmp).with_raw(text("C3A8F09F9880")),
        ),
        Case::new(
            "testo vuoto",
            "S",
            text(""),
            exact(&text("")).with_raw(text("")),
        ),
        Case::new(
            "testo con NUL",
            "S",
            text("a\0b"),
            exact(&text("a\0b")).with_raw(text("610062")),
        ),
        Case::new(
            "decimal esatto",
            "D",
            decimal.clone(),
            exact(&decimal).distinct_from(ParameterValue::Decimal("12345.67890001".to_owned())),
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
            float.clone(),
            exact(&float)
                .with_raw(text("9A9999999999B93F"))
                .distinct_from(next.clone()),
        ),
        Case::new(
            "double 0.1 + ulp",
            "F",
            next.clone(),
            exact(&next).with_raw(text("9B9999999999B93F")),
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
            exact(&ParameterValue::F64(f64::MAX)).with_raw(text("FFFFFFFFFFFFEF7F")),
        ),
        Case::new("double NaN", "F", ParameterValue::F64(f64::NAN), refused()),
        Case::new(
            "double +inf",
            "F",
            ParameterValue::F64(f64::INFINITY),
            refused(),
        ),
        Case::new("double 0.1 in REAL", "R", float.clone(), mapping()),
        Case::new(
            "double 0.5 in REAL",
            "R",
            ParameterValue::F64(0.5),
            exact(&ParameterValue::F64(0.5)).with_raw(text("0000003F")),
        ),
        Case::new("double in DECIMAL", "D", float, mapping()),
        Case::new(
            "bigint 2^53 + 1 in DOUBLE",
            "F",
            ParameterValue::I64((1 << 53) + 1),
            mapping(),
        ),
        Case::new(
            "bigint minimo",
            "I",
            ParameterValue::I64(i64::MIN),
            exact(&ParameterValue::I64(i64::MIN)),
        ),
        Case::new(
            "bigint massimo",
            "I",
            ParameterValue::I64(i64::MAX),
            exact(&ParameterValue::I64(i64::MAX)),
        ),
        Case::new(
            "integer minimo",
            "N",
            ParameterValue::I32(i32::MIN),
            exact(&ParameterValue::I32(i32::MIN)),
        ),
        Case::new(
            "boolean true",
            "BO",
            ParameterValue::Bool(true),
            exact(&ParameterValue::Bool(true)),
        ),
        Case::new(
            "boolean false",
            "BO",
            ParameterValue::Bool(false),
            exact(&ParameterValue::Bool(false)),
        ),
        Case::new(
            "date bisestile",
            "DT",
            ParameterValue::Date("2024-02-29".to_owned()),
            exact(&ParameterValue::Date("2024-02-29".to_owned())),
        ),
        Case::new(
            "date minima",
            "DT",
            ParameterValue::Date("0001-01-01".to_owned()),
            exact(&ParameterValue::Date("0001-01-01".to_owned())),
        ),
        Case::new(
            "timestamp al microsecondo",
            "TS",
            timestamp.clone(),
            exact(&timestamp).distinct_from(ParameterValue::Timestamp(
                "2024-01-01T10:20:30.123457".to_owned(),
            )),
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
        ),
        Case::new(
            "uuid",
            "U",
            ParameterValue::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            exact(&text("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60")),
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
            exact(&text("happy")),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "json",
            "S",
            ParameterValue::Json(serde_json::json!({"a": 1})),
            refused(),
        ),
        Case::new(
            "NULL integer",
            "N",
            ParameterValue::Null {
                type_name: "integer".to_owned(),
            },
            exact(&ParameterValue::Null {
                type_name: "integer".to_owned(),
            }),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "NULL binary",
            "B",
            ParameterValue::Null {
                type_name: "binary".to_owned(),
            },
            exact(&ParameterValue::Null {
                type_name: "binary".to_owned(),
            }),
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
             D DECIMAL(12, 4), F DOUBLE, R REAL, I BIGINT, N INTEGER, BO BOOLEAN, \
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
