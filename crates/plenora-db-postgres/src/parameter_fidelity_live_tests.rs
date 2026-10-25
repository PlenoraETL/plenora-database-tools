//! Fedelta dei parametri PostgreSQL: ogni variante di `ParameterValue` o
//! arriva al server esatta, o e rifiutata in `Prepare`
//! (`plenora_database_testkit::parameter_fidelity`, con l'oracolo della resa
//! del server e i vicini generati per famiglia).

use crate::PostgresProvider;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::provider::{ParameterValue as V, Provider, SecretString};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_database_testkit::parameter_fidelity::{
    report, run, Case, Expect, FidelityPath, NativeSql, Target,
};

const SCHEMA: &str = "public";
const TABLE: &str = "param_fidelity";
const NATIVE_PORTABLE: &[FidelityPath] = &[FidelityPath::Native, FidelityPath::Portable];

fn live_dsn_or_skip() -> Option<String> {
    let declared = std::env::var("PLENORA_TEST_POSTGRES_DSN").ok();
    if declared.is_none() {
        assert!(
            std::env::var("PLENORA_REQUIRE_LIVE_POSTGRES").is_err(),
            "PLENORA_REQUIRE_LIVE_POSTGRES e acceso ma PLENORA_TEST_POSTGRES_DSN \
             manca: questa prova avrebbe saltato la misura dichiarandosi passata"
        );
    }
    declared
}

/// La resa canonica del server, indipendente dai decoder del repository.
fn raw_sql(column: &str) -> String {
    match column {
        "b" => "encode(b, 'hex')".to_owned(),
        "s" => "encode(convert_to(s, 'UTF8'), 'hex')".to_owned(),
        "f" => "encode(float8send(f), 'hex')".to_owned(),
        "r" => "encode(float4send(r), 'hex')".to_owned(),
        "ts" | "ts0" => format!("to_char({column}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')"),
        "tz" | "tz3" => {
            format!("to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US')")
        }
        _ => format!("{column}::text"),
    }
}

fn native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (id, {column}) VALUES ($1, $2)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE id = $1"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE id = $1 AND {column} = $2")
        }
        NativeSql::Raw => format!(
            "SELECT COALESCE({}, '') FROM {table} WHERE id = $1",
            raw_sql(column)
        ),
        NativeSql::Delete => format!("DELETE FROM {table} WHERE id = $1"),
    }
}

fn text(value: &str) -> V {
    V::String(value.to_owned())
}

fn utf8_hex(value: &str) -> String {
    value
        .bytes()
        .fold(String::new(), |out, byte| out + &format!("{byte:02x}"))
}

fn double_hex(value: f64) -> String {
    format!("{:016x}", value.to_bits())
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
        Case::new("bytes 0xAB01", "b", bytes.clone(), exact(&bytes, "ab01")),
        Case::new(
            "bytes vuoti",
            "b",
            V::Bytes(Vec::new()),
            exact(&V::Bytes(Vec::new()), ""),
        ),
        Case::new(
            "testo non-BMP",
            "s",
            nonbmp.clone(),
            exact(&nonbmp, utf8_hex("\u{e8}\u{1F600}")),
        ),
        Case::new("testo vuoto", "s", text(""), exact(&text(""), "")),
        Case::new("testo con NUL", "s", text("a\0b"), mapping.clone()),
        Case::new(
            "decimal esatto",
            "d",
            decimal.clone(),
            exact(&decimal, "12345.6789"),
        ),
        // Il difetto 1 del secondo giro: numeric(12, 4) arrotondava.
        Case::new(
            "decimal oltre la scala della colonna",
            "d",
            V::Decimal("12345.67890001".to_owned()),
            Expect::Assigned {
                read: decimal.clone(),
                raw: "12345.6789".to_owned(),
            },
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
        Case::new(
            "decimal con esponente",
            "d",
            V::Decimal("1.5E2".to_owned()),
            Expect::Refused(ErrorCategory::Unsupported),
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
        Case::new(
            "double 0.1",
            "f",
            V::F64(0.1),
            exact(&V::F64(0.1), double_hex(0.1)),
        ),
        Case::new(
            "double 2^53",
            "f",
            V::F64(float_two_53),
            exact(&V::F64(float_two_53), double_hex(float_two_53)),
        ),
        Case::new(
            "double -0.0",
            "f",
            V::F64(-0.0),
            exact(&V::F64(-0.0), double_hex(-0.0)),
        ),
        Case::new(
            "double NaN",
            "f",
            V::F64(f64::NAN),
            exact(&V::F64(f64::NAN), double_hex(f64::NAN)),
        )
        .only(&[FidelityPath::Native]),
        Case::new(
            "double -inf",
            "f",
            V::F64(f64::NEG_INFINITY),
            exact(&V::F64(f64::NEG_INFINITY), double_hex(f64::NEG_INFINITY)),
        )
        .only(&[FidelityPath::Native]),
        Case::new(
            "double subnormale",
            "f",
            V::F64(5e-324),
            exact(&V::F64(5e-324), double_hex(5e-324)),
        ),
        Case::new("double 0.1 in real", "r", V::F64(0.1), mapping.clone()),
        Case::new(
            "double 0.5 in real",
            "r",
            V::F64(0.5),
            exact(&V::F64(0.5), "3f000000"),
        ),
        Case::new(
            "bigint 2^53 + 1",
            "i",
            V::I64(two_53 + 1),
            exact(&V::I64(two_53 + 1), (two_53 + 1).to_string()),
        ),
        Case::new(
            "bigint minimo",
            "i",
            V::I64(i64::MIN),
            exact(&V::I64(i64::MIN), i64::MIN.to_string()),
        ),
        Case::new(
            "integer minimo",
            "n",
            V::I32(i32::MIN),
            exact(&V::I32(i32::MIN), i32::MIN.to_string()),
        ),
        Case::new(
            "boolean",
            "bo",
            V::Bool(true),
            exact(&V::Bool(true), "true"),
        ),
        Case::new(
            "date",
            "dt",
            V::Date("2024-02-29".to_owned()),
            exact(&V::Date("2024-02-29".to_owned()), "2024-02-29"),
        ),
        Case::new(
            "timestamp al microsecondo",
            "ts",
            timestamp.clone(),
            exact(&timestamp, "2024-01-01T10:20:30.123456"),
        ),
        // Il difetto 1: timestamp(0) perdeva la frazione.
        Case::new(
            "timestamp oltre la precisione della colonna",
            "ts0",
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
            "ts",
            V::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            mapping.clone(),
        ),
        Case::new(
            "timestamptz",
            "tz",
            V::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            exact(
                &V::TimestampTz("2024-01-01T08:20:30.123456+00:00".to_owned()),
                "2024-01-01T08:20:30.123456",
            ),
        ),
        // Il difetto 1: timestamptz(3) arrotondava.
        Case::new(
            "timestamptz oltre la precisione della colonna",
            "tz3",
            V::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            Expect::Assigned {
                read: V::TimestampTz("2024-01-01T08:20:30.123+00:00".to_owned()),
                raw: "2024-01-01T08:20:30.123000".to_owned(),
            },
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
        Case::new(
            "uuid",
            "u",
            V::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            exact(
                &V::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
                "6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60",
            ),
        ),
        Case::new(
            "uuid non canonico",
            "u",
            V::Uuid("6f1c2a2e7-d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            mapping,
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "json in jsonb",
            "j",
            V::Json(serde_json::json!({"a": 1})),
            exact(&V::Json(serde_json::json!({"a": 1})), "{\"a\": 1}"),
        ),
        Case::new(
            "enum in text",
            "s",
            V::Enum {
                type_name: "mood".to_owned(),
                label: "happy".to_owned(),
            },
            exact(&text("happy"), utf8_hex("happy")),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "NULL int4",
            "n",
            V::Null {
                type_name: "int4".to_owned(),
            },
            exact(
                &V::Null {
                    type_name: "int4".to_owned(),
                },
                "",
            ),
        )
        .only(&[FidelityPath::Native]),
    ]
}

#[tokio::test]
async fn live_parameter_values_bind_exactly_or_are_refused() {
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let provider = PostgresProvider::insecure_local();
    let secret = SecretString::new(dsn);
    let cancellation = CancellationToken::new();
    for sql in [
        format!("DROP TABLE IF EXISTS {SCHEMA}.{TABLE}"),
        format!(
            "CREATE TABLE {SCHEMA}.{TABLE} (id int4 PRIMARY KEY, b bytea, s text, \
             d numeric(12, 4), f float8, r float4, i int8, n int4, bo bool, dt date, \
             ts timestamp(6), ts0 timestamp(0), tz timestamptz(6), tz3 timestamptz(3), \
             u uuid, j jsonb)"
        ),
    ] {
        provider
            .execute_ddl(&secret, &sql, &cancellation)
            .await
            .expect("DDL tabella sonda PostgreSQL");
    }
    let target = Target {
        provider: &provider,
        secret: &secret,
        limits: ResourceLimits::default(),
        table: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: TABLE.to_owned(),
        },
        id_column: "id",
        native: &native,
        native_cross_type_declared: false,
    };
    let outcomes = run(&target, &cases()).await;
    provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {SCHEMA}.{TABLE}"),
            &cancellation,
        )
        .await
        .expect("pulizia tabella sonda PostgreSQL");
    let (text, failures) = report(&outcomes);
    println!("{text}");
    assert!(
        failures.is_empty(),
        "binding PostgreSQL non esatti: {failures:#?}"
    );
}
