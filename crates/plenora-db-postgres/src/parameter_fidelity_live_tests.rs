//! Fedelta dei parametri PostgreSQL: ogni variante di `ParameterValue` o
//! arriva al server esatta, o e rifiutata in `Prepare`
//! (`plenora_database_testkit::parameter_fidelity`).

use crate::PostgresProvider;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::provider::{ParameterValue, Provider, SecretString};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_database_testkit::parameter_fidelity::{
    report, run, Case, Expect, FidelityPath, NativeSql, Target,
};

const SCHEMA: &str = "public";
const TABLE: &str = "param_fidelity";

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

fn native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (id, {column}) VALUES ($1, $2)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE id = $1"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE id = $1 AND {column} = $2")
        }
        NativeSql::Raw => match column {
            "b" => format!("SELECT encode(b, 'hex') FROM {table} WHERE id = $1"),
            "s" => {
                format!("SELECT encode(convert_to(s, 'UTF8'), 'hex') FROM {table} WHERE id = $1")
            }
            _ => format!("SELECT {column}::text FROM {table} WHERE id = $1"),
        },
        NativeSql::Delete => format!("DELETE FROM {table} WHERE id = $1"),
    }
}

fn text(value: &str) -> ParameterValue {
    ParameterValue::String(value.to_owned())
}

fn exact(value: &ParameterValue) -> Expect {
    Expect::exact(value.clone())
}

#[allow(clippy::too_many_lines)]
fn cases() -> Vec<Case> {
    let bytes = ParameterValue::Bytes(vec![0xAB, 0x01]);
    let nonbmp = text("\u{e8}\u{1F600}");
    let decimal = ParameterValue::Decimal("12345.6789".to_owned());
    let float = ParameterValue::F64(0.1);
    let timestamp = ParameterValue::Timestamp("2024-01-01T10:20:30.123456".to_owned());
    vec![
        Case::new(
            "bytes 0xAB01",
            "b",
            bytes.clone(),
            exact(&bytes)
                .with_raw(text("ab01"))
                .distinct_from(ParameterValue::Bytes(b"AB01".to_vec())),
        ),
        Case::new(
            "bytes vuoti",
            "b",
            ParameterValue::Bytes(Vec::new()),
            exact(&ParameterValue::Bytes(Vec::new())).with_raw(text("")),
        ),
        Case::new(
            "testo non-BMP",
            "s",
            nonbmp.clone(),
            exact(&nonbmp).with_raw(text("c3a8f09f9880")),
        ),
        Case::new("testo vuoto", "s", text(""), exact(&text(""))),
        Case::new(
            "testo con NUL",
            "s",
            text("a\0b"),
            Expect::Refused(ErrorCategory::DataMapping),
        ),
        Case::new(
            "decimal esatto",
            "d",
            decimal.clone(),
            exact(&decimal).distinct_from(ParameterValue::Decimal("12345.67890001".to_owned())),
        ),
        Case::new(
            "decimal con esponente",
            "d",
            ParameterValue::Decimal("1.5E2".to_owned()),
            Expect::Refused(ErrorCategory::Unsupported),
        ),
        Case::new(
            "double 0.1",
            "f",
            float.clone(),
            exact(&float)
                .with_raw(text("0.1"))
                .distinct_from(ParameterValue::F64(f64::from_bits(0.1_f64.to_bits() + 1))),
        ),
        Case::new(
            "double -0.0",
            "f",
            ParameterValue::F64(-0.0),
            exact(&ParameterValue::F64(-0.0)).with_raw(text("-0")),
        ),
        Case::new(
            "double NaN",
            "f",
            ParameterValue::F64(f64::NAN),
            exact(&ParameterValue::F64(f64::NAN)).with_raw(text("NaN")),
        ),
        Case::new(
            "double -inf",
            "f",
            ParameterValue::F64(f64::NEG_INFINITY),
            exact(&ParameterValue::F64(f64::NEG_INFINITY)).with_raw(text("-Infinity")),
        ),
        Case::new(
            "double subnormale",
            "f",
            ParameterValue::F64(5e-324),
            exact(&ParameterValue::F64(5e-324)),
        ),
        Case::new(
            "double 0.1 in real",
            "r",
            float,
            Expect::Refused(ErrorCategory::DataMapping),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "double 0.5 in real",
            "r",
            ParameterValue::F64(0.5),
            exact(&ParameterValue::F64(0.5)),
        ),
        Case::new(
            "bigint minimo",
            "i",
            ParameterValue::I64(i64::MIN),
            exact(&ParameterValue::I64(i64::MIN)),
        ),
        Case::new(
            "bigint massimo",
            "i",
            ParameterValue::I64(i64::MAX),
            exact(&ParameterValue::I64(i64::MAX)),
        ),
        Case::new(
            "integer minimo",
            "n",
            ParameterValue::I32(i32::MIN),
            exact(&ParameterValue::I32(i32::MIN)),
        ),
        Case::new(
            "boolean",
            "bo",
            ParameterValue::Bool(true),
            exact(&ParameterValue::Bool(true)),
        ),
        Case::new(
            "date",
            "dt",
            ParameterValue::Date("2024-02-29".to_owned()),
            exact(&ParameterValue::Date("2024-02-29".to_owned())),
        ),
        Case::new(
            "timestamp al microsecondo",
            "ts",
            timestamp.clone(),
            exact(&timestamp).distinct_from(ParameterValue::Timestamp(
                "2024-01-01T10:20:30.123457".to_owned(),
            )),
        ),
        Case::new(
            "timestamp sotto il microsecondo",
            "ts",
            ParameterValue::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            Expect::Refused(ErrorCategory::DataMapping),
        ),
        Case::new(
            "timestamptz",
            "tz",
            ParameterValue::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            exact(&ParameterValue::TimestampTz(
                "2024-01-01T08:20:30.123456+00:00".to_owned(),
            )),
        ),
        Case::new(
            "timestamptz sotto il microsecondo",
            "tz",
            ParameterValue::TimestampTz("2024-01-01T10:20:30.1234567+02:00".to_owned()),
            Expect::Refused(ErrorCategory::DataMapping),
        ),
        Case::new(
            "uuid",
            "u",
            ParameterValue::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            exact(&ParameterValue::Uuid(
                "6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned(),
            )),
        ),
        Case::new(
            "uuid non canonico",
            "u",
            ParameterValue::Uuid("6f1c2a2e7-d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            Expect::Refused(ErrorCategory::DataMapping),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "json in jsonb",
            "j",
            ParameterValue::Json(serde_json::json!({"a": 1})),
            exact(&ParameterValue::Json(serde_json::json!({"a": 1}))),
        ),
        Case::new(
            "enum in text",
            "s",
            ParameterValue::Enum {
                type_name: "mood".to_owned(),
                label: "happy".to_owned(),
            },
            exact(&text("happy")),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "NULL int4",
            "n",
            ParameterValue::Null {
                type_name: "int4".to_owned(),
            },
            exact(&ParameterValue::Null {
                type_name: "int4".to_owned(),
            }),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
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
             ts timestamp(6), tz timestamptz(6), u uuid, j jsonb)"
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
