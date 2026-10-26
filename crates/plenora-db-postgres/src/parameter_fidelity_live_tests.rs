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
use plenora_database_testkit::parameter_sweep::Column;

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

// ---------------------------------------------------------------------------
// Censimento generato (`plenora_database_testkit::parameter_sweep`)
// ---------------------------------------------------------------------------

const SWEEP_TABLE: &str = "param_sweep";

fn sweep_native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{SWEEP_TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (id, {column}) VALUES ($1, $2)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE id = $1"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE id = $1 AND {column} = $2")
        }
        NativeSql::Raw => format!("SELECT {column}::text FROM {table} WHERE id = $1"),
        NativeSql::Delete => format!("DELETE FROM {table} WHERE id = $1"),
    }
}

#[allow(clippy::too_many_lines)]
fn sweep_columns(negative_scale: bool) -> Vec<(Column, &'static str)> {
    use plenora_database_testkit::parameter_sweep::{Kind, Render, Set, Unit};
    let integer = |min: i128, max: i128| Kind::Integer { min, max };
    let text = |length, padded| Kind::Text {
        length,
        padded,
        unit: Unit::Characters,
        set: Set::Unicode,
    };
    let mut columns = vec![
        (
            Column {
                name: "c_i2",
                kind: integer(-32_768, 32_767),
                render: Render::Decimal,
            },
            "int2",
        ),
        (
            Column {
                name: "c_i4",
                kind: integer(i128::from(i32::MIN), i128::from(i32::MAX)),
                render: Render::Decimal,
            },
            "int4",
        ),
        (
            Column {
                name: "c_i8",
                kind: integer(i128::from(i64::MIN), i128::from(i64::MAX)),
                render: Render::Decimal,
            },
            "int8",
        ),
        (
            Column {
                name: "c_f4",
                kind: Kind::Float { bits: 24 },
                render: Render::FloatHex {
                    bits: 24,
                    little_endian: false,
                },
            },
            "float4",
        ),
        (
            Column {
                name: "c_f8",
                kind: Kind::Float { bits: 53 },
                render: Render::FloatHex {
                    bits: 53,
                    little_endian: false,
                },
            },
            "float8",
        ),
        (
            Column {
                name: "c_n",
                kind: Kind::UnboundedDecimal { digits: 100_000 },
                render: Render::Decimal,
            },
            "numeric",
        ),
        (
            Column {
                name: "c_n124",
                kind: Kind::Decimal {
                    precision: 12,
                    scale: 4,
                },
                render: Render::Decimal,
            },
            "numeric(12, 4)",
        ),
        (
            Column {
                name: "c_n55",
                kind: Kind::Decimal {
                    precision: 5,
                    scale: 5,
                },
                render: Render::Decimal,
            },
            "numeric(5, 5)",
        ),
        (
            Column {
                name: "c_v3",
                kind: text(Some(3), false),
                render: Render::Utf8Hex,
            },
            "varchar(3)",
        ),
        (
            Column {
                name: "c_c4",
                kind: text(Some(4), true),
                render: Render::Utf8Hex,
            },
            "char(4)",
        ),
        (
            Column {
                name: "c_t",
                kind: text(None, false),
                render: Render::Utf8Hex,
            },
            "text",
        ),
        (
            Column {
                name: "c_b",
                kind: Kind::Binary {
                    length: None,
                    padded: false,
                },
                render: Render::BytesHex,
            },
            "bytea",
        ),
        (
            Column {
                name: "c_d",
                kind: Kind::Date,
                render: Render::Date,
            },
            "date",
        ),
        (
            Column {
                name: "c_ts0",
                kind: Kind::Timestamp { fraction: 0 },
                render: Render::Timestamp,
            },
            "timestamp(0)",
        ),
        (
            Column {
                name: "c_ts3",
                kind: Kind::Timestamp { fraction: 3 },
                render: Render::Timestamp,
            },
            "timestamp(3)",
        ),
        (
            Column {
                name: "c_ts6",
                kind: Kind::Timestamp { fraction: 6 },
                render: Render::Timestamp,
            },
            "timestamp(6)",
        ),
        (
            Column {
                name: "c_tz0",
                kind: Kind::TimestampTz { fraction: 0 },
                render: Render::Instant,
            },
            "timestamptz(0)",
        ),
        (
            Column {
                name: "c_tz6",
                kind: Kind::TimestampTz { fraction: 6 },
                render: Render::Instant,
            },
            "timestamptz(6)",
        ),
        (
            Column {
                name: "c_bo",
                kind: Kind::Boolean,
                render: Render::Boolean,
            },
            "bool",
        ),
    ];
    if negative_scale {
        columns.push((
            Column {
                name: "c_nm2",
                kind: Kind::Decimal {
                    precision: 6,
                    scale: -2,
                },
                render: Render::Decimal,
            },
            "numeric(6, -2)",
        ));
    }
    columns
}

/// La resa del server della colonna: esadecimale dei byte e dei bit, testo
/// a precisione piena; `NULL` resta `NULL`.
fn sweep_raw(column: &str) -> String {
    let table = format!("{SCHEMA}.{SWEEP_TABLE}");
    let expression = match column {
        "c_f4" => "encode(float4send(c_f4), 'hex')".to_owned(),
        "c_f8" => "encode(float8send(c_f8), 'hex')".to_owned(),
        "c_v3" | "c_c4" | "c_t" => format!("encode(convert_to({column}, 'UTF8'), 'hex')"),
        "c_b" => "encode(c_b, 'hex')".to_owned(),
        "c_d" => "to_char(c_d, 'YYYY-MM-DD')".to_owned(),
        "c_ts0" | "c_ts3" | "c_ts6" => {
            format!("to_char({column}, 'YYYY-MM-DD\"T\"HH24:MI:SS.US')")
        }
        "c_tz0" | "c_tz6" => {
            format!("to_char({column} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US')")
        }
        _ => format!("{column}::text"),
    };
    format!("SELECT {expression} FROM {table} WHERE id = $1")
}

/// Il censimento generato: per ogni tipo di colonna, ogni valore generato
/// per famiglia, su ogni percorso, contro la resa del server e il modello
/// indipendente del tipo. Fallisce sui difetti del terzo giro della #135
/// (testo oltre la lunghezza, frazioni oltre la precisione) prima della
/// correzione.
#[tokio::test]
#[ignore = "richiede PostgreSQL live"]
async fn live_generated_values_bind_exactly_or_are_refused() {
    use plenora_database_testkit::parameter_sweep::{
        sweep, sweep_report, ServerFacts, SweepTarget,
    };
    let Some(dsn) = live_dsn_or_skip() else {
        return;
    };
    let provider = PostgresProvider::insecure_local();
    let secret = SecretString::new(dsn);
    let cancellation = CancellationToken::new();
    // `numeric(p, -s)` esiste da PostgreSQL 15.
    let negative_scale = provider
        .execute_ddl(
            &secret,
            "DO $$ BEGIN IF current_setting('server_version_num')::int < 150000 THEN \
             RAISE EXCEPTION 'senza scala negativa'; END IF; END $$",
            &cancellation,
        )
        .await
        .is_ok();
    let columns = sweep_columns(negative_scale);
    let definition = columns
        .iter()
        .map(|(column, sql)| format!("{} {sql}", column.name))
        .collect::<Vec<_>>()
        .join(", ");
    for sql in [
        format!("DROP TABLE IF EXISTS {SCHEMA}.{SWEEP_TABLE}"),
        format!("CREATE TABLE {SCHEMA}.{SWEEP_TABLE} (id int4 PRIMARY KEY, {definition})"),
    ] {
        provider
            .execute_ddl(&secret, &sql, &cancellation)
            .await
            .expect("DDL tabella del censimento generato PostgreSQL");
    }
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
            id_column: "id",
            native: &sweep_native,
            native_cross_type_declared: true,
        },
        raw: &sweep_raw,
        part: None,
        facts: ServerFacts {
            refuses_nul: true,
            hides_padding: true,
            native_declared: true,
            ..ServerFacts::default()
        },
    };
    let columns = columns
        .into_iter()
        .map(|(column, _)| column)
        .collect::<Vec<_>>();
    let outcomes = sweep(
        &target,
        &columns,
        &[
            FidelityPath::Native,
            FidelityPath::Portable,
            FidelityPath::Read,
        ],
    )
    .await;
    provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {SCHEMA}.{SWEEP_TABLE}"),
            &cancellation,
        )
        .await
        .expect("pulizia tabella del censimento generato PostgreSQL");
    let (text, failures) = sweep_report(&outcomes);
    println!("{text}");
    assert!(
        failures.is_empty(),
        "valori generati non esatti su PostgreSQL: {failures:#?}"
    );
}
