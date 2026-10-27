//! Fedelta dei parametri SQL Server: ogni variante di `ParameterValue` o
//! arriva al server esatta, o e rifiutata in `Prepare`
//! (`plenora_database_testkit::parameter_fidelity`, con l'oracolo della resa
//! del server e i vicini generati per famiglia).

use super::{live_provider, live_secret};
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::provider::{ParameterValue as V, Provider};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_database_testkit::parameter_fidelity::{
    report, run, Case, Expect, FidelityPath, NativeSql, Target,
};

const SCHEMA: &str = "dbo";
const TABLE: &str = "param_fidelity";
const NATIVE_PORTABLE: &[FidelityPath] = &[FidelityPath::Native, FidelityPath::Portable];

/// La resa canonica del server, indipendente dai decoder del repository.
fn raw_sql(column: &str) -> String {
    match column {
        "b" => "CONVERT(varchar(max), b, 2)".to_owned(),
        "s" => "CONVERT(varchar(max), CAST(s AS varbinary(max)), 2)".to_owned(),
        "f" => "CONVERT(varchar(32), CAST(f AS binary(8)), 2)".to_owned(),
        "r" => "CONVERT(varchar(32), CAST(r AS binary(4)), 2)".to_owned(),
        "dt" => "CONVERT(varchar(10), dt, 23)".to_owned(),
        "ts" | "ts7" | "dto" => format!("CONVERT(varchar(40), {column}, 126)"),
        "dtl" => "CONVERT(varchar(30), dtl, 121)".to_owned(),
        "sdt" => "CONVERT(varchar(30), sdt, 120)".to_owned(),
        _ => format!("CONVERT(varchar(64), {column})"),
    }
}

fn native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (id, {column}) VALUES (@P1, @P2)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE id = @P1"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE id = @P1 AND {column} = @P2")
        }
        NativeSql::Raw => format!(
            "SELECT COALESCE({}, '') FROM {table} WHERE id = @P1",
            raw_sql(column)
        ),
        NativeSql::Delete => format!("DELETE FROM {table} WHERE id = @P1"),
    }
}

fn text(value: &str) -> V {
    V::String(value.to_owned())
}

fn utf16_hex(value: &str) -> String {
    value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .fold(String::new(), |out, byte| out + &format!("{byte:02X}"))
}

fn double_hex(value: f64) -> String {
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
        Case::new("bytes 0xAB01", "b", bytes.clone(), exact(&bytes, "AB01")),
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
            exact(&nonbmp, utf16_hex("\u{e8}\u{1F600}")),
        ),
        Case::new("testo vuoto", "s", text(""), exact(&text(""), "")),
        Case::new(
            "testo con NUL",
            "s",
            text("a\0b"),
            exact(&text("a\0b"), utf16_hex("a\0b")),
        ),
        Case::new(
            "decimal esatto",
            "d",
            decimal.clone(),
            exact(&decimal, "12345.6789"),
        ),
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
        Case::new("double NaN", "f", V::F64(f64::NAN), mapping.clone()),
        Case::new("double +inf", "f", V::F64(f64::INFINITY), mapping.clone()),
        Case::new(
            "double 0.1 in real",
            "r",
            V::F64(0.1),
            Expect::Assigned {
                read: V::F64(f64::from(0.1_f32)),
                raw: "3DCCCCCD".to_owned(),
            },
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
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
        Case::new("boolean", "bo", V::Bool(true), exact(&V::Bool(true), "1")),
        // Il difetto 4 del secondo giro: in nvarchar `date = @P1` ritrovava la
        // riga con un timestamp di quel giorno (vicino generato).
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
        Case::new(
            "timestamp sotto il microsecondo",
            "ts7",
            V::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            mapping.clone(),
        ),
        // Il difetto 4: un smalldatetime arrotondava il parametro al minuto e
        // ritrovava 10:20:29 cercando 10:20 (vicini generati).
        Case::new(
            "timestamp in smalldatetime",
            "sdt",
            V::Timestamp("2024-01-01T10:20:00".to_owned()),
            exact(
                &V::Timestamp("2024-01-01T10:20:00".to_owned()),
                "2024-01-01 10:20:00",
            ),
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping.clone()),
        // Il datetime legacy si legge come la resa del server (regola
        // dichiarata su `arrow::canonical_legacy_datetime`); il confronto con
        // un datetime2 avviene nel valore esatto del tick: limite dichiarato.
        Case::new(
            "timestamp in datetime legacy",
            "dtl",
            V::Timestamp("2024-01-01T10:20:30.003".to_owned()),
            Expect::Assigned {
                read: V::Timestamp("2024-01-01T10:20:30.003".to_owned()),
                raw: "2024-01-01 10:20:30.003".to_owned(),
            },
        )
        .on(FidelityPath::Portable, mapping.clone())
        .on(FidelityPath::Read, mapping),
        Case::new(
            "timestamptz",
            "dto",
            V::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            exact(
                &V::TimestampTz("2024-01-01T08:20:30.123456+00:00".to_owned()),
                "2024-01-01T10:20:30.123456+02:00",
            ),
        ),
        Case::new(
            "uuid",
            "u",
            V::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            exact(
                &V::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
                "6F1C2A2E-7D3B-4C55-9A1E-0B2C3D4E5F60",
            ),
        ),
        Case::new(
            "json in nvarchar",
            "s",
            V::Json(serde_json::json!({"a": 1})),
            exact(&text("{\"a\":1}"), utf16_hex("{\"a\":1}")),
        ),
        Case::new(
            "enum",
            "s",
            V::Enum {
                type_name: "mood".to_owned(),
                label: "happy".to_owned(),
            },
            exact(&text("happy"), utf16_hex("happy")),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "NULL int",
            "n",
            V::Null {
                type_name: "int".to_owned(),
            },
            Expect::Refused(ErrorCategory::Unsupported),
        )
        .only(NATIVE_PORTABLE),
        Case::new(
            "NULL varbinary",
            "b",
            V::Null {
                type_name: "varbinary".to_owned(),
            },
            exact(
                &V::Null {
                    type_name: "varbinary".to_owned(),
                },
                "",
            ),
        )
        .only(NATIVE_PORTABLE),
    ]
}

#[tokio::test]
#[ignore = "richiede SQL Server live"]
async fn live_parameter_values_bind_exactly_or_are_refused() {
    let provider = live_provider();
    let secret = live_secret();
    let cancellation = CancellationToken::new();
    for sql in [
        format!("DROP TABLE IF EXISTS {SCHEMA}.{TABLE}"),
        format!(
            "CREATE TABLE {SCHEMA}.{TABLE} (id INT NOT NULL PRIMARY KEY, b VARBINARY(64), \
             s NVARCHAR(64), d DECIMAL(12, 4), f FLOAT(53), r REAL, i BIGINT, n INT, bo BIT, \
             dt DATE, ts DATETIME2(6), ts7 DATETIME2(7), dto DATETIMEOFFSET(6), \
             dtl DATETIME, sdt SMALLDATETIME, u UNIQUEIDENTIFIER)"
        ),
    ] {
        provider
            .execute_ddl(&secret, &sql, &cancellation)
            .await
            .expect("DDL tabella sonda SQL Server");
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
        native_cross_type_declared: true,
    };
    let outcomes = run(&target, &cases()).await;
    provider
        .execute_ddl(
            &secret,
            &format!("DROP TABLE {SCHEMA}.{TABLE}"),
            &cancellation,
        )
        .await
        .expect("pulizia tabella sonda SQL Server");
    let (text, failures) = report(&outcomes);
    println!("{text}");
    assert!(
        failures.is_empty(),
        "binding SQL Server non esatti: {failures:#?}"
    );
}

// ---------------------------------------------------------------------------
// Censimento generato (`plenora_database_testkit::parameter_sweep`)
// ---------------------------------------------------------------------------

const SWEEP_TABLE: &str = "param_sweep";

fn sweep_native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{SWEEP_TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (id, {column}) VALUES (@P1, @P2)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE id = @P1"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE id = @P1 AND {column} = @P2")
        }
        NativeSql::Raw => {
            format!("SELECT CONVERT(varchar(64), {column}) FROM {table} WHERE id = @P1")
        }
        NativeSql::Delete => format!("DELETE FROM {table} WHERE id = @P1"),
    }
}

/// Le colonne del censimento: ogni parametrizzazione che il catalogo di SQL
/// Server riporta (precisione, scala, lunghezza in byte e in unita UTF-16,
/// codepage della collation).
#[allow(clippy::too_many_lines)]
fn sweep_columns() -> Vec<(
    plenora_database_testkit::parameter_sweep::Column,
    &'static str,
)> {
    use plenora_database_testkit::parameter_sweep::{Column, Kind, Render, Set, Unit};
    let integer = |min: i128, max: i128| Kind::Integer { min, max };
    let text = |length, padded, unit, set| Kind::Text {
        length,
        padded,
        unit,
        set,
    };
    let column = |name, kind, render| Column { name, kind, render };
    vec![
        (column("c_ti", integer(0, 255), Render::Decimal), "tinyint"),
        (
            column(
                "c_i",
                integer(i128::from(i32::MIN), i128::from(i32::MAX)),
                Render::Decimal,
            ),
            "int",
        ),
        (
            column(
                "c_bi",
                integer(i128::from(i64::MIN), i128::from(i64::MAX)),
                Render::Decimal,
            ),
            "bigint",
        ),
        (
            column(
                "c_r",
                Kind::Float { bits: 24 },
                Render::FloatHex {
                    bits: 24,
                    little_endian: false,
                },
            ),
            "real",
        ),
        (
            column(
                "c_f",
                Kind::Float { bits: 53 },
                Render::FloatHex {
                    bits: 53,
                    little_endian: false,
                },
            ),
            "float",
        ),
        (
            column(
                "c_n124",
                Kind::Decimal {
                    precision: 12,
                    scale: 4,
                },
                Render::Decimal,
            ),
            "decimal(12, 4)",
        ),
        (
            column(
                "c_n55",
                Kind::Decimal {
                    precision: 5,
                    scale: 5,
                },
                Render::Decimal,
            ),
            "decimal(5, 5)",
        ),
        (
            column(
                "c_v3",
                text(Some(3), false, Unit::Bytes, Set::Windows1252),
                Render::Utf16LeHex,
            ),
            "varchar(3) COLLATE SQL_Latin1_General_CP1_CI_AS",
        ),
        (
            column(
                "c_v20",
                text(Some(20), false, Unit::Bytes, Set::Windows1252),
                Render::Utf16LeHex,
            ),
            "varchar(20) COLLATE SQL_Latin1_General_CP1_CI_AS",
        ),
        (
            column(
                "c_v10u",
                text(Some(10), false, Unit::Bytes, Set::Unicode),
                Render::Utf16LeHex,
            ),
            "varchar(10) COLLATE Latin1_General_100_CI_AS_SC_UTF8",
        ),
        (
            column(
                "c_nv3",
                text(Some(3), false, Unit::Utf16, Set::Unicode),
                Render::Utf16LeHex,
            ),
            "nvarchar(3)",
        ),
        (
            column(
                "c_c4",
                text(Some(4), true, Unit::Bytes, Set::Windows1252),
                Render::Utf16LeHex,
            ),
            "char(4) COLLATE SQL_Latin1_General_CP1_CI_AS",
        ),
        (
            column(
                "c_nc4",
                text(Some(4), true, Unit::Utf16, Set::Unicode),
                Render::Utf16LeHex,
            ),
            "nchar(4)",
        ),
        (
            column(
                "c_vb4",
                Kind::Binary {
                    length: Some(4),
                    padded: false,
                },
                Render::BytesHex,
            ),
            "varbinary(4)",
        ),
        (
            column(
                "c_b4",
                Kind::Binary {
                    length: Some(4),
                    padded: true,
                },
                Render::BytesHex,
            ),
            "binary(4)",
        ),
        (column("c_dt", Kind::Date, Render::Date), "date"),
        (
            column("c_ts0", Kind::Timestamp { fraction: 0 }, Render::Timestamp),
            "datetime2(0)",
        ),
        (
            column("c_ts3", Kind::Timestamp { fraction: 3 }, Render::Timestamp),
            "datetime2(3)",
        ),
        (
            column("c_ts7", Kind::Timestamp { fraction: 7 }, Render::Timestamp),
            "datetime2(7)",
        ),
        (
            column("c_tz0", Kind::TimestampTz { fraction: 0 }, Render::Instant),
            "datetimeoffset(0)",
        ),
        (
            column("c_tz7", Kind::TimestampTz { fraction: 7 }, Render::Instant),
            "datetimeoffset(7)",
        ),
        (column("c_bo", Kind::Boolean, Render::Boolean), "bit"),
    ]
}

/// La resa del server: il testo come le sue unita UTF-16 qualunque sia la
/// codepage, i floating point come i loro bit, i temporali a sette cifre e
/// gli istanti in UTC; `NULL` resta `NULL`.
fn sweep_raw(column: &str) -> String {
    let expression = match column {
        "c_r" => "CONVERT(varchar(32), CAST(c_r AS binary(4)), 2)".to_owned(),
        "c_f" => "CONVERT(varchar(32), CAST(c_f AS binary(8)), 2)".to_owned(),
        "c_v3" | "c_v20" | "c_v10u" | "c_nv3" | "c_c4" | "c_nc4" => format!(
            "CONVERT(varchar(max), CAST(CAST({column} AS nvarchar(max)) AS varbinary(max)), 2)"
        ),
        "c_vb4" | "c_b4" => format!("CONVERT(varchar(max), {column}, 2)"),
        "c_dt" => "CONVERT(varchar(10), c_dt, 23)".to_owned(),
        "c_ts0" | "c_ts3" | "c_ts7" => {
            format!("CONVERT(varchar(40), CAST({column} AS datetime2(7)), 126)")
        }
        "c_tz0" | "c_tz7" => format!(
            "CONVERT(varchar(40), CAST(SWITCHOFFSET({column}, '+00:00') AS datetime2(7)), 126)"
        ),
        _ => format!("CONVERT(varchar(64), {column})"),
    };
    format!("SELECT {expression} FROM {SCHEMA}.{SWEEP_TABLE} WHERE id = @P1")
}

/// Il censimento generato: per ogni tipo di colonna, ogni valore generato
/// per famiglia, su ogni percorso, contro la resa del server e il modello
/// indipendente del tipo (la codepage e la lunghezza del testo comprese: il
/// difetto 4 del terzo giro della #135, `'łę日x'` in CP1252 diventava `'l?x'`).
#[tokio::test]
#[ignore = "richiede SQL Server live"]
async fn live_generated_values_bind_exactly_or_are_refused() {
    use plenora_database_testkit::parameter_sweep::{
        sweep, sweep_report, ServerFacts, SweepTarget,
    };
    let provider = live_provider();
    let secret = live_secret();
    let cancellation = CancellationToken::new();
    let columns = sweep_columns();
    let definition = columns
        .iter()
        .map(|(column, sql)| format!("{} {sql}", column.name))
        .collect::<Vec<_>>()
        .join(", ");
    for sql in [
        format!("DROP TABLE IF EXISTS {SCHEMA}.{SWEEP_TABLE}"),
        format!("CREATE TABLE {SCHEMA}.{SWEEP_TABLE} (id INT NOT NULL PRIMARY KEY, {definition})"),
    ] {
        provider
            .execute_ddl(&secret, &sql, &cancellation)
            .await
            .expect("DDL tabella del censimento generato SQL Server");
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
        .expect("pulizia tabella del censimento generato SQL Server");
    let (text, failures) = sweep_report(&outcomes);
    println!("{text}");
    assert!(
        failures.is_empty(),
        "valori generati non esatti su SQL Server: {failures:#?}"
    );
}
