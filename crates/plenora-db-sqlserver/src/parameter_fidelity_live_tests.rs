//! Fedelta dei parametri SQL Server: ogni variante di `ParameterValue` o
//! arriva al server esatta, o e rifiutata in `Prepare`
//! (`plenora_database_testkit::parameter_fidelity`).

use super::{live_provider, live_secret};
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::provider::{ParameterValue, Provider};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_database_testkit::parameter_fidelity::{
    report, run, Case, Expect, FidelityPath, NativeSql, Target,
};

const SCHEMA: &str = "dbo";
const TABLE: &str = "param_fidelity";

fn native(kind: NativeSql, column: &str) -> String {
    let table = format!("{SCHEMA}.{TABLE}");
    match kind {
        NativeSql::Insert => format!("INSERT INTO {table} (id, {column}) VALUES (@P1, @P2)"),
        NativeSql::Select => format!("SELECT {column} FROM {table} WHERE id = @P1"),
        NativeSql::Count => {
            format!("SELECT COUNT(*) FROM {table} WHERE id = @P1 AND {column} = @P2")
        }
        NativeSql::Raw => match column {
            "b" => format!("SELECT CONVERT(varchar(max), b, 2) FROM {table} WHERE id = @P1"),
            "s" => format!(
                "SELECT CONVERT(varchar(max), CAST(s AS varbinary(max)), 2) FROM {table} WHERE id = @P1"
            ),
            "f" => format!("SELECT CONVERT(varchar(64), f, 3) FROM {table} WHERE id = @P1"),
            "dto" => format!("SELECT CONVERT(varchar(64), dto, 127) FROM {table} WHERE id = @P1"),
            _ => format!("SELECT CONVERT(varchar(64), {column}, 121) FROM {table} WHERE id = @P1"),
        },
        NativeSql::Delete => format!("DELETE FROM {table} WHERE id = @P1"),
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
                .with_raw(text("AB01"))
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
            exact(&nonbmp).with_raw(text("E8003DD800DE")),
        ),
        Case::new("testo vuoto", "s", text(""), exact(&text(""))),
        Case::new(
            "testo con NUL",
            "s",
            text("a\0b"),
            exact(&text("a\0b")).with_raw(text("610000006200")),
        ),
        Case::new(
            "decimal esatto",
            "d",
            decimal.clone(),
            exact(&decimal).distinct_from(ParameterValue::Decimal("12345.67890001".to_owned())),
        ),
        Case::new(
            "double 0.1",
            "f",
            float.clone(),
            exact(&float)
                .with_raw(text("1.0000000000000001e-001"))
                .distinct_from(ParameterValue::F64(f64::from_bits(0.1_f64.to_bits() + 1))),
        ),
        Case::new(
            "double -0.0",
            "f",
            ParameterValue::F64(-0.0),
            exact(&ParameterValue::F64(-0.0)),
        ),
        Case::new(
            "double NaN",
            "f",
            ParameterValue::F64(f64::NAN),
            Expect::Refused(ErrorCategory::DataMapping),
        ),
        Case::new(
            "double +inf",
            "f",
            ParameterValue::F64(f64::INFINITY),
            Expect::Refused(ErrorCategory::DataMapping),
        ),
        // Limite dichiarato (`plenora_database_core::parameter_fidelity`):
        // il server riceve il double esatto e lo assegna al `real` con le
        // proprie regole; lo statement del chiamante non descrive il
        // segnaposto.
        Case::new(
            "double 0.1 in real",
            "r",
            float,
            Expect::Assigned {
                read: ParameterValue::F64(f64::from(0.1_f32)),
            },
        )
        .only(&[FidelityPath::Native]),
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
            "ts7",
            ParameterValue::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            Expect::Refused(ErrorCategory::DataMapping),
        ),
        Case::new(
            "timestamptz",
            "dto",
            ParameterValue::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            exact(&ParameterValue::TimestampTz(
                "2024-01-01T08:20:30.123456+00:00".to_owned(),
            ))
            .with_raw(text("2024-01-01T08:20:30.123456Z")),
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
            "json in nvarchar",
            "s",
            ParameterValue::Json(serde_json::json!({"a": 1})),
            exact(&text("{\"a\":1}")),
        ),
        Case::new(
            "enum",
            "s",
            ParameterValue::Enum {
                type_name: "mood".to_owned(),
                label: "happy".to_owned(),
            },
            exact(&text("happy")),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "NULL int",
            "n",
            ParameterValue::Null {
                type_name: "int".to_owned(),
            },
            Expect::Refused(ErrorCategory::Unsupported),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "NULL varbinary",
            "b",
            ParameterValue::Null {
                type_name: "varbinary".to_owned(),
            },
            exact(&ParameterValue::Null {
                type_name: "varbinary".to_owned(),
            }),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
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
             u UNIQUEIDENTIFIER)"
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
