//! Fedelta dei parametri Oracle: ogni variante di `ParameterValue` o arriva
//! al server esatta, o e rifiutata in `Prepare`
//! (`plenora_database_testkit::parameter_fidelity`).

use super::fixture;
use plenora_database_core::plan::ObjectRef;
use plenora_database_core::provider::{ParameterValue, Provider};
use plenora_database_core::resource::ResourceLimits;
use plenora_database_core::{CancellationToken, ErrorCategory};
use plenora_database_testkit::parameter_fidelity::{
    report, run, Case, Expect, FidelityPath, NativeSql, Target,
};

const TABLE: &str = "PARAM_FIDELITY";

fn native(kind: NativeSql, column: &str) -> String {
    match kind {
        NativeSql::Insert => format!("INSERT INTO {TABLE} (ID, {column}) VALUES (:1, {})", value(column)),
        NativeSql::Select => format!("SELECT {column} FROM {TABLE} WHERE ID = :1"),
        NativeSql::Count => format!(
            "SELECT COUNT(*) FROM {TABLE} WHERE ID = :1 AND {column} = {}",
            value(column)
        ),
        NativeSql::Raw => match column {
            "B" => format!("SELECT RAWTOHEX(B) FROM {TABLE} WHERE ID = :1"),
            "S" => format!("SELECT RAWTOHEX(UTL_RAW.CAST_TO_RAW(S)) FROM {TABLE} WHERE ID = :1"),
            "TZ" => format!(
                "SELECT TO_CHAR(TZ, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6TZH:TZM') FROM {TABLE} WHERE ID = :1"
            ),
            _ => format!("SELECT TO_CHAR({column}) FROM {TABLE} WHERE ID = :1"),
        },
        NativeSql::Delete => format!("DELETE FROM {TABLE} WHERE ID = :1"),
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
            "bytes 0xAB01 in RAW",
            "B",
            bytes.clone(),
            exact(&bytes)
                .with_raw(text("AB01"))
                .distinct_from(ParameterValue::Bytes(b"AB01".to_vec())),
        ),
        Case::new(
            "bytes vuoti",
            "B",
            ParameterValue::Bytes(Vec::new()),
            Expect::Refused(ErrorCategory::DataMapping),
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
            Expect::Refused(ErrorCategory::DataMapping),
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
            "decimal con esponente",
            "D",
            ParameterValue::Decimal("1.5E2".to_owned()),
            exact(&ParameterValue::Decimal("150".to_owned())),
        )
        .only(&[FidelityPath::Native]),
        Case::new(
            "double 0.1 in BINARY_DOUBLE",
            "F",
            float.clone(),
            exact(&float).distinct_from(ParameterValue::F64(f64::from_bits(0.1_f64.to_bits() + 1))),
        ),
        Case::new(
            "double -0.0",
            "F",
            ParameterValue::F64(-0.0),
            exact(&ParameterValue::F64(-0.0)),
        ),
        Case::new(
            "double massimo",
            "F",
            ParameterValue::F64(f64::MAX),
            exact(&ParameterValue::F64(f64::MAX)),
        ),
        Case::new(
            "double NaN",
            "F",
            ParameterValue::F64(f64::NAN),
            Expect::Refused(ErrorCategory::DataMapping),
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
            "boolean",
            "BO",
            ParameterValue::Bool(true),
            exact(&ParameterValue::I64(1)),
        ),
        Case::new(
            "date",
            "DT",
            ParameterValue::Date("2024-02-29".to_owned()),
            exact(&ParameterValue::Timestamp("2024-02-29T00:00:00".to_owned())),
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
            "timestamp sotto il microsecondo",
            "TS9",
            ParameterValue::Timestamp("2024-01-01T10:20:30.1234567".to_owned()),
            Expect::Refused(ErrorCategory::DataMapping),
        ),
        Case::new(
            "timestamptz",
            "TZ",
            ParameterValue::TimestampTz("2024-01-01T10:20:30.123456+02:00".to_owned()),
            exact(&ParameterValue::TimestampTz(
                "2024-01-01T10:20:30.123456+02:00".to_owned(),
            ))
            .with_raw(text("2024-01-01T10:20:30.123456+02:00")),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "timestamptz sotto il microsecondo",
            "TZ",
            ParameterValue::TimestampTz("2024-01-01T10:20:30.1234567+02:00".to_owned()),
            Expect::Refused(ErrorCategory::DataMapping),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "uuid",
            "U",
            ParameterValue::Uuid("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60".to_owned()),
            exact(&text("6f1c2a2e-7d3b-4c55-9a1e-0b2c3d4e5f60")),
        ),
        Case::new(
            "json",
            "J",
            ParameterValue::Json(serde_json::json!({"a": 1})),
            exact(&ParameterValue::Json(serde_json::json!({"a": 1}))),
        )
        .only(&[FidelityPath::Native]),
        Case::new(
            "enum",
            "S",
            ParameterValue::Enum {
                type_name: "mood".to_owned(),
                label: "happy".to_owned(),
            },
            exact(&text("happy")),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
        Case::new(
            "NULL number",
            "I",
            ParameterValue::Null {
                type_name: "number".to_owned(),
            },
            exact(&ParameterValue::Null {
                type_name: "decimal".to_owned(),
            }),
        )
        .only(&[FidelityPath::Native, FidelityPath::Portable]),
    ]
}

#[tokio::test]
#[ignore = "richiede Oracle Free live esplicito"]
async fn live_parameter_values_bind_exactly_or_are_refused() {
    let (provider, secret) = fixture();
    let cancellation = CancellationToken::new();
    let _ = provider
        .execute_ddl(&secret, &format!("DROP TABLE {TABLE} PURGE"), &cancellation)
        .await;
    provider
        .execute_ddl(
            &secret,
            &format!(
                "CREATE TABLE {TABLE} (ID NUMBER(10) PRIMARY KEY, B RAW(64), \
                 S VARCHAR2(64 CHAR), D NUMBER(12, 4), F BINARY_DOUBLE, I NUMBER(19), \
                 BO NUMBER(1), DT DATE, TS TIMESTAMP(6), TS9 TIMESTAMP(9), \
                 TZ TIMESTAMP(6) WITH TIME ZONE, U VARCHAR2(36), J JSON)"
            ),
            &cancellation,
        )
        .await
        .expect("DDL tabella sonda Oracle");
    let target = Target {
        provider: &provider,
        secret: &secret,
        limits: ResourceLimits::default(),
        table: ObjectRef {
            catalog: None,
            schema: Some(
                std::env::var("PLENORA_ORACLE_USER")
                    .unwrap_or_else(|_| "plenora".to_owned())
                    .to_ascii_uppercase(),
            ),
            object: TABLE.to_owned(),
        },
        id_column: "ID",
        native: &native,
    };
    let outcomes = run(&target, &cases()).await;
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
