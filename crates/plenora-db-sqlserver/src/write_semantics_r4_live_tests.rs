//! La quarta rilettura indipendente del 25/10, contro SQL Server 2022 live.
//! Le prove che fallivano su 04029d4:
//!
//! - regola T: con lo schema predefinito dell'utente `plenora_t4`, un
//!   `ObjectRef` senza schema si leggeva e si descriveva in `dbo`, mentre
//!   la sessione lo risolve nello schema predefinito;
//! - S7: `2026-01-02T10:00:00.12345670001Z` verso `datetimeoffset(7)` si
//!   committava `.1234567`: chrono tronca oltre il nanosecondo;
//! - regola J: `<a> <c/></a>` verso `xml` perdeva lo spazio, un nodo di testo
//!   (`CONVERT(xml, @P)` con lo stile 0);
//! - i valori esatti verso `datetime2(0)`, `datetime`, `smalldatetime` e
//!   `time(0)` si rifiutavano al piano con `Strict`; `time(p)` con `p < 7`
//!   nel bulk TDS falliva dal server (4875) dopo l'apertura della
//!   transazione.
//!
//! S6 non era silenziosa: la prova fissa che un Upsert in conflitto con un
//! secondo indice unico si rifiuta senza effetti.

use super::*;
use crate::SqlServerProvider;
use plenora_database_core::arrow::array::{Time64MicrosecondArray, TimestampMicrosecondArray};
use plenora_database_core::arrow::schema::TimeUnit;
use plenora_database_core::loss::{LossCategory, LossSeverity};
use plenora_database_core::plan::{Operation, ReadOperation};
use plenora_database_core::provider::{ParameterBag, Provider};

async fn admin() -> SqlServerSession {
    SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("admin")
}

async fn run(admin: &mut SqlServerSession, sql: &str) {
    admin
        .execute_query(
            Query::new(sql.to_owned()),
            ErrorPhase::Write,
            &CancellationToken::new(),
        )
        .await
        .expect("fixture");
}

async fn texts(admin: &mut SqlServerSession, sql: &str) -> Vec<String> {
    admin
        .execute_query(
            Query::new(sql.to_owned()),
            ErrorPhase::Probe,
            &CancellationToken::new(),
        )
        .await
        .expect("lettura")
        .into_iter()
        .flatten()
        .filter_map(|row| row.try_get::<&str, _>(0).ok().flatten().map(str::to_owned))
        .collect()
}

fn provider() -> SqlServerProvider {
    SqlServerProvider::new(live_config(), 100, 2).expect("provider")
}

fn secret() -> SecretString {
    SecretString::new(
        std::env::var("PLENORA_SQLSERVER_PASSWORD")
            .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned()),
    )
}

fn unqualified(object: &str) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: None,
        object: object.to_owned(),
    }
}

/// La regola T: un `ObjectRef` senza schema si risolve una volta come lo
/// risolverebbe la sessione (lo schema predefinito dell'utente, poi `dbo`),
/// e la lettura e la descrizione nominano quello.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e cambia lo schema predefinito dell'utente"]
async fn live_t_senza_schema_si_risolve_come_la_sessione() {
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    run(
        &mut admin,
        "IF SCHEMA_ID(N'plenora_t4') IS NULL EXEC(N'CREATE SCHEMA [plenora_t4]'); \
         DROP TABLE IF EXISTS [plenora_t4].[r4t]; DROP TABLE IF EXISTS [dbo].[r4t]; \
         CREATE TABLE [plenora_t4].[r4t] ([id] int NOT NULL PRIMARY KEY, [dove] nvarchar(10) NULL); \
         CREATE TABLE [dbo].[r4t] ([id] int NOT NULL PRIMARY KEY, [altro] int NULL); \
         INSERT INTO [plenora_t4].[r4t] VALUES (1, N'app'); \
         INSERT INTO [dbo].[r4t] VALUES (7, 7); \
         ALTER USER [dataflow] WITH DEFAULT_SCHEMA = [plenora_t4];",
    )
    .await;
    let provider = provider();
    let secret = secret();
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let described = provider
        .inspect(
            &secret,
            &Operation::DatabaseDescribeObject {
                source: unqualified("r4t"),
            },
            &cancellation,
        )
        .await
        .map(|inspection| inspection.document["relation"]["schema"].clone());
    let read = async {
        let mut stream = provider
            .read(
                &secret,
                &ReadOperation {
                    source: unqualified("r4t"),
                    projection: Vec::new(),
                    order_by: Vec::new(),
                    row_limit: None,
                    row_offset: None,
                    filter: None,
                    declared_crs: Vec::new(),
                },
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await?;
        let mut names = Vec::new();
        while let Some(batch) = stream.next_batch(&cancellation).await? {
            names.extend(
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone()),
            );
        }
        Ok::<_, plenora_database_core::DatabaseError>(names)
    }
    .await;
    run(
        &mut admin,
        "ALTER USER [dataflow] WITH DEFAULT_SCHEMA = [dbo]; \
         DROP TABLE IF EXISTS [plenora_t4].[r4t]; DROP TABLE IF EXISTS [dbo].[r4t];",
    )
    .await;
    assert_eq!(
        described.as_ref().ok(),
        Some(&serde_json::json!("plenora_t4")),
        "{described:?}"
    );
    assert_eq!(
        read.as_ref().ok(),
        Some(&vec!["id".to_owned(), "dove".to_owned()]),
        "{read:?}"
    );
}

/// L'Append di `(1, at)` in `semantica_r4` con una politica.
async fn append(
    insert_mode: SqlServerInsertMode,
    policy: MappingPolicy,
    field: Field,
    value: Arc<dyn Array>,
) -> plenora_database_core::Result<WriteOutcome> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        field,
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>, value],
    )
    .expect("batch");
    let pool = SqlServerPool::new(live_config(), 1).expect("pool");
    let mut target = operation(WriteMode::Append);
    target.target.object = "semantica_r4".to_owned();
    target.mapping_policy = policy;
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    let prepared = prepare_write_with_mode(
        &pool,
        &target,
        Arc::clone(&schema),
        &budget,
        &cancellation,
        insert_mode,
    )
    .await?;
    write_prepared(
        prepared,
        Box::new(Rows {
            schema,
            batch: Some(batch),
        }),
        &cancellation,
    )
    .await
}

async fn table(admin: &mut SqlServerSession, column: &str) {
    run(
        admin,
        &format!(
            "DROP TABLE IF EXISTS [plenora_test].[semantica_r4]; \
             CREATE TABLE [plenora_test].[semantica_r4] \
             ([id] int NOT NULL PRIMARY KEY, [at] {column} NULL);"
        ),
    )
    .await;
}

const STORED: &str = "SELECT CONCAT([id], N'=', CONVERT(nvarchar(max), [at], 121)) \
     FROM [plenora_test].[semantica_r4] ORDER BY [id];";

/// S7: un `datetimeoffset` con piu di nove cifre frazionarie si rifiuta in
/// ogni politica e in ogni profilo; chrono le troncherebbe.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_s7_datetimeoffset_oltre_il_nanosecondo_si_rifiuta() {
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    let mut difformi = Vec::new();
    for policy in [MappingPolicy::Strict, MappingPolicy::Lossy] {
        for insert_mode in [SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk] {
            table(&mut admin, "datetimeoffset(7)").await;
            let result = append(
                insert_mode,
                policy,
                Field::new("at", DataType::Utf8, true),
                Arc::new(StringArray::from(vec![Some(
                    "2026-01-02T10:00:00.12345670001+00:00",
                )])),
            )
            .await;
            let rows = texts(&mut admin, STORED).await;
            let refused = result
                .as_ref()
                .is_err_and(|error| error.category == ErrorCategory::DataMapping);
            if !refused || !rows.is_empty() {
                difformi.push(format!(
                    "{policy:?} {insert_mode:?}: {:?} -> {rows:?}",
                    result.map(|outcome| outcome.rows.confirmed)
                ));
            }
        }
    }
    run(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[semantica_r4];",
    )
    .await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La regola J su `xml`: lo spazio di un nodo di testo si conserva (stile 1
/// di `CONVERT`); dichiarazione, apici e forma degli elementi vuoti sono la
/// semantica del tipo, ammessa con `Strict` e dichiarata nel `LossReport`
/// delle altre politiche.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_j_xml_conserva_i_nodi_di_testo() {
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    table(&mut admin, "xml").await;
    let result = append(
        SqlServerInsertMode::Prepared,
        MappingPolicy::Strict,
        Field::new("at", DataType::Utf8, true),
        Arc::new(StringArray::from(vec![Some(
            "<?xml version=\"1.0\"?><a b='1'> <c></c></a>",
        )])),
    )
    .await;
    let rows = texts(
        &mut admin,
        "SELECT CONCAT([id], N'=', CONVERT(nvarchar(max), [at])) \
         FROM [plenora_test].[semantica_r4] ORDER BY [id];",
    )
    .await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("at", DataType::Utf8, true),
    ]));
    let mut lossy = operation(WriteMode::Append);
    lossy.target.object = "semantica_r4".to_owned();
    lossy.mapping_policy = MappingPolicy::Lossy;
    let report = provider()
        .prepare_write(
            &secret(),
            &lossy,
            schema,
            &ResourceBudget::new(ResourceLimits::default()).expect("budget"),
            &CancellationToken::new(),
        )
        .await
        .map(|prepared| prepared.loss_report.losses);
    run(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[semantica_r4];",
    )
    .await;
    assert!(result.is_ok(), "{:?}", result.map(|outcome| outcome.rows));
    assert_eq!(rows, ["1=<a b=\"1\">&#x20;<c/></a>"]);
    assert!(
        report
            .as_ref()
            .is_ok_and(|losses| losses.iter().any(|loss| {
                loss.field_id == 1
                    && loss.category == LossCategory::NativeType
                    && loss.severity == LossSeverity::Warning
            })),
        "{report:?}"
    );
}

/// I valori esatti verso le colonne temporali ridotte si scrivono con
/// `Strict`, in ogni profilo che le ammette; i valori che arrotonderebbero
/// si rifiutano valore per valore.
/// Un caso: colonna, valore, profili, valore letto o rifiuto.
type Case = (
    &'static str,
    (Field, Arc<dyn Array>),
    &'static [SqlServerInsertMode],
    Option<&'static str>,
);

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
#[allow(clippy::too_many_lines)]
async fn live_p_valori_esatti_verso_colonne_temporali_ridotte() {
    fn timestamp(micros: i64) -> (Field, Arc<dyn Array>) {
        (
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Arc::new(TimestampMicrosecondArray::from(vec![micros])) as Arc<dyn Array>,
        )
    }
    fn time(micros: i64) -> (Field, Arc<dyn Array>) {
        (
            Field::new("at", DataType::Time64(TimeUnit::Microsecond), true),
            Arc::new(Time64MicrosecondArray::from(vec![micros])) as Arc<dyn Array>,
        )
    }
    // 2026-01-02 10:01:00 UTC.
    const BASE: i64 = 1_767_348_060_000_000;
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    let both: &'static [SqlServerInsertMode] =
        &[SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk];
    let prepared: &'static [SqlServerInsertMode] = &[SqlServerInsertMode::Prepared];
    // (colonna, valore, profili, valore letto o rifiuto)
    let cases: [Case; 11] = [
        (
            "datetime2(0)",
            timestamp(BASE + 1_000_000),
            both,
            Some("1=2026-01-02 10:01:01"),
        ),
        ("datetime2(0)", timestamp(BASE + 1_500_000), both, None),
        (
            "datetime2(2)",
            timestamp(BASE + 10_000),
            both,
            Some("1=2026-01-02 10:01:00.01"),
        ),
        (
            "datetime",
            timestamp(BASE + 10_000),
            prepared,
            Some("1=2026-01-02 10:01:00.010"),
        ),
        ("datetime", timestamp(BASE + 3_000), prepared, None),
        (
            "smalldatetime",
            timestamp(BASE),
            prepared,
            Some("1=2026-01-02 10:01:00.000"),
        ),
        ("smalldatetime", timestamp(BASE + 1_000_000), prepared, None),
        (
            "time(0)",
            time(36_000_000_000),
            prepared,
            Some("1=10:00:00"),
        ),
        ("time(0)", time(36_000_000_001), prepared, None),
        (
            "time(6)",
            time(36_000_000_001),
            prepared,
            Some("1=10:00:00.000001"),
        ),
        (
            "time(7)",
            time(36_000_000_001),
            both,
            Some("1=10:00:00.0000010"),
        ),
    ];
    let mut difformi = Vec::new();
    for (column, (field, value), profiles, expected) in cases {
        for insert_mode in profiles {
            table(&mut admin, column).await;
            let result = append(
                *insert_mode,
                MappingPolicy::Strict,
                field.clone(),
                Arc::clone(&value),
            )
            .await;
            let rows = texts(&mut admin, STORED).await;
            let conforme = expected.map_or_else(
                || {
                    result.as_ref().is_err_and(|error| {
                        error.category == ErrorCategory::DataMapping
                            && error.message.contains("arrotonderebbe")
                    }) && rows.is_empty()
                },
                |expected| result.is_ok() && rows == [expected],
            );
            if !conforme {
                difformi.push(format!(
                    "{column} {insert_mode:?}: {:?} -> {rows:?}",
                    result.map(|outcome| outcome.rows.confirmed)
                ));
            }
        }
    }
    // `time(p)` con `p < 7` nel bulk TDS: il server lo rifiutava dopo
    // l'apertura della transazione (4875); ora il piano, prima di ogni
    // effetto.
    for column in ["time(0)", "time(6)"] {
        table(&mut admin, column).await;
        let (field, value) = time(36_000_000_000);
        let result = append(
            SqlServerInsertMode::TdsBulk,
            MappingPolicy::Strict,
            field,
            value,
        )
        .await;
        if !result
            .as_ref()
            .is_err_and(|error| error.category == ErrorCategory::Unsupported)
        {
            difformi.push(format!(
                "{column} TdsBulk: {:?}",
                result.map(|outcome| outcome.rows.confirmed)
            ));
        }
    }
    run(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[semantica_r4];",
    )
    .await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S6 su SQL Server: l'Upsert e un `UPDATE` sulle chiavi seguito da un
/// `INSERT`, senza inferenza di arbitri. Un conflitto con un secondo indice
/// unico e un errore del server, senza effetti: nessuna riga si sostituisce a
/// un'altra.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_s6_upsert_con_un_secondo_indice_unico_si_rifiuta() {
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    let mut difformi = Vec::new();
    for values in [[(5, Some("a"))], [(1, Some("b"))]] {
        run(&mut admin, RESET).await;
        run(
            &mut admin,
            "DELETE FROM [plenora_test].[semantica] WHERE [id] = 3; \
             CREATE UNIQUE INDEX [semantica_label_ux] ON [plenora_test].[semantica] ([label]);",
        )
        .await;
        let pool = SqlServerPool::new(live_config(), 1).expect("pool");
        let result = write(
            &pool,
            WriteMode::Upsert,
            SqlServerInsertMode::Prepared,
            &values,
        )
        .await;
        let rows = texts(
            &mut admin,
            "SELECT CONCAT([id], N':', [label]) FROM [plenora_test].[semantica] ORDER BY [id];",
        )
        .await;
        if result.is_ok() || rows != ["1:a", "2:b"] {
            difformi.push(format!(
                "{values:?}: {:?} -> {rows:?}",
                result.map(|outcome| outcome.rows)
            ));
        }
    }
    run(&mut admin, RESET).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
