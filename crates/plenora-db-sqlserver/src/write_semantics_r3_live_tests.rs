//! La regola P della terza rilettura indipendente del 25/10, contro SQL
//! Server 2022 live. Ciascuna prova falliva su 1b02559:
//!
//! - F4: 10:00:00.999999 verso `datetime2(0)` diventava 10:00:01 con
//!   l'INSERT preparato e 10:00:00 con il bulk TDS (lo stesso piano, due
//!   valori diversi); `datetime` arrotondava a 1/300 di secondo,
//!   `datetimeoffset(0)` alla scala;
//! - F6: un testo cirillico e cinese verso `varchar(20) COLLATE
//!   Latin1_General_CI_AS` si committava con i caratteri fuori dalla code
//!   page diventati `?`;
//! - `abc` con tre spazi verso `varchar(3)` e `nvarchar(3)` diventava `abc`.

use super::*;
use plenora_database_core::arrow::array::TimestampMicrosecondArray;
use plenora_database_core::arrow::schema::TimeUnit;

const LOSSY: &str = "arrotonderebbe, troncherebbe o convertirebbe il valore";

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

async fn stored(admin: &mut SqlServerSession) -> Vec<String> {
    admin
        .execute_query(
            Query::new(
                "SELECT CONCAT([id], N'=', CONVERT(nvarchar(80), [at], 121)) \
                 FROM [plenora_test].[semantica_p] ORDER BY [id];",
            ),
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

/// L'Append di una riga `(1, at)` in `semantica_p`.
async fn append(
    insert_mode: SqlServerInsertMode,
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
    target.target.object = "semantica_p".to_owned();
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

fn refused_lossy(result: &plenora_database_core::Result<WriteOutcome>) -> bool {
    result.as_ref().is_err_and(|error| {
        error.category == ErrorCategory::DataMapping && error.message.contains(LOSSY)
    })
}

/// F4: con `Strict` una colonna che arrotonda il microsecondo si rifiuta al
/// piano, in entrambi i profili; un `datetimeoffset(0)` rifiuta il testo con
/// una frazione; `datetime2(6)` scrive il valore com'e.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_p_precisione_temporale_identica_in_ogni_profilo() {
    fn timestamp() -> (Field, Arc<dyn Array>) {
        // 2026-01-02 10:00:00.999999 UTC.
        let micros = 1_767_348_000_000_000_i64 + 999_999;
        (
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Arc::new(TimestampMicrosecondArray::from(vec![micros])) as Arc<dyn Array>,
        )
    }
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    let offset = || {
        (
            Field::new("at", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some(
                "2026-01-02T10:00:00.999999+00:00",
            )])) as Arc<dyn Array>,
        )
    };
    let mut difformi = Vec::new();
    for (column, input) in [
        ("datetime2(0)", timestamp as fn() -> (Field, Arc<dyn Array>)),
        ("datetime", timestamp),
        ("datetimeoffset(0)", offset),
    ] {
        for insert_mode in [SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk] {
            run(
                &mut admin,
                &format!(
                    "DROP TABLE IF EXISTS [plenora_test].[semantica_p]; \
                     CREATE TABLE [plenora_test].[semantica_p] \
                     ([id] int NOT NULL PRIMARY KEY, [at] {column} NULL);"
                ),
            )
            .await;
            let (field, value) = input();
            let result = append(insert_mode, field, value).await;
            let rows = stored(&mut admin).await;
            if !refused_lossy(&result) || !rows.is_empty() {
                difformi.push(format!(
                    "{column} {insert_mode:?}: {:?} -> {rows:?}",
                    result.map(|outcome| outcome.rows.confirmed)
                ));
            }
        }
    }
    for insert_mode in [SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk] {
        run(
            &mut admin,
            "DROP TABLE IF EXISTS [plenora_test].[semantica_p]; \
             CREATE TABLE [plenora_test].[semantica_p] \
             ([id] int NOT NULL PRIMARY KEY, [at] datetime2(6) NULL);",
        )
        .await;
        let (field, value) = timestamp();
        let result = append(insert_mode, field, value).await;
        let rows = stored(&mut admin).await;
        if result.is_err() || rows != ["1=2026-01-02 10:00:00.999999"] {
            difformi.push(format!(
                "datetime2(6) {insert_mode:?}: {:?} -> {rows:?}",
                result.map(|outcome| outcome.rows.confirmed)
            ));
        }
    }
    run(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[semantica_p];",
    )
    .await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// F6 e gli spazi finali: con `Strict` il testo che la colonna cambierebbe
/// (code page della collation, lunghezza massima) si rifiuta prima di ogni
/// effetto; `nvarchar` riceve il testo Unicode com'e.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_p_testo_che_la_colonna_cambierebbe() {
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    let unicode = "\u{416}\u{20ac}\u{4e2d}";
    let mut difformi = Vec::new();
    // (colonna, valore, profili, accettato con il valore letto)
    let cases: [(&str, &str, &[SqlServerInsertMode], Option<&str>); 5] = [
        (
            "varchar(20) COLLATE Latin1_General_CI_AS",
            unicode,
            &[SqlServerInsertMode::Prepared],
            None,
        ),
        // Windows-1252 rappresenta `è` e `€`: il valore si conserva.
        (
            "varchar(20) COLLATE Latin1_General_CI_AS",
            "caff\u{e8} \u{20ac}",
            &[SqlServerInsertMode::Prepared],
            Some("1=caff\u{e8} \u{20ac}"),
        ),
        (
            "varchar(3) COLLATE Latin1_General_CI_AS",
            "abc   ",
            &[SqlServerInsertMode::Prepared],
            None,
        ),
        (
            "nvarchar(3)",
            "abc   ",
            &[SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk],
            None,
        ),
        (
            "nvarchar(20)",
            unicode,
            &[SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk],
            Some("1=\u{416}\u{20ac}\u{4e2d}"),
        ),
    ];
    for (column, value, profiles, expected) in cases {
        for insert_mode in profiles {
            run(
                &mut admin,
                &format!(
                    "DROP TABLE IF EXISTS [plenora_test].[semantica_p]; \
                     CREATE TABLE [plenora_test].[semantica_p] \
                     ([id] int NOT NULL PRIMARY KEY, [at] {column} NULL);"
                ),
            )
            .await;
            let result = append(
                *insert_mode,
                Field::new("at", DataType::Utf8, true),
                Arc::new(StringArray::from(vec![Some(value)])),
            )
            .await;
            let rows = stored(&mut admin).await;
            let conforme = expected.map_or_else(
                || refused_lossy(&result) && rows.is_empty(),
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
    run(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[semantica_p];",
    )
    .await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La regola K2 su SQL Server: l'indice unico che prova le chiavi non e
/// filtrato ne disabilitato, ha come colonne chiave esattamente le chiavi
/// (le colonne `INCLUDE` non contano) e confronta con la collation della
/// colonna, perche SQL Server non ne ammette un'altra nella chiave di un
/// indice. Un indice unico con la chiave fra le colonne incluse non prova
/// niente.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_k2_l_indice_con_la_chiave_inclusa_non_prova() {
    let _serial = SERIAL.lock().await;
    let mut admin = admin().await;
    run(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[semantica]; \
         CREATE TABLE [plenora_test].[semantica] ([id] int NOT NULL, [label] nvarchar(100) NULL, \
         [altro] int NOT NULL);",
    )
    .await;
    // Un batch a parte: compilato con la tabella di prima, l'INSERT
    // sbaglierebbe il numero delle colonne (213).
    run(
        &mut admin,
        "INSERT INTO [plenora_test].[semantica] VALUES (1, N'a', 1), (1, N'b', 2); \
         CREATE UNIQUE INDEX [semantica_ux] ON [plenora_test].[semantica] ([altro]) INCLUDE ([id]);",
    )
    .await;
    let pool = SqlServerPool::new(live_config(), 1).expect("pool");
    let result = write(
        &pool,
        WriteMode::Update,
        SqlServerInsertMode::Prepared,
        &[(1, Some("z"))],
    )
    .await;
    run(&mut admin, RESET).await;
    assert!(
        result
            .as_ref()
            .is_err_and(|error| error.category == ErrorCategory::Schema),
        "{:?}",
        result.map(|outcome| outcome.rows)
    );
}
