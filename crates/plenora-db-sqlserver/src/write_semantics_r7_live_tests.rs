//! La regola S (`plenora_database_core::store_rewrite`) su SQL Server 2022,
//! live. Misurato il 25/10 (sesta rilettura indipendente, la classe del testo
//! vuoto di Oracle): `char(3)` salva `'a'` come `'a  '`, `binary(3)` salva
//! `0x01` come `0x010000`, una colonna creata con `ANSI_PADDING OFF` salva
//! `'a  '` come `'a'` e `0x0100` come `0x01`; tutti `committed` con `Strict`
//! e il `LossReport` vuoto. Il confronto di SQL Server ignora gli spazi e gli
//! `0x00` finali: due chiavi `'x'` e `'x '` (o `0x01` e `0x0100`)
//! nell'ingresso di un Upsert erano la stessa voce dell'indice.

use super::*;
use plenora_database_core::arrow::array::BinaryArray;
use plenora_database_core::store_rewrite::{REWRITTEN_KEY, REWRITTEN_VALUE};
use plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS;

const TABLES: &str = "[plenora_test].[riscritture], [plenora_test].[riscritture_k], \
     [plenora_test].[riscritture_chiavi], [plenora_test].[riscritture_off], \
     [plenora_test].[riscritture_bulk]";

const SETUP: &str = "DROP TABLE IF EXISTS [plenora_test].[riscritture], \
     [plenora_test].[riscritture_k], [plenora_test].[riscritture_chiavi], \
     [plenora_test].[riscritture_off], [plenora_test].[riscritture_bulk]; \
     CREATE TABLE [plenora_test].[riscritture] ([id] int NOT NULL PRIMARY KEY, \
       [c] char(3) NULL, [nc] nchar(3) NULL, [b] binary(3) NULL); \
     CREATE TABLE [plenora_test].[riscritture_k] ([id] int NOT NULL PRIMARY KEY, \
       [k] char(3) NOT NULL UNIQUE); \
     CREATE TABLE [plenora_test].[riscritture_chiavi] ([id] int NOT NULL PRIMARY KEY, \
       [t] nvarchar(10) NOT NULL UNIQUE, [vb] varbinary(8) NOT NULL UNIQUE); \
     SET ANSI_PADDING OFF; \
     CREATE TABLE [plenora_test].[riscritture_off] ([id] int NOT NULL PRIMARY KEY, \
       [v] varchar(10) NULL, [vb] varbinary(8) NULL); \
     CREATE TABLE [plenora_test].[riscritture_bulk] ([id] int NOT NULL PRIMARY KEY, \
       [vb] varbinary(8) NULL); \
     SET ANSI_PADDING ON;";

enum Value<'a> {
    Text(&'a str),
    Bytes(&'a [u8]),
}

/// `(id int, <column>)` con un valore per riga.
fn input(column: &str, values: &[(i32, Value<'_>)]) -> Rows {
    let ids: Arc<dyn Array> = Arc::new(Int32Array::from(
        values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    ));
    let (data_type, array): (DataType, Arc<dyn Array>) = match values.first() {
        Some((_, Value::Bytes(_))) => (
            DataType::Binary,
            Arc::new(BinaryArray::from(
                values
                    .iter()
                    .map(|(_, value)| match value {
                        Value::Bytes(bytes) => *bytes,
                        Value::Text(_) => panic!("tipi misti"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
        _ => (
            DataType::Utf8,
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|(_, value)| match value {
                        Value::Text(text) => *text,
                        Value::Bytes(_) => panic!("tipi misti"),
                    })
                    .collect::<Vec<_>>(),
            )),
        ),
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(column, data_type, false),
    ]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![ids, array]).expect("batch");
    Rows {
        schema,
        batch: Some(batch),
    }
}

fn target(object: &str, mode: WriteMode, policy: MappingPolicy, keys: &[&str]) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: object.to_owned(),
        },
        mode,
        mapping_policy: policy,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        update_columns: if mode == WriteMode::Update {
            vec!["id".to_owned()]
        } else {
            Vec::new()
        },
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn run(
    pool: &Arc<SqlServerPool>,
    operation: &WriteOperation,
    insert_mode: SqlServerInsertMode,
    rows: Rows,
) -> (
    plenora_database_core::Result<WriteOutcome>,
    Vec<plenora_database_core::loss::MappingLoss>,
) {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = match prepare_write_with_mode(
        pool,
        operation,
        rows.schema(),
        &budget,
        &cancellation,
        insert_mode,
    )
    .await
    {
        Ok(prepared) => prepared,
        Err(error) => return (Err(error), Vec::new()),
    };
    let losses = prepared.loss_report().losses.clone();
    (
        write_prepared(prepared, Box::new(rows), &cancellation).await,
        losses,
    )
}

async fn texts(admin: &mut SqlServerSession, sql: &'static str) -> Vec<String> {
    let mut results = admin
        .execute_query(
            Query::new(sql),
            ErrorPhase::Probe,
            &CancellationToken::new(),
        )
        .await
        .expect("osservazione");
    results
        .remove(0)
        .into_iter()
        .map(|row| {
            row.try_get::<&str, _>(0)
                .ok()
                .flatten()
                .unwrap_or("<null>")
                .to_owned()
        })
        .collect()
}

fn refused(result: &plenora_database_core::Result<WriteOutcome>, reason: &str) -> bool {
    result.as_ref().is_err_and(|error| {
        error.category == ErrorCategory::DataMapping && error.message.contains(reason)
    })
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
#[allow(clippy::too_many_lines)]
async fn live_s_char_binary_e_ansi_padding_off_non_si_riscrivono() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("admin");
    admin
        .execute_query(
            Query::new(SETUP),
            ErrorPhase::Write,
            &CancellationToken::new(),
        )
        .await
        .expect("fixture");
    let strict = |object| target(object, WriteMode::Append, MappingPolicy::Strict, &[]);
    let prepared = SqlServerInsertMode::Prepared;
    let mut difformi = Vec::new();
    // Strict: ogni riscrittura si rifiuta.
    for (object, column, value) in [
        ("riscritture", "c", Value::Text("a")),
        ("riscritture", "nc", Value::Text("")),
        ("riscritture", "b", Value::Bytes(b"\x01")),
        ("riscritture_off", "v", Value::Text("a  ")),
        ("riscritture_off", "vb", Value::Bytes(b"\x01\x00")),
    ] {
        let (written, _) = run(
            &pool,
            &strict(object),
            prepared,
            input(column, &[(1, value)]),
        )
        .await;
        if !refused(&written, REWRITTEN_VALUE) {
            difformi.push(format!("Strict {object}.{column}: {written:?}"));
        }
    }
    // Anche nel bulk, dove il tipo e ammesso.
    let (written, _) = run(
        &pool,
        &strict("riscritture_bulk"),
        SqlServerInsertMode::TdsBulk,
        input("vb", &[(1, Value::Bytes(b"\x01\x00"))]),
    )
    .await;
    if !refused(&written, REWRITTEN_VALUE) {
        difformi.push(format!("Strict TdsBulk riscritture_bulk.vb: {written:?}"));
    }
    let stored = texts(
        &mut admin,
        "SELECT CONCAT((SELECT COUNT(*) FROM [plenora_test].[riscritture]), ':', \
         (SELECT COUNT(*) FROM [plenora_test].[riscritture_off]))",
    )
    .await;
    if stored != ["0:0"] {
        difformi.push(format!("righe scritte con Strict: {stored:?}"));
    }
    // Il controllo: i valori pieni si scrivono con Strict.
    for (id, object, column, value) in [
        (7, "riscritture", "c", Value::Text("abc")),
        (8, "riscritture", "b", Value::Bytes(b"\x01\x02\x03")),
    ] {
        let (written, _) = run(
            &pool,
            &strict(object),
            prepared,
            input(column, &[(id, value)]),
        )
        .await;
        if written.is_err() {
            difformi.push(format!("controllo {object}.{column}: {written:?}"));
        }
    }
    // Dal 25/10 (regola A) `varchar(n)` con `ANSI_PADDING OFF` e misurato
    // `ogni`: il server toglie gli spazi finali ma conserva uno spazio di un
    // testo di soli spazi, che la classe `toglie_finali` non spiega. Con
    // `Strict` si rifiuta anche ` a`, che la colonna conserverebbe.
    let (written, _) = run(
        &pool,
        &strict("riscritture_off"),
        prepared,
        input("v", &[(9, Value::Text(" a"))]),
    )
    .await;
    if !refused(&written, REWRITTEN_VALUE) {
        difformi.push(format!("ogni riscritture_off.v: {written:?}"));
    }
    // Compatible: la perdita e nel LossReport e il valore passa riscritto.
    let (written, losses) = run(
        &pool,
        &target(
            "riscritture",
            WriteMode::Append,
            MappingPolicy::Compatible,
            &[],
        ),
        prepared,
        input("c", &[(2, Value::Text("a"))]),
    )
    .await;
    if written.is_err()
        || !losses
            .iter()
            .any(|loss| loss.field_id == 1 && loss.reason.contains("regola S"))
    {
        difformi.push(format!("Compatible: {written:?} {losses:?}"));
    }
    let stored = texts(
        &mut admin,
        "SELECT CONCAT('[', [c], ']') FROM [plenora_test].[riscritture] WHERE [id] = 2",
    )
    .await;
    if stored != ["[a  ]"] {
        difformi.push(format!("Compatible riga: {stored:?}"));
    }
    // Una chiave riscritta: rifiutata con ogni politica.
    for policy in [MappingPolicy::Strict, MappingPolicy::Compatible] {
        for mode in [WriteMode::Upsert, WriteMode::Update] {
            let (written, _) = run(
                &pool,
                &target("riscritture_k", mode, policy, &["k"]),
                prepared,
                input("k", &[(1, Value::Text("a"))]),
            )
            .await;
            if !refused(&written, REWRITTEN_KEY) {
                difformi.push(format!("chiave {mode:?} {policy:?}: {written:?}"));
            }
        }
    }
    // Due chiavi che SQL Server confronta uguali: gli spazi e gli 0x00 finali.
    admin
        .execute_query(
            Query::new("INSERT INTO [plenora_test].[riscritture_chiavi] VALUES (1, N'x', 0x01)"),
            ErrorPhase::Write,
            &CancellationToken::new(),
        )
        .await
        .expect("riga della chiave");
    for (column, first, second) in [
        ("t", Value::Text("x"), Value::Text("x ")),
        ("vb", Value::Bytes(b"\x01"), Value::Bytes(b"\x01\x00")),
    ] {
        let (written, _) = run(
            &pool,
            &target(
                "riscritture_chiavi",
                WriteMode::Update,
                MappingPolicy::Strict,
                &[column],
            ),
            prepared,
            input(column, &[(5, first), (6, second)]),
        )
        .await;
        if !refused(&written, DUPLICATE_INPUT_KEYS) {
            difformi.push(format!(
                "chiavi uguali per il server, {column}: {written:?}"
            ));
        }
    }
    let stored = texts(
        &mut admin,
        "SELECT CONCAT([id], ':', [t]) FROM [plenora_test].[riscritture_chiavi] \
         UNION ALL SELECT CONCAT([id], ':', [k]) FROM [plenora_test].[riscritture_k]",
    )
    .await;
    if stored != ["1:x"] {
        difformi.push(format!("righe delle chiavi: {stored:?}"));
    }
    admin
        .execute_query(
            Query::new(format!("DROP TABLE {TABLES};")),
            ErrorPhase::Write,
            &CancellationToken::new(),
        )
        .await
        .expect("pulizia");
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
