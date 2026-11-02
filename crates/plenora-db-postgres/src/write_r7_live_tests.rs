//! La regola S (`plenora_database_core::store_rewrite`) su PostgreSQL, contro
//! il fixture: `char(n)` completa con spazi un testo piu corto, e il valore
//! letto non e quello scritto (`'a'` torna `'a  '`). Fino al 25/10 (sesta
//! rilettura indipendente) era dichiarato «semantica del tipo» e si
//! scriveva con `Strict`; ora e una perdita come il testo vuoto di Oracle.

use super::write_visibility_live_tests::{
    dry_run_stream, live_dsn, operation, prepare, reset, write_stream, SERIAL,
};
use super::*;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::WriteMode;
use plenora_database_core::provider::{BatchStream, Provider};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::store_rewrite::{REWRITTEN_KEY, REWRITTEN_VALUE};
use plenora_database_core::{CancellationToken, ErrorCategory};

const PROFILES: [PostgresInsertMode; 3] = [
    PostgresInsertMode::Prepared,
    PostgresInsertMode::CopyText,
    PostgresInsertMode::CopyBinary,
];

struct One(SchemaRef, Option<RecordBatch>);

impl BatchStream for One {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.0)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.1.take())))
    }
}

/// `(id bigint, <column> text)`.
fn row(id: i64, column: &str, value: &str) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(column, DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![id])) as ArrayRef,
            Arc::new(StringArray::from(vec![value])) as ArrayRef,
        ],
    )
    .expect("batch");
    Box::new(One(schema, Some(batch)))
}

fn with(mut operation: WriteOperation, policy: MappingPolicy, keys: &[&str]) -> WriteOperation {
    operation.mapping_policy = policy;
    operation.keys = keys.iter().map(|key| (*key).to_owned()).collect();
    // Update aggiorna `id`; le altre mode usano le colonne non chiave.
    operation.update_columns = if operation.mode == WriteMode::Update {
        vec!["id".to_owned()]
    } else {
        Vec::new()
    };
    operation
}

fn rewritten(result: &plenora_database_core::Result<impl std::fmt::Debug>, key: bool) -> bool {
    result.as_ref().is_err_and(|error| {
        error.category == ErrorCategory::DataMapping
            && error
                .message
                .contains(if key { REWRITTEN_KEY } else { REWRITTEN_VALUE })
    })
}

async fn texts(admin: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    admin
        .query(sql, &[])
        .await
        .expect("lettura")
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

/// `char(3)`: con `Strict` un testo piu corto si rifiuta in ogni profilo e
/// nel dry run; con `Compatible` il `LossReport` dichiara la perdita e il
/// valore si scrive completato; in una chiave si rifiuta con ogni politica.
/// Su f500235 `Strict` committava `'a'` come `'a  '` con il `LossReport`
/// vuoto, e l'Upsert sulla chiave `'a'` confermava 1.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_s_char_n_completa_e_non_si_scrive_in_silenzio() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut difformi = Vec::new();
    for profile in PROFILES {
        prepare(
            &admin,
            "CREATE TABLE {s}.c (id bigint PRIMARY KEY, c char(3), k char(3) UNIQUE, n text);",
        )
        .await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let append = |policy| with(operation("c", WriteMode::Append), policy, &[]);
        // Strict: 'a' e '' si completerebbero.
        for value in ["a", ""] {
            let written = write_stream(
                &provider,
                &secret,
                &append(MappingPolicy::Strict),
                row(1, "c", value),
            )
            .await;
            if !rewritten(&written, false) {
                difformi.push(format!("{profile:?} Strict {value:?}: {written:?}"));
            }
            if profile != PostgresInsertMode::CopyBinary {
                let dry = dry_run_stream(
                    &provider,
                    &secret,
                    &append(MappingPolicy::Strict),
                    row(1, "c", value),
                )
                .await;
                if !rewritten(&dry, false) {
                    difformi.push(format!(
                        "{profile:?} dry run Strict {value:?}: {:?}",
                        dry.map(|dry| dry.rows)
                    ));
                }
            }
        }
        if !texts(&admin, "SELECT c FROM pln_regola_r.c")
            .await
            .is_empty()
        {
            difformi.push(format!("{profile:?}: righe scritte con Strict"));
        }
        // Il controllo: la lunghezza piena si scrive com'e.
        write_stream(
            &provider,
            &secret,
            &append(MappingPolicy::Strict),
            row(1, "c", "abc"),
        )
        .await
        .expect("lunghezza piena");
        // Compatible: la perdita e nel LossReport, il valore passa.
        let compatible = append(MappingPolicy::Compatible);
        let prepared = Provider::prepare_write(
            &provider,
            &secret,
            &compatible,
            row(2, "c", "a").schema(),
            &budget,
            &CancellationToken::new(),
        )
        .await
        .expect("prepare Compatible");
        if !prepared.loss_report.losses.iter().any(|loss| {
            loss.field_id == 1 && loss.reason.contains("regola S") && loss.reason.contains("spazi")
        }) {
            difformi.push(format!(
                "{profile:?} LossReport Compatible: {:?}",
                prepared.loss_report.losses
            ));
        }
        write_stream(&provider, &secret, &compatible, row(2, "c", "a"))
            .await
            .expect("Compatible dichiarato");
        let stored = texts(&admin, "SELECT c FROM pln_regola_r.c ORDER BY id").await;
        if stored != ["abc", "a  "] {
            difformi.push(format!("{profile:?} righe: {stored:?}"));
        }
        // Una chiave completata: rifiutata con ogni politica, anche in Update
        // e DeleteByKeys.
        for policy in [MappingPolicy::Strict, MappingPolicy::Compatible] {
            for mode in [WriteMode::Upsert, WriteMode::Update] {
                let keyed = with(operation("c", mode), policy, &["k"]);
                let written = write_stream(&provider, &secret, &keyed, row(3, "k", "a")).await;
                if !rewritten(&written, true) {
                    difformi.push(format!(
                        "{profile:?} {mode:?} {policy:?} chiave: {written:?}"
                    ));
                }
            }
        }
        if texts(&admin, "SELECT c FROM pln_regola_r.c").await.len() != 2 {
            difformi.push(format!("{profile:?}: righe scritte dalle chiavi"));
        }
        // Il controllo: una chiave piena si scrive.
        write_stream(
            &provider,
            &secret,
            &with(
                operation("c", WriteMode::Upsert),
                MappingPolicy::Strict,
                &["k"],
            ),
            row(3, "k", "abc"),
        )
        .await
        .expect("chiave piena");
        // Il carattere NUL: il server lo rifiuta, mai in silenzio.
        let nul = write_stream(
            &provider,
            &secret,
            &append(MappingPolicy::Strict),
            row(9, "n", "a\0b"),
        )
        .await;
        if nul.is_ok() {
            difformi.push(format!("{profile:?} NUL scritto: {nul:?}"));
        }
        // `float8` conserva -0.0: nessuna riscrittura da dichiarare.
        admin
            .batch_execute("CREATE TABLE pln_regola_r.f (id bigint PRIMARY KEY, d float8);")
            .await
            .expect("tabella dei float");
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("d", DataType::Float64, true),
        ]));
        let zero = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(arrow_array::Float64Array::from(vec![-0.0])) as ArrayRef,
            ],
        )
        .expect("batch");
        write_stream(
            &provider,
            &secret,
            &with(
                operation("f", WriteMode::Append),
                MappingPolicy::Strict,
                &[],
            ),
            Box::new(One(schema, Some(zero))),
        )
        .await
        .expect("-0.0");
        let stored = texts(&admin, "SELECT d::text FROM pln_regola_r.f").await;
        if stored != ["-0"] {
            difformi.push(format!("{profile:?} -0.0: {stored:?}"));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
