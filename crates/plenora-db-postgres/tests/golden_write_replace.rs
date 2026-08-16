//! Contratto Replace / `TruncateInsert` su PostgreSQL.
//!
//! Replace non ricrea il target: lo svuota con `DELETE FROM` e lo riempie
//! nella stessa transazione. Questi test provano cosa significa "non
//! ricrearlo" — identita dell'oggetto, indici, vincoli, trigger, default,
//! grant e sequence sopravvivono — e cosa significa "stessa transazione":
//! un errore o una cancellazione dopo il DELETE riporta indietro le righe
//! di prima.
//!
//! `TruncateInsert` conserva `TRUNCATE`, che su PostgreSQL e transazionale:
//! anche li un fallimento a meta stream deve lasciare il target come era.
//!
//! `#[ignore]` per default: richiedono Postgres su `dataflow-postgres`.

#![cfg(test)]
#![allow(clippy::doc_markdown, clippy::too_many_lines)]

use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::{
    BatchStream, ParameterValue, Provider, ProviderFuture, SecretString,
};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::transaction::{Statement, TransactionOptions};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorCategory};
use plenora_db_postgres::PostgresProvider;
use std::collections::VecDeque;
use std::sync::Arc;

const DSN: &str = "host=dataflow-postgres user=dataflow password=dataflow_test_2026 \
                   dbname=dataflow_test";
const TARGET: &str = "_replace_target";
const PARENT: &str = "_replace_parent";
const AUDIT: &str = "_replace_audit";

fn secret() -> SecretString {
    SecretString::new(DSN.to_owned())
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

fn provider() -> PostgresProvider {
    PostgresProvider::insecure_local_with_batch_rows(1_024)
}

fn public_ref(object: &str) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("public".to_owned()),
        object: object.to_owned(),
        layer_id: None,
    }
}

fn write_op(target: &str, mode: WriteMode) -> WriteOperation {
    WriteOperation {
        target: public_ref(target),
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

// ============================================================================
//  Stream in-memory: nominale, che fallisce, e che cancella
// ============================================================================

struct MemoryStream {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
}

impl BatchStream for MemoryStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.batches.pop_front())))
    }
}

/// Consegna il primo batch e poi fallisce: il DELETE e gia avvenuto e alcune
/// righe nuove sono gia scritte quando arriva l'errore.
struct FailingStream {
    schema: SchemaRef,
    first: Option<RecordBatch>,
}

impl BatchStream for FailingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        let next = self.first.take().map_or_else(
            || {
                Err(DatabaseError::invalid_plan(
                    "sorgente interrotta a meta stream",
                ))
            },
            |batch| Ok(Some(batch)),
        );
        Box::pin(std::future::ready(next))
    }
}

/// Cancella il token mentre consegna il batch: la scrittura si trova
/// cancellata dopo il DELETE, con il target gia svuotato dentro la
/// transazione.
struct CancellingStream {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
    token: CancellationToken,
}

impl BatchStream for CancellingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        let next = self.batch.take();
        if next.is_some() {
            self.token.cancel();
        }
        Box::pin(std::future::ready(Ok(next)))
    }
}

// ============================================================================
//  Fixture: target con PK identity, default, CHECK, unique index, FK, trigger
// ============================================================================

fn target_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new("parent_id", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, &str, i64)]) -> RecordBatch {
    let mut ids = Int64Builder::with_capacity(rows.len());
    let mut labels = StringBuilder::new();
    let mut parents = Int64Builder::with_capacity(rows.len());
    for (id, label, parent) in rows {
        ids.append_value(*id);
        labels.append_value(*label);
        parents.append_value(*parent);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(ids.finish()),
        Arc::new(labels.finish()),
        Arc::new(parents.finish()),
    ];
    RecordBatch::try_new(target_schema(), columns).expect("batch")
}

async fn execute_sql(statements: &[String]) {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin ddl");
    for statement in statements {
        transaction
            .execute(&Statement::new(statement.clone()), &cancel)
            .await
            .unwrap_or_else(|error| panic!("statement fallito: {statement} -> {error:?}"));
    }
    Box::new(transaction)
        .commit(&cancel)
        .await
        .expect("commit ddl");
}

async fn scalar_text(sql: &str) -> String {
    let provider = provider();
    let cancel = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret(),
            &TransactionOptions::default(),
            &budget(),
            &cancel,
        )
        .await
        .expect("begin query");
    let rows = transaction
        .query(&Statement::new(sql.to_owned()), &cancel)
        .await
        .unwrap_or_else(|error| panic!("query fallita: {sql} -> {error:?}"));
    let value = match rows.first().and_then(|row| row.get_index(0)) {
        Some(ParameterValue::String(text)) => text.clone(),
        other => panic!("valore non testuale: {other:?}"),
    };
    let _ = Box::new(transaction).rollback(&cancel).await;
    value
}

/// Impronta di tutto cio che Replace deve conservare: identita dell'oggetto,
/// vincoli, indici, trigger, default e nullability, grant, opzioni di tabella
/// e stato della sequence.
async fn metadata_digest() -> String {
    scalar_text(&format!(
        "SELECT concat_ws('|',
            (SELECT c.oid::text FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = 'public' AND c.relname = '{TARGET}'),
            COALESCE((SELECT string_agg(conname || '=' || pg_get_constraintdef(oid), ',' ORDER BY conname)
                        FROM pg_constraint WHERE conrelid = 'public.{TARGET}'::regclass), ''),
            COALESCE((SELECT string_agg(indexname || '=' || indexdef, ',' ORDER BY indexname)
                        FROM pg_indexes WHERE schemaname = 'public' AND tablename = '{TARGET}'), ''),
            COALESCE((SELECT string_agg(tgname, ',' ORDER BY tgname)
                        FROM pg_trigger WHERE tgrelid = 'public.{TARGET}'::regclass AND NOT tgisinternal), ''),
            COALESCE((SELECT string_agg(a.attname || '=' || COALESCE(pg_get_expr(d.adbin, d.adrelid), '-')
                                        || '/' || a.attnotnull::text || '/' || a.attidentity::text, ',' ORDER BY a.attnum)
                        FROM pg_attribute a
                        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                       WHERE a.attrelid = 'public.{TARGET}'::regclass
                         AND a.attnum > 0 AND NOT a.attisdropped), ''),
            COALESCE((SELECT relacl::text FROM pg_class WHERE oid = 'public.{TARGET}'::regclass), '-'),
            COALESCE((SELECT reloptions::text FROM pg_class WHERE oid = 'public.{TARGET}'::regclass), '-'),
            COALESCE((SELECT last_value::text FROM pg_sequences
                       WHERE schemaname = 'public'
                         AND sequencename = pg_get_serial_sequence('public.{TARGET}', 'id')
                             ::regclass::text), '-')
        )"
    ))
    .await
}

async fn rows_digest(table: &str) -> String {
    scalar_text(&format!(
        "SELECT COALESCE(string_agg(id || ':' || label || ':' || parent_id, ',' ORDER BY id), '')
           FROM public.{table}"
    ))
    .await
}

async fn audit_count() -> String {
    scalar_text(&format!("SELECT n::text FROM public.{AUDIT}")).await
}

async fn reset_fixture() {
    execute_sql(&[
        format!("DROP TABLE IF EXISTS public.{TARGET}"),
        format!("DROP TABLE IF EXISTS public.{PARENT}"),
        format!("DROP TABLE IF EXISTS public.{AUDIT}"),
        "DROP FUNCTION IF EXISTS public._replace_audit_fn()".to_owned(),
        format!("CREATE TABLE public.{PARENT} (parent_id BIGINT PRIMARY KEY)"),
        format!("INSERT INTO public.{PARENT} VALUES (1), (2)"),
        format!("CREATE TABLE public.{AUDIT} (n BIGINT NOT NULL)"),
        format!("INSERT INTO public.{AUDIT} VALUES (0)"),
        format!(
            "CREATE TABLE public.{TARGET} (
                 id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                 label TEXT NOT NULL DEFAULT 'etichetta-default',
                 parent_id BIGINT NOT NULL REFERENCES public.{PARENT}(parent_id),
                 CONSTRAINT {TARGET}_label_non_vuota CHECK (char_length(label) > 0)
             )"
        ),
        format!("CREATE UNIQUE INDEX {TARGET}_label_uidx ON public.{TARGET} (label)"),
        format!("GRANT SELECT ON public.{TARGET} TO PUBLIC"),
        format!(
            "CREATE FUNCTION public._replace_audit_fn() RETURNS trigger LANGUAGE plpgsql AS $fn$
                 BEGIN UPDATE public.{AUDIT} SET n = n + 1; RETURN NEW; END
             $fn$"
        ),
        format!(
            "CREATE TRIGGER {TARGET}_audit AFTER INSERT ON public.{TARGET}
                 FOR EACH ROW EXECUTE FUNCTION public._replace_audit_fn()"
        ),
        format!(
            "INSERT INTO public.{TARGET} (id, label, parent_id)
             VALUES (1, 'prima', 1), (2, 'seconda', 2), (3, 'terza', 1)"
        ),
        format!("UPDATE public.{AUDIT} SET n = 0"),
    ])
    .await;
}

async fn drop_fixture() {
    execute_sql(&[
        format!("DROP TABLE IF EXISTS public.{TARGET}"),
        format!("DROP TABLE IF EXISTS public.{PARENT}"),
        format!("DROP TABLE IF EXISTS public.{AUDIT}"),
        "DROP FUNCTION IF EXISTS public._replace_audit_fn()".to_owned(),
    ])
    .await;
}

async fn run_write(
    mode: WriteMode,
    stream: Box<dyn BatchStream>,
    cancellation: &CancellationToken,
) -> plenora_database_core::Result<plenora_database_core::outcome::WriteOutcome> {
    let provider = provider();
    let resources = budget();
    let prepared = provider
        .prepare_write(
            &secret(),
            &write_op(TARGET, mode),
            target_schema(),
            &resources,
            cancellation,
        )
        .await?;
    provider
        .write(&secret(), prepared, stream, &resources, cancellation)
        .await
}

fn memory_stream(rows: &[(i64, &str, i64)]) -> Box<dyn BatchStream> {
    Box::new(MemoryStream {
        schema: target_schema(),
        batches: VecDeque::from(vec![batch(rows)]),
    })
}

// ============================================================================
//  Identita e metadata
// ============================================================================

/// Replace scrive nel target esistente: l'oggetto e lo stesso, con gli stessi
/// indici, vincoli, trigger, default, grant e sequence. Se fosse ancora
/// staging + rename, l'`oid` cambierebbe e con lui tutto cio che pende dalla
/// tabella.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_replace_preserves_object_identity_indexes_constraints_and_triggers() {
    reset_fixture().await;
    let before = metadata_digest().await;
    assert!(before.contains("_label_uidx"), "fixture senza unique index");
    assert!(before.contains("_audit"), "fixture senza trigger");
    assert!(
        before.contains("etichetta-default"),
        "fixture senza default"
    );

    let cancel = CancellationToken::new();
    let outcome = run_write(
        WriteMode::Replace,
        memory_stream(&[(10, "nuova-a", 1), (11, "nuova-b", 2)]),
        &cancel,
    )
    .await
    .expect("replace");

    assert_eq!(outcome.rows.confirmed, 2);
    assert_eq!(
        metadata_digest().await,
        before,
        "metadata del target mutato"
    );
    assert_eq!(rows_digest(TARGET).await, "10:nuova-a:1,11:nuova-b:2");
    assert_eq!(audit_count().await, "2", "il trigger non ha visto le righe");

    drop_fixture().await;
}

/// Il target di Replace deve esistere: non e un `Create` mascherato.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_replace_on_a_missing_target_is_not_found() {
    reset_fixture().await;
    execute_sql(&[format!("DROP TABLE public.{TARGET}")]).await;

    let cancel = CancellationToken::new();
    let error = run_write(
        WriteMode::Replace,
        memory_stream(&[(10, "nuova-a", 1)]),
        &cancel,
    )
    .await
    .expect_err("target assente accettato");
    assert_eq!(error.category, ErrorCategory::NotFound);

    drop_fixture().await;
}

/// `create_spatial_index` appartiene alla sola mode che costruisce la
/// tabella. Le altre scrivono in un target che ha gia i propri indici, e
/// `CREATE INDEX` viene emesso senza `IF NOT EXISTS`: onorare il flag
/// fallirebbe alla seconda esecuzione, ignorarlo in silenzio e peggio.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_create_spatial_index_is_rejected_for_every_mode_but_create() {
    reset_fixture().await;
    let before = rows_digest(TARGET).await;
    let provider = provider();
    let cancel = CancellationToken::new();

    for mode in [
        WriteMode::Replace,
        WriteMode::Append,
        WriteMode::TruncateInsert,
        WriteMode::Update,
        WriteMode::Upsert,
        WriteMode::DeleteByKeys,
    ] {
        let mut operation = write_op(TARGET, mode);
        operation.create_spatial_index = true;
        if matches!(
            mode,
            WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
        ) {
            operation.keys = vec!["id".to_owned()];
        }
        let Err(error) = provider
            .prepare_write(&secret(), &operation, target_schema(), &budget(), &cancel)
            .await
        else {
            panic!("create_spatial_index accettato su {mode:?}");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan, "{mode:?}");
    }

    assert_eq!(rows_digest(TARGET).await, before);
    drop_fixture().await;
}

/// Replace non ha semantica di chiave: dichiararne una descrive una tabella
/// che Replace non costruisce. Accettarla e ignorarla lascerebbe credere che
/// il target venga creato o riconciliato su quelle chiavi.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_replace_rejects_keys_and_update_columns() {
    reset_fixture().await;
    let before = rows_digest(TARGET).await;
    let provider = provider();
    let cancel = CancellationToken::new();

    for (label, mutate) in [
        (
            "keys",
            Box::new(|operation: &mut WriteOperation| {
                operation.keys = vec!["id".to_owned()];
            }) as Box<dyn Fn(&mut WriteOperation)>,
        ),
        (
            "update_columns",
            Box::new(|operation: &mut WriteOperation| {
                operation.update_columns = vec!["label".to_owned()];
            }),
        ),
    ] {
        let mut operation = write_op(TARGET, WriteMode::Replace);
        mutate(&mut operation);
        let Err(error) = provider
            .prepare_write(&secret(), &operation, target_schema(), &budget(), &cancel)
            .await
        else {
            panic!("Replace ha accettato {label}");
        };
        assert_eq!(error.category, ErrorCategory::InvalidPlan, "{label}");
    }

    assert_eq!(rows_digest(TARGET).await, before);
    drop_fixture().await;
}

// ============================================================================
//  Rollback: errore e cancellazione dopo il DELETE
// ============================================================================

/// Un errore a meta stream arriva quando il DELETE e gia passato e parte
/// delle righe nuove e gia scritta: il rollback deve riportare esattamente le
/// righe di prima, non un target vuoto.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_replace_restores_the_previous_rows_when_the_stream_fails() {
    reset_fixture().await;
    let before = rows_digest(TARGET).await;
    assert_eq!(before, "1:prima:1,2:seconda:2,3:terza:1");

    let cancel = CancellationToken::new();
    let stream: Box<dyn BatchStream> = Box::new(FailingStream {
        schema: target_schema(),
        first: Some(batch(&[(10, "nuova-a", 1)])),
    });
    let error = run_write(WriteMode::Replace, stream, &cancel)
        .await
        .expect_err("stream interrotto accettato");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(rows_digest(TARGET).await, before, "righe non ripristinate");
    assert_eq!(audit_count().await, "0", "trigger committato dopo rollback");

    drop_fixture().await;
}

/// La cancellazione arriva dopo il DELETE: il target dentro la transazione e
/// gia vuoto, e solo il rollback lo riporta allo stato precedente.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_replace_restores_the_previous_rows_on_cancellation_after_the_delete() {
    reset_fixture().await;
    let before = rows_digest(TARGET).await;

    let cancel = CancellationToken::new();
    let stream: Box<dyn BatchStream> = Box::new(CancellingStream {
        schema: target_schema(),
        batch: Some(batch(&[(10, "nuova-a", 1)])),
        token: cancel.clone(),
    });
    let error = run_write(WriteMode::Replace, stream, &cancel)
        .await
        .expect_err("cancellazione accettata come successo");
    assert_eq!(error.category, ErrorCategory::Cancelled);
    assert_eq!(rows_digest(TARGET).await, before, "righe non ripristinate");

    drop_fixture().await;
}

/// `TruncateInsert` resta su `TRUNCATE` perche PostgreSQL lo esegue dentro la
/// transazione: la prova e che un fallimento successivo lo annulli.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_truncate_insert_restores_the_previous_rows_when_the_stream_fails() {
    reset_fixture().await;
    let before = rows_digest(TARGET).await;

    let cancel = CancellationToken::new();
    let stream: Box<dyn BatchStream> = Box::new(FailingStream {
        schema: target_schema(),
        first: Some(batch(&[(10, "nuova-a", 1)])),
    });
    let error = run_write(WriteMode::TruncateInsert, stream, &cancel)
        .await
        .expect_err("stream interrotto accettato");
    assert_eq!(error.category, ErrorCategory::InvalidPlan);
    assert_eq!(
        rows_digest(TARGET).await,
        before,
        "TRUNCATE non annullato dal rollback"
    );

    drop_fixture().await;
}

/// `TruncateInsert` che va a buon fine sostituisce le righe e lascia intatto
/// il resto: e la controprova del test di rollback.
#[ignore = "live: richiede Postgres su dataflow-postgres"]
#[tokio::test]
async fn pg_truncate_insert_replaces_the_rows_and_keeps_the_metadata() {
    reset_fixture().await;
    let before = metadata_digest().await;

    let cancel = CancellationToken::new();
    let outcome = run_write(
        WriteMode::TruncateInsert,
        memory_stream(&[(20, "trunc-a", 1)]),
        &cancel,
    )
    .await
    .expect("truncate insert");

    assert_eq!(outcome.rows.confirmed, 1);
    assert_eq!(rows_digest(TARGET).await, "20:trunc-a:1");
    assert_eq!(metadata_digest().await, before);

    drop_fixture().await;
}
