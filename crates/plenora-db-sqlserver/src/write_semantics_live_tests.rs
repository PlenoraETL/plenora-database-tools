//! La regola R per ogni mode su SQL Server, contro un server live: i trigger
//! `INSTEAD OF` per gli eventi della mode e il profilo `TdsBulk` su un target
//! con vincoli `CHECK`, foreign key, trigger `INSERT` o default rifiutano la
//! scrittura prima di ogni effetto; le tabelle ordinarie restano ammesse.
//!
//! Gli ingressi sono quelli della rilettura del 25/10 su d30d1d2 (SQL Server
//! 2022): `TdsBulk` con `CHECK (id > 0)` e la riga -5 → `committed` e il
//! vincolo `is_not_trusted = 1`; foreign key verso un padre assente → orfano
//! confermato; `AFTER INSERT` con `THROW` → non scattato; Upsert con
//! `INSTEAD OF UPDATE` vuoto → `updated 1` e la riga invariata, con `INSTEAD
//! OF INSERT` vuoto → `inserted 1` e nessuna riga.

use crate::{
    prepare_write_with_mode, write_prepared, CertificatePolicy, SqlServerConfig,
    SqlServerInsertMode, SqlServerPool, SqlServerSession,
};
use plenora_database_core::arrow::array::{Array, Int32Array, StringArray};
use plenora_database_core::arrow::{DataType, Field, RecordBatch, Schema, SchemaRef};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteOutcome;
use plenora_database_core::plan::{ObjectRef, TransactionProfile, WriteMode, WriteOperation};
use plenora_database_core::provider::{BatchStream, ProviderFuture, SecretString};
use plenora_database_core::row_visibility::{BYPASSED_BY_PATH, HIDDEN_BY_INSTEAD_TRIGGER};
use plenora_database_core::{
    CancellationToken, ErrorCategory, ErrorPhase, ResourceBudget, ResourceLimits,
};
use std::sync::Arc;
use tiberius::Query;

/// Le prove condividono le tabelle: una alla volta.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn live_config() -> SqlServerConfig {
    let host = std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".to_owned());
    let database =
        std::env::var("PLENORA_SQLSERVER_DATABASE").unwrap_or_else(|_| "dataflow_test".to_owned());
    let username =
        std::env::var("PLENORA_SQLSERVER_USER").unwrap_or_else(|_| "dataflow".to_owned());
    let password = std::env::var("PLENORA_SQLSERVER_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    let port = std::env::var("PLENORA_SQLSERVER_PORT")
        .ok()
        .map_or(1_433, |port| port.parse().expect("porta SQL Server"));
    SqlServerConfig::new(host, database, username, SecretString::new(password))
        .with_port(port)
        .with_certificate_policy(CertificatePolicy::TrustServerCertificate)
}

struct Rows {
    schema: SchemaRef,
    batch: Option<RecordBatch>,
}

impl BatchStream for Rows {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next_batch<'a>(
        &'a mut self,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.batch.take())))
    }
}

/// L'ingresso `(id, label)`; `DeleteByKeys` riceve soltanto `id`. Un'etichetta
/// `None` rende la colonna nullable.
fn rows(mode: WriteMode, values: &[(i32, Option<&str>)]) -> Rows {
    let nullable = values.iter().any(|(_, label)| label.is_none());
    let mut fields = vec![Field::new("id", DataType::Int32, false)];
    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int32Array::from(
        values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    ))];
    if mode != WriteMode::DeleteByKeys {
        fields.push(Field::new("label", DataType::Utf8, nullable));
        columns.push(Arc::new(StringArray::from(
            values.iter().map(|(_, label)| *label).collect::<Vec<_>>(),
        )));
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
    Rows {
        schema,
        batch: Some(batch),
    }
}

fn operation(mode: WriteMode) -> WriteOperation {
    // Replace con la chiave: la chiave primaria del target e rappresentata
    // dal piano, e la prova arriva ai legami della colonna.
    let keyed = matches!(
        mode,
        WriteMode::Update | WriteMode::DeleteByKeys | WriteMode::Upsert | WriteMode::Replace
    );
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: "semantica".to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: if keyed {
            vec!["id".to_owned()]
        } else {
            Vec::new()
        },
        update_columns: if mode == WriteMode::Update {
            vec!["label".to_owned()]
        } else {
            Vec::new()
        },
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn write(
    pool: &Arc<SqlServerPool>,
    mode: WriteMode,
    insert_mode: SqlServerInsertMode,
    values: &[(i32, Option<&str>)],
) -> plenora_database_core::Result<WriteOutcome> {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let input = rows(mode, values);
    let prepared = prepare_write_with_mode(
        pool,
        &operation(mode),
        input.schema(),
        &budget,
        &cancellation,
        insert_mode,
    )
    .await?;
    write_prepared(prepared, Box::new(input), &cancellation).await
}

const RESET: &str = "DROP TABLE IF EXISTS [plenora_test].[semantica]; \
     DROP TABLE IF EXISTS [plenora_test].[semantica_padre]; \
     DROP TABLE IF EXISTS [plenora_test].[semantica_tipizzata]; \
     IF TYPE_ID(N'plenora_test.semantica_positivo') IS NOT NULL \
       BEGIN EXEC sp_unbindrule N'plenora_test.semantica_positivo'; \
       DROP TYPE [plenora_test].[semantica_positivo]; END; \
     DROP RULE IF EXISTS [plenora_test].[semantica_regola]; \
     DROP DEFAULT IF EXISTS [plenora_test].[semantica_predefinita]; \
     CREATE TABLE [plenora_test].[semantica_padre] ([id] int NOT NULL PRIMARY KEY); \
     INSERT INTO [plenora_test].[semantica_padre] VALUES (1), (2), (3), (10), (11); \
     CREATE TABLE [plenora_test].[semantica] \
     ([id] int NOT NULL PRIMARY KEY, [label] nvarchar(100) NULL); \
     INSERT INTO [plenora_test].[semantica] VALUES (1, N'a'), (2, N'b'), (3, N'a');";
const CHECK: &str = "ALTER TABLE [plenora_test].[semantica] \
     ADD CONSTRAINT [semantica_positivo] CHECK ([id] > 0);";
const FK: &str = "ALTER TABLE [plenora_test].[semantica] ADD CONSTRAINT [semantica_padre_fk] \
     FOREIGN KEY ([id]) REFERENCES [plenora_test].[semantica_padre] ([id]);";
const DEFAULT: &str = "ALTER TABLE [plenora_test].[semantica] \
     ADD CONSTRAINT [semantica_default] DEFAULT N'predefinita' FOR [label];";
const AFTER_THROW: &str = "EXEC(N'CREATE TRIGGER [plenora_test].[semantica_t] \
     ON [plenora_test].[semantica] AFTER INSERT AS BEGIN THROW 50001, N''no'', 1; END;');";
const INSTEAD_INSERT: &str = "EXEC(N'CREATE TRIGGER [plenora_test].[semantica_t] \
     ON [plenora_test].[semantica] INSTEAD OF INSERT AS BEGIN SET NOCOUNT ON; END;');";
// Le regole e i default legati (`sp_bindrule`, `sp_bindefault`): oggetti di
// prima di `CHECK` e `DEFAULT`, che il bulk senza `CHECK_CONSTRAINTS` ne
// `KEEP_NULLS` scavalca come i vincoli.
const BOUND_RULE: &str = "EXEC(N'CREATE RULE [plenora_test].[semantica_regola] AS @v > 0'); \
     EXEC sp_bindrule N'plenora_test.semantica_regola', N'plenora_test.semantica.id';";
const BOUND_DEFAULT: &str = "EXEC(N'CREATE DEFAULT [plenora_test].[semantica_predefinita] \
     AS N''legacy'''); \
     EXEC sp_bindefault N'plenora_test.semantica_predefinita', N'plenora_test.semantica.label';";
// Un tipo alias con la regola legata, poi una colonna di quel tipo: la
// regola vale per la colonna anche senza un legame sulla colonna.
const ALIAS_RULE: &str = "EXEC(N'CREATE RULE [plenora_test].[semantica_regola] AS @v > 0'); \
     CREATE TYPE [plenora_test].[semantica_positivo] FROM int NOT NULL; \
     EXEC sp_bindrule N'plenora_test.semantica_regola', N'plenora_test.semantica_positivo'; \
     DROP TABLE [plenora_test].[semantica]; \
     EXEC(N'CREATE TABLE [plenora_test].[semantica] \
     ([id] [plenora_test].[semantica_positivo] PRIMARY KEY, [label] nvarchar(100) NULL)'); \
     INSERT INTO [plenora_test].[semantica] VALUES (1, N'a'), (2, N'b'), (3, N'a');";
// Un trigger AFTER per ogni evento che non fa niente: `OUTPUT` senza `INTO`
// lo rifiuta (334), `OUTPUT ... INTO` no.
const AFTER_ALL: &str = "EXEC(N'CREATE TRIGGER [plenora_test].[semantica_t] \
     ON [plenora_test].[semantica] AFTER INSERT, UPDATE, DELETE AS BEGIN SET NOCOUNT ON; END;');";
// Un trigger AFTER INSERT che toglie la riga appena scritta: logica del
// target, non silenziosa (N7), ammessa e dichiarata.
const AFTER_DELETES: &str = "EXEC(N'CREATE TRIGGER [plenora_test].[semantica_t] \
     ON [plenora_test].[semantica] AFTER INSERT AS BEGIN SET NOCOUNT ON; \
     DELETE s FROM [plenora_test].[semantica] AS s JOIN inserted AS i ON i.[id] = s.[id]; END;');";
const INSTEAD_UPDATE: &str = "EXEC(N'CREATE TRIGGER [plenora_test].[semantica_t] \
     ON [plenora_test].[semantica] INSTEAD OF UPDATE AS BEGIN SET NOCOUNT ON; END;');";

/// Le righe del target (`id:label`) e i vincoli non piu fidati.
async fn observe(admin: &mut SqlServerSession) -> (Vec<String>, Vec<String>) {
    let mut results = admin
        .execute_query(
            Query::new(
                "SELECT CONCAT([id], ':', ISNULL([label], N'<null>')) \
                 FROM [plenora_test].[semantica] ORDER BY [id]; \
                 SELECT name FROM sys.check_constraints \
                  WHERE parent_object_id = OBJECT_ID(N'[plenora_test].[semantica]') \
                    AND is_not_trusted = 1 \
                 UNION ALL SELECT name FROM sys.foreign_keys \
                  WHERE parent_object_id = OBJECT_ID(N'[plenora_test].[semantica]') \
                    AND is_not_trusted = 1;",
            ),
            ErrorPhase::Probe,
            &CancellationToken::new(),
        )
        .await
        .expect("osservazione");
    let text = |rows: Vec<tiberius::Row>| {
        rows.into_iter()
            .map(|row| {
                row.try_get::<&str, _>(0)
                    .ok()
                    .flatten()
                    .unwrap_or("")
                    .to_owned()
            })
            .collect::<Vec<_>>()
    };
    let untrusted = text(results.pop().unwrap_or_default());
    let rows = text(results.pop().unwrap_or_default());
    (rows, untrusted)
}

/// Il caso: nome, preparazione dopo [`RESET`], mode, profilo, ingresso,
/// motivo atteso.
type Refusal = (
    &'static str,
    &'static str,
    WriteMode,
    SqlServerInsertMode,
    &'static [(i32, Option<&'static str>)],
    &'static str,
);

#[rustfmt::skip]
const REFUSALS: &[Refusal] = &[
    ("bulk-check", CHECK, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(-5, Some("x")), (10, Some("y"))], BYPASSED_BY_PATH),
    // Anche con righe valide: il bulk senza CHECK_CONSTRAINTS lascia il
    // vincolo non piu fidato.
    ("bulk-check-righe-valide", CHECK, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(10, Some("y"))], BYPASSED_BY_PATH),
    ("bulk-fk", FK, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(99, Some("x")), (10, Some("y"))], BYPASSED_BY_PATH),
    ("bulk-after-trigger", AFTER_THROW, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(10, Some("y"))], BYPASSED_BY_PATH),
    ("bulk-default", DEFAULT, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(10, None)], BYPASSED_BY_PATH),
    ("truncate-insert-bulk-check", CHECK, WriteMode::TruncateInsert, SqlServerInsertMode::TdsBulk,
        &[(-5, Some("x")), (10, Some("y"))], BYPASSED_BY_PATH),
    ("append-instead-insert", INSTEAD_INSERT, WriteMode::Append, SqlServerInsertMode::Prepared,
        &[(10, Some("y")), (11, Some("z"))], HIDDEN_BY_INSTEAD_TRIGGER),
    ("bulk-instead-insert", INSTEAD_INSERT, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(10, Some("y")), (11, Some("z"))], HIDDEN_BY_INSTEAD_TRIGGER),
    ("truncate-insert-instead-insert", INSTEAD_INSERT, WriteMode::TruncateInsert,
        SqlServerInsertMode::Prepared, &[(10, Some("y"))], HIDDEN_BY_INSTEAD_TRIGGER),
    ("upsert-instead-insert", INSTEAD_INSERT, WriteMode::Upsert, SqlServerInsertMode::Prepared,
        &[(10, Some("y")), (2, Some("z"))], HIDDEN_BY_INSTEAD_TRIGGER),
    ("upsert-instead-update", INSTEAD_UPDATE, WriteMode::Upsert, SqlServerInsertMode::Prepared,
        &[(10, Some("y")), (2, Some("z"))], HIDDEN_BY_INSTEAD_TRIGGER),
    ("update-instead-update", INSTEAD_UPDATE, WriteMode::Update, SqlServerInsertMode::Prepared,
        &[(2, Some("z"))], HIDDEN_BY_INSTEAD_TRIGGER),
    // La regola W1 (25/10): regole e default legati. Fino ad allora la -5
    // passava contro la regola e un NULL diventava 'legacy'; il percorso
    // preparato rifiuta la -5 (513) e conserva il NULL.
    ("bulk-regola-legata", BOUND_RULE, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(-5, Some("x")), (10, Some("y"))], BYPASSED_BY_PATH),
    ("bulk-default-legato", BOUND_DEFAULT, WriteMode::Append, SqlServerInsertMode::TdsBulk,
        &[(10, None)], BYPASSED_BY_PATH),
    ("truncate-insert-bulk-regola-legata", BOUND_RULE, WriteMode::TruncateInsert,
        SqlServerInsertMode::TdsBulk, &[(-5, Some("x"))], BYPASSED_BY_PATH),
];

/// Il controllo positivo: nome, preparazione dopo [`RESET`], mode, profilo,
/// ingresso, righe attese, righe confermate.
type Admission<'a> = (
    &'a str,
    &'a str,
    WriteMode,
    SqlServerInsertMode,
    &'a [(i32, Option<&'a str>)],
    &'a [&'a str],
    u64,
);

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_regola_r_rifiuta_bulk_senza_garanzie_e_instead_of_per_ogni_mode() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let config = live_config();
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    let pool = SqlServerPool::new(config, 2).expect("pool");
    let mut difformi = Vec::new();
    for (name, setup, mode, insert_mode, values, reason) in REFUSALS {
        admin
            .execute_query(
                Query::new(format!("{RESET} {setup}")),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("preparazione {name}: {error:?}"));
        let before = observe(&mut admin).await;
        let written = write(&pool, *mode, *insert_mode, values).await;
        let after = observe(&mut admin).await;
        let refused = written.as_ref().err().is_some_and(|error| {
            error.category == ErrorCategory::Unsupported
                && error.phase == ErrorPhase::Prepare
                && error.message.ends_with(reason)
        });
        if !refused || after != before {
            difformi.push(format!(
                "{name}: scrittura {:?}, righe {before:?} -> {after:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("pulizia");
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// I controlli positivi: le tabelle ordinarie (anche con `CHECK`, foreign key
/// e default, nel profilo preparato) restano ammesse in ogni mode che passa
/// dalla regola, e il bulk su una tabella senza vincoli ne trigger ne default
/// scrive. Replace non c'e: non passa dalla regola, e le sue condizioni
/// (nessun trigger, vincoli rappresentati dal piano) restano quelle di
/// `validate_replace_external_state`.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_regola_r_ammette_le_tabelle_ordinarie() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let config = live_config();
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    let pool = SqlServerPool::new(config, 2).expect("pool");
    let constrained = format!("{CHECK} {FK} {DEFAULT}");
    let prepared = SqlServerInsertMode::Prepared;
    let bulk = SqlServerInsertMode::TdsBulk;
    let y: &[(i32, Option<&str>)] = &[(10, Some("y"))];
    let upsert: &[(i32, Option<&str>)] = &[(10, Some("y")), (2, Some("z"))];
    let keyed: &[(i32, Option<&str>)] = &[(2, Some("z")), (99, Some("z"))];
    let after_all = AFTER_ALL;
    let rule = BOUND_RULE;
    let default = BOUND_DEFAULT;
    let null: &[(i32, Option<&str>)] = &[(10, None)];
    #[rustfmt::skip]
    let cases: [Admission<'_>; 21] = [
        ("append", "", WriteMode::Append, prepared, y, &["1:a", "2:b", "3:a", "10:y"], 1),
        ("append-bulk", "", WriteMode::Append, bulk, y, &["1:a", "2:b", "3:a", "10:y"], 1),
        ("truncate-insert", "", WriteMode::TruncateInsert, prepared, y, &["10:y"], 1),
        ("truncate-insert-bulk", "", WriteMode::TruncateInsert, bulk, y, &["10:y"], 1),
        ("upsert", "", WriteMode::Upsert, prepared, upsert, &["1:a", "2:z", "3:a", "10:y"], 2),
        ("update", "", WriteMode::Update, prepared, keyed, &["1:a", "2:z", "3:a"], 1),
        ("delete-by-keys", "", WriteMode::DeleteByKeys, prepared, keyed, &["1:a", "3:a"], 1),
        ("append-vincoli", &constrained, WriteMode::Append, prepared, y, &["1:a", "2:b", "3:a", "10:y"], 1),
        ("truncate-insert-vincoli", &constrained, WriteMode::TruncateInsert, prepared, y, &["10:y"], 1),
        ("upsert-vincoli", &constrained, WriteMode::Upsert, prepared, upsert, &["1:a", "2:z", "3:a", "10:y"], 2),
        ("update-vincoli", &constrained, WriteMode::Update, prepared, keyed, &["1:a", "2:z", "3:a"], 1),
        ("delete-by-keys-vincoli", &constrained, WriteMode::DeleteByKeys, prepared, keyed, &["1:a", "3:a"], 1),
        // N6 (25/10): con un trigger AFTER abilitato `OUTPUT` senza `INTO`
        // falliva con 334 in Append, `TruncateInsert`, Update e
        // `DeleteByKeys`; ora l'`OUTPUT` va in una variabile tabella.
        ("append-after", after_all, WriteMode::Append, prepared, y, &["1:a", "2:b", "3:a", "10:y"], 1),
        ("truncate-insert-after", after_all, WriteMode::TruncateInsert, prepared, y, &["10:y"], 1),
        ("upsert-after", after_all, WriteMode::Upsert, prepared, upsert, &["1:a", "2:z", "3:a", "10:y"], 2),
        ("update-after", after_all, WriteMode::Update, prepared, keyed, &["1:a", "2:z", "3:a"], 1),
        ("delete-by-keys-after", after_all, WriteMode::DeleteByKeys, prepared, keyed, &["1:a", "3:a"], 1),
        // N7: il trigger AFTER che toglie la riga scritta e logica del target,
        // ammessa: la riga e stata scritta (e contata) e il trigger l'ha tolta.
        ("append-after-cancella", AFTER_DELETES, WriteMode::Append, prepared, y, &["1:a", "2:b", "3:a"], 1),
        // La regola e il default legati nel percorso preparato: la riga valida
        // passa, il NULL resta NULL.
        ("append-regola-legata", rule, WriteMode::Append, prepared, y, &["1:a", "2:b", "3:a", "10:y"], 1),
        ("append-default-legato", default, WriteMode::Append, prepared, null, &["1:a", "2:b", "3:a", "10:<null>"], 1),
        ("update-default-legato", default, WriteMode::Update, prepared, &[(2, None)], &["1:a", "2:<null>", "3:a"], 1),
    ];
    let mut difformi = Vec::new();
    for (name, setup, mode, insert_mode, values, expected, confirmed) in cases {
        admin
            .execute_query(
                Query::new(format!("{RESET} {setup}")),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("preparazione {name}: {error:?}"));
        let written = write(&pool, mode, insert_mode, values).await;
        let (rows, untrusted) = observe(&mut admin).await;
        let counted = written
            .as_ref()
            .is_ok_and(|outcome| outcome.rows.confirmed == confirmed);
        if !counted || rows != expected || !untrusted.is_empty() {
            difformi.push(format!(
                "{name}: scrittura {:?}, righe {rows:?}, non fidati {untrusted:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("pulizia");
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Il percorso preparato applica la regola legata: la -5 si rifiuta (513),
/// senza effetti. E il riferimento che il bulk non applicava.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_regola_legata_rifiuta_nel_percorso_preparato() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let config = live_config();
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    let pool = SqlServerPool::new(config, 2).expect("pool");
    let mut difformi = Vec::new();
    for (name, setup) in [("colonna", BOUND_RULE), ("tipo-alias", ALIAS_RULE)] {
        admin
            .execute_query(
                Query::new(format!("{RESET} {setup}")),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("preparazione {name}: {error:?}"));
        for insert_mode in [SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk] {
            let before = observe(&mut admin).await;
            let written = write(
                &pool,
                WriteMode::Append,
                insert_mode,
                &[(-5, Some("x")), (10, Some("y"))],
            )
            .await;
            let after = observe(&mut admin).await;
            if written.is_ok() || after != before {
                difformi.push(format!(
                    "{name} {insert_mode:?}: {:?}, {before:?} -> {after:?}",
                    written.map(|outcome| outcome.rows)
                ));
            }
        }
    }
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("pulizia");
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La regola W2 su SQL Server: una sessione del pool su cui SQL del
/// chiamante ha eseguito `SET ROWCOUNT 1` o `SET ANSI_WARNINGS OFF` non
/// cambia la scrittura successiva. Con quelle impostazioni Update su due
/// chiavi presenti ne aggiornerebbe una e pubblicherebbe l'altra `skipped`, e
/// una stringa oltre `nvarchar(100)` sarebbe troncata senza errore.
///
/// Misurato il 25/10, gia vero prima della regola: lo SQL del pool viaggia
/// come chiamata RPC (`sp_executesql`), e un `SET` dentro la chiamata dura
/// fino alla sua fine. La prova resta a guardia del percorso, insieme al
/// `SET ROWCOUNT 0` e alla verifica di `@@OPTIONS` della sessione.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_w2_le_impostazioni_lasciate_nella_sessione_non_arrivano_alla_scrittura() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let config = live_config();
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let leak = |sql: &'static str| {
        let pool = Arc::clone(&pool);
        async move {
            let mut pooled = pool
                .checkout(&CancellationToken::new())
                .await
                .expect("checkout");
            pooled
                .session_mut()
                .expect("sessione")
                .execute_query(
                    Query::new(sql),
                    ErrorPhase::Write,
                    &CancellationToken::new(),
                )
                .await
                .expect("impostazione lasciata");
            // Torna al pool: la sessione e pronta.
            drop(pooled);
        }
    };
    let mut difformi = Vec::new();
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("preparazione");
    leak("SET ROWCOUNT 1;").await;
    let written = write(
        &pool,
        WriteMode::Update,
        SqlServerInsertMode::Prepared,
        &[(1, Some("z")), (2, Some("z"))],
    )
    .await;
    let (rows, _) = observe(&mut admin).await;
    if !written
        .as_ref()
        .is_ok_and(|outcome| (outcome.rows.confirmed, outcome.rows.skipped) == (2, 0))
        || rows != ["1:z", "2:z", "3:a"]
    {
        difformi.push(format!(
            "rowcount: {:?}, righe {rows:?}",
            written.map(|outcome| outcome.rows)
        ));
    }
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("preparazione");
    leak("SET ANSI_WARNINGS OFF;").await;
    let long = "x".repeat(150);
    let written = write(
        &pool,
        WriteMode::Append,
        SqlServerInsertMode::Prepared,
        &[(10, Some(long.as_str()))],
    )
    .await;
    let (rows, _) = observe(&mut admin).await;
    if written.is_ok() || rows != ["1:a", "2:b", "3:a"] {
        difformi.push(format!(
            "ansi_warnings: {:?}, righe {} ",
            written.map(|outcome| outcome.rows),
            rows.len()
        ));
    }
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("pulizia");
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// La regola W1 per il percorso di Replace (staging e scambio dei nomi): la
/// tabella nuova nasce dal piano, e una regola o un default legati alla
/// colonna del target non la seguono. Trovato il 25/10 cercando la classe di
/// S2, su 15b57f8 e dopo la correzione del bulk: Replace con la riga -5 su
/// una colonna con `@v > 0` legata -> `committed`, la -5 nel target e la
/// regola sparita; con il default legato, il default sparito. Ora Replace si
/// rifiuta prima di ogni effetto, e il legame resta.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_w1_replace_non_perde_regole_e_default_legati() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let config = live_config();
    let mut admin = SqlServerSession::open(&config, &cancellation)
        .await
        .expect("admin");
    let pool = SqlServerPool::new(config, 2).expect("pool");
    let mut difformi = Vec::new();
    for (name, setup, values) in [
        (
            "regola",
            BOUND_RULE,
            &[(-5, Some("x")), (10, Some("y"))][..],
        ),
        ("default", BOUND_DEFAULT, &[(10, None)][..]),
    ] {
        admin
            .execute_query(
                Query::new(format!("{RESET} {setup}")),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .unwrap_or_else(|error| panic!("preparazione {name}: {error:?}"));
        let before = observe(&mut admin).await;
        let written = write(
            &pool,
            WriteMode::Replace,
            SqlServerInsertMode::Prepared,
            values,
        )
        .await;
        let after = observe(&mut admin).await;
        let links = bound_links(&mut admin).await;

        let refused = written
            .as_ref()
            .is_err_and(|error| error.category == ErrorCategory::Unsupported);
        if !refused || after != before || links != 1 {
            difformi.push(format!(
                "{name}: {:?}, {before:?} -> {after:?}, legami {links}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("pulizia");
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Le colonne del target con una regola o un default legati.
async fn bound_links(admin: &mut SqlServerSession) -> i64 {
    admin
        .execute_query(
            Query::new(
                "SELECT COUNT_BIG(*) FROM sys.columns                   WHERE object_id = OBJECT_ID(N'[plenora_test].[semantica]')                     AND (rule_object_id <> 0 OR default_object_id <> 0);",
            ),
            ErrorPhase::Probe,
            &CancellationToken::new(),
        )
        .await
        .expect("legami")
        .first()
        .and_then(|rows| rows.first())
        .and_then(|row| row.try_get::<i64, _>(0).ok().flatten())
        .unwrap_or_default()
}

// La seconda rilettura del 25/10 (regole K, V, isolamento, IGNORE_DUP_KEY).
#[cfg(test)]
#[path = "write_semantics_r2_live_tests.rs"]
mod r2;

// La terza rilettura del 25/10 (regole P e K2).
#[cfg(test)]
#[path = "write_semantics_r3_live_tests.rs"]
mod r3;

// La quarta rilettura del 25/10 (regole T, J, S7, valori esatti, S6).
#[cfg(test)]
#[path = "write_semantics_r4_live_tests.rs"]
mod r4;
