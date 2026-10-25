//! La regola R contro il fixture PostgreSQL: le scritture distruttive che non
//! vedono tutte le righe si rifiutano prima di ogni effetto, e il dry run
//! riporta lo stesso rifiuto.
//!
//! Gli ingressi sono quelli verificati dalla rilettura del 25/10: su
//! 14960f9 ciascun rifiuto qui sotto era un `committed` con righe vecchie
//! rimaste nel target (Replace) o una chiave presente pubblicata come
//! `skipped` (`DeleteByKeys`, Update).

use super::*;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{TransactionProfile, WriteMode};
use plenora_database_core::provider::Provider;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::row_visibility::{
    HIDDEN_BY_ROW_SECURITY, HIDDEN_BY_RULE, HIDDEN_BY_TRIGGER, HIDDEN_BY_VIEW,
};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase};

pub const SCHEMA: &str = "pln_regola_r";
const ROLE: &str = "pln_regola_r_ruolo";
const PASSWORD: &str = "pln_regola_r_segreto";

/// Le prove condividono schema e ruolo: una alla volta.
pub static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub fn live_dsn() -> Option<String> {
    let declared = std::env::var("PLENORA_TEST_POSTGRES_DSN").ok();
    if declared.is_none() {
        assert!(
            std::env::var("PLENORA_REQUIRE_LIVE_POSTGRES").is_err(),
            "PLENORA_REQUIRE_LIVE_POSTGRES e acceso ma PLENORA_TEST_POSTGRES_DSN manca"
        );
    }
    declared
}

/// La stessa DSN con il ruolo dello scenario: forma URL o `chiave=valore`.
fn dsn_as_role(dsn: &str) -> String {
    if let Some((scheme, rest)) = dsn.split_once("://") {
        let host = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
        return format!("{scheme}://{ROLE}:{PASSWORD}@{host}");
    }
    let kept = dsn
        .split_whitespace()
        .filter(|part| !part.starts_with("user=") && !part.starts_with("password="))
        .collect::<Vec<_>>()
        .join(" ");
    format!("{kept} user={ROLE} password={PASSWORD}")
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

fn rows(values: &[(i64, &str)]) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("owner", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(
                values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                values.iter().map(|(_, owner)| *owner).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .expect("batch");
    Box::new(Rows {
        schema,
        batch: Some(batch),
    })
}

pub fn operation(object: &str, mode: WriteMode) -> WriteOperation {
    let keyed = matches!(
        mode,
        WriteMode::Update | WriteMode::DeleteByKeys | WriteMode::Upsert
    );
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: object.to_owned(),
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
            vec!["owner".to_owned()]
        } else {
            Vec::new()
        },
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

pub async fn write(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &WriteOperation,
    input: &[(i64, &str)],
) -> Result<WriteOutcome> {
    let cancellation = CancellationToken::new();
    let budget = budget();
    let stream = rows(input);
    let prepared = Provider::prepare_write(
        provider,
        secret,
        operation,
        stream.schema(),
        &budget,
        &cancellation,
    )
    .await?;
    Provider::write(provider, secret, prepared, stream, &budget, &cancellation).await
}

pub async fn dry_run(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &WriteOperation,
    input: &[(i64, &str)],
) -> Result<PostgresWriteDryRun> {
    provider
        .dry_run_write(
            Some(secret),
            operation,
            rows(input),
            &budget(),
            &CancellationToken::new(),
        )
        .await
}

pub async fn ids(client: &tokio_postgres::Client, object: &str) -> Vec<i64> {
    client
        .query(
            &format!("SELECT id FROM {SCHEMA}.{object} ORDER BY id"),
            &[],
        )
        .await
        .expect("righe del target")
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect()
}

/// Lo scenario: la preparazione del target (`{s}` lo schema, `{r}` il
/// ruolo), la mode, il ruolo che scrive, la modalita di insert, il motivo
/// atteso del rifiuto.
struct Scenario {
    name: &'static str,
    setup: &'static str,
    object: &'static str,
    mode: WriteMode,
    as_role: bool,
    insert_mode: PostgresInsertMode,
    reason: &'static str,
}

const TABLE: &str = "CREATE TABLE {s}.t (id bigint PRIMARY KEY, owner text); \
     INSERT INTO {s}.t VALUES (1, 'a'), (2, 'b'), (3, 'a'); \
     GRANT USAGE ON SCHEMA {s} TO {r}; GRANT ALL ON {s}.t TO {r};";

#[rustfmt::skip]
const SCENARIOS: &[Scenario] = &[
    // Policy permissiva di DELETE/SELECT: il DELETE di Replace vedeva 1 e 3.
    Scenario { name: "rls-replace-prepared", object: "t", mode: WriteMode::Replace, as_role: true,
        insert_mode: PostgresInsertMode::Prepared, reason: HIDDEN_BY_ROW_SECURITY,
        setup: "ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
                CREATE POLICY vede_a ON {s}.t FOR ALL USING (owner = 'a') WITH CHECK (true);" },
    Scenario { name: "rls-replace-restrittiva", object: "t", mode: WriteMode::Replace, as_role: true,
        insert_mode: PostgresInsertMode::Prepared, reason: HIDDEN_BY_ROW_SECURITY,
        setup: "ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
                CREATE POLICY tutto ON {s}.t USING (true) WITH CHECK (true); \
                CREATE POLICY cancella_a ON {s}.t AS RESTRICTIVE FOR DELETE USING (owner = 'a');" },
    // Le chiavi 2 nascoste dalla policy erano `skipped` come se mancassero.
    Scenario { name: "rls-delete-by-keys", object: "t", mode: WriteMode::DeleteByKeys, as_role: true,
        insert_mode: PostgresInsertMode::CopyText, reason: HIDDEN_BY_ROW_SECURITY,
        setup: "ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
                CREATE POLICY vede_a ON {s}.t FOR ALL USING (owner = 'a') WITH CHECK (true);" },
    Scenario { name: "rls-update", object: "t", mode: WriteMode::Update, as_role: true,
        insert_mode: PostgresInsertMode::CopyText, reason: HIDDEN_BY_ROW_SECURITY,
        setup: "ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
                CREATE POLICY vede_a ON {s}.t FOR ALL USING (owner = 'a') WITH CHECK (true);" },
    // `DO INSTEAD NOTHING`: COMMIT riuscito, nulla sostituito.
    Scenario { name: "regola-replace-copy", object: "t", mode: WriteMode::Replace, as_role: false,
        insert_mode: PostgresInsertMode::CopyText, reason: HIDDEN_BY_RULE,
        setup: "CREATE RULE niente AS ON DELETE TO {s}.t DO INSTEAD NOTHING;" },
    Scenario { name: "trigger-replace", object: "t", mode: WriteMode::Replace, as_role: false,
        insert_mode: PostgresInsertMode::CopyText, reason: HIDDEN_BY_TRIGGER,
        setup: "CREATE FUNCTION {s}.salva_due() RETURNS trigger LANGUAGE plpgsql AS \
                $$ BEGIN IF OLD.id = 2 THEN RETURN NULL; END IF; RETURN OLD; END $$; \
                CREATE TRIGGER salva_due BEFORE DELETE ON {s}.t \
                FOR EACH ROW EXECUTE FUNCTION {s}.salva_due();" },
    // Un trigger di riga su una partizione scatta anche dal DELETE del padre.
    Scenario { name: "trigger-partizione-replace", object: "p", mode: WriteMode::Replace,
        as_role: false, insert_mode: PostgresInsertMode::CopyText, reason: HIDDEN_BY_TRIGGER,
        setup: "CREATE TABLE {s}.p (id bigint, owner text) PARTITION BY RANGE (id); \
                CREATE TABLE {s}.p1 PARTITION OF {s}.p FOR VALUES FROM (0) TO (100); \
                INSERT INTO {s}.p VALUES (1, 'a'), (2, 'b'), (3, 'a'); \
                CREATE FUNCTION {s}.salva_due() RETURNS trigger LANGUAGE plpgsql AS \
                $$ BEGIN IF OLD.id = 2 THEN RETURN NULL; END IF; RETURN OLD; END $$; \
                CREATE TRIGGER salva_due BEFORE DELETE ON {s}.p1 \
                FOR EACH ROW EXECUTE FUNCTION {s}.salva_due();" },
    // Una vista sopra una tabella con RLS: Replace attraverso la vista
    // cancellava soltanto le righe visibili, `DeleteByKeys` le altre le
    // pubblicava `skipped`.
    Scenario { name: "vista-rls-replace", object: "v", mode: WriteMode::Replace, as_role: true,
        insert_mode: PostgresInsertMode::Prepared, reason: HIDDEN_BY_ROW_SECURITY,
        setup: "ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
                CREATE POLICY vede_a ON {s}.t FOR ALL USING (owner = 'a') WITH CHECK (true); \
                CREATE VIEW {s}.v WITH (security_invoker = true) AS SELECT id, owner FROM {s}.t; \
                GRANT ALL ON {s}.v TO {r};" },
    Scenario { name: "vista-rls-delete-by-keys", object: "v", mode: WriteMode::DeleteByKeys,
        as_role: true, insert_mode: PostgresInsertMode::Prepared, reason: HIDDEN_BY_ROW_SECURITY,
        setup: "ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
                CREATE POLICY vede_a ON {s}.t FOR ALL USING (owner = 'a') WITH CHECK (true); \
                CREATE VIEW {s}.v WITH (security_invoker = true) AS SELECT id, owner FROM {s}.t; \
                GRANT ALL ON {s}.v TO {r};" },
    // Una vista semplice: Replace non prova che la definizione non filtri.
    Scenario { name: "vista-replace", object: "v", mode: WriteMode::Replace, as_role: false,
        insert_mode: PostgresInsertMode::Prepared, reason: HIDDEN_BY_VIEW,
        setup: "CREATE VIEW {s}.v AS SELECT id, owner FROM {s}.t WHERE owner = 'a';" },
];

pub async fn reset(admin: &tokio_postgres::Client) {
    admin
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; \
             DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{ROLE}') THEN \
               EXECUTE 'DROP OWNED BY {ROLE}'; EXECUTE 'DROP ROLE {ROLE}'; END IF; END $$;"
        ))
        .await
        .expect("pulizia");
}

pub async fn prepare(admin: &tokio_postgres::Client, setup: &str) {
    reset(admin).await;
    let sql = format!(
        "CREATE ROLE {ROLE} LOGIN PASSWORD '{PASSWORD}'; CREATE SCHEMA {SCHEMA}; {TABLE} {setup}"
    )
    .replace("{s}", SCHEMA)
    .replace("{r}", ROLE);
    admin.batch_execute(&sql).await.expect("preparazione");
}

#[tokio::test]
async fn live_regola_r_rifiuta_prima_di_ogni_effetto_e_il_dry_run_concorda() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    let input = [(10, "a"), (11, "a"), (2, "z")];
    let mut difformi = Vec::new();
    for scenario in SCENARIOS {
        prepare(&admin, scenario.setup).await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(scenario.insert_mode);
        let secret = if scenario.as_role {
            &role_secret
        } else {
            &admin_secret
        };
        let operation = operation(scenario.object, scenario.mode);
        // Replace scrive 10 e 11; DeleteByKeys e Update toccano la 2, che la
        // policy nasconde.
        let values: &[(i64, &str)] = if scenario.mode == WriteMode::Replace {
            &input[..2]
        } else {
            &input[2..]
        };
        let before = ids(&admin, scenario.object).await;
        let dry = dry_run(&provider, secret, &operation, values).await;
        let written = write(&provider, secret, &operation, values).await;
        let after = ids(&admin, scenario.object).await;
        let expected = |error: Option<&DatabaseError>| {
            error.is_some_and(|error| {
                error.category == ErrorCategory::Unsupported
                    && error.phase == ErrorPhase::Write
                    && error.message.ends_with(scenario.reason)
            })
        };
        if !expected(written.as_ref().err()) || after != before {
            difformi.push(format!(
                "{}: scrittura {:?}, righe {before:?} -> {after:?}",
                scenario.name,
                written.map(|outcome| outcome.rows)
            ));
            continue;
        }
        let dry_error = dry.as_ref().err().map(|error| error.message.clone());
        if !expected(dry.as_ref().err()) || dry_error != written.err().map(|error| error.message) {
            difformi.push(format!("{}: dry run {dry:?}", scenario.name));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// I controlli positivi: la regola non rifiuta cio che vede tutte le righe,
/// e Replace pubblica `deleted: None` (le righe svuotate non sono confermate),
/// come SQL Server; fino al 25/10 era `Some(0)` con tre righe cancellate.
#[tokio::test]
async fn live_regola_r_ammette_cio_che_vede_tutte_le_righe() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    for insert_mode in [PostgresInsertMode::Prepared, PostgresInsertMode::CopyText] {
        // Un trigger AFTER non sopprime righe; il proprietario senza FORCE
        // non e soggetto alla policy.
        prepare(
            &admin,
            "CREATE FUNCTION {s}.nota() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN RETURN NULL; END $$; \
             CREATE TRIGGER nota AFTER DELETE ON {s}.t FOR EACH ROW EXECUTE FUNCTION {s}.nota(); \
             ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
             CREATE POLICY vede_a ON {s}.t FOR ALL USING (owner = 'a') WITH CHECK (true);",
        )
        .await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(insert_mode);
        let replace = operation("t", WriteMode::Replace);
        let values = [(10, "a"), (11, "a")];
        dry_run(&provider, &secret, &replace, &values)
            .await
            .expect("dry run replace");
        let outcome = write(&provider, &secret, &replace, &values)
            .await
            .expect("replace");
        assert_eq!(ids(&admin, "t").await, vec![10, 11], "{insert_mode:?}");
        assert_eq!(outcome.rows.inserted, Some(2));
        assert_eq!(outcome.rows.deleted, None, "{insert_mode:?}");
        let delete = operation("t", WriteMode::DeleteByKeys);
        let outcome = write(&provider, &secret, &delete, &[(10, "a"), (99, "a")])
            .await
            .expect("delete_by_keys");
        assert_eq!((outcome.rows.deleted, outcome.rows.skipped), (Some(1), 1));
    }
    reset(&admin).await;
}

/// Sotto una vista la policy della tabella di base decide l'INSERT: il dry
/// run lo dichiara (`not_performed`) invece di `not_applicable`.
#[tokio::test]
async fn live_append_su_una_vista_sopra_rls_dichiara_la_policy() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    prepare(
        &admin,
        "ALTER TABLE {s}.t ENABLE ROW LEVEL SECURITY; \
         CREATE POLICY vede_a ON {s}.t FOR ALL USING (owner = 'a') WITH CHECK (owner = 'a'); \
         CREATE VIEW {s}.v WITH (security_invoker = true) AS SELECT id, owner FROM {s}.t; \
         GRANT ALL ON {s}.v TO {r};",
    )
    .await;
    let provider =
        PostgresProvider::insecure_local().with_insert_mode(PostgresInsertMode::Prepared);
    let append = operation("v", WriteMode::Append);
    let dry = dry_run(&provider, &role_secret, &append, &[(20, "b")])
        .await
        .expect("dry run append");
    assert_eq!(dry.row_security, write::target::RowSecurity::NotPerformed);
    // La scrittura lo conferma: la policy rifiuta la riga.
    let error = write(&provider, &role_secret, &append, &[(20, "b")])
        .await
        .expect_err("policy di INSERT");
    assert_eq!(error.category, ErrorCategory::Authorization);
    reset(&admin).await;
}
