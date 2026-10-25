//! Le regole T, F e J, il percorso della sessione al COMMIT e l'arbitro
//! unico dell'upsert contro il fixture PostgreSQL, sugli ingressi della
//! quarta rilettura indipendente del 25/10 (verificati live su 04029d4):
//! ciascuna prova qui sotto falliva su quel commit.

use super::write_visibility_live_tests::{
    dry_run_stream, dsn_as_role, live_dsn, operation, prepare, reset, write_stream, ROLE, SCHEMA,
    SERIAL,
};
use super::*;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::loss::{LossSeverity, MappingPolicy};
use plenora_database_core::plan::{ReadOperation, WriteMode};
use plenora_database_core::provider::{BatchStream, ParameterBag, Provider};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
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

fn with_native(field: Field, native: Option<&str>) -> Field {
    match native {
        Some(native) => field.with_metadata(std::collections::HashMap::from([(
            plenora_database_core::protocol::POSTGRES_NATIVE_TYPE.to_owned(),
            native.to_owned(),
        )])),
        None => field,
    }
}

/// `(id bigint, v text)` con il tipo nativo dichiarato su `v`.
fn input(id: i64, value: &str, native: Option<&str>) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        with_native(Field::new("v", DataType::Utf8, true), native),
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

/// `(id text, a text)`.
fn text_pair(id: &str, a: &str) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("a", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec![id])) as ArrayRef,
            Arc::new(StringArray::from(vec![a])) as ArrayRef,
        ],
    )
    .expect("batch");
    Box::new(One(schema, Some(batch)))
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

fn outcome_of(result: &plenora_database_core::Result<WriteOutcome>) -> String {
    match result {
        Ok(outcome) => format!("ok {:?}", outcome.rows),
        Err(error) => format!("{:?} {}", error.category, error.message),
    }
}

fn with_policy(mut operation: WriteOperation, policy: MappingPolicy) -> WriteOperation {
    operation.mapping_policy = policy;
    operation
}

fn unqualified(mut operation: WriteOperation) -> WriteOperation {
    operation.target.schema = None;
    operation
}

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

async fn read_all(
    provider: &PostgresProvider,
    secret: &SecretString,
    operation: &ReadOperation,
) -> plenora_database_core::Result<Vec<RecordBatch>> {
    let cancellation = CancellationToken::new();
    let budget = budget();
    let mut stream = Provider::read(
        provider,
        secret,
        operation,
        &ParameterBag::default(),
        &budget,
        &cancellation,
    )
    .await?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await? {
        batches.push(batch);
    }
    Ok(batches)
}

/// S1: il ruolo con `search_path = pln_regola_r, public`, due tabelle
/// `pln_r4t` con colonne diverse, e un `ObjectRef` senza schema. Su 04029d4
/// preflight, dry run e scrittura andavano in `public.pln_r4t` (il default
/// della libreria), la lettura rendeva le righe di `pln_regola_r.pln_r4t`
/// con le colonne di `public.pln_r4t` (`FROM "pln_r4t"` risolto dalla
/// sessione), e describe guardava `public`. Ora l'oggetto si risolve una
/// volta con il percorso della sessione, e ogni passo nomina lo schema
/// risolto.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_t_il_target_senza_schema_si_risolve_una_volta() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    let cleanup = "DROP TABLE IF EXISTS public.pln_r4t; DROP TABLE IF EXISTS public.pln_r4n;";
    admin.batch_execute(cleanup).await.expect("pulizia public");
    prepare(
        &admin,
        "CREATE TABLE {s}.pln_r4t (id bigint PRIMARY KEY, owner text); \
         INSERT INTO {s}.pln_r4t VALUES (10, 'app'); \
         GRANT ALL ON {s}.pln_r4t TO {r}; GRANT CREATE ON SCHEMA {s} TO {r}; \
         CREATE TABLE public.pln_r4t (id bigint PRIMARY KEY, owner text, nota text); \
         INSERT INTO public.pln_r4t VALUES (20, 'pub', 'n'); \
         GRANT ALL ON public.pln_r4t TO {r}; \
         ALTER ROLE {r} SET search_path = pln_regola_r, public;",
    )
    .await;
    let mut difformi = Vec::new();
    let rows = |id: i64| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("owner", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![id])) as ArrayRef,
                Arc::new(StringArray::from(vec!["nuovo"])) as ArrayRef,
            ],
        )
        .expect("batch");
        Box::new(One(schema, Some(batch))) as Box<dyn BatchStream>
    };
    for (profile, id) in PROFILES.into_iter().zip([1_i64, 2, 3]) {
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let append = unqualified(operation("pln_r4t", WriteMode::Append));
        if profile != PostgresInsertMode::CopyBinary {
            match dry_run_stream(&provider, &role_secret, &append, rows(id)).await {
                Ok(dry) if dry.target.schema.as_deref() == Some(SCHEMA) => {}
                other => difformi.push(format!(
                    "{profile:?} dry run: {:?}",
                    other.map(|dry| dry.target)
                )),
            }
        }
        let written = write_stream(&provider, &role_secret, &append, rows(id)).await;
        if written.is_err() {
            difformi.push(format!("{profile:?} append: {}", outcome_of(&written)));
        }
        let update = unqualified(operation("pln_r4t", WriteMode::Update));
        let written = write_stream(&provider, &role_secret, &update, rows(10)).await;
        if written.is_err() {
            difformi.push(format!("{profile:?} update: {}", outcome_of(&written)));
        }
    }
    let app = texts(
        &admin,
        "SELECT id || ':' || owner FROM pln_regola_r.pln_r4t ORDER BY id",
    )
    .await;
    let public = texts(
        &admin,
        "SELECT id || ':' || owner FROM public.pln_r4t ORDER BY id",
    )
    .await;
    if app != ["1:nuovo", "2:nuovo", "3:nuovo", "10:nuovo"] || public != ["20:pub"] {
        difformi.push(format!("scritture: app {app:?}, public {public:?}"));
    }
    // La lettura: righe e colonne dello stesso oggetto.
    let provider = PostgresProvider::insecure_local();
    let read: ReadOperation = serde_json::from_value(serde_json::json!({
        "source": {"object": "pln_r4t"},
        "projection": [],
        "order_by": [],
        "row_limit": null,
        "filter": null
    }))
    .expect("lettura");
    match read_all(&provider, &role_secret, &read).await {
        Ok(batches) => {
            let names = batches.first().map(|batch| {
                batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>()
            });
            let total: usize = batches.iter().map(RecordBatch::num_rows).sum();
            if names != Some(vec!["id".to_owned(), "owner".to_owned()]) || total != 4 {
                difformi.push(format!("lettura: {names:?} {total} righe"));
            }
        }
        Err(error) => difformi.push(format!("lettura: {:?} {}", error.category, error.message)),
    }
    // Describe: le colonne dell'oggetto che la sessione risolve.
    let describe = plenora_database_core::plan::Operation::DatabaseDescribeObject {
        source: ObjectRef {
            catalog: None,
            schema: None,
            object: "pln_r4t".to_owned(),
        },
    };
    match Provider::inspect(
        &provider,
        &role_secret,
        &describe,
        &CancellationToken::new(),
    )
    .await
    {
        Ok(inspection) => {
            let columns = inspection.document["columns"]
                .as_array()
                .map(|columns| {
                    columns
                        .iter()
                        .filter_map(|column| column["name"].as_str().map(str::to_owned))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if columns != ["id", "owner"] {
                difformi.push(format!("describe: {columns:?}"));
            }
        }
        Err(error) => difformi.push(format!("describe: {:?} {}", error.category, error.message)),
    }
    // Create senza schema: nello schema corrente della sessione.
    let create = unqualified(operation("pln_r4n", WriteMode::Create));
    let mut create = create;
    create.keys = vec!["id".to_owned()];
    let written = write_stream(&provider, &role_secret, &create, rows(7)).await;
    let created = texts(
        &admin,
        "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE c.relname = 'pln_r4n'",
    )
    .await;
    if written.is_err() || created != [SCHEMA] {
        difformi.push(format!("create: {} -> {created:?}", outcome_of(&written)));
    }
    // Uno schema temporaneo esplicito: un'altra sessione non vede lo stesso
    // `pg_temp`, e la scrittura si rifiuta.
    let mut temporary = operation("pln_r4t", WriteMode::Append);
    temporary.target.schema = Some("pg_temp".to_owned());
    let written = write_stream(&provider, &role_secret, &temporary, rows(8)).await;
    if !written
        .as_ref()
        .is_err_and(|error| error.category == ErrorCategory::InvalidPlan)
    {
        difformi.push(format!("pg_temp: {}", outcome_of(&written)));
    }
    admin
        .batch_execute(&format!("ALTER ROLE {ROLE} RESET search_path; {cleanup}"))
        .await
        .expect("pulizia");
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S2: un testo con `plenora.postgres.native_type` verso una colonna `text`.
/// Su 04029d4 Prepared e Upsert, in ogni politica, chiedevano al server di
/// convertire il valore nel tipo dichiarato e poi di nuovo in `text`:
/// `numeric(5,2)` '1.005' diventava '1.01', `real` '16777217'
/// '1.6777216e+07', `timetz` '10:00' prendeva l'offset della sessione,
/// `jsonb` toglieva la chiave ripetuta; `COPY` scriveva il testo com'era.
/// Ora il valore arriva alla colonna con il suo tipo, letto dal catalogo:
/// nessuna conversione del server verso un altro tipo.
#[tokio::test]
async fn live_f_il_tipo_dichiarato_non_converte_il_valore() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let cases = [
        ("numeric(5,2)", "1.005"),
        ("real", "16777217"),
        ("time with time zone", "10:00"),
        ("timestamp without time zone", "2026-01-01 10:00:00+02"),
        ("jsonb", "{\"b\":1,\"a\":2,\"a\":3}"),
        ("interval", "1 2:03"),
    ];
    let mut difformi = Vec::new();
    for (native, value) in cases {
        for policy in [MappingPolicy::Strict, MappingPolicy::Lossy] {
            for mode in [WriteMode::Append, WriteMode::Upsert] {
                for profile in PROFILES {
                    prepare(
                        &admin,
                        "CREATE TABLE {s}.n (id bigint PRIMARY KEY, v text); \
                         ALTER ROLE {r} SET TimeZone = 'Asia/Tokyo';",
                    )
                    .await;
                    let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
                    let mut plan = with_policy(operation("n", mode), policy);
                    if mode == WriteMode::Upsert {
                        plan.keys = vec!["id".to_owned()];
                    }
                    let written =
                        write_stream(&provider, &secret, &plan, input(1, value, Some(native)))
                            .await;
                    let stored = texts(&admin, "SELECT v FROM pln_regola_r.n").await;
                    // Il testo arriva alla colonna `text` com'e, in ogni
                    // profilo: nessun passaggio dal tipo dichiarato.
                    if written.is_err() || stored != [value] {
                        difformi.push(format!(
                            "{native} {policy:?} {mode:?} {profile:?}: {} -> {stored:?}",
                            outcome_of(&written)
                        ));
                    }
                }
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Non silenzioso, stessa classe di S2: un testo verso una colonna numerica.
/// Con `Strict` il preflight lo rifiuta (tipo Arrow non equivalente); con
/// `Lossy` su 04029d4 Prepared falliva con `protocol` mentre `CopyText`
/// scriveva. Ora l'INSERT preparato manda il testo alla colonna con il suo
/// tipo, e il valore e quello della funzione d'ingresso della colonna,
/// dichiarato come perdita nel `LossReport`, come con `CopyText`.
#[tokio::test]
async fn live_f_un_testo_verso_una_colonna_numerica_e_uguale_in_ogni_profilo() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let cases = [
        ("numeric(5,2)", "1.005", "1.01"),
        ("real", "16777217", "1.6777216e+07"),
        ("double precision", "0.1", "0.1"),
    ];
    let mut difformi = Vec::new();
    for (column, value, expected) in cases {
        for profile in PROFILES {
            prepare(
                &admin,
                &format!("CREATE TABLE {{s}}.n (id bigint PRIMARY KEY, v {column});"),
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let strict = operation("n", WriteMode::Append);
            let written = write_stream(&provider, &secret, &strict, input(1, value, None)).await;
            let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.n").await;
            if !written
                .as_ref()
                .is_err_and(|error| error.category == ErrorCategory::DataMapping)
                || !stored.is_empty()
            {
                difformi.push(format!(
                    "{column} {profile:?} Strict: {} -> {stored:?}",
                    outcome_of(&written)
                ));
            }
            let lossy = with_policy(operation("n", WriteMode::Append), MappingPolicy::Lossy);
            let written = write_stream(&provider, &secret, &lossy, input(2, value, None)).await;
            let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.n").await;
            // `CopyBinary` non codifica un testo verso un tipo non testuale
            // (limite dichiarato in `binary_codec`): lo rifiuta, senza
            // effetti.
            let uniform = if profile == PostgresInsertMode::CopyBinary {
                written.is_err() && stored.is_empty()
            } else {
                written.is_ok() && stored == [expected]
            };
            if !uniform {
                difformi.push(format!(
                    "{column} {profile:?} Lossy: {} -> {stored:?}",
                    outcome_of(&written)
                ));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S3 e regola J: `CopyBinary` codificava il JSON passando da
/// `serde_json::Value` (interi grandi e decimali arrotondati a `f64`,
/// chiavi riordinate, ripetute tolte anche verso `json`); `jsonb` toglie la
/// chiave ripetuta in ogni profilo. Ora `json` riceve il testo com'e, e con
/// `Strict` un testo con chiavi ripetute verso `jsonb` si rifiuta.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_j_il_json_arriva_com_e_e_le_chiavi_ripetute_si_rifiutano() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let verbatim = "{\"b\": 12345678901234567890123, \"a\": 0.1000000000000000055511151231257827, \
                    \"a\": 3}";
    let precise = "{\"n\": 12345678901234567890123, \"f\": 0.1000000000000000055511151231257827}";
    let precise_stored = "{\"f\": 0.1000000000000000055511151231257827, \"n\": \
                          12345678901234567890123}";
    let mut difformi = Vec::new();
    for profile in PROFILES {
        for native in [Some("json"), None] {
            prepare(
                &admin,
                "CREATE TABLE {s}.j (id bigint PRIMARY KEY, v json);",
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let strict = operation("j", WriteMode::Append);
            let written =
                write_stream(&provider, &secret, &strict, input(1, verbatim, native)).await;
            let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.j").await;
            if written.is_err() || stored != [verbatim] {
                difformi.push(format!(
                    "json {native:?} {profile:?}: {} -> {stored:?}",
                    outcome_of(&written)
                ));
            }
        }
        for native in [Some("jsonb"), None] {
            prepare(
                &admin,
                "CREATE TABLE {s}.j (id bigint PRIMARY KEY, v jsonb);",
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let strict = operation("j", WriteMode::Append);
            let written =
                write_stream(&provider, &secret, &strict, input(1, precise, native)).await;
            let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.j").await;
            if written.is_err() || stored != [precise_stored] {
                difformi.push(format!(
                    "jsonb {native:?} {profile:?}: {} -> {stored:?}",
                    outcome_of(&written)
                ));
            }
            for (id, duplicated) in [
                (2, "{\"b\":1,\"a\":2,\"a\":3}"),
                (5, "[{\"x\": {\"k\": 1, \"k\": 2}}]"),
                (6, "{\"a\\u0062\": 1, \"ab\": 2}"),
            ] {
                let written =
                    write_stream(&provider, &secret, &strict, input(id, duplicated, native)).await;
                let refused = written.as_ref().is_err_and(|error| {
                    error.category == ErrorCategory::DataMapping
                        && error.message.ends_with(write::duplicate_keys_reason())
                });
                if !refused {
                    difformi.push(format!(
                        "jsonb {native:?} {profile:?} {duplicated}: {}",
                        outcome_of(&written)
                    ));
                }
                if profile != PostgresInsertMode::CopyBinary {
                    let dry =
                        dry_run_stream(&provider, &secret, &strict, input(2, duplicated, native))
                            .await;
                    if !dry
                        .as_ref()
                        .is_err_and(|error| error.message.ends_with(write::duplicate_keys_reason()))
                    {
                        difformi.push(format!(
                            "jsonb dry run {native:?} {profile:?}: {:?}",
                            dry.map(|_| "ok")
                        ));
                    }
                }
            }
            let lossy = with_policy(operation("j", WriteMode::Append), MappingPolicy::Lossy);
            let written = write_stream(
                &provider,
                &secret,
                &lossy,
                input(3, "{\"b\":1,\"a\":2,\"a\":3}", native),
            )
            .await;
            if written.is_err() {
                difformi.push(format!(
                    "jsonb Lossy {native:?} {profile:?}: {}",
                    outcome_of(&written)
                ));
            }
            // La normalizzazione del tipo e dichiarata nel LossReport.
            let cancellation = CancellationToken::new();
            let budget = budget();
            let prepared = Provider::prepare_write(
                &provider,
                &secret,
                &lossy,
                input(4, "{}", native).schema(),
                &budget,
                &cancellation,
            )
            .await;
            let declared = prepared.as_ref().is_ok_and(|prepared| {
                prepared
                    .loss_report
                    .losses
                    .iter()
                    .any(|loss| loss.severity == LossSeverity::Warning && loss.field_id == 1)
            });
            if !declared {
                difformi.push(format!(
                    "jsonb Lossy {native:?}: LossReport senza la normalizzazione"
                ));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S5: un trigger di vincolo differito scatta al COMMIT. Su 04029d4 il
/// COMMIT partiva con il percorso chiuso della regola Q2, e il confronto
/// `citext = citext` del trigger (in `public`) diventava `text = text`:
/// 'ABC' = 'abc' falso, in silenzio. Ora il percorso della sessione torna
/// al suo posto prima del COMMIT.
#[tokio::test]
async fn live_q2_il_commit_scatta_con_il_percorso_della_sessione() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let setup = "CREATE EXTENSION IF NOT EXISTS citext SCHEMA public; \
        CREATE TABLE {s}.c (id bigint PRIMARY KEY, owner public.citext); \
        INSERT INTO {s}.c VALUES (5, 'ABC'); \
        CREATE TABLE {s}.esito (uguale boolean); \
        CREATE FUNCTION {s}.confronta() RETURNS trigger LANGUAGE plpgsql AS \
          $$ BEGIN INSERT INTO pln_regola_r.esito \
             VALUES (NEW.owner = 'abc'::public.citext); RETURN NULL; END $$; \
        CREATE CONSTRAINT TRIGGER confronta AFTER INSERT OR UPDATE ON {s}.c \
          DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {s}.confronta();";
    let rows = |id: i64| {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("owner", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![id])) as ArrayRef,
                Arc::new(StringArray::from(vec!["ABC"])) as ArrayRef,
            ],
        )
        .expect("batch");
        Box::new(One(schema, Some(batch))) as Box<dyn BatchStream>
    };
    let mut difformi = Vec::new();
    for profile in PROFILES {
        for mode in [WriteMode::Append, WriteMode::Update, WriteMode::Upsert] {
            prepare(&admin, setup).await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let mut plan = operation("c", mode);
            if mode == WriteMode::Upsert {
                plan.keys = vec!["id".to_owned()];
            }
            let id = if mode == WriteMode::Append { 6 } else { 5 };
            let written = write_stream(&provider, &secret, &plan, rows(id)).await;
            let seen = texts(&admin, "SELECT uguale::text FROM pln_regola_r.esito").await;
            if written.is_err() || seen != ["true"] {
                difformi.push(format!(
                    "{profile:?} {mode:?}: {} -> {seen:?}",
                    outcome_of(&written)
                ));
            }
        }
    }
    // Il percorso row-scoped (Append in una transazione con il totale
    // dichiarato) e lo stesso COMMIT.
    prepare(&admin, setup).await;
    let provider = PostgresProvider::insecure_local();
    let plan = operation("c", WriteMode::Append);
    let written = super::write_visibility_live_tests::write_stream(
        &provider,
        &secret,
        &plan,
        super::write_visibility_live_tests::declared_rows(&[(7, "ABC")]),
    )
    .await;
    let seen = texts(&admin, "SELECT uguale::text FROM pln_regola_r.esito").await;
    if written.is_err() || seen != ["true"] {
        difformi.push(format!("row-scoped: {} -> {seen:?}", outcome_of(&written)));
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S6: `ON CONFLICT (id)` sceglie come arbitri tutti gli indici unici sulle
/// stesse colonne. Con `id text COLLATE "C" PRIMARY KEY` e `UNIQUE (id
/// COLLATE ci)` non deterministico, su 04029d4 l'upsert di `('a', 'nuovo')`
/// aggiornava la riga `A` (`a = nuovo`, nessuna riga `a`). Ora l'upsert
/// vuole un solo indice unico che coinvolga le chiavi.
#[tokio::test]
async fn live_k2_l_upsert_vuole_un_solo_indice_unico_sulle_chiavi() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let collation = "CREATE COLLATION {s}.ci (provider = icu, locale = 'und-u-ks-level2', \
                     deterministic = false);";
    let setups = [
        format!(
            "{collation} CREATE TABLE {{s}}.u (id text COLLATE \"C\" PRIMARY KEY, a text); \
             CREATE UNIQUE INDEX u_ci ON {{s}}.u (id COLLATE {{s}}.ci); \
             INSERT INTO {{s}}.u VALUES ('A', 'vecchio');"
        ),
        "CREATE TABLE {s}.u (id text PRIMARY KEY, a text); \
         CREATE UNIQUE INDEX u_low ON {s}.u (pg_catalog.lower(id)); \
         INSERT INTO {s}.u VALUES ('A', 'vecchio');"
            .to_owned(),
        // Il padre di un'ereditarieta: nessun indice prova le chiavi
        // (`keys_index` falso): la condizione 4 vale anche qui, e la seconda
        // lettura del diff lo ha trovato scoperto.
        format!(
            "{collation} CREATE TABLE {{s}}.u (id text COLLATE \"C\" PRIMARY KEY, a text); \
             CREATE UNIQUE INDEX u_ci ON {{s}}.u (id COLLATE {{s}}.ci); \
             CREATE TABLE {{s}}.u_figlio () INHERITS ({{s}}.u); \
             INSERT INTO {{s}}.u VALUES ('A', 'vecchio');"
        ),
    ];
    let read = "SELECT id || ':' || a FROM pln_regola_r.u ORDER BY (id || ':' || a) COLLATE \"C\"";
    let mut difformi = Vec::new();
    for setup in &setups {
        for profile in PROFILES {
            prepare(&admin, setup).await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let plan = operation("u", WriteMode::Upsert);
            let written = write_stream(&provider, &secret, &plan, text_pair("a", "nuovo")).await;
            let stored = texts(&admin, read).await;
            let refused = written.as_ref().is_err_and(|error| {
                error.category == ErrorCategory::Schema
                    && error.message.ends_with(write::upsert_arbiter_reason())
            });
            if !refused || stored != ["A:vecchio"] {
                difformi.push(format!(
                    "{profile:?}: {} -> {stored:?}",
                    outcome_of(&written)
                ));
            }
            if profile != PostgresInsertMode::CopyBinary {
                let dry = dry_run_stream(&provider, &secret, &plan, text_pair("a", "nuovo")).await;
                if !dry
                    .as_ref()
                    .is_err_and(|error| error.message.ends_with(write::upsert_arbiter_reason()))
                {
                    difformi.push(format!("{profile:?} dry run: {:?}", dry.map(|_| "ok")));
                }
            }
        }
    }
    // Un solo indice unico: l'upsert resta come prima.
    prepare(
        &admin,
        "CREATE TABLE {s}.u (id text PRIMARY KEY, a text); INSERT INTO {s}.u VALUES ('A', 'x');",
    )
    .await;
    let provider = PostgresProvider::insecure_local();
    let written = write_stream(
        &provider,
        &secret,
        &operation("u", WriteMode::Upsert),
        text_pair("A", "nuovo"),
    )
    .await;
    let stored = texts(&admin, read).await;
    if written.is_err() || stored != ["A:nuovo"] {
        difformi.push(format!(
            "indice unico solo: {} -> {stored:?}",
            outcome_of(&written)
        ));
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Non silenzioso, su 04029d4: un testo verso una colonna `enum` senza
/// metadati falliva con `protocol` con Prepared, Upsert e `CopyBinary`,
/// mentre `CopyText` lo scriveva. Ora ogni profilo lo scrive.
#[tokio::test]
async fn live_f_un_enum_senza_metadati_si_scrive_in_ogni_profilo() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    for profile in PROFILES {
        for mode in [WriteMode::Append, WriteMode::Upsert] {
            prepare(
                &admin,
                "CREATE TYPE {s}.umore AS ENUM ('felice', 'triste'); \
                 CREATE TABLE {s}.e (id bigint PRIMARY KEY, v {s}.umore);",
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let mut plan = operation("e", mode);
            if mode == WriteMode::Upsert {
                plan.keys = vec!["id".to_owned()];
            }
            let written = write_stream(&provider, &secret, &plan, input(1, "triste", None)).await;
            let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.e").await;
            if written.is_err() || stored != ["triste"] {
                difformi.push(format!(
                    "{profile:?} {mode:?}: {} -> {stored:?}",
                    outcome_of(&written)
                ));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Seconda lettura: con Create un testo dichiarato `numeric(5,2)` o `real`
/// crea la colonna di quel tipo, e con `Strict` `'1.005'` diventava `1.01`
/// (verso una colonna esistente il preflight rifiutava gia il tipo Arrow).
/// Un dominio su `jsonb` con `Lossy` dichiara la normalizzazione.
#[tokio::test]
async fn live_p_create_non_arrotonda_un_testo_con_strict() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    for (native, value) in [("numeric(5,2)", "1.005"), ("real", "16777217")] {
        for profile in PROFILES {
            prepare(&admin, "").await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let mut create = operation("nuova", WriteMode::Create);
            create.keys = vec!["id".to_owned()];
            let written =
                write_stream(&provider, &secret, &create, input(1, value, Some(native))).await;
            let exists = texts(
                &admin,
                "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n \
                 ON n.oid = c.relnamespace WHERE n.nspname = 'pln_regola_r' \
                 AND c.relname = 'nuova'",
            )
            .await;
            if !written
                .as_ref()
                .is_err_and(|error| error.message.ends_with(write::lossy_reason()))
                || !exists.is_empty()
            {
                difformi.push(format!(
                    "{native} {profile:?} Strict: {} -> {exists:?}",
                    outcome_of(&written)
                ));
            }
            let lossy = with_policy(create.clone(), MappingPolicy::Lossy);
            let written =
                write_stream(&provider, &secret, &lossy, input(1, value, Some(native))).await;
            // `CopyBinary` non codifica un testo verso un numero (limite
            // dichiarato in `binary_codec`).
            if written.is_err() != (profile == PostgresInsertMode::CopyBinary) {
                difformi.push(format!(
                    "{native} {profile:?} Lossy: {}",
                    outcome_of(&written)
                ));
            }
        }
    }
    prepare(
        &admin,
        "CREATE DOMAIN {s}.doc AS jsonb; CREATE TABLE {s}.j (id bigint PRIMARY KEY, v {s}.doc);",
    )
    .await;
    let provider = PostgresProvider::insecure_local();
    let lossy = with_policy(operation("j", WriteMode::Append), MappingPolicy::Lossy);
    let cancellation = CancellationToken::new();
    let budget = budget();
    let prepared = Provider::prepare_write(
        &provider,
        &secret,
        &lossy,
        input(1, "{}", None).schema(),
        &budget,
        &cancellation,
    )
    .await;
    if !prepared.as_ref().is_ok_and(|prepared| {
        prepared
            .loss_report
            .losses
            .iter()
            .any(|loss| loss.severity == LossSeverity::Warning && loss.field_id == 1)
    }) {
        difformi.push("dominio su jsonb: LossReport senza la normalizzazione".to_owned());
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
