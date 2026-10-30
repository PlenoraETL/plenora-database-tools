//! Le regole T (catalog), K (chiavi `NULL`), P con le politiche non strette
//! e J negli array, contro il fixture PostgreSQL, sugli ingressi della quinta
//! rilettura indipendente del 25/10 (verificati live su ff8841c): ciascuna
//! prova qui sotto falliva su quel commit.

use super::write_visibility_live_tests::{
    dry_run_stream, live_dsn, operation, prepare, reset, rule_a, write_stream, SCHEMA, SERIAL,
};
use super::*;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::plan::{Operation, WriteMode};
use plenora_database_core::provider::{BatchStream, Provider};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::{CancellationToken, ErrorCategory};

const PROFILES: [PostgresInsertMode; 3] = [
    PostgresInsertMode::Prepared,
    PostgresInsertMode::CopyText,
    PostgresInsertMode::CopyBinary,
];

/// Il motivo pubblico di un catalog diverso, lo stesso della lettura.
const OTHER_CATALOG: &str = "catalog PostgreSQL diverso dalla connessione corrente";

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

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

/// `(id bigint, v text)`.
fn text_row(id: i64, value: &str) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
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

/// `(id bigint, v jsonb[])` come lista di testi.
fn list_row(id: i64, items: &[&str]) -> Box<dyn BatchStream> {
    let mut builder = ListBuilder::new(StringBuilder::new());
    for item in items {
        builder.values().append_value(item);
    }
    builder.append(true);
    let values = builder.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", values.data_type().clone(), true).with_metadata(
            std::collections::HashMap::from([(
                plenora_database_core::protocol::POSTGRES_NATIVE_TYPE.to_owned(),
                "jsonb[]".to_owned(),
            )]),
        ),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![id])) as ArrayRef,
            Arc::new(values) as ArrayRef,
        ],
    )
    .expect("batch");
    Box::new(One(schema, Some(batch)))
}

/// `(k text, v text)` con le chiavi date.
fn key_rows(keys: &[Option<&str>], values: &[&str]) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(keys.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
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

fn other_catalog(error: &plenora_database_core::DatabaseError) -> bool {
    error.category == ErrorCategory::NotFound && error.message.contains(OTHER_CATALOG)
}

/// Item 1 della quinta rilettura: un target con un catalog che non e il
/// database della connessione. Su ff8841c preflight, dry run e scrittura lo
/// ignoravano: il dry run pubblicava quel catalog e Append `Strict`
/// committava nel database della connessione; describe ed elenchi lo
/// tacevano. Ora ogni operazione lo rifiuta come la lettura.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_t_un_catalog_diverso_si_rifiuta_in_ogni_operazione() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    prepare(
        &admin,
        "CREATE TABLE {s}.cat (id bigint PRIMARY KEY, v text);",
    )
    .await;
    let database: String = admin
        .query_one("SELECT current_database()::text", &[])
        .await
        .expect("database")
        .get(0);
    let mut difformi = Vec::new();
    let elsewhere = |mut operation: WriteOperation| {
        operation.target.catalog = Some("database_che_non_esiste".to_owned());
        operation
    };
    for profile in PROFILES {
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        for mode in [WriteMode::Append, WriteMode::Upsert] {
            let append = elsewhere(operation("cat", mode));
            let schema = text_row(1, "x").schema();
            let prepared = Provider::prepare_write(
                &provider,
                &secret,
                &append,
                schema,
                &budget(),
                &CancellationToken::new(),
            )
            .await;
            if !prepared.as_ref().is_err_and(other_catalog) {
                difformi.push(format!(
                    "{profile:?} {mode:?} prepare: {:?}",
                    prepared.map(|prepared| prepared.operation.target)
                ));
            }
            let written = write_stream(&provider, &secret, &append, text_row(1, "x")).await;
            if !written.as_ref().is_err_and(other_catalog) {
                difformi.push(format!(
                    "{profile:?} {mode:?} scrittura: {}",
                    outcome_of(&written)
                ));
            }
            let dry = dry_run_stream(&provider, &secret, &append, text_row(1, "x")).await;
            if !dry.as_ref().is_err_and(other_catalog) {
                difformi.push(format!(
                    "{profile:?} {mode:?} dry run: {:?}",
                    dry.map(|dry| dry.target)
                ));
            }
        }
        // Il catalog della connessione si ammette.
        let mut here = operation("cat", WriteMode::Append);
        here.target.catalog = Some(database.clone());
        let id = match profile {
            PostgresInsertMode::Prepared => 10,
            PostgresInsertMode::CopyText => 11,
            PostgresInsertMode::CopyBinary => 12,
        };
        let written = write_stream(&provider, &secret, &here, text_row(id, "qui")).await;
        if written.is_err() {
            difformi.push(format!(
                "{profile:?} catalog corrente: {}",
                outcome_of(&written)
            ));
        }
    }
    let stored = texts(
        &admin,
        "SELECT id::text || ':' || v FROM pln_regola_r.cat ORDER BY id",
    )
    .await;
    if stored != ["10:qui", "11:qui", "12:qui"] {
        difformi.push(format!("righe: {stored:?}"));
    }
    let provider = PostgresProvider::insecure_local();
    let source = ObjectRef {
        catalog: Some("database_che_non_esiste".to_owned()),
        schema: Some(SCHEMA.to_owned()),
        object: "cat".to_owned(),
    };
    for inspection in [
        Operation::DatabaseDescribeObject {
            source: source.clone(),
        },
        Operation::DatabaseListObjects {
            source: Some(source.clone()),
        },
        Operation::DatabaseListSchemas {
            source: Some(source.clone()),
        },
    ] {
        let result =
            Provider::inspect(&provider, &secret, &inspection, &CancellationToken::new()).await;
        if !result.as_ref().is_err_and(other_catalog) {
            difformi.push(format!(
                "{inspection:?}: {:?}",
                result.map(|inspection| inspection.operation)
            ));
        }
    }
    let here = ObjectRef {
        catalog: Some(database),
        ..source
    };
    let described = Provider::inspect(
        &provider,
        &secret,
        &Operation::DatabaseDescribeObject { source: here },
        &CancellationToken::new(),
    )
    .await;
    if described.is_err() {
        difformi.push("describe con il catalog corrente rifiutato".to_owned());
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Item 2: `UNIQUE NULLS NOT DISTINCT (k)` con la riga `(NULL,'vecchio')`.
/// Su ff8841c l'Upsert su `k` di `[(NULL,'uno'),(NULL,'due')]` dava
/// `confirmed 2` con una sola riga `'due'`: la regola K non confrontava le
/// chiavi `NULL`. Ora due chiavi che l'indice tratta come una sola si
/// rifiutano, nella scrittura e nel dry run; con un indice `NULLS DISTINCT`
/// le due chiavi `NULL` restano due righe nuove.
#[tokio::test]
async fn live_k_le_chiavi_null_seguono_l_indice() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let upsert = |object: &str| {
        let mut operation = operation(object, WriteMode::Upsert);
        operation.keys = vec!["k".to_owned()];
        operation
    };
    let rows = || key_rows(&[None, None], &["uno", "due"]);
    let duplicate = |error: &plenora_database_core::DatabaseError| {
        error.category == ErrorCategory::DataMapping
            && error
                .message
                .contains(plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS)
    };
    let mut difformi = Vec::new();
    for profile in PROFILES {
        prepare(
            &admin,
            "CREATE TABLE {s}.nnd (k text, v text, UNIQUE NULLS NOT DISTINCT (k)); \
             INSERT INTO {s}.nnd VALUES (NULL, 'vecchio'); \
             CREATE TABLE {s}.nd (k text, v text, UNIQUE (k)); \
             INSERT INTO {s}.nd VALUES (NULL, 'vecchio');",
        )
        .await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let written = write_stream(&provider, &secret, &upsert("nnd"), rows()).await;
        if !written.as_ref().is_err_and(duplicate) {
            difformi.push(format!(
                "{profile:?} NULLS NOT DISTINCT: {}",
                outcome_of(&written)
            ));
        }
        let stored = texts(&admin, "SELECT v FROM pln_regola_r.nnd").await;
        if stored != ["vecchio"] {
            difformi.push(format!("{profile:?} NULLS NOT DISTINCT righe: {stored:?}"));
        }
        if profile != PostgresInsertMode::CopyBinary {
            let dry = dry_run_stream(&provider, &secret, &upsert("nnd"), rows()).await;
            if !dry.as_ref().is_err_and(duplicate) {
                difformi.push(format!(
                    "{profile:?} dry run NULLS NOT DISTINCT: {:?}",
                    dry.map(|dry| dry.rows)
                ));
            }
        }
        let written = write_stream(&provider, &secret, &upsert("nd"), rows()).await;
        if written.is_err() {
            difformi.push(format!(
                "{profile:?} NULLS DISTINCT: {}",
                outcome_of(&written)
            ));
        }
        let stored = texts(&admin, "SELECT v FROM pln_regola_r.nd ORDER BY v").await;
        if stored != ["due", "uno", "vecchio"] {
            difformi.push(format!("{profile:?} NULLS DISTINCT righe: {stored:?}"));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// I motivi delle perdite che il preflight dichiara.
const BOUNDED: &str = "spazi finali";
const JSONB: &str = "jsonb tiene l'ultimo valore";

/// I tipi della prova della regola P con le politiche non strette.
const LOSSY_TYPES: &str = "CREATE DOMAIN {s}.dn AS numeric(5,2); \
     CREATE DOMAIN {s}.dr AS real; \
     CREATE DOMAIN {s}.dv AS varchar(3); \
     CREATE DOMAIN {s}.dj AS jsonb; \
     CREATE TYPE {s}.misto AS (n numeric(5,2), t varchar(3), r real);";

/// Item 3: le perdite che le foglie del tipo danno ai valori, con
/// `Compatible`, `Lossy` e `Native`. Su ff8841c il `LossReport` era vuoto in
/// prepare e nel dry run, e la scrittura committava `1.01`, `{1.01}`,
/// `1.6777216e+07` e il composto arrotondato e troncato. Ora ogni foglia che
/// `Strict` rifiuterebbe ha la sua perdita nel `LossReport`; il valore
/// passa, come la politica chiede. Dal 25/10 (regola A) il testo verso una
/// foglia numerica, gli array e i composti non sono coppie misurate: si
/// rifiutano nel preflight con ogni politica, e le perdite delle foglie si
/// provano sui domini delle foglie misurate (`varchar(3)`, `jsonb`).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_p_le_perdite_delle_foglie_si_dichiarano_con_le_politiche_non_strette() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    // (tipo della colonna, valore, motivi attesi, valore committato)
    let cases: &[(&str, &str, &[&str], &str)] = &[
        ("{s}.dv", "ab   ", &[BOUNDED], "ab "),
        ("{s}.dj", "{\"a\":1,\"a\":2}", &[JSONB], "{\"a\": 2}"),
    ];
    // Le coppie non misurate: rifiutate nel preflight con ogni politica.
    let unmeasured: &[(&str, &str)] = &[
        ("{s}.dn", "1.005"),
        ("numeric(5,2)[]", "{1.005}"),
        ("{s}.dr", "16777217"),
        ("{s}.misto", "(1.005,\"ab   \",16777217)"),
        ("{s}.dv[]", "{\"ab   \"}"),
        ("{s}.dj[]", "{\"{\\\"a\\\":1,\\\"a\\\":2}\"}"),
    ];
    let mut difformi = Vec::new();
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for (column, value, reasons, committed) in cases {
            for profile in [PostgresInsertMode::Prepared, PostgresInsertMode::CopyText] {
                prepare(
                    &admin,
                    &format!(
                        "{LOSSY_TYPES} CREATE TABLE {{s}}.p (id bigint PRIMARY KEY, v {column});"
                    ),
                )
                .await;
                let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
                let mut append = operation("p", WriteMode::Append);
                append.mapping_policy = policy;
                let declared = |report: &plenora_database_core::loss::LossReport| {
                    reasons.iter().all(|reason| {
                        report
                            .losses
                            .iter()
                            .any(|loss| loss.field_id == 1 && loss.reason.contains(reason))
                    })
                };
                let prepared = Provider::prepare_write(
                    &provider,
                    &secret,
                    &append,
                    text_row(1, value).schema(),
                    &budget(),
                    &CancellationToken::new(),
                )
                .await;
                match &prepared {
                    Ok(prepared) if declared(&prepared.loss_report) => {}
                    other => difformi.push(format!(
                        "{policy:?} {profile:?} {column} prepare: {:?}",
                        other.as_ref().map(|prepared| &prepared.loss_report.losses)
                    )),
                }
                let dry = dry_run_stream(&provider, &secret, &append, text_row(1, value)).await;
                match &dry {
                    Ok(dry) if dry.loss_report.as_ref().is_some_and(declared) => {}
                    other => difformi.push(format!(
                        "{policy:?} {profile:?} {column} dry run: {:?}",
                        other.as_ref().map(|dry| &dry.loss_report)
                    )),
                }
                let written = write_stream(&provider, &secret, &append, text_row(1, value)).await;
                let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.p").await;
                if written.is_err() || stored != [*committed] {
                    difformi.push(format!(
                        "{policy:?} {profile:?} {column}: {} -> {stored:?}",
                        outcome_of(&written)
                    ));
                }
            }
        }
    }
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for (column, value) in unmeasured {
            prepare(
                &admin,
                &format!("{LOSSY_TYPES} CREATE TABLE {{s}}.p (id bigint PRIMARY KEY, v {column});"),
            )
            .await;
            let provider = PostgresProvider::insecure_local();
            let mut append = operation("p", WriteMode::Append);
            append.mapping_policy = policy;
            let dry = dry_run_stream(&provider, &secret, &append, text_row(1, value)).await;
            let written = write_stream(&provider, &secret, &append, text_row(1, value)).await;
            let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.p").await;
            if !rule_a(dry.as_ref().err()) || !rule_a(written.as_ref().err()) || !stored.is_empty()
            {
                difformi.push(format!(
                    "{policy:?} {column} non misurata: {} -> {stored:?}",
                    outcome_of(&written)
                ));
            }
        }
    }
    // Create con una colonna di un dominio su `numeric(5,2)`: le foglie del
    // tipo dal catalogo, prima che la colonna esista; il testo verso la foglia
    // numerica non e misurato (regola A).
    prepare(&admin, LOSSY_TYPES).await;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true).with_metadata(std::collections::HashMap::from([
            (
                plenora_database_core::protocol::POSTGRES_NATIVE_TYPE.to_owned(),
                format!("{SCHEMA}.dn"),
            ),
            (
                plenora_database_core::protocol::POSTGRES_TYPE_KIND.to_owned(),
                "d".to_owned(),
            ),
        ])),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(StringArray::from(vec!["1.005"])) as ArrayRef,
        ],
    )
    .expect("batch");
    let mut create = operation("pc", WriteMode::Create);
    create.mapping_policy = MappingPolicy::Lossy;
    let provider = PostgresProvider::insecure_local();
    let prepared = Provider::prepare_write(
        &provider,
        &secret,
        &create,
        Arc::clone(&schema),
        &budget(),
        &CancellationToken::new(),
    )
    .await;
    if !rule_a(prepared.as_ref().err()) {
        difformi.push(format!(
            "Create dominio: {:?}",
            prepared.map(|prepared| prepared.loss_report.losses)
        ));
    }
    let written = write_stream(
        &provider,
        &secret,
        &create,
        Box::new(One(schema, Some(batch))),
    )
    .await;
    let created = texts(
        &admin,
        "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = 'pln_regola_r' AND c.relname = 'pc'",
    )
    .await;
    if !rule_a(written.as_ref().err()) || !created.is_empty() {
        difformi.push(format!(
            "Create dominio: {} -> {created:?}",
            outcome_of(&written)
        ));
    }
    // Con `Strict` le stesse foglie rifiutano il valore.
    for (column, value, _, _) in cases {
        prepare(
            &admin,
            &format!("{LOSSY_TYPES} CREATE TABLE {{s}}.p (id bigint PRIMARY KEY, v {column});"),
        )
        .await;
        let provider = PostgresProvider::insecure_local();
        let written = write_stream(
            &provider,
            &secret,
            &operation("p", WriteMode::Append),
            text_row(1, value),
        )
        .await;
        let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.p").await;
        if !written
            .as_ref()
            .is_err_and(|error| error.category == ErrorCategory::DataMapping)
            || !stored.is_empty()
        {
            difformi.push(format!(
                "Strict {column}: {} -> {stored:?}",
                outcome_of(&written)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Item 4: un oggetto con chiavi ripetute dentro un elemento di `jsonb[]` (e
/// di un array di un dominio su `jsonb`), con `Strict`. Su ff8841c la
/// verifica leggeva come JSON l'intero letterale `{"{\"b\":1,...}"}`, il dry
/// run lo accettava e `Prepared` e `CopyText` committavano
/// `{"{\"a\": 3, \"b\": 1}"}`. Ora il letterale si legge elemento per
/// elemento nella forma ristretta che si legge in un solo modo, e ogni altra
/// forma si rifiuta. Dal 25/10 un array non e una forma misurata (regola A):
/// ogni scrittura verso `jsonb[]` si rifiuta nel preflight, e il dry run lo
/// decide allo stesso modo.
#[tokio::test]
async fn live_j_le_chiavi_ripetute_negli_elementi_di_un_array_si_rifiutano() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let literal = "{\"{\\\"b\\\":1,\\\"a\\\":2,\\\"a\\\":3}\"}";
    let element = "{\"b\":1,\"a\":2,\"a\":3}";
    let refused =
        |result: &plenora_database_core::Result<WriteOutcome>| rule_a(result.as_ref().err());
    let mut difformi = Vec::new();
    for column in ["jsonb[]", "{s}.dj[]"] {
        for profile in [PostgresInsertMode::Prepared, PostgresInsertMode::CopyText] {
            prepare(
                &admin,
                &format!(
                    "CREATE DOMAIN {{s}}.dj AS jsonb; \
                     CREATE TABLE {{s}}.ja (id bigint PRIMARY KEY, v {column});"
                ),
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let strict = operation("ja", WriteMode::Append);
            for (label, input) in [
                ("letterale", text_row(1, literal)),
                ("lista", list_row(2, &[element])),
            ] {
                let written = write_stream(&provider, &secret, &strict, input).await;
                if !refused(&written) {
                    difformi.push(format!(
                        "{column} {profile:?} {label}: {}",
                        outcome_of(&written)
                    ));
                }
            }
            let dry = dry_run_stream(&provider, &secret, &strict, text_row(3, literal)).await;
            if !rule_a(dry.as_ref().err()) {
                difformi.push(format!("{column} {profile:?} dry run letterale: ok"));
            }
            // Anche un letterale senza chiavi ripetute: la forma non e misurata.
            let written = write_stream(
                &provider,
                &secret,
                &strict,
                text_row(4, "{ \"{\\\"a\\\":1}\" , \"[1,2]\" }"),
            )
            .await;
            if !refused(&written) {
                difformi.push(format!(
                    "{column} {profile:?} letterale senza ripetute: {}",
                    outcome_of(&written)
                ));
            }
            let written =
                write_stream(&provider, &secret, &strict, text_row(5, "[1:1]={\"{}\"}")).await;
            if !refused(&written) {
                difformi.push(format!(
                    "{column} {profile:?} letterale con le dimensioni: {}",
                    outcome_of(&written)
                ));
            }
            let stored = texts(&admin, "SELECT v::text FROM pln_regola_r.ja").await;
            if !stored.is_empty() {
                difformi.push(format!("{column} {profile:?} righe: {stored:?}"));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
