//! Le regole Q2, K2, V2 e P contro il fixture PostgreSQL, sugli ingressi
//! della terza rilettura indipendente del 25/10 (verificati live su
//! 1b02559): ciascuna prova qui sotto falliva su quel commit con un
//! `committed` silenzioso.

use super::write_visibility_live_tests::{
    dry_run_stream, dsn_as_role, live_dsn, operation, prepare, reset, write_stream, SERIAL,
};
use super::*;
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{
    ArrayRef, Int64Array, RecordBatch, StringArray, Time64MicrosecondArray,
    TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use plenora_database_core::loss::{LossCategory, MappingPolicy};
use plenora_database_core::plan::WriteMode;
use plenora_database_core::provider::{BatchStream, Provider};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::write_keys::KEYS_WITHOUT_UNIQUE_INDEX;
use plenora_database_core::{CancellationToken, ErrorCategory};

const PROFILES: [PostgresInsertMode; 3] = [
    PostgresInsertMode::Prepared,
    PostgresInsertMode::CopyText,
    PostgresInsertMode::CopyBinary,
];

/// Un ingresso di un batch.
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

fn one(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch");
    Box::new(One(schema, Some(batch)))
}

fn native(field: Field, native: &str) -> Field {
    field.with_metadata(std::collections::HashMap::from([(
        plenora_database_core::protocol::POSTGRES_NATIVE_TYPE.to_owned(),
        native.to_owned(),
    )]))
}

/// `(id, at)` con `at` testo.
fn text_input(id: i64, at: &str) -> Box<dyn BatchStream> {
    one(
        vec![
            Field::new("id", DataType::Int64, false),
            Field::new("at", DataType::Utf8, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![id])) as ArrayRef,
            Arc::new(StringArray::from(vec![at])) as ArrayRef,
        ],
    )
}

/// `(id, owner)` con chiavi di testo.
fn text_keys(values: &[(&str, &str)]) -> Box<dyn BatchStream> {
    one(
        vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("owner", DataType::Utf8, true),
        ],
        vec![
            Arc::new(StringArray::from(
                values.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(StringArray::from(
                values.iter().map(|(_, owner)| *owner).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
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

fn rows_of(result: &plenora_database_core::Result<WriteOutcome>) -> String {
    match result {
        Ok(outcome) => format!("{:?}", outcome.rows),
        Err(error) => format!("{:?} {}", error.category, error.message),
    }
}

fn refused_with(error: Option<&DatabaseError>, category: ErrorCategory, reason: &str) -> bool {
    error.is_some_and(|error| error.category == category && error.message.ends_with(reason))
}

fn with_policy(mut operation: WriteOperation, policy: MappingPolicy) -> WriteOperation {
    operation.mapping_policy = policy;
    operation
}

/// F1: `public.<>(oid, integer)` che risponde `false`, in uno schema del
/// percorso del ruolo dopo `pg_catalog` (la guardia della regola Q non lo
/// vede), e `DateStyle = DMY`. Su 1b02559 il confronto `typelem <> 0` della
/// regola V saltava il ramo degli array e `['01/02/2026']` verso `date[]` si
/// committava come `{2026-02-01}`, in ogni profilo.
#[tokio::test]
async fn live_q2_operatore_dirottato_non_cambia_lo_sql_della_libreria() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    let hijack = "CREATE TABLE {s}.d (id bigint PRIMARY KEY, at date[]); \
        GRANT ALL ON {s}.d TO {r}; \
        CREATE FUNCTION {s}.falso(oid, integer) RETURNS boolean LANGUAGE sql \
          AS 'SELECT false'; \
        CREATE OPERATOR {s}.<> (LEFTARG = oid, RIGHTARG = integer, PROCEDURE = {s}.falso); \
        ALTER ROLE {r} SET search_path = \"$user\", public, pln_regola_r; \
        ALTER ROLE {r} SET DateStyle = 'ISO, DMY';";
    let input = |item: &str| {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value(item);
        builder.append(true);
        one(
            vec![
                Field::new("id", DataType::Int64, false),
                native(
                    Field::new(
                        "at",
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                        true,
                    ),
                    "date[]",
                ),
            ],
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(builder.finish()) as ArrayRef,
            ],
        )
    };
    let mut difformi = Vec::new();
    for profile in PROFILES {
        prepare(&admin, hijack).await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        // `Lossy`: il preflight ammette il testo verso `date[]` (su 1b02559
        // la scrittura cosi committava).
        let append = with_policy(operation("d", WriteMode::Append), MappingPolicy::Lossy);
        let written = write_stream(&provider, &role_secret, &append, input("01/02/2026")).await;
        let stored = texts(&admin, "SELECT at::text FROM pln_regola_r.d").await;
        if !refused_with(
            written.as_ref().err(),
            ErrorCategory::DataMapping,
            write::canonical_reason(),
        ) || !stored.is_empty()
        {
            difformi.push(format!(
                "{profile:?} non canonico: {} -> {stored:?}",
                rows_of(&written)
            ));
        }
        if profile != PostgresInsertMode::CopyBinary {
            let dry = dry_run_stream(&provider, &role_secret, &append, input("01/02/2026")).await;
            if !refused_with(
                dry.as_ref().err(),
                ErrorCategory::DataMapping,
                write::canonical_reason(),
            ) {
                difformi.push(format!("{profile:?} dry run: {:?}", dry.map(|_| "ok")));
            }
        }
        // `CopyBinary` non codifica un elenco di testo verso `date[]`
        // (`protocol`, anche su 1b02559): la forma canonica si prova sugli
        // altri profili.
        if profile == PostgresInsertMode::CopyBinary {
            continue;
        }
        let written = write_stream(&provider, &role_secret, &append, input("2026-01-02")).await;
        let stored = texts(&admin, "SELECT at::text FROM pln_regola_r.d").await;
        if written.is_err() || stored != ["{2026-01-02}"] {
            difformi.push(format!(
                "{profile:?} canonico: {} -> {stored:?}",
                rows_of(&written)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// F2: una colonna con collation ICU non deterministica e l'indice unico
/// sotto `"C"`. Su 1b02559 Update di `(A,N),(zzz,B)` dava `confirmed 2
/// updated 2`, con `A` e `a` aggiornate e `zzz` assente. Anche un'opclass
/// che non e la predefinita del tipo non prova l'unicita della colonna.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_k2_l_indice_unico_confronta_come_la_colonna() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let collation = "CREATE COLLATION {s}.ci (provider = icu, locale = 'und-u-ks-level2', \
                     deterministic = false);";
    let refusing = [
        (
            "collation-diversa",
            format!(
                "{collation} CREATE TABLE {{s}}.k (id text COLLATE {{s}}.ci, owner text); \
                 CREATE UNIQUE INDEX k_c ON {{s}}.k (id COLLATE \"C\"); \
                 INSERT INTO {{s}}.k VALUES ('A', 'x'), ('a', 'y');"
            ),
        ),
        (
            "opclass-non-predefinita",
            "CREATE TABLE {s}.k (id text, owner text); \
             CREATE UNIQUE INDEX k_p ON {s}.k (id text_pattern_ops); \
             INSERT INTO {s}.k VALUES ('A', 'x'), ('a', 'y');"
                .to_owned(),
        ),
    ];
    let read =
        "SELECT id || ':' || owner FROM pln_regola_r.k ORDER BY (id || ':' || owner) COLLATE \"C\"";
    let mut difformi = Vec::new();
    for (name, setup) in &refusing {
        for mode in [
            WriteMode::Update,
            WriteMode::DeleteByKeys,
            WriteMode::Upsert,
        ] {
            if *name == "opclass-non-predefinita" && mode == WriteMode::Upsert {
                // `text_pattern_ops` confronta come `text_ops`: l'arbitro
                // non cambia il conflitto, e la prova resta su Update e
                // DeleteByKeys.
                continue;
            }
            prepare(&admin, setup).await;
            let before = texts(&admin, read).await;
            let provider = PostgresProvider::insecure_local();
            let operation = operation("k", mode);
            let input = || text_keys(&[("A", "N"), ("zzz", "B")]);
            let dry = dry_run_stream(&provider, &secret, &operation, input()).await;
            let written = write_stream(&provider, &secret, &operation, input()).await;
            let after = texts(&admin, read).await;
            let refused = |error: Option<&DatabaseError>| {
                refused_with(error, ErrorCategory::Schema, KEYS_WITHOUT_UNIQUE_INDEX)
            };
            if !refused(written.as_ref().err()) || !refused(dry.as_ref().err()) || after != before {
                difformi.push(format!(
                    "{name} {mode:?}: dry {:?}, scrittura {}, {before:?} -> {after:?}",
                    dry.map(|_| "ok").map_err(|error| error.message),
                    rows_of(&written)
                ));
            }
        }
    }
    // L'indice con la collation della colonna: `a` trova `A`, come lo
    // statement la confronta; `citext` con la sua opclass predefinita.
    let accepted = [
        (
            "collation-della-colonna",
            format!(
                "{collation} CREATE TABLE {{s}}.k (id text COLLATE {{s}}.ci, owner text); \
                 CREATE UNIQUE INDEX k_ci ON {{s}}.k (id); \
                 INSERT INTO {{s}}.k VALUES ('A', 'x'), ('b', 'y');"
            ),
        ),
        (
            "citext",
            "CREATE EXTENSION IF NOT EXISTS citext; \
             CREATE TABLE {s}.k (id public.citext PRIMARY KEY, owner text); \
             INSERT INTO {s}.k VALUES ('A', 'x'), ('b', 'y');"
                .to_owned(),
        ),
    ];
    for (name, setup) in &accepted {
        for profile in [PostgresInsertMode::Prepared, PostgresInsertMode::CopyText] {
            prepare(&admin, setup).await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let written = write_stream(
                &provider,
                &secret,
                &operation("k", WriteMode::Update),
                text_keys(&[("a", "N"), ("zzz", "B")]),
            )
            .await;
            let after = texts(&admin, read).await;
            let confirmed = written.as_ref().map(|outcome| outcome.rows.confirmed);
            if confirmed.as_ref().ok() != Some(&1) || after != ["A:N", "b:y"] {
                difformi.push(format!(
                    "{name} {profile:?}: {} -> {after:?}",
                    rows_of(&written)
                ));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// F3: `'now'` verso `time` con `Lossy` e `CopyText` si scriveva come l'ora
/// del `TimeZone` della sessione. I valori speciali si rifiutano ovunque, e
/// un tipo fuori dall'elenco chiuso (`xml`, che dipende da `xmloption`) non
/// riceve testo; `citext` legge con `textin` e lo riceve.
#[tokio::test]
async fn live_v2_testo_soltanto_verso_l_elenco_chiuso() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    let refused = [
        ("time", "now"),
        ("time", "allballs"),
        ("date", "today"),
        ("timestamp", "infinity"),
        ("timestamptz", "epoch"),
        ("xml", "<a/>"),
    ];
    for (native, value) in refused {
        for profile in PROFILES {
            prepare(
                &admin,
                &format!("CREATE TABLE {{s}}.h (id bigint PRIMARY KEY, at {native});"),
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let append = with_policy(operation("h", WriteMode::Append), MappingPolicy::Lossy);
            let written = write_stream(&provider, &secret, &append, text_input(1, value)).await;
            let stored = texts(&admin, "SELECT at::text FROM pln_regola_r.h").await;
            if !refused_with(
                written.as_ref().err(),
                ErrorCategory::DataMapping,
                write::canonical_reason(),
            ) || !stored.is_empty()
            {
                difformi.push(format!(
                    "{native} {value} {profile:?}: {} -> {stored:?}",
                    rows_of(&written)
                ));
            }
        }
    }
    for profile in PROFILES {
        prepare(
            &admin,
            "CREATE EXTENSION IF NOT EXISTS citext; \
             CREATE TABLE {s}.h (id bigint PRIMARY KEY, at public.citext);",
        )
        .await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let written = write_stream(
            &provider,
            &secret,
            &operation("h", WriteMode::Append),
            text_input(1, "Testo"),
        )
        .await;
        let stored = texts(&admin, "SELECT at::text FROM pln_regola_r.h").await;
        if written.is_err() || stored != ["Testo"] {
            difformi.push(format!(
                "citext {profile:?}: {} -> {stored:?}",
                rows_of(&written)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// F5: 10:00:00.999999 verso `timestamp(0)` e `time(0)` diventava 10:00:01
/// in ogni profilo, con `Strict`, dove la scala di un `numeric` si rifiuta.
/// Con `Lossy` la perdita e nel `LossReport` (`precision`).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_p_precisione_temporale_ridotta() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let micros = 10 * 3_600 * 1_000_000 + 999_999;
    let input = |native: &str| {
        let field = if native == "time" {
            Field::new("at", DataType::Time64(TimeUnit::Microsecond), true)
        } else {
            Field::new("at", DataType::Timestamp(TimeUnit::Microsecond, None), true)
        };
        let array: ArrayRef = if native == "time" {
            Arc::new(Time64MicrosecondArray::from(vec![micros]))
        } else {
            // 2026-01-02 10:00:00.999999
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_767_312_000_000_000 + micros,
            ]))
        };
        one(
            vec![Field::new("id", DataType::Int64, false), field],
            vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef, array],
        )
    };
    let mut difformi = Vec::new();
    for (native, column) in [("timestamp", "timestamp(0)"), ("time", "time(0)")] {
        for profile in PROFILES {
            prepare(
                &admin,
                &format!("CREATE TABLE {{s}}.p (id bigint PRIMARY KEY, at {column});"),
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let strict = operation("p", WriteMode::Append);
            let written = write_stream(&provider, &secret, &strict, input(native)).await;
            let stored = texts(&admin, "SELECT at::text FROM pln_regola_r.p").await;
            let refused = written
                .as_ref()
                .err()
                .is_some_and(|error| error.category == ErrorCategory::DataMapping);
            if !refused || !stored.is_empty() {
                difformi.push(format!(
                    "{column} Strict {profile:?}: {} -> {stored:?}",
                    rows_of(&written)
                ));
            }
            if profile != PostgresInsertMode::CopyBinary {
                let dry = dry_run_stream(&provider, &secret, &strict, input(native)).await;
                if !dry
                    .as_ref()
                    .err()
                    .is_some_and(|error| error.category == ErrorCategory::DataMapping)
                {
                    difformi.push(format!(
                        "{column} dry run {profile:?}: {:?}",
                        dry.map(|_| "ok")
                    ));
                }
            }
            // Con `Lossy` il valore passa e la perdita e dichiarata.
            let lossy = with_policy(strict, MappingPolicy::Lossy);
            let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
            let prepared = Provider::prepare_write(
                &provider,
                &secret,
                &lossy,
                input(native).schema(),
                &budget,
                &CancellationToken::new(),
            )
            .await
            .expect("preparazione Lossy");
            let declared = prepared
                .loss_report
                .losses
                .iter()
                .any(|loss| loss.category == LossCategory::Precision);
            let written = Provider::write(
                &provider,
                &secret,
                prepared,
                input(native),
                &budget,
                &CancellationToken::new(),
            )
            .await;
            if !declared || written.is_err() {
                difformi.push(format!(
                    "{column} Lossy {profile:?}: dichiarata {declared}, {}",
                    rows_of(&written)
                ));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// `'abc   '` verso `varchar(3)` diventava `'abc'` in ogni profilo: il
/// server tronca in silenzio gli spazi oltre la lunghezza. Con `Strict` si
/// rifiuta; con `Lossy` passa, e la perdita possibile e dichiarata.
#[tokio::test]
async fn live_p_spazi_finali_oltre_la_lunghezza() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    for column in ["varchar(3)", "char(3)"] {
        for profile in PROFILES {
            prepare(
                &admin,
                &format!("CREATE TABLE {{s}}.v (id bigint PRIMARY KEY, at {column});"),
            )
            .await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let strict = operation("v", WriteMode::Append);
            let written = write_stream(&provider, &secret, &strict, text_input(1, "abc   ")).await;
            let stored = texts(&admin, "SELECT at::text FROM pln_regola_r.v").await;
            if !refused_with(
                written.as_ref().err(),
                ErrorCategory::DataMapping,
                write::lossy_reason(),
            ) || !stored.is_empty()
            {
                difformi.push(format!(
                    "{column} Strict {profile:?}: {} -> {stored:?}",
                    rows_of(&written)
                ));
            }
            // Entro la lunghezza il valore si scrive com'e.
            let written = write_stream(&provider, &secret, &strict, text_input(2, "ab ")).await;
            if written.is_err() {
                difformi.push(format!("{column} entro {profile:?}: {}", rows_of(&written)));
            }
            let lossy = with_policy(strict, MappingPolicy::Lossy);
            let written = write_stream(&provider, &secret, &lossy, text_input(3, "abc   ")).await;
            if written.is_err() {
                difformi.push(format!("{column} Lossy {profile:?}: {}", rows_of(&written)));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
