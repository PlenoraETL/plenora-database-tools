//! Le regole Q, V e K contro il fixture PostgreSQL, sugli ingressi della
//! seconda rilettura indipendente del 25/10 (verificati live su 675e322):
//! ciascuna prova qui sotto falliva su quel commit con un `committed`
//! silenzioso, o con un `protocol` dove `COPY` riusciva.

use super::write_visibility_live_tests::{
    dry_run, dry_run_stream, dsn_as_role, ids, live_dsn, operation, prepare, reset, write,
    write_stream, ROLE, SCHEMA, SERIAL,
};
use super::*;
use arrow_array::{
    ArrayRef, BooleanArray, Int64Array, IntervalMonthDayNanoArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Fields, IntervalUnit, Schema};
use plenora_database_core::plan::WriteMode;
use plenora_database_core::provider::{BatchStream, Provider};
use plenora_database_core::write_keys::{DUPLICATE_INPUT_KEYS, KEYS_WITHOUT_UNIQUE_INDEX};
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

/// S1: un ruolo con `session_replication_role = replica` dal ruolo, senza
/// permesso di cambiarlo, e uno schema prima di `pg_catalog` con una
/// `current_setting(text)` che risponde `origin`. Su 675e322: dry run `ok` e
/// l'orfano 99 committato, in ogni profilo.
#[tokio::test]
async fn live_q_search_path_che_oscura_pg_catalog_rifiuta() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    let hijack = "CREATE TABLE {s}.padre (id bigint PRIMARY KEY); \
        INSERT INTO {s}.padre VALUES (1), (2), (3), (10); GRANT SELECT ON {s}.padre TO {r}; \
        ALTER TABLE {s}.t ADD CONSTRAINT verso_padre FOREIGN KEY (id) REFERENCES {s}.padre (id); \
        CREATE FUNCTION {s}.current_setting(text) RETURNS text LANGUAGE sql AS \
        $$ SELECT CASE WHEN $1 = 'session_replication_role' THEN 'origin'::text \
           ELSE pg_catalog.current_setting($1) END $$; \
        GRANT EXECUTE ON FUNCTION {s}.current_setting(text) TO {r}; \
        ALTER ROLE {r} SET session_replication_role = replica; \
        ALTER ROLE {r} SET search_path = pln_regola_r, pg_catalog, public;";
    let mut difformi = Vec::new();
    for profile in PROFILES {
        prepare(&admin, hijack).await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let append = operation("t", WriteMode::Append);
        let values = [(10, "a"), (99, "a")];
        let dry = dry_run(&provider, &role_secret, &append, &values).await;
        let written = write(&provider, &role_secret, &append, &values).await;
        let after = ids(&admin, "t").await;
        let refused = |error: Option<&DatabaseError>| {
            error.is_some_and(|error| {
                error.category == ErrorCategory::Unsupported
                    && error
                        .message
                        .ends_with(write::visibility::SEARCH_PATH_SHADOWS_CATALOG)
            })
        };
        // Il dry run `CopyBinary` non c'e (`unsupported` per costruzione):
        // conta che non dica `ok`.
        let dry_ok = if profile == PostgresInsertMode::CopyBinary {
            dry.is_err()
        } else {
            refused(dry.as_ref().err())
        };
        if !refused(written.as_ref().err()) || after != vec![1, 2, 3] || !dry_ok {
            difformi.push(format!(
                "{profile:?}: dry {:?}, scrittura {}, righe {after:?}",
                dry.map(|_| "ok").map_err(|error| error.message),
                rows_of(&written)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// N3 e la classe: `spatial_ref_sys` senza schema si risolveva dal
/// `search_path`, e lo schema `"$user"` del ruolo precede `public` nel
/// default. Su 675e322 la colonna 4326 si descriveva con l'autorita della
/// tabella del ruolo.
#[tokio::test]
async fn live_q_spatial_ref_sys_dallo_schema_di_postgis() {
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
        "CREATE TABLE {s}.g (id bigint PRIMARY KEY, geom geometry(Point, 4326)); \
         GRANT ALL ON {s}.g TO {r}; \
         CREATE SCHEMA {r} AUTHORIZATION {r}; \
         CREATE TABLE {r}.spatial_ref_sys (srid integer, auth_name varchar(256), \
           auth_srid integer, srtext varchar(2048), proj4text varchar(2048)); \
         INSERT INTO {r}.spatial_ref_sys VALUES (4326, 'FALSO', 4326, 'GEOGCS', '+proj=longlat'); \
         ALTER TABLE {r}.spatial_ref_sys OWNER TO {r};",
    )
    .await;
    let provider = PostgresProvider::insecure_local();
    let inspection = Provider::inspect(
        &provider,
        &role_secret,
        &plenora_database_core::plan::Operation::DatabaseDescribeObject {
            source: ObjectRef {
                catalog: None,
                schema: Some(SCHEMA.to_owned()),
                object: "g".to_owned(),
            },
        },
        &CancellationToken::new(),
    )
    .await
    .expect("descrizione");
    let crs = inspection.document["columns"]
        .as_array()
        .and_then(|columns| columns.iter().find(|column| column["name"] == "geom"))
        .map(|column| column["spatial_crs_id"].clone());
    reset(&admin).await;
    admin
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {ROLE} CASCADE"))
        .await
        .expect("pulizia");
    assert_eq!(crs, Some(serde_json::json!("EPSG:4326")));
}

/// S2: `IntervalStyle = sql_standard` e un `MonthDayNano` di -1 mese, 2
/// giorni, 3 ore. Su 675e322 `CopyText` scriveva -1 mese -2 giorni -3 ore.
#[tokio::test]
async fn live_v_intervallo_indipendente_da_interval_style() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    let mut difformi = Vec::new();
    for style in ["postgres", "sql_standard", "iso_8601"] {
        for profile in PROFILES {
            prepare(
                &admin,
                &format!(
                    "CREATE TABLE {{s}}.iv (id bigint PRIMARY KEY, at interval); \
                     GRANT ALL ON {{s}}.iv TO {{r}}; ALTER ROLE {{r}} SET IntervalStyle = '{style}';"
                ),
            )
            .await;
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("at", DataType::Interval(IntervalUnit::MonthDayNano), true),
            ]));
            let value =
                arrow_array::types::IntervalMonthDayNano::new(-1, 2, 3 * 3_600 * 1_000_000_000);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                    Arc::new(IntervalMonthDayNanoArray::from(vec![value])) as ArrayRef,
                ],
            )
            .expect("batch");
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let written = write_stream(
                &provider,
                &role_secret,
                &operation("iv", WriteMode::Append),
                Box::new(One(schema, Some(batch))),
            )
            .await;
            let stored = texts(
                &admin,
                "SELECT format('%s %s %s', extract(month FROM at), extract(day FROM at), \
                 extract(epoch FROM (at - date_trunc('day', at)))::bigint) FROM pln_regola_r.iv",
            )
            .await;
            if written.is_err() || stored != vec!["-1 2 10800".to_owned()] {
                difformi.push(format!(
                    "{style} {profile:?}: {} -> {stored:?}",
                    rows_of(&written)
                ));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

fn range_field(native: &str) -> Field {
    let fields: Fields = vec![
        Field::new("lower", DataType::Utf8, true),
        Field::new("upper", DataType::Utf8, true),
        Field::new("lower_inclusive", DataType::Boolean, false),
        Field::new("upper_inclusive", DataType::Boolean, false),
        Field::new("lower_unbounded", DataType::Boolean, false),
        Field::new("upper_unbounded", DataType::Boolean, false),
        Field::new("empty", DataType::Boolean, false),
    ]
    .into();
    Field::new("at", DataType::Struct(fields), true).with_metadata(std::collections::HashMap::from(
        [(
            plenora_database_core::protocol::POSTGRES_NATIVE_TYPE.to_owned(),
            native.to_owned(),
        )],
    ))
}

fn range_input(native: &str, lower: &str, upper: &str) -> Box<dyn BatchStream> {
    let field = range_field(native);
    let DataType::Struct(fields) = field.data_type().clone() else {
        unreachable!("campo struct");
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        field,
    ]));
    let range = StructArray::new(
        fields,
        vec![
            Arc::new(StringArray::from(vec![Some(lower)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some(upper)])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
            Arc::new(range) as ArrayRef,
        ],
    )
    .expect("batch");
    Box::new(One(schema, Some(batch)))
}

/// S3: i limiti di un range arrivano come testo libero. Su 675e322 un
/// `tstzrange` senza offset cambiava istante con il `TimeZone` del ruolo, un
/// `daterange` `'01/02/2026'` cambiava giorno con `DateStyle`, e il dry run
/// diceva `ok`.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_v_limiti_di_range_soltanto_canonici() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    let mut difformi = Vec::new();
    // (tipo, limiti non canonici, limiti canonici, impostazione, valore atteso)
    let cases = [
        (
            "tstzrange",
            ("2026-01-02 10:00:00", "2026-01-03 10:00:00"),
            ("2026-01-02 10:00:00+00", "2026-01-03T10:00:00Z"),
            "TimeZone = 'Asia/Tokyo'",
            "SELECT extract(epoch FROM lower(at))::bigint::text FROM pln_regola_r.rg",
            "1767348000",
        ),
        (
            "daterange",
            ("01/02/2026", "01/03/2026"),
            ("2026-01-02", "2026-01-03"),
            "DateStyle = 'ISO, DMY'",
            "SELECT lower(at)::text FROM pln_regola_r.rg",
            "2026-01-02",
        ),
    ];
    for (native, ambiguous, canonical, setting, read, expected) in cases {
        for profile in PROFILES {
            let setup = format!(
                "CREATE TABLE {{s}}.rg (id bigint PRIMARY KEY, at {native}); \
                 GRANT ALL ON {{s}}.rg TO {{r}}; ALTER ROLE {{r}} SET {setting};"
            );
            prepare(&admin, &setup).await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let append = operation("rg", WriteMode::Append);
            let refused = |result: &plenora_database_core::Result<WriteOutcome>| {
                result.as_ref().err().is_some_and(|error| {
                    error.category == ErrorCategory::DataMapping
                        && error.message.ends_with(write::canonical_reason())
                })
            };
            let written = write_stream(
                &provider,
                &role_secret,
                &append,
                range_input(native, ambiguous.0, ambiguous.1),
            )
            .await;
            let stored = texts(&admin, read).await;
            if !refused(&written) || !stored.is_empty() {
                difformi.push(format!(
                    "{native} non canonico {profile:?}: {} -> {stored:?}",
                    rows_of(&written)
                ));
            }
            if profile != PostgresInsertMode::CopyBinary {
                let dry = dry_run_stream(
                    &provider,
                    &role_secret,
                    &append,
                    range_input(native, ambiguous.0, ambiguous.1),
                )
                .await;
                if !dry.as_ref().err().is_some_and(|error| {
                    error.category == ErrorCategory::DataMapping
                        && error.message.ends_with(write::canonical_reason())
                }) {
                    difformi.push(format!("{native} dry run {profile:?}: {dry:?}"));
                }
                // La forma canonica si scrive e non dipende dalla sessione.
                let written = write_stream(
                    &provider,
                    &role_secret,
                    &append,
                    range_input(native, canonical.0, canonical.1),
                )
                .await;
                let stored = texts(&admin, read).await;
                if written.is_err() || stored != vec![expected.to_owned()] {
                    difformi.push(format!(
                        "{native} canonico {profile:?}: {} -> {stored:?}",
                        rows_of(&written)
                    ));
                }
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S5: chiavi senza indice unico. Su 675e322, con `(1,a),(1,b),(2,c)`,
/// Update e `DeleteByKeys` di `(1,z),(99,q)` davano `confirmed 2 skipped 0`
/// con la chiave 99 assente. Anche un indice unico parziale o differibile, o
/// un figlio di ereditarieta, non provano l'unicita.
#[tokio::test]
async fn live_k_update_e_delete_senza_indice_unico_rifiutano() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let setups = [
        (
            "non-unico",
            "DROP TABLE {s}.t; CREATE TABLE {s}.t (id bigint, owner text); \
             INSERT INTO {s}.t VALUES (1, 'a'), (1, 'b'), (2, 'c');",
        ),
        (
            "unico-parziale",
            "DROP TABLE {s}.t; CREATE TABLE {s}.t (id bigint, owner text); \
             CREATE UNIQUE INDEX parziale ON {s}.t (id) WHERE owner <> 'b'; \
             INSERT INTO {s}.t VALUES (1, 'a'), (1, 'b'), (2, 'c');",
        ),
        (
            "unico-differibile",
            "DROP TABLE {s}.t; CREATE TABLE {s}.t (id bigint, owner text, \
             CONSTRAINT differita UNIQUE (id) DEFERRABLE INITIALLY DEFERRED); \
             INSERT INTO {s}.t VALUES (1, 'a'), (2, 'c');",
        ),
        (
            "figlio-di-ereditarieta",
            "CREATE TABLE {s}.figlio () INHERITS ({s}.t); INSERT INTO {s}.figlio VALUES (1, 'b');",
        ),
    ];
    let mut difformi = Vec::new();
    for (name, setup) in setups {
        for mode in [WriteMode::Update, WriteMode::DeleteByKeys] {
            for profile in [PostgresInsertMode::Prepared, PostgresInsertMode::CopyText] {
                prepare(&admin, setup).await;
                let before = texts(
                    &admin,
                    "SELECT id || ':' || owner FROM pln_regola_r.t ORDER BY 1",
                )
                .await;
                let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
                let operation = operation("t", mode);
                let values = [(1, "z"), (99, "q")];
                let dry = dry_run(&provider, &secret, &operation, &values).await;
                let written = write(&provider, &secret, &operation, &values).await;
                let after = texts(
                    &admin,
                    "SELECT id || ':' || owner FROM pln_regola_r.t ORDER BY 1",
                )
                .await;
                let refused = |error: Option<&DatabaseError>| {
                    error.is_some_and(|error| {
                        error.category == ErrorCategory::Schema
                            && error.message.ends_with(KEYS_WITHOUT_UNIQUE_INDEX)
                    })
                };
                if !refused(written.as_ref().err())
                    || !refused(dry.as_ref().err())
                    || after != before
                {
                    difformi.push(format!(
                        "{name} {mode:?} {profile:?}: dry {:?}, scrittura {}, {before:?} -> {after:?}",
                        dry.map(|_| "ok").map_err(|error| error.message),
                        rows_of(&written)
                    ));
                }
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// S7: una chiave ripetuta nell'ingresso. Su 675e322 Update di `(1,x),(1,y)`
/// dava `confirmed 2` con `1:y` (su `MySQL` `confirmed 1 skipped 1` con
/// `1:x`).
#[tokio::test]
async fn live_k_chiavi_ripetute_nell_ingresso_rifiutano() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    for mode in [
        WriteMode::Update,
        WriteMode::DeleteByKeys,
        WriteMode::Upsert,
    ] {
        prepare(&admin, "").await;
        let provider = PostgresProvider::insecure_local();
        let operation = operation("t", mode);
        let values = [(1, "x"), (1, "y")];
        let dry = dry_run(&provider, &secret, &operation, &values).await;
        let written = write(&provider, &secret, &operation, &values).await;
        let after = texts(
            &admin,
            "SELECT id || ':' || owner FROM pln_regola_r.t ORDER BY 1",
        )
        .await;
        let refused = |error: Option<&DatabaseError>| {
            error.is_some_and(|error| {
                error.category == ErrorCategory::DataMapping
                    && error.message.ends_with(DUPLICATE_INPUT_KEYS)
            })
        };
        if !refused(written.as_ref().err())
            || !refused(dry.as_ref().err())
            || after != ["1:a", "2:b", "3:a"]
        {
            difformi.push(format!(
                "{mode:?}: dry {:?}, scrittura {}, righe {after:?}",
                dry.map(|_| "ok").map_err(|error| error.message),
                rows_of(&written)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// N2: il percorso preparato su una colonna con un dominio. Su 675e322 ogni
/// valore, valido o no, usciva `protocol`; `COPY` scriveva quelli validi e
/// rifiutava gli altri con `conflict`.
#[tokio::test]
async fn live_n2_dominio_nel_percorso_preparato_come_copy() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let setup = "CREATE DOMAIN {s}.dck AS text CHECK (VALUE <> 'vietato'); \
                 ALTER TABLE {s}.t ALTER COLUMN owner TYPE {s}.dck;";
    let mut difformi = Vec::new();
    for profile in PROFILES {
        prepare(&admin, setup).await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let append = operation("t", WriteMode::Append);
        let valid = write(&provider, &secret, &append, &[(10, "a")]).await;
        let invalid = write(&provider, &secret, &append, &[(11, "vietato")]).await;
        let update = write(
            &provider,
            &secret,
            &operation("t", WriteMode::Update),
            &[(1, "z")],
        )
        .await;
        let after = texts(
            &admin,
            "SELECT id || ':' || owner FROM pln_regola_r.t ORDER BY 1",
        )
        .await;
        let conflict = invalid
            .as_ref()
            .err()
            .is_some_and(|error| error.category == ErrorCategory::Conflict);
        if valid.is_err() || !conflict || update.is_err() || after != ["10:a", "1:z", "2:b", "3:a"]
        {
            difformi.push(format!(
                "{profile:?}: valido {}, vietato {}, update {}, righe {after:?}",
                rows_of(&valid),
                rows_of(&invalid),
                rows_of(&update)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// L'isolamento chiesto a `begin_transaction` e quello in vigore nella
/// transazione (gia vero prima: il `BEGIN` e un batch semplice; la prova
/// fissa la verifica aggiunta).
#[tokio::test]
async fn live_isolamento_chiesto_e_quello_in_vigore() {
    use plenora_database_core::transaction::{IsolationLevel, TransactionOptions};
    let Some(dsn) = live_dsn() else {
        return;
    };
    let secret = SecretString::new(dsn);
    let provider = PostgresProvider::insecure_local();
    let budget = plenora_database_core::resource::ResourceBudget::new(
        plenora_database_core::resource::ResourceLimits::default(),
    )
    .expect("budget");
    for level in [
        IsolationLevel::ReadCommitted,
        IsolationLevel::RepeatableRead,
        IsolationLevel::Serializable,
    ] {
        let options = TransactionOptions {
            isolation: Some(level),
            ..TransactionOptions::default()
        };
        let transaction = Provider::begin_transaction(
            &provider,
            &secret,
            &options,
            &budget,
            &CancellationToken::new(),
        )
        .await
        .expect("transazione");
        transaction
            .rollback(&CancellationToken::new())
            .await
            .expect("rollback");
    }
}
