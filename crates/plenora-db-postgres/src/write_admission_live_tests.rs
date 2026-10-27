//! La regola A su PostgreSQL, live (`plenora_database_core::write_admission`,
//! `crate::write::admission`).
//!
//! - [`live_sonde_scritture_server`]: la sonda. Misura ogni chiave
//!   d'ammissione di [`specs`] con il percorso di scrittura della libreria, in
//!   ogni percorso ([`PATHS`]: l'INSERT preparato row-scoped, l'INSERT
//!   preparato, `COPY` testo e `COPY` binario), le unisce ([`merge_key`]: una
//!   riscrittura di un percorso vale per tutti), scrive il referto (`PLENORA_SONDE_SCRITTURE_REFERTO`) e
//!   verifica che il server dia ancora il fixture: e l'oracolo di regressione.
//!   La esegue `scripts/sonde_scritture_server.py`.
//!
//!   Limite dichiarato (i percorsi): **regola** una riscrittura misurata in
//!   un percorso vale per ogni percorso; **ambito** i decimali, che il primo
//!   percorso misura per ogni `(p,s)` e gli altri per le precisioni di
//!   [`SAMPLED_PRECISIONS`] (ogni scala compare); **hazard** una riscrittura
//!   di un codec di un altro percorso a una precisione non campionata non si
//!   vedrebbe; **rientro** ogni `(p,s)` in ogni percorso.
//! - I difetti dell'ottava rilettura del 25/10 (su 092c859): `jsonb` che
//!   riscrive la forma dei numeri con `Strict`, `committed` e il `LossReport`
//!   vuoto (`1e2` in `100`, `-0` in `0`, la stessa classe del `JSON` di
//!   Oracle); una coppia di tipi mai misurata (`text` verso `inet`) che si
//!   scriveva; la regola K sotto una collation non deterministica (l'Upsert di
//!   `'a'` e `'A'` su una chiave ICU `und-u-ks-level2` confermava due righe e
//!   ne lasciava una).

use super::write_visibility_live_tests::{dry_run_stream, live_dsn, write_stream, SERIAL};
use super::*;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::outcome::WriteStatus;
use plenora_database_core::plan::{TransactionProfile, WriteMode};
use plenora_database_core::provider::{BatchStream, ConnectionInfo, Inspection, Provider};
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::write_admission::{
    AdmissionKey, MeasuredKey, MeasuredOutcome, MeasuredRewrite, TargetDimensions,
};
use plenora_database_core::{CancellationToken, ErrorCategory, ErrorPhase};
use plenora_database_testkit::write_probe::{self, ProbeHooks, ProbeSpec, TextDomain, PROBE_TABLE};

/// Lo schema della sonda e dei test di questo modulo.
const SCHEMA: &str = "pln_sonda";

/// L'enum della sonda: le etichette sono i testi della batteria che un enum
/// puo avere (1..63 byte, senza NUL). Gli altri testi si rifiutano.
const ENUM_LABELS: &[&str] = &[
    " ",
    "a",
    "a ",
    "a   ",
    " a",
    "a\tb\r\nc",
    "àéîõü",
    "ΩЖł€",
    "😀𝄞",
    "e\u{301}",
    "\u{e9}",
    "\u{feff}a",
    "\u{fffd}",
    "a\u{2028}b",
    "a\\b'c\"d",
];

/// I percorsi di scrittura: (nome, modalita di insert, totale dichiarato).
/// Con il totale dichiarato Append in una transazione prende il percorso
/// row-scoped (un INSERT preparato), senza il percorso della modalita.
const PATHS: [(&str, PostgresInsertMode, bool); 4] = [
    ("row_scoped", PostgresInsertMode::Prepared, true),
    ("prepared", PostgresInsertMode::Prepared, false),
    ("copy_text", PostgresInsertMode::CopyText, false),
    ("copy_binary", PostgresInsertMode::CopyBinary, false),
];

/// Il provider con un percorso: senza il totale dichiarato, se `hide_rows`.
struct PathProvider {
    inner: PostgresProvider,
    hide_rows: bool,
}

struct Undeclared(Box<dyn BatchStream>);

impl BatchStream for Undeclared {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        self.0.next_batch(cancellation)
    }
}

impl Provider for PathProvider {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }

    fn test_connection<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ConnectionInfo> {
        self.inner.test_connection(secret, cancellation)
    }

    fn probe_capabilities<'a>(
        &'a self,
        secret: &'a SecretString,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ProviderCapabilities> {
        self.inner.probe_capabilities(secret, cancellation)
    }

    fn inspect<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a Operation,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Inspection> {
        self.inner.inspect(secret, operation, cancellation)
    }

    fn read<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a ReadOperation,
        parameters: &'a ParameterBag,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn BatchStream>> {
        self.inner
            .read(secret, operation, parameters, budget, cancellation)
    }

    fn prepare_write<'a>(
        &'a self,
        secret: &'a SecretString,
        operation: &'a WriteOperation,
        input_schema: SchemaRef,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, PreparedWrite> {
        self.inner
            .prepare_write(secret, operation, input_schema, budget, cancellation)
    }

    fn write<'a>(
        &'a self,
        secret: &'a SecretString,
        prepared: PreparedWrite,
        input: Box<dyn BatchStream>,
        budget: &'a ResourceBudget,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, plenora_database_core::outcome::WriteOutcome> {
        let input: Box<dyn BatchStream> = if self.hide_rows {
            Box::new(Undeclared(input))
        } else {
            input
        };
        self.inner
            .write(secret, prepared, input, budget, cancellation)
    }

    fn run_ddl<'a>(
        &'a self,
        secret: &'a SecretString,
        sql: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()> {
        plenora_database_core::caller_sql::run_ddl(
            self.kind(),
            cancellation,
            self.inner.run_ddl(secret, sql, cancellation),
        )
    }
}

struct Hooks {
    provider: PathProvider,
    secret: SecretString,
}

impl Hooks {
    fn new(dsn: &str, insert_mode: PostgresInsertMode, declared: bool) -> Self {
        Self {
            provider: PathProvider {
                inner: PostgresProvider::insecure_local().with_insert_mode(insert_mode),
                hide_rows: !declared,
            },
            secret: SecretString::new(dsn.to_owned()),
        }
    }
}

fn quoted_enum_labels() -> String {
    ENUM_LABELS
        .iter()
        .map(|label| format!("'{}'", label.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ")
}

impl ProbeHooks for Hooks {
    fn provider(&self) -> &dyn Provider {
        &self.provider
    }

    fn secret(&self) -> &SecretString {
        &self.secret
    }

    fn schema(&self) -> Option<String> {
        Some(SCHEMA.to_owned())
    }

    fn columns(&self) -> (&'static str, &'static str) {
        ("ID", "V")
    }

    fn create_sql(&self, natives: &[String]) -> Vec<String> {
        let columns = natives
            .iter()
            .enumerate()
            .map(|(index, native)| format!("\"V{index}\" {native}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut statements = vec![format!("CREATE SCHEMA IF NOT EXISTS {SCHEMA}")];
        if natives
            .iter()
            .any(|native| native.contains("pln_sonda_enum"))
        {
            statements.push(format!(
                "DROP TYPE IF EXISTS {SCHEMA}.pln_sonda_enum CASCADE"
            ));
            statements.push(format!(
                "CREATE TYPE {SCHEMA}.pln_sonda_enum AS ENUM ({})",
                quoted_enum_labels()
            ));
        }
        statements.push(format!(
            "CREATE TABLE {SCHEMA}.\"{PROBE_TABLE}\" (\"ID\" bigint PRIMARY KEY, {columns})"
        ));
        statements
    }

    fn drop_sql(&self) -> String {
        format!("DROP TABLE IF EXISTS {SCHEMA}.\"{PROBE_TABLE}\"")
    }

    fn truncate_sql(&self) -> String {
        format!("TRUNCATE TABLE {SCHEMA}.\"{PROBE_TABLE}\"")
    }

    fn admission_keys<'a>(
        &'a self,
        columns: &'a [(String, DataType)],
    ) -> ProviderFuture<'a, Vec<(AdmissionKey, TargetDimensions)>> {
        Box::pin(async move {
            let client = PostgresProvider::connect(&self.secret).await?;
            let schema: SchemaRef = Arc::new(Schema::new(
                columns
                    .iter()
                    .map(|(name, arrow)| Field::new(name, arrow.clone(), true))
                    .collect::<Vec<_>>(),
            ));
            let operation = probe_operation();
            let keys = crate::write::admission::target_keys(&client, &operation, &schema).await?;
            Ok(keys
                .into_iter()
                .map(|key| key.expect("colonna della sonda"))
                .collect())
        })
    }
}

fn probe_operation() -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: PROBE_TABLE.to_owned(),
        },
        mode: WriteMode::Append,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: Vec::new(),
        update_columns: Vec::new(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

/// Le chiavi che PostgreSQL ammette in scrittura: le foglie scalari che la
/// lettura restituisce con lo stesso tipo Arrow (le sole che la sonda, con
/// `Strict`, puo scrivere) e i tipi di Create. Ogni altra coppia resta
/// rifiutata (mondo chiuso).
fn specs() -> Vec<ProbeSpec> {
    specs_with(|_| true)
}

/// Le precisioni dei decimali che i percorsi diversi dal primo misurano: ogni
/// scala da 0 a 38 compare, con le precisioni ai bordi delle cifre di base
/// 10000 di `numeric` e del limite di `decimal128`.
const SAMPLED_PRECISIONS: [u8; 9] = [1, 2, 3, 9, 10, 18, 19, 37, 38];

/// [`specs`] con i decimali soltanto per le precisioni che `precision` ammette.
fn specs_with(precision_filter: fn(u8) -> bool) -> Vec<ProbeSpec> {
    let mut specs = vec![
        ProbeSpec::new("boolean", DataType::Boolean),
        ProbeSpec::new("smallint", DataType::Int32),
        ProbeSpec::new("integer", DataType::Int32),
        ProbeSpec::new("bigint", DataType::Int64),
        ProbeSpec::new("real", DataType::Float32),
        ProbeSpec::new("double precision", DataType::Float64),
    ];
    for precision in (1..=38_u8).filter(|precision| precision_filter(*precision)) {
        for scale in 0..=precision {
            specs.push(ProbeSpec::new(
                format!("numeric({precision},{scale})"),
                DataType::Decimal128(precision, i8::try_from(scale).expect("scala")),
            ));
        }
    }
    // `numeric` senza typmod si legge come `decimal128(38,18)`.
    specs.push(ProbeSpec::new("numeric", DataType::Decimal128(38, 18)));
    for native in [
        "text",
        "character varying",
        "bpchar",
        "pln_sonda.pln_sonda_enum",
    ] {
        specs.push(ProbeSpec::new(native, DataType::Utf8));
    }
    for (native, lengths) in [
        ("character varying({})", &[1_u64, 10, 4000, 10_485_760][..]),
        // `character(n)` oltre 4000 non si misura: la batteria arriva a 4000
        // caratteri, e con `Strict` ogni testo piu corto si rifiuta (regola S).
        ("character({})", &[1, 10, 4000][..]),
    ] {
        for &length in lengths {
            specs.push(
                ProbeSpec::new(native.replace("{}", &length.to_string()), DataType::Utf8)
                    .with_dimension(length),
            );
        }
    }
    specs.push(ProbeSpec::new("json", DataType::Utf8).document());
    specs.push(ProbeSpec::new("jsonb", DataType::Utf8).document());
    specs.push(ProbeSpec::new("uuid", DataType::Utf8).with_text_domain(TextDomain::Uuid));
    specs.push(ProbeSpec::new("bytea", DataType::Binary));
    specs.push(ProbeSpec::new("date", DataType::Date32));
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    specs.push(ProbeSpec::new("timestamp", naive.clone()));
    for precision in 0..=6 {
        specs.push(ProbeSpec::new(
            format!("timestamp({precision})"),
            naive.clone(),
        ));
    }
    specs.push(ProbeSpec::new("timestamptz", utc.clone()));
    for precision in 0..=6 {
        specs.push(ProbeSpec::new(
            format!("timestamptz({precision})"),
            utc.clone(),
        ));
    }
    let time = DataType::Time64(TimeUnit::Microsecond);
    specs.push(ProbeSpec::new("time", time.clone()));
    for precision in 0..=6 {
        specs.push(ProbeSpec::new(format!("time({precision})"), time.clone()));
    }
    let interval = DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano);
    specs.push(ProbeSpec::new("interval", interval.clone()));
    for precision in 0..=6 {
        specs.push(ProbeSpec::new(
            format!("interval({precision})"),
            interval.clone(),
        ));
    }
    specs
}

async fn server_facts(dsn: &str) -> crate::write::admission::ServerFacts {
    let client = PostgresProvider::connect(&SecretString::new(dsn.to_owned()))
        .await
        .expect("client");
    crate::write::admission::server_facts(&client, ErrorPhase::Prepare)
        .await
        .expect("versione del server")
}

/// La misura di una chiave su piu percorsi: riscritta se un percorso la
/// riscrive (le classi di tutti, `ogni` se un percorso non ne ha), esatta se
/// nessun percorso la riscrive e almeno uno la scrive, rifiutata soltanto se
/// ogni percorso la rifiuta. Un valore e rifiutato se ogni percorso lo
/// rifiuta: un percorso che rifiuta un valore che un altro scrive esatto da un
/// errore, non una perdita silenziosa.
fn merge_key(keys: &[&MeasuredKey]) -> MeasuredKey {
    let mut merged = keys[0].clone();
    let any = |outcome| keys.iter().any(|key| key.esito == outcome);
    merged.esito = if any(MeasuredOutcome::Riscrive) {
        MeasuredOutcome::Riscrive
    } else if any(MeasuredOutcome::Illeggibile) {
        MeasuredOutcome::Illeggibile
    } else if any(MeasuredOutcome::Esatto) {
        MeasuredOutcome::Esatto
    } else {
        MeasuredOutcome::Rifiuta
    };
    let union = |field: fn(&MeasuredKey) -> &Vec<String>| {
        let mut out: Vec<String> = Vec::new();
        for key in keys {
            for name in field(key) {
                if !out.contains(name) {
                    out.push(name.clone());
                }
            }
        }
        out
    };
    merged.riscritti = union(|key| &key.riscritti);
    let excess = union(|key| &key.per_eccesso);
    merged.per_eccesso = excess
        .into_iter()
        .filter(|name| !merged.riscritti.contains(name))
        .collect();
    merged.rifiutati = keys[0]
        .rifiutati
        .iter()
        .filter(|name| keys.iter().all(|key| key.rifiutati.contains(name)))
        .cloned()
        .collect();
    merged.riscritture = Vec::new();
    if merged.esito == MeasuredOutcome::Riscrive {
        for key in keys {
            for class in &key.riscritture {
                if !merged.riscritture.contains(class) {
                    merged.riscritture.push(*class);
                }
            }
        }
        if merged.riscritture.contains(&MeasuredRewrite::Ogni) {
            merged.riscritture = vec![MeasuredRewrite::Ogni];
        }
    }
    merged
}

#[tokio::test]
#[ignore = "la sonda della regola A: la esegue scripts/sonde_scritture_server.py"]
async fn live_sonde_scritture_server() {
    let dsn = live_dsn().expect("PLENORA_TEST_POSTGRES_DSN");
    let _serial = SERIAL.lock().await;
    let server = server_facts(&dsn).await;
    let mut measured = Vec::new();
    for (name, insert_mode, declared) in PATHS {
        let specs = if measured.is_empty() {
            specs()
        } else {
            specs_with(|precision| SAMPLED_PRECISIONS.contains(&precision))
        };
        let hooks = Hooks::new(&dsn, insert_mode, declared);
        let path =
            write_probe::measure_server(&hooks, &specs, &server.major, &server.version).await;
        if let Some((first_name, first)) = measured.first() {
            for difference in write_probe::differences(first, &path)
                .into_iter()
                .filter(|difference| !difference.starts_with("assente nella misura"))
            {
                eprintln!("percorso {name} rispetto a {first_name}: {difference}");
            }
        }
        measured.push((name, path));
    }
    let (_, first) = &measured[0];
    let mut merged = first.clone();
    merged.chiavi = first
        .chiavi
        .iter()
        .map(|key| {
            let same = measured
                .iter()
                .filter_map(|(_, path)| {
                    path.chiavi.iter().find(|other| {
                        other.arrow == key.arrow
                            && other.nativo == key.nativo
                            && other.dimensione == key.dimensione
                    })
                })
                .collect::<Vec<_>>();
            merge_key(&same)
        })
        .collect();
    write_probe::report_and_check(&merged, crate::write::admission::FIXTURE);
}

fn unproven<T>(result: &plenora_database_core::Result<T>) -> bool {
    matches!(result, Err(error) if error.category == ErrorCategory::Unsupported
        && error.phase == ErrorPhase::Prepare
        && error.message.contains(plenora_database_core::write_admission::UNPROVEN_PAIR))
}

fn nondeterministic<T>(result: &plenora_database_core::Result<T>) -> bool {
    matches!(result, Err(error) if error.category == ErrorCategory::Unsupported
        && error.message.contains(crate::write::visibility::NONDETERMINISTIC_KEY))
}

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

/// `(id bigint, v text)`, con `v` non nullable (una chiave NOT NULL lo
/// vuole).
fn texts(values: &[&str]) -> Box<dyn BatchStream> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, false),
    ]));
    let ids = (0..i64::try_from(values.len()).expect("righe")).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
        ],
    )
    .expect("batch");
    Box::new(One(schema, Some(batch)))
}

fn operation(object: &str, mode: WriteMode, policy: MappingPolicy) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: object.to_owned(),
        },
        mode,
        mapping_policy: policy,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: if mode == WriteMode::Upsert {
            vec!["v".to_owned()]
        } else {
            Vec::new()
        },
        update_columns: if mode == WriteMode::Upsert {
            vec!["id".to_owned()]
        } else {
            Vec::new()
        },
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

async fn setup(admin: &tokio_postgres::Client, sql: &str) {
    admin
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA}; {}",
            sql.replace("{s}", SCHEMA)
        ))
        .await
        .expect("preparazione");
}

async fn stored(admin: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    admin
        .query(&sql.replace("{s}", SCHEMA), &[])
        .await
        .expect("lettura")
        .iter()
        .map(|row| row.get::<_, String>(0))
        .collect()
}

const PROFILES: [PostgresInsertMode; 3] = [
    PostgresInsertMode::Prepared,
    PostgresInsertMode::CopyText,
    PostgresInsertMode::CopyBinary,
];

/// `jsonb` riscrive la forma dei numeri: su 092c859 `Strict` committava
/// `{"a":1e2}` come `{"a": 100}` e `{"a":-0}` come `{"a": 0}` in ogni
/// profilo, con il `LossReport` vuoto («semantica del tipo»). Ora la classe
/// `json_numeri` della misura: con `Strict` si rifiuta, con `Compatible` la
/// perdita e nel `LossReport`.
#[tokio::test]
async fn live_a_jsonb_non_riscrive_i_numeri_in_silenzio() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    for profile in PROFILES {
        setup(
            &admin,
            "CREATE TABLE {s}.j (id bigint PRIMARY KEY, v jsonb);",
        )
        .await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let strict = operation("j", WriteMode::Append, MappingPolicy::Strict);
        for document in [r#"{"a":1e2}"#, r#"{"a":-0}"#, r#"{"a":1e400}"#] {
            let written = write_stream(&provider, &secret, &strict, texts(&[document])).await;
            let refused = written.as_ref().is_err_and(|error| {
                error.category == ErrorCategory::DataMapping
                    && error
                        .message
                        .contains(plenora_database_core::store_rewrite::REWRITTEN_VALUE)
            });
            if !refused {
                difformi.push(format!("{profile:?} Strict {document}: {written:?}"));
            }
        }
        let rows = stored(&admin, "SELECT v::text FROM {s}.j").await;
        if !rows.is_empty() {
            difformi.push(format!("{profile:?}: righe scritte {rows:?}"));
        }
        // Un intero breve si rilegge uguale.
        let written = write_stream(&provider, &secret, &strict, texts(&[r#"{"a":1}"#])).await;
        if !written
            .as_ref()
            .is_ok_and(|outcome| outcome.status == WriteStatus::Committed)
        {
            difformi.push(format!("{profile:?} Strict intero breve: {written:?}"));
        }
        // Con `Compatible` la perdita si dichiara, e il valore passa.
        let compatible = operation("j", WriteMode::Append, MappingPolicy::Compatible);
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let stream = texts(&[r#"{"a":1e2}"#]);
        let prepared = Provider::prepare_write(
            &provider,
            &secret,
            &compatible,
            stream.schema(),
            &budget,
            &CancellationToken::new(),
        )
        .await
        .expect("preparazione Compatible");
        let declared = prepared.loss_report.losses.iter().any(|loss| {
            loss.reason == plenora_database_core::store_rewrite::StoreRewrite::JsonNumbers.reason()
        });
        if !declared {
            difformi.push(format!(
                "{profile:?} Compatible: perdita non dichiarata {:?}",
                prepared.loss_report
            ));
        }
    }
    assert!(difformi.is_empty(), "{difformi:#?}");
}

/// Una coppia mai misurata (`text` verso `inet`) si rifiuta nel preflight
/// con ogni politica, e il dry run con la DSN lo decide allo stesso modo. Su
/// 092c859 `Compatible` la scriveva (`'10.1.2.3/32'` riletto `'10.1.2.3'`).
#[tokio::test]
async fn live_a_una_coppia_non_misurata_si_rifiuta_nel_preflight() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    setup(
        &admin,
        "CREATE TABLE {s}.n (id bigint PRIMARY KEY, v inet);",
    )
    .await;
    let provider = PostgresProvider::insecure_local();
    let mut difformi = Vec::new();
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
    ] {
        let target = operation("n", WriteMode::Append, policy);
        let written = write_stream(&provider, &secret, &target, texts(&["10.1.2.3/32"])).await;
        if !unproven(&written) {
            difformi.push(format!("{policy:?} scrittura: {written:?}"));
        }
        let decided = dry_run_stream(&provider, &secret, &target, texts(&["10.1.2.3/32"])).await;
        if !unproven(&decided) {
            difformi.push(format!("{policy:?} dry run: {decided:?}"));
        }
    }
    let rows = stored(&admin, "SELECT v::text FROM {s}.n").await;
    assert!(rows.is_empty(), "righe scritte: {rows:?}");
    assert!(difformi.is_empty(), "{difformi:#?}");
}

/// La regola K sotto una collation non deterministica: su 092c859 l'Upsert
/// `Strict` di `'a'` e `'A'` su una chiave `COLLATE` ICU `und-u-ks-level2`
/// (`deterministic = false`) dava `committed` con `confirmed 2` e una riga
/// sola nel target. Ora si rifiuta prima di ogni effetto, in ogni profilo e
/// nel dry run.
#[tokio::test]
async fn live_k_chiave_con_collation_non_deterministica_si_rifiuta() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    for profile in PROFILES {
        setup(
            &admin,
            "CREATE COLLATION {s}.ci (provider = icu, locale = 'und-u-ks-level2', \
             deterministic = false); \
             CREATE TABLE {s}.k (id bigint, v text COLLATE {s}.ci PRIMARY KEY);",
        )
        .await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let upsert = operation("k", WriteMode::Upsert, MappingPolicy::Strict);
        let written = write_stream(&provider, &secret, &upsert, texts(&["a", "A"])).await;
        if !nondeterministic(&written) {
            difformi.push(format!("{profile:?} scrittura: {written:?}"));
        }
        let decided = dry_run_stream(&provider, &secret, &upsert, texts(&["a", "A"])).await;
        if !nondeterministic(&decided) {
            difformi.push(format!("{profile:?} dry run: {decided:?}"));
        }
        let rows = stored(&admin, "SELECT v FROM {s}.k ORDER BY id").await;
        if !rows.is_empty() {
            difformi.push(format!("{profile:?}: righe scritte {rows:?}"));
        }
    }
    // Una chiave sotto una collation deterministica resta ammessa.
    setup(
        &admin,
        "CREATE TABLE {s}.k (id bigint, v text COLLATE \"C\" PRIMARY KEY);",
    )
    .await;
    let provider = PostgresProvider::insecure_local();
    let upsert = operation("k", WriteMode::Upsert, MappingPolicy::Strict);
    let written = write_stream(&provider, &secret, &upsert, texts(&["a", "A"])).await;
    if !written
        .as_ref()
        .is_ok_and(|outcome| outcome.status == WriteStatus::Committed)
    {
        difformi.push(format!("collation C: {written:?}"));
    }
    assert!(difformi.is_empty(), "{difformi:#?}");
}

async fn read_all(
    provider: &PostgresProvider,
    secret: &SecretString,
    object: &str,
    projection: &[&str],
) -> plenora_database_core::Result<Vec<RecordBatch>> {
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let cancellation = CancellationToken::new();
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some(SCHEMA.to_owned()),
            object: object.to_owned(),
        },
        projection: projection.iter().map(|name| (*name).to_owned()).collect(),
        order_by: vec![plenora_database_core::plan::OrderBy {
            field: "id".to_owned(),
            direction: plenora_database_core::plan::SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let mut stream = Provider::read(
        provider,
        secret,
        &operation,
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

/// La lettura di `smallint` e `smallint[]`: il codec di `i32` accetta
/// soltanto `int4`, e su 092c859 ogni lettura di una colonna `smallint`
/// falliva con `data_mapping` (la sonda della regola A la misurava
/// illeggibile). Ora `int2` si legge come `i16` e si allarga a `int32`.
#[tokio::test]
async fn live_lettura_di_smallint_e_smallint_array() {
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_array::Array;
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    setup(
        &admin,
        "CREATE TABLE {s}.s (id bigint PRIMARY KEY, v smallint, a smallint[]); \
         INSERT INTO {s}.s VALUES (1, 0, '{0,NULL}'), (2, -1, '{-1}'), (3, -32768, '{-32768}'), \
         (4, 32767, '{32767}'), (5, NULL, NULL);",
    )
    .await;
    let provider = PostgresProvider::insecure_local();
    let batches = read_all(&provider, &secret, "s", &["id", "v", "a"])
        .await
        .expect("lettura di smallint");
    let mut values = Vec::new();
    let mut items = Vec::new();
    for batch in &batches {
        assert_eq!(batch.column(1).data_type(), &DataType::Int32);
        let column = batch.column(1).as_primitive::<Int32Type>();
        let lists = batch.column(2).as_list::<i32>();
        for row in 0..batch.num_rows() {
            values.push((!column.is_null(row)).then(|| column.value(row)));
            items.push((!lists.is_null(row)).then(|| {
                let list = lists.value(row);
                let list = list.as_primitive::<Int32Type>();
                (0..list.len())
                    .map(|index| (!list.is_null(index)).then(|| list.value(index)))
                    .collect::<Vec<_>>()
            }));
        }
    }
    assert_eq!(
        values,
        vec![Some(0), Some(-1), Some(-32_768), Some(32_767), None]
    );
    assert_eq!(
        items,
        vec![
            Some(vec![Some(0), None]),
            Some(vec![Some(-1)]),
            Some(vec![Some(-32_768)]),
            Some(vec![Some(32_767)]),
            None
        ]
    );
}

/// La stessa classe nel percorso delle query: le colonne che la query
/// restituisce si leggono con il tipo del risultato (`from_statement_column`),
/// senza la proiezione in testo della lettura di una tabella. Ogni tipo
/// mappato si rilegge: `smallint`, `smallint[]`, `json`, `jsonb`, `uuid`,
/// `name`, `bpchar`, `varchar[]`.
#[tokio::test]
async fn live_la_query_rilegge_ogni_tipo_mappato() {
    use plenora_database_core::relational::{
        ColumnRef, QueryExpression, QueryOperation, QueryProjection, QuerySource,
    };
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    setup(
        &admin,
        "CREATE TABLE {s}.q (id bigint PRIMARY KEY, s smallint, sa smallint[], j json, \
         b jsonb, u uuid, n name, c char(3), va varchar[]); \
         INSERT INTO {s}.q VALUES (1, -2, '{3}', '{\"a\":1}', '{\"a\":1}', \
         '6f9619ff-8b86-d011-b42d-00c04fc964ff', 'nome', 'ab', '{x}');",
    )
    .await;
    let provider = PostgresProvider::insecure_local();
    let mut difformi = Vec::new();
    for column in ["s", "sa", "j", "b", "u", "n", "c", "va"] {
        let query = QueryOperation {
            declared_crs: Vec::new(),
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some(SCHEMA.to_owned()),
                    object: "q".to_owned(),
                },
                alias: None,
            }),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: column.to_owned(),
                    },
                },
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(1),
            row_offset: None,
            locking: None,
        };
        let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
        let cancellation = CancellationToken::new();
        let read = async {
            let mut stream = Provider::query(
                &provider,
                &secret,
                &query,
                &ParameterBag::default(),
                &budget,
                &cancellation,
            )
            .await?;
            let mut rows = 0;
            let mut text = None;
            while let Some(batch) = stream.next_batch(&cancellation).await? {
                rows += batch.num_rows();
                if let Some(strings) = batch.column(0).as_any().downcast_ref::<StringArray>() {
                    text = Some(strings.value(0).to_owned());
                }
            }
            Ok::<_, plenora_database_core::DatabaseError>((rows, text))
        }
        .await;
        // Il testo e quello di `::text` sul server; `char(n)` con il
        // riempimento, come lo rilegge la lettura di una tabella (`::text`
        // lo toglie).
        let expected = if column == "c" {
            vec!["ab ".to_owned()]
        } else {
            stored(&admin, &format!("SELECT {column}::text FROM {{s}}.q")).await
        };
        let same = match &read {
            Ok((1, Some(text))) => expected == [text.clone()],
            Ok((1, None)) => true,
            _ => false,
        };
        if !same {
            difformi.push(format!("{column}: {read:?} invece di {expected:?}"));
        }
    }
    assert!(difformi.is_empty(), "{difformi:#?}");
}

/// UUID equivalenti non possono confermare due righe lasciandone una.
#[tokio::test]
async fn live_s_uuid_alias_non_sono_chiavi_distinte() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    for alias in [
        "6F9619FF-8B86-D011-B42D-00C04FC964FF",
        "6f9619ff8b86d011b42d00c04fc964ff",
    ] {
        setup(
            &admin,
            "CREATE TABLE {s}.k (id bigint, v uuid PRIMARY KEY);",
        )
        .await;
        let provider = PostgresProvider::insecure_local();
        let upsert = operation("k", WriteMode::Upsert, MappingPolicy::Strict);
        let result = write_stream(
            &provider,
            &secret,
            &upsert,
            texts(&["6f9619ff-8b86-d011-b42d-00c04fc964ff", alias]),
        )
        .await;
        let error = result.expect_err("alias della chiave");
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert!(error
            .message
            .contains(plenora_database_core::store_rewrite::REWRITTEN_KEY));
        assert!(stored(&admin, "SELECT v::text FROM {s}.k").await.is_empty());
        let canonical = "6f9619ff-8b86-d011-b42d-00c04fc964ff";
        let result = write_stream(&provider, &secret, &upsert, texts(&[canonical]))
            .await
            .expect("UUID canonico");
        assert_eq!(result.status, WriteStatus::Committed);
        assert_eq!(
            stored(&admin, "SELECT v::text FROM {s}.k").await,
            vec![canonical]
        );
    }
}
