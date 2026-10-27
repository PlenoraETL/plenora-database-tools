//! La regola A (`plenora_database_core::write_admission`) e la regola K sotto
//! la collation su SQL Server, live.
//!
//! - [`live_sonde_scritture_server`]: la sonda. Misura ogni chiave
//!   d'ammissione di [`specs`] con il percorso di scrittura della libreria,
//!   scrive il referto (`PLENORA_SONDE_SCRITTURE_REFERTO`) e verifica che il
//!   server dia ancora il fixture: e l'oracolo di regressione. La esegue
//!   `scripts/sonde_scritture_server.py`, e gira con la suite live di ogni
//!   server della matrice.
//! - I difetti dell'ottava rilettura del 25/10, ciascuno misurato su 092c859
//!   (tutti `Strict` e `committed`).

use super::*;
use crate::{SqlServerProvider, SqlServerSession};
use plenora_database_core::arrow::array::{BinaryArray, Decimal128Array};
use plenora_database_core::arrow::schema::TimeUnit;
use plenora_database_core::outcome::WriteStatus;
use plenora_database_core::provider::Provider;
use plenora_database_core::write_admission::{AdmissionKey, TargetDimensions};
use plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS;
use plenora_database_core::RemoteEffect;
use plenora_database_testkit::write_probe::{self, ProbeHooks, ProbeSpec, TextDomain, PROBE_TABLE};

/// La collation del container (code page 1252) e una `_UTF8` (65001): le
/// due code page misurate. Ogni altra code page resta rifiutata.
const CODE_PAGES: [&str; 2] = [
    "SQL_Latin1_General_CP1_CI_AS",
    "Latin1_General_100_CI_AS_SC_UTF8",
];

/// Il prefisso di un tipo che la sonda crea con `ANSI_PADDING OFF`.
const PADDING_OFF: &str = "ANSI_PADDING_OFF ";

struct Hooks {
    provider: SqlServerProvider,
    secret: SecretString,
}

impl Hooks {
    fn new() -> Self {
        let password = std::env::var("PLENORA_SQLSERVER_PASSWORD")
            .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
        Self {
            provider: SqlServerProvider::new(live_config(), 2, 3).expect("provider SQL Server"),
            secret: SecretString::new(password),
        }
    }
}

impl ProbeHooks for Hooks {
    fn provider(&self) -> &dyn Provider {
        &self.provider
    }

    fn secret(&self) -> &SecretString {
        &self.secret
    }

    fn schema(&self) -> Option<String> {
        Some("dbo".to_owned())
    }

    fn columns(&self) -> (&'static str, &'static str) {
        ("ID", "V")
    }

    fn create_sql(&self, natives: &[String]) -> Vec<String> {
        let mut statements = vec![format!(
            "CREATE TABLE [dbo].[{PROBE_TABLE}] ([ID] bigint NOT NULL PRIMARY KEY);"
        )];
        for (index, native) in natives.iter().enumerate() {
            // `SET` dentro lo stesso batch (`sp_executesql`) vale fino alla
            // fine del batch: la colonna nasce con `is_ansi_padded = 0`.
            statements.push(native.strip_prefix(PADDING_OFF).map_or_else(
                || format!("ALTER TABLE [dbo].[{PROBE_TABLE}] ADD [V{index}] {native};"),
                |native| {
                    format!(
                        "SET ANSI_PADDING OFF; ALTER TABLE [dbo].[{PROBE_TABLE}] \
                         ADD [V{index}] {native}; SET ANSI_PADDING ON;"
                    )
                },
            ));
        }
        statements
    }

    fn drop_sql(&self) -> String {
        format!("DROP TABLE IF EXISTS [dbo].[{PROBE_TABLE}];")
    }

    fn truncate_sql(&self) -> String {
        format!("TRUNCATE TABLE [dbo].[{PROBE_TABLE}];")
    }

    fn admission_keys<'a>(
        &'a self,
        columns: &'a [(String, DataType)],
    ) -> ProviderFuture<'a, Vec<(AdmissionKey, TargetDimensions)>> {
        Box::pin(async move {
            let cancellation = CancellationToken::new();
            let mut session = SqlServerSession::open(&live_config(), &cancellation).await?;
            let facts =
                crate::catalog::load_write_facts(&mut session, "dbo", PROBE_TABLE, &cancellation)
                    .await?;
            Ok(columns
                .iter()
                .map(|(name, arrow)| {
                    let fact = facts
                        .iter()
                        .find(|fact| &fact.name == name)
                        .expect("colonna della sonda");
                    crate::write::admission::admission_key(arrow, fact)
                })
                .collect())
        })
    }
}

/// Le chiavi che SQL Server ammette in scrittura: ogni coppia che
/// `validate_arrow_type` lascia passare, con i parametri enumerati dove il
/// dominio e finito e le lunghezze su rappresentanti. Ogni altra coppia resta
/// rifiutata (mondo chiuso). L'ordine separa i tipi a lunghezza fissa di 8000
/// byte, che in una tabella sola supererebbero la riga.
#[allow(clippy::too_many_lines)]
fn specs() -> Vec<ProbeSpec> {
    let mut specs = vec![
        ProbeSpec::new("bit NULL", DataType::Boolean),
        ProbeSpec::new("tinyint NULL", DataType::UInt8),
        ProbeSpec::new("smallint NULL", DataType::Int16),
        ProbeSpec::new("int NULL", DataType::Int32),
        ProbeSpec::new("bigint NULL", DataType::Int64),
        ProbeSpec::new("real NULL", DataType::Float32),
        ProbeSpec::new("float NULL", DataType::Float64),
        ProbeSpec::new("money NULL", DataType::Decimal128(19, 4)),
        ProbeSpec::new("smallmoney NULL", DataType::Decimal128(10, 4)),
        ProbeSpec::new("date NULL", DataType::Date32),
        ProbeSpec::new(
            "datetime NULL",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        ProbeSpec::new(
            "smalldatetime NULL",
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
    ];
    for scale in 0..=7 {
        specs.push(ProbeSpec::new(
            format!("datetime2({scale}) NULL"),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ));
    }
    for scale in 0..=7 {
        specs.push(ProbeSpec::new(
            format!("time({scale}) NULL"),
            DataType::Time64(TimeUnit::Microsecond),
        ));
    }
    for scale in 0..=7 {
        specs.push(
            ProbeSpec::new(format!("datetimeoffset({scale}) NULL"), DataType::Utf8)
                .with_text_domain(TextDomain::OffsetInstant),
        );
    }
    specs.push(
        ProbeSpec::new("uniqueidentifier NULL", DataType::Utf8).with_text_domain(TextDomain::Uuid),
    );
    for native in ["decimal", "numeric"] {
        for precision in 1..=38_u8 {
            for scale in 0..=precision {
                specs.push(ProbeSpec::new(
                    format!("{native}({precision},{scale}) NULL"),
                    DataType::Decimal128(precision, i8::try_from(scale).expect("scala")),
                ));
            }
        }
    }
    specs.push(ProbeSpec::new("xml NULL", DataType::Utf8));
    for null in ["NULL", "NOT NULL"] {
        for padding in ["", PADDING_OFF] {
            for collation in CODE_PAGES {
                for (native, lengths) in [("char", [1_u64, 10, 8000]), ("varchar", [1, 10, 8000])] {
                    for length in lengths {
                        specs.push(
                            ProbeSpec::new(
                                format!("{padding}{native}({length}) COLLATE {collation} {null}"),
                                DataType::Utf8,
                            )
                            .with_dimension(length),
                        );
                    }
                }
                specs.push(ProbeSpec::new(
                    format!("{padding}varchar(max) COLLATE {collation} {null}"),
                    DataType::Utf8,
                ));
            }
            for (native, lengths) in [("binary", [1_u64, 10, 8000]), ("varbinary", [1, 10, 8000])] {
                for length in lengths {
                    specs.push(
                        ProbeSpec::new(
                            format!("{padding}{native}({length}) {null}"),
                            DataType::Binary,
                        )
                        .with_dimension(length),
                    );
                }
            }
            specs.push(ProbeSpec::new(
                format!("{padding}varbinary(max) {null}"),
                DataType::Binary,
            ));
        }
        // I tipi LOB legacy nascono sempre con `is_ansi_padded = 0`, e non
        // ammettono le collation `_UTF8`: una chiave sola (misurato il
        // 25/10).
        specs.push(ProbeSpec::new(
            format!("text COLLATE {} {null}", CODE_PAGES[0]),
            DataType::Utf8,
        ));
        specs.push(ProbeSpec::new(format!("ntext {null}"), DataType::Utf8));
        specs.push(ProbeSpec::new(format!("image {null}"), DataType::Binary));
        // `nchar` e `nvarchar` nascono con `is_ansi_padded = 1`
        // anche sotto `ANSI_PADDING OFF` (misurato il 25/10): una chiave
        // sola.
        for (native, lengths) in [("nchar", [1_u64, 10, 4000]), ("nvarchar", [1, 10, 4000])] {
            for length in lengths {
                specs.push(
                    ProbeSpec::new(format!("{native}({length}) {null}"), DataType::Utf8)
                        .with_dimension(length),
                );
            }
        }
        specs.push(ProbeSpec::new(
            format!("nvarchar(max) {null}"),
            DataType::Utf8,
        ));
    }
    specs
}

#[tokio::test]
#[ignore = "richiede SQL Server live esplicito"]
async fn live_sonde_scritture_server() {
    let _serial = SERIAL.lock().await;
    let hooks = Hooks::new();
    let cancellation = CancellationToken::new();
    let mut session = SqlServerSession::open(&live_config(), &cancellation)
        .await
        .expect("sessione");
    let server = crate::write::admission::server_facts(&mut session, &cancellation)
        .await
        .expect("versione del server");
    drop(session);
    let measured =
        write_probe::measure_server(&hooks, &specs(), &server.major, &server.version).await;
    write_probe::report_and_check(&measured, crate::write::admission::FIXTURE);
}

fn keyed(object: &str, mode: WriteMode, keys: &[&str], update: &[&str]) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: object.to_owned(),
        },
        mode,
        mapping_policy: MappingPolicy::Strict,
        transaction_profile: TransactionProfile::SingleTransaction,
        keys: keys.iter().map(|key| (*key).to_owned()).collect(),
        update_columns: update.iter().map(|name| (*name).to_owned()).collect(),
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}

/// Un ingresso di colonne di testo, tutte non nullable.
fn text_rows(columns: &[(&str, &[&str])]) -> Rows {
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, _)| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let arrays = columns
        .iter()
        .map(|(_, values)| Arc::new(StringArray::from(values.to_vec())) as Arc<dyn Array>)
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).expect("batch");
    Rows {
        schema,
        batch: Some(batch),
    }
}

async fn run_r8(
    pool: &Arc<SqlServerPool>,
    operation: &WriteOperation,
    input: Rows,
) -> plenora_database_core::Result<WriteOutcome> {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let prepared = prepare_write_with_mode(
        pool,
        operation,
        input.schema(),
        &budget,
        &cancellation,
        SqlServerInsertMode::Prepared,
    )
    .await?;
    write_prepared(prepared, Box::new(input), &cancellation).await
}

async fn admin_texts(admin: &mut SqlServerSession, sql: &str) -> Vec<String> {
    let mut results = admin
        .execute_query(
            Query::new(sql.to_owned()),
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

async fn admin_batch(admin: &mut SqlServerSession, sql: &str) {
    admin
        .execute_query(
            Query::new(sql.to_owned()),
            ErrorPhase::Probe,
            &CancellationToken::new(),
        )
        .await
        .expect("preparazione");
}

fn duplicate_rolled_back(result: &plenora_database_core::Result<WriteOutcome>) {
    let error = result
        .as_ref()
        .expect_err("due chiavi uguali per l'indice del target si rifiutano");
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert!(error.message.contains(DUPLICATE_INPUT_KEYS), "{error:?}");
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack, "{error:?}");
}

/// La regola K sotto la collation: su 092c859 l'Upsert `[('a','x'),('A','y')]`
/// su una chiave `nvarchar` con `SQL_Latin1_General_CP1_CI_AS` dava
/// `committed`, `confirmed 2` e una riga sola; Update e `DeleteByKeys`
/// contavano ciascuna riga. Due testi di `datetimeoffset` con offset diversi
/// per lo stesso istante sono la stessa voce dell'indice.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_k_chiavi_uguali_sotto_la_collation_si_rifiutano() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    let reset = "DROP TABLE IF EXISTS [plenora_test].[r8_collation], [plenora_test].[r8_offset]; \
         CREATE TABLE [plenora_test].[r8_collation] ([k] nvarchar(10) \
           COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL PRIMARY KEY, [v] nvarchar(10) NULL); \
         CREATE TABLE [plenora_test].[r8_offset] ([k] datetimeoffset(7) NOT NULL PRIMARY KEY, \
           [v] nvarchar(10) NULL);";
    admin_batch(&mut admin, reset).await;
    let rows = "SELECT CONCAT([k], N'=', [v]) FROM [plenora_test].[r8_collation] ORDER BY [k]";

    let upsert = keyed("r8_collation", WriteMode::Upsert, &["k"], &[]);
    duplicate_rolled_back(
        &run_r8(
            &pool,
            &upsert,
            text_rows(&[("k", &["a", "A"]), ("v", &["x", "y"])]),
        )
        .await,
    );
    assert!(
        admin_texts(&mut admin, rows).await.is_empty(),
        "nessun effetto"
    );

    // Il controllo: chiavi distinte anche per la collation si scrivono.
    let outcome = run_r8(
        &pool,
        &upsert,
        text_rows(&[("k", &["a", "b"]), ("v", &["x", "y"])]),
    )
    .await
    .expect("chiavi distinte");
    assert_eq!(outcome.status, WriteStatus::Committed);
    assert_eq!(outcome.rows.confirmed, 2);

    let update = keyed("r8_collation", WriteMode::Update, &["k"], &["v"]);
    duplicate_rolled_back(
        &run_r8(
            &pool,
            &update,
            text_rows(&[("k", &["a", "A"]), ("v", &["u", "w"])]),
        )
        .await,
    );
    let delete = keyed("r8_collation", WriteMode::DeleteByKeys, &["k"], &[]);
    duplicate_rolled_back(&run_r8(&pool, &delete, text_rows(&[("k", &["b", "B"])])).await);
    assert_eq!(admin_texts(&mut admin, rows).await, ["a=x", "b=y"]);

    let offset = keyed("r8_offset", WriteMode::Upsert, &["k"], &[]);
    duplicate_rolled_back(
        &run_r8(
            &pool,
            &offset,
            text_rows(&[
                (
                    "k",
                    &["2026-01-01T10:00:00+02:00", "2026-01-01T08:00:00+00:00"],
                ),
                ("v", &["x", "y"]),
            ]),
        )
        .await,
    );
    assert!(admin_texts(
        &mut admin,
        "SELECT CONVERT(nvarchar(40), [k], 126) FROM [plenora_test].[r8_offset]"
    )
    .await
    .is_empty());
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_collation], [plenora_test].[r8_offset];",
    )
    .await;
}

/// La lettura di un `datetimeoffset` conserva l'offset: su 092c859
/// `2026-01-01T10:00:00+02:00` si rileggeva `2026-01-01T08:00:00Z` (stile
/// 127 di `CONVERT`).
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_a_datetimeoffset_si_rilegge_con_il_suo_offset() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_dto]; \
         CREATE TABLE [plenora_test].[r8_dto] ([k] nvarchar(10) NOT NULL PRIMARY KEY, \
           [at] datetimeoffset(7) NOT NULL);",
    )
    .await;
    let append = keyed("r8_dto", WriteMode::Append, &[], &[]);
    run_r8(
        &pool,
        &append,
        text_rows(&[
            ("k", &["a", "b"]),
            (
                "at",
                &["2026-01-01T10:00:00+02:00", "2026-01-01T10:00:00.5-05:30"],
            ),
        ]),
    )
    .await
    .expect("append");
    let provider = SqlServerProvider::new(live_config(), 2, 3).expect("provider");
    let secret = SecretString::new(
        std::env::var("PLENORA_SQLSERVER_PASSWORD")
            .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned()),
    );
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let read = plenora_database_core::plan::ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: "r8_dto".to_owned(),
        },
        projection: vec!["at".to_owned()],
        order_by: vec![plenora_database_core::plan::OrderBy {
            field: "k".to_owned(),
            direction: plenora_database_core::plan::SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let mut stream = provider
        .read(
            &secret,
            &read,
            &plenora_database_core::provider::ParameterBag::new(std::collections::BTreeMap::new()),
            &budget,
            &cancellation,
        )
        .await
        .expect("lettura");
    let mut values = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await.expect("batch") {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("testo");
        values.extend(
            column
                .iter()
                .map(|value| value.unwrap_or("<null>").to_owned()),
        );
    }
    assert_eq!(
        values,
        [
            "2026-01-01T10:00:00+02:00",
            "2026-01-01T10:00:00.5000000-05:30"
        ]
    );
    admin_batch(&mut admin, "DROP TABLE IF EXISTS [plenora_test].[r8_dto];").await;
}

/// Un UUID con una coda si rifiuta: su 092c859
/// `6f9619ff-8b86-d011-b42d-00c04fc964ffxyz` si salvava come
/// `6f9619ff-8b86-d011-b42d-00c04fc964ff` (`CONVERT(uniqueidentifier, ...)`
/// ignora i caratteri dopo il trentaseiesimo), `committed` con `Strict`.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_a_uuid_con_una_coda_non_si_tronca() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_uuid]; \
         CREATE TABLE [plenora_test].[r8_uuid] ([k] nvarchar(10) NOT NULL PRIMARY KEY, \
           [u] uniqueidentifier NOT NULL);",
    )
    .await;
    let append = keyed("r8_uuid", WriteMode::Append, &[], &[]);
    for tail in [
        "6f9619ff-8b86-d011-b42d-00c04fc964ffxyz",
        "6f9619ff-8b86-d011-b42d-00c04fc964ff-0000",
    ] {
        let error = run_r8(&pool, &append, text_rows(&[("k", &["a"]), ("u", &[tail])]))
            .await
            .expect_err("un UUID con una coda non si scrive");
        assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    }
    assert!(admin_texts(
        &mut admin,
        "SELECT CONVERT(nvarchar(36), [u]) FROM [plenora_test].[r8_uuid]"
    )
    .await
    .is_empty());
    run_r8(
        &pool,
        &append,
        text_rows(&[
            ("k", &["a"]),
            ("u", &["6F9619FF-8B86-D011-B42D-00C04FC964FF"]),
        ]),
    )
    .await
    .expect("un UUID valido si scrive");
    admin_batch(&mut admin, "DROP TABLE IF EXISTS [plenora_test].[r8_uuid];").await;
}

/// Un valore temporale prima dell'anno 1 (in UTC per `datetimeoffset`) o il
/// secondo intercalare si rifiutano con un errore: su 092c859 il driver
/// andava in panico codificando i giorni (`tds/time.rs`, `assertion left ==
/// right`) e la scrittura cadeva con il task; il secondo intercalare
/// arrivava al server come un giorno di 86400 secondi.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_a_le_date_prima_dell_anno_1_si_rifiutano_senza_panico() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_anni]; \
         CREATE TABLE [plenora_test].[r8_anni] ([d] date NULL, [t] datetime2(7) NULL, \
           [o] datetimeoffset(7) NULL);",
    )
    .await;
    // Il giorno prima di 0001-01-01, in giorni e in microsecondi dall'epoca.
    let day_before = -719_163_i32;
    let micro_before = i64::from(day_before) * 86_400_000_000;
    let cases: Vec<(&str, Arc<dyn Array>, DataType)> = vec![
        (
            "d",
            Arc::new(plenora_database_core::arrow::array::Date32Array::from(
                vec![day_before],
            )),
            DataType::Date32,
        ),
        (
            "t",
            Arc::new(
                plenora_database_core::arrow::array::TimestampMicrosecondArray::from(vec![
                    micro_before,
                ]),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, None),
        ),
        (
            "o",
            Arc::new(StringArray::from(vec!["0001-01-01T00:00:00+01:00"])),
            DataType::Utf8,
        ),
        (
            "o",
            Arc::new(StringArray::from(vec!["2016-12-31T23:59:60Z"])),
            DataType::Utf8,
        ),
    ];
    let append = keyed("r8_anni", WriteMode::Append, &[], &[]);
    for (column, array, data_type) in cases {
        let schema = Arc::new(Schema::new(vec![Field::new(column, data_type, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).expect("batch");
        let input = Rows {
            schema,
            batch: Some(batch),
        };
        let pending = tokio::spawn({
            let pool = Arc::clone(&pool);
            let append = append.clone();
            async move { run_r8(&pool, &append, input).await }
        });
        let result = pending.await.expect("la scrittura non va in panico");
        let error = result.expect_err("il valore non e di SQL Server");
        assert_eq!(
            error.category,
            ErrorCategory::DataMapping,
            "{column}: {error:?}"
        );
    }
    assert!(admin_texts(
        &mut admin,
        "SELECT CONVERT(nvarchar(40), [o], 126) FROM [plenora_test].[r8_anni]"
    )
    .await
    .is_empty());
    admin_batch(&mut admin, "DROP TABLE IF EXISTS [plenora_test].[r8_anni];").await;
}

/// `money` e `smallmoney` si rileggono con le quattro cifre del tipo: su
/// 092c859 la lettura (`CONVERT(varchar(50), ...)`, stile 0) le arrotondava a
/// due, e `0.0001` si rileggeva `0.0000`, `1.2345` `1.2300`.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_a_money_si_rilegge_con_quattro_cifre() {
    let _serial = SERIAL.lock().await;
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_money]; \
         CREATE TABLE [plenora_test].[r8_money] ([k] int NOT NULL PRIMARY KEY, \
           [m] money NOT NULL, [s] smallmoney NOT NULL); \
         INSERT INTO [plenora_test].[r8_money] VALUES (1, 0.0001, 1.2345), (2, -7.0009, 0.5);",
    )
    .await;
    let provider = SqlServerProvider::new(live_config(), 2, 3).expect("provider");
    let secret = SecretString::new(
        std::env::var("PLENORA_SQLSERVER_PASSWORD")
            .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned()),
    );
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let read = plenora_database_core::plan::ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".to_owned()),
            object: "r8_money".to_owned(),
        },
        projection: vec!["m".to_owned(), "s".to_owned()],
        order_by: vec![plenora_database_core::plan::OrderBy {
            field: "k".to_owned(),
            direction: plenora_database_core::plan::SortDirection::Asc,
        }],
        row_limit: None,
        row_offset: None,
        filter: None,
        declared_crs: Vec::new(),
    };
    let mut stream = provider
        .read(
            &secret,
            &read,
            &plenora_database_core::provider::ParameterBag::new(std::collections::BTreeMap::new()),
            &budget,
            &cancellation,
        )
        .await
        .expect("lettura");
    let mut values = Vec::new();
    while let Some(batch) = stream.next_batch(&cancellation).await.expect("batch") {
        for column in 0..2 {
            let decimals = batch
                .column(column)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimale");
            values.extend(decimals.iter().map(|value| value.unwrap_or(i128::MIN)));
        }
    }
    // Le unita alla scala 4, colonna per colonna.
    assert_eq!(values, [1, -70_009, 12_345, 5_000]);
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_money];",
    )
    .await;
}

/// Un binario piu lungo della colonna si rifiuta con `Strict`: su 092c859
/// `0x0100` verso `varbinary(1)` si salvava `0x01` (SQL Server tronca in
/// silenzio gli `0x00` finali oltre la lunghezza), `committed`.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_a_varbinary_non_tronca_gli_zeri_finali() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_binario]; \
         CREATE TABLE [plenora_test].[r8_binario] ([b] varbinary(1) NULL);",
    )
    .await;
    let append = keyed("r8_binario", WriteMode::Append, &[], &[]);
    let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
    let input = |value: &'static [u8]| {
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(BinaryArray::from(vec![value])) as Arc<dyn Array>],
        )
        .expect("batch");
        Rows {
            schema: Arc::clone(&schema),
            batch: Some(batch),
        }
    };
    let error = run_r8(&pool, &append, input(&[1, 0]))
        .await
        .expect_err("un binario oltre la colonna non si scrive con Strict");
    assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
    assert!(admin_texts(
        &mut admin,
        "SELECT CONVERT(nvarchar(10), [b], 1) FROM [plenora_test].[r8_binario]"
    )
    .await
    .is_empty());
    run_r8(&pool, &append, input(&[1]))
        .await
        .expect("un binario che entra si scrive");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_binario];",
    )
    .await;
}

/// `xml` con `Strict` si rifiuta: il server conserva il documento, non il
/// testo, e la sonda della regola A misura riscritture che nessuna classe
/// spiega (`a\tb\r\nc` riletto `a\tb\nc`). Su 092c859 la scrittura era
/// `committed` con `Strict` e il `LossReport` vuoto («semantica del tipo»).
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_a_xml_non_riscrive_il_testo_in_silenzio() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[r8_xml]; \
         CREATE TABLE [plenora_test].[r8_xml] ([x] xml NULL);",
    )
    .await;
    let append = keyed("r8_xml", WriteMode::Append, &[], &[]);
    let result = run_r8(&pool, &append, text_rows(&[("x", &["a\tb\r\nc"])])).await;
    assert!(
        result.as_ref().is_err_and(|error| matches!(
            error.category,
            ErrorCategory::DataMapping | ErrorCategory::Unsupported
        )),
        "{result:?}"
    );
    assert!(admin_texts(
        &mut admin,
        "SELECT CONVERT(nvarchar(max), [x]) FROM [plenora_test].[r8_xml]"
    )
    .await
    .is_empty());
    admin_batch(&mut admin, "DROP TABLE IF EXISTS [plenora_test].[r8_xml];").await;
}

/// La frazione della chiave non deve aggiornare il secondo intero vicino.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_s_chiave_datetimeoffset_compatible_non_arrotonda() {
    let _serial = SERIAL.lock().await;
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut admin = SqlServerSession::open(&live_config(), &CancellationToken::new())
        .await
        .expect("sessione");
    admin_batch(
        &mut admin,
        "DROP TABLE IF EXISTS [plenora_test].[s_offset];
         CREATE TABLE [plenora_test].[s_offset] (k datetimeoffset(0) PRIMARY KEY, v nvarchar(10));
         INSERT INTO [plenora_test].[s_offset] VALUES ('2026-01-01T10:00:00+00:00', N'prima');",
    )
    .await;
    for policy in [MappingPolicy::Compatible, MappingPolicy::Lossy] {
        let mut upsert = keyed("s_offset", WriteMode::Upsert, &["k"], &[]);
        upsert.mapping_policy = policy;
        run_r8(
            &pool,
            &upsert,
            text_rows(&[("k", &["2026-01-01T10:00:00.4+00:00"]), ("v", &["nuovo"])]),
        )
        .await
        .expect_err("la chiave non si arrotonda");
        assert_eq!(
            admin_texts(&mut admin, "SELECT v FROM [plenora_test].[s_offset]").await,
            vec!["prima"]
        );
    }
    admin_batch(&mut admin, "DROP TABLE [plenora_test].[s_offset];").await;
}
