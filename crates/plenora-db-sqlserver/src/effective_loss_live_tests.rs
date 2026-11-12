//! B3 sul fixture Compose, stessa connessione CA dei test TLS esistenti.
use super::*;
use plenora_database_core::arrow::array::TimestampMicrosecondArray;
use plenora_database_core::execution_loss::{ExecutionLossReport, VersionedLossReport};
use plenora_database_core::loss::LossConsent;
use plenora_database_core::outcome::WriteOutcome;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
struct Input {
    schema: SchemaRef,
    batches: VecDeque<RecordBatch>,
    declared: Option<u64>,
}
impl BatchStream for Input {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn declared_input_rows(&self) -> Option<u64> {
        self.declared
    }
    fn next_batch<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<RecordBatch>> {
        Box::pin(std::future::ready(Ok(self.batches.pop_front())))
    }
}
fn input(native: &str, arrays: Vec<Arc<dyn Array>>, declared: bool) -> Box<dyn BatchStream> {
    let data_type = arrays.first().unwrap().data_type().clone();
    let field = Field::new("v", data_type, true).with_metadata(HashMap::from([
        (protocol::SQLSERVER_NATIVE_DECLARATION.into(), native.into()),
        (
            protocol::SQLSERVER_COLLATION.into(),
            "Latin1_General_100_CI_AS".into(),
        ),
    ]));
    // Collation si dichiara soltanto per il testo.
    let field = if matches!(field.data_type(), DataType::Utf8) {
        field
    } else {
        Field::new("v", field.data_type().clone(), true).with_metadata(HashMap::from([(
            protocol::SQLSERVER_NATIVE_DECLARATION.into(),
            native.into(),
        )]))
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        field,
    ]));
    let mut batches = VecDeque::from([RecordBatch::new_empty(schema.clone())]);
    let mut total = 0;
    for array in arrays {
        let ids: Vec<i32> = (1..=array.len())
            .map(|n| i32::try_from(n + total).unwrap())
            .collect();
        total += array.len();
        batches.push_back(
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids)), array])
                .unwrap(),
        );
    }
    Box::new(Input {
        schema,
        batches,
        declared: declared.then_some(total as u64),
    })
}
fn times(values: &[Option<i64>], declared: bool) -> Box<dyn BatchStream> {
    input(
        "datetime2(3)",
        values
            .iter()
            .map(|v| Arc::new(TimestampMicrosecondArray::from(vec![*v])) as Arc<dyn Array>)
            .collect(),
        declared,
    )
}
fn operation(mode: WriteMode, policy: MappingPolicy) -> WriteOperation {
    WriteOperation {
        target: ObjectRef {
            catalog: None,
            schema: Some("plenora_test".into()),
            object: "b3".into(),
        },
        mode,
        mapping_policy: policy,
        transaction_profile: if mode == WriteMode::Replace {
            TransactionProfile::StagedSwap
        } else {
            TransactionProfile::SingleTransaction
        },
        keys: if matches!(
            mode,
            WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys | WriteMode::Replace
        ) {
            vec!["id".into()]
        } else {
            vec![]
        },
        update_columns: if mode == WriteMode::Update {
            vec!["v".into()]
        } else {
            vec![]
        },
        srid_policy: None,
        create_spatial_index: false,
        allow_partial: false,
    }
}
fn config() -> SqlServerConfig {
    private_ca_live_config(
        std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".into()),
    )
}
async fn sql(admin: &mut SqlServerSession, sql: impl Into<String>) -> Vec<Vec<tiberius::Row>> {
    admin
        .execute_query(
            Query::new(sql.into()),
            ErrorPhase::Write,
            &CancellationToken::new(),
        )
        .await
        .unwrap()
}
async fn reset(admin: &mut SqlServerSession, mode: WriteMode, native: &str) {
    sql(admin, "DROP TABLE IF EXISTS plenora_test.b3;").await;
    if mode != WriteMode::Create {
        sql(admin, format!("CREATE TABLE plenora_test.b3(id int NOT NULL PRIMARY KEY, v {native} NULL); CREATE INDEX b3_v ON plenora_test.b3(v);")).await;
    }
}
async fn snapshot(admin: &mut SqlServerSession) -> Vec<String> {
    sql(admin, "SELECT CONCAT(s.name,'.',t.name,':',c.name,':',TYPE_NAME(c.user_type_id),':',c.max_length,':',c.scale) FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id JOIN sys.columns c ON c.object_id=t.object_id WHERE s.name='plenora_test' AND (t.name='b3' OR t.name LIKE '%stage%' OR t.name LIKE '%backup%') UNION ALL SELECT CONCAT('index:',i.name) FROM sys.indexes i WHERE i.object_id=OBJECT_ID('plenora_test.b3') AND i.name IS NOT NULL ORDER BY 1;").await.into_iter().flatten().map(|r| r.get::<&str,_>(0).unwrap().to_owned()).collect()
}
async fn contents(admin: &mut SqlServerSession) -> Vec<String> {
    sql(admin, "SELECT CONCAT(id,':',COALESCE(CONVERT(varchar(max),CONVERT(varbinary(max),v),2),'NULL')) FROM plenora_test.b3 ORDER BY id;").await.into_iter().flatten().map(|row| row.get::<&str,_>(0).unwrap().to_owned()).collect()
}
fn report(report: &VersionedLossReport) -> &ExecutionLossReport {
    match report {
        VersionedLossReport::Execution(r) => r,
        VersionedLossReport::Legacy(_) => panic!("legacy"),
    }
}
async fn run(
    op: &WriteOperation,
    transport: SqlServerInsertMode,
    input: Box<dyn BatchStream>,
    consent: LossConsent,
    direct: bool,
) -> plenora_database_core::Result<WriteOutcome> {
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let cancel = CancellationToken::new();
    if direct {
        let pool = SqlServerPool::new(config(), 2)?;
        let prepared =
            prepare_write_with_mode(&pool, op, input.schema(), &budget, &cancel, transport).await?;
        write_prepared(prepared, input, &cancel, consent).await
    } else {
        let provider = SqlServerProvider::new(config(), 2, 2)?.with_insert_mode(transport);
        let secret = live_secret();
        let prepared = provider
            .prepare_write(&secret, op, input.schema(), &budget, &cancel)
            .await?;
        provider
            .write(&secret, prepared, input, &budget, &cancel, consent)
            .await
    }
}
// La matrice mantiene affiancati i due percorsi e gli oracoli a-e.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_b3_matrix_a_to_e_direct_trait_and_staging() {
    let _serial = SERIAL.lock().await;
    let mut admin = SqlServerSession::open(&config(), &CancellationToken::new())
        .await
        .unwrap();
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for transport in [SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk] {
            for mode in [
                WriteMode::Create,
                WriteMode::Append,
                WriteMode::Replace,
                WriteMode::TruncateInsert,
                WriteMode::Update,
                WriteMode::Upsert,
                WriteMode::DeleteByKeys,
            ] {
                if transport == SqlServerInsertMode::TdsBulk
                    && !matches!(mode, WriteMode::Append | WriteMode::TruncateInsert)
                {
                    continue;
                }
                for (case, values, consent, fails) in [
                    ("a", vec![Some(1000)], LossConsent::Deny, false),
                    ("b", vec![Some(1234)], LossConsent::Deny, true),
                    ("c", vec![Some(1234)], LossConsent::Accept, false),
                    ("d", vec![None, Some(0)], LossConsent::Deny, false),
                    ("e", vec![Some(1000), Some(1234)], LossConsent::Deny, true),
                ] {
                    let mut reports = vec![];
                    for direct in [false, true] {
                        reset(&mut admin, mode, "datetime2(3)").await;
                        if matches!(
                            mode,
                            WriteMode::Update
                                | WriteMode::Upsert
                                | WriteMode::DeleteByKeys
                                | WriteMode::TruncateInsert
                                | WriteMode::Replace
                        ) {
                            sql(&mut admin, "INSERT INTO plenora_test.b3 VALUES(1,'1970-01-01'),(2,'1970-01-01');").await;
                        }
                        let before = snapshot(&mut admin).await;
                        let before_rows = if mode == WriteMode::Create {
                            vec![]
                        } else {
                            contents(&mut admin).await
                        };
                        let result = run(
                            &operation(mode, policy),
                            transport,
                            times(&values, false),
                            consent,
                            direct,
                        )
                        .await;
                        let fails = fails && mode != WriteMode::DeleteByKeys;
                        if fails {
                            let error = result.unwrap_err();
                            assert_eq!(
                                error.category,
                                ErrorCategory::DataMapping,
                                "{case} {mode:?} {transport:?}"
                            );
                            assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
                            let r = report(error.loss_report.as_ref().unwrap());
                            assert!(r.effective.is_empty());
                            assert!(!r.evaluation.complete);
                            assert!(r.evaluation.entries.iter().any(|e| e.would_change > 0));
                            reports.push(r.clone());
                            assert_eq!(snapshot(&mut admin).await, before);
                            if mode != WriteMode::Create {
                                assert_eq!(contents(&mut admin).await, before_rows);
                            }
                            if mode != WriteMode::Create {
                                let count: i64 = sql(&mut admin, "SELECT COUNT_BIG(*) FROM plenora_test.b3 WHERE v <> '1970-01-01';").await[0][0].get(0).unwrap();
                                assert_eq!(count, 0);
                            }
                        } else {
                            let outcome = result
                                .unwrap_or_else(|e| panic!("{case} {mode:?} {transport:?}: {e:?}"));
                            assert_eq!(outcome.status, WriteStatus::Committed);
                            let r = report(outcome.loss_report.as_ref().unwrap());
                            assert!(r.evaluation.complete);
                            if case == "c" && mode != WriteMode::DeleteByKeys {
                                assert_eq!(r.effective.iter().map(|e| e.committed).sum::<u64>(), 1);
                            } else {
                                assert!(r.effective.is_empty());
                            }
                            reports.push(r.clone());
                            if mode != WriteMode::DeleteByKeys {
                                let stored = sql(&mut admin, "SELECT DATEDIFF_BIG(microsecond,CAST('1970-01-01' AS datetime2),v) FROM plenora_test.b3 WHERE id=1;").await;
                                assert_eq!(
                                    stored[0][0].get::<i64, _>(0),
                                    values[0].map(|v| v / 1000 * 1000)
                                );
                            }
                        }
                    }
                    assert_eq!(reports[0], reports[1]);
                }
            }
        }
    }
}
#[tokio::test]
async fn live_b3_warning_code_page_and_second_batch_text() {
    let _serial = SERIAL.lock().await;
    let mut admin = SqlServerSession::open(&config(), &CancellationToken::new())
        .await
        .unwrap();
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for (native, exact, changed) in [
            (
                "varchar(4) COLLATE Latin1_General_100_CI_AS",
                "abcd",
                "\u{4e2d}",
            ),
            ("nvarchar(4)", "abcd", "abcd "),
            ("char(4)", "abcd", "a"),
            ("nchar(4)", "abcd", "a"),
            (
                "datetimeoffset(3)",
                "2026-01-01T00:00:00.120000+00:00",
                "2026-01-01T00:00:00.123400+00:00",
            ),
        ] {
            for consent in [LossConsent::Deny, LossConsent::Accept] {
                reset(&mut admin, WriteMode::Append, native).await;
                let arrays = [exact, changed]
                    .into_iter()
                    .map(|v| Arc::new(StringArray::from(vec![v])) as Arc<dyn Array>)
                    .collect();
                let declaration = native.split(" COLLATE").next().unwrap();
                let result = run(
                    &operation(WriteMode::Append, policy),
                    SqlServerInsertMode::Prepared,
                    input(declaration, arrays, true),
                    consent,
                    true,
                )
                .await;
                if consent == LossConsent::Deny {
                    let error = result.unwrap_err();
                    assert_eq!(error.category, ErrorCategory::DataMapping);
                    assert!(report(error.loss_report.as_ref().unwrap())
                        .effective
                        .is_empty());
                    assert_eq!(
                        sql(&mut admin, "SELECT COUNT_BIG(*) FROM plenora_test.b3").await[0][0]
                            .get::<i64, _>(0),
                        Some(0)
                    );
                } else {
                    let outcome = result.unwrap();
                    if native.starts_with("datetimeoffset") {
                        assert_eq!(
                            sql(
                                &mut admin,
                                "SELECT DATEPART(nanosecond,v) FROM plenora_test.b3 WHERE id=2"
                            )
                            .await[0][0]
                                .get::<i32, _>(0),
                            Some(123_000_000)
                        );
                    } else {
                        let expected = if native.starts_with("varchar") {
                            "?"
                        } else if native.starts_with("nvarchar") {
                            "abcd"
                        } else {
                            "a   "
                        };
                        assert_eq!(
                            sql(
                                &mut admin,
                                "SELECT CONVERT(nvarchar(max),v) FROM plenora_test.b3 WHERE id=2"
                            )
                            .await[0][0]
                                .get::<&str, _>(0),
                            Some(expected)
                        );
                    }
                    assert!(report(outcome.loss_report.as_ref().unwrap())
                        .effective
                        .iter()
                        .any(|e| e.committed == 1));
                    assert_eq!(
                        sql(&mut admin, "SELECT COUNT_BIG(*) FROM plenora_test.b3").await[0][0]
                            .get::<i64, _>(0),
                        Some(2)
                    );
                }
            }
        }
    }
}
#[tokio::test]
async fn live_b3_negative_profiles_strict_and_tds() {
    let _serial = SERIAL.lock().await;
    let mut admin = SqlServerSession::open(&config(), &CancellationToken::new())
        .await
        .unwrap();
    for consent in [LossConsent::Deny, LossConsent::Accept] {
        for profile in [
            TransactionProfile::ChunkCommitted,
            TransactionProfile::BestEffortDdl,
            TransactionProfile::StagedSwap,
        ] {
            reset(&mut admin, WriteMode::Append, "datetime2(3)").await;
            let mut op = operation(WriteMode::Append, MappingPolicy::Compatible);
            op.transaction_profile = profile;
            let error = run(
                &op,
                SqlServerInsertMode::Prepared,
                times(&[Some(1000)], false),
                consent,
                true,
            )
            .await
            .unwrap_err();
            assert_eq!(error.remote_effect, RemoteEffect::None);
        }
        reset(&mut admin, WriteMode::Append, "datetime2(3)").await;
        for transport in [SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk] {
            let error = run(
                &operation(WriteMode::Append, MappingPolicy::Strict),
                transport,
                times(&[Some(1234)], false),
                consent,
                true,
            )
            .await
            .unwrap_err();
            assert_eq!(error.category, ErrorCategory::DataMapping);
        }
        for native in ["nvarchar(4)", "varbinary(4)"] {
            reset(&mut admin, WriteMode::Append, native).await;
            let arrays: Vec<Arc<dyn Array>> = if native.starts_with("nvarchar") {
                vec![
                    Arc::new(StringArray::from(vec!["abcd"])),
                    Arc::new(StringArray::from(vec!["abcd "])),
                ]
            } else {
                vec![
                    Arc::new(BinaryArray::from(vec![b"abcd".as_slice()])),
                    Arc::new(BinaryArray::from(vec![b"abcd\0".as_slice()])),
                ]
            };
            let error = run(
                &operation(WriteMode::Append, MappingPolicy::Compatible),
                SqlServerInsertMode::TdsBulk,
                input(native, arrays, false),
                consent,
                true,
            )
            .await
            .unwrap_err();
            assert!(report(error.loss_report.as_ref().unwrap())
                .effective
                .is_empty());
            assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
            assert_eq!(
                sql(&mut admin, "SELECT COUNT_BIG(*) FROM plenora_test.b3").await[0][0]
                    .get::<i64, _>(0),
                Some(0)
            );
        }
        for native in ["binary(4)", "char(4)", "nchar(4)", "varchar(4)"] {
            reset(&mut admin, WriteMode::Append, native).await;
            let a: Arc<dyn Array> = if native.starts_with("binary") {
                Arc::new(BinaryArray::from(vec![b"abcd".as_slice()]))
            } else {
                Arc::new(StringArray::from(vec!["abcd"]))
            };
            let error = run(
                &operation(WriteMode::Append, MappingPolicy::Compatible),
                SqlServerInsertMode::TdsBulk,
                input(native, vec![a], false),
                consent,
                true,
            )
            .await
            .unwrap_err();
            assert_eq!(error.category, ErrorCategory::Unsupported);
            assert_eq!(error.remote_effect, RemoteEffect::None);
        }
    }
}

#[tokio::test]
async fn live_b3_binary_and_surrogate_boundaries() {
    let _serial = SERIAL.lock().await;
    let mut admin = SqlServerSession::open(&config(), &CancellationToken::new())
        .await
        .unwrap();
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for native in ["varbinary(4)", "binary(4)"] {
            for bytes in [b"abcd\0".as_slice(), b"abcde", b"abcd ", b"abc"] {
                for consent in [LossConsent::Deny, LossConsent::Accept] {
                    reset(&mut admin, WriteMode::Append, native).await;
                    let values: Arc<dyn Array> = Arc::new(BinaryArray::from(vec![bytes]));
                    let result = run(
                        &operation(WriteMode::Append, policy),
                        SqlServerInsertMode::Prepared,
                        input(native, vec![values], false),
                        consent,
                        true,
                    )
                    .await;
                    let changes = bytes.len() > 4 || native == "binary(4)";
                    if changes && consent == LossConsent::Deny {
                        let error = result.unwrap_err();
                        assert_eq!(error.category, ErrorCategory::DataMapping);
                        assert!(report(error.loss_report.as_ref().unwrap())
                            .effective
                            .is_empty());
                    } else {
                        let outcome = result.unwrap();
                        assert_eq!(
                            !report(outcome.loss_report.as_ref().unwrap())
                                .effective
                                .is_empty(),
                            changes
                        );
                        let stored = sql(&mut admin, "SELECT v FROM plenora_test.b3").await;
                        let actual: &[u8] = stored[0][0].get(0).unwrap();
                        let mut expected = bytes[..bytes.len().min(4)].to_vec();
                        if native == "binary(4)" {
                            expected.resize(4, 0);
                        }
                        assert_eq!(actual, expected);
                    }
                }
            }
        }
        for collation in ["Latin1_General_100_CI_AS", "Latin1_General_100_CI_AS_SC"] {
            for native in ["nvarchar", "nchar"] {
                for value in ["ab\u{1f600}", "abc\u{1f600}"] {
                    for consent in [LossConsent::Deny, LossConsent::Accept] {
                        reset(
                            &mut admin,
                            WriteMode::Append,
                            &format!("{native}(4) COLLATE {collation}"),
                        )
                        .await;
                        let values: Arc<dyn Array> = Arc::new(StringArray::from(vec![value]));
                        let result = run(
                            &operation(WriteMode::Append, policy),
                            SqlServerInsertMode::Prepared,
                            input(&format!("{native}(4)"), vec![values], false),
                            consent,
                            true,
                        )
                        .await;
                        if value.starts_with("abc") {
                            assert_eq!(result.unwrap_err().category, ErrorCategory::DataMapping);
                            assert_eq!(
                                sql(&mut admin, "SELECT COUNT_BIG(*) FROM plenora_test.b3").await
                                    [0][0]
                                    .get::<i64, _>(0),
                                Some(0)
                            );
                        } else {
                            assert!(report(result.unwrap().loss_report.as_ref().unwrap())
                                .effective
                                .is_empty());
                            assert_eq!(
                                sql(&mut admin, "SELECT v FROM plenora_test.b3").await[0][0]
                                    .get::<&str, _>(0),
                                Some(value)
                            );
                        }
                    }
                }
            }
        }
    }
}
#[tokio::test]
async fn live_b3_uncertain_commit_no_match_and_schema_change() {
    let _serial = SERIAL.lock().await;
    let mut admin = SqlServerSession::open(&config(), &CancellationToken::new())
        .await
        .unwrap();
    for declared in [false, true] {
        reset(&mut admin, WriteMode::Append, "datetime2(3)").await;
        let input = times(&[Some(1234)], declared);
        let pool = SqlServerPool::new(config(), 2).unwrap();
        let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
        let cancel = CancellationToken::new();
        let prepared = prepare_write_with_mode(
            &pool,
            &operation(WriteMode::Append, MappingPolicy::Compatible),
            input.schema(),
            &budget,
            &cancel,
            SqlServerInsertMode::Prepared,
        )
        .await
        .unwrap();
        let outcome = crate::write::write_prepared_with_fault(
            prepared,
            input,
            &cancel,
            LossConsent::Accept,
            crate::write::WriteFaultPoint::CommitConfirmationLost,
        )
        .await
        .unwrap();
        assert_eq!(outcome.status, WriteStatus::OutcomeUnknown);
        let r = report(outcome.loss_report.as_ref().unwrap());
        assert_eq!(r.effective[0].committed, 0);
        assert_eq!(r.effective[0].uncertain, 1);
        assert_eq!(
            sql(&mut admin, "SELECT COUNT_BIG(*) FROM plenora_test.b3").await[0][0]
                .get::<i64, _>(0),
            Some(1)
        );
    }
    reset(&mut admin, WriteMode::Update, "datetime2(3)").await;
    let outcome = run(
        &operation(WriteMode::Update, MappingPolicy::Compatible),
        SqlServerInsertMode::Prepared,
        times(&[Some(1234)], false),
        LossConsent::Accept,
        true,
    )
    .await
    .unwrap();
    let r = report(outcome.loss_report.as_ref().unwrap());
    assert!(r.effective.is_empty());
    assert!(r.evaluation.entries.iter().any(|e| e.would_change == 1));
    let pool = SqlServerPool::new(config(), 2).unwrap();
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let cancel = CancellationToken::new();
    let input = times(&[Some(1000)], false);
    let prepared = prepare_write_with_mode(
        &pool,
        &operation(WriteMode::Append, MappingPolicy::Compatible),
        input.schema(),
        &budget,
        &cancel,
        SqlServerInsertMode::Prepared,
    )
    .await
    .unwrap();
    sql(&mut admin, "DROP INDEX b3_v ON plenora_test.b3; ALTER TABLE plenora_test.b3 ALTER COLUMN v datetime2(0) NULL;").await;
    let error = write_prepared(prepared, input, &cancel, LossConsent::Deny)
        .await
        .unwrap_err();
    assert!(report(error.loss_report.as_ref().unwrap())
        .effective
        .is_empty());
    assert_eq!(
        sql(&mut admin, "SELECT COUNT_BIG(*) FROM plenora_test.b3").await[0][0].get::<i64, _>(0),
        Some(0)
    );
}
