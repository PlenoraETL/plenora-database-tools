//! B2: oracoli indipendenti sul fixture docker-compose.postgres.yml.
use super::*;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use plenora_database_core::execution_loss::{ExecutionLossReport, VersionedLossReport};
use plenora_database_core::loss::{LossConsent, MappingPolicy};
use plenora_database_core::plan::{TransactionProfile, WriteMode};
use plenora_database_core::provider::Provider;
use plenora_database_core::resource::{ResourceBudget, ResourceLimits};
use plenora_database_core::{CancellationToken, ErrorCategory, RemoteEffect};
use std::collections::VecDeque;

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

fn input(
    native: &str,
    values: &[(i64, Option<&str>)],
    declare_input_rows: bool,
) -> Box<dyn BatchStream> {
    let field = Field::new("v", DataType::Utf8, true).with_metadata(
        [
            (protocol::POSTGRES_NATIVE_TYPE.to_owned(), native.to_owned()),
            (
                protocol::POSTGRES_NATIVE_DECLARATION.to_owned(),
                native.to_owned(),
            ),
        ]
        .into_iter()
        .collect::<arrow_schema::Metadata>(),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        field,
    ]));
    // Un batch per riga: e verifica realmente il rollback del primo invio.
    let mut batches = VecDeque::from([RecordBatch::new_empty(schema.clone())]);
    for &(id, value) in values {
        batches.push_back(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![id])),
                    Arc::new(StringArray::from(vec![value])),
                ],
            )
            .unwrap(),
        );
    }
    Box::new(Input {
        schema,
        batches,
        declared: declare_input_rows.then_some(values.len() as u64),
    })
}

fn operation(mode: WriteMode, policy: MappingPolicy) -> WriteOperation {
    let mut operation = crate::write_visibility_live_tests::operation("target", mode);
    operation.target.schema = Some("pln_b2".to_owned());
    operation.mapping_policy = policy;
    if mode == WriteMode::Update {
        operation.update_columns = vec!["v".to_owned()];
    }
    operation
}

async fn run(
    provider: &PostgresProvider,
    secret: &SecretString,
    op: &WriteOperation,
    input: Box<dyn BatchStream>,
    consent: LossConsent,
) -> Result<WriteOutcome> {
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let cancel = CancellationToken::new();
    let prepared = provider
        .prepare_write(secret, op, input.schema(), &budget, &cancel)
        .await?;
    provider
        .write(secret, prepared, input, &budget, &cancel, consent)
        .await
}

fn execution(report: &VersionedLossReport) -> &ExecutionLossReport {
    let VersionedLossReport::Execution(report) = report else {
        panic!("rapporto legacy nel writer B2")
    };
    report.validate().unwrap();
    report
}

async fn reset(client: &tokio_postgres::Client, mode: WriteMode, native: &str) {
    client
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS pln_b2; DROP TABLE IF EXISTS pln_b2.target CASCADE",
        )
        .await
        .unwrap();
    if mode != WriteMode::Create {
        client.batch_execute(&format!("CREATE TABLE pln_b2.target(id bigint PRIMARY KEY, v {native}); CREATE INDEX b2_v ON pln_b2.target(id, v)")).await.unwrap();
        client
            .execute(
                "INSERT INTO pln_b2.target VALUES(1, $1::text::jsonb), (2, $1::text::jsonb)",
                &[&r#"{"old":1}"#],
            )
            .await
            .unwrap();
    }
}

async fn rows(client: &tokio_postgres::Client) -> Vec<(i64, Option<String>)> {
    client
        .query("SELECT id, v::text FROM pln_b2.target ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_b2_matrix_a_to_e_all_modes_transports_policies() {
    let _serial = crate::write_visibility_live_tests::SERIAL.lock().await;
    let Some(dsn) = crate::write_visibility_live_tests::live_dsn() else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let secret = SecretString::new(dsn);
    for policy in [
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for transport in [
            PostgresInsertMode::CopyText,
            PostgresInsertMode::CopyBinary,
            PostgresInsertMode::Prepared,
        ] {
            let provider = PostgresProvider::insecure_local().with_insert_mode(transport);
            for declared in [false, true] {
                for mode in [
                    WriteMode::Create,
                    WriteMode::Append,
                    WriteMode::Replace,
                    WriteMode::TruncateInsert,
                    WriteMode::Update,
                    WriteMode::Upsert,
                    WriteMode::DeleteByKeys,
                ] {
                    for case in ['a', 'b', 'c', 'd', 'e'] {
                        reset(&client, mode, "jsonb").await;
                        let before = if mode == WriteMode::Create {
                            vec![]
                        } else {
                            rows(&client).await
                        };
                        let op = operation(mode, policy);
                        let first_id = if mode == WriteMode::Append { 3 } else { 1 };
                        let values = match case {
                            'a' => vec![(first_id, Some(r#" {"b":2,"a":1} "#))],
                            'b' | 'c' => vec![(first_id, Some(r#"{"a":1,"a":2}"#))],
                            'd' => vec![(first_id, None)],
                            'e' => vec![
                                (first_id, Some(r#"{"a":1}"#)),
                                (first_id + 1, Some(r#"{"a":1,"a":2}"#)),
                            ],
                            _ => unreachable!(),
                        };
                        let result = run(
                            &provider,
                            &secret,
                            &op,
                            input("jsonb", &values, declared),
                            if case == 'c' {
                                LossConsent::Accept
                            } else {
                                LossConsent::Deny
                            },
                        )
                        .await;
                        if matches!(case, 'b' | 'e') && mode != WriteMode::DeleteByKeys {
                            let error = result.unwrap_err();
                            assert_eq!(
                                error.category,
                                ErrorCategory::DataMapping,
                                "{mode:?}/{transport:?}/{case}"
                            );
                            assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
                            let report = execution(error.loss_report.as_ref().unwrap());
                            assert!(report.effective.is_empty());
                            assert!(!report.evaluation.complete);
                            assert!(report.evaluation.entries.iter().any(|e| e.would_change > 0));
                            if mode == WriteMode::Create {
                                assert!(client
                                    .query_one("SELECT to_regclass('pln_b2.target')::text", &[])
                                    .await
                                    .unwrap()
                                    .get::<_, Option<String>>(0)
                                    .is_none());
                            } else {
                                assert_eq!(rows(&client).await, before);
                                assert!(client
                                    .query_one("SELECT to_regclass('pln_b2.b2_v')::text", &[])
                                    .await
                                    .unwrap()
                                    .get::<_, Option<String>>(0)
                                    .is_some());
                            }
                        } else {
                            let outcome = result.unwrap();
                            let report = execution(outcome.loss_report.as_ref().unwrap());
                            assert!(!report.risk.is_empty());
                            assert!(report.evaluation.complete);
                            if case == 'c' && mode != WriteMode::DeleteByKeys {
                                assert_eq!(
                                    report.effective.iter().map(|e| e.committed).sum::<u64>(),
                                    1
                                );
                                assert!(rows(&client)
                                    .await
                                    .contains(&(first_id, Some(r#"{"a": 2}"#.to_owned()))));
                            } else {
                                assert!(report.effective.is_empty());
                            }
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn live_b2_text_second_batch_rollback_and_strict_baseline() {
    let _serial = crate::write_visibility_live_tests::SERIAL.lock().await;
    let Some(dsn) = crate::write_visibility_live_tests::live_dsn() else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let secret = SecretString::new(dsn);
    for policy in [
        MappingPolicy::Strict,
        MappingPolicy::Compatible,
        MappingPolicy::Lossy,
        MappingPolicy::Native,
    ] {
        for transport in [
            PostgresInsertMode::CopyText,
            PostgresInsertMode::CopyBinary,
            PostgresInsertMode::Prepared,
        ] {
            let provider = PostgresProvider::insecure_local().with_insert_mode(transport);
            for mode in [
                WriteMode::Create,
                WriteMode::Append,
                WriteMode::Replace,
                WriteMode::TruncateInsert,
                WriteMode::Update,
                WriteMode::Upsert,
            ] {
                for consent in [LossConsent::Deny, LossConsent::Accept] {
                    client.batch_execute("CREATE SCHEMA IF NOT EXISTS pln_b2; DROP TABLE IF EXISTS pln_b2.target").await.unwrap();
                    if mode != WriteMode::Create {
                        client.batch_execute("CREATE TABLE pln_b2.target(id bigint PRIMARY KEY, v varchar(3)); INSERT INTO pln_b2.target VALUES(1,'old'),(2,'old')").await.unwrap();
                    }
                    let first = if mode == WriteMode::Append { 3 } else { 1 };
                    let result = run(
                        &provider,
                        &secret,
                        &operation(mode, policy),
                        input(
                            "character varying(3)",
                            &[(first, Some("abc")), (first + 1, Some("abcd"))],
                            false,
                        ),
                        consent,
                    )
                    .await;
                    // Baseline 9.0.3: pg_type/native_declaration_sql ripiega su text
                    // per questa dichiarazione non qualificata. Nessuna troncatura.
                    if mode == WriteMode::Create {
                        let outcome = result.unwrap();
                        assert!(execution(outcome.loss_report.as_ref().unwrap())
                            .effective
                            .is_empty());
                        assert_eq!(
                            rows(&client).await,
                            vec![(1, Some("abc".to_owned())), (2, Some("abcd".to_owned()))]
                        );
                        let native: String = client.query_one("SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'pln_b2.target'::regclass AND attname = 'v'", &[]).await.unwrap().get(0);
                        assert_eq!(native, "text");
                        continue;
                    }
                    let error = result.unwrap_err();
                    assert_eq!(error.category, ErrorCategory::DataMapping);
                    assert!(execution(error.loss_report.as_ref().unwrap())
                        .effective
                        .is_empty());
                    assert_eq!(
                        rows(&client).await,
                        vec![(1, Some("old".to_owned())), (2, Some("old".to_owned()))]
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn live_b2_no_match_suppressed_rows_and_profiles() {
    let _serial = crate::write_visibility_live_tests::SERIAL.lock().await;
    let Some(dsn) = crate::write_visibility_live_tests::live_dsn() else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let secret = SecretString::new(dsn);
    reset(&client, WriteMode::Append, "jsonb").await;
    client.batch_execute("CREATE OR REPLACE FUNCTION pln_b2.suppress() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN NULL; END'; CREATE TRIGGER b2_suppress BEFORE INSERT ON pln_b2.target FOR EACH ROW EXECUTE FUNCTION pln_b2.suppress()").await.unwrap();
    for transport in [
        PostgresInsertMode::CopyText,
        PostgresInsertMode::CopyBinary,
        PostgresInsertMode::Prepared,
    ] {
        let provider = PostgresProvider::insecure_local().with_insert_mode(transport);
        // Nella 9.0.3 None salta la diagnostica; Some(0) la seleziona e fallisce.
        for policy in [
            MappingPolicy::Compatible,
            MappingPolicy::Lossy,
            MappingPolicy::Native,
        ] {
            for consent in [LossConsent::Deny, LossConsent::Accept] {
                let before = rows(&client).await;
                let outcome = run(
                    &provider,
                    &secret,
                    &operation(WriteMode::Append, policy),
                    input("jsonb", &[], false),
                    consent,
                )
                .await
                .unwrap();
                assert_eq!(
                    outcome.status,
                    plenora_database_core::outcome::WriteStatus::Committed
                );
                let report = execution(outcome.loss_report.as_ref().unwrap());
                assert!(report.evaluation.complete);
                assert!(report.effective.is_empty());
                assert_eq!(rows(&client).await, before);
                let error = run(
                    &provider,
                    &secret,
                    &operation(WriteMode::Append, policy),
                    input("jsonb", &[], true),
                    consent,
                )
                .await
                .unwrap_err();
                assert_eq!(error.category, ErrorCategory::InvalidPlan);
                assert_eq!(error.remote_effect, RemoteEffect::None);
                let report = execution(error.loss_report.as_ref().unwrap());
                assert!(!report.evaluation.complete);
                assert!(report.effective.is_empty());
                assert_eq!(rows(&client).await, before);
            }
        }
        for mode in [WriteMode::Append, WriteMode::Update] {
            let outcome = run(
                &provider,
                &secret,
                &operation(mode, MappingPolicy::Compatible),
                input("jsonb", &[(99, Some(r#"{"a":1,"a":2}"#))], false),
                LossConsent::Accept,
            )
            .await
            .unwrap();
            assert!(execution(outcome.loss_report.as_ref().unwrap())
                .effective
                .is_empty());
        }
        for profile in [
            TransactionProfile::ChunkCommitted,
            TransactionProfile::BestEffortDdl,
        ] {
            let mut op = operation(WriteMode::Append, MappingPolicy::Compatible);
            op.transaction_profile = profile;
            let error = run(
                &provider,
                &secret,
                &op,
                input("jsonb", &[], false),
                LossConsent::Deny,
            )
            .await
            .unwrap_err();
            assert_eq!(error.category, ErrorCategory::Unsupported);
            assert_eq!(error.remote_effect, RemoteEffect::None);
        }
    }
}

#[tokio::test]
async fn live_b2_schema_change_keys_and_add_column_rollback() {
    let _serial = crate::write_visibility_live_tests::SERIAL.lock().await;
    let Some(dsn) = crate::write_visibility_live_tests::live_dsn() else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let secret = SecretString::new(dsn);
    let provider = PostgresProvider::insecure_local();
    reset(&client, WriteMode::Append, "jsonb").await;
    let op = operation(WriteMode::Append, MappingPolicy::Compatible);
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let cancellation = CancellationToken::new();
    let stream = input("jsonb", &[(3, Some(r#"{"a":1}"#))], false);
    let prepared = provider
        .prepare_write(&secret, &op, stream.schema(), &budget, &cancellation)
        .await
        .unwrap();
    client
        .batch_execute("ALTER TABLE pln_b2.target ADD COLUMN concurrent integer")
        .await
        .unwrap();
    let error = provider
        .write(
            &secret,
            prepared,
            stream,
            &budget,
            &cancellation,
            LossConsent::Accept,
        )
        .await
        .unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    assert_eq!(rows(&client).await.len(), 2);

    // ADD COLUMN e field identity devono tornare indietro con il batch 1.
    client.batch_execute("DROP TABLE pln_b2.target; CREATE TABLE pln_b2.target(id bigint PRIMARY KEY); INSERT INTO pln_b2.target VALUES(1)").await.unwrap();
    let provider = provider.with_schema_evolution(PostgresSchemaEvolution::AddNullableColumns);
    let before = crate::catalog::schema_token(&client, &op.target)
        .await
        .unwrap();
    let error = run(
        &provider,
        &secret,
        &op,
        input(
            "jsonb",
            &[(3, Some(r#"{"a":1}"#)), (4, Some(r#"{"a":1,"a":2}"#))],
            false,
        ),
        LossConsent::Deny,
    )
    .await
    .unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    assert!(execution(error.loss_report.as_ref().unwrap())
        .effective
        .is_empty());
    let after = crate::catalog::schema_token(&client, &op.target)
        .await
        .unwrap();
    assert_eq!(before.exact_signature, after.exact_signature);
    assert_eq!(
        client
            .query_one("SELECT count(*) FROM pln_b2.target", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );

    // Una PRIMARY KEY creata nella stessa transazione non sopravvive al rifiuto.
    client
        .batch_execute("DROP TABLE pln_b2.target")
        .await
        .unwrap();
    let mut op = operation(WriteMode::Create, MappingPolicy::Compatible);
    op.keys = vec!["id".to_owned()];
    let error = run(
        &provider,
        &secret,
        &op,
        input(
            "jsonb",
            &[(1, Some(r#"{"a":1}"#)), (2, Some(r#"{"a":1,"a":2}"#))],
            false,
        ),
        LossConsent::Deny,
    )
    .await
    .unwrap_err();
    assert_eq!(error.remote_effect, RemoteEffect::RolledBack);
    assert!(client
        .query_one("SELECT to_regclass('pln_b2.target')::text", &[])
        .await
        .unwrap()
        .get::<_, Option<String>>(0)
        .is_none());
}

#[tokio::test]
async fn live_b2_strict_and_uncertain_commit_reports() {
    let _serial = crate::write_visibility_live_tests::SERIAL.lock().await;
    let Some(dsn) = crate::write_visibility_live_tests::live_dsn() else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    let secret = SecretString::new(dsn);
    for declared in [false, true] {
        for consent in [LossConsent::Deny, LossConsent::Accept] {
            reset(&client, WriteMode::Append, "jsonb").await;
            let before = rows(&client).await;
            let error = run(
                &PostgresProvider::insecure_local(),
                &secret,
                &operation(WriteMode::Append, MappingPolicy::Strict),
                input("jsonb", &[(3, Some(r#"{"a":1,"a":2}"#))], declared),
                consent,
            )
            .await
            .unwrap_err();
            assert_eq!(error.category, ErrorCategory::DataMapping);
            assert_eq!(rows(&client).await, before);
        }
        reset(&client, WriteMode::Append, "jsonb").await;
        let provider = PostgresProvider::insecure_local()
            .with_fault_injection(PostgresFaultPoint::AfterCommitAcknowledgement);
        let result = run(
            &provider,
            &secret,
            &operation(WriteMode::Append, MappingPolicy::Compatible),
            input("jsonb", &[(3, Some(r#"{"a":1,"a":2}"#))], declared),
            LossConsent::Accept,
        )
        .await;
        let report = match &result {
            Ok(outcome) => execution(outcome.loss_report.as_ref().unwrap()),
            Err(error) => execution(error.loss_report.as_ref().unwrap()),
        };
        assert_eq!(report.effective.iter().map(|e| e.committed).sum::<u64>(), 0);
        assert_eq!(report.effective.iter().map(|e| e.uncertain).sum::<u64>(), 1);
        assert_eq!(rows(&client).await.len(), 3);
    }
}
