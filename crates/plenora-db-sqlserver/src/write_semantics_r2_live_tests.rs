//! La seconda rilettura indipendente del 25/10 su 675e322, contro SQL Server
//! 2022 live. Ciascuna prova falliva su 675e322:
//!
//! - l'isolamento chiesto a `begin_transaction` passava da `sp_executesql` e
//!   finiva con la chiamata: `Serializable` chiesto, `READ COMMITTED` in
//!   vigore, senza errore;
//! - Upsert con un indice unico `IGNORE_DUP_KEY = ON` su un'altra colonna:
//!   la riga scartata dal server usciva `skipped` con il COMMIT, dove Append
//!   la rifiutava;
//! - Update con una chiave ripetuta nell'ingresso: `confirmed 2`, vince
//!   l'ultima (`MySQL`: `confirmed 1`, vince la prima);
//! - il percorso preparato convertiva il testo di un `datetimeoffset` con
//!   `CONVERT` sotto la lingua della sessione, dove il bulk rifiutava ogni
//!   testo fuori da RFC 3339.

use super::*;
use crate::SqlServerProvider;
use plenora_database_core::provider::{ParameterValue, Provider};
use plenora_database_core::transaction::{IsolationLevel, Statement, TransactionOptions};
use plenora_database_core::write_keys::DUPLICATE_INPUT_KEYS;

const ISOLATION_SQL: &str = "SELECT CAST(transaction_isolation_level AS int) AS [livello] \
     FROM sys.dm_exec_sessions WHERE session_id = @@SPID";

fn password() -> String {
    std::env::var("PLENORA_SQLSERVER_PASSWORD").unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned())
}

async fn effective_isolation(
    scope: &mut Box<dyn plenora_database_core::transaction::TransactionScope>,
) -> i32 {
    let rows = scope
        .query(&Statement::new(ISOLATION_SQL), &CancellationToken::new())
        .await
        .expect("livello della sessione");
    match rows.first().and_then(|row| row.get_index(0)) {
        Some(ParameterValue::I32(level)) => *level,
        other => panic!("livello illeggibile: {other:?}"),
    }
}

/// Ogni livello chiesto e quello in vigore nella transazione; una sessione
/// ripresa dal pool dopo una transazione `Serializable` non lo porta a chi
/// non lo chiede.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_isolamento_chiesto_e_quello_in_vigore() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let provider = SqlServerProvider::new(live_config(), 1024, 1).expect("provider");
    let secret = SecretString::new(password());
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let mut difformi = Vec::new();
    for (level, expected) in [
        (Some(IsolationLevel::ReadUncommitted), 1),
        (Some(IsolationLevel::ReadCommitted), 2),
        (Some(IsolationLevel::RepeatableRead), 3),
        (Some(IsolationLevel::Serializable), 4),
        // Dopo `Serializable`, con un pool di una sessione.
        (None, 2),
    ] {
        let options = TransactionOptions {
            isolation: level,
            ..TransactionOptions::default()
        };
        let mut scope = provider
            .begin_transaction(&secret, &options, &budget, &cancellation)
            .await
            .expect("la transazione si apre");
        let effective = effective_isolation(&mut scope).await;
        scope.rollback(&cancellation).await.expect("rollback");
        if effective != expected {
            difformi.push(format!(
                "{level:?}: atteso {expected}, in vigore {effective}"
            ));
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Upsert con un indice unico `IGNORE_DUP_KEY = ON` su `label`: la riga 10
/// con un'etichetta gia presente e scartata dal server senza errore. Append
/// la rifiutava gia (nessuna conferma `OUTPUT`, o il conteggio del bulk);
/// Upsert la pubblicava `skipped` e confermava la 11.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_upsert_con_ignore_dup_key_rifiuta_come_append() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let setup = "UPDATE [plenora_test].[semantica] SET [label] = N'c' WHERE [id] = 3; \
         CREATE UNIQUE INDEX [semantica_ux] ON [plenora_test].[semantica] ([label]) \
         WITH (IGNORE_DUP_KEY = ON);";
    let mut difformi = Vec::new();
    for (mode, insert_mode) in [
        (WriteMode::Append, SqlServerInsertMode::Prepared),
        (WriteMode::Append, SqlServerInsertMode::TdsBulk),
        (WriteMode::Upsert, SqlServerInsertMode::Prepared),
    ] {
        let mut admin = SqlServerSession::open(&live_config(), &cancellation)
            .await
            .expect("admin");
        admin
            .execute_query(
                Query::new(format!("{RESET} {setup}")),
                ErrorPhase::Write,
                &cancellation,
            )
            .await
            .expect("fixture");
        let result = write(
            &pool,
            mode,
            insert_mode,
            &[(10, Some("a")), (11, Some("q"))],
        )
        .await;
        let (rows, _) = observe(&mut admin).await;
        if result.is_ok() || rows != ["1:a", "2:b", "3:c"] {
            difformi.push(format!(
                "{mode:?} {insert_mode:?}: {:?} righe {rows:?}",
                result.map(|outcome| outcome.rows)
            ));
        }
    }
    // Il controllo che ammette: Upsert con le sole chiavi su una chiave
    // presente resta `skipped`, su una nuova `inserted`.
    let mut admin = SqlServerSession::open(&live_config(), &cancellation)
        .await
        .expect("admin");
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("fixture");
    match keys_only_upsert(&pool, &[1, 20]).await {
        Ok(outcome) if outcome.rows.confirmed == 1 && outcome.rows.skipped == 1 => {}
        other => difformi.push(format!(
            "upsert sole chiavi: {:?}",
            other.map(|outcome| outcome.rows)
        )),
    }
    admin
        .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
        .await
        .expect("pulizia");
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

async fn keys_only_upsert(
    pool: &Arc<SqlServerPool>,
    ids: &[i32],
) -> plenora_database_core::Result<WriteOutcome> {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(ids.to_vec())) as Arc<dyn Array>],
    )
    .expect("batch");
    let input = Rows {
        schema: Arc::clone(&schema),
        batch: Some(batch),
    };
    let prepared = prepare_write_with_mode(
        pool,
        &operation(WriteMode::Upsert),
        schema,
        &budget,
        &cancellation,
        SqlServerInsertMode::Prepared,
    )
    .await?;
    write_prepared(prepared, Box::new(input), &cancellation).await
}

/// Una chiave ripetuta nell'ingresso di Update, Upsert e `DeleteByKeys`
/// rifiuta la scrittura (`data_mapping`), e il target resta com'era.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_chiave_ripetuta_nell_ingresso_rifiuta() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let pool = SqlServerPool::new(live_config(), 2).expect("pool");
    let mut difformi = Vec::new();
    for (mode, values) in [
        (WriteMode::Update, &[(1, Some("x")), (1, Some("y"))][..]),
        (WriteMode::Upsert, &[(10, Some("x")), (10, Some("y"))][..]),
        (WriteMode::DeleteByKeys, &[(1, None), (1, None)][..]),
    ] {
        let mut admin = SqlServerSession::open(&live_config(), &cancellation)
            .await
            .expect("admin");
        admin
            .execute_query(Query::new(RESET), ErrorPhase::Write, &cancellation)
            .await
            .expect("fixture");
        let result = write(&pool, mode, SqlServerInsertMode::Prepared, values).await;
        let (rows, _) = observe(&mut admin).await;
        let refused = result.as_ref().is_err_and(|error| {
            error.category == ErrorCategory::DataMapping
                && error.message.contains(DUPLICATE_INPUT_KEYS)
        });
        if !refused || rows != ["1:a", "2:b", "3:a"] {
            difformi.push(format!(
                "{mode:?}: {:?} righe {rows:?}",
                result.map(|outcome| outcome.rows)
            ));
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

fn config_as(user: &str, secret: String) -> SqlServerConfig {
    SqlServerConfig::new(
        std::env::var("PLENORA_SQLSERVER_HOST").unwrap_or_else(|_| "sqlserver".to_owned()),
        "dataflow_test".to_owned(),
        user.to_owned(),
        SecretString::new(secret),
    )
    .with_port(live_config().port())
    .with_certificate_policy(CertificatePolicy::TrustServerCertificate)
}

const ITALIAN_LOGIN: &str = "pln_r2_italiano";
const ITALIAN_PASSWORD: &str = "Pln_R2_Italiano_2026!";

async fn append_timestamp(
    config: SqlServerConfig,
    insert_mode: SqlServerInsertMode,
    text: &str,
) -> plenora_database_core::Result<WriteOutcome> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("at", DataType::Utf8, true),
    ]));
    let pool = SqlServerPool::new(config, 1).expect("pool");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int32Array::from(vec![1])) as Arc<dyn Array>,
            Arc::new(StringArray::from(vec![Some(text)])) as Arc<dyn Array>,
        ],
    )
    .expect("batch");
    let mut target = operation(WriteMode::Append);
    target.target.object = "semantica_dto".to_owned();
    let budget = ResourceBudget::new(ResourceLimits::default()).expect("budget");
    let input = Rows {
        schema: Arc::clone(&schema),
        batch: Some(batch),
    };
    let cancellation = CancellationToken::new();
    let prepared =
        prepare_write_with_mode(&pool, &target, schema, &budget, &cancellation, insert_mode)
            .await?;
    write_prepared(prepared, Box::new(input), &cancellation).await
}

async fn stored_timestamps(admin: &mut SqlServerSession) -> Vec<String> {
    admin
        .execute_query(
            Query::new(
                "SELECT CONCAT([id], N'=', CONVERT(nvarchar(40), [at], 127)) \
                 FROM [plenora_test].[semantica_dto] ORDER BY [id];",
            ),
            ErrorPhase::Probe,
            &CancellationToken::new(),
        )
        .await
        .expect("lettura")
        .into_iter()
        .flatten()
        .filter_map(|row| row.try_get::<&str, _>(0).ok().flatten().map(str::to_owned))
        .collect()
}

/// Il testo di un `datetimeoffset` vale lo stesso istante con ogni lingua di
/// sessione, in entrambi i percorsi, oppure si rifiuta (`data_mapping`): il
/// percorso preparato valida RFC 3339 come il bulk.
#[tokio::test]
#[ignore = "richiede SQL Server live esplicito e muta fixture DML isolate"]
async fn live_datetimeoffset_indipendente_dalla_lingua_della_sessione() {
    let _serial = SERIAL.lock().await;
    let cancellation = CancellationToken::new();
    let sa_password = std::env::var("PLENORA_SQLSERVER_SA_PASSWORD")
        .unwrap_or_else(|_| "DataFlow_Test_2026!".to_owned());
    let mut admin = SqlServerSession::open(&config_as("sa", sa_password), &cancellation)
        .await
        .expect("sa");
    admin
        .execute_query(
            Query::new(format!(
                "IF SUSER_ID(N'{ITALIAN_LOGIN}') IS NULL CREATE LOGIN [{ITALIAN_LOGIN}] \
                 WITH PASSWORD = N'{ITALIAN_PASSWORD}', CHECK_POLICY = OFF, \
                 DEFAULT_LANGUAGE = Italiano; \
                 IF USER_ID(N'{ITALIAN_LOGIN}') IS NULL BEGIN \
                 CREATE USER [{ITALIAN_LOGIN}] FOR LOGIN [{ITALIAN_LOGIN}]; \
                 ALTER ROLE db_owner ADD MEMBER [{ITALIAN_LOGIN}]; END; \
                 DROP TABLE IF EXISTS [plenora_test].[semantica_dto]; \
                 CREATE TABLE [plenora_test].[semantica_dto] \
                 ([id] int NOT NULL PRIMARY KEY, [at] datetimeoffset(7) NULL);"
            )),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("fixture");
    let mut difformi = Vec::new();
    for login in [None, Some(ITALIAN_LOGIN)] {
        for insert_mode in [SqlServerInsertMode::Prepared, SqlServerInsertMode::TdsBulk] {
            for (text, expected) in [
                ("01/02/2026 10:00:00 +00:00", None),
                ("2026-01-02T10:00:00+00:00", Some("1=2026-01-02T10:00:00Z")),
            ] {
                let config = login.map_or_else(live_config, |user| {
                    config_as(user, ITALIAN_PASSWORD.to_owned())
                });
                admin
                    .execute_query(
                        Query::new("DELETE FROM [plenora_test].[semantica_dto];"),
                        ErrorPhase::Write,
                        &cancellation,
                    )
                    .await
                    .expect("svuota");
                let result = append_timestamp(config, insert_mode, text).await;
                let stored = stored_timestamps(&mut admin).await;
                let conforme = expected.map_or_else(
                    || {
                        result
                            .as_ref()
                            .is_err_and(|error| error.category == ErrorCategory::DataMapping)
                            && stored.is_empty()
                    },
                    |value| result.is_ok() && stored == [value],
                );
                if !conforme {
                    difformi.push(format!(
                        "{login:?} {insert_mode:?} {text}: {:?} -> {stored:?}",
                        result.map(|outcome| outcome.rows.confirmed)
                    ));
                }
            }
        }
    }
    admin
        .execute_query(
            Query::new(format!(
                "DROP TABLE IF EXISTS [plenora_test].[semantica_dto]; \
                 DROP USER IF EXISTS [{ITALIAN_LOGIN}];"
            )),
            ErrorPhase::Write,
            &cancellation,
        )
        .await
        .expect("pulizia");
    let _ = admin
        .execute_query(
            Query::new(format!("DROP LOGIN [{ITALIAN_LOGIN}];")),
            ErrorPhase::Write,
            &cancellation,
        )
        .await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
