//! Qualifica applicativa offline dell'Engine su carico concorrente.

use super::*;
use tokio::sync::Barrier;

const TABLES: usize = 512;
const USERS: usize = 256;

fn source(index: usize) -> ObjectRef {
    ObjectRef {
        catalog: None,
        schema: Some("application".to_owned()),
        object: format!("table_{index:04}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hundreds_of_tables_keep_identity_under_concurrent_reflection() {
    let provider = Arc::new(TestProvider::default());
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("fixture"),
    );
    let mut tasks = Vec::with_capacity(TABLES);
    for index in 0..TABLES {
        let concurrent = engine.clone();
        tasks.push(tokio::spawn(async move {
            let source = source(index);
            let metadata = concurrent
                .reflect_table(&source, false, &CancellationToken::new())
                .await
                .expect("reflection concorrente");
            assert_eq!(metadata.one_table().expect("tabella").name(), source.object);
        }));
    }
    for task in tasks {
        task.await.expect("task reflection");
    }
    assert_eq!(engine.metadata_cache_entries(), TABLES);
    assert_eq!(mutex(&provider.state).inspections, TABLES as u64);

    for index in 0..TABLES {
        engine
            .reflect_table(&source(index), false, &CancellationToken::new())
            .await
            .expect("cache metadata");
    }
    assert_eq!(mutex(&provider.state).inspections, TABLES as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_session_per_user_remains_exclusive_at_concurrent_peak() {
    let provider = Arc::new(TestProvider::default());
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("fixture"),
    );
    let opened = Arc::new(Barrier::new(USERS + 1));
    let release = Arc::new(Barrier::new(USERS + 1));
    let mut tasks = Vec::with_capacity(USERS);
    for _ in 0..USERS {
        let concurrent = engine.clone();
        let opened = Arc::clone(&opened);
        let release = Arc::clone(&release);
        tasks.push(tokio::spawn(async move {
            let mut session = concurrent.session().expect("sessione per request");
            opened.wait().await;
            release.wait().await;
            let mut transaction = session
                .begin_transaction(
                    &TransactionOptions::default(),
                    &budget(),
                    &CancellationToken::new(),
                )
                .await
                .expect("transazione per request");
            transaction
                .execute(&Statement::new("SELECT 1"))
                .await
                .expect("statement per request");
            transaction.commit().await.expect("commit per request");
        }));
    }
    opened.wait().await;
    assert_eq!(engine.statistics().active_sessions, USERS as u64);
    release.wait().await;
    for task in tasks {
        task.await.expect("task utente");
    }
    assert_eq!(engine.statistics().active_sessions, 0);
    assert_eq!(engine.statistics().sessions_opened, USERS as u64);
    assert_eq!(provider.executions.load(Ordering::Relaxed), USERS as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_cancellation_never_crosses_another_session() {
    let provider = Arc::new(TestProvider::default());
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("fixture"),
    );
    let mut tasks = Vec::with_capacity(USERS);
    for index in 0..USERS {
        let concurrent = engine.clone();
        tasks.push(tokio::spawn(async move {
            let cancellation = CancellationToken::new();
            if index % 2 == 0 {
                cancellation.cancel();
            }
            let mut session = concurrent.session().expect("sessione isolata");
            let transaction = session
                .begin_transaction(&TransactionOptions::default(), &budget(), &cancellation)
                .await;
            if index % 2 == 0 {
                let Err(error) = transaction else {
                    panic!("request cancellata accettata")
                };
                assert_eq!(error.category, ErrorCategory::Cancelled);
                return;
            }
            let mut transaction = transaction.expect("request non cancellata");
            transaction
                .execute(&Statement::new("SELECT 1"))
                .await
                .expect("request indipendente");
            transaction.rollback().await.expect("rollback request");
        }));
    }
    for task in tasks {
        task.await.expect("task cancellazione");
    }
    assert_eq!(
        provider.executions.load(Ordering::Relaxed),
        (USERS / 2) as u64
    );
    assert_eq!(engine.statistics().active_sessions, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_provider_saturates_without_starving_requests() {
    const POOL_LIMIT: usize = 8;
    let provider = Arc::new(TestProvider::with_pool_limit(
        POOL_LIMIT,
        Duration::from_millis(4),
    ));
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("fixture"),
    );
    let mut tasks = Vec::with_capacity(USERS);
    for _ in 0..USERS {
        let concurrent = engine.clone();
        tasks.push(tokio::spawn(async move {
            let mut session = concurrent.session().expect("sessione in coda");
            let mut transaction = session
                .begin_transaction(
                    &TransactionOptions::default(),
                    &budget(),
                    &CancellationToken::new(),
                )
                .await
                .expect("checkout bounded");
            transaction
                .execute(&Statement::new("SELECT 1"))
                .await
                .expect("esecuzione bounded");
            transaction.rollback().await.expect("rollback bounded");
        }));
    }
    for task in tasks {
        task.await.expect("task bounded");
    }
    assert_eq!(provider.peak_transactions.load(Ordering::Acquire), 8);
    assert_eq!(provider.active_transactions.load(Ordering::Acquire), 0);
    assert_eq!(provider.executions.load(Ordering::Acquire), USERS as u64);
}

#[tokio::test]
async fn cancellation_while_pool_is_saturated_releases_the_waiter() {
    let provider = Arc::new(TestProvider::with_pool_limit(1, Duration::ZERO));
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("fixture"),
    );
    let mut blocker_session = engine.session().expect("sessione blocker");
    let blocker = blocker_session
        .begin_transaction(
            &TransactionOptions::default(),
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .expect("checkout blocker");

    let cancellation = CancellationToken::new();
    let waiter_token = cancellation.clone();
    let waiter_engine = engine.clone();
    let waiter = tokio::spawn(async move {
        let mut session = waiter_engine.session().expect("sessione waiter");
        session
            .begin_transaction(&TransactionOptions::default(), &budget(), &waiter_token)
            .await
            .map(|_| ())
    });
    tokio::task::yield_now().await;
    cancellation.cancel();
    let error = waiter
        .await
        .expect("task waiter")
        .expect_err("checkout cancellato accettato");
    assert_eq!(error.category, ErrorCategory::Cancelled);
    blocker.rollback().await.expect("rilascio blocker");

    let mut recovery_session = engine.session().expect("sessione dopo cancellazione");
    recovery_session
        .begin_transaction(
            &TransactionOptions::default(),
            &budget(),
            &CancellationToken::new(),
        )
        .await
        .expect("pool riutilizzabile")
        .rollback()
        .await
        .expect("rollback recovery");
    assert_eq!(provider.active_transactions.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_injected_fault_does_not_contaminate_other_users() {
    let provider = Arc::new(TestProvider::with_pool_limit(16, Duration::ZERO));
    provider.fail_next_execution.store(true, Ordering::Release);
    let engine = Engine::new(
        Arc::clone(&provider) as Arc<dyn Provider>,
        SecretString::new("fixture"),
    );
    let mut tasks = Vec::with_capacity(USERS);
    for _ in 0..USERS {
        let concurrent = engine.clone();
        tasks.push(tokio::spawn(async move {
            let mut session = concurrent.session().expect("sessione fault test");
            let mut transaction = session
                .begin_transaction(
                    &TransactionOptions::default(),
                    &budget(),
                    &CancellationToken::new(),
                )
                .await
                .expect("transazione fault test");
            let result = transaction.execute(&Statement::new("SELECT 1")).await;
            transaction.rollback().await.expect("rollback fault test");
            result
        }));
    }
    let mut failures = 0;
    for task in tasks {
        match task.await.expect("task fault test") {
            Ok(1) => {}
            Err(error) => {
                failures += 1;
                assert_eq!(error.category, ErrorCategory::Transient);
            }
            Ok(affected) => panic!("conteggio sintetico inatteso: {affected}"),
        }
    }
    assert_eq!(failures, 1);
    assert_eq!(
        provider.executions.load(Ordering::Acquire),
        USERS as u64 - 1
    );
    assert_eq!(provider.active_transactions.load(Ordering::Acquire), 0);
}
