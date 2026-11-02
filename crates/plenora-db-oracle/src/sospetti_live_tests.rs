//! Regressione audit 3: precisione, valore e identita delle chiavi.
use super::*;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::transaction::{Statement, TransactionOptions};

async fn snapshot(provider: &dyn Provider, secret: &SecretString, sql: &str) -> Vec<String> {
    let cancellation = CancellationToken::new();
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let mut tx = provider
        .begin_transaction(
            secret,
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .unwrap();
    let rows = tx.query(&Statement::new(sql), &cancellation).await.unwrap();
    tx.rollback(&cancellation).await.unwrap();
    rows.iter()
        .map(|row| match row.values().first().unwrap() {
            ParameterValue::String(value) => value.clone(),
            other => panic!("proiezione ASCII attesa: {other:?}"),
        })
        .collect()
}

#[tokio::test]
#[ignore = "richiede il fixture live del provider"]
async fn live_sospetti_timestamp_precision_identity() {
    let hooks = Hooks::new();
    let provider = &hooks.provider;
    let secret = &hooks.secret;
    let table = "PLN_S900_TS";
    let qualified = table.to_owned();
    for precision in [0, 3, 6] {
        for (mode, keys, id) in [
            (WriteMode::Append, &[][..], 30),
            (WriteMode::Upsert, &["ID"][..], 10),
            (WriteMode::Upsert, &["V"][..], 30),
        ] {
            drop_table(&hooks, table).await;
            let sql = format!("CREATE TABLE {qualified} (ID NUMBER(19) NOT NULL PRIMARY KEY, V TIMESTAMP({precision}) NOT NULL UNIQUE)");
            ddl(&hooks, &sql).await;
            let cancellation = CancellationToken::new();
            let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
            let mut seed = provider
                .begin_transaction(
                    secret,
                    &TransactionOptions::default(),
                    &budget,
                    &cancellation,
                )
                .await
                .unwrap();
            let mut expected_seed = Vec::new();
            // Due vicini: una chiave troncata/arrotondata non deve aggiornare ID=10.
            for (key, fraction) in [(10, "123000"), (20, "999000")] {
                let fraction = if precision == 0 {
                    "000000"
                } else if precision == 6 && key == 10 && keys == ["V"] {
                    "123456"
                } else {
                    fraction
                };
                let sql = format!("INSERT INTO {qualified} VALUES ({key}, TIMESTAMP '2026-01-01 00:00:00.{fraction}')");
                let sql = if key == 20 {
                    sql.replace("00.00.00", "00.00.02")
                        .replace("00:00:00", "00:00:02")
                } else {
                    sql
                };
                let second = if key == 20 { "02" } else { "00" };
                expected_seed.push(format!("{key}:2026-01-01 00:00:{second}.{fraction}"));
                seed.execute(&Statement::new(sql), &cancellation)
                    .await
                    .unwrap();
            }
            assert!(matches!(
                seed.commit(&cancellation).await.unwrap(),
                plenora_database_core::transaction::CommitOutcome::Committed
            ));
            let projection = format!("SELECT TO_CHAR(ID) || ':' || TO_CHAR(V, 'YYYY-MM-DD HH24:MI:SS.FF6') FROM {qualified} ORDER BY ID");
            let before = snapshot(provider, secret, &projection).await;
            assert_eq!(
                before, expected_seed,
                "sentinelle esatte prima della scrittura"
            );
            // Lo snapshot termina con ROLLBACK: ripeterlo prova la persistenza.
            assert_eq!(
                snapshot(provider, secret, &projection).await,
                before,
                "sentinelle committate prima della scrittura sotto prova"
            );
            let value = Arc::new(TimestampMicrosecondArray::from(vec![
                1_767_225_600_123_456_i64,
            ]));
            let result =
                audit_write_declared(&hooks, table, mode, keys, vec![id], (value, false)).await;
            let after = snapshot(provider, secret, &projection).await;
            if precision < 6 {
                let error = result.expect_err("riduzione temporale rifiutata");
                assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
                assert_eq!(error.phase, ErrorPhase::Write, "{error:?}");
                let reason = if keys == ["V"] {
                    plenora_database_core::store_rewrite::REWRITTEN_KEY
                } else {
                    plenora_database_core::store_rewrite::REWRITTEN_VALUE
                };
                assert_eq!(error.message, format!("scrittura rifiutata: {reason}"));
                assert_eq!(
                    after, before,
                    "rifiuto senza alterazioni, precisione {precision}"
                );
            } else {
                assert_eq!(result.unwrap().status, WriteStatus::Committed);
                let expected_id = if mode == WriteMode::Upsert && keys == ["ID"] {
                    10
                } else {
                    30
                };
                let mut expected = before.clone();
                if mode == WriteMode::Upsert {
                    expected.remove(0);
                }
                expected.push(format!("{expected_id}:2026-01-01 00:00:00.123456"));
                expected.sort();
                assert_eq!(after, expected, "identita e valori di tutte le righe");
            }
        }
    }
    drop_table(&hooks, table).await;
}
