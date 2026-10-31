//! Distingue il troncamento del server dal limite del buffer client.

use super::*;
use plenora_database_core::arrow::array::{Array, StringArray};

#[tokio::test]
#[ignore = "richiede Db2 live esplicito"]
async fn live_long_text_is_complete_or_refused_never_truncated() {
    let provider = provider();
    let secret = SecretString::new(environment("PLENORA_DB2_PASSWORD", "plenora_test"));
    let cancellation = CancellationToken::new();
    let mut transaction = provider
        .begin_transaction(
            &secret,
            &TransactionOptions::default(),
            &budget(),
            &cancellation,
        )
        .await
        .unwrap();
    for (expression, length) in [
        ("REPEAT(CAST('a' AS VARCHAR(32000)), 32000)", 32000),
        ("REPEAT(CAST('a' AS CLOB(100K)), 40000)", 40000),
        ("REPEAT(CAST('a' AS CLOB(100K)), 70000)", 70000),
    ] {
        let result = transaction
            .query(
                &Statement::new(format!("SELECT {expression} AS V FROM SYSIBM.SYSDUMMY1")),
                &cancellation,
            )
            .await;
        match result {
            Ok(rows) => {
                assert_eq!(rows.len(), 1);
                assert_eq!(
                    rows[0].get("V"),
                    Some(&ParameterValue::String("a".repeat(length)))
                );
            }
            Err(error) => {
                assert_eq!(error.phase, ErrorPhase::Read, "{error:?}");
                assert!(
                    matches!(
                        error.category,
                        ErrorCategory::DataMapping
                            | ErrorCategory::ResourceLimit
                            | ErrorCategory::Unsupported
                    ),
                    "{error:?}"
                );
            }
        }
    }
    for expression in [
        "CAST(REPEAT(CAST('a' AS CLOB(100K)), 40000) AS VARCHAR(32672))",
        "LENGTH(CAST(REPEAT(CAST('a' AS CLOB(100K)), 40000) AS VARCHAR(32672)))",
    ] {
        let error = transaction
            .query(
                &Statement::new(format!("SELECT {expression} AS V FROM SYSIBM.SYSDUMMY1")),
                &cancellation,
            )
            .await
            .expect_err("01004 del server non deve consegnare un risultato parziale");
        assert_eq!(error.category, ErrorCategory::DataMapping, "{error:?}");
        assert_eq!(error.phase, ErrorPhase::Read);
        assert!(error.message.contains("01004"), "{error:?}");
    }
    transaction.rollback(&cancellation).await.unwrap();

    arrow_long_text(&provider, &secret, &cancellation).await;
}

async fn arrow_long_text(
    provider: &Db2Provider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) {
    provider
        .run_ddl(
            secret,
            "DROP TABLE IF EXISTS PLENORA.LONG_READ",
            cancellation,
        )
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE PLENORA.LONG_READ (C CLOB(100K))",
        "INSERT INTO PLENORA.LONG_READ VALUES (REPEAT(CAST('a' AS CLOB(100K)), 70000))",
    ] {
        provider.run_ddl(secret, sql, cancellation).await.unwrap();
    }
    let operation = ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("PLENORA".into()),
            object: "LONG_READ".into(),
        },
        projection: vec!["C".into()],
        filter: None,
        order_by: vec![],
        row_limit: None,
        row_offset: None,
        declared_crs: vec![],
    };
    // Un buffer sufficiente e uno intenzionalmente piccolo: se il driver
    // consegna una riga, il confronto copre l'intero contenuto e la coda.
    for cell_bytes in [100_000, 1024] {
        let limits = ResourceLimits {
            cell_bytes,
            ..ResourceLimits::default()
        };
        let budget = ResourceBudget::new(limits).unwrap();
        let mut stream = provider
            .read(
                secret,
                &operation,
                &ParameterBag::default(),
                &budget,
                cancellation,
            )
            .await
            .unwrap();
        match stream.next_batch(cancellation).await {
            Ok(Some(batch)) => {
                assert_eq!(batch.num_rows(), 1);
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert!(!column.is_null(0));
                assert_eq!(column.value(0), "a".repeat(70000));
                assert!(stream.next_batch(cancellation).await.unwrap().is_none());
            }
            Err(error) => {
                assert_eq!(error.phase, ErrorPhase::Read, "{error:?}");
                assert!(
                    matches!(
                        error.category,
                        ErrorCategory::DataMapping | ErrorCategory::ResourceLimit
                    ),
                    "{error:?}"
                );
            }
            Ok(None) => panic!("riga lunga persa"),
        }
    }
    provider
        .run_ddl(secret, "DROP TABLE PLENORA.LONG_READ", cancellation)
        .await
        .unwrap();
}
