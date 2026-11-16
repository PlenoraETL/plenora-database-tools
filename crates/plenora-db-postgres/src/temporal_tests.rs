use super::*;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

#[test]
fn temporal_postgres_wire_boundaries() {
    for raw in [
        i64::MIN,
        i64::MIN + 1,
        i64::MAX - 1,
        i64::MAX,
        9_223_339_708_800_000_000, // 294276-01-01
        9_223_371_331_200_000_000, // 294277-01-01
    ] {
        assert!(NaiveDateTime::from_sql(&Type::TIMESTAMP, &raw.to_be_bytes()).is_err());
        assert!(DateTime::<Utc>::from_sql(&Type::TIMESTAMPTZ, &raw.to_be_bytes()).is_err());
    }
    for raw in [i32::MIN, i32::MAX, 106_751_617, 106_751_983] {
        assert!(NaiveDate::from_sql(&Type::DATE, &raw.to_be_bytes()).is_err());
    }
    let base = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
    let epoch = base
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_micros();
    for endpoint in [NaiveDateTime::MIN, NaiveDateTime::MAX] {
        let unix = endpoint.and_utc().timestamp_micros();
        let raw = unix - epoch;
        let decoded = NaiveDateTime::from_sql(&Type::TIMESTAMP, &raw.to_be_bytes()).unwrap();
        assert_eq!(decoded.and_utc().timestamp_micros(), unix);
        let outside = if endpoint == NaiveDateTime::MIN {
            raw - 1
        } else {
            raw + 1
        };
        assert!(NaiveDateTime::from_sql(&Type::TIMESTAMP, &outside.to_be_bytes()).is_err());
        assert!(DateTime::<Utc>::from_sql(&Type::TIMESTAMPTZ, &outside.to_be_bytes()).is_err());
    }
    for date in [NaiveDate::MIN, NaiveDate::MAX, base] {
        let days = i32::try_from(date.signed_duration_since(base).num_days()).unwrap();
        assert_eq!(
            NaiveDate::from_sql(&Type::DATE, &days.to_be_bytes()).unwrap(),
            date
        );
    }
    for days in [
        NaiveDate::MIN.signed_duration_since(base).num_days() - 1,
        NaiveDate::MAX.signed_duration_since(base).num_days() + 1,
    ] {
        assert!(
            NaiveDate::from_sql(&Type::DATE, &i32::try_from(days).unwrap().to_be_bytes()).is_err()
        );
    }
    for raw in [i64::MIN, -1, 86_400_000_000, i64::MAX] {
        assert!(CheckedTime::from_sql(&Type::TIME, &raw.to_be_bytes()).is_err());
    }
    for raw in [0_i64, 1, 86_399_999_999] {
        assert_eq!(
            CheckedTime::from_sql(&Type::TIME, &raw.to_be_bytes())
                .unwrap()
                .0,
            NaiveTime::from_sql(&Type::TIME, &raw.to_be_bytes()).unwrap()
        );
    }
    for length in [0, 1, 7, 9, 16] {
        let raw = vec![0; length];
        assert!(CheckedTime::from_sql(&Type::TIME, &raw).is_err());
        assert!(NaiveDateTime::from_sql(&Type::TIMESTAMP, &raw).is_err());
    }
}

#[test]
fn temporal_dependency_time_wrap_is_reproduced_and_blocked() {
    let raw = 86_400_000_000_i64.to_be_bytes();
    assert_eq!(
        NaiveTime::from_sql(&Type::TIME, &raw).unwrap(),
        NaiveTime::MIN
    );
    assert!(CheckedTime::from_sql(&Type::TIME, &raw).is_err());
}

#[test]
fn temporal_network_options_reject_extremes_before_connect() {
    use crate::connection::{connection_config, PostgresNetworkOptions};
    use plenora_database_core::provider::SecretString;
    let secret = SecretString::new("host=127.0.0.1 dbname=test".to_owned());
    for value in [0, 86_400_001, u64::MAX] {
        let options = PostgresNetworkOptions {
            connect_timeout_ms: value,
            ..PostgresNetworkOptions::default()
        };
        assert!(connection_config(&secret, options).is_err());
    }
    let options = PostgresNetworkOptions {
        keepalive_idle_secs: u64::MAX,
        ..PostgresNetworkOptions::default()
    };
    assert!(connection_config(&secret, options).is_err());
    let options = PostgresNetworkOptions {
        connect_timeout_ms: 86_400_000,
        ..PostgresNetworkOptions::default()
    };
    assert!(connection_config(&secret, options).is_ok());
}

#[tokio::test]
async fn temporal_pool_timeout_rejected_without_network() {
    use crate::connection::{PostgresNetworkOptions, PostgresTlsConfig, PostgresTlsMode};
    use crate::pool::{PostgresPool, PostgresSessionOptions};
    use std::sync::Arc;
    let pool = Arc::new(PostgresPool::new(
        1,
        Arc::new(crate::metrics::PostgresMetrics::default()),
    ));
    let result = pool
        .checkout(
            &plenora_database_core::provider::SecretString::new(
                "host=127.0.0.1 port=1 dbname=test".to_owned(),
            ),
            PostgresTlsMode::Disabled,
            &PostgresTlsConfig::webpki(),
            PostgresNetworkOptions::default(),
            PostgresSessionOptions::new(1000, 1000),
            u64::MAX,
        )
        .await;
    assert_eq!(
        result.err().unwrap().category,
        plenora_database_core::ErrorCategory::InvalidPlan
    );
}

#[tokio::test]
#[ignore = "richiede PostgreSQL reale su PLENORA_TEST_POSTGRES_DSN o PLENORA_POSTGRES_URL"]
async fn live_temporal_decoder_boundaries() {
    use crate::arrow::ColumnBuffer;
    use arrow_array::builder::{
        Date32Builder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    };
    use plenora_database_core::provider::{Provider, SecretString};
    use plenora_database_core::transaction::{Statement, TransactionOptions};
    use plenora_database_core::{CancellationToken, ErrorCategory, ResourceBudget, ResourceLimits};
    let url = std::env::var("PLENORA_TEST_POSTGRES_DSN")
        .or_else(|_| std::env::var("PLENORA_POSTGRES_URL"))
        .expect("DSN obbligatorio");
    let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    let task = tokio::spawn(connection);
    let provider = crate::PostgresProvider::insecure_local_with_batch_rows(32);
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let cancellation = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(
            &SecretString::new(url),
            &TransactionOptions::default(),
            &budget,
            &cancellation,
        )
        .await
        .unwrap();
    for kind in ["date", "timestamp", "timestamptz"] {
        for text in ["infinity", "-infinity", "294276-12-31"] {
            let sql = format!("SELECT '{text}'::{kind} AS t");
            let row = client.query_one(&sql, &[]).await.unwrap();
            let mut buffer = match kind {
                "date" => ColumnBuffer::Date(Date32Builder::new()),
                "timestamp" => ColumnBuffer::Timestamp(TimestampMicrosecondBuilder::new()),
                _ => ColumnBuffer::TimestampTz(TimestampMicrosecondBuilder::new()),
            };
            assert_eq!(
                buffer.append(&row, 0).unwrap_err().category,
                ErrorCategory::DataMapping
            );
            assert_eq!(
                tx.query(&Statement::new(sql), &cancellation)
                    .await
                    .unwrap_err()
                    .category,
                ErrorCategory::DataMapping
            );
        }
    }
    let row = client
        .query_one("SELECT '24:00:00'::time", &[])
        .await
        .unwrap();
    assert!(ColumnBuffer::Time(Time64MicrosecondBuilder::new())
        .append(&row, 0)
        .is_err());
    tx.rollback(&cancellation).await.unwrap();
    drop(client);
    task.await.unwrap().unwrap();
}
