use super::*;

impl PooledSqlServerSession {
    pub(crate) fn without_connection() -> Self {
        Self {
            session: None,
            reusable: false,
            pool: SqlServerPool::new(
                SqlServerConfig::new(
                    "localhost",
                    "test",
                    "test",
                    plenora_database_core::provider::SecretString::new("test"),
                ),
                1,
            )
            .unwrap(),
            _permit: Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .unwrap(),
        }
    }
}
