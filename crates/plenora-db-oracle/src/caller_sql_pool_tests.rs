use super::*;

impl PooledOracleConnection {
    pub(crate) fn without_connection() -> Self {
        Self {
            connection: None,
            reusable: false,
            pool: OraclePool::new(
                OracleConfig::new("localhost", "test", "test"),
                SecretString::new("test"),
                1,
            )
            .unwrap(),
            _permit: Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .unwrap(),
        }
    }
}
