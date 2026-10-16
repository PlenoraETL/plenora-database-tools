use super::*;

impl PooledClient {
    pub(crate) fn without_connection() -> Self {
        Self {
            client: None,
            key: [0; 32],
            reused: false,
            reusable: false,
            pool: Arc::new(PostgresPool::new(1, Arc::new(PostgresMetrics::default()))),
            _permit: Arc::new(tokio::sync::Semaphore::new(1))
                .try_acquire_owned()
                .unwrap(),
        }
    }
}
