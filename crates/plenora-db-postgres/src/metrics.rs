use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct PostgresMetrics {
    pool_checkouts: AtomicU64,
    pool_reuses: AtomicU64,
    pool_new_connections: AtomicU64,
    pool_timeouts: AtomicU64,
    invalidated_sessions: AtomicU64,
    session_resets: AtomicU64,
    catalog_introspections: AtomicU64,
    schema_token_checks: AtomicU64,
    schema_cache_hits: AtomicU64,
    schema_cache_misses: AtomicU64,
    schema_cache_evictions: AtomicU64,
    schema_cache_invalidations: AtomicU64,
    cancellations: AtomicU64,
    read_typed_fast_paths: AtomicU64,
    read_parameterized_typed_fast_paths: AtomicU64,
    read_prepared_fallbacks: AtomicU64,
    query_typed_fast_paths: AtomicU64,
    query_prepared_fallbacks: AtomicU64,
    read_batches: AtomicU64,
    read_target_limited_batches: AtomicU64,
    read_rows: AtomicU64,
    read_bytes: AtomicU64,
    writes_committed: AtomicU64,
    writes_outcome_unknown: AtomicU64,
    write_rows: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresMetricsSnapshot {
    pub pool_checkouts: u64,
    pub pool_reuses: u64,
    pub pool_new_connections: u64,
    pub pool_timeouts: u64,
    pub invalidated_sessions: u64,
    pub session_resets: u64,
    pub catalog_introspections: u64,
    pub schema_token_checks: u64,
    pub schema_cache_hits: u64,
    pub schema_cache_misses: u64,
    pub schema_cache_evictions: u64,
    pub schema_cache_invalidations: u64,
    pub cancellations: u64,
    pub read_typed_fast_paths: u64,
    pub read_parameterized_typed_fast_paths: u64,
    pub read_prepared_fallbacks: u64,
    pub query_typed_fast_paths: u64,
    pub query_prepared_fallbacks: u64,
    pub read_batches: u64,
    pub read_target_limited_batches: u64,
    pub read_rows: u64,
    pub read_bytes: u64,
    pub writes_committed: u64,
    pub writes_outcome_unknown: u64,
    pub write_rows: u64,
}

impl PostgresMetrics {
    pub(super) fn checkout(&self) {
        self.pool_checkouts.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn reuse(&self) {
        self.pool_reuses.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn new_connection(&self) {
        self.pool_new_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn pool_timeout(&self) {
        self.pool_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn invalidate(&self) {
        self.invalidated_sessions.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn session_reset(&self) {
        self.session_resets.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn catalog_introspection(&self) {
        self.catalog_introspections.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn schema_token_check(&self) {
        self.schema_token_checks.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn schema_cache_hit(&self) {
        self.schema_cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn schema_cache_miss(&self) {
        self.schema_cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn schema_cache_eviction(&self) {
        self.schema_cache_evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn schema_cache_invalidation(&self) {
        self.schema_cache_invalidations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn cancellation(&self) {
        self.cancellations.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn read_typed_fast_path(&self) {
        self.read_typed_fast_paths.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn read_parameterized_typed_fast_path(&self) {
        self.read_parameterized_typed_fast_paths
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn read_prepared_fallback(&self) {
        self.read_prepared_fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn query_typed_fast_path(&self) {
        self.query_typed_fast_paths.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn query_prepared_fallback(&self) {
        self.query_prepared_fallbacks
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn read_batch(&self, rows: u64, bytes: u64) {
        self.read_batches.fetch_add(1, Ordering::Relaxed);
        self.read_rows.fetch_add(rows, Ordering::Relaxed);
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(super) fn target_limited_batch(&self) {
        self.read_target_limited_batches
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn write_committed(&self, rows: u64) {
        self.writes_committed.fetch_add(1, Ordering::Relaxed);
        self.write_rows.fetch_add(rows, Ordering::Relaxed);
    }

    pub(super) fn write_outcome_unknown(&self) {
        self.writes_outcome_unknown.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self) -> PostgresMetricsSnapshot {
        PostgresMetricsSnapshot {
            pool_checkouts: self.pool_checkouts.load(Ordering::Relaxed),
            pool_reuses: self.pool_reuses.load(Ordering::Relaxed),
            pool_new_connections: self.pool_new_connections.load(Ordering::Relaxed),
            pool_timeouts: self.pool_timeouts.load(Ordering::Relaxed),
            invalidated_sessions: self.invalidated_sessions.load(Ordering::Relaxed),
            session_resets: self.session_resets.load(Ordering::Relaxed),
            catalog_introspections: self.catalog_introspections.load(Ordering::Relaxed),
            schema_token_checks: self.schema_token_checks.load(Ordering::Relaxed),
            schema_cache_hits: self.schema_cache_hits.load(Ordering::Relaxed),
            schema_cache_misses: self.schema_cache_misses.load(Ordering::Relaxed),
            schema_cache_evictions: self.schema_cache_evictions.load(Ordering::Relaxed),
            schema_cache_invalidations: self.schema_cache_invalidations.load(Ordering::Relaxed),
            cancellations: self.cancellations.load(Ordering::Relaxed),
            read_typed_fast_paths: self.read_typed_fast_paths.load(Ordering::Relaxed),
            read_parameterized_typed_fast_paths: self
                .read_parameterized_typed_fast_paths
                .load(Ordering::Relaxed),
            read_prepared_fallbacks: self.read_prepared_fallbacks.load(Ordering::Relaxed),
            query_typed_fast_paths: self.query_typed_fast_paths.load(Ordering::Relaxed),
            query_prepared_fallbacks: self.query_prepared_fallbacks.load(Ordering::Relaxed),
            read_batches: self.read_batches.load(Ordering::Relaxed),
            read_target_limited_batches: self.read_target_limited_batches.load(Ordering::Relaxed),
            read_rows: self.read_rows.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            writes_committed: self.writes_committed.load(Ordering::Relaxed),
            writes_outcome_unknown: self.writes_outcome_unknown.load(Ordering::Relaxed),
            write_rows: self.write_rows.load(Ordering::Relaxed),
        }
    }
}
