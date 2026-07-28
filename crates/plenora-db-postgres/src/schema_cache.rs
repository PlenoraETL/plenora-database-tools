use super::{catalog::CatalogSchemaToken, lock_recover, types::ColumnSpec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Identità strutturale `PostgreSQL` di un oggetto introspezionato.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresSchemaToken {
    pub schema_version: u32,
    pub database_oid: u32,
    pub namespace_oid: u32,
    pub relation_oid: u32,
    pub structural_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaCacheKey {
    connection: [u8; 32],
    schema: String,
    object: String,
}

impl SchemaCacheKey {
    pub const fn new(connection: [u8; 32], schema: String, object: String) -> Self {
        Self {
            connection,
            schema,
            object,
        }
    }
}

#[derive(Clone)]
struct SchemaCacheEntry {
    token: CatalogSchemaToken,
    columns: Arc<Vec<ColumnSpec>>,
    last_used: u64,
}

#[derive(Default)]
struct SchemaCacheState {
    entries: HashMap<SchemaCacheKey, SchemaCacheEntry>,
    clock: u64,
}

pub struct PostgresSchemaCache {
    state: Mutex<SchemaCacheState>,
    max_entries: usize,
}

impl std::fmt::Debug for PostgresSchemaCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresSchemaCache")
            .field("max_entries", &self.max_entries)
            .field("entries", &self.len())
            .finish_non_exhaustive()
    }
}

impl PostgresSchemaCache {
    pub fn new(max_entries: usize) -> Self {
        Self {
            state: Mutex::new(SchemaCacheState::default()),
            max_entries,
        }
    }

    pub fn candidate(
        &self,
        key: &SchemaCacheKey,
    ) -> Option<(CatalogSchemaToken, Arc<Vec<ColumnSpec>>)> {
        lock_recover(&self.state)
            .entries
            .get(key)
            .map(|entry| (entry.token.clone(), Arc::clone(&entry.columns)))
    }

    pub fn touch(&self, key: &SchemaCacheKey) {
        let mut state = lock_recover(&self.state);
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        if let Some(entry) = state.entries.get_mut(key) {
            entry.last_used = clock;
        }
    }

    pub fn insert(
        &self,
        key: SchemaCacheKey,
        token: CatalogSchemaToken,
        columns: Arc<Vec<ColumnSpec>>,
    ) -> bool {
        if self.max_entries == 0 {
            return false;
        }
        let mut state = lock_recover(&self.state);
        state.clock = state.clock.saturating_add(1);
        let clock = state.clock;
        state.entries.insert(
            key,
            SchemaCacheEntry {
                token,
                columns,
                last_used: clock,
            },
        );
        let evicted = if state.entries.len() > self.max_entries {
            let oldest = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(key, _)| key.clone());
            oldest.is_some_and(|oldest| state.entries.remove(&oldest).is_some())
        } else {
            false
        };
        drop(state);
        evicted
    }

    pub fn invalidate(&self, key: &SchemaCacheKey) -> bool {
        lock_recover(&self.state).entries.remove(key).is_some()
    }

    pub fn len(&self) -> usize {
        lock_recover(&self.state).entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{PostgresSchemaCache, PostgresSchemaToken, SchemaCacheKey};
    use crate::catalog::CatalogSchemaToken;
    use std::sync::Arc;

    fn key(id: u8) -> SchemaCacheKey {
        SchemaCacheKey::new([id; 32], "public".to_owned(), format!("object_{id}"))
    }

    fn token(id: u32) -> CatalogSchemaToken {
        CatalogSchemaToken {
            public: PostgresSchemaToken {
                schema_version: 1,
                database_oid: 1,
                namespace_oid: 2,
                relation_oid: id,
                structural_fingerprint: format!("fingerprint-{id}"),
            },
            exact_signature: format!("signature-{id}"),
        }
    }

    #[test]
    fn lru_eviction_is_bounded_and_touch_is_observable() {
        let cache = PostgresSchemaCache::new(2);
        let columns = Arc::new(Vec::new());
        let first = key(1);
        let second = key(2);
        let third = key(3);

        assert!(!cache.insert(first.clone(), token(1), Arc::clone(&columns)));
        assert!(!cache.insert(second.clone(), token(2), Arc::clone(&columns)));
        cache.touch(&first);
        assert!(cache.insert(third.clone(), token(3), Arc::clone(&columns)));

        assert!(cache.candidate(&first).is_some());
        assert!(cache.candidate(&second).is_none());
        assert!(cache.candidate(&third).is_some());
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn zero_capacity_cache_fails_closed_without_retaining_entries() {
        let cache = PostgresSchemaCache::new(0);
        assert!(!cache.insert(key(1), token(1), Arc::new(Vec::new())));
        assert_eq!(cache.len(), 0);
    }
}
