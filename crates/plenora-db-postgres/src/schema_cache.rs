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
#[path = "schema_cache_tests.rs"]
mod tests;
