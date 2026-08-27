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
