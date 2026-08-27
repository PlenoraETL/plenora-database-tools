use super::*;

#[test]
fn schema_fingerprint_is_stable_and_sensitive() {
    let first = hex_digest(b"schema-a").expect("digest");
    assert_eq!(first, hex_digest(b"schema-a").expect("same digest"));
    assert_ne!(first, hex_digest(b"schema-b").expect("different digest"));
    assert_eq!(first.len(), 64);
}
