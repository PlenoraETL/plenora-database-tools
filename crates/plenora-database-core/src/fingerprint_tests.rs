use super::fingerprint::canonical_json_sha256;

#[test]
fn digest_is_stable_and_lowercase() {
    let digest = canonical_json_sha256(&(1_u8, "x")).expect("serializza");
    assert_eq!(digest.len(), 64);
    assert!(digest.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(digest, canonical_json_sha256(&(1_u8, "x")).expect("ripete"));
}
