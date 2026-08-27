use super::decode_mapping_error;

#[test]
fn public_decode_error_contains_only_operational_context() {
    let error = decode_mapping_error(7, "decimal");
    assert_eq!(error.message, "decode decimal idx=7 fallito");
}
