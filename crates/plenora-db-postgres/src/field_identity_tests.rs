use super::*;

#[test]
fn registration_preserves_foreign_comments_and_unbounded_ids() {
    let foreign = "owner's comment\\with\nUnicode: città";
    let id = "0001234567890123456789012345678901234567890";
    let comment = registration(Some(foreign), id).unwrap().unwrap();
    assert_eq!(registered_id(Some(&comment)).unwrap(), Some(id));
    assert!(comment.ends_with(foreign));
    assert!(registration(Some(&comment), id.trim_start_matches('0'))
        .unwrap()
        .is_none());
    assert_eq!(registered_id(Some(foreign)).unwrap(), None);
    assert_eq!(registered_id(None).unwrap(), None);
}

#[test]
fn conflicting_and_corrupt_identity_fail_without_payload() {
    let comment = registration(None, "987654321").unwrap().unwrap();
    let error = registration(Some(&comment), "123456789").unwrap_err();
    assert_eq!(error.category, ErrorCategory::Conflict);
    assert!(!error.message.contains("987654321"));
    assert!(!error.message.contains("123456789"));
    for comment in ["plenora.field_id.v1:secret", "plenora.field_id.v1:secret\n"] {
        let error = registered_id(Some(comment)).unwrap_err();
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert!(!error.message.contains("secret"));
    }
}
