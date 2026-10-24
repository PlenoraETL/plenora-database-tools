use super::*;

#[test]
fn registration_preserves_foreign_comments_and_unbounded_ids() {
    let foreign = "owner's comment\\with\nUnicode: città";
    let id = "0001234567890123456789012345678901234567890";
    let comment = registration(Some(foreign), id).unwrap().unwrap();
    // Si registra e si rilegge la forma canonica (GEO-016).
    let canonical = id.trim_start_matches('0');
    assert_eq!(
        registered_id(Some(&comment)).unwrap().as_deref(),
        Some(canonical)
    );
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

/// Un commento registrato con zeri iniziali, come le versioni precedenti lo
/// scrivevano, resta leggibile e si pubblica canonico (deviazione GEO-016).
/// Fino al 24/10 la lettura lo pubblicava tale e quale, e la verifica dello
/// schema faceva fallire `query` sulla tabella.
#[test]
fn legacy_registered_id_with_leading_zeros_is_read_canonical() {
    let comment = "plenora.field_id.v1:0042\nowner";
    assert_eq!(registered_id(Some(comment)).unwrap().as_deref(), Some("42"));
    assert_eq!(
        registered_id(Some("plenora.field_id.v1:000\n"))
            .unwrap()
            .as_deref(),
        Some("0")
    );
    // Lo stesso id in forma canonica non e un conflitto.
    assert!(registration(Some(comment), "42").unwrap().is_none());
}
