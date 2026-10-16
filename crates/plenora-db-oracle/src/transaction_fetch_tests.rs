use super::*;

#[test]
fn empty_pages_can_be_transient_but_not_persistent() {
    let mut progress = FetchProgress::default();
    for _ in 0..20 {
        progress.observe(0, true).unwrap();
        progress.observe(1, true).unwrap();
    }
    for _ in 0..7 {
        progress.observe(0, true).unwrap();
    }
    let error = progress.observe(0, true).unwrap_err();
    assert_eq!(error.category, ErrorCategory::Protocol);
    assert_eq!(error.phase, ErrorPhase::Read);
    assert_eq!(
        error.message,
        "FETCH Oracle senza avanzamento dopo pagine vuote consecutive"
    );
    progress.observe(0, false).unwrap();
    progress.observe(0, true).unwrap();
}
