use super::*;

#[test]
fn fetch_deadline_is_overall_and_not_a_page_count() {
    let progress = FetchProgress::new(Duration::from_secs(30));
    let before = progress.deadline - Duration::from_secs(1);
    // Arbitrarily many empty or nonempty pages do not change the deadline.
    for _ in 0..100 {
        assert_eq!(
            progress.remaining_at(before).unwrap(),
            Duration::from_secs(1)
        );
    }
    for now in [
        progress.deadline,
        progress.deadline + Duration::from_secs(1),
    ] {
        let error = progress.remaining_at(now).unwrap_err();
        assert_eq!(error.category, ErrorCategory::Timeout);
        assert_eq!(error.phase, ErrorPhase::Read);
    }
}
