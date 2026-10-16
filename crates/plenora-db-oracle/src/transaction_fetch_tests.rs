use super::*;

#[test]
fn only_empty_fetch_waits_exhaust_progress_budget() {
    let mut progress = FetchProgress::new(Duration::from_secs(30));
    for _ in 0..100 {
        progress.record(Duration::from_secs(20), true);
        assert_eq!(progress.remaining().unwrap(), Duration::from_secs(30));
    }
    progress.record(Duration::from_secs(10), false);
    assert_eq!(progress.remaining().unwrap(), Duration::from_secs(20));
    progress.record(Duration::from_secs(20), false);
    let error = progress.remaining().unwrap_err();
    assert_eq!(error.category, ErrorCategory::Timeout);
    assert_eq!(error.phase, ErrorPhase::Read);
    progress.record(Duration::ZERO, true);
    assert_eq!(progress.remaining().unwrap(), Duration::from_secs(30));
}

#[tokio::test]
async fn consumer_and_lob_time_do_not_spend_fetch_budget() {
    let progress = FetchProgress::new(Duration::from_millis(1));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(progress.remaining().unwrap(), Duration::from_millis(1));
}
