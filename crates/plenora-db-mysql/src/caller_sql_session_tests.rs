use super::*;

impl MysqlSession {
    pub(crate) fn without_connection(profile: &'static dyn ProductProfile) -> Self {
        Self {
            profile,
            connection: None,
            expected_profile: None,
            raw_write_probe: false,
            expected_database: "warehouse".to_owned(),
            state: MysqlSessionState::Ready,
            operation_timeout: std::time::Duration::from_secs(1),
            pool_permit: None,
        }
    }
}

#[tokio::test]
async fn raw_probe_bypass_is_local_to_the_measurement_session() {
    let cancel = CancellationToken::new();
    let mut raw = MysqlSession::without_connection(&crate::profile::MYSQL_PROFILE);
    raw.raw_write_probe = true;
    raw.check_warnings(ErrorPhase::Write, &cancel)
        .await
        .unwrap();
    raw.verify_session_profile(ErrorPhase::Commit, &cancel)
        .await
        .unwrap();
    crate::write::verify_write_session(&mut raw, &cancel)
        .await
        .unwrap();
    drop(raw);
    // Una sessione distinta (replay compreso) deve ancora attraversare i controlli.
    let mut replay = MysqlSession::without_connection(&crate::profile::MYSQL_PROFILE);
    assert!(replay
        .check_warnings(ErrorPhase::Write, &cancel)
        .await
        .is_err());
    assert!(replay
        .verify_session_profile(ErrorPhase::Commit, &cancel)
        .await
        .is_err());
    drop(replay);
}
