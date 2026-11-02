use super::*;

impl MysqlSession {
    pub(crate) fn without_connection(profile: &'static dyn ProductProfile) -> Self {
        Self {
            profile,
            connection: None,
            expected_profile: None,
            expected_database: "warehouse".to_owned(),
            state: MysqlSessionState::Ready,
            operation_timeout: std::time::Duration::from_secs(1),
            pool_permit: None,
        }
    }
}
