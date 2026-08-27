use super::validate_options;
use plenora_database_core::native_query_policy::NativeQueryPolicy;
use plenora_database_core::session_context::{SessionEntry, SessionValue};
use plenora_database_core::transaction::{AccessMode, TransactionOptions};
use plenora_database_core::ErrorCategory;

#[test]
fn unsupported_options_fail_closed_before_io() {
    let read_only = TransactionOptions {
        access_mode: Some(AccessMode::ReadOnly),
        ..TransactionOptions::default()
    };
    assert_eq!(
        validate_options(&read_only)
            .expect_err("read-only")
            .category,
        ErrorCategory::Unsupported
    );

    let timeout = TransactionOptions {
        statement_timeout_ms: Some(50),
        ..TransactionOptions::default()
    };
    assert_eq!(
        validate_options(&timeout).expect_err("timeout").category,
        ErrorCategory::Unsupported
    );

    let mut with_context = TransactionOptions::default();
    with_context
        .context
        .insert(
            "app.tenant",
            SessionEntry::public(SessionValue::Text("tenant".to_owned())),
        )
        .expect("context valido");
    assert_eq!(
        validate_options(&with_context)
            .expect_err("context")
            .category,
        ErrorCategory::Unsupported
    );
}

#[test]
fn native_query_policy_is_an_enforced_option() {
    let options = TransactionOptions {
        native_query_policy: NativeQueryPolicy::Deny,
        ..TransactionOptions::default()
    };
    validate_options(&options).expect("la policy e applicata dagli statement");
}
