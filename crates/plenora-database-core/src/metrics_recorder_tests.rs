use super::*;

#[test]
fn metric_name_as_str_matches_expected_dot_names() {
    assert_eq!(
        MetricName::DbOperationDuration.as_str(),
        "db.operation.duration"
    );
    assert_eq!(MetricName::DbPoolWait.as_str(), "db.pool.wait");
    assert_eq!(
        MetricName::DbTransactionOutcomeUnknown.as_str(),
        "db.transaction.outcome_unknown"
    );
}

#[test]
fn noop_recorder_does_not_panic() {
    let recorder = NoopRecorder;
    recorder.record(MetricEvent::new(MetricName::DbRetry, MetricValue::Count(1)));
}

#[test]
fn collect_recorder_captures_events_in_order() {
    let recorder = CollectRecorder::new();
    recorder.record(MetricEvent::new(
        MetricName::DbOperationDuration,
        MetricValue::DurationMs(42),
    ));
    recorder.record(
        MetricEvent::new(MetricName::DbRowsRead, MetricValue::Rows(100))
            .with_tags(MetricTags::new().with_provider(ProviderKind::Postgres)),
    );
    let snapshot = recorder.snapshot();
    assert_eq!(snapshot.len(), 2);
    assert_eq!(snapshot[0].name, MetricName::DbOperationDuration);
    assert_eq!(snapshot[1].name, MetricName::DbRowsRead);
    assert_eq!(
        snapshot[1].tags.provider_family,
        Some(ProviderKind::Postgres)
    );
}

#[test]
fn event_serializes_with_snake_case_names() {
    let event = MetricEvent::new(
        MetricName::DbTransactionOutcomeUnknown,
        MetricValue::Count(1),
    )
    .with_tags(
        MetricTags::new()
            .with_provider(ProviderKind::Postgres)
            .with_operation(OperationKind::Transaction),
    );
    let json = serde_json::to_string(&event).expect("serialize");
    assert!(json.contains("db_transaction_outcome_unknown"));
    assert!(json.contains("provider_family"));
    assert!(json.contains("postgres"));
    assert!(json.contains("transaction"));
}

#[test]
fn tags_are_all_optional() {
    let tags = MetricTags::default();
    assert!(tags.provider_family.is_none());
    assert!(tags.operation_kind.is_none());
    assert!(tags.execution_id.is_none());
    assert!(tags.correlation_id.is_none());
}

#[test]
fn shared_recorder_is_arc_dispatched() {
    let recorder: SharedRecorder = Arc::new(CollectRecorder::new());
    recorder.record(MetricEvent::new(
        MetricName::DbCancelled,
        MetricValue::Count(1),
    ));
}
