use super::*;
use plenora_database_core::resource::ResourceLimits;

fn budget() -> ResourceBudget {
    ResourceBudget::new(ResourceLimits::default()).expect("budget")
}

#[tokio::test]
async fn deadline_guard_propagates_the_budget_deadline() {
    let limits = ResourceLimits {
        duration_ms: 20,
        ..ResourceLimits::default()
    };
    let budget = ResourceBudget::new(limits).expect("budget");
    let parent = CancellationToken::new();
    let guard = DeadlineGuard::new(&parent, &budget).expect("guard");
    guard.token().cancelled().await;
    assert!(guard.token().is_cancelled());
}

#[test]
fn deadline_guard_without_a_runtime_returns_an_error() {
    let error = DeadlineGuard::new(&CancellationToken::new(), &budget())
        .err()
        .expect("runtime assente");
    assert_eq!(error.category, ErrorCategory::Internal);
}

#[test]
fn contract_leases_are_returned_on_drop() {
    let budget = budget();
    let operations = budget.remaining(ResourceKind::ConcurrentOperations);
    let columns = budget.remaining(ResourceKind::Columns);
    {
        let leases = ContractLeases::acquire(&budget, 3).expect("leases");
        assert_eq!(
            budget.remaining(ResourceKind::ConcurrentOperations),
            operations - 1
        );
        assert_eq!(budget.remaining(ResourceKind::Columns), columns - 3);
        drop(leases);
    }
    assert_eq!(
        budget.remaining(ResourceKind::ConcurrentOperations),
        operations
    );
    assert_eq!(budget.remaining(ResourceKind::Columns), columns);
}

#[test]
fn prepared_budget_requires_shared_counters() {
    let prepared = budget();
    assert!(validate_prepared_budget(&prepared, &prepared.clone()).is_ok());
    assert!(validate_prepared_budget(&prepared, &budget()).is_err());
}

#[test]
fn read_batch_reservation_is_fail_closed_on_required_quotas() {
    let budget = budget();
    assert!(ReadBatchReservation::acquire(&budget, 0, None, false).is_err());
    let components = budget.remaining(ResourceKind::GeometryComponents);
    budget
        .try_lease(ResourceKind::GeometryComponents, components)
        .expect("geometry lease")
        .commit(components)
        .expect("consume geometry");
    assert!(ReadBatchReservation::acquire(&budget, 1, None, true).is_err());
}

#[test]
fn read_batch_reservation_releases_unused_quota() {
    let budget = budget();
    let remaining = budget.remaining(ResourceKind::Rows);
    ReadBatchReservation::acquire(&budget, 8, Some(1_024), false)
        .expect("reservation")
        .commit(3, 128, 0)
        .expect("commit");
    assert_eq!(budget.remaining(ResourceKind::Rows), remaining - 3);
}
