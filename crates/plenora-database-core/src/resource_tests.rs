use super::*;

fn budget(memory_bytes: u64) -> ResourceBudget {
    let limits = ResourceLimits {
        memory_bytes,
        cell_bytes: memory_bytes,
        ..ResourceLimits::default()
    };
    ResourceBudget::new(limits).expect("valid budget")
}

#[test]
fn leases_cross_clones_and_return_quota_on_drop() {
    let budget = budget(100);
    let clone = budget.clone();
    let lease = clone
        .try_lease(ResourceKind::MemoryBytes, 60)
        .expect("lease");
    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), 40);
    assert!(budget.try_lease(ResourceKind::MemoryBytes, 41).is_err());
    drop(lease);
    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), 100);
}

#[test]
fn subtraction_and_input_boundaries_fail_closed() {
    let budget = budget(1);
    assert!(budget.try_lease(ResourceKind::MemoryBytes, 0).is_err());
    assert!(budget
        .try_lease(ResourceKind::MemoryBytes, u64::MAX)
        .is_err());
}

#[test]
fn commit_returns_only_unused_quota() {
    let budget = budget(100);
    let lease = budget
        .try_lease(ResourceKind::MemoryBytes, 80)
        .expect("lease");
    lease.commit(30).expect("commit");
    assert_eq!(budget.remaining(ResourceKind::MemoryBytes), 70);
}

#[test]
fn budget_identity_is_explicit() {
    let first = budget(100);
    assert!(first.is_same_budget(&first.clone()));
    assert!(!first.is_same_budget(&budget(100)));
}

#[test]
fn duration_budget_expires_monotonically() {
    let limits = ResourceLimits {
        duration_ms: 1,
        ..ResourceLimits::default()
    };
    let budget = ResourceBudget::new(limits).expect("budget");
    std::thread::sleep(Duration::from_millis(5));
    assert!(budget.ensure_active().is_err());
    assert!(budget.remaining_duration().is_none());
}
