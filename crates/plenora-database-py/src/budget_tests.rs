use super::{session_budget, write_bulk_budget};
use std::time::{Duration, Instant};

/// I due preset si costruiscono, ed e la prova dell'`expect` che li apre:
/// ogni condizione che `ResourceBudget::new` controlla e soddisfatta.
#[test]
fn both_presets_build_within_every_check_of_resource_budget_new() {
    for budget in [session_budget(), write_bulk_budget()] {
        let limits = budget.limits();
        assert!(limits.validate().is_ok());
        assert!(limits.rows > 0 && limits.memory_bytes > 0 && limits.duration_ms > 0);
        assert!(limits.cell_bytes <= limits.memory_bytes);
        assert!(Instant::now()
            .checked_add(Duration::from_millis(limits.duration_ms))
            .is_some());
    }
}
