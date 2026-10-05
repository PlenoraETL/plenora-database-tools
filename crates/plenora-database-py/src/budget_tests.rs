use super::{session_budget, write_bulk_budget};

/// I due preset si costruiscono: e la prova dell'`expect` che li apre.
#[test]
fn both_presets_build() {
    assert!(session_budget().limits().rows > 0);
    assert!(write_bulk_budget().limits().rows > 0);
}
