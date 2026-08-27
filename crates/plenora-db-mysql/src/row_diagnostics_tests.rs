use super::{checked_batch_end, checked_consumed_batch_end};
use crate::profile::ProductProfile;
use plenora_database_core::row_diagnostics::CAUSE_CONSTRAINT_VIOLATION;

/// Le cause row-scoped nascono dal codice del server, non dal testo: ogni
/// codice di vincolo mappa sulla causa di contratto, e nessun altro.
#[test]
fn row_rejection_causes_come_from_server_codes_only() {
    for code in [1_048_u16, 1_062, 1_452, 3_819, 4_025] {
        assert_eq!(
            crate::profile::MYSQL_PROFILE.row_rejection_cause(code),
            Some(CAUSE_CONSTRAINT_VIOLATION),
            "il codice {code} dichiara un rifiuto di riga"
        );
    }
    for code in [0_u16, 1_045, 1_213, 1_205, 2_006, 65_535] {
        assert_eq!(
            crate::profile::MYSQL_PROFILE.row_rejection_cause(code),
            None,
            "il codice {code} non appartiene a un rifiuto di riga"
        );
    }
}

#[test]
fn a_current_batch_with_rows_beyond_the_declared_total_is_rejected() {
    assert_eq!(checked_consumed_batch_end(4_000, 1_200, 5_200), Ok(5_200));
    assert!(checked_consumed_batch_end(4_000, 1_201, 5_200).is_err());
}

#[test]
fn source_offsets_advance_absolutely_across_batch_boundaries() {
    let second_batch_start = checked_batch_end(0, 1_000).expect("primo batch");
    assert_eq!(second_batch_start, 1_000);
    assert_eq!(
        checked_batch_end(second_batch_start, 25).expect("secondo batch"),
        1_025
    );
    assert!(checked_batch_end(u64::MAX, 1).is_err());
}
