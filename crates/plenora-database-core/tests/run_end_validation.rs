//! RUSTSEC-2026-0442: il costruttore pubblico deve rifiutare run insufficienti.
use plenora_database_core::arrow::array::types::{Int16Type, Int32Type, Int64Type};
use plenora_database_core::arrow::array::{Array, Int16Array, Int32Array, Int64Array, RunArray};

#[test]
fn run_ends_cover_length_and_offset() {
    macro_rules! check {
        ($index:ty, $array:ty) => {{
            let runs = RunArray::<$index>::try_new(
                &<$array>::from(vec![2, 3]),
                &Int32Array::from(vec![7, 9]),
            )
            .expect("valid runs");
            for (offset, length) in [(0, 5), (2, 2), (3, 1)] {
                assert!(runs
                    .to_data()
                    .into_builder()
                    .offset(offset)
                    .len(length)
                    .build()
                    .is_err());
            }
            assert!(runs
                .to_data()
                .into_builder()
                .offset(1)
                .len(2)
                .build()
                .is_ok());
        }};
    }
    check!(Int16Type, Int16Array);
    check!(Int32Type, Int32Array);
    check!(Int64Type, Int64Array);
}
