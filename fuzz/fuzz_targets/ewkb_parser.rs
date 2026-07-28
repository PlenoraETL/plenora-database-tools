#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_database_core::ewkb::{inspect_ewkb, inspect_ewkb_detailed};
use plenora_database_core::ErrorCategory;

fuzz_target!(|input: &[u8]| {
    let (control, payload) = if input.len() >= 3 {
        (&input[..3], &input[3..])
    } else {
        (&[0_u8, 0, 0][..], input)
    };
    let max_components =
        u64::from(u16::from_le_bytes([control[0], control[1]])).saturating_add(1);
    let max_depth = u64::from(control[2] % 64).saturating_add(1);

    assert_eq!(
        inspect_ewkb(payload, 0, max_depth)
            .expect_err("zero component limit must fail closed")
            .category,
        ErrorCategory::ResourceLimit
    );
    assert_eq!(
        inspect_ewkb(payload, max_components, 0)
            .expect_err("zero depth limit must fail closed")
            .category,
        ErrorCategory::ResourceLimit
    );

    let simple = inspect_ewkb(payload, max_components, max_depth);
    let detailed = inspect_ewkb_detailed(payload, max_components, max_depth);
    match (simple, detailed) {
        (Ok(stats), Ok(inspection)) => {
            assert_eq!(stats, inspection.stats);
            assert!(stats.components > 0);
            assert!(stats.components <= max_components);
            assert!(stats.max_depth > 0);
            assert!(stats.max_depth <= max_depth);
            assert!(inspection.root.geometry_type_name().is_some());
            assert!(matches!(
                inspection.root.dimensions_label(),
                "xy" | "xyz" | "xym" | "xyzm"
            ));

            let looser = inspect_ewkb_detailed(
                payload,
                stats.components.saturating_add(1),
                stats.max_depth.saturating_add(1),
            )
            .expect("loosening accepted limits must preserve acceptance");
            assert_eq!(inspection, looser);

            if stats.components > 1 {
                assert_eq!(
                    inspect_ewkb(payload, stats.components - 1, max_depth)
                        .expect_err("component limit below observed use must fail")
                        .category,
                    ErrorCategory::ResourceLimit
                );
            }
            if stats.max_depth > 1 {
                assert_eq!(
                    inspect_ewkb(payload, max_components, stats.max_depth - 1)
                        .expect_err("depth limit below observed use must fail")
                        .category,
                    ErrorCategory::ResourceLimit
                );
            }

            let mut trailing = payload.to_vec();
            trailing.push(0);
            assert_eq!(
                inspect_ewkb(&trailing, max_components, max_depth)
                    .expect_err("accepted geometry with trailing bytes must fail")
                    .category,
                ErrorCategory::DataMapping
            );
        }
        (Err(simple_error), Err(detailed_error)) => {
            assert_eq!(simple_error.category, detailed_error.category);
        }
        _ => panic!("simple and detailed EWKB inspection diverged"),
    }
});
