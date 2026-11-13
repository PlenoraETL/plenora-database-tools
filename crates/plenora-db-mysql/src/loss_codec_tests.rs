use super::*;
use plenora_database_core::arrow::array::Float32Array;
use plenora_database_core::arrow::schema::TimeUnit;
use plenora_database_core::loss::{LossConsent, MappingPolicy};
use plenora_database_core::{ResourceBudget, ResourceLimits};

fn codec(native: &str, rewrite: StoreRewrite) -> RuleCodec {
    let arrow = if native.starts_with("float") {
        DataType::Float32
    } else if native.starts_with("double") {
        DataType::Float64
    } else {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    };
    RuleCodec::new(ProviderKind::Mysql, 0, arrow, native, rewrite).unwrap()
}

fn context(
    c: &RuleCodec,
    policy: MappingPolicy,
    consent: LossConsent,
    rows: usize,
) -> ExecutionLossContext {
    let budget = ResourceBudget::new(ResourceLimits::default()).unwrap();
    let (mut context, _) = ExecutionLossContext::new(
        ProviderKind::Mysql,
        policy,
        consent,
        c.instances.clone(),
        &budget,
    )
    .unwrap();
    context
        .begin_source_batch(1, rows, c.instances.len(), rows as u64 * 128)
        .unwrap();
    context
}

#[test]
fn temporal_boundaries_are_euclidean_and_checked() {
    for p in 0..=6 {
        let q = 10_i64.pow(6 - u32::from(p));
        for second in [
            -1_000_000,
            0,
            1_000_000,
            86_399_000_000,
            946_684_799_000_000,
        ] {
            for (offset, expected) in [(0, 0), (q, q), (q + 1, if q == 1 { q + 1 } else { q })] {
                assert_eq!(
                    temporal_candidate(second + offset, p, false),
                    Some(second + expected)
                );
            }
            if p < 6 {
                for (offset, expected) in [
                    (q / 2 - 1, 0),
                    (q / 2, q),
                    (q / 2 + 1, q),
                    (q - 1, q),
                    (999_999, 1_000_000),
                ] {
                    assert_eq!(
                        temporal_candidate(second + offset, p, false),
                        Some(second + expected)
                    );
                }
            }
        }
        assert_eq!(temporal_candidate(i64::MIN, p, false), None);
        assert_eq!(temporal_candidate(i64::MAX, p, false), None);
        assert_eq!(temporal_candidate(999_999, p, true), None);
        assert_eq!(temporal_candidate(1_000_000, p, true), Some(1_000_000));
        assert_eq!(temporal_candidate(-62_167_219_200_000_001, p, false), None);
        assert_eq!(
            temporal_candidate(-62_167_219_200_000_000, p, false),
            Some(-62_167_219_200_000_000)
        );
        assert_eq!(
            temporal_candidate(253_402_300_799_999_999, p, false),
            if p == 6 {
                Some(253_402_300_799_999_999)
            } else {
                None
            }
        );
        assert_eq!(
            temporal_candidate(2_147_483_647_999_999, p, true),
            if p == 6 {
                Some(2_147_483_647_999_999)
            } else {
                None
            }
        );
        assert_eq!(temporal_candidate(2_147_483_648_000_000, p, true), None);
    }
    assert_eq!(temporal_candidate(-500_000, 0, false), Some(0));
    assert_eq!(temporal_candidate(-500_001, 0, false), Some(-1_000_000));
    assert_eq!(temporal_candidate(0, 7, false), None);
}

#[test]
fn codec_uses_common_consent_and_replay_does_not_recount() {
    for base in ["datetime", "timestamp"] {
        for p in 0..6 {
            let c = codec(
                &format!("{base}({p})"),
                StoreRewrite::FractionalSeconds { digits: p },
            );
            let q = 10_i64.pow(6 - u32::from(p));
            let array = TimestampMicrosecondArray::from(vec![
                Some(1_000_000 + q / 2),
                Some(1_000_000),
                None,
            ]);
            for policy in [
                MappingPolicy::Strict,
                MappingPolicy::Compatible,
                MappingPolicy::Lossy,
                MappingPolicy::Native,
            ] {
                for consent in [LossConsent::Deny, LossConsent::Accept] {
                    let mut ctx = context(&c, policy, consent, 3);
                    let result = c.evaluate(&mut ctx, &array, 0, false);
                    if policy == MappingPolicy::Strict || consent == LossConsent::Deny {
                        assert!(result.is_err());
                    } else {
                        let (token, value) = result.unwrap();
                        let expected = bind_value(
                            &TimestampMicrosecondArray::from(vec![1_000_000 + q]),
                            0,
                            &MysqlColumnKind::Timestamp,
                        )
                        .unwrap();
                        assert_eq!(value, expected);
                        assert_eq!(value.clone(), expected);
                        for token in token {
                            ctx.ledger.sent_token(token).unwrap();
                            ctx.ledger.applied_token(token).unwrap();
                        }
                    }
                    c.evaluate(&mut ctx, &array, 1, false).unwrap();
                    assert_eq!(
                        c.evaluate(&mut ctx, &array, 2, false).unwrap().1,
                        Value::NULL
                    );
                    assert_eq!(ctx.ledger.report().evaluation.entries[0].would_change, 1);
                    let mut keys = context(&c, policy, consent, 3);
                    assert!(c.evaluate(&mut keys, &array, 0, true).is_err());
                }
            }
        }
    }
}

#[test]
#[allow(clippy::cast_possible_truncation)]
fn negative_zero_is_explicit_and_conservative_floats_stay_closed() {
    let c = codec("double(255,30)", StoreRewrite::NegativeZero);
    for consent in [LossConsent::Accept, LossConsent::Deny] {
        for value in [-0.0, 0.0, 1e-10, -1e-10, 1e225] {
            let mut ctx = context(&c, MappingPolicy::Compatible, consent, 1);
            let result = c.evaluate(&mut ctx, &Float64Array::from(vec![value]), 0, false);
            if value.to_bits() == (-0.0_f64).to_bits() && consent == LossConsent::Deny {
                assert!(result.is_err());
            } else {
                let Value::Double(actual) = result.unwrap().1 else {
                    panic!("double")
                };
                assert_eq!(
                    actual.to_bits(),
                    if value == 0.0 {
                        0.0_f64.to_bits()
                    } else {
                        value.to_bits()
                    }
                );
            }
        }
        for value in [
            f64::from_bits(1),
            f64::MIN_POSITIVE,
            1e-10_f64.next_down(),
            (-1e-10_f64).next_up(),
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            -f64::MAX,
            1e225_f64.next_up(),
        ] {
            let mut ctx = context(&c, MappingPolicy::Compatible, consent, 1);
            assert!(c
                .evaluate(&mut ctx, &Float64Array::from(vec![value]), 0, false)
                .is_err());
            assert!(ctx
                .ledger
                .report()
                .evaluation
                .entries
                .iter()
                .all(|e| e.would_change == 0));
        }
        for native in [
            "float(7,3)",
            "float(10,2)",
            "float(12,0)",
            "double(10,2)",
            "double(16,4)",
            "double(22,0)",
        ] {
            let c = codec(native, StoreRewrite::Every);
            for value in [0.0, 1.0, 0.1] {
                let array: Box<dyn Array> = if native.starts_with("float") {
                    Box::new(Float32Array::from(vec![Some(value as f32), None]))
                } else {
                    Box::new(Float64Array::from(vec![Some(value), None]))
                };
                let mut ctx = context(&c, MappingPolicy::Compatible, consent, 2);
                assert!(c.evaluate(&mut ctx, array.as_ref(), 0, false).is_err());
                assert_eq!(
                    c.evaluate(&mut ctx, array.as_ref(), 1, false).unwrap().1,
                    Value::NULL
                );
            }
        }
    }
    assert!(RuleCodec::new(
        ProviderKind::Mariadb,
        0,
        DataType::Float64,
        "double(255,30)",
        StoreRewrite::NegativeZero
    )
    .is_err());
}

#[test]
fn range_rejection_precedes_loss_accounting() {
    for (native, value) in [
        ("datetime", 253_402_300_799_999_999),
        ("timestamp", 2_147_483_647_499_999),
        ("timestamp", 0),
    ] {
        let c = codec(native, StoreRewrite::FractionalSeconds { digits: 0 });
        let mut ctx = context(&c, MappingPolicy::Compatible, LossConsent::Accept, 1);
        let array = TimestampMicrosecondArray::from(vec![value]);
        let result = c.evaluate(&mut ctx, &array, 0, false);
        if native == "timestamp" && value == 2_147_483_647_499_999 {
            result.unwrap();
        } else {
            assert!(result.is_err());
            assert!(ctx
                .ledger
                .report()
                .evaluation
                .entries
                .iter()
                .all(|e| e.checked == 0));
        }
    }
}

#[test]
fn double_rules_compose_in_both_fixture_orders() {
    for first in [
        StoreRewrite::NegativeZero,
        StoreRewrite::Underflow { min_abs: 1e-10 },
    ] {
        let c = codec("double(255,30)", first);
        assert_eq!(c.instances.len(), 2);
        let mut ctx = context(&c, MappingPolicy::Compatible, LossConsent::Accept, 1);
        let (tokens, value) = c
            .evaluate(&mut ctx, &Float64Array::from(vec![-0.0]), 0, false)
            .unwrap();
        assert_eq!(tokens.len(), 2);
        let Value::Double(value) = value else {
            panic!("double")
        };
        assert_eq!(value.to_bits(), 0.0_f64.to_bits());
        for token in tokens {
            ctx.ledger.sent_token(token).unwrap();
            ctx.ledger.applied_token(token).unwrap();
        }
        ctx.prepare_commit(true).unwrap();
        assert_eq!(ctx.confirmed.effective.len(), 1);
        assert_eq!(ctx.confirmed.effective[0].committed, 1);
        assert_eq!(
            ctx.ledger
                .report()
                .evaluation
                .entries
                .iter()
                .map(|e| e.checked)
                .sum::<u64>(),
            2
        );
    }
}
