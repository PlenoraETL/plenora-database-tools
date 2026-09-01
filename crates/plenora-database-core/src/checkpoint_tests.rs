use super::*;
use crate::plan::SortDirection::{Asc, Desc};
use std::sync::Arc;

fn read() -> ReadOperation {
    ReadOperation {
        source: ObjectRef {
            catalog: None,
            schema: Some("app".to_owned()),
            object: "events".to_owned(),
        },
        projection: vec!["tenant_id".to_owned(), "event_id".to_owned()],
        order_by: vec![
            OrderBy {
                field: "tenant_id".to_owned(),
                direction: Asc,
            },
            OrderBy {
                field: "event_id".to_owned(),
                direction: Desc,
            },
        ],
        row_limit: Some(500),
        row_offset: None,
        filter: Some(FilterExpression::Eq {
            field: "active".to_owned(),
            parameter: "active".to_owned(),
        }),
        declared_crs: Vec::new(),
    }
}

#[test]
fn checkpoint_builds_a_strict_composite_keyset_after_the_original_filter() {
    let operation = read();
    let parameters = ParameterBag::new(BTreeMap::from([(
        "active".to_owned(),
        ParameterValue::Bool(true),
    )]));
    let checkpoint = ReadCheckpoint::new(
        ProviderKind::Postgres,
        &operation,
        &parameters,
        vec![ParameterValue::I64(7), ParameterValue::I64(42)],
    )
    .expect("checkpoint");

    let (resumed, bound) = checkpoint
        .resume(ProviderKind::Postgres, &operation, &parameters)
        .expect("resume");

    assert_eq!(bound.len(), 3);
    assert_eq!(
        bound.get("__plenora_resume_0"),
        Some(&ParameterValue::I64(7))
    );
    assert_eq!(
        bound.get("__plenora_resume_1"),
        Some(&ParameterValue::I64(42))
    );
    assert_eq!(
        resumed.filter,
        Some(FilterExpression::And {
            args: vec![
                operation.filter.expect("original filter"),
                FilterExpression::Or {
                    args: vec![
                        FilterExpression::Gt {
                            field: "tenant_id".to_owned(),
                            parameter: "__plenora_resume_0".to_owned(),
                        },
                        FilterExpression::And {
                            args: vec![
                                FilterExpression::Eq {
                                    field: "tenant_id".to_owned(),
                                    parameter: "__plenora_resume_0".to_owned(),
                                },
                                FilterExpression::Lt {
                                    field: "event_id".to_owned(),
                                    parameter: "__plenora_resume_1".to_owned(),
                                },
                            ],
                        },
                    ],
                },
            ],
        })
    );
}

#[test]
fn checkpoint_round_trips_and_can_be_captured_from_a_row() {
    let operation = read();
    let row = Row::try_new(
        Arc::from(["tenant_id".to_owned(), "event_id".to_owned()]),
        vec![ParameterValue::I64(9), ParameterValue::I64(3)],
    )
    .expect("row");
    let checkpoint = ReadCheckpoint::from_row(
        ProviderKind::Db2,
        &operation,
        &ParameterBag::default(),
        &row,
    )
    .expect("from row");
    let json = checkpoint.to_json().expect("json");
    assert_eq!(
        ReadCheckpoint::from_json(&json).expect("round trip"),
        checkpoint
    );
}

#[test]
fn checkpoint_is_fail_closed_for_wrong_scope_offsets_and_unordered_values() {
    let operation = read();
    let checkpoint = ReadCheckpoint::new(
        ProviderKind::Mysql,
        &operation,
        &ParameterBag::default(),
        vec![ParameterValue::I64(1), ParameterValue::I64(2)],
    )
    .expect("checkpoint");
    assert!(checkpoint
        .resume(ProviderKind::Mariadb, &operation, &ParameterBag::default())
        .is_err());

    let mut offset = operation.clone();
    offset.row_offset = Some(1);
    assert!(checkpoint
        .resume(ProviderKind::Mysql, &offset, &ParameterBag::default())
        .is_err());

    assert!(ReadCheckpoint::new(
        ProviderKind::Mysql,
        &operation,
        &ParameterBag::default(),
        vec![
            ParameterValue::Null {
                type_name: "bigint".to_owned()
            },
            ParameterValue::I64(2)
        ],
    )
    .is_err());
}

#[test]
fn reserved_parameter_collision_is_rejected_without_exposing_the_value() {
    let operation = read();
    let checkpoint = ReadCheckpoint::new(
        ProviderKind::Sqlserver,
        &operation,
        &ParameterBag::default(),
        vec![ParameterValue::I64(1), ParameterValue::I64(2)],
    )
    .expect("checkpoint");
    let parameters = ParameterBag::new(BTreeMap::from([(
        "__plenora_resume_0".to_owned(),
        ParameterValue::String("private".to_owned()),
    )]));
    let error = checkpoint
        .resume(ProviderKind::Sqlserver, &operation, &parameters)
        .expect_err("collision");
    assert!(!error.message.contains("private"));
}

#[test]
fn checkpoint_rejects_a_changed_logical_scope_but_allows_a_new_page_size() {
    let operation = read();
    let parameters = ParameterBag::new(BTreeMap::from([(
        "active".to_owned(),
        ParameterValue::Bool(true),
    )]));
    let checkpoint = ReadCheckpoint::new(
        ProviderKind::Postgres,
        &operation,
        &parameters,
        vec![ParameterValue::I64(7), ParameterValue::I64(42)],
    )
    .expect("checkpoint");

    let changed_parameters = ParameterBag::new(BTreeMap::from([(
        "active".to_owned(),
        ParameterValue::Bool(false),
    )]));
    assert!(checkpoint
        .resume(ProviderKind::Postgres, &operation, &changed_parameters)
        .is_err());

    let mut next_page = operation;
    next_page.row_limit = Some(25);
    assert!(checkpoint
        .resume(ProviderKind::Postgres, &next_page, &parameters)
        .is_ok());
}
