use super::*;
use crate::write::plan::TargetLifecycle;
use plenora_database_core::arrow::{DataType, Field, Schema};
use plenora_database_core::plan::WriteMode;
use std::sync::Arc;

#[test]
fn decimal_formatter_handles_boundaries_without_abs_overflow() {
    assert_eq!(decimal_string(12_345, 2).expect("decimal"), "123.45");
    assert_eq!(decimal_string(-1, 3).expect("decimal"), "-0.001");
    assert_eq!(
        decimal_string(i128::MIN, 0).expect("minimum"),
        "-170141183460469231731687303715884105728"
    );
    assert!(decimal_string(1, -1).is_err());
}

#[test]
fn temporal_extremes_fail_without_panicking() {
    assert!(time64(-1).is_err());
    assert!(time64(86_400_000_000).is_err());
    assert!(timestamp(i64::MAX).is_err());
}

#[test]
fn null_key_is_rejected_before_any_row_is_bound() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
    )
    .expect("nullable key batch");
    let plan = WritePlan {
        input_schema: schema,
        columns: vec![WriteColumnPlan {
            input_index: 0,
            name: "id".to_owned(),
            kind: crate::SqlServerColumnKind::I32,
            native_type: "int".to_owned(),
            native_declaration: "int".to_owned(),
            nullable: true,
            collation: None,
            spatial_srid: None,
        }],
        mode: WriteMode::DeleteByKeys,
        row_sql: String::new(),
        key_input_indices: vec![0],
        bulk_table: String::new(),
        bulk_columns_aligned: false,
        lifecycle: TargetLifecycle::Existing {
            lock_sql: String::new(),
            truncate_sql: None,
            add_columns_sql: Vec::new(),
            schema_fingerprint: String::new(),
        },
        schema: "dbo".to_owned(),
        object: "target".to_owned(),
        added_columns: Vec::new(),
        spatial_indexes: Vec::new(),
    };
    let error =
        inspect_batch(&batch, &plan, 1024, 1024, 16).expect_err("NULL key must fail closed");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}

#[test]
fn fixed_width_rows_consume_output_and_memory_bytes() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(Int32Array::from(vec![2_i32])),
        ],
    )
    .expect("fixed-width batch");
    let plan = WritePlan {
        input_schema: schema,
        columns: vec![
            WriteColumnPlan {
                input_index: 0,
                name: "id".to_owned(),
                kind: crate::SqlServerColumnKind::I64,
                native_type: "bigint".to_owned(),
                native_declaration: "bigint".to_owned(),
                nullable: false,
                collation: None,
                spatial_srid: None,
            },
            WriteColumnPlan {
                input_index: 1,
                name: "value".to_owned(),
                kind: crate::SqlServerColumnKind::I32,
                native_type: "int".to_owned(),
                native_declaration: "int".to_owned(),
                nullable: false,
                collation: None,
                spatial_srid: None,
            },
        ],
        mode: WriteMode::Append,
        row_sql: String::new(),
        key_input_indices: Vec::new(),
        bulk_table: String::new(),
        bulk_columns_aligned: false,
        lifecycle: TargetLifecycle::Existing {
            lock_sql: String::new(),
            truncate_sql: None,
            add_columns_sql: Vec::new(),
            schema_fingerprint: String::new(),
        },
        schema: "dbo".to_owned(),
        object: "target".to_owned(),
        added_columns: Vec::new(),
        spatial_indexes: Vec::new(),
    };

    let inspection = inspect_row(&batch, 0, &plan, 1024, 1024, 16).expect("fixed-width inspection");
    assert_eq!(inspection.bytes, 12);
}
