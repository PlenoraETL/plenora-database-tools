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
        strict: true,
        precision_losses: Vec::new(),
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
        strict: true,
        precision_losses: Vec::new(),
    };

    let inspection = inspect_row(&batch, 0, &plan, 1024, 1024, 16).expect("fixed-width inspection");
    assert_eq!(inspection.bytes, 12);
}

/// GEO-011: un valore di un tipo fuori dalla lista dichiarata si rifiuta.
/// Fino al 24/10 questo percorso confrontava soltanto SRID e dimensioni, e un
/// poligono in un campo `point` arrivava al server.
#[test]
fn geometry_outside_the_declared_types_is_rejected_before_binding() {
    use plenora_database_core::protocol;
    use std::collections::HashMap;
    let field = Field::new("shape", DataType::Binary, true).with_metadata(HashMap::from(
        [
            (protocol::GEOARROW_EXTENSION_NAME, "geoarrow.wkb"),
            (protocol::FIELD_ID, "0"),
            (protocol::GEOMETRY_ENCODING, "wkb"),
            (protocol::GEOMETRY_DIMENSIONS, "xy"),
            (protocol::GEOMETRY_SPATIAL_SEMANTICS, "geometry"),
            (protocol::GEOMETRY_PRECISION, "float64"),
            (protocol::GEOMETRY_TYPES_DECLARATION, "exact"),
            (protocol::GEOMETRY_TYPES, "point"),
            (protocol::GEOMETRY_SRID, "4326"),
            (protocol::GEOMETRY_CRS_RESOLUTION, "missing"),
        ]
        .map(|(key, value)| (key.to_owned(), value.to_owned())),
    ));
    let schema = Arc::new(Schema::new(vec![field]));
    // Un poligono con un anello chiuso di quattro punti.
    let mut polygon = vec![1_u8, 3, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0];
    for value in [0.0_f64, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0] {
        polygon.extend_from_slice(&value.to_le_bytes());
    }
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(BinaryArray::from(vec![Some(polygon.as_slice())]))],
    )
    .expect("batch geometrico");
    let plan = WritePlan {
        input_schema: schema,
        columns: vec![WriteColumnPlan {
            input_index: 0,
            name: "shape".to_owned(),
            kind: crate::SqlServerColumnKind::Geometry,
            native_type: "geometry".to_owned(),
            native_declaration: "geometry".to_owned(),
            nullable: true,
            collation: None,
            spatial_srid: Some(4326),
        }],
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
        strict: true,
        precision_losses: Vec::new(),
    };
    for error in [
        inspect_batch(&batch, &plan, 1024, 1024, 16).expect_err("tipo fuori lista"),
        inspect_row(&batch, 0, &plan, 1024, 1024, 16)
            .map(|_| ())
            .expect_err("tipo fuori lista"),
    ] {
        assert_eq!(error.category, ErrorCategory::DataMapping);
        assert_eq!(error.phase, ErrorPhase::Write);
    }
}

fn text_plan(native: &str, declaration: &str, collation: Option<&str>, strict: bool) -> WritePlan {
    let schema = Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)]));
    WritePlan {
        input_schema: schema,
        columns: vec![WriteColumnPlan {
            input_index: 0,
            name: "label".to_owned(),
            kind: crate::SqlServerColumnKind::Utf8,
            native_type: native.to_owned(),
            native_declaration: declaration.to_owned(),
            nullable: true,
            collation: collation.map(str::to_owned),
            spatial_srid: None,
        }],
        mode: WriteMode::Append,
        row_sql: String::new(),
        key_input_indices: Vec::new(),
        bulk_table: String::new(),
        bulk_columns_aligned: true,
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
        strict,
        precision_losses: Vec::new(),
    }
}

/// F6: un testo cirillico e cinese verso `varchar(20) COLLATE
/// Latin1_General_CI_AS` si committava con i caratteri fuori dalla code page
/// diventati punti interrogativi; `abc` con tre spazi verso `varchar(3)`
/// perdeva gli spazi. Con `Strict` entrambi si rifiutano, nel preparato e
/// nel bulk.
#[test]
fn la_regola_p_rifiuta_il_testo_che_la_colonna_cambierebbe() {
    let latin = text_plan("varchar", "varchar(20)", Some("Latin1_General_CI_AS"), true);
    let column = &latin.columns[0];
    assert!(check_text(&latin, column, "\u{416}\u{20ac}\u{4e2d}").is_err());
    assert!(check_text(&latin, column, "ascii ok").is_ok());
    // Windows-1252 rappresenta `è` e `€`.
    assert!(check_text(&latin, column, "caff\u{e8} \u{20ac}").is_ok());
    let short = text_plan("varchar", "varchar(3)", Some("Latin1_General_CI_AS"), true);
    assert!(check_text(&short, &short.columns[0], "abc   ").is_err());
    assert!(check_text(&short, &short.columns[0], "ab ").is_ok());
    let national = text_plan("nvarchar", "nvarchar(3)", None, true);
    assert!(check_text(&national, &national.columns[0], "\u{416}\u{20ac}\u{4e2d}").is_ok());
    assert!(check_text(&national, &national.columns[0], "\u{416}\u{20ac}\u{4e2d} ").is_err());
    let lossy = text_plan("varchar", "varchar(3)", Some("Latin1_General_CI_AS"), false);
    assert!(check_text(&lossy, &lossy.columns[0], "\u{416}bc   ").is_ok());
    let batch = RecordBatch::try_new(
        Arc::clone(&latin.input_schema),
        vec![Arc::new(StringArray::from(vec![Some(
            "\u{416}\u{20ac}\u{4e2d}",
        )]))],
    )
    .expect("batch");
    assert!(bulk_rows(&latin, &batch).is_err());
    assert!(bind_row(&latin, &batch, 0).is_err());
}

/// S7: piu di nove cifre frazionarie si rifiutano in ogni politica; su
/// 04029d4 chrono le troncava e `.12345670001` si committava `.1234567`.
#[test]
fn datetimeoffset_rifiuta_piu_di_nove_cifre_frazionarie() {
    assert!(datetimeoffset("2026-01-02T10:00:00.123456789+00:00").is_ok());
    let refused = datetimeoffset("2026-01-02T10:00:00.12345670001+00:00").expect_err("dieci cifre");
    assert_eq!(refused.category, ErrorCategory::DataMapping);
    assert!(datetimeoffset("2026-01-02T10:00:00.1234567890Z").is_err());
}

/// I valori esatti verso una colonna temporale ridotta passano con
/// `Strict`; quelli che arrotonderebbero si rifiutano.
#[test]
fn la_regola_p_giudica_il_valore_temporale() {
    let mut plan = text_plan("datetime2", "datetime2(0)", None, true);
    plan.columns[0].kind = crate::SqlServerColumnKind::Timestamp;
    assert!(check_temporal(&plan, &plan.columns[0], 1_767_348_001_000_000).is_ok());
    assert!(check_temporal(&plan, &plan.columns[0], 1_767_348_001_500_000).is_err());
    assert!(check_temporal(&plan, &plan.columns[0], -1_000_000).is_ok());
    assert!(check_temporal(&plan, &plan.columns[0], -1).is_err());
    plan.columns[0].native_type = "datetime".to_owned();
    plan.columns[0].native_declaration = "datetime".to_owned();
    assert!(check_temporal(&plan, &plan.columns[0], 10_000).is_ok());
    assert!(check_temporal(&plan, &plan.columns[0], 3_000).is_err());
    plan.columns[0].native_type = "smalldatetime".to_owned();
    plan.columns[0].native_declaration = "smalldatetime".to_owned();
    assert!(check_temporal(&plan, &plan.columns[0], 60_000_000).is_ok());
    assert!(check_temporal(&plan, &plan.columns[0], 1_000_000).is_err());
    let lossy = text_plan("datetime2", "datetime2(0)", None, false);
    assert!(check_temporal(&lossy, &lossy.columns[0], 1).is_ok());
}

#[test]
fn la_regola_p_rifiuta_le_frazioni_oltre_la_scala_di_datetimeoffset() {
    let mut plan = text_plan("datetimeoffset", "datetimeoffset(0)", None, true);
    plan.columns[0].kind = crate::SqlServerColumnKind::TimestampTz;
    let exact = datetimeoffset("2026-01-02T10:00:00Z").expect("rfc3339");
    let fraction = datetimeoffset("2026-01-02T10:00:00.999999Z").expect("rfc3339");
    assert!(check_offset(&plan, &plan.columns[0], &exact).is_ok());
    assert!(check_offset(&plan, &plan.columns[0], &fraction).is_err());
    plan.columns[0].native_declaration = "datetimeoffset(7)".to_owned();
    assert!(check_offset(&plan, &plan.columns[0], &fraction).is_ok());
}
