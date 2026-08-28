use crate::catalog::{build_indexes, schema_token};
use crate::{Db2Column, Db2Index, Db2ObjectSummary};
use plenora_database_core::ErrorCategory;

fn text(value: &str) -> String {
    value.to_owned()
}

#[test]
fn index_rows_are_grouped_in_catalog_order() {
    let rows = vec![
        vec![
            Some(text("PK_ITEMS")),
            Some(text("P")),
            Some(text("1")),
            Some(text("ID")),
            Some(text("A")),
        ],
        vec![
            Some(text("UQ_ITEMS")),
            Some(text("U")),
            Some(text("1")),
            Some(text("CODE")),
            Some(text("A")),
        ],
        vec![
            Some(text("UQ_ITEMS")),
            Some(text("U")),
            Some(text("2")),
            Some(text("REV")),
            Some(text("D")),
        ],
    ];

    let indexes = build_indexes(&rows).expect("indici Db2");
    assert_eq!(indexes.len(), 2);
    assert!(indexes[0].primary && indexes[0].unique);
    assert_eq!(indexes[0].columns, ["ID"]);
    assert!(indexes[1].unique && !indexes[1].primary);
    assert_eq!(indexes[1].columns, ["CODE", "REV"]);
    assert_eq!(indexes[1].descending, [false, true]);
}

#[test]
fn a_non_contiguous_index_fails_closed() {
    let rows = vec![vec![
        Some(text("UQ_ITEMS")),
        Some(text("U")),
        Some(text("2")),
        Some(text("CODE")),
        Some(text("A")),
    ]];

    let error = build_indexes(&rows).expect_err("sequenza indice non valida");
    assert_eq!(error.category, ErrorCategory::DataMapping);
}

#[test]
fn schema_token_is_stable_and_observes_column_and_index_changes() {
    let summary = Db2ObjectSummary {
        schema: "PLENORA_TEST".to_owned(),
        name: "ITEMS".to_owned(),
        kind: "TABLE".to_owned(),
    };
    let column = Db2Column {
        name: "ID".to_owned(),
        ordinal: 1,
        data_type: "INTEGER".to_owned(),
        length: 4,
        scale: 0,
        nullable: false,
        default_expression: None,
        generated: false,
        identity: false,
    };
    let index = Db2Index {
        name: "PK_ITEMS".to_owned(),
        unique: true,
        primary: true,
        columns: vec!["ID".to_owned()],
        descending: vec![false],
    };

    let first = schema_token(
        &summary,
        std::slice::from_ref(&column),
        std::slice::from_ref(&index),
    )
    .expect("token Db2");
    let same = schema_token(
        &summary,
        std::slice::from_ref(&column),
        std::slice::from_ref(&index),
    )
    .expect("token Db2 stabile");
    let changed = schema_token(
        &summary,
        &[Db2Column {
            nullable: true,
            ..column
        }],
        &[index],
    )
    .expect("token Db2 modificato");

    assert_eq!(first, same);
    assert_ne!(first, changed);
}
