use super::*;
use plenora_database_core::arrow::schema::TimeUnit;

fn facts(native: &str, max_length: i16, precision: u8, scale: u8) -> SqlServerWriteFacts {
    SqlServerWriteFacts {
        name: "v".to_owned(),
        native_type: native.to_owned(),
        max_length,
        precision,
        scale,
        collation: None,
        code_page: None,
        ansi_padded: true,
        nullable: true,
    }
}

#[test]
fn la_chiave_porta_precisione_scala_e_frazione() {
    let key = |arrow: DataType, facts: &SqlServerWriteFacts| admission_key(&arrow, facts).0;
    assert_eq!(
        key(DataType::Decimal128(10, 2), &facts("decimal", 9, 10, 2)).nativo,
        "decimal(10,2)"
    );
    assert_eq!(
        key(DataType::Decimal128(10, 2), &facts("numeric", 9, 10, 2)).nativo,
        "numeric(10,2)"
    );
    assert_eq!(
        key(
            DataType::Timestamp(TimeUnit::Microsecond, None),
            &facts("datetime2", 8, 27, 3)
        )
        .nativo,
        "datetime2(3)"
    );
    assert_eq!(
        key(DataType::Utf8, &facts("datetimeoffset", 10, 34, 7)).nativo,
        "datetimeoffset(7)"
    );
    // `float(10)` e `real` nel catalogo.
    assert_eq!(
        key(DataType::Float32, &facts("real", 4, 24, 0)).nativo,
        "real(24)"
    );
    assert_eq!(
        key(DataType::Float64, &facts("float", 8, 53, 0)).nativo,
        "float(53)"
    );
    assert_eq!(
        key(DataType::Decimal128(19, 4), &facts("money", 8, 19, 4)).nativo,
        "money"
    );
    let datetime = key(
        DataType::Timestamp(TimeUnit::Microsecond, None),
        &facts("datetime", 8, 23, 3),
    );
    assert_eq!(datetime.nativo, "datetime");
    assert_eq!(datetime.arrow, "timestamp[us]");
    assert_eq!(datetime.dimensione, None);
}

#[test]
fn la_chiave_dei_testi_porta_code_page_padding_e_nullabilita() {
    let mut text = facts("varchar", 10, 0, 0);
    text.code_page = Some(1252);
    let (key, dimensions) = admission_key(&DataType::Utf8, &text);
    assert_eq!(key.nativo, "varchar(n);cp=1252;ansi_padding=on;null");
    assert_eq!(key.dimensione, Some(10));
    assert_eq!(dimensions.bytes, Some(10));
    text.code_page = Some(65001);
    text.ansi_padded = false;
    text.nullable = false;
    assert_eq!(
        admission_key(&DataType::Utf8, &text).0.nativo,
        "varchar(n);cp=65001;ansi_padding=off;not null"
    );
    text.max_length = -1;
    let (key, _) = admission_key(&DataType::Utf8, &text);
    assert_eq!(
        key.nativo,
        "varchar(max);cp=65001;ansi_padding=off;not null"
    );
    assert_eq!(key.dimensione, None);
    text.code_page = None;
    assert!(admission_key(&DataType::Utf8, &text)
        .0
        .nativo
        .contains("cp=?"));
    let national = facts("nchar", 20, 0, 0);
    let (key, dimensions) = admission_key(&DataType::Utf8, &national);
    assert_eq!(key.nativo, "nchar(n);ansi_padding=on;null");
    assert_eq!(key.dimensione, Some(10));
    assert_eq!(dimensions.chars, Some(10));
    assert_eq!(dimensions.bytes, Some(20));
    let binary = facts("binary", 3, 0, 0);
    let (key, dimensions) = admission_key(&DataType::Binary, &binary);
    assert_eq!(key.nativo, "binary(n);ansi_padding=on;null");
    assert_eq!(dimensions.bytes, Some(3));
}

#[test]
fn i_fatti_di_una_colonna_creata_sono_quelli_del_catalogo() {
    let created = created_facts("v", "nvarchar", "nvarchar(max)", None, Some(1252), true);
    assert_eq!(created.max_length, -1);
    assert!(created.ansi_padded);
    assert_eq!(created.code_page, Some(1252));
    let created = created_facts("v", "nchar", "nchar(10)", None, Some(1252), false);
    assert_eq!(created.max_length, 20);
    assert!(!created.nullable);
    let created = created_facts("v", "decimal", "decimal(12,3)", None, None, true);
    assert_eq!((created.precision, created.scale), (12, 3));
    let created = created_facts("v", "datetime2", "datetime2(6)", None, None, true);
    assert_eq!(created.scale, 6);
    let created = created_facts("v", "int", "int", None, Some(1252), true);
    assert_eq!(created.code_page, None);
    let created = created_facts("v", "float", "float", None, None, true);
    assert_eq!(created.precision, 53);
}

#[test]
fn il_fixture_incorporato_e_valido() {
    let table = admission_table().expect("fixture della regola A valido");
    for major in ["15", "16", "17"] {
        assert!(
            table.server(major).is_ok(),
            "la major {major} della matrice e misurata"
        );
    }
    let text = created_facts("v", "text", "text", None, Some(1252), true);
    assert!(!text.ansi_padded);
}
