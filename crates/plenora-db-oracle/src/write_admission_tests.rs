use super::*;

fn server() -> ServerFacts {
    ServerFacts {
        major: "23".to_owned(),
        version: "23.0.0.0.0".to_owned(),
        charset: "AL32UTF8".to_owned(),
        national_charset: "AL16UTF16".to_owned(),
        nls_comp: "BINARY".to_owned(),
    }
}

fn column(data_type: &str, precision: Option<u64>, scale: Option<i64>) -> ColumnFacts {
    ColumnFacts {
        name: "V".to_owned(),
        data_type: data_type.to_owned(),
        data_length: 40,
        char_length: 10,
        char_used: None,
        precision,
        scale,
        collation: None,
    }
}

#[test]
fn la_chiave_porta_precisione_scala_semantica_e_charset() {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
    let (key, _) = admission_key(
        &timestamp,
        &column("TIMESTAMP(3)", None, Some(3)),
        &server(),
    );
    assert_eq!(key.nativo, "TIMESTAMP(3)");
    assert_eq!(key.arrow, "timestamp[us]");
    let (key, _) = admission_key(
        &DataType::Decimal128(10, 2),
        &column("NUMBER", Some(10), Some(2)),
        &server(),
    );
    assert_eq!(key.nativo, "NUMBER(10,2)");
    let (key, _) = admission_key(
        &DataType::Int64,
        &column("NUMBER", None, Some(0)),
        &server(),
    );
    assert_eq!(key.nativo, "NUMBER(*,0)");
    let (key, _) = admission_key(
        &DataType::Float64,
        &column("FLOAT", Some(126), None),
        &server(),
    );
    assert_eq!(key.nativo, "FLOAT(126)");
    let mut text = column("VARCHAR2", None, None);
    text.char_used = Some("C".to_owned());
    let (key, dimensions) = admission_key(&DataType::Utf8, &text, &server());
    assert_eq!(key.nativo, "VARCHAR2(n CHAR);AL32UTF8");
    assert_eq!(key.dimensione, Some(10));
    assert_eq!(dimensions.chars, Some(10));
    assert_eq!(dimensions.bytes, Some(40));
    text.char_used = Some("B".to_owned());
    let (key, _) = admission_key(&DataType::Utf8, &text, &server());
    assert_eq!(key.nativo, "VARCHAR2(n BYTE);AL32UTF8");
    assert_eq!(key.dimensione, Some(40));
    let (key, _) = admission_key(&DataType::Utf8, &column("NCHAR", None, None), &server());
    assert_eq!(key.nativo, "NCHAR(n);AL16UTF16");
    let (key, _) = admission_key(&DataType::Binary, &column("RAW", None, None), &server());
    assert_eq!((key.nativo.as_str(), key.dimensione), ("RAW(n)", Some(40)));
    let mut other = server();
    other.charset = "WE8ISO8859P1".to_owned();
    let (key, _) = admission_key(&DataType::Utf8, &column("CLOB", None, None), &other);
    assert_eq!(key.nativo, "CLOB;WE8ISO8859P1");
}

#[test]
fn i_tipi_di_create_hanno_la_chiave_del_catalogo() {
    let (key, _) = admission_key(
        &DataType::Int16,
        &create_facts("V", &DataType::Int16).expect("intero"),
        &server(),
    );
    assert_eq!(key.nativo, "NUMBER(5,0)");
    let (key, _) = admission_key(
        &DataType::Utf8,
        &create_facts("V", &DataType::Utf8).expect("testo"),
        &server(),
    );
    assert_eq!(key.nativo, "CLOB;AL32UTF8");
    assert!(create_facts("V", &DataType::Timestamp(TimeUnit::Nanosecond, None)).is_none());
}

#[test]
fn il_fixture_incorporato_e_valido() {
    admission_table().expect("fixture Oracle");
}

#[test]
fn una_chiave_di_testo_sotto_una_collation_non_binaria_si_rifiuta() {
    let mut text = column("VARCHAR2", None, None);
    text.collation = Some("USING_NLS_COMP".to_owned());
    refuse_non_binary_keys(&[&text], &server()).expect("binaria");
    let mut linguistic = server();
    linguistic.nls_comp = "LINGUISTIC".to_owned();
    let error = refuse_non_binary_keys(&[&text], &linguistic).expect_err("linguistica");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    text.collation = Some("BINARY_CI".to_owned());
    refuse_non_binary_keys(&[&text], &server()).expect_err("BINARY_CI");
    text.collation = None;
    refuse_non_binary_keys(&[&text], &server()).expect_err("collation non provata");
    // Un numero non ha collation.
    let mut number = column("NUMBER", Some(10), Some(0));
    number.collation = Some("BINARY_CI".to_owned());
    refuse_non_binary_keys(&[&number], &server()).expect("numero");
}

#[test]
fn ogni_float_dichiara_la_riscrittura_dello_zero_negativo() {
    use plenora_database_core::store_rewrite::StoreRewrite;
    for arrow in [DataType::Float32, DataType::Float64] {
        assert_eq!(
            store_rewrites(Vec::new(), &arrow),
            vec![StoreRewrite::NegativeZero]
        );
        assert_eq!(
            store_rewrites(vec![StoreRewrite::NegativeZero], &arrow),
            vec![StoreRewrite::NegativeZero]
        );
    }
    assert!(store_rewrites(Vec::new(), &DataType::Int64).is_empty());
    let underflow = StoreRewrite::Underflow { min_abs: 1e-130 };
    assert_eq!(
        store_rewrites(vec![underflow], &DataType::Float64),
        vec![underflow, StoreRewrite::NegativeZero]
    );
}

/// Precisione e scala diverse non sono mai ammesse come conversioni esatte.
#[test]
fn le_chiavi_non_possono_perdere_precisione_o_cambiare_tipo() {
    let measured = admission_table()
        .expect("fixture")
        .server("23")
        .expect("server");
    for (arrow, facts) in [
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            column("TIMESTAMP(0)", None, Some(0)),
        ),
        (
            DataType::Decimal128(10, 3),
            column("NUMBER", Some(10), Some(2)),
        ),
        (DataType::Utf8, column("RAW", None, None)),
    ] {
        let (key, dimensions) = admission_key(&arrow, &facts, &server());
        let admitted =
            plenora_database_core::admission_gate::write_column(&measured, &key, dimensions);
        if matches!(arrow, DataType::Timestamp(_, _)) {
            let rewrites = admitted.expect("precisione misurata come riscrittura");
            let values =
                plenora_database_core::arrow::array::TimestampMicrosecondArray::from(vec![
                    400_000, 0,
                ]);
            assert!(rewrites.iter().any(|rewrite| rewrite.rewrites(&values, 0)));
            assert!(!rewrites.iter().any(|rewrite| rewrite.rewrites(&values, 1)));
        } else {
            assert!(admitted.is_err(), "{key:?}");
        }
    }
}
