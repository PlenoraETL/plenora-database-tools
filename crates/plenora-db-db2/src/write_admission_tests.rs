use super::*;
use plenora_database_core::plan::WriteMode;

fn server() -> ServerFacts {
    ServerFacts {
        major: "12.1".to_owned(),
        version: "DB2 v12.1.5.0".to_owned(),
        codepage: "1208".to_owned(),
        codeset: "UTF-8".to_owned(),
        string_units: "SYSTEM".to_owned(),
        varchar2_compat: false,
    }
}

fn column(type_name: &str, length: u64, scale: i64) -> ColumnFacts {
    ColumnFacts {
        name: "V".to_owned(),
        type_schema: "SYSIBM".to_owned(),
        type_name: type_name.to_owned(),
        length,
        scale,
        codepage: 0,
        string_units: String::new(),
        string_units_length: 0,
        collation: String::new(),
    }
}

fn text(type_name: &str, length: u64, units: &str, units_length: u64) -> ColumnFacts {
    ColumnFacts {
        codepage: 1208,
        string_units: units.to_owned(),
        string_units_length: units_length,
        collation: "SYSIBM.IDENTITY".to_owned(),
        ..column(type_name, length, 0)
    }
}

#[test]
fn la_chiave_porta_la_precisione_frazionaria_di_timestamp() {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
    let (key, _) = admission_key(&timestamp, &column("TIMESTAMP", 10, 3), &server());
    assert_eq!(key.nativo, "TIMESTAMP(3)");
    assert_eq!(key.arrow, "timestamp[us]");
    assert_eq!(key.dimensione, None);
    let (key, _) = admission_key(&timestamp, &column("TIMESTAMP", 10, 6), &server());
    assert_eq!(key.nativo, "TIMESTAMP(6)");
}

#[test]
fn la_chiave_porta_precisione_e_scala_e_lo_schema_del_tipo() {
    let (key, _) = admission_key(
        &DataType::Decimal128(10, 2),
        &column("DECIMAL", 10, 2),
        &server(),
    );
    assert_eq!(key.nativo, "DECIMAL(10,2)");
    let (key, _) = admission_key(&DataType::Int32, &column("INTEGER", 4, 0), &server());
    assert_eq!(key.nativo, "INTEGER");
    // Un tipo fuori da SYSIBM non e mai la chiave di un tipo predefinito.
    let mut distinct = column("INTEGER", 4, 0);
    distinct.type_schema = "APP".to_owned();
    let (key, _) = admission_key(&DataType::Int32, &distinct, &server());
    assert_eq!(key.nativo, "APP.INTEGER");
}

#[test]
fn la_chiave_dei_testi_porta_unita_code_page_e_varchar2_compat() {
    let (key, dimensions) = admission_key(
        &DataType::Utf8,
        &text("VARCHAR", 10, "OCTETS", 10),
        &server(),
    );
    assert_eq!(
        key.nativo,
        "VARCHAR(n OCTETS);cp=1208;db=UTF-8/1208;su=SYSTEM;v2c=OFF"
    );
    assert_eq!(key.dimensione, Some(10));
    assert_eq!(dimensions.chars, Some(10));
    assert_eq!(dimensions.bytes, Some(10));
    // CODEUNITS32: la lunghezza nelle unita della colonna.
    let (key, dimensions) = admission_key(
        &DataType::Utf8,
        &text("CHARACTER", 40, "CODEUNITS32", 10),
        &server(),
    );
    assert_eq!(
        key.nativo,
        "CHARACTER(n CODEUNITS32);cp=1208;db=UTF-8/1208;su=SYSTEM;v2c=OFF"
    );
    assert_eq!(key.dimensione, Some(10));
    assert_eq!(dimensions.chars, Some(10));
    // FOR BIT DATA: code page 0.
    let mut bits = text("VARCHAR", 10, "", 0);
    bits.codepage = 0;
    let (key, _) = admission_key(&DataType::Utf8, &bits, &server());
    assert_eq!(
        key.nativo,
        "VARCHAR(n) FOR BIT DATA;cp=0;db=UTF-8/1208;su=SYSTEM;v2c=OFF"
    );
    // varchar2_compat e la code page del database sono nella chiave.
    let mut other = server();
    other.varchar2_compat = true;
    other.codepage = "819".to_owned();
    other.codeset = "ISO8859-1".to_owned();
    let (key, _) = admission_key(&DataType::Utf8, &text("CLOB", 1024, "OCTETS", 1024), &other);
    assert_eq!(
        key.nativo,
        "CLOB(n OCTETS);cp=1208;db=ISO8859-1/819;su=SYSTEM;v2c=ON"
    );
}

#[test]
fn i_tipi_di_create_hanno_la_chiave_del_catalogo() {
    let timestamp = DataType::Timestamp(TimeUnit::Microsecond, None);
    let (key, _) = admission_key(
        &timestamp,
        &create_facts("V", &timestamp, &server()).expect("timestamp"),
        &server(),
    );
    assert_eq!(key.nativo, "TIMESTAMP(6)");
    let (key, _) = admission_key(
        &DataType::Decimal128(31, 4),
        &create_facts("V", &DataType::Decimal128(31, 4), &server()).expect("decimale"),
        &server(),
    );
    assert_eq!(key.nativo, "DECIMAL(31,4)");
    let (key, _) = admission_key(
        &DataType::Utf8,
        &create_facts("V", &DataType::Utf8, &server()).expect("testo"),
        &server(),
    );
    assert_eq!(
        (key.nativo.as_str(), key.dimensione),
        (
            "CLOB(n OCTETS);cp=1208;db=UTF-8/1208;su=SYSTEM;v2c=OFF",
            Some(2_147_483_647)
        )
    );
    // Con CODEUNITS32 il CLOB di Create non e previsto.
    let mut units = server();
    units.string_units = "CODEUNITS32".to_owned();
    assert!(create_facts("V", &DataType::Utf8, &units).is_none());
    assert!(create_facts("V", &DataType::Binary, &server()).is_none());
}

#[test]
fn la_major_e_versione_e_release() {
    assert_eq!(parse_major("DB2 v12.1.5.0").as_deref(), Some("12.1"));
    assert_eq!(parse_major("DB2 v11.5.9.0").as_deref(), Some("11.5"));
    assert_eq!(parse_major("12.1.5.0"), None);
    assert_eq!(parse_major("DB2 vX.1"), None);
    assert_eq!(parse_major("DB2 v12"), None);
}

fn row(name: &str, value: &str) -> Vec<Option<String>> {
    vec![Some(name.to_owned()), Some(value.to_owned())]
}

#[test]
fn i_parametri_del_database_sono_completi_e_uguali_fra_i_membri() {
    let mut rows = vec![
        row("codepage", "1208"),
        row("codeset", "utf-8"),
        row("string_units", "SYSTEM"),
        row("varchar2_compat", "OFF"),
    ];
    let parameters = parse_parameters(&rows).expect("parametri");
    assert_eq!(parameters["codepage"], "1208");
    rows.push(row("codepage", "1208"));
    assert!(parse_parameters(&rows).is_ok(), "due membri uguali");
    rows.push(row("codepage", "819"));
    let error = parse_parameters(&rows).expect_err("membri diversi");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    let error = parse_parameters(&rows[..3]).expect_err("varchar2_compat assente");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
}

#[test]
fn solo_identity_e_una_collation_binaria() {
    let mut facts = text("VARCHAR", 10, "OCTETS", 10);
    assert!(facts.binary_collation());
    facts.collation = "SYSIBM.IDENTITY_16BIT".to_owned();
    assert!(facts.binary_collation());
    for collation in [
        "SYSIBM.SYSTEM_819",
        "SYSIBM.UCA500R1_S1",
        "SYSIBM.CLDR181_LEN_S2",
        "SYSIBM.NLSCHAR",
        "",
    ] {
        facts.collation = collation.to_owned();
        assert!(!facts.binary_collation(), "{collation}");
    }
}

#[test]
fn le_chiavi_di_testo_sotto_una_collation_non_binaria_si_rifiutano() {
    use crate::write::{refuse_text_keys_under_collation, TEXT_KEYS_UNDER_COLLATION};
    let mut facts = text("VARCHAR", 10, "OCTETS", 10);
    let number = column("INTEGER", 4, 0);
    let utf8 = DataType::Utf8;
    let int = DataType::Int32;
    for mode in [
        WriteMode::Update,
        WriteMode::Upsert,
        WriteMode::DeleteByKeys,
    ] {
        refuse_text_keys_under_collation(mode, &[(&utf8, &facts)]).expect("IDENTITY");
    }
    facts.collation = "SYSIBM.UCA500R1_S1".to_owned();
    for mode in [
        WriteMode::Update,
        WriteMode::Upsert,
        WriteMode::DeleteByKeys,
    ] {
        let error = refuse_text_keys_under_collation(mode, &[(&int, &number), (&utf8, &facts)])
            .expect_err("chiave di testo sotto una collation non binaria");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
        assert_eq!(error.message, TEXT_KEYS_UNDER_COLLATION);
        refuse_text_keys_under_collation(mode, &[(&int, &number)]).expect("chiave non di testo");
    }
    refuse_text_keys_under_collation(WriteMode::Append, &[]).expect("senza chiavi");
}

#[test]
fn il_fixture_incorporato_e_valido() {
    admission_table().expect("fixture della regola A per Db2");
}
