use super::*;
use plenora_database_core::arrow::schema::TimeUnit;

fn facts(data_type: &str, column_type: &str, charset: Option<&str>) -> ColumnFacts {
    ColumnFacts {
        name: "V".to_owned(),
        data_type: data_type.to_owned(),
        column_type: column_type.to_owned(),
        character_set: charset.map(str::to_owned),
        json_valid: false,
    }
}

#[test]
fn la_major_e_major_punto_minor() {
    assert_eq!(server_major("8.4.11").as_deref(), Some("8.4"));
    assert_eq!(server_major("9.7.2").as_deref(), Some("9.7"));
    assert_eq!(
        server_major("11.8.8-MariaDB-ubu2404").as_deref(),
        Some("11.8")
    );
    assert_eq!(server_major("10.11.19-MariaDB").as_deref(), Some("10.11"));
    assert_eq!(server_major("8"), None);
    assert_eq!(server_major("x.4.1"), None);
    assert_eq!(server_major(""), None);
}

#[test]
fn la_chiave_porta_column_type_per_intero() {
    let (key, dimensions) = admission_key(
        &DataType::Float64,
        &facts("double", "double(10,2) unsigned zerofill", None),
    );
    assert_eq!(key.arrow, "float64");
    assert_eq!(key.nativo, "double(10,2) unsigned zerofill");
    assert_eq!(key.dimensione, None);
    assert_eq!(dimensions, TargetDimensions::default());
    let (key, _) = admission_key(
        &DataType::Timestamp(TimeUnit::Microsecond, None),
        &facts("datetime", "datetime(3)", None),
    );
    assert_eq!(key.nativo, "datetime(3)");
    assert_eq!(key.arrow, "timestamp[us]");
}

#[test]
fn il_testo_porta_il_charset_e_la_lunghezza_simbolica() {
    let (key, dimensions) = admission_key(
        &DataType::Utf8,
        &facts("varchar", "varchar(10)", Some("latin1")),
    );
    assert_eq!(key.nativo, "varchar(n);latin1");
    assert_eq!(key.dimensione, Some(10));
    assert_eq!(dimensions.chars, Some(10));
    let (key, _) = admission_key(&DataType::Utf8, &facts("text", "text", Some("utf8mb4")));
    assert_eq!(key.nativo, "text;utf8mb4");
    assert_eq!(key.dimensione, None);
    let mut json = facts("longtext", "longtext", Some("utf8mb4"));
    json.json_valid = true;
    let (key, _) = admission_key(&DataType::Utf8, &json);
    assert_eq!(key.nativo, "longtext;utf8mb4;json_valid");
}

#[test]
fn il_binario_porta_la_lunghezza_simbolica() {
    let (key, dimensions) = admission_key(
        &DataType::Binary,
        &facts("varbinary", "varbinary(16)", None),
    );
    assert_eq!(key.nativo, "varbinary(n)");
    assert_eq!(key.dimensione, Some(16));
    assert_eq!(dimensions.bytes, Some(16));
    let (key, _) = admission_key(&DataType::Binary, &facts("blob", "blob", None));
    assert_eq!(key.nativo, "blob");
}

#[test]
fn i_tipi_di_create_hanno_la_forma_del_catalogo_del_prodotto() {
    let mysql = create_facts("V", &MysqlColumnKind::I32, ProviderKind::Mysql).expect("int");
    assert_eq!(mysql.column_type, "int");
    let mariadb = create_facts("V", &MysqlColumnKind::I32, ProviderKind::Mariadb).expect("int");
    assert_eq!(mariadb.column_type, "int(11)");
    let unsigned = create_facts("V", &MysqlColumnKind::U64, ProviderKind::Mariadb).expect("u64");
    assert_eq!(unsigned.column_type, "bigint(20) unsigned");
    let text = create_facts("V", &MysqlColumnKind::Utf8, ProviderKind::Mysql).expect("text");
    assert_eq!(text.character_set.as_deref(), Some("utf8mb4"));
    let decimal = create_facts(
        "V",
        &MysqlColumnKind::Decimal {
            precision: 10,
            scale: 2,
        },
        ProviderKind::Mysql,
    )
    .expect("decimal");
    assert_eq!(decimal.column_type, "decimal(10,2)");
    assert!(create_facts("V", &MysqlColumnKind::Geometry, ProviderKind::Mysql).is_none());
    assert!(create_facts("V", &MysqlColumnKind::Time, ProviderKind::Mysql).is_none());
}

#[test]
fn i_fixture_incorporati_si_leggono() {
    admission_table(ProviderKind::Mysql).expect("fixture MySQL");
    admission_table(ProviderKind::Mariadb).expect("fixture MariaDB");
}

#[test]
fn una_major_non_misurata_si_rifiuta_con_ogni_politica() {
    let table = admission_table(ProviderKind::Mysql).expect("fixture MySQL");
    let error = table.server("0.1").expect_err("major non misurata");
    assert_eq!(error.category, ErrorCategory::Unsupported);
    assert_eq!(error.phase, ErrorPhase::Prepare);
    assert_eq!(
        error.message,
        plenora_database_core::write_admission::UNPROVEN_SERVER
    );
}

#[test]
fn ogni_major_qualificata_e_misurata() {
    for (kind, majors) in [
        (ProviderKind::Mysql, ["9.7", "8.4", "8.0"]),
        (ProviderKind::Mariadb, ["12.3", "11.8", "10.11"]),
    ] {
        let table = admission_table(kind).expect("fixture");
        for major in majors {
            let server = table
                .server(major)
                .unwrap_or_else(|error| panic!("{kind:?} {major}: {error:?}"));
            assert_eq!(server.server_major(), major);
        }
    }
}

#[test]
fn double_m_d_non_e_esatto() {
    // Il difetto della settima rilettura: `DOUBLE(M,D)` e `FLOAT(M,D)`
    // arrotondano e perdono `-0`; il preflight dava sempre `losses []`.
    for (kind, major) in [
        (ProviderKind::Mysql, "9.7"),
        (ProviderKind::Mariadb, "12.3"),
    ] {
        let server = admission_table(kind)
            .expect("fixture")
            .server(major)
            .expect("major");
        for (arrow, data_type, column_type) in [
            (DataType::Float64, "double", "double(10,2)"),
            (DataType::Float32, "float", "float(7,3)"),
        ] {
            let (key, dimensions) = admission_key(&arrow, &facts(data_type, column_type, None));
            let rewrites = server
                .admit(&key, dimensions)
                .unwrap_or_else(|error| panic!("{kind:?} {column_type}: {error:?}"));
            assert!(!rewrites.is_empty(), "{kind:?} {column_type}");
        }
        // Una scala non misurata si rifiuta: non e provata.
        let (key, dimensions) =
            admission_key(&DataType::Float64, &facts("double", "double(11,3)", None));
        let error = server.admit(&key, dimensions).expect_err("non misurata");
        assert_eq!(error.category, ErrorCategory::Unsupported);
    }
}

/// Entrambi i prodotti rifiutano le conversioni non esatte delle chiavi.
#[test]
fn le_chiavi_non_possono_perdere_precisione_o_cambiare_tipo() {
    for (kind, major) in [
        (ProviderKind::Mysql, "9.7"),
        (ProviderKind::Mariadb, "12.3"),
    ] {
        let measured = admission_table(kind)
            .expect("fixture")
            .server(major)
            .expect("server");
        for (arrow, fact) in [
            (
                DataType::Timestamp(TimeUnit::Microsecond, None),
                facts("datetime", "datetime(0)", None),
            ),
            (
                DataType::Decimal128(10, 3),
                facts("decimal", "decimal(10,2)", None),
            ),
            (DataType::Utf8, facts("binary", "binary(16)", None)),
            (DataType::Utf8, facts("uuid", "uuid", None)),
        ] {
            let (key, dimensions) = admission_key(&arrow, &fact);
            assert!(
                plenora_database_core::admission_gate::write_column(&measured, &key, dimensions)
                    .is_err(),
                "{kind:?}: {key:?}"
            );
        }
    }
}
