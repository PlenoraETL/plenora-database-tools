use super::{
    decimal_digits, operand_type, rendered_target, value_fits, widens, CatalogType, OperandType,
    ValueFact,
};
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::relational::QueryParameterType;

const ALL: [ProviderKind; 6] = [
    ProviderKind::Postgres,
    ProviderKind::Mysql,
    ProviderKind::Mariadb,
    ProviderKind::Sqlserver,
    ProviderKind::Oracle,
    ProviderKind::Db2,
];

fn fact(value: &ParameterValue) -> ValueFact {
    ValueFact::of(value)
}

fn catalog(
    name: &str,
    precision: Option<u64>,
    scale: Option<i64>,
    length: Option<u64>,
) -> CatalogType {
    CatalogType {
        name: name.to_owned(),
        length,
        precision,
        scale,
        ..CatalogType::default()
    }
}

/// La quindicesima lettura del 25/10: `CAST(:p AS decimal)` con undici cifre
/// frazionarie arrotondava, e un `F64` anche; Db2 rende `DECIMAL(31, 10)`.
#[test]
fn un_decimale_oltre_la_scala_resa_non_sta() {
    for provider in ALL {
        let target = rendered_target(provider, QueryParameterType::Decimal);
        let wide = fact(&ParameterValue::Decimal("0.12345678901".to_owned()));
        let fits = fact(&ParameterValue::Decimal("0.1234567890".to_owned()));
        assert!(!value_fits(provider, &wide, &target), "{provider:?}");
        assert!(value_fits(provider, &fits, &target), "{provider:?}");
        assert!(!value_fits(
            provider,
            &fact(&ParameterValue::F64(0.123_456_789_01)),
            &target
        ));
        assert!(!value_fits(
            provider,
            &fact(&ParameterValue::Decimal("1e3".to_owned())),
            &target
        ));
    }
    let db2 = rendered_target(ProviderKind::Db2, QueryParameterType::Decimal);
    assert_eq!(
        db2,
        OperandType::Decimal {
            precision: 31,
            scale: 10
        }
    );
    let twenty_two = fact(&ParameterValue::Decimal(
        "1234567890123456789012".to_owned(),
    ));
    assert!(!value_fits(ProviderKind::Db2, &twenty_two, &db2));
    let postgres = rendered_target(ProviderKind::Postgres, QueryParameterType::Decimal);
    assert!(value_fits(ProviderKind::Postgres, &twenty_two, &postgres));
}

/// `numeric(30, 12)` non sta in `DECIMAL(38, 10)`; `numeric(20, 4)` si.
#[test]
fn una_colonna_decimale_sta_soltanto_entro_precisione_e_scala() {
    let target = rendered_target(ProviderKind::Postgres, QueryParameterType::Decimal);
    let wide = catalog("numeric", Some(30), Some(12), None);
    let narrow = catalog("numeric", Some(20), Some(4), None);
    let unbounded = catalog("numeric", None, None, None);
    let float = catalog("float8", None, None, None);
    let source = |catalog: &CatalogType| operand_type(ProviderKind::Postgres, catalog);
    assert!(!widens(
        ProviderKind::Postgres,
        &source(&wide),
        "numeric",
        &target
    ));
    assert!(widens(
        ProviderKind::Postgres,
        &source(&narrow),
        "numeric",
        &target
    ));
    assert!(!widens(
        ProviderKind::Postgres,
        &source(&unbounded),
        "numeric",
        &target
    ));
    assert!(!widens(
        ProviderKind::Postgres,
        &source(&float),
        "float8",
        &target
    ));
    let integer = catalog("int8", None, None, None);
    assert!(widens(
        ProviderKind::Postgres,
        &source(&integer),
        "int8",
        &target
    ));
}

/// Il testo reso in byte (Oracle, Db2) tiene un testo del catalogo soltanto
/// a quattro byte per carattere; un `CLOB` no.
#[test]
fn il_testo_reso_in_byte_tiene_soltanto_i_testi_che_stanno() {
    let target = rendered_target(ProviderKind::Db2, QueryParameterType::String);
    let short = operand_type(
        ProviderKind::Db2,
        &catalog("VARCHAR", None, None, Some(8000)),
    );
    let long = operand_type(
        ProviderKind::Db2,
        &catalog("VARCHAR", None, None, Some(9000)),
    );
    let clob = operand_type(
        ProviderKind::Db2,
        &catalog("CLOB", None, None, Some(1_048_576)),
    );
    assert!(widens(ProviderKind::Db2, &short, "VARCHAR", &target));
    assert!(!widens(ProviderKind::Db2, &long, "VARCHAR", &target));
    assert!(!widens(ProviderKind::Db2, &clob, "CLOB", &target));
    let text = |bytes: usize| fact(&ParameterValue::String("é".repeat(bytes / 2)));
    assert!(value_fits(ProviderKind::Db2, &text(32672), &target));
    assert!(!value_fits(ProviderKind::Db2, &text(32674), &target));
}

/// Un `timestamp_tz` sta in un tipo con fuso: non su `MariaDB`, che scarta
/// l'offset, ne su Db2, che non ha il tipo.
#[test]
fn un_timestamp_con_fuso_sta_soltanto_dove_il_tipo_reso_tiene_il_fuso() {
    let value = fact(&ParameterValue::TimestampTz(
        "2024-01-01T10:00:00+02:00".to_owned(),
    ));
    for (provider, fits) in [
        (ProviderKind::Postgres, true),
        (ProviderKind::Mysql, true),
        (ProviderKind::Mariadb, false),
        (ProviderKind::Sqlserver, true),
        (ProviderKind::Oracle, true),
        (ProviderKind::Db2, false),
    ] {
        let target = rendered_target(provider, QueryParameterType::TimestampTz);
        assert_eq!(value_fits(provider, &value, &target), fits, "{provider:?}");
    }
}

#[test]
fn le_cifre_di_un_decimale_si_leggono_soltanto_in_notazione_posizionale() {
    let digits = decimal_digits("-00120.4500").expect("posizionale");
    assert_eq!((digits.integer, digits.fraction), (3, 2));
    for form in ["", "1e3", " 1", "1.2.3", "+", "0x10"] {
        assert!(decimal_digits(form).is_none(), "{form}");
    }
}

/// Un `CAST` fra famiglie (un numero a testo, un testo a numero di colonna,
/// una data da un timestamp) non si prova esatto.
#[test]
fn un_cast_fra_famiglie_di_colonne_non_si_prova() {
    let provider = ProviderKind::Postgres;
    let text = operand_type(provider, &catalog("text", None, None, None));
    let number = operand_type(provider, &catalog("int4", None, None, None));
    let timestamp = operand_type(provider, &catalog("timestamp", None, None, None));
    let padded = operand_type(provider, &catalog("bpchar", None, None, Some(4)));
    let to = |parameter_type| rendered_target(provider, parameter_type);
    assert!(!widens(
        provider,
        &number,
        "int4",
        &to(QueryParameterType::String)
    ));
    assert!(!widens(
        provider,
        &text,
        "text",
        &to(QueryParameterType::Integer)
    ));
    assert!(!widens(
        provider,
        &timestamp,
        "timestamp",
        &to(QueryParameterType::Date)
    ));
    assert!(widens(
        provider,
        &timestamp,
        "timestamp",
        &to(QueryParameterType::Timestamp)
    ));
    // `bpchar` perde gli spazi finali in `TEXT`.
    assert!(!widens(
        provider,
        &padded,
        "bpchar",
        &to(QueryParameterType::String)
    ));
    assert!(widens(
        provider,
        &text,
        "text",
        &to(QueryParameterType::String)
    ));
    // `json` si normalizza in `JSONB`.
    let json = operand_type(provider, &catalog("json", None, None, None));
    assert!(!widens(
        provider,
        &json,
        "json",
        &to(QueryParameterType::Json)
    ));
    assert!(widens(
        provider,
        &json,
        "jsonb",
        &to(QueryParameterType::Json)
    ));
}
