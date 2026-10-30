use crate::parameter_catalog::bindings;

#[test]
fn direct_markers_keep_positions_and_identifier_case() {
    for (sql, expected) in [
        (
            "INSERT INTO s.t (id, c) VALUES (?, ?)",
            vec![Some("ID"), Some("C")],
        ),
        (
            "INSERT INTO s.t (id, c) VALUES (1, ?), (2, ?)",
            vec![Some("C"), Some("C")],
        ),
        (
            "SELECT '?' FROM s.t WHERE id = ? AND c = ? -- ?",
            vec![Some("ID"), Some("C")],
        ),
        (
            "SELECT * FROM s.t WHERE c IN (?, ?)",
            vec![Some("C"), Some("C")],
        ),
        (
            "UPDATE s.t SET c = ? WHERE id = ?",
            vec![Some("C"), Some("ID")],
        ),
        ("DELETE FROM s.t WHERE c = ?", vec![Some("C")]),
        (
            "INSERT INTO \"s\".\"t\" (\"Mixed\") VALUES (?)",
            vec![Some("Mixed")],
        ),
        (
            "SELECT * FROM s.t WHERE LENGTH(c) = ? AND c = ?",
            vec![None, Some("C")],
        ),
    ] {
        let (_, actual) = bindings(sql, expected.len()).expect("SQL diretto risolto");
        assert_eq!(
            actual
                .into_iter()
                .map(|binding| binding.map(|binding| binding.column))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .map(|name| name.map(str::to_owned))
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }
}

#[test]
fn unproven_scopes_and_expressions_do_not_override_descriptors() {
    for sql in [
        "WITH t AS (SELECT 'x' AS c FROM s.t) SELECT * FROM t WHERE c = ?",
        "SELECT * FROM s.t JOIN s.u ON t.id = u.id WHERE c = ?",
        "SELECT * FROM (SELECT c FROM s.t) x WHERE c = ?",
        "SELECT * FROM s.t AS x(other, c) WHERE c = ?",
        "INSERT INTO s.t (c) SELECT ? FROM s.u",
        "SELECT * FROM s.t WHERE c = ?; DELETE FROM s.t",
    ] {
        assert!(bindings(sql, 1).is_none(), "{sql}");
    }
    for sql in [
        "SELECT * FROM s.t WHERE c = SUBSTR(?, 1, 2)",
        "SELECT * FROM s.t WHERE other.c = ?",
    ] {
        assert_eq!(bindings(sql, 1).expect("analizzabile").1, vec![None]);
    }
}

#[test]
fn intermediate_casts_admit_exact_boundaries_for_scalar_and_array_binds() {
    use crate::operand_catalog::catalog_type;
    use crate::parameter::{admit_with_catalog, bind_with_catalog};
    use crate::write::intermediate::TargetType;
    use crate::write_admission::ColumnFacts;
    use plenora_database_core::operand_admission::operand_type;
    use plenora_database_core::plan::ProviderKind;
    use plenora_database_core::provider::ParameterValue;
    use std::num::NonZeroUsize;

    for native in ["CHARACTER", "VARCHAR", "CLOB"] {
        for units in ["OCTETS", "CODEUNITS32"] {
            let facts = ColumnFacts {
                name: "C".into(),
                type_schema: "SYSIBM".into(),
                type_name: native.into(),
                length: if units == "OCTETS" { 4 } else { 16 },
                scale: 0,
                codepage: 1208,
                string_units: units.into(),
                string_units_length: 4,
                collation: "SYSIBM.IDENTITY".into(),
            };
            let target = TargetType::from_catalog(&facts).unwrap().operand();
            let kind = operand_type(ProviderKind::Db2, &catalog_type(native, 4, 0, 1208, units));
            let described = odbc_api::DataType::Varchar {
                length: NonZeroUsize::new(4),
            };
            for (expression, expected) in [
                (target.clone(), true),
                (format!("({target})"), true),
                (target.to_ascii_lowercase(), true),
                (target.replace('?', &target), true),
                ("CAST(? AS INTEGER)".into(), false),
                ("CAST(? AS SYSIBM.VARCHAR(4 OCTETS))".into(), false),
                ("CAST(? AS \"VARCHAR\"(4 OCTETS))".into(), false),
                ("CAST(? AS VARCHAR(2 OCTETS))".into(), false),
                ("CAST(? AS VARCHAR(4))".into(), false),
                ("CAST(? AS VARCHAR(4 CHARACTERS))".into(), false),
                (
                    format!(
                        "CAST(? AS {native}(4 {}))",
                        if units == "OCTETS" {
                            "CODEUNITS32"
                        } else {
                            "OCTETS"
                        }
                    ),
                    false,
                ),
                (target.replace('?', "CAST(? AS VARCHAR(2 OCTETS))"), false),
                (format!("CAST({target} AS VARCHAR(2 OCTETS))"), false),
            ] {
                for sql in [
                    format!("INSERT INTO s.t (c) VALUES ({expression})"),
                    format!("UPDATE s.t SET c = {expression}"),
                    format!("DELETE FROM s.t WHERE c = {expression}"),
                    format!("SELECT * FROM s.t WHERE c IN ({expression})"),
                ] {
                    let (_, bindings) = bindings(&sql, 1).expect("SQL Db2 analizzabile");
                    let binding = bindings[0].as_ref().expect("candidato conservato");
                    let admitted = binding.matches(std::slice::from_ref(&facts)).unwrap();
                    assert_eq!(admitted, expected, "{sql}");
                    assert!(!binding.matches(&[]).unwrap(), "senza catalogo: {sql}");
                    let catalog = admitted.then_some(&kind);
                    for (text, accepted) in [("a", true), ("abcd", expected), ("abcde", false)] {
                        let value = ParameterValue::String(text.into());
                        assert_eq!(
                            bind_with_catalog(&value, &described, catalog).is_ok(),
                            accepted,
                            "scalare: {sql}"
                        );
                        let array_value = admit_with_catalog(&value, &described, catalog);
                        assert_eq!(array_value.is_ok(), accepted, "parameter-array: {sql}");
                        if let Ok(array_value) = array_value {
                            assert!(
                                crate::transaction::ArrayColumns::encode(
                                    &[vec![array_value.clone()], vec![array_value]],
                                    1
                                )
                                .is_ok(),
                                "buffer parameter-array: {sql}"
                            );
                        }
                    }
                    let mut unknown = facts.clone();
                    unknown.type_schema = "OTHER".into();
                    assert!(
                        !binding.matches(&[unknown]).unwrap(),
                        "tipo non qualificato: {sql}"
                    );
                }
            }
        }
    }
}

#[test]
fn catalog_identity_cannot_borrow_a_neighbours_cast_limit() {
    use crate::parameter::{admit_with_catalog, bind_with_catalog};
    use plenora_database_core::provider::ParameterValue;
    let fact = |name: &str, length: &str| {
        crate::write_admission::parse_column(
            &[
                name,
                "SYSIBM",
                "VARCHAR",
                length,
                "0",
                "1208",
                "OCTETS",
                length,
                "SYSIBM.IDENTITY",
            ]
            .map(|v| Some(v.to_owned())),
        )
        .unwrap()
    };
    let facts = [fact(" C", "4"), fact("C", "100")];
    let described = odbc_api::DataType::Varchar {
        length: std::num::NonZeroUsize::new(4),
    };
    for column in ["C", "\" C\""] {
        let sql = format!("INSERT INTO s.t ({column}) VALUES (CAST(? AS VARCHAR(4 OCTETS)))");
        let (_, bindings) = bindings(&sql, 1).unwrap();
        let binding = bindings[0].as_ref().unwrap();
        let kind = binding.text_operand(&facts).unwrap();
        assert_eq!(kind.is_some(), column != "C");
        for text in ["abcd ", "abcde"] {
            let value = ParameterValue::String(text.into());
            assert!(bind_with_catalog(&value, &described, kind.as_ref()).is_err());
            assert!(admit_with_catalog(&value, &described, kind.as_ref()).is_err());
        }
    }
    let (_, bindings) = bindings("INSERT INTO s.t (C) VALUES (?)", 1).unwrap();
    let binding = bindings[0].as_ref().unwrap();
    for ambiguous in [
        vec![fact("C", "4"), fact("C", "100")],
        vec![fact("C ", "4")],
        vec![fact("C", "4"), fact("C ", "100")],
    ] {
        let error = binding.text_operand(&ambiguous).unwrap_err();
        assert!(error.message.contains("ambigua o non esatta"));
    }
}
