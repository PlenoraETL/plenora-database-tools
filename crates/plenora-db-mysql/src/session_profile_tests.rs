use super::*;
use mysql_async::consts::ColumnType;
use mysql_async::{Column, Value};

fn row(values: Vec<Value>) -> Row {
    let columns = vec![Column::new(ColumnType::MYSQL_TYPE_VAR_STRING); values.len()];
    mysql_common::row::new_row(values, columns.into())
}

fn expected_values() -> Vec<Value> {
    std::iter::once("warehouse")
        .chain(VARIABLES.iter().map(|(_, v)| *v))
        .map(Value::from)
        .collect()
}

#[test]
fn profile_rejects_each_drift_null_missing_and_extra_value() {
    let expected = expected_values();
    let profile = SessionProfile::capture(&[row(expected.clone())], "warehouse").unwrap();
    assert_eq!(
        profile.divergence(&[row(expected.clone())], "warehouse"),
        None
    );
    assert!(profile.divergence(&[], "warehouse").is_some());
    for index in 0..expected.len() {
        for replacement in [Value::NULL, Value::from("drift-secret")] {
            let mut values = expected.clone();
            values[index] = replacement;
            let name = if index == 0 {
                "database"
            } else {
                VARIABLES[index - 1].0
            };
            assert_eq!(profile.divergence(&[row(values)], "warehouse"), Some(name));
        }
    }
    let mut short = expected.clone();
    short.pop();
    assert!(profile.divergence(&[row(short)], "warehouse").is_some());
    let mut long = expected.clone();
    long.push(Value::from("extra"));
    assert!(profile.divergence(&[row(long)], "warehouse").is_some());
    assert!(profile
        .divergence(&[row(expected.clone()), row(expected)], "warehouse")
        .is_some());
}

#[test]
fn profile_normalizes_modes_and_booleans_without_hiding_drift() {
    let expected = expected_values();
    let profile = SessionProfile::capture(&[row(expected.clone())], "warehouse").unwrap();
    for (offset, (name, value)) in VARIABLES.iter().enumerate() {
        let alternative = match *name {
            "sql_mode" => value
                .split(',')
                .rev()
                .collect::<Vec<_>>()
                .join(", ")
                .to_ascii_lowercase(),
            "autocommit" | "sql_notes" | "foreign_key_checks" | "unique_checks" => "ON".to_owned(),
            _ => continue,
        };
        let mut values = expected.clone();
        values[offset + 1] = Value::from(alternative);
        assert_eq!(
            profile.divergence(&[row(values)], "warehouse"),
            None,
            "{name}"
        );
    }
    for (name, _) in VARIABLES {
        if *name == "max_error_count" {
            continue;
        }
        let index = VARIABLES.iter().position(|(key, _)| key == name).unwrap() + 1;
        let mut values = expected.clone();
        values[index] = Value::from("invalid-secret");
        assert_eq!(
            SessionProfile::capture(&[row(values)], "warehouse").err(),
            Some(*name)
        );
    }
    for (name, input, canonical) in [
        ("div_precision_increment", "04", "4"),
        ("max_sort_length", "08388608", "8388608"),
        ("sql_auto_is_null", "OFF", "0"),
        ("check_constraint_checks", "ON", "1"),
    ] {
        assert_eq!(normalized(name, input), canonical);
        let index = VARIABLES.iter().position(|(key, _)| *key == name).unwrap() + 1;
        let mut values = expected.clone();
        values[index] = Value::from(input);
        assert_eq!(profile.divergence(&[row(values)], "warehouse"), None);
        let mut values = expected.clone();
        values[index] = Value::from(if canonical == "0" { "1" } else { "0" });
        assert_eq!(
            SessionProfile::capture(&[row(values.clone())], "warehouse").err(),
            Some(name)
        );
        assert_eq!(profile.divergence(&[row(values)], "warehouse"), Some(name));
    }
    let mut values = expected.clone();
    let index = VARIABLES
        .iter()
        .position(|(name, _)| *name == "max_error_count")
        .unwrap()
        + 1;
    values[index] = Value::from("64");
    let baseline = SessionProfile::capture(&[row(values.clone())], "warehouse").unwrap();
    assert_eq!(baseline.divergence(&[row(values)], "warehouse"), None);
    assert_eq!(
        baseline.divergence(&[row(expected)], "warehouse"),
        Some("max_error_count")
    );
}

#[test]
fn profile_covers_every_bootstrap_assignment_in_one_select() {
    let mut expected = crate::evidence::sql_assignments(crate::SESSION_BOOTSTRAP_SQL);
    // Variabile osservata, non impostata: il limite non richiede privilegi admin.
    expected.push(("max_error_count".to_owned(), "65535".to_owned()));
    expected.extend(crate::evidence::sql_assignments(
        crate::session::MARIADB_BOOTSTRAP_SQL,
    ));
    expected.sort();
    let mut actual: Vec<_> = VARIABLES
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    actual.sort();
    assert_eq!(actual, expected);
    let sql = select_sql(true);
    assert!(sql.starts_with("SELECT DATABASE(), "));
    assert_eq!(sql.matches("@@session.").count(), VARIABLES.len());
    assert!(!sql.contains(';'));
}

#[test]
fn mysql_does_not_read_mariadb_only_variable() {
    assert!(!select_sql(false).contains("@@session.check_constraint_checks"));
    assert!(select_sql(true).contains("@@session.check_constraint_checks"));
}

#[test]
fn database_case_mismatch_is_explicit_and_never_exposes_the_name() {
    assert_eq!(
        SessionProfile::capture(&[row(expected_values())], "Warehouse").err(),
        Some("database")
    );
}
