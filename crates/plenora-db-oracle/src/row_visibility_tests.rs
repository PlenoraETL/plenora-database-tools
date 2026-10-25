use super::*;
use plenora_database_core::row_visibility::{
    HIDDEN_BY_OPAQUE_RELATION, HIDDEN_BY_ROW_SECURITY, HIDDEN_BY_TRIGGER, HIDDEN_UNREADABLE,
};
use std::sync::Arc;

fn row(kind: Option<&str>, policy: &str, trigger: &str) -> Row {
    let kind = kind.map_or_else(
        || ParameterValue::Null {
            type_name: "VARCHAR2".to_owned(),
        },
        |kind| ParameterValue::String(kind.to_owned()),
    );
    Row::try_new(
        Arc::from(vec!["K".to_owned(), "P".to_owned(), "T".to_owned()]),
        vec![
            kind,
            ParameterValue::String(policy.to_owned()),
            ParameterValue::String(trigger.to_owned()),
        ],
    )
    .expect("riga")
}

fn verdict(rows: &[Row], mode: WriteMode) -> Option<String> {
    filters_from_row(rows).and_then(|filters| {
        refuse_filtered_rows(ProviderKind::Oracle, mode, ErrorPhase::Write, filters)
            .err()
            .map(|error| error.message)
    })
}

/// La tabella dei fatti del dizionario, per esteso: la riga letta, la mode,
/// il motivo atteso (`None` = ammessa).
#[test]
fn i_fatti_del_dizionario_oracle_decidono_la_regola_r() {
    let table: [(&str, Vec<Row>, WriteMode, Option<&str>); 9] = [
        (
            "tabella pulita",
            vec![row(Some("TABLE"), "N", "N")],
            WriteMode::Replace,
            None,
        ),
        (
            "policy VPD",
            vec![row(Some("TABLE"), "Y", "N")],
            WriteMode::Replace,
            Some(HIDDEN_BY_ROW_SECURITY),
        ),
        (
            "policy VPD, update",
            vec![row(Some("TABLE"), "Y", "N")],
            WriteMode::Update,
            Some(HIDDEN_BY_ROW_SECURITY),
        ),
        (
            "trigger",
            vec![row(Some("TABLE"), "N", "Y")],
            WriteMode::DeleteByKeys,
            Some(HIDDEN_BY_TRIGGER),
        ),
        (
            "vista",
            vec![row(Some("VIEW"), "N", "N")],
            WriteMode::DeleteByKeys,
            Some(HIDDEN_BY_OPAQUE_RELATION),
        ),
        (
            "sinonimo",
            vec![row(Some("SYNONYM"), "N", "N")],
            WriteMode::Replace,
            Some(HIDDEN_BY_OPAQUE_RELATION),
        ),
        (
            "target assente",
            vec![row(None, "N", "N")],
            WriteMode::Replace,
            None,
        ),
        (
            "nessuna riga",
            Vec::new(),
            WriteMode::Replace,
            Some(HIDDEN_UNREADABLE),
        ),
        (
            "append",
            vec![row(Some("TABLE"), "Y", "Y")],
            WriteMode::Append,
            None,
        ),
    ];
    let mut difformi = Vec::new();
    for (name, rows, mode, expected) in table {
        let observed = verdict(&rows, mode);
        let conforme = match (expected, &observed) {
            (None, None) => true,
            (Some(reason), Some(message)) => message.ends_with(reason),
            _ => false,
        };
        if !conforme {
            difformi.push(format!(
                "{name}: atteso {expected:?}, osservato {observed:?}"
            ));
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

#[test]
fn ogni_bind_compare_una_volta() {
    let parameters = facts_parameters("OWNER", "T", DestructiveEvent::Delete);
    for index in 1..=parameters.len() {
        assert_eq!(
            FACTS_SQL.matches(&format!(":{index}")).count(),
            1,
            ":{index}"
        );
    }
    assert!(!FACTS_SQL.contains(&format!(":{}", parameters.len() + 1)));
}
