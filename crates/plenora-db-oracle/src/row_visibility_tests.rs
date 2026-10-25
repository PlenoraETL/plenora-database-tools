use super::*;
use plenora_database_core::row_visibility::{
    HIDDEN_BY_INSTEAD_TRIGGER, HIDDEN_BY_OPAQUE_RELATION, HIDDEN_BY_ROW_SECURITY,
    HIDDEN_BY_TRIGGER, HIDDEN_UNREADABLE,
};
use std::sync::Arc;

fn row(kind: Option<&str>, policy: &str, trigger: &str) -> Row {
    row_with_instead(kind, policy, trigger, "N")
}

fn row_with_instead(kind: Option<&str>, policy: &str, trigger: &str, instead: &str) -> Row {
    let kind = kind.map_or_else(
        || ParameterValue::Null {
            type_name: "VARCHAR2".to_owned(),
        },
        |kind| ParameterValue::String(kind.to_owned()),
    );
    Row::try_new(
        Arc::from(vec![
            "K".to_owned(),
            "P".to_owned(),
            "T".to_owned(),
            "I".to_owned(),
        ]),
        vec![
            kind,
            ParameterValue::String(policy.to_owned()),
            ParameterValue::String(trigger.to_owned()),
            ParameterValue::String(instead.to_owned()),
        ],
    )
    .expect("riga")
}

fn verdict(rows: &[Row], mode: WriteMode) -> Option<String> {
    filters_from_row(rows).and_then(|filters| {
        refuse_unfaithful_write(ProviderKind::Oracle, mode, ErrorPhase::Write, filters)
            .err()
            .map(|error| error.message)
    })
}

/// La tabella dei fatti del dizionario, per esteso: la riga letta, la mode,
/// il motivo atteso (`None` = ammessa).
#[test]
fn i_fatti_del_dizionario_oracle_decidono_la_regola_r() {
    let table: [(&str, Vec<Row>, WriteMode, Option<&str>); 13] = [
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
        // Per un INSERT ne la policy ne un trigger BEFORE nascondono righe.
        (
            "append",
            vec![row(Some("TABLE"), "Y", "Y")],
            WriteMode::Append,
            None,
        ),
        // Fino al 25/10 (d30d1d2) le quattro righe seguenti erano ammesse.
        (
            "instead of, append",
            vec![row_with_instead(Some("VIEW"), "N", "N", "Y")],
            WriteMode::Append,
            Some(HIDDEN_BY_INSTEAD_TRIGGER),
        ),
        (
            "instead of, upsert",
            vec![row_with_instead(Some("TABLE"), "N", "N", "Y")],
            WriteMode::Upsert,
            Some(HIDDEN_BY_INSTEAD_TRIGGER),
        ),
        (
            "vista, append",
            vec![row(Some("VIEW"), "N", "N")],
            WriteMode::Append,
            Some(HIDDEN_BY_OPAQUE_RELATION),
        ),
        (
            "sinonimo, upsert",
            vec![row(Some("SYNONYM"), "N", "N")],
            WriteMode::Upsert,
            Some(HIDDEN_BY_OPAQUE_RELATION),
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

/// I bind `:n` di [`FACTS_SQL`], ciascuno letto per intero (`:1` non e il
/// prefisso di `:10`).
fn binds() -> Vec<usize> {
    let bytes = FACTS_SQL.as_bytes();
    let mut found = Vec::new();
    for (index, byte) in bytes.iter().enumerate() {
        if *byte != b':' {
            continue;
        }
        let digits = bytes[index + 1..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        if digits > 0 {
            let text = std::str::from_utf8(&bytes[index + 1..=index + digits]).expect("cifre");
            found.push(text.parse().expect("indice"));
        }
    }
    found
}

#[test]
fn ogni_bind_compare_una_volta() {
    for mode in [WriteMode::Replace, WriteMode::Append, WriteMode::Upsert] {
        let parameters = facts_parameters("OWNER", "T", mode);
        let mut found = binds();
        found.sort_unstable();
        assert_eq!(
            found,
            (1..=parameters.len()).collect::<Vec<_>>(),
            "{mode:?}"
        );
    }
}

/// Gli eventi passati per mode: il distruttivo due volte (policy e trigger),
/// poi i due eventi della mode per gli `INSTEAD OF`, con il segnaposto dove
/// la mode non ne ha.
#[test]
fn gli_eventi_dei_parametri_seguono_la_mode() {
    let events = |mode| {
        facts_parameters("O", "T", mode)
            .into_iter()
            .enumerate()
            .filter(|(index, _)| matches!(index, 4 | 7 | 10 | 11))
            .map(|(_, value)| match value {
                ParameterValue::String(value) => value,
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(
        events(WriteMode::Replace),
        ["DELETE", "DELETE", "DELETE", "INSERT"]
    );
    assert_eq!(
        events(WriteMode::Append),
        [NO_EVENT, NO_EVENT, "INSERT", NO_EVENT]
    );
    assert_eq!(
        events(WriteMode::Upsert),
        [NO_EVENT, NO_EVENT, "INSERT", "UPDATE"]
    );
    assert_eq!(
        events(WriteMode::Update),
        ["UPDATE", "UPDATE", "UPDATE", NO_EVENT]
    );
}
