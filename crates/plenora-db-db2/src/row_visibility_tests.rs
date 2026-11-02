use super::*;
use plenora_database_core::row_visibility::{
    HIDDEN_BY_INSTEAD_TRIGGER, HIDDEN_BY_OPAQUE_RELATION, HIDDEN_BY_ROW_SECURITY,
    HIDDEN_BY_TRIGGER, HIDDEN_UNREADABLE,
};
use std::sync::Arc;

fn row(kind: &str, control: &str, trigger: &str) -> Row {
    row_with_instead(kind, control, trigger, "N")
}

fn row_with_instead(kind: &str, control: &str, trigger: &str, instead: &str) -> Row {
    Row::try_new(
        Arc::from(vec![
            "K".to_owned(),
            "C".to_owned(),
            "T".to_owned(),
            "I".to_owned(),
        ]),
        [kind, control, trigger, instead]
            .map(|value| ParameterValue::String(value.to_owned()))
            .to_vec(),
    )
    .expect("riga")
}

/// La tabella dei fatti del catalogo, per esteso: la riga letta, la mode,
/// il motivo atteso (`None` = ammessa).
#[test]
fn i_fatti_del_catalogo_db2_decidono_la_regola_r() {
    #[rustfmt::skip]
    let table: [(&str, Vec<Row>, WriteMode, Option<&str>); 12] = [
        ("tabella pulita", vec![row("T", "", "N")], WriteMode::Replace, None),
        ("row permission", vec![row("T", "R", "N")], WriteMode::Replace, Some(HIDDEN_BY_ROW_SECURITY)),
        ("row e column", vec![row("T", "B", "N")], WriteMode::Update, Some(HIDDEN_BY_ROW_SECURITY)),
        ("solo column mask", vec![row("T", "C", "N")], WriteMode::DeleteByKeys, None),
        ("trigger", vec![row("T", "", "Y")], WriteMode::DeleteByKeys, Some(HIDDEN_BY_TRIGGER)),
        ("nickname", vec![row("N", "", "N")], WriteMode::Replace, Some(HIDDEN_BY_OPAQUE_RELATION)),
        ("target assente", vec![row("-", "", "N")], WriteMode::Replace, None),
        ("nessuna riga", Vec::new(), WriteMode::Update, Some(HIDDEN_UNREADABLE)),
        ("append", vec![row("T", "R", "Y")], WriteMode::Append, None),
        // Fino al 25/10 (d30d1d2) le tre righe seguenti erano ammesse.
        ("instead of, append", vec![row_with_instead("V", "", "N", "Y")], WriteMode::Append, Some(HIDDEN_BY_INSTEAD_TRIGGER)),
        ("instead of, upsert", vec![row_with_instead("T", "", "N", "Y")], WriteMode::Upsert, Some(HIDDEN_BY_INSTEAD_TRIGGER)),
        ("alias, append", vec![row("A", "", "N")], WriteMode::Append, Some(HIDDEN_BY_OPAQUE_RELATION)),
    ];
    let mut difformi = Vec::new();
    for (name, rows, mode, expected) in table {
        let observed = filters_from_row(&rows).and_then(|filters| {
            refuse_unfaithful_write(ProviderKind::Db2, mode, ErrorPhase::Write, filters)
                .err()
                .map(|error| error.message)
        });
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
fn i_parametri_coprono_ogni_segnaposto() {
    for mode in [WriteMode::Replace, WriteMode::Append, WriteMode::Upsert] {
        let parameters = facts_parameters("S", "T", mode);
        assert_eq!(FACTS_SQL.matches('?').count(), parameters.len(), "{mode:?}");
    }
}

/// Gli eventi passati per mode: il distruttivo, poi i due eventi della mode
/// per gli `INSTEAD OF`, con il segnaposto dove la mode non ne ha.
#[test]
fn gli_eventi_dei_parametri_seguono_la_mode() {
    let events = |mode| {
        facts_parameters("S", "T", mode)
            .into_iter()
            .enumerate()
            .filter(|(index, _)| matches!(index, 6 | 9 | 10))
            .map(|(_, value)| match value {
                ParameterValue::String(value) => value,
                other => panic!("{other:?}"),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(events(WriteMode::Replace), ["D", "D", "I"]);
    assert_eq!(events(WriteMode::Append), [NO_EVENT, "I", NO_EVENT]);
    assert_eq!(events(WriteMode::Upsert), [NO_EVENT, "I", "U"]);
    assert_eq!(events(WriteMode::DeleteByKeys), ["D", "D", NO_EVENT]);
}
