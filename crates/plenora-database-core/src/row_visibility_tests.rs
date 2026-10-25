use super::*;
use crate::ErrorCategory;

const MODES: [WriteMode; 7] = [
    WriteMode::Create,
    WriteMode::Append,
    WriteMode::Replace,
    WriteMode::TruncateInsert,
    WriteMode::Update,
    WriteMode::Upsert,
    WriteMode::DeleteByKeys,
];

fn filters(set: fn(&mut RowFilters)) -> RowFilters {
    let mut filters = RowFilters::default();
    set(&mut filters);
    filters
}

/// La tabella della regola, per esteso: il fatto del catalogo, poi il motivo
/// atteso per Create, Append, Replace, `TruncateInsert`, Update, Upsert,
/// `DeleteByKeys` (`None` = ammessa).
#[test]
fn la_regola_r_per_mode_e_meccanismo() {
    let rls = Some(HIDDEN_BY_ROW_SECURITY);
    let rule = Some(HIDDEN_BY_RULE);
    let trig = Some(HIDDEN_BY_TRIGGER);
    let view = Some(HIDDEN_BY_VIEW);
    let opaq = Some(HIDDEN_BY_OPAQUE_RELATION);
    let unrd = Some(HIDDEN_UNREADABLE);
    #[rustfmt::skip]
    let table: [(&str, RowFilters, [Option<&str>; 7]); 8] = [
        ("nessun filtro", RowFilters::default(), [None; 7]),
        ("sicurezza di riga", filters(|f| f.row_security = true), [None, None, rls, None, rls, None, rls]),
        ("regola", filters(|f| f.rewrite_rule = true), [None, None, rule, None, rule, None, rule]),
        ("trigger", filters(|f| f.row_trigger = true), [None, None, trig, None, trig, None, trig]),
        ("vista", filters(|f| f.view = true), [None, None, view, None, None, None, None]),
        ("opaca", filters(|f| f.opaque_relation = true), [None, None, opaq, None, opaq, None, opaq]),
        ("illeggibile", filters(|f| f.unreadable = true), [None, None, unrd, None, unrd, None, unrd]),
        // Il primo meccanismo nell'ordine dichiarato.
        ("tutti", RowFilters {
            row_security: true, rewrite_rule: true, row_trigger: true, view: true,
            opaque_relation: true, unreadable: true,
        }, [None, None, unrd, None, unrd, None, unrd]),
    ];
    let mut difformi = Vec::new();
    for (name, facts, expected) in table {
        for (mode, expected) in MODES.iter().zip(expected) {
            let observed =
                refuse_filtered_rows(ProviderKind::Postgres, *mode, ErrorPhase::Write, facts);
            let conforme = match (expected, &observed) {
                (None, Ok(())) => true,
                (Some(reason), Err(error)) => {
                    error.category == ErrorCategory::Unsupported
                        && error.phase == ErrorPhase::Write
                        && error.message.ends_with(reason)
                }
                _ => false,
            };
            if !conforme {
                difformi.push(format!(
                    "{name} {mode:?}: atteso {expected:?}, osservato {observed:?}"
                ));
            }
        }
    }
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

#[test]
fn soltanto_replace_delete_e_update_hanno_un_evento_distruttivo() {
    let events = MODES.map(destructive_event);
    assert_eq!(
        events,
        [
            None,
            None,
            Some(DestructiveEvent::Delete),
            None,
            Some(DestructiveEvent::Update),
            None,
            Some(DestructiveEvent::Delete),
        ]
    );
}

#[test]
fn il_messaggio_nomina_la_mode_e_non_porta_dati() {
    let error = refuse_filtered_rows(
        ProviderKind::Oracle,
        WriteMode::DeleteByKeys,
        ErrorPhase::Write,
        filters(|f| f.row_security = true),
    )
    .expect_err("rifiuto");
    assert_eq!(
        error.message,
        format!("delete_by_keys rifiutata: {HIDDEN_BY_ROW_SECURITY}")
    );
    assert_eq!(error.provider, Some(ProviderKind::Oracle));
}
