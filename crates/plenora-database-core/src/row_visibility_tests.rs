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
///
/// Fino al 25/10 (d30d1d2) le righe «regola», «instead of», «percorso» e
/// «opaca» ammettevano Append, `TruncateInsert` e Upsert: le scritture
/// pubblicavano `inserted` per righe finite altrove o da nessuna parte.
#[test]
fn la_regola_r_per_mode_e_meccanismo() {
    let rls = Some(HIDDEN_BY_ROW_SECURITY);
    let rule = Some(HIDDEN_BY_RULE);
    let inst = Some(HIDDEN_BY_INSTEAD_TRIGGER);
    let trig = Some(HIDDEN_BY_TRIGGER);
    let path = Some(BYPASSED_BY_PATH);
    let view = Some(HIDDEN_BY_VIEW);
    let opaq = Some(HIDDEN_BY_OPAQUE_RELATION);
    let unrd = Some(HIDDEN_UNREADABLE);
    #[rustfmt::skip]
    let table: [(&str, RowFilters, [Option<&str>; 7]); 10] = [
        ("nessun filtro", RowFilters::default(), [None; 7]),
        ("sicurezza di riga", filters(|f| f.row_security = true), [None, None, rls, None, rls, None, rls]),
        ("regola", filters(|f| f.rewrite_rule = true), [None, rule, rule, rule, rule, rule, rule]),
        ("instead of", filters(|f| f.instead_trigger = true), [None, inst, inst, inst, inst, inst, inst]),
        ("trigger di riga", filters(|f| f.row_trigger = true), [None, None, trig, None, trig, None, trig]),
        ("percorso", filters(|f| f.path_bypass = true), [None, path, path, path, path, path, path]),
        ("vista", filters(|f| f.view = true), [None, None, view, None, None, None, None]),
        ("opaca", filters(|f| f.opaque_relation = true), [None, opaq, opaq, opaq, opaq, opaq, opaq]),
        ("illeggibile", filters(|f| f.unreadable = true), [None, unrd, unrd, unrd, unrd, unrd, unrd]),
        // Il primo meccanismo nell'ordine dichiarato.
        ("tutti", RowFilters {
            row_security: true, rewrite_rule: true, instead_trigger: true, row_trigger: true,
            path_bypass: true, view: true, opaque_relation: true, unreadable: true,
        }, [None, unrd, unrd, unrd, unrd, unrd, unrd]),
    ];
    let mut difformi = Vec::new();
    for (name, facts, expected) in table {
        for (mode, expected) in MODES.iter().zip(expected) {
            let observed =
                refuse_unfaithful_write(ProviderKind::Postgres, *mode, ErrorPhase::Write, facts);
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

/// L'ordine dei motivi, un meccanismo alla volta tolto dalla testa: ogni
/// motivo vince su quelli che lo seguono.
#[test]
fn i_motivi_seguono_l_ordine_dichiarato() {
    let mut facts = RowFilters {
        row_security: true,
        rewrite_rule: true,
        instead_trigger: true,
        row_trigger: true,
        path_bypass: true,
        view: true,
        opaque_relation: true,
        unreadable: true,
    };
    let mut observed = Vec::new();
    let steps: [fn(&mut RowFilters); 8] = [
        |f| f.unreadable = false,
        |f| f.row_security = false,
        |f| f.rewrite_rule = false,
        |f| f.instead_trigger = false,
        |f| f.row_trigger = false,
        |f| f.path_bypass = false,
        |f| f.opaque_relation = false,
        |f| f.view = false,
    ];
    for step in steps {
        observed.push(refusal_reason(WriteMode::Replace, facts));
        step(&mut facts);
    }
    observed.push(refusal_reason(WriteMode::Replace, facts));
    assert_eq!(
        observed,
        [
            Some(HIDDEN_UNREADABLE),
            Some(HIDDEN_BY_ROW_SECURITY),
            Some(HIDDEN_BY_RULE),
            Some(HIDDEN_BY_INSTEAD_TRIGGER),
            Some(HIDDEN_BY_TRIGGER),
            Some(BYPASSED_BY_PATH),
            Some(HIDDEN_BY_OPAQUE_RELATION),
            Some(HIDDEN_BY_VIEW),
            None,
        ]
    );
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
fn gli_eventi_di_ogni_mode() {
    use WriteEvent::{Delete, Insert, Update};
    let events = MODES.map(mode_events);
    assert_eq!(
        events,
        [
            &[][..],
            &[Insert][..],
            &[Delete, Insert][..],
            &[Insert][..],
            &[Update][..],
            &[Insert, Update][..],
            &[Delete][..],
        ]
    );
    // L'evento distruttivo e sempre fra gli eventi della mode.
    for mode in MODES {
        if let Some(event) = destructive_event(mode) {
            let event = match event {
                DestructiveEvent::Delete => Delete,
                DestructiveEvent::Update => Update,
            };
            assert!(mode_events(mode).contains(&event), "{mode:?}");
        }
    }
}

#[test]
fn il_messaggio_nomina_la_mode_e_non_porta_dati() {
    let error = refuse_unfaithful_write(
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
    let error = refuse_unfaithful_write(
        ProviderKind::Sqlserver,
        WriteMode::Append,
        ErrorPhase::Prepare,
        filters(|f| f.path_bypass = true),
    )
    .expect_err("rifiuto");
    assert_eq!(
        error.message,
        format!("append rifiutata: {BYPASSED_BY_PATH}")
    );
    assert_eq!(error.phase, ErrorPhase::Prepare);
}
