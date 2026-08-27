use super::{
    generated_index_mismatch, qualified_filter_forms, read_mismatch, refusal_mismatch,
    server_code_in_message, sql_assignments, ExpressionIndexDdl, ReadContract, ReadOutcome,
    RefusalContract, QUALIFIED_FILTER_FORMS,
};
#[test]
fn the_expected_ddl_outcome_is_the_measured_one() {
    // I due prodotti partono da due punti diversi, e ciascuno ha il suo:
    // MySQL accetta l'indice su espressione, MariaDB lo rifiuta con 1064.
    assert_eq!(
        ExpressionIndexDdl::of(&crate::profile::MYSQL_PROFILE),
        ExpressionIndexDdl::Accepted
    );
    assert_eq!(
        ExpressionIndexDdl::of(&crate::profile::MARIADB_PROFILE),
        ExpressionIndexDdl::Refused(1_064)
    );

    assert_eq!(ExpressionIndexDdl::Accepted.mismatch(Ok(())), None);
    assert_eq!(
        ExpressionIndexDdl::Refused(1_064).mismatch(Err(Some(1_064))),
        None
    );

    // E ogni altro esito e una premessa che manca. Il caso che conta e il
    // terzo: un errore diverso da quello misurato — un privilegio, un
    // timeout — rendeva la sonda verde, perche "l'indice non c'e" era
    // indistinguibile da "il server lo ha rifiutato come sappiamo".
    for (what, expectation, observed, expected) in [
        (
            "accettata ma rifiutata",
            ExpressionIndexDdl::Accepted,
            Err(Some(1_142)),
            "doveva essere accettata",
        ),
        (
            "accettata ma rifiutata senza codice",
            ExpressionIndexDdl::Accepted,
            Err(None),
            "senza codice del server",
        ),
        (
            "rifiutata con un altro codice",
            ExpressionIndexDdl::Refused(1_064),
            Err(Some(1_142)),
            "osservato 1142",
        ),
        (
            "rifiutata senza codice",
            ExpressionIndexDdl::Refused(1_064),
            Err(None),
            "non porta un codice del server",
        ),
        (
            "rifiutata ma passata",
            ExpressionIndexDdl::Refused(1_064),
            Ok(()),
            "ed e passata",
        ),
    ] {
        let reported = expectation
            .mismatch(observed)
            .unwrap_or_else(|| panic!("{what}: scambiato per l'esito atteso"));
        assert!(
            reported.contains(expected),
            "{what}: il verdetto non dice cosa non torna — {reported}"
        );
    }
}

fn generated_description() -> crate::MysqlObjectDescription {
    let column = |name: &str, generation: &str| crate::MysqlColumn {
        name: name.to_owned(),
        ordinal: 1,
        data_type: "varchar".to_owned(),
        native_declaration: "varchar(32)".to_owned(),
        nullable: true,
        default_expression: None,
        character_set: None,
        collation: None,
        numeric_precision: None,
        numeric_scale: None,
        datetime_precision: None,
        spatial_srid: None,
        extra: String::new(),
        generation_expression: generation.to_owned(),
    };
    crate::MysqlObjectDescription {
        schema: "dataflow_test".to_owned(),
        name: "generata".to_owned(),
        kind: "BASE TABLE".to_owned(),
        engine: Some("InnoDB".to_owned()),
        columns: vec![column("name", ""), column("lname", "lower(`name`)")],
        indexes: vec![crate::MysqlIndex {
            name: "uq_lname".to_owned(),
            unique: true,
            column_backed: true,
            columns: vec!["lname".to_owned()],
        }],
        token: crate::MysqlSchemaToken(String::new()),
    }
}

#[test]
fn the_generated_index_contract_is_verified_in_full() {
    assert_eq!(
        generated_index_mismatch(&generated_description(), "lname", "uq_lname"),
        None
    );

    // Cinque modi di perdere la forma, e ciascuno cambia una delle due
    // decisioni che da quella forma dipendono: se la colonna sia
    // scrivibile, e se l'indice sia confrontabile con le keys.
    let without = |mutate: fn(&mut crate::MysqlObjectDescription)| {
        let mut description = generated_description();
        mutate(&mut description);
        description
    };
    for (what, description, expected) in [
        (
            "colonna assente",
            without(|description| description.columns.retain(|column| column.name != "lname")),
            "non compare",
        ),
        (
            "indice assente",
            without(|description| description.indexes.clear()),
            "non compare",
        ),
        (
            "colonna non piu generata",
            without(|description| {
                for column in &mut description.columns {
                    column.generation_expression.clear();
                }
            }),
            "sarebbe scrivibile",
        ),
        (
            "indice su piu colonne",
            without(|description| {
                description.indexes[0].columns.push("name".to_owned());
            }),
            "non e sulla sola colonna generata",
        ),
        (
            "indice non unico",
            without(|description| description.indexes[0].unique = false),
            "non risulta unico",
        ),
        (
            "indice non confrontabile",
            without(|description| description.indexes[0].column_backed = false),
            "non risulta confrontabile",
        ),
    ] {
        let reported = generated_index_mismatch(&description, "lname", "uq_lname")
            .unwrap_or_else(|| panic!("{what}: la forma perduta e passata per buona"));
        assert!(
            reported.contains(expected),
            "{what}: il verdetto non dice cosa manca — {reported}"
        );
    }
}

use plenora_database_core::plan::FilterExpression;
use plenora_database_core::{ErrorCategory, ErrorPhase, RemoteEffect, RetryDisposition};

#[test]
fn the_qualified_filter_forms_are_the_thirteen_declared() {
    // L'elenco e la tabella devono coincidere nome per nome e in ordine.
    // Senza, togliere una voce dalla tabella lascerebbe la sonda aggregata
    // verde: cambierebbe solo la stringa di dettaglio, e nessuno dei tre
    // server avrebbe niente da dire.
    let observed: Vec<&str> = qualified_filter_forms()
        .iter()
        .map(|case| case.name)
        .collect();
    assert_eq!(observed, QUALIFIED_FILTER_FORMS);
    assert_eq!(observed.len(), 13, "le forme qualificate sono tredici");

    // E nessuna delle due forme che il renderer rifiuta compare qui: se
    // ci finissero, `filter` si aprirebbe su una superficie che il flag
    // non sostiene.
    for closed in ["like_case_insensitive", "spatial"] {
        assert!(!observed.contains(&closed), "{closed} non e qualificata");
    }

    // Ogni forma porta i parametri che lega, e nessun altro: il provider
    // rifiuta un bag con voci che il piano non usa.
    for case in qualified_filter_forms() {
        let bound = bound_parameters(&case.expression);
        let provided: Vec<String> = case
            .parameters
            .iter()
            .map(|(name, _)| name.clone())
            .collect();
        let mut expected = bound;
        expected.sort();
        expected.dedup();
        let mut observed = provided;
        observed.sort();
        assert_eq!(observed, expected, "parametri della forma {}", case.name);
    }
}

/// I nomi dei parametri che un'espressione lega, in profondita.
fn bound_parameters(expression: &FilterExpression) -> Vec<String> {
    match expression {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            args.iter().flat_map(bound_parameters).collect()
        }
        FilterExpression::Eq { parameter, .. }
        | FilterExpression::Ne { parameter, .. }
        | FilterExpression::Lt { parameter, .. }
        | FilterExpression::Lte { parameter, .. }
        | FilterExpression::Gt { parameter, .. }
        | FilterExpression::Gte { parameter, .. }
        | FilterExpression::Like { parameter, .. } => vec![parameter.clone()],
        FilterExpression::In { parameters, .. } => parameters.clone(),
        FilterExpression::Between {
            lower_parameter,
            upper_parameter,
            ..
        } => vec![lower_parameter.clone(), upper_parameter.clone()],
        FilterExpression::IsNull { .. } | FilterExpression::IsNotNull { .. } => Vec::new(),
        FilterExpression::Spatial {
            geometry_parameter,
            distance_parameter,
            ..
        } => geometry_parameter
            .iter()
            .chain(distance_parameter)
            .cloned()
            .collect(),
    }
}

fn deliberate() -> RefusalContract {
    RefusalContract {
        category: ErrorCategory::Unsupported,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        message_contains: "filtro spatial richiede",
    }
}

fn refused() -> plenora_database_core::DatabaseError {
    plenora_database_core::DatabaseError {
        category: ErrorCategory::Unsupported,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: "filtro spatial richiede validazione WKB e SRID".to_owned(),
        diagnostics: None,
    }
}

#[test]
fn the_deliberate_refusal_is_recognised() {
    assert_eq!(refusal_mismatch(&deliberate(), &refused()), None);
}

#[test]
fn a_refusal_for_another_reason_is_not_the_one_expected() {
    // E il caso che conta: la sonda sul fail-close riceve un `Err` anche
    // quando la colonna non esiste o il parametro e del tipo sbagliato, e
    // senza questo confronto lo scambierebbe per la prova che cercava.
    for (what, error, expected) in [
        (
            "colonna inesistente",
            plenora_database_core::DatabaseError {
                category: ErrorCategory::Schema,
                message: "colonna non trovata".to_owned(),
                ..refused()
            },
            "categoria attesa",
        ),
        (
            "rifiuto in lettura invece che in prepare",
            plenora_database_core::DatabaseError {
                phase: ErrorPhase::Read,
                ..refused()
            },
            "fase attesa",
        ),
        (
            "effetto remoto ignoto",
            plenora_database_core::DatabaseError {
                remote_effect: RemoteEffect::Unknown,
                ..refused()
            },
            "effetto remoto atteso",
        ),
        (
            "rifiuto dichiarato ritentabile",
            plenora_database_core::DatabaseError {
                retry: RetryDisposition::Safe,
                ..refused()
            },
            "retry atteso",
        ),
        (
            "altro rifiuto della stessa famiglia",
            plenora_database_core::DatabaseError {
                message: "parametro spatial non valido".to_owned(),
                ..refused()
            },
            "il messaggio non porta",
        ),
    ] {
        let reported = refusal_mismatch(&deliberate(), &error)
            .unwrap_or_else(|| panic!("{what}: scambiato per il rifiuto atteso"));
        assert!(
            reported.contains(expected),
            "{what}: il verdetto non dice cosa non torna — {reported}"
        );
    }
}

fn observed() -> ReadOutcome {
    ReadOutcome {
        batches: 2,
        rows: 8_193,
        names: vec!["id".to_owned(), "payload".to_owned()],
        schema: String::new(),
        first_batch: String::new(),
        digest: String::new(),
        own_namespace: 2,
        foreign_namespace: 0,
        first_integer: Some(1),
    }
}

fn contract() -> ReadContract {
    ReadContract {
        columns: &["id", "payload"],
        rows: 8_193,
        batches: Some(2),
        first_integer: Some(1),
    }
}

#[test]
fn a_read_that_meets_the_contract_has_nothing_to_report() {
    assert_eq!(read_mismatch(&contract(), &observed()), None);
}

#[test]
fn every_clause_of_the_read_contract_can_fail_on_its_own() {
    // Una per difetto, e sono i difetti che le sonde live non potrebbero
    // distinguere da un successo: la projection ignorata, il filtro che
    // non filtra, l'ordinamento che non ordina, lo stream che consegna
    // tutto in un colpo, il namespace dell'altro prodotto.
    //
    // Sono tutti casi che restituiscono `Ok` al chiamante, ed e la
    // ragione per cui esiste questo validatore invece di un `is_ok()`.
    let perturbations: Vec<(&str, ReadOutcome, &str)> = vec![
        (
            "projection ignorata",
            ReadOutcome {
                names: vec!["id".to_owned(), "payload".to_owned(), "label".to_owned()],
                own_namespace: 3,
                ..observed()
            },
            "colonne attese",
        ),
        (
            "filtro che non filtra",
            ReadOutcome {
                rows: 8_192,
                ..observed()
            },
            "righe attese",
        ),
        (
            "stream consegnato in un colpo solo",
            ReadOutcome {
                batches: 1,
                ..observed()
            },
            "batch attesi",
        ),
        (
            "ordinamento che non ordina",
            ReadOutcome {
                first_integer: Some(8_193),
                ..observed()
            },
            "primo valore atteso",
        ),
        (
            "namespace dell'altro prodotto",
            ReadOutcome {
                foreign_namespace: 1,
                ..observed()
            },
            "namespace dell'altro prodotto",
        ),
        (
            "campo senza annotazione",
            ReadOutcome {
                own_namespace: 1,
                ..observed()
            },
            "campi annotati",
        ),
    ];
    for (what, outcome, expected) in perturbations {
        let reported = read_mismatch(&contract(), &outcome)
            .unwrap_or_else(|| panic!("{what}: il validatore non se n'e accorto"));
        assert!(
            reported.contains(expected),
            "{what}: il verdetto non dice cosa manca — {reported}"
        );
    }
}

#[test]
fn a_contract_without_a_question_does_not_invent_one() {
    // `columns` vuoto e `batches`/`first_integer` assenti significano "non
    // e questa la domanda", non "va bene qualunque cosa": cio che resta
    // dichiarato continua a essere verificato.
    let loose = ReadContract {
        columns: &[],
        rows: 8_193,
        batches: None,
        first_integer: None,
    };
    let different = ReadOutcome {
        names: vec!["altro".to_owned()],
        own_namespace: 1,
        batches: 9,
        first_integer: Some(42),
        ..observed()
    };
    assert_eq!(read_mismatch(&loose, &different), None);
    assert!(read_mismatch(
        &loose,
        &ReadOutcome {
            rows: 1,
            ..observed()
        }
    )
    .is_some());
}

#[test]
fn assignments_survive_a_comma_inside_a_quoted_value() {
    let parsed =
        sql_assignments("SET SESSION autocommit = 1, time_zone = '+00:00', sql_mode = 'A,B,C'");
    assert_eq!(
        parsed,
        vec![
            ("autocommit".to_owned(), "1".to_owned()),
            ("time_zone".to_owned(), "+00:00".to_owned()),
            ("sql_mode".to_owned(), "A,B,C".to_owned()),
        ],
        "le virgole dentro gli apici non separano assegnazioni"
    );
}

#[test]
fn the_real_bootstrap_parses_into_three_assignments() {
    let parsed = sql_assignments(crate::SESSION_BOOTSTRAP_SQL);
    let names: Vec<&str> = parsed.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(names, vec!["autocommit", "time_zone", "sql_mode"]);
}

#[test]
fn the_server_code_is_read_from_the_message_or_absent() {
    assert_eq!(
        server_code_in_message("errore server MySQL redatto (codice 1792)"),
        Some(1_792)
    );
    assert_eq!(
        server_code_in_message("colonna MySQL non valida (codice 1054)"),
        Some(1_054)
    );
    // Nessun codice: un errore che nasce prima del server non ne ha uno,
    // e dedurne zero sarebbe peggio che dire "assente".
    assert_eq!(
        server_code_in_message("schema Arrow vuoto per append"),
        None
    );
    assert_eq!(server_code_in_message("codice non numerico"), None);
}
