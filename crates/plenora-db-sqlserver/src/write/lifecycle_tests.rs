use super::*;

#[test]
fn derived_names_are_bounded_distinct_and_keep_the_role() {
    let base = "a".repeat(crate::MAX_IDENTIFIER_CHARACTERS);
    let stage = derived_object_name(&base, "stage", 41).expect("stage name");
    let backup = derived_object_name(&base, "backup", 41).expect("backup name");
    assert!(stage.chars().count() <= crate::MAX_IDENTIFIER_CHARACTERS);
    assert!(backup.chars().count() <= crate::MAX_IDENTIFIER_CHARACTERS);
    assert_ne!(stage, backup);
    assert!(stage.contains("__pln_stage_"));
    assert!(backup.contains("__pln_backup_"));
}

#[test]
fn derived_names_reject_an_unrepresentable_role() {
    let role = "r".repeat(crate::MAX_IDENTIFIER_CHARACTERS);
    let error = derived_object_name("asset", &role, 1).expect_err("oversized suffix");
    assert_eq!(error.category, ErrorCategory::ResourceLimit);
}

#[test]
fn replace_catalog_query_mentions_only_columns_present_on_the_server() {
    let legacy = replace_external_state_sql(false, false);
    assert!(!legacy.contains("ledger_type"));
    assert!(!legacy.contains("xml_compression"));
    let current = replace_external_state_sql(true, true);
    assert!(current.contains("ledger_type"));
    assert!(current.contains("xml_compression"));
}

/// Il ciclo di publish di una `Replace`, per intero e nell'ordine.
///
/// SQL Server non rinomina in un colpo solo: il target diventa il backup,
/// lo staging diventa il target, e il backup sparisce. Ogni passo dipende
/// dal precedente, e invertirne due lascerebbe il target con i dati
/// vecchi o senza dati affatto. Il test fissa quindi l'intera sequenza di
/// publish, composta da due `sp_rename` e dal drop del backup.
#[test]
fn the_replace_publish_renames_twice_and_drops_only_the_backup() {
    let publish = PublishStatement::new(
        "[dbo].[assets]",
        "[dbo].[assets__pln_stage_1_2]",
        "[dbo].[assets__pln_backup_1_2]",
        "assets",
        "assets__pln_backup_1_2",
    );

    // Due rinomine, e in quest'ordine.
    assert_eq!(publish.sql.matches("sp_rename").count(), 2);
    let first = publish.sql.find("sp_rename").expect("prima rinomina");
    let second = publish.sql[first + 1..]
        .find("sp_rename")
        .expect("seconda rinomina")
        + first
        + 1;
    let drop = publish.sql.find("DROP TABLE").expect("drop del backup");
    assert!(first < second && second < drop, "{}", publish.sql);

    // I quattro parametri dicono chi diventa cosa.
    assert_eq!(
        publish.binds,
        vec![
            // 1. il target corrente prende il nome del backup
            "[dbo].[assets]".to_owned(),
            "assets__pln_backup_1_2".to_owned(),
            // 2. lo staging prende il nome del target
            "[dbo].[assets__pln_stage_1_2]".to_owned(),
            "assets".to_owned(),
        ]
    );

    // 3. e sparisce il backup, non il target.
    assert!(publish
        .sql
        .contains("DROP TABLE [dbo].[assets__pln_backup_1_2];"));
    assert!(!publish.sql.contains("DROP TABLE [dbo].[assets];"));
}

/// Il nome nuovo passato a `sp_rename` non e qualificato.
///
/// `sp_rename` interpreta il secondo argomento come il nome che l'oggetto
/// deve assumere, non come un riferimento: passarlo qualificato produce un
/// oggetto il cui nome contiene le parentesi quadre.
#[test]
fn the_new_names_are_bare_and_the_old_ones_qualified() {
    let publish = PublishStatement::new(
        "[dbo].[assets]",
        "[dbo].[assets__pln_stage_1_2]",
        "[dbo].[assets__pln_backup_1_2]",
        "assets",
        "assets__pln_backup_1_2",
    );
    assert!(publish.binds[0].starts_with('['));
    assert!(!publish.binds[1].contains('['));
    assert!(publish.binds[2].starts_with('['));
    assert!(!publish.binds[3].contains('['));
}
