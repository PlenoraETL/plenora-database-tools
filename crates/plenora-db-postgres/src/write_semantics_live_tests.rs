//! La regola R per le mode che inseriscono, contro il fixture PostgreSQL:
//! regole di riscrittura e trigger `INSTEAD OF` per gli eventi della mode
//! rifiutano la scrittura prima di ogni effetto, in ogni profilo, e il dry run
//! riporta lo stesso rifiuto; cio che conta onestamente resta ammesso.
//!
//! Gli ingressi sono quelli della rilettura del 25/10 su d30d1d2: ciascun
//! rifiuto qui sotto era un `committed` con `inserted` contato per righe
//! finite in `altrove` o da nessuna parte (Prepared), o scritte nel target
//! scavalcando la regola (`COPY`), e Replace e `TruncateInsert` svuotavano il
//! target.

use super::write_visibility_live_tests::{
    dry_run, ids, live_dsn, operation, prepare, reset, write, SCHEMA, SERIAL,
};
use super::*;
use plenora_database_core::plan::WriteMode;
use plenora_database_core::row_visibility::{HIDDEN_BY_INSTEAD_TRIGGER, HIDDEN_BY_RULE};
use plenora_database_core::{ErrorCategory, ErrorPhase};

const PROFILES: [PostgresInsertMode; 3] = [
    PostgresInsertMode::Prepared,
    PostgresInsertMode::CopyText,
    PostgresInsertMode::CopyBinary,
];

const RULE_ELSEWHERE: &str = "CREATE TABLE {s}.altrove (id bigint, owner text); \
     CREATE RULE r AS ON INSERT TO {s}.t DO INSTEAD INSERT INTO {s}.altrove VALUES (NEW.id, NEW.owner);";
const RULE_NOTHING: &str = "CREATE TABLE {s}.altrove (id bigint, owner text); \
     CREATE RULE r AS ON INSERT TO {s}.t DO INSTEAD NOTHING;";
// `DO ALSO`: la riga arriva nel target, ma l'effetto in piu non e nel
// documento e `COPY` non lo produrrebbe.
const RULE_ALSO: &str = "CREATE TABLE {s}.altrove (id bigint, owner text); \
     CREATE RULE r AS ON INSERT TO {s}.t DO ALSO INSERT INTO {s}.altrove VALUES (NEW.id, NEW.owner);";
const RULE_UPDATE: &str = "CREATE TABLE {s}.altrove (id bigint, owner text); \
     CREATE RULE r AS ON UPDATE TO {s}.t DO INSTEAD NOTHING;";
// La vista con un `INSTEAD OF` che restituisce la riga senza scriverla.
const VIEW_INSTEAD_INSERT: &str = "CREATE TABLE {s}.altrove (id bigint, owner text); \
     CREATE VIEW {s}.v AS SELECT id, owner FROM {s}.t; \
     CREATE FUNCTION {s}.finge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; \
     CREATE TRIGGER finge INSTEAD OF INSERT ON {s}.v FOR EACH ROW EXECUTE FUNCTION {s}.finge();";
const VIEW_INSTEAD_UPDATE: &str = "CREATE TABLE {s}.altrove (id bigint, owner text); \
     CREATE VIEW {s}.v AS SELECT id, owner FROM {s}.t; \
     CREATE FUNCTION {s}.finge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$; \
     CREATE TRIGGER finge INSTEAD OF UPDATE ON {s}.v FOR EACH ROW EXECUTE FUNCTION {s}.finge();";
// Una vista sopra una tabella con la regola: l'INSERT riscritto sulla base
// incontra la regola della base.
const VIEW_OVER_RULE: &str = "CREATE TABLE {s}.altrove (id bigint, owner text); \
     CREATE RULE r AS ON INSERT TO {s}.t DO INSTEAD INSERT INTO {s}.altrove VALUES (NEW.id, NEW.owner); \
     CREATE VIEW {s}.v AS SELECT id, owner FROM {s}.t;";

/// Il caso: nome, preparazione, oggetto, mode, motivo atteso.
type Case = (
    &'static str,
    &'static str,
    &'static str,
    WriteMode,
    &'static str,
);

#[rustfmt::skip]
const CASES: &[Case] = &[
    ("regola-altrove-append", RULE_ELSEWHERE, "t", WriteMode::Append, HIDDEN_BY_RULE),
    ("regola-altrove-replace", RULE_ELSEWHERE, "t", WriteMode::Replace, HIDDEN_BY_RULE),
    ("regola-altrove-truncate-insert", RULE_ELSEWHERE, "t", WriteMode::TruncateInsert, HIDDEN_BY_RULE),
    ("regola-altrove-upsert", RULE_ELSEWHERE, "t", WriteMode::Upsert, HIDDEN_BY_RULE),
    ("regola-niente-append", RULE_NOTHING, "t", WriteMode::Append, HIDDEN_BY_RULE),
    ("regola-niente-replace", RULE_NOTHING, "t", WriteMode::Replace, HIDDEN_BY_RULE),
    ("regola-niente-truncate-insert", RULE_NOTHING, "t", WriteMode::TruncateInsert, HIDDEN_BY_RULE),
    ("regola-also-append", RULE_ALSO, "t", WriteMode::Append, HIDDEN_BY_RULE),
    ("regola-update-upsert", RULE_UPDATE, "t", WriteMode::Upsert, HIDDEN_BY_RULE),
    ("vista-instead-insert-append", VIEW_INSTEAD_INSERT, "v", WriteMode::Append, HIDDEN_BY_INSTEAD_TRIGGER),
    ("vista-instead-update-update", VIEW_INSTEAD_UPDATE, "v", WriteMode::Update, HIDDEN_BY_INSTEAD_TRIGGER),
    ("vista-sopra-regola-append", VIEW_OVER_RULE, "v", WriteMode::Append, HIDDEN_BY_RULE),
];

/// Le righe di `altrove`, dove una regola o una funzione manderebbero
/// l'ingresso.
async fn elsewhere(client: &tokio_postgres::Client) -> i64 {
    client
        .query_one(&format!("SELECT count(*) FROM {SCHEMA}.altrove"), &[])
        .await
        .expect("righe altrove")
        .get(0)
}

#[tokio::test]
async fn live_regola_r_rifiuta_regole_e_instead_of_per_le_mode_che_inseriscono() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let mut difformi = Vec::new();
    for (name, setup, object, mode, reason) in CASES {
        for profile in PROFILES {
            prepare(&admin, setup).await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let operation = operation(object, *mode);
            // 10 e 11 nuove; 2 esistente, per Upsert e Update che la toccano.
            let values: &[(i64, &str)] = if matches!(*mode, WriteMode::Upsert | WriteMode::Update) {
                &[(10, "a"), (2, "z")]
            } else {
                &[(10, "a"), (11, "a")]
            };
            let before = ids(&admin, "t").await;
            let dry = dry_run(&provider, &secret, &operation, values).await;
            let written = write(&provider, &secret, &operation, values).await;
            let after = ids(&admin, "t").await;
            let moved = elsewhere(&admin).await;
            let expected = |error: Option<&DatabaseError>| {
                error.is_some_and(|error| {
                    error.category == ErrorCategory::Unsupported
                        && error.phase == ErrorPhase::Write
                        && error.message.ends_with(reason)
                })
            };
            if !expected(written.as_ref().err()) || after != before || moved != 0 {
                difformi.push(format!(
                    "{name} {profile:?}: scrittura {:?}, righe {before:?} -> {after:?}, altrove {moved}",
                    written.map(|outcome| outcome.rows)
                ));
                continue;
            }
            // Il dry run non sa codificare `CopyBinary` senza i tipi del
            // target e lo dice: lo stesso rifiuto si prova negli altri due.
            if profile == PostgresInsertMode::CopyBinary
                && matches!(
                    *mode,
                    WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert
                )
            {
                continue;
            }
            let dry_error = dry.as_ref().err().map(|error| error.message.clone());
            if !expected(dry.as_ref().err())
                || dry_error != written.err().map(|error| error.message)
            {
                difformi.push(format!("{name} {profile:?}: dry run {dry:?}"));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// I controlli positivi: cio che conta onestamente non si rifiuta.
///
/// - un `BEFORE INSERT` di riga che restituisce `NULL` per la 11: la riga non
///   e scritta e non e contata (`confirmed` 1, `skipped` 1), in ogni mode
///   che inserisce e in ogni profilo;
/// - una tabella ordinaria con foreign key verso un padre, identita, default,
///   trigger `AFTER` di riga e trigger di statement, e una tabella
///   partizionata: ogni mode passa, con i conteggi delle righe davvero
///   scritte.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_regola_r_ammette_le_tabelle_ordinarie_e_i_before_insert() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let before_null = "CREATE FUNCTION {s}.salta() RETURNS trigger LANGUAGE plpgsql AS \
        $$ BEGIN IF NEW.id = 11 THEN RETURN NULL; END IF; RETURN NEW; END $$; \
        CREATE TRIGGER salta BEFORE INSERT ON {s}.t FOR EACH ROW EXECUTE FUNCTION {s}.salta();";
    let mut difformi = Vec::new();
    for profile in PROFILES {
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        for (mode, expected) in [
            (WriteMode::Append, vec![1, 2, 3, 10]),
            (WriteMode::TruncateInsert, vec![10]),
            (WriteMode::Replace, vec![10]),
            (WriteMode::Upsert, vec![1, 2, 3, 10]),
        ] {
            prepare(&admin, before_null).await;
            let outcome = write(
                &provider,
                &secret,
                &operation("t", mode),
                &[(10, "a"), (11, "a")],
            )
            .await;
            let rows = ids(&admin, "t").await;
            let counted = outcome
                .as_ref()
                .is_ok_and(|outcome| (outcome.rows.confirmed, outcome.rows.skipped) == (1, 1));
            if !counted || rows != expected {
                difformi.push(format!(
                    "before-insert-null {mode:?} {profile:?}: {:?}, righe {rows:?}",
                    outcome.map(|outcome| outcome.rows)
                ));
            }
        }
    }
    // La tabella ordinaria: il target `t` con una foreign key verso `padre`,
    // un'identita, un default, trigger AFTER di riga per ogni evento e un
    // trigger BEFORE di statement.
    let ordinary = "CREATE TABLE {s}.padre (id bigint PRIMARY KEY); \
        INSERT INTO {s}.padre SELECT generate_series(1, 20); \
        ALTER TABLE {s}.t ADD CONSTRAINT verso_padre FOREIGN KEY (id) REFERENCES {s}.padre (id), \
            ADD COLUMN n bigint GENERATED BY DEFAULT AS IDENTITY, \
            ADD COLUMN creato timestamptz NOT NULL DEFAULT now(); \
        CREATE FUNCTION {s}.nota() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$; \
        CREATE TRIGGER nota AFTER INSERT OR UPDATE OR DELETE ON {s}.t \
            FOR EACH ROW EXECUTE FUNCTION {s}.nota(); \
        CREATE TRIGGER nota_statement BEFORE INSERT OR UPDATE OR DELETE ON {s}.t \
            FOR EACH STATEMENT EXECUTE FUNCTION {s}.nota();";
    let partitioned = "DROP TABLE {s}.t; \
        CREATE TABLE {s}.t (id bigint PRIMARY KEY, owner text) PARTITION BY RANGE (id); \
        CREATE TABLE {s}.t_basse PARTITION OF {s}.t FOR VALUES FROM (0) TO (5); \
        CREATE TABLE {s}.t_alte PARTITION OF {s}.t FOR VALUES FROM (5) TO (100); \
        INSERT INTO {s}.t VALUES (1, 'a'), (2, 'b'), (3, 'a');";
    for (table, setup) in [("ordinaria", ordinary), ("partizionata", partitioned)] {
        for profile in PROFILES {
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            for (mode, input, expected, confirmed) in [
                (WriteMode::Append, vec![(10, "a")], vec![1, 2, 3, 10], 1),
                (WriteMode::TruncateInsert, vec![(10, "a")], vec![10], 1),
                (WriteMode::Replace, vec![(10, "a")], vec![10], 1),
                (
                    WriteMode::Upsert,
                    vec![(10, "a"), (2, "z")],
                    vec![1, 2, 3, 10],
                    2,
                ),
                (
                    WriteMode::Update,
                    vec![(2, "z"), (99, "z")],
                    vec![1, 2, 3],
                    1,
                ),
                (
                    WriteMode::DeleteByKeys,
                    vec![(2, "z"), (99, "z")],
                    vec![1, 3],
                    1,
                ),
            ] {
                // `TRUNCATE` di una tabella con una foreign key uscente e
                // ammesso; la partizionata non ha il padre.
                prepare(&admin, setup).await;
                let operation = operation("t", mode);
                let dry = if profile == PostgresInsertMode::CopyBinary {
                    Ok(())
                } else {
                    dry_run(&provider, &secret, &operation, &input)
                        .await
                        .map(|_| ())
                };
                let outcome = write(&provider, &secret, &operation, &input).await;
                let rows = ids(&admin, "t").await;
                let counted = outcome
                    .as_ref()
                    .is_ok_and(|outcome| outcome.rows.confirmed == confirmed);
                if dry.is_err() || !counted || rows != expected {
                    difformi.push(format!(
                        "{table} {mode:?} {profile:?}: dry {dry:?}, {:?}, righe {rows:?}",
                        outcome.map(|outcome| outcome.rows)
                    ));
                }
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
