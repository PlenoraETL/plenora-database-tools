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

/// La regola W1 (fedelta del percorso) per `IDENTITY ALWAYS`: `COPY` scrive
/// il valore dell'ingresso in una colonna `GENERATED ALWAYS AS IDENTITY`
/// (come `OVERRIDING SYSTEM VALUE`) e non avanza la sequenza, dove l'INSERT
/// preparato rifiuta la scrittura (428C9, `invalid_plan`). Fino al 25/10 le
/// righe 10 e 11 finivano nella colonna e il dry run `CopyText` diceva `ok`.
/// Ora `COPY` si rifiuta prima di ogni effetto, e il dry run concorda; con
/// l'ingresso che non porta la colonna `COPY` resta ammesso (la colonna
/// prende la sequenza, come nell'INSERT).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_w1_copy_non_scavalca_identity_always() {
    use super::write_visibility_live_tests::{dry_run_stream, owner_rows, write_stream};
    use plenora_database_core::row_visibility::BYPASSED_BY_PATH;
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let identity = "DROP TABLE {s}.t; \
        CREATE TABLE {s}.t (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, owner text); \
        INSERT INTO {s}.t (owner) VALUES ('a'), ('b'), ('a');";
    let mut difformi = Vec::new();
    for mode in [
        WriteMode::Append,
        WriteMode::TruncateInsert,
        WriteMode::Replace,
    ] {
        for profile in PROFILES {
            prepare(&admin, identity).await;
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let operation = operation("t", mode);
            let values = [(10, "a"), (11, "a")];
            let before = (ids(&admin, "t").await, last_value(&admin).await);
            let dry = dry_run(&provider, &secret, &operation, &values).await;
            let written = write(&provider, &secret, &operation, &values).await;
            let after = (ids(&admin, "t").await, last_value(&admin).await);
            let expected = |error: Option<&DatabaseError>| {
                error.is_some_and(|error| match profile {
                    PostgresInsertMode::Prepared => error.category == ErrorCategory::InvalidPlan,
                    _ => {
                        error.category == ErrorCategory::Unsupported
                            && error.phase == ErrorPhase::Write
                            && error.message.ends_with(BYPASSED_BY_PATH)
                    }
                })
            };
            if !expected(written.as_ref().err()) || after != before {
                difformi.push(format!(
                    "identity {mode:?} {profile:?}: scrittura {:?}, {before:?} -> {after:?}",
                    written.map(|outcome| outcome.rows)
                ));
                continue;
            }
            if profile == PostgresInsertMode::CopyBinary {
                continue;
            }
            let dry_error = dry.as_ref().err().map(|error| error.message.clone());
            if !expected(dry.as_ref().err())
                || dry_error != written.err().map(|error| error.message)
            {
                difformi.push(format!("identity {mode:?} {profile:?}: dry run {dry:?}"));
            }
        }
    }
    // Il controllo positivo: senza la colonna d'identita nell'ingresso
    // `COPY` applica la sequenza come l'INSERT.
    for profile in PROFILES {
        prepare(&admin, identity).await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let operation = operation("t", WriteMode::Append);
        let dry = if profile == PostgresInsertMode::CopyBinary {
            Ok(())
        } else {
            dry_run_stream(&provider, &secret, &operation, owner_rows(&["z"]))
                .await
                .map(|_| ())
        };
        let written = write_stream(&provider, &secret, &operation, owner_rows(&["z"])).await;
        let rows = ids(&admin, "t").await;
        if dry.is_err()
            || !written
                .as_ref()
                .is_ok_and(|outcome| outcome.rows.confirmed == 1)
            || rows != vec![1, 2, 3, 4]
        {
            difformi.push(format!(
                "identity senza colonna {profile:?}: dry {dry:?}, {:?}, righe {rows:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// L'ultimo valore della sequenza d'identita di `t`.
async fn last_value(client: &tokio_postgres::Client) -> Option<i64> {
    client
        .query_one(
            &format!(
                "SELECT last_value FROM pg_catalog.pg_sequences \
                 WHERE schemaname = '{SCHEMA}' AND sequencename = 't_id_seq'"
            ),
            &[],
        )
        .await
        .expect("sequenza")
        .get(0)
}

/// La regola W2 (sessione fissata) per `session_replication_role`: un ruolo
/// con `ALTER ROLE ... SET session_replication_role = replica` scriveva con
/// i trigger ordinari spenti, foreign key comprese (sono trigger di sistema
/// `ORIGIN`): Append committava un orfano e un `BEFORE INSERT` che solleva
/// non scattava, e il dry run diceva `ok`. Ora la scrittura riporta la
/// sessione a `origin` nella propria transazione; se il ruolo non puo, si
/// rifiuta prima di ogni effetto e il dry run concorda. Con il permesso
/// (`GRANT SET ON PARAMETER`) la sessione si fissa e il target applica le
/// proprie garanzie: l'orfano e la riga che il trigger rifiuta falliscono.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn live_w2_session_replication_role_fissato_a_origin() {
    use super::write_visibility_live_tests::dsn_as_role;
    use plenora_database_core::row_visibility::SESSION_WITHOUT_GUARANTEES;
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let admin_secret = SecretString::new(dsn.clone());
    let role_secret = SecretString::new(dsn_as_role(&dsn));
    let admin = PostgresProvider::connect(&admin_secret)
        .await
        .expect("client");
    let foreign_key = "CREATE TABLE {s}.padre (id bigint PRIMARY KEY); \
        INSERT INTO {s}.padre VALUES (1), (2), (3), (10); \
        GRANT SELECT ON {s}.padre TO {r}; \
        ALTER TABLE {s}.t ADD CONSTRAINT verso_padre FOREIGN KEY (id) REFERENCES {s}.padre (id); \
        ALTER ROLE {r} SET session_replication_role = replica;";
    let before_raise = "CREATE FUNCTION {s}.vieta() RETURNS trigger LANGUAGE plpgsql AS \
        $$ BEGIN RAISE EXCEPTION 'vietato'; END $$; \
        CREATE TRIGGER vieta BEFORE INSERT ON {s}.t FOR EACH ROW EXECUTE FUNCTION {s}.vieta(); \
        ALTER ROLE {r} SET session_replication_role = replica;";
    let grant = " GRANT SET ON PARAMETER session_replication_role TO {r};";
    let mut difformi = Vec::new();
    for (name, setup) in [("fk", foreign_key), ("before-raise", before_raise)] {
        for profile in PROFILES {
            let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
            let operation = operation("t", WriteMode::Append);
            // 99 non ha un padre; il trigger rifiuta ogni riga.
            let values = [(10, "a"), (99, "a")];
            // Senza permesso: rifiuto prima di ogni effetto.
            prepare(&admin, setup).await;
            let before = ids(&admin, "t").await;
            let dry = dry_run(&provider, &role_secret, &operation, &values).await;
            let written = write(&provider, &role_secret, &operation, &values).await;
            let after = ids(&admin, "t").await;
            let refused = |error: Option<&DatabaseError>| {
                error.is_some_and(|error| {
                    error.category == ErrorCategory::Unsupported
                        && error.message.ends_with(SESSION_WITHOUT_GUARANTEES)
                })
            };
            if !refused(written.as_ref().err()) || after != before {
                difformi.push(format!(
                    "{name} {profile:?} senza permesso: {:?}, {before:?} -> {after:?}",
                    written.map(|outcome| outcome.rows)
                ));
            } else if profile != PostgresInsertMode::CopyBinary
                && (!refused(dry.as_ref().err())
                    || dry.as_ref().err().map(|error| error.message.clone())
                        != written.err().map(|error| error.message))
            {
                difformi.push(format!(
                    "{name} {profile:?} senza permesso: dry run {dry:?}"
                ));
            }
            // Con il permesso: la sessione si fissa e il target rifiuta.
            prepare(&admin, &format!("{setup}{grant}")).await;
            let before = ids(&admin, "t").await;
            let written = write(&provider, &role_secret, &operation, &values).await;
            let after = ids(&admin, "t").await;
            // Il rifiuto e del target (foreign key, trigger), non della regola.
            let by_target = written
                .as_ref()
                .is_err_and(|error| error.category != ErrorCategory::Unsupported);
            if !by_target || after != before {
                difformi.push(format!(
                    "{name} {profile:?} con permesso: {:?}, {before:?} -> {after:?}",
                    written.map(|outcome| outcome.rows)
                ));
            }
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// N8: il percorso row-scoped (Append con il totale dichiarato) e un `BEFORE
/// INSERT` che restituisce `NULL`. Fino al 25/10 la riga soppressa usciva
/// come `protocol` («INSERT diagnostico deve interessare esattamente una
/// riga») e la scrittura non partiva; negli altri percorsi la stessa riga e
/// `skipped`. Ora e `skipped` anche qui: `confirmed` 2, `skipped` 1.
#[tokio::test]
async fn live_row_scoped_before_insert_null_conta_skipped() {
    use super::write_visibility_live_tests::{declared_rows, write_stream};
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
        prepare(&admin, before_null).await;
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let outcome = write_stream(
            &provider,
            &secret,
            &operation("t", WriteMode::Append),
            declared_rows(&[(10, "a"), (11, "a"), (12, "a")]),
        )
        .await;
        let rows = ids(&admin, "t").await;
        let counted = outcome.as_ref().is_ok_and(|outcome| {
            (
                outcome.rows.received,
                outcome.rows.confirmed,
                outcome.rows.inserted,
                outcome.rows.skipped,
            ) == (3, 2, Some(2), 1)
        });
        if !counted || rows != vec![1, 2, 3, 10, 12] {
            difformi.push(format!(
                "row-scoped {profile:?}: {:?}, righe {rows:?}",
                outcome.map(|outcome| outcome.rows)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}

/// Uno scenario che il target rifiuta: nome, preparazione, ingresso.
type Refusal = (&'static str, &'static str, &'static [(i64, &'static str)]);

/// La regola W1, il lato che ammette: ogni altra garanzia del target `COPY`
/// la applica come l'INSERT preparato, ed e cio che rende chiuso l'elenco di
/// `write::visibility`. Su ogni profilo una riga che viola un `CHECK`, il
/// vincolo di un dominio, l'unicita o un `BEFORE INSERT` che solleva fa
/// fallire la scrittura senza effetti; una colonna generata assente
/// dall'ingresso e calcolata.
#[tokio::test]
async fn live_w1_copy_applica_le_altre_garanzie_del_target() {
    let Some(dsn) = live_dsn() else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let secret = SecretString::new(dsn);
    let admin = PostgresProvider::connect(&secret).await.expect("client");
    let refusals: [Refusal; 4] = [
        (
            "check",
            "ALTER TABLE {s}.t ADD CONSTRAINT piccolo CHECK (id < 100);",
            &[(10, "a"), (200, "a")],
        ),
        (
            "dominio",
            "CREATE DOMAIN {s}.etichetta AS text CHECK (VALUE <> 'x'); \
             ALTER TABLE {s}.t ALTER COLUMN owner TYPE {s}.etichetta;",
            &[(10, "a"), (11, "x")],
        ),
        ("unicita", "", &[(10, "a"), (2, "a")]),
        (
            "before-solleva",
            "CREATE FUNCTION {s}.vieta() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN IF NEW.id = 11 THEN RAISE EXCEPTION 'vietato'; END IF; RETURN NEW; END $$; \
             CREATE TRIGGER vieta BEFORE INSERT ON {s}.t FOR EACH ROW EXECUTE FUNCTION {s}.vieta();",
            &[(10, "a"), (11, "a")],
        ),
    ];
    let mut difformi = Vec::new();
    for profile in PROFILES {
        let provider = PostgresProvider::insecure_local().with_insert_mode(profile);
        let append = operation("t", WriteMode::Append);
        for (name, setup, values) in refusals {
            prepare(&admin, setup).await;
            let before = ids(&admin, "t").await;
            let written = write(&provider, &secret, &append, values).await;
            let after = ids(&admin, "t").await;
            if written.is_ok() || after != before {
                difformi.push(format!(
                    "{name} {profile:?}: {:?}, {before:?} -> {after:?}",
                    written.map(|outcome| outcome.rows)
                ));
            }
        }
        prepare(
            &admin,
            "ALTER TABLE {s}.t ADD COLUMN g text GENERATED ALWAYS AS (owner || '!') STORED;",
        )
        .await;
        let written = write(&provider, &secret, &append, &[(10, "a")]).await;
        let generated: Option<String> = admin
            .query_one(&format!("SELECT g FROM {SCHEMA}.t WHERE id = 10"), &[])
            .await
            .ok()
            .and_then(|row| row.get(0));
        if written.is_err() || generated.as_deref() != Some("a!") {
            difformi.push(format!(
                "generata {profile:?}: {:?}, {generated:?}",
                written.map(|outcome| outcome.rows)
            ));
        }
    }
    reset(&admin).await;
    assert!(difformi.is_empty(), "{}", difformi.join("\n"));
}
