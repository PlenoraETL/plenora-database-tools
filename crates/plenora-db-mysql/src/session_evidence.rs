//! Misura della semantica di sessione sui tre riferimenti accesi.
//!
//! La fase 1 ha lasciato fuori dal profilo il bootstrap di sessione, i livelli
//! di isolamento e `START TRANSACTION`, dichiarandoli residui. Un residuo non
//! e una decisione: prima di aggiungere un secondo profilo bisogna sapere se
//! quelle superfici **coincidono** sui tre server, perche la regola dipende
//! dalla risposta — cio che coincide resta codice condiviso presidiato da una
//! prova, cio che diverge entra nel profilo, e nient'altro vi entra per
//! simmetria.
//!
//! Due cose che questa misura fa e che un client non farebbe:
//!
//! * esegue **l'esatto** `SESSION_BOOTSTRAP_SQL` del pool, non una sua
//!   parafrasi, e rilegge le variabili che pretende di fissare;
//! * attraversa il **percorso reale** del pool, dove il bootstrap arriva come
//!   `setup` del driver e viene applicato prima di qualunque probe. Eseguire
//!   lo stesso SQL a mano direbbe del server, non del pool.
//!
//! **`accepted` significa contratto soddisfatto, non misura riuscita.** E la
//! differenza che rende utile la matrice: una sonda che registrasse "ho
//! ottenuto una risposta" farebbe passare per accordo tre server che sbagliano
//! allo stesso modo — un READ ONLY che accetta scritture ovunque, quattro
//! livelli che riportano tutti lo stesso valore, un rollback che non annulla.
//! Ogni sonda dichiara cosa si aspetta, e cio che ha osservato viene
//! confrontato con quello.
//!
//! Nessuna sonda fa `panic` su un errore del server: l'errore e la misura. Il
//! test fallisce solo se l'harness non riesce a misurare — server
//! irraggiungibile, TLS assente, fixture non creabile.

// La sessione vive fino alla fine della sonda per costruzione: e cio che si
// sta misurando. Rilasciarla prima renderebbe la misura piu corta della
// domanda.
#![allow(clippy::significant_drop_tightening)]

use crate::evidence::{condense, config, environment, Observation, Recorder};
use crate::profile::MYSQL_PROFILE;
use mysql_async::prelude::Queryable;
use plenora_database_core::provider::ParameterValue;
use plenora_database_core::session_context::{SessionContext, SessionEntry, SessionValue};
use plenora_database_core::transaction::{
    AccessMode, IsolationLevel, Statement, TransactionOptions, TransactionScope,
};
use plenora_database_core::{CancellationToken, DatabaseError, ErrorPhase};
use serde_json::json;

/// Il marcatore che il runner cerca nell'output di `cargo test --nocapture`.
const MARKER: &str = "PLENORA_SESSION_EVIDENCE ";

/// Tabella di lavoro: la crea la misura, e la droppa.
const SCRATCH: &str = "plenora_session_evidence";

/// Cio che il bootstrap pretende di fissare.
const OBSERVED_VARIABLES: &str = "SELECT @@autocommit, @@time_zone, @@sql_mode";

/// I valori che il bootstrap deve produrre.
///
/// Scritti qui e non dedotti dalla costante: dedurli significherebbe
/// confrontare la costante con se stessa, e una sonda che si aspetta cio che
/// ha appena letto non verifica niente. Che descrivano davvero
/// `SESSION_BOOTSTRAP_SQL` lo controlla `expectations_match_the_bootstrap`
/// prima di misurare — se qualcuno cambia la costante e non queste attese, e
/// un guasto dell'harness e va visto subito.
const EXPECTED_AUTOCOMMIT: &str = "1";
const EXPECTED_TIME_ZONE: &str = "+00:00";
const EXPECTED_SQL_MODE: &str =
    "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";

/// L'esito di una sonda: cosa si e osservato, cosa serviva, e se coincidono.
struct Measured {
    detail: String,
    expectation: String,
    satisfied: bool,
}

impl Measured {
    fn new(detail: impl Into<String>, expectation: impl Into<String>, satisfied: bool) -> Self {
        Self {
            detail: detail.into(),
            expectation: expectation.into(),
            satisfied,
        }
    }
}

type Probe = Result<Measured, DatabaseError>;

/// Registra una sonda. `accepted` solo se il contratto e soddisfatto.
fn record(
    recorder: &mut Recorder,
    probe: &'static str,
    surface: &'static str,
    question: &'static str,
    outcome: Probe,
) {
    match outcome {
        Ok(measured) if measured.satisfied => {
            recorder.accepted(probe, "session", surface, question, measured.detail);
        }
        Ok(measured) => recorder.rejected(
            probe,
            "session",
            surface,
            question,
            format!(
                "osservato: {} — atteso: {}",
                measured.detail, measured.expectation
            ),
            None,
        ),
        Err(error) => recorder.rejected(
            probe,
            "session",
            surface,
            question,
            condense(&format!("{:?}: {}", error.category, error.message)),
            None,
        ),
    }
}

fn pool(connections: usize) -> Result<crate::MysqlPool, DatabaseError> {
    crate::MysqlPool::new_with_profile(&config(), connections, &MYSQL_PROFILE)
}

/// Le attese del bootstrap devono descrivere la costante che si sta misurando.
///
/// # Panics
///
/// Se divergono: e un guasto dell'harness, va chiuso prima di leggere i
/// numeri e non registrato come divergenza fra server.
fn expectations_match_the_bootstrap() {
    for expected in [
        format!("autocommit = {EXPECTED_AUTOCOMMIT}"),
        format!("time_zone = '{EXPECTED_TIME_ZONE}'"),
        format!("sql_mode = '{EXPECTED_SQL_MODE}'"),
    ] {
        assert!(
            crate::SESSION_BOOTSTRAP_SQL.contains(&expected),
            "l'attesa {expected} non compare in SESSION_BOOTSTRAP_SQL: harness, non divergenza"
        );
    }
}

fn describe(autocommit: &str, time_zone: &str, sql_mode: &str) -> String {
    format!("autocommit={autocommit} time_zone={time_zone} sql_mode={sql_mode}")
}

fn bootstrap_expectation() -> String {
    describe(EXPECTED_AUTOCOMMIT, EXPECTED_TIME_ZONE, EXPECTED_SQL_MODE)
}

fn bootstrap_satisfied(observed: &(String, String, String)) -> bool {
    observed.0 == EXPECTED_AUTOCOMMIT
        && observed.1 == EXPECTED_TIME_ZONE
        && observed.2 == EXPECTED_SQL_MODE
}

/// Il bootstrap eseguito a mano: dice del server.
async fn bootstrap_statement() -> Probe {
    let mut connection = mysql_async::Conn::new(
        config()
            .driver_opts("MySQL")
            .expect("opzioni driver: harness, non divergenza"),
    )
    .await
    .expect("connessione: harness, non divergenza");
    if let Err(error) = connection.query_drop(crate::SESSION_BOOTSTRAP_SQL).await {
        return Ok(Measured::new(
            format!("statement rifiutato: {}", condense(&format!("{error}"))),
            bootstrap_expectation(),
            false,
        ));
    }
    let observed = connection
        .query_first::<(String, String, String), _>(OBSERVED_VARIABLES)
        .await
        .expect("lettura variabili: harness, non divergenza")
        .expect("variabili presenti");
    Ok(Measured::new(
        describe(&observed.0, &observed.1, &observed.2),
        bootstrap_expectation(),
        bootstrap_satisfied(&observed),
    ))
}

/// Il bootstrap ricevuto dal pool: dice del pool.
async fn bootstrap_through_pool() -> Probe {
    let cancel = CancellationToken::new();
    let pool = pool(2)?;
    let mut session = pool.checkout(&cancel).await?;
    let observed = read_variables(&mut session, &cancel).await?;
    Ok(Measured::new(
        describe(&observed.0, &observed.1, &observed.2),
        bootstrap_expectation(),
        bootstrap_satisfied(&observed),
    ))
}

/// Il bootstrap su una sessione consegnata **dopo** che una sporca e tornata.
///
/// La domanda e se lo stato alterato da una sessione possa arrivare alla
/// successiva. Si sporcano `autocommit`, `time_zone` e `sql_mode`, si restituisce
/// sessione a un pool a una connessione sola, e si rilegge sulla successiva
/// registrando l'identita di entrambe.
///
/// L'identita e osservata, non pretesa: `mysql_async` non ha riconsegnato la
/// stessa connessione in nessuno dei tre riferimenti — ne apre una nuova — e
/// pretendere il riuso avrebbe fatto fallire una sonda su cio che il driver
/// non fa. Il dettaglio lo dice, perche la conseguenza va letta: cio che
/// resta provato e che **ogni sessione consegnata dal pool e bootstrappata**,
/// che e la proprieta su cui il provider poggia; la riapplicazione del
/// bootstrap su una connessione riusata non e esercitata da questa
/// configurazione, ed e scritto nella matrice invece di essere sottinteso.
async fn bootstrap_after_return() -> Probe {
    let cancel = CancellationToken::new();
    let pool = pool(1)?;

    let mut session = pool.checkout(&cancel).await?;
    let first = connection_id(&mut session, &cancel).await?;
    session
        .query_rows(
            "SET SESSION autocommit = 0, time_zone = '+05:00', sql_mode = 'ANSI_QUOTES'",
            ErrorPhase::Prepare,
            &cancel,
        )
        .await?;
    let dirtied = read_variables(&mut session, &cancel).await?;
    if bootstrap_satisfied(&dirtied) {
        // Se lo stato non si e sporcato, la sonda non prova nulla: sarebbe di
        // nuovo la lettura di una sessione gia corretta, cioe il difetto che
        // questa versione corregge.
        return Ok(Measured::new(
            format!(
                "lo stato non si e sporcato: {}",
                describe(&dirtied.0, &dirtied.1, &dirtied.2)
            ),
            "stato alterato prima della restituzione",
            false,
        ));
    }
    drop(session);

    // La restituzione al pool e asincrona: chiedere subito la successiva
    // misurerebbe l'apertura invece del comportamento del pool.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut next = pool.checkout(&cancel).await?;
    let second = connection_id(&mut next, &cancel).await?;
    let observed = read_variables(&mut next, &cancel).await?;
    Ok(Measured::new(
        // Gli identificatori non entrano nel dettaglio: cambiano a ogni
        // corsa e per server, e renderebbero la matrice divergente per
        // costruzione — un rumore che poi si zittisce mettendo la sonda fra
        // quelle confrontate per solo esito, cioe smettendo di guardarne il
        // testo. Entra il fatto derivato, che e stabile e confrontabile.
        format!(
            "connessione riusata={} {}",
            first == second,
            describe(&observed.0, &observed.1, &observed.2)
        ),
        bootstrap_expectation(),
        bootstrap_satisfied(&observed),
    ))
}

async fn read_variables(
    session: &mut crate::MysqlSession,
    cancel: &CancellationToken,
) -> Result<(String, String, String), DatabaseError> {
    let rows = session
        .query_rows(OBSERVED_VARIABLES, ErrorPhase::Probe, cancel)
        .await?;
    let row = rows.first().expect("riga delle variabili");
    Ok((
        row.get::<String, _>(0).unwrap_or_default(),
        row.get::<String, _>(1).unwrap_or_default(),
        row.get::<String, _>(2).unwrap_or_default(),
    ))
}

async fn connection_id(
    session: &mut crate::MysqlSession,
    cancel: &CancellationToken,
) -> Result<u64, DatabaseError> {
    let rows = session
        .query_rows("SELECT CONNECTION_ID()", ErrorPhase::Probe, cancel)
        .await?;
    Ok(rows
        .first()
        .and_then(|row| row.get(0))
        .expect("identita della connessione: harness, non divergenza"))
}

/// Il valore di un `ParameterValue` senza la decorazione del `Debug`.
fn plain(value: &ParameterValue) -> String {
    match value {
        ParameterValue::String(value) => value.clone(),
        other => condense(&format!("{other:?}")),
    }
}

/// Il livello che la sessione dichiara dentro la transazione.
async fn isolation_probe(level: IsolationLevel, expected: &str) -> Probe {
    let options = TransactionOptions {
        isolation: Some(level),
        ..TransactionOptions::default()
    };
    let cancel = CancellationToken::new();
    let pool = pool(2)?;
    let session = pool.checkout(&cancel).await?;
    let mut transaction =
        crate::transaction::MysqlTransaction::begin(session, &options, &cancel).await?;
    let rows = transaction
        .query(&Statement::new("SELECT @@transaction_isolation"), &cancel)
        .await?;
    let observed = rows
        .first()
        .and_then(|row| row.values().first())
        .map_or_else(|| "assente".to_owned(), plain);
    Box::new(transaction).commit(&cancel).await?;
    Ok(Measured::new(
        observed.clone(),
        expected.to_owned(),
        observed == expected,
    ))
}

/// Una scrittura dentro la transazione: ammessa o rifiutata dalla modalita.
async fn write_inside_transaction(mode: Option<AccessMode>, admits: bool) -> Probe {
    let options = TransactionOptions {
        access_mode: mode,
        ..TransactionOptions::default()
    };
    let cancel = CancellationToken::new();
    let pool = pool(2)?;
    let table = format!("{SCRATCH}_rw");
    let mut setup = pool.checkout(&cancel).await?;
    for sql in [
        format!("DROP TABLE IF EXISTS {table}"),
        format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
    ] {
        setup.query_rows(&sql, ErrorPhase::Prepare, &cancel).await?;
    }
    drop(setup);

    let session = pool.checkout(&cancel).await?;
    let mut transaction =
        crate::transaction::MysqlTransaction::begin(session, &options, &cancel).await?;
    let attempt = transaction
        .execute(
            &Statement::new(format!("INSERT INTO {table} (id) VALUES (1)")),
            &cancel,
        )
        .await;
    let admitted = attempt.is_ok();
    let observed = match &attempt {
        Ok(_) => "scrittura ammessa".to_owned(),
        Err(error) => format!("scrittura rifiutata ({:?})", error.category),
    };
    let _ = Box::new(transaction).rollback(&cancel).await;

    let mut cleanup = pool.checkout(&cancel).await?;
    let _ = cleanup
        .query_rows(
            &format!("DROP TABLE IF EXISTS {table}"),
            ErrorPhase::Prepare,
            &cancel,
        )
        .await;
    Ok(Measured::new(
        observed,
        if admits {
            "scrittura ammessa"
        } else {
            "scrittura rifiutata"
        },
        admitted == admits,
    ))
}

/// La variabile utente che il provider imposta per il session context.
async fn context_inside_transaction() -> Probe {
    let mut context = SessionContext::new();
    context
        .insert(
            "plenora.tenant",
            SessionEntry::public(SessionValue::Text("acme".to_owned())),
        )
        .expect("chiave di contesto valida: harness, non divergenza");
    let options = TransactionOptions {
        context,
        ..TransactionOptions::default()
    };
    let cancel = CancellationToken::new();
    let pool = pool(2)?;
    let session = pool.checkout(&cancel).await?;
    let mut transaction =
        crate::transaction::MysqlTransaction::begin(session, &options, &cancel).await?;
    let rows = transaction
        .query(
            &Statement::new("SELECT @`plenora_ctx_plenora.tenant`"),
            &cancel,
        )
        .await?;
    let observed = rows
        .first()
        .and_then(|row| row.values().first())
        .map_or_else(|| "assente".to_owned(), plain);
    Box::new(transaction).commit(&cancel).await?;
    Ok(Measured::new(
        observed.clone(),
        "acme".to_owned(),
        observed == "acme",
    ))
}

/// Commit e rollback misurati sull'effetto, su una seconda sessione.
async fn durability_round(id: u8, durable: bool) -> Probe {
    let cancel = CancellationToken::new();
    let pool = pool(2)?;
    let mut setup = pool.checkout(&cancel).await?;
    for sql in [
        format!("DROP TABLE IF EXISTS {SCRATCH}"),
        format!("CREATE TABLE {SCRATCH} (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
    ] {
        setup.query_rows(&sql, ErrorPhase::Prepare, &cancel).await?;
    }
    drop(setup);

    let session = pool.checkout(&cancel).await?;
    let mut transaction = crate::transaction::MysqlTransaction::begin(
        session,
        &TransactionOptions::default(),
        &cancel,
    )
    .await?;
    transaction
        .execute(
            &Statement::new(format!("INSERT INTO {SCRATCH} (id) VALUES ({id})")),
            &cancel,
        )
        .await?;
    if durable {
        Box::new(transaction).commit(&cancel).await?;
    } else {
        Box::new(transaction).rollback(&cancel).await?;
    }

    // L'effetto si rilegge su una **seconda** sessione: leggerlo su quella
    // della transazione confonderebbe la durabilita con la visibilita.
    let mut reader = pool.checkout(&cancel).await?;
    let rows = reader
        .query_rows(
            &format!("SELECT COUNT(*) FROM {SCRATCH} WHERE id = {id}"),
            ErrorPhase::Read,
            &cancel,
        )
        .await?;
    let observed: i64 = rows
        .first()
        .and_then(|row| row.get(0))
        .expect("conteggio della misura: harness, non divergenza");
    let _ = reader
        .query_rows(
            &format!("DROP TABLE IF EXISTS {SCRATCH}"),
            ErrorPhase::Prepare,
            &cancel,
        )
        .await;
    let expected = i64::from(durable);
    Ok(Measured::new(
        format!("righe={observed}"),
        format!("righe={expected}"),
        observed == expected,
    ))
}

/// La misura della semantica di sessione sui tre riferimenti.
///
/// `#[ignore]` come per ADR 0014: pretende un server live esplicito, e il nome
/// non porta il prefisso `live_` che i runner del gate filtrano.
///
/// # Panics
///
/// Se l'harness non riesce a misurare. Sono guasti suoi, non divergenze.
#[tokio::test]
#[ignore = "misura della semantica di sessione: richiede un riferimento live esplicito"]
#[allow(clippy::too_many_lines)]
async fn session_semantics_evidence() {
    expectations_match_the_bootstrap();

    // Il bypass serve ai due riferimenti MariaDB: senza, la probe li rifiuta
    // e nessuna sonda arriva alla sessione. Non tocca nient'altro.
    let _bypass = crate::catalog::MariadbRejectionBypass::engage();

    let mut connection = mysql_async::Conn::new(
        config()
            .driver_opts("MySQL")
            .expect("opzioni driver: harness, non divergenza"),
    )
    .await
    .expect("connessione: harness, non divergenza");
    let identity: (String, String) = connection
        .query_first("SELECT VERSION(), @@version_comment")
        .await
        .expect("identita del server: harness, non divergenza")
        .expect("identita presente");

    let mut recorder = Recorder(Vec::new());

    record(
        &mut recorder,
        "bootstrap.statement",
        "bootstrap",
        "il server applica l'esatto SESSION_BOOTSTRAP_SQL del pool",
        bootstrap_statement().await,
    );
    record(
        &mut recorder,
        "bootstrap.pool",
        "bootstrap",
        "il pool consegna sessioni gia bootstrappate",
        bootstrap_through_pool().await,
    );
    record(
        &mut recorder,
        "bootstrap.after_return",
        "bootstrap",
        "una sessione consegnata dopo il rientro di una sporca e bootstrappata",
        bootstrap_after_return().await,
    );

    for (probe, level, expected) in [
        (
            "transaction.isolation.read_uncommitted",
            IsolationLevel::ReadUncommitted,
            "READ-UNCOMMITTED",
        ),
        (
            "transaction.isolation.read_committed",
            IsolationLevel::ReadCommitted,
            "READ-COMMITTED",
        ),
        (
            "transaction.isolation.repeatable_read",
            IsolationLevel::RepeatableRead,
            "REPEATABLE-READ",
        ),
        (
            "transaction.isolation.serializable",
            IsolationLevel::Serializable,
            "SERIALIZABLE",
        ),
    ] {
        record(
            &mut recorder,
            probe,
            "isolation",
            "la sessione dichiara il livello richiesto",
            isolation_probe(level, expected).await,
        );
    }

    for (probe, mode, admits) in [
        ("transaction.access_mode.absent", None, true),
        (
            "transaction.access_mode.read_only",
            Some(AccessMode::ReadOnly),
            false,
        ),
        (
            "transaction.access_mode.read_write",
            Some(AccessMode::ReadWrite),
            true,
        ),
    ] {
        record(
            &mut recorder,
            probe,
            "access_mode",
            "una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita",
            write_inside_transaction(mode, admits).await,
        );
    }

    record(
        &mut recorder,
        "transaction.context",
        "context",
        "il session context e leggibile dalla variabile utente dopo START TRANSACTION",
        context_inside_transaction().await,
    );

    for (probe, id, durable) in [
        ("transaction.commit", 1_u8, true),
        ("transaction.rollback", 2, false),
    ] {
        record(
            &mut recorder,
            probe,
            "durability",
            "l'esito dichiarato corrisponde a cio che resta sul server",
            durability_round(id, durable).await,
        );
    }

    let document = json!({
        "schema_version": 1,
        "server": {
            "label": environment("PLENORA_EVIDENCE_LABEL", "sconosciuto"),
            "host": environment("PLENORA_MYSQL_HOST", "127.0.0.1"),
            "product_version": identity.0,
            "version_comment": identity.1,
            "digest": environment("PLENORA_EVIDENCE_DIGEST", ""),
        },
        "bootstrap_sql": crate::SESSION_BOOTSTRAP_SQL,
        "observations": recorder
            .0
            .iter()
            .map(Observation::to_json)
            .collect::<Vec<_>>(),
    });
    println!(
        "{MARKER}{}",
        serde_json::to_string(&document).expect("verdetto serializzabile")
    );
}
