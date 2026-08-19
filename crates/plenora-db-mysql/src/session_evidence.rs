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
//! Come per ADR 0014, nessuna sonda fa `panic` su un errore del server:
//! l'errore e la misura. Il test fallisce solo se l'harness non riesce a
//! misurare — server irraggiungibile, TLS assente, fixture non creabile.

// La sessione vive fino alla fine della sonda per costruzione: e cio che si
// sta misurando. Rilasciarla prima renderebbe la misura piu corta della
// domanda.
#![allow(clippy::significant_drop_tightening)]

use crate::evidence::{condense, config, environment, server_code, Observation, Recorder};
use crate::profile::MYSQL_PROFILE;
use mysql_async::prelude::Queryable;
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

/// Cio che il bootstrap pretende di fissare, piu l'isolamento di partenza.
const OBSERVED_VARIABLES: &str =
    "SELECT @@autocommit, @@time_zone, @@sql_mode, @@transaction_isolation";

/// Lo stato che una transazione aperta rende osservabile.
const TRANSACTION_STATE: &str =
    "SELECT @@transaction_isolation, @@transaction_read_only, @@autocommit";

fn pool() -> Result<crate::MysqlPool, DatabaseError> {
    crate::MysqlPool::new_with_profile(&config(), 2, &MYSQL_PROFILE)
}

/// Il bootstrap: prima eseguito a mano, poi ricevuto dal pool.
async fn bootstrap_probes(recorder: &mut Recorder) {
    let cancel = CancellationToken::new();

    // 1. L'esatto SQL del pool su una connessione qualunque. Dice del server.
    let mut connection = mysql_async::Conn::new(
        config()
            .driver_opts("MySQL")
            .expect("opzioni driver: harness, non divergenza"),
    )
    .await
    .expect("connessione: harness, non divergenza");
    match connection.query_drop(crate::SESSION_BOOTSTRAP_SQL).await {
        Ok(()) => {
            let observed = connection
                .query_first::<(String, String, String, String), _>(OBSERVED_VARIABLES)
                .await
                .expect("lettura variabili: harness, non divergenza")
                .expect("variabili presenti");
            recorder.accepted(
                "bootstrap.statement",
                "session",
                "bootstrap",
                "il server accetta l'esatto SESSION_BOOTSTRAP_SQL del pool",
                format!(
                    "autocommit={} time_zone={} sql_mode={} isolation={}",
                    observed.0, observed.1, observed.2, observed.3
                ),
            );
        }
        Err(error) => recorder.rejected(
            "bootstrap.statement",
            "session",
            "bootstrap",
            "il server accetta l'esatto SESSION_BOOTSTRAP_SQL del pool",
            condense(&format!("{error}")),
            server_code(&error),
        ),
    }

    // 2. Il percorso reale. Il pool passa il bootstrap come `setup` del
    //    driver, quindi cio che si legge qui e cio che il provider trova
    //    all'inizio di ogni operazione, prima della probe. La seconda
    //    iterazione riusa una connessione gia restituita al pool: con
    //    `reset_connection` attivo, il bootstrap deve essere riapplicato.
    let Ok(pool) = pool() else {
        for probe in ["bootstrap.pool", "bootstrap.pool_reuse"] {
            recorder.rejected(
                probe,
                "session",
                "bootstrap",
                "il pool consegna sessioni gia bootstrappate",
                "pool non costruibile".to_owned(),
                None,
            );
        }
        return;
    };
    for probe in ["bootstrap.pool", "bootstrap.pool_reuse"] {
        let observed = read_through_pool(&pool, &cancel).await;
        record(
            recorder,
            probe,
            "bootstrap",
            "il pool consegna sessioni gia bootstrappate",
            observed,
        );
    }
}

async fn read_through_pool(
    pool: &crate::MysqlPool,
    cancel: &CancellationToken,
) -> Result<Vec<String>, DatabaseError> {
    let mut session = pool.checkout(cancel).await?;
    let rows = session
        .query_rows(OBSERVED_VARIABLES, ErrorPhase::Probe, cancel)
        .await?;
    let row = rows.first().expect("riga delle variabili");
    Ok(vec![format!(
        "autocommit={} time_zone={} sql_mode={} isolation={}",
        row.get::<String, _>(0).unwrap_or_default(),
        row.get::<String, _>(1).unwrap_or_default(),
        row.get::<String, _>(2).unwrap_or_default(),
        row.get::<String, _>(3).unwrap_or_default(),
    )])
}

/// Apre una transazione con le opzioni date e ne legge lo stato osservabile.
async fn transaction_state(options: &TransactionOptions) -> Result<Vec<String>, DatabaseError> {
    let cancel = CancellationToken::new();
    let pool = pool()?;
    let session = pool.checkout(&cancel).await?;
    let mut transaction =
        crate::transaction::MysqlTransaction::begin(session, options, &cancel).await?;
    let rows = transaction
        .query(&Statement::new(TRANSACTION_STATE), &cancel)
        .await?;
    let state = rows.first().map_or_else(Vec::new, |row| {
        row.values()
            .iter()
            .map(|value| condense(&format!("{value:?}")))
            .collect::<Vec<_>>()
    });
    Box::new(transaction).commit(&cancel).await?;
    Ok(state)
}

/// I quattro livelli, le tre modalita di accesso e il contesto di sessione.
async fn transaction_probes(recorder: &mut Recorder) {
    for (probe, level) in [
        (
            "transaction.isolation.read_uncommitted",
            IsolationLevel::ReadUncommitted,
        ),
        (
            "transaction.isolation.read_committed",
            IsolationLevel::ReadCommitted,
        ),
        (
            "transaction.isolation.repeatable_read",
            IsolationLevel::RepeatableRead,
        ),
        (
            "transaction.isolation.serializable",
            IsolationLevel::Serializable,
        ),
    ] {
        let options = TransactionOptions {
            isolation: Some(level),
            ..TransactionOptions::default()
        };
        record(
            recorder,
            probe,
            "isolation",
            "il livello richiesto e quello che la sessione dichiara",
            transaction_state(&options).await,
        );
    }

    // L'access mode si osserva sull'**effetto**, non su `@@transaction_read_only`:
    // quella variabile riflette `SET TRANSACTION`, non `START TRANSACTION READ
    // ONLY`, e leggerla dava lo stesso valore per tutte e tre le modalita.
    // Una sonda che non distingue i casi che dichiara di distinguere non e
    // una misura.
    for (probe, mode) in [
        ("transaction.access_mode.absent", None),
        (
            "transaction.access_mode.read_only",
            Some(AccessMode::ReadOnly),
        ),
        (
            "transaction.access_mode.read_write",
            Some(AccessMode::ReadWrite),
        ),
    ] {
        let options = TransactionOptions {
            access_mode: mode,
            ..TransactionOptions::default()
        };
        record(
            recorder,
            probe,
            "access_mode",
            "una scrittura dentro la transazione e ammessa o rifiutata secondo la modalita",
            write_inside_transaction(&options).await,
        );
    }

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
    // Il contesto si rilegge dalla variabile utente che il provider imposta:
    // leggere di nuovo isolamento e autocommit non diceva nulla del contesto,
    // e infatti la sonda era indistinguibile da quella senza contesto.
    record(
        recorder,
        "transaction.context",
        "context",
        "il session context e leggibile dalla variabile utente dopo START TRANSACTION",
        context_inside_transaction(&options).await,
    );

    durability_probes(recorder).await;
}

/// Una scrittura dentro la transazione: ammessa o rifiutata dalla modalita.
async fn write_inside_transaction(
    options: &TransactionOptions,
) -> Result<Vec<String>, DatabaseError> {
    let cancel = CancellationToken::new();
    let pool = pool()?;
    let mut setup = pool.checkout(&cancel).await?;
    for sql in [
        format!("DROP TABLE IF EXISTS {SCRATCH}_rw"),
        format!("CREATE TABLE {SCRATCH}_rw (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
    ] {
        setup.query_rows(&sql, ErrorPhase::Prepare, &cancel).await?;
    }
    drop(setup);

    let session = pool.checkout(&cancel).await?;
    let mut transaction =
        crate::transaction::MysqlTransaction::begin(session, options, &cancel).await?;
    let attempt = transaction
        .execute(
            &Statement::new(format!("INSERT INTO {SCRATCH}_rw (id) VALUES (1)")),
            &cancel,
        )
        .await;
    let observed = match &attempt {
        Ok(_) => "scrittura ammessa".to_owned(),
        Err(error) => format!("scrittura rifiutata: {:?}", error.category),
    };
    let _ = Box::new(transaction).rollback(&cancel).await;

    let mut cleanup = pool.checkout(&cancel).await?;
    let _ = cleanup
        .query_rows(
            &format!("DROP TABLE IF EXISTS {SCRATCH}_rw"),
            ErrorPhase::Prepare,
            &cancel,
        )
        .await;
    Ok(vec![observed])
}

/// La variabile utente che il provider imposta per il session context.
async fn context_inside_transaction(
    options: &TransactionOptions,
) -> Result<Vec<String>, DatabaseError> {
    let cancel = CancellationToken::new();
    let pool = pool()?;
    let session = pool.checkout(&cancel).await?;
    let mut transaction =
        crate::transaction::MysqlTransaction::begin(session, options, &cancel).await?;
    let rows = transaction
        .query(
            &Statement::new("SELECT @`plenora_ctx_plenora.tenant`"),
            &cancel,
        )
        .await?;
    let observed = rows.first().map_or_else(
        || "nessuna riga".to_owned(),
        |row| {
            row.values()
                .iter()
                .map(|value| condense(&format!("{value:?}")))
                .collect::<Vec<_>>()
                .join(" | ")
        },
    );
    Box::new(transaction).commit(&cancel).await?;
    Ok(vec![observed])
}

/// Commit e rollback misurati sull'effetto, non sull'assenza di errore.
async fn durability_probes(recorder: &mut Recorder) {
    let cancel = CancellationToken::new();
    let Ok(pool) = pool() else {
        for probe in ["transaction.commit", "transaction.rollback"] {
            recorder.rejected(
                probe,
                "session",
                "durability",
                "l'esito dichiarato corrisponde a cio che resta sul server",
                "pool non costruibile".to_owned(),
                None,
            );
        }
        return;
    };
    let mut setup = pool
        .checkout(&cancel)
        .await
        .expect("sessione della fixture: harness, non divergenza");
    for sql in [
        format!("DROP TABLE IF EXISTS {SCRATCH}"),
        format!("CREATE TABLE {SCRATCH} (id BIGINT PRIMARY KEY) ENGINE=InnoDB"),
    ] {
        setup
            .query_rows(&sql, ErrorPhase::Prepare, &cancel)
            .await
            .expect("fixture della misura: harness, non divergenza");
    }
    drop(setup);

    for (probe, id, durable) in [
        ("transaction.commit", 1_u8, true),
        ("transaction.rollback", 2, false),
    ] {
        let outcome = durability_round(&pool, id, durable).await;
        record(
            recorder,
            probe,
            "durability",
            "l'esito dichiarato corrisponde a cio che resta sul server",
            outcome,
        );
    }

    if let Ok(mut session) = pool.checkout(&cancel).await {
        let _ = session
            .query_rows(
                &format!("DROP TABLE IF EXISTS {SCRATCH}"),
                ErrorPhase::Prepare,
                &cancel,
            )
            .await;
    }
}

async fn durability_round(
    pool: &crate::MysqlPool,
    id: u8,
    durable: bool,
) -> Result<Vec<String>, DatabaseError> {
    let cancel = CancellationToken::new();
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
    let mut session = pool.checkout(&cancel).await?;
    let rows = session
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
    let expected = i64::from(durable);
    Ok(vec![format!(
        "righe={observed} attese={expected} coerente={}",
        observed == expected
    )])
}

/// Registra l'esito di una sonda che restituisce uno stato o un errore.
fn record(
    recorder: &mut Recorder,
    probe: &'static str,
    surface: &'static str,
    question: &'static str,
    outcome: Result<Vec<String>, DatabaseError>,
) {
    match outcome {
        Ok(state) => recorder.accepted(probe, "session", surface, question, state.join(" | ")),
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

/// La misura della semantica di sessione sui tre riferimenti.
///
/// `#[ignore]` come per ADR 0014: pretende un server live esplicito, e il nome
/// non porta il prefisso `live_` che i runner del gate filtrano.
///
/// # Panics
///
/// Se l'harness non riesce a misurare. Sono guasti suoi, non divergenze, e
/// vanno chiusi prima di leggere i numeri.
#[tokio::test]
#[ignore = "misura della semantica di sessione: richiede un riferimento live esplicito"]
async fn session_semantics_evidence() {
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
    bootstrap_probes(&mut recorder).await;
    transaction_probes(&mut recorder).await;

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
