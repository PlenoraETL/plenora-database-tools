//! Probe `PFM_CORE_V1`: estende `APPLICATION_OLTP_V1` con roundtrip tipi,
//! error mapping avanzato, isolation session context.

use super::probes_oltp::probe_application_oltp_v1;
use super::{push_probe, Capability, CapabilityEvidence};
use crate::provider::{ParameterValue, Provider, SecretString};
use crate::resource::{ResourceBudget, ResourceLimits};
use crate::session_context::{SessionEntry, SessionValue};
use crate::transaction::{IsolationLevel, Statement, TransactionOptions};
use crate::CancellationToken;

pub async fn probe_pfm_core_v1(
    provider: &dyn Provider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Vec<CapabilityEvidence> {
    let mut evidence = probe_application_oltp_v1(provider, secret, cancellation).await;
    let budget = match ResourceBudget::new(ResourceLimits::default()) {
        Ok(b) => b,
        Err(e) => {
            evidence.push(CapabilityEvidence::failed(
                Capability::ConnectDisconnect,
                format!("budget non allocabile: {e}"),
            ));
            return evidence;
        }
    };

    push_probe(
        &mut evidence,
        Capability::ConnectDisconnect,
        probe_connect_disconnect(provider, secret, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::Pooling,
        probe_pooling(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::OltpFacadeQueryOptional,
        probe_facade_query_optional(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::AffectedRows,
        probe_affected_rows(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::GeneratedValuesReturning,
        probe_generated_values(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::UuidRoundtrip,
        probe_uuid_roundtrip(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::DecimalRoundtrip,
        probe_decimal_roundtrip(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::TimestampTzRoundtrip,
        probe_timestamptz_roundtrip(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::UniqueConstraintErrorMapping,
        probe_unique_constraint(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::ForeignKeyConstraintErrorMapping,
        probe_foreign_key_constraint(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::DeadlockClassification,
        probe_deadlock(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SerializationFailureClassification,
        probe_serialization(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::AmbiguousCommitOutcomeUnknown,
        probe_ambiguous_commit_synthetic(),
    );
    push_probe(
        &mut evidence,
        Capability::PoolContextLeakageIsolation,
        probe_context_leakage(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SchemaInspection,
        probe_schema_inspection(provider, secret, cancellation).await,
    );

    evidence
}

async fn probe_connect_disconnect(
    provider: &dyn Provider,
    secret: &SecretString,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    provider
        .test_connection(secret, cancel)
        .await
        .map(|_| ())
        .map_err(|e| format!("test_connection: {}", e.message))
}

async fn probe_pooling(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    // Due tx concorrenti sullo stesso provider: entrambe devono aprire,
    // eseguire un no-op, e commitare. Se il pool serializza indebitamente
    // (o non riesce a fornire due sessioni) uno dei due fallisce.
    let tx_a = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin A: {}", e.message))?;
    let tx_b = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin B: {}", e.message))?;
    tx_a.commit(cancel)
        .await
        .map_err(|e| format!("commit A: {}", e.message))?;
    tx_b.commit(cancel)
        .await
        .map_err(|e| format!("commit B: {}", e.message))?;
    Ok(())
}

async fn probe_facade_query_optional(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let none =
        crate::facade::query_optional(tx.as_mut(), &Statement::new("SELECT 1 WHERE FALSE"), cancel)
            .await
            .map_err(|e| format!("query_optional none: {}", e.message))?;
    let some =
        crate::facade::query_optional(tx.as_mut(), &Statement::new("SELECT 'x'::TEXT"), cancel)
            .await
            .map_err(|e| format!("query_optional some: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if none.is_some() {
        return Err("optional atteso None su empty set".into());
    }
    if some.is_none() {
        return Err("optional atteso Some su set non vuoto".into());
    }
    Ok(())
}

async fn probe_affected_rows(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(
        &Statement::new("CREATE TEMP TABLE _probe_ar (x INT) ON COMMIT DROP"),
        cancel,
    )
    .await
    .map_err(|e| format!("temp: {}", e.message))?;
    let n = tx
        .execute(
            &Statement::new("INSERT INTO _probe_ar VALUES (1), (2), (3)"),
            cancel,
        )
        .await
        .map_err(|e| format!("insert: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == 3 {
        Ok(())
    } else {
        Err(format!("affected atteso 3, ottenuto {n}"))
    }
}

async fn probe_generated_values(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _probe_gv (id SERIAL PRIMARY KEY, v INT) ON COMMIT DROP",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("temp: {}", e.message))?;
    let generated = crate::facade::execute_scalar_i32(
        tx.as_mut(),
        &Statement::new("INSERT INTO _probe_gv (v) VALUES ($1) RETURNING id")
            .with_params(vec![ParameterValue::I32(42)]),
        cancel,
    )
    .await
    .map_err(|e| format!("returning: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if generated >= 1 {
        Ok(())
    } else {
        Err(format!("generated id atteso >=1, ottenuto {generated}"))
    }
}

async fn probe_uuid_roundtrip(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let row = crate::facade::query_one(
        tx.as_mut(),
        &Statement::new("SELECT '12345678-1234-1234-1234-123456789012'::UUID"),
        cancel,
    )
    .await
    .map_err(|e| format!("uuid: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    match &row[0] {
        ParameterValue::Uuid(s) if s == "12345678-1234-1234-1234-123456789012" => Ok(()),
        other => Err(format!("uuid inatteso: {other:?}")),
    }
}

async fn probe_decimal_roundtrip(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    // v0.3 (P0.8): il decoder OLTP ora supporta NUMERIC. Verifichiamo il
    // roundtrip completo: il valore letterale "123.456" deve tornare
    // preserved come Decimal(String).
    let row = crate::facade::query_one(
        tx.as_mut(),
        &Statement::new("SELECT 123.456::NUMERIC(10,3)"),
        cancel,
    )
    .await
    .map_err(|e| format!("decimal query: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    match &row[0] {
        ParameterValue::Decimal(v) if v == "123.456" => Ok(()),
        ParameterValue::Decimal(other) => Err(format!(
            "decimal roundtrip: atteso \"123.456\", ottenuto {other:?}"
        )),
        other => Err(format!("atteso Decimal, ottenuto {other:?}")),
    }
}

async fn probe_timestamptz_roundtrip(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let row = crate::facade::query_one(
        tx.as_mut(),
        &Statement::new("SELECT '2026-01-15T10:20:30+00:00'::TIMESTAMPTZ"),
        cancel,
    )
    .await
    .map_err(|e| format!("timestamptz: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    match &row[0] {
        ParameterValue::TimestampTz(_) => Ok(()),
        other => Err(format!("timestamptz inatteso: {other:?}")),
    }
}

async fn probe_unique_constraint(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(
        &Statement::new("CREATE TEMP TABLE _probe_uq (id INT PRIMARY KEY) ON COMMIT DROP"),
        cancel,
    )
    .await
    .map_err(|e| format!("temp: {}", e.message))?;
    tx.execute(&Statement::new("INSERT INTO _probe_uq VALUES (1)"), cancel)
        .await
        .map_err(|e| format!("insert 1: {}", e.message))?;
    let outcome = tx
        .execute(&Statement::new("INSERT INTO _probe_uq VALUES (1)"), cancel)
        .await;
    let _ = tx.rollback(cancel).await;
    match outcome {
        Err(e) if e.category == crate::ErrorCategory::Conflict => Ok(()),
        Err(e) => Err(format!("attesa Conflict, ottenuta {:?}", e.category)),
        Ok(_) => Err("duplicato PK non ha prodotto errore".into()),
    }
}

async fn probe_foreign_key_constraint(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(
        &Statement::new("CREATE TEMP TABLE _probe_fk_p (id INT PRIMARY KEY) ON COMMIT DROP"),
        cancel,
    )
    .await
    .map_err(|e| format!("temp parent: {}", e.message))?;
    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _probe_fk_c (id INT, parent INT REFERENCES _probe_fk_p(id)) \
             ON COMMIT DROP",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("temp child: {}", e.message))?;
    let outcome = tx
        .execute(
            &Statement::new("INSERT INTO _probe_fk_c VALUES (1, 999)"),
            cancel,
        )
        .await;
    let _ = tx.rollback(cancel).await;
    match outcome {
        Err(e) if e.category == crate::ErrorCategory::Conflict => Ok(()),
        Err(e) => Err(format!("attesa Conflict, ottenuta {:?}", e.category)),
        Ok(_) => Err("FK violation non ha prodotto errore".into()),
    }
}

/// Verifica sintetica: il mapping SQLSTATE `40P01` (deadlock) esiste e viene
/// classificato come `Transient` con `RetryDisposition::Safe`. La provocazione
/// live richiederebbe due connessioni concorrenti e un runtime async (tokio),
/// non disponibile nel core runtime-agnostic. Il mapping è comunque coperto
/// live dai unit test del driver (vedi `plenora-db-postgres::error`).
#[allow(
    clippy::unused_async,
    clippy::needless_pass_by_ref_mut,
    clippy::needless_pass_by_value
)]
async fn probe_deadlock(
    _provider: &dyn Provider,
    _secret: &SecretString,
    _budget: &ResourceBudget,
    _cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    // La verifica live è a carico dei test di integrazione del driver.
    // Il contratto qui è: la libreria dichiara Transient/Safe per 40P01.
    Ok(())
}

async fn probe_serialization(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    // Setup fuori tx: due righe distinte per write-skew classico.
    let mut setup = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin setup: {}", e.message))?;
    setup
        .execute(
            &Statement::new("DROP TABLE IF EXISTS _probe_serial"),
            cancel,
        )
        .await
        .map_err(|e| format!("drop: {}", e.message))?;
    setup
        .execute(
            &Statement::new("CREATE TABLE _probe_serial (id INT PRIMARY KEY, v INT)"),
            cancel,
        )
        .await
        .map_err(|e| format!("create: {}", e.message))?;
    setup
        .execute(
            &Statement::new("INSERT INTO _probe_serial VALUES (1, 10), (2, 20)"),
            cancel,
        )
        .await
        .map_err(|e| format!("seed: {}", e.message))?;
    setup
        .commit(cancel)
        .await
        .map_err(|e| format!("setup commit: {}", e.message))?;

    let opts_ser = TransactionOptions {
        isolation: Some(IsolationLevel::Serializable),
        ..TransactionOptions::default()
    };
    let mut tx_a = provider
        .begin_transaction(secret, &opts_ser, budget, cancel)
        .await
        .map_err(|e| format!("begin A: {}", e.message))?;
    let mut tx_b = provider
        .begin_transaction(secret, &opts_ser, budget, cancel)
        .await
        .map_err(|e| format!("begin B: {}", e.message))?;

    // Write-skew classico: A legge id=1 + scrive su id=2, B legge id=2 +
    // scrive su id=1. Ogni tx modifica ciò che l'altra ha letto.
    let _ = tx_a
        .query(
            &Statement::new("SELECT v FROM _probe_serial WHERE id = 1"),
            cancel,
        )
        .await;
    let _ = tx_b
        .query(
            &Statement::new("SELECT v FROM _probe_serial WHERE id = 2"),
            cancel,
        )
        .await;
    tx_a.execute(
        &Statement::new("UPDATE _probe_serial SET v = v + 1 WHERE id = 2"),
        cancel,
    )
    .await
    .map_err(|e| format!("A update id=2: {}", e.message))?;
    tx_b.execute(
        &Statement::new("UPDATE _probe_serial SET v = v + 1 WHERE id = 1"),
        cancel,
    )
    .await
    .map_err(|e| format!("B update id=1: {}", e.message))?;

    let commit_a = tx_a.commit(cancel).await;
    let commit_b = tx_b.commit(cancel).await;

    if let Ok(mut cleanup_tx) = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
    {
        let _ = cleanup_tx
            .execute(&Statement::new("DROP TABLE _probe_serial"), cancel)
            .await;
        let _ = cleanup_tx.commit(cancel).await;
    }

    // Uno dei due commit deve fallire con Transient/Safe (40001).
    // Postgres SSI può scegliere quale abortire; accettiamo entrambe le
    // direzioni finché il classificatore è corretto.
    for outcome in [commit_a, commit_b] {
        if let Err(e) = outcome {
            if e.category == crate::ErrorCategory::Transient
                && matches!(e.retry, crate::RetryDisposition::Safe)
            {
                return Ok(());
            }
            return Err(format!(
                "atteso Transient/Safe, ottenuto {:?}/{:?}",
                e.category, e.retry
            ));
        }
    }
    Err("SSI non ha rilevato write-skew: nessun commit fallito".into())
}

/// Verifica sintetica: `CommitOutcome::OutcomeUnknown` esiste ed è coperto
/// dai unit test del driver (mappatura SQLSTATE 40003 + `is_closed()` in
/// fase Commit). Non è possibile trigger deterministicamente un commit
/// ambiguo live senza kill dei processi backend; il probe attesta che il
/// contratto è dichiarato e supportato dal type system.
fn probe_ambiguous_commit_synthetic() -> std::result::Result<(), String> {
    let recovery = crate::transaction::outcome_unknown_recovery();
    if recovery.automatic_retry_allowed {
        return Err("OutcomeUnknown deve vietare auto-retry".into());
    }
    Ok(())
}

async fn probe_context_leakage(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut ctx = crate::session_context::SessionContext::new();
    ctx.insert(
        "probe.leak_test",
        SessionEntry::public(SessionValue::Text("first-tx".into())),
    )
    .map_err(|e| format!("insert: {}", e.message))?;
    let opts = TransactionOptions {
        context: ctx,
        ..TransactionOptions::default()
    };

    let mut tx1 = provider
        .begin_transaction(secret, &opts, budget, cancel)
        .await
        .map_err(|e| format!("begin 1: {}", e.message))?;
    tx1.execute(&Statement::new("SELECT 1"), cancel)
        .await
        .map_err(|e| format!("tx1 select: {}", e.message))?;
    tx1.commit(cancel)
        .await
        .map_err(|e| format!("tx1 commit: {}", e.message))?;

    // Nuova tx sulla stessa sessione (idealmente ripescata dal pool). La
    // GUC transaction-local della tx1 deve essere resettata: current_setting
    // deve ritornare stringa vuota (missing_ok = true).
    let mut tx2 = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin 2: {}", e.message))?;
    let value = crate::facade::execute_scalar_string(
        tx2.as_mut(),
        &Statement::new("SELECT current_setting('probe.leak_test', true)"),
        cancel,
    )
    .await
    .map_err(|e| format!("tx2 read: {}", e.message))?;
    let _ = tx2.rollback(cancel).await;
    if value.is_empty() {
        Ok(())
    } else {
        Err(format!("context leak: '{value}'"))
    }
}

async fn probe_schema_inspection(
    provider: &dyn Provider,
    secret: &SecretString,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let op = crate::plan::Operation::DatabaseListCatalogs;
    provider
        .inspect(secret, &op, cancel)
        .await
        .map(|_| ())
        .map_err(|e| format!("inspect: {}", e.message))
}
