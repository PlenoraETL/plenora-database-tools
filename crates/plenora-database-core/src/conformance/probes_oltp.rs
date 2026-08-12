//! Probe live per il profilo `APPLICATION_OLTP_V1` e i sub-probe che
//! verificano ciascuna capability del contratto minimo application plane.

use super::{Capability, CapabilityEvidence, APPLICATION_OLTP_V1};
use crate::provider::{ParameterValue, Provider, SecretString};
use crate::resource::{ResourceBudget, ResourceLimits};
use crate::transaction::{IsolationLevel, Statement, TransactionOptions};
use crate::CancellationToken;
use crate::session_context::{SessionEntry, SessionValue};

pub async fn probe_application_oltp_v1(
    provider: &dyn Provider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Vec<CapabilityEvidence> {
    let mut evidence = Vec::with_capacity(APPLICATION_OLTP_V1.required.len());
    let budget = match ResourceBudget::new(ResourceLimits::default()) {
        Ok(b) => b,
        Err(e) => {
            for cap in APPLICATION_OLTP_V1.required {
                evidence.push(CapabilityEvidence::failed(
                    *cap,
                    format!("budget iniziale non allocabile: {e}"),
                ));
            }
            return evidence;
        }
    };

    // --- Transactions: begin + commit
    match probe_transaction_commit(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::Transactions)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::Transactions, e)),
    }

    // --- BoundParameters
    match probe_bound_parameters(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::BoundParameters)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::BoundParameters, e)),
    }

    // --- OltpFacadeScalar
    match probe_facade_scalar(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::OltpFacadeScalar)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::OltpFacadeScalar, e)),
    }

    // --- OltpFacadeQueryOne
    match probe_facade_query_one(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::OltpFacadeQueryOne)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::OltpFacadeQueryOne, e)),
    }

    // --- Savepoints
    match probe_savepoints(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::Savepoints)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::Savepoints, e)),
    }

    // --- OptimisticConcurrency (assertion: no-op UPDATE non deve applicare)
    match probe_optimistic(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::OptimisticConcurrency)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::OptimisticConcurrency, e)),
    }

    // --- SessionContext
    match probe_session_context(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::SessionContext)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::SessionContext, e)),
    }

    // --- Cancellation (usa un token già cancellato)
    match probe_cancellation(provider, secret, &budget).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::Cancellation)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::Cancellation, e)),
    }

    // --- StatementTimeout (deve produrre errore Cancelled entro il timeout)
    match probe_statement_timeout(provider, secret, &budget, cancellation).await {
        Ok(()) => evidence.push(CapabilityEvidence::verified(Capability::StatementTimeout)),
        Err(e) => evidence.push(CapabilityEvidence::failed(Capability::StatementTimeout, e)),
    }

    // --- Isolation levels
    for (cap, level) in [
        (Capability::IsolationReadCommitted, IsolationLevel::ReadCommitted),
        (Capability::IsolationRepeatableRead, IsolationLevel::RepeatableRead),
        (Capability::IsolationSerializable, IsolationLevel::Serializable),
    ] {
        match probe_isolation(provider, secret, &budget, cancellation, level).await {
            Ok(()) => evidence.push(CapabilityEvidence::verified(cap)),
            Err(e) => evidence.push(CapabilityEvidence::failed(cap, e)),
        }
    }

    evidence
}

async fn probe_transaction_commit(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin fallito: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map(|_| ())
        .map_err(|e| format!("commit fallito: {}", e.message))
}

async fn probe_bound_parameters(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let rows = tx
        .query(
            &Statement::new("SELECT $1::INT + $2::INT")
                .with_params(vec![ParameterValue::I32(2), ParameterValue::I32(3)]),
            cancel,
        )
        .await
        .map_err(|e| format!("bound query: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if rows.len() != 1 || rows[0].len() != 1 {
        return Err(format!(
            "shape inatteso: {}x{}",
            rows.len(),
            rows.first().map_or(0, crate::row::Row::len)
        ));
    }
    match rows[0].get_index(0) {
        Some(ParameterValue::I32(5)) => Ok(()),
        other => Err(format!("valore inatteso: {other:?}")),
    }
}

async fn probe_facade_scalar(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let v = crate::facade::execute_scalar_i64(
        tx.as_mut(),
        &Statement::new("SELECT 7::BIGINT"),
        cancel,
    )
    .await
    .map_err(|e| format!("scalar: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if v == 7 {
        Ok(())
    } else {
        Err(format!("scalar atteso 7, ottenuto {v}"))
    }
}

async fn probe_facade_query_one(
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
        &Statement::new("SELECT 'ok'::TEXT"),
        cancel,
    )
    .await
    .map_err(|e| format!("query_one: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if row.len() == 1 {
        Ok(())
    } else {
        Err(format!("shape inatteso: {}", row.len()))
    }
}

async fn probe_savepoints(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.savepoint("probe_sp", cancel)
        .await
        .map_err(|e| format!("savepoint: {}", e.message))?;
    tx.release_savepoint("probe_sp", cancel)
        .await
        .map_err(|e| format!("release: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map(|_| ())
        .map_err(|e| format!("commit: {}", e.message))
}

async fn probe_optimistic(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    use crate::transaction::ConditionalUpdate;

    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(
        &Statement::new(
            "CREATE TEMP TABLE _probe_oc (id INT PRIMARY KEY, v INT) ON COMMIT DROP",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("temp: {}", e.message))?;
    tx.execute(&Statement::new("INSERT INTO _probe_oc VALUES (1, 100)"), cancel)
        .await
        .map_err(|e| format!("insert: {}", e.message))?;

    // Update con expected_version errata: attendo ConcurrentModification.
    let update = Statement::new(
        "UPDATE _probe_oc SET v = v + 1 WHERE id = $1 AND v = $2",
    )
    .with_params(vec![ParameterValue::I32(1), ParameterValue::I32(999)]);
    let probe = Statement::new("SELECT 1 FROM _probe_oc WHERE id = $1")
        .with_params(vec![ParameterValue::I32(1)]);
    let request = ConditionalUpdate {
        update: &update,
        key_probe: Some(&probe),
        expected_affected_rows: 1,
    };
    let outcome = tx.execute_conditional_update(request, cancel).await;
    let _ = tx.rollback(cancel).await;
    match outcome {
        Err(err) if err.category == crate::ErrorCategory::ConcurrentModification => Ok(()),
        Err(other) => Err(format!("atteso ConcurrentModification, ottenuto {:?}", other.category)),
        Ok(()) => Err("update no-op non ha prodotto errore".to_owned()),
    }
}

async fn probe_session_context(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut ctx = crate::session_context::SessionContext::new();
    ctx.insert(
        "probe.value",
        SessionEntry::public(SessionValue::Text("marker".into())),
    )
    .map_err(|e| format!("insert: {}", e.message))?;
    let opts = TransactionOptions {
        context: ctx,
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(secret, &opts, budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let v = crate::facade::execute_scalar_string(
        tx.as_mut(),
        &Statement::new("SELECT current_setting('probe.value', true)"),
        cancel,
    )
    .await
    .map_err(|e| format!("current_setting: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if v == "marker" {
        Ok(())
    } else {
        Err(format!("context non applicato: '{v}'"))
    }
}

async fn probe_cancellation(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
) -> std::result::Result<(), String> {
    let cancel = CancellationToken::new();
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, &cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    cancel.cancel();
    let outcome = tx.execute(&Statement::new("SELECT 1"), &cancel).await;
    let _ = tx.rollback(&cancel).await;
    match outcome {
        Err(err) if err.category == crate::ErrorCategory::Cancelled => Ok(()),
        Err(other) => Err(format!("atteso Cancelled, ottenuto {:?}", other.category)),
        Ok(_) => Err("cancel prima di execute non ha bloccato".to_owned()),
    }
}

async fn probe_statement_timeout(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let opts = TransactionOptions {
        statement_timeout_ms: Some(50),
        ..TransactionOptions::default()
    };
    let mut tx = provider
        .begin_transaction(secret, &opts, budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let outcome = tx
        .execute(&Statement::new("SELECT pg_sleep(2)"), cancel)
        .await;
    let _ = tx.rollback(cancel).await;
    match outcome {
        Err(err) if err.category == crate::ErrorCategory::Cancelled => Ok(()),
        Err(other) => Err(format!("atteso Cancelled, ottenuto {:?}", other.category)),
        Ok(_) => Err("statement_timeout non ha interrotto".to_owned()),
    }
}

async fn probe_isolation(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
    level: IsolationLevel,
) -> std::result::Result<(), String> {
    let opts = TransactionOptions {
        isolation: Some(level),
        ..TransactionOptions::default()
    };
    let tx = provider
        .begin_transaction(secret, &opts, budget, cancel)
        .await
        .map_err(|e| format!("begin {level:?}: {}", e.message))?;
    tx.rollback(cancel)
        .await
        .map_err(|e| format!("rollback {level:?}: {}", e.message))
}
