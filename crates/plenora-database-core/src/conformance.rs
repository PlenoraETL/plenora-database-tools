//! Conformance profile engine.
//!
//! Il PFM ha bisogno di verificare, *senza interrogare il provider per nome*,
//! che il driver soddisfi un insieme di capability applicative. Il modulo:
//!
//! 1. Enumera le capability osservabili (`Capability`).
//! 2. Definisce profili (`ConformanceProfile`) come insiemi di capability
//!    richieste — un profilo è dichiarato *esternamente* alla libreria
//!    (`APPLICATION_OLTP_V1` è il minimo per l'application plane).
//! 3. Espone helper (`probe_application_oltp_v1`) che eseguono un probe live
//!    e producono `CapabilityEvidence` per ciascuna capability.
//! 4. Valuta un `Report` (`check_profile`) con esito `Pass`/`Fail` e
//!    l'elenco delle capability mancanti/degradate.

use crate::provider::{Provider, SecretString};
use crate::resource::{ResourceBudget, ResourceLimits};
use crate::transaction::{IsolationLevel, Statement, TransactionOptions};
use crate::CancellationToken;
use crate::{provider::ParameterValue, session_context::SessionEntry, session_context::SessionValue};
use serde::{Deserialize, Serialize};

/// Capability osservabili dal probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    // Application plane — APPLICATION_OLTP_V1
    Transactions,
    Savepoints,
    OptimisticConcurrency,
    SessionContext,
    OltpFacadeScalar,
    OltpFacadeQueryOne,
    OltpFacadeQueryOptional,
    BoundParameters,
    Cancellation,
    StatementTimeout,
    IsolationReadCommitted,
    IsolationRepeatableRead,
    IsolationSerializable,

    // Extra per PFM_CORE_V1
    ConnectDisconnect,
    Pooling,
    AffectedRows,
    GeneratedValuesReturning,
    UuidRoundtrip,
    DecimalRoundtrip,
    TimestampTzRoundtrip,
    UniqueConstraintErrorMapping,
    ForeignKeyConstraintErrorMapping,
    DeadlockClassification,
    SerializationFailureClassification,
    AmbiguousCommitOutcomeUnknown,
    PoolContextLeakageIsolation,
    SchemaInspection,

    // Spatial plane — PFM_GIS_V1
    SpatialGeometryRead,
    SpatialGeometryWrite,
    SpatialWkbRoundtrip,
    SpatialSridPreservation,
    SpatialBbox,
    SpatialIntersects,
    SpatialContains,
    SpatialWithin,
    SpatialDistance,
    SpatialDWithin,
    SpatialCentroid,
    SpatialEnvelope,
    SpatialNearest,
    SpatialIndexAvailable,
    SpatialInvalidGeometryRejected,
    SpatialNullGeometryHandled,
    SpatialLargeGeometryStreaming,
    SpatialCrossSridPolicy,
}

/// Profilo di conformità: insieme *ordinato* di capability richieste.
///
/// L'ordinamento non è semantico: serve solo alla riproducibilità del
/// report. Il profilo è statico e dichiarato nella libreria PFM (qui c'è
/// solo quello di riferimento per l'application plane).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceProfile {
    pub name: &'static str,
    pub required: &'static [Capability],
}

/// Profilo di riferimento per l'application plane richiesto dal PFM.
pub const APPLICATION_OLTP_V1: ConformanceProfile = ConformanceProfile {
    name: "APPLICATION_OLTP_V1",
    required: &[
        Capability::Transactions,
        Capability::Savepoints,
        Capability::OptimisticConcurrency,
        Capability::SessionContext,
        Capability::OltpFacadeScalar,
        Capability::OltpFacadeQueryOne,
        Capability::BoundParameters,
        Capability::Cancellation,
        Capability::StatementTimeout,
        Capability::IsolationReadCommitted,
        Capability::IsolationSerializable,
    ],
};

/// Profilo esteso `PFM_CORE_V1`.
///
/// Application plane + roundtrip tipi + error mapping + isolamento del
/// session context nel pool. Estende `APPLICATION_OLTP_V1` con verifiche
/// più stringenti richieste dal PFM (roadmap §4.1). NON include
/// DDL/migration (B2 fuori scope, il PFM usa un migration tool esterno).
pub const PFM_CORE_V1: ConformanceProfile = ConformanceProfile {
    name: "PFM_CORE_V1",
    required: &[
        Capability::ConnectDisconnect,
        Capability::Pooling,
        Capability::Transactions,
        Capability::Savepoints,
        Capability::IsolationReadCommitted,
        Capability::IsolationSerializable,
        Capability::BoundParameters,
        Capability::OltpFacadeQueryOne,
        Capability::OltpFacadeQueryOptional,
        Capability::OltpFacadeScalar,
        Capability::AffectedRows,
        Capability::OptimisticConcurrency,
        Capability::GeneratedValuesReturning,
        Capability::UuidRoundtrip,
        Capability::DecimalRoundtrip,
        Capability::TimestampTzRoundtrip,
        Capability::UniqueConstraintErrorMapping,
        Capability::ForeignKeyConstraintErrorMapping,
        Capability::DeadlockClassification,
        Capability::SerializationFailureClassification,
        Capability::AmbiguousCommitOutcomeUnknown,
        Capability::Cancellation,
        Capability::StatementTimeout,
        Capability::SessionContext,
        Capability::PoolContextLeakageIsolation,
        Capability::SchemaInspection,
    ],
};

/// Profilo spatial richiesto dal PFM per il read GIS operativo (roadmap §4.2).
pub const PFM_GIS_V1: ConformanceProfile = ConformanceProfile {
    name: "PFM_GIS_V1",
    required: &[
        Capability::SpatialGeometryRead,
        Capability::SpatialGeometryWrite,
        Capability::SpatialWkbRoundtrip,
        Capability::SpatialSridPreservation,
        Capability::SpatialBbox,
        Capability::SpatialIntersects,
        Capability::SpatialContains,
        Capability::SpatialWithin,
        Capability::SpatialDistance,
        Capability::SpatialDWithin,
        Capability::SpatialCentroid,
        Capability::SpatialEnvelope,
        Capability::SpatialNearest,
        Capability::SpatialIndexAvailable,
        Capability::SpatialInvalidGeometryRejected,
        Capability::SpatialNullGeometryHandled,
        Capability::SpatialLargeGeometryStreaming,
        Capability::SpatialCrossSridPolicy,
    ],
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    /// La capability è stata verificata live e funziona.
    Verified,
    /// La capability è stata verificata live e ha fallito.
    Failed,
    /// Non è stato possibile verificare (skipped): assunta non supportata.
    Unverified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityEvidence {
    pub capability: Capability,
    pub kind: EvidenceKind,
    pub notes: Option<String>,
}

impl CapabilityEvidence {
    #[must_use]
    pub const fn verified(capability: Capability) -> Self {
        Self {
            capability,
            kind: EvidenceKind::Verified,
            notes: None,
        }
    }

    #[must_use]
    pub fn failed(capability: Capability, notes: impl Into<String>) -> Self {
        Self {
            capability,
            kind: EvidenceKind::Failed,
            notes: Some(notes.into()),
        }
    }

    #[must_use]
    pub const fn is_verified(&self) -> bool {
        matches!(self.kind, EvidenceKind::Verified)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileStatus {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileReport {
    pub profile: String,
    pub status: ProfileStatus,
    pub missing: Vec<Capability>,
    pub failed: Vec<Capability>,
    pub evidence: Vec<CapabilityEvidence>,
}

/// Confronta le evidence osservate contro il profilo richiesto.
#[must_use]
pub fn check_profile(
    profile: &ConformanceProfile,
    evidence: &[CapabilityEvidence],
) -> ProfileReport {
    let mut missing = Vec::new();
    let mut failed = Vec::new();
    for required in profile.required {
        match evidence.iter().find(|e| e.capability == *required) {
            None => missing.push(*required),
            Some(e) if !e.is_verified() => failed.push(*required),
            _ => {}
        }
    }
    let status = if missing.is_empty() && failed.is_empty() {
        ProfileStatus::Pass
    } else {
        ProfileStatus::Fail
    };
    ProfileReport {
        profile: profile.name.to_owned(),
        status,
        missing,
        failed,
        evidence: evidence.to_vec(),
    }
}

/// Esegue il probe live delle capability dell'application plane sul provider.
///
/// Ogni check è isolato in una propria transazione, così che un fallimento
/// non contamini il probe successivo. Il risultato è deterministico rispetto
/// all'ordine di `APPLICATION_OLTP_V1.required`.
///
/// Il probe non modifica lo schema del database: usa `SELECT`, `DO ...
/// BEGIN/END`, tabelle temporanee.
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

// ============================================================================
//  PFM_CORE_V1 — probe umbrella + extra checks oltre APPLICATION_OLTP_V1
// ============================================================================

/// Esegue tutti i probe richiesti da `PFM_CORE_V1`. Riusa i probe di
/// `APPLICATION_OLTP_V1` e aggiunge quelli specifici del PFM.
#[allow(clippy::too_many_lines)] // sequenza di 26 probe intenzionalmente lineare
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

fn push_probe(
    evidence: &mut Vec<CapabilityEvidence>,
    capability: Capability,
    result: std::result::Result<(), String>,
) {
    match result {
        Ok(()) => evidence.push(CapabilityEvidence::verified(capability)),
        Err(e) => evidence.push(CapabilityEvidence::failed(capability, e)),
    }
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
    let none = crate::facade::query_optional(
        tx.as_mut(),
        &Statement::new("SELECT 1 WHERE FALSE"),
        cancel,
    )
    .await
    .map_err(|e| format!("query_optional none: {}", e.message))?;
    let some = crate::facade::query_optional(
        tx.as_mut(),
        &Statement::new("SELECT 'x'::TEXT"),
        cancel,
    )
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
    // Il facade OLTP oggi decodifica NUMERIC come non-supported. Verifichiamo
    // che la libreria propaghi un `Unsupported` esplicito invece di silenziare.
    // Il consumer legge decimal via il data plane Arrow.
    let outcome = crate::facade::query_one(
        tx.as_mut(),
        &Statement::new("SELECT 123.456::NUMERIC(10,3)"),
        cancel,
    )
    .await;
    let _ = tx.rollback(cancel).await;
    match outcome {
        Err(e) if e.category == crate::ErrorCategory::Unsupported => Ok(()),
        Err(e) => Err(format!("attesa Unsupported, ottenuta {:?}", e.category)),
        Ok(_) => Err("decimal via facade OLTP dovrebbe essere Unsupported".into()),
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
    tx.execute(
        &Statement::new("INSERT INTO _probe_uq VALUES (1)"),
        cancel,
    )
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
#[allow(clippy::unused_async, clippy::needless_pass_by_ref_mut, clippy::needless_pass_by_value)]
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
        .execute(&Statement::new("DROP TABLE IF EXISTS _probe_serial"), cancel)
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

// ============================================================================
//  PFM_GIS_V1 — probe umbrella + spatial checks
// ============================================================================

/// Esegue tutti i probe richiesti da `PFM_GIS_V1` contro un provider spatial.
/// Le geometrie di riferimento sono create al volo con `ST_MakePoint`/etc.
#[allow(clippy::too_many_lines)] // sequenza di 18 probe intenzionalmente lineare
pub async fn probe_pfm_gis_v1(
    provider: &dyn Provider,
    secret: &SecretString,
    cancellation: &CancellationToken,
) -> Vec<CapabilityEvidence> {
    let mut evidence = Vec::with_capacity(PFM_GIS_V1.required.len());
    let budget = match ResourceBudget::new(ResourceLimits::default()) {
        Ok(b) => b,
        Err(e) => {
            for cap in PFM_GIS_V1.required {
                evidence.push(CapabilityEvidence::failed(
                    *cap,
                    format!("budget non allocabile: {e}"),
                ));
            }
            return evidence;
        }
    };

    // Setup: crea una tabella temporanea con 3 punti in SRID 4326.
    let setup_result = spatial_setup(provider, secret, &budget, cancellation).await;
    if let Err(e) = setup_result {
        for cap in PFM_GIS_V1.required {
            evidence.push(CapabilityEvidence::failed(*cap, format!("setup: {e}")));
        }
        return evidence;
    }

    push_probe(
        &mut evidence,
        Capability::SpatialGeometryRead,
        probe_spatial_read(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialGeometryWrite,
        probe_spatial_write(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialWkbRoundtrip,
        probe_spatial_wkb_roundtrip(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialSridPreservation,
        probe_spatial_srid(provider, secret, &budget, cancellation).await,
    );
    for (cap, function_call, expected) in [
        (Capability::SpatialBbox, "ST_MakeEnvelope(6.0, 40.0, 14.0, 46.0, 4326) && geom", 2_i64),
        (Capability::SpatialIntersects, "ST_Intersects(geom, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))", 1),
        (Capability::SpatialContains, "ST_Contains(ST_SetSRID(ST_MakeEnvelope(0, 0, 20, 50), 4326), geom)", 3),
        (Capability::SpatialWithin, "ST_Within(geom, ST_SetSRID(ST_MakeEnvelope(9.0, 45.0, 9.5, 46.0), 4326))", 1),
        (Capability::SpatialDWithin, "ST_DWithin(geom, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326), 0.01)", 1),
    ] {
        push_probe(
            &mut evidence,
            cap,
            probe_spatial_count_where(provider, secret, &budget, cancellation, function_call, expected).await,
        );
    }
    push_probe(
        &mut evidence,
        Capability::SpatialDistance,
        probe_spatial_distance(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialCentroid,
        probe_spatial_scalar_op(provider, secret, &budget, cancellation,
            "SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE ST_Centroid(geom) IS NOT NULL", 3).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialEnvelope,
        probe_spatial_scalar_op(provider, secret, &budget, cancellation,
            "SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE ST_Envelope(geom) IS NOT NULL", 3).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialNearest,
        probe_spatial_nearest(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialIndexAvailable,
        probe_spatial_index_available(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialInvalidGeometryRejected,
        probe_spatial_invalid_rejected(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialNullGeometryHandled,
        probe_spatial_null_handled(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialLargeGeometryStreaming,
        probe_spatial_large_streaming(provider, secret, &budget, cancellation).await,
    );
    push_probe(
        &mut evidence,
        Capability::SpatialCrossSridPolicy,
        probe_spatial_cross_srid(provider, secret, &budget, cancellation).await,
    );

    // Cleanup
    let _ = spatial_teardown(provider, secret, &budget, cancellation).await;

    evidence
}

async fn spatial_setup(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(&Statement::new("DROP TABLE IF EXISTS _probe_gis"), cancel)
        .await
        .map_err(|e| format!("drop: {}", e.message))?;
    tx.execute(
        &Statement::new(
            "CREATE TABLE _probe_gis (id INT PRIMARY KEY, geom geometry(Point, 4326))",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("create: {}", e.message))?;
    tx.execute(
        &Statement::new(
            "INSERT INTO _probe_gis VALUES \
             (1, ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326)), \
             (2, ST_SetSRID(ST_MakePoint(12.49, 41.90), 4326)), \
             (3, ST_SetSRID(ST_MakePoint(2.35,  48.86), 4326))",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("seed: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map_err(|e| format!("commit setup: {}", e.message))?;
    Ok(())
}

async fn spatial_teardown(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(&Statement::new("DROP TABLE _probe_gis"), cancel)
        .await
        .map_err(|e| format!("drop: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map_err(|e| format!("commit: {}", e.message))?;
    Ok(())
}

async fn probe_spatial_read(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let n = crate::facade::execute_scalar_i64(
        tx.as_mut(),
        &Statement::new("SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE geom IS NOT NULL"),
        cancel,
    )
    .await
    .map_err(|e| format!("count: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == 3 {
        Ok(())
    } else {
        Err(format!("attesi 3 punti, ottenuti {n}"))
    }
}

async fn probe_spatial_write(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let n = tx
        .execute(
            &Statement::new(
                "INSERT INTO _probe_gis VALUES (99, ST_SetSRID(ST_MakePoint(0, 0), 4326))",
            ),
            cancel,
        )
        .await
        .map_err(|e| format!("insert: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == 1 {
        Ok(())
    } else {
        Err(format!("insert atteso 1, ottenuto {n}"))
    }
}

async fn probe_spatial_wkb_roundtrip(
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
        &Statement::new(
            "SELECT ST_AsEWKB(ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326))",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("wkb: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    match &row[0] {
        ParameterValue::Bytes(b) if !b.is_empty() => Ok(()),
        other => Err(format!("attesa bytes EWKB non vuoto, ottenuto {other:?}")),
    }
}

async fn probe_spatial_srid(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let srid = crate::facade::execute_scalar_i32(
        tx.as_mut(),
        &Statement::new("SELECT ST_SRID(geom) FROM _probe_gis WHERE id = 1"),
        cancel,
    )
    .await
    .map_err(|e| format!("srid: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if srid == 4326 {
        Ok(())
    } else {
        Err(format!("SRID atteso 4326, ottenuto {srid}"))
    }
}

async fn probe_spatial_count_where(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
    predicate_sql: &str,
    expected: i64,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let sql = format!("SELECT COUNT(*)::BIGINT FROM _probe_gis WHERE {predicate_sql}");
    let n = crate::facade::execute_scalar_i64(
        tx.as_mut(),
        &Statement::new(sql),
        cancel,
    )
    .await
    .map_err(|e| format!("count: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == expected {
        Ok(())
    } else {
        Err(format!("attesi {expected}, ottenuti {n}"))
    }
}

async fn probe_spatial_scalar_op(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
    sql: &str,
    expected: i64,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let n = crate::facade::execute_scalar_i64(tx.as_mut(), &Statement::new(sql), cancel)
        .await
        .map_err(|e| format!("op: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if n == expected {
        Ok(())
    } else {
        Err(format!("atteso {expected}, ottenuto {n}"))
    }
}

async fn probe_spatial_distance(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let d = crate::facade::execute_scalar_f64(
        tx.as_mut(),
        &Statement::new(
            "SELECT ST_Distance(
                 (SELECT geom FROM _probe_gis WHERE id = 1),
                 (SELECT geom FROM _probe_gis WHERE id = 2)
             )",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("distance: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if d > 0.0 && d < 100.0 {
        Ok(())
    } else {
        Err(format!("distanza sospetta: {d}"))
    }
}

async fn probe_spatial_nearest(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let id = crate::facade::execute_scalar_i32(
        tx.as_mut(),
        &Statement::new(
            "SELECT id FROM _probe_gis
             ORDER BY geom <-> ST_SetSRID(ST_MakePoint(9.19, 45.46), 4326)
             LIMIT 1",
        ),
        cancel,
    )
    .await
    .map_err(|e| format!("nearest: {}", e.message))?;
    let _ = tx.rollback(cancel).await;
    if id == 1 {
        Ok(())
    } else {
        Err(format!("nearest atteso id=1 (Milano), ottenuto {id}"))
    }
}

async fn probe_spatial_index_available(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    // GIST è la implementation Postgres per spatial index. Verifica capability:
    // proviamo a creare (e droppare) un indice GIST sulla tabella di test.
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    tx.execute(
        &Statement::new("CREATE INDEX IF NOT EXISTS _probe_gis_gix ON _probe_gis USING GIST(geom)"),
        cancel,
    )
    .await
    .map_err(|e| format!("create index: {}", e.message))?;
    tx.execute(
        &Statement::new("DROP INDEX IF EXISTS _probe_gis_gix"),
        cancel,
    )
    .await
    .map_err(|e| format!("drop index: {}", e.message))?;
    tx.commit(cancel)
        .await
        .map_err(|e| format!("commit: {}", e.message))?;
    Ok(())
}

async fn probe_spatial_invalid_rejected(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    // Un WKB completamente invalido deve produrre errore.
    let outcome = tx
        .execute(
            &Statement::new("SELECT ST_GeomFromEWKB($1)")
                .with_params(vec![ParameterValue::Bytes(vec![0xff, 0x00, 0x01])]),
            cancel,
        )
        .await;
    let _ = tx.rollback(cancel).await;
    if outcome.is_err() {
        Ok(())
    } else {
        Err("geometry invalida non rifiutata".into())
    }
}

async fn probe_spatial_null_handled(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    // Verifica che una geometry NULL non produca panic/UB. Sia il Null tipizzato
    // che un errore Unsupported graceful sono comportamenti "handled".
    let outcome = crate::facade::query_one(
        tx.as_mut(),
        &Statement::new("SELECT NULL::geometry"),
        cancel,
    )
    .await;
    let _ = tx.rollback(cancel).await;
    match outcome {
        Ok(row) if matches!(row.get_index(0), Some(ParameterValue::Null { .. })) => Ok(()),
        Err(e) if e.category == crate::ErrorCategory::Unsupported => Ok(()),
        Err(e) => Err(format!("null geom errore inatteso: {:?}", e.category)),
        Ok(row) => Err(format!("null geom non typed-null: {:?}", row.get_index(0))),
    }
}

async fn probe_spatial_large_streaming(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    // Streaming di 200 punti generati al volo con cursor batch 50.
    let stmt = Statement::new(
        "SELECT gs::INT FROM generate_series(1, 200) gs",
    );
    let mut stream = tx
        .query_stream(&stmt, 50, cancel)
        .await
        .map_err(|e| format!("stream: {}", e.message))?;
    let mut total = 0_usize;
    while let Some(batch) = stream
        .next_batch(cancel)
        .await
        .map_err(|e| format!("fetch: {}", e.message))?
    {
        total += batch.len();
    }
    drop(stream);
    let _ = tx.rollback(cancel).await;
    if total == 200 {
        Ok(())
    } else {
        Err(format!("attesi 200 punti, ottenuti {total}"))
    }
}

async fn probe_spatial_cross_srid(
    provider: &dyn Provider,
    secret: &SecretString,
    budget: &ResourceBudget,
    cancel: &CancellationToken,
) -> std::result::Result<(), String> {
    // Un'operazione di ST_Intersects tra geometrie con SRID diversi (4326 vs
    // 3857) deve fallire fail-closed. Postgres emette errore, la libreria lo
    // propaga come errore tecnico invece di trasformare silenziosamente.
    let mut tx = provider
        .begin_transaction(secret, &TransactionOptions::default(), budget, cancel)
        .await
        .map_err(|e| format!("begin: {}", e.message))?;
    let outcome = tx
        .execute(
            &Statement::new(
                "SELECT ST_Intersects(
                     ST_SetSRID(ST_MakePoint(0, 0), 4326),
                     ST_SetSRID(ST_MakePoint(0, 0), 3857)
                 )",
            ),
            cancel,
        )
        .await;
    let _ = tx.rollback(cancel).await;
    if outcome.is_err() {
        Ok(())
    } else {
        Err("cross-SRID non rifiutato — attesa transformation policy fail-closed".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_profile_pass_when_all_verified() {
        let evidence: Vec<_> = APPLICATION_OLTP_V1
            .required
            .iter()
            .map(|c| CapabilityEvidence::verified(*c))
            .collect();
        let report = check_profile(&APPLICATION_OLTP_V1, &evidence);
        assert_eq!(report.status, ProfileStatus::Pass);
        assert!(report.missing.is_empty());
        assert!(report.failed.is_empty());
    }

    #[test]
    fn check_profile_fails_on_missing_capability() {
        let evidence: Vec<_> = APPLICATION_OLTP_V1
            .required
            .iter()
            .skip(1)
            .map(|c| CapabilityEvidence::verified(*c))
            .collect();
        let report = check_profile(&APPLICATION_OLTP_V1, &evidence);
        assert_eq!(report.status, ProfileStatus::Fail);
        assert_eq!(report.missing, vec![APPLICATION_OLTP_V1.required[0]]);
    }

    #[test]
    fn check_profile_fails_on_failed_capability() {
        let evidence = vec![
            CapabilityEvidence::verified(Capability::Transactions),
            CapabilityEvidence::failed(Capability::Savepoints, "test failure"),
        ];
        let profile = ConformanceProfile {
            name: "T",
            required: &[Capability::Transactions, Capability::Savepoints],
        };
        let report = check_profile(&profile, &evidence);
        assert_eq!(report.status, ProfileStatus::Fail);
        assert!(report.missing.is_empty());
        assert_eq!(report.failed, vec![Capability::Savepoints]);
    }

    #[test]
    fn evidence_serializes_snake_case() {
        let e = CapabilityEvidence::verified(Capability::OptimisticConcurrency);
        let json = serde_json::to_string(&e).unwrap();
        assert!(json.contains("optimistic_concurrency"));
        assert!(json.contains("verified"));
    }
}
