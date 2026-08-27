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
/// più stringenti richieste dal PFM. NON include
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

/// Profilo spatial richiesto dal PFM per il read GIS operativo.
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

mod probes_oltp;
mod probes_pfm_core;
mod probes_pfm_gis;

pub use probes_oltp::probe_application_oltp_v1;
pub use probes_pfm_core::probe_pfm_core_v1;
pub use probes_pfm_gis::probe_pfm_gis_v1;

/// Utility condivisa dai probe: converte `Result<(), String>` in evidence
/// (verified/failed) e la accoda al vettore.
pub(super) fn push_probe(
    evidence: &mut Vec<CapabilityEvidence>,
    capability: Capability,
    result: std::result::Result<(), String>,
) {
    match result {
        Ok(()) => evidence.push(CapabilityEvidence::verified(capability)),
        Err(e) => evidence.push(CapabilityEvidence::failed(capability, e)),
    }
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;
