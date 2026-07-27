//! Supporto di conformità indipendente dai database reali.

use plenora_database_core::{DatabaseError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

pub const GOLDEN_V1: &str = include_str!("../../../golden/v1/cases.json");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoldenSuite {
    pub schema_version: u32,
    pub suite: String,
    pub cases: Vec<GoldenCase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoldenCase {
    pub id: String,
    pub dataset: String,
    pub category: String,
    pub classification: String,
    pub providers: Vec<String>,
    pub status: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub input: Value,
    pub expected: Value,
    pub notes: Option<String>,
}

/// Carica e verifica il catalogo golden incorporato nel binario di test.
///
/// # Errors
///
/// Restituisce `InvalidPlan` se il JSON è invalido, vuoto o contiene ID
/// duplicati.
pub fn golden_v1() -> Result<GoldenSuite> {
    let suite: GoldenSuite = serde_json::from_str(GOLDEN_V1)
        .map_err(|_| DatabaseError::invalid_plan("golden v1 non valido"))?;
    let mut ids = BTreeSet::new();
    if suite.schema_version != 1
        || suite.suite != "golden-v1"
        || suite.cases.is_empty()
        || !suite.cases.iter().all(|case| ids.insert(case.id.clone()))
    {
        return Err(DatabaseError::invalid_plan(
            "golden v1 incoerente o con id duplicati",
        ));
    }
    Ok(suite)
}

#[must_use]
pub fn contains_secret_marker(text: &str, markers: &[&str]) -> bool {
    let lowercase = text.to_lowercase();
    markers
        .iter()
        .any(|marker| lowercase.contains(&marker.to_lowercase()))
}

/// Verifica che nessun marker segreto compaia nel testo pubblico.
///
/// # Errors
///
/// Restituisce `InvalidPlan` quando trova un marker, senza includere il marker
/// nel messaggio di errore.
pub fn assert_redacted(text: &str, markers: &[&str]) -> Result<()> {
    if contains_secret_marker(text, markers) {
        Err(DatabaseError::invalid_plan(
            "artefatto pubblico contenente marker segreto",
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_golden_suite_is_valid() {
        let suite = golden_v1().expect("golden");
        assert_eq!(suite.cases.len(), 20);
    }

    #[test]
    fn secret_markers_are_case_insensitive() {
        assert!(contains_secret_marker(
            "Authorization: Bearer TOP-SECRET",
            &["top-secret"]
        ));
        assert_redacted("authentication failed", &["top-secret"]).expect("redacted");
    }
}
