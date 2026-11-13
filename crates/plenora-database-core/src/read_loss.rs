//! Rapporto di lettura separato dal wire esecutivo v3 della scrittura.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadStatus {
    Open,
    Complete,
    Failed,
    Interrupted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadLossEntry {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::metadata_subject::present"
    )]
    pub subject: Option<crate::metadata_subject::MetadataSubject>,
    pub position: u32,
    pub field_id: Option<String>,
    pub property: String,
    pub rule: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadLossReport {
    pub schema_version: u32,
    pub status: ReadStatus,
    pub entries: Vec<ReadLossEntry>,
}

impl ReadLossReport {
    #[must_use]
    pub fn new(entries: Vec<ReadLossEntry>) -> Self {
        Self {
            schema_version: if entries.iter().any(|e| e.subject.is_some()) {
                2
            } else {
                1
            },
            status: ReadStatus::Open,
            entries,
        }
    }
}
