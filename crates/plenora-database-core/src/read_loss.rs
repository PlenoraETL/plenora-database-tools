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
    pub const fn new(entries: Vec<ReadLossEntry>) -> Self {
        Self {
            schema_version: 1,
            status: ReadStatus::Open,
            entries,
        }
    }
}
