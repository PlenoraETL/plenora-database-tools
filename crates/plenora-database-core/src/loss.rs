use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MappingPolicy {
    Strict,
    Compatible,
    Lossy,
    Native,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LossSeverity {
    Information,
    Warning,
    DataLoss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LossCategory {
    Precision,
    Scale,
    Range,
    Timezone,
    Encoding,
    Collation,
    NativeType,
    GeometryType,
    Dimensions,
    Srid,
    Crs,
    Nullability,
    Default,
    Generated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MappingLoss {
    pub field_id: u32,
    pub category: LossCategory,
    pub severity: LossSeverity,
    pub reason: String,
    pub source_type: Option<String>,
    pub target_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LossReport {
    pub schema_version: u32,
    pub policy: MappingPolicy,
    pub losses: Vec<MappingLoss>,
}

impl LossReport {
    #[must_use]
    pub fn permits_execution(&self) -> bool {
        self.policy != MappingPolicy::Strict
            || !self
                .losses
                .iter()
                .any(|loss| loss.severity == LossSeverity::DataLoss)
    }
}
