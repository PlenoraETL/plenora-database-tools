use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    pub max_batches: u64,
    pub max_memory_bytes: u64,
    pub max_batch_bytes: u64,
    pub max_wkb_cell_bytes: u64,
    pub timeout_ms: u64,
    #[serde(skip)]
    pub max_plan_json_bytes: usize,
    #[serde(skip)]
    pub max_identifier_bytes: usize,
    #[serde(skip)]
    pub max_filter_depth: usize,
    #[serde(skip)]
    pub max_filter_nodes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_rows: None,
            max_batches: 65_536,
            max_memory_bytes: 512 * 1024 * 1024,
            max_batch_bytes: 16 * 1024 * 1024,
            max_wkb_cell_bytes: 64 * 1024 * 1024,
            timeout_ms: 30_000,
            max_plan_json_bytes: 4 * 1024 * 1024,
            max_identifier_bytes: 256,
            max_filter_depth: 64,
            max_filter_nodes: 4_096,
        }
    }
}

impl Limits {
    /// Verifica coerenza e non-nullità dei limiti hard.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` quando un limite hard è zero o quando un
    /// singolo batch può eccedere il budget memoria totale.
    pub fn validate(&self) -> crate::Result<()> {
        if self.max_batches == 0
            || self.max_memory_bytes == 0
            || self.max_batch_bytes == 0
            || self.max_wkb_cell_bytes == 0
            || self.timeout_ms == 0
            || self.max_plan_json_bytes == 0
            || self.max_identifier_bytes == 0
            || self.max_filter_depth == 0
            || self.max_filter_nodes == 0
        {
            return Err(crate::DatabaseError::invalid_plan(
                "i limiti hard devono essere maggiori di zero",
            ));
        }
        if self.max_batch_bytes > self.max_memory_bytes {
            return Err(crate::DatabaseError::invalid_plan(
                "max_batch_bytes supera max_memory_bytes",
            ));
        }
        Ok(())
    }
}
