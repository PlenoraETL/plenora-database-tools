//! Limiti serializzabili applicati a letture, scritture e ispezioni.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    /// Assente vale «nessun tetto»; `null` si rifiuta. Lo schema non lo
    /// ammette e il serializzatore non lo scrive, quindi nessun documento
    /// legittimo lo contiene.
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_not_null"
    )]
    pub max_rows: Option<u64>,
    pub max_batches: u64,
    pub max_memory_bytes: u64,
    pub max_batch_bytes: u64,
    pub max_wkb_cell_bytes: u64,
    pub timeout_ms: u64,
    #[serde(skip)]
    pub max_plan_json_bytes: usize,
    #[serde(skip)]
    /// Il tetto sui nomi, in **code point**: e il `maxLength: 256` di
    /// `common.schema.json#/$defs/identifier`, non un budget di byte. Il nome
    /// del campo dice altro per ragioni storiche e non compare in
    /// `plan.schema.json`, che ha `additionalProperties: false`: nessun piano
    /// puo dichiararlo, quindi cambiarlo non e una modifica di contratto.
    pub max_identifier_bytes: usize,
    #[serde(skip)]
    pub max_filter_depth: usize,
    #[serde(skip)]
    pub max_filter_nodes: usize,
}

/// Legge un campo opzionale distinguendo l'assenza da `null`.
///
/// Va usato con `#[serde(default)]`: l'assenza la decide il default, e il
/// deserializzatore viene chiamato solo per un valore presente, che quindi
/// non puo essere `null`. Per un campo il cui schema non ammette `null`,
/// accettarlo come assenza sarebbe una tolleranza non dichiarata.
///
/// # Errors
///
/// Rifiuta `null` e ogni valore che `T` rifiuta.
pub(crate) fn present_not_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
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
