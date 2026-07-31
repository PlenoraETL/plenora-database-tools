#![allow(clippy::struct_excessive_bools)]
// Il wire contract usa flag indipendenti: un bitset o enum renderebbe il JSON
// meno stabile e non rappresenterebbe capability combinabili liberamente.

use crate::geometry::Dimensions;
use crate::plan::ProviderKind;
use crate::query::SpatialFunction;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadCapabilities {
    pub streaming: bool,
    pub server_cursor: bool,
    pub pagination: bool,
    pub object_id_windows: bool,
    pub projection: bool,
    pub filter: bool,
    pub ordering: bool,
    pub resumable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteCapabilities {
    pub create: bool,
    pub append: bool,
    pub update: bool,
    pub upsert: bool,
    pub replace: bool,
    pub delete_by_keys: bool,
    pub bulk: bool,
    pub array_binding: bool,
    pub returning: bool,
    pub apply_edits: bool,
    pub rollback_on_failure: bool,
    pub use_global_ids: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionScope {
    None,
    Statement,
    Transaction,
    EditRequest,
    Layer,
    Service,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionCapabilities {
    pub single_transaction: bool,
    pub savepoints: bool,
    pub transactional_ddl: bool,
    pub staged_swap: bool,
    pub scope: TransactionScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpatialCapabilities {
    pub read_wkb: bool,
    pub write_wkb: bool,
    pub geometry: bool,
    pub geography: bool,
    pub spatial_index: bool,
    pub mixed_geometry_types: bool,
    pub dimensions: Vec<Dimensions>,
    /// Sottoinsieme garantito per ogni semantica spatial pubblicizzata.
    ///
    /// Un provider con capability native asimmetriche deve sotto-dichiarare
    /// l'intersezione; il contratto v1 non consente di attribuire una funzione
    /// soltanto a `geometry` o soltanto a `geography`.
    #[serde(default)]
    pub functions: Vec<SpatialFunction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderLimits {
    pub max_identifier_bytes: Option<u64>,
    pub max_bind_parameters: Option<u64>,
    pub max_statement_bytes: Option<u64>,
    pub max_batch_rows: Option<u64>,
    pub max_payload_bytes: Option<u64>,
    pub max_record_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCapabilities {
    pub schema_version: u32,
    pub provider: ProviderKind,
    pub provider_version: String,
    pub extension_versions: BTreeMap<String, String>,
    pub reads: ReadCapabilities,
    pub writes: WriteCapabilities,
    pub transactions: TransactionCapabilities,
    pub spatial: SpatialCapabilities,
    pub limits: ProviderLimits,
}
