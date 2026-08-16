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
    /// Un fallimento prima del commit annulla **le righe** scritte
    /// dall'operazione.
    ///
    /// Il flag riguarda i dati, non lo schema. Una mode che prepara il target
    /// con del DDL — `Create`, e su alcuni provider `Replace` — puo lasciarlo
    /// dietro di se anche quando le righe tornano indietro, se il motore
    /// esegue il DDL fuori dalla transazione. Quel comportamento e descritto
    /// da [`TransactionCapabilities::transactional_ddl`]: `false` significa
    /// che il DDL di preparazione sopravvive al rollback.
    ///
    /// Letti insieme, i due flag dicono cosa aspettarsi:
    ///
    /// | `rollback_on_failure` | `transactional_ddl` | esito di un `Create` fallito |
    /// | --- | --- | --- |
    /// | `true` | `true` | niente resta: righe e tabella annullate |
    /// | `true` | `false` | la tabella resta vuota, le righe no |
    /// | `false` | — | possono restare righe parziali |
    ///
    /// Il caso `true`/`false` non e ambiguo per il chiamante: l'esito
    /// dell'operazione lo dichiara riga per riga con
    /// [`crate::error::RemoteEffect::Partial`] e
    /// [`crate::error::RetryDisposition::RequiresRecovery`], perche un retry
    /// cieco troverebbe il target gia esistente.
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
