//! Catalogo pubblico condiviso fra Rust, CLI e SDK Python.
//!
//! Le capability del provider descrivono il database realmente raggiunto;
//! questo modulo descrive invece l'artefatto che sta rispondendo, come
//! richiesto da `plenora-capabilities-v2`. Le due informazioni restano
//! separate e il dettaglio del provider entra soltanto negli attributi
//! versionati dell'operazione di scrittura.

use crate::capabilities::ProviderCapabilities;
use serde::{Deserialize, Serialize};
use serde_json::json;

pub const COMPONENT: &str = "plenora-database-tools";
pub const CAPABILITIES_CONTRACT: &str = "plenora-capabilities-v2";
pub const WRITE_ATTRIBUTES_CONTRACT: &str = "plenora-database-capability-attributes-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicSurface {
    Rust,
    Cli,
    PythonSdk,
}

impl PublicSurface {
    const fn contract(self) -> &'static str {
        match self {
            Self::Rust => "plenora-rust-public-v1",
            Self::Cli => "plenora-cli-v2",
            Self::PythonSdk => "plenora-python-sdk-v1",
        }
    }

    const fn version(self) -> u32 {
        match self {
            Self::Cli => 2,
            Self::Rust | Self::PythonSdk => 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicInterface {
    pub kind: PublicSurface,
    pub contract: String,
    pub version: u32,
    pub artifact: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicPayload {
    pub contract: String,
    pub content_types: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicOperationStatus {
    Available,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicSideEffect {
    None,
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicControls {
    pub cancellation: bool,
    pub deadline: bool,
    pub idempotency_key: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicOperation {
    pub id: String,
    pub version: u32,
    pub status: PublicOperationStatus,
    pub surfaces: Vec<PublicSurface>,
    pub input: PublicPayload,
    pub output: PublicPayload,
    pub side_effect: PublicSideEffect,
    pub controls: PublicControls,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicCapabilities {
    pub schema_version: u32,
    pub component: String,
    pub component_version: String,
    pub interfaces: Vec<PublicInterface>,
    pub operations: Vec<PublicOperation>,
}

/// Mappatura versionata di un'operazione verso gli export pubblici Rust.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RustSurfaceBinding {
    pub operation: String,
    pub version: u32,
    pub entrypoints: Vec<String>,
}

/// Documento pubblicabile della superficie Rust del crate core.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RustSurfaceBindings {
    pub schema_version: u32,
    pub artifact: String,
    pub artifact_version: String,
    pub bindings: Vec<RustSurfaceBinding>,
}

#[derive(Clone, Copy)]
struct OperationSpec {
    id: &'static str,
    input: &'static str,
    output: &'static str,
    input_content_types: &'static [&'static str],
    output_content_types: &'static [&'static str],
    side_effect: PublicSideEffect,
    cancellation: bool,
    cli: bool,
    rust_entrypoints: &'static [&'static str],
}

const JSON: &[&str] = &["application/json"];
const ARROW_OUTPUT: &[&str] = &[
    "application/vnd.apache.arrow.stream",
    "application/vnd.apache.arrow.file",
];
const WRITE_INPUT: &[&str] = &[
    "application/json",
    "application/vnd.apache.arrow.stream",
    "application/vnd.apache.arrow.file",
];
const QUERY_OUTPUT: &[&str] = &["application/json", "application/vnd.apache.arrow.stream"];

const OPERATIONS: &[OperationSpec] = &[
    OperationSpec {
        id: "database.test_connection",
        input: "plenora-database-connection-test-input-v1",
        output: "plenora-database-connection-test-result-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::None,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::provider::Provider::test_connection"],
    },
    OperationSpec {
        id: "database.list_catalogs",
        input: "plenora-database-list-catalogs-input-v1",
        output: "plenora-database-list-catalogs-result-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::None,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::provider::Provider::inspect"],
    },
    OperationSpec {
        id: "database.list_schemas",
        input: "plenora-database-list-schemas-input-v1",
        output: "plenora-database-list-schemas-result-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::None,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::provider::Provider::inspect"],
    },
    OperationSpec {
        id: "database.list_objects",
        input: "plenora-database-list-objects-input-v1",
        output: "plenora-database-list-objects-result-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::None,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::provider::Provider::inspect"],
    },
    OperationSpec {
        id: "database.describe_object",
        input: "plenora-database-describe-object-input-v1",
        output: "plenora-database-describe-object-result-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::None,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::provider::Provider::inspect"],
    },
    OperationSpec {
        id: "database.read",
        input: "plenora-database-read-input-v1",
        output: "plenora-database-read-result-v1",
        input_content_types: JSON,
        output_content_types: ARROW_OUTPUT,
        side_effect: PublicSideEffect::None,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::provider::Provider::read"],
    },
    OperationSpec {
        id: "database.write",
        input: "plenora-database-write-input-v1",
        output: "plenora-database-write-result-v1",
        input_content_types: WRITE_INPUT,
        output_content_types: JSON,
        side_effect: PublicSideEffect::Remote,
        cancellation: true,
        cli: true,
        rust_entrypoints: &[
            "plenora_database_core::provider::Provider::prepare_write",
            "plenora_database_core::provider::Provider::write",
        ],
    },
    OperationSpec {
        id: "database.query",
        input: "plenora-database-query-input-v1",
        output: "plenora-database-query-result-v1",
        input_content_types: JSON,
        output_content_types: QUERY_OUTPUT,
        side_effect: PublicSideEffect::None,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::provider::Provider::query"],
    },
    OperationSpec {
        id: "database.execute",
        input: "plenora-database-execute-input-v1",
        output: "plenora-database-execute-result-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::Remote,
        cancellation: true,
        cli: true,
        rust_entrypoints: &["plenora_database_core::transaction::TransactionScope::execute"],
    },
    OperationSpec {
        id: "database.transaction.begin",
        input: "plenora-database-transaction-begin-input-v1",
        output: "plenora-database-transaction-handle-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::Remote,
        cancellation: true,
        cli: false,
        rust_entrypoints: &["plenora_database_core::provider::Provider::begin_transaction"],
    },
    OperationSpec {
        id: "database.transaction.commit",
        input: "plenora-database-transaction-handle-v1",
        output: "plenora-database-transaction-outcome-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::Remote,
        cancellation: false,
        cli: false,
        rust_entrypoints: &["plenora_database_core::transaction::TransactionScope::commit"],
    },
    OperationSpec {
        id: "database.transaction.rollback",
        input: "plenora-database-transaction-handle-v1",
        output: "plenora-database-transaction-outcome-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::Remote,
        cancellation: false,
        cli: false,
        rust_entrypoints: &["plenora_database_core::transaction::TransactionScope::rollback"],
    },
    OperationSpec {
        id: "database.transaction.savepoint",
        input: "plenora-database-savepoint-input-v1",
        output: "plenora-database-transaction-outcome-v1",
        input_content_types: JSON,
        output_content_types: JSON,
        side_effect: PublicSideEffect::Remote,
        cancellation: true,
        cli: false,
        rust_entrypoints: &[
            "plenora_database_core::transaction::TransactionScope::savepoint",
            "plenora_database_core::transaction::TransactionScope::release_savepoint",
            "plenora_database_core::transaction::TransactionScope::rollback_to_savepoint",
        ],
    },
];

/// Descrive l'esatta superficie dell'artefatto che risponde.
///
/// `provider` restringe gli attributi di scrittura alle sole mode realmente
/// sondate. Un documento artifact-only lo omette: non inventa capability di
/// un server che non e stato contattato.
#[must_use]
pub fn public_capabilities(
    surface: PublicSurface,
    artifact: impl Into<String>,
    provider: Option<&ProviderCapabilities>,
) -> PublicCapabilities {
    let operations = OPERATIONS
        .iter()
        .filter(|operation| surface != PublicSurface::Cli || operation.cli)
        .map(|operation| PublicOperation {
            id: operation.id.to_owned(),
            version: 1,
            status: PublicOperationStatus::Available,
            surfaces: vec![surface],
            input: payload(operation.input, operation.input_content_types),
            output: payload(operation.output, operation.output_content_types),
            side_effect: operation.side_effect,
            controls: PublicControls {
                cancellation: operation.cancellation,
                deadline: true,
                idempotency_key: false,
            },
            attributes: (operation.id == "database.write")
                .then(|| provider.map(write_attributes))
                .flatten(),
        })
        .collect();

    PublicCapabilities {
        schema_version: 2,
        component: COMPONENT.to_owned(),
        component_version: env!("CARGO_PKG_VERSION").to_owned(),
        interfaces: vec![PublicInterface {
            kind: surface,
            contract: surface.contract().to_owned(),
            version: surface.version(),
            artifact: artifact.into(),
        }],
        operations,
    }
}

/// Restituisce la mappa normativa delle operazioni verso l'API Rust pubblica.
///
/// La mappa nasce dallo stesso catalogo che produce Capability Discovery, cosi
/// una nuova operazione non puo essere pubblicata senza un binding Rust.
#[must_use]
pub fn rust_surface_bindings() -> RustSurfaceBindings {
    RustSurfaceBindings {
        schema_version: 1,
        artifact: "plenora-database-core".to_owned(),
        artifact_version: env!("CARGO_PKG_VERSION").to_owned(),
        bindings: OPERATIONS
            .iter()
            .map(|operation| RustSurfaceBinding {
                operation: operation.id.to_owned(),
                version: 1,
                entrypoints: operation
                    .rust_entrypoints
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            })
            .collect(),
    }
}

fn payload(contract: &str, content_types: &[&str]) -> PublicPayload {
    PublicPayload {
        contract: contract.to_owned(),
        content_types: content_types.iter().map(ToString::to_string).collect(),
    }
}

fn write_attributes(capabilities: &ProviderCapabilities) -> serde_json::Value {
    let writes = &capabilities.writes;
    let candidates = [
        ("create", writes.create),
        ("append", writes.append),
        ("truncate_insert", writes.truncate_insert),
        ("update", writes.update),
        ("upsert", writes.upsert),
        ("replace", writes.replace),
        ("delete_by_keys", writes.delete_by_keys),
    ];
    let write_modes = candidates
        .into_iter()
        .filter_map(|(name, available)| available.then_some(name))
        .collect::<Vec<_>>();
    json!({
        "contract": WRITE_ATTRIBUTES_CONTRACT,
        "provider": capabilities.provider,
        "write_modes": write_modes,
    })
}

#[cfg(test)]
#[path = "public_contract_tests.rs"]
mod tests;
