use plenora_database_core::capabilities::{ProviderCapabilities, TransactionScope};
use plenora_database_core::plan::{Operation, ProviderKind};
use plenora_database_core::provider::{ConnectionInfo, Inspection, Provider, SecretString};
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use serde::{Deserialize, Serialize};

/// Evidenza serializzabile prodotta dalla suite comune di conformità.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConformanceReport {
    pub schema_version: u32,
    pub provider: ProviderKind,
    pub provider_version: String,
    pub inspected_operations: Vec<String>,
    pub pre_cancelled_connection_verified: bool,
    pub unsupported_inspection_verified: bool,
}

/// Esegue il contratto comune che ogni provider concreto deve rispettare.
///
/// Le operazioni in `supported_inspections` devono essere realmente
/// disponibili nell'ambiente di prova. `unsupported_inspection`, quando
/// presente, deve appartenere intenzionalmente a un altro perimetro.
///
/// # Errors
///
/// Restituisce l'errore del provider oppure `InvalidPlan` se un risultato
/// pubblico viola il contratto comune.
pub async fn verify_provider_contract<P: Provider + ?Sized>(
    provider: &P,
    secret: &SecretString,
    supported_inspections: &[Operation],
    unsupported_inspection: Option<&Operation>,
) -> Result<ProviderConformanceReport> {
    let expected_provider = provider.kind();
    let cancellation = CancellationToken::new();

    let connection = provider.test_connection(secret, &cancellation).await?;
    validate_connection(expected_provider, &connection)?;

    let capabilities = provider.probe_capabilities(secret, &cancellation).await?;
    validate_capabilities(expected_provider, &capabilities)?;

    let mut inspected_operations = Vec::with_capacity(supported_inspections.len());
    for operation in supported_inspections {
        let inspection = provider.inspect(secret, operation, &cancellation).await?;
        validate_inspection(operation, &inspection)?;
        inspected_operations.push(inspection.operation);
    }

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancellation_error = provider
        .test_connection(secret, &cancelled)
        .await
        .map_or_else(Ok, |_| {
            Err(contract_error(
                "test_connection ha ignorato una cancellazione già richiesta",
            ))
        })?;
    validate_pre_cancelled_error(expected_provider, &cancellation_error)?;

    let unsupported_inspection_verified = if let Some(operation) = unsupported_inspection {
        let unsupported_error = provider
            .inspect(secret, operation, &cancellation)
            .await
            .map_or_else(Ok, |_| {
                Err(contract_error(
                    "inspect ha accettato un'operazione dichiarata non supportata",
                ))
            })?;
        validate_unsupported_error(expected_provider, &unsupported_error)?;
        true
    } else {
        false
    };

    Ok(ProviderConformanceReport {
        schema_version: 1,
        provider: expected_provider,
        provider_version: capabilities.provider_version,
        inspected_operations,
        pre_cancelled_connection_verified: true,
        unsupported_inspection_verified,
    })
}

/// Verifica l'envelope restituito dal test di connessione.
///
/// # Errors
///
/// Restituisce `InvalidPlan` se identità, versione o provider sono incoerenti.
pub fn validate_connection(
    expected_provider: ProviderKind,
    connection: &ConnectionInfo,
) -> Result<()> {
    if connection.provider != expected_provider {
        return Err(contract_error(
            "test_connection ha restituito un provider differente",
        ));
    }
    if connection.server_version.trim().is_empty() {
        return Err(contract_error(
            "test_connection ha restituito una versione server vuota",
        ));
    }
    if connection
        .connection_identity
        .as_ref()
        .is_some_and(|identity| identity.trim().is_empty())
    {
        return Err(contract_error(
            "test_connection ha restituito un'identità vuota",
        ));
    }
    Ok(())
}

/// Verifica invarianti provider-independent del documento capability v1.
///
/// # Errors
///
/// Restituisce `InvalidPlan` quando le capability sono contraddittorie,
/// duplicate o non bounded.
#[allow(clippy::too_many_lines)]
pub fn validate_capabilities(
    expected_provider: ProviderKind,
    capabilities: &ProviderCapabilities,
) -> Result<()> {
    if capabilities.schema_version != 1 {
        return Err(contract_error(
            "documento capability con schema_version non supportata",
        ));
    }
    if capabilities.provider != expected_provider {
        return Err(contract_error(
            "documento capability attribuito a un provider differente",
        ));
    }
    if capabilities.provider_version.trim().is_empty() {
        return Err(contract_error(
            "documento capability senza versione provider",
        ));
    }
    if capabilities
        .extension_versions
        .iter()
        .any(|(name, version)| name.trim().is_empty() || version.trim().is_empty())
    {
        return Err(contract_error(
            "documento capability con estensione o versione vuota",
        ));
    }
    if has_duplicates(&capabilities.spatial.dimensions) {
        return Err(contract_error(
            "documento capability con dimensioni spatial duplicate",
        ));
    }
    if has_duplicates(&capabilities.spatial.functions) {
        return Err(contract_error(
            "documento capability con funzioni spatial duplicate",
        ));
    }

    let spatial = &capabilities.spatial;
    let has_spatial_semantics = spatial.geometry || spatial.geography;
    let claims_spatial_behavior = spatial.read_wkb
        || spatial.write_wkb
        || spatial.spatial_index
        || spatial.mixed_geometry_types
        || !spatial.dimensions.is_empty()
        || !spatial.functions.is_empty();
    if claims_spatial_behavior && !has_spatial_semantics {
        return Err(contract_error(
            "documento capability spatial senza geometry o geography",
        ));
    }
    if has_spatial_semantics && spatial.dimensions.is_empty() {
        return Err(contract_error(
            "documento capability spatial senza dimensionalità",
        ));
    }

    let reads = &capabilities.reads;
    if reads.server_cursor && !reads.streaming {
        return Err(contract_error(
            "server_cursor richiede streaming nel documento capability",
        ));
    }

    let transactions = &capabilities.transactions;
    if transactions.savepoints && !transactions.single_transaction {
        return Err(contract_error("savepoints richiede single_transaction"));
    }
    if transactions.staged_swap
        && (!transactions.single_transaction || !transactions.transactional_ddl)
    {
        return Err(contract_error(
            "staged_swap richiede transazione singola e DDL transazionale",
        ));
    }
    if transactions.single_transaction && transactions.scope == TransactionScope::None {
        return Err(contract_error(
            "single_transaction non può avere scope none",
        ));
    }
    if !transactions.single_transaction && transactions.scope == TransactionScope::Transaction {
        return Err(contract_error(
            "scope transaction richiede single_transaction",
        ));
    }

    let limits = &capabilities.limits;
    let bounded_limits = [
        limits.max_identifier_bytes,
        limits.max_bind_parameters,
        limits.max_statement_bytes,
        limits.max_batch_rows,
        limits.max_payload_bytes,
        limits.max_record_count,
    ];
    if bounded_limits.into_iter().flatten().any(|limit| limit == 0) {
        return Err(contract_error(
            "documento capability con limite esplicito pari a zero",
        ));
    }
    Ok(())
}

/// Verifica nome canonico e forma JSON di un risultato di introspezione.
///
/// # Errors
///
/// Restituisce `InvalidPlan` se il risultato non corrisponde all'operazione.
pub fn validate_inspection(operation: &Operation, inspection: &Inspection) -> Result<()> {
    let expected_operation = operation_id(operation);
    if inspection.operation != expected_operation {
        return Err(contract_error(
            "inspect ha restituito un identificatore operazione differente",
        ));
    }
    if !inspection.document.is_object() {
        return Err(contract_error(
            "inspect deve restituire un documento JSON object",
        ));
    }
    Ok(())
}

/// Identificatore wire canonico di un'operazione di introspezione.
#[must_use]
pub const fn operation_id(operation: &Operation) -> &'static str {
    match operation {
        Operation::DatabaseTestConnection => "database.test_connection",
        Operation::ArcgisTestConnection => "arcgis.test_connection",
        Operation::DatabaseListCatalogs => "database.list_catalogs",
        Operation::DatabaseListSchemas { .. } => "database.list_schemas",
        Operation::DatabaseListObjects { .. } => "database.list_objects",
        Operation::DatabaseDescribeObject { .. } => "database.describe_object",
        Operation::ArcgisListFolders => "arcgis.list_folders",
        Operation::ArcgisListItems { .. } => "arcgis.list_items",
        Operation::ArcgisListServices { .. } => "arcgis.list_services",
        Operation::ArcgisListLayers { .. } => "arcgis.list_layers",
        Operation::ArcgisDescribeLayer { .. } => "arcgis.describe_layer",
        Operation::DatabaseRead { .. } => "database.read",
        Operation::ArcgisRead { .. } => "arcgis.read",
        Operation::DatabaseWrite { .. } => "database.write",
        Operation::ArcgisWrite { .. } => "arcgis.write",
    }
}

fn validate_pre_cancelled_error(
    expected_provider: ProviderKind,
    error: &DatabaseError,
) -> Result<()> {
    validate_error_envelope(
        expected_provider,
        error,
        ErrorCategory::Cancelled,
        ErrorPhase::Connect,
    )
}

fn validate_unsupported_error(
    expected_provider: ProviderKind,
    error: &DatabaseError,
) -> Result<()> {
    validate_error_envelope(
        expected_provider,
        error,
        ErrorCategory::Unsupported,
        ErrorPhase::Probe,
    )
}

fn validate_error_envelope(
    expected_provider: ProviderKind,
    error: &DatabaseError,
    category: ErrorCategory,
    phase: ErrorPhase,
) -> Result<()> {
    if error.category != category
        || error.phase != phase
        || error.remote_effect != RemoteEffect::None
        || error.retry != RetryDisposition::Never
        || error.provider != Some(expected_provider)
    {
        return Err(contract_error(
            "errore provider non conforme all'envelope richiesto",
        ));
    }
    Ok(())
}

fn has_duplicates<T: PartialEq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[index + 1..].contains(value))
}

fn contract_error(message: &'static str) -> DatabaseError {
    DatabaseError::invalid_plan(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::capabilities::{
        ProviderLimits, ReadCapabilities, SpatialCapabilities, TransactionCapabilities,
        WriteCapabilities,
    };
    use plenora_database_core::geometry::Dimensions;
    use plenora_database_core::query::SpatialFunction;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn valid_capabilities() -> ProviderCapabilities {
        ProviderCapabilities {
            schema_version: 1,
            provider: ProviderKind::Postgres,
            provider_version: "16.9".to_owned(),
            extension_versions: BTreeMap::from([("postgis".to_owned(), "3.5.2".to_owned())]),
            reads: ReadCapabilities {
                streaming: true,
                server_cursor: true,
                pagination: true,
                object_id_windows: false,
                projection: true,
                filter: true,
                ordering: true,
                resumable: false,
            },
            writes: WriteCapabilities {
                create: true,
                append: true,
                update: true,
                upsert: true,
                replace: true,
                delete_by_keys: true,
                bulk: true,
                array_binding: false,
                returning: true,
                apply_edits: false,
                rollback_on_failure: true,
                use_global_ids: false,
            },
            transactions: TransactionCapabilities {
                single_transaction: true,
                savepoints: true,
                transactional_ddl: true,
                staged_swap: true,
                scope: TransactionScope::Transaction,
            },
            spatial: SpatialCapabilities {
                read_wkb: true,
                write_wkb: true,
                geometry: true,
                geography: true,
                spatial_index: true,
                mixed_geometry_types: true,
                dimensions: vec![Dimensions::Xy, Dimensions::Xyz],
                functions: vec![SpatialFunction::Intersects],
            },
            limits: ProviderLimits {
                max_identifier_bytes: Some(63),
                max_bind_parameters: Some(65_535),
                max_statement_bytes: None,
                max_batch_rows: None,
                max_payload_bytes: None,
                max_record_count: None,
            },
        }
    }

    #[test]
    fn coherent_capabilities_are_accepted() {
        validate_capabilities(ProviderKind::Postgres, &valid_capabilities())
            .expect("capability coerenti");
    }

    #[test]
    fn spatial_claim_without_semantics_is_rejected() {
        let mut capabilities = valid_capabilities();
        capabilities.spatial.geometry = false;
        capabilities.spatial.geography = false;
        assert!(validate_capabilities(ProviderKind::Postgres, &capabilities).is_err());
    }

    #[test]
    fn duplicate_spatial_entries_are_rejected() {
        let mut capabilities = valid_capabilities();
        capabilities.spatial.dimensions.push(Dimensions::Xy);
        assert!(validate_capabilities(ProviderKind::Postgres, &capabilities).is_err());
    }

    #[test]
    fn transaction_claims_are_fail_closed() {
        let mut capabilities = valid_capabilities();
        capabilities.transactions.single_transaction = false;
        assert!(validate_capabilities(ProviderKind::Postgres, &capabilities).is_err());
    }

    #[test]
    fn inspection_requires_canonical_operation_and_object() {
        let inspection = Inspection {
            operation: "database.list_schemas".to_owned(),
            document: json!({"schemas": []}),
        };
        validate_inspection(
            &Operation::DatabaseListSchemas { source: None },
            &inspection,
        )
        .expect("inspection coerente");

        let invalid = Inspection {
            operation: "database.list_catalogs".to_owned(),
            document: json!([]),
        };
        assert!(validate_inspection(&Operation::DatabaseListCatalogs, &invalid).is_err());
    }
}
