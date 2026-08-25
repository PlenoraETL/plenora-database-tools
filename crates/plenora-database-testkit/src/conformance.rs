use plenora_database_core::capabilities::ProviderCapabilities;
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
/// presente, deve essere un'operazione che `inspect` non serve — non
/// un'ispezione mal formata, ma una che appartiene a un'altra superficie del
/// provider.
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

/// Verifica che il documento capability appartenga al provider atteso e sia
/// coerente.
///
/// Le invarianti provider-independent — major, lunghezze, duplicati, limiti a
/// zero e combinazioni contraddittorie — vivono ora in
/// [`ProviderCapabilities::validate`], nel core. Qui restava l'unica copia, e
/// il consumatore vero (`plenora_database_engine::prepare`) non la
/// attraversava: due strade per la stessa domanda, e quella percorsa era la
/// piu povera.
///
/// # Errors
///
/// Restituisce `InvalidPlan` quando il documento e attribuito a un altro
/// provider o viola una delle invarianti del contratto.
pub fn validate_capabilities(
    expected_provider: ProviderKind,
    capabilities: &ProviderCapabilities,
) -> Result<()> {
    if capabilities.provider != expected_provider {
        return Err(contract_error(
            "documento capability attribuito a un provider differente",
        ));
    }
    // Qui si **pubblica**, non si consuma: un provider di questo workspace
    // non deve poter emettere un documento incoerente, anche quando la
    // coerenza non e scritta nel contratto. Il percorso di consumo
    // (`prepare`) si ferma invece a `validate()`, perche rifiutare la
    // coerenza li restringerebbe la major v2.
    capabilities.validate()?;
    capabilities.validate_coherence()
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
        Operation::DatabaseListCatalogs => "database.list_catalogs",
        Operation::DatabaseListSchemas { .. } => "database.list_schemas",
        Operation::DatabaseListObjects { .. } => "database.list_objects",
        Operation::DatabaseDescribeObject { .. } => "database.describe_object",
        Operation::DatabaseRead { .. } => "database.read",
        Operation::DatabaseWrite { .. } => "database.write",
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

fn contract_error(message: &'static str) -> DatabaseError {
    DatabaseError::invalid_plan(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::capabilities::{
        ProviderLimits, ReadCapabilities, SpatialCapabilities, TransactionCapabilities,
        TransactionScope, WriteCapabilities,
    };
    use plenora_database_core::geometry::Dimensions;
    use plenora_database_core::query::SpatialFunction;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn valid_capabilities() -> ProviderCapabilities {
        ProviderCapabilities {
            schema_version: 2,
            provider: ProviderKind::Postgres,
            provider_version: "16.9".to_owned(),
            extension_versions: BTreeMap::from([("postgis".to_owned(), "3.5.2".to_owned())]),
            reads: ReadCapabilities {
                streaming: true,
                server_cursor: true,
                pagination: true,
                projection: true,
                filter: true,
                ordering: true,
                resumable: false,
            },
            writes: WriteCapabilities {
                create: true,
                append: true,
                truncate_insert: true,
                update: true,
                upsert: true,
                replace: true,
                delete_by_keys: true,
                bulk: true,
                array_binding: false,
                returning: true,
                rollback_on_failure: true,
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
