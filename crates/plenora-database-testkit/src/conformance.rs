//! Verifiche provider-neutral riutilizzate dalle suite di conformità.

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
/// zero e combinazioni contraddittorie — sono delegate a
/// [`ProviderCapabilities::validate`], nel core.
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
    // Questo percorso qualifica ciò che un provider pubblica e applica anche
    // la coerenza interna. Il consumo tollerante usa invece `validate()` per
    // non restringere il contratto v2.
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
#[path = "conformance_tests.rs"]
mod tests;
