//! Fasi offline `parse -> validate -> fingerprint -> prepare`.
//!
//! L'esecuzione remota sarà aggiunta dagli adapter provider senza introdurre
//! `match` sui provider nell'executor.

use plenora_database_core::capabilities::ProviderCapabilities;
use plenora_database_core::plan::{
    FilterExpression, ObjectRef, Operation, Plan, ProviderKind, TransactionProfile, WriteMode,
    WriteOperation,
};
use plenora_database_core::query::SpatialFunction;
use plenora_database_core::{DatabaseError, Result};
use sha2::{Digest, Sha256};

const HEX: &[u8; 16] = b"0123456789abcdef";

#[derive(Debug, Clone)]
pub struct ValidatedPlan {
    plan: Plan,
    fingerprint: String,
}

impl ValidatedPlan {
    #[must_use]
    pub const fn plan(&self) -> &Plan {
        &self.plan
    }

    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

#[derive(Debug, Clone)]
pub struct PreparedPlan {
    validated: ValidatedPlan,
    capabilities: ProviderCapabilities,
}

impl PreparedPlan {
    #[must_use]
    pub const fn validated(&self) -> &ValidatedPlan {
        &self.validated
    }

    #[must_use]
    pub const fn capabilities(&self) -> &ProviderCapabilities {
        &self.capabilities
    }
}

/// Deserializza e valida un piano entro il limite hard di parsing.
///
/// # Errors
///
/// Restituisce un errore redatto se il JSON è invalido, troppo grande oppure
/// viola un'invariante del contratto.
pub fn parse_and_validate(input: &[u8]) -> Result<ValidatedPlan> {
    let hard_limit = plenora_database_core::limits::Limits::default().max_plan_json_bytes;
    if input.len() > hard_limit {
        return Err(DatabaseError::invalid_plan(
            "piano oltre il limite di parsing",
        ));
    }
    let plan: Plan = serde_json::from_slice(input)
        .map_err(|_| DatabaseError::invalid_plan("JSON piano non valido"))?;
    validate(plan)
}

/// Valida un piano già deserializzato e ne calcola il fingerprint.
///
/// # Errors
///
/// Restituisce `InvalidPlan` per versioni, limiti, riferimenti, operazioni o
/// profili incompatibili.
pub fn validate(plan: Plan) -> Result<ValidatedPlan> {
    if plan.schema_version != 1 {
        return Err(DatabaseError::invalid_plan(
            "schema_version del piano non supportata",
        ));
    }
    plan.limits.validate()?;
    validate_connection_ref(&plan.connection_ref)?;
    let operation_is_arcgis = plan.operation.is_arcgis();
    if (plan.provider == ProviderKind::Arcgis) != operation_is_arcgis {
        return Err(DatabaseError::invalid_plan(
            "provider e operation appartengono a famiglie diverse",
        ));
    }
    validate_operation(&plan)?;
    let canonical = serde_json::to_vec(&plan)
        .map_err(|_| DatabaseError::invalid_plan("piano non serializzabile"))?;
    let digest = Sha256::digest(canonical);
    let mut fingerprint = String::with_capacity(digest.len() * 2);
    for byte in digest {
        fingerprint.push(char::from(HEX[usize::from(byte >> 4)]));
        fingerprint.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(ValidatedPlan { plan, fingerprint })
}

/// Verifica che le capability runtime possano eseguire il piano.
///
/// # Errors
///
/// Restituisce `InvalidPlan` per provider discordante o `Unsupported` quando
/// la capability necessaria non è pubblicizzata.
pub fn prepare(
    validated: ValidatedPlan,
    capabilities: ProviderCapabilities,
) -> Result<PreparedPlan> {
    if validated.plan.provider != capabilities.provider {
        return Err(DatabaseError::invalid_plan(
            "capability riferite a un provider diverso",
        ));
    }
    match &validated.plan.operation {
        Operation::DatabaseRead { .. } | Operation::ArcgisRead { .. } => {
            if !capabilities.reads.streaming {
                return Err(DatabaseError::unsupported(
                    capabilities.provider,
                    plenora_database_core::ErrorPhase::Prepare,
                    "read streaming non supportato",
                ));
            }
        }
        Operation::DatabaseWrite { write } | Operation::ArcgisWrite { write } => {
            validate_write_capability(write.mode, &capabilities)?;
        }
        _ => {}
    }
    Ok(PreparedPlan {
        validated,
        capabilities,
    })
}

fn validate_connection_ref(connection_ref: &str) -> Result<()> {
    let valid_prefix = ["env:", "secret:", "connection:"]
        .iter()
        .any(|prefix| connection_ref.starts_with(prefix));
    if !valid_prefix || connection_ref.len() > 256 || connection_ref.contains('\0') {
        return Err(DatabaseError::invalid_plan(
            "connection_ref deve usare env:, secret: o connection:",
        ));
    }
    Ok(())
}

fn validate_operation(plan: &Plan) -> Result<()> {
    match &plan.operation {
        Operation::DatabaseDescribeObject { source }
        | Operation::ArcgisListLayers { source }
        | Operation::ArcgisDescribeLayer { source } => {
            validate_object(source, &plan.limits)?;
        }
        Operation::DatabaseListSchemas { source }
        | Operation::DatabaseListObjects { source }
        | Operation::ArcgisListItems { source }
        | Operation::ArcgisListServices { source } => {
            if let Some(source) = source {
                validate_object(source, &plan.limits)?;
            }
        }
        Operation::DatabaseRead { read } | Operation::ArcgisRead { read } => {
            validate_object(&read.source, &plan.limits)?;
            for field in &read.projection {
                validate_identifier(field, plan.limits.max_identifier_bytes)?;
            }
            for order in &read.order_by {
                validate_identifier(&order.field, plan.limits.max_identifier_bytes)?;
            }
            if let Some(filter) = &read.filter {
                let mut nodes = 0;
                validate_filter(filter, 1, &mut nodes, &plan.limits)?;
            }
        }
        Operation::DatabaseWrite { write } | Operation::ArcgisWrite { write } => {
            validate_write(write, plan)?;
        }
        Operation::DatabaseTestConnection
        | Operation::ArcgisTestConnection
        | Operation::DatabaseListCatalogs
        | Operation::ArcgisListFolders => {}
    }
    Ok(())
}

fn validate_write(write: &WriteOperation, plan: &Plan) -> Result<()> {
    validate_object(&write.target, &plan.limits)?;
    for key in &write.keys {
        validate_identifier(key, plan.limits.max_identifier_bytes)?;
    }
    for field in &write.update_columns {
        validate_identifier(field, plan.limits.max_identifier_bytes)?;
    }
    if matches!(
        write.mode,
        WriteMode::Update | WriteMode::Upsert | WriteMode::DeleteByKeys
    ) && write.keys.is_empty()
    {
        return Err(DatabaseError::invalid_plan(
            "la modalità write richiede almeno una chiave",
        ));
    }
    if write.mode == WriteMode::Update && write.update_columns.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "update richiede update_columns",
        ));
    }
    let arcgis_profile = write.transaction_profile == TransactionProfile::ArcgisApplyEdits;
    if (plan.provider == ProviderKind::Arcgis) != arcgis_profile {
        return Err(DatabaseError::invalid_plan(
            "transaction_profile incompatibile con il provider",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_filter(
    expression: &FilterExpression,
    depth: usize,
    nodes: &mut usize,
    limits: &plenora_database_core::limits::Limits,
) -> Result<()> {
    *nodes += 1;
    if depth > limits.max_filter_depth || *nodes > limits.max_filter_nodes {
        return Err(DatabaseError::invalid_plan(
            "filtro oltre i limiti di profondità o nodi",
        ));
    }
    match expression {
        FilterExpression::And { args } | FilterExpression::Or { args } => {
            if args.is_empty() {
                return Err(DatabaseError::invalid_plan("gruppo filtro vuoto"));
            }
            for arg in args {
                validate_filter(arg, depth + 1, nodes, limits)?;
            }
        }
        FilterExpression::Eq { field, parameter }
        | FilterExpression::Ne { field, parameter }
        | FilterExpression::Lt { field, parameter }
        | FilterExpression::Lte { field, parameter }
        | FilterExpression::Gt { field, parameter }
        | FilterExpression::Gte { field, parameter }
        | FilterExpression::Like {
            field, parameter, ..
        } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            validate_parameter(parameter)?;
        }
        FilterExpression::IsNull { field } | FilterExpression::IsNotNull { field } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
        }
        FilterExpression::In { field, parameters } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            if parameters.is_empty() || parameters.len() > 1_024 {
                return Err(DatabaseError::invalid_plan(
                    "IN richiede da 1 a 1024 parametri",
                ));
            }
            for parameter in parameters {
                validate_parameter(parameter)?;
            }
        }
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            validate_parameter(lower_parameter)?;
            validate_parameter(upper_parameter)?;
        }
        FilterExpression::Spatial {
            function,
            field,
            geometry_parameter,
            distance_parameter,
        } => {
            validate_identifier(field, limits.max_identifier_bytes)?;
            if let Some(parameter) = geometry_parameter {
                validate_parameter(parameter)?;
            }
            if let Some(parameter) = distance_parameter {
                validate_parameter(parameter)?;
            }
            match function {
                SpatialFunction::IsEmpty | SpatialFunction::IsValid => {
                    if geometry_parameter.is_some() || distance_parameter.is_some() {
                        return Err(DatabaseError::invalid_plan(
                            "predicato spatial unario con parametri inattesi",
                        ));
                    }
                }
                SpatialFunction::DWithin => {
                    if geometry_parameter.is_none() || distance_parameter.is_none() {
                        return Err(DatabaseError::invalid_plan(
                            "d_within richiede geometria e distanza",
                        ));
                    }
                }
                SpatialFunction::Intersects
                | SpatialFunction::Contains
                | SpatialFunction::Within
                | SpatialFunction::Covers
                | SpatialFunction::Touches
                | SpatialFunction::Crosses
                | SpatialFunction::Overlaps
                | SpatialFunction::Disjoint => {
                    if geometry_parameter.is_none() || distance_parameter.is_some() {
                        return Err(DatabaseError::invalid_plan(
                            "predicato spatial binario non valido",
                        ));
                    }
                }
                _ => {
                    return Err(DatabaseError::invalid_plan(
                        "funzione spatial non utilizzabile come filtro",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_object(
    object: &ObjectRef,
    limits: &plenora_database_core::limits::Limits,
) -> Result<()> {
    if let Some(catalog) = &object.catalog {
        validate_identifier(catalog, limits.max_identifier_bytes)?;
    }
    if let Some(schema) = &object.schema {
        validate_identifier(schema, limits.max_identifier_bytes)?;
    }
    validate_identifier(&object.object, limits.max_identifier_bytes)
}

fn validate_identifier(value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.len() > max_bytes {
        return Err(DatabaseError::invalid_plan(
            "identificatore vuoto, con NUL o oltre limite",
        ));
    }
    Ok(())
}

fn validate_parameter(value: &str) -> Result<()> {
    if value.is_empty() || value.contains('\0') || value.len() > 256 {
        return Err(DatabaseError::invalid_plan("nome parametro non valido"));
    }
    Ok(())
}

fn validate_write_capability(mode: WriteMode, capabilities: &ProviderCapabilities) -> Result<()> {
    let supported = match mode {
        WriteMode::Create => capabilities.writes.create,
        WriteMode::Append | WriteMode::TruncateInsert => capabilities.writes.append,
        WriteMode::Replace => capabilities.writes.replace,
        WriteMode::Update => capabilities.writes.update,
        WriteMode::Upsert => capabilities.writes.upsert,
        WriteMode::DeleteByKeys => capabilities.writes.delete_by_keys,
    };
    if supported {
        Ok(())
    } else {
        Err(DatabaseError::unsupported(
            capabilities.provider,
            plenora_database_core::ErrorPhase::Prepare,
            "modalità write non pubblicizzata dal provider",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_postgres_contract_example() {
        let bytes = include_bytes!("../../../contracts/v1/examples/plan-postgres-read.json");
        let validated = parse_and_validate(bytes).expect("valid plan");
        assert_eq!(validated.fingerprint().len(), 64);
        assert_eq!(validated.plan().provider, ProviderKind::Postgres);
    }

    #[test]
    fn fingerprint_is_stable() {
        let bytes = include_bytes!("../../../contracts/v1/examples/plan-arcgis-write.json");
        let first = parse_and_validate(bytes).expect("first");
        let second = parse_and_validate(bytes).expect("second");
        assert_eq!(first.fingerprint(), second.fingerprint());
    }

    #[test]
    fn rejects_inline_dsn() {
        let bytes = br#"{
          "schema_version": 1,
          "connection_ref": "postgres://user:password@host/db",
          "provider": "postgres",
          "operation": {"id": "database.test_connection"}
        }"#;
        let error = parse_and_validate(bytes).expect_err("inline DSN");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
    }
}
