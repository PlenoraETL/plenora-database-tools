//! Identita del tipo del target nella transazione della scrittura.
//!
//! I parametri inferiti e COPY devono usare lo stesso OID del catalogo.
//! I predicati conservano anche typmod e collation; non trasformano la
//! colonna nel tipo dell'opclass. La verifica precede l'invio dei valori.

use super::plan::WriteColumnPlan;
use arrow_schema::SchemaRef;
use plenora_database_core::plan::{ObjectRef, ProviderKind};
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use tokio_postgres::{types::Type, GenericClient};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct TargetType {
    pub(super) oid: u32,
    pub(super) declaration: String,
    pub(super) collation: Option<String>,
}

impl TargetType {
    pub(super) fn parameter(&self, ordinal: usize) -> String {
        let value = format!("${ordinal}::{}", self.declaration);
        match &self.collation {
            Some(collation) => format!("({value} COLLATE {collation})"),
            None => value,
        }
    }

    pub(super) fn verify(&self, actual: &Type) -> Result<()> {
        (self.oid == actual.oid())
            .then_some(())
            .ok_or_else(unsupported)
    }
}

pub(super) fn unsupported() -> DatabaseError {
    DatabaseError::unsupported(
        ProviderKind::Postgres,
        ErrorPhase::Prepare,
        "tipo intermedio PostgreSQL non identico al tipo della colonna",
    )
}

// Il chiamante usa il search_path chiuso: format_type qualifica i tipi
// dell'utente; quote_ident conserva i nomi delle collation senza interpolare
// dati di ingresso nello SQL.
const CATALOG_SQL: &str = "SELECT a.attname::pg_catalog.text, a.atttypid,
    pg_catalog.format_type(a.atttypid, a.atttypmod),
    CASE WHEN a.attcollation <> 0::pg_catalog.oid THEN
      pg_catalog.quote_ident(cn.nspname) || '.' || pg_catalog.quote_ident(co.collname)
    END
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation
    LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace
    WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text
      AND a.attnum > 0 AND NOT a.attisdropped";

pub(super) async fn assign<C: GenericClient + Sync>(
    client: &C,
    target: &ObjectRef,
    schema: &SchemaRef,
    plans: &mut [WriteColumnPlan],
) -> Result<()> {
    let rows = client
        .query(
            CATALOG_SQL,
            &[&crate::target_schema::schema_of(target)?, &target.object],
        )
        .await
        .map_err(|error| crate::error::classify_error(ErrorPhase::Prepare, &error))?;
    for (field, plan) in schema.fields().iter().zip(plans) {
        // Il dry run puo precedere ADD COLUMN. L'assenza resta esplicita;
        // la scrittura rilegge dopo il DDL e verify_parameters la rifiuta.
        plan.target_type = rows
            .iter()
            .find(|row| row.get::<_, String>(0) == *field.name())
            .map(|row| TargetType {
                oid: row.get(1),
                declaration: super::sql::qualify_builtin(&row.get::<_, String>(2)),
                collation: row.get(3),
            });
    }
    Ok(())
}

pub(super) fn verify_parameters(
    plans: &[WriteColumnPlan],
    indexes: &[usize],
    types: &[Type],
) -> Result<()> {
    if indexes.len() != types.len() {
        return Err(unsupported());
    }
    indexes.iter().zip(types).try_for_each(|(index, actual)| {
        let plan = plans.get(*index).ok_or_else(unsupported)?;
        plan.target_type
            .as_ref()
            .ok_or_else(unsupported)?
            .verify(actual)
    })
}

/// Describe dei parametri conserva l'OID del dominio; `RowDescription` di una
/// SELECT di colonne espone invece il tipo base. Non usare quest'ultimo per
/// provare l'identita del target COPY.
pub(super) fn copy_type_probe(plans: &[WriteColumnPlan], target: &str) -> Result<String> {
    let parameters = plans
        .iter()
        .enumerate()
        .map(|(index, plan)| {
            plan.target_type
                .as_ref()
                .map(|target| target.parameter(index + 1))
                .ok_or_else(unsupported)
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(format!("SELECT {parameters} FROM {target} LIMIT 0"))
}

#[cfg(test)]
#[path = "intermediate_tests.rs"]
mod tests;
