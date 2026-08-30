//! Compilazione dell'IR relazionale canonico per la expression language Python.

use plenora_database_core::plan::ProviderKind;
use plenora_database_core::relational::{MutationOperation, QueryOperation};
use plenora_database_sql::{Dialect, DialectCapabilities, Renderer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn provider(value: &str) -> Option<(Dialect, ProviderKind)> {
    match value {
        "postgres" => Some((Dialect::Postgres, ProviderKind::Postgres)),
        "mysql" => Some((Dialect::Mysql, ProviderKind::Mysql)),
        "mariadb" => Some((Dialect::Mysql, ProviderKind::Mariadb)),
        "sqlserver" => Some((Dialect::SqlServer, ProviderKind::Sqlserver)),
        "db2" => Some((Dialect::Db2, ProviderKind::Db2)),
        _ => None,
    }
}

/// Compila uno statement senza ricevere o restituire valori applicativi.
///
/// # Errors
///
/// Rifiuta provider, JSON o IR non validi con messaggi privi del contenuto
/// ricevuto. Le differenze di dialetto restano nel renderer canonico Rust.
#[pyfunction]
pub fn compile_relational_query(
    ast_json: &str,
    provider_name: &str,
) -> PyResult<(String, Vec<String>)> {
    let (dialect, _) = provider(provider_name)
        .ok_or_else(|| PyValueError::new_err("provider relazionale non supportato"))?;
    let query: QueryOperation = serde_json::from_str(ast_json)
        .map_err(|_| PyValueError::new_err("IR relazionale non valido"))?;
    let rendered = Renderer::new(
        dialect,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
    .render_query(&query)
    .map_err(crate::errors::to_py_err)?;
    Ok((
        rendered.sql,
        rendered.binds.into_iter().map(|bind| bind.name).collect(),
    ))
}

/// Compila una mutazione canonica senza ricevere valori applicativi.
///
/// # Errors
///
/// Rifiuta provider, JSON o forme DML non qualificate senza includere il
/// payload ricevuto nel messaggio pubblico.
#[pyfunction]
pub fn compile_relational_mutation(
    ast_json: &str,
    provider_name: &str,
) -> PyResult<(String, Vec<String>, bool)> {
    let (_, provider) = provider(provider_name)
        .ok_or_else(|| PyValueError::new_err("provider relazionale non supportato"))?;
    let mutation: MutationOperation = serde_json::from_str(ast_json)
        .map_err(|_| PyValueError::new_err("mutazione relazionale non valida"))?;
    let lowered = plenora_database_core::compile_relational_mutation(provider, &mutation)
        .map_err(crate::errors::to_py_err)?;
    Ok((lowered.sql, lowered.bind_names, lowered.returns_rows))
}
