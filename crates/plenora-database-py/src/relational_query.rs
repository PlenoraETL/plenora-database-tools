//! Compilazione dell'IR relazionale canonico per la expression language Python.

use plenora_database_core::relational::QueryOperation;
use plenora_database_sql::{Dialect, DialectCapabilities, Renderer};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn provider(value: &str) -> Option<Dialect> {
    match value {
        "postgres" => Some(Dialect::Postgres),
        "mysql" | "mariadb" => Some(Dialect::Mysql),
        "sqlserver" => Some(Dialect::SqlServer),
        "db2" => Some(Dialect::Db2),
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
    let dialect = provider(provider_name)
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
