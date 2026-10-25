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
        "oracle" => Some((Dialect::Oracle, ProviderKind::Oracle)),
        _ => None,
    }
}

/// Le colonne che il mapping ORM o la reflection dichiarano `Geometry`, come
/// (schema, tabella, colonna) della tabella di base.
type DeclaredGeometry = Vec<(Option<String>, String, String)>;

/// La regola degli operandi geometrici con i tipi che il chiamante conosce
/// (`spatial_operand::ScalarOperands::judge`): lo SDK compila senza
/// catalogo, e una colonna dichiarata `Geometry` sotto un operatore non
/// spatial si rifiuta come la rifiuterebbe un provider sul catalogo. Senza
/// dichiarazioni la colonna non si giudica: e il limite dichiarato in
/// `spatial_operand`.
fn judge_declared_geometry(
    provider: ProviderKind,
    operands: &plenora_database_core::spatial_operand::ScalarOperands,
    declared: &DeclaredGeometry,
) -> PyResult<()> {
    operands
        .judge(
            provider,
            |column| {
                declared.iter().any(|(schema, object, name)| {
                    *schema == column.schema && *object == column.object && *name == column.column
                })
            },
            |field| declared.iter().any(|(_, _, name)| name == field),
        )
        .map_err(crate::errors::to_py_err)
}

/// Compila uno statement senza ricevere o restituire valori applicativi.
///
/// `geometry_columns` sono le colonne che il mapping ORM o la reflection
/// dichiarano `Geometry`: sotto un operatore non spatial si rifiutano.
///
/// # Errors
///
/// Rifiuta provider, JSON o IR non validi con messaggi privi del contenuto
/// ricevuto. Le differenze di dialetto restano nel renderer canonico Rust.
#[pyfunction]
#[pyo3(signature = (ast_json, provider_name, geometry_columns = None))]
pub fn compile_relational_query(
    ast_json: &str,
    provider_name: &str,
    geometry_columns: Option<DeclaredGeometry>,
) -> PyResult<(String, Vec<String>)> {
    let (dialect, provider) = provider(provider_name)
        .ok_or_else(|| PyValueError::new_err("provider relazionale non supportato"))?;
    let query: QueryOperation = serde_json::from_str(ast_json)
        .map_err(|_| PyValueError::new_err("IR relazionale non valido"))?;
    let rendered = Renderer::new(
        dialect,
        DialectCapabilities {
            // L'ORM verifica ogni nome contro `capabilities.spatial.functions`
            // della sessione prima di arrivare qui. Questo interruttore apre
            // il lowering soltanto sui provider con I/O Geometry ORM
            // qualificato. La superficie Python mantiene poi chiuse per
            // provider semantiche, dimensioni e tipi non attraversati dai
            // rispettivi gate live.
            spatial_intersects: matches!(
                provider,
                ProviderKind::Postgres
                    | ProviderKind::Mysql
                    | ProviderKind::Mariadb
                    | ProviderKind::Sqlserver
                    | ProviderKind::Db2
                    | ProviderKind::Oracle
            ),
        },
    )
    // GEO-001: `MySQL` scambia il WKB con la longitudine per prima (default
    // del dialetto); `MariaDB`, che condivide il dialetto, non ha l'opzione.
    .with_wkb_axis_option(match provider {
        ProviderKind::Mariadb => None,
        _ if dialect == Dialect::Mysql => Some(plenora_database_sql::MYSQL_WKB_AXIS_OPTION),
        _ => None,
    })
    // `MariaDB` parla il dialetto `MySQL` ma non confronta gli SRID: la
    // regola delle sonde lo deve sapere. Nessun catalogo e stato letto qui,
    // quindi le sonde passano soltanto dove il server ha una guardia misurata
    // (`spatial_probe::server_guard`): PostgreSQL con la guardia sul typmod,
    // MySQL.
    .with_provider(provider)
    .render_query(&query)
    .map_err(crate::errors::to_py_err)?;
    let declared = geometry_columns.unwrap_or_default();
    if !declared.is_empty() {
        let operands = plenora_database_core::spatial_operand::scalar_operands(provider, &query)
            .map_err(crate::errors::to_py_err)?;
        judge_declared_geometry(provider, &operands, &declared)?;
    }
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
#[pyo3(signature = (ast_json, provider_name, geometry_columns = None))]
pub fn compile_relational_mutation(
    ast_json: &str,
    provider_name: &str,
    geometry_columns: Option<DeclaredGeometry>,
) -> PyResult<(String, Vec<String>, bool)> {
    let (_, provider) = provider(provider_name)
        .ok_or_else(|| PyValueError::new_err("provider relazionale non supportato"))?;
    let mutation: MutationOperation = serde_json::from_str(ast_json)
        .map_err(|_| PyValueError::new_err("mutazione relazionale non valida"))?;
    let declared = geometry_columns.unwrap_or_default();
    let filtered = match &mutation {
        MutationOperation::Update(update) => update
            .filter
            .as_ref()
            .map(|filter| (&update.target, filter)),
        MutationOperation::Delete(delete) => delete
            .filter
            .as_ref()
            .map(|filter| (&delete.target, filter)),
        MutationOperation::Insert(_) | MutationOperation::Upsert(_) => None,
    };
    if let (Some((target, filter)), false) = (filtered, declared.is_empty()) {
        let operands = plenora_database_core::spatial_operand::scalar_operands_of_predicate(
            provider, target, filter,
        )
        .map_err(crate::errors::to_py_err)?;
        judge_declared_geometry(provider, &operands, &declared)?;
    }
    let lowered = plenora_database_core::compile_relational_mutation(provider, &mutation)
        .map_err(crate::errors::to_py_err)?;
    Ok((lowered.sql, lowered.bind_names, lowered.returns_rows))
}
