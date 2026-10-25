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
/// spatial si rifiuta come la rifiuterebbe un provider sul catalogo. Ogni
/// relazione del piano ha le colonne dichiarate per lei
/// (`RelationColumns::Declared`): le altre colonne si assumono scalari, e
/// un riferimento che non si risolve (alias, nome generato, `*`) in un piano
/// con geometrie si rifiuta. E un rifiuto anticipato, non il giudizio: lo
/// fa l'esecuzione sul catalogo del provider (`relational_plan`,
/// `facade::verify_operands`), anche senza dichiarazioni.
fn judge_declared_geometry(
    provider: ProviderKind,
    operands: &plenora_database_core::spatial_operand::ScalarOperands,
    declared: &DeclaredGeometry,
) -> PyResult<()> {
    use plenora_database_core::spatial_operand::{NameRule, OperandCatalog, RelationColumns};
    let rule = NameRule::of(provider);
    let mut catalog = OperandCatalog::new(provider);
    for (schema, object) in operands.relations() {
        let columns = declared
            .iter()
            .filter(|(declared_schema, declared_object, _)| {
                let same_schema = match (declared_schema.as_deref(), schema.as_deref()) {
                    (None, None) => true,
                    (Some(declared_schema), Some(schema)) => rule.matches(schema, declared_schema),
                    _ => false,
                };
                same_schema && rule.matches(declared_object, object)
            })
            .map(|(_, _, name)| name.clone())
            .collect();
        catalog.insert(
            schema.clone(),
            object.clone(),
            RelationColumns::Declared(columns),
        );
    }
    operands.judge(&catalog).map_err(crate::errors::to_py_err)
}

/// Il piano relazionale che accompagna lo SQL compilato dallo SDK fino
/// all'esecuzione: la transazione lo giudica sul catalogo del provider
/// (`facade::verify_operands`) prima di eseguire.
pub enum RelationalPlan {
    Query(Box<QueryOperation>),
    Mutation(Box<MutationOperation>),
}

impl RelationalPlan {
    /// Il piano come lo giudica il core.
    #[must_use]
    pub const fn as_plan(&self) -> plenora_database_core::spatial_operand::OperandPlan<'_> {
        use plenora_database_core::spatial_operand::OperandPlan;
        match self {
            Self::Query(query) => OperandPlan::Query(query),
            Self::Mutation(mutation) => OperandPlan::Mutation(mutation),
        }
    }
}

/// Il piano di `relational` (`("query" | "mutation", IR JSON)`), passato
/// dallo SDK a `execute` ed `execute_returning_rows` insieme allo SQL che ne
/// ha compilato. Il piano non si confronta con lo SQL: chi chiama il metodo
/// nativo con uno SQL diverso esegue SQL del chiamante, come senza piano; lo
/// SDK li compila dallo stesso statement (`expression._execute_statement`).
///
/// # Errors
///
/// Rifiuta un tipo o un IR non validi senza il contenuto ricevuto.
pub fn relational_plan(relational: Option<(String, String)>) -> PyResult<Option<RelationalPlan>> {
    let Some((kind, ast_json)) = relational else {
        return Ok(None);
    };
    match kind.as_str() {
        "query" => serde_json::from_str(&ast_json)
            .map(|query| Some(RelationalPlan::Query(Box::new(query))))
            .map_err(|_| PyValueError::new_err("IR relazionale non valido")),
        "mutation" => serde_json::from_str(&ast_json)
            .map(|mutation| Some(RelationalPlan::Mutation(Box::new(mutation))))
            .map_err(|_| PyValueError::new_err("mutazione relazionale non valida")),
        _ => Err(PyValueError::new_err(
            "tipo di piano relazionale non valido",
        )),
    }
}

/// `query` di una transazione con il giudizio del piano, se c'e.
///
/// # Errors
///
/// Quelli di `facade::query_judged`.
pub async fn query_rows(
    tx: &mut dyn plenora_database_core::transaction::TransactionScope,
    statement: &plenora_database_core::transaction::Statement,
    plan: Option<&RelationalPlan>,
    cancellation: &plenora_database_core::CancellationToken,
) -> plenora_database_core::Result<Vec<plenora_database_core::row::Row>> {
    let plan = plan.map(RelationalPlan::as_plan);
    plenora_database_core::facade::query_judged(tx, statement, plan.as_ref(), cancellation).await
}

/// `execute` di una transazione con il giudizio del piano, se c'e.
///
/// # Errors
///
/// Quelli di `facade::execute_judged`.
pub async fn execute(
    tx: &mut dyn plenora_database_core::transaction::TransactionScope,
    statement: &plenora_database_core::transaction::Statement,
    plan: Option<&RelationalPlan>,
    cancellation: &plenora_database_core::CancellationToken,
) -> plenora_database_core::Result<u64> {
    let plan = plan.map(RelationalPlan::as_plan);
    plenora_database_core::facade::execute_judged(tx, statement, plan.as_ref(), cancellation).await
}

/// Compila uno statement senza ricevere o restituire valori applicativi.
///
/// Non esegue e non legge il catalogo: il giudizio degli operandi geometrici
/// su ogni colonna lo fa l'esecuzione, con il catalogo del provider letto
/// nella transazione. Qui si rifiutano soltanto le forme che il piano dice da
/// se (regola N, funzioni spatial che rendono una geometria) e le colonne che
/// il mapping ORM o la reflection dichiarano `Geometry` (`geometry_columns`).
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
