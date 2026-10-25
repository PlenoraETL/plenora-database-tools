use crate::error::public_error;
use crate::types::{ColumnKind, ColumnSpec};
use plenora_database_core::plan::{FilterExpression, ReadOperation, SortDirection};
use plenora_database_core::relational::{QueryExpression, QueryOperation};
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    lower_filter, select_columns_by_name, Dialect, DialectCapabilities, Expression, FilterLowering,
    Identifier, ObjectName, RenderedSql, Renderer,
};
use std::collections::BTreeSet;

pub struct PostgresReadPlan {
    pub sql: String,
    pub bind_names: Vec<String>,
    pub columns: Vec<ColumnSpec>,
}

pub fn plan_read(
    operation: &ReadOperation,
    available_columns: &[ColumnSpec],
) -> Result<PostgresReadPlan> {
    let columns = select_columns(available_columns, &operation.projection)?;
    let (sql, bind_names) = build_read_sql(operation, &columns, available_columns)?;
    Ok(PostgresReadPlan {
        sql,
        bind_names,
        columns,
    })
}

pub fn render_query(operation: &QueryOperation) -> Result<RenderedSql> {
    postgres_renderer().render_query(operation)
}

pub fn mark_query_spatial_columns(operation: &QueryOperation, columns: &mut [ColumnSpec]) {
    for (column, projection) in columns.iter_mut().zip(&operation.projection) {
        if matches!(
            projection.expression,
            QueryExpression::Spatial { function, .. } if function.returns_geometry()
        ) {
            column.kind = ColumnKind::Geometry;
            "geometry".clone_into(&mut column.native_type);
            column.spatial_type = Some("Geometry".to_owned());
        }
    }
}

/// La colonna da cui una geometria calcolata eredita il frame: una colonna,
/// o una funzione con regola `preserves` il cui unico argomento geometrico
/// eredita a sua volta (`Centroid(Centroid(geom))`). La profondita e quella
/// dell'espressione, gia limitata dalla validazione del piano.
fn preserved_source(
    expression: &QueryExpression,
) -> Option<&plenora_database_core::relational::ColumnRef> {
    use plenora_database_core::spatial_catalog::CrsRule;
    match expression {
        QueryExpression::Column { column } => Some(column),
        QueryExpression::Spatial {
            function,
            arguments,
        } if function.returns_geometry() && function.crs_rule() == Some(CrsRule::Preserves) => {
            let geometry_arguments = arguments
                .iter()
                .enumerate()
                .filter(|(index, _)| function.takes_geometry_at(*index))
                .map(|(_, argument)| argument)
                .collect::<Vec<_>>();
            match geometry_arguments.as_slice() {
                [argument] => preserved_source(argument),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Una geometria calcolata dalla query che eredita il CRS di una colonna
/// sorgente: la regola della funzione e `preserves` e l'unico argomento
/// geometrico e quella colonna.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InheritedGeometry {
    /// La posizione nella lista di selezione.
    pub position: usize,
    pub schema: Option<String>,
    pub object: String,
    pub column: String,
}

/// Le geometrie calcolate il cui SRID si legge dal catalogo della colonna
/// d'ingresso (GEO-009, regola del produttore).
///
/// Il risultato di una funzione spatial non ha typmod, e senza uno SRID
/// dichiarato il campo si pubblicherebbe `missing` con valori EWKB che lo
/// portano. La regola della funzione dice se il risultato sta nel frame
/// dell'ingresso; il catalogo dice quale e; la lettura confronta poi lo SRID
/// di ogni valore con quello dichiarato, e un valore senza SRID o con un
/// altro si rifiuta: una dichiarazione che passa e vera anche se la
/// risoluzione avesse sbagliato tabella. In un'unione vale il primo ramo, e
/// gli altri li verifica la stessa lettura. Cio che non si risolve cosi (piu
/// argomenti geometrici, un'espressione annidata, una relazione ambigua, una
/// regola diversa) resta `missing`, e un valore con SRID si rifiuta in
/// lettura.
#[must_use]
pub fn inherited_geometries(operation: &QueryOperation) -> Vec<InheritedGeometry> {
    let sources = operation
        .source
        .iter()
        .chain(
            operation
                .joins
                .iter()
                .filter_map(|join| join.source.as_ref()),
        )
        .collect::<Vec<_>>();
    let derived = operation.derived_source.is_some()
        || operation
            .joins
            .iter()
            .any(|join| join.derived_source.is_some())
        || !operation.common_table_expressions.is_empty();
    let mut inherited = Vec::new();
    for (position, projection) in operation.projection.iter().enumerate() {
        if !matches!(projection.expression, QueryExpression::Spatial { .. }) {
            continue;
        }
        let Some(column) = preserved_source(&projection.expression) else {
            continue;
        };
        // Una sorgente derivata, una CTE o un'altra query in unione possono
        // ridefinire il nome: non si indovina quale colonna sia.
        if derived {
            continue;
        }
        let source = match &column.relation {
            Some(relation) => {
                let named = sources
                    .iter()
                    .filter(|source| {
                        source.alias.as_deref().map_or_else(
                            || source.object.object == *relation,
                            |alias| alias == relation,
                        )
                    })
                    .collect::<Vec<_>>();
                match named.as_slice() {
                    [source] => **source,
                    _ => continue,
                }
            }
            None => match sources.as_slice() {
                [source] => *source,
                _ => continue,
            },
        };
        inherited.push(InheritedGeometry {
            position,
            schema: source.object.schema.clone(),
            object: source.object.object.clone(),
            column: column.field.clone(),
        });
    }
    inherited
}

fn select_columns(available: &[ColumnSpec], projection: &[String]) -> Result<Vec<ColumnSpec>> {
    select_columns_by_name(
        available,
        projection,
        |column| column.name.as_str(),
        || {
            public_error(
                ErrorCategory::NotFound,
                ErrorPhase::Prepare,
                false,
                "colonna PostgreSQL richiesta non trovata",
            )
        },
    )
}

fn build_read_sql(
    operation: &ReadOperation,
    columns: &[ColumnSpec],
    available_columns: &[ColumnSpec],
) -> Result<(String, Vec<String>)> {
    let renderer = postgres_renderer();
    let source = ObjectName {
        // PostgreSQL non supporta nomi cross-database a tre componenti. Il
        // catalogo è verificato contro current_database prima del rendering.
        catalog: None,
        schema: operation
            .source
            .schema
            .as_ref()
            .map(|value| Identifier::new(value.clone()))
            .transpose()?,
        object: Identifier::new(operation.source.object.clone())?,
    };
    let projection = columns
        .iter()
        .map(|column| column.projection_sql(&renderer))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let mut sql = format!(
        "SELECT {projection} FROM {}",
        renderer.quote_object(&source)?
    );
    let mut bind_names = Vec::new();
    // Un campo geometrico sotto un operatore non spatial o in ORDER BY: la
    // stessa regola degli altri provider (`spatial_operand`).
    plenora_database_core::spatial_operand::ensure_scalar_filter_fields(
        plenora_database_core::plan::ProviderKind::Postgres,
        operation.filter.as_ref(),
        &operation.order_by,
        |field| {
            available_columns.iter().any(|column| {
                column.name == field
                    && matches!(column.kind, ColumnKind::Geometry | ColumnKind::Geography)
            })
        },
        // La regola F la giudica l'esecuzione, sul catalogo e con i valori.
        |_| None,
        &plenora_database_core::spatial_operand::ParameterFamilies::default(),
    )?;
    if let Some(filter) = &operation.filter {
        ensure_filter_columns(filter, available_columns)?;
        let rendered_filter = renderer.render_filter(&convert_filter(filter)?)?;
        sql.push_str(" WHERE ");
        sql.push_str(&rendered_filter.sql);
        bind_names.extend(rendered_filter.binds.into_iter().map(|bind| bind.name));
    }
    if !operation.order_by.is_empty() {
        let available = available_columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        let mut orders = Vec::with_capacity(operation.order_by.len());
        for order in &operation.order_by {
            if !available.contains(order.field.as_str()) {
                return Err(public_error(
                    ErrorCategory::NotFound,
                    ErrorPhase::Prepare,
                    false,
                    "colonna ORDER BY non presente nella projection",
                ));
            }
            let quoted = renderer.quote_identifier(&Identifier::new(order.field.clone())?)?;
            let direction = match order.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            orders.push(format!("{quoted} {direction}"));
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&orders.join(", "));
    }
    if let Some(limit) = operation.row_limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }
    // `PostgreSQL` accetta `OFFSET` da solo, senza tetto: non serve il
    // massimo del tipo che il dialetto `MySQL` pretende.
    if let Some(offset) = operation.row_offset {
        sql.push_str(" OFFSET ");
        sql.push_str(&offset.to_string());
    }
    Ok((sql, bind_names))
}

/// Il renderer del path read e query: ogni sonda che rende e gia stata
/// confrontata con lo SRID della colonna sul catalogo, prima dell'esecuzione
/// (`spatial_probe::verify_read_filter` nel read, `plan_query_probes` e
/// `verify_query_columns` nel query), e lo statement non parte senza.
const fn postgres_renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
    .with_spatial_probes_verified()
}

fn convert_filter(expression: &FilterExpression) -> Result<Expression> {
    lower_filter(
        expression,
        FilterLowering {
            provider: plenora_database_core::plan::ProviderKind::Postgres,
            case_insensitive_like: true,
            spatial: true,
        },
        |field| Identifier::new(field.to_owned()),
    )
}

fn ensure_filter_columns(expression: &FilterExpression, columns: &[ColumnSpec]) -> Result<()> {
    let available = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if expression.all_fields(&|field| available.contains(field)) {
        Ok(())
    } else {
        Err(public_error(
            ErrorCategory::NotFound,
            ErrorPhase::Prepare,
            false,
            "colonna filtro non presente nella projection",
        ))
    }
}

#[cfg(test)]
#[path = "query_plan_tests.rs"]
mod tests;
