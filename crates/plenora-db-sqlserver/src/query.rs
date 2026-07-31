use crate::error::driver_error;
use crate::parameter::parameter_declarations;
use crate::types::{SqlServerColumnSpec, SqlServerReadPlan};
use crate::SqlServerSession;
use plenora_database_core::plan::{ObjectRef, ProviderKind};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::query::{ColumnRef, QueryExpression, QueryOperation, SpatialFunction};
use plenora_database_core::resource::ResourceBudget;
use plenora_database_core::{
    CancellationToken, DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result,
    RetryDisposition,
};
use plenora_database_sql::{
    Dialect, DialectCapabilities, RenderedSql, Renderer, SqlServerSpatialParameter,
};
use std::collections::{BTreeMap, BTreeSet};
use tiberius::{FromSql, Query, Row};

const DESCRIBE_QUERY_SQL: &str = r"
SELECT
    column_ordinal,
    name,
    is_nullable,
    system_type_name,
    collation_name,
    user_type_name,
    error_number,
    error_message
FROM sys.dm_exec_describe_first_result_set(@P1, @P2, 0)
WHERE is_hidden = 0
ORDER BY column_ordinal;
";

pub fn render_query(
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
) -> Result<RenderedSql> {
    sql_server_renderer()
        .with_sql_server_spatial_parameters(spatial_parameter_profiles(parameters, budget)?)
        .render_query(operation)
}

fn spatial_parameter_profiles(
    parameters: &ParameterBag,
    budget: &ResourceBudget,
) -> Result<BTreeMap<String, SqlServerSpatialParameter>> {
    let max_components = budget.limits().geometry_components;
    let max_depth = budget.limits().nesting_depth;
    let mut profiles = BTreeMap::new();
    for (name, value) in parameters.iter() {
        let ParameterValue::Wkb {
            bytes,
            srid,
            dimensions,
            semantics,
        } = value
        else {
            continue;
        };
        if u64::try_from(bytes.len()).map_or(true, |length| length > budget.limits().cell_bytes) {
            return Err(DatabaseError::resource_limit(
                "parametro WKB SQL Server oltre il limite cella",
            ));
        }
        let inspection =
            plenora_database_core::ewkb::inspect_ewkb_detailed(bytes, max_components, max_depth)?;
        if inspection.root.srid.is_some() {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "parametro SQL Server richiede WKB senza SRID embedded",
            ));
        }
        let expected = match dimensions {
            plenora_database_core::geometry::Dimensions::Xy => "xy",
            plenora_database_core::geometry::Dimensions::Xyz => "xyz",
            plenora_database_core::geometry::Dimensions::Xym => "xym",
            plenora_database_core::geometry::Dimensions::Xyzm => "xyzm",
            plenora_database_core::geometry::Dimensions::Unknown => {
                return Err(query_error(
                    ErrorCategory::DataMapping,
                    "parametro WKB SQL Server richiede dimensioni risolte",
                ));
            }
        };
        if inspection.root.dimensions_label() != expected {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "dimensioni parametro WKB diverse dal contratto",
            ));
        }
        let srid = srid.ok_or_else(|| {
            query_error(
                ErrorCategory::DataMapping,
                "parametro WKB SQL Server richiede SRID risolto",
            )
        })?;
        profiles.insert(
            name.clone(),
            SqlServerSpatialParameter {
                semantics: *semantics,
                srid,
            },
        );
    }
    Ok(profiles)
}

/// Descrive l'output senza eseguire il piano utente.
///
/// `sys.dm_exec_describe_first_result_set` applica le regole di binding e type
/// inference del server anche a CTE, join, aggregati, window e set operation.
/// I metadati TDS reali vengono ricontrollati dal worker al momento
/// dell'esecuzione.
pub async fn describe_query(
    session: &mut SqlServerSession,
    rendered: RenderedSql,
    parameters: &ParameterBag,
    cancellation: &CancellationToken,
) -> Result<SqlServerReadPlan> {
    let bind_names = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let declarations = parameter_declarations(&bind_names, parameters)?;
    let mut query = Query::new(DESCRIBE_QUERY_SQL);
    query.bind(rendered.sql.clone());
    query.bind(declarations);
    let mut results = session
        .execute_query(query, ErrorPhase::Prepare, cancellation)
        .await?;
    if results.len() != 1 {
        return Err(query_error(
            ErrorCategory::Protocol,
            "descrizione QueryOperation SQL Server con numero result set inatteso",
        ));
    }
    let rows = results.pop().ok_or_else(|| {
        query_error(
            ErrorCategory::Protocol,
            "descrizione QueryOperation SQL Server senza result set",
        )
    })?;
    let columns = result_columns(&rows)?;
    SqlServerReadPlan::from_query_result(rendered.sql, bind_names, columns)
}

#[allow(clippy::too_many_lines)]
pub async fn validate_spatial_inputs(
    session: &mut SqlServerSession,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    cancellation: &CancellationToken,
) -> Result<()> {
    if !operation_has_spatial(operation) {
        return Ok(());
    }
    let mut uses = Vec::new();
    collect_operation_spatial_uses(operation, &mut uses)?;
    if !operation.common_table_expressions.is_empty()
        || operation.derived_source.is_some()
        || !operation.joins.is_empty()
        || !operation.set_operations.is_empty()
    {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "AST spatial SQL Server iniziale richiede una sola source fisica",
        ));
    }
    if uses.is_empty() {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "AST spatial SQL Server non risolto sulla query principale",
        ));
    }
    let source = operation.source.as_ref().ok_or_else(|| {
        query_error(
            ErrorCategory::InvalidPlan,
            "AST spatial SQL Server senza source fisica",
        )
    })?;
    let schema = source.object.schema.as_deref().unwrap_or("dbo");
    let description =
        crate::catalog::describe_object(session, schema, &source.object.object, cancellation)
            .await?;
    let renderer = sql_server_renderer();
    let quoted_schema = renderer.quote_identifier(&plenora_database_sql::Identifier::new(schema)?);
    let quoted_object = renderer.quote_identifier(&plenora_database_sql::Identifier::new(
        source.object.object.clone(),
    )?);
    for usage in uses {
        if let Some(relation) = &usage.column.relation {
            let expected = source.alias.as_deref().unwrap_or(&source.object.object);
            if relation != expected {
                return Err(query_error(
                    ErrorCategory::Schema,
                    "relazione colonna spatial non risolta sulla source SQL Server",
                ));
            }
        }
        let column = description
            .columns
            .iter()
            .find(|column| column.name == usage.column.field)
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::Schema,
                    "colonna spatial SQL Server assente dal catalogo",
                )
            })?;
        let observed_semantics = match column.native_type.as_str() {
            "geometry" => plenora_database_core::geometry::SpatialSemantics::Geometry,
            "geography" => plenora_database_core::geometry::SpatialSemantics::Geography,
            _ => {
                return Err(query_error(
                    ErrorCategory::DataMapping,
                    "funzione spatial applicata a colonna SQL Server non spatial",
                ));
            }
        };
        let Some(parameter_name) = usage.parameter else {
            continue;
        };
        let ParameterValue::Wkb {
            srid: Some(expected_srid),
            semantics,
            ..
        } = parameters.get(&parameter_name).ok_or_else(|| {
            query_error(
                ErrorCategory::InvalidPlan,
                "parametro spatial SQL Server mancante",
            )
        })?
        else {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "operando spatial SQL Server richiede un parametro WKB risolto",
            ));
        };
        if *semantics != observed_semantics {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "semantica parametro spatial diversa dalla colonna SQL Server",
            ));
        }
        let quoted_column = renderer.quote_identifier(&plenora_database_sql::Identifier::new(
            usage.column.field.clone(),
        )?);
        let sql = format!(
            "SELECT COUNT_BIG(DISTINCT {quoted_column}.STSrid), \
             MIN({quoted_column}.STSrid) FROM {quoted_schema}.{quoted_object} \
             WHERE {quoted_column} IS NOT NULL;"
        );
        let mut results = session
            .execute_query(Query::new(sql), ErrorPhase::Prepare, cancellation)
            .await?;
        let rows = results.pop().ok_or_else(|| {
            query_error(
                ErrorCategory::Protocol,
                "preflight SRID AST spatial senza result set",
            )
        })?;
        if !results.is_empty() || rows.len() != 1 {
            return Err(query_error(
                ErrorCategory::Protocol,
                "preflight SRID AST spatial con cardinalita inattesa",
            ));
        }
        let distinct: i64 = required(&rows[0], 0, "numero SRID spatial")?;
        let observed_srid: Option<i32> = optional(&rows[0], 1, "SRID spatial")?;
        if distinct > 1 {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "colonna AST spatial SQL Server con SRID misti",
            ));
        }
        if let Some(observed_srid) = observed_srid {
            if u32::try_from(observed_srid).ok() != Some(*expected_srid) {
                return Err(query_error(
                    ErrorCategory::DataMapping,
                    "SRID parametro spatial diverso dalla colonna SQL Server",
                ));
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct SpatialUse {
    column: ColumnRef,
    parameter: Option<String>,
}

fn collect_operation_spatial_uses(
    operation: &QueryOperation,
    uses: &mut Vec<SpatialUse>,
) -> Result<()> {
    for projection in &operation.projection {
        collect_expression_spatial_uses(&projection.expression, uses)?;
    }
    if let Some(filter) = &operation.filter {
        collect_expression_spatial_uses(filter, uses)?;
    }
    for expression in &operation.group_by {
        collect_expression_spatial_uses(expression, uses)?;
    }
    if let Some(having) = &operation.having {
        collect_expression_spatial_uses(having, uses)?;
    }
    for ordering in &operation.order_by {
        collect_expression_spatial_uses(&ordering.expression, uses)?;
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn collect_expression_spatial_uses(
    expression: &QueryExpression,
    uses: &mut Vec<SpatialUse>,
) -> Result<()> {
    match expression {
        QueryExpression::Spatial {
            function,
            arguments,
        } => {
            let QueryExpression::Column { column } = arguments.first().ok_or_else(|| {
                query_error(
                    ErrorCategory::InvalidPlan,
                    "AST spatial SQL Server senza ricevitore",
                )
            })?
            else {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "AST spatial SQL Server richiede una colonna come ricevitore",
                ));
            };
            if *function != SpatialFunction::Intersects {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "funzione spatial fuori dal sottoinsieme SQL Server verificato",
                ));
            }
            let QueryExpression::Parameter { name } = arguments.get(1).ok_or_else(|| {
                query_error(
                    ErrorCategory::InvalidPlan,
                    "AST spatial SQL Server senza secondo operando",
                )
            })?
            else {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "AST spatial SQL Server richiede WKB bindato come secondo operando",
                ));
            };
            uses.push(SpatialUse {
                column: column.clone(),
                parameter: Some(name.clone()),
            });
        }
        QueryExpression::Scalar { arguments, .. }
        | QueryExpression::And { arguments }
        | QueryExpression::Or { arguments } => {
            for argument in arguments {
                collect_expression_spatial_uses(argument, uses)?;
            }
        }
        QueryExpression::Compare { left, right, .. }
        | QueryExpression::SpatialOperator { left, right, .. } => {
            collect_expression_spatial_uses(left, uses)?;
            collect_expression_spatial_uses(right, uses)?;
        }
        QueryExpression::IsNull { expression, .. } => {
            collect_expression_spatial_uses(expression, uses)?;
        }
        QueryExpression::Window {
            arguments,
            partition_by,
            order_by,
            ..
        }
        | QueryExpression::SpatialWindow {
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            for argument in arguments.iter().chain(partition_by) {
                collect_expression_spatial_uses(argument, uses)?;
            }
            for ordering in order_by {
                collect_expression_spatial_uses(&ordering.expression, uses)?;
            }
        }
        QueryExpression::ScalarSubquery { query } | QueryExpression::Exists { query, .. } => {
            if operation_has_spatial(query) {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "AST spatial SQL Server non attraversa subquery correlate",
                ));
            }
        }
        QueryExpression::InSubquery {
            expression, query, ..
        } => {
            collect_expression_spatial_uses(expression, uses)?;
            if operation_has_spatial(query) {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "AST spatial SQL Server non attraversa subquery correlate",
                ));
            }
        }
        QueryExpression::Wildcard { .. }
        | QueryExpression::Column { .. }
        | QueryExpression::Parameter { .. } => {}
    }
    Ok(())
}

fn operation_has_spatial(operation: &QueryOperation) -> bool {
    operation
        .projection
        .iter()
        .any(|projection| expression_has_spatial(&projection.expression))
        || operation
            .filter
            .as_ref()
            .is_some_and(expression_has_spatial)
        || operation.group_by.iter().any(expression_has_spatial)
        || operation
            .having
            .as_ref()
            .is_some_and(expression_has_spatial)
        || operation
            .order_by
            .iter()
            .any(|ordering| expression_has_spatial(&ordering.expression))
        || operation
            .common_table_expressions
            .iter()
            .any(|cte| operation_has_spatial(&cte.query))
        || operation
            .derived_source
            .as_ref()
            .is_some_and(|derived| operation_has_spatial(&derived.query))
        || operation.joins.iter().any(|join| {
            join.on.as_ref().is_some_and(expression_has_spatial)
                || join
                    .derived_source
                    .as_ref()
                    .is_some_and(|derived| operation_has_spatial(&derived.query))
        })
        || operation
            .set_operations
            .iter()
            .any(|set| operation_has_spatial(&set.query))
}

fn expression_has_spatial(expression: &QueryExpression) -> bool {
    match expression {
        QueryExpression::Spatial { .. }
        | QueryExpression::SpatialOperator { .. }
        | QueryExpression::SpatialWindow { .. } => true,
        QueryExpression::Scalar { arguments, .. }
        | QueryExpression::And { arguments }
        | QueryExpression::Or { arguments } => arguments.iter().any(expression_has_spatial),
        QueryExpression::Compare { left, right, .. } => {
            expression_has_spatial(left) || expression_has_spatial(right)
        }
        QueryExpression::Window {
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            arguments.iter().any(expression_has_spatial)
                || partition_by.iter().any(expression_has_spatial)
                || order_by
                    .iter()
                    .any(|ordering| expression_has_spatial(&ordering.expression))
        }
        QueryExpression::ScalarSubquery { query } | QueryExpression::Exists { query, .. } => {
            operation_has_spatial(query)
        }
        QueryExpression::InSubquery {
            expression, query, ..
        } => expression_has_spatial(expression) || operation_has_spatial(query),
        QueryExpression::IsNull { expression, .. } => expression_has_spatial(expression),
        QueryExpression::Wildcard { .. }
        | QueryExpression::Column { .. }
        | QueryExpression::Parameter { .. } => false,
    }
}

/// Verifica le sorgenti a ogni profondità dopo che il renderer ha già
/// applicato i limiti strutturali comuni.
pub fn validate_query_sources(operation: &QueryOperation, database: &str) -> Result<()> {
    validate_operation_sources(operation, database)
}

fn validate_operation_sources(operation: &QueryOperation, database: &str) -> Result<()> {
    if let Some(source) = &operation.source {
        validate_source(&source.object, database)?;
    }
    if let Some(derived) = &operation.derived_source {
        validate_operation_sources(&derived.query, database)?;
    }
    for cte in &operation.common_table_expressions {
        validate_operation_sources(&cte.query, database)?;
    }
    for join in &operation.joins {
        if let Some(source) = &join.source {
            validate_source(&source.object, database)?;
        }
        if let Some(derived) = &join.derived_source {
            validate_operation_sources(&derived.query, database)?;
        }
        if let Some(on) = &join.on {
            validate_expression_sources(on, database)?;
        }
    }
    for projection in &operation.projection {
        validate_expression_sources(&projection.expression, database)?;
    }
    if let Some(filter) = &operation.filter {
        validate_expression_sources(filter, database)?;
    }
    for expression in &operation.group_by {
        validate_expression_sources(expression, database)?;
    }
    if let Some(having) = &operation.having {
        validate_expression_sources(having, database)?;
    }
    for ordering in &operation.order_by {
        validate_expression_sources(&ordering.expression, database)?;
    }
    for expression in &operation.distinct_on {
        validate_expression_sources(expression, database)?;
    }
    for set in &operation.set_operations {
        validate_operation_sources(&set.query, database)?;
    }
    Ok(())
}

fn validate_expression_sources(expression: &QueryExpression, database: &str) -> Result<()> {
    match expression {
        QueryExpression::Scalar { arguments, .. }
        | QueryExpression::Spatial { arguments, .. }
        | QueryExpression::And { arguments }
        | QueryExpression::Or { arguments } => {
            for argument in arguments {
                validate_expression_sources(argument, database)?;
            }
        }
        QueryExpression::SpatialOperator { left, right, .. }
        | QueryExpression::Compare { left, right, .. } => {
            validate_expression_sources(left, database)?;
            validate_expression_sources(right, database)?;
        }
        QueryExpression::Window {
            arguments,
            partition_by,
            order_by,
            ..
        }
        | QueryExpression::SpatialWindow {
            arguments,
            partition_by,
            order_by,
            ..
        } => {
            for argument in arguments {
                validate_expression_sources(argument, database)?;
            }
            for partition in partition_by {
                validate_expression_sources(partition, database)?;
            }
            for ordering in order_by {
                validate_expression_sources(&ordering.expression, database)?;
            }
        }
        QueryExpression::ScalarSubquery { query } | QueryExpression::Exists { query, .. } => {
            validate_operation_sources(query, database)?;
        }
        QueryExpression::InSubquery {
            expression, query, ..
        } => {
            validate_expression_sources(expression, database)?;
            validate_operation_sources(query, database)?;
        }
        QueryExpression::IsNull { expression, .. } => {
            validate_expression_sources(expression, database)?;
        }
        QueryExpression::Wildcard { .. }
        | QueryExpression::Column { .. }
        | QueryExpression::Parameter { .. } => {}
    }
    Ok(())
}

fn validate_source(source: &ObjectRef, database: &str) -> Result<()> {
    if source.layer_id.is_some() {
        return Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Prepare,
            "layer_id non appartiene al provider SQL Server",
        ));
    }
    if source
        .catalog
        .as_deref()
        .is_some_and(|catalog| catalog != database)
    {
        return Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            ErrorPhase::Prepare,
            "accesso cross-database SQL Server non supportato dal provider",
        ));
    }
    Ok(())
}

fn result_columns(rows: &[Row]) -> Result<Vec<SqlServerColumnSpec>> {
    if rows.is_empty() {
        return Err(query_error(
            ErrorCategory::Schema,
            "QueryOperation SQL Server non produce colonne",
        ));
    }
    let mut names = BTreeSet::new();
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            if let Some(number) = optional::<i32>(row, 6, "error_number")? {
                return Err(query_error(
                    ErrorCategory::Schema,
                    format!("descrizione QueryOperation SQL Server fallita (codice {number})"),
                ));
            }
            let ordinal = required::<i32>(row, 0, "column_ordinal")?;
            let expected = i32::try_from(index + 1).map_err(|_| {
                query_error(
                    ErrorCategory::ResourceLimit,
                    "numero colonne QueryOperation non rappresentabile",
                )
            })?;
            if ordinal != expected {
                return Err(query_error(
                    ErrorCategory::Protocol,
                    "ordinali output QueryOperation SQL Server non contigui",
                ));
            }
            let name = optional::<&str>(row, 1, "name")?
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    query_error(
                        ErrorCategory::Schema,
                        "projection calcolata SQL Server senza alias",
                    )
                })?
                .to_owned();
            if !names.insert(name.clone()) {
                return Err(query_error(
                    ErrorCategory::Schema,
                    "nomi colonna output QueryOperation SQL Server duplicati",
                ));
            }
            let nullable = required::<bool>(row, 2, "is_nullable")?;
            let user_type = optional::<&str>(row, 5, "user_type_name")?;
            if user_type.is_some_and(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "geometry" | "geography"
                )
            }) {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "output spatial QueryOperation SQL Server non ancora tipizzato",
                ));
            }
            let declaration = optional::<&str>(row, 3, "system_type_name")?
                .ok_or_else(|| {
                    query_error(
                        ErrorCategory::Unsupported,
                        "tipo output QueryOperation SQL Server non descrivibile",
                    )
                })?
                .to_owned();
            let collation = optional::<&str>(row, 4, "collation_name")?.map(ToOwned::to_owned);
            SqlServerColumnSpec::from_query_metadata(name, declaration, nullable, collation)
        })
        .collect()
}

fn required<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<T>
where
    T: FromSql<'a>,
{
    optional(row, index, field)?.ok_or_else(|| {
        query_error(
            ErrorCategory::DataMapping,
            format!("campo descrizione QueryOperation obbligatorio assente: {field}"),
        )
    })
}

fn optional<'a, T>(row: &'a Row, index: usize, field: &'static str) -> Result<Option<T>>
where
    T: FromSql<'a>,
{
    row.try_get(index).map_err(|error| {
        let mut public = driver_error(&error, ErrorPhase::Prepare, RemoteEffect::None);
        public.message =
            format!("tipo descrizione QueryOperation SQL Server incompatibile: {field}");
        public
    })
}

const fn sql_server_renderer() -> Renderer {
    Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}

fn query_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::plan::ObjectRef;
    use plenora_database_core::query::{
        ColumnRef, CommonTableExpression, QueryProjection, QuerySource, ScalarFunction,
    };

    fn source(object: &str, alias: &str) -> QuerySource {
        QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: Some("dbo".to_owned()),
                object: object.to_owned(),
                layer_id: None,
            },
            alias: Some(alias.to_owned()),
        }
    }

    fn column(relation: &str, field: &str) -> QueryExpression {
        QueryExpression::Column {
            column: ColumnRef {
                relation: Some(relation.to_owned()),
                field: field.to_owned(),
            },
        }
    }

    fn base_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: column("e", "id"),
                alias: Some("event_id".to_owned()),
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        }
    }

    #[test]
    fn rich_query_is_rendered_instead_of_lowered_to_table_read() {
        let mut query = base_query();
        query.common_table_expressions.push(CommonTableExpression {
            name: "filtered".to_owned(),
            recursive: false,
            query: Box::new(base_query()),
        });
        query.projection.push(QueryProjection {
            expression: QueryExpression::Scalar {
                function: ScalarFunction::Count,
                arguments: vec![column("e", "id")],
            },
            alias: Some("event_count".to_owned()),
        });
        let budget =
            ResourceBudget::new(plenora_database_core::ResourceLimits::default()).expect("budget");
        let rendered = render_query(&query, &ParameterBag::default(), &budget).expect("rendered");
        assert!(rendered.sql.starts_with("WITH [filtered] AS"));
        assert!(rendered
            .sql
            .contains("COUNT_BIG([e].[id]) AS [event_count]"));
    }

    #[test]
    fn nested_cross_database_source_fails_before_io() {
        let mut inner = base_query();
        inner.source.as_mut().expect("source").object.catalog = Some("other".to_owned());
        let mut query = base_query();
        query.projection[0].expression = QueryExpression::ScalarSubquery {
            query: Box::new(inner),
        };
        assert_eq!(
            validate_query_sources(&query, "dataflow_test")
                .expect_err("cross database")
                .category,
            ErrorCategory::Unsupported
        );
    }
}
