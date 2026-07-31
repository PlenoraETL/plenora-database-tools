use crate::error::driver_error;
use crate::parameter::{bind_parameters, parameter_declarations};
use crate::types::{SqlServerColumnSpec, SqlServerReadPlan};
use crate::SqlServerSession;
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::{ObjectRef, ProviderKind};
use plenora_database_core::provider::{ParameterBag, ParameterValue};
use plenora_database_core::query::{
    ColumnRef, QueryExpression, QueryOperation, QuerySource, SpatialFunction,
};
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
   OR error_number IS NOT NULL
ORDER BY column_ordinal;
";

pub const VERIFIED_SPATIAL_FUNCTIONS: &[SpatialFunction] = &[
    SpatialFunction::GeometryType,
    SpatialFunction::Srid,
    SpatialFunction::NPoints,
    SpatialFunction::IsEmpty,
    SpatialFunction::IsValid,
    SpatialFunction::IsClosed,
    SpatialFunction::Intersects,
    SpatialFunction::Contains,
    SpatialFunction::Within,
    SpatialFunction::Disjoint,
    SpatialFunction::Equals,
    SpatialFunction::Distance,
    SpatialFunction::Area,
    SpatialFunction::Length,
    SpatialFunction::StartPoint,
    SpatialFunction::EndPoint,
    SpatialFunction::PointN,
    SpatialFunction::Buffer,
    SpatialFunction::Intersection,
    SpatialFunction::Difference,
    SpatialFunction::SymDifference,
    SpatialFunction::Union,
    SpatialFunction::ConvexHull,
];

const MAX_SPATIAL_OUTPUTS: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpatialOutputContract {
    pub projection_index: usize,
    pub semantics: SpatialSemantics,
    pub srid: Option<u32>,
    pub dimensions: Dimensions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpatialValidation {
    pub outputs: Vec<SpatialOutputContract>,
    pub source_tokens: Vec<SpatialSourceToken>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpatialSourceToken {
    pub object: ObjectRef,
    pub token: crate::SqlServerSchemaToken,
}

pub fn render_query(
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
) -> Result<RenderedSql> {
    validate_bound_spatial_arguments(operation, parameters)?;
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
    budget: &ResourceBudget,
    cancellation: &CancellationToken,
) -> Result<SpatialValidation> {
    if !operation_has_spatial(operation) {
        return Ok(SpatialValidation {
            outputs: Vec::new(),
            source_tokens: Vec::new(),
        });
    }
    let mut uses = Vec::new();
    collect_operation_spatial_uses(operation, &mut uses)?;
    if !operation.common_table_expressions.is_empty()
        || operation.derived_source.is_some()
        || !operation.set_operations.is_empty()
        || operation
            .joins
            .iter()
            .any(|join| join.derived_source.is_some() || join.lateral || join.source.is_none())
    {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "AST spatial SQL Server non supporta ancora CTE, derived, lateral o set operation",
        ));
    }
    if uses.is_empty() {
        return Err(query_error(
            ErrorCategory::Unsupported,
            "AST spatial SQL Server non risolto sulla query principale",
        ));
    }
    let primary_source = operation.source.as_ref().ok_or_else(|| {
        query_error(
            ErrorCategory::InvalidPlan,
            "AST spatial SQL Server senza source fisica",
        )
    })?;
    let mut query_sources = vec![primary_source];
    query_sources.extend(
        operation
            .joins
            .iter()
            .filter_map(|join| join.source.as_ref()),
    );
    let mut sources = Vec::with_capacity(query_sources.len());
    let mut source_relations = BTreeSet::new();
    for source in query_sources {
        let relation = source
            .alias
            .as_deref()
            .unwrap_or(&source.object.object)
            .to_owned();
        if !source_relations.insert(relation.clone()) {
            return Err(query_error(
                ErrorCategory::InvalidPlan,
                "alias sorgente SQL Server duplicato nella query spatial",
            ));
        }
        let schema = source.object.schema.as_deref().unwrap_or("dbo");
        let description =
            crate::catalog::describe_object(session, schema, &source.object.object, cancellation)
                .await?;
        sources.push(SpatialPhysicalSource {
            relation,
            source: source.clone(),
            description,
        });
    }
    let mut checked_uses = BTreeSet::new();
    for usage in uses {
        let use_identity = (
            usage.column.relation.clone(),
            usage.column.field.clone(),
            usage.argument.clone(),
        );
        if !checked_uses.insert(use_identity) {
            continue;
        }
        let receiver_source = resolve_spatial_source(&usage.column, &sources)?;
        let column = receiver_source
            .description
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
        match &usage.argument {
            SpatialArgument::None => {}
            SpatialArgument::PointIndex(name) => {
                if !matches!(parameters.get(name), Some(ParameterValue::I32(value)) if *value >= 1)
                {
                    return Err(query_error(
                        ErrorCategory::InvalidPlan,
                        "STPointN SQL Server richiede un indice int bindato maggiore o uguale a 1",
                    ));
                }
            }
            SpatialArgument::Distance(name) => {
                if !matches!(parameters.get(name), Some(ParameterValue::F64(value)) if value.is_finite())
                {
                    return Err(query_error(
                        ErrorCategory::InvalidPlan,
                        "STBuffer SQL Server richiede una distanza float finita bindata",
                    ));
                }
            }
            SpatialArgument::Geometry(name) => {
                let ParameterValue::Wkb {
                    srid: Some(expected_srid),
                    semantics,
                    ..
                } = parameters.get(name).ok_or_else(|| {
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
                if observe_spatial_srid(session, receiver_source, &usage.column.field, cancellation)
                    .await?
                    .is_some_and(|observed| u32::try_from(observed).ok() != Some(*expected_srid))
                {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "SRID parametro spatial diverso dalla colonna SQL Server",
                    ));
                }
            }
            SpatialArgument::GeometryColumn(other) => {
                let other_source =
                    resolve_spatial_source_parts(other.relation.as_deref(), &sources)?;
                let other_column = other_source
                    .description
                    .columns
                    .iter()
                    .find(|column| column.name == other.field)
                    .ok_or_else(|| {
                        query_error(
                            ErrorCategory::Schema,
                            "operando colonna spatial SQL Server assente dal catalogo",
                        )
                    })?;
                let other_semantics = match other_column.native_type.as_str() {
                    "geometry" => SpatialSemantics::Geometry,
                    "geography" => SpatialSemantics::Geography,
                    _ => {
                        return Err(query_error(
                            ErrorCategory::DataMapping,
                            "operando colonna SQL Server non spatial",
                        ));
                    }
                };
                if observed_semantics != other_semantics {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "semantica diversa fra colonne spatial SQL Server",
                    ));
                }
                let receiver_srid = observe_spatial_srid(
                    session,
                    receiver_source,
                    &usage.column.field,
                    cancellation,
                )
                .await?;
                let other_srid =
                    observe_spatial_srid(session, other_source, &other.field, cancellation).await?;
                if matches!((receiver_srid, other_srid), (Some(left), Some(right)) if left != right)
                {
                    return Err(query_error(
                        ErrorCategory::DataMapping,
                        "SRID diverso fra colonne spatial SQL Server",
                    ));
                }
            }
        }
    }
    let outputs = profile_spatial_outputs(
        session,
        operation,
        parameters,
        budget,
        &sources,
        cancellation,
    )
    .await?;
    Ok(SpatialValidation {
        outputs,
        source_tokens: sources
            .into_iter()
            .map(|source| SpatialSourceToken {
                object: source.source.object,
                token: source.description.token,
            })
            .collect(),
    })
}

struct SpatialPhysicalSource {
    relation: String,
    source: QuerySource,
    description: crate::catalog::SqlServerObjectDescription,
}

fn resolve_spatial_source<'a>(
    column: &ColumnRef,
    sources: &'a [SpatialPhysicalSource],
) -> Result<&'a SpatialPhysicalSource> {
    resolve_spatial_source_parts(column.relation.as_deref(), sources)
}

fn resolve_spatial_source_parts<'a>(
    relation: Option<&str>,
    sources: &'a [SpatialPhysicalSource],
) -> Result<&'a SpatialPhysicalSource> {
    if let Some(relation) = relation {
        return sources
            .iter()
            .find(|source| source.relation == relation)
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::Schema,
                    "relazione colonna spatial non risolta sulle source SQL Server",
                )
            });
    }
    if sources.len() == 1 {
        return Ok(&sources[0]);
    }
    Err(query_error(
        ErrorCategory::Schema,
        "colonna spatial non qualificata in query SQL Server con più source",
    ))
}

async fn observe_spatial_srid(
    session: &mut SqlServerSession,
    source: &SpatialPhysicalSource,
    column: &str,
    cancellation: &CancellationToken,
) -> Result<Option<i32>> {
    let renderer = sql_server_renderer();
    let schema = source.source.object.schema.as_deref().unwrap_or("dbo");
    let quoted_schema = renderer.quote_identifier(&plenora_database_sql::Identifier::new(schema)?);
    let quoted_object = renderer.quote_identifier(&plenora_database_sql::Identifier::new(
        source.source.object.object.clone(),
    )?);
    let quoted_column = renderer.quote_identifier(&plenora_database_sql::Identifier::new(column)?);
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
    if distinct > 1 {
        return Err(query_error(
            ErrorCategory::DataMapping,
            "colonna AST spatial SQL Server con SRID misti",
        ));
    }
    optional(&rows[0], 1, "SRID spatial")
}

#[derive(Debug, Clone, Copy)]
struct SpatialOutputCandidate {
    projection_index: usize,
    semantics: SpatialSemantics,
}

#[allow(clippy::too_many_lines)]
async fn profile_spatial_outputs(
    session: &mut SqlServerSession,
    operation: &QueryOperation,
    parameters: &ParameterBag,
    budget: &ResourceBudget,
    sources: &[SpatialPhysicalSource],
    cancellation: &CancellationToken,
) -> Result<Vec<SpatialOutputContract>> {
    let mut candidates = Vec::new();
    for (projection_index, projection) in operation.projection.iter().enumerate() {
        let QueryExpression::Spatial {
            function,
            arguments,
        } = &projection.expression
        else {
            continue;
        };
        if !function.returns_geometry() {
            continue;
        }
        if !matches!(
            function,
            SpatialFunction::StartPoint
                | SpatialFunction::EndPoint
                | SpatialFunction::PointN
                | SpatialFunction::Buffer
                | SpatialFunction::Intersection
                | SpatialFunction::Difference
                | SpatialFunction::SymDifference
                | SpatialFunction::Union
                | SpatialFunction::ConvexHull
        ) {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "output spatial SQL Server fuori dal sottoinsieme verificato",
            ));
        }
        let QueryExpression::Column { column } = &arguments[0] else {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "output spatial SQL Server richiede una colonna come ricevitore",
            ));
        };
        let source = resolve_spatial_source(column, sources)?;
        let source_column = source
            .description
            .columns
            .iter()
            .find(|candidate| candidate.name == column.field)
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::Schema,
                    "colonna output spatial SQL Server assente dal catalogo",
                )
            })?;
        let semantics = match source_column.native_type.as_str() {
            "geometry" => SpatialSemantics::Geometry,
            "geography" => SpatialSemantics::Geography,
            _ => {
                return Err(query_error(
                    ErrorCategory::DataMapping,
                    "output spatial applicato a colonna SQL Server non spatial",
                ));
            }
        };
        candidates.push(SpatialOutputCandidate {
            projection_index,
            semantics,
        });
    }
    if candidates.is_empty() {
        return Ok(Vec::new());
    }
    if candidates.len() > MAX_SPATIAL_OUTPUTS {
        return Err(DatabaseError::resource_limit(
            "troppi output spatial SQL Server da profilare",
        ));
    }

    let mut profile_operation = operation.clone();
    for (index, projection) in profile_operation.projection.iter_mut().enumerate() {
        projection.alias = Some(format!("_plenora_spatial_profile_{index}"));
    }
    if profile_operation.row_limit.is_none() && profile_operation.row_offset.is_none() {
        profile_operation.order_by.clear();
    }
    let rendered = sql_server_renderer()
        .with_sql_server_spatial_parameters(spatial_parameter_profiles(parameters, budget)?)
        .render_query_native_spatial(&profile_operation)?;
    let bind_names = rendered
        .binds
        .iter()
        .map(|bind| bind.name.clone())
        .collect::<Vec<_>>();
    let identifier_renderer = sql_server_renderer();
    let derived_alias = identifier_renderer.quote_identifier(
        &plenora_database_sql::Identifier::new("_plenora_spatial_result")?,
    );
    let mut contracts = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let field = identifier_renderer.quote_identifier(&plenora_database_sql::Identifier::new(
            format!("_plenora_spatial_profile_{}", candidate.projection_index),
        )?);
        let value = format!("{derived_alias}.{field}");
        let sql = format!(
            "SELECT \
             COUNT_BIG(DISTINCT CASE WHEN {value} IS NULL THEN NULL ELSE {value}.STSrid END), \
             MIN(CASE WHEN {value} IS NULL THEN NULL ELSE {value}.STSrid END), \
             COALESCE(SUM(CONVERT(bigint, CASE WHEN {value}.STGeometryType() = N'FullGlobe' \
             THEN 1 ELSE 0 END)), 0), \
             COUNT_BIG(DISTINCT CASE WHEN {value} IS NULL THEN NULL ELSE \
             CONVERT(int, {value}.HasZ) * 2 + CONVERT(int, {value}.HasM) END), \
             MIN(CASE WHEN {value} IS NULL THEN NULL ELSE \
             CONVERT(int, {value}.HasZ) * 2 + CONVERT(int, {value}.HasM) END) \
             FROM ({}) AS {derived_alias};",
            rendered.sql
        );
        let mut query = Query::new(sql);
        bind_parameters(&mut query, &bind_names, parameters)?;
        let mut results = session
            .execute_query(query, ErrorPhase::Prepare, cancellation)
            .await?;
        if results.len() != 1 || results.first().is_none_or(|rows| rows.len() != 1) {
            return Err(query_error(
                ErrorCategory::Protocol,
                "profilo output spatial SQL Server con cardinalita inattesa",
            ));
        }
        let row = results
            .pop()
            .and_then(|mut rows| rows.pop())
            .ok_or_else(|| {
                query_error(
                    ErrorCategory::Protocol,
                    "profilo output spatial SQL Server senza riga",
                )
            })?;
        let srid_count: i64 = required(&row, 0, "numero SRID output spatial")?;
        let srid: Option<i32> = optional(&row, 1, "SRID output spatial")?;
        let full_globe: i64 = required(&row, 2, "FullGlobe output spatial")?;
        let dimension_count: i64 = required(&row, 3, "numero profili dimensionali output spatial")?;
        let dimension_code: Option<i32> = optional(&row, 4, "profilo dimensionale output spatial")?;
        if srid_count > 1 {
            return Err(query_error(
                ErrorCategory::DataMapping,
                "output spatial SQL Server con SRID misti",
            ));
        }
        if full_globe > 0 {
            return Err(query_error(
                ErrorCategory::Unsupported,
                "output FullGlobe SQL Server non rappresentabile nel profilo WKB",
            ));
        }
        let srid = srid
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    query_error(
                        ErrorCategory::DataMapping,
                        "SRID output spatial SQL Server negativo",
                    )
                })
            })
            .transpose()?;
        let dimensions =
            crate::types::spatial_dimensions_from_profile(dimension_count, dimension_code)?;
        contracts.push(SpatialOutputContract {
            projection_index: candidate.projection_index,
            semantics: candidate.semantics,
            srid,
            dimensions,
        });
    }
    Ok(contracts)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum SpatialArgument {
    None,
    Geometry(String),
    GeometryColumn(SpatialColumnRef),
    PointIndex(String),
    Distance(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SpatialColumnRef {
    relation: Option<String>,
    field: String,
}

fn validate_bound_spatial_arguments(
    operation: &QueryOperation,
    parameters: &ParameterBag,
) -> Result<()> {
    let mut uses = Vec::new();
    collect_operation_spatial_uses(operation, &mut uses)?;
    for usage in uses {
        match usage.argument {
            SpatialArgument::PointIndex(name)
                if !matches!(
                    parameters.get(&name),
                    Some(ParameterValue::I32(value)) if *value >= 1
                ) =>
            {
                return Err(query_error(
                    ErrorCategory::InvalidPlan,
                    "STPointN SQL Server richiede un indice int bindato maggiore o uguale a 1",
                ));
            }
            SpatialArgument::Distance(name)
                if !matches!(
                    parameters.get(&name),
                    Some(ParameterValue::F64(value)) if value.is_finite()
                ) =>
            {
                return Err(query_error(
                    ErrorCategory::InvalidPlan,
                    "STBuffer SQL Server richiede una distanza float finita bindata",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Debug)]
struct SpatialUse {
    column: ColumnRef,
    argument: SpatialArgument,
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
            if sql_server_unary_spatial_function(*function) {
                if arguments.len() != 1 {
                    return Err(query_error(
                        ErrorCategory::Unsupported,
                        "overload unary spatial SQL Server fuori dal sottoinsieme verificato",
                    ));
                }
                uses.push(SpatialUse {
                    column: column.clone(),
                    argument: SpatialArgument::None,
                });
            } else if sql_server_binary_spatial_function(*function) {
                if arguments.len() != 2 {
                    return Err(query_error(
                        ErrorCategory::Unsupported,
                        "overload binary spatial SQL Server fuori dal sottoinsieme verificato",
                    ));
                }
                let argument = match &arguments[1] {
                    QueryExpression::Parameter { name } => SpatialArgument::Geometry(name.clone()),
                    QueryExpression::Column { column } => {
                        SpatialArgument::GeometryColumn(SpatialColumnRef {
                            relation: column.relation.clone(),
                            field: column.field.clone(),
                        })
                    }
                    _ => {
                        return Err(query_error(
                            ErrorCategory::Unsupported,
                            "AST spatial SQL Server richiede WKB bindato o una colonna",
                        ));
                    }
                };
                uses.push(SpatialUse {
                    column: column.clone(),
                    argument,
                });
            } else if matches!(function, SpatialFunction::PointN | SpatialFunction::Buffer) {
                if arguments.len() != 2 {
                    return Err(query_error(
                        ErrorCategory::Unsupported,
                        "overload numerico spatial SQL Server fuori dal sottoinsieme verificato",
                    ));
                }
                let QueryExpression::Parameter { name } = &arguments[1] else {
                    return Err(query_error(
                        ErrorCategory::Unsupported,
                        "AST spatial SQL Server richiede argomento numerico bindato",
                    ));
                };
                uses.push(SpatialUse {
                    column: column.clone(),
                    argument: if *function == SpatialFunction::PointN {
                        SpatialArgument::PointIndex(name.clone())
                    } else {
                        SpatialArgument::Distance(name.clone())
                    },
                });
            } else {
                return Err(query_error(
                    ErrorCategory::Unsupported,
                    "funzione spatial fuori dal sottoinsieme SQL Server verificato",
                ));
            }
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

const fn sql_server_unary_spatial_function(function: SpatialFunction) -> bool {
    matches!(
        function,
        SpatialFunction::GeometryType
            | SpatialFunction::Srid
            | SpatialFunction::NPoints
            | SpatialFunction::IsEmpty
            | SpatialFunction::IsValid
            | SpatialFunction::IsClosed
            | SpatialFunction::Area
            | SpatialFunction::Length
            | SpatialFunction::StartPoint
            | SpatialFunction::EndPoint
            | SpatialFunction::ConvexHull
    )
}

const fn sql_server_binary_spatial_function(function: SpatialFunction) -> bool {
    matches!(
        function,
        SpatialFunction::Intersects
            | SpatialFunction::Contains
            | SpatialFunction::Within
            | SpatialFunction::Disjoint
            | SpatialFunction::Equals
            | SpatialFunction::Distance
            | SpatialFunction::Intersection
            | SpatialFunction::Difference
            | SpatialFunction::SymDifference
            | SpatialFunction::Union
    )
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

    #[test]
    fn spatial_use_collection_accepts_only_verified_sql_server_signatures() {
        for function in VERIFIED_SPATIAL_FUNCTIONS {
            let mut arguments = vec![column("e", "shape")];
            if sql_server_binary_spatial_function(*function) {
                arguments.push(QueryExpression::Parameter {
                    name: "needle".to_owned(),
                });
            } else if *function == SpatialFunction::PointN {
                arguments.push(QueryExpression::Parameter {
                    name: "point_index".to_owned(),
                });
            } else if *function == SpatialFunction::Buffer {
                arguments.push(QueryExpression::Parameter {
                    name: "distance".to_owned(),
                });
            }
            let mut uses = Vec::new();
            collect_expression_spatial_uses(
                &QueryExpression::Spatial {
                    function: *function,
                    arguments,
                },
                &mut uses,
            )
            .expect("verified spatial signature");
            assert_eq!(uses.len(), 1);
            let expected = if sql_server_binary_spatial_function(*function) {
                SpatialArgument::Geometry("needle".to_owned())
            } else if *function == SpatialFunction::PointN {
                SpatialArgument::PointIndex("point_index".to_owned())
            } else if *function == SpatialFunction::Buffer {
                SpatialArgument::Distance("distance".to_owned())
            } else {
                SpatialArgument::None
            };
            assert_eq!(uses[0].argument, expected);
        }

        let mut column_uses = Vec::new();
        collect_expression_spatial_uses(
            &QueryExpression::Spatial {
                function: SpatialFunction::Distance,
                arguments: vec![column("left", "shape"), column("right", "shape")],
            },
            &mut column_uses,
        )
        .expect("verified spatial column signature");
        assert_eq!(
            column_uses[0].argument,
            SpatialArgument::GeometryColumn(SpatialColumnRef {
                relation: Some("right".to_owned()),
                field: "shape".to_owned(),
            })
        );

        for expression in [
            QueryExpression::Spatial {
                function: SpatialFunction::MakeValid,
                arguments: vec![column("e", "shape")],
            },
            QueryExpression::Spatial {
                function: SpatialFunction::IsValid,
                arguments: vec![
                    column("e", "shape"),
                    QueryExpression::Parameter {
                        name: "flags".to_owned(),
                    },
                ],
            },
        ] {
            assert_eq!(
                collect_expression_spatial_uses(&expression, &mut Vec::new())
                    .expect_err("unverified spatial signature")
                    .category,
                ErrorCategory::Unsupported
            );
        }
    }

    #[test]
    fn numeric_spatial_arguments_fail_before_database_io() {
        let budget =
            ResourceBudget::new(plenora_database_core::ResourceLimits::default()).expect("budget");
        for (function, value) in [
            (SpatialFunction::PointN, ParameterValue::I32(0)),
            (SpatialFunction::Buffer, ParameterValue::F64(f64::NAN)),
        ] {
            let mut query = base_query();
            query.projection[0] = QueryProjection {
                expression: QueryExpression::Spatial {
                    function,
                    arguments: vec![
                        column("e", "shape"),
                        QueryExpression::Parameter {
                            name: "value".to_owned(),
                        },
                    ],
                },
                alias: Some("result".to_owned()),
            };
            let parameters = ParameterBag::new(BTreeMap::from([("value".to_owned(), value)]));
            assert_eq!(
                render_query(&query, &parameters, &budget)
                    .expect_err("invalid numeric spatial argument")
                    .category,
                ErrorCategory::InvalidPlan
            );
        }
    }
}
