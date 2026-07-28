use crate::error::public_error;
use crate::types::{ColumnKind, ColumnSpec};
use plenora_database_core::plan::{
    ComparisonOperator, FilterExpression, ReadOperation, SortDirection,
};
use plenora_database_core::query::{QueryExpression, QueryOperation};
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    Dialect, DialectCapabilities, Expression, Identifier, ObjectName, RenderedSql, Renderer,
};
use std::collections::{BTreeSet, HashMap};

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

fn select_columns(available: &[ColumnSpec], projection: &[String]) -> Result<Vec<ColumnSpec>> {
    if projection.is_empty() {
        return Ok(available.to_vec());
    }
    let by_name = available
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();
    projection
        .iter()
        .map(|name| {
            by_name
                .get(name.as_str())
                .map(|column| (*column).clone())
                .ok_or_else(|| {
                    public_error(
                        ErrorCategory::NotFound,
                        ErrorPhase::Prepare,
                        false,
                        "colonna PostgreSQL richiesta non trovata",
                    )
                })
        })
        .collect()
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
        renderer.quote_object(&source)
    );
    let mut bind_names = Vec::new();
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
            let quoted = renderer.quote_identifier(&Identifier::new(order.field.clone())?);
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
    Ok((sql, bind_names))
}

const fn postgres_renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
}

fn convert_filter(expression: &FilterExpression) -> Result<Expression> {
    match expression {
        FilterExpression::And { args } => Ok(Expression::And(
            args.iter()
                .map(convert_filter)
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Or { args } => Ok(Expression::Or(
            args.iter()
                .map(convert_filter)
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Eq { field, parameter } => {
            comparison(field, ComparisonOperator::Eq, parameter)
        }
        FilterExpression::Ne { field, parameter } => {
            comparison(field, ComparisonOperator::Ne, parameter)
        }
        FilterExpression::Lt { field, parameter } => {
            comparison(field, ComparisonOperator::Lt, parameter)
        }
        FilterExpression::Lte { field, parameter } => {
            comparison(field, ComparisonOperator::Lte, parameter)
        }
        FilterExpression::Gt { field, parameter } => {
            comparison(field, ComparisonOperator::Gt, parameter)
        }
        FilterExpression::Gte { field, parameter } => {
            comparison(field, ComparisonOperator::Gte, parameter)
        }
        FilterExpression::IsNull { field } => {
            Ok(Expression::IsNull(Identifier::new(field.clone())?))
        }
        FilterExpression::IsNotNull { field } => {
            Ok(Expression::IsNotNull(Identifier::new(field.clone())?))
        }
        FilterExpression::In { field, parameters } => Ok(Expression::In {
            field: Identifier::new(field.clone())?,
            parameters: parameters.clone(),
        }),
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => Ok(Expression::Between {
            field: Identifier::new(field.clone())?,
            lower_parameter: lower_parameter.clone(),
            upper_parameter: upper_parameter.clone(),
        }),
        FilterExpression::Like {
            field,
            parameter,
            case_insensitive,
        } => Ok(Expression::Like {
            field: Identifier::new(field.clone())?,
            parameter: parameter.clone(),
            case_insensitive: *case_insensitive,
        }),
        FilterExpression::Spatial {
            function,
            field,
            geometry_parameter,
            distance_parameter,
        } => Ok(Expression::SpatialPredicate {
            function: *function,
            field: Identifier::new(field.clone())?,
            geometry_parameter: geometry_parameter.clone(),
            distance_parameter: distance_parameter.clone(),
        }),
    }
}

fn comparison(field: &str, operator: ComparisonOperator, parameter: &str) -> Result<Expression> {
    Ok(Expression::Compare {
        field: Identifier::new(field.to_owned())?,
        operator,
        parameter: parameter.to_owned(),
    })
}

fn ensure_filter_columns(expression: &FilterExpression, columns: &[ColumnSpec]) -> Result<()> {
    fn visit(expression: &FilterExpression, available: &BTreeSet<&str>) -> bool {
        match expression {
            FilterExpression::And { args } | FilterExpression::Or { args } => {
                args.iter().all(|arg| visit(arg, available))
            }
            FilterExpression::Eq { field, .. }
            | FilterExpression::Ne { field, .. }
            | FilterExpression::Lt { field, .. }
            | FilterExpression::Lte { field, .. }
            | FilterExpression::Gt { field, .. }
            | FilterExpression::Gte { field, .. }
            | FilterExpression::IsNull { field }
            | FilterExpression::IsNotNull { field }
            | FilterExpression::In { field, .. }
            | FilterExpression::Between { field, .. }
            | FilterExpression::Like { field, .. }
            | FilterExpression::Spatial { field, .. } => available.contains(field.as_str()),
        }
    }
    let available = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if visit(expression, &available) {
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
