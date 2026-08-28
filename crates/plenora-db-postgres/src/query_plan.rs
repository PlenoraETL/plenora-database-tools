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

const fn postgres_renderer() -> Renderer {
    Renderer::new(
        Dialect::Postgres,
        DialectCapabilities {
            spatial_intersects: true,
        },
    )
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
