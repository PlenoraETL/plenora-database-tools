use plenora_database_core::plan::{
    ComparisonOperator, FilterExpression, OrderBy, ProviderKind, ReadOperation,
};
use plenora_database_core::query::{
    ColumnRef, QueryExpression, QueryOperation, QueryOrdering, QuerySource,
};
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use plenora_database_sql::{Dialect, DialectCapabilities, Renderer};

/// Riduce il profilo relazionale SQL Server già dimostrabile al piano read
/// tabellare tipizzato. Il renderer T-SQL valida prima l'intero albero con i
/// limiti strutturali comuni; le forme che richiedono inferenza di schema
/// espressione-specifica falliscono poi esplicitamente.
pub fn lower_query(operation: &QueryOperation) -> Result<ReadOperation> {
    sql_server_renderer().render_query(operation)?;
    if !operation.common_table_expressions.is_empty()
        || operation.derived_source.is_some()
        || !operation.joins.is_empty()
        || !operation.group_by.is_empty()
        || operation.having.is_some()
        || operation.distinct
        || !operation.distinct_on.is_empty()
        || !operation.set_operations.is_empty()
        || operation.row_offset.is_some()
        || operation.locking.is_some()
    {
        return Err(unsupported(
            "profilo QueryOperation SQL Server iniziale limitato a una singola source",
        ));
    }
    let source = operation
        .source
        .as_ref()
        .ok_or_else(|| unsupported("QueryOperation SQL Server senza source tabellare"))?;
    let projection = lower_projection(operation, source)?;
    let filter = operation
        .filter
        .as_ref()
        .map(|expression| lower_filter(expression, source))
        .transpose()?;
    let order_by = operation
        .order_by
        .iter()
        .map(|ordering| lower_ordering(ordering, source))
        .collect::<Result<Vec<_>>>()?;
    Ok(ReadOperation {
        source: source.object.clone(),
        projection,
        order_by,
        row_limit: operation.row_limit,
        filter,
    })
}

fn lower_projection(operation: &QueryOperation, source: &QuerySource) -> Result<Vec<String>> {
    if operation.projection.len() == 1 {
        if let QueryExpression::Wildcard { relation } = &operation.projection[0].expression {
            validate_relation(relation.as_deref(), source)?;
            if operation.projection[0].alias.is_some() {
                return Err(unsupported("wildcard SQL Server con alias"));
            }
            return Ok(Vec::new());
        }
    }
    operation
        .projection
        .iter()
        .map(|projection| {
            if projection.alias.is_some() {
                return Err(unsupported(
                    "alias di projection SQL Server richiede schema output derivato",
                ));
            }
            let QueryExpression::Column { column } = &projection.expression else {
                return Err(unsupported(
                    "projection SQL Server non-colonna richiede schema output derivato",
                ));
            };
            validate_column(column, source)?;
            Ok(column.field.clone())
        })
        .collect()
}

fn lower_filter(expression: &QueryExpression, source: &QuerySource) -> Result<FilterExpression> {
    match expression {
        QueryExpression::And { arguments } => Ok(FilterExpression::And {
            args: arguments
                .iter()
                .map(|argument| lower_filter(argument, source))
                .collect::<Result<Vec<_>>>()?,
        }),
        QueryExpression::Or { arguments } => Ok(FilterExpression::Or {
            args: arguments
                .iter()
                .map(|argument| lower_filter(argument, source))
                .collect::<Result<Vec<_>>>()?,
        }),
        QueryExpression::Compare {
            left,
            operator,
            right,
        } => {
            if let (QueryExpression::Column { column }, QueryExpression::Parameter { name }) =
                (left.as_ref(), right.as_ref())
            {
                validate_column(column, source)?;
                return Ok(comparison(column.field.clone(), *operator, name.clone()));
            }
            if let (QueryExpression::Parameter { name }, QueryExpression::Column { column }) =
                (left.as_ref(), right.as_ref())
            {
                validate_column(column, source)?;
                return Ok(comparison(
                    column.field.clone(),
                    reverse_comparison(*operator),
                    name.clone(),
                ));
            }
            Err(unsupported(
                "confronto QueryOperation SQL Server richiede colonna e parametro",
            ))
        }
        QueryExpression::IsNull {
            expression,
            negated,
        } => {
            let QueryExpression::Column { column } = expression.as_ref() else {
                return Err(unsupported(
                    "IS NULL QueryOperation SQL Server richiede una colonna",
                ));
            };
            validate_column(column, source)?;
            if *negated {
                Ok(FilterExpression::IsNotNull {
                    field: column.field.clone(),
                })
            } else {
                Ok(FilterExpression::IsNull {
                    field: column.field.clone(),
                })
            }
        }
        _ => Err(unsupported(
            "espressione QueryOperation SQL Server non ancora tipizzabile",
        )),
    }
}

const fn comparison(
    field: String,
    operator: ComparisonOperator,
    parameter: String,
) -> FilterExpression {
    match operator {
        ComparisonOperator::Eq => FilterExpression::Eq { field, parameter },
        ComparisonOperator::Ne => FilterExpression::Ne { field, parameter },
        ComparisonOperator::Lt => FilterExpression::Lt { field, parameter },
        ComparisonOperator::Lte => FilterExpression::Lte { field, parameter },
        ComparisonOperator::Gt => FilterExpression::Gt { field, parameter },
        ComparisonOperator::Gte => FilterExpression::Gte { field, parameter },
    }
}

const fn reverse_comparison(operator: ComparisonOperator) -> ComparisonOperator {
    match operator {
        ComparisonOperator::Eq => ComparisonOperator::Eq,
        ComparisonOperator::Ne => ComparisonOperator::Ne,
        ComparisonOperator::Lt => ComparisonOperator::Gt,
        ComparisonOperator::Lte => ComparisonOperator::Gte,
        ComparisonOperator::Gt => ComparisonOperator::Lt,
        ComparisonOperator::Gte => ComparisonOperator::Lte,
    }
}

fn lower_ordering(ordering: &QueryOrdering, source: &QuerySource) -> Result<OrderBy> {
    let QueryExpression::Column { column } = &ordering.expression else {
        return Err(unsupported(
            "ORDER BY QueryOperation SQL Server richiede una colonna",
        ));
    };
    validate_column(column, source)?;
    Ok(OrderBy {
        field: column.field.clone(),
        direction: ordering.direction,
    })
}

fn validate_column(column: &ColumnRef, source: &QuerySource) -> Result<()> {
    validate_relation(column.relation.as_deref(), source)
}

fn validate_relation(relation: Option<&str>, source: &QuerySource) -> Result<()> {
    if relation.is_some_and(|relation| {
        relation != source.object.object
            && source
                .alias
                .as_deref()
                .is_none_or(|alias| relation != alias)
    }) {
        Err(unsupported(
            "qualificatore colonna SQL Server diverso dalla source",
        ))
    } else {
        Ok(())
    }
}

const fn sql_server_renderer() -> Renderer {
    Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
}

fn unsupported(message: &'static str) -> DatabaseError {
    DatabaseError::unsupported(ProviderKind::Sqlserver, ErrorPhase::Prepare, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::plan::ObjectRef;
    use plenora_database_core::query::QueryProjection;

    fn base_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("dbo".to_owned()),
                    object: "events".to_owned(),
                    layer_id: None,
                },
                alias: Some("source".to_owned()),
            }),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: Some("source".to_owned()),
                        field: "id".to_owned(),
                    },
                },
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(10),
            row_offset: None,
            locking: None,
        }
    }

    #[test]
    fn reversed_comparisons_preserve_semantics() {
        assert_eq!(
            reverse_comparison(ComparisonOperator::Lt),
            ComparisonOperator::Gt
        );
        assert_eq!(
            reverse_comparison(ComparisonOperator::Gte),
            ComparisonOperator::Lte
        );
    }

    #[test]
    fn base_query_lowers_without_losing_source_projection_or_limit() {
        let lowered = lower_query(&base_query()).expect("lowered");
        assert_eq!(lowered.source.object, "events");
        assert_eq!(lowered.projection, ["id"]);
        assert_eq!(lowered.row_limit, Some(10));
    }

    #[test]
    fn calculated_projection_and_rich_shape_fail_closed() {
        let mut calculated = base_query();
        calculated.projection[0].alias = Some("renamed".to_owned());
        assert_eq!(
            lower_query(&calculated).expect_err("alias").category,
            plenora_database_core::ErrorCategory::Unsupported
        );

        let mut rich = base_query();
        rich.distinct = true;
        assert_eq!(
            lower_query(&rich).expect_err("rich").category,
            plenora_database_core::ErrorCategory::Unsupported
        );
    }
}
