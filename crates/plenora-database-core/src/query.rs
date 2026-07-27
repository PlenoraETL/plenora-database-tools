//! AST portabile per query relazionali e funzioni scalari/spatial.
//!
//! Contiene solo riferimenti a colonne e parametri: i valori restano nel
//! `ParameterBag` e non possono essere interpolati nel testo SQL.

use crate::plan::{ComparisonOperator, ObjectRef, SortDirection};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnRef {
    pub relation: Option<String>,
    pub field: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScalarFunction {
    Lower,
    Upper,
    Coalesce,
    Count,
    Sum,
    Average,
    Minimum,
    Maximum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialFunction {
    GeometryType,
    Srid,
    Dimensions,
    IsEmpty,
    IsValid,
    Intersects,
    Contains,
    Within,
    Covers,
    Touches,
    Crosses,
    Overlaps,
    Disjoint,
    DWithin,
    Transform,
    Buffer,
    Intersection,
    Difference,
    Union,
    Simplify,
    MakeValid,
    Centroid,
    Envelope,
    Distance,
    Area,
    Length,
    Perimeter,
    Collect,
    Extent,
}

impl SpatialFunction {
    pub const ALL: &'static [Self] = &[
        Self::GeometryType,
        Self::Srid,
        Self::Dimensions,
        Self::IsEmpty,
        Self::IsValid,
        Self::Intersects,
        Self::Contains,
        Self::Within,
        Self::Covers,
        Self::Touches,
        Self::Crosses,
        Self::Overlaps,
        Self::Disjoint,
        Self::DWithin,
        Self::Transform,
        Self::Buffer,
        Self::Intersection,
        Self::Difference,
        Self::Union,
        Self::Simplify,
        Self::MakeValid,
        Self::Centroid,
        Self::Envelope,
        Self::Distance,
        Self::Area,
        Self::Length,
        Self::Perimeter,
        Self::Collect,
        Self::Extent,
    ];

    #[must_use]
    pub const fn returns_geometry(self) -> bool {
        matches!(
            self,
            Self::Transform
                | Self::Buffer
                | Self::Intersection
                | Self::Difference
                | Self::Union
                | Self::Simplify
                | Self::MakeValid
                | Self::Centroid
                | Self::Envelope
                | Self::Collect
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryExpression {
    Column {
        column: ColumnRef,
    },
    Parameter {
        name: String,
    },
    Scalar {
        function: ScalarFunction,
        arguments: Vec<Self>,
    },
    Spatial {
        function: SpatialFunction,
        arguments: Vec<Self>,
    },
    Compare {
        left: Box<Self>,
        operator: ComparisonOperator,
        right: Box<Self>,
    },
    And {
        arguments: Vec<Self>,
    },
    Or {
        arguments: Vec<Self>,
    },
    IsNull {
        expression: Box<Self>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryProjection {
    pub expression: QueryExpression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySource {
    pub object: ObjectRef,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryJoin {
    pub kind: JoinKind,
    pub source: QuerySource,
    pub on: Option<QueryExpression>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryOrdering {
    pub expression: QueryExpression,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryOperation {
    #[serde(default)]
    pub common_table_expressions: Vec<CommonTableExpression>,
    pub source: QuerySource,
    pub projection: Vec<QueryProjection>,
    #[serde(default)]
    pub joins: Vec<QueryJoin>,
    pub filter: Option<QueryExpression>,
    #[serde(default)]
    pub group_by: Vec<QueryExpression>,
    pub having: Option<QueryExpression>,
    #[serde(default)]
    pub order_by: Vec<QueryOrdering>,
    #[serde(default)]
    pub distinct: bool,
    pub row_limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommonTableExpression {
    pub name: String,
    pub query: Box<QueryOperation>,
}

/// Valida in modo iterativo la struttura di un AST relazionale.
///
/// L'algoritmo non usa ricorsione, quindi anche input avversari vengono
/// rifiutati senza consumare lo stack del processo.
///
/// # Errors
///
/// Restituisce `InvalidPlan` per budget superati, identificatori non validi o
/// strutture incomplete come projection vuote e join senza condizione.
#[allow(clippy::too_many_lines)]
pub fn validate_query_operation(
    query: &QueryOperation,
    limits: &crate::limits::Limits,
) -> crate::Result<()> {
    enum Node<'a> {
        Operation(&'a QueryOperation, usize),
        Expression(&'a QueryExpression, usize),
    }

    fn identifier(value: &str, max_bytes: usize) -> crate::Result<()> {
        if value.is_empty() || value.contains('\0') || value.len() > max_bytes {
            return Err(crate::DatabaseError::invalid_plan(
                "identificatore query vuoto, con NUL o oltre limite",
            ));
        }
        Ok(())
    }

    fn source(value: &QuerySource, max_bytes: usize) -> crate::Result<()> {
        if let Some(catalog) = &value.object.catalog {
            identifier(catalog, max_bytes)?;
        }
        if let Some(schema) = &value.object.schema {
            identifier(schema, max_bytes)?;
        }
        identifier(&value.object.object, max_bytes)?;
        if let Some(alias) = &value.alias {
            identifier(alias, max_bytes)?;
        }
        Ok(())
    }

    let mut stack = vec![Node::Operation(query, 1)];
    let mut nodes = 0_usize;
    while let Some(node) = stack.pop() {
        let depth = match node {
            Node::Operation(operation, depth) => {
                if operation.projection.is_empty() {
                    return Err(crate::DatabaseError::invalid_plan("query senza projection"));
                }
                let structural_nodes = 1_usize
                    .saturating_add(operation.common_table_expressions.len())
                    .saturating_add(operation.projection.len())
                    .saturating_add(operation.joins.len())
                    .saturating_add(operation.group_by.len())
                    .saturating_add(operation.order_by.len());
                nodes = nodes.saturating_add(structural_nodes);
                source(&operation.source, limits.max_identifier_bytes)?;
                for cte in &operation.common_table_expressions {
                    identifier(&cte.name, limits.max_identifier_bytes)?;
                    stack.push(Node::Operation(&cte.query, depth.saturating_add(1)));
                }
                for projection in &operation.projection {
                    if let Some(alias) = &projection.alias {
                        identifier(alias, limits.max_identifier_bytes)?;
                    }
                    stack.push(Node::Expression(
                        &projection.expression,
                        depth.saturating_add(1),
                    ));
                }
                for join in &operation.joins {
                    source(&join.source, limits.max_identifier_bytes)?;
                    match (&join.kind, &join.on) {
                        (JoinKind::Cross, Some(_)) => {
                            return Err(crate::DatabaseError::invalid_plan(
                                "CROSS JOIN con clausola ON",
                            ));
                        }
                        (JoinKind::Cross, None) => {}
                        (_, Some(on)) => stack.push(Node::Expression(on, depth.saturating_add(1))),
                        (_, None) => {
                            return Err(crate::DatabaseError::invalid_plan(
                                "JOIN senza clausola ON",
                            ));
                        }
                    }
                }
                if let Some(filter) = &operation.filter {
                    stack.push(Node::Expression(filter, depth.saturating_add(1)));
                }
                for expression in &operation.group_by {
                    stack.push(Node::Expression(expression, depth.saturating_add(1)));
                }
                if let Some(having) = &operation.having {
                    stack.push(Node::Expression(having, depth.saturating_add(1)));
                }
                for ordering in &operation.order_by {
                    stack.push(Node::Expression(
                        &ordering.expression,
                        depth.saturating_add(1),
                    ));
                }
                depth
            }
            Node::Expression(expression, depth) => {
                nodes = nodes.saturating_add(1);
                match expression {
                    QueryExpression::Column { column } => {
                        if let Some(relation) = &column.relation {
                            identifier(relation, limits.max_identifier_bytes)?;
                        }
                        identifier(&column.field, limits.max_identifier_bytes)?;
                    }
                    QueryExpression::Parameter { name } => identifier(name, 256)?,
                    QueryExpression::Scalar { arguments, .. }
                    | QueryExpression::Spatial { arguments, .. } => {
                        if arguments.is_empty() {
                            return Err(crate::DatabaseError::invalid_plan(
                                "funzione query senza argomenti",
                            ));
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1)));
                        }
                    }
                    QueryExpression::Compare { left, right, .. } => {
                        stack.push(Node::Expression(left, depth.saturating_add(1)));
                        stack.push(Node::Expression(right, depth.saturating_add(1)));
                    }
                    QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
                        if arguments.is_empty() {
                            return Err(crate::DatabaseError::invalid_plan("gruppo query vuoto"));
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1)));
                        }
                    }
                    QueryExpression::IsNull { expression, .. } => {
                        stack.push(Node::Expression(expression, depth.saturating_add(1)));
                    }
                }
                depth
            }
        };
        if depth > limits.max_filter_depth || nodes > limits.max_filter_nodes {
            return Err(crate::DatabaseError::invalid_plan(
                "query oltre i limiti di profondità o nodi",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod validation_tests {
    use super::*;
    use crate::limits::Limits;
    use crate::plan::ObjectRef;

    fn query_with_filter(filter: QueryExpression) -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("public".to_owned()),
                    object: "events".to_owned(),
                    layer_id: None,
                },
                alias: None,
            },
            projection: vec![QueryProjection {
                expression: QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                },
                alias: None,
            }],
            joins: Vec::new(),
            filter: Some(filter),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            row_limit: None,
        }
    }

    #[test]
    fn rejects_deep_query_without_recursive_validation() {
        let mut expression = QueryExpression::Parameter {
            name: "value".to_owned(),
        };
        for _ in 0..80 {
            expression = QueryExpression::IsNull {
                expression: Box::new(expression),
                negated: false,
            };
        }
        let error = validate_query_operation(&query_with_filter(expression), &Limits::default())
            .expect_err("depth limit");
        assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
    }

    #[test]
    fn rejects_query_over_node_budget() {
        let arguments = (0..4_096)
            .map(|_| QueryExpression::Parameter {
                name: "value".to_owned(),
            })
            .collect();
        let error = validate_query_operation(
            &query_with_filter(QueryExpression::And { arguments }),
            &Limits::default(),
        )
        .expect_err("node limit");
        assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
    }
}
