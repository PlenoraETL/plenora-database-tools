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
