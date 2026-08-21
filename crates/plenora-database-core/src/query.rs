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
    RowNumber,
    Rank,
    DenseRank,
    Lag,
    Lead,
}

impl ScalarFunction {
    #[must_use]
    pub const fn accepts_argument_count(self, count: usize) -> bool {
        match self {
            Self::Lower
            | Self::Upper
            | Self::Count
            | Self::Sum
            | Self::Average
            | Self::Minimum
            | Self::Maximum => count == 1,
            Self::Coalesce => count >= 1,
            Self::RowNumber | Self::Rank | Self::DenseRank => count == 0,
            Self::Lag | Self::Lead => count >= 1 && count <= 3,
        }
    }

    #[must_use]
    pub const fn requires_window(self) -> bool {
        matches!(
            self,
            Self::RowNumber | Self::Rank | Self::DenseRank | Self::Lag | Self::Lead
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialOperator {
    BoundingBoxIntersects,
    BoundingBoxContains,
    BoundingBoxContainedBy,
    KnnDistance,
    KnnCentroidDistance,
}

impl SpatialOperator {
    #[must_use]
    pub const fn returns_boolean(self) -> bool {
        matches!(
            self,
            Self::BoundingBoxIntersects | Self::BoundingBoxContains | Self::BoundingBoxContainedBy
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpatialFunction {
    GeometryType,
    Srid,
    Dimensions,
    X,
    Y,
    Z,
    M,
    NPoints,
    NRings,
    StartPoint,
    EndPoint,
    PointN,
    IsEmpty,
    IsValid,
    IsSimple,
    IsClosed,
    Intersects,
    Contains,
    ContainsProperly,
    Within,
    Covers,
    CoveredBy,
    Touches,
    Crosses,
    Overlaps,
    Disjoint,
    Equals,
    Relate,
    DWithin,
    SetSrid,
    Transform,
    #[serde(rename = "force_2d")]
    Force2d,
    #[serde(rename = "force_3d")]
    Force3d,
    #[serde(rename = "force_3dm")]
    Force3dm,
    #[serde(rename = "force_4d")]
    Force4d,
    Buffer,
    OffsetCurve,
    Intersection,
    Difference,
    SymDifference,
    Union,
    UnaryUnion,
    Simplify,
    SimplifyPreserveTopology,
    MakeValid,
    Centroid,
    PointOnSurface,
    Envelope,
    ConvexHull,
    OrientedEnvelope,
    Boundary,
    LineMerge,
    Reverse,
    Subdivide,
    SnapToGrid,
    Distance,
    #[serde(rename = "distance_3d")]
    Distance3d,
    MaxDistance,
    HausdorffDistance,
    FrechetDistance,
    Azimuth,
    Area,
    Length,
    Perimeter,
    Collect,
    Extent,
    AsGeoJson,
    AsMvtGeom,
    AsMvt,
    AsGeobuf,
    ClusterDbscan,
    ClusterKMeans,
}

impl SpatialFunction {
    pub const ALL: &'static [Self] = &[
        Self::GeometryType,
        Self::Srid,
        Self::Dimensions,
        Self::X,
        Self::Y,
        Self::Z,
        Self::M,
        Self::NPoints,
        Self::NRings,
        Self::StartPoint,
        Self::EndPoint,
        Self::PointN,
        Self::IsEmpty,
        Self::IsValid,
        Self::IsSimple,
        Self::IsClosed,
        Self::Intersects,
        Self::Contains,
        Self::ContainsProperly,
        Self::Within,
        Self::Covers,
        Self::CoveredBy,
        Self::Touches,
        Self::Crosses,
        Self::Overlaps,
        Self::Disjoint,
        Self::Equals,
        Self::Relate,
        Self::DWithin,
        Self::SetSrid,
        Self::Transform,
        Self::Force2d,
        Self::Force3d,
        Self::Force3dm,
        Self::Force4d,
        Self::Buffer,
        Self::OffsetCurve,
        Self::Intersection,
        Self::Difference,
        Self::SymDifference,
        Self::Union,
        Self::UnaryUnion,
        Self::Simplify,
        Self::SimplifyPreserveTopology,
        Self::MakeValid,
        Self::Centroid,
        Self::PointOnSurface,
        Self::Envelope,
        Self::ConvexHull,
        Self::OrientedEnvelope,
        Self::Boundary,
        Self::LineMerge,
        Self::Reverse,
        Self::Subdivide,
        Self::SnapToGrid,
        Self::Distance,
        Self::Distance3d,
        Self::MaxDistance,
        Self::HausdorffDistance,
        Self::FrechetDistance,
        Self::Azimuth,
        Self::Area,
        Self::Length,
        Self::Perimeter,
        Self::Collect,
        Self::Extent,
        Self::AsGeoJson,
        Self::AsMvtGeom,
        Self::AsMvt,
        Self::AsGeobuf,
        Self::ClusterDbscan,
        Self::ClusterKMeans,
    ];

    #[must_use]
    pub const fn returns_geometry(self) -> bool {
        matches!(
            self,
            Self::StartPoint
                | Self::EndPoint
                | Self::PointN
                | Self::SetSrid
                | Self::Transform
                | Self::Force2d
                | Self::Force3d
                | Self::Force3dm
                | Self::Force4d
                | Self::Buffer
                | Self::OffsetCurve
                | Self::Intersection
                | Self::Difference
                | Self::SymDifference
                | Self::Union
                | Self::UnaryUnion
                | Self::Simplify
                | Self::SimplifyPreserveTopology
                | Self::MakeValid
                | Self::Centroid
                | Self::PointOnSurface
                | Self::Envelope
                | Self::ConvexHull
                | Self::OrientedEnvelope
                | Self::Boundary
                | Self::LineMerge
                | Self::Reverse
                | Self::Subdivide
                | Self::SnapToGrid
                | Self::Collect
                | Self::AsMvtGeom
        )
    }

    #[must_use]
    pub const fn is_unary_predicate(self) -> bool {
        matches!(
            self,
            Self::IsEmpty | Self::IsValid | Self::IsSimple | Self::IsClosed
        )
    }

    #[must_use]
    pub const fn is_binary_predicate(self) -> bool {
        matches!(
            self,
            Self::Intersects
                | Self::Contains
                | Self::ContainsProperly
                | Self::Within
                | Self::Covers
                | Self::CoveredBy
                | Self::Touches
                | Self::Crosses
                | Self::Overlaps
                | Self::Disjoint
                | Self::Equals
        )
    }

    #[must_use]
    pub const fn returns_boolean(self, argument_count: usize) -> bool {
        self.is_unary_predicate()
            || self.is_binary_predicate()
            || matches!(self, Self::DWithin)
            || matches!(self, Self::Relate) && argument_count == 3
    }

    #[must_use]
    pub const fn takes_geometry_at(self, index: usize) -> bool {
        if index == 0 {
            return !matches!(self, Self::AsMvt | Self::AsGeobuf);
        }
        index == 1
            && matches!(
                self,
                Self::Intersects
                    | Self::Contains
                    | Self::ContainsProperly
                    | Self::Within
                    | Self::Covers
                    | Self::CoveredBy
                    | Self::Touches
                    | Self::Crosses
                    | Self::Overlaps
                    | Self::Disjoint
                    | Self::Equals
                    | Self::Relate
                    | Self::DWithin
                    | Self::Intersection
                    | Self::Difference
                    | Self::SymDifference
                    | Self::Union
                    | Self::Distance
                    | Self::Distance3d
                    | Self::MaxDistance
                    | Self::HausdorffDistance
                    | Self::FrechetDistance
                    | Self::Azimuth
                    | Self::Collect
                    | Self::AsMvtGeom
            )
    }

    #[must_use]
    pub const fn accepts_argument_count(self, count: usize) -> bool {
        match self {
            Self::GeometryType
            | Self::Srid
            | Self::Dimensions
            | Self::X
            | Self::Y
            | Self::Z
            | Self::M
            | Self::NPoints
            | Self::NRings
            | Self::StartPoint
            | Self::EndPoint
            | Self::IsEmpty
            | Self::IsSimple
            | Self::IsClosed
            | Self::Force2d
            | Self::Centroid
            | Self::PointOnSurface
            | Self::Envelope
            | Self::ConvexHull
            | Self::OrientedEnvelope
            | Self::Boundary
            | Self::LineMerge
            | Self::Reverse
            | Self::Extent => count == 1,
            Self::Intersects
            | Self::Contains
            | Self::ContainsProperly
            | Self::Within
            | Self::Covers
            | Self::CoveredBy
            | Self::Touches
            | Self::Crosses
            | Self::Overlaps
            | Self::Disjoint
            | Self::Equals
            | Self::MaxDistance
            | Self::Distance3d
            | Self::Azimuth
            | Self::PointN
            | Self::SetSrid
            | Self::SimplifyPreserveTopology => count == 2,
            Self::DWithin => matches!(count, 3 | 4),
            Self::Relate
            | Self::Intersection
            | Self::Difference
            | Self::Distance
            | Self::HausdorffDistance
            | Self::FrechetDistance
            | Self::SymDifference
            | Self::Transform
            | Self::OffsetCurve
            | Self::Simplify
            | Self::Buffer
            | Self::ClusterKMeans => matches!(count, 2 | 3),
            Self::IsValid
            | Self::Force3d
            | Self::Force3dm
            | Self::UnaryUnion
            | Self::MakeValid
            | Self::Collect
            | Self::AsGeobuf
            | Self::Area
            | Self::Length
            | Self::Perimeter => matches!(count, 1 | 2),
            Self::Force4d | Self::Union | Self::Subdivide | Self::AsGeoJson => {
                matches!(count, 1..=3)
            }
            // L'overload a sei argomenti usa una geometria come origine e
            // richiede una firma distinta. Il catalogo portabile espone solo
            // gli overload numerici non ambigui.
            Self::SnapToGrid => matches!(count, 2 | 3 | 5),
            Self::AsMvtGeom => matches!(count, 2..=5),
            Self::AsMvt => matches!(count, 1..=5),
            Self::ClusterDbscan => count == 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum QueryExpression {
    Wildcard {
        relation: Option<String>,
    },
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
    SpatialOperator {
        operator: SpatialOperator,
        left: Box<Self>,
        right: Box<Self>,
    },
    Window {
        function: ScalarFunction,
        arguments: Vec<Self>,
        #[serde(default)]
        partition_by: Vec<Self>,
        #[serde(default)]
        order_by: Vec<QueryOrdering>,
        frame: Option<WindowFrame>,
    },
    SpatialWindow {
        function: SpatialFunction,
        arguments: Vec<Self>,
        #[serde(default)]
        partition_by: Vec<Self>,
        #[serde(default)]
        order_by: Vec<QueryOrdering>,
        frame: Option<WindowFrame>,
    },
    ScalarSubquery {
        query: Box<QueryOperation>,
    },
    Exists {
        query: Box<QueryOperation>,
        #[serde(default)]
        negated: bool,
    },
    InSubquery {
        expression: Box<Self>,
        query: Box<QueryOperation>,
        #[serde(default)]
        negated: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "offset", rename_all = "snake_case")]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(u64),
    CurrentRow,
    Following(u64),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowFrameBound,
    pub end: Option<WindowFrameBound>,
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
    pub source: Option<QuerySource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_source: Option<QueryDerivedSource>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub lateral: bool,
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
pub struct QueryDerivedSource {
    pub query: Box<QueryOperation>,
    pub alias: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuerySetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuerySetOperation {
    pub operator: QuerySetOperator,
    #[serde(default, skip_serializing_if = "is_false")]
    pub all: bool,
    pub query: Box<QueryOperation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryLockStrength {
    Update,
    NoKeyUpdate,
    Share,
    KeyShare,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryLockWait {
    Wait,
    NoWait,
    SkipLocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryLock {
    pub strength: QueryLockStrength,
    #[serde(default)]
    pub relations: Vec<String>,
    pub wait: QueryLockWait,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryOperation {
    #[serde(default)]
    pub common_table_expressions: Vec<CommonTableExpression>,
    pub source: Option<QuerySource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_source: Option<QueryDerivedSource>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub distinct_on: Vec<QueryExpression>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub set_operations: Vec<QuerySetOperation>,
    pub row_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locking: Option<QueryLock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommonTableExpression {
    pub name: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub recursive: bool,
    pub query: Box<QueryOperation>,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
const fn is_false(value: &bool) -> bool {
    !*value
}

fn validate_window_frame(frame: &WindowFrame) -> crate::Result<()> {
    fn position(bound: &WindowFrameBound) -> i128 {
        match bound {
            WindowFrameBound::UnboundedPreceding => i128::MIN,
            WindowFrameBound::Preceding(offset) => -i128::from(*offset),
            WindowFrameBound::CurrentRow => 0,
            WindowFrameBound::Following(offset) => i128::from(*offset),
            WindowFrameBound::UnboundedFollowing => i128::MAX,
        }
    }

    if let Some(end) = &frame.end {
        if position(&frame.start) > position(end) {
            return Err(crate::DatabaseError::invalid_plan(
                "frame window con limite iniziale successivo al finale",
            ));
        }
    }
    Ok(())
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

    fn predicate(expression: &QueryExpression) -> crate::Result<()> {
        let mut stack = vec![expression];
        while let Some(item) = stack.pop() {
            match item {
                QueryExpression::Compare { .. }
                | QueryExpression::IsNull { .. }
                | QueryExpression::Exists { .. }
                | QueryExpression::InSubquery { .. } => {}
                QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
                    stack.extend(arguments);
                }
                QueryExpression::Spatial {
                    function,
                    arguments,
                } if function.returns_boolean(arguments.len()) => {}
                QueryExpression::SpatialOperator { operator, .. } if operator.returns_boolean() => {
                }
                _ => {
                    return Err(crate::DatabaseError::invalid_plan(
                        "espressione non booleana usata come predicato",
                    ));
                }
            }
        }
        Ok(())
    }

    fn contains_locking_incompatible_expression(expression: &QueryExpression) -> bool {
        let mut stack = vec![expression];
        while let Some(item) = stack.pop() {
            match item {
                QueryExpression::Window { .. } | QueryExpression::SpatialWindow { .. } => {
                    return true;
                }
                QueryExpression::Scalar {
                    function:
                        ScalarFunction::Count
                        | ScalarFunction::Sum
                        | ScalarFunction::Average
                        | ScalarFunction::Minimum
                        | ScalarFunction::Maximum,
                    ..
                }
                | QueryExpression::Spatial {
                    function:
                        SpatialFunction::Collect
                        | SpatialFunction::Extent
                        | SpatialFunction::Union
                        | SpatialFunction::AsMvt
                        | SpatialFunction::AsGeobuf,
                    ..
                } => return true,
                QueryExpression::Scalar { arguments, .. }
                | QueryExpression::Spatial { arguments, .. }
                | QueryExpression::And { arguments }
                | QueryExpression::Or { arguments } => stack.extend(arguments),
                QueryExpression::SpatialOperator { left, right, .. }
                | QueryExpression::Compare { left, right, .. } => {
                    stack.push(left);
                    stack.push(right);
                }
                QueryExpression::IsNull { expression, .. }
                | QueryExpression::InSubquery { expression, .. } => stack.push(expression),
                QueryExpression::Wildcard { .. }
                | QueryExpression::Column { .. }
                | QueryExpression::Parameter { .. }
                | QueryExpression::ScalarSubquery { .. }
                | QueryExpression::Exists { .. } => {}
            }
        }
        false
    }

    let mut stack = vec![Node::Operation(query, 1)];
    let mut nodes = 0_usize;
    while let Some(node) = stack.pop() {
        let depth = match node {
            Node::Operation(operation, depth) => {
                if operation.projection.is_empty() {
                    return Err(crate::DatabaseError::invalid_plan("query senza projection"));
                }
                if operation.distinct && !operation.distinct_on.is_empty() {
                    return Err(crate::DatabaseError::invalid_plan(
                        "DISTINCT e DISTINCT ON non possono coesistere",
                    ));
                }
                if operation.row_offset.is_some() && operation.order_by.is_empty() {
                    return Err(crate::DatabaseError::invalid_plan(
                        "OFFSET richiede ORDER BY deterministico",
                    ));
                }
                if !operation.distinct_on.is_empty()
                    && (operation.order_by.len() < operation.distinct_on.len()
                        || operation
                            .distinct_on
                            .iter()
                            .zip(&operation.order_by)
                            .any(|(distinct, order)| distinct != &order.expression))
                {
                    return Err(crate::DatabaseError::invalid_plan(
                        "DISTINCT ON deve coincidere col prefisso ORDER BY",
                    ));
                }
                if operation
                    .set_operations
                    .iter()
                    .any(|set| set.query.projection.len() != operation.projection.len())
                {
                    return Err(crate::DatabaseError::invalid_plan(
                        "set operation con numero di colonne differente",
                    ));
                }
                if operation.locking.is_some() && !operation.set_operations.is_empty() {
                    return Err(crate::DatabaseError::invalid_plan(
                        "locking non ammesso su query con set operation",
                    ));
                }
                if operation.locking.is_some()
                    && (operation.distinct
                        || !operation.distinct_on.is_empty()
                        || !operation.group_by.is_empty()
                        || operation.having.is_some()
                        || operation.projection.iter().any(|projection| {
                            contains_locking_incompatible_expression(&projection.expression)
                        }))
                {
                    return Err(crate::DatabaseError::invalid_plan(
                        "locking non ammesso con distinct, aggregazioni o window",
                    ));
                }
                let structural_nodes = 1_usize
                    .saturating_add(operation.common_table_expressions.len())
                    .saturating_add(operation.projection.len())
                    .saturating_add(operation.joins.len())
                    .saturating_add(operation.group_by.len())
                    .saturating_add(operation.order_by.len())
                    .saturating_add(operation.distinct_on.len())
                    .saturating_add(operation.set_operations.len());
                nodes = nodes.saturating_add(structural_nodes);
                match (&operation.source, &operation.derived_source) {
                    (Some(value), None) => source(value, limits.max_identifier_bytes)?,
                    (None, Some(value)) => {
                        identifier(&value.alias, limits.max_identifier_bytes)?;
                        stack.push(Node::Operation(&value.query, depth.saturating_add(1)));
                    }
                    _ => {
                        return Err(crate::DatabaseError::invalid_plan(
                            "query richiede una sola source, tabella o subquery",
                        ));
                    }
                }
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
                    if join.lateral && join.derived_source.is_none() {
                        return Err(crate::DatabaseError::invalid_plan(
                            "LATERAL richiede una subquery",
                        ));
                    }
                    match (&join.source, &join.derived_source) {
                        (Some(value), None) => source(value, limits.max_identifier_bytes)?,
                        (None, Some(value)) => {
                            identifier(&value.alias, limits.max_identifier_bytes)?;
                            stack.push(Node::Operation(&value.query, depth.saturating_add(1)));
                        }
                        _ => {
                            return Err(crate::DatabaseError::invalid_plan(
                                "join richiede una sola source, tabella o subquery",
                            ));
                        }
                    }
                    match (&join.kind, &join.on) {
                        (JoinKind::Cross, Some(_)) => {
                            return Err(crate::DatabaseError::invalid_plan(
                                "CROSS JOIN con clausola ON",
                            ));
                        }
                        (JoinKind::Cross, None) => {}
                        (_, Some(on)) => {
                            predicate(on)?;
                            stack.push(Node::Expression(on, depth.saturating_add(1)));
                        }
                        (_, None) => {
                            return Err(crate::DatabaseError::invalid_plan(
                                "JOIN senza clausola ON",
                            ));
                        }
                    }
                }
                if let Some(filter) = &operation.filter {
                    predicate(filter)?;
                    stack.push(Node::Expression(filter, depth.saturating_add(1)));
                }
                for expression in &operation.group_by {
                    stack.push(Node::Expression(expression, depth.saturating_add(1)));
                }
                for expression in &operation.distinct_on {
                    stack.push(Node::Expression(expression, depth.saturating_add(1)));
                }
                if let Some(having) = &operation.having {
                    predicate(having)?;
                    stack.push(Node::Expression(having, depth.saturating_add(1)));
                }
                for ordering in &operation.order_by {
                    stack.push(Node::Expression(
                        &ordering.expression,
                        depth.saturating_add(1),
                    ));
                }
                for set_operation in &operation.set_operations {
                    stack.push(Node::Operation(
                        &set_operation.query,
                        depth.saturating_add(1),
                    ));
                }
                if let Some(locking) = &operation.locking {
                    let mut lockable_relations = Vec::new();
                    if let Some(source) = &operation.source {
                        lockable_relations.push(
                            source
                                .alias
                                .as_deref()
                                .unwrap_or(source.object.object.as_str()),
                        );
                    }
                    for join in &operation.joins {
                        if let Some(source) = &join.source {
                            lockable_relations.push(
                                source
                                    .alias
                                    .as_deref()
                                    .unwrap_or(source.object.object.as_str()),
                            );
                        }
                    }
                    for relation in &locking.relations {
                        identifier(relation, limits.max_identifier_bytes)?;
                        if !lockable_relations.contains(&relation.as_str()) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "relazione FOR UPDATE/SHARE non direttamente lockable",
                            ));
                        }
                    }
                }
                depth
            }
            Node::Expression(expression, depth) => {
                nodes = nodes.saturating_add(1);
                match expression {
                    QueryExpression::Wildcard { relation } => {
                        if let Some(relation) = relation {
                            identifier(relation, limits.max_identifier_bytes)?;
                        }
                    }
                    QueryExpression::Column { column } => {
                        if let Some(relation) = &column.relation {
                            identifier(relation, limits.max_identifier_bytes)?;
                        }
                        identifier(&column.field, limits.max_identifier_bytes)?;
                    }
                    QueryExpression::Parameter { name } => identifier(name, 256)?,
                    QueryExpression::Scalar {
                        function,
                        arguments,
                    } => {
                        if function.requires_window() {
                            return Err(crate::DatabaseError::invalid_plan(
                                "funzione window usata senza clausola OVER",
                            ));
                        }
                        if !function.accepts_argument_count(arguments.len()) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "numero argomenti funzione scalare non valido",
                            ));
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1)));
                        }
                    }
                    QueryExpression::Spatial {
                        function,
                        arguments,
                    } => {
                        if !function.accepts_argument_count(arguments.len()) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "numero argomenti funzione spatial non valido",
                            ));
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1)));
                        }
                    }
                    QueryExpression::SpatialOperator { left, right, .. }
                    | QueryExpression::Compare { left, right, .. } => {
                        stack.push(Node::Expression(left, depth.saturating_add(1)));
                        stack.push(Node::Expression(right, depth.saturating_add(1)));
                    }
                    QueryExpression::Window {
                        function,
                        arguments,
                        partition_by,
                        order_by,
                        frame,
                    } => {
                        if !matches!(
                            function,
                            ScalarFunction::Count
                                | ScalarFunction::Sum
                                | ScalarFunction::Average
                                | ScalarFunction::Minimum
                                | ScalarFunction::Maximum
                                | ScalarFunction::RowNumber
                                | ScalarFunction::Rank
                                | ScalarFunction::DenseRank
                                | ScalarFunction::Lag
                                | ScalarFunction::Lead
                        ) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "funzione non valida come window",
                            ));
                        }
                        if !function.accepts_argument_count(arguments.len()) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "numero argomenti funzione window non valido",
                            ));
                        }
                        if let Some(frame) = frame {
                            validate_window_frame(frame)?;
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1)));
                        }
                        for expression in partition_by {
                            stack.push(Node::Expression(expression, depth.saturating_add(1)));
                        }
                        for ordering in order_by {
                            stack.push(Node::Expression(
                                &ordering.expression,
                                depth.saturating_add(1),
                            ));
                        }
                    }
                    QueryExpression::SpatialWindow {
                        function,
                        arguments,
                        partition_by,
                        order_by,
                        frame,
                    } => {
                        if !matches!(
                            function,
                            SpatialFunction::ClusterDbscan | SpatialFunction::ClusterKMeans
                        ) || !function.accepts_argument_count(arguments.len())
                        {
                            return Err(crate::DatabaseError::invalid_plan(
                                "funzione spatial window non valida",
                            ));
                        }
                        if let Some(frame) = frame {
                            validate_window_frame(frame)?;
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1)));
                        }
                        for expression in partition_by {
                            stack.push(Node::Expression(expression, depth.saturating_add(1)));
                        }
                        for ordering in order_by {
                            stack.push(Node::Expression(
                                &ordering.expression,
                                depth.saturating_add(1),
                            ));
                        }
                    }
                    QueryExpression::ScalarSubquery { query }
                    | QueryExpression::Exists { query, .. } => {
                        stack.push(Node::Operation(query, depth.saturating_add(1)));
                    }
                    QueryExpression::InSubquery {
                        expression, query, ..
                    } => {
                        stack.push(Node::Expression(expression, depth.saturating_add(1)));
                        stack.push(Node::Operation(query, depth.saturating_add(1)));
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
            source: Some(QuerySource {
                object: ObjectRef {
                    catalog: None,
                    schema: Some("public".to_owned()),
                    object: "events".to_owned(),
                },
                alias: None,
            }),
            derived_source: None,
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
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
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

    #[test]
    fn rejects_non_boolean_filters_and_invalid_spatial_arity() {
        let error = validate_query_operation(
            &query_with_filter(QueryExpression::SpatialOperator {
                operator: SpatialOperator::KnnDistance,
                left: Box::new(QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "geom".to_owned(),
                    },
                }),
                right: Box::new(QueryExpression::Parameter {
                    name: "probe".to_owned(),
                }),
            }),
            &Limits::default(),
        )
        .expect_err("KNN distance is not boolean");
        assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);

        let error = validate_query_operation(
            &query_with_filter(QueryExpression::Spatial {
                function: SpatialFunction::DWithin,
                arguments: vec![QueryExpression::Parameter {
                    name: "probe".to_owned(),
                }],
            }),
            &Limits::default(),
        )
        .expect_err("invalid spatial arity");
        assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
    }

    #[test]
    fn postgis_overload_arities_are_fail_closed() {
        assert!(!SpatialFunction::Buffer.accepts_argument_count(1));
        assert!(SpatialFunction::Buffer.accepts_argument_count(2));
        assert!(SpatialFunction::Buffer.accepts_argument_count(3));
        assert!(SpatialFunction::Union.accepts_argument_count(1));
        assert!(SpatialFunction::Union.accepts_argument_count(3));
        assert!(SpatialFunction::UnaryUnion.accepts_argument_count(2));
        assert!(SpatialFunction::Transform.accepts_argument_count(3));
        assert!(SpatialFunction::Force4d.accepts_argument_count(3));
        assert!(SpatialFunction::SnapToGrid.accepts_argument_count(5));
        assert!(!SpatialFunction::SnapToGrid.accepts_argument_count(4));
        assert!(!SpatialFunction::SnapToGrid.accepts_argument_count(6));
    }

    #[test]
    fn rejects_reversed_window_frame() {
        let mut query = query_with_filter(QueryExpression::Compare {
            left: Box::new(QueryExpression::Parameter {
                name: "left".to_owned(),
            }),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "right".to_owned(),
            }),
        });
        query.projection = vec![QueryProjection {
            expression: QueryExpression::Window {
                function: ScalarFunction::Sum,
                arguments: vec![QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                }],
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: Some(WindowFrame {
                    units: WindowFrameUnits::Rows,
                    start: WindowFrameBound::Following(2),
                    end: Some(WindowFrameBound::Preceding(1)),
                }),
            },
            alias: None,
        }];
        let error =
            validate_query_operation(&query, &Limits::default()).expect_err("reversed frame");
        assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
    }
}
