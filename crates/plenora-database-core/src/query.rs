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

    /// La funzione collassa un gruppo di righe in un valore.
    ///
    /// Serve a due domande diverse — se l'espressione e compatibile con
    /// `FOR UPDATE` e se puo contenere una window fra i suoi argomenti — che
    /// prima portavano ciascuna la propria copia della lista.
    #[must_use]
    pub const fn is_aggregate(self) -> bool {
        matches!(
            self,
            Self::Count | Self::Sum | Self::Average | Self::Minimum | Self::Maximum
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

    /// In quale sistema di riferimento cade il risultato di questa funzione.
    ///
    /// Restituisce `None` per le funzioni che rendono uno scalare: un'area non
    /// sta in nessun sistema di riferimento, e la domanda non si pone.
    ///
    /// # Perché esiste
    ///
    /// Un provider che riceve una geometria calcolata senza SRID non può
    /// consegnarla: un WKB privo di sistema di riferimento non è una geometria
    /// che questo contratto sappia descrivere. Ma il frame spesso è noto —
    /// quello della colonna d'ingresso, che su `MySQL` e `MariaDB` è dichiarato
    /// e verificato — e serve solo sapere se il risultato lo eredita.
    ///
    /// Questa è la risposta, ed è la stessa su ogni motore perché è una
    /// proprietà della geometria e non del prodotto. La sua controparte
    /// pubblicata sta nel catalogo spatial, e
    /// `crs_rules_match_the_versioned_catalog` non lascia che le due divergano.
    #[must_use]
    pub const fn crs_rule(self) -> Option<crate::spatial_catalog::CrsRule> {
        use crate::spatial_catalog::CrsRule;
        match self {
            // Il chiamante nomina l'SRID di destinazione, e il risultato ci sta
            // per costruzione.
            Self::SetSrid | Self::Transform => Some(CrsRule::Argument),
            // Entrano due geometrie: il risultato conserva il frame soltanto se
            // i due lo condividono, e questa è una condizione da dimostrare a
            // ogni chiamata, non una regola da dichiarare. `AsMvtGeom` ci sta
            // per una seconda ragione — porta le coordinate nello spazio di una
            // tile, che non è un CRS.
            Self::Intersection
            | Self::Difference
            | Self::SymDifference
            | Self::Union
            | Self::AsMvtGeom => Some(CrsRule::Undefined),
            _ if self.returns_geometry() => Some(CrsRule::Preserves),
            _ => None,
        }
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

    /// La funzione **puo** collassare un gruppo di geometrie in un valore.
    ///
    /// «Puo», non «lo fa»: per `ST_Collect` e `ST_Union` l'arita non identifica
    /// l'overload, e questo AST non porta i tipi che servirebbero a
    /// identificarlo. Misurato su `PostGIS` 3.4 (`pg_proc.prokind`):
    ///
    /// | chiamata | overload |
    /// |---|---|
    /// | `ST_Collect(x)` | `(geometry set)` aggregata **o** `(geometry[])` scalare |
    /// | `ST_Collect(x, y)` | `(geometry, geometry)` scalare |
    /// | `ST_Union(x)` | `(geometry set)` aggregata **o** `(geometry[])` scalare |
    /// | `ST_Union(x, y)` | `(geometry, geometry)` scalare **o** `(geometry set, float8)` aggregata |
    /// | `ST_Union(x, y, z)` | `(geometry, geometry, float8)` scalare |
    ///
    /// Le forme ambigue restano esprimibili: il wrapping in
    /// `ST_GeomFromEWKB` tocca solo i `QueryExpression::Parameter`
    /// (`plenora_database_sql`, `render_spatial_function`). Una
    /// `QueryExpression::Column` passa invariata, quindi
    /// `ST_Union(geom, gridsize)` con due colonne e formabile e il server
    /// risolve l'aggregata.
    ///
    /// Dove l'aggregata e possibile, questa funzione risponde `true`. E' la
    /// risposta conservativa nel verso giusto: sbagliarla per eccesso rifiuta
    /// prima della rete un piano scalare valido — l'utente lo riscrive —
    /// mentre sbagliarla per difetto lascia passare una window annidata in
    /// un'aggregata, o un `FOR UPDATE` su una query che aggrega, e il rifiuto
    /// arriva dal server a transazione aperta.
    ///
    /// Per rispondere «lo fa» servirebbe l'identita dell'overload nel piano,
    /// cioe un campo nuovo nel contratto: additivo, ma non gratis, e finche non
    /// c'e la risposta resta questa.
    #[must_use]
    pub const fn is_aggregate(self, argument_count: usize) -> bool {
        match self {
            // Le arita sono quelle che `PostGIS` pubblica davvero: rispondere
            // `true` a zero argomenti, o a un'arita inesistente, faceva dire a
            // questa funzione qualcosa su una chiamata che non esiste — e il
            // controllo del locking la interroga **prima** che la validazione
            // delle arita abbia rifiutato il piano, quindi l'errore riportato
            // sarebbe stato quello sbagliato.
            Self::Extent | Self::Collect => argument_count == 1,
            Self::AsMvt => matches!(argument_count, 1..=5),
            Self::AsGeobuf | Self::Union => matches!(argument_count, 1 | 2),
            _ => false,
        }
    }

    /// La funzione esiste **solo** come window function.
    ///
    /// `PostgreSQL` non sa eseguirla senza `OVER`, quindi e componibile solo
    /// con [`QueryExpression::SpatialWindow`]. La variante ordinaria
    /// [`QueryExpression::Spatial`] la accettava, e il piano arrivava al server
    /// come chiamata semplice — rifiutata li invece che qui.
    #[must_use]
    pub const fn is_window_only(self) -> bool {
        matches!(self, Self::ClusterDbscan | Self::ClusterKMeans)
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
    /// Il CRS che il chiamante dichiara per una colonna geometrica sorgente.
    ///
    /// Ha la stessa forma e le stesse regole di
    /// [`crate::plan::ReadOperation::declared_crs`], e serve alla stessa cosa
    /// per una ragione che qui è più stretta: una query può **calcolare** una
    /// geometria, e cio che il motore restituisce spesso non porta il sistema
    /// di riferimento — `MySQL` e `MariaDB` rendono SRID 0 per un `ST_Buffer`
    /// su una colonna in 4326.
    ///
    /// Il frame però non è ignoto: è quello della geometria d'ingresso, e
    /// [`SpatialFunction::crs_rule`] dice se il risultato lo eredita. Questa
    /// dichiarazione è l'altra metà — dove sta l'ingresso — e senza di essa
    /// non c'è niente da ereditare.
    ///
    /// Come sul path di lettura, non è una promessa che si crede sulla parola:
    /// il provider che l'accetta la verifica riga per riga, e una colonna che
    /// porta geometrie in sistemi diversi fa fallire la lettura invece di
    /// pubblicare un CRS falso.
    ///
    /// Additivo e opzionale: senza dichiarazione una geometria calcolata che
    /// richiede un CRS dimostrabile resta rifiutata.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub declared_crs: Vec<crate::plan::DeclaredCrs>,
}

impl QueryOperation {
    /// Espressioni dichiarate direttamente nelle clausole dell'operazione.
    ///
    /// Non include le condizioni dei join ne le query annidate: i chiamanti
    /// che devono attraversare l'intero albero usano [`walk_query`].
    pub fn clause_expressions(&self) -> impl Iterator<Item = &QueryExpression> {
        self.projection
            .iter()
            .map(|projection| &projection.expression)
            .chain(self.filter.iter())
            .chain(&self.group_by)
            .chain(self.having.iter())
            .chain(self.order_by.iter().map(|ordering| &ordering.expression))
            .chain(&self.distinct_on)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommonTableExpression {
    pub name: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub recursive: bool,
    pub query: Box<QueryOperation>,
}

/// Nodo osservabile durante la visita iterativa di una query.
///
/// La visita espone anche le sorgenti per evitare che ogni provider debba
/// replicare il traversal dell'intero AST solo per applicare una propria
/// politica su cataloghi e schemi.
#[derive(Debug, Clone, Copy)]
pub enum QueryWalkNode<'a> {
    Operation(&'a QueryOperation),
    Expression(&'a QueryExpression),
    Source(&'a QuerySource),
}

/// Controllo restituito da un visitor dell'AST query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryWalkControl {
    /// Visita anche i figli del nodo corrente.
    Continue,
    /// Non visita i figli del nodo corrente, ma continua con gli altri nodi.
    Skip,
    /// Interrompe immediatamente l'intera visita.
    Break,
}

/// Visita iterativamente un'operazione e tutte le query annidate.
///
/// Restituisce `false` quando il visitor interrompe la visita con
/// [`QueryWalkControl::Break`], `true` quando l'AST viene percorso per intero.
pub fn walk_query<'a>(
    operation: &'a QueryOperation,
    visitor: impl FnMut(QueryWalkNode<'a>) -> QueryWalkControl,
) -> bool {
    walk_query_nodes(vec![QueryWalkNode::Operation(operation)], visitor)
}

/// Visita iterativamente un'espressione, comprese le eventuali query annidate.
///
/// Restituisce `false` quando il visitor interrompe la visita con
/// [`QueryWalkControl::Break`], `true` quando l'AST viene percorso per intero.
pub fn walk_query_expression<'a>(
    expression: &'a QueryExpression,
    visitor: impl FnMut(QueryWalkNode<'a>) -> QueryWalkControl,
) -> bool {
    walk_query_nodes(vec![QueryWalkNode::Expression(expression)], visitor)
}

fn walk_query_nodes<'a>(
    mut stack: Vec<QueryWalkNode<'a>>,
    mut visitor: impl FnMut(QueryWalkNode<'a>) -> QueryWalkControl,
) -> bool {
    while let Some(node) = stack.pop() {
        match visitor(node) {
            QueryWalkControl::Break => return false,
            QueryWalkControl::Skip => {}
            QueryWalkControl::Continue => match node {
                QueryWalkNode::Operation(operation) => {
                    push_operation_children(operation, &mut stack);
                }
                QueryWalkNode::Expression(expression) => {
                    push_expression_children(expression, &mut stack);
                }
                QueryWalkNode::Source(_) => {}
            },
        }
    }
    true
}

fn push_operation_children<'a>(operation: &'a QueryOperation, stack: &mut Vec<QueryWalkNode<'a>>) {
    stack.extend(
        operation
            .set_operations
            .iter()
            .rev()
            .map(|set| QueryWalkNode::Operation(&set.query)),
    );
    stack.extend(
        operation
            .distinct_on
            .iter()
            .rev()
            .map(QueryWalkNode::Expression),
    );
    stack.extend(
        operation
            .order_by
            .iter()
            .rev()
            .map(|ordering| QueryWalkNode::Expression(&ordering.expression)),
    );
    stack.extend(operation.having.iter().map(QueryWalkNode::Expression));
    stack.extend(
        operation
            .group_by
            .iter()
            .rev()
            .map(QueryWalkNode::Expression),
    );
    stack.extend(operation.filter.iter().map(QueryWalkNode::Expression));
    stack.extend(
        operation
            .projection
            .iter()
            .rev()
            .map(|projection| QueryWalkNode::Expression(&projection.expression)),
    );
    for join in operation.joins.iter().rev() {
        stack.extend(join.on.iter().map(QueryWalkNode::Expression));
        if let Some(derived) = &join.derived_source {
            stack.push(QueryWalkNode::Operation(&derived.query));
        }
        stack.extend(join.source.iter().map(QueryWalkNode::Source));
    }
    for cte in operation.common_table_expressions.iter().rev() {
        stack.push(QueryWalkNode::Operation(&cte.query));
    }
    if let Some(derived) = &operation.derived_source {
        stack.push(QueryWalkNode::Operation(&derived.query));
    }
    stack.extend(operation.source.iter().map(QueryWalkNode::Source));
}

fn push_expression_children<'a>(
    expression: &'a QueryExpression,
    stack: &mut Vec<QueryWalkNode<'a>>,
) {
    match expression {
        QueryExpression::Scalar { arguments, .. }
        | QueryExpression::Spatial { arguments, .. }
        | QueryExpression::And { arguments }
        | QueryExpression::Or { arguments } => {
            stack.extend(arguments.iter().rev().map(QueryWalkNode::Expression));
        }
        QueryExpression::SpatialOperator { left, right, .. }
        | QueryExpression::Compare { left, right, .. } => {
            stack.push(QueryWalkNode::Expression(right));
            stack.push(QueryWalkNode::Expression(left));
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
            stack.extend(
                order_by
                    .iter()
                    .rev()
                    .map(|ordering| QueryWalkNode::Expression(&ordering.expression)),
            );
            stack.extend(partition_by.iter().rev().map(QueryWalkNode::Expression));
            stack.extend(arguments.iter().rev().map(QueryWalkNode::Expression));
        }
        QueryExpression::ScalarSubquery { query } | QueryExpression::Exists { query, .. } => {
            stack.push(QueryWalkNode::Operation(query));
        }
        QueryExpression::InSubquery {
            expression, query, ..
        } => {
            stack.push(QueryWalkNode::Operation(query));
            stack.push(QueryWalkNode::Expression(expression));
        }
        QueryExpression::IsNull { expression, .. } => {
            stack.push(QueryWalkNode::Expression(expression));
        }
        QueryExpression::Wildcard { .. }
        | QueryExpression::Column { .. }
        | QueryExpression::Parameter { .. } => {}
    }
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
        Expression(&'a QueryExpression, usize, WindowPosition),
    }

    /// Dove, nella query, si trova l'espressione che stiamo visitando.
    ///
    /// Una window function e valida solo nella projection e nell'`ORDER BY`
    /// finale, e mai dentro gli argomenti di un'altra window o di
    /// un'aggregata: il valore che ordina la finestra non e ancora calcolato
    /// quando la clausola che lo userebbe viene valutata. Il validatore
    /// visitava le espressioni senza sapere da quale clausola venissero,
    /// quindi accettava `WHERE row_number() OVER () > 1` e
    /// `rank() OVER (PARTITION BY row_number() OVER ())`, che ogni provider
    /// rifiuta — a rete gia aperta e con il messaggio del server invece che
    /// con un `InvalidPlan` prima della connessione.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum WindowPosition {
        /// Projection e `ORDER BY` della stessa operazione.
        Allowed,
        /// `WHERE`, `ON`, `GROUP BY`, `DISTINCT ON`, `HAVING`.
        Clause,
        /// Dentro gli argomenti di un'altra window o di un'aggregata.
        Nested,
        /// L'`ORDER BY` che il renderer emette **dopo** le set operation.
        ///
        /// Quell'`ORDER BY` ordina il risultato dell'unione, non la query che
        /// lo dichiara: le sue righe non appartengono piu a nessuna delle
        /// finestre dei rami, e la clausola puo riferirsi solo alle colonne di
        /// uscita.
        SetOperation,
    }

    fn reject_window(position: WindowPosition) -> crate::Result<()> {
        match position {
            WindowPosition::Allowed => Ok(()),
            WindowPosition::Clause => Err(crate::DatabaseError::invalid_plan(
                "funzione window fuori da projection e ORDER BY",
            )),
            WindowPosition::SetOperation => Err(crate::DatabaseError::invalid_plan(
                "funzione window nell'ORDER BY di una set operation",
            )),
            WindowPosition::Nested => Err(crate::DatabaseError::invalid_plan(
                "funzione window annidata in una window o in un'aggregata",
            )),
        }
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
        !walk_query_expression(expression, |node| match node {
            QueryWalkNode::Expression(
                QueryExpression::Window { .. } | QueryExpression::SpatialWindow { .. },
            ) => QueryWalkControl::Break,
            QueryWalkNode::Expression(QueryExpression::Scalar { function, .. })
                if function.is_aggregate() =>
            {
                QueryWalkControl::Break
            }
            QueryWalkNode::Expression(QueryExpression::Spatial {
                function,
                arguments,
            }) if function.is_aggregate(arguments.len()) => QueryWalkControl::Break,
            QueryWalkNode::Operation(_) => QueryWalkControl::Skip,
            QueryWalkNode::Expression(_) | QueryWalkNode::Source(_) => QueryWalkControl::Continue,
        })
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
                        WindowPosition::Allowed,
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
                            stack.push(Node::Expression(
                                on,
                                depth.saturating_add(1),
                                WindowPosition::Clause,
                            ));
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
                    stack.push(Node::Expression(
                        filter,
                        depth.saturating_add(1),
                        WindowPosition::Clause,
                    ));
                }
                for expression in &operation.group_by {
                    stack.push(Node::Expression(
                        expression,
                        depth.saturating_add(1),
                        WindowPosition::Clause,
                    ));
                }
                for expression in &operation.distinct_on {
                    stack.push(Node::Expression(
                        expression,
                        depth.saturating_add(1),
                        WindowPosition::Clause,
                    ));
                }
                if let Some(having) = &operation.having {
                    predicate(having)?;
                    stack.push(Node::Expression(
                        having,
                        depth.saturating_add(1),
                        WindowPosition::Clause,
                    ));
                }
                // Il renderer emette questo `ORDER BY` dopo i rami dell'unione:
                // con una set operation ordina il risultato combinato, e li una
                // window non ha piu una partizione a cui riferirsi.
                let ordering_position = if operation.set_operations.is_empty() {
                    WindowPosition::Allowed
                } else {
                    WindowPosition::SetOperation
                };
                for ordering in &operation.order_by {
                    stack.push(Node::Expression(
                        &ordering.expression,
                        depth.saturating_add(1),
                        ordering_position,
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
            Node::Expression(expression, depth, position) => {
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
                        let inner = if function.is_aggregate() {
                            WindowPosition::Nested
                        } else {
                            position
                        };
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1), inner));
                        }
                    }
                    QueryExpression::Spatial {
                        function,
                        arguments,
                    } => {
                        if function.is_window_only() {
                            return Err(crate::DatabaseError::invalid_plan(
                                "funzione spatial window usata come chiamata ordinaria: \
                                 richiede QueryExpression::SpatialWindow",
                            ));
                        }
                        if !function.accepts_argument_count(arguments.len()) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "numero argomenti funzione spatial non valido",
                            ));
                        }
                        let inner = if function.is_aggregate(arguments.len()) {
                            WindowPosition::Nested
                        } else {
                            position
                        };
                        for argument in arguments {
                            stack.push(Node::Expression(argument, depth.saturating_add(1), inner));
                        }
                    }
                    QueryExpression::SpatialOperator { left, right, .. }
                    | QueryExpression::Compare { left, right, .. } => {
                        stack.push(Node::Expression(left, depth.saturating_add(1), position));
                        stack.push(Node::Expression(right, depth.saturating_add(1), position));
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
                        reject_window(position)?;
                        if !function.accepts_argument_count(arguments.len()) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "numero argomenti funzione window non valido",
                            ));
                        }
                        if let Some(frame) = frame {
                            validate_window_frame(frame)?;
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(
                                argument,
                                depth.saturating_add(1),
                                WindowPosition::Nested,
                            ));
                        }
                        for expression in partition_by {
                            stack.push(Node::Expression(
                                expression,
                                depth.saturating_add(1),
                                WindowPosition::Nested,
                            ));
                        }
                        for ordering in order_by {
                            stack.push(Node::Expression(
                                &ordering.expression,
                                depth.saturating_add(1),
                                WindowPosition::Nested,
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
                        // La stessa domanda, una risposta sola: prima la
                        // lista era scritta a mano qui e la variante ordinaria
                        // non ne sapeva niente.
                        if !function.is_window_only()
                            || !function.accepts_argument_count(arguments.len())
                        {
                            return Err(crate::DatabaseError::invalid_plan(
                                "funzione spatial window non valida",
                            ));
                        }
                        reject_window(position)?;
                        if let Some(frame) = frame {
                            validate_window_frame(frame)?;
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(
                                argument,
                                depth.saturating_add(1),
                                WindowPosition::Nested,
                            ));
                        }
                        for expression in partition_by {
                            stack.push(Node::Expression(
                                expression,
                                depth.saturating_add(1),
                                WindowPosition::Nested,
                            ));
                        }
                        for ordering in order_by {
                            stack.push(Node::Expression(
                                &ordering.expression,
                                depth.saturating_add(1),
                                WindowPosition::Nested,
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
                        stack.push(Node::Expression(
                            expression,
                            depth.saturating_add(1),
                            position,
                        ));
                        stack.push(Node::Operation(query, depth.saturating_add(1)));
                    }
                    QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
                        if arguments.is_empty() {
                            return Err(crate::DatabaseError::invalid_plan("gruppo query vuoto"));
                        }
                        for argument in arguments {
                            stack.push(Node::Expression(
                                argument,
                                depth.saturating_add(1),
                                position,
                            ));
                        }
                    }
                    QueryExpression::IsNull { expression, .. } => {
                        stack.push(Node::Expression(
                            expression,
                            depth.saturating_add(1),
                            position,
                        ));
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
            declared_crs: Vec::new(),
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
    fn query_walker_reaches_sources_inside_subqueries() {
        let nested = query_with_filter(QueryExpression::IsNull {
            expression: Box::new(QueryExpression::Parameter {
                name: "nested".to_owned(),
            }),
            negated: false,
        });
        let query = query_with_filter(QueryExpression::Exists {
            query: Box::new(nested),
            negated: false,
        });
        let mut sources = 0;
        let mut parameters = Vec::new();

        assert!(walk_query(&query, |node| {
            match node {
                QueryWalkNode::Source(_) => sources += 1,
                QueryWalkNode::Expression(QueryExpression::Parameter { name }) => {
                    parameters.push(name.as_str());
                }
                QueryWalkNode::Operation(_) | QueryWalkNode::Expression(_) => {}
            }
            QueryWalkControl::Continue
        }));

        assert_eq!(sources, 2);
        assert_eq!(parameters, ["nested"]);
    }

    #[test]
    fn query_walker_can_skip_or_break_a_subtree() {
        let expression = QueryExpression::And {
            arguments: vec![
                QueryExpression::Parameter {
                    name: "first".to_owned(),
                },
                QueryExpression::IsNull {
                    expression: Box::new(QueryExpression::Parameter {
                        name: "hidden".to_owned(),
                    }),
                    negated: false,
                },
            ],
        };
        let mut visited = Vec::new();
        assert!(walk_query_expression(&expression, |node| {
            if let QueryWalkNode::Expression(expression) = node {
                match expression {
                    QueryExpression::IsNull { .. } => return QueryWalkControl::Skip,
                    QueryExpression::Parameter { name } => visited.push(name.as_str()),
                    _ => {}
                }
            }
            QueryWalkControl::Continue
        }));
        assert_eq!(visited, ["first"]);

        assert!(!walk_query_expression(&expression, |_| {
            QueryWalkControl::Break
        }));
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
    fn trivial_predicate() -> QueryExpression {
        QueryExpression::Compare {
            left: Box::new(QueryExpression::Parameter {
                name: "left".to_owned(),
            }),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "right".to_owned(),
            }),
        }
    }

    fn row_number() -> QueryExpression {
        QueryExpression::Window {
            function: ScalarFunction::RowNumber,
            arguments: Vec::new(),
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }
    }

    /// Una query minima con la window nella posizione indicata dal chiamante.
    fn query_without_filter() -> QueryOperation {
        let mut query = query_with_filter(trivial_predicate());
        query.filter = None;
        query
    }

    #[test]
    fn a_window_in_the_projection_and_in_order_by_is_valid() {
        let mut query = query_without_filter();
        query.projection = vec![QueryProjection {
            expression: row_number(),
            alias: Some("position".to_owned()),
        }];
        query.order_by = vec![QueryOrdering {
            expression: row_number(),
            direction: SortDirection::Asc,
        }];
        validate_query_operation(&query, &Limits::default())
            .expect("le due sole clausole che ammettono una window");
    }

    #[test]
    fn a_window_in_the_order_by_of_a_set_operation_is_rejected() {
        // Lo stesso `ORDER BY` che sopra e valido diventa invalido appena la
        // query acquista un ramo: il renderer lo emette dopo l'unione.
        let mut query = query_without_filter();
        query.order_by = vec![QueryOrdering {
            expression: row_number(),
            direction: SortDirection::Asc,
        }];
        query.set_operations = vec![QuerySetOperation {
            operator: QuerySetOperator::Union,
            all: false,
            query: Box::new(query_without_filter()),
        }];
        let error = validate_query_operation(&query, &Limits::default())
            .expect_err("window nell'ORDER BY di una UNION");
        assert!(error.message.contains("set operation"), "{error:?}");
    }

    #[test]
    fn a_window_below_a_comparison_in_the_projection_stays_valid() {
        // `SELECT row_number() OVER () = $1 AS first` e SQL valido: la regola
        // e sulla clausola, non sulla profondita, e restringerla al solo nodo
        // di testa rifiuterebbe piani corretti.
        let mut query = query_without_filter();
        query.projection = vec![QueryProjection {
            expression: QueryExpression::Compare {
                left: Box::new(row_number()),
                operator: ComparisonOperator::Eq,
                right: Box::new(QueryExpression::Parameter {
                    name: "first".to_owned(),
                }),
            },
            alias: Some("first".to_owned()),
        }];
        validate_query_operation(&query, &Limits::default())
            .expect("una window annidata nella projection resta valida");
    }

    #[test]
    fn a_window_in_the_filter_is_rejected() {
        // `WHERE row_number() OVER () = $1`: il `Compare` supera il controllo
        // di booleanita, e senza la posizione sintattica il piano arrivava
        // intatto al provider.
        let query = query_with_filter(QueryExpression::Compare {
            left: Box::new(row_number()),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "first".to_owned(),
            }),
        });
        let error =
            validate_query_operation(&query, &Limits::default()).expect_err("window in WHERE");
        assert_eq!(error.category, crate::ErrorCategory::InvalidPlan);
        assert!(error.message.contains("fuori da projection"), "{error:?}");
    }

    #[test]
    fn a_window_in_group_by_and_in_having_is_rejected() {
        for clause in ["group_by", "having"] {
            let mut query = query_without_filter();
            let expression = QueryExpression::Compare {
                left: Box::new(row_number()),
                operator: ComparisonOperator::Eq,
                right: Box::new(QueryExpression::Parameter {
                    name: "first".to_owned(),
                }),
            };
            if clause == "group_by" {
                query.group_by = vec![expression];
            } else {
                query.having = Some(expression);
            }
            let error = validate_query_operation(&query, &Limits::default())
                .expect_err("window fuori clausola");
            assert_eq!(
                error.category,
                crate::ErrorCategory::InvalidPlan,
                "{clause}"
            );
        }
    }

    #[test]
    fn a_window_nested_in_another_window_is_rejected() {
        for position in ["argument", "partition_by", "order_by"] {
            let mut query = query_without_filter();
            let mut outer = QueryExpression::Window {
                function: ScalarFunction::Lag,
                arguments: vec![QueryExpression::Column {
                    column: ColumnRef {
                        relation: None,
                        field: "event_id".to_owned(),
                    },
                }],
                partition_by: Vec::new(),
                order_by: Vec::new(),
                frame: None,
            };
            if let QueryExpression::Window {
                arguments,
                partition_by,
                order_by,
                ..
            } = &mut outer
            {
                match position {
                    "argument" => arguments.push(row_number()),
                    "partition_by" => partition_by.push(row_number()),
                    _ => order_by.push(QueryOrdering {
                        expression: row_number(),
                        direction: SortDirection::Asc,
                    }),
                }
            }
            query.projection = vec![QueryProjection {
                expression: outer,
                alias: None,
            }];
            let error =
                validate_query_operation(&query, &Limits::default()).expect_err("window annidata");
            assert_eq!(
                error.category,
                crate::ErrorCategory::InvalidPlan,
                "{position}"
            );
            assert!(error.message.contains("annidata"), "{position}: {error:?}");
        }
    }

    fn geometry_column() -> QueryExpression {
        QueryExpression::Column {
            column: ColumnRef {
                relation: None,
                field: "geom".to_owned(),
            },
        }
    }

    fn lag_geometry() -> QueryExpression {
        QueryExpression::Window {
            function: ScalarFunction::Lag,
            arguments: vec![geometry_column()],
            partition_by: Vec::new(),
            order_by: Vec::new(),
            frame: None,
        }
    }

    fn spatial_projection(
        function: SpatialFunction,
        arguments: Vec<QueryExpression>,
    ) -> QueryOperation {
        let mut query = query_without_filter();
        query.projection = vec![QueryProjection {
            expression: QueryExpression::Spatial {
                function,
                arguments,
            },
            alias: Some("shape".to_owned()),
        }];
        query
    }

    /// `ST_Union` e `ST_Collect` esistono in due forme omonime: l'aggregata
    /// unaria su un insieme di righe e quella scalare che combina le geometrie
    /// che riceve. Solo la prima chiude la finestra ai suoi argomenti.
    #[test]
    fn a_window_inside_an_unambiguously_scalar_overload_stays_valid() {
        // `ST_Collect(x, y)` e `ST_Union(x, y, z)`: a quelle arita `PostGIS`
        // pubblica solo la forma scalare, quindi la window non e annidata in
        // nessuna aggregata e il piano resta valido.
        let collect = spatial_projection(
            SpatialFunction::Collect,
            vec![geometry_column(), lag_geometry()],
        );
        validate_query_operation(&collect, &Limits::default())
            .expect("ST_Collect binaria e solo scalare");

        let union = spatial_projection(
            SpatialFunction::Union,
            vec![geometry_column(), lag_geometry(), geometry_column()],
        );
        validate_query_operation(&union, &Limits::default())
            .expect("ST_Union ternaria e solo scalare");
    }

    #[test]
    fn a_window_inside_an_ambiguous_overload_is_rejected() {
        // `ST_Union(x, y)` puo essere l'aggregata `(geometry set, float8)`: il
        // piano non porta i tipi che lo escluderebbero, e rifiutare prima
        // della rete costa meno che scoprirlo dal server.
        let query = spatial_projection(
            SpatialFunction::Union,
            vec![geometry_column(), lag_geometry()],
        );
        let error = validate_query_operation(&query, &Limits::default())
            .expect_err("ST_Union binaria e ambigua");
        assert!(error.message.contains("annidata"), "{error:?}");
    }

    /// Dove `PostGIS` pubblica un'aggregata a quell'arita, la risposta e
    /// `true` — anche quando esiste **anche** una scalare.
    ///
    /// La tabella viene da `pg_proc.prokind` misurato su `PostGIS` 3.4, non da
    /// una lettura della documentazione. Il caso che conta e
    /// `ST_Union` a due argomenti: `(geometry, geometry)` e scalare e
    /// `(geometry set, float8)` e aggregata, e il piano non porta i tipi che
    /// li distinguerebbero. La risposta deve essere `true`: il renderer tipizza
    /// solo i `Parameter` e lascia passare invariata una `Column`.
    #[test]
    fn an_ambiguous_arity_answers_that_the_aggregate_is_possible() {
        // Ambigue: aggregata **o** scalare, e il piano non lo dice.
        assert!(SpatialFunction::Collect.is_aggregate(1));
        assert!(SpatialFunction::Union.is_aggregate(1));
        assert!(SpatialFunction::Union.is_aggregate(2));

        // Non ambigue: a quell'arita `PostGIS` ha solo la scalare.
        assert!(!SpatialFunction::Collect.is_aggregate(2));
        assert!(!SpatialFunction::Union.is_aggregate(3));

        // Solo aggregate, ma **alle arita che PostGIS pubblica**: fuori da
        // quelle la chiamata non esiste, e dirne qualcosa sarebbe una risposta
        // su un piano che la validazione delle arita rifiutera comunque.
        for (function, valid) in [
            (SpatialFunction::Extent, vec![1]),
            (SpatialFunction::AsMvt, vec![1, 2, 3, 4, 5]),
            (SpatialFunction::AsGeobuf, vec![1, 2]),
        ] {
            for count in 0..=6 {
                assert_eq!(
                    function.is_aggregate(count),
                    valid.contains(&count),
                    "{function:?} a {count}"
                );
                assert_eq!(
                    function.is_aggregate(count),
                    function.accepts_argument_count(count),
                    "{function:?} a {count}: aggregata e arita valida devono coincidere"
                );
            }
        }
        // Nemmeno le due ambigue rispondono fuori dalle proprie arita.
        assert!(!SpatialFunction::Collect.is_aggregate(0));
        assert!(!SpatialFunction::Union.is_aggregate(0));
        assert!(!SpatialFunction::Union.is_aggregate(4));

        // Nessuna delle due e una window: la domanda resta distinta.
        assert!(!SpatialFunction::Union.is_window_only());
        assert!(!SpatialFunction::Collect.is_window_only());
    }

    // Il fatto che rende ambigua l'arita — il renderer tipizza come geometria
    // solo i `Parameter`, e lascia passare invariata una `Column` — e fissato
    // dove quel comportamento vive: `plenora_database_sql`,
    // `a_column_in_a_geometry_position_is_not_typed_by_the_renderer`.

    #[test]
    fn a_window_inside_the_unary_spatial_aggregate_is_rejected() {
        for function in [SpatialFunction::Union, SpatialFunction::Collect] {
            let query = spatial_projection(function, vec![lag_geometry()]);
            let error = validate_query_operation(&query, &Limits::default())
                .expect_err("window dentro l'aggregata unaria");
            assert!(
                error.message.contains("annidata"),
                "{function:?}: {error:?}"
            );
        }
    }

    /// La stessa distinzione vale per `FOR UPDATE`: e l'aggregata a essere
    /// incompatibile con il locking di riga, non il nome della funzione.
    #[test]
    fn locking_survives_a_scalar_spatial_overload_and_not_the_aggregate() {
        let lock = QueryLock {
            strength: QueryLockStrength::Update,
            relations: Vec::new(),
            wait: QueryLockWait::Wait,
        };

        let mut scalar = spatial_projection(
            SpatialFunction::Collect,
            vec![geometry_column(), geometry_column()],
        );
        scalar.locking = Some(lock.clone());
        validate_query_operation(&scalar, &Limits::default())
            .expect("ST_Collect binaria non aggrega niente");

        let mut aggregate = spatial_projection(SpatialFunction::Union, vec![geometry_column()]);
        aggregate.locking = Some(lock);
        let error = validate_query_operation(&aggregate, &Limits::default())
            .expect_err("ST_Union unaria e aggregata");
        assert!(error.message.contains("locking non ammesso"), "{error:?}");
    }

    #[test]
    fn a_window_in_an_aggregate_argument_is_rejected() {
        let mut query = query_without_filter();
        query.projection = vec![QueryProjection {
            expression: QueryExpression::Scalar {
                function: ScalarFunction::Sum,
                arguments: vec![row_number()],
            },
            alias: None,
        }];
        let error = validate_query_operation(&query, &Limits::default())
            .expect_err("window dentro un'aggregata");
        assert!(error.message.contains("annidata"), "{error:?}");
    }

    #[test]
    fn a_window_in_the_projection_of_a_subquery_is_valid() {
        // Ogni operazione apre la propria projection: la posizione si
        // ricalcola, non si eredita dal contesto che contiene la subquery.
        let mut inner = query_without_filter();
        inner.projection = vec![QueryProjection {
            expression: row_number(),
            alias: None,
        }];
        let query = query_with_filter(QueryExpression::Compare {
            left: Box::new(QueryExpression::ScalarSubquery {
                query: Box::new(inner),
            }),
            operator: ComparisonOperator::Eq,
            right: Box::new(QueryExpression::Parameter {
                name: "first".to_owned(),
            }),
        });
        validate_query_operation(&query, &Limits::default())
            .expect("la subquery ha la propria projection");
    }

    /// Il filtro `spatial` del piano ammette esattamente i predicati che questo
    /// motore sa valutare come booleani.
    ///
    /// Il test confronta direttamente lo schema v2 con `returns_boolean`, cosi
    /// il contratto non puo omettere un predicato che l'engine valuta.
    ///
    /// `relate` resta fuori di proposito: e booleana solo con tre argomenti, e
    /// la forma del filtro nel piano ne prevede due.
    #[test]
    fn the_plan_admits_exactly_the_spatial_predicates_this_engine_evaluates() {
        /// La variante `spatial` del filtro, cercata per struttura: e l'unico
        /// sottoschema che fissa `op` a `spatial`.
        fn spatial_variant(node: &serde_json::Value) -> Option<&serde_json::Value> {
            if node
                .pointer("/properties/op/const")
                .and_then(serde_json::Value::as_str)
                == Some("spatial")
            {
                return Some(node);
            }
            match node {
                serde_json::Value::Object(map) => map.values().find_map(spatial_variant),
                serde_json::Value::Array(items) => items.iter().find_map(spatial_variant),
                _ => None,
            }
        }

        let schema: serde_json::Value =
            serde_json::from_str(include_str!("../../../contracts/v2/plan.schema.json"))
                .expect("schema del piano");

        let declared: std::collections::BTreeSet<String> = spatial_variant(&schema)
            .and_then(|variant| variant.pointer("/properties/function/enum"))
            .and_then(serde_json::Value::as_array)
            .expect("enum delle funzioni nel filtro spatial")
            .iter()
            .map(|item| item.as_str().expect("funzione come stringa").to_owned())
            .collect();

        let evaluated: std::collections::BTreeSet<String> = SpatialFunction::ALL
            .iter()
            .filter(|function| function.returns_boolean(2) && **function != SpatialFunction::Relate)
            .map(|function| {
                serde_json::to_value(function)
                    .expect("funzione serializzabile")
                    .as_str()
                    .expect("funzione come stringa")
                    .to_owned()
            })
            .collect();

        assert_eq!(declared, evaluated);
    }
}
