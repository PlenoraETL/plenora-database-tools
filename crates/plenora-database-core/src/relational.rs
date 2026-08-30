//! IR relazionale canonico e provider-neutral.
//!
//! Contiene solo riferimenti a colonne e parametri: i valori restano nel
//! `ParameterBag` e non possono essere interpolati nel testo SQL.

use crate::geometry::SpatialSemantics;
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
    TypedParameter {
        name: String,
        parameter_type: QueryParameterType,
    },
    Scalar {
        function: ScalarFunction,
        arguments: Vec<Self>,
    },
    Spatial {
        function: SpatialFunction,
        arguments: Vec<Self>,
    },
    /// Serializza sul filo una colonna spatial nel formato binario canonico.
    ///
    /// Non e una funzione SQL scelta dal consumer: il renderer qualificato
    /// decide l'involucro del provider (per esempio `ST_AsEWKB` su Postgres).
    SpatialOutput {
        expression: Box<Self>,
        semantics: SpatialSemantics,
    },
    /// Costruisce un valore spatial da un bind EWKB e dal frame dichiarato.
    ///
    /// Il payload resta nel bind; l'IR porta soltanto metadati e struttura.
    SpatialValue {
        expression: Box<Self>,
        srid: u32,
        semantics: SpatialSemantics,
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
    InList {
        expression: Box<Self>,
        values: Vec<Self>,
        #[serde(default)]
        negated: bool,
    },
    Between {
        expression: Box<Self>,
        lower: Box<Self>,
        upper: Box<Self>,
        #[serde(default)]
        negated: bool,
    },
    Like {
        expression: Box<Self>,
        pattern: Box<Self>,
        #[serde(default)]
        case_insensitive: bool,
        #[serde(default)]
        negated: bool,
    },
    Not {
        expression: Box<Self>,
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

/// Tipo logico chiuso di un bind relazionale.
///
/// Non contiene SQL nativo: il renderer sceglie la dichiarazione corretta per
/// il dialect, cosi un hint di tipo non diventa un escape hatch testuale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryParameterType {
    Boolean,
    Integer,
    BigInteger,
    Float,
    String,
    Binary,
    Date,
    Timestamp,
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

/// Assegnazione DML canonica; il valore resta una espressione senza payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationAssignment {
    pub column: String,
    pub value: QueryExpression,
}

/// Inserimento relazionale. Le righe contengono espressioni allineate alle
/// colonne e non valori applicativi.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InsertOperation {
    pub target: crate::plan::ObjectRef,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryExpression>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// Aggiornamento relazionale con predicato canonico condiviso dalle query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateOperation {
    pub target: crate::plan::ObjectRef,
    pub assignments: Vec<MutationAssignment>,
    pub filter: Option<QueryExpression>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// Cancellazione relazionale con eventuale proiezione delle righe eliminate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteOperation {
    pub target: crate::plan::ObjectRef,
    pub filter: Option<QueryExpression>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// Inserimento con gestione atomica del conflitto.
///
/// Il bersaglio resta esplicito anche sui prodotti che lo ricavano dagli indici unici: e parte
/// del significato portabile dell'operazione e serve ai lowering che devono
/// costruire il predicato di match.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertOperation {
    pub target: crate::plan::ObjectRef,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<QueryExpression>>,
    pub conflict_target: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub update_on_conflict: Vec<MutationAssignment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// Mutazioni del Core v3. `SELECT` resta [`QueryOperation`] per compatibilita
/// seriale; questo enum e il nuovo confine DML additivo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MutationOperation {
    Insert(InsertOperation),
    Update(UpdateOperation),
    Delete(DeleteOperation),
    Upsert(UpsertOperation),
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
        QueryExpression::InList {
            expression, values, ..
        } => {
            stack.extend(values.iter().rev().map(QueryWalkNode::Expression));
            stack.push(QueryWalkNode::Expression(expression));
        }
        QueryExpression::Between {
            expression,
            lower,
            upper,
            ..
        } => {
            stack.push(QueryWalkNode::Expression(upper));
            stack.push(QueryWalkNode::Expression(lower));
            stack.push(QueryWalkNode::Expression(expression));
        }
        QueryExpression::Like {
            expression,
            pattern,
            ..
        } => {
            stack.push(QueryWalkNode::Expression(pattern));
            stack.push(QueryWalkNode::Expression(expression));
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
        QueryExpression::SpatialOutput { expression, .. }
        | QueryExpression::SpatialValue { expression, .. }
        | QueryExpression::IsNull { expression, .. }
        | QueryExpression::Not { expression } => {
            stack.push(QueryWalkNode::Expression(expression));
        }
        QueryExpression::Wildcard { .. }
        | QueryExpression::Column { .. }
        | QueryExpression::Parameter { .. }
        | QueryExpression::TypedParameter { .. } => {}
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

    fn push_window_children<'a>(
        stack: &mut Vec<Node<'a>>,
        depth: usize,
        arguments: &'a [QueryExpression],
        partition_by: &'a [QueryExpression],
        order_by: &'a [QueryOrdering],
    ) {
        let child_depth = depth.saturating_add(1);
        let nested = |value| Node::Expression(value, child_depth, WindowPosition::Nested);
        stack.extend(arguments.iter().chain(partition_by).map(nested));
        stack.extend(order_by.iter().map(|ordering| nested(&ordering.expression)));
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
                | QueryExpression::InList { .. }
                | QueryExpression::Between { .. }
                | QueryExpression::Like { .. }
                | QueryExpression::IsNull { .. }
                | QueryExpression::Exists { .. }
                | QueryExpression::InSubquery { .. } => {}
                QueryExpression::And { arguments } | QueryExpression::Or { arguments } => {
                    stack.extend(arguments);
                }
                QueryExpression::Not { expression } => stack.push(expression),
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

    fn contains_direct_source_reference(expression: &QueryExpression) -> bool {
        !walk_query_expression(expression, |node| match node {
            QueryWalkNode::Expression(
                QueryExpression::Column { .. } | QueryExpression::Wildcard { .. },
            ) => QueryWalkControl::Break,
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
                    (None, None)
                        if operation.joins.is_empty()
                            && operation.locking.is_none()
                            && operation.declared_crs.is_empty()
                            && !operation
                                .clause_expressions()
                                .any(contains_direct_source_reference) => {}
                    _ => {
                        return Err(crate::DatabaseError::invalid_plan(
                            "query con riferimenti relazionali richiede una sola source",
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
                    QueryExpression::Parameter { name }
                    | QueryExpression::TypedParameter { name, .. } => identifier(name, 256)?,
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
                    QueryExpression::SpatialOutput { expression, .. } => {
                        if !matches!(expression.as_ref(), QueryExpression::Column { .. }) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "projection spatial richiede una colonna",
                            ));
                        }
                        stack.push(Node::Expression(
                            expression,
                            depth.saturating_add(1),
                            position,
                        ));
                    }
                    QueryExpression::SpatialValue {
                        expression, srid, ..
                    } => {
                        if *srid == 0 {
                            return Err(crate::DatabaseError::invalid_plan(
                                "valore spatial richiede SRID positivo",
                            ));
                        }
                        if !matches!(
                            expression.as_ref(),
                            QueryExpression::Parameter { .. }
                                | QueryExpression::TypedParameter { .. }
                        ) {
                            return Err(crate::DatabaseError::invalid_plan(
                                "valore spatial richiede un bind",
                            ));
                        }
                        stack.push(Node::Expression(
                            expression,
                            depth.saturating_add(1),
                            position,
                        ));
                    }
                    QueryExpression::SpatialOperator { left, right, .. }
                    | QueryExpression::Compare { left, right, .. } => {
                        stack.push(Node::Expression(left, depth.saturating_add(1), position));
                        stack.push(Node::Expression(right, depth.saturating_add(1), position));
                    }
                    QueryExpression::InList {
                        expression, values, ..
                    } => {
                        if values.is_empty() {
                            return Err(crate::DatabaseError::invalid_plan("lista IN query vuota"));
                        }
                        stack.push(Node::Expression(
                            expression,
                            depth.saturating_add(1),
                            position,
                        ));
                        for value in values {
                            stack.push(Node::Expression(value, depth.saturating_add(1), position));
                        }
                    }
                    QueryExpression::Between {
                        expression,
                        lower,
                        upper,
                        ..
                    } => {
                        for child in [expression, lower, upper] {
                            stack.push(Node::Expression(child, depth.saturating_add(1), position));
                        }
                    }
                    QueryExpression::Like {
                        expression,
                        pattern,
                        ..
                    } => {
                        for child in [expression, pattern] {
                            stack.push(Node::Expression(child, depth.saturating_add(1), position));
                        }
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
                        push_window_children(&mut stack, depth, arguments, partition_by, order_by);
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
                        push_window_children(&mut stack, depth, arguments, partition_by, order_by);
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
                    QueryExpression::IsNull { expression, .. }
                    | QueryExpression::Not { expression } => {
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
#[path = "query_validation_tests.rs"]
mod validation_tests;
