//! Piano provider-neutral per letture, scritture e ispezioni.

use crate::limits::Limits;
use crate::loss::MappingPolicy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderKind {
    Postgres,
    Mysql,
    Mariadb,
    Sqlserver,
    Oracle,
    Db2,
    Sqlite,
    Duckdb,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub object: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderBy {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparisonOperator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum FilterExpression {
    And {
        args: Vec<Self>,
    },
    Or {
        args: Vec<Self>,
    },
    Eq {
        field: String,
        parameter: String,
    },
    Ne {
        field: String,
        parameter: String,
    },
    Lt {
        field: String,
        parameter: String,
    },
    Lte {
        field: String,
        parameter: String,
    },
    Gt {
        field: String,
        parameter: String,
    },
    Gte {
        field: String,
        parameter: String,
    },
    IsNull {
        field: String,
    },
    IsNotNull {
        field: String,
    },
    In {
        field: String,
        parameters: Vec<String>,
    },
    Between {
        field: String,
        lower_parameter: String,
        upper_parameter: String,
    },
    Like {
        field: String,
        parameter: String,
        /// Lo schema non lo elenca fra i `required`: un filtro `like` senza
        /// questo campo e un documento v2 valido, e senza `default` la
        /// deserializzazione lo rifiutava **prima** di qualunque validatore —
        /// il piano non arrivava neppure a essere giudicato. Assente vuol dire
        /// `false`, cioe il confronto sensibile alle maiuscole.
        #[serde(default)]
        case_insensitive: bool,
    },
    Spatial {
        function: crate::query::SpatialFunction,
        field: String,
        geometry_parameter: Option<String>,
        distance_parameter: Option<String>,
    },
}

impl FilterExpression {
    /// Verifica una proprieta su ogni campo referenziato dal filtro.
    ///
    /// La visita vive accanto all'AST: i provider non devono replicare un
    /// match esaustivo ogni volta che una nuova forma di filtro viene
    /// aggiunta al contratto.
    pub fn all_fields(&self, predicate: &impl Fn(&str) -> bool) -> bool {
        match self {
            Self::And { args } | Self::Or { args } => {
                args.iter().all(|argument| argument.all_fields(predicate))
            }
            Self::Eq { field, .. }
            | Self::Ne { field, .. }
            | Self::Lt { field, .. }
            | Self::Lte { field, .. }
            | Self::Gt { field, .. }
            | Self::Gte { field, .. }
            | Self::IsNull { field }
            | Self::IsNotNull { field }
            | Self::In { field, .. }
            | Self::Between { field, .. }
            | Self::Like { field, .. }
            | Self::Spatial { field, .. } => predicate(field),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadOperation {
    pub source: ObjectRef,
    #[serde(default)]
    pub projection: Vec<String>,
    #[serde(default)]
    pub order_by: Vec<OrderBy>,
    pub row_limit: Option<u64>,
    /// Le prime righe da saltare, cioe la finestra.
    ///
    /// Un piano di lettura non poteva chiederla: il campo esisteva su
    /// `QueryOperation` e non qui, mentre `ReadCapabilities::pagination` —
    /// che sta nella sezione della **lettura** — prometteva proprio questo. Un
    /// campo del contratto che descrive una superficie che la sua operazione
    /// non espone e una promessa che nessun piano puo riscuotere, ed e
    /// esattamente cio che quella bandiera era.
    ///
    /// Additivo e opzionale: un piano che non lo dichiara si comporta come
    /// prima. Cio che cambia e che ora la bandiera ha qualcosa da governare —
    /// l'engine rifiuta un offset a un provider che non lo pubblica — e i
    /// provider hanno qualcosa da rendere.
    ///
    /// Come `row_limit`, pretende `order_by`: una finestra su un risultato
    /// non ordinato non e riproducibile, e due letture consecutive possono
    /// rendere righe diverse. La regola e quella che `QueryOperation` ha gia,
    /// e vale qui per la stessa ragione.
    #[serde(default)]
    pub row_offset: Option<u64>,
    pub filter: Option<FilterExpression>,
    /// Il CRS che il chiamante dichiara per una colonna geometrica.
    ///
    /// Esiste per i prodotti in cui il catalogo **non puo** dirlo. Su `MariaDB`
    /// il registro OGC c'e e porta una colonna `SRID`, ma vale sempre zero:
    /// nessuna DDL puo vincolare una geometry a un sistema di riferimento — ne
    /// `SRID 4326`, ne `REF_SYSTEM_ID=4326`, entrambe rifiutate con 1064 su
    /// tutte le versioni misurate. Non e che l'SRID sia sconosciuto: e
    /// **assente**, e nessuna query di catalogo lo fara comparire.
    ///
    /// Senza una dichiarazione quei provider rifiutano la colonna, ed e
    /// l'unica risposta onesta: il contratto `GeoArrow` pubblica un CRS, e
    /// pubblicarlo senza saperlo sarebbe peggio del rifiuto.
    ///
    /// La dichiarazione non e una promessa che si crede sulla parola. Un
    /// provider che l'accetta deve **verificarla valore per valore** — e
    /// `ProviderCapabilities` lo dichiara con
    /// [`crate::capabilities::SpatialCapabilities::requires_declared_crs`] —
    /// perche una colonna che nessuna DDL vincola puo contenere geometrie con
    /// SRID diversi fra loro, e credere alla dichiarazione trasformerebbe
    /// quella eterogeneita in un CRS pubblicato falso.
    ///
    /// Additivo e opzionale: un piano che non lo dichiara si comporta come
    /// prima. Una dichiarazione su una colonna che il catalogo sa gia
    /// descrivere e un errore, non un rinforzo: due fonti per lo stesso fatto
    /// sono una fonte di troppo.
    #[serde(default)]
    pub declared_crs: Vec<DeclaredCrs>,
}

/// Il CRS di una colonna, dichiarato da chi scrive il piano.
///
/// Vedi [`ReadOperation::declared_crs`] per il perche.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeclaredCrs {
    pub column: String,
    /// L'identificatore del sistema di riferimento, nel registro del prodotto.
    ///
    /// Zero non e ammesso, ed e la ragione per cui questo campo esiste: zero e
    /// l'«indefinito» OGC, cioe esattamente cio che il registro di `MariaDB`
    /// gia risponde da solo. Dichiararlo non aggiungerebbe niente a cio che il
    /// catalogo dice, e darebbe l'aria di averlo fatto.
    pub srid: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    Create,
    Append,
    Replace,
    TruncateInsert,
    Update,
    Upsert,
    DeleteByKeys,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionProfile {
    ReadOnly,
    SingleTransaction,
    StagedSwap,
    ChunkCommitted,
    BestEffortDdl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SridPolicy {
    RequireMatch,
    AllowUnknown,
    RejectSpatial,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WriteOperation {
    pub target: ObjectRef,
    pub mode: WriteMode,
    pub mapping_policy: MappingPolicy,
    pub transaction_profile: TransactionProfile,
    #[serde(default)]
    pub keys: Vec<String>,
    #[serde(default)]
    pub update_columns: Vec<String>,
    pub srid_policy: Option<SridPolicy>,
    #[serde(default)]
    pub create_spatial_index: bool,
    #[serde(default)]
    pub allow_partial: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "id", deny_unknown_fields)]
pub enum Operation {
    #[serde(rename = "database.test_connection")]
    DatabaseTestConnection,
    #[serde(rename = "database.list_catalogs")]
    DatabaseListCatalogs,
    #[serde(rename = "database.list_schemas")]
    DatabaseListSchemas { source: Option<ObjectRef> },
    #[serde(rename = "database.list_objects")]
    DatabaseListObjects { source: Option<ObjectRef> },
    #[serde(rename = "database.describe_object")]
    DatabaseDescribeObject { source: ObjectRef },
    #[serde(rename = "database.read")]
    DatabaseRead {
        #[serde(flatten)]
        read: ReadOperation,
    },
    #[serde(rename = "database.write")]
    DatabaseWrite {
        #[serde(flatten)]
        write: WriteOperation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub schema_version: u32,
    pub connection_ref: String,
    pub provider: ProviderKind,
    pub operation: Operation,
    #[serde(default)]
    pub limits: Limits,
}

#[cfg(test)]
#[path = "plan_like_default_tests.rs"]
mod like_default_tests;
