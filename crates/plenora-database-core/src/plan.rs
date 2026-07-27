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
    Arcgis,
}

impl ProviderKind {
    #[must_use]
    pub fn is_sql(self) -> bool {
        self != Self::Arcgis
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRef {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub object: String,
    pub layer_id: Option<LayerId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LayerId {
    Number(u64),
    Name(String),
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
        case_insensitive: bool,
    },
    Spatial {
        function: crate::query::SpatialFunction,
        field: String,
        geometry_parameter: Option<String>,
        distance_parameter: Option<String>,
    },
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
    pub filter: Option<FilterExpression>,
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
    ArcgisApplyEdits,
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
    #[serde(rename = "arcgis.test_connection")]
    ArcgisTestConnection,
    #[serde(rename = "database.list_catalogs")]
    DatabaseListCatalogs,
    #[serde(rename = "database.list_schemas")]
    DatabaseListSchemas { source: Option<ObjectRef> },
    #[serde(rename = "database.list_objects")]
    DatabaseListObjects { source: Option<ObjectRef> },
    #[serde(rename = "database.describe_object")]
    DatabaseDescribeObject { source: ObjectRef },
    #[serde(rename = "arcgis.list_folders")]
    ArcgisListFolders,
    #[serde(rename = "arcgis.list_items")]
    ArcgisListItems { source: Option<ObjectRef> },
    #[serde(rename = "arcgis.list_services")]
    ArcgisListServices { source: Option<ObjectRef> },
    #[serde(rename = "arcgis.list_layers")]
    ArcgisListLayers { source: ObjectRef },
    #[serde(rename = "arcgis.describe_layer")]
    ArcgisDescribeLayer { source: ObjectRef },
    #[serde(rename = "database.read")]
    DatabaseRead {
        #[serde(flatten)]
        read: ReadOperation,
    },
    #[serde(rename = "arcgis.read")]
    ArcgisRead {
        #[serde(flatten)]
        read: ReadOperation,
    },
    #[serde(rename = "database.write")]
    DatabaseWrite {
        #[serde(flatten)]
        write: WriteOperation,
    },
    #[serde(rename = "arcgis.write")]
    ArcgisWrite {
        #[serde(flatten)]
        write: WriteOperation,
    },
}

impl Operation {
    #[must_use]
    pub const fn is_arcgis(&self) -> bool {
        matches!(
            self,
            Self::ArcgisTestConnection
                | Self::ArcgisListFolders
                | Self::ArcgisListItems { .. }
                | Self::ArcgisListServices { .. }
                | Self::ArcgisListLayers { .. }
                | Self::ArcgisDescribeLayer { .. }
                | Self::ArcgisRead { .. }
                | Self::ArcgisWrite { .. }
        )
    }
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
