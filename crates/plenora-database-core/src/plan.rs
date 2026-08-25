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
mod like_default_tests {
    use super::FilterExpression;

    /// `$defs` del filtro `like` non elenca `case_insensitive` fra i
    /// `required`: un documento che lo omette e valido, e prima del
    /// `#[serde(default)]` non si deserializzava affatto — il piano falliva
    /// alla lettura, prima di qualunque validatore, e l'errore parlava di un
    /// campo mancante come se il contratto lo pretendesse.
    #[test]
    fn a_like_filter_without_case_insensitive_is_read_as_case_sensitive() {
        let filter: FilterExpression =
            serde_json::from_str(r#"{"op":"like","field":"nome","parameter":"needle"}"#)
                .expect("il contratto ammette l'omissione");
        assert_eq!(
            filter,
            FilterExpression::Like {
                field: "nome".to_owned(),
                parameter: "needle".to_owned(),
                case_insensitive: false,
            }
        );
    }
}
