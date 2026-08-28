use super::{
    Column, Constraint, Db2ColumnMetadata, ForeignKey, Index, IndexElement, Inspection, MetaData,
    MysqlColumnMetadata, MysqlTableMetadata, NativeColumnMetadata, NativeConstraintMetadata,
    NativeIndexElementMetadata, NativeIndexMetadata, NativeTableMetadata, ObjectRef, Observation,
    PostgresColumnMetadata, PostgresCompositeField, PostgresPartition, PostgresPolicy,
    PostgresPrivilege, PostgresRelationRef, PostgresTableMetadata, ProviderKind, Result,
    SchemaToken, SpatialColumnMetadata, SqlServerColumnMetadata, SqlServerSpatialBoundingBox,
    SqlServerSpatialIndexMetadata, SqlServerTableMetadata, Table,
};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase};
use serde::de::DeserializeOwned;
use serde::Deserialize;

pub(super) fn mapping_error(message: &'static str) -> DatabaseError {
    DatabaseError::new(ErrorCategory::DataMapping, ErrorPhase::Probe, None, message)
}

fn parse<T: DeserializeOwned>(inspection: Inspection) -> Result<T> {
    if inspection.operation != "database.describe_object" {
        return Err(mapping_error(
            "operazione incompatibile con la reflection di un oggetto",
        ));
    }
    serde_json::from_value(inspection.document)
        .map_err(|_| mapping_error("documento reflection incompatibile con il provider"))
}

pub(super) fn metadata_from_inspection(
    provider: ProviderKind,
    source: &ObjectRef,
    inspection: Inspection,
) -> Result<MetaData> {
    let table = match provider {
        ProviderKind::Postgres => postgres(source, parse(inspection)?),
        ProviderKind::Mysql => mysql(false, parse(inspection)?),
        ProviderKind::Mariadb => mysql(true, parse(inspection)?),
        ProviderKind::Sqlserver => sqlserver(parse(inspection)?),
        ProviderKind::Db2 => db2(parse(inspection)?),
        unsupported => {
            return Err(DatabaseError::unsupported(
                unsupported,
                ErrorPhase::Probe,
                "reflection tipizzata non disponibile per il provider",
            ));
        }
    };
    Ok(MetaData {
        provider,
        tables: vec![table].into_boxed_slice(),
    })
}

#[derive(Deserialize)]
struct PostgresDocument {
    columns: Vec<PostgresColumnWire>,
    schema_token: PostgresTokenWire,
    relation: PostgresRelationWire,
    constraints: Vec<PostgresConstraintWire>,
    indexes: Vec<PostgresIndexWire>,
    #[serde(default)]
    policies: Vec<PostgresPolicyWire>,
    #[serde(default)]
    privileges: Vec<PostgresPrivilegeWire>,
}

#[derive(Deserialize)]
struct PostgresTokenWire {
    schema_version: u32,
    database_oid: u32,
    namespace_oid: u32,
    relation_oid: u32,
    structural_fingerprint: String,
}

#[derive(Deserialize)]
#[allow(clippy::struct_excessive_bools)]
struct PostgresRelationWire {
    kind: String,
    is_partition: bool,
    partition_key: Option<String>,
    view_definition: Option<String>,
    comment: Option<String>,
    row_security: bool,
    force_row_security: bool,
    replica_identity: String,
    persistence: String,
    is_populated: bool,
    partition_bound: Option<String>,
    owner: String,
    tablespace: String,
    #[serde(default)]
    parents: Vec<PostgresRelationRefWire>,
    #[serde(default)]
    partitions: Vec<PostgresPartitionWire>,
}

#[derive(Deserialize)]
struct PostgresRelationRefWire {
    schema: String,
    name: String,
}

#[derive(Deserialize)]
struct PostgresPartitionWire {
    schema: String,
    name: String,
    bound: Option<String>,
}

#[derive(Deserialize)]
struct PostgresColumnWire {
    name: String,
    native_type: String,
    nullable: bool,
    numeric_precision: Option<u8>,
    numeric_scale: Option<i8>,
    spatial_srid: Option<u32>,
    spatial_dimensions: Option<String>,
    spatial_type: Option<String>,
    spatial_crs_id: Option<String>,
    default_expression: Option<String>,
    identity_kind: Option<String>,
    generated_kind: Option<String>,
    native_declaration: Option<String>,
    type_kind: Option<String>,
    #[serde(default)]
    composite_fields: Vec<PostgresCompositeFieldWire>,
    #[serde(default)]
    enum_labels: Vec<String>,
    domain_base_type: Option<String>,
    #[serde(default)]
    domain_constraints: Vec<String>,
    collation: Option<String>,
}

#[derive(Deserialize)]
struct PostgresCompositeFieldWire {
    name: String,
    declaration: String,
}

#[derive(Deserialize)]
struct PostgresConstraintWire {
    name: String,
    kind: String,
    definition: Option<String>,
    validated: bool,
    deferrable: bool,
    initially_deferred: bool,
    #[serde(default)]
    columns: Vec<String>,
    referenced_schema: Option<String>,
    referenced_object: Option<String>,
    #[serde(default)]
    referenced_columns: Vec<String>,
    on_update: Option<String>,
    on_delete: Option<String>,
    #[serde(rename = "match")]
    match_kind: Option<String>,
}

#[derive(Deserialize)]
#[allow(clippy::struct_excessive_bools)]
struct PostgresIndexWire {
    name: String,
    primary: bool,
    unique: bool,
    valid: bool,
    method: String,
    definition: String,
    ready: bool,
    clustered: bool,
    predicate: Option<String>,
    size_bytes: i64,
    #[serde(default)]
    keys: Vec<PostgresIndexElementWire>,
    spatial: bool,
}

#[derive(Deserialize)]
struct PostgresIndexElementWire {
    position: u64,
    expression: String,
    opclass: String,
    included: bool,
}

#[derive(Deserialize)]
struct PostgresPolicyWire {
    name: String,
    permissive: bool,
    command: String,
    #[serde(default)]
    roles: Vec<String>,
    #[serde(rename = "using")]
    using_expression: Option<String>,
    #[serde(rename = "check")]
    check_expression: Option<String>,
}

#[derive(Deserialize)]
struct PostgresPrivilegeWire {
    grantee: String,
    privilege: String,
    grantable: bool,
}

#[allow(clippy::too_many_lines)]
fn postgres(source: &ObjectRef, document: PostgresDocument) -> Table {
    let PostgresDocument {
        columns,
        schema_token,
        relation,
        constraints,
        indexes,
        policies,
        privileges,
    } = document;
    let foreign_keys = constraints
        .iter()
        .filter_map(|constraint| {
            constraint
                .referenced_object
                .as_ref()
                .map(|referenced_object| ForeignKey {
                    name: constraint.name.clone(),
                    columns: Observation::Observed(constraint.columns.clone().into_boxed_slice()),
                    referenced_schema: constraint.referenced_schema.clone(),
                    referenced_object: referenced_object.clone(),
                    referenced_columns: Observation::Observed(
                        constraint.referenced_columns.clone().into_boxed_slice(),
                    ),
                    on_update: constraint.on_update.clone(),
                    on_delete: constraint.on_delete.clone(),
                    match_kind: constraint.match_kind.clone(),
                })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let spatial = |column: &PostgresColumnWire| {
        (column.native_type == "geometry"
            || column.native_type == "geography"
            || column.spatial_srid.is_some()
            || column.spatial_type.is_some())
        .then(|| SpatialColumnMetadata {
            srid: column.spatial_srid,
            dimensions: column.spatial_dimensions.clone(),
            geometry_type: column.spatial_type.clone(),
            crs_id: column.spatial_crs_id.clone(),
        })
    };
    let columns = columns
        .into_iter()
        .map(|column| {
            let spatial = spatial(&column);
            Column {
                name: column.name,
                ordinal: None,
                native_type: column.native_type,
                native_declaration: column.native_declaration,
                nullable: Some(column.nullable),
                default_expression: column.default_expression,
                identity: Some(column.identity_kind.is_some()),
                generated: Some(column.generated_kind.is_some()),
                numeric_precision: column.numeric_precision.map(u64::from),
                numeric_scale: column.numeric_scale.map(i64::from),
                spatial,
                native: NativeColumnMetadata::Postgres(PostgresColumnMetadata {
                    identity_kind: column.identity_kind,
                    generated_kind: column.generated_kind,
                    type_kind: column.type_kind,
                    composite_fields: column
                        .composite_fields
                        .into_iter()
                        .map(|field| PostgresCompositeField {
                            name: field.name,
                            declaration: field.declaration,
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                    enum_labels: column.enum_labels.into_boxed_slice(),
                    domain_base_type: column.domain_base_type,
                    domain_constraints: column.domain_constraints.into_boxed_slice(),
                    collation: column.collation,
                }),
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let indexes = indexes
        .into_iter()
        .map(|index| Index {
            name: Some(index.name),
            unique: Some(index.unique),
            primary: Some(index.primary),
            elements: Observation::Observed(
                index
                    .keys
                    .into_iter()
                    .map(|key| IndexElement {
                        expression: key.expression,
                        included: Some(key.included),
                        descending: None,
                        native: NativeIndexElementMetadata::Postgres {
                            position: key.position,
                            opclass: key.opclass,
                        },
                    })
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            predicate: index.predicate,
            spatial: Some(index.spatial),
            native: NativeIndexMetadata::Postgres {
                valid: index.valid,
                method: index.method,
                definition: index.definition,
                ready: index.ready,
                clustered: index.clustered,
                size_bytes: index.size_bytes,
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let constraints = constraints
        .into_iter()
        .map(|constraint| Constraint {
            name: constraint.name,
            kind: constraint.kind,
            definition: constraint.definition,
            columns: Observation::Observed(constraint.columns.into_boxed_slice()),
            native: NativeConstraintMetadata::Postgres {
                validated: constraint.validated,
                deferrable: constraint.deferrable,
                initially_deferred: constraint.initially_deferred,
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let native = NativeTableMetadata::Postgres(Box::new(PostgresTableMetadata {
        is_partition: relation.is_partition,
        partition_key: relation.partition_key,
        view_definition: relation.view_definition,
        comment: relation.comment,
        row_security: relation.row_security,
        force_row_security: relation.force_row_security,
        replica_identity: relation.replica_identity,
        persistence: relation.persistence,
        is_populated: relation.is_populated,
        partition_bound: relation.partition_bound,
        owner: relation.owner,
        tablespace: relation.tablespace,
        parents: relation
            .parents
            .into_iter()
            .map(|parent| PostgresRelationRef {
                schema: parent.schema,
                name: parent.name,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        partitions: relation
            .partitions
            .into_iter()
            .map(|partition| PostgresPartition {
                schema: partition.schema,
                name: partition.name,
                bound: partition.bound,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        policies: policies
            .into_iter()
            .map(|policy| PostgresPolicy {
                name: policy.name,
                permissive: policy.permissive,
                command: policy.command,
                roles: policy.roles.into_boxed_slice(),
                using_expression: policy.using_expression,
                check_expression: policy.check_expression,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        privileges: privileges
            .into_iter()
            .map(|privilege| PostgresPrivilege {
                grantee: privilege.grantee,
                privilege: privilege.privilege,
                grantable: privilege.grantable,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }));
    Table {
        catalog: source.catalog.clone(),
        schema: Some(source.schema.clone().unwrap_or_else(|| "public".to_owned())),
        name: source.object.clone(),
        kind: relation.kind,
        schema_token: SchemaToken::Postgres {
            schema_version: schema_token.schema_version,
            database_oid: schema_token.database_oid,
            namespace_oid: schema_token.namespace_oid,
            relation_oid: schema_token.relation_oid,
            structural_fingerprint: schema_token.structural_fingerprint,
        },
        columns,
        indexes: Observation::Observed(indexes),
        constraints: Observation::Observed(constraints),
        foreign_keys: Observation::Observed(foreign_keys),
        native,
    }
}

#[derive(Deserialize)]
struct MysqlDocument {
    schema: String,
    name: String,
    kind: String,
    engine: Option<String>,
    columns: Vec<MysqlColumnWire>,
    indexes: Vec<MysqlIndexWire>,
    token: String,
}

#[derive(Deserialize)]
struct MysqlColumnWire {
    name: String,
    ordinal: u64,
    data_type: String,
    native_declaration: String,
    nullable: bool,
    default_expression: Option<String>,
    character_set: Option<String>,
    collation: Option<String>,
    numeric_precision: Option<u64>,
    numeric_scale: Option<u64>,
    datetime_precision: Option<u64>,
    spatial_srid: Option<u32>,
    extra: String,
    generation_expression: String,
}

#[derive(Deserialize)]
struct MysqlIndexWire {
    name: String,
    unique: bool,
    column_backed: bool,
    columns: Vec<String>,
}

#[allow(clippy::too_many_lines)]
fn mysql(mariadb: bool, document: MysqlDocument) -> Table {
    let columns = document
        .columns
        .into_iter()
        .map(|column| {
            let spatial = (column.spatial_srid.is_some() || mysql_spatial_type(&column.data_type))
                .then_some(SpatialColumnMetadata {
                    srid: column.spatial_srid,
                    dimensions: None,
                    geometry_type: Some(column.data_type.clone()),
                    crs_id: None,
                });
            let identity = column.extra.to_ascii_lowercase().contains("auto_increment");
            let generated = !column.generation_expression.is_empty()
                || column.extra.to_ascii_lowercase().contains("generated");
            let native = MysqlColumnMetadata {
                character_set: column.character_set,
                collation: column.collation,
                datetime_precision: column.datetime_precision,
                extra: column.extra,
                generation_expression: column.generation_expression,
            };
            Column {
                name: column.name,
                ordinal: Some(column.ordinal),
                native_type: column.data_type,
                native_declaration: Some(column.native_declaration),
                nullable: Some(column.nullable),
                default_expression: column.default_expression,
                identity: Some(identity),
                generated: Some(generated),
                numeric_precision: column.numeric_precision,
                numeric_scale: column
                    .numeric_scale
                    .and_then(|value| i64::try_from(value).ok()),
                spatial,
                native: if mariadb {
                    NativeColumnMetadata::Mariadb(native)
                } else {
                    NativeColumnMetadata::Mysql(native)
                },
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let indexes = document
        .indexes
        .into_iter()
        .map(|index| {
            let elements = if index.column_backed {
                Observation::Observed(
                    index
                        .columns
                        .into_iter()
                        .map(|column| IndexElement {
                            expression: column,
                            included: Some(false),
                            descending: None,
                            native: if mariadb {
                                NativeIndexElementMetadata::Mariadb
                            } else {
                                NativeIndexElementMetadata::Mysql
                            },
                        })
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                )
            } else {
                Observation::NotMeasured
            };
            Index {
                name: Some(index.name.clone()),
                unique: Some(index.unique),
                primary: Some(index.name == "PRIMARY"),
                elements,
                predicate: None,
                spatial: None,
                native: if mariadb {
                    NativeIndexMetadata::Mariadb {
                        column_backed: index.column_backed,
                    }
                } else {
                    NativeIndexMetadata::Mysql {
                        column_backed: index.column_backed,
                    }
                },
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let token = if mariadb {
        SchemaToken::Mariadb(document.token)
    } else {
        SchemaToken::Mysql(document.token)
    };
    let native = MysqlTableMetadata {
        engine: document.engine,
    };
    Table {
        catalog: None,
        schema: Some(document.schema),
        name: document.name,
        kind: document.kind,
        schema_token: token,
        columns,
        indexes: Observation::Observed(indexes),
        constraints: Observation::NotMeasured,
        foreign_keys: Observation::NotMeasured,
        native: if mariadb {
            NativeTableMetadata::Mariadb(native)
        } else {
            NativeTableMetadata::Mysql(native)
        },
    }
}

fn mysql_spatial_type(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "geometry"
            | "point"
            | "linestring"
            | "polygon"
            | "multipoint"
            | "multilinestring"
            | "multipolygon"
            | "geometrycollection"
    )
}

#[derive(Deserialize)]
struct SqlServerDocument {
    columns: Vec<SqlServerColumnWire>,
    schema_token: SqlServerTokenWire,
    relation: SqlServerRelationWire,
    constraints: Vec<SqlServerConstraintWire>,
    indexes: Vec<SqlServerIndexWire>,
}

#[derive(Deserialize)]
struct SqlServerTokenWire {
    schema_version: u32,
    database_id: i32,
    object_id: i32,
    structural_fingerprint: String,
}

#[derive(Deserialize)]
struct SqlServerRelationWire {
    catalog: String,
    schema: String,
    name: String,
    kind: String,
    temporal_type: u8,
    memory_optimized: bool,
    durability: Option<String>,
}

#[derive(Deserialize)]
#[allow(clippy::struct_excessive_bools)]
struct SqlServerColumnWire {
    ordinal: i32,
    name: String,
    type_schema: String,
    native_type: String,
    max_length: i16,
    precision: u8,
    scale: u8,
    nullable: bool,
    identity: bool,
    computed: bool,
    generated_always_type: u8,
    collation: Option<String>,
    default_definition: Option<String>,
    computed_definition: Option<String>,
    computed_persisted: bool,
}

#[derive(Deserialize)]
struct SqlServerConstraintWire {
    name: String,
    kind: String,
    definition: Option<String>,
    columns: Option<String>,
    referenced_object: Option<String>,
    disabled: bool,
    not_trusted: bool,
}

#[derive(Deserialize)]
#[allow(clippy::struct_excessive_bools)]
struct SqlServerIndexWire {
    index_id: i32,
    name: Option<String>,
    kind: String,
    unique: bool,
    primary_key: bool,
    unique_constraint: bool,
    disabled: bool,
    filtered: bool,
    filter_definition: Option<String>,
    columns: Option<String>,
    spatial: Option<SqlServerSpatialIndexWire>,
}

#[derive(Deserialize)]
struct SqlServerSpatialIndexWire {
    spatial_type: String,
    tessellation_scheme: String,
    bounding_box: Option<SqlServerSpatialBoundingBoxWire>,
}

#[derive(Deserialize)]
struct SqlServerSpatialBoundingBoxWire {
    xmin: f64,
    ymin: f64,
    xmax: f64,
    ymax: f64,
}

#[allow(clippy::too_many_lines)]
fn sqlserver(document: SqlServerDocument) -> Table {
    let foreign_keys = document
        .constraints
        .iter()
        .filter_map(|constraint| {
            constraint
                .referenced_object
                .as_ref()
                .map(|referenced_object| ForeignKey {
                    name: constraint.name.clone(),
                    columns: Observation::NotMeasured,
                    referenced_schema: None,
                    referenced_object: referenced_object.clone(),
                    referenced_columns: Observation::NotMeasured,
                    on_update: None,
                    on_delete: None,
                    match_kind: None,
                })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let columns = document
        .columns
        .into_iter()
        .map(|column| {
            let ordinal = u64::try_from(column.ordinal).ok();
            let spatial =
                matches!(column.native_type.as_str(), "geometry" | "geography").then(|| {
                    SpatialColumnMetadata {
                        srid: None,
                        dimensions: None,
                        geometry_type: Some(column.native_type.clone()),
                        crs_id: None,
                    }
                });
            Column {
                name: column.name,
                ordinal,
                native_type: column.native_type.clone(),
                native_declaration: Some(column.native_type),
                nullable: Some(column.nullable),
                default_expression: column.default_definition,
                identity: Some(column.identity),
                generated: Some(column.computed || column.generated_always_type != 0),
                numeric_precision: Some(u64::from(column.precision)),
                numeric_scale: Some(i64::from(column.scale)),
                spatial,
                native: NativeColumnMetadata::SqlServer(SqlServerColumnMetadata {
                    type_schema: column.type_schema,
                    max_length: column.max_length,
                    computed: column.computed,
                    generated_always_type: column.generated_always_type,
                    collation: column.collation,
                    computed_definition: column.computed_definition,
                    computed_persisted: column.computed_persisted,
                }),
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let constraints = document
        .constraints
        .into_iter()
        .map(|constraint| Constraint {
            name: constraint.name,
            kind: constraint.kind,
            definition: constraint.definition,
            columns: Observation::NotMeasured,
            native: NativeConstraintMetadata::SqlServer {
                columns: constraint.columns,
                referenced_object: constraint.referenced_object,
                disabled: constraint.disabled,
                not_trusted: constraint.not_trusted,
            },
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let indexes = document
        .indexes
        .into_iter()
        .map(|index| {
            let spatial = index.spatial.is_some().then_some(true);
            Index {
                name: index.name,
                unique: Some(index.unique),
                primary: Some(index.primary_key),
                elements: Observation::NotMeasured,
                predicate: index.filter_definition,
                spatial,
                native: NativeIndexMetadata::SqlServer {
                    index_id: index.index_id,
                    kind: index.kind,
                    unique_constraint: index.unique_constraint,
                    disabled: index.disabled,
                    filtered: index.filtered,
                    columns: index.columns,
                    spatial: index.spatial.map(|value| SqlServerSpatialIndexMetadata {
                        spatial_type: value.spatial_type,
                        tessellation_scheme: value.tessellation_scheme,
                        bounding_box: value.bounding_box.map(|bounds| {
                            SqlServerSpatialBoundingBox {
                                xmin: bounds.xmin,
                                ymin: bounds.ymin,
                                xmax: bounds.xmax,
                                ymax: bounds.ymax,
                            }
                        }),
                    }),
                },
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Table {
        catalog: Some(document.relation.catalog),
        schema: Some(document.relation.schema),
        name: document.relation.name,
        kind: document.relation.kind,
        schema_token: SchemaToken::SqlServer {
            schema_version: document.schema_token.schema_version,
            database_id: document.schema_token.database_id,
            object_id: document.schema_token.object_id,
            structural_fingerprint: document.schema_token.structural_fingerprint,
        },
        columns,
        indexes: Observation::Observed(indexes),
        constraints: Observation::Observed(constraints),
        foreign_keys: Observation::Observed(foreign_keys),
        native: NativeTableMetadata::SqlServer(SqlServerTableMetadata {
            temporal_type: document.relation.temporal_type,
            memory_optimized: document.relation.memory_optimized,
            durability: document.relation.durability,
        }),
    }
}

#[derive(Deserialize)]
struct Db2Document {
    schema: String,
    name: String,
    kind: String,
    columns: Vec<Db2ColumnWire>,
    indexes: Vec<Db2IndexWire>,
    schema_token: String,
}

#[derive(Deserialize)]
struct Db2ColumnWire {
    name: String,
    ordinal: u64,
    data_type: String,
    length: u64,
    scale: i64,
    nullable: bool,
    default_expression: Option<String>,
    generated: bool,
    identity: bool,
}

#[derive(Deserialize)]
struct Db2IndexWire {
    name: String,
    unique: bool,
    primary: bool,
    columns: Vec<String>,
    descending: Vec<bool>,
}

fn db2(document: Db2Document) -> Table {
    let columns = document
        .columns
        .into_iter()
        .map(|column| {
            let spatial = db2_spatial_type(&column.data_type).then(|| SpatialColumnMetadata {
                srid: None,
                dimensions: None,
                geometry_type: Some(column.data_type.clone()),
                crs_id: None,
            });
            Column {
                name: column.name,
                ordinal: Some(column.ordinal),
                native_type: column.data_type.clone(),
                native_declaration: Some(column.data_type),
                nullable: Some(column.nullable),
                default_expression: column.default_expression,
                identity: Some(column.identity),
                generated: Some(column.generated),
                numeric_precision: Some(column.length),
                numeric_scale: Some(column.scale),
                spatial,
                native: NativeColumnMetadata::Db2(Db2ColumnMetadata {
                    length: column.length,
                }),
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    let indexes = document
        .indexes
        .into_iter()
        .map(|index| {
            let descending = index.descending;
            let elements = index
                .columns
                .into_iter()
                .enumerate()
                .map(|(position, column)| IndexElement {
                    expression: column,
                    included: Some(false),
                    descending: descending.get(position).copied(),
                    native: NativeIndexElementMetadata::Db2,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice();
            Index {
                name: Some(index.name),
                unique: Some(index.unique),
                primary: Some(index.primary),
                elements: Observation::Observed(elements),
                predicate: None,
                spatial: None,
                native: NativeIndexMetadata::Db2,
            }
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Table {
        catalog: None,
        schema: Some(document.schema),
        name: document.name,
        kind: document.kind,
        schema_token: SchemaToken::Db2(document.schema_token),
        columns,
        indexes: Observation::Observed(indexes),
        constraints: Observation::NotMeasured,
        foreign_keys: Observation::NotMeasured,
        native: NativeTableMetadata::Db2,
    }
}

fn db2_spatial_type(value: &str) -> bool {
    let value = value.to_ascii_uppercase();
    value.starts_with("ST_") || value.contains("GEOMETRY") || value.contains("GEOGRAPHY")
}
