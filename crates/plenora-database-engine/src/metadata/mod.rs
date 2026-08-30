//! Reflection tipizzata e immutabile sopra il contratto JSON v2.

mod wire;

use plenora_database_core::plan::{ObjectRef, ProviderKind};
use plenora_database_core::provider::Inspection;
use plenora_database_core::Result;
use serde::Serialize;

/// Distingue una superficie osservata da una che il provider non misura.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum Observation<T> {
    NotMeasured,
    Observed(T),
}

impl<T> Observation<T> {
    #[must_use]
    pub const fn is_measured(&self) -> bool {
        matches!(self, Self::Observed(_))
    }

    #[must_use]
    pub const fn as_ref(&self) -> Observation<&T> {
        match self {
            Self::NotMeasured => Observation::NotMeasured,
            Self::Observed(value) => Observation::Observed(value),
        }
    }
}

/// Token strutturale senza collisioni semantiche fra provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum SchemaToken {
    Postgres {
        schema_version: u32,
        database_oid: u32,
        namespace_oid: u32,
        relation_oid: u32,
        structural_fingerprint: String,
    },
    Mysql(String),
    Mariadb(String),
    SqlServer {
        schema_version: u32,
        database_id: i32,
        object_id: i32,
        structural_fingerprint: String,
    },
    Db2(String),
}

/// Catalogo tipizzato prodotto da una o piu operazioni di reflection.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct MetaData {
    provider: ProviderKind,
    tables: Box<[Table]>,
}

impl MetaData {
    /// Converte una descrizione oggetto v2 nella superficie tipizzata.
    ///
    /// # Errors
    ///
    /// `DataMapping` se il provider restituisce un documento incoerente.
    pub(crate) fn from_inspection(
        provider: ProviderKind,
        source: &ObjectRef,
        inspection: Inspection,
    ) -> Result<Self> {
        wire::metadata_from_inspection(provider, source, inspection)
    }

    #[must_use]
    pub const fn provider(&self) -> ProviderKind {
        self.provider
    }

    #[must_use]
    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    /// Restituisce la sola tabella prodotta da `describe_object`.
    ///
    /// # Errors
    ///
    /// `DataMapping` se il catalogo non contiene esattamente una tabella.
    pub fn one_table(&self) -> Result<&Table> {
        if self.tables.len() == 1 {
            return Ok(&self.tables[0]);
        }
        Err(wire::mapping_error(
            "reflection tipizzata senza un unico oggetto",
        ))
    }
}

/// Oggetto relazionale riflesso con token e attributi nativi separati.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Table {
    catalog: Option<String>,
    schema: Option<String>,
    name: String,
    kind: String,
    schema_token: SchemaToken,
    columns: Box<[Column]>,
    indexes: Observation<Box<[Index]>>,
    constraints: Observation<Box<[Constraint]>>,
    foreign_keys: Observation<Box<[ForeignKey]>>,
    native: NativeTableMetadata,
}

impl Table {
    #[must_use]
    pub fn catalog(&self) -> Option<&str> {
        self.catalog.as_deref()
    }

    #[must_use]
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub const fn schema_token(&self) -> &SchemaToken {
        &self.schema_token
    }

    #[must_use]
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    #[must_use]
    pub const fn indexes(&self) -> Observation<&[Index]> {
        match &self.indexes {
            Observation::NotMeasured => Observation::NotMeasured,
            Observation::Observed(values) => Observation::Observed(values),
        }
    }

    #[must_use]
    pub const fn constraints(&self) -> Observation<&[Constraint]> {
        match &self.constraints {
            Observation::NotMeasured => Observation::NotMeasured,
            Observation::Observed(values) => Observation::Observed(values),
        }
    }

    #[must_use]
    pub const fn foreign_keys(&self) -> Observation<&[ForeignKey]> {
        match &self.foreign_keys {
            Observation::NotMeasured => Observation::NotMeasured,
            Observation::Observed(values) => Observation::Observed(values),
        }
    }

    #[must_use]
    pub const fn native(&self) -> &NativeTableMetadata {
        &self.native
    }
}

/// Colonna comune; i campi non osservabili restano `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Column {
    name: String,
    ordinal: Option<u64>,
    native_type: String,
    native_declaration: Option<String>,
    nullable: Option<bool>,
    default_expression: Option<String>,
    identity: Option<bool>,
    generated: Option<bool>,
    numeric_precision: Option<u64>,
    numeric_scale: Option<i64>,
    spatial: Option<SpatialColumnMetadata>,
    native: NativeColumnMetadata,
}

impl Column {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn ordinal(&self) -> Option<u64> {
        self.ordinal
    }

    #[must_use]
    pub fn native_type(&self) -> &str {
        &self.native_type
    }

    #[must_use]
    pub fn native_declaration(&self) -> Option<&str> {
        self.native_declaration.as_deref()
    }

    #[must_use]
    pub const fn nullable(&self) -> Option<bool> {
        self.nullable
    }

    #[must_use]
    pub fn default_expression(&self) -> Option<&str> {
        self.default_expression.as_deref()
    }

    #[must_use]
    pub const fn identity(&self) -> Option<bool> {
        self.identity
    }

    #[must_use]
    pub const fn generated(&self) -> Option<bool> {
        self.generated
    }

    #[must_use]
    pub const fn numeric_precision(&self) -> Option<u64> {
        self.numeric_precision
    }

    #[must_use]
    pub const fn numeric_scale(&self) -> Option<i64> {
        self.numeric_scale
    }

    #[must_use]
    pub const fn spatial(&self) -> Option<&SpatialColumnMetadata> {
        self.spatial.as_ref()
    }

    #[must_use]
    pub const fn native(&self) -> &NativeColumnMetadata {
        &self.native
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SpatialColumnMetadata {
    srid: Option<u32>,
    dimensions: Option<String>,
    geometry_type: Option<String>,
    crs_id: Option<String>,
}

impl SpatialColumnMetadata {
    #[must_use]
    pub const fn srid(&self) -> Option<u32> {
        self.srid
    }

    #[must_use]
    pub fn dimensions(&self) -> Option<&str> {
        self.dimensions.as_deref()
    }

    #[must_use]
    pub fn geometry_type(&self) -> Option<&str> {
        self.geometry_type.as_deref()
    }

    #[must_use]
    pub fn crs_id(&self) -> Option<&str> {
        self.crs_id.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Index {
    name: Option<String>,
    unique: Option<bool>,
    primary: Option<bool>,
    elements: Observation<Box<[IndexElement]>>,
    predicate: Option<String>,
    spatial: Option<bool>,
    native: NativeIndexMetadata,
}

impl Index {
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    #[must_use]
    pub const fn unique(&self) -> Option<bool> {
        self.unique
    }

    #[must_use]
    pub const fn primary(&self) -> Option<bool> {
        self.primary
    }

    #[must_use]
    pub const fn elements(&self) -> Observation<&[IndexElement]> {
        match &self.elements {
            Observation::NotMeasured => Observation::NotMeasured,
            Observation::Observed(values) => Observation::Observed(values),
        }
    }

    #[must_use]
    pub fn predicate(&self) -> Option<&str> {
        self.predicate.as_deref()
    }

    #[must_use]
    pub const fn spatial(&self) -> Option<bool> {
        self.spatial
    }

    #[must_use]
    pub const fn native(&self) -> &NativeIndexMetadata {
        &self.native
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexElement {
    expression: String,
    included: Option<bool>,
    descending: Option<bool>,
    native: NativeIndexElementMetadata,
}

impl IndexElement {
    #[must_use]
    pub fn expression(&self) -> &str {
        &self.expression
    }

    #[must_use]
    pub const fn included(&self) -> Option<bool> {
        self.included
    }

    #[must_use]
    pub const fn descending(&self) -> Option<bool> {
        self.descending
    }

    #[must_use]
    pub const fn native(&self) -> &NativeIndexElementMetadata {
        &self.native
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Constraint {
    name: String,
    kind: String,
    definition: Option<String>,
    columns: Observation<Box<[String]>>,
    native: NativeConstraintMetadata,
}

impl Constraint {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn kind(&self) -> &str {
        &self.kind
    }

    #[must_use]
    pub fn definition(&self) -> Option<&str> {
        self.definition.as_deref()
    }

    #[must_use]
    pub const fn columns(&self) -> Observation<&[String]> {
        match &self.columns {
            Observation::NotMeasured => Observation::NotMeasured,
            Observation::Observed(values) => Observation::Observed(values),
        }
    }

    #[must_use]
    pub const fn native(&self) -> &NativeConstraintMetadata {
        &self.native
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ForeignKey {
    name: String,
    columns: Observation<Box<[String]>>,
    referenced_schema: Option<String>,
    referenced_object: String,
    referenced_columns: Observation<Box<[String]>>,
    on_update: Option<String>,
    on_delete: Option<String>,
    match_kind: Option<String>,
}

impl ForeignKey {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn columns(&self) -> Observation<&[String]> {
        match &self.columns {
            Observation::NotMeasured => Observation::NotMeasured,
            Observation::Observed(values) => Observation::Observed(values),
        }
    }

    #[must_use]
    pub fn referenced_schema(&self) -> Option<&str> {
        self.referenced_schema.as_deref()
    }

    #[must_use]
    pub fn referenced_object(&self) -> &str {
        &self.referenced_object
    }

    #[must_use]
    pub const fn referenced_columns(&self) -> Observation<&[String]> {
        match &self.referenced_columns {
            Observation::NotMeasured => Observation::NotMeasured,
            Observation::Observed(values) => Observation::Observed(values),
        }
    }

    #[must_use]
    pub fn on_update(&self) -> Option<&str> {
        self.on_update.as_deref()
    }

    #[must_use]
    pub fn on_delete(&self) -> Option<&str> {
        self.on_delete.as_deref()
    }

    #[must_use]
    pub fn match_kind(&self) -> Option<&str> {
        self.match_kind.as_deref()
    }
}

/// Attributi di relazione non portabili, separati dal catalogo comune.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub enum NativeTableMetadata {
    Postgres(Box<PostgresTableMetadata>),
    Mysql(MysqlTableMetadata),
    Mariadb(MysqlTableMetadata),
    SqlServer(SqlServerTableMetadata),
    Db2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct PostgresTableMetadata {
    pub is_partition: bool,
    pub partition_key: Option<String>,
    pub view_definition: Option<String>,
    pub comment: Option<String>,
    pub row_security: bool,
    pub force_row_security: bool,
    pub replica_identity: String,
    pub persistence: String,
    pub is_populated: bool,
    pub partition_bound: Option<String>,
    pub owner: String,
    pub tablespace: String,
    pub parents: Box<[PostgresRelationRef]>,
    pub partitions: Box<[PostgresPartition]>,
    pub policies: Box<[PostgresPolicy]>,
    pub privileges: Box<[PostgresPrivilege]>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PostgresRelationRef {
    pub schema: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PostgresPartition {
    pub schema: String,
    pub name: String,
    pub bound: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PostgresPolicy {
    pub name: String,
    pub permissive: bool,
    pub command: String,
    pub roles: Box<[String]>,
    pub using_expression: Option<String>,
    pub check_expression: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PostgresPrivilege {
    pub grantee: String,
    pub privilege: String,
    pub grantable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MysqlTableMetadata {
    pub engine: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SqlServerTableMetadata {
    pub temporal_type: u8,
    pub memory_optimized: bool,
    pub durability: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum NativeColumnMetadata {
    Postgres(PostgresColumnMetadata),
    Mysql(MysqlColumnMetadata),
    Mariadb(MysqlColumnMetadata),
    SqlServer(SqlServerColumnMetadata),
    Db2(Db2ColumnMetadata),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PostgresColumnMetadata {
    pub identity_kind: Option<String>,
    pub generated_kind: Option<String>,
    pub type_kind: Option<String>,
    pub composite_fields: Box<[PostgresCompositeField]>,
    pub enum_labels: Box<[String]>,
    pub domain_base_type: Option<String>,
    pub domain_constraints: Box<[String]>,
    pub collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PostgresCompositeField {
    pub name: String,
    pub declaration: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MysqlColumnMetadata {
    pub character_set: Option<String>,
    pub collation: Option<String>,
    pub datetime_precision: Option<u64>,
    pub extra: String,
    pub generation_expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SqlServerColumnMetadata {
    pub type_schema: String,
    pub max_length: i16,
    pub computed: bool,
    pub generated_always_type: u8,
    pub collation: Option<String>,
    pub computed_definition: Option<String>,
    pub computed_persisted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Db2ColumnMetadata {
    pub length: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub enum NativeIndexMetadata {
    Postgres {
        valid: bool,
        method: String,
        definition: String,
        ready: bool,
        clustered: bool,
        size_bytes: i64,
    },
    Mysql {
        column_backed: bool,
    },
    Mariadb {
        column_backed: bool,
    },
    SqlServer {
        index_id: i32,
        kind: String,
        unique_constraint: bool,
        disabled: bool,
        filtered: bool,
        columns: Option<String>,
        spatial: Option<SqlServerSpatialIndexMetadata>,
    },
    Db2,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SqlServerSpatialIndexMetadata {
    pub spatial_type: String,
    pub tessellation_scheme: String,
    pub bounding_box: Option<SqlServerSpatialBoundingBox>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SqlServerSpatialBoundingBox {
    pub xmin: f64,
    pub ymin: f64,
    pub xmax: f64,
    pub ymax: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum NativeIndexElementMetadata {
    Postgres { position: u64, opclass: String },
    Mysql,
    Mariadb,
    SqlServer,
    Db2,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub enum NativeConstraintMetadata {
    Postgres {
        validated: bool,
        deferrable: bool,
        initially_deferred: bool,
    },
    SqlServer {
        columns: Option<String>,
        referenced_object: Option<String>,
        disabled: bool,
        not_trusted: bool,
    },
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
