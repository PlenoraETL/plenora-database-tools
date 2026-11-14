//! Registro delle dichiarazioni Arrow conservate dal protocollo di colonna.
use crate::column_metadata::invalid;
use crate::error::{classify_error, public_error};
use crate::field_contract::FieldContract;
use crate::pool::PooledClient;
use arrow_schema::Field;
use arrow_schema::SchemaRef;
use plenora_database_core::loss::MappingPolicy;
use plenora_database_core::protocol;
use plenora_database_core::protocol as p;
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};
use serde::{Deserialize, Serialize};

/// La conservazione del testo e distinta dalla verifica della forma fisica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalCheck {
    Contract,
    Identity,
    Types,
    Dimensions,
    Representation,
    Semantics,
    Precision,
    Srid,
    Axis,
    Metadata,
}

/// Required declarations cannot be removed even with consent (including GEO-009).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    Required,
    CrsPair,
    Optional,
}
#[derive(Debug, Clone)]
pub enum SchemaCheck {
    Declared,
    Dimensions,
    Representation,
    Semantics,
    Axis,
}
impl SchemaCheck {
    fn validate(&self, contract: &FieldContract<'_>) -> Result<()> {
        match self {
            Self::Declared => Ok(()),
            Self::Dimensions => validate_dimensions(contract),
            Self::Representation => validate_representation(contract),
            Self::Semantics => validate_semantics(contract),
            Self::Axis => validate_axis(contract),
        }
    }
}
#[derive(Debug, Clone)]
pub struct Property {
    pub name: &'static str,
    pub key: &'static str,
    pub check: PhysicalCheck,
    pub storage: Storage,
    pub schema_check: SchemaCheck,
    pub target_rule: Option<plenora_database_core::loss_registry::RuleId>,
    pub metadata_rule: Option<plenora_database_core::loss_registry::RuleId>,
    pub value_rule: Option<plenora_database_core::loss_registry::RuleId>,
    pub aliases: &'static [&'static str],
    pub removal_group: &'static [&'static str],
    pub protected_keys: &'static [&'static str],
    pub protected_prefixes: &'static [&'static str],
}

/// Tutte le proprieta conservano presenza e testo nel commento versionato.
/// Le verifiche fisiche non possono sostituire la dichiarazione conservata.
const CRS_PAIR: &[&str] = &[
    p::GEOMETRY_CRS_DEFINITION,
    p::GEOMETRY_CRS_DEFINITION_FORMAT,
    "ARROW:extension:metadata",
];
pub const PROPERTY_REGISTRY: [Property; 15] = [
    Property {
        name: "contract_version",
        key: p::CONTRACT_VERSION_KEY,
        check: PhysicalCheck::Contract,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "field_id",
        key: p::FIELD_ID,
        check: PhysicalCheck::Identity,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "types_declaration",
        key: p::GEOMETRY_TYPES_DECLARATION,
        check: PhysicalCheck::Types,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "geometry_types",
        key: p::GEOMETRY_TYPES,
        check: PhysicalCheck::Types,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "dimensions",
        key: p::GEOMETRY_DIMENSIONS,
        check: PhysicalCheck::Dimensions,
        storage: Storage::Required,
        schema_check: SchemaCheck::Dimensions,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "encoding",
        key: p::GEOMETRY_ENCODING,
        check: PhysicalCheck::Representation,
        storage: Storage::Required,
        schema_check: SchemaCheck::Representation,
        target_rule: None,
        metadata_rule: None,
        value_rule: Some(plenora_database_core::loss_registry::RuleId::PgGeometryRepresentation),
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "geometry_or_geography",
        key: p::GEOMETRY_SPATIAL_SEMANTICS,
        check: PhysicalCheck::Semantics,
        storage: Storage::Required,
        schema_check: SchemaCheck::Semantics,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "precision",
        key: p::GEOMETRY_PRECISION,
        check: PhysicalCheck::Precision,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "srid",
        key: p::GEOMETRY_SRID,
        check: PhysicalCheck::Srid,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: Some(plenora_database_core::loss_registry::RuleId::PgSrid),
        metadata_rule: None,
        value_rule: None,
        aliases: &["plenora.srid"],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "crs_id",
        key: p::GEOMETRY_CRS_ID,
        check: PhysicalCheck::Metadata,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: Some(plenora_database_core::loss_registry::RuleId::PgCrs),
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "crs_resolution",
        key: p::GEOMETRY_CRS_RESOLUTION,
        check: PhysicalCheck::Metadata,
        storage: Storage::Required,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "crs_definition",
        key: p::GEOMETRY_CRS_DEFINITION,
        check: PhysicalCheck::Metadata,
        storage: Storage::CrsPair,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: Some(
            plenora_database_core::loss_registry::RuleId::PgMetadataKeyNotPreserved,
        ),
        value_rule: None,
        aliases: &[],
        removal_group: CRS_PAIR,
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "crs_definition_format",
        key: p::GEOMETRY_CRS_DEFINITION_FORMAT,
        check: PhysicalCheck::Metadata,
        storage: Storage::CrsPair,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: Some(
            plenora_database_core::loss_registry::RuleId::PgMetadataKeyNotPreserved,
        ),
        value_rule: None,
        aliases: &[],
        removal_group: CRS_PAIR,
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "axis_order",
        key: p::GEOMETRY_AXIS_ORDER,
        check: PhysicalCheck::Axis,
        storage: Storage::Required,
        schema_check: SchemaCheck::Axis,
        target_rule: None,
        metadata_rule: None,
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[],
        protected_prefixes: &[],
    },
    Property {
        name: "native_metadata",
        key: "*",
        check: PhysicalCheck::Metadata,
        storage: Storage::Optional,
        schema_check: SchemaCheck::Declared,
        target_rule: None,
        metadata_rule: Some(
            plenora_database_core::loss_registry::RuleId::PgMetadataKeyNotPreserved,
        ),
        value_rule: None,
        aliases: &[],
        removal_group: &[],
        protected_keys: &[
            "ARROW:extension:name",
            "plenora.native_type",
            "plenora.native_declaration",
            "plenora.postgres_type_kind",
        ],
        protected_prefixes: &["plenora.postgres."],
    },
];

/// B-12: il typmod non puo attestare uno SRID senza imporre dimensioni.
pub fn validate_dimensions(contract: &FieldContract<'_>) -> Result<()> {
    if contract.canonical.spatial
        && contract.dimensions == Some("unknown")
        && (contract.srid.is_some() || contract.target_srid.is_some())
    {
        return Err(crate::error::public_error(
            ErrorCategory::Crs,
            ErrorPhase::Validate,
            false,
            "dimensioni geometriche unknown con SRID non rappresentabili in PostgreSQL",
        ));
    }
    Ok(())
}

/// Le mappe persistite si pubblicano solo dopo il confronto col catalogo.
pub fn validate_catalog(column: &crate::types::ColumnSpec) -> Result<()> {
    validate_registry(&PROPERTY_REGISTRY)?;
    let Some(saved) = &column.arrow_metadata else {
        return Ok(());
    };
    let field = arrow_schema::Field::new("column", column.arrow_data_type(), column.nullable)
        .with_metadata(
            saved
                .field
                .clone()
                .into_iter()
                .collect::<std::collections::HashMap<_, _>>(),
        );
    let contract = FieldContract::parse(&field)?;
    ensure_spatial_support(&contract)?;
    if contract.canonical.spatial != saved.physical.is_some()
        || saved
            .physical
            .as_ref()
            .is_some_and(|physical| !physical.matches(column))
        || column.registered_field_id.as_deref() != Some(saved.id.as_str())
    {
        return Err(crate::column_metadata::invalid());
    }

    for property in &PROPERTY_REGISTRY {
        let value = saved.field.get(property.key).map(String::as_str);
        let valid = match property.check {
            PhysicalCheck::Contract => saved
                .schema
                .get(property.key)
                .is_none_or(|v| v == p::CONTRACT_VERSION),
            PhysicalCheck::Identity => {
                value.is_none_or(|id| column.registered_field_id.as_deref() == Some(id))
            }
            PhysicalCheck::Semantics => {
                !contract.canonical.spatial
                    || (matches!(
                        column.kind,
                        crate::types::ColumnKind::Geometry | crate::types::ColumnKind::Geography
                    ) && contract.is_geography()
                        == matches!(column.kind, crate::types::ColumnKind::Geography))
            }
            PhysicalCheck::Dimensions if contract.canonical.spatial => {
                value == Some("unknown")
                    || value.is_none()
                    || column
                        .spatial_dimensions
                        .as_deref()
                        .is_some_and(|v| value.is_some_and(|s| s.eq_ignore_ascii_case(v)))
            }
            PhysicalCheck::Srid if contract.canonical.spatial => {
                contract.target_srid.filter(|v| *v != 0) == column.spatial_srid
            }
            PhysicalCheck::Types
                if property.key == p::GEOMETRY_TYPES && contract.canonical.spatial =>
            {
                column.spatial_type.as_deref().is_none_or(|physical| {
                    physical.eq_ignore_ascii_case("geometry")
                        || value.is_none_or(|types| {
                            types.split(',').any(|t| t.eq_ignore_ascii_case(physical))
                        })
                })
            }
            PhysicalCheck::Axis => {
                contract.canonical.ensure_east_first_axis("PostgreSQL")?;
                true
            }
            _ => true,
        };
        if !valid {
            return Err(crate::error::public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Prepare,
                false,
                &format!(
                    "proprieta Arrow incompatibile con il catalogo PostgreSQL: {}",
                    property.name
                ),
            ));
        }
    }
    Ok(())
}

pub fn read_schema(columns: &[crate::types::ColumnSpec]) -> Result<arrow_schema::SchemaRef> {
    let fallback = p::contract_schema(
        columns
            .iter()
            .map(crate::types::ColumnSpec::arrow_field)
            .collect(),
    )?;
    let Some(first) = columns.iter().find_map(|c| c.arrow_metadata.as_ref()) else {
        return Ok(fallback);
    };
    for column in columns {
        match &column.arrow_metadata {
            Some(saved)
                if saved.schema == first.schema
                    && schema_omissions(saved) == schema_omissions(first) => {}
            Some(_) => return Err(crate::column_metadata::invalid()),
            None => (),
        }
    }
    let schema = arrow_schema::Schema::new_with_metadata(
        fallback.fields().clone(),
        first
            .schema
            .clone()
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>(),
    );
    plenora_database_core::field_contract::validate_schema_contract(&schema)?;
    Ok(std::sync::Arc::new(schema))
}

/// Istanze localizzate per posizione del campo, incluso Create.
pub fn representation_rules(
    schema: &arrow_schema::Schema,
) -> Result<Vec<plenora_database_core::loss_registry::RuleInstance>> {
    use plenora_database_core::loss_registry::{RuleInstance, RuleParameters};
    let rule_id = PROPERTY_REGISTRY
        .iter()
        .find_map(|p| p.value_rule)
        .ok_or_else(invalid)?;
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| match FieldContract::parse(field) {
            Ok(contract) if contract.canonical.spatial => Some(
                u32::try_from(index)
                    .map_err(|_| crate::column_metadata::invalid())
                    .map(|field_id| RuleInstance {
                        subject: None,
                        field_id,
                        rule_id,
                        parameters: RuleParameters::default(),
                        source_type: None,
                        target_type: None,
                    }),
            ),
            Ok(_) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

pub fn read_loss(
    columns: &[crate::types::ColumnSpec],
) -> plenora_database_core::read_loss::ReadLossReport {
    use plenora_database_core::read_loss::{ReadLossEntry, ReadLossReport};
    let mut entries = Vec::new();
    let mut schema_keys = std::collections::BTreeSet::new();
    for (index, column) in columns.iter().enumerate() {
        let position = u32::try_from(index).unwrap_or(u32::MAX);
        if let Some(saved) = &column.arrow_metadata {
            for subject in &saved.omitted {
                if subject.scope == plenora_database_core::metadata_subject::MetadataScope::Schema
                    && !schema_keys.insert(subject.key_sha256.clone())
                {
                    continue;
                }
                entries.push(ReadLossEntry {
                    position,
                    field_id: subject.field_identity.clone(),
                    property: subject.property.clone(),
                    rule: "pg.metadata.key_not_preserved".to_owned(),
                    subject: Some(subject.clone()),
                });
            }
        } else {
            let identity = column.arrow_field().metadata().get(p::FIELD_ID).cloned();
            for property in &PROPERTY_REGISTRY {
                if property.check == PhysicalCheck::Identity && column.registered_field_id.is_some()
                {
                    continue;
                }
                entries.push(ReadLossEntry {
                    position,
                    field_id: identity.clone(),
                    property: property.name.to_owned(),
                    rule: "pg.metadata.unavailable".to_owned(),
                    subject: None,
                });
            }
        }
    }
    ReadLossReport::new(entries)
}

/// Una sorgente SQL non registrata ha dichiarazioni fisiche, mai una
/// promessa sull'identita Arrow originaria (vedi `read_loss`).
pub fn catalog_metadata(
    column: &crate::types::ColumnSpec,
) -> std::collections::HashMap<String, String> {
    use crate::types::ColumnKind;
    let mut metadata = std::collections::HashMap::new();
    for (key, value) in [
        (p::POSTGRES_NATIVE_TYPE, Some(column.native_type.clone())),
        (
            p::POSTGRES_NATIVE_DECLARATION,
            column.native_declaration.clone(),
        ),
        (p::POSTGRES_TYPE_KIND, column.type_kind.clone()),
        (
            p::POSTGRES_DOMAIN_BASE_TYPE,
            column.domain_base_type.clone(),
        ),
        (p::POSTGRES_COLLATION, column.collation.clone()),
        (
            p::POSTGRES_ENUM_LABELS,
            (!column.enum_labels.is_empty())
                .then(|| serde_json::to_string(&column.enum_labels).ok())
                .flatten(),
        ),
        (
            p::POSTGRES_DOMAIN_CONSTRAINTS,
            (!column.domain_constraints.is_empty())
                .then(|| serde_json::to_string(&column.domain_constraints).ok())
                .flatten(),
        ),
    ] {
        if let Some(value) = value {
            metadata.insert(key.to_owned(), value);
        }
    }
    if !matches!(column.kind, ColumnKind::Geometry | ColumnKind::Geography) {
        return metadata;
    }
    metadata.insert(
        p::GEOARROW_EXTENSION_NAME.to_owned(),
        "geoarrow.wkb".to_owned(),
    );
    let mut crs = std::collections::HashMap::new();
    p::insert_declared_crs(&mut crs, "POSTGIS", column.spatial_srid);
    if let (Some(id), Some(srid)) = (&column.spatial_crs_id, column.spatial_srid) {
        crs.insert(p::GEOMETRY_CRS_ID.to_owned(), id.clone());
        crs.insert(p::GEOMETRY_CRS_RESOLUTION.to_owned(), "resolved".to_owned());
        crs.insert(p::GEOMETRY_SRID.to_owned(), srid.to_string());
        crs.insert(p::GEOMETRY_AXIS_ORDER.to_owned(), "unknown".to_owned());
    }
    for property in &PROPERTY_REGISTRY {
        let value = match property.key {
            p::GEOMETRY_SPATIAL_SEMANTICS => Some(
                if matches!(column.kind, ColumnKind::Geography) {
                    "geography"
                } else {
                    "geometry"
                }
                .to_owned(),
            ),
            p::GEOMETRY_DIMENSIONS => Some(
                column
                    .spatial_dimensions
                    .as_deref()
                    .unwrap_or("unknown")
                    .to_ascii_lowercase(),
            ),
            p::GEOMETRY_PRECISION => Some("float64".to_owned()),
            p::GEOMETRY_ENCODING => Some("ewkb".to_owned()),
            p::GEOMETRY_TYPES_DECLARATION => Some(
                match column.spatial_type.as_deref() {
                    None => "unresolved",
                    Some(t) if t.eq_ignore_ascii_case("geometry") => "mixed",
                    Some(_) => "exact",
                }
                .to_owned(),
            ),
            p::GEOMETRY_TYPES => column
                .spatial_type
                .as_ref()
                .filter(|t| !t.eq_ignore_ascii_case("geometry"))
                .map(|t| t.to_ascii_lowercase()),
            _ => crs.remove(property.key),
        };
        if let Some(value) = value {
            metadata.insert(property.key.to_owned(), value);
        }
    }
    metadata
}

/// Physical guarantees required by the registered Arrow spatial declaration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalGeometry {
    geography: bool,
    dimensions: Option<String>,
    srid: Option<u32>,
    geometry_type: String,
}

impl PhysicalGeometry {
    pub fn plan(field: &Field) -> Result<Option<Self>> {
        let contract = crate::field_contract::FieldContract::parse(field)?;
        let logical = GeometryPlan::compile(&contract)?;
        if !logical.spatial {
            return Ok(None);
        }
        let dimensions = match logical.dimensions.as_deref() {
            Some("unknown") => None,
            Some("xy" | "xyz" | "xym" | "xyzm") => logical.dimensions.clone(),
            None => Some("xy".to_owned()),
            _ => return Err(invalid()),
        };
        let geometry_type = if dimensions.is_none() {
            "Geometry"
        } else {
            logical
                .geometry_types
                .as_deref()
                .filter(|types| !types.contains(','))
                .unwrap_or("Geometry")
        }
        .to_owned();
        if !geometry_type.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(invalid());
        }
        Ok(Some(Self {
            geography: logical.geography,
            dimensions,
            srid: logical.target_srid.filter(|v| *v != 0),
            geometry_type,
        }))
    }

    pub fn declaration(field: &Field) -> Result<String> {
        let physical = Self::plan(field)?.ok_or_else(invalid)?;
        let base = if physical.geography {
            "geography"
        } else {
            "geometry"
        };
        let Some(dimensions) = &physical.dimensions else {
            return Ok(base.to_owned());
        };
        let suffix = match dimensions.as_str() {
            "xy" => "",
            "xyz" => "Z",
            "xym" => "M",
            "xyzm" => "ZM",
            _ => return Err(invalid()),
        };
        Ok(format!(
            "{base}({}{suffix},{})",
            physical.geometry_type,
            physical.srid.unwrap_or(0)
        ))
    }

    pub fn matches(&self, column: &crate::types::ColumnSpec) -> bool {
        use crate::types::ColumnKind;
        matches!(column.kind, ColumnKind::Geometry | ColumnKind::Geography)
            && self.geography == matches!(column.kind, ColumnKind::Geography)
            && self.dimensions
                == column
                    .spatial_dimensions
                    .as_deref()
                    .map(str::to_ascii_lowercase)
            && self.srid == column.spatial_srid.filter(|v| *v != 0)
            && self
                .geometry_type
                .eq_ignore_ascii_case(column.spatial_type.as_deref().unwrap_or("geometry"))
    }
}

/// Total mapping: opaque keys belong to `native_metadata`, never silently ignored.
pub fn storage_for(key: &str) -> Storage {
    let property = property_for(key);
    if property.protected_keys.contains(&key)
        || property
            .protected_prefixes
            .iter()
            .any(|p| key.starts_with(p))
    {
        Storage::Required
    } else {
        property.storage
    }
}
pub fn property_for(key: &str) -> &'static Property {
    PROPERTY_REGISTRY
        .iter()
        .find(|p| p.key == key || p.aliases.contains(&key))
        .unwrap_or(&PROPERTY_REGISTRY[14])
}

/// Compilation and read reconstruction both enforce a total declarative table.
pub fn validate_registry(registry: &[Property]) -> Result<()> {
    use std::collections::BTreeSet;
    let names: BTreeSet<_> = registry.iter().map(|p| p.name).collect();
    let keys: BTreeSet<_> = registry
        .iter()
        .flat_map(|p| std::iter::once(p.key).chain(p.aliases.iter().copied()))
        .collect();
    for property in registry {
        let metadata = property.metadata_rule.map(|r| r.definition().predicate);
        if (property.storage == Storage::Required) != metadata.is_none()
            || metadata.is_some_and(|p| {
                p != plenora_database_core::loss_registry::PredicateId::MetadataKey
            })
            || (property.check == PhysicalCheck::Representation && property.value_rule.is_none())
            || (property.storage == Storage::CrsPair && property.removal_group.is_empty())
        {
            return Err(invalid());
        }
    }
    if registry.len() != 15
        || names.len() != registry.len()
        || keys.len() != registry.iter().map(|p| 1 + p.aliases.len()).sum::<usize>()
        || registry
            .last()
            .is_none_or(|p| p.key != "*" || p.storage != Storage::Optional)
    {
        return Err(invalid());
    }
    Ok(())
}

#[path = "geometry_metadata_plan.rs"]
mod metadata_plan;
pub use metadata_plan::{Registration, SchemaPlan};

/// I limiti di supporto di un campo geometrico che si decidono dallo schema,
/// prima di ogni effetto (GEO-000, verdetto di supporto) e prima di contattare
/// il server: un dry run li vede come la scrittura.
fn validate_axis(contract: &FieldContract<'_>) -> Result<()> {
    if contract.canonical.spatial {
        contract.canonical.ensure_east_first_axis("PostgreSQL")?;
    }
    Ok(())
}
fn validate_representation(contract: &FieldContract<'_>) -> Result<()> {
    if contract.canonical.spatial && contract.field.data_type() != &arrow_schema::DataType::Binary {
        return Err(public_error(
            ErrorCategory::Unsupported,
            ErrorPhase::Validate,
            false,
            "write PostgreSQL di geoarrow.wkb richiede un campo Arrow Binary, non LargeBinary",
        ));
    }
    Ok(())
}
fn validate_semantics(contract: &FieldContract<'_>) -> Result<()> {
    if contract.is_geography() {
        if contract.target_srid.is_none_or(|srid| srid == 0) {
            return Err(public_error(ErrorCategory::Crs, ErrorPhase::Validate, false,
                "geography PostgreSQL senza SRID: PostGIS assegnerebbe 4326 a un CRS non dichiarato"));
        }
        if matches!(
            contract.canonical.axis_order,
            Some("easting_northing" | "other")
        ) {
            return Err(public_error(ErrorCategory::Unsupported, ErrorPhase::Validate, false,
                "geography PostgreSQL richiede un CRS geografico: l'ordine degli assi dichiarato e proiettato"));
        }
    }
    Ok(())
}
pub fn ensure_spatial_support(contract: &FieldContract<'_>) -> Result<()> {
    for property in &PROPERTY_REGISTRY {
        property.schema_check.validate(contract)?;
    }
    Ok(())
}

pub fn projection_function(column: &crate::types::ColumnSpec) -> Result<&'static str> {
    let encoding = PROPERTY_REGISTRY
        .iter()
        .find(|p| p.check == PhysicalCheck::Representation)
        .ok_or_else(|| {
            public_error(
                ErrorCategory::Internal,
                ErrorPhase::Read,
                false,
                "registro geometrico PostgreSQL privo della proprieta di rappresentazione",
            )
        })?;
    Ok(
        if column
            .arrow_metadata
            .as_ref()
            .and_then(|s| s.field.get(encoding.key))
            .is_some_and(|s| s == "wkb")
        {
            "ST_AsBinary"
        } else {
            "ST_AsEWKB"
        },
    )
}

pub fn crs_changes(
    policy: MappingPolicy,
    source: Option<&String>,
    target: Option<&String>,
    source_srid: Option<&String>,
    target_srid: Option<&String>,
) -> bool {
    let same_srid = source_srid.is_some_and(|srid| Some(srid) == target_srid);
    source != target && (policy == MappingPolicy::Strict || !(source.is_none() && same_srid))
}

// Deviazione dichiarata (REJ-003, contracts/adoption-source.json): i rifiuti decisi
// leggendo il bersaglio escono con fase `prepare`, non `probe`.
/// Il CRS di ogni campo geometrico contro `spatial_ref_sys` del bersaglio,
/// prima di ogni effetto.
///
/// - uno SRID **derivato** da `crs_id` `EPSG:<n>` (GEO-008) deve essere
///   registrato come `EPSG:<n>`, qualunque sia lo stato di risoluzione: lo
///   stesso numero puo nominare un altro CRS (`54009` e `ESRI:54009` in
///   `PostGIS`) o nessuno;
/// - un CRS `resolved` con `crs_id` deve coincidere con l'autorita
///   registrata per il suo SRID;
/// - una colonna `geography` vuole uno SRID geografico: uno proiettato e un
///   limite del bersaglio (`unsupported`), uno sconosciuto un CRS che il
///   bersaglio non conosce (`crs`). Senza questa lettura il rifiuto arrivava
///   dalla DDL come `protocol`.
pub async fn validate_resolved_crs_against_postgis(
    client: &PooledClient,
    input_schema: &SchemaRef,
) -> Result<()> {
    let crs_error =
        |message: &str| public_error(ErrorCategory::Crs, ErrorPhase::Prepare, false, message);
    // `spatial_ref_sys` dello schema di PostGIS (regola Q), letto una volta.
    let mut postgis: Option<String> = None;
    for field in input_schema.fields() {
        let contract = plenora_database_core::field_contract::FieldContract::parse(field)?;
        if !contract.spatial {
            continue;
        }
        let resolved = contract.crs_resolution == Some("resolved") && contract.crs_id.is_some();
        let geography = contract.is_geography();
        // Lo SRID della colonna, dichiarato o derivato (GEO-008); lo schema e
        // gia stato verificato da `write::prepare_state`.
        let Some(target) = contract.target_srid_source()? else {
            continue;
        };
        let Ok(srid) = i32::try_from(target.srid) else {
            continue;
        };
        if srid == 0 || !(target.derived || resolved || geography) {
            continue;
        }
        let qualifier = if let Some(qualifier) = &postgis {
            qualifier.clone()
        } else {
            let qualifier =
                crate::postgis_schema::qualifier(client.client()?, ErrorPhase::Prepare).await?;
            postgis = Some(qualifier.clone());
            qualifier
        };
        let row = client
            .client()?
            .query_opt(
                &crate::postgis_schema::qualify(
                    "SELECT auth_name, auth_srid, \
                        (srtext::pg_catalog.text LIKE 'GEOGCS%' \
                         OR srtext::pg_catalog.text LIKE 'GEOGCRS%' \
                         OR proj4text::pg_catalog.text LIKE '%+proj=longlat%' \
                         OR proj4text::pg_catalog.text LIKE '%+proj=latlong%') \
                     FROM spatial_ref_sys WHERE srid = $1",
                    &qualifier,
                ),
                &[&srid],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?
            .ok_or_else(|| crs_error("SRID del campo assente da spatial_ref_sys"))?;
        let authority: Option<String> = row.get(0);
        let authority_code: Option<i32> = row.get(1);
        let geographic: Option<bool> = row.get(2);
        if target.derived
            && !(authority
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case("EPSG"))
                && authority_code == Some(srid))
        {
            return Err(crs_error(
                "SRID derivato da crs_id EPSG registrato in spatial_ref_sys con un'altra identita",
            ));
        }
        // Una definizione senza identificatore non si confronta con il
        // catalogo: il campo registra lo SRID che dichiara (GEO-004), e
        // `resolved` qui vuol dire `resolved` con `crs_id`.
        if let (true, Some(declared_id)) = (resolved, contract.crs_id) {
            let observed_id = authority
                .as_deref()
                .filter(|value| !value.is_empty())
                .zip(authority_code)
                .map(|(name, code)| format!("{name}:{code}"))
                .ok_or_else(|| crs_error("spatial_ref_sys non risolve un authority ID"))?;
            if !declared_id.eq_ignore_ascii_case(&observed_id) {
                return Err(crs_error(
                    "identificatore CRS non coerente con spatial_ref_sys",
                ));
            }
        }
        if geography && geographic != Some(true) {
            return Err(public_error(
                ErrorCategory::Unsupported,
                ErrorPhase::Prepare,
                false,
                "geography PostgreSQL richiede uno SRID geografico in spatial_ref_sys",
            ));
        }
    }
    Ok(())
}

pub fn target_rules(
    source: &Field,
    target_field: &Field,
    field_id: u32,
    policy: MappingPolicy,
) -> Result<Vec<plenora_database_core::loss_registry::RuleInstance>> {
    use plenora_database_core::loss_registry::{RuleId, RuleInstance, RuleParameters};
    let mapping_loss = |field_id, rule_id, source_type, target_type| -> Result<RuleInstance> {
        Ok(RuleInstance {
            subject: None,
            field_id,
            rule_id,
            source_type,
            target_type,
            parameters: RuleParameters::default(),
        })
    };
    let mut losses = Vec::new();
    // Lo SRID che la colonna riceverebbe, dichiarato o derivato da
    // `EPSG:<n>` (GEO-008): un campo senza `srid` ma con `EPSG:4326`
    // verso una colonna 4326 non perde niente.
    let derived_srid = plenora_database_core::field_contract::FieldContract::parse(source)?
        .target_srid()?
        .map(|srid| srid.to_string());
    let source_srid = source
        .metadata()
        .get(protocol::GEOMETRY_SRID)
        .or_else(|| source.metadata().get("plenora.srid"))
        .or(derived_srid.as_ref());
    let target_srid = target_field
        .metadata()
        .get(protocol::GEOMETRY_SRID)
        .or_else(|| target_field.metadata().get("plenora.srid"));
    let source_crs = source.metadata().get(protocol::GEOMETRY_CRS_ID);
    let target_crs = target_field.metadata().get(protocol::GEOMETRY_CRS_ID);
    for property in &PROPERTY_REGISTRY {
        let Some(rule) = property.target_rule else {
            continue;
        };
        let change = match rule {
            RuleId::PgSrid => (source_srid != target_srid
                && (source_srid.is_some() || target_srid.is_some()))
            .then_some((source_srid, target_srid)),
            RuleId::PgCrs => crs_changes(policy, source_crs, target_crs, source_srid, target_srid)
                .then_some((source_crs, target_crs)),
            _ => return Err(invalid()),
        };
        if let Some((source, target)) = change {
            losses.push(mapping_loss(
                field_id,
                rule,
                source.cloned(),
                target.cloned(),
            )?);
        }
    }
    Ok(losses)
}

/// Owned value contract compiled by the same property table as persistence.
#[derive(Debug, Clone)]
pub struct GeometryPlan {
    pub geometry_types: Option<String>,
    pub encoding: Option<String>,
    pub dimensions: Option<String>,
    pub srid: Option<u32>,
    pub target_srid: Option<u32>,
    spatial: bool,
    geography: bool,
}
impl GeometryPlan {
    pub fn compile(contract: &FieldContract<'_>) -> Result<Self> {
        ensure_spatial_support(contract)?;
        let mut plan = Self {
            geometry_types: None,
            encoding: None,
            dimensions: None,
            srid: None,
            target_srid: None,
            spatial: contract.canonical.spatial,
            geography: false,
        };
        for property in &PROPERTY_REGISTRY {
            match property.check {
                PhysicalCheck::Types if property.key == p::GEOMETRY_TYPES => {
                    plan.geometry_types = contract.geometry_type.map(str::to_owned);
                }
                PhysicalCheck::Representation => {
                    plan.encoding = contract.canonical.encoding.map(str::to_owned);
                }
                PhysicalCheck::Dimensions => {
                    plan.dimensions = contract.dimensions.map(str::to_owned);
                }
                PhysicalCheck::Srid => {
                    plan.srid = contract.srid;
                    plan.target_srid = contract.target_srid;
                }
                PhysicalCheck::Semantics => plan.geography = contract.is_geography(),
                _ => (),
            }
        }
        Ok(plan)
    }
    pub fn needs_srid_stamp(&self) -> bool {
        self.spatial
            && matches!(self.encoding.as_deref(), Some("wkb" | "ewkb"))
            && self.target_srid.is_some_and(|srid| srid != 0)
    }
    pub fn value_contract(&self) -> plenora_database_core::geometry_value::ValueContract<'_> {
        plenora_database_core::geometry_value::ValueContract {
            ewkb: self.encoding.as_deref().map(|encoding| encoding == "ewkb"),
            srid: self.srid,
            types: self.geometry_types.as_deref(),
            dimensions: self.dimensions.as_deref(),
        }
    }
    pub fn classify_representation(
        &self,
        bytes: &[u8],
    ) -> Result<plenora_database_core::loss_ledger::Classification> {
        use plenora_database_core::{execution_loss::Basis, loss_ledger::Classification};
        let exact = plenora_database_core::ewkb::has_ndr_layout(
            bytes,
            self.encoding.as_deref() != Some("wkb"),
            self.target_srid,
            32,
            self.geography,
        )?;
        Ok(if exact {
            Classification::Exact(Basis::Exact)
        } else {
            Classification::WouldChange(Basis::Conservative)
        })
    }
    pub fn needs_complete_prevalidation(&self) -> bool {
        self.spatial && self.dimensions.as_deref() == Some("unknown")
    }
}

pub fn published_srid_conforms(
    columns: &[crate::types::ColumnSpec],
    inspection: &plenora_database_core::ewkb::EwkbInspection,
    column: usize,
) -> Result<()> {
    if columns
        .get(column)
        .map(projection_function)
        .transpose()?
        .is_some_and(|function| function == "ST_AsBinary")
    {
        return if inspection.root.srid.is_none() {
            Ok(())
        } else {
            Err(public_error(
                ErrorCategory::DataMapping,
                ErrorPhase::Read,
                false,
                "SRID incorporato nella proiezione WKB PostgreSQL",
            ))
        };
    }
    let declared = columns.get(column).and_then(|spec| spec.spatial_srid);
    match (declared, inspection.root.srid) {
        (None, None | Some(0)) => Ok(()),
        (Some(declared), Some(value)) if declared == value => Ok(()),
        (None, Some(_)) => Err(public_error(
            ErrorCategory::Crs,
            ErrorPhase::Read,
            false,
            "colonna geometrica PostgreSQL senza SRID nel catalogo con valori dotati di SRID: \
             dichiararlo con un typmod o con un cast nella query",
        )),
        (Some(_), _) => Err(public_error(
            ErrorCategory::Crs,
            ErrorPhase::Read,
            false,
            "valore geometrico PostgreSQL senza lo SRID dichiarato per la colonna",
        )),
    }
}

fn schema_omissions(
    saved: &crate::column_metadata::ColumnMetadata,
) -> std::collections::BTreeMap<
    &str,
    (
        &str,
        &plenora_database_core::metadata_subject::MetadataReason,
    ),
> {
    saved
        .omitted
        .iter()
        .filter(|s| s.scope == plenora_database_core::metadata_subject::MetadataScope::Schema)
        .map(|s| (s.key_sha256.as_str(), (s.property.as_str(), &s.reason)))
        .collect()
}
