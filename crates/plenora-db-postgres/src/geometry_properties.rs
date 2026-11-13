//! Registro delle dichiarazioni Arrow conservate dal protocollo di colonna.
use crate::field_contract::FieldContract;
use plenora_database_core::protocol as p;
use plenora_database_core::{ErrorCategory, ErrorPhase, Result};

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

#[derive(Debug)]
pub struct Property {
    pub name: &'static str,
    pub key: &'static str,
    pub check: PhysicalCheck,
}

/// Tutte le proprieta conservano presenza e testo nel commento versionato.
/// Le verifiche fisiche non possono sostituire la dichiarazione conservata.
pub const PROPERTY_REGISTRY: [Property; 15] = [
    Property {
        name: "contract_version",
        key: p::CONTRACT_VERSION_KEY,
        check: PhysicalCheck::Contract,
    },
    Property {
        name: "field_id",
        key: p::FIELD_ID,
        check: PhysicalCheck::Identity,
    },
    Property {
        name: "types_declaration",
        key: p::GEOMETRY_TYPES_DECLARATION,
        check: PhysicalCheck::Types,
    },
    Property {
        name: "geometry_types",
        key: p::GEOMETRY_TYPES,
        check: PhysicalCheck::Types,
    },
    Property {
        name: "dimensions",
        key: p::GEOMETRY_DIMENSIONS,
        check: PhysicalCheck::Dimensions,
    },
    Property {
        name: "encoding",
        key: p::GEOMETRY_ENCODING,
        check: PhysicalCheck::Representation,
    },
    Property {
        name: "geometry_or_geography",
        key: p::GEOMETRY_SPATIAL_SEMANTICS,
        check: PhysicalCheck::Semantics,
    },
    Property {
        name: "precision",
        key: p::GEOMETRY_PRECISION,
        check: PhysicalCheck::Precision,
    },
    Property {
        name: "srid",
        key: p::GEOMETRY_SRID,
        check: PhysicalCheck::Srid,
    },
    Property {
        name: "crs_id",
        key: p::GEOMETRY_CRS_ID,
        check: PhysicalCheck::Metadata,
    },
    Property {
        name: "crs_resolution",
        key: p::GEOMETRY_CRS_RESOLUTION,
        check: PhysicalCheck::Metadata,
    },
    Property {
        name: "crs_definition",
        key: p::GEOMETRY_CRS_DEFINITION,
        check: PhysicalCheck::Metadata,
    },
    Property {
        name: "crs_definition_format",
        key: p::GEOMETRY_CRS_DEFINITION_FORMAT,
        check: PhysicalCheck::Metadata,
    },
    Property {
        name: "axis_order",
        key: p::GEOMETRY_AXIS_ORDER,
        check: PhysicalCheck::Axis,
    },
    Property {
        name: "native_metadata",
        key: "*",
        check: PhysicalCheck::Metadata,
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
    let Some(saved) = &column.arrow_metadata else {
        return Ok(());
    };
    let field = arrow_schema::Field::new(
        "column",
        column.arrow_field().data_type().clone(),
        column.nullable,
    )
    .with_metadata(
        saved
            .field
            .clone()
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>(),
    );
    let contract = FieldContract::parse(&field)?;
    validate_dimensions(&contract)?;
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
            Some(saved) if saved.schema == first.schema => (),
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
    use plenora_database_core::loss_registry::{RuleId, RuleInstance, RuleParameters};
    schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| match FieldContract::parse(field) {
            Ok(contract) if contract.canonical.spatial => Some(
                u32::try_from(index)
                    .map_err(|_| crate::column_metadata::invalid())
                    .map(|field_id| RuleInstance {
                        field_id,
                        rule_id: RuleId::PgGeometryRepresentation,
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
    let entries = columns
        .iter()
        .enumerate()
        .flat_map(|(index, column)| {
            PROPERTY_REGISTRY
                .iter()
                .filter(|property| {
                    column.arrow_metadata.is_none()
                        && !(property.check == PhysicalCheck::Identity
                            && column.registered_field_id.is_some())
                })
                .map(move |property| ReadLossEntry {
                    position: u32::try_from(index).unwrap_or(u32::MAX),
                    field_id: column.arrow_field().metadata().get(p::FIELD_ID).cloned(),
                    property: property.name.to_owned(),
                    rule: "pg.metadata.unavailable".to_owned(),
                })
        })
        .collect();
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
