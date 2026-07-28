use super::usize_to_u64;
use crate::arrow::range_fields;
use crate::catalog::{catalog_field, catalog_json_list};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
use plenora_database_core::geometry::GEOARROW_WKB_EXTENSION_NAME;
use plenora_database_core::plan::ProviderKind;
use plenora_database_core::protocol;
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use plenora_database_sql::{Identifier, Renderer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::Row;

#[derive(Debug, Clone, Serialize)]
pub struct ColumnSpec {
    pub name: String,
    pub native_type: String,
    pub nullable: bool,
    pub numeric_precision: Option<u8>,
    pub numeric_scale: Option<i8>,
    pub spatial_srid: Option<u32>,
    pub spatial_dimensions: Option<String>,
    pub spatial_type: Option<String>,
    pub spatial_crs_id: Option<String>,
    pub default_expression: Option<String>,
    pub identity_kind: Option<String>,
    pub generated_kind: Option<String>,
    pub native_declaration: Option<String>,
    pub type_kind: Option<String>,
    pub composite_fields: Vec<CompositeFieldSpec>,
    pub enum_labels: Vec<String>,
    pub domain_base_type: Option<String>,
    pub domain_constraints: Vec<String>,
    pub collation: Option<String>,
    #[serde(skip)]
    pub kind: ColumnKind,
}

#[derive(Debug, Clone, Copy)]
pub enum ColumnKind {
    Bool,
    I32,
    I64,
    F32,
    F64,
    Utf8,
    Binary,
    Date,
    Timestamp,
    TimestampTz,
    Time,
    Interval,
    Range,
    Composite,
    Decimal { precision: u8, scale: i8 },
    BoolArray,
    I32Array,
    I64Array,
    F32Array,
    F64Array,
    Utf8Array,
    Geometry,
    Geography,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeFieldSpec {
    pub name: String,
    pub declaration: String,
}

impl ColumnSpec {
    pub fn from_statement_column(column: &tokio_postgres::Column) -> Result<Self> {
        let native_type = column.type_().name().to_owned();
        let kind = match native_type.as_str() {
            "bool" => ColumnKind::Bool,
            "int2" | "int4" => ColumnKind::I32,
            "int8" => ColumnKind::I64,
            "float4" => ColumnKind::F32,
            "float8" => ColumnKind::F64,
            "bytea" => ColumnKind::Binary,
            "date" => ColumnKind::Date,
            "time" => ColumnKind::Time,
            "interval" => ColumnKind::Interval,
            "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
                ColumnKind::Range
            }
            "timestamp" => ColumnKind::Timestamp,
            "timestamptz" => ColumnKind::TimestampTz,
            "text" | "varchar" | "bpchar" | "name" | "json" | "jsonb" | "uuid" => ColumnKind::Utf8,
            "_bool" => ColumnKind::BoolArray,
            "_int2" | "_int4" => ColumnKind::I32Array,
            "_int8" => ColumnKind::I64Array,
            "_float4" => ColumnKind::F32Array,
            "_float8" => ColumnKind::F64Array,
            "_text" | "_varchar" | "_bpchar" => ColumnKind::Utf8Array,
            "geometry" | "geography" => {
                return Err(DatabaseError::unsupported(
                    ProviderKind::Postgres,
                    ErrorPhase::Prepare,
                    "projection geometry raw: usare una funzione spatial che produca EWKB",
                ));
            }
            _ => {
                return Err(DatabaseError::unsupported(
                    ProviderKind::Postgres,
                    ErrorPhase::Prepare,
                    "tipo risultato query PostgreSQL non ancora mappato",
                ));
            }
        };
        Ok(Self {
            name: column.name().to_owned(),
            native_type,
            nullable: true,
            numeric_precision: None,
            numeric_scale: None,
            spatial_srid: None,
            spatial_dimensions: None,
            spatial_type: None,
            spatial_crs_id: None,
            default_expression: None,
            identity_kind: None,
            generated_kind: None,
            native_declaration: None,
            type_kind: None,
            composite_fields: Vec::new(),
            enum_labels: Vec::new(),
            domain_base_type: None,
            domain_constraints: Vec::new(),
            collation: None,
            kind,
        })
    }

    pub fn from_catalog_row(row: &Row) -> Result<Self> {
        let name: String = catalog_field(row, "attname")?;
        let native_type: String = catalog_field(row, "typname")?;
        let nullable: bool = catalog_field(row, "nullable")?;
        let precision_i32: Option<i32> = catalog_field(row, "numeric_precision")?;
        let scale_i32: Option<i32> = catalog_field(row, "numeric_scale")?;
        let srid_i32: Option<i32> = catalog_field(row, "spatial_srid")?;
        let spatial_dimension_count: Option<i32> = catalog_field(row, "spatial_dims")?;
        let spatial_typmod_type: Option<String> = catalog_field(row, "spatial_type")?;
        let spatial_crs_auth_name: Option<String> = catalog_field(row, "spatial_crs_auth_name")?;
        let spatial_crs_auth_srid: Option<i32> = catalog_field(row, "spatial_crs_auth_srid")?;
        let spatial_dimensions =
            spatial_dimensions_label(spatial_dimension_count, spatial_typmod_type.as_deref());
        let spatial_type = spatial_typmod_type.as_deref().map(spatial_base_type);
        let spatial_crs_id = spatial_crs_auth_name
            .filter(|value| !value.is_empty())
            .zip(spatial_crs_auth_srid.and_then(|value| u32::try_from(value).ok()))
            .map(|(authority, code)| format!("{authority}:{code}"));
        let default_expression = catalog_field(row, "default_expression")?;
        let identity_kind = catalog_field(row, "identity_kind")?;
        let generated_kind = catalog_field(row, "generated_kind")?;
        let native_declaration = catalog_field(row, "native_declaration")?;
        let type_kind: Option<String> = catalog_field(row, "type_kind")?;
        let composite_fields = catalog_json_list(row, "composite_fields")?;
        let enum_labels = catalog_json_list(row, "enum_labels")?;
        let domain_base_type = catalog_field(row, "domain_base_type")?;
        let domain_constraints = catalog_json_list(row, "domain_constraints")?;
        let collation = catalog_field(row, "collation")?;
        let numeric_precision = precision_i32.and_then(|value| u8::try_from(value).ok());
        let numeric_scale = scale_i32.and_then(|value| i8::try_from(value).ok());
        let kind = if type_kind.as_deref() == Some("c") {
            ColumnKind::Composite
        } else {
            match native_type.as_str() {
                "bool" => ColumnKind::Bool,
                "int2" | "int4" => ColumnKind::I32,
                "int8" => ColumnKind::I64,
                "float4" => ColumnKind::F32,
                "float8" => ColumnKind::F64,
                "bytea" => ColumnKind::Binary,
                "date" => ColumnKind::Date,
                "time" => ColumnKind::Time,
                "interval" => ColumnKind::Interval,
                "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
                    ColumnKind::Range
                }
                "timestamp" => ColumnKind::Timestamp,
                "timestamptz" => ColumnKind::TimestampTz,
                "numeric" => ColumnKind::Decimal {
                    precision: numeric_precision.unwrap_or(38),
                    scale: numeric_scale.unwrap_or(18),
                },
                "_bool" => ColumnKind::BoolArray,
                "_int2" | "_int4" => ColumnKind::I32Array,
                "_int8" => ColumnKind::I64Array,
                "_float4" => ColumnKind::F32Array,
                "_float8" => ColumnKind::F64Array,
                "_text" | "_varchar" | "_bpchar" => ColumnKind::Utf8Array,
                "geometry" => ColumnKind::Geometry,
                "geography" => ColumnKind::Geography,
                _ => ColumnKind::Utf8,
            }
        };
        Ok(Self {
            name,
            native_type,
            nullable,
            numeric_precision,
            numeric_scale,
            spatial_srid: srid_i32
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value > 0),
            spatial_dimensions,
            spatial_type,
            spatial_crs_id,
            default_expression,
            identity_kind,
            generated_kind,
            native_declaration,
            type_kind,
            composite_fields,
            enum_labels,
            domain_base_type,
            domain_constraints,
            collation,
            kind,
        })
    }

    pub fn arrow_field(&self) -> Field {
        Field::new(&self.name, self.arrow_data_type(), self.nullable)
            .with_metadata(self.arrow_metadata())
    }

    fn arrow_data_type(&self) -> DataType {
        match self.kind {
            ColumnKind::Bool => DataType::Boolean,
            ColumnKind::I32 => DataType::Int32,
            ColumnKind::I64 => DataType::Int64,
            ColumnKind::F32 => DataType::Float32,
            ColumnKind::F64 => DataType::Float64,
            ColumnKind::Utf8 => DataType::Utf8,
            ColumnKind::Binary | ColumnKind::Geometry | ColumnKind::Geography => DataType::Binary,
            ColumnKind::Date => DataType::Date32,
            ColumnKind::Time => DataType::Time64(TimeUnit::Microsecond),
            ColumnKind::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
            ColumnKind::Range => DataType::Struct(range_fields().into()),
            ColumnKind::Composite => DataType::Struct(
                self.composite_fields
                    .iter()
                    .map(|item| {
                        let mut metadata = HashMap::new();
                        metadata.insert(
                            protocol::POSTGRES_NATIVE_DECLARATION.to_owned(),
                            item.declaration.clone(),
                        );
                        Arc::new(
                            Field::new(&item.name, DataType::Utf8, true).with_metadata(metadata),
                        )
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            ColumnKind::TimestampTz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
            ColumnKind::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
            ColumnKind::BoolArray => {
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)))
            }
            ColumnKind::I32Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
            }
            ColumnKind::I64Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
            }
            ColumnKind::F32Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Float32, true)))
            }
            ColumnKind::F64Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true)))
            }
            ColumnKind::Utf8Array => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
        }
    }

    fn arrow_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        self.insert_postgres_metadata(&mut metadata);
        self.insert_spatial_metadata(&mut metadata);
        metadata
    }

    fn insert_postgres_metadata(&self, metadata: &mut HashMap<String, String>) {
        metadata.insert(
            protocol::POSTGRES_NATIVE_TYPE.to_owned(),
            self.native_type.clone(),
        );
        if let Some(declaration) = &self.native_declaration {
            metadata.insert(
                protocol::POSTGRES_NATIVE_DECLARATION.to_owned(),
                declaration.clone(),
            );
        }
        if let Some(type_kind) = &self.type_kind {
            metadata.insert(protocol::POSTGRES_TYPE_KIND.to_owned(), type_kind.clone());
        }
        if !self.enum_labels.is_empty() {
            if let Ok(value) = serde_json::to_string(&self.enum_labels) {
                metadata.insert(protocol::POSTGRES_ENUM_LABELS.to_owned(), value);
            }
        }
        if let Some(base_type) = &self.domain_base_type {
            metadata.insert(
                protocol::POSTGRES_DOMAIN_BASE_TYPE.to_owned(),
                base_type.clone(),
            );
        }
        if !self.domain_constraints.is_empty() {
            if let Ok(value) = serde_json::to_string(&self.domain_constraints) {
                metadata.insert(protocol::POSTGRES_DOMAIN_CONSTRAINTS.to_owned(), value);
            }
        }
        if let Some(collation) = &self.collation {
            metadata.insert(protocol::POSTGRES_COLLATION.to_owned(), collation.clone());
        }
    }

    fn insert_spatial_metadata(&self, metadata: &mut HashMap<String, String>) {
        if !matches!(self.kind, ColumnKind::Geometry | ColumnKind::Geography) {
            return;
        }
        metadata.insert(
            protocol::GEOARROW_EXTENSION_NAME.to_owned(),
            GEOARROW_WKB_EXTENSION_NAME.to_owned(),
        );
        metadata.insert(
            protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
            if matches!(self.kind, ColumnKind::Geography) {
                "geography"
            } else {
                "geometry"
            }
            .to_owned(),
        );
        if let Some(srid) = self.spatial_srid {
            metadata.insert(protocol::GEOMETRY_SRID.to_owned(), srid.to_string());
            metadata.insert(
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                if self.spatial_crs_id.is_some() {
                    "resolved"
                } else {
                    "declared_unresolved"
                }
                .to_owned(),
            );
            metadata.insert(
                protocol::GEOMETRY_AXIS_ORDER.to_owned(),
                "unknown".to_owned(),
            );
        } else {
            metadata.insert(
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "missing".to_owned(),
            );
        }
        if let Some(crs_id) = &self.spatial_crs_id {
            metadata.insert(protocol::GEOMETRY_CRS_ID.to_owned(), crs_id.clone());
        }
        if let Some(dimensions) = &self.spatial_dimensions {
            metadata.insert(
                protocol::GEOMETRY_DIMENSIONS.to_owned(),
                dimensions.to_ascii_lowercase(),
            );
        }
        if let Some(spatial_type) = &self.spatial_type {
            if spatial_type.eq_ignore_ascii_case("geometry") {
                metadata.insert(
                    protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                    "mixed".to_owned(),
                );
            } else {
                metadata.insert(
                    protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                    "exact".to_owned(),
                );
                metadata.insert(
                    protocol::GEOMETRY_TYPES.to_owned(),
                    spatial_type.to_ascii_lowercase(),
                );
            }
        }
        metadata.insert(protocol::GEOMETRY_ENCODING.to_owned(), "ewkb".to_owned());
    }

    pub fn projection_sql(&self, renderer: &Renderer) -> Result<String> {
        let identifier = Identifier::new(self.name.clone())?;
        let quoted = renderer.quote_identifier(&identifier);
        let expression = match self.kind {
            ColumnKind::Decimal { .. } => format!("{quoted}::text"),
            ColumnKind::Range => format!(
                "CASE WHEN {quoted} IS NULL THEN NULL \
                 WHEN isempty({quoted}) THEN \
                   jsonb_build_object('lower', NULL, 'upper', NULL, \
                     'lower_inclusive', false, 'upper_inclusive', false, \
                     'lower_unbounded', false, 'upper_unbounded', false, \
                     'empty', true)::text \
                 ELSE jsonb_build_object(\
                     'lower', lower({quoted})::text, \
                     'upper', upper({quoted})::text, \
                     'lower_inclusive', lower_inc({quoted}), \
                     'upper_inclusive', upper_inc({quoted}), \
                     'lower_unbounded', lower_inf({quoted}), \
                     'upper_unbounded', upper_inf({quoted}), \
                     'empty', false)::text END"
            ),
            ColumnKind::Composite => format!("to_jsonb({quoted})::text"),
            ColumnKind::Utf8 if matches!(self.native_type.as_str(), "json" | "jsonb" | "uuid") => {
                format!("{quoted}::text")
            }
            ColumnKind::Utf8
                if !matches!(
                    self.native_type.as_str(),
                    "text" | "varchar" | "bpchar" | "name"
                ) =>
            {
                format!("{quoted}::text")
            }
            ColumnKind::Geometry => format!("ST_AsEWKB({quoted})"),
            ColumnKind::Geography => {
                format!("ST_AsEWKB({quoted}::geometry)")
            }
            _ => quoted.clone(),
        };
        Ok(format!("{expression} AS {quoted}"))
    }

    pub fn initial_arrow_bytes_per_row(&self) -> u64 {
        match self.kind {
            ColumnKind::Bool => 2,
            ColumnKind::I32 | ColumnKind::F32 | ColumnKind::Date => 5,
            ColumnKind::I64
            | ColumnKind::F64
            | ColumnKind::Time
            | ColumnKind::Timestamp
            | ColumnKind::TimestampTz => 9,
            ColumnKind::Interval | ColumnKind::Decimal { .. } => 17,
            ColumnKind::Utf8 => 21,
            ColumnKind::Binary | ColumnKind::Geometry | ColumnKind::Geography => 37,
            ColumnKind::Range => 32,
            ColumnKind::Composite => usize_to_u64(self.composite_fields.len())
                .saturating_mul(21)
                .saturating_add(1),
            ColumnKind::BoolArray => 13,
            ColumnKind::I32Array | ColumnKind::F32Array => 25,
            ColumnKind::I64Array | ColumnKind::F64Array => 41,
            ColumnKind::Utf8Array => 137,
        }
    }
}

fn spatial_dimensions_label(count: Option<i32>, typmod_type: Option<&str>) -> Option<String> {
    let uppercase = typmod_type.unwrap_or_default().to_ascii_uppercase();
    if uppercase.ends_with("ZM") {
        return Some("xyzm".to_owned());
    }
    if uppercase.ends_with('M') {
        return Some("xym".to_owned());
    }
    if uppercase.ends_with('Z') {
        return Some("xyz".to_owned());
    }
    match count {
        Some(2) => Some("xy".to_owned()),
        Some(4) => Some("xyzm".to_owned()),
        _ => None,
    }
}

fn spatial_base_type(typmod_type: &str) -> String {
    let base = typmod_type
        .strip_suffix("ZM")
        .or_else(|| typmod_type.strip_suffix('Z'))
        .or_else(|| typmod_type.strip_suffix('M'))
        .unwrap_or(typmod_type);
    if base.eq_ignore_ascii_case("tin") {
        "TIN".to_owned()
    } else {
        base.to_owned()
    }
}
