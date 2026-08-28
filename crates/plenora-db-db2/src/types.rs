use crate::{Db2Column, Db2ObjectDescription};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::plan::{FilterExpression, ReadOperation, SortDirection};
use plenora_database_core::protocol::{self, contract_schema};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    lower_filter, select_columns_by_name, Dialect, DialectCapabilities, FilterLowering, Identifier,
    ObjectName, Renderer,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Db2ColumnKind {
    Bool,
    I16,
    I32,
    I64,
    F32,
    F64,
    Decimal { precision: u8, scale: i8 },
    Utf8,
    Geometry,
    Date,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Db2ColumnSpec {
    pub name: String,
    pub native_type: String,
    pub nullable: bool,
    pub kind: Db2ColumnKind,
    pub text_capacity: usize,
    pub spatial_srid: Option<u32>,
    pub geometry_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Db2SpatialCheck {
    pub column_index: usize,
    pub expected_srid: u32,
}

#[derive(Debug, Clone)]
pub struct Db2ReadPlan {
    pub columns: Vec<Db2ColumnSpec>,
    pub schema: SchemaRef,
    pub sql: String,
    pub bind_names: Vec<String>,
    pub schema_token: String,
    pub schema_name: String,
    pub object_name: String,
    pub spatial_checks: Vec<Db2SpatialCheck>,
}

impl Db2ColumnSpec {
    /// Traduce una colonna del catalogo nel sottoinsieme Arrow qualificato.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per tipi o decimali non rappresentabili e per metadati
    /// numerici che eccedono il contratto Arrow.
    pub fn from_catalog(column: &Db2Column, cell_limit: u64) -> Result<Self> {
        Self::from_catalog_declaring(column, cell_limit, None)
    }

    fn from_catalog_declaring(
        column: &Db2Column,
        cell_limit: u64,
        declared_srid: Option<u32>,
    ) -> Result<Self> {
        let native_type = column.data_type.to_ascii_uppercase();
        let geometry_type = canonical_geometry_type(&native_type).map(str::to_owned);
        let kind = match native_type.as_str() {
            "BOOLEAN" => Db2ColumnKind::Bool,
            "SMALLINT" => Db2ColumnKind::I16,
            "INTEGER" => Db2ColumnKind::I32,
            "BIGINT" => Db2ColumnKind::I64,
            "REAL" => Db2ColumnKind::F32,
            "DOUBLE" => Db2ColumnKind::F64,
            "DECIMAL" | "NUMERIC" => {
                let precision = u8::try_from(column.length).map_err(|_| {
                    mapping_error("precisione DECIMAL Db2 non rappresentabile in Arrow")
                })?;
                let scale = i8::try_from(column.scale)
                    .map_err(|_| mapping_error("scala DECIMAL Db2 non rappresentabile in Arrow"))?;
                let signed_precision = i8::try_from(precision)
                    .map_err(|_| mapping_error("precisione DECIMAL Db2 non valida"))?;
                if precision == 0 || precision > 38 || scale < 0 || scale > signed_precision {
                    return Err(mapping_error("DECIMAL Db2 oltre il contratto Decimal128"));
                }
                Db2ColumnKind::Decimal { precision, scale }
            }
            "CHARACTER" | "CHAR" | "VARCHAR" | "CLOB" => Db2ColumnKind::Utf8,
            spatial if is_spatial_type(spatial) => {
                if declared_srid.is_none() {
                    return Err(prepare_error(
                        ErrorCategory::Crs,
                        "colonna spatial Db2 senza declared_crs",
                    ));
                }
                Db2ColumnKind::Geometry
            }
            "DATE" => Db2ColumnKind::Date,
            "TIMESTAMP" => Db2ColumnKind::Timestamp,
            _ => {
                return Err(DatabaseError::unsupported(
                    plenora_database_core::plan::ProviderKind::Db2,
                    ErrorPhase::Prepare,
                    "tipo Db2 non ancora qualificato per Arrow",
                ))
            }
        };
        let fixed = match kind {
            Db2ColumnKind::Bool => 8,
            Db2ColumnKind::I16
            | Db2ColumnKind::I32
            | Db2ColumnKind::I64
            | Db2ColumnKind::F32
            | Db2ColumnKind::F64
            | Db2ColumnKind::Decimal { .. }
            | Db2ColumnKind::Timestamp => 64,
            Db2ColumnKind::Date => 32,
            Db2ColumnKind::Utf8 => usize::try_from(column.length)
                .unwrap_or(usize::MAX)
                .saturating_mul(4)
                .max(1),
            // Il driver CLI converte il BLOB di `ST_AsBinary` in due caratteri
            // esadecimali per byte quando il result set e legato come testo.
            Db2ColumnKind::Geometry => usize::try_from(cell_limit)
                .unwrap_or(usize::MAX)
                .saturating_mul(2),
        };
        let cell_limit = usize::try_from(cell_limit).unwrap_or(usize::MAX);
        let text_capacity = if kind == Db2ColumnKind::Geometry {
            fixed
        } else {
            fixed.min(cell_limit)
        };
        Ok(Self {
            name: column.name.clone(),
            native_type,
            nullable: column.nullable,
            kind,
            text_capacity,
            spatial_srid: declared_srid,
            geometry_type,
        })
    }

    #[must_use]
    /// # Panics
    ///
    /// Solo se viene costruito manualmente uno spec `Geometry` senza lo SRID
    /// che il compilatore Db2 rende obbligatorio.
    pub fn arrow_field(&self) -> Field {
        let data_type = match self.kind {
            Db2ColumnKind::Bool => DataType::Boolean,
            Db2ColumnKind::I16 => DataType::Int16,
            Db2ColumnKind::I32 => DataType::Int32,
            Db2ColumnKind::I64 => DataType::Int64,
            Db2ColumnKind::F32 => DataType::Float32,
            Db2ColumnKind::F64 => DataType::Float64,
            Db2ColumnKind::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
            Db2ColumnKind::Utf8 => DataType::Utf8,
            Db2ColumnKind::Geometry => DataType::Binary,
            Db2ColumnKind::Date => DataType::Date32,
            Db2ColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        };
        let mut metadata = HashMap::from([(
            "plenora.db2.native_type".to_owned(),
            self.native_type.clone(),
        )]);
        if self.kind == Db2ColumnKind::Geometry {
            metadata.extend([
                (
                    protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                    "geoarrow.wkb".to_owned(),
                ),
                (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
                (
                    protocol::GEOMETRY_DIMENSIONS.to_owned(),
                    "unknown".to_owned(),
                ),
                (
                    protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                    if self.geometry_type.is_some() {
                        "exact".to_owned()
                    } else {
                        "mixed".to_owned()
                    },
                ),
                (
                    protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                    "geometry".to_owned(),
                ),
                (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
                (
                    protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                    "declared_unresolved".to_owned(),
                ),
                (
                    protocol::GEOMETRY_SRID.to_owned(),
                    self.spatial_srid
                        .expect("una geometry compilata ha sempre un SRID dichiarato")
                        .to_string(),
                ),
            ]);
            if let Some(geometry_type) = &self.geometry_type {
                metadata.insert(protocol::GEOMETRY_TYPES.to_owned(), geometry_type.clone());
            }
        }
        Field::new(&self.name, data_type, self.nullable).with_metadata(metadata)
    }
}

impl Db2ReadPlan {
    /// Compila una lettura nel dialetto Db2 e nello schema Arrow pubblico.
    ///
    /// # Errors
    ///
    /// Rifiuta prima della rete identificatori, projection, filtri,
    /// paginazione, tipi o CRS che non rispettano il contratto qualificato.
    pub fn compile(
        description: &Db2ObjectDescription,
        operation: &ReadOperation,
        cell_limit: u64,
    ) -> Result<Self> {
        validate_read_operation(operation)?;
        let declared = resolve_declared_crs(description, operation)?;
        let columns = selected_columns(description, operation, cell_limit, &declared)?;
        if columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "projection Db2 vuota",
            ));
        }
        let renderer = Renderer::new(
            Dialect::Db2,
            DialectCapabilities {
                spatial_intersects: false,
            },
        );
        let mut projections = columns
            .iter()
            .map(|column| {
                let quoted = renderer.quote_identifier(&Identifier::new(column.name.clone())?)?;
                if column.kind == Db2ColumnKind::Geometry {
                    Ok(format!("ST_ASBINARY({quoted}) AS {quoted}"))
                } else {
                    Ok(quoted)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let spatial_checks = columns
            .iter()
            .enumerate()
            .filter_map(|(column_index, column)| {
                column.spatial_srid.map(|expected_srid| Db2SpatialCheck {
                    column_index,
                    expected_srid,
                })
            })
            .collect::<Vec<_>>();
        for (index, check) in spatial_checks.iter().enumerate() {
            let quoted = renderer
                .quote_identifier(&Identifier::new(columns[check.column_index].name.clone())?)?;
            projections.push(format!("ST_SRID({quoted}) AS \"__PLENORA_SRID_{index}\""));
            projections.push(format!(
                "ST_COORDDIM({quoted}) AS \"__PLENORA_COORDDIM_{index}\""
            ));
        }
        let projection = projections.join(", ");
        let object = ObjectName {
            catalog: None,
            schema: Some(Identifier::new(description.schema.clone())?),
            object: Identifier::new(description.name.clone())?,
        };
        let mut sql = format!(
            "SELECT {projection} FROM {}",
            renderer.quote_object(&object)?
        );
        let available_names = description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        let bind_names = if let Some((filter_sql, filter_binds)) =
            render_filter(&renderer, operation.filter.as_ref(), &available_names)?
        {
            sql.push_str(" WHERE ");
            sql.push_str(&filter_sql);
            filter_binds
        } else {
            Vec::new()
        };
        if let Some(ordering) = render_ordering(&renderer, operation, &available_names)? {
            sql.push_str(" ORDER BY ");
            sql.push_str(&ordering);
        }
        if let Some(offset) = operation.row_offset {
            sql.push_str(" OFFSET ");
            sql.push_str(&offset.to_string());
            sql.push_str(" ROWS");
        }
        if let Some(limit) = operation.row_limit {
            sql.push_str(" FETCH FIRST ");
            sql.push_str(&limit.to_string());
            sql.push_str(" ROWS ONLY");
        }
        let schema = contract_schema(columns.iter().map(Db2ColumnSpec::arrow_field).collect());
        Ok(Self {
            columns,
            schema,
            sql,
            bind_names,
            schema_token: description.schema_token.clone(),
            schema_name: description.schema.clone(),
            object_name: description.name.clone(),
            spatial_checks,
        })
    }

    #[must_use]
    pub fn text_buffer_bytes(&self, rows: usize) -> u64 {
        self.wire_text_capacities()
            .into_iter()
            .map(|capacity| capacity.saturating_add(1 + size_of::<isize>()))
            .sum::<usize>()
            .saturating_mul(rows) as u64
    }

    #[must_use]
    pub fn wire_text_capacities(&self) -> Vec<usize> {
        let mut capacities = self
            .columns
            .iter()
            .map(|column| column.text_capacity)
            .collect::<Vec<_>>();
        capacities.extend(std::iter::repeat_n(16, self.spatial_checks.len() * 2));
        capacities
    }
}

fn validate_read_operation(operation: &ReadOperation) -> Result<()> {
    if (operation.row_limit.is_some() || operation.row_offset.is_some())
        && operation.order_by.is_empty()
    {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "FETCH/OFFSET Db2 richiedono ORDER BY esplicito",
        ));
    }
    Ok(())
}

fn selected_columns(
    description: &Db2ObjectDescription,
    operation: &ReadOperation,
    cell_limit: u64,
    declared: &BTreeMap<&str, u32>,
) -> Result<Vec<Db2ColumnSpec>> {
    select_columns_by_name(
        &description.columns,
        &operation.projection,
        |column| column.name.as_str(),
        || {
            prepare_error(
                ErrorCategory::NotFound,
                "colonna projection Db2 non trovata",
            )
        },
    )?
    .into_iter()
    .map(|column| {
        let srid = declared.get(column.name.as_str()).copied();
        Db2ColumnSpec::from_catalog_declaring(&column, cell_limit, srid)
    })
    .collect()
}

fn resolve_declared_crs<'a>(
    description: &Db2ObjectDescription,
    operation: &'a ReadOperation,
) -> Result<BTreeMap<&'a str, u32>> {
    let mut declared = BTreeMap::new();
    for declaration in &operation.declared_crs {
        if declaration.srid == 0 {
            return Err(prepare_error(
                ErrorCategory::Crs,
                "declared_crs Db2 con SRID zero",
            ));
        }
        let column = description
            .columns
            .iter()
            .find(|column| column.name == declaration.column)
            .ok_or_else(|| {
                prepare_error(
                    ErrorCategory::NotFound,
                    "declared_crs Db2 su colonna non trovata",
                )
            })?;
        if !is_spatial_type(&column.data_type.to_ascii_uppercase()) {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "declared_crs Db2 su colonna non spatial",
            ));
        }
        if declared
            .insert(declaration.column.as_str(), declaration.srid)
            .is_some()
        {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "declared_crs Db2 duplicato",
            ));
        }
    }
    Ok(declared)
}

#[must_use]
pub fn is_spatial_type(native_type: &str) -> bool {
    matches!(
        native_type,
        "ST_GEOMETRY"
            | "ST_POINT"
            | "ST_LINESTRING"
            | "ST_POLYGON"
            | "ST_MULTIPOINT"
            | "ST_MULTILINESTRING"
            | "ST_MULTIPOLYGON"
            | "ST_GEOMCOLLECTION"
    )
}

fn canonical_geometry_type(native_type: &str) -> Option<&'static str> {
    match native_type {
        "ST_POINT" => Some("point"),
        "ST_LINESTRING" => Some("linestring"),
        "ST_POLYGON" => Some("polygon"),
        "ST_MULTIPOINT" => Some("multipoint"),
        "ST_MULTILINESTRING" => Some("multilinestring"),
        "ST_MULTIPOLYGON" => Some("multipolygon"),
        "ST_GEOMCOLLECTION" => Some("geometrycollection"),
        _ => None,
    }
}

fn render_filter(
    renderer: &Renderer,
    filter: Option<&FilterExpression>,
    available_names: &BTreeSet<&str>,
) -> Result<Option<(String, Vec<String>)>> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    if !filter.all_fields(&|field| available_names.contains(field)) {
        return Err(prepare_error(
            ErrorCategory::NotFound,
            "filtro Db2 su colonna non trovata",
        ));
    }
    let lowered = lower_filter(
        filter,
        FilterLowering {
            provider: plenora_database_core::plan::ProviderKind::Db2,
            case_insensitive_like: false,
            spatial: false,
        },
        |value| Identifier::new(value.to_owned()),
    )?;
    let rendered_filter = renderer.render_filter(&lowered)?;
    Ok(Some((
        rendered_filter.sql,
        rendered_filter
            .binds
            .into_iter()
            .map(|bind| bind.name)
            .collect(),
    )))
}

fn render_ordering(
    renderer: &Renderer,
    operation: &ReadOperation,
    available_names: &BTreeSet<&str>,
) -> Result<Option<String>> {
    if operation.order_by.is_empty() {
        return Ok(None);
    }
    operation
        .order_by
        .iter()
        .map(|order| {
            if !available_names.contains(order.field.as_str()) {
                return Err(prepare_error(
                    ErrorCategory::NotFound,
                    "colonna ORDER BY Db2 non trovata",
                ));
            }
            let field = renderer.quote_identifier(&Identifier::new(order.field.clone())?)?;
            let direction = match order.direction {
                SortDirection::Asc => "ASC",
                SortDirection::Desc => "DESC",
            };
            Ok(format!("{field} {direction}"))
        })
        .collect::<Result<Vec<_>>>()
        .map(|ordering| Some(ordering.join(", ")))
}

fn prepare_error(category: ErrorCategory, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(plenora_database_core::plan::ProviderKind::Db2),
        message,
    )
}

fn mapping_error(message: &'static str) -> DatabaseError {
    prepare_error(ErrorCategory::DataMapping, message)
}
