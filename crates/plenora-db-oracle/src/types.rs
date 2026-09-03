use crate::{OracleColumn, OracleObjectDescription};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::plan::{ProviderKind, ReadOperation, SortDirection};
use plenora_database_core::protocol::{self, contract_schema};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    lower_filter, select_columns_by_name, Dialect, DialectCapabilities, FilterLowering, Identifier,
    ObjectName, Renderer,
};
use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleColumnKind {
    Bool,
    I64,
    F32,
    F64,
    Decimal { precision: u8, scale: i8 },
    Utf8,
    Binary,
    DateTime,
    TimestampTz,
    Geometry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleColumnSpec {
    pub name: String,
    pub native_type: String,
    pub nullable: bool,
    pub kind: OracleColumnKind,
    pub spatial_srid: Option<u32>,
    pub spatial_dimensions: Option<u8>,
}

#[derive(Debug, Clone)]
pub struct OracleReadPlan {
    pub columns: Vec<OracleColumnSpec>,
    pub schema: SchemaRef,
    pub sql: String,
    pub bind_names: Vec<String>,
    pub schema_token: String,
    pub schema_name: String,
    pub object_name: String,
    pub spatial_columns: Vec<usize>,
}

impl OracleColumnSpec {
    /// Converte una colonna catalogata nel sottoinsieme Arrow qualificato.
    ///
    /// # Errors
    ///
    /// Rifiuta tipi, precisioni o metadati Spatial non rappresentabili.
    pub fn from_catalog(column: &OracleColumn) -> Result<Self> {
        let native_type = column.data_type.to_ascii_uppercase();
        let kind = match native_type.as_str() {
            "BOOLEAN" => OracleColumnKind::Bool,
            "NUMBER" if column.scale.unwrap_or(0) == 0 && column.precision.unwrap_or(38) <= 18 => {
                OracleColumnKind::I64
            }
            "NUMBER" => decimal_kind(column)?,
            "BINARY_FLOAT" => OracleColumnKind::F32,
            "BINARY_DOUBLE" | "FLOAT" => OracleColumnKind::F64,
            "CHAR" | "NCHAR" | "VARCHAR2" | "NVARCHAR2" | "CLOB" | "NCLOB" | "JSON" => {
                OracleColumnKind::Utf8
            }
            "RAW" | "LONG RAW" | "BLOB" => OracleColumnKind::Binary,
            "DATE" => OracleColumnKind::DateTime,
            value if value.starts_with("TIMESTAMP") && value.contains("WITH TIME ZONE") => {
                OracleColumnKind::TimestampTz
            }
            value if value.starts_with("TIMESTAMP") && !value.contains("TIME ZONE") => {
                OracleColumnKind::DateTime
            }
            "SDO_GEOMETRY" => {
                if column.spatial_srid.is_none()
                    || !matches!(column.spatial_dimensions, Some(2 | 3))
                {
                    return Err(prepare_error(
                        ErrorCategory::Crs,
                        "colonna SDO_GEOMETRY Oracle senza CRS e dimensioni qualificate",
                    ));
                }
                OracleColumnKind::Geometry
            }
            _ => {
                return Err(DatabaseError::unsupported(
                    ProviderKind::Oracle,
                    ErrorPhase::Prepare,
                    "tipo Oracle non ancora qualificato per Arrow",
                ));
            }
        };
        Ok(Self {
            name: column.name.clone(),
            native_type,
            nullable: column.nullable,
            kind,
            spatial_srid: column.spatial_srid,
            spatial_dimensions: column.spatial_dimensions,
        })
    }

    /// Costruisce il campo Arrow e i metadati canonici `GeoArrow`.
    ///
    /// # Panics
    ///
    /// Una `OracleColumnSpec` costruita manualmente con kind Geometry e senza
    /// SRID viola l'invariante stabilita da `from_catalog`.
    #[must_use]
    pub fn arrow_field(&self) -> Field {
        let data_type = match self.kind {
            OracleColumnKind::Bool => DataType::Boolean,
            OracleColumnKind::I64 => DataType::Int64,
            OracleColumnKind::F32 => DataType::Float32,
            OracleColumnKind::F64 => DataType::Float64,
            OracleColumnKind::Decimal { precision, scale } => {
                DataType::Decimal128(precision, scale)
            }
            OracleColumnKind::Utf8 => DataType::Utf8,
            OracleColumnKind::Binary | OracleColumnKind::Geometry => DataType::Binary,
            OracleColumnKind::DateTime => DataType::Timestamp(TimeUnit::Microsecond, None),
            OracleColumnKind::TimestampTz => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
        };
        let mut metadata = HashMap::from([(
            "plenora.oracle.native_type".to_owned(),
            self.native_type.clone(),
        )]);
        if self.kind == OracleColumnKind::Geometry {
            metadata.extend([
                (
                    protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                    "geoarrow.wkb".to_owned(),
                ),
                (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
                (
                    protocol::GEOMETRY_DIMENSIONS.to_owned(),
                    if self.spatial_dimensions == Some(3) {
                        "xyz"
                    } else {
                        "xy"
                    }
                    .to_owned(),
                ),
                (
                    protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                    "mixed".to_owned(),
                ),
                (
                    protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                    "geometry".to_owned(),
                ),
                (protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned()),
                (
                    protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                    "resolved".to_owned(),
                ),
                (
                    protocol::GEOMETRY_SRID.to_owned(),
                    self.spatial_srid
                        .expect("geometry compilata con SRID")
                        .to_string(),
                ),
            ]);
        }
        Field::new(&self.name, data_type, self.nullable).with_metadata(metadata)
    }
}

impl OracleReadPlan {
    /// Compila la lettura catalogata nel dialetto Oracle.
    ///
    /// # Errors
    ///
    /// Rifiuta projection, filtri, ordinamenti, CRS o tipi non qualificati.
    #[allow(clippy::too_many_lines)]
    pub fn compile(
        description: &OracleObjectDescription,
        operation: &ReadOperation,
    ) -> Result<Self> {
        if (operation.row_limit.is_some() || operation.row_offset.is_some())
            && operation.order_by.is_empty()
        {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "FETCH/OFFSET Oracle richiedono ORDER BY esplicito",
            ));
        }
        if !operation.declared_crs.is_empty() {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "declared_crs non ammesso: Oracle legge il CRS dal catalogo Spatial",
            ));
        }
        let selected = select_columns_by_name(
            &description.columns,
            &operation.projection,
            |column| column.name.as_str(),
            || {
                prepare_error(
                    ErrorCategory::NotFound,
                    "colonna projection Oracle non trovata",
                )
            },
        )?;
        if selected.is_empty() {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "projection Oracle vuota",
            ));
        }
        let columns = selected
            .iter()
            .map(OracleColumnSpec::from_catalog)
            .collect::<Result<Vec<_>>>()?;
        let renderer = Renderer::new(
            Dialect::Oracle,
            DialectCapabilities {
                spatial_intersects: true,
            },
        );
        let mut projections = columns
            .iter()
            .map(|column| {
                let quoted = renderer.quote_identifier(&Identifier::new(column.name.clone())?)?;
                let source = format!("T.{quoted}");
                if column.kind == OracleColumnKind::Geometry {
                    Ok(format!(
                        "MDSYS.SDO_UTIL.TO_WKBGEOMETRY({source}) AS {quoted}"
                    ))
                } else if column.kind == OracleColumnKind::Bool {
                    Ok(format!(
                        "CASE WHEN {source} THEN 1 WHEN NOT {source} THEN 0 END AS {quoted}"
                    ))
                } else if matches!(column.kind, OracleColumnKind::F32 | OracleColumnKind::F64) {
                    Ok(format!("CAST({source} AS NUMBER) AS {quoted}"))
                } else {
                    Ok(format!("{source} AS {quoted}"))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let spatial_columns = columns
            .iter()
            .enumerate()
            .filter_map(|(index, column)| {
                (column.kind == OracleColumnKind::Geometry).then_some(index)
            })
            .collect::<Vec<_>>();
        for (check, column_index) in spatial_columns.iter().enumerate() {
            let quoted = renderer
                .quote_identifier(&Identifier::new(columns[*column_index].name.clone())?)?;
            projections.push(format!("T.{quoted}.SDO_SRID AS \"__PLENORA_SRID_{check}\""));
            projections.push(format!(
                "T.{quoted}.ST_CoordDim() AS \"__PLENORA_DIM_{check}\""
            ));
        }
        let projection = projections.join(", ");
        let object = ObjectName {
            catalog: None,
            schema: Some(Identifier::new(description.schema.clone())?),
            object: Identifier::new(description.name.clone())?,
        };
        let mut sql = format!(
            "SELECT {projection} FROM {} T",
            renderer.quote_object(&object)?
        );
        let available = description
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<BTreeSet<_>>();
        let bind_names = if let Some(filter) = &operation.filter {
            if !filter.all_fields(&|name| available.contains(name)) {
                return Err(prepare_error(
                    ErrorCategory::NotFound,
                    "filtro Oracle su colonna non trovata",
                ));
            }
            let lowered = lower_filter(
                filter,
                FilterLowering {
                    provider: ProviderKind::Oracle,
                    case_insensitive_like: false,
                    spatial: true,
                },
                |name| Identifier::new(name.to_owned()),
            )?;
            let filter_sql = renderer.render_filter(&lowered)?;
            sql.push_str(" WHERE ");
            sql.push_str(&filter_sql.sql);
            filter_sql.binds.into_iter().map(|bind| bind.name).collect()
        } else {
            Vec::new()
        };
        if !operation.order_by.is_empty() {
            let order = operation
                .order_by
                .iter()
                .map(|item| {
                    if !available.contains(item.field.as_str()) {
                        return Err(prepare_error(
                            ErrorCategory::NotFound,
                            "ORDER BY Oracle su colonna non trovata",
                        ));
                    }
                    let name = renderer.quote_identifier(&Identifier::new(item.field.clone())?)?;
                    Ok(format!(
                        "T.{name} {}",
                        if item.direction == SortDirection::Asc {
                            "ASC"
                        } else {
                            "DESC"
                        }
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(" ORDER BY ");
            sql.push_str(&order.join(", "));
        }
        if let Some(offset) = operation.row_offset {
            write!(sql, " OFFSET {offset} ROWS").expect("scrittura su String infallibile");
        }
        if let Some(limit) = operation.row_limit {
            sql.push_str(if operation.row_offset.is_some() {
                " FETCH NEXT "
            } else {
                " FETCH FIRST "
            });
            sql.push_str(&limit.to_string());
            sql.push_str(" ROWS ONLY");
        }
        let schema = contract_schema(columns.iter().map(OracleColumnSpec::arrow_field).collect());
        Ok(Self {
            columns,
            schema,
            sql,
            bind_names,
            schema_token: description.schema_token.clone(),
            schema_name: description.schema.clone(),
            object_name: description.name.clone(),
            spatial_columns,
        })
    }
}

fn decimal_kind(column: &OracleColumn) -> Result<OracleColumnKind> {
    let precision = column.precision.unwrap_or(38);
    let scale = column.scale.unwrap_or(0);
    let precision = u8::try_from(precision).map_err(|_| {
        prepare_error(
            ErrorCategory::DataMapping,
            "precisione NUMBER Oracle non rappresentabile",
        )
    })?;
    let scale = i8::try_from(scale).map_err(|_| {
        prepare_error(
            ErrorCategory::DataMapping,
            "scala NUMBER Oracle non rappresentabile",
        )
    })?;
    if precision == 0 || precision > 38 || scale < 0 || scale > precision.cast_signed() {
        return Err(prepare_error(
            ErrorCategory::DataMapping,
            "NUMBER Oracle oltre Decimal128",
        ));
    }
    Ok(OracleColumnKind::Decimal { precision, scale })
}

fn prepare_error(category: ErrorCategory, message: &'static str) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(ProviderKind::Oracle),
        message,
    )
}
