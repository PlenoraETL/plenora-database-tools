use crate::catalog::{SqlServerColumn, SqlServerObjectDescription};
use plenora_database_core::arrow::schema::{DataType, Field, SchemaRef, TimeUnit};
use plenora_database_core::geometry::{Dimensions, SpatialSemantics};
use plenora_database_core::plan::{FilterExpression, ReadOperation, SortDirection};
use plenora_database_core::protocol::{self, contract_schema};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use plenora_database_sql::{
    lower_filter, select_columns_by_name, Dialect, DialectCapabilities, Expression, FilterLowering,
    Identifier, ObjectName, Renderer,
};
use std::collections::{BTreeSet, HashMap};

pub fn spatial_dimensions_from_profile(count: i64, code: Option<i32>) -> Result<Dimensions> {
    if count > 1 {
        return Err(prepare_error(
            ErrorCategory::DataMapping,
            "colonna spatial SQL Server con dimensioni miste",
        ));
    }
    match (count, code) {
        (0, None) => Ok(Dimensions::Unknown),
        (1, Some(0)) => Ok(Dimensions::Xy),
        (1, Some(1)) => Ok(Dimensions::Xym),
        (1, Some(2)) => Ok(Dimensions::Xyz),
        (1, Some(3)) => Ok(Dimensions::Xyzm),
        _ => Err(prepare_error(
            ErrorCategory::Protocol,
            "profilo dimensionale SQL Server incoerente",
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlServerColumnKind {
    Bool,
    U8,
    I16,
    I32,
    I64,
    F32,
    F64,
    Utf8,
    Binary,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Decimal { precision: u8, scale: i8 },
    Geometry,
    Geography,
}

/// Rappresentazione effettiva ricevuta sul wire TDS.
///
/// Il read tabellare proietta alcuni tipi in testo o WKB; il percorso
/// relazionale ricco riceve invece i valori nativi descritti dal server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlServerWireEncoding {
    Projected,
    Native,
}

impl SqlServerColumnKind {
    #[must_use]
    pub const fn spatial_semantics(&self) -> Option<SpatialSemantics> {
        match self {
            Self::Geometry => Some(SpatialSemantics::Geometry),
            Self::Geography => Some(SpatialSemantics::Geography),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlServerColumnSpec {
    pub name: String,
    pub native_type: String,
    pub native_declaration: String,
    pub nullable: bool,
    pub collation: Option<String>,
    pub kind: SqlServerColumnKind,
    pub spatial_srid: Option<u32>,
    pub spatial_dimensions: Option<Dimensions>,
    pub wire_encoding: SqlServerWireEncoding,
}

#[derive(Debug, Clone)]
pub struct SqlServerReadPlan {
    pub columns: Vec<SqlServerColumnSpec>,
    pub schema: SchemaRef,
    pub sql: String,
    pub bind_names: Vec<String>,
    pub structural_fingerprint: String,
}

impl SqlServerReadPlan {
    /// Compila un piano immutabile e una proiezione T-SQL deterministica.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per tipi non rappresentabili, identificatori oltre i
    /// limiti SQL Server o oggetti senza colonne.
    pub fn compile(description: &SqlServerObjectDescription) -> Result<Self> {
        if description.columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "oggetto SQL Server privo di colonne leggibili",
            ));
        }
        let renderer = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        );
        let object = ObjectName {
            catalog: None,
            schema: Some(sql_server_identifier(&description.schema)?),
            object: sql_server_identifier(&description.name)?,
        };
        let columns = description
            .columns
            .iter()
            .map(SqlServerColumnSpec::from_catalog)
            .collect::<Result<Vec<_>>>()?;
        let projection = columns
            .iter()
            .map(|column| column.projection(&renderer))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let fields = columns
            .iter()
            .map(SqlServerColumnSpec::arrow_field)
            .collect::<Vec<_>>();
        Ok(Self {
            columns,
            schema: contract_schema(fields),
            sql: format!(
                "SELECT {projection} FROM {} ORDER BY (SELECT NULL);",
                renderer.quote_object(&object)?
            ),
            bind_names: Vec::new(),
            structural_fingerprint: description.token.structural_fingerprint.clone(),
        })
    }

    /// Compila projection, filtri, ordinamento e limite del contratto read
    /// comune mantenendo tutti i valori fuori dal testo SQL.
    pub(crate) fn compile_operation(
        description: &SqlServerObjectDescription,
        operation: &ReadOperation,
    ) -> Result<Self> {
        if description.columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "oggetto SQL Server privo di colonne leggibili",
            ));
        }
        let renderer = sql_server_renderer();
        let available = description
            .columns
            .iter()
            .map(SqlServerColumnSpec::from_catalog)
            .collect::<Result<Vec<_>>>()?;
        let columns = select_columns(&available, &operation.projection)?;
        let projection = columns
            .iter()
            .map(|column| column.projection(&renderer))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let object = ObjectName {
            catalog: None,
            schema: Some(sql_server_identifier(&description.schema)?),
            object: sql_server_identifier(&description.name)?,
        };
        let mut sql = String::from("SELECT ");
        // `TOP` **solo** senza finestra. SQL Server rifiuta `TOP` insieme a
        // `OFFSET ... FETCH` nella stessa espressione — e un errore di
        // sintassi, non una preferenza — quindi quando il piano chiede un
        // offset il tetto viaggia in coda come `FETCH NEXT`.
        if operation.row_offset.is_none() {
            if let Some(limit) = operation.row_limit {
                sql.push_str("TOP (");
                sql.push_str(&limit.to_string());
                sql.push_str(") ");
            }
        }
        sql.push_str(&projection);
        sql.push_str(" FROM ");
        sql.push_str(&renderer.quote_object(&object)?);

        let mut bind_names = Vec::new();
        if let Some(filter) = &operation.filter {
            ensure_filter_columns(filter, &available)?;
            let rendered_filter = renderer.render_filter(&convert_filter(filter)?)?;
            sql.push_str(" WHERE ");
            sql.push_str(&rendered_filter.sql);
            bind_names.extend(rendered_filter.binds.into_iter().map(|bind| bind.name));
        }
        if operation.order_by.is_empty() {
            sql.push_str(" ORDER BY (SELECT NULL)");
        } else {
            let available_names = available
                .iter()
                .map(|column| column.name.as_str())
                .collect::<BTreeSet<_>>();
            let ordering = operation
                .order_by
                .iter()
                .map(|order| {
                    if !available_names.contains(order.field.as_str()) {
                        return Err(prepare_error(
                            ErrorCategory::NotFound,
                            "colonna ORDER BY SQL Server non trovata",
                        ));
                    }
                    let field = renderer.quote_identifier(&sql_server_identifier(&order.field)?)?;
                    let direction = match order.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    };
                    Ok(format!("{field} {direction}"))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(" ORDER BY ");
            sql.push_str(&ordering.join(", "));
        }
        // La forma del dialetto e `OFFSET n ROWS [FETCH NEXT m ROWS ONLY]`, e
        // va dopo l'`ORDER BY` — che questo ramo scrive sempre, anche quando
        // il piano non ne chiede uno, con `(SELECT NULL)`. Il tetto arriva qui
        // e non come `TOP` per la ragione detta sopra: le due forme non
        // convivono.
        if let Some(offset) = operation.row_offset {
            sql.push_str(" OFFSET ");
            sql.push_str(&offset.to_string());
            sql.push_str(" ROWS");
            if let Some(limit) = operation.row_limit {
                sql.push_str(" FETCH NEXT ");
                sql.push_str(&limit.to_string());
                sql.push_str(" ROWS ONLY");
            }
        }
        sql.push(';');
        let fields = columns
            .iter()
            .map(SqlServerColumnSpec::arrow_field)
            .collect::<Vec<_>>();
        Ok(Self {
            columns,
            schema: contract_schema(fields),
            sql,
            bind_names,
            structural_fingerprint: description.token.structural_fingerprint.clone(),
        })
    }

    pub(crate) fn apply_spatial_contract(
        &mut self,
        index: usize,
        srid: Option<u32>,
        dimensions: Dimensions,
    ) -> Result<()> {
        let column = self
            .columns
            .get_mut(index)
            .ok_or_else(|| prepare_error(ErrorCategory::Internal, "indice spatial non valido"))?;
        column.spatial_srid = srid;
        column.spatial_dimensions = Some(dimensions);
        let fields = self
            .columns
            .iter()
            .map(SqlServerColumnSpec::arrow_field)
            .collect::<Vec<_>>();
        self.schema = contract_schema(fields);
        Ok(())
    }

    pub(crate) fn apply_query_spatial_contract(
        &mut self,
        index: usize,
        semantics: SpatialSemantics,
        srid: Option<u32>,
        dimensions: Dimensions,
    ) -> Result<()> {
        let column = self
            .columns
            .get_mut(index)
            .ok_or_else(|| prepare_error(ErrorCategory::Internal, "indice spatial non valido"))?;
        if column.kind != SqlServerColumnKind::Binary {
            return Err(prepare_error(
                ErrorCategory::Protocol,
                "output spatial SQL Server non descritto come varbinary",
            ));
        }
        let (kind, native) = match semantics {
            SpatialSemantics::Geometry => (SqlServerColumnKind::Geometry, "geometry"),
            SpatialSemantics::Geography => (SqlServerColumnKind::Geography, "geography"),
        };
        column.kind = kind;
        native.clone_into(&mut column.native_type);
        native.clone_into(&mut column.native_declaration);
        column.collation = None;
        column.spatial_srid = srid;
        column.spatial_dimensions = Some(dimensions);
        column.wire_encoding = SqlServerWireEncoding::Projected;
        let fields = self
            .columns
            .iter()
            .map(SqlServerColumnSpec::arrow_field)
            .collect::<Vec<_>>();
        self.schema = contract_schema(fields);
        Ok(())
    }

    pub(crate) fn from_query_result(
        sql: String,
        bind_names: Vec<String>,
        columns: Vec<SqlServerColumnSpec>,
    ) -> Result<Self> {
        if columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "QueryOperation SQL Server priva di colonne risultanti",
            ));
        }
        let fields = columns
            .iter()
            .map(SqlServerColumnSpec::arrow_field)
            .collect::<Vec<_>>();
        Ok(Self {
            columns,
            schema: contract_schema(fields),
            sql,
            bind_names,
            structural_fingerprint: "query-result-metadata-v1".to_owned(),
        })
    }
}

const fn sql_server_renderer() -> Renderer {
    Renderer::new(
        Dialect::SqlServer,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
}

fn select_columns(
    available: &[SqlServerColumnSpec],
    projection: &[String],
) -> Result<Vec<SqlServerColumnSpec>> {
    select_columns_by_name(
        available,
        projection,
        |column| column.name.as_str(),
        || {
            prepare_error(
                ErrorCategory::NotFound,
                "colonna projection SQL Server non trovata",
            )
        },
    )
}

fn convert_filter(expression: &FilterExpression) -> Result<Expression> {
    lower_filter(
        expression,
        FilterLowering {
            provider: plenora_database_core::plan::ProviderKind::Sqlserver,
            case_insensitive_like: false,
            spatial: false,
        },
        sql_server_identifier,
    )
}

fn ensure_filter_columns(
    expression: &FilterExpression,
    columns: &[SqlServerColumnSpec],
) -> Result<()> {
    let available = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if expression.all_fields(&|field| available.contains(field)) {
        Ok(())
    } else {
        Err(prepare_error(
            ErrorCategory::NotFound,
            "colonna filtro SQL Server non trovata",
        ))
    }
}

impl SqlServerColumnSpec {
    /// Traduce una colonna di catalogo nel contratto Arrow/TDS supportato.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per tipi o dichiarazioni non rappresentabili.
    pub fn from_catalog(column: &SqlServerColumn) -> Result<Self> {
        let native = column.native_type.to_ascii_lowercase();
        let kind = column_kind(&native, column.precision, column.scale)?;
        Ok(Self {
            name: column.name.clone(),
            native_declaration: native_declaration(column),
            native_type: native,
            nullable: column.nullable,
            collation: column.collation.clone(),
            kind,
            spatial_srid: None,
            spatial_dimensions: None,
            wire_encoding: SqlServerWireEncoding::Projected,
        })
    }

    /// Compila il tipo DDL di una colonna Arrow senza accettare frammenti SQL
    /// liberi dai metadati.
    pub(crate) fn from_create_field(field: &Field) -> Result<Self> {
        let declared = field
            .metadata()
            .get(protocol::SQLSERVER_NATIVE_DECLARATION)
            .cloned()
            .unwrap_or_else(|| default_create_declaration(field));
        let (native_declaration, native_type, precision, scale) =
            validate_create_declaration(&declared)?;
        if field
            .metadata()
            .get(protocol::SQLSERVER_NATIVE_TYPE)
            .is_some_and(|expected| !expected.eq_ignore_ascii_case(&native_type))
        {
            return Err(prepare_error(
                ErrorCategory::DataMapping,
                "native_type e native_declaration SQL Server incoerenti",
            ));
        }
        let kind = column_kind(&native_type, precision, scale)?;
        Ok(Self {
            name: field.name().clone(),
            native_type,
            native_declaration,
            nullable: field.is_nullable(),
            collation: field.metadata().get(protocol::SQLSERVER_COLLATION).cloned(),
            kind,
            spatial_srid: None,
            spatial_dimensions: None,
            wire_encoding: SqlServerWireEncoding::Projected,
        })
    }

    pub(crate) fn from_query_metadata(
        name: String,
        native_declaration: String,
        nullable: bool,
        collation: Option<String>,
    ) -> Result<Self> {
        let (native_type, precision, scale) = parse_native_declaration(&native_declaration)?;
        if matches!(native_type.as_str(), "money" | "smallmoney") {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "output money QueryOperation SQL Server richiede conversione testuale esplicita",
            ));
        }
        let kind = column_kind(&native_type, precision, scale)?;
        if kind.spatial_semantics().is_some() {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                "output spatial QueryOperation SQL Server richiede WKB e SRID risolto",
            ));
        }
        Ok(Self {
            name,
            native_type,
            native_declaration,
            nullable,
            collation,
            kind,
            spatial_srid: None,
            spatial_dimensions: None,
            wire_encoding: SqlServerWireEncoding::Native,
        })
    }

    pub(crate) fn accepts_tds_column_type(&self, actual: tiberius::ColumnType) -> bool {
        use tiberius::ColumnType;
        if self.wire_encoding == SqlServerWireEncoding::Projected {
            match (&self.kind, self.native_type.as_str()) {
                (
                    SqlServerColumnKind::Decimal { .. }
                    | SqlServerColumnKind::Date
                    | SqlServerColumnKind::Time
                    | SqlServerColumnKind::Timestamp
                    | SqlServerColumnKind::TimestampTz,
                    _,
                )
                | (SqlServerColumnKind::Utf8, "uniqueidentifier" | "xml") => {
                    return is_tds_text(actual);
                }
                (SqlServerColumnKind::Geometry | SqlServerColumnKind::Geography, _) => {
                    return is_tds_binary(actual)
                }
                _ => {}
            }
        }
        match (&self.kind, self.native_type.as_str()) {
            (SqlServerColumnKind::Utf8, "uniqueidentifier") => actual == ColumnType::Guid,
            (SqlServerColumnKind::Utf8, "xml") => actual == ColumnType::Xml,
            (SqlServerColumnKind::Decimal { .. }, "money") => actual == ColumnType::Money,
            (SqlServerColumnKind::Decimal { .. }, "smallmoney") => actual == ColumnType::Money4,
            (SqlServerColumnKind::Decimal { .. }, _) => {
                matches!(actual, ColumnType::Decimaln | ColumnType::Numericn)
            }
            (SqlServerColumnKind::Date, _) => actual == ColumnType::Daten,
            (SqlServerColumnKind::Time, _) => actual == ColumnType::Timen,
            (SqlServerColumnKind::Timestamp, "smalldatetime") => actual == ColumnType::Datetime4,
            (SqlServerColumnKind::Timestamp, "datetime") => actual == ColumnType::Datetime,
            (SqlServerColumnKind::Timestamp, "datetime2") => actual == ColumnType::Datetime2,
            (
                SqlServerColumnKind::Timestamp
                | SqlServerColumnKind::Geometry
                | SqlServerColumnKind::Geography,
                _,
            ) => false,
            (SqlServerColumnKind::TimestampTz, _) => actual == ColumnType::DatetimeOffsetn,
            (SqlServerColumnKind::Bool, _) => {
                matches!(actual, ColumnType::Bit | ColumnType::Bitn)
            }
            (SqlServerColumnKind::U8, _) => actual == ColumnType::Int1,
            (SqlServerColumnKind::I16, _) => actual == ColumnType::Int2,
            (SqlServerColumnKind::I32, _) => actual == ColumnType::Int4,
            (SqlServerColumnKind::I64, _) => actual == ColumnType::Int8,
            (SqlServerColumnKind::F32, _) => actual == ColumnType::Float4,
            (SqlServerColumnKind::F64, _) => actual == ColumnType::Float8,
            (SqlServerColumnKind::Utf8, _) => is_tds_text(actual),
            (SqlServerColumnKind::Binary, _) => is_tds_binary(actual),
        }
    }

    #[must_use]
    pub(crate) fn native_scale(&self) -> Option<u8> {
        parse_declaration_arguments(&self.native_declaration)
            .and_then(|arguments| arguments.split(',').next_back())
            .and_then(|value| value.trim().parse().ok())
    }

    fn projection(&self, renderer: &Renderer) -> Result<String> {
        let identifier = sql_server_identifier(&self.name)?;
        let quoted = renderer.quote_identifier(&identifier)?;
        let expression = match (&self.kind, self.native_type.as_str()) {
            (SqlServerColumnKind::Decimal { .. }, _) => {
                format!("CONVERT(varchar(50), {quoted})")
            }
            (SqlServerColumnKind::Utf8, "uniqueidentifier") => {
                format!("CONVERT(nvarchar(36), {quoted})")
            }
            (SqlServerColumnKind::Utf8, "xml") => {
                format!("CONVERT(nvarchar(max), {quoted})")
            }
            (SqlServerColumnKind::Date, _) => format!("CONVERT(char(10), {quoted}, 23)"),
            (SqlServerColumnKind::Time, _) => format!("CONVERT(nvarchar(32), {quoted})"),
            (SqlServerColumnKind::Timestamp, _) => {
                format!("CONVERT(nvarchar(33), {quoted}, 126)")
            }
            (SqlServerColumnKind::TimestampTz, _) => {
                format!("CONVERT(nvarchar(40), {quoted}, 127)")
            }
            (SqlServerColumnKind::Geometry | SqlServerColumnKind::Geography, _) => {
                format!("{quoted}.AsBinaryZM()")
            }
            _ => quoted.clone(),
        };
        Ok(format!("{expression} AS {quoted}"))
    }

    fn arrow_field(&self) -> Field {
        let data_type = match self.kind {
            SqlServerColumnKind::Bool => DataType::Boolean,
            SqlServerColumnKind::U8 => DataType::UInt8,
            SqlServerColumnKind::I16 => DataType::Int16,
            SqlServerColumnKind::I32 => DataType::Int32,
            SqlServerColumnKind::I64 => DataType::Int64,
            SqlServerColumnKind::F32 => DataType::Float32,
            SqlServerColumnKind::F64 => DataType::Float64,
            // `datetimeoffset` conserva un offset per valore, che Arrow
            // Timestamp non può rappresentare senza normalizzarlo.
            SqlServerColumnKind::Utf8 | SqlServerColumnKind::TimestampTz => DataType::Utf8,
            SqlServerColumnKind::Binary
            | SqlServerColumnKind::Geometry
            | SqlServerColumnKind::Geography => DataType::Binary,
            SqlServerColumnKind::Date => DataType::Date32,
            SqlServerColumnKind::Time => DataType::Time64(TimeUnit::Microsecond),
            SqlServerColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            SqlServerColumnKind::Decimal { precision, scale } => {
                DataType::Decimal128(precision, scale)
            }
        };
        let mut metadata = HashMap::new();
        metadata.insert(
            protocol::SQLSERVER_NATIVE_TYPE.to_owned(),
            self.native_type.clone(),
        );
        metadata.insert(
            protocol::SQLSERVER_NATIVE_DECLARATION.to_owned(),
            self.native_declaration.clone(),
        );
        if let Some(collation) = &self.collation {
            metadata.insert(protocol::SQLSERVER_COLLATION.to_owned(), collation.clone());
        }
        if let Some(semantics) = self.kind.spatial_semantics() {
            metadata.insert(
                protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                "geoarrow.wkb".to_owned(),
            );
            metadata.insert(protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned());
            let dimensions = match self.spatial_dimensions.unwrap_or(Dimensions::Unknown) {
                Dimensions::Xy => "xy",
                Dimensions::Xyz => "xyz",
                Dimensions::Xym => "xym",
                Dimensions::Xyzm => "xyzm",
                Dimensions::Unknown => "unknown",
            };
            metadata.insert(
                protocol::GEOMETRY_DIMENSIONS.to_owned(),
                dimensions.to_owned(),
            );
            metadata.insert(
                protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                "mixed".to_owned(),
            );
            metadata.insert(protocol::GEOMETRY_PRECISION.to_owned(), "native".to_owned());
            metadata.insert(
                protocol::GEOMETRY_SPATIAL_SEMANTICS.to_owned(),
                match semantics {
                    SpatialSemantics::Geometry => "geometry",
                    SpatialSemantics::Geography => "geography",
                }
                .to_owned(),
            );
            if let Some(srid) = self.spatial_srid {
                metadata.insert(protocol::GEOMETRY_SRID.to_owned(), srid.to_string());
            }
            metadata.insert(
                protocol::GEOMETRY_CRS_RESOLUTION.to_owned(),
                "declared_unresolved".to_owned(),
            );
        }
        Field::new(&self.name, data_type, self.nullable).with_metadata(metadata)
    }
}

fn default_create_declaration(field: &Field) -> String {
    if field.data_type() == &DataType::Binary {
        match field
            .metadata()
            .get(protocol::GEOMETRY_SPATIAL_SEMANTICS)
            .map(String::as_str)
        {
            Some("geometry") => return "geometry".to_owned(),
            Some("geography") => return "geography".to_owned(),
            _ => {}
        }
    }
    match field.data_type() {
        DataType::Boolean => "bit".to_owned(),
        DataType::UInt8 => "tinyint".to_owned(),
        DataType::Int16 => "smallint".to_owned(),
        DataType::Int32 => "int".to_owned(),
        DataType::Int64 => "bigint".to_owned(),
        DataType::Float32 => "real".to_owned(),
        DataType::Float64 => "float".to_owned(),
        DataType::Utf8 => "nvarchar(max)".to_owned(),
        DataType::Binary => "varbinary(max)".to_owned(),
        DataType::Date32 => "date".to_owned(),
        DataType::Time64(TimeUnit::Microsecond) => "time(6)".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "datetime2(6)".to_owned(),
        DataType::Decimal128(precision, scale) => format!("decimal({precision},{scale})"),
        _ => String::new(),
    }
}

fn validate_create_declaration(declaration: &str) -> Result<(String, String, u8, u8)> {
    let normalized = declaration.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || !normalized.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'(' | b')' | b',')
        })
    {
        return Err(prepare_error(
            ErrorCategory::DataMapping,
            "dichiarazione DDL SQL Server fuori dalla grammatica ammessa",
        ));
    }
    let (native, arguments) = normalized.split_once('(').map_or_else(
        || (normalized.as_str(), None),
        |(native, suffix)| (native, suffix.strip_suffix(')')),
    );
    if normalized.contains('(') && arguments.is_none() {
        return Err(prepare_error(
            ErrorCategory::DataMapping,
            "dichiarazione DDL SQL Server con parentesi non bilanciate",
        ));
    }
    let no_arguments = [
        "bit",
        "tinyint",
        "smallint",
        "int",
        "bigint",
        "real",
        "float",
        "money",
        "smallmoney",
        "date",
        "datetime",
        "smalldatetime",
        "uniqueidentifier",
        "xml",
        "text",
        "ntext",
        "image",
        "geometry",
        "geography",
    ];
    let (precision, scale) = match native {
        "decimal" | "numeric" => {
            let (precision, scale) = parse_two_u8(arguments, "decimal")?;
            if precision == 0 || precision > 38 || scale > precision {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "precisione/scala DDL SQL Server non valida",
                ));
            }
            (precision, scale)
        }
        "char" | "binary" => {
            validate_length(arguments, false, 8_000)?;
            (0, 0)
        }
        "nchar" => {
            validate_length(arguments, false, 4_000)?;
            (0, 0)
        }
        "varchar" | "varbinary" => {
            validate_length(arguments, true, 8_000)?;
            (0, 0)
        }
        "nvarchar" => {
            validate_length(arguments, true, 4_000)?;
            (0, 0)
        }
        "time" | "datetime2" | "datetimeoffset" => {
            let value = parse_one_u8(arguments, native)?;
            if value > 7 {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "scala temporale DDL SQL Server oltre 7",
                ));
            }
            (0, value)
        }
        _ if no_arguments.contains(&native) && arguments.is_none() => match native {
            "money" => (19, 4),
            "smallmoney" => (10, 4),
            _ => (0, 0),
        },
        _ => {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                format!("tipo DDL SQL Server non ammesso: {normalized}"),
            ));
        }
    };
    let native = native.to_owned();
    Ok((normalized, native, precision, scale))
}

fn parse_one_u8(arguments: Option<&str>, name: &str) -> Result<u8> {
    arguments
        .and_then(|value| value.parse::<u8>().ok())
        .ok_or_else(|| {
            prepare_error(
                ErrorCategory::DataMapping,
                format!("argomento DDL SQL Server non valido per {name}"),
            )
        })
}

fn parse_two_u8(arguments: Option<&str>, name: &str) -> Result<(u8, u8)> {
    let mut values = arguments.unwrap_or_default().split(',');
    let first = values.next().and_then(|value| value.parse::<u8>().ok());
    let second = values.next().and_then(|value| value.parse::<u8>().ok());
    if values.next().is_some() || first.is_none() || second.is_none() {
        return Err(prepare_error(
            ErrorCategory::DataMapping,
            format!("argomenti DDL SQL Server non validi per {name}"),
        ));
    }
    Ok((first.unwrap_or_default(), second.unwrap_or_default()))
}

fn validate_length(arguments: Option<&str>, allow_max: bool, maximum: u16) -> Result<()> {
    let value = arguments.unwrap_or_default();
    if (allow_max && value == "max")
        || value
            .parse::<u16>()
            .is_ok_and(|length| (1..=maximum).contains(&length))
    {
        Ok(())
    } else {
        Err(prepare_error(
            ErrorCategory::DataMapping,
            "lunghezza DDL SQL Server non valida",
        ))
    }
}

fn column_kind(native: &str, precision: u8, scale: u8) -> Result<SqlServerColumnKind> {
    let kind = match native {
        "bit" => SqlServerColumnKind::Bool,
        "tinyint" => SqlServerColumnKind::U8,
        "smallint" => SqlServerColumnKind::I16,
        "int" => SqlServerColumnKind::I32,
        "bigint" => SqlServerColumnKind::I64,
        "real" => SqlServerColumnKind::F32,
        "float" => SqlServerColumnKind::F64,
        "decimal" | "numeric" => {
            if precision == 0 || precision > 38 || scale > precision {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "precisione/scala decimal SQL Server non rappresentabile",
                ));
            }
            SqlServerColumnKind::Decimal {
                precision,
                scale: i8::try_from(scale).map_err(|_| {
                    prepare_error(
                        ErrorCategory::DataMapping,
                        "scala decimal SQL Server non rappresentabile",
                    )
                })?,
            }
        }
        "money" => SqlServerColumnKind::Decimal {
            precision: 19,
            scale: 4,
        },
        "smallmoney" => SqlServerColumnKind::Decimal {
            precision: 10,
            scale: 4,
        },
        "char" | "varchar" | "text" | "nchar" | "nvarchar" | "ntext" | "uniqueidentifier"
        | "xml" => SqlServerColumnKind::Utf8,
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => {
            SqlServerColumnKind::Binary
        }
        "date" => SqlServerColumnKind::Date,
        "time" => SqlServerColumnKind::Time,
        "smalldatetime" | "datetime" | "datetime2" => SqlServerColumnKind::Timestamp,
        "datetimeoffset" => SqlServerColumnKind::TimestampTz,
        "geometry" => SqlServerColumnKind::Geometry,
        "geography" => SqlServerColumnKind::Geography,
        _ => {
            return Err(prepare_error(
                ErrorCategory::Unsupported,
                format!("tipo SQL Server non supportato nel profilo read: {native}"),
            ));
        }
    };
    Ok(kind)
}

fn parse_native_declaration(declaration: &str) -> Result<(String, u8, u8)> {
    let normalized = declaration.trim().to_ascii_lowercase();
    let native = normalized
        .split_once('(')
        .map_or(normalized.as_str(), |(name, _)| name)
        .trim();
    if native.is_empty()
        || !native
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(prepare_error(
            ErrorCategory::DataMapping,
            "dichiarazione tipo output SQL Server non canonica",
        ));
    }
    let (precision, scale) = match native {
        "decimal" | "numeric" => {
            let arguments = parse_declaration_arguments(&normalized).ok_or_else(|| {
                prepare_error(
                    ErrorCategory::DataMapping,
                    "decimal output SQL Server senza precisione/scala",
                )
            })?;
            let mut values = arguments.split(',').map(str::trim);
            let precision = values
                .next()
                .and_then(|value| value.parse::<u8>().ok())
                .ok_or_else(|| {
                    prepare_error(
                        ErrorCategory::DataMapping,
                        "precisione decimal output SQL Server non valida",
                    )
                })?;
            let scale = values
                .next()
                .and_then(|value| value.parse::<u8>().ok())
                .ok_or_else(|| {
                    prepare_error(
                        ErrorCategory::DataMapping,
                        "scala decimal output SQL Server non valida",
                    )
                })?;
            if values.next().is_some() {
                return Err(prepare_error(
                    ErrorCategory::DataMapping,
                    "dichiarazione decimal output SQL Server non valida",
                ));
            }
            (precision, scale)
        }
        "money" => (19, 4),
        "smallmoney" => (10, 4),
        _ => (0, 0),
    };
    Ok((native.to_owned(), precision, scale))
}

fn parse_declaration_arguments(declaration: &str) -> Option<&str> {
    let (_, suffix) = declaration.split_once('(')?;
    suffix.strip_suffix(')')
}

const fn is_tds_text(actual: tiberius::ColumnType) -> bool {
    matches!(
        actual,
        tiberius::ColumnType::BigVarChar
            | tiberius::ColumnType::BigChar
            | tiberius::ColumnType::NVarchar
            | tiberius::ColumnType::NChar
            | tiberius::ColumnType::Text
            | tiberius::ColumnType::NText
    )
}

const fn is_tds_binary(actual: tiberius::ColumnType) -> bool {
    matches!(
        actual,
        tiberius::ColumnType::BigVarBin
            | tiberius::ColumnType::BigBinary
            | tiberius::ColumnType::Image
            | tiberius::ColumnType::Udt
    )
}

fn sql_server_identifier(value: &str) -> Result<Identifier> {
    if value.chars().count() > crate::MAX_IDENTIFIER_CHARACTERS {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "identificatore oltre 128 caratteri SQL Server",
        ));
    }
    Identifier::new(value.to_owned())
}

fn native_declaration(column: &SqlServerColumn) -> String {
    match column.native_type.as_str() {
        "decimal" | "numeric" => {
            format!(
                "{}({},{})",
                column.native_type, column.precision, column.scale
            )
        }
        "char" | "varchar" | "binary" | "varbinary" => {
            length_declaration(&column.native_type, column.max_length)
        }
        "nchar" | "nvarchar" => {
            let length = if column.max_length < 0 {
                -1
            } else {
                column.max_length / 2
            };
            length_declaration(&column.native_type, length)
        }
        "time" | "datetime2" | "datetimeoffset" => {
            format!("{}({})", column.native_type, column.scale)
        }
        _ => column.native_type.clone(),
    }
}

fn length_declaration(native: &str, length: i16) -> String {
    if length < 0 {
        format!("{native}(max)")
    } else {
        format!("{native}({length})")
    }
}

fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError::new(
        category,
        ErrorPhase::Prepare,
        Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        message,
    )
}

#[cfg(test)]
#[path = "types_tests.rs"]
mod tests;
