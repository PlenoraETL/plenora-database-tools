use crate::catalog::{SqlServerColumn, SqlServerObjectDescription};
use plenora_database_core::arrow::schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::plan::{
    ComparisonOperator, FilterExpression, ReadOperation, SortDirection,
};
use plenora_database_core::protocol;
use plenora_database_core::{
    DatabaseError, ErrorCategory, ErrorPhase, RemoteEffect, Result, RetryDisposition,
};
use plenora_database_sql::{
    Dialect, DialectCapabilities, Expression, Identifier, ObjectName, Renderer,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

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
            schema: Arc::new(Schema::new(fields)),
            sql: format!(
                "SELECT {projection} FROM {} ORDER BY (SELECT NULL);",
                renderer.quote_object(&object)
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
        if let Some(limit) = operation.row_limit {
            sql.push_str("TOP (");
            sql.push_str(&limit.to_string());
            sql.push_str(") ");
        }
        sql.push_str(&projection);
        sql.push_str(" FROM ");
        sql.push_str(&renderer.quote_object(&object));

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
                    let field = renderer.quote_identifier(&sql_server_identifier(&order.field)?);
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
        sql.push(';');
        let fields = columns
            .iter()
            .map(SqlServerColumnSpec::arrow_field)
            .collect::<Vec<_>>();
        Ok(Self {
            columns,
            schema: Arc::new(Schema::new(fields)),
            sql,
            bind_names,
            structural_fingerprint: description.token.structural_fingerprint.clone(),
        })
    }

    pub(crate) fn apply_spatial_srid(&mut self, index: usize, srid: Option<u32>) -> Result<()> {
        let column = self
            .columns
            .get_mut(index)
            .ok_or_else(|| prepare_error(ErrorCategory::Internal, "indice spatial non valido"))?;
        column.spatial_srid = srid;
        let fields = self
            .columns
            .iter()
            .map(SqlServerColumnSpec::arrow_field)
            .collect::<Vec<_>>();
        self.schema = Arc::new(Schema::new(fields));
        Ok(())
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
    if projection.is_empty() {
        return Ok(available.to_vec());
    }
    let by_name = available
        .iter()
        .map(|column| (column.name.as_str(), column))
        .collect::<HashMap<_, _>>();
    projection
        .iter()
        .map(|name| {
            by_name.get(name.as_str()).map_or_else(
                || {
                    Err(prepare_error(
                        ErrorCategory::NotFound,
                        "colonna projection SQL Server non trovata",
                    ))
                },
                |column| Ok((*column).clone()),
            )
        })
        .collect()
}

fn convert_filter(expression: &FilterExpression) -> Result<Expression> {
    match expression {
        FilterExpression::And { args } => Ok(Expression::And(
            args.iter()
                .map(convert_filter)
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Or { args } => Ok(Expression::Or(
            args.iter()
                .map(convert_filter)
                .collect::<Result<Vec<_>>>()?,
        )),
        FilterExpression::Eq { field, parameter } => {
            comparison(field, ComparisonOperator::Eq, parameter)
        }
        FilterExpression::Ne { field, parameter } => {
            comparison(field, ComparisonOperator::Ne, parameter)
        }
        FilterExpression::Lt { field, parameter } => {
            comparison(field, ComparisonOperator::Lt, parameter)
        }
        FilterExpression::Lte { field, parameter } => {
            comparison(field, ComparisonOperator::Lte, parameter)
        }
        FilterExpression::Gt { field, parameter } => {
            comparison(field, ComparisonOperator::Gt, parameter)
        }
        FilterExpression::Gte { field, parameter } => {
            comparison(field, ComparisonOperator::Gte, parameter)
        }
        FilterExpression::IsNull { field } => Ok(Expression::IsNull(sql_server_identifier(field)?)),
        FilterExpression::IsNotNull { field } => {
            Ok(Expression::IsNotNull(sql_server_identifier(field)?))
        }
        FilterExpression::In { field, parameters } => Ok(Expression::In {
            field: sql_server_identifier(field)?,
            parameters: parameters.clone(),
        }),
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => Ok(Expression::Between {
            field: sql_server_identifier(field)?,
            lower_parameter: lower_parameter.clone(),
            upper_parameter: upper_parameter.clone(),
        }),
        FilterExpression::Like {
            field,
            parameter,
            case_insensitive,
        } => {
            if *case_insensitive {
                return Err(prepare_error(
                    ErrorCategory::Unsupported,
                    "LIKE case-insensitive SQL Server richiede collation esplicita",
                ));
            }
            Ok(Expression::Like {
                field: sql_server_identifier(field)?,
                parameter: parameter.clone(),
                case_insensitive: false,
            })
        }
        FilterExpression::Spatial { .. } => Err(prepare_error(
            ErrorCategory::Unsupported,
            "filtro spatial SQL Server richiede tipo e SRID risolti",
        )),
    }
}

fn comparison(field: &str, operator: ComparisonOperator, parameter: &str) -> Result<Expression> {
    Ok(Expression::Compare {
        field: sql_server_identifier(field)?,
        operator,
        parameter: parameter.to_owned(),
    })
}

fn ensure_filter_columns(
    expression: &FilterExpression,
    columns: &[SqlServerColumnSpec],
) -> Result<()> {
    fn visit(expression: &FilterExpression, available: &BTreeSet<&str>) -> bool {
        match expression {
            FilterExpression::And { args } | FilterExpression::Or { args } => {
                args.iter().all(|argument| visit(argument, available))
            }
            FilterExpression::Eq { field, .. }
            | FilterExpression::Ne { field, .. }
            | FilterExpression::Lt { field, .. }
            | FilterExpression::Lte { field, .. }
            | FilterExpression::Gt { field, .. }
            | FilterExpression::Gte { field, .. }
            | FilterExpression::IsNull { field }
            | FilterExpression::IsNotNull { field }
            | FilterExpression::In { field, .. }
            | FilterExpression::Between { field, .. }
            | FilterExpression::Like { field, .. }
            | FilterExpression::Spatial { field, .. } => available.contains(field.as_str()),
        }
    }
    let available = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    if visit(expression, &available) {
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
        let kind = match native.as_str() {
            "bit" => SqlServerColumnKind::Bool,
            "tinyint" => SqlServerColumnKind::U8,
            "smallint" => SqlServerColumnKind::I16,
            "int" => SqlServerColumnKind::I32,
            "bigint" => SqlServerColumnKind::I64,
            "real" => SqlServerColumnKind::F32,
            "float" => SqlServerColumnKind::F64,
            "decimal" | "numeric" => {
                if column.precision == 0 || column.precision > 38 || column.scale > column.precision
                {
                    return Err(prepare_error(
                        ErrorCategory::DataMapping,
                        "precisione/scala decimal SQL Server non rappresentabile",
                    ));
                }
                SqlServerColumnKind::Decimal {
                    precision: column.precision,
                    scale: i8::try_from(column.scale).map_err(|_| {
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
        Ok(Self {
            name: column.name.clone(),
            native_declaration: native_declaration(column),
            native_type: native,
            nullable: column.nullable,
            collation: column.collation.clone(),
            kind,
            spatial_srid: None,
        })
    }

    fn projection(&self, renderer: &Renderer) -> Result<String> {
        let identifier = sql_server_identifier(&self.name)?;
        let quoted = renderer.quote_identifier(&identifier);
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
                format!("{quoted}.STAsBinary()")
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
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Sqlserver),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SqlServerSchemaToken;

    fn description(native_type: &str, precision: u8, scale: u8) -> SqlServerObjectDescription {
        SqlServerObjectDescription {
            database_id: 1,
            object_id: 2,
            catalog: "db".to_owned(),
            schema: "dbo".to_owned(),
            name: "fixture".to_owned(),
            kind: "USER_TABLE".to_owned(),
            temporal_type: 0,
            memory_optimized: false,
            durability: None,
            columns: vec![SqlServerColumn {
                ordinal: 1,
                name: "value".to_owned(),
                type_schema: "sys".to_owned(),
                native_type: native_type.to_owned(),
                max_length: 8,
                precision,
                scale,
                nullable: true,
                identity: false,
                computed: false,
                generated_always_type: 0,
                collation: None,
                default_definition: None,
                computed_definition: None,
                computed_persisted: false,
            }],
            constraints: Vec::new(),
            indexes: Vec::new(),
            token: SqlServerSchemaToken {
                schema_version: 1,
                database_id: 1,
                object_id: 2,
                structural_fingerprint: "abc".to_owned(),
            },
        }
    }

    #[test]
    fn decimal_projection_and_type_are_exact() {
        let plan = SqlServerReadPlan::compile(&description("decimal", 38, 12)).expect("plan");
        assert!(plan.sql.contains("CONVERT(varchar(50), [value])"));
        assert_eq!(
            plan.schema.field(0).data_type(),
            &DataType::Decimal128(38, 12)
        );
    }

    #[test]
    fn unsupported_type_fails_before_io() {
        let error =
            SqlServerReadPlan::compile(&description("sql_variant", 0, 0)).expect_err("unsupported");
        assert_eq!(error.category, ErrorCategory::Unsupported);
        assert_eq!(error.phase, ErrorPhase::Prepare);
    }
}
