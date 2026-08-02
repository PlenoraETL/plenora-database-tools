use crate::{MysqlColumn, MysqlObjectDescription};
use plenora_database_core::arrow::schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
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
pub enum MysqlColumnKind {
    Bool,
    I8,
    U8,
    I16,
    U16,
    I32,
    U32,
    I64,
    U64,
    F32,
    F64,
    Utf8,
    Binary,
    Date,
    Time,
    Timestamp,
    Decimal { precision: u8, scale: i8 },
    Geometry,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MysqlColumnSpec {
    pub name: String,
    pub native_type: String,
    pub native_declaration: String,
    pub nullable: bool,
    pub collation: Option<String>,
    pub kind: MysqlColumnKind,
    pub spatial_srid: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct MysqlReadPlan {
    pub columns: Vec<MysqlColumnSpec>,
    pub schema: SchemaRef,
    pub sql: String,
    pub bind_names: Vec<String>,
    pub schema_token: String,
}

impl MysqlReadPlan {
    /// Compila un piano di lettura senza interpolare valori nel testo SQL.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per tipi, colonne, identificatori o filtri non
    /// rappresentabili. Un limite senza ordinamento esplicito e rifiutato.
    pub fn compile(
        description: &MysqlObjectDescription,
        operation: &ReadOperation,
    ) -> Result<Self> {
        if description.columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "oggetto MySQL privo di colonne leggibili",
            ));
        }
        if operation.row_limit.is_some() && operation.order_by.is_empty() {
            return Err(prepare_error(
                ErrorCategory::InvalidPlan,
                "LIMIT MySQL richiede ORDER BY esplicito per un risultato deterministico",
            ));
        }
        let renderer = mysql_renderer();
        let available = description
            .columns
            .iter()
            .map(MysqlColumnSpec::from_catalog)
            .collect::<Result<Vec<_>>>()?;
        let columns = select_columns(&available, &operation.projection)?;
        let projection = columns
            .iter()
            .map(|column| column.projection(&renderer))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let object = ObjectName {
            catalog: None,
            schema: Some(mysql_identifier(&description.schema)?),
            object: mysql_identifier(&description.name)?,
        };
        let mut sql = format!(
            "SELECT {projection} FROM {}",
            renderer.quote_object(&object)
        );
        let mut bind_names = Vec::new();
        if let Some(filter) = &operation.filter {
            ensure_filter_columns(filter, &available)?;
            let rendered_filter = renderer.render_filter(&convert_filter(filter)?)?;
            sql.push_str(" WHERE ");
            sql.push_str(&rendered_filter.sql);
            bind_names.extend(rendered_filter.binds.into_iter().map(|bind| bind.name));
        }
        if !operation.order_by.is_empty() {
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
                            "colonna ORDER BY MySQL non trovata",
                        ));
                    }
                    let field = renderer.quote_identifier(&mysql_identifier(&order.field)?);
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
        if let Some(limit) = operation.row_limit {
            sql.push_str(" LIMIT ");
            sql.push_str(&limit.to_string());
        }
        sql.push(';');
        let schema = contract_schema(columns.iter().map(MysqlColumnSpec::arrow_field).collect());
        Ok(Self {
            columns,
            schema,
            sql,
            bind_names,
            schema_token: description.token.0.clone(),
        })
    }

    /// Costruisce il piano di una `QueryOperation` dai metadati del prepared
    /// statement, che sono l'unica descrizione autoritativa dell'output.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso quando il result set non espone colonne.
    pub(crate) fn from_query_columns(
        sql: String,
        bind_names: Vec<String>,
        columns: Vec<MysqlColumnSpec>,
    ) -> Result<Self> {
        if columns.is_empty() {
            return Err(prepare_error(
                ErrorCategory::Schema,
                "QueryOperation MySQL priva di colonne risultanti",
            ));
        }
        let schema = contract_schema(columns.iter().map(MysqlColumnSpec::arrow_field).collect());
        Ok(Self {
            columns,
            schema,
            sql,
            bind_names,
            schema_token: QUERY_RESULT_SCHEMA_TOKEN.to_owned(),
        })
    }
}

/// Il token strutturale di una query non e un token di catalogo: l'unica
/// verifica successiva possibile e il ricontrollo dei metadati di riga.
const QUERY_RESULT_SCHEMA_TOKEN: &str = "mysql-query-result-metadata-v1";

impl MysqlColumnSpec {
    /// Traduce una colonna del catalogo nel sottoinsieme Arrow supportato.
    ///
    /// # Errors
    ///
    /// Fallisce chiuso per tipi sconosciuti, decimal oltre 128 bit e profili
    /// spatial senza SRID dichiarato.
    pub fn from_catalog(column: &MysqlColumn) -> Result<Self> {
        let native_type = column.data_type.to_ascii_lowercase();
        let native_declaration = column.native_declaration.to_ascii_lowercase();
        let unsigned = native_declaration
            .split_ascii_whitespace()
            .any(|part| part == "unsigned");
        let kind = match native_type.as_str() {
            "tinyint" if native_declaration.starts_with("tinyint(1)") && !unsigned => {
                MysqlColumnKind::Bool
            }
            "tinyint" if unsigned => MysqlColumnKind::U8,
            "tinyint" => MysqlColumnKind::I8,
            "smallint" if unsigned => MysqlColumnKind::U16,
            "smallint" | "year" => MysqlColumnKind::I16,
            "mediumint" | "int" | "integer" if unsigned => MysqlColumnKind::U32,
            "mediumint" | "int" | "integer" => MysqlColumnKind::I32,
            "bigint" if unsigned => MysqlColumnKind::U64,
            "bigint" => MysqlColumnKind::I64,
            "float" => MysqlColumnKind::F32,
            "double" | "real" => MysqlColumnKind::F64,
            "decimal" | "numeric" => decimal_kind(column)?,
            "char" | "varchar" | "tinytext" | "text" | "mediumtext" | "longtext" | "enum"
            | "set" | "json" => MysqlColumnKind::Utf8,
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" | "bit" => {
                MysqlColumnKind::Binary
            }
            "date" => MysqlColumnKind::Date,
            "time" => MysqlColumnKind::Time,
            "datetime" | "timestamp" => MysqlColumnKind::Timestamp,
            "geometry" | "point" | "linestring" | "polygon" | "multipoint" | "multilinestring"
            | "multipolygon" | "geometrycollection" | "geomcollection" => {
                if column.spatial_srid.is_none() {
                    return Err(prepare_error(
                        ErrorCategory::Crs,
                        "colonna spatial MySQL senza SRID dichiarato",
                    ));
                }
                MysqlColumnKind::Geometry
            }
            _ => {
                return Err(prepare_error(
                    ErrorCategory::Unsupported,
                    format!("tipo MySQL non supportato: {native_type}"),
                ));
            }
        };
        Ok(Self {
            name: column.name.clone(),
            native_type,
            native_declaration,
            nullable: column.nullable,
            collation: column.collation.clone(),
            kind,
            spatial_srid: column.spatial_srid,
        })
    }

    fn projection(&self, renderer: &Renderer) -> Result<String> {
        let identifier = mysql_identifier(&self.name)?;
        let quoted = renderer.quote_identifier(&identifier);
        if self.kind == MysqlColumnKind::Geometry {
            Ok(format!("ST_AsBinary({quoted}) AS {quoted}"))
        } else {
            Ok(quoted)
        }
    }

    #[must_use]
    pub fn arrow_field(&self) -> Field {
        let data_type = match self.kind {
            MysqlColumnKind::Bool => DataType::Boolean,
            MysqlColumnKind::I8 => DataType::Int8,
            MysqlColumnKind::U8 => DataType::UInt8,
            MysqlColumnKind::I16 => DataType::Int16,
            MysqlColumnKind::U16 => DataType::UInt16,
            MysqlColumnKind::I32 => DataType::Int32,
            MysqlColumnKind::U32 => DataType::UInt32,
            MysqlColumnKind::I64 => DataType::Int64,
            MysqlColumnKind::U64 => DataType::UInt64,
            MysqlColumnKind::F32 => DataType::Float32,
            MysqlColumnKind::F64 => DataType::Float64,
            // MySQL TIME rappresenta anche durate negative fino a 838 ore e
            // non e semanticamente equivalente ad Arrow Time64.
            MysqlColumnKind::Utf8 | MysqlColumnKind::Time => DataType::Utf8,
            MysqlColumnKind::Binary | MysqlColumnKind::Geometry => DataType::Binary,
            MysqlColumnKind::Date => DataType::Date32,
            MysqlColumnKind::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
            MysqlColumnKind::Decimal { precision, scale } => DataType::Decimal128(precision, scale),
        };
        let mut metadata = HashMap::from([(
            protocol::MYSQL_NATIVE_TYPE.to_owned(),
            self.native_type.clone(),
        )]);
        if !self.native_declaration.is_empty() {
            metadata.insert(
                protocol::MYSQL_NATIVE_DECLARATION.to_owned(),
                self.native_declaration.clone(),
            );
        }
        if let Some(collation) = &self.collation {
            metadata.insert(protocol::MYSQL_COLLATION.to_owned(), collation.clone());
        }
        if self.kind == MysqlColumnKind::Geometry {
            metadata.extend([
                (
                    protocol::GEOARROW_EXTENSION_NAME.to_owned(),
                    "geoarrow.wkb".to_owned(),
                ),
                (protocol::GEOMETRY_ENCODING.to_owned(), "wkb".to_owned()),
                (protocol::GEOMETRY_DIMENSIONS.to_owned(), "xy".to_owned()),
                (
                    protocol::GEOMETRY_TYPES_DECLARATION.to_owned(),
                    if self.native_type == "geometry" {
                        "mixed".to_owned()
                    } else {
                        "exact".to_owned()
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
            ]);
            if let Some(srid) = self.spatial_srid {
                metadata.insert(protocol::GEOMETRY_SRID.to_owned(), srid.to_string());
            }
            if self.native_type != "geometry" {
                metadata.insert(
                    protocol::GEOMETRY_TYPES.to_owned(),
                    canonical_geometry_type(&self.native_type).to_owned(),
                );
            }
        }
        Field::new(&self.name, data_type, self.nullable).with_metadata(metadata)
    }
}

fn canonical_geometry_type(native_type: &str) -> &str {
    if native_type == "geomcollection" {
        "geometrycollection"
    } else {
        native_type
    }
}

fn decimal_kind(column: &MysqlColumn) -> Result<MysqlColumnKind> {
    let precision = column.numeric_precision.ok_or_else(|| {
        prepare_error(ErrorCategory::DataMapping, "decimal MySQL senza precisione")
    })?;
    let scale = column
        .numeric_scale
        .ok_or_else(|| prepare_error(ErrorCategory::DataMapping, "decimal MySQL senza scala"))?;
    let precision = u8::try_from(precision).map_err(|_| {
        prepare_error(
            ErrorCategory::Unsupported,
            "precisione decimal MySQL non rappresentabile",
        )
    })?;
    let scale = i8::try_from(scale).map_err(|_| {
        prepare_error(
            ErrorCategory::Unsupported,
            "scala decimal MySQL non rappresentabile",
        )
    })?;
    if precision == 0 || precision > 38 || scale < 0 || scale > precision.cast_signed() {
        return Err(prepare_error(
            ErrorCategory::Unsupported,
            "decimal MySQL oltre Decimal128 Arrow",
        ));
    }
    Ok(MysqlColumnKind::Decimal { precision, scale })
}

fn select_columns(
    available: &[MysqlColumnSpec],
    projection: &[String],
) -> Result<Vec<MysqlColumnSpec>> {
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
                        "colonna projection MySQL non trovata",
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
        FilterExpression::IsNull { field } => Ok(Expression::IsNull(mysql_identifier(field)?)),
        FilterExpression::IsNotNull { field } => {
            Ok(Expression::IsNotNull(mysql_identifier(field)?))
        }
        FilterExpression::In { field, parameters } => Ok(Expression::In {
            field: mysql_identifier(field)?,
            parameters: parameters.clone(),
        }),
        FilterExpression::Between {
            field,
            lower_parameter,
            upper_parameter,
        } => Ok(Expression::Between {
            field: mysql_identifier(field)?,
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
                    "LIKE case-insensitive MySQL richiede collation esplicita",
                ));
            }
            Ok(Expression::Like {
                field: mysql_identifier(field)?,
                parameter: parameter.clone(),
                case_insensitive: false,
            })
        }
        FilterExpression::Spatial { .. } => Err(prepare_error(
            ErrorCategory::Unsupported,
            "filtro spatial MySQL richiede validazione WKB e SRID",
        )),
    }
}

fn comparison(field: &str, operator: ComparisonOperator, parameter: &str) -> Result<Expression> {
    Ok(Expression::Compare {
        field: mysql_identifier(field)?,
        operator,
        parameter: parameter.to_owned(),
    })
}

fn ensure_filter_columns(expression: &FilterExpression, columns: &[MysqlColumnSpec]) -> Result<()> {
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
            "colonna filtro MySQL non trovata",
        ))
    }
}

fn contract_schema(fields: Vec<Field>) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        fields,
        HashMap::from([(
            protocol::CONTRACT_VERSION_KEY.to_owned(),
            protocol::CONTRACT_VERSION.to_owned(),
        )]),
    ))
}

fn mysql_identifier(value: &str) -> Result<Identifier> {
    if value.chars().count() > crate::MAX_IDENTIFIER_CHARACTERS {
        return Err(prepare_error(
            ErrorCategory::InvalidPlan,
            "identificatore oltre 64 caratteri MySQL",
        ));
    }
    Identifier::new(value.to_owned())
}

pub const fn mysql_renderer() -> Renderer {
    Renderer::new(
        Dialect::Mysql,
        DialectCapabilities {
            spatial_intersects: false,
        },
    )
}

fn prepare_error(category: ErrorCategory, message: impl Into<String>) -> DatabaseError {
    DatabaseError {
        category,
        phase: ErrorPhase::Prepare,
        remote_effect: RemoteEffect::None,
        retry: RetryDisposition::Never,
        provider: Some(plenora_database_core::plan::ProviderKind::Mysql),
        execution_id: None,
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::field_contract::validate_schema_contract;
    use plenora_database_core::plan::{ObjectRef, OrderBy};

    fn column(name: &str, data_type: &str, declaration: &str) -> MysqlColumn {
        MysqlColumn {
            name: name.to_owned(),
            ordinal: 1,
            data_type: data_type.to_owned(),
            native_declaration: declaration.to_owned(),
            nullable: false,
            default_expression: None,
            character_set: None,
            collation: None,
            numeric_precision: None,
            numeric_scale: None,
            datetime_precision: None,
            spatial_srid: None,
            extra: String::new(),
            generation_expression: String::new(),
        }
    }

    #[test]
    fn mapping_preserves_signedness_and_rejects_wide_decimal() {
        assert_eq!(
            MysqlColumnSpec::from_catalog(&column("id", "bigint", "bigint unsigned"))
                .expect("unsigned bigint")
                .kind,
            MysqlColumnKind::U64
        );
        let mut decimal = column("amount", "decimal", "decimal(39,0)");
        decimal.numeric_precision = Some(39);
        decimal.numeric_scale = Some(0);
        assert_eq!(
            MysqlColumnSpec::from_catalog(&decimal)
                .expect_err("wide decimal")
                .category,
            ErrorCategory::Unsupported
        );
    }

    #[test]
    fn spatial_projection_is_wkb_xy_with_declared_srid() {
        let mut geometry = column("geom", "geometry", "geometry");
        geometry.spatial_srid = Some(4_326);
        let spec = MysqlColumnSpec::from_catalog(&geometry).expect("geometry");
        let field = spec.arrow_field();
        assert_eq!(field.data_type(), &DataType::Binary);
        assert_eq!(
            field.metadata().get(protocol::GEOMETRY_DIMENSIONS),
            Some(&"xy".to_owned())
        );
        assert_eq!(
            spec.projection(&mysql_renderer()).expect("projection"),
            "ST_AsBinary(`geom`) AS `geom`"
        );
    }

    #[test]
    fn concrete_spatial_type_produces_an_exact_valid_contract() {
        let mut point = column("geom", "point", "point");
        point.spatial_srid = Some(4_326);
        let field = MysqlColumnSpec::from_catalog(&point)
            .expect("point MySQL")
            .arrow_field();
        assert_eq!(
            field.metadata().get(protocol::GEOMETRY_TYPES_DECLARATION),
            Some(&"exact".to_owned())
        );
        validate_schema_contract(contract_schema(vec![field]).as_ref())
            .expect("contratto geometrico MySQL canonico");
    }

    #[test]
    fn mysql_geomcollection_alias_produces_the_canonical_exact_type() {
        let mut collection = column("geom", "geomcollection", "geomcollection");
        collection.spatial_srid = Some(4_326);
        let field = MysqlColumnSpec::from_catalog(&collection)
            .expect("geomcollection MySQL")
            .arrow_field();
        assert_eq!(
            field.metadata().get(protocol::GEOMETRY_TYPES_DECLARATION),
            Some(&"exact".to_owned())
        );
        assert_eq!(
            field.metadata().get(protocol::GEOMETRY_TYPES),
            Some(&"geometrycollection".to_owned())
        );
        validate_schema_contract(contract_schema(vec![field]).as_ref())
            .expect("contratto geometrycollection canonico");
    }

    #[test]
    fn limit_without_order_is_rejected_fail_closed() {
        let description = MysqlObjectDescription {
            schema: "data".to_owned(),
            name: "items".to_owned(),
            kind: "BASE TABLE".to_owned(),
            engine: Some("InnoDB".to_owned()),
            columns: vec![column("id", "bigint", "bigint")],
            token: crate::MysqlSchemaToken("token".to_owned()),
        };
        let operation = ReadOperation {
            source: ObjectRef {
                catalog: None,
                schema: Some("data".to_owned()),
                object: "items".to_owned(),
                layer_id: None,
            },
            projection: Vec::new(),
            order_by: Vec::new(),
            row_limit: Some(1),
            filter: None,
        };
        assert_eq!(
            MysqlReadPlan::compile(&description, &operation)
                .expect_err("nondeterministic limit")
                .category,
            ErrorCategory::InvalidPlan
        );

        let ordered = ReadOperation {
            order_by: vec![OrderBy {
                field: "id".to_owned(),
                direction: SortDirection::Asc,
            }],
            ..operation
        };
        let plan = MysqlReadPlan::compile(&description, &ordered).expect("ordered plan");
        assert_eq!(
            plan.sql,
            "SELECT `id` FROM `data`.`items` ORDER BY `id` ASC LIMIT 1;"
        );
    }
}
