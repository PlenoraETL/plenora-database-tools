//! SQL portabile: identificatori, AST, placeholder e rendering deterministico.
//!
//! L'AST non contiene valori. Il risultato associa il testo SQL ai nomi dei
//! parametri che il driver dovrà bindare.

use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::plan::{ComparisonOperator, SortDirection};
use plenora_database_core::query::SpatialFunction;
use plenora_database_core::query::{
    validate_query_operation, JoinKind, QueryDerivedSource, QueryExpression, QueryLockStrength,
    QueryLockWait, QueryOperation, QuerySetOperator, QuerySource, ScalarFunction, SpatialOperator,
    WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

const SQL_SERVER_MAX_IDENTIFIER_CHARS: usize = 128;
const SQL_SERVER_MAX_BIND_PARAMETERS: usize = 2_100;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier(String);

impl Identifier {
    /// Costruisce un identificatore validato ma non ancora quotato.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per stringa vuota, NUL o oltre 256 byte.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.contains('\0') {
            return Err(DatabaseError::invalid_plan(
                "identificatore SQL vuoto o contenente NUL",
            ));
        }
        if value.len() > 256 {
            return Err(DatabaseError::invalid_plan(
                "identificatore SQL oltre il limite del core",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Serialize for Identifier {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Identifier {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectName {
    pub catalog: Option<Identifier>,
    pub schema: Option<Identifier>,
    pub object: Identifier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Mysql,
    SqlServer,
    Oracle,
    Db2,
    Sqlite,
    Duckdb,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialectCapabilities {
    pub spatial_intersects: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Expression {
    And(Vec<Self>),
    Or(Vec<Self>),
    Compare {
        field: Identifier,
        operator: ComparisonOperator,
        parameter: String,
    },
    IsNull(Identifier),
    IsNotNull(Identifier),
    In {
        field: Identifier,
        parameters: Vec<String>,
    },
    Between {
        field: Identifier,
        lower_parameter: String,
        upper_parameter: String,
    },
    Like {
        field: Identifier,
        parameter: String,
        case_insensitive: bool,
    },
    SpatialIntersects {
        field: Identifier,
        wkb_parameter: String,
    },
    SpatialPredicate {
        function: SpatialFunction,
        field: Identifier,
        geometry_parameter: Option<String>,
        distance_parameter: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ordering {
    pub field: Identifier,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Select {
    pub source: ObjectName,
    pub projection: Vec<Identifier>,
    pub filter: Option<Expression>,
    pub order_by: Vec<Ordering>,
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindParameter {
    pub ordinal: usize,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedSql {
    pub sql: String,
    pub binds: Vec<BindParameter>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlServerSpatialParameter {
    pub semantics: SpatialSemantics,
    pub srid: u32,
}

pub struct Renderer {
    dialect: Dialect,
    capabilities: DialectCapabilities,
    sql_server_spatial_parameters: BTreeMap<String, SqlServerSpatialParameter>,
}

impl Renderer {
    #[must_use]
    pub const fn new(dialect: Dialect, capabilities: DialectCapabilities) -> Self {
        Self {
            dialect,
            capabilities,
            sql_server_spatial_parameters: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_sql_server_spatial_parameters(
        mut self,
        parameters: BTreeMap<String, SqlServerSpatialParameter>,
    ) -> Self {
        self.sql_server_spatial_parameters = parameters;
        self
    }

    /// Renderizza una SELECT senza interpolare valori.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per projection o gruppi vuoti e
    /// `Unsupported` per funzioni non pubblicizzate dal dialect.
    pub fn render_select(&self, select: &Select) -> Result<RenderedSql> {
        if select.projection.is_empty() {
            return Err(DatabaseError::invalid_plan(
                "la projection SQL deve essere esplicita e non vuota",
            ));
        }
        self.validate_select_dialect_limits(select)?;
        let mut sql = String::from("SELECT ");
        if self.dialect == Dialect::SqlServer {
            if let Some(limit) = select.limit {
                sql.push_str("TOP (");
                sql.push_str(&limit.to_string());
                sql.push_str(") ");
            }
        }
        sql.push_str(
            &select
                .projection
                .iter()
                .map(|field| self.quote(field))
                .collect::<Vec<_>>()
                .join(", "),
        );
        sql.push_str(" FROM ");
        sql.push_str(&self.render_object(&select.source));

        let mut binds = Vec::new();
        if let Some(filter) = &select.filter {
            sql.push_str(" WHERE ");
            sql.push_str(&self.render_expression(filter, &mut binds)?);
        }
        if !select.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            sql.push_str(
                &select
                    .order_by
                    .iter()
                    .map(|order| {
                        let direction = match order.direction {
                            SortDirection::Asc => "ASC",
                            SortDirection::Desc => "DESC",
                        };
                        format!("{} {direction}", self.quote(&order.field))
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        if let Some(limit) = select.limit {
            match self.dialect {
                Dialect::Postgres | Dialect::Mysql | Dialect::Sqlite | Dialect::Duckdb => {
                    sql.push_str(" LIMIT ");
                    sql.push_str(&limit.to_string());
                }
                Dialect::Oracle | Dialect::Db2 => {
                    sql.push_str(" FETCH FIRST ");
                    sql.push_str(&limit.to_string());
                    sql.push_str(" ROWS ONLY");
                }
                Dialect::SqlServer => {}
            }
        }
        self.validate_bind_count(&binds)?;
        Ok(RenderedSql { sql, binds })
    }

    /// Renderizza il nuovo AST relazionale mantenendo tutti i valori nei bind.
    ///
    /// # Errors
    ///
    /// Fallisce per query strutturalmente incomplete o funzioni non supportate.
    pub fn render_query(&self, query: &QueryOperation) -> Result<RenderedSql> {
        self.render_query_with_spatial_encoding(query, true)
    }

    /// Renderizza una query mantenendo gli output spatial nel tipo nativo.
    ///
    /// Questa variante è riservata ai preflight del provider: un output UDT
    /// SQL Server non è direttamente un contratto Arrow trasportabile.
    ///
    /// # Errors
    ///
    /// Fallisce per query strutturalmente incomplete o funzioni non supportate.
    pub fn render_query_native_spatial(&self, query: &QueryOperation) -> Result<RenderedSql> {
        self.render_query_with_spatial_encoding(query, false)
    }

    fn render_query_with_spatial_encoding(
        &self,
        query: &QueryOperation,
        encode_spatial_output: bool,
    ) -> Result<RenderedSql> {
        let mut limits = plenora_database_core::limits::Limits::default();
        if self.dialect == Dialect::SqlServer {
            // È volutamente più conservativo del limite SQL Server espresso
            // in caratteri: il core limita anche i byte allocabili.
            limits.max_identifier_bytes = SQL_SERVER_MAX_IDENTIFIER_CHARS;
        }
        validate_query_operation(query, &limits)?;
        let mut binds = Vec::new();
        let sql = self.render_query_inner(query, &mut binds, encode_spatial_output)?;
        self.validate_bind_count(&binds)?;
        Ok(RenderedSql { sql, binds })
    }

    #[allow(clippy::too_many_lines)]
    fn render_query_inner(
        &self,
        query: &QueryOperation,
        binds: &mut Vec<BindParameter>,
        encode_spatial_output: bool,
    ) -> Result<String> {
        if query.projection.is_empty() {
            return Err(DatabaseError::invalid_plan("query senza projection"));
        }
        let mut sql = String::new();
        if !query.common_table_expressions.is_empty() {
            sql.push_str("WITH ");
            if query
                .common_table_expressions
                .iter()
                .any(|cte| cte.recursive)
            {
                if !matches!(self.dialect, Dialect::Postgres | Dialect::SqlServer) {
                    return Err(DatabaseError::unsupported(
                        self.provider_kind(),
                        ErrorPhase::Prepare,
                        "CTE ricorsiva non supportata dal dialect",
                    ));
                }
                if self.dialect == Dialect::Postgres {
                    sql.push_str("RECURSIVE ");
                }
            }
            let ctes = query
                .common_table_expressions
                .iter()
                .map(|cte| {
                    let name = self.quote(&Identifier::new(cte.name.clone())?);
                    let body = self.render_query_inner(&cte.query, binds, false)?;
                    Ok(format!("{name} AS ({body})"))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(&ctes.join(", "));
            sql.push(' ');
        }
        sql.push_str("SELECT ");
        if !query.distinct_on.is_empty() {
            if self.dialect != Dialect::Postgres {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "DISTINCT ON supportato solo da PostgreSQL",
                ));
            }
            let expressions = query
                .distinct_on
                .iter()
                .map(|expression| self.render_query_expression(expression, binds))
                .collect::<Result<Vec<_>>>()?;
            sql.push_str("DISTINCT ON (");
            sql.push_str(&expressions.join(", "));
            sql.push_str(") ");
        } else if query.distinct {
            sql.push_str("DISTINCT ");
        }
        if self.dialect == Dialect::SqlServer {
            if let Some(limit) = query.row_limit.filter(|_| query.row_offset.is_none()) {
                sql.push_str("TOP (");
                sql.push_str(&limit.to_string());
                sql.push_str(") ");
            }
        }
        let projection = query
            .projection
            .iter()
            .map(|item| {
                let mut rendered = match &item.expression {
                    QueryExpression::Spatial {
                        function,
                        arguments,
                    } if self.dialect == Dialect::SqlServer
                        && function.returns_boolean(arguments.len()) =>
                    {
                        self.render_sql_server_spatial_function_value(
                            *function, arguments, binds, false,
                        )?
                    }
                    _ => self.render_query_expression(&item.expression, binds)?,
                };
                if matches!(
                    &item.expression,
                    QueryExpression::Spatial { function, .. } if function.returns_geometry()
                ) && encode_spatial_output
                {
                    rendered = match self.dialect {
                        Dialect::Postgres => format!("ST_AsEWKB({rendered})"),
                        Dialect::SqlServer => format!("({rendered}).AsBinaryZM()"),
                        _ => rendered,
                    };
                }
                if let Some(alias) = &item.alias {
                    rendered.push_str(" AS ");
                    rendered.push_str(&self.quote(&Identifier::new(alias.clone())?));
                }
                Ok(rendered)
            })
            .collect::<Result<Vec<_>>>()?;
        sql.push_str(&projection.join(", "));
        sql.push_str(" FROM ");
        sql.push_str(&self.render_query_relation(
            query.source.as_ref(),
            query.derived_source.as_ref(),
            false,
            binds,
        )?);
        for join in &query.joins {
            let keyword = match join.kind {
                JoinKind::Inner => " INNER JOIN ",
                JoinKind::Left => " LEFT JOIN ",
                JoinKind::Right => " RIGHT JOIN ",
                JoinKind::Full => " FULL JOIN ",
                JoinKind::Cross => " CROSS JOIN ",
            };
            sql.push_str(keyword);
            sql.push_str(&self.render_query_relation(
                join.source.as_ref(),
                join.derived_source.as_ref(),
                join.lateral,
                binds,
            )?);
            if join.kind == JoinKind::Cross {
                if join.on.is_some() {
                    return Err(DatabaseError::invalid_plan("CROSS JOIN con clausola ON"));
                }
            } else {
                let on = join
                    .on
                    .as_ref()
                    .ok_or_else(|| DatabaseError::invalid_plan("JOIN senza clausola ON"))?;
                sql.push_str(" ON ");
                sql.push_str(&self.render_query_expression(on, binds)?);
            }
        }
        if let Some(filter) = &query.filter {
            sql.push_str(" WHERE ");
            sql.push_str(&self.render_query_expression(filter, binds)?);
        }
        if !query.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            let group = query
                .group_by
                .iter()
                .map(|expression| self.render_query_expression(expression, binds))
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(&group.join(", "));
        }
        if let Some(having) = &query.having {
            sql.push_str(" HAVING ");
            sql.push_str(&self.render_query_expression(having, binds)?);
        }
        for set_operation in &query.set_operations {
            let operator = match set_operation.operator {
                QuerySetOperator::Union => " UNION",
                QuerySetOperator::Intersect => " INTERSECT",
                QuerySetOperator::Except => " EXCEPT",
            };
            sql.push_str(operator);
            if set_operation.all {
                sql.push_str(" ALL");
            }
            sql.push_str(" (");
            sql.push_str(&self.render_query_inner(
                &set_operation.query,
                binds,
                encode_spatial_output,
            )?);
            sql.push(')');
        }
        if !query.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let ordering = query
                .order_by
                .iter()
                .map(|order| {
                    let expression = self.render_query_expression(&order.expression, binds)?;
                    let direction = match order.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    };
                    Ok(format!("{expression} {direction}"))
                })
                .collect::<Result<Vec<_>>>()?;
            sql.push_str(&ordering.join(", "));
        }
        if let Some(limit) = query.row_limit {
            match self.dialect {
                Dialect::Postgres | Dialect::Mysql | Dialect::Sqlite | Dialect::Duckdb => {
                    sql.push_str(" LIMIT ");
                    sql.push_str(&limit.to_string());
                }
                Dialect::Oracle | Dialect::Db2 => {
                    if query.row_offset.is_none() {
                        sql.push_str(" FETCH FIRST ");
                        sql.push_str(&limit.to_string());
                        sql.push_str(" ROWS ONLY");
                    }
                }
                Dialect::SqlServer => {}
            }
        }
        if let Some(offset) = query.row_offset {
            match self.dialect {
                Dialect::Postgres | Dialect::Mysql | Dialect::Sqlite | Dialect::Duckdb => {
                    sql.push_str(" OFFSET ");
                    sql.push_str(&offset.to_string());
                }
                Dialect::Oracle | Dialect::Db2 | Dialect::SqlServer => {
                    sql.push_str(" OFFSET ");
                    sql.push_str(&offset.to_string());
                    sql.push_str(" ROWS");
                    if let Some(limit) = query.row_limit {
                        sql.push_str(" FETCH NEXT ");
                        sql.push_str(&limit.to_string());
                        sql.push_str(" ROWS ONLY");
                    }
                }
            }
        }
        if let Some(locking) = &query.locking {
            if self.dialect != Dialect::Postgres {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "locking avanzato supportato solo da PostgreSQL",
                ));
            }
            sql.push_str(match locking.strength {
                QueryLockStrength::Update => " FOR UPDATE",
                QueryLockStrength::NoKeyUpdate => " FOR NO KEY UPDATE",
                QueryLockStrength::Share => " FOR SHARE",
                QueryLockStrength::KeyShare => " FOR KEY SHARE",
            });
            if !locking.relations.is_empty() {
                sql.push_str(" OF ");
                let relations = locking
                    .relations
                    .iter()
                    .map(|relation| {
                        Identifier::new(relation.clone()).map(|identifier| self.quote(&identifier))
                    })
                    .collect::<Result<Vec<_>>>()?;
                sql.push_str(&relations.join(", "));
            }
            match locking.wait {
                QueryLockWait::Wait => {}
                QueryLockWait::NoWait => sql.push_str(" NOWAIT"),
                QueryLockWait::SkipLocked => sql.push_str(" SKIP LOCKED"),
            }
        }
        Ok(sql)
    }

    fn render_query_source(&self, source: &QuerySource) -> Result<String> {
        let mut value = self.render_object(&ObjectName {
            catalog: source
                .object
                .catalog
                .as_ref()
                .map(|part| Identifier::new(part.clone()))
                .transpose()?,
            schema: source
                .object
                .schema
                .as_ref()
                .map(|part| Identifier::new(part.clone()))
                .transpose()?,
            object: Identifier::new(source.object.object.clone())?,
        });
        if let Some(alias) = &source.alias {
            value.push_str(" AS ");
            value.push_str(&self.quote(&Identifier::new(alias.clone())?));
        }
        Ok(value)
    }

    fn render_query_relation(
        &self,
        source: Option<&QuerySource>,
        derived: Option<&QueryDerivedSource>,
        lateral: bool,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        if lateral && self.dialect == Dialect::SqlServer {
            return Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "subquery laterale SQL Server richiede APPLY tipizzato",
            ));
        }
        match (source, derived) {
            (Some(source), None) => {
                if lateral {
                    return Err(DatabaseError::invalid_plan(
                        "LATERAL richiede una subquery come source",
                    ));
                }
                self.render_query_source(source)
            }
            (None, Some(derived)) => {
                if lateral && self.dialect != Dialect::Postgres {
                    return Err(DatabaseError::unsupported(
                        self.provider_kind(),
                        ErrorPhase::Prepare,
                        "LATERAL supportato solo dal renderer PostgreSQL",
                    ));
                }
                let body = self.render_query_inner(&derived.query, binds, false)?;
                let alias = self.quote(&Identifier::new(derived.alias.clone())?);
                Ok(format!(
                    "{}({body}) AS {alias}",
                    if lateral { "LATERAL " } else { "" }
                ))
            }
            _ => Err(DatabaseError::invalid_plan(
                "query richiede una sola source, tabella o subquery",
            )),
        }
    }

    fn render_query_expression(
        &self,
        expression: &QueryExpression,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        match expression {
            QueryExpression::Wildcard { relation } => {
                if let Some(relation) = relation {
                    Ok(format!(
                        "{}.*",
                        self.quote(&Identifier::new(relation.clone())?)
                    ))
                } else {
                    Ok("*".to_owned())
                }
            }
            QueryExpression::Column { column } => {
                let field = self.quote(&Identifier::new(column.field.clone())?);
                if let Some(relation) = &column.relation {
                    Ok(format!(
                        "{}.{field}",
                        self.quote(&Identifier::new(relation.clone())?)
                    ))
                } else {
                    Ok(field)
                }
            }
            QueryExpression::Parameter { name } => Ok(self.bind(name, binds)),
            QueryExpression::Scalar {
                function,
                arguments,
            } => self.render_function(self.scalar_name(*function), arguments, binds),
            QueryExpression::Spatial {
                function,
                arguments,
            } => self.render_spatial_function(*function, arguments, binds),
            QueryExpression::SpatialOperator {
                operator,
                left,
                right,
            } => self.render_spatial_operator(*operator, left, right, binds),
            QueryExpression::Window {
                function,
                arguments,
                partition_by,
                order_by,
                frame,
            } => {
                let call = self.render_function(self.scalar_name(*function), arguments, binds)?;
                self.render_window_call(&call, partition_by, order_by, frame.as_ref(), binds)
            }
            QueryExpression::SpatialWindow {
                function,
                arguments,
                partition_by,
                order_by,
                frame,
            } => {
                let call = self.render_spatial_function(*function, arguments, binds)?;
                self.render_window_call(&call, partition_by, order_by, frame.as_ref(), binds)
            }
            QueryExpression::ScalarSubquery { query } => Ok(format!(
                "({})",
                self.render_query_inner(query, binds, false)?
            )),
            QueryExpression::Exists { query, negated } => Ok(format!(
                "{}EXISTS ({})",
                if *negated { "NOT " } else { "" },
                self.render_query_inner(query, binds, false)?
            )),
            QueryExpression::InSubquery {
                expression,
                query,
                negated,
            } => Ok(format!(
                "{} {}IN ({})",
                self.render_query_expression(expression, binds)?,
                if *negated { "NOT " } else { "" },
                self.render_query_inner(query, binds, false)?
            )),
            QueryExpression::Compare {
                left,
                operator,
                right,
            } => {
                let symbol = comparison_symbol(*operator);
                Ok(format!(
                    "{} {symbol} {}",
                    self.render_query_expression(left, binds)?,
                    self.render_query_expression(right, binds)?
                ))
            }
            QueryExpression::And { arguments } => self.render_query_group("AND", arguments, binds),
            QueryExpression::Or { arguments } => self.render_query_group("OR", arguments, binds),
            QueryExpression::IsNull {
                expression,
                negated,
            } => Ok(format!(
                "{} IS {}NULL",
                self.render_query_expression(expression, binds)?,
                if *negated { "NOT " } else { "" }
            )),
        }
    }

    fn render_spatial_operator(
        &self,
        operator: SpatialOperator,
        left: &QueryExpression,
        right: &QueryExpression,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        if self.dialect != Dialect::Postgres {
            return Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "operatori spatial indicizzati supportati solo da PostgreSQL/PostGIS",
            ));
        }
        let operand = |renderer: &Self,
                       expression: &QueryExpression,
                       binds: &mut Vec<BindParameter>|
         -> Result<String> {
            let value = renderer.render_query_expression(expression, binds)?;
            if matches!(expression, QueryExpression::Parameter { .. }) {
                Ok(format!("ST_GeomFromEWKB({value})"))
            } else {
                Ok(value)
            }
        };
        let left = operand(self, left, binds)?;
        let right = operand(self, right, binds)?;
        let symbol = match operator {
            SpatialOperator::BoundingBoxIntersects => "&&",
            SpatialOperator::BoundingBoxContains => "~",
            SpatialOperator::BoundingBoxContainedBy => "@",
            SpatialOperator::KnnDistance => "<->",
            SpatialOperator::KnnCentroidDistance => "<<->>",
        };
        Ok(format!("{left} {symbol} {right}"))
    }

    fn render_window_call(
        &self,
        call: &str,
        partition_by: &[QueryExpression],
        order_by: &[plenora_database_core::query::QueryOrdering],
        frame: Option<&WindowFrame>,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        let mut clauses = Vec::new();
        if !partition_by.is_empty() {
            let partition = partition_by
                .iter()
                .map(|item| self.render_query_expression(item, binds))
                .collect::<Result<Vec<_>>>()?;
            clauses.push(format!("PARTITION BY {}", partition.join(", ")));
        }
        if !order_by.is_empty() {
            let ordering = order_by
                .iter()
                .map(|item| {
                    let expression = self.render_query_expression(&item.expression, binds)?;
                    let direction = match item.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    };
                    Ok(format!("{expression} {direction}"))
                })
                .collect::<Result<Vec<_>>>()?;
            clauses.push(format!("ORDER BY {}", ordering.join(", ")));
        }
        if let Some(frame) = frame {
            clauses.push(render_window_frame(frame));
        }
        Ok(format!("{call} OVER ({})", clauses.join(" ")))
    }

    fn render_function(
        &self,
        name: &str,
        arguments: &[QueryExpression],
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        let arguments = arguments
            .iter()
            .map(|argument| self.render_query_expression(argument, binds))
            .collect::<Result<Vec<_>>>()?;
        Ok(format!("{name}({})", arguments.join(", ")))
    }

    fn render_spatial_function(
        &self,
        function: SpatialFunction,
        arguments: &[QueryExpression],
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        if self.dialect == Dialect::SqlServer {
            return self.render_sql_server_spatial_function(function, arguments, binds);
        }
        let rendered = arguments
            .iter()
            .enumerate()
            .map(|(index, argument)| {
                let value = self.render_query_expression(argument, binds)?;
                if spatial_geometry_argument(function, index)
                    && matches!(argument, QueryExpression::Parameter { .. })
                {
                    Ok(match self.dialect {
                        Dialect::Postgres => format!("ST_GeomFromEWKB({value})"),
                        Dialect::Mysql | Dialect::Sqlite | Dialect::Duckdb => {
                            format!("ST_GeomFromWKB({value})")
                        }
                        Dialect::SqlServer => {
                            return Err(DatabaseError::unsupported(
                                self.provider_kind(),
                                ErrorPhase::Prepare,
                                "spatial SQL Server senza tipo e SRID risolti",
                            ));
                        }
                        Dialect::Oracle => {
                            format!("SDO_UTIL.FROM_WKBGEOMETRY({value})")
                        }
                        Dialect::Db2 => {
                            format!("DB2GSE.ST_GeomFromWKB({value}, 0)")
                        }
                    })
                } else {
                    Ok(value)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(format!(
            "{}({})",
            spatial_name(function),
            rendered.join(", ")
        ))
    }

    fn render_sql_server_spatial_function(
        &self,
        function: SpatialFunction,
        arguments: &[QueryExpression],
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        self.render_sql_server_spatial_function_value(function, arguments, binds, true)
    }

    fn render_sql_server_spatial_function_value(
        &self,
        function: SpatialFunction,
        arguments: &[QueryExpression],
        binds: &mut Vec<BindParameter>,
        predicate_context: bool,
    ) -> Result<String> {
        if !self.capabilities.spatial_intersects {
            return Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "AST spatial SQL Server non abilitato",
            ));
        }
        let receiver = arguments.first().ok_or_else(|| {
            DatabaseError::invalid_plan("funzione spatial SQL Server senza ricevitore")
        })?;
        let receiver = self.render_sql_server_spatial_operand(receiver, binds)?;
        let unary = |method: &str| Ok(format!("{receiver}.{method}()"));
        let predicate = |call: String| {
            if predicate_context {
                format!("({call} = 1)")
            } else {
                call
            }
        };
        let unary_predicate = |method: &str| Ok(predicate(format!("{receiver}.{method}()")));
        let binary = |renderer: &Self,
                      method: &str,
                      binds: &mut Vec<BindParameter>|
         -> Result<String> {
            let right = arguments.get(1).ok_or_else(|| {
                DatabaseError::invalid_plan("funzione spatial SQL Server senza secondo operando")
            })?;
            let right = renderer.render_sql_server_spatial_operand(right, binds)?;
            Ok(format!("{receiver}.{method}({right})"))
        };
        let binary_predicate =
            |renderer: &Self, method: &str, binds: &mut Vec<BindParameter>| -> Result<String> {
                Ok(predicate(binary(renderer, method, binds)?))
            };
        let numeric = |renderer: &Self,
                       method: &str,
                       binds: &mut Vec<BindParameter>|
         -> Result<String> {
            let argument = arguments.get(1).ok_or_else(|| {
                DatabaseError::invalid_plan("funzione spatial SQL Server senza argomento numerico")
            })?;
            let argument = renderer.render_query_expression(argument, binds)?;
            Ok(format!("{receiver}.{method}({argument})"))
        };
        match function {
            SpatialFunction::GeometryType => unary("STGeometryType"),
            SpatialFunction::Srid => Ok(format!("{receiver}.STSrid")),
            SpatialFunction::NPoints => unary("STNumPoints"),
            SpatialFunction::IsEmpty => unary_predicate("STIsEmpty"),
            SpatialFunction::IsValid => unary_predicate("STIsValid"),
            SpatialFunction::IsClosed => unary_predicate("STIsClosed"),
            SpatialFunction::Intersects => binary_predicate(self, "STIntersects", binds),
            SpatialFunction::Contains => binary_predicate(self, "STContains", binds),
            SpatialFunction::Within => binary_predicate(self, "STWithin", binds),
            SpatialFunction::Disjoint => binary_predicate(self, "STDisjoint", binds),
            SpatialFunction::Equals => binary_predicate(self, "STEquals", binds),
            SpatialFunction::Distance => binary(self, "STDistance", binds),
            SpatialFunction::Area => unary("STArea"),
            SpatialFunction::Length => unary("STLength"),
            SpatialFunction::StartPoint => unary("STStartPoint"),
            SpatialFunction::EndPoint => unary("STEndPoint"),
            SpatialFunction::PointN => numeric(self, "STPointN", binds),
            SpatialFunction::Buffer => numeric(self, "STBuffer", binds),
            SpatialFunction::Intersection => binary(self, "STIntersection", binds),
            SpatialFunction::Difference => binary(self, "STDifference", binds),
            SpatialFunction::SymDifference => binary(self, "STSymDifference", binds),
            SpatialFunction::Union => binary(self, "STUnion", binds),
            SpatialFunction::ConvexHull => unary("STConvexHull"),
            _ => Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "funzione spatial non disponibile nel sottoinsieme SQL Server verificato",
            )),
        }
    }

    fn render_sql_server_spatial_operand(
        &self,
        expression: &QueryExpression,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        let QueryExpression::Parameter { name } = expression else {
            return self.render_query_expression(expression, binds);
        };
        let profile = self
            .sql_server_spatial_parameters
            .get(name)
            .ok_or_else(|| {
                DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "parametro spatial SQL Server senza semantica e SRID risolti",
                )
            })?;
        let srid = i32::try_from(profile.srid).map_err(|_| {
            DatabaseError::invalid_plan("SRID parametro spatial oltre il range int SQL Server")
        })?;
        let constructor = match profile.semantics {
            SpatialSemantics::Geometry => "geometry::STGeomFromWKB",
            SpatialSemantics::Geography => "geography::STGeomFromWKB",
        };
        let value = self.bind(name, binds);
        Ok(format!("{constructor}({value}, {srid})"))
    }

    fn render_query_group(
        &self,
        operator: &str,
        arguments: &[QueryExpression],
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        if arguments.is_empty() {
            return Err(DatabaseError::invalid_plan("gruppo query vuoto"));
        }
        let values = arguments
            .iter()
            .map(|argument| self.render_query_expression(argument, binds))
            .collect::<Result<Vec<_>>>()?;
        Ok(format!("({})", values.join(&format!(" {operator} "))))
    }

    /// Renderizza soltanto una condizione e l'ordine dei bind.
    ///
    /// # Errors
    ///
    /// Restituisce gli stessi errori capability/strutturali di
    /// [`Self::render_select`].
    pub fn render_filter(&self, expression: &Expression) -> Result<RenderedSql> {
        self.validate_expression_dialect_limits(expression)?;
        let mut binds = Vec::new();
        let sql = self.render_expression(expression, &mut binds)?;
        self.validate_bind_count(&binds)?;
        Ok(RenderedSql { sql, binds })
    }

    #[must_use]
    pub fn quote_identifier(&self, identifier: &Identifier) -> String {
        self.quote(identifier)
    }

    #[must_use]
    pub fn quote_object(&self, object: &ObjectName) -> String {
        self.render_object(object)
    }

    #[allow(clippy::too_many_lines)]
    fn render_expression(
        &self,
        expression: &Expression,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        match expression {
            Expression::And(args) => self.render_group("AND", args, binds),
            Expression::Or(args) => self.render_group("OR", args, binds),
            Expression::Compare {
                field,
                operator,
                parameter,
            } => {
                let symbol = match operator {
                    ComparisonOperator::Eq => "=",
                    ComparisonOperator::Ne => "<>",
                    ComparisonOperator::Lt => "<",
                    ComparisonOperator::Lte => "<=",
                    ComparisonOperator::Gt => ">",
                    ComparisonOperator::Gte => ">=",
                };
                let placeholder = self.bind(parameter, binds);
                Ok(format!("{} {symbol} {placeholder}", self.quote(field)))
            }
            Expression::IsNull(field) => Ok(format!("{} IS NULL", self.quote(field))),
            Expression::IsNotNull(field) => Ok(format!("{} IS NOT NULL", self.quote(field))),
            Expression::In { field, parameters } => {
                if parameters.is_empty() {
                    return Err(DatabaseError::invalid_plan("filtro IN senza parametri"));
                }
                let placeholders = parameters
                    .iter()
                    .map(|parameter| self.bind(parameter, binds))
                    .collect::<Vec<_>>();
                Ok(format!(
                    "{} IN ({})",
                    self.quote(field),
                    placeholders.join(", ")
                ))
            }
            Expression::Between {
                field,
                lower_parameter,
                upper_parameter,
            } => {
                let lower = self.bind(lower_parameter, binds);
                let upper = self.bind(upper_parameter, binds);
                Ok(format!("{} BETWEEN {lower} AND {upper}", self.quote(field)))
            }
            Expression::Like {
                field,
                parameter,
                case_insensitive,
            } => {
                let placeholder = self.bind(parameter, binds);
                let operator = if *case_insensitive && self.dialect == Dialect::Postgres {
                    "ILIKE"
                } else {
                    "LIKE"
                };
                Ok(format!("{} {operator} {placeholder}", self.quote(field)))
            }
            Expression::SpatialIntersects {
                field,
                wkb_parameter,
            } => {
                if !self.capabilities.spatial_intersects {
                    return Err(DatabaseError::unsupported(
                        self.provider_kind(),
                        ErrorPhase::Prepare,
                        "spatial intersects non supportato dal dialect",
                    ));
                }
                if self.dialect == Dialect::SqlServer {
                    return Err(DatabaseError::unsupported(
                        self.provider_kind(),
                        ErrorPhase::Prepare,
                        "spatial SQL Server richiede tipo geometry/geography e SRID risolti",
                    ));
                }
                let placeholder = self.bind(wkb_parameter, binds);
                let quoted = self.quote(field);
                let expression = match self.dialect {
                    Dialect::Postgres | Dialect::Mysql | Dialect::Sqlite | Dialect::Duckdb => {
                        format!("ST_Intersects({quoted}, ST_GeomFromWKB({placeholder}))")
                    }
                    Dialect::SqlServer => {
                        return Err(DatabaseError::unsupported(
                            self.provider_kind(),
                            ErrorPhase::Prepare,
                            "spatial SQL Server senza tipo e SRID risolti",
                        ));
                    }
                    Dialect::Oracle => {
                        format!(
                            "SDO_RELATE({quoted}, SDO_UTIL.FROM_WKBGEOMETRY({placeholder}), \
                             'mask=ANYINTERACT') = 'TRUE'"
                        )
                    }
                    Dialect::Db2 => {
                        format!(
                            "DB2GSE.ST_INTERSECTS(\
                             {quoted}, DB2GSE.ST_GeomFromWKB({placeholder}, 0)) = 1"
                        )
                    }
                };
                Ok(expression)
            }
            Expression::SpatialPredicate {
                function,
                field,
                geometry_parameter,
                distance_parameter,
            } => self.render_spatial_predicate(
                *function,
                field,
                geometry_parameter.as_deref(),
                distance_parameter.as_deref(),
                binds,
            ),
        }
    }

    fn render_spatial_predicate(
        &self,
        function: SpatialFunction,
        field: &Identifier,
        geometry_parameter: Option<&str>,
        distance_parameter: Option<&str>,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        if !self.capabilities.spatial_intersects || self.dialect != Dialect::Postgres {
            return Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "predicato spatial non supportato dal dialect",
            ));
        }
        let quoted = self.quote(field);
        if function.is_unary_predicate() {
            return Ok(format!("{}({quoted})", spatial_name(function)));
        }
        let geometry_name = geometry_parameter.ok_or_else(|| {
            DatabaseError::invalid_plan("predicato spatial senza parametro geometria")
        })?;
        let geometry = self.bind(geometry_name, binds);
        let right = format!("ST_GeomFromEWKB({geometry})");
        if function == SpatialFunction::DWithin {
            let distance_name = distance_parameter
                .ok_or_else(|| DatabaseError::invalid_plan("d_within senza parametro distanza"))?;
            let distance = self.bind(distance_name, binds);
            return Ok(format!("ST_DWithin({quoted}, {right}, {distance})"));
        }
        if !function.is_binary_predicate() {
            return Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "funzione spatial non valida come filtro",
            ));
        }
        let name = spatial_name(function);
        Ok(format!("{name}({quoted}, {right})"))
    }

    fn render_group(
        &self,
        operator: &str,
        args: &[Expression],
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        if args.is_empty() {
            return Err(DatabaseError::invalid_plan("gruppo filtro SQL vuoto"));
        }
        let rendered = args
            .iter()
            .map(|arg| self.render_expression(arg, binds))
            .collect::<Result<Vec<_>>>()?;
        Ok(format!("({})", rendered.join(&format!(" {operator} "))))
    }

    fn bind(&self, name: &str, binds: &mut Vec<BindParameter>) -> String {
        let ordinal = binds.len() + 1;
        binds.push(BindParameter {
            ordinal,
            name: name.to_owned(),
        });
        match self.dialect {
            Dialect::Postgres => format!("${ordinal}"),
            Dialect::Oracle => format!(":{ordinal}"),
            Dialect::SqlServer => format!("@p{ordinal}"),
            Dialect::Mysql | Dialect::Db2 | Dialect::Sqlite | Dialect::Duckdb => "?".to_owned(),
        }
    }

    fn quote(&self, identifier: &Identifier) -> String {
        match self.dialect {
            Dialect::Mysql => {
                format!("`{}`", identifier.as_str().replace('`', "``"))
            }
            Dialect::SqlServer => {
                format!("[{}]", identifier.as_str().replace(']', "]]"))
            }
            Dialect::Postgres
            | Dialect::Oracle
            | Dialect::Db2
            | Dialect::Sqlite
            | Dialect::Duckdb => {
                format!("\"{}\"", identifier.as_str().replace('"', "\"\""))
            }
        }
    }

    fn render_object(&self, object: &ObjectName) -> String {
        object
            .catalog
            .iter()
            .chain(object.schema.iter())
            .chain(std::iter::once(&object.object))
            .map(|part| self.quote(part))
            .collect::<Vec<_>>()
            .join(".")
    }

    fn validate_select_dialect_limits(&self, select: &Select) -> Result<()> {
        if self.dialect != Dialect::SqlServer {
            return Ok(());
        }
        for identifier in select
            .source
            .catalog
            .iter()
            .chain(select.source.schema.iter())
            .chain(std::iter::once(&select.source.object))
            .chain(select.projection.iter())
            .chain(select.order_by.iter().map(|ordering| &ordering.field))
        {
            Self::validate_identifier(identifier)?;
        }
        if let Some(filter) = &select.filter {
            self.validate_expression_dialect_limits(filter)?;
        }
        Ok(())
    }

    fn validate_expression_dialect_limits(&self, expression: &Expression) -> Result<()> {
        if self.dialect != Dialect::SqlServer {
            return Ok(());
        }
        let mut stack = vec![expression];
        while let Some(value) = stack.pop() {
            match value {
                Expression::And(arguments) | Expression::Or(arguments) => {
                    stack.extend(arguments);
                }
                Expression::Compare { field, .. }
                | Expression::IsNull(field)
                | Expression::IsNotNull(field)
                | Expression::In { field, .. }
                | Expression::Between { field, .. }
                | Expression::Like { field, .. }
                | Expression::SpatialIntersects { field, .. }
                | Expression::SpatialPredicate { field, .. } => {
                    Self::validate_identifier(field)?;
                }
            }
        }
        Ok(())
    }

    fn validate_identifier(identifier: &Identifier) -> Result<()> {
        if identifier.as_str().chars().count() > SQL_SERVER_MAX_IDENTIFIER_CHARS {
            return Err(DatabaseError::invalid_plan(
                "identificatore SQL Server oltre 128 caratteri",
            ));
        }
        Ok(())
    }

    fn validate_bind_count(&self, binds: &[BindParameter]) -> Result<()> {
        if self.dialect == Dialect::SqlServer && binds.len() > SQL_SERVER_MAX_BIND_PARAMETERS {
            return Err(DatabaseError::resource_limit(
                "query SQL Server oltre il limite di 2100 parametri",
            ));
        }
        Ok(())
    }

    fn scalar_name(&self, function: ScalarFunction) -> &'static str {
        if self.dialect == Dialect::SqlServer && matches!(function, ScalarFunction::Count) {
            "COUNT_BIG"
        } else {
            scalar_name(function)
        }
    }

    const fn provider_kind(&self) -> plenora_database_core::plan::ProviderKind {
        use plenora_database_core::plan::ProviderKind;
        match self.dialect {
            Dialect::Postgres => ProviderKind::Postgres,
            Dialect::Mysql => ProviderKind::Mysql,
            Dialect::SqlServer => ProviderKind::Sqlserver,
            Dialect::Oracle => ProviderKind::Oracle,
            Dialect::Db2 => ProviderKind::Db2,
            Dialect::Sqlite => ProviderKind::Sqlite,
            Dialect::Duckdb => ProviderKind::Duckdb,
        }
    }
}

const fn comparison_symbol(operator: ComparisonOperator) -> &'static str {
    match operator {
        ComparisonOperator::Eq => "=",
        ComparisonOperator::Ne => "<>",
        ComparisonOperator::Lt => "<",
        ComparisonOperator::Lte => "<=",
        ComparisonOperator::Gt => ">",
        ComparisonOperator::Gte => ">=",
    }
}

const fn scalar_name(function: ScalarFunction) -> &'static str {
    match function {
        ScalarFunction::Lower => "LOWER",
        ScalarFunction::Upper => "UPPER",
        ScalarFunction::Coalesce => "COALESCE",
        ScalarFunction::Count => "COUNT",
        ScalarFunction::Sum => "SUM",
        ScalarFunction::Average => "AVG",
        ScalarFunction::Minimum => "MIN",
        ScalarFunction::Maximum => "MAX",
        ScalarFunction::RowNumber => "ROW_NUMBER",
        ScalarFunction::Rank => "RANK",
        ScalarFunction::DenseRank => "DENSE_RANK",
        ScalarFunction::Lag => "LAG",
        ScalarFunction::Lead => "LEAD",
    }
}

fn render_window_frame(frame: &WindowFrame) -> String {
    let units = match frame.units {
        WindowFrameUnits::Rows => "ROWS",
        WindowFrameUnits::Range => "RANGE",
        WindowFrameUnits::Groups => "GROUPS",
    };
    let bound = |value: &WindowFrameBound| match value {
        WindowFrameBound::UnboundedPreceding => "UNBOUNDED PRECEDING".to_owned(),
        WindowFrameBound::Preceding(offset) => format!("{offset} PRECEDING"),
        WindowFrameBound::CurrentRow => "CURRENT ROW".to_owned(),
        WindowFrameBound::Following(offset) => format!("{offset} FOLLOWING"),
        WindowFrameBound::UnboundedFollowing => "UNBOUNDED FOLLOWING".to_owned(),
    };
    frame.end.as_ref().map_or_else(
        || format!("{units} {}", bound(&frame.start)),
        |end| format!("{units} BETWEEN {} AND {}", bound(&frame.start), bound(end)),
    )
}

const fn spatial_geometry_argument(function: SpatialFunction, index: usize) -> bool {
    function.takes_geometry_at(index)
}

const fn spatial_name(function: SpatialFunction) -> &'static str {
    match function {
        SpatialFunction::GeometryType => "ST_GeometryType",
        SpatialFunction::Srid => "ST_SRID",
        SpatialFunction::Dimensions => "ST_NDims",
        SpatialFunction::X => "ST_X",
        SpatialFunction::Y => "ST_Y",
        SpatialFunction::Z => "ST_Z",
        SpatialFunction::M => "ST_M",
        SpatialFunction::NPoints => "ST_NPoints",
        SpatialFunction::NRings => "ST_NRings",
        SpatialFunction::StartPoint => "ST_StartPoint",
        SpatialFunction::EndPoint => "ST_EndPoint",
        SpatialFunction::PointN => "ST_PointN",
        SpatialFunction::IsEmpty => "ST_IsEmpty",
        SpatialFunction::IsValid => "ST_IsValid",
        SpatialFunction::IsSimple => "ST_IsSimple",
        SpatialFunction::IsClosed => "ST_IsClosed",
        SpatialFunction::Intersects => "ST_Intersects",
        SpatialFunction::Contains => "ST_Contains",
        SpatialFunction::ContainsProperly => "ST_ContainsProperly",
        SpatialFunction::Within => "ST_Within",
        SpatialFunction::Covers => "ST_Covers",
        SpatialFunction::CoveredBy => "ST_CoveredBy",
        SpatialFunction::Touches => "ST_Touches",
        SpatialFunction::Crosses => "ST_Crosses",
        SpatialFunction::Overlaps => "ST_Overlaps",
        SpatialFunction::Disjoint => "ST_Disjoint",
        SpatialFunction::Equals => "ST_Equals",
        SpatialFunction::Relate => "ST_Relate",
        SpatialFunction::DWithin => "ST_DWithin",
        SpatialFunction::SetSrid => "ST_SetSRID",
        SpatialFunction::Transform => "ST_Transform",
        SpatialFunction::Force2d => "ST_Force2D",
        SpatialFunction::Force3d => "ST_Force3D",
        SpatialFunction::Force3dm => "ST_Force3DM",
        SpatialFunction::Force4d => "ST_Force4D",
        SpatialFunction::Buffer => "ST_Buffer",
        SpatialFunction::OffsetCurve => "ST_OffsetCurve",
        SpatialFunction::Intersection => "ST_Intersection",
        SpatialFunction::Difference => "ST_Difference",
        SpatialFunction::SymDifference => "ST_SymDifference",
        SpatialFunction::Union => "ST_Union",
        SpatialFunction::UnaryUnion => "ST_UnaryUnion",
        SpatialFunction::Simplify => "ST_Simplify",
        SpatialFunction::SimplifyPreserveTopology => "ST_SimplifyPreserveTopology",
        SpatialFunction::MakeValid => "ST_MakeValid",
        SpatialFunction::Centroid => "ST_Centroid",
        SpatialFunction::PointOnSurface => "ST_PointOnSurface",
        SpatialFunction::Envelope => "ST_Envelope",
        SpatialFunction::ConvexHull => "ST_ConvexHull",
        SpatialFunction::OrientedEnvelope => "ST_OrientedEnvelope",
        SpatialFunction::Boundary => "ST_Boundary",
        SpatialFunction::LineMerge => "ST_LineMerge",
        SpatialFunction::Reverse => "ST_Reverse",
        SpatialFunction::Subdivide => "ST_Subdivide",
        SpatialFunction::SnapToGrid => "ST_SnapToGrid",
        SpatialFunction::Distance => "ST_Distance",
        SpatialFunction::Distance3d => "ST_3DDistance",
        SpatialFunction::MaxDistance => "ST_MaxDistance",
        SpatialFunction::HausdorffDistance => "ST_HausdorffDistance",
        SpatialFunction::FrechetDistance => "ST_FrechetDistance",
        SpatialFunction::Azimuth => "ST_Azimuth",
        SpatialFunction::Area => "ST_Area",
        SpatialFunction::Length => "ST_Length",
        SpatialFunction::Perimeter => "ST_Perimeter",
        SpatialFunction::Collect => "ST_Collect",
        SpatialFunction::Extent => "ST_Extent",
        SpatialFunction::AsGeoJson => "ST_AsGeoJSON",
        SpatialFunction::AsMvtGeom => "ST_AsMVTGeom",
        SpatialFunction::AsMvt => "ST_AsMVT",
        SpatialFunction::AsGeobuf => "ST_AsGeobuf",
        SpatialFunction::ClusterDbscan => "ST_ClusterDBSCAN",
        SpatialFunction::ClusterKMeans => "ST_ClusterKMeans",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use plenora_database_core::plan::ObjectRef;
    use plenora_database_core::query::{
        ColumnRef, CommonTableExpression, QueryJoin, QueryLock, QueryOrdering, QueryProjection,
        QuerySetOperation,
    };

    fn identifier(value: &str) -> Identifier {
        Identifier::new(value).expect("identifier fixture")
    }

    fn source() -> ObjectName {
        ObjectName {
            catalog: None,
            schema: Some(identifier("public")),
            object: identifier("events"),
        }
    }

    fn query_column(relation: &str, field: &str) -> QueryExpression {
        QueryExpression::Column {
            column: ColumnRef {
                relation: Some(relation.to_owned()),
                field: field.to_owned(),
            },
        }
    }

    fn query_source(object: &str, alias: &str) -> QuerySource {
        QuerySource {
            object: ObjectRef {
                catalog: None,
                schema: None,
                object: object.to_owned(),
                layer_id: None,
            },
            alias: Some(alias.to_owned()),
        }
    }

    fn simple_query() -> QueryOperation {
        QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("e", "id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        }
    }

    #[test]
    fn postgres_uses_quoted_identifiers_and_binds() {
        let select = Select {
            source: source(),
            projection: vec![identifier("select"), identifier("a\"b")],
            filter: Some(Expression::Compare {
                field: identifier("name"),
                operator: ComparisonOperator::Eq,
                parameter: "secret_value".to_owned(),
            }),
            order_by: vec![],
            limit: Some(10),
        };
        let rendered = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_select(&select)
        .expect("render");
        assert_eq!(
            rendered.sql,
            "SELECT \"select\", \"a\"\"b\" FROM \"public\".\"events\" WHERE \"name\" = $1 LIMIT 10"
        );
        assert_eq!(rendered.binds[0].name, "secret_value");
        assert!(!rendered.sql.contains("secret_value"));
    }

    #[test]
    fn postgres_spatial_renderer_matches_the_versioned_catalog() {
        let catalog = plenora_database_core::spatial_catalog::spatial_function_catalog()
            .expect("embedded spatial catalog");
        assert_eq!(SpatialFunction::ALL.len(), catalog.functions.len());
        for (function, specification) in SpatialFunction::ALL.iter().zip(&catalog.functions) {
            assert_eq!(
                spatial_name(*function),
                specification.postgres,
                "{}",
                specification.id
            );
        }
    }

    #[test]
    fn sqlserver_escapes_closing_bracket() {
        let select = Select {
            source: source(),
            projection: vec![identifier("a]b")],
            filter: None,
            order_by: vec![],
            limit: Some(5),
        };
        let rendered = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_select(&select)
        .expect("render");
        assert_eq!(rendered.sql, "SELECT TOP (5) [a]]b] FROM [public].[events]");
    }

    #[test]
    fn sqlserver_rejects_identifier_over_128_characters() {
        let select = Select {
            source: source(),
            projection: vec![identifier(&"x".repeat(129))],
            filter: None,
            order_by: Vec::new(),
            limit: None,
        };
        let error = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_select(&select)
        .expect_err("identifier must fail");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::InvalidPlan
        );
    }

    #[test]
    fn sqlserver_enforces_2100_bind_limit() {
        let render = |count| {
            Renderer::new(
                Dialect::SqlServer,
                DialectCapabilities {
                    spatial_intersects: false,
                },
            )
            .render_filter(&Expression::In {
                field: identifier("id"),
                parameters: (0..count).map(|index| format!("p{index}")).collect(),
            })
        };
        assert_eq!(render(2_100).expect("2100 binds").binds.len(), 2_100);
        let error = render(2_101).expect_err("2101 binds must fail");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::ResourceLimit
        );
    }

    #[test]
    fn sqlserver_uses_offset_fetch_without_top() {
        let mut query = simple_query();
        query.order_by.push(QueryOrdering {
            expression: query_column("e", "id"),
            direction: SortDirection::Asc,
        });
        query.row_offset = Some(5);
        query.row_limit = Some(10);
        let sql = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_query(&query)
        .expect("SQL Server pagination")
        .sql;
        assert!(sql.ends_with("ORDER BY [e].[id] ASC OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY"));
        assert!(!sql.contains("TOP"));
    }

    #[test]
    fn sqlserver_renders_count_big_and_recursive_cte_syntax() {
        let cte_body = simple_query();
        let mut query = simple_query();
        query.common_table_expressions.push(CommonTableExpression {
            name: "tree".to_owned(),
            recursive: true,
            query: Box::new(cte_body),
        });
        query.projection[0].expression = QueryExpression::Scalar {
            function: ScalarFunction::Count,
            arguments: vec![query_column("e", "id")],
        };
        let sql = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_query(&query)
        .expect("SQL Server CTE")
        .sql;
        assert!(sql.starts_with("WITH [tree] AS ("));
        assert!(!sql.starts_with("WITH RECURSIVE"));
        assert!(sql.contains("COUNT_BIG([e].[id])"));
    }

    #[test]
    fn sqlserver_spatial_ast_fails_without_resolved_type_and_srid() {
        let mut query = simple_query();
        query.filter = Some(QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![
                query_column("e", "geom"),
                QueryExpression::Parameter {
                    name: "probe".to_owned(),
                },
            ],
        });
        let error = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect_err("unresolved spatial input must fail");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported
        );
    }

    #[test]
    fn sqlserver_spatial_ast_uses_typed_wkb_constructor_and_bound_value() {
        let mut query = simple_query();
        query.filter = Some(QueryExpression::Spatial {
            function: SpatialFunction::Intersects,
            arguments: vec![
                query_column("e", "shape"),
                QueryExpression::Parameter {
                    name: "needle".to_owned(),
                },
            ],
        });
        let rendered = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .with_sql_server_spatial_parameters(BTreeMap::from([(
            "needle".to_owned(),
            SqlServerSpatialParameter {
                semantics: SpatialSemantics::Geometry,
                srid: 4_326,
            },
        )]))
        .render_query(&query)
        .expect("typed SQL Server spatial query");
        assert!(rendered
            .sql
            .contains("([e].[shape].STIntersects(geometry::STGeomFromWKB(@p1, 4326)) = 1)"));
        assert_eq!(rendered.binds[0].name, "needle");
        assert!(!rendered.sql.contains("needle"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn sqlserver_renders_only_the_verified_native_scalar_spatial_subset() {
        let renderer = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .with_sql_server_spatial_parameters(BTreeMap::from([(
            "needle".to_owned(),
            SqlServerSpatialParameter {
                semantics: SpatialSemantics::Geography,
                srid: 4_326,
            },
        )]));
        for (function, fragment) in [
            (
                SpatialFunction::GeometryType,
                "[e].[shape].STGeometryType()",
            ),
            (SpatialFunction::Srid, "[e].[shape].STSrid"),
            (SpatialFunction::NPoints, "[e].[shape].STNumPoints()"),
            (SpatialFunction::IsEmpty, "[e].[shape].STIsEmpty()"),
            (SpatialFunction::IsValid, "[e].[shape].STIsValid()"),
            (SpatialFunction::IsClosed, "[e].[shape].STIsClosed()"),
            (SpatialFunction::Area, "[e].[shape].STArea()"),
            (SpatialFunction::Length, "[e].[shape].STLength()"),
            (SpatialFunction::StartPoint, "[e].[shape].STStartPoint()"),
            (SpatialFunction::EndPoint, "[e].[shape].STEndPoint()"),
            (SpatialFunction::ConvexHull, "[e].[shape].STConvexHull()"),
        ] {
            let mut query = simple_query();
            query.projection[0] = QueryProjection {
                expression: QueryExpression::Spatial {
                    function,
                    arguments: vec![query_column("e", "shape")],
                },
                alias: Some("value".to_owned()),
            };
            let rendered_sql = renderer.render_query(&query).expect("unary spatial method");
            assert!(rendered_sql.sql.contains(fragment), "{function:?}");
            if function.returns_boolean(1) {
                assert!(!rendered_sql.sql.contains("CASE WHEN"));
                assert!(!rendered_sql.sql.contains(" = 1) AS [value]"));
            }
            if function.returns_geometry() {
                assert!(rendered_sql
                    .sql
                    .contains(&format!("({fragment}).AsBinaryZM() AS [value]")));
                let native = renderer
                    .render_query_native_spatial(&query)
                    .expect("native spatial profile");
                assert!(native.sql.contains(&format!("{fragment} AS [value]")));
                assert!(!native.sql.contains("AsBinaryZM"));
            }
            assert!(rendered_sql.binds.is_empty());
        }
        for (function, method) in [
            (SpatialFunction::Intersects, "STIntersects"),
            (SpatialFunction::Contains, "STContains"),
            (SpatialFunction::Within, "STWithin"),
            (SpatialFunction::Disjoint, "STDisjoint"),
            (SpatialFunction::Equals, "STEquals"),
            (SpatialFunction::Distance, "STDistance"),
            (SpatialFunction::Intersection, "STIntersection"),
            (SpatialFunction::Difference, "STDifference"),
            (SpatialFunction::SymDifference, "STSymDifference"),
            (SpatialFunction::Union, "STUnion"),
        ] {
            let mut query = simple_query();
            query.projection[0] = QueryProjection {
                expression: QueryExpression::Spatial {
                    function,
                    arguments: vec![
                        query_column("e", "shape"),
                        QueryExpression::Parameter {
                            name: "needle".to_owned(),
                        },
                    ],
                },
                alias: Some("value".to_owned()),
            };
            let rendered_sql = renderer
                .render_query(&query)
                .expect("binary spatial method");
            assert!(
                rendered_sql.sql.contains(&format!(
                    "[e].[shape].{method}(geography::STGeomFromWKB(@p1, 4326))"
                )),
                "{function:?}"
            );
            if function.returns_boolean(2) {
                assert!(!rendered_sql.sql.contains("CASE WHEN"));
                assert!(!rendered_sql.sql.contains(" = 1) AS [value]"));
            }
            assert_eq!(rendered_sql.binds[0].name, "needle");
        }
        for (function, method, parameter) in [
            (SpatialFunction::PointN, "STPointN", "point_index"),
            (SpatialFunction::Buffer, "STBuffer", "distance"),
        ] {
            let mut query = simple_query();
            query.projection[0] = QueryProjection {
                expression: QueryExpression::Spatial {
                    function,
                    arguments: vec![
                        query_column("e", "shape"),
                        QueryExpression::Parameter {
                            name: parameter.to_owned(),
                        },
                    ],
                },
                alias: Some("value".to_owned()),
            };
            let rendered_sql = renderer
                .render_query(&query)
                .expect("numeric spatial method");
            assert!(rendered_sql
                .sql
                .contains(&format!("([e].[shape].{method}(@p1)).AsBinaryZM()")));
            assert_eq!(rendered_sql.binds[0].name, parameter);
        }

        let mut unsupported = simple_query();
        unsupported.projection[0] = QueryProjection {
            expression: QueryExpression::Spatial {
                function: SpatialFunction::MakeValid,
                arguments: vec![query_column("e", "shape")],
            },
            alias: Some("value".to_owned()),
        };
        assert_eq!(
            renderer
                .render_query(&unsupported)
                .expect_err("unverified spatial method")
                .category,
            plenora_database_core::ErrorCategory::Unsupported
        );
    }

    #[test]
    fn spatial_is_capability_gated() {
        let select = Select {
            source: source(),
            projection: vec![identifier("geom")],
            filter: Some(Expression::SpatialIntersects {
                field: identifier("geom"),
                wkb_parameter: "area".to_owned(),
            }),
            order_by: vec![],
            limit: None,
        };
        let error = Renderer::new(
            Dialect::Db2,
            DialectCapabilities {
                spatial_intersects: false,
            },
        )
        .render_select(&select)
        .expect_err("capability must fail");
        assert_eq!(
            error.category,
            plenora_database_core::ErrorCategory::Unsupported
        );
    }

    #[test]
    fn postgres_renders_typed_d_within_with_ewkb_binds() {
        let rendered = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_filter(&Expression::SpatialPredicate {
            function: SpatialFunction::DWithin,
            field: identifier("geom"),
            geometry_parameter: Some("probe".to_owned()),
            distance_parameter: Some("radius".to_owned()),
        })
        .expect("spatial render");
        assert_eq!(
            rendered.sql,
            "ST_DWithin(\"geom\", ST_GeomFromEWKB($1), $2)"
        );
        assert_eq!(rendered.binds[0].name, "probe");
        assert_eq!(rendered.binds[1].name, "radius");
    }

    #[test]
    fn postgres_query_ast_wraps_spatial_wkb_parameters() {
        let query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("e", "id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: Some(QueryExpression::Spatial {
                function: SpatialFunction::DWithin,
                arguments: vec![
                    query_column("e", "geom"),
                    QueryExpression::Parameter {
                        name: "probe".to_owned(),
                    },
                    QueryExpression::Parameter {
                        name: "radius".to_owned(),
                    },
                ],
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(10),
            row_offset: None,
            locking: None,
        };
        let rendered = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("query spatial");
        assert!(rendered
            .sql
            .contains("ST_DWithin(\"e\".\"geom\", ST_GeomFromEWKB($1), $2)"));
        assert_eq!(rendered.binds[0].name, "probe");
        assert_eq!(rendered.binds[1].name, "radius");
    }

    #[test]
    fn query_ast_limit_uses_each_dialect_syntax() {
        let query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("e", "id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: true,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(7),
            row_offset: None,
            locking: None,
        };
        for (dialect, expected) in [
            (Dialect::Postgres, " LIMIT 7"),
            (Dialect::Mysql, " LIMIT 7"),
            (Dialect::Sqlite, " LIMIT 7"),
            (Dialect::Duckdb, " LIMIT 7"),
            (Dialect::Oracle, " FETCH FIRST 7 ROWS ONLY"),
            (Dialect::Db2, " FETCH FIRST 7 ROWS ONLY"),
        ] {
            let sql = Renderer::new(
                dialect,
                DialectCapabilities {
                    spatial_intersects: true,
                },
            )
            .render_query(&query)
            .expect("dialect query")
            .sql;
            assert!(sql.ends_with(expected), "{dialect:?}: {sql}");
        }
        let sql = Renderer::new(
            Dialect::SqlServer,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("SQL Server query")
        .sql;
        assert!(sql.starts_with("SELECT DISTINCT TOP (7) "), "{sql}");
        assert!(!sql.contains(" LIMIT "));
    }

    #[test]
    fn oracle_and_db2_convert_wkb_before_spatial_predicates() {
        let select = Select {
            source: source(),
            projection: vec![identifier("id")],
            filter: Some(Expression::SpatialIntersects {
                field: identifier("geom"),
                wkb_parameter: "probe".to_owned(),
            }),
            order_by: Vec::new(),
            limit: None,
        };
        let oracle = Renderer::new(
            Dialect::Oracle,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_select(&select)
        .expect("Oracle spatial")
        .sql;
        assert!(oracle.contains("SDO_UTIL.FROM_WKBGEOMETRY(:1)"));
        let db2 = Renderer::new(
            Dialect::Db2,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_select(&select)
        .expect("Db2 spatial")
        .sql;
        assert!(db2.contains("DB2GSE.ST_GeomFromWKB(?, 0)"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn query_ast_renders_cte_join_group_having_and_stable_binds() {
        let count_id = QueryExpression::Scalar {
            function: ScalarFunction::Count,
            arguments: vec![query_column("f", "id")],
        };
        let cte = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: query_column("e", "id"),
                    alias: None,
                },
                QueryProjection {
                    expression: query_column("e", "owner_id"),
                    alias: None,
                },
            ],
            joins: Vec::new(),
            filter: Some(QueryExpression::Compare {
                left: Box::new(query_column("e", "id")),
                operator: ComparisonOperator::Gt,
                right: Box::new(QueryExpression::Parameter {
                    name: "minimum_id".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        };
        let query = QueryOperation {
            common_table_expressions: vec![CommonTableExpression {
                name: "filtered".to_owned(),
                recursive: false,
                query: Box::new(cte),
            }],
            source: Some(query_source("filtered", "f")),
            derived_source: None,
            projection: vec![
                QueryProjection {
                    expression: query_column("o", "name"),
                    alias: Some("owner".to_owned()),
                },
                QueryProjection {
                    expression: count_id.clone(),
                    alias: Some("events".to_owned()),
                },
            ],
            joins: vec![QueryJoin {
                kind: JoinKind::Inner,
                source: Some(query_source("owners", "o")),
                derived_source: None,
                lateral: false,
                on: Some(QueryExpression::Compare {
                    left: Box::new(query_column("f", "owner_id")),
                    operator: ComparisonOperator::Eq,
                    right: Box::new(query_column("o", "id")),
                }),
            }],
            filter: None,
            group_by: vec![query_column("o", "name")],
            having: Some(QueryExpression::Compare {
                left: Box::new(count_id.clone()),
                operator: ComparisonOperator::Gte,
                right: Box::new(QueryExpression::Parameter {
                    name: "minimum_count".to_owned(),
                }),
            }),
            order_by: vec![QueryOrdering {
                expression: count_id,
                direction: SortDirection::Desc,
            }],
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(25),
            row_offset: None,
            locking: None,
        };
        let rendered = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("query render");
        assert_eq!(
            rendered.sql,
            "WITH \"filtered\" AS (SELECT \"e\".\"id\", \"e\".\"owner_id\" FROM \"events\" AS \"e\" WHERE \"e\".\"id\" > $1) SELECT \"o\".\"name\" AS \"owner\", COUNT(\"f\".\"id\") AS \"events\" FROM \"filtered\" AS \"f\" INNER JOIN \"owners\" AS \"o\" ON \"f\".\"owner_id\" = \"o\".\"id\" GROUP BY \"o\".\"name\" HAVING COUNT(\"f\".\"id\") >= $2 ORDER BY COUNT(\"f\".\"id\") DESC LIMIT 25"
        );
        assert_eq!(
            rendered
                .binds
                .iter()
                .map(|bind| bind.name.as_str())
                .collect::<Vec<_>>(),
            ["minimum_id", "minimum_count"]
        );
    }

    #[test]
    fn postgres_renders_index_aware_spatial_query() {
        let query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("e", "id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: Some(QueryExpression::SpatialOperator {
                operator: SpatialOperator::BoundingBoxIntersects,
                left: Box::new(query_column("e", "geom")),
                right: Box::new(QueryExpression::Parameter {
                    name: "probe".to_owned(),
                }),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: QueryExpression::SpatialOperator {
                    operator: SpatialOperator::KnnDistance,
                    left: Box::new(query_column("e", "geom")),
                    right: Box::new(QueryExpression::Parameter {
                        name: "probe".to_owned(),
                    }),
                },
                direction: SortDirection::Asc,
            }],
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(5),
            row_offset: None,
            locking: None,
        };
        let rendered = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("index-aware spatial query");
        assert!(rendered
            .sql
            .contains("\"e\".\"geom\" && ST_GeomFromEWKB($1)"));
        assert!(rendered
            .sql
            .contains("\"e\".\"geom\" <-> ST_GeomFromEWKB($2) ASC"));
        assert_eq!(
            rendered
                .binds
                .iter()
                .map(|bind| bind.name.as_str())
                .collect::<Vec<_>>(),
            ["probe", "probe"]
        );
    }

    #[test]
    fn postgres_renders_spatial_clustering_as_a_window() {
        let query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: QueryExpression::SpatialWindow {
                    function: SpatialFunction::ClusterDbscan,
                    arguments: vec![
                        query_column("e", "geom"),
                        QueryExpression::Parameter {
                            name: "epsilon".to_owned(),
                        },
                        QueryExpression::Parameter {
                            name: "minimum_points".to_owned(),
                        },
                    ],
                    partition_by: vec![query_column("e", "region_id")],
                    order_by: Vec::new(),
                    frame: None,
                },
                alias: Some("cluster_id".to_owned()),
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        };
        let sql = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("spatial clustering")
        .sql;
        assert!(sql.contains(
            "ST_ClusterDBSCAN(\"e\".\"geom\", $1, $2) OVER \
             (PARTITION BY \"e\".\"region_id\")"
        ));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn postgres_renders_derived_window_lateral_pagination_and_locking() {
        let inner = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("e", "id"),
                alias: Some("id".to_owned()),
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        };
        let lateral = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("details", "x")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("x", "event_id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: Some(QueryExpression::Compare {
                left: Box::new(query_column("x", "event_id")),
                operator: ComparisonOperator::Eq,
                right: Box::new(query_column("d", "id")),
            }),
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(1),
            row_offset: None,
            locking: None,
        };
        let query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: None,
            derived_source: Some(QueryDerivedSource {
                query: Box::new(inner),
                alias: "d".to_owned(),
            }),
            projection: vec![
                QueryProjection {
                    expression: query_column("d", "id"),
                    alias: None,
                },
                QueryProjection {
                    expression: QueryExpression::Window {
                        function: ScalarFunction::RowNumber,
                        arguments: Vec::new(),
                        partition_by: Vec::new(),
                        order_by: vec![QueryOrdering {
                            expression: query_column("d", "id"),
                            direction: SortDirection::Asc,
                        }],
                        frame: None,
                    },
                    alias: Some("ordinal".to_owned()),
                },
            ],
            joins: vec![QueryJoin {
                kind: JoinKind::Cross,
                source: None,
                derived_source: Some(QueryDerivedSource {
                    query: Box::new(lateral),
                    alias: "latest".to_owned(),
                }),
                lateral: true,
                on: None,
            }],
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: vec![QueryOrdering {
                expression: query_column("d", "id"),
                direction: SortDirection::Asc,
            }],
            distinct: false,
            distinct_on: vec![query_column("d", "id")],
            set_operations: Vec::new(),
            row_limit: Some(10),
            row_offset: Some(5),
            locking: None,
        };
        let sql = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("advanced PostgreSQL query")
        .sql;
        assert!(sql.contains("DISTINCT ON (\"d\".\"id\")"));
        assert!(sql.contains("ROW_NUMBER() OVER (ORDER BY \"d\".\"id\" ASC)"));
        assert!(sql.contains("CROSS JOIN LATERAL (SELECT"));
        assert!(sql.ends_with("LIMIT 10 OFFSET 5"));

        let mut locking_query = QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source("events", "e")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("e", "id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: Some(1),
            row_offset: None,
            locking: None,
        };
        locking_query.locking = Some(QueryLock {
            strength: QueryLockStrength::Share,
            relations: vec!["e".to_owned()],
            wait: QueryLockWait::SkipLocked,
        });
        let locking_sql = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&locking_query)
        .expect("locking PostgreSQL query")
        .sql;
        assert!(locking_sql.ends_with("LIMIT 1 FOR SHARE OF \"e\" SKIP LOCKED"));
    }

    #[test]
    fn postgres_renders_set_operations_and_recursive_cte() {
        let leaf = |table: &str, alias: &str| QueryOperation {
            common_table_expressions: Vec::new(),
            source: Some(query_source(table, alias)),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column(alias, "id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        };
        let mut cte_body = leaf("roots", "r");
        cte_body.set_operations.push(QuerySetOperation {
            operator: QuerySetOperator::Union,
            all: true,
            query: Box::new(leaf("tree", "t")),
        });
        let query = QueryOperation {
            common_table_expressions: vec![CommonTableExpression {
                name: "tree".to_owned(),
                recursive: true,
                query: Box::new(cte_body),
            }],
            source: Some(query_source("tree", "result")),
            derived_source: None,
            projection: vec![QueryProjection {
                expression: query_column("result", "id"),
                alias: None,
            }],
            joins: Vec::new(),
            filter: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            distinct: false,
            distinct_on: Vec::new(),
            set_operations: Vec::new(),
            row_limit: None,
            row_offset: None,
            locking: None,
        };
        let sql = Renderer::new(
            Dialect::Postgres,
            DialectCapabilities {
                spatial_intersects: true,
            },
        )
        .render_query(&query)
        .expect("recursive CTE")
        .sql;
        assert!(sql.starts_with("WITH RECURSIVE \"tree\" AS ("));
        assert!(sql.contains(" UNION ALL (SELECT "));
    }
}
