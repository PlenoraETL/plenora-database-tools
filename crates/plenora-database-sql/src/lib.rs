//! SQL portabile: identificatori, AST, placeholder e rendering deterministico.
//!
//! L'AST non contiene valori. Il risultato associa il testo SQL ai nomi dei
//! parametri che il driver dovrà bindare.

use plenora_database_core::geometry::SpatialSemantics;
use plenora_database_core::plan::{ComparisonOperator, SortDirection};
use plenora_database_core::query::SpatialFunction;
use plenora_database_core::query::{
    validate_query_operation, JoinKind, QueryDerivedSource, QueryExpression, QueryLock,
    QueryLockStrength, QueryLockWait, QueryOperation, QuerySetOperator, QuerySource,
    ScalarFunction, SpatialOperator, WindowFrame, WindowFrameBound, WindowFrameUnits,
};
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

const SQL_SERVER_MAX_IDENTIFIER_CHARS: usize = 128;
const SQL_SERVER_MAX_BIND_PARAMETERS: usize = 2_100;

mod filter;
pub use filter::{
    lower_filter, select_columns_by_name, FilterLowering, CASE_INSENSITIVE_LIKE_REFUSAL,
    SPATIAL_FILTER_REFUSAL,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Identifier(String);

impl Identifier {
    /// Costruisce un identificatore validato ma non ancora quotato.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` per stringa vuota, NUL o oltre 256 caratteri.
    ///
    /// Il limite conta code point come `maxLength` di JSON Schema, non byte
    /// UTF-8. I renderer applicano poi il limite piu stretto del dialetto.
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.contains('\0') {
            return Err(DatabaseError::invalid_plan(
                "identificatore SQL vuoto o contenente NUL",
            ));
        }
        if value.chars().count() > 256 {
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

impl Dialect {
    /// Mappa il dialetto ricco del renderer sul dialetto ridotto usato
    /// da `plenora-database-core::identifier`. Oracle/Db2/Sqlite/Duckdb
    /// usano il quoting SQL standard (double-quote) come Postgres.
    #[must_use]
    const fn to_identifier_dialect(self) -> plenora_database_core::identifier::IdentifierDialect {
        use plenora_database_core::identifier::IdentifierDialect as D;
        match self {
            Self::Postgres | Self::Oracle | Self::Db2 | Self::Sqlite | Self::Duckdb => D::Postgres,
            Self::Mysql => D::Mysql,
            Self::SqlServer => D::SqlServer,
        }
    }
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

/// Parti di una query nativa separabili senza analizzare il testo SQL.
///
/// Serve ai preflight provider che devono avvolgere il corpo in una derived
/// table mantenendo un'eventuale clausola `WITH` al livello sintattico valido.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedQueryParts {
    pub with_clause: String,
    pub body: String,
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
    /// La semantica di ogni colonna spatial che il piano nomina.
    ///
    /// Serve a una cosa sola e la fa per tutte: `geometry` e `geography` non
    /// espongono sempre lo stesso membro, e le coordinate di un punto si
    /// leggono `STX`/`STY` sulla prima e `Long`/`Lat` sulla seconda. Senza
    /// questa mappa il renderer poteva scrivere solo i membri che i due tipi
    /// chiamano allo stesso modo.
    ///
    /// La popola il preflight del provider, che la legge dal catalogo. Il
    /// renderer non la deduce e non la indovina: una colonna che non e qui e
    /// una colonna di cui non si sa la semantica, e le funzioni che ne hanno
    /// bisogno vengono rifiutate.
    sql_server_spatial_columns: BTreeMap<(Option<String>, String), SpatialSemantics>,
}

impl Renderer {
    #[must_use]
    pub const fn new(dialect: Dialect, capabilities: DialectCapabilities) -> Self {
        Self {
            dialect,
            capabilities,
            sql_server_spatial_parameters: BTreeMap::new(),
            sql_server_spatial_columns: BTreeMap::new(),
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

    /// La semantica delle colonne spatial, letta dal catalogo dal chiamante.
    ///
    /// # Errors
    ///
    /// Nessuno: un renderer senza questa mappa e un renderer che rifiutera le
    /// funzioni il cui membro T-SQL dipende dalla semantica, e il rifiuto e
    /// esplicito.
    #[must_use]
    pub fn with_sql_server_spatial_columns(
        mut self,
        columns: BTreeMap<(Option<String>, String), SpatialSemantics>,
    ) -> Self {
        self.sql_server_spatial_columns = columns;
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
        let projection_sql: Result<Vec<String>> = select
            .projection
            .iter()
            .map(|field| self.quote(field))
            .collect();
        sql.push_str(&projection_sql?.join(", "));
        sql.push_str(" FROM ");
        sql.push_str(&self.render_object(&select.source)?);

        let mut binds = Vec::new();
        if let Some(filter) = &select.filter {
            sql.push_str(" WHERE ");
            sql.push_str(&self.render_expression(filter, &mut binds)?);
        }
        if !select.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_parts: Result<Vec<String>> = select
                .order_by
                .iter()
                .map(|order| {
                    let direction = match order.direction {
                        SortDirection::Asc => "ASC",
                        SortDirection::Desc => "DESC",
                    };
                    Ok(format!("{} {direction}", self.quote(&order.field)?))
                })
                .collect();
            sql.push_str(&order_parts?.join(", "));
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

    /// Renderizza separatamente la clausola `WITH` e il corpo della query.
    ///
    /// SQL Server non ammette `WITH` direttamente dentro una derived table.
    /// Restituire parti strutturali evita parsing o riscrittura lessicale del
    /// SQL già renderizzato nei preflight spatial del provider.
    ///
    /// # Errors
    ///
    /// Applica gli stessi limiti e controlli di
    /// [`Self::render_query_native_spatial`].
    pub fn render_query_native_spatial_parts(
        &self,
        query: &QueryOperation,
    ) -> Result<RenderedQueryParts> {
        let mut limits = plenora_database_core::limits::Limits::default();
        if self.dialect == Dialect::SqlServer {
            limits.max_identifier_bytes = SQL_SERVER_MAX_IDENTIFIER_CHARS;
        }
        validate_query_operation(query, &limits)?;

        let mut binds = Vec::new();
        let mut with_clause = String::new();
        if !query.common_table_expressions.is_empty() {
            with_clause.push_str("WITH ");
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
                    with_clause.push_str("RECURSIVE ");
                }
            }
            let ctes = query
                .common_table_expressions
                .iter()
                .map(|cte| {
                    let name = self.quote(&Identifier::new(cte.name.clone())?)?;
                    let body = self.render_query_inner(&cte.query, &mut binds, false)?;
                    Ok(format!("{name} AS ({body})"))
                })
                .collect::<Result<Vec<_>>>()?;
            with_clause.push_str(&ctes.join(", "));
            with_clause.push(' ');
        }

        let mut body_query = query.clone();
        body_query.common_table_expressions.clear();
        let body = self.render_query_inner(&body_query, &mut binds, false)?;
        self.validate_bind_count(&binds)?;
        Ok(RenderedQueryParts {
            with_clause,
            body,
            binds,
        })
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
                    let name = self.quote(&Identifier::new(cte.name.clone())?)?;
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
                        // `ST_AsBinary` di `MySQL` produce WKB **senza** SRID,
                        // a differenza di `ST_AsEWKB`: qui l'involucro rende
                        // trasportabile il valore e non porta il frame. Chi lo
                        // riceve deve saperlo da altrove — dalla regola di CRS
                        // della funzione e dal CRS dichiarato per la colonna
                        // d'ingresso — e il provider rifiuta la geometria
                        // quando quelle due cose non bastano.
                        //
                        // La stessa forma incapsula gia le colonne geometriche
                        // sul path di lettura, e per la stessa ragione: senza
                        // involucro il valore arriva come `MYSQL_TYPE_GEOMETRY`
                        // nel formato interno del prodotto, che non e WKB.
                        Dialect::Mysql => format!("ST_AsBinary({rendered})"),
                        _ => rendered,
                    };
                }
                if let Some(alias) = &item.alias {
                    rendered.push_str(" AS ");
                    rendered.push_str(&self.quote(&Identifier::new(alias.clone())?)?);
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
            query.locking.as_ref(),
            binds,
        )?);
        for join in &query.joins {
            let sql_server_apply = self.dialect == Dialect::SqlServer && join.lateral;
            let keyword = match (&join.kind, sql_server_apply) {
                (JoinKind::Cross, true) => " CROSS APPLY ",
                (JoinKind::Inner, false) => " INNER JOIN ",
                (JoinKind::Left, false) => " LEFT JOIN ",
                (JoinKind::Right, false) => " RIGHT JOIN ",
                (JoinKind::Full, false) => " FULL JOIN ",
                (JoinKind::Cross, false) => " CROSS JOIN ",
                _ => {
                    return Err(DatabaseError::unsupported(
                        self.provider_kind(),
                        ErrorPhase::Prepare,
                        "SQL Server qualifica soltanto CROSS APPLY per lateral v1",
                    ))
                }
            };
            sql.push_str(keyword);
            sql.push_str(&self.render_query_relation(
                join.source.as_ref(),
                join.derived_source.as_ref(),
                join.lateral,
                query.locking.as_ref(),
                binds,
            )?);
            if join.kind == JoinKind::Cross || sql_server_apply {
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
            if self.dialect == Dialect::SqlServer {
                self.validate_sql_server_lock_targets(query, locking)?;
            } else if self.dialect != Dialect::Postgres {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "locking avanzato supportato solo da PostgreSQL",
                ));
            }
            if self.dialect == Dialect::Postgres {
                sql.push_str(match locking.strength {
                    QueryLockStrength::Update => " FOR UPDATE",
                    QueryLockStrength::NoKeyUpdate => " FOR NO KEY UPDATE",
                    QueryLockStrength::Share => " FOR SHARE",
                    QueryLockStrength::KeyShare => " FOR KEY SHARE",
                });
                if !locking.relations.is_empty() {
                    sql.push_str(" OF ");
                    let relations: Result<Vec<String>> = locking
                        .relations
                        .iter()
                        .map(|relation| {
                            let ident = Identifier::new(relation.clone())?;
                            self.quote(&ident)
                        })
                        .collect();
                    sql.push_str(&relations?.join(", "));
                }
                match locking.wait {
                    QueryLockWait::Wait => {}
                    QueryLockWait::NoWait => sql.push_str(" NOWAIT"),
                    QueryLockWait::SkipLocked => sql.push_str(" SKIP LOCKED"),
                }
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
        })?;
        if let Some(alias) = &source.alias {
            value.push_str(" AS ");
            value.push_str(&self.quote(&Identifier::new(alias.clone())?)?);
        }
        Ok(value)
    }

    fn render_query_relation(
        &self,
        source: Option<&QuerySource>,
        derived: Option<&QueryDerivedSource>,
        lateral: bool,
        locking: Option<&QueryLock>,
        binds: &mut Vec<BindParameter>,
    ) -> Result<String> {
        match (source, derived) {
            (Some(source), None) => {
                if lateral {
                    return Err(DatabaseError::invalid_plan(
                        "LATERAL richiede una subquery come source",
                    ));
                }
                let mut relation = self.render_query_source(source)?;
                if let Some(hints) = self.sql_server_lock_hints(source, locking)? {
                    relation.push_str(" WITH (");
                    relation.push_str(&hints.join(", "));
                    relation.push(')');
                }
                Ok(relation)
            }
            (None, Some(derived)) => {
                if lateral
                    && self.dialect != Dialect::Postgres
                    && self.dialect != Dialect::SqlServer
                {
                    return Err(DatabaseError::unsupported(
                        self.provider_kind(),
                        ErrorPhase::Prepare,
                        "LATERAL supportato solo dal renderer PostgreSQL",
                    ));
                }
                let body = self.render_query_inner(&derived.query, binds, false)?;
                let alias = self.quote(&Identifier::new(derived.alias.clone())?)?;
                Ok(format!(
                    "{}({body}) AS {alias}",
                    if lateral && self.dialect == Dialect::Postgres {
                        "LATERAL "
                    } else {
                        ""
                    }
                ))
            }
            _ => Err(DatabaseError::invalid_plan(
                "query richiede una sola source, tabella o subquery",
            )),
        }
    }

    fn sql_server_lock_hints(
        &self,
        source: &QuerySource,
        locking: Option<&QueryLock>,
    ) -> Result<Option<Vec<&'static str>>> {
        if self.dialect != Dialect::SqlServer {
            return Ok(None);
        }
        let Some(locking) = locking else {
            return Ok(None);
        };
        let relation = source.alias.as_deref().unwrap_or(&source.object.object);
        if !locking.relations.is_empty()
            && !locking
                .relations
                .iter()
                .any(|candidate| candidate == relation)
        {
            return Ok(None);
        }
        let strength = match locking.strength {
            QueryLockStrength::Update => "UPDLOCK",
            QueryLockStrength::Share => "HOLDLOCK",
            QueryLockStrength::NoKeyUpdate | QueryLockStrength::KeyShare => {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "forza di locking PostgreSQL senza equivalente SQL Server esatto",
                ))
            }
        };
        let mut hints = vec![strength];
        match locking.wait {
            QueryLockWait::Wait => {}
            QueryLockWait::NoWait => hints.push("NOWAIT"),
            QueryLockWait::SkipLocked => {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "READPAST SQL Server non equivale a SKIP LOCKED in ogni isolamento",
                ))
            }
        }
        Ok(Some(hints))
    }

    fn validate_sql_server_lock_targets(
        &self,
        query: &QueryOperation,
        locking: &QueryLock,
    ) -> Result<()> {
        let mut physical = Vec::new();
        let mut derived = Vec::new();
        if let Some(source) = &query.source {
            physical.push(source.alias.as_deref().unwrap_or(&source.object.object));
        }
        if let Some(source) = &query.derived_source {
            derived.push(source.alias.as_str());
        }
        for join in &query.joins {
            if let Some(source) = &join.source {
                physical.push(source.alias.as_deref().unwrap_or(&source.object.object));
            }
            if let Some(source) = &join.derived_source {
                derived.push(source.alias.as_str());
            }
        }
        if locking.relations.is_empty() {
            if !derived.is_empty() {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "locking SQL Server senza target non puo includere derived source",
                ));
            }
            return Ok(());
        }
        for target in &locking.relations {
            if derived.iter().any(|relation| *relation == target) {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "table hint SQL Server non applicabile a una derived source",
                ));
            }
            if !physical.iter().any(|relation| *relation == target) {
                return Err(DatabaseError::invalid_plan(
                    "target locking SQL Server non presente fra le source fisiche",
                ));
            }
        }
        Ok(())
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
                        self.quote(&Identifier::new(relation.clone())?)?
                    ))
                } else {
                    Ok("*".to_owned())
                }
            }
            QueryExpression::Column { column } => {
                let field = self.quote(&Identifier::new(column.field.clone())?)?;
                if let Some(relation) = &column.relation {
                    Ok(format!(
                        "{}.{field}",
                        self.quote(&Identifier::new(relation.clone())?)?
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
                            return Err(DatabaseError::unsupported(
                                self.provider_kind(),
                                ErrorPhase::Prepare,
                                "parametro geometry Db2 richiede SRID dichiarato nel piano portable",
                            ));
                        }
                    })
                } else {
                    Ok(value)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(format!(
            "{}({})",
            dialect_spatial_name(self.dialect, function),
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
        // Nome e forma vengono dalla stessa tabella usata dalle capability.
        // La semantica del **ricevitore**, quando il piano la porta: e cio che
        // permette di scrivere le due coordinate, che i due tipi chiamano in
        // modo diverso.
        let receiver_semantics = match arguments.first() {
            Some(QueryExpression::Column { column }) => self
                .sql_server_spatial_columns
                .get(&(column.relation.clone(), column.field.clone()))
                .copied(),
            _ => None,
        };
        let Some((method, shape)) = sql_server_spatial_method(function, receiver_semantics) else {
            return Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "funzione spatial non disponibile nel sottoinsieme SQL Server verificato",
            ));
        };
        match shape {
            SqlServerSpatialShape::Property => Ok(format!("{receiver}.{method}")),
            SqlServerSpatialShape::Unary => unary(method),
            SqlServerSpatialShape::UnaryPredicate => unary_predicate(method),
            SqlServerSpatialShape::BinaryValue => binary(self, method, binds),
            SqlServerSpatialShape::BinaryPredicate => binary_predicate(self, method, binds),
            SqlServerSpatialShape::Numeric => numeric(self, method, binds),
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

    /// # Errors
    /// Restituisce `InvalidPlan` per identificatori vuoti, con
    /// caratteri di controllo o oltre il limite del dialetto.
    pub fn quote_identifier(&self, identifier: &Identifier) -> Result<String> {
        self.quote(identifier)
    }

    /// # Errors
    /// Come `quote_identifier` per ognuna delle componenti.
    pub fn quote_object(&self, object: &ObjectName) -> Result<String> {
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
                Ok(format!("{} {symbol} {placeholder}", self.quote(field)?))
            }
            Expression::IsNull(field) => Ok(format!("{} IS NULL", self.quote(field)?)),
            Expression::IsNotNull(field) => Ok(format!("{} IS NOT NULL", self.quote(field)?)),
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
                    self.quote(field)?,
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
                Ok(format!(
                    "{} BETWEEN {lower} AND {upper}",
                    self.quote(field)?
                ))
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
                Ok(format!("{} {operator} {placeholder}", self.quote(field)?))
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
                let quoted = self.quote(field)?;
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
                        return Err(DatabaseError::unsupported(
                            self.provider_kind(),
                            ErrorPhase::Prepare,
                            "spatial Db2 richiede SRID dichiarato nel piano portable",
                        ));
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
        if !self.capabilities.spatial_intersects {
            return Err(DatabaseError::unsupported(
                self.provider_kind(),
                ErrorPhase::Prepare,
                "AST spatial non abilitato per il dialect",
            ));
        }
        // Il profilo MySQL abilita soltanto il sottoinsieme verificato in
        // `plenora_db_mysql::query::VERIFIED_SPATIAL_FUNCTIONS`.
        // DWithin non è nativo MySQL (no ST_DWithin diretto); resta unsupported
        // finché non emergono profili che ne giustifichino l'emulazione via
        // ST_Distance + confronto scalare.
        match self.dialect {
            Dialect::Postgres | Dialect::Mysql => {}
            _ => {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "predicato spatial non supportato dal dialect",
                ));
            }
        }
        let quoted = self.quote(field)?;
        if function.is_unary_predicate() {
            return Ok(format!(
                "{}({quoted})",
                dialect_spatial_name(self.dialect, function)
            ));
        }
        let geometry_name = geometry_parameter.ok_or_else(|| {
            DatabaseError::invalid_plan("predicato spatial senza parametro geometria")
        })?;
        let geometry = self.bind(geometry_name, binds);
        // Postgres accetta EWKB (WKB con SRID embedded); MySQL usa WKB puro
        // + SRID come secondo argomento opzionale (non passiamo SRID —
        // il consumer deve fornire WKB con SRID già settato via ST_SRID).
        let right = match self.dialect {
            Dialect::Postgres => format!("ST_GeomFromEWKB({geometry})"),
            Dialect::Mysql => format!("ST_GeomFromWKB({geometry})"),
            _ => unreachable!("dialect check sopra"),
        };
        if function == SpatialFunction::DWithin {
            if self.dialect != Dialect::Postgres {
                return Err(DatabaseError::unsupported(
                    self.provider_kind(),
                    ErrorPhase::Prepare,
                    "d_within richiede Postgres/PostGIS (MySQL non ha ST_DWithin nativo)",
                ));
            }
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
        let name = dialect_spatial_name(self.dialect, function);
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

    fn quote(&self, identifier: &Identifier) -> Result<String> {
        // Il quoting delega alla fonte comune e propaga ogni rifiuto. Un
        // fallback produrrebbe SQL anche per identificatori oltre il limite,
        // caratteri di controllo o regole di quoting del dialetto sbagliato.
        plenora_database_core::identifier::quote_identifier(
            self.dialect.to_identifier_dialect(),
            identifier.as_str(),
        )
    }

    fn render_object(&self, object: &ObjectName) -> Result<String> {
        let parts: Result<Vec<String>> = object
            .catalog
            .iter()
            .chain(object.schema.iter())
            .chain(std::iter::once(&object.object))
            .map(|part| self.quote(part))
            .collect();
        Ok(parts?.join("."))
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

/// I nomi che `MySQL` scrive **diversamente** da `PostGIS`.
///
/// `spatial_name` non e la tabella dei nomi spatial: e la tabella dei nomi
/// **`PostGIS`**, e un test di questo modulo la verifica riga per riga contro
/// la colonna `postgres` del catalogo versionato. Ogni altro dialetto che la
/// usasse cosi com'e starebbe deducendo il proprio vocabolario da quello di un
/// altro prodotto — la deduzione per analogia che la regola 1 vieta.
///
/// Queste due righe non vengono dalla documentazione: vengono dal riferimento.
/// Interrogato con i ventisei nomi che il provider `MySQL` dichiara verified,
/// ha risposto `1305` — «`FUNCTION` does not exist» — a `ST_NDims` e a
/// `ST_NPoints`, e ha riconosciuto gli altri ventiquattro. `ST_Dimension` e
/// `ST_NumPoints` esistono, e sono i nomi che quel server da alle stesse due
/// domande.
///
/// La tabella e volutamente **corta**: non traduce le funzioni che `MySQL` non
/// ha. Chiedere `ST_Covers` a `MySQL` deve continuare a fallire, perche
/// `MySQL` non ce l'ha; tradurne il nome lo farebbe fallire piu tardi e con un
/// errore peggiore.
const fn mysql_spatial_name(function: SpatialFunction) -> Option<&'static str> {
    match function {
        SpatialFunction::Dimensions => Some("ST_Dimension"),
        SpatialFunction::NPoints => Some("ST_NumPoints"),
        _ => None,
    }
}

const fn db2_spatial_name(function: SpatialFunction) -> Option<&'static str> {
    match function {
        SpatialFunction::Dimensions => Some("ST_COORDDIM"),
        _ => None,
    }
}

/// La forma con cui un membro T-SQL si scrive.
///
/// Su ogni altro dialetto una funzione spatial e una chiamata e il nome basta a
/// comporla. Qui `geometry` e `geography` sono tipi CLR e la funzione e un
/// **membro del valore**, che si scrive in modi diversi: `g.STSrid` non ha
/// parentesi, `g.STIsValid()` rende un bit da confrontare, `g.Reduce(1)` vuole
/// un numero. La forma e percio una proprieta del metodo T-SQL, non una cosa
/// che si deduca dal contratto.
///
/// # Perche non si deduce
///
/// Ci si e provato, e il tentativo e durato quanto una prova: l'arieta del
/// contratto sembrava bastare — due argomenti di cui il secondo non geometrico
/// vuol dire «numerico» — e `ST_IsValid` l'ha smentito. Il contratto ne ammette
/// due, perche `PostGIS` accetta un secondo argomento di flag; `STIsValid()` di
/// T-SQL non ne prende nessuno. Cio che il **piano** puo esprimere e cio che il
/// **metodo** accetta sono due cose diverse, e confonderle avrebbe reso
/// `shape.STIsValid(@p1)`, valido per il renderer e rifiutato dal server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlServerSpatialShape {
    /// `g.Nome` — una proprieta: nessuna parentesi.
    Property,
    /// `g.Nome()`.
    Unary,
    /// `g.Nome()`, e il risultato e un bit: in posizione di predicato va
    /// confrontato con 1.
    UnaryPredicate,
    /// `g.Nome(altra)`, con una seconda geometria.
    BinaryValue,
    /// `g.Nome(altra)` il cui risultato e un bit.
    BinaryPredicate,
    /// `g.Nome(numero)`.
    Numeric,
}

/// Il membro T-SQL che il renderer invoca per questa funzione, e la sua forma.
///
/// # Perche esiste
///
/// Il nome stava come letterale dentro ogni arma del match del renderer, e la
/// forma stava in **tre** elenchi scritti a mano nel crate del provider —
/// unarie, binarie, numeriche. Quattro fonti per la stessa firma, e nessuna che
/// incrociasse le altre: aprire `Overlaps` significava ricordarsene in tutte e
/// quattro, e il giorno in cui non ce se ne ricordava la funzione moriva in
/// prepare mentre la capability la offriva.
///
/// Qui la firma sta scritta una volta. Il renderer ne ricava come comporre la
/// chiamata, il provider come validare gli argomenti, e una sonda puo chiedere
/// **lo stesso nome** che verrebbe emesso — che e cio che permette di misurare
/// il prodotto invece di misurare le proprie convinzioni.
///
/// # `None` non significa «assente dal prodotto»
///
/// Significa che **questo renderer** non la scrive. SQL Server puo averla:
/// `STCentroid`, `STEnvelope` e `STBoundary` esistono su `geometry`, e nessuna
/// delle tre e qui. La differenza fra «il prodotto non ce l'ha» e «noi non la
/// scriviamo» e cio che il censimento misura, e sono due chiusure diverse — la
/// prima e un fatto, la seconda e lavoro.
#[must_use]
pub const fn sql_server_spatial_method(
    function: SpatialFunction,
    semantics: Option<SpatialSemantics>,
) -> Option<(&'static str, SqlServerSpatialShape)> {
    // Le due coordinate sono le sole a cambiare nome fra i due tipi, e senza
    // sapere quale sia non si puo scrivere ne l'uno ne l'altro. `None` qui non
    // significa «SQL Server non ce l'ha»: significa che il piano non dice su
    // quale semantica sta la colonna, e indovinare renderebbe SQL che il
    // server rifiuta su meta delle tabelle.
    if let SpatialFunction::X | SpatialFunction::Y = function {
        let Some(semantics) = semantics else {
            return None;
        };
        return Some((
            match (function, semantics) {
                (SpatialFunction::X, SpatialSemantics::Geometry) => "STX",
                (SpatialFunction::X, SpatialSemantics::Geography) => "Long",
                (SpatialFunction::Y, SpatialSemantics::Geometry) => "STY",
                (_, _) => "Lat",
            },
            SqlServerSpatialShape::Property,
        ));
    }
    Some(match function {
        SpatialFunction::GeometryType => ("STGeometryType", SqlServerSpatialShape::Unary),
        SpatialFunction::Srid => ("STSrid", SqlServerSpatialShape::Property),
        SpatialFunction::Dimensions => ("STDimension", SqlServerSpatialShape::Unary),
        SpatialFunction::NPoints => ("STNumPoints", SqlServerSpatialShape::Unary),
        SpatialFunction::IsEmpty => ("STIsEmpty", SqlServerSpatialShape::UnaryPredicate),
        SpatialFunction::IsValid => ("STIsValid", SqlServerSpatialShape::UnaryPredicate),
        SpatialFunction::IsClosed => ("STIsClosed", SqlServerSpatialShape::UnaryPredicate),
        SpatialFunction::Intersects => ("STIntersects", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Contains => ("STContains", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Within => ("STWithin", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Disjoint => ("STDisjoint", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Equals => ("STEquals", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Overlaps => ("STOverlaps", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Distance => ("STDistance", SqlServerSpatialShape::BinaryValue),
        SpatialFunction::Area => ("STArea", SqlServerSpatialShape::Unary),
        SpatialFunction::Length => ("STLength", SqlServerSpatialShape::Unary),
        SpatialFunction::StartPoint => ("STStartPoint", SqlServerSpatialShape::Unary),
        SpatialFunction::EndPoint => ("STEndPoint", SqlServerSpatialShape::Unary),
        SpatialFunction::PointN => ("STPointN", SqlServerSpatialShape::Numeric),
        SpatialFunction::Buffer => ("STBuffer", SqlServerSpatialShape::Numeric),
        SpatialFunction::Simplify => ("Reduce", SqlServerSpatialShape::Numeric),
        SpatialFunction::Intersection => ("STIntersection", SqlServerSpatialShape::BinaryValue),
        SpatialFunction::Difference => ("STDifference", SqlServerSpatialShape::BinaryValue),
        SpatialFunction::SymDifference => ("STSymDifference", SqlServerSpatialShape::BinaryValue),
        SpatialFunction::Union => ("STUnion", SqlServerSpatialShape::BinaryValue),
        SpatialFunction::ConvexHull => ("STConvexHull", SqlServerSpatialShape::Unary),
        SpatialFunction::MakeValid => ("MakeValid", SqlServerSpatialShape::Unary),
        // Le sette che esistono su `geometry` e non su `geography`. Il nome e
        // la forma sono gli stessi sui due tipi; cio che cambia e che
        // sull'altro il membro **non esiste**, e non e questa tabella a
        // saperlo — lo dice la capability, che le pubblica solo nella voce di
        // `geometry`.
        //
        // Il renderer le scrive comunque, e deve: rifiutarle anche qui sarebbe
        // la stessa decisione presa due volte, e la seconda e quella che
        // invecchia. Chi impedisce di chiamarle sul tipo sbagliato e il
        // preflight, che la semantica della colonna la legge dal catalogo.
        //
        // `STRelate` non e fra loro benche esista su `geometry`: il contratto
        // ammette `Relate` a **due** argomenti e T-SQL il pattern DE-9IM lo
        // pretende. E' la stessa regola che lo tiene fuori da `MariaDB`, che lo
        // vuole a tre — una funzione e qualificata quando lo e a ogni arieta
        // che il piano ammette.
        SpatialFunction::Centroid => ("STCentroid", SqlServerSpatialShape::Unary),
        SpatialFunction::Envelope => ("STEnvelope", SqlServerSpatialShape::Unary),
        SpatialFunction::Boundary => ("STBoundary", SqlServerSpatialShape::Unary),
        SpatialFunction::PointOnSurface => ("STPointOnSurface", SqlServerSpatialShape::Unary),
        SpatialFunction::IsSimple => ("STIsSimple", SqlServerSpatialShape::UnaryPredicate),
        SpatialFunction::Touches => ("STTouches", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Crosses => ("STCrosses", SqlServerSpatialShape::BinaryPredicate),
        SpatialFunction::Z => ("Z", SqlServerSpatialShape::Property),
        SpatialFunction::M => ("M", SqlServerSpatialShape::Property),
        _ => return None,
    })
}

/// Il nome che un dialetto da a una funzione spatial.
///
/// Pubblica perche una sonda possa chiedere al server **lo stesso** nome che il
/// renderer emetterebbe. Ricavarlo altrove — dal catalogo, o a mano — misurerebbe
/// una funzione che questo codice non scrive mai, ed e esattamente l'errore che
/// ha tenuto `ST_NDims` e `ST_NPoints` nella lista verified di `MySQL`: nomi
/// `PostGIS` che il server non ha, dedotti invece che letti da qui.
#[must_use]
pub const fn spatial_function_name(dialect: Dialect, function: SpatialFunction) -> &'static str {
    dialect_spatial_name(dialect, function)
}

/// Il nome che **questo** dialetto da alla funzione.
const fn dialect_spatial_name(dialect: Dialect, function: SpatialFunction) -> &'static str {
    match dialect {
        Dialect::Mysql => match mysql_spatial_name(function) {
            Some(name) => name,
            None => spatial_name(function),
        },
        Dialect::Db2 => match db2_spatial_name(function) {
            Some(name) => name,
            None => spatial_name(function),
        },
        _ => spatial_name(function),
    }
}

/// Il nome `PostGIS`. Vedi [`dialect_spatial_name`] prima di chiamarlo diretto.
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
#[path = "lib_tests.rs"]
mod tests;
