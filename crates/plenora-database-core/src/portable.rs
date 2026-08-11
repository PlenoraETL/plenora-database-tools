//! `PortableStatement` — AST portable per l'application plane OLTP.
//!
//! Il consumer PFM costruisce lo statement tramite l'AST (`select`, `insert`,
//! `update`, `delete`, `upsert`) e chiama [`compile_portable`] con il
//! `ProviderKind` corrente per ottenere uno `Statement` (SQL + parametri
//! positional) pronto da eseguire tramite `TransactionScope::execute` /
//! `query`.
//!
//! Il vantaggio è duplice:
//!
//! 1. **Zero SQL vendor-specific nel dominio PFM**: nessun `RETURNING`,
//!    `OUTPUT`, `ON CONFLICT`, `ON DUPLICATE KEY UPDATE` scritto a mano.
//! 2. **Governance uniforme**: la validazione (identificatori, keyword,
//!    bind safety) vive in un solo posto.
//!
//! Scope Fase 1: primitive minime CRUD + RETURNING. `JOIN`, `CTE`, window
//! functions, aggregation sono estensioni additive future.

use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::spatial_predicate::{SpatialPredicate, SpatialReference};
use crate::transaction::Statement;
use crate::{DatabaseError, Result};
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

// ============================================================================
//  AST
// ============================================================================

/// Riferimento a una tabella, opzionalmente qualificato dallo schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            schema: None,
            name: name.into(),
        }
    }

    #[must_use]
    pub fn qualified(schema: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            schema: Some(schema.into()),
            name: name.into(),
        }
    }
}

/// Espressione atomica: valore letterale (verrà bindato) o riferimento a
/// colonna del target.
#[allow(clippy::derive_partial_eq_without_eq)] // ParameterValue::F64 contiene f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Expression {
    Literal(ParameterValue),
    Column(String),
}

impl Expression {
    #[must_use]
    pub const fn literal(value: ParameterValue) -> Self {
        Self::Literal(value)
    }

    #[must_use]
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column(name.into())
    }
}

/// Ordinamento singolo (una colonna, una direzione, opzionale null-order).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderBy {
    pub column: String,
    pub direction: Direction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nulls: Option<Nulls>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Nulls {
    First,
    Last,
}

/// Predicato del WHERE clause. Tutti gli operatori sono bind-safe: il
/// consumer non può iniettare SQL, solo valori.
#[allow(clippy::derive_partial_eq_without_eq)] // Expression contiene f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Predicate {
    Eq { column: String, value: Expression },
    Ne { column: String, value: Expression },
    Lt { column: String, value: Expression },
    Lte { column: String, value: Expression },
    Gt { column: String, value: Expression },
    Gte { column: String, value: Expression },
    In { column: String, values: Vec<Expression> },
    Between { column: String, low: Expression, high: Expression },
    Like { column: String, pattern: Expression },
    IsNull { column: String },
    IsNotNull { column: String },
    And { predicates: Vec<Self> },
    Or { predicates: Vec<Self> },
    Not { predicate: Box<Self> },
    /// Predicato spaziale su una colonna geometry. La geometria di
    /// riferimento viene bindata come `bytea` (EWKB) e rehydratata
    /// server-side (`ST_GeomFromEWKB($n)::geometry` su `PostGIS`).
    Spatial {
        column: String,
        predicate: SpatialPredicate,
        reference: SpatialReference,
    },
}

/// Costruttore fluente per un predicato spaziale.
#[must_use]
pub fn spatial(
    column: impl Into<String>,
    predicate: SpatialPredicate,
    reference: SpatialReference,
) -> Predicate {
    Predicate::Spatial {
        column: column.into(),
        predicate,
        reference,
    }
}

/// Projection: tutte le colonne o lista esplicita.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Projection {
    All,
    Columns(Vec<String>),
}

#[allow(clippy::derive_partial_eq_without_eq)] // filter contiene Expression con f64
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectStatement {
    pub table: TableRef,
    pub projection: Projection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<OrderBy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InsertStatement {
    pub table: TableRef,
    pub columns: Vec<String>,
    /// Vec di righe; ogni riga è Vec di espressioni allineato a `columns`.
    pub values: Vec<Vec<Expression>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateStatement {
    pub table: TableRef,
    /// SET column = expression. Ordine preservato.
    pub assignments: Vec<(String, Expression)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteStatement {
    pub table: TableRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Predicate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// UPSERT (INSERT + on-conflict update). `conflict_target` è la chiave che
/// definisce il conflict (tipicamente PK o unique key).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertStatement {
    pub table: TableRef,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>,
    pub conflict_target: Vec<String>,
    /// Assignments applicati in caso di conflict; vuoto = ON CONFLICT DO NOTHING.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub update_on_conflict: Vec<(String, Expression)>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub returning: Vec<String>,
}

/// Nodo top-level dell'AST portable.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PortableStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Upsert(UpsertStatement),
}

// ============================================================================
//  Builder API
// ============================================================================

/// Costruisce un `SelectStatement` con `Projection::All`.
#[must_use]
pub fn select_all(table: impl Into<String>) -> SelectStatement {
    SelectStatement {
        table: TableRef::new(table),
        projection: Projection::All,
        filter: None,
        order_by: Vec::new(),
        limit: None,
    }
}

/// Costruisce un `SelectStatement` con projection esplicita.
#[must_use]
pub fn select(table: impl Into<String>, columns: Vec<&str>) -> SelectStatement {
    SelectStatement {
        table: TableRef::new(table),
        projection: Projection::Columns(columns.into_iter().map(String::from).collect()),
        filter: None,
        order_by: Vec::new(),
        limit: None,
    }
}

impl SelectStatement {
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.table.schema = Some(schema.into());
        self
    }

    #[must_use]
    pub fn where_(mut self, predicate: Predicate) -> Self {
        self.filter = Some(predicate);
        self
    }

    #[must_use]
    pub fn order_by(mut self, column: impl Into<String>, direction: Direction) -> Self {
        self.order_by.push(OrderBy {
            column: column.into(),
            direction,
            nulls: None,
        });
        self
    }

    #[must_use]
    pub const fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    #[must_use]
    pub const fn into_statement(self) -> PortableStatement {
        PortableStatement::Select(self)
    }
}

/// Predicato di uguaglianza tra colonna e valore letterale.
#[must_use]
pub fn eq(column: impl Into<String>, value: ParameterValue) -> Predicate {
    Predicate::Eq {
        column: column.into(),
        value: Expression::literal(value),
    }
}

#[must_use]
pub const fn and(predicates: Vec<Predicate>) -> Predicate {
    Predicate::And { predicates }
}

#[must_use]
pub const fn or(predicates: Vec<Predicate>) -> Predicate {
    Predicate::Or { predicates }
}

// ============================================================================
//  Compilazione
// ============================================================================

/// Compila un `PortableStatement` per il provider indicato.
///
/// # Errors
///
/// - `Unsupported` se il provider non è supportato dal compilatore
/// - `InvalidPlan` se lo statement viola un vincolo (columns vuoto,
///   values shape mismatch, identificatori non validi, ecc.)
pub fn compile_portable(
    kind: ProviderKind,
    statement: &PortableStatement,
) -> Result<Statement> {
    match kind {
        ProviderKind::Postgres => compile_postgres(statement),
        other => Err(DatabaseError::unsupported(
            other,
            crate::ErrorPhase::Prepare,
            format!("compile_portable non supportato per {other:?}"),
        )),
    }
}

/// Compilatore per `PostgreSQL`.
fn compile_postgres(statement: &PortableStatement) -> Result<Statement> {
    let mut ctx = CompileContext::new();
    let sql = match statement {
        PortableStatement::Select(s) => compile_select(s, &mut ctx)?,
        PortableStatement::Insert(s) => compile_insert(s, &mut ctx)?,
        PortableStatement::Update(s) => compile_update(s, &mut ctx)?,
        PortableStatement::Delete(s) => compile_delete(s, &mut ctx)?,
        PortableStatement::Upsert(s) => compile_upsert(s, &mut ctx)?,
    };
    Ok(Statement {
        sql,
        params: ctx.params,
    })
}

struct CompileContext {
    params: Vec<ParameterValue>,
}

impl CompileContext {
    const fn new() -> Self {
        Self { params: Vec::new() }
    }

    /// Registra un parametro e ritorna il placeholder `$N` (1-based).
    fn bind(&mut self, value: ParameterValue) -> String {
        self.params.push(value);
        format!("${}", self.params.len())
    }
}

// ---- Helpers ----------------------------------------------------------------

fn quote_identifier(name: &str) -> Result<String> {
    validate_identifier(name)?;
    let escaped = name.replace('"', "\"\"");
    Ok(format!("\"{escaped}\""))
}

fn validate_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(DatabaseError::invalid_plan("identificatore vuoto"));
    }
    if name.len() > 63 {
        return Err(DatabaseError::invalid_plan(
            "identificatore eccede 63 caratteri",
        ));
    }
    if name.chars().any(char::is_control) {
        return Err(DatabaseError::invalid_plan(
            "identificatore contiene caratteri di controllo",
        ));
    }
    Ok(())
}

fn qualify_table(table: &TableRef) -> Result<String> {
    let table_id = quote_identifier(&table.name)?;
    if let Some(schema) = &table.schema {
        let schema_id = quote_identifier(schema)?;
        Ok(format!("{schema_id}.{table_id}"))
    } else {
        Ok(table_id)
    }
}

fn compile_expression(expr: &Expression, ctx: &mut CompileContext) -> Result<String> {
    match expr {
        Expression::Literal(v) => Ok(ctx.bind(v.clone())),
        Expression::Column(name) => quote_identifier(name),
    }
}

fn compile_predicate(pred: &Predicate, ctx: &mut CompileContext) -> Result<String> {
    match pred {
        Predicate::Eq { column, value } => {
            let c = quote_identifier(column)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} = {v}"))
        }
        Predicate::Ne { column, value } => {
            let c = quote_identifier(column)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} <> {v}"))
        }
        Predicate::Lt { column, value } => {
            let c = quote_identifier(column)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} < {v}"))
        }
        Predicate::Lte { column, value } => {
            let c = quote_identifier(column)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} <= {v}"))
        }
        Predicate::Gt { column, value } => {
            let c = quote_identifier(column)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} > {v}"))
        }
        Predicate::Gte { column, value } => {
            let c = quote_identifier(column)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} >= {v}"))
        }
        Predicate::In { column, values } => {
            if values.is_empty() {
                return Err(DatabaseError::invalid_plan("IN richiede almeno un valore"));
            }
            let c = quote_identifier(column)?;
            let items: Result<Vec<_>> =
                values.iter().map(|e| compile_expression(e, ctx)).collect();
            let joined = items?.join(", ");
            Ok(format!("{c} IN ({joined})"))
        }
        Predicate::Between { column, low, high } => {
            let c = quote_identifier(column)?;
            let l = compile_expression(low, ctx)?;
            let h = compile_expression(high, ctx)?;
            Ok(format!("{c} BETWEEN {l} AND {h}"))
        }
        Predicate::Like { column, pattern } => {
            let c = quote_identifier(column)?;
            let p = compile_expression(pattern, ctx)?;
            Ok(format!("{c} LIKE {p}"))
        }
        Predicate::IsNull { column } => {
            let c = quote_identifier(column)?;
            Ok(format!("{c} IS NULL"))
        }
        Predicate::IsNotNull { column } => {
            let c = quote_identifier(column)?;
            Ok(format!("{c} IS NOT NULL"))
        }
        Predicate::And { predicates } => {
            if predicates.is_empty() {
                return Err(DatabaseError::invalid_plan("AND richiede almeno un predicato"));
            }
            let parts: Result<Vec<_>> = predicates
                .iter()
                .map(|p| compile_predicate(p, ctx))
                .collect();
            Ok(format!("({})", parts?.join(" AND ")))
        }
        Predicate::Or { predicates } => {
            if predicates.is_empty() {
                return Err(DatabaseError::invalid_plan("OR richiede almeno un predicato"));
            }
            let parts: Result<Vec<_>> = predicates
                .iter()
                .map(|p| compile_predicate(p, ctx))
                .collect();
            Ok(format!("({})", parts?.join(" OR ")))
        }
        Predicate::Not { predicate } => {
            let inner = compile_predicate(predicate, ctx)?;
            Ok(format!("NOT ({inner})"))
        }
        Predicate::Spatial {
            column,
            predicate,
            reference,
        } => compile_spatial(column, predicate, reference, ctx),
    }
}

fn compile_spatial(
    column: &str,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
    ctx: &mut CompileContext,
) -> Result<String> {
    let col = quote_identifier(column)?;
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let geom_expr = format!("ST_GeomFromEWKB({geom_placeholder})::geometry");
    match predicate {
        SpatialPredicate::Intersects => Ok(format!("ST_Intersects({col}, {geom_expr})")),
        SpatialPredicate::Contains => Ok(format!("ST_Contains({col}, {geom_expr})")),
        SpatialPredicate::Within => Ok(format!("ST_Within({col}, {geom_expr})")),
        SpatialPredicate::BoundingBox => Ok(format!("{col} && {geom_expr}")),
        SpatialPredicate::DWithin { distance_meters } => {
            if !distance_meters.is_finite() || *distance_meters < 0.0 {
                return Err(DatabaseError::invalid_plan(
                    "DWithin richiede distanza finita non-negativa",
                ));
            }
            let dist_placeholder = ctx.bind(ParameterValue::F64(*distance_meters));
            Ok(format!(
                "ST_DWithin({col}, {geom_expr}, {dist_placeholder})"
            ))
        }
    }
}

fn compile_projection(projection: &Projection) -> Result<String> {
    match projection {
        Projection::All => Ok("*".to_owned()),
        Projection::Columns(cols) => {
            if cols.is_empty() {
                return Err(DatabaseError::invalid_plan(
                    "projection esplicita non può essere vuota",
                ));
            }
            let quoted: Result<Vec<_>> = cols.iter().map(|c| quote_identifier(c)).collect();
            Ok(quoted?.join(", "))
        }
    }
}

fn compile_order_by(order_by: &[OrderBy]) -> Result<String> {
    let parts: Result<Vec<_>> = order_by
        .iter()
        .map(|o| {
            let col = quote_identifier(&o.column)?;
            let dir = match o.direction {
                Direction::Asc => "ASC",
                Direction::Desc => "DESC",
            };
            let mut clause = format!("{col} {dir}");
            if let Some(nulls) = o.nulls {
                clause.push_str(match nulls {
                    Nulls::First => " NULLS FIRST",
                    Nulls::Last => " NULLS LAST",
                });
            }
            Ok(clause)
        })
        .collect();
    Ok(parts?.join(", "))
}

fn compile_returning(returning: &[String]) -> Result<String> {
    if returning.is_empty() {
        return Ok(String::new());
    }
    let cols: Result<Vec<_>> = returning.iter().map(|c| quote_identifier(c)).collect();
    Ok(format!(" RETURNING {}", cols?.join(", ")))
}

// ---- Statement compilers ---------------------------------------------------

fn compile_select(s: &SelectStatement, ctx: &mut CompileContext) -> Result<String> {
    let projection = compile_projection(&s.projection)?;
    let table = qualify_table(&s.table)?;
    let mut sql = format!("SELECT {projection} FROM {table}");
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    if !s.order_by.is_empty() {
        let ob = compile_order_by(&s.order_by)?;
        write!(sql, " ORDER BY {ob}").expect("write String");
    }
    if let Some(limit) = s.limit {
        write!(sql, " LIMIT {limit}").expect("write String");
    }
    Ok(sql)
}

fn compile_insert(s: &InsertStatement, ctx: &mut CompileContext) -> Result<String> {
    if s.columns.is_empty() {
        return Err(DatabaseError::invalid_plan("INSERT richiede almeno una colonna"));
    }
    if s.values.is_empty() {
        return Err(DatabaseError::invalid_plan("INSERT richiede almeno una riga"));
    }
    for (i, row) in s.values.iter().enumerate() {
        if row.len() != s.columns.len() {
            return Err(DatabaseError::invalid_plan(format!(
                "INSERT riga {i}: arity {} non allineata a colonne {}",
                row.len(),
                s.columns.len()
            )));
        }
    }
    let table = qualify_table(&s.table)?;
    let cols: Result<Vec<_>> = s.columns.iter().map(|c| quote_identifier(c)).collect();
    let cols_sql = cols?.join(", ");
    let rows: Result<Vec<String>> = s
        .values
        .iter()
        .map(|row| {
            let placeholders: Result<Vec<_>> =
                row.iter().map(|e| compile_expression(e, ctx)).collect();
            Ok(format!("({})", placeholders?.join(", ")))
        })
        .collect();
    let mut sql = format!("INSERT INTO {table} ({cols_sql}) VALUES {}", rows?.join(", "));
    sql.push_str(&compile_returning(&s.returning)?);
    Ok(sql)
}

fn compile_update(s: &UpdateStatement, ctx: &mut CompileContext) -> Result<String> {
    if s.assignments.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "UPDATE richiede almeno un assignment",
        ));
    }
    let table = qualify_table(&s.table)?;
    let sets: Result<Vec<_>> = s
        .assignments
        .iter()
        .map(|(col, expr)| {
            let c = quote_identifier(col)?;
            let e = compile_expression(expr, ctx)?;
            Ok(format!("{c} = {e}"))
        })
        .collect();
    let mut sql = format!("UPDATE {table} SET {}", sets?.join(", "));
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    sql.push_str(&compile_returning(&s.returning)?);
    Ok(sql)
}

fn compile_delete(s: &DeleteStatement, ctx: &mut CompileContext) -> Result<String> {
    let table = qualify_table(&s.table)?;
    let mut sql = format!("DELETE FROM {table}");
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    sql.push_str(&compile_returning(&s.returning)?);
    Ok(sql)
}

fn compile_upsert(s: &UpsertStatement, ctx: &mut CompileContext) -> Result<String> {
    if s.columns.is_empty() || s.values.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "UPSERT richiede colonne e valori",
        ));
    }
    if s.conflict_target.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "UPSERT richiede conflict_target non vuoto",
        ));
    }
    for (i, row) in s.values.iter().enumerate() {
        if row.len() != s.columns.len() {
            return Err(DatabaseError::invalid_plan(format!(
                "UPSERT riga {i}: arity {} non allineata a colonne {}",
                row.len(),
                s.columns.len()
            )));
        }
    }
    let table = qualify_table(&s.table)?;
    let cols: Result<Vec<_>> = s.columns.iter().map(|c| quote_identifier(c)).collect();
    let cols_sql = cols?.join(", ");
    let rows: Result<Vec<String>> = s
        .values
        .iter()
        .map(|row| {
            let placeholders: Result<Vec<_>> =
                row.iter().map(|e| compile_expression(e, ctx)).collect();
            Ok(format!("({})", placeholders?.join(", ")))
        })
        .collect();
    let conflict: Result<Vec<_>> = s
        .conflict_target
        .iter()
        .map(|c| quote_identifier(c))
        .collect();
    let conflict_sql = conflict?.join(", ");
    let mut sql = format!(
        "INSERT INTO {table} ({cols_sql}) VALUES {} ON CONFLICT ({conflict_sql})",
        rows?.join(", ")
    );
    if s.update_on_conflict.is_empty() {
        sql.push_str(" DO NOTHING");
    } else {
        let sets: Result<Vec<_>> = s
            .update_on_conflict
            .iter()
            .map(|(col, expr)| {
                let c = quote_identifier(col)?;
                let e = compile_expression(expr, ctx)?;
                Ok(format!("{c} = {e}"))
            })
            .collect();
        write!(sql, " DO UPDATE SET {}", sets?.join(", ")).expect("write String");
    }
    sql.push_str(&compile_returning(&s.returning)?);
    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_all_produces_select_star() {
        let stmt = select_all("users").into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(compiled.sql, r#"SELECT * FROM "users""#);
        assert!(compiled.params.is_empty());
    }

    #[test]
    fn select_columns_where_order_limit() {
        let stmt = select("users", vec!["id", "email"])
            .schema("app")
            .where_(eq("tenant_id", ParameterValue::I64(42)))
            .order_by("id", Direction::Asc)
            .limit(100)
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"SELECT "id", "email" FROM "app"."users" WHERE "tenant_id" = $1 ORDER BY "id" ASC LIMIT 100"#
        );
        assert_eq!(compiled.params, vec![ParameterValue::I64(42)]);
    }

    #[test]
    fn insert_binds_positional_and_returns() {
        let stmt = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("t"),
            columns: vec!["a".into(), "b".into()],
            values: vec![vec![
                Expression::literal(ParameterValue::I32(1)),
                Expression::literal(ParameterValue::String("x".into())),
            ]],
            returning: vec!["id".into()],
        });
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"INSERT INTO "t" ("a", "b") VALUES ($1, $2) RETURNING "id""#
        );
        assert_eq!(
            compiled.params,
            vec![ParameterValue::I32(1), ParameterValue::String("x".into())]
        );
    }

    #[test]
    fn update_with_where_and_returning() {
        let stmt = PortableStatement::Update(UpdateStatement {
            table: TableRef::new("work_order"),
            assignments: vec![
                (
                    "status".into(),
                    Expression::literal(ParameterValue::String("done".into())),
                ),
                (
                    "version".into(),
                    Expression::literal(ParameterValue::I64(18)),
                ),
            ],
            filter: Some(and(vec![
                eq("id", ParameterValue::I64(42)),
                eq("version", ParameterValue::I64(17)),
            ])),
            returning: vec!["version".into()],
        });
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"UPDATE "work_order" SET "status" = $1, "version" = $2 WHERE ("id" = $3 AND "version" = $4) RETURNING "version""#
        );
        assert_eq!(compiled.params.len(), 4);
    }

    #[test]
    fn delete_with_where() {
        let stmt = PortableStatement::Delete(DeleteStatement {
            table: TableRef::new("session"),
            filter: Some(eq("token", ParameterValue::String("abc".into()))),
            returning: Vec::new(),
        });
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"DELETE FROM "session" WHERE "token" = $1"#
        );
    }

    #[test]
    fn upsert_do_nothing() {
        let stmt = PortableStatement::Upsert(UpsertStatement {
            table: TableRef::new("cache"),
            columns: vec!["k".into(), "v".into()],
            values: vec![vec![
                Expression::literal(ParameterValue::String("x".into())),
                Expression::literal(ParameterValue::I32(1)),
            ]],
            conflict_target: vec!["k".into()],
            update_on_conflict: Vec::new(),
            returning: Vec::new(),
        });
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"INSERT INTO "cache" ("k", "v") VALUES ($1, $2) ON CONFLICT ("k") DO NOTHING"#
        );
    }

    #[test]
    fn upsert_do_update_set() {
        let stmt = PortableStatement::Upsert(UpsertStatement {
            table: TableRef::new("cache"),
            columns: vec!["k".into(), "v".into()],
            values: vec![vec![
                Expression::literal(ParameterValue::String("x".into())),
                Expression::literal(ParameterValue::I32(1)),
            ]],
            conflict_target: vec!["k".into()],
            update_on_conflict: vec![("v".into(), Expression::literal(ParameterValue::I32(2)))],
            returning: Vec::new(),
        });
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"INSERT INTO "cache" ("k", "v") VALUES ($1, $2) ON CONFLICT ("k") DO UPDATE SET "v" = $3"#
        );
    }

    #[test]
    fn predicates_compose_and_or_not() {
        let stmt = select("t", vec!["id"])
            .where_(and(vec![
                Predicate::Or {
                    predicates: vec![
                        eq("a", ParameterValue::I32(1)),
                        eq("b", ParameterValue::I32(2)),
                    ],
                },
                Predicate::Not {
                    predicate: Box::new(Predicate::IsNull {
                        column: "c".into(),
                    }),
                },
            ]))
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"SELECT "id" FROM "t" WHERE (("a" = $1 OR "b" = $2) AND NOT ("c" IS NULL))"#
        );
    }

    #[test]
    fn in_predicate_binds_each_value() {
        let stmt = select("t", vec!["id"])
            .where_(Predicate::In {
                column: "status".into(),
                values: vec![
                    Expression::literal(ParameterValue::String("a".into())),
                    Expression::literal(ParameterValue::String("b".into())),
                    Expression::literal(ParameterValue::String("c".into())),
                ],
            })
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert_eq!(
            compiled.sql,
            r#"SELECT "id" FROM "t" WHERE "status" IN ($1, $2, $3)"#
        );
        assert_eq!(compiled.params.len(), 3);
    }

    #[test]
    fn between_and_like() {
        let stmt = select("t", vec!["id"])
            .where_(and(vec![
                Predicate::Between {
                    column: "created".into(),
                    low: Expression::literal(ParameterValue::Date("2026-01-01".into())),
                    high: Expression::literal(ParameterValue::Date("2026-12-31".into())),
                },
                Predicate::Like {
                    column: "name".into(),
                    pattern: Expression::literal(ParameterValue::String("acme%".into())),
                },
            ]))
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert!(compiled.sql.contains("BETWEEN $1 AND $2"));
        assert!(compiled.sql.contains(r#""name" LIKE $3"#));
    }

    #[test]
    fn invalid_identifier_is_rejected() {
        let stmt = select("t\x00evil", vec!["id"]).into_statement();
        let err = compile_portable(ProviderKind::Postgres, &stmt).unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::InvalidPlan);
    }

    #[test]
    fn identifiers_are_quoted_and_double_quotes_escaped() {
        let stmt = select("evil\"table", vec![r#"c"ol"#]).into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert!(compiled.sql.contains(r#""evil""table""#));
        assert!(compiled.sql.contains(r#""c""ol""#));
    }

    #[test]
    fn unsupported_provider_returns_unsupported() {
        let stmt = select_all("t").into_statement();
        let err = compile_portable(ProviderKind::Mysql, &stmt).unwrap_err();
        assert_eq!(err.category, crate::ErrorCategory::Unsupported);
    }

    #[test]
    fn empty_in_predicate_is_rejected() {
        let stmt = select("t", vec!["id"])
            .where_(Predicate::In {
                column: "x".into(),
                values: Vec::new(),
            })
            .into_statement();
        assert!(compile_portable(ProviderKind::Postgres, &stmt).is_err());
    }

    #[test]
    fn spatial_predicate_intersects_binds_ewkb() {
        use crate::geometry::{Dimensions, SpatialSemantics};
        let stmt = select("buildings", vec!["id"])
            .where_(spatial(
                "geom",
                SpatialPredicate::Intersects,
                SpatialReference {
                    ewkb: vec![0x01, 0x02, 0x03],
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert!(
            compiled
                .sql
                .contains(r#"ST_Intersects("geom", ST_GeomFromEWKB($1)::geometry)"#),
            "sql inatteso: {}",
            compiled.sql
        );
        assert_eq!(compiled.params.len(), 1);
        assert!(matches!(&compiled.params[0], ParameterValue::Bytes(b) if b == &[0x01, 0x02, 0x03]));
    }

    #[test]
    fn spatial_predicate_dwithin_binds_distance() {
        use crate::geometry::{Dimensions, SpatialSemantics};
        let stmt = select("poi", vec!["id"])
            .where_(spatial(
                "geom",
                SpatialPredicate::DWithin {
                    distance_meters: 250.0,
                },
                SpatialReference {
                    ewkb: vec![0xff],
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert!(compiled
            .sql
            .contains(r#"ST_DWithin("geom", ST_GeomFromEWKB($1)::geometry, $2)"#));
        assert_eq!(compiled.params.len(), 2);
        assert!(matches!(&compiled.params[1], ParameterValue::F64(v) if *v == 250.0));
    }

    #[test]
    fn spatial_dwithin_negative_distance_is_rejected() {
        use crate::geometry::{Dimensions, SpatialSemantics};
        let stmt = select("t", vec!["id"])
            .where_(spatial(
                "geom",
                SpatialPredicate::DWithin {
                    distance_meters: -1.0,
                },
                SpatialReference {
                    ewkb: vec![0x00],
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();
        assert!(compile_portable(ProviderKind::Postgres, &stmt).is_err());
    }

    #[test]
    fn spatial_bounding_box_uses_index_operator() {
        use crate::geometry::{Dimensions, SpatialSemantics};
        let stmt = select("t", vec!["id"])
            .where_(spatial(
                "geom",
                SpatialPredicate::BoundingBox,
                SpatialReference {
                    ewkb: vec![0x01],
                    srid: 4326,
                    dimensions: Dimensions::Xy,
                    semantics: SpatialSemantics::Geometry,
                },
            ))
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert!(compiled.sql.contains(r#""geom" && ST_GeomFromEWKB"#));
    }

    #[test]
    fn spatial_composes_with_scalar_predicates() {
        use crate::geometry::{Dimensions, SpatialSemantics};
        // Filtro composto: bbox AND status = 'active'
        let stmt = select("buildings", vec!["id", "name"])
            .where_(and(vec![
                spatial(
                    "geom",
                    SpatialPredicate::Intersects,
                    SpatialReference {
                        ewkb: vec![0x0a, 0x0b],
                        srid: 4326,
                        dimensions: Dimensions::Xy,
                        semantics: SpatialSemantics::Geometry,
                    },
                ),
                eq("status", ParameterValue::String("active".into())),
            ]))
            .into_statement();
        let compiled = compile_portable(ProviderKind::Postgres, &stmt).unwrap();
        assert!(compiled.sql.contains("ST_Intersects"));
        assert!(compiled.sql.contains(r#""status" = $2"#));
        assert_eq!(compiled.params.len(), 2);
    }

    #[test]
    fn insert_arity_mismatch_is_rejected() {
        let stmt = PortableStatement::Insert(InsertStatement {
            table: TableRef::new("t"),
            columns: vec!["a".into(), "b".into()],
            values: vec![vec![Expression::literal(ParameterValue::I32(1))]],
            returning: Vec::new(),
        });
        assert!(compile_portable(ProviderKind::Postgres, &stmt).is_err());
    }
}
