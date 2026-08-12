//! Compilatore SQL per il `PortableStatement`. Attualmente supporta solo
//! `PostgreSQL`; altri provider verranno aggiunti in Fase 2 (parity).

use super::{
    DeleteStatement, Direction, Expression, InsertStatement, Nulls, OrderBy, PortableStatement,
    Predicate, Projection, SelectStatement, TableRef, UpdateStatement, UpsertStatement,
};
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::spatial_predicate::{SpatialPredicate, SpatialReference};
use crate::transaction::Statement;
use crate::{DatabaseError, Result};
use std::fmt::Write as _;

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

