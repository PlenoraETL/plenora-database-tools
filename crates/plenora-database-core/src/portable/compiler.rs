//! Compilatore SQL per il `PortableStatement`. Supporta `PostgreSQL` e
//! `MySQL` (parity essenziale — v1.2 base senza `RETURNING` / senza
//! spatial `DWithin`). Altri provider (`SQL Server`, `Oracle`) fail-closed.
//!
//! Design: unico compile pass con `DialectKind` dispatch sui punti dove
//! il dialect diverge (placeholder `$N` vs `?`, quoting `"` vs `` ` ``,
//! `ON CONFLICT` vs `ON DUPLICATE KEY UPDATE`, `RETURNING` vs no `RETURNING`,
//! spatial `ST_GeomFromEWKB` vs `ST_GeomFromWKB`). Le funzioni comuni
//! (`predicate`, `expression`, `projection`, `order_by`, ecc.) sono uniche.

use super::{
    DeleteStatement, Direction, Expression, InsertStatement, Nulls, OrderBy, PortableStatement,
    Predicate, Projection, SelectStatement, TableRef, UpdateStatement, UpsertStatement,
};
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
use crate::geometry::SpatialSemantics;
use crate::identifier::{self, IdentifierDialect};
use crate::spatial_policy;
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
    let dialect = match kind {
        ProviderKind::Postgres => DialectKind::Postgres,
        ProviderKind::Mysql => DialectKind::Mysql,
        other => {
            return Err(DatabaseError::unsupported(
                other,
                crate::ErrorPhase::Prepare,
                format!("compile_portable non supportato per {other:?}"),
            ));
        }
    };
    let mut ctx = CompileContext::new(dialect);
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum DialectKind {
    Postgres,
    Mysql,
}

struct CompileContext {
    params: Vec<ParameterValue>,
    dialect: DialectKind,
}

impl CompileContext {
    const fn new(dialect: DialectKind) -> Self {
        Self {
            params: Vec::new(),
            dialect,
        }
    }

    /// Registra un parametro e ritorna il placeholder dialect-specifico
    /// (`$N` per `Postgres` 1-based, `?` per `MySQL`).
    fn bind(&mut self, value: ParameterValue) -> String {
        self.params.push(value);
        match self.dialect {
            DialectKind::Postgres => format!("${}", self.params.len()),
            DialectKind::Mysql => "?".to_owned(),
        }
    }
}

// ---- Helpers ----------------------------------------------------------------

/// Delega a `plenora-database-core::identifier` (Fase A2). Prima
/// duplicava le stesse regole di `plenora-database-sql::Renderer::quote`
/// con rischio di divergenza.
fn quote_identifier(name: &str, dialect: DialectKind) -> Result<String> {
    identifier::quote_identifier(dialect.into(), name)
}

impl From<DialectKind> for IdentifierDialect {
    fn from(kind: DialectKind) -> Self {
        match kind {
            DialectKind::Postgres => Self::Postgres,
            DialectKind::Mysql => Self::Mysql,
        }
    }
}

fn qualify_table(table: &TableRef, dialect: DialectKind) -> Result<String> {
    let table_id = quote_identifier(&table.name, dialect)?;
    if let Some(schema) = &table.schema {
        let schema_id = quote_identifier(schema, dialect)?;
        Ok(format!("{schema_id}.{table_id}"))
    } else {
        Ok(table_id)
    }
}

fn compile_expression(expr: &Expression, ctx: &mut CompileContext) -> Result<String> {
    match expr {
        Expression::Literal(v) => Ok(ctx.bind(v.clone())),
        Expression::Column(name) => quote_identifier(name, ctx.dialect),
    }
}

fn compile_predicate(pred: &Predicate, ctx: &mut CompileContext) -> Result<String> {
    match pred {
        Predicate::Eq { column, value } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} = {v}"))
        }
        Predicate::Ne { column, value } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} <> {v}"))
        }
        Predicate::Lt { column, value } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} < {v}"))
        }
        Predicate::Lte { column, value } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} <= {v}"))
        }
        Predicate::Gt { column, value } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} > {v}"))
        }
        Predicate::Gte { column, value } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let v = compile_expression(value, ctx)?;
            Ok(format!("{c} >= {v}"))
        }
        Predicate::In { column, values } => {
            if values.is_empty() {
                return Err(DatabaseError::invalid_plan("IN richiede almeno un valore"));
            }
            let c = quote_identifier(column, ctx.dialect)?;
            let items: Result<Vec<_>> =
                values.iter().map(|e| compile_expression(e, ctx)).collect();
            let joined = items?.join(", ");
            Ok(format!("{c} IN ({joined})"))
        }
        Predicate::Between { column, low, high } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let l = compile_expression(low, ctx)?;
            let h = compile_expression(high, ctx)?;
            Ok(format!("{c} BETWEEN {l} AND {h}"))
        }
        Predicate::Like { column, pattern } => {
            let c = quote_identifier(column, ctx.dialect)?;
            let p = compile_expression(pattern, ctx)?;
            Ok(format!("{c} LIKE {p}"))
        }
        Predicate::IsNull { column } => {
            let c = quote_identifier(column, ctx.dialect)?;
            Ok(format!("{c} IS NULL"))
        }
        Predicate::IsNotNull { column } => {
            let c = quote_identifier(column, ctx.dialect)?;
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
    let col = quote_identifier(column, ctx.dialect)?;
    match ctx.dialect {
        DialectKind::Postgres => compile_spatial_postgres(&col, predicate, reference, ctx),
        DialectKind::Mysql => compile_spatial_mysql(&col, predicate, reference, ctx),
    }
}

fn compile_spatial_postgres(
    col: &str,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
    ctx: &mut CompileContext,
) -> Result<String> {
    // Fase B: validazione + cast delegati a `spatial_policy` (single
    // source of truth). Prima erano inline duplicati con `spatial.rs`
    // e `spatial.py`.
    spatial_policy::validate_predicate(ProviderKind::Postgres, predicate, reference)?;
    let cast = spatial_policy::postgres_cast_for(reference.semantics);
    let col_cast = if reference.semantics == SpatialSemantics::Geography {
        format!("{col}::geography")
    } else {
        col.to_owned()
    };
    // Fix review #5 completo: applica il SRID dichiarato via
    // `ST_SetSRID` per intercettare il caso WKB puro (senza SRID
    // embedded), che altrimenti arriverebbe al server come SRID=0
    // producendo silent wrong result. Se l'EWKB ha già un SRID
    // embedded coerente, `ST_SetSRID` è idempotente. La coerenza
    // fra SRID embedded e dichiarato è garantita da
    // `SpatialReference::new_validated` upstream.
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let srid_placeholder = ctx.bind(ParameterValue::I32(
        i32::try_from(reference.srid).unwrap_or(i32::MAX),
    ));
    let geom_expr = format!(
        "ST_SetSRID(ST_GeomFromEWKB({geom_placeholder}), {srid_placeholder}){cast}"
    );
    match predicate {
        SpatialPredicate::Intersects => Ok(format!("ST_Intersects({col_cast}, {geom_expr})")),
        SpatialPredicate::Contains => Ok(format!("ST_Contains({col_cast}, {geom_expr})")),
        SpatialPredicate::Within => Ok(format!("ST_Within({col_cast}, {geom_expr})")),
        SpatialPredicate::BoundingBox => Ok(format!("{col_cast} && {geom_expr}")),
        SpatialPredicate::DWithin { distance_meters } => {
            let dist_placeholder = ctx.bind(ParameterValue::F64(*distance_meters));
            Ok(format!(
                "ST_DWithin({col_cast}, {geom_expr}, {dist_placeholder})"
            ))
        }
    }
}

fn compile_spatial_mysql(
    col: &str,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
    ctx: &mut CompileContext,
) -> Result<String> {
    // MySQL: no distinzione geometry/geography a livello tipo — la semantica
    // deriva dal SRID della colonna. Validazione (DWithin unsupported,
    // distanza finita) delegata a `spatial_policy` in Fase B.
    spatial_policy::validate_predicate(ProviderKind::Mysql, predicate, reference)?;
    // Fix review #5: MySQL 8.0+ `ST_GeomFromWKB(wkb, srid)` accetta
    // il SRID come secondo argomento. Passiamo sempre il SRID
    // dichiarato per intercettare il caso WKB puro (senza SRID
    // embedded) che altrimenti arriverebbe come SRID 0.
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let srid_placeholder = ctx.bind(ParameterValue::I32(
        i32::try_from(reference.srid).unwrap_or(i32::MAX),
    ));
    let geom_expr = format!("ST_GeomFromWKB({geom_placeholder}, {srid_placeholder})");
    match predicate {
        SpatialPredicate::Intersects => Ok(format!("ST_Intersects({col}, {geom_expr})")),
        SpatialPredicate::Contains => Ok(format!("ST_Contains({col}, {geom_expr})")),
        SpatialPredicate::Within => Ok(format!("ST_Within({col}, {geom_expr})")),
        SpatialPredicate::BoundingBox => Ok(format!("MBRIntersects({col}, {geom_expr})")),
        // DWithin è già escluso da validate_predicate (Unsupported), qui
        // non è raggiungibile — ma il match deve essere exhaustive.
        SpatialPredicate::DWithin { .. } => unreachable!(
            "spatial_policy::validate_predicate deve aver già rifiutato DWithin su MySQL"
        ),
    }
}

fn compile_projection(projection: &Projection, dialect: DialectKind) -> Result<String> {
    match projection {
        Projection::All => Ok("*".to_owned()),
        Projection::Columns(cols) => {
            if cols.is_empty() {
                return Err(DatabaseError::invalid_plan(
                    "projection esplicita non può essere vuota",
                ));
            }
            let quoted: Result<Vec<_>> = cols.iter().map(|c| quote_identifier(c, dialect)).collect();
            Ok(quoted?.join(", "))
        }
    }
}

fn compile_order_by(order_by: &[OrderBy], dialect: DialectKind) -> Result<String> {
    let parts: Result<Vec<_>> = order_by
        .iter()
        .map(|o| {
            let col = quote_identifier(&o.column, dialect)?;
            let dir = match o.direction {
                Direction::Asc => "ASC",
                Direction::Desc => "DESC",
            };
            let mut clause = format!("{col} {dir}");
            // MySQL non supporta NULLS FIRST/LAST; il default MySQL è
            // "NULL first per ASC, last per DESC" — non emettiamo la
            // clausola per MySQL (semantic degradation esplicita).
            if let Some(nulls) = o.nulls {
                if dialect == DialectKind::Postgres {
                    clause.push_str(match nulls {
                        Nulls::First => " NULLS FIRST",
                        Nulls::Last => " NULLS LAST",
                    });
                }
                // MySQL: skip silently. Il consumer deve sapere che il
                // default MySQL è deterministic ma diverso da Postgres.
            }
            Ok(clause)
        })
        .collect();
    Ok(parts?.join(", "))
}

fn compile_returning(returning: &[String], dialect: DialectKind) -> Result<String> {
    if returning.is_empty() {
        return Ok(String::new());
    }
    match dialect {
        DialectKind::Postgres => {
            let cols: Result<Vec<_>> = returning.iter().map(|c| quote_identifier(c, dialect)).collect();
            Ok(format!(" RETURNING {}", cols?.join(", ")))
        }
        DialectKind::Mysql => {
            // MySQL non ha RETURNING universale (solo 8.0.20+ per INSERT).
            // Fail-closed esplicito invece di produrre SQL rotto.
            Err(DatabaseError::unsupported(
                ProviderKind::Mysql,
                crate::ErrorPhase::Prepare,
                "RETURNING non supportato dal compilatore portable MySQL. \
                 Usa una SELECT esplicita post-DML.",
            ))
        }
    }
}

// ---- Statement compilers ---------------------------------------------------

fn compile_select(s: &SelectStatement, ctx: &mut CompileContext) -> Result<String> {
    let projection = compile_projection(&s.projection, ctx.dialect)?;
    let table = qualify_table(&s.table, ctx.dialect)?;
    let mut sql = format!("SELECT {projection} FROM {table}");
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    if !s.order_by.is_empty() {
        let ob = compile_order_by(&s.order_by, ctx.dialect)?;
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
    let table = qualify_table(&s.table, ctx.dialect)?;
    let cols: Result<Vec<_>> = s.columns.iter().map(|c| quote_identifier(c, ctx.dialect)).collect();
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
    sql.push_str(&compile_returning(&s.returning, ctx.dialect)?);
    Ok(sql)
}

fn compile_update(s: &UpdateStatement, ctx: &mut CompileContext) -> Result<String> {
    if s.assignments.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "UPDATE richiede almeno un assignment",
        ));
    }
    let table = qualify_table(&s.table, ctx.dialect)?;
    let sets: Result<Vec<_>> = s
        .assignments
        .iter()
        .map(|(col, expr)| {
            let c = quote_identifier(col, ctx.dialect)?;
            let e = compile_expression(expr, ctx)?;
            Ok(format!("{c} = {e}"))
        })
        .collect();
    let mut sql = format!("UPDATE {table} SET {}", sets?.join(", "));
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    sql.push_str(&compile_returning(&s.returning, ctx.dialect)?);
    Ok(sql)
}

fn compile_delete(s: &DeleteStatement, ctx: &mut CompileContext) -> Result<String> {
    let table = qualify_table(&s.table, ctx.dialect)?;
    let mut sql = format!("DELETE FROM {table}");
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    sql.push_str(&compile_returning(&s.returning, ctx.dialect)?);
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
    let table = qualify_table(&s.table, ctx.dialect)?;
    let cols: Result<Vec<_>> = s.columns.iter().map(|c| quote_identifier(c, ctx.dialect)).collect();
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
    let mut sql = format!(
        "INSERT INTO {table} ({cols_sql}) VALUES {}",
        rows?.join(", ")
    );
    match ctx.dialect {
        DialectKind::Postgres => {
            let conflict: Result<Vec<_>> = s
                .conflict_target
                .iter()
                .map(|c| quote_identifier(c, ctx.dialect))
                .collect();
            let conflict_sql = conflict?.join(", ");
            write!(sql, " ON CONFLICT ({conflict_sql})").expect("write String");
            if s.update_on_conflict.is_empty() {
                sql.push_str(" DO NOTHING");
            } else {
                let sets: Result<Vec<_>> = s
                    .update_on_conflict
                    .iter()
                    .map(|(col, expr)| {
                        let c = quote_identifier(col, ctx.dialect)?;
                        let e = compile_expression(expr, ctx)?;
                        Ok(format!("{c} = {e}"))
                    })
                    .collect();
                write!(sql, " DO UPDATE SET {}", sets?.join(", ")).expect("write String");
            }
        }
        DialectKind::Mysql => {
            // MySQL: ON DUPLICATE KEY UPDATE. Il conflict_target NON è
            // esplicito in MySQL (usa la primary key / unique index
            // automatico) — accettiamo il campo per compat portable ma
            // non lo emettiamo nel SQL. Il consumer deve garantire che
            // conflict_target coincida con un unique index del target.
            if s.update_on_conflict.is_empty() {
                // MySQL non ha equivalente diretto di "DO NOTHING".
                // Simuliamo con INSERT IGNORE (silent skip su duplicate).
                // Nota: la parte INSERT INTO ... VALUES ... resta;
                // pre-pendo IGNORE dopo INSERT.
                sql = sql.replacen("INSERT INTO", "INSERT IGNORE INTO", 1);
            } else {
                let sets: Result<Vec<_>> = s
                    .update_on_conflict
                    .iter()
                    .map(|(col, expr)| {
                        let c = quote_identifier(col, ctx.dialect)?;
                        let e = compile_expression(expr, ctx)?;
                        Ok(format!("{c} = {e}"))
                    })
                    .collect();
                write!(sql, " ON DUPLICATE KEY UPDATE {}", sets?.join(", "))
                    .expect("write String");
            }
        }
    }
    sql.push_str(&compile_returning(&s.returning, ctx.dialect)?);
    Ok(sql)
}

