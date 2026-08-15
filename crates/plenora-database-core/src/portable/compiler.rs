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

fn quote_identifier(name: &str, dialect: DialectKind) -> Result<String> {
    validate_identifier(name)?;
    match dialect {
        DialectKind::Postgres => {
            let escaped = name.replace('"', "\"\"");
            Ok(format!("\"{escaped}\""))
        }
        DialectKind::Mysql => {
            if name.contains('`') {
                return Err(DatabaseError::invalid_plan(
                    "identificatore MySQL non può contenere backtick",
                ));
            }
            Ok(format!("`{name}`"))
        }
    }
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

/// SRID che rappresentano coordinate geografiche (lat/lon in gradi).
/// Su questi SRID, `Geometry` + `DWithin` produce distanza in **gradi**,
/// non in metri — silent wrong result rispetto al nome `distance_meters`.
/// Il compiler blocca fail-closed la combinazione e chiede
/// `SpatialSemantics::Geography` esplicito.
///
/// Lista non esaustiva ma copre i casi PFM più comuni.
/// Riferimento: EPSG registry.
const GEOGRAPHIC_SRIDS: &[u32] = &[
    4326, // WGS 84 (GPS)
    4269, // NAD 83
    4267, // NAD 27
    4258, // ETRS89
    4283, // GDA94
];

fn is_geographic_srid(srid: u32) -> bool {
    GEOGRAPHIC_SRIDS.contains(&srid)
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
    // Fix review #4: `SpatialSemantics` decide il cast PostGIS.
    // - `Geometry`  → `::geometry` (planare, unità = unità di SRID)
    // - `Geography` → `::geography` (WGS84 sphere, unità sempre = metri)
    // Il cast della colonna è necessario perché la colonna può essere
    // dichiarata `geometry` anche se il consumer vuole semantica geographic.
    let (cast, col_cast) = match reference.semantics {
        SpatialSemantics::Geometry => ("::geometry", col.to_owned()),
        SpatialSemantics::Geography => ("::geography", format!("{col}::geography")),
    };
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let geom_expr = format!("ST_GeomFromEWKB({geom_placeholder}){cast}");
    match predicate {
        SpatialPredicate::Intersects => Ok(format!("ST_Intersects({col_cast}, {geom_expr})")),
        SpatialPredicate::Contains => Ok(format!("ST_Contains({col_cast}, {geom_expr})")),
        SpatialPredicate::Within => Ok(format!("ST_Within({col_cast}, {geom_expr})")),
        SpatialPredicate::BoundingBox => {
            // Operator `&&` funziona solo su `geometry`. Per `geography`
            // non c'è equivalente index-friendly nativo — fail-closed.
            if reference.semantics == SpatialSemantics::Geography {
                return Err(DatabaseError::unsupported(
                    ProviderKind::Postgres,
                    crate::ErrorPhase::Prepare,
                    "BoundingBox con SpatialSemantics::Geography non supportato \
                     (operator && è solo geometry). Usa Intersects.",
                ));
            }
            Ok(format!("{col_cast} && {geom_expr}"))
        }
        SpatialPredicate::DWithin { distance_meters } => {
            if !distance_meters.is_finite() || *distance_meters < 0.0 {
                return Err(DatabaseError::invalid_plan(
                    "DWithin richiede distanza finita non-negativa",
                ));
            }
            // Fix review #5: fail-closed su Geometry + SRID geografico.
            // Su SRID 4326 (WGS84) con semantics=Geometry, PostGIS produce
            // distanza in GRADI, non in metri — silent wrong result
            // rispetto al nome del campo `distance_meters`.
            // Chiede al consumer semantics=Geography esplicito.
            if reference.semantics == SpatialSemantics::Geometry
                && is_geographic_srid(reference.srid)
            {
                return Err(DatabaseError::invalid_plan(format!(
                    "DWithin con SpatialSemantics::Geometry su SRID geografico {} \
                     produrrebbe distanza in gradi (fuorviante rispetto al nome \
                     `distance_meters`). Usa SpatialSemantics::Geography per metri \
                     reali, oppure riproietta su un SRID planare.",
                    reference.srid
                )));
            }
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
    // deriva dal SRID della colonna (geografico → funzioni usano metri via
    // ST_Distance_Sphere/ST_Distance con SRID axis order). Fix review #4:
    // `Geography` è accettato ma trattato come hint semantico, non come cast
    // (MySQL non ha `::geography`). Se il consumer vuole distanze in metri
    // deve usare SRID geografico sulla colonna target.
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let geom_expr = format!("ST_GeomFromWKB({geom_placeholder})");
    match predicate {
        SpatialPredicate::Intersects => Ok(format!("ST_Intersects({col}, {geom_expr})")),
        SpatialPredicate::Contains => Ok(format!("ST_Contains({col}, {geom_expr})")),
        SpatialPredicate::Within => Ok(format!("ST_Within({col}, {geom_expr})")),
        SpatialPredicate::BoundingBox => Ok(format!("MBRIntersects({col}, {geom_expr})")),
        SpatialPredicate::DWithin { .. } => Err(DatabaseError::unsupported(
            ProviderKind::Mysql,
            crate::ErrorPhase::Prepare,
            "DWithin non supportato da MySQL (no ST_DWithin nativo). \
             Usa ST_Distance() < X come workaround via SQL raw.",
        )),
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

