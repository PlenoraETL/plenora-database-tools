//! Compilatore SQL per il `PortableStatement`. Supporta `PostgreSQL`, `MySQL`,
//! `MariaDB`, `SQL Server`, `Oracle` e `Db2`; gli altri provider fail-closed.
//!
//! Design: unico compile pass con `DialectKind` dispatch sui punti dove
//! il dialect diverge (placeholder `$N` vs `?`, quoting `"` vs `` ` ``,
//! `ON CONFLICT` vs `ON DUPLICATE KEY UPDATE`, `RETURNING` dove il prodotto
//! **e la forma** lo ammettono, spatial `ST_GeomFromEWKB` vs
//! `ST_GeomFromWKB`). Le funzioni comuni (`predicate`, `expression`,
//! `projection`, `order_by`, ecc.) sono uniche.
//! `MariaDB` condivide il dialetto `MySQL` salvo le forme `RETURNING` qualificate;
//! SQL Server usa placeholder, quoting e forme DML T-SQL dedicate.

use super::{
    DeleteStatement, Direction, Expression, InsertStatement, Nulls, OrderBy, PortableStatement,
    Predicate, Projection, SelectStatement, TableRef, UpdateStatement, UpsertStatement,
};
use crate::geometry::SpatialSemantics;
use crate::identifier::{self, IdentifierDialect};
use crate::plan::ProviderKind;
use crate::provider::ParameterValue;
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
pub fn compile_portable(kind: ProviderKind, statement: &PortableStatement) -> Result<Statement> {
    let dialect = match kind {
        ProviderKind::Postgres => DialectKind::Postgres,
        ProviderKind::Mysql => DialectKind::Mysql,
        ProviderKind::Mariadb => DialectKind::Mariadb,
        ProviderKind::Sqlserver => DialectKind::SqlServer,
        ProviderKind::Oracle => DialectKind::Oracle,
        ProviderKind::Db2 => DialectKind::Db2,
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
    /// T-SQL, e diverge dagli altri tre piu di quanto i tre divergano fra
    /// loro: il segnaposto ha un nome (`@P1`), l'identificatore sta fra
    /// parentesi quadre, il tetto di righe e un `TOP` che precede la
    /// projection invece di un `LIMIT` che la segue, cio che una scrittura
    /// restituisce si chiede **in mezzo** allo statement e non in coda, e le
    /// funzioni spatial sono metodi del valore invece che chiamate.
    SqlServer,
    /// La sintassi di `MySQL`, tranne dove i due prodotti divergono davvero.
    ///
    /// Anche se la divergenza corrente è `RETURNING`,
    /// una bandiera dentro `DialectKind::Mysql` sarebbe bastata. Sarebbe
    /// bastata a scrivere il codice, non a leggerlo: `MariaDB` e un prodotto
    /// diverso, e la prossima divergenza si presenta come una seconda
    /// bandiera dentro un dialetto che nel nome dichiara di essere di
    /// qualcun altro. Il costo di una variante e un `match` in piu; il costo
    /// dell'alternativa e non sapere piu di chi si sta parlando.
    Mariadb,
    /// Db2 LUW: identificatori SQL standard, marker posizionali `?`, limite
    /// espresso con `FETCH FIRST` e upsert tramite `MERGE`.
    Db2,
    /// Oracle: identificatori SQL standard, marker nominati per posizione
    /// `:N`, limite `FETCH FIRST` e upsert atomico tramite `MERGE`.
    Oracle,
}

/// La forma di scrittura a cui la clausola `RETURNING` si attacca.
///
/// Serve perche su `MariaDB` `RETURNING` **non** e una proprieta del dialetto:
/// e una proprieta della coppia dialetto-forma. Il server accetta la clausola
/// su `INSERT`, `REPLACE`, `DELETE` e sull'upsert, e la rifiuta su `UPDATE`
/// con un errore di sintassi. Passare solo il dialetto costringerebbe a
/// scegliere fra aprire anche l'`UPDATE`, che fallirebbe sul server, e
/// chiudere anche le altre tre, che funzionano.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReturningForm {
    Insert,
    Update,
    Delete,
    Upsert,
}

impl ReturningForm {
    /// Il nome della forma, per il messaggio di rifiuto.
    const fn label(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Upsert => "l'upsert",
        }
    }
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
        let oracle_timestamp_tz =
            self.dialect == DialectKind::Oracle && matches!(&value, ParameterValue::TimestampTz(_));
        self.params.push(value);
        let placeholder = match self.dialect {
            DialectKind::Postgres => format!("${}", self.params.len()),
            // Il segnaposto e il primo punto in cui MariaDB e MySQL: la lista
            // di cio che i due condividono e lunga, e ogni riga di questo file
            // che li tratta insieme e una riga in cui si somigliano davvero.
            DialectKind::Mysql | DialectKind::Mariadb | DialectKind::Db2 => "?".to_owned(),
            DialectKind::Oracle => format!(":{}", self.params.len()),
            // Posizionale come gli altri, e con un nome: `tiberius` lega per
            // ordine e si aspetta `@P1`, `@P2`. Il numero e lo stesso di
            // PostgreSQL, la sintassi no.
            DialectKind::SqlServer => format!("@P{}", self.params.len()),
        };
        if oracle_timestamp_tz {
            format!(
                "TO_TIMESTAMP_TZ({placeholder}, '{}')",
                crate::provider::ORACLE_TIMESTAMP_TZ_FORMAT_MODEL
            )
        } else {
            placeholder
        }
    }
}

// ---- Helpers ----------------------------------------------------------------

/// Applica la policy canonica di quoting degli identificatori condivisa dai
/// renderer, evitando regole locali che possano divergere.
fn quote_identifier(name: &str, dialect: DialectKind) -> Result<String> {
    identifier::quote_identifier(dialect.into(), name)
}

impl From<DialectKind> for IdentifierDialect {
    fn from(kind: DialectKind) -> Self {
        match kind {
            // Il quoting e ANSI come PostgreSQL. Il limite di 63 byte resta
            // intenzionalmente conservativo finche il contratto pubblico di
            // `IdentifierDialect` non potra aggiungere Db2 in una nuova major.
            DialectKind::Postgres | DialectKind::Oracle | DialectKind::Db2 => Self::Postgres,
            // Il quoting: backtick raddoppiato, identico sui due prodotti.
            DialectKind::Mysql | DialectKind::Mariadb => Self::Mysql,
            // Parentesi quadre, con la chiusa raddoppiata. La regola c'era
            // gia: e lo stesso modulo che serve gli altri tre.
            DialectKind::SqlServer => Self::SqlServer,
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
        Expression::SpatialValue {
            expression,
            srid,
            semantics,
        } => {
            if *srid == 0 {
                return Err(DatabaseError::invalid_plan(
                    "valore spatial richiede SRID positivo",
                ));
            }
            let value = compile_expression(expression, ctx)?;
            match ctx.dialect {
                DialectKind::Postgres => {
                    let geometry = format!("ST_SetSRID(ST_GeomFromEWKB({value}), {srid})");
                    Ok(match semantics {
                        SpatialSemantics::Geometry => geometry,
                        SpatialSemantics::Geography => format!("({geometry})::geography"),
                    })
                }
                DialectKind::Mysql | DialectKind::Mariadb
                    if *semantics == SpatialSemantics::Geometry =>
                {
                    // Entrambi i prodotti richiedono un argomento binario
                    // tipizzato. MariaDB rifiuta il placeholder nudo con
                    // 4079; la forma con CAST e stata misurata dalla sonda
                    // `raw.spatial_write_forms` ed e condivisa con il writer
                    // Arrow dello stesso crate provider.
                    Ok(format!("ST_GeomFromWKB(CAST({value} AS BINARY), {srid})"))
                }
                DialectKind::SqlServer => {
                    let constructor = match semantics {
                        SpatialSemantics::Geometry => "geometry::STGeomFromWKB",
                        SpatialSemantics::Geography => "geography::STGeomFromWKB",
                    };
                    Ok(format!("{constructor}({value}, {srid})"))
                }
                DialectKind::Db2 if *semantics == SpatialSemantics::Geometry => {
                    Ok(format!("ST_GEOMETRY(BLOB(HEXTORAW({value})), {srid})"))
                }
                // Oracle conserva entrambe le semantiche nel tipo
                // `SDO_GEOMETRY`; geography e resa esplicita da SRID e policy
                // delle operazioni, non da un costruttore distinto.
                DialectKind::Oracle => Ok(format!(
                    "MDSYS.SDO_UTIL.FROM_WKBGEOMETRY(TO_BLOB({value}), {srid})"
                )),
                _ => Err(DatabaseError::unsupported(
                    match ctx.dialect {
                        DialectKind::Mysql => ProviderKind::Mysql,
                        DialectKind::Mariadb => ProviderKind::Mariadb,
                        DialectKind::SqlServer => ProviderKind::Sqlserver,
                        DialectKind::Db2 => ProviderKind::Db2,
                        DialectKind::Oracle => ProviderKind::Oracle,
                        DialectKind::Postgres => unreachable!(),
                    },
                    crate::ErrorPhase::Prepare,
                    "bind spatial OLTP non qualificato per il provider",
                )),
            }
        }
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
            let items: Result<Vec<_>> = values.iter().map(|e| compile_expression(e, ctx)).collect();
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
                return Err(DatabaseError::invalid_plan(
                    "AND richiede almeno un predicato",
                ));
            }
            let parts: Result<Vec<_>> = predicates
                .iter()
                .map(|p| compile_predicate(p, ctx))
                .collect();
            Ok(format!("({})", parts?.join(" AND ")))
        }
        Predicate::Or { predicates } => {
            if predicates.is_empty() {
                return Err(DatabaseError::invalid_plan(
                    "OR richiede almeno un predicato",
                ));
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
        // `ST_GeomFromWKB` e i predicati spatial: nessuna divergenza misurata
        // fra i due prodotti — `raw.spatial_functions` ha registrato lo stesso
        // esito sui tre riferimenti.
        DialectKind::Mysql | DialectKind::Mariadb => {
            compile_spatial_mysql(&col, predicate, reference, ctx)
        }
        DialectKind::SqlServer => compile_spatial_sqlserver(&col, predicate, reference, ctx),
        DialectKind::Db2 => compile_spatial_db2(&col, predicate, reference, ctx),
        DialectKind::Oracle => compile_spatial_oracle(&col, predicate, reference, ctx),
    }
}

fn compile_spatial_oracle(
    col: &str,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
    ctx: &mut CompileContext,
) -> Result<String> {
    reference.validate()?;
    spatial_policy::validate_predicate(ProviderKind::Oracle, predicate, reference)?;
    let geometry = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let srid = i32::try_from(reference.srid).map_err(|_| {
        DatabaseError::invalid_plan(format!(
            "SRID {} eccede il range i32 supportato da Oracle Spatial (max {})",
            reference.srid,
            i32::MAX
        ))
    })?;
    let srid = ctx.bind(ParameterValue::I32(srid));
    let reference = format!("MDSYS.SDO_UTIL.FROM_WKBGEOMETRY(TO_BLOB({geometry}), {srid})");
    match predicate {
        SpatialPredicate::Intersects => Ok(format!(
            "(MDSYS.SDO_RELATE({col}, {reference}, 'mask=ANYINTERACT') = 'TRUE')"
        )),
        SpatialPredicate::Contains => Ok(format!(
            "(MDSYS.SDO_RELATE({col}, {reference}, 'mask=CONTAINS') = 'TRUE')"
        )),
        SpatialPredicate::Within => Ok(format!(
            "(MDSYS.SDO_RELATE({col}, {reference}, 'mask=INSIDE') = 'TRUE')"
        )),
        SpatialPredicate::BoundingBox => Ok(format!(
            "(MDSYS.SDO_FILTER({col}, {reference}, 'querytype=WINDOW') = 'TRUE')"
        )),
        SpatialPredicate::DWithin { distance_meters } => {
            let distance = ctx.bind(ParameterValue::F64(*distance_meters));
            Ok(format!(
                "(MDSYS.SDO_WITHIN_DISTANCE({col}, {reference}, 'distance=' || {distance} || ' unit=M') = 'TRUE')"
            ))
        }
    }
}

fn compile_spatial_db2(
    col: &str,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
    ctx: &mut CompileContext,
) -> Result<String> {
    reference.validate()?;
    spatial_policy::validate_predicate(ProviderKind::Db2, predicate, reference)?;
    let geometry = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let srid = i32::try_from(reference.srid).map_err(|_| {
        DatabaseError::invalid_plan(format!(
            "SRID {} eccede il range i32 supportato da Db2 (max {})",
            reference.srid,
            i32::MAX
        ))
    })?;
    let srid = ctx.bind(ParameterValue::I32(srid));
    let reference = format!("ST_GEOMETRY(BLOB(HEXTORAW({geometry})), {srid})");
    let function = match predicate {
        SpatialPredicate::Intersects => "ST_INTERSECTS",
        SpatialPredicate::Contains => "ST_CONTAINS",
        SpatialPredicate::Within => "ST_WITHIN",
        SpatialPredicate::DWithin { .. } | SpatialPredicate::BoundingBox => {
            unreachable!("spatial_policy::validate_predicate deve rifiutare il predicato Db2")
        }
    };
    Ok(format!("({function}({col}, {reference}) = 1)"))
}

/// Il predicato spatial in T-SQL: un **metodo della colonna**.
///
/// Negli altri tre dialetti il predicato e una funzione che prende due
/// geometrie. Qui la colonna e il ricevitore e il riferimento l'argomento, e
/// cio che torna e un `bit` da confrontare con uno — non un booleano.
///
/// Il costruttore dipende dalla semantica dichiarata nel riferimento:
/// `geometry` e `geography` sono due tipi, e chiamare il metodo dell'uno su un
/// valore dell'altro non compila. E' l'unica cosa che il piano deve dire e che
/// negli altri dialetti non serviva.
fn compile_spatial_sqlserver(
    col: &str,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
    ctx: &mut CompileContext,
) -> Result<String> {
    reference.validate()?;
    spatial_policy::validate_predicate(ProviderKind::Sqlserver, predicate, reference)?;
    let constructor = match reference.semantics {
        SpatialSemantics::Geometry => "geometry::STGeomFromWKB",
        SpatialSemantics::Geography => "geography::STGeomFromWKB",
    };
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    let srid_i32 = i32::try_from(reference.srid).map_err(|_| {
        DatabaseError::invalid_plan(format!(
            "SRID {} eccede il range i32 supportato da SQL Server (max {})",
            reference.srid,
            i32::MAX
        ))
    })?;
    let srid_placeholder = ctx.bind(ParameterValue::I32(srid_i32));
    let geom_expr = format!("{constructor}({geom_placeholder}, {srid_placeholder})");
    let method = match predicate {
        SpatialPredicate::Intersects => "STIntersects",
        SpatialPredicate::Contains => "STContains",
        SpatialPredicate::Within => "STWithin",
        // Il rettangolo che contiene, su entrambi i lati. Non e
        // un'approssimazione come il `Filter()` di T-SQL, che rende righe che
        // **potrebbero** intersecare e che non e stato misurato: questa e la
        // stessa cosa che `MBRIntersects` dice su MySQL, scritta con due
        // metodi qualificati.
        SpatialPredicate::BoundingBox => {
            return Ok(format!(
                "({col}.STEnvelope().STIntersects(({geom_expr}).STEnvelope()) = 1)"
            ));
        }
        // Gia rifiutato da `validate_predicate`, e il match deve restare
        // esaustivo.
        SpatialPredicate::DWithin { .. } => unreachable!(
            "spatial_policy::validate_predicate deve aver gia rifiutato DWithin su SQL Server"
        ),
    };
    Ok(format!("({col}.{method}({geom_expr}) = 1)"))
}

fn compile_spatial_postgres(
    col: &str,
    predicate: &SpatialPredicate,
    reference: &SpatialReference,
    ctx: &mut CompileContext,
) -> Result<String> {
    // La validazione EWKB deve precedere la generazione SQL. Blocca
    // `SpatialReference` deserializzati da JSON
    // o costruiti literal con SRID/dimensioni divergenti dal buffer
    // EWKB reale. Senza questo check, il consumer poteva aggirare la
    // spatial_policy dichiarando `srid: 3857` con EWKB WGS84 →
    // ST_SetSRID sovrascriveva silenziosamente.
    reference.validate()?;
    // Validazione e cast restano delegati a `spatial_policy`, unica fonte
    // delle regole condivise con le altre superfici.
    spatial_policy::validate_predicate(ProviderKind::Postgres, predicate, reference)?;
    let cast = spatial_policy::postgres_cast_for(reference.semantics);
    let col_cast = if reference.semantics == SpatialSemantics::Geography {
        format!("{col}::geography")
    } else {
        col.to_owned()
    };
    // Applica il SRID dichiarato via `ST_SetSRID` per intercettare il caso
    // WKB puro (senza SRID
    // embedded), che altrimenti arriverebbe al server come SRID=0
    // producendo silent wrong result. Se l'EWKB ha già un SRID
    // embedded coerente, `ST_SetSRID` è idempotente. La coerenza
    // fra SRID embedded e dichiarato è garantita da
    // `SpatialReference::new_validated` upstream.
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    // La conversione deve fallire oltre `i32::MAX`: saturare
    // silenziosamente farebbe usare al consumer un SRID
    // passa un SRID sopra 2^31-1 avrebbe ottenuto risultati con SRID
    // sbagliato invece di errore.
    let srid_i32 = i32::try_from(reference.srid).map_err(|_| {
        DatabaseError::invalid_plan(format!(
            "SRID {} eccede il range i32 supportato da PostGIS (max {})",
            reference.srid,
            i32::MAX
        ))
    })?;
    let srid_placeholder = ctx.bind(ParameterValue::I32(srid_i32));
    let geom_expr =
        format!("ST_SetSRID(ST_GeomFromEWKB({geom_placeholder}), {srid_placeholder}){cast}");
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
    // Anche il percorso MySQL valida l'EWKB prima di produrre SQL; la
    // motivazione è la stessa del percorso PostgreSQL.
    reference.validate()?;
    // MySQL: no distinzione geometry/geography a livello tipo — la semantica
    // deriva dal SRID della colonna. Validazione (DWithin unsupported,
    // distanza finita) delegata a `spatial_policy`.
    spatial_policy::validate_predicate(ProviderKind::Mysql, predicate, reference)?;
    // `ST_GeomFromWKB(wkb, srid)` accetta il SRID come secondo argomento.
    // Lo passiamo sempre
    // dichiarato per intercettare il caso WKB puro (senza SRID
    // embedded) che altrimenti arriverebbe come SRID 0.
    let geom_placeholder = ctx.bind(ParameterValue::Bytes(reference.ewkb.clone()));
    // La conversione oltre `i32::MAX` segue la stessa policy fail-closed del
    // percorso PostgreSQL.
    let srid_i32 = i32::try_from(reference.srid).map_err(|_| {
        DatabaseError::invalid_plan(format!(
            "SRID {} eccede il range i32 supportato da MySQL (max {})",
            reference.srid,
            i32::MAX
        ))
    })?;
    let srid_placeholder = ctx.bind(ParameterValue::I32(srid_i32));
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
            let quoted: Result<Vec<_>> =
                cols.iter().map(|c| quote_identifier(c, dialect)).collect();
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
                if matches!(dialect, DialectKind::Postgres | DialectKind::Oracle) {
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

/// La clausola `RETURNING`, dove il prodotto e la forma la ammettono.
///
/// Il supporto dipende dalla coppia provider/forma DML. Le combinazioni non
/// qualificate vengono rifiutate senza emulazioni che alterino le righe
/// restituite; i test live dei provider sostengono i rami aperti qui sotto.
fn compile_returning(
    returning: &[String],
    dialect: DialectKind,
    form: ReturningForm,
) -> Result<ReturningClause> {
    if returning.is_empty() {
        return Ok(ReturningClause::Nothing);
    }
    let refusal = match dialect {
        DialectKind::Postgres => None,
        DialectKind::Db2 => Some((
            ProviderKind::Db2,
            format!(
                "Db2 non espone RETURNING nella forma portable {}: usa una SELECT esplicita post-DML",
                form.label()
            ),
        )),
        DialectKind::Oracle => Some((
            ProviderKind::Oracle,
            format!(
                "Oracle richiede bind OUT per RETURNING nella forma {}: usa una SELECT esplicita post-DML",
                form.label()
            ),
        )),
        DialectKind::Mysql => Some((
            ProviderKind::Mysql,
            "RETURNING non esiste su MySQL, a nessuna versione e in nessuna forma. \
             Usa una SELECT esplicita post-DML."
                .to_owned(),
        )),
        // `OUTPUT` esiste su INSERT, UPDATE e DELETE, e le tre forme
        // funzionano. Non sull'upsert, e non per una mancanza di T-SQL: quello
        // che questo compilatore emette per l'upsert sono **due** statement
        // sotto un lock — un UPDATE e, se non ha toccato niente, un INSERT — e
        // un `OUTPUT` appartiene a uno statement solo. Chiederlo renderebbe le
        // righe di quello dei due che ha agito, senza che il chiamante sappia
        // quale: un risultato che dipende da cosa c'era gia nella tabella.
        DialectKind::SqlServer => match form {
            ReturningForm::Insert | ReturningForm::Update | ReturningForm::Delete => None,
            ReturningForm::Upsert => Some((
                ProviderKind::Sqlserver,
                "l'upsert T-SQL e un UPDATE seguito da un INSERT condizionale sotto lock, e \
                 OUTPUT appartiene a un solo statement: le righe restituite dipenderebbero \
                 da quale dei due ha agito. Usa una SELECT esplicita post-scrittura."
                    .to_owned(),
            )),
        },
        DialectKind::Mariadb => match form {
            ReturningForm::Insert | ReturningForm::Delete | ReturningForm::Upsert => None,
            ReturningForm::Update => Some((
                ProviderKind::Mariadb,
                format!(
                    "MariaDB non ammette RETURNING su {}: il server risponde con un errore di \
                     sintassi. Le altre forme di scrittura lo ammettono.",
                    form.label()
                ),
            )),
        },
    };
    if let Some((kind, message)) = refusal {
        return Err(DatabaseError::unsupported(
            kind,
            crate::ErrorPhase::Prepare,
            message,
        ));
    }
    let cols: Result<Vec<_>> = returning
        .iter()
        .map(|c| quote_identifier(c, dialect))
        .collect();
    let cols = cols?;
    if dialect == DialectKind::SqlServer {
        // `DELETED` per la cancellazione, `INSERTED` per tutto il resto: sono
        // due pseudo-tabelle, e chiedere la riga inserita a una `DELETE`
        // renderebbe colonne nulle invece di un errore.
        let table = if form == ReturningForm::Delete {
            "DELETED"
        } else {
            "INSERTED"
        };
        let projected = cols
            .iter()
            .map(|column| format!("{table}.{column}"))
            .collect::<Vec<_>>();
        return Ok(ReturningClause::Output(format!(
            " OUTPUT {}",
            projected.join(", ")
        )));
    }
    Ok(ReturningClause::Suffix(format!(
        " RETURNING {}",
        cols.join(", ")
    )))
}

/// Dove il dialetto vuole le colonne che una scrittura restituisce.
///
/// Erano una stringa da appendere, e su tre dialetti su quattro lo sono
/// ancora. T-SQL no: `OUTPUT` sta **prima** di `VALUES`, prima di `WHERE`, e
/// subito dopo `DELETE FROM t`. Appenderlo in coda avrebbe prodotto SQL che il
/// server rifiuta, e il posto in cui va non e una proprieta dello statement ma
/// del dialetto — per questo la decisione torna a chi compone lo statement
/// invece di restare dentro `compile_returning`.
enum ReturningClause {
    Nothing,
    /// ` RETURNING a, b` — in coda.
    Suffix(String),
    /// ` OUTPUT INSERTED.a, INSERTED.b` — in mezzo.
    Output(String),
}

impl ReturningClause {
    /// Cio che va in mezzo allo statement, dove il dialetto lo vuole.
    fn inline(&self) -> &str {
        match self {
            Self::Output(clause) => clause,
            Self::Nothing | Self::Suffix(_) => "",
        }
    }

    /// Cio che va in coda.
    fn suffix(&self) -> &str {
        match self {
            Self::Suffix(clause) => clause,
            Self::Nothing | Self::Output(_) => "",
        }
    }

    const fn is_empty(&self) -> bool {
        matches!(self, Self::Nothing)
    }
}

// ---- Statement compilers ---------------------------------------------------

fn compile_select(s: &SelectStatement, ctx: &mut CompileContext) -> Result<String> {
    let projection = compile_projection(&s.projection, ctx.dialect)?;
    let table = qualify_table(&s.table, ctx.dialect)?;
    // `TOP` precede la projection, e va deciso prima di scriverla. Non
    // richiede un ORDER BY come farebbe `OFFSET ... FETCH`, ed e la sola forma
    // che serve qui: il `SelectStatement` portabile ha un tetto e non un
    // offset.
    let top = match (ctx.dialect, s.limit) {
        (DialectKind::SqlServer, Some(limit)) => format!("TOP ({limit}) "),
        _ => String::new(),
    };
    let mut sql = format!("SELECT {top}{projection} FROM {table}");
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    if !s.order_by.is_empty() {
        let ob = compile_order_by(&s.order_by, ctx.dialect)?;
        write!(sql, " ORDER BY {ob}").expect("write String");
    }
    if let Some(limit) = s.limit {
        match ctx.dialect {
            DialectKind::SqlServer => {}
            DialectKind::Db2 | DialectKind::Oracle => {
                write!(sql, " FETCH FIRST {limit} ROWS ONLY").expect("write String");
            }
            _ => write!(sql, " LIMIT {limit}").expect("write String"),
        }
    }
    Ok(sql)
}

fn compile_insert(s: &InsertStatement, ctx: &mut CompileContext) -> Result<String> {
    if s.columns.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "INSERT richiede almeno una colonna",
        ));
    }
    if s.values.is_empty() {
        return Err(DatabaseError::invalid_plan(
            "INSERT richiede almeno una riga",
        ));
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
    let cols: Result<Vec<_>> = s
        .columns
        .iter()
        .map(|c| quote_identifier(c, ctx.dialect))
        .collect();
    let cols = cols?;
    let cols_sql = cols.join(", ");
    let rows: Result<Vec<String>> = s
        .values
        .iter()
        .map(|row| {
            let placeholders: Result<Vec<_>> =
                row.iter().map(|e| compile_expression(e, ctx)).collect();
            Ok(format!("({})", placeholders?.join(", ")))
        })
        .collect();
    let returning = compile_returning(&s.returning, ctx.dialect, ReturningForm::Insert)?;
    let mut sql = format!(
        "INSERT INTO {table} ({cols_sql}){} VALUES {}",
        returning.inline(),
        rows?.join(", ")
    );
    sql.push_str(returning.suffix());
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
    let returning = compile_returning(&s.returning, ctx.dialect, ReturningForm::Update)?;
    let mut sql = format!(
        "UPDATE {table} SET {}{}",
        sets?.join(", "),
        returning.inline()
    );
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    sql.push_str(returning.suffix());
    Ok(sql)
}

fn compile_delete(s: &DeleteStatement, ctx: &mut CompileContext) -> Result<String> {
    let table = qualify_table(&s.table, ctx.dialect)?;
    let returning = compile_returning(&s.returning, ctx.dialect, ReturningForm::Delete)?;
    let mut sql = format!("DELETE FROM {table}{}", returning.inline());
    if let Some(filter) = &s.filter {
        let where_sql = compile_predicate(filter, ctx)?;
        write!(sql, " WHERE {where_sql}").expect("write String");
    }
    sql.push_str(returning.suffix());
    Ok(sql)
}

/// L'upsert in T-SQL: un UPDATE e, se non ha toccato niente, un INSERT.
///
/// # Perche non `MERGE`
///
/// Perche il percorso di scrittura di questo repository lo evita gia, e
/// deliberatamente. Due forme diverse per la stessa operazione dentro lo stesso
/// prodotto sarebbero due comportamenti da spiegare a chi li incontra: qui si
/// segue la decisione che c'e, invece di prenderne una seconda.
///
/// # Il lock
///
/// `WITH (UPDLOCK, HOLDLOCK)` sulla lettura e sull'UPDATE, come nel percorso di
/// scrittura. Senza, fra l'UPDATE che non tocca niente e l'INSERT che segue
/// c'e una finestra in cui un'altra transazione inserisce la stessa chiave, e
/// cio che si ottiene e una violazione di chiave invece di un upsert. Il lock
/// e la ragione per cui questa forma e corretta, non un ornamento.
///
/// # Una riga sola
///
/// La forma condizionale ragiona su **una** chiave: `IF @@ROWCOUNT = 0` dice
/// che l'UPDATE non ha trovato la riga, e con due righe non direbbe quale.
/// Renderla multi-riga vorrebbe dire ripetere la coppia per ogni riga —
/// possibile, e un altro disegno: il numero di round trip diventa il numero di
/// righe, e il conteggio che `execute` restituisce smette di significare
/// quello che significa altrove.
///
/// Il rifiuto e percio esplicito e dice quale forma usare: un `INSERT`
/// multi-riga per i dati nuovi, o un upsert per riga.
fn compile_upsert_sqlserver(
    s: &UpsertStatement,
    table: &str,
    cols_sql: &str,
    rows: &[Vec<String>],
    ctx: &mut CompileContext,
) -> Result<String> {
    // Prima del resto: se il chiamante chiede le righe indietro, la ragione la
    // da `compile_returning`, che di questa forma sa dire perche non puo.
    let returning = compile_returning(&s.returning, ctx.dialect, ReturningForm::Upsert)?;
    debug_assert!(returning.is_empty(), "il rifiuto precede questa riga");
    let [row] = rows else {
        return Err(DatabaseError::unsupported(
            ProviderKind::Sqlserver,
            crate::ErrorPhase::Prepare,
            format!(
                "l'upsert T-SQL vale su una riga per volta e ne sono arrivate {}: la forma \
                 condizionale ragiona su una chiave sola. Usa un INSERT multi-riga per i dati \
                 nuovi, oppure un upsert per riga.",
                rows.len()
            ),
        ));
    };
    if s.conflict_target.is_empty() {
        // Su PostgreSQL il bersaglio del conflitto e obbligatorio e su MySQL e
        // ignorato — li lo sceglie il server dagli indici unici. Qui serve
        // davvero: e la clausola WHERE della lettura e dell'UPDATE, e senza di
        // essa non c'e niente su cui decidere.
        return Err(DatabaseError::invalid_plan(
            "l'upsert T-SQL richiede un conflict_target esplicito: e la condizione con cui              cerca la riga esistente",
        ));
    }
    // Il segnaposto della colonna, riletto: `@P1` compare nella VALUES **e**
    // nella condizione, e lega lo stesso valore in tutte e due.
    let placeholder_of = |column: &str| -> Result<String> {
        let index = s
            .columns
            .iter()
            .position(|candidate| candidate == column)
            .ok_or_else(|| {
                DatabaseError::invalid_plan(format!(
                    "conflict_target nomina una colonna che l'upsert non scrive: {column}"
                ))
            })?;
        row.get(index).cloned().ok_or_else(|| {
            DatabaseError::invalid_plan("riga upsert piu corta dell'elenco delle colonne")
        })
    };
    let conditions: Result<Vec<_>> = s
        .conflict_target
        .iter()
        .map(|column| {
            let quoted = quote_identifier(column, ctx.dialect)?;
            let value = placeholder_of(column)?;
            Ok(format!("{quoted} = {value}"))
        })
        .collect();
    let condition = conditions?.join(" AND ");
    let values = format!("({})", row.join(", "));

    if s.update_on_conflict.is_empty() {
        // Il «non fare niente»: si guarda se la riga c'e, tenendo il lock, e si
        // inserisce soltanto se non c'e.
        return Ok(format!(
            "IF NOT EXISTS (SELECT 1 FROM {table} WITH (UPDLOCK, HOLDLOCK) WHERE {condition})              INSERT INTO {table} ({cols_sql}) VALUES {values};"
        ));
    }
    let sets: Result<Vec<_>> = s
        .update_on_conflict
        .iter()
        .map(|(column, expression)| {
            let quoted = quote_identifier(column, ctx.dialect)?;
            let value = compile_expression(expression, ctx)?;
            Ok(format!("{quoted} = {value}"))
        })
        .collect();
    Ok(format!(
        "UPDATE {table} WITH (UPDLOCK, HOLDLOCK) SET {} WHERE {condition};          IF @@ROWCOUNT = 0 INSERT INTO {table} ({cols_sql}) VALUES {values};",
        sets?.join(", ")
    ))
}

/// L'upsert Db2 usa una source table `VALUES` e un singolo `MERGE` atomico.
///
/// I marker della source sono legati una sola volta. Gli assignment espliciti
/// vengono compilati dopo la source e i riferimenti a colonna sono qualificati
/// con `T` per non confonderli con le colonne omonime di `S`.
fn compile_upsert_db2(
    s: &UpsertStatement,
    table: &str,
    cols: &[String],
    rows: &[Vec<String>],
    ctx: &mut CompileContext,
) -> Result<String> {
    let source_columns = cols.join(", ");
    let source_rows = rows
        .iter()
        .map(|row| format!("({})", row.join(", ")))
        .collect::<Vec<_>>()
        .join(", ");
    let source = format!("(VALUES {source_rows}) AS S ({source_columns})");
    compile_upsert_merge(
        s,
        table,
        cols,
        &source,
        MergeSyntax {
            target_alias: " AS T",
            predicate_parentheses: false,
            provider: ProviderKind::Db2,
        },
        ctx,
    )
}

/// Oracle usa lo stesso `MERGE` atomico di Db2, ma costruisce la source con
/// `SELECT ... FROM DUAL` e non ammette `AS` sull'alias della tabella target.
fn compile_upsert_oracle(
    s: &UpsertStatement,
    table: &str,
    cols: &[String],
    rows: &[Vec<String>],
    ctx: &mut CompileContext,
) -> Result<String> {
    let source_rows = rows
        .iter()
        .map(|row| {
            let projection = row
                .iter()
                .zip(cols)
                .map(|(value, column)| format!("{value} AS {column}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("SELECT {projection} FROM DUAL")
        })
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let source = format!("({source_rows}) S");
    compile_upsert_merge(
        s,
        table,
        cols,
        &source,
        MergeSyntax {
            target_alias: " T",
            predicate_parentheses: true,
            provider: ProviderKind::Oracle,
        },
        ctx,
    )
}

/// Compone le clausole comuni del `MERGE` usato da Db2 e Oracle. La source e
/// la presenza di `AS` restano parametri perché sono divergenze sintattiche;
/// validazione del conflict target e semantica degli assignment restano una
/// sola implementazione.
#[derive(Clone, Copy)]
struct MergeSyntax {
    target_alias: &'static str,
    predicate_parentheses: bool,
    provider: ProviderKind,
}

fn compile_upsert_merge(
    s: &UpsertStatement,
    table: &str,
    cols: &[String],
    source: &str,
    syntax: MergeSyntax,
    ctx: &mut CompileContext,
) -> Result<String> {
    compile_returning(&s.returning, ctx.dialect, ReturningForm::Upsert)?;
    let source_column = |name: &str| -> Result<String> {
        if !s.columns.iter().any(|candidate| candidate == name) {
            return Err(DatabaseError::invalid_plan(format!(
                "conflict_target nomina una colonna che l'upsert non scrive: {name}"
            )));
        }
        quote_identifier(name, ctx.dialect)
    };
    let predicates: Result<Vec<_>> = s
        .conflict_target
        .iter()
        .map(|column| {
            let quoted = source_column(column)?;
            Ok(format!("T.{quoted} = S.{quoted}"))
        })
        .collect();
    let insert_values = cols
        .iter()
        .map(|column| format!("S.{column}"))
        .collect::<Vec<_>>()
        .join(", ");
    let predicate = predicates?.join(" AND ");
    let predicate = if syntax.predicate_parentheses {
        format!("({predicate})")
    } else {
        predicate
    };
    let mut sql = format!(
        "MERGE INTO {table}{} USING {source} ON {predicate}",
        syntax.target_alias
    );
    if !s.update_on_conflict.is_empty() {
        let assignments: Result<Vec<_>> = s
            .update_on_conflict
            .iter()
            .map(|(column, expression)| {
                let column = quote_identifier(column, ctx.dialect)?;
                let expression = match expression {
                    Expression::Literal(value) => ctx.bind(value.clone()),
                    Expression::Column(name) => {
                        format!("T.{}", quote_identifier(name, ctx.dialect)?)
                    }
                    Expression::SpatialValue { .. } => {
                        return Err(DatabaseError::unsupported(
                            syntax.provider,
                            crate::ErrorPhase::Prepare,
                            "bind spatial non qualificato nell'upsert MERGE",
                        ));
                    }
                };
                Ok(format!("T.{column} = {expression}"))
            })
            .collect();
        write!(
            sql,
            " WHEN MATCHED THEN UPDATE SET {}",
            assignments?.join(", ")
        )
        .expect("write String");
    }
    write!(
        sql,
        " WHEN NOT MATCHED THEN INSERT ({}) VALUES ({insert_values})",
        cols.join(", ")
    )
    .expect("write String");
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
    let cols: Result<Vec<_>> = s
        .columns
        .iter()
        .map(|c| quote_identifier(c, ctx.dialect))
        .collect();
    let cols = cols?;
    let cols_sql = cols.join(", ");
    // Le righe restano scomposte finche non si sa chi le usa: T-SQL ha
    // bisogno di **rileggere** il segnaposto di una colonna per costruire la
    // condizione di conflitto, e una stringa gia unita non lo permetterebbe.
    // Che si possa rileggerlo e una proprieta del suo segnaposto: `@P1` ha un
    // nome, quindi comparire due volte nello stesso statement lega lo stesso
    // valore, mentre un `?` legherebbe il successivo.
    let rows: Result<Vec<Vec<String>>> = s
        .values
        .iter()
        .map(|row| row.iter().map(|e| compile_expression(e, ctx)).collect())
        .collect();
    let rows = rows?;
    if ctx.dialect == DialectKind::SqlServer {
        return compile_upsert_sqlserver(s, &table, &cols_sql, &rows, ctx);
    }
    if ctx.dialect == DialectKind::Db2 {
        return compile_upsert_db2(s, &table, &cols, &rows, ctx);
    }
    if ctx.dialect == DialectKind::Oracle {
        return compile_upsert_oracle(s, &table, &cols, &rows, ctx);
    }
    let mut sql = format!(
        "INSERT INTO {table} ({cols_sql}) VALUES {}",
        rows.iter()
            .map(|row| format!("({})", row.join(", ")))
            .collect::<Vec<_>>()
            .join(", ")
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
        // `ON DUPLICATE KEY UPDATE` e `INSERT IGNORE`: stessa sintassi sui due
        // prodotti. La divergenza dell'upsert non e qui, e in cosa il server
        // consegna dopo — vedi `compile_returning`.
        // Trattato prima del match, perche non e una variante della stessa
        // forma: T-SQL non ha una clausola di conflitto e lo statement e un
        // altro.
        DialectKind::SqlServer => unreachable!("l'upsert T-SQL esce prima di questo match"),
        DialectKind::Db2 => unreachable!("l'upsert Db2 esce prima di questo match"),
        DialectKind::Oracle => unreachable!("l'upsert Oracle esce prima di questo match"),
        DialectKind::Mysql | DialectKind::Mariadb => {
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
                write!(sql, " ON DUPLICATE KEY UPDATE {}", sets?.join(", ")).expect("write String");
            }
        }
    }
    sql.push_str(compile_returning(&s.returning, ctx.dialect, ReturningForm::Upsert)?.suffix());
    Ok(sql)
}
