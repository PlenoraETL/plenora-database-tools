//! Cio che la scrittura chiede al target, letto in sola lettura per il dry
//! run: schema, privilegi, proprieta della tabella, colonne, l'indice unico
//! che arbitra un upsert.
//!
//! Fino al 25/10 il dry run con la DSN diceva `dry_run` dove la scrittura
//! rifiutava: un utente con solo SELECT (Append, Create: `authorization`), uno
//! schema assente (Create: `not_found`), un upsert senza vincolo unico sulle
//! chiavi (`protocol`), un mapping `lossy` con una colonna assente dal target
//! (`not_found`, che il dry run riportava soltanto nel `loss_report`). Qui la
//! stessa decisione si prende sul catalogo, con la categoria e la fase che
//! la scrittura darebbe, **nel punto in cui la darebbe**: prima dell'ingresso
//! (DDL, `DELETE`/`TRUNCATE`, `ALTER`, registrazione degli id), alla
//! preparazione dello statement, o alla prima riga eseguita.
//!
//! Resta della scrittura cio che il catalogo non decide: vincoli e trigger
//! sui valori, conversioni della colonna, timeout e lock concorrenti.

use crate::error::{classify_error, public_error};
use crate::{PostgresInsertMode, PostgresSchemaEvolution};
use arrow_schema::SchemaRef;
use plenora_database_core::plan::{WriteMode, WriteOperation};
use plenora_database_core::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::collections::BTreeMap;
use tokio_postgres::Client;

/// Il percorso che la scrittura prenderebbe sullo stesso ingresso.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WritePath {
    /// Append in una transazione con il totale dichiarato: un INSERT
    /// preparato prima dell'ingresso, eseguito riga per riga.
    RowScoped,
    /// `COPY ... FROM STDIN`, aperto al primo batch non vuoto.
    Copy,
    /// INSERT, UPDATE o DELETE preparati al primo batch non vuoto ed
    /// eseguiti riga per riga.
    Parameters,
}

impl WritePath {
    #[must_use]
    pub const fn of(
        operation: &WriteOperation,
        row_scoped: bool,
        mode: PostgresInsertMode,
    ) -> Self {
        let bulk = matches!(
            operation.mode,
            WriteMode::Create | WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert
        );
        if row_scoped {
            Self::RowScoped
        } else if bulk && !matches!(mode, PostgresInsertMode::Prepared) {
            Self::Copy
        } else {
            Self::Parameters
        }
    }
}

/// I rifiuti del target che la scrittura darebbe, ciascuno nel suo punto.
#[derive(Debug, Default)]
pub struct TargetVerdict {
    /// Prima di leggere l'ingresso: DDL, svuotamento, evoluzione dello
    /// schema, `LOCK` della registrazione degli id.
    pub before_input: Option<DatabaseError>,
    /// Alla preparazione dello statement (o all'apertura di `COPY`).
    pub at_statement: Option<DatabaseError>,
    /// Alla prima riga eseguita.
    pub at_first_row: Option<DatabaseError>,
    /// La registrazione degli id dei campi puo scrivere `COMMENT ON
    /// COLUMN`: serve la proprieta della tabella.
    pub owner: bool,
    /// `LOCK TABLE ... SHARE ROW EXCLUSIVE` della registrazione.
    pub may_lock: bool,
}

fn denied(phase: ErrorPhase) -> DatabaseError {
    public_error(
        ErrorCategory::Authorization,
        phase,
        false,
        "permesso PostgreSQL insufficiente",
    )
}

fn missing_column() -> DatabaseError {
    public_error(
        ErrorCategory::NotFound,
        ErrorPhase::Prepare,
        false,
        "colonna del piano assente nel target PostgreSQL",
    )
}

/// I privilegi di una colonna esistente.
#[derive(Debug, Clone, Copy, Default)]
struct ColumnPrivileges {
    insert: bool,
    update: bool,
    select: bool,
}

/// I privilegi con cui la scrittura svuota il target (Replace, `TruncateInsert`).
#[derive(Debug, Clone, Copy)]
struct Emptying {
    delete: bool,
    truncate: bool,
}

/// Il target esistente, dal catalogo.
struct Table {
    oid: u32,
    owner: bool,
    lock: bool,
    emptying: Emptying,
    columns: BTreeMap<String, ColumnPrivileges>,
}

async fn table(client: &Client, schema: &str, object: &str) -> Result<Option<Table>> {
    let read = |error: tokio_postgres::Error| classify_error(ErrorPhase::Prepare, &error);
    let Some(row) = client
        .query_opt(
            "SELECT c.oid, pg_has_role(c.relowner, 'USAGE'), \
                    has_table_privilege(c.oid, 'DELETE'), \
                    has_table_privilege(c.oid, 'TRUNCATE'), \
                    has_table_privilege(c.oid, 'UPDATE') \
                    OR has_table_privilege(c.oid, 'DELETE') \
                    OR has_table_privilege(c.oid, 'TRUNCATE') \
                    OR CASE WHEN current_setting('server_version_num')::int >= 170000 \
                       THEN has_table_privilege(c.oid, 'MAINTAIN') ELSE false END \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema, &object],
        )
        .await
        .map_err(read)?
    else {
        return Ok(None);
    };
    let oid: u32 = row.get(0);
    let columns = client
        .query(
            "SELECT a.attname::text, \
                    has_column_privilege(a.attrelid, a.attnum, 'INSERT'), \
                    has_column_privilege(a.attrelid, a.attnum, 'UPDATE'), \
                    has_column_privilege(a.attrelid, a.attnum, 'SELECT') \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped",
            &[&oid],
        )
        .await
        .map_err(read)?
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                ColumnPrivileges {
                    insert: row.get(1),
                    update: row.get(2),
                    select: row.get(3),
                },
            )
        })
        .collect();
    Ok(Some(Table {
        oid,
        owner: row.get(1),
        lock: row.get(4),
        emptying: Emptying {
            delete: row.get(2),
            truncate: row.get(3),
        },
        columns,
    }))
}

/// Esiste l'indice che `ON CONFLICT (chiavi)` sceglierebbe: unico,
/// immediato, valido, senza predicato ne espressioni, con le colonne chiave
/// esattamente quelle del piano. Un vincolo differibile non arbitra: il
/// server lo salta nell'inferenza e risponde come senza indice (misurato su
/// PostgreSQL 16, scenario `upsert-unique-differibile` del runner).
async fn arbiter(client: &Client, oid: u32, keys: &[String]) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS (SELECT 1 \
             FROM pg_catalog.pg_index i \
             WHERE i.indrelid = $1 AND i.indisunique AND i.indimmediate AND i.indisvalid \
               AND i.indpred IS NULL AND i.indexprs IS NULL \
               AND (SELECT array_agg(DISTINCT a.attname::text ORDER BY a.attname::text) \
                    FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, position) \
                    JOIN pg_catalog.pg_attribute a \
                      ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
                    WHERE k.position <= i.indnkeyatts) \
                   = (SELECT array_agg(DISTINCT key ORDER BY key) FROM unnest($2::text[]) key))",
            &[&oid, &keys],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
    Ok(row.get(0))
}

/// Le colonne che lo statement della mode nomina, con il privilegio che
/// chiede su ciascuna.
fn statement_columns(operation: &WriteOperation, schema: &SchemaRef) -> Vec<(String, Need)> {
    let all = || schema.fields().iter().map(|field| field.name().clone());
    match operation.mode {
        WriteMode::Update => operation
            .update_columns
            .iter()
            .map(|name| (name.clone(), Need::Update))
            .chain(
                operation
                    .keys
                    .iter()
                    .map(|name| (name.clone(), Need::Select)),
            )
            .collect(),
        WriteMode::DeleteByKeys => operation
            .keys
            .iter()
            .map(|name| (name.clone(), Need::Select))
            .collect(),
        WriteMode::Upsert => all()
            .map(|name| (name, Need::Insert))
            .chain(
                all()
                    .filter(|name| !operation.keys.contains(name))
                    .map(|name| (name, Need::Update)),
            )
            .collect(),
        _ => all().map(|name| (name, Need::Insert)).collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Need {
    Insert,
    Update,
    Select,
}

/// Il verdetto del target per la scrittura di `operation` lungo `path`.
///
/// # Errors
///
/// Soltanto gli errori delle letture del catalogo; i rifiuti che la
/// scrittura darebbe stanno nel verdetto.
pub async fn inspect(
    client: &Client,
    operation: &WriteOperation,
    schema: &SchemaRef,
    path: WritePath,
    evolution: PostgresSchemaEvolution,
) -> Result<TargetVerdict> {
    let schema_name = operation.target.schema.as_deref().unwrap_or("public");
    let mut verdict = TargetVerdict {
        owner: true,
        may_lock: true,
        ..TargetVerdict::default()
    };
    if operation.mode == WriteMode::Create {
        // `CREATE TABLE`: lo schema deve esistere e accettare la creazione.
        // La tabella nuova e di chi la crea: il resto non chiede privilegi.
        let row = client
            .query_opt(
                "SELECT has_schema_privilege(n.oid, 'CREATE') \
                 FROM pg_catalog.pg_namespace n WHERE n.nspname = $1",
                &[&schema_name],
            )
            .await
            .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
        verdict.before_input = match row.map(|row| row.get::<_, bool>(0)) {
            None => Some(public_error(
                ErrorCategory::NotFound,
                ErrorPhase::Write,
                false,
                "schema PostgreSQL non trovato",
            )),
            Some(false) => Some(denied(ErrorPhase::Write)),
            Some(true) => None,
        };
        return Ok(verdict);
    }
    // `prepare_write` ha gia rifiutato un target che non esiste.
    let Some(table) = table(client, schema_name, &operation.target.object).await? else {
        return Ok(verdict);
    };
    verdict.owner = table.owner;
    verdict.may_lock = table.lock;
    let evolves = path != WritePath::RowScoped
        && evolution == PostgresSchemaEvolution::AddNullableColumns
        && matches!(
            operation.mode,
            WriteMode::Append
                | WriteMode::Replace
                | WriteMode::TruncateInsert
                | WriteMode::Update
                | WriteMode::Upsert
        );
    // Lo stesso ordine di `execute`: evoluzione dello schema (ALTER, del
    // proprietario), poi svuotamento del target.
    verdict.before_input = if evolves && !table.owner {
        Some(denied(ErrorPhase::Write))
    } else {
        match operation.mode {
            WriteMode::Replace if !table.emptying.delete => Some(denied(ErrorPhase::Write)),
            WriteMode::TruncateInsert if !table.emptying.truncate => {
                Some(denied(ErrorPhase::Write))
            }
            _ => None,
        }
    };
    let columns = statement_columns(operation, schema);
    // Una colonna assente rompe la preparazione dello statement; con
    // l'evoluzione dello schema la scrittura l'avrebbe aggiunta.
    if !evolves
        && columns
            .iter()
            .any(|(name, _)| !table.columns.contains_key(name))
    {
        verdict.at_statement = Some(missing_column());
        return Ok(verdict);
    }
    let lacks = columns.iter().any(|(name, need)| {
        table
            .columns
            .get(name)
            .is_some_and(|privileges| match need {
                Need::Insert => !privileges.insert,
                Need::Update => !privileges.update,
                Need::Select => !privileges.select,
            })
    }) || (operation.mode == WriteMode::DeleteByKeys && !table.emptying.delete);
    // `ON CONFLICT` sceglie l'arbitro pianificando, prima che l'esecutore
    // controlli i privilegi.
    // La scrittura riceve 42P10, che il provider non mappa: la sua categoria
    // e `protocol`, e il dry run la ripete.
    let arbiter_error = (operation.mode == WriteMode::Upsert
        && !operation.keys.is_empty()
        && !arbiter(client, table.oid, &operation.keys).await?)
        .then(|| {
            public_error(
                ErrorCategory::Protocol,
                ErrorPhase::Write,
                false,
                "nessun indice unico immediato sulle chiavi dell'upsert nel target PostgreSQL",
            )
        });
    match path {
        // `COPY` controlla i privilegi all'apertura, che la scrittura
        // classifica in fase `prepare`.
        WritePath::Copy => {
            verdict.at_statement = lacks.then(|| denied(ErrorPhase::Prepare));
        }
        WritePath::RowScoped | WritePath::Parameters => {
            verdict.at_first_row =
                arbiter_error.or_else(|| lacks.then(|| denied(ErrorPhase::Write)));
        }
    }
    Ok(verdict)
}
