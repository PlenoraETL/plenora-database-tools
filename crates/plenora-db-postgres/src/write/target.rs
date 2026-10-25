//! Cio che la scrittura chiede al target, letto in sola lettura per il dry
//! run: un **elenco chiuso** di condizioni che la scrittura rifiuterebbe in
//! modo deterministico, ciascuna con la categoria e la fase della scrittura e
//! **nel punto in cui la scrittura la darebbe**: prima dell'ingresso (DDL,
//! `DELETE`/`TRUNCATE`, `ALTER`, registrazione degli id), alla preparazione
//! dello statement o all'apertura di `COPY`, o alla prima riga eseguita.
//!
//! L'elenco:
//!
//! 1. schema: esistenza e `CREATE` (Create), `USAGE` (ogni altra mode);
//! 2. tabella: esistenza (gia di `prepare_write`), proprieta per `ALTER` e
//!    `COMMENT`, `LOCK` della registrazione degli id;
//! 3. privilegi di tabella e di colonna: `INSERT`, `UPDATE`, `DELETE`,
//!    `TRUNCATE`, e `SELECT` su ogni colonna che lo statement legge — le
//!    chiavi di `WHERE`, le chiavi che arbitrano un upsert e le colonne che
//!    `DO UPDATE SET c = EXCLUDED.c` legge;
//! 4. row-level security attiva per il ruolo (`row_security_active`): `COPY
//!    FROM` e rifiutato sempre, un INSERT senza policy permissiva di INSERT
//!    per il ruolo rifiuta ogni riga; dove l'esito dipende dai valori o dalle
//!    righe visibili (policy presenti, Update, Delete, Upsert, Replace con
//!    INSERT) il dry run non lo decide e lo dichiara
//!    ([`RowSecurity::NotPerformed`]);
//! 5. colonne dell'ingresso che non ricevono valori: `GENERATED ALWAYS ...
//!    STORED` (INSERT, UPDATE e `COPY`), `IDENTITY ALWAYS` (INSERT e UPDATE;
//!    `COPY` lo scavalca e scrive il valore dell'ingresso);
//! 6. aggiornabilita del target: vista non aggiornabile per il comando
//!    (`pg_relation_is_updatable`, trigger `INSTEAD OF` compresi), `COPY` su
//!    una relazione che non e una tabella, `TRUNCATE` su una vista;
//! 7. colonne `NOT NULL` senza default (ne identita ne generate) assenti
//!    dall'ingresso, nelle mode che inseriscono;
//! 8. l'indice unico immediato che arbitra `ON CONFLICT`;
//! 9. colonne che lo statement nomina, presenti nel target.
//!
//! Resta della scrittura, e il dry run lo dichiara (`checks.server:
//! not_performed`), cio che il catalogo non decide: trigger, vincoli `CHECK`
//! ed esclusioni, unicita e foreign key sui valori, vincoli differiti,
//! conversioni della colonna, timeout e lock concorrenti, le colonne `NOT
//! NULL` di una tabella sotto una vista.
//!
//! Fino al 25/10 il dry run con la DSN diceva `dry_run` dove la scrittura
//! rifiutava per le classi 4-7 e per `SELECT` sulle colonne di un upsert
//! (quarta rilettura). La scrittura pubblicava alcuni di questi rifiuti con
//! una categoria che non diceva che cosa era fallito: `protocol` per 42P10
//! (nessun arbitro, colonna generata in `COPY`) e per 55000/42809 (vista non
//! aggiornabile), `conflict` per 428C9 (colonna generata o `IDENTITY ALWAYS`
//! in un INSERT). Ora sono `invalid_plan` e `unsupported`, e il dry run li
//! costruisce dalla stessa tabella degli SQLSTATE
//! (`crate::error::sqlstate_error`).

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

/// Se una policy di row-level security decide sull'ingresso.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RowSecurity {
    /// RLS non si applica al ruolo sul target (o il target e nuovo).
    #[default]
    NotApplicable,
    /// RLS si applica e l'esito dipende dai valori o dalle righe visibili:
    /// il dry run non lo decide, e lo dice.
    NotPerformed,
}

impl RowSecurity {
    /// Il valore di `checks.row_security` nella busta del dry run.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::NotPerformed => "not_performed",
        }
    }
}

/// Il rifiuto alla prima riga eseguita.
#[derive(Debug)]
pub enum FirstRow {
    /// Un errore del server che la scrittura pubblica cosi com'e.
    Error(DatabaseError),
    /// Una colonna `NOT NULL` senza default assente dall'ingresso: la prima
    /// riga viola il vincolo. Il percorso row-scoped la pubblica come riga
    /// rifiutata (`data_mapping`, con la diagnostica), gli altri come la
    /// violazione che il server riporta (`conflict`).
    NotNullViolation,
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
    pub at_first_row: Option<FirstRow>,
    /// La registrazione degli id dei campi puo scrivere `COMMENT ON
    /// COLUMN`: serve la proprieta della tabella.
    pub owner: bool,
    /// `LOCK TABLE ... SHARE ROW EXCLUSIVE` della registrazione.
    pub may_lock: bool,
    /// Se una policy RLS decide sull'ingresso.
    pub row_security: RowSecurity,
}

/// Il rifiuto che la scrittura pubblica per lo SQLSTATE `code` nella fase
/// `phase`: la stessa tabella (`crate::error::sqlstate_error`), quindi la
/// stessa categoria e lo stesso messaggio.
fn server(code: &str, phase: ErrorPhase) -> DatabaseError {
    crate::error::sqlstate_error(code, phase)
}

/// 42501: permesso insufficiente (tabella, colonna, schema, riga RLS).
fn denied(phase: ErrorPhase) -> DatabaseError {
    server("42501", phase)
}

fn missing_column() -> DatabaseError {
    public_error(
        ErrorCategory::NotFound,
        ErrorPhase::Prepare,
        false,
        "colonna del piano assente nel target PostgreSQL",
    )
}

/// I privilegi e la forma di una colonna esistente.
///
/// Fatti indipendenti del catalogo, uno per condizione dell'elenco: un enum
/// li fonderebbe in stati che il catalogo non conosce.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default)]
struct Column {
    insert: bool,
    update: bool,
    select: bool,
    /// `GENERATED ALWAYS AS (...) STORED`.
    generated: bool,
    /// `GENERATED ALWAYS AS IDENTITY`.
    identity_always: bool,
    /// `NOT NULL` senza default, ne identita ne generata.
    required: bool,
}

/// I privilegi con cui la scrittura svuota il target (Replace, `TruncateInsert`).
#[derive(Debug, Clone, Copy)]
struct Emptying {
    delete: bool,
    truncate: bool,
}

/// I comandi che il target ammette (`pg_relation_is_updatable`, con i
/// trigger `INSTEAD OF`).
#[derive(Debug, Clone, Copy)]
struct Updatable {
    insert: bool,
    update: bool,
    delete: bool,
}

/// Il target esistente, dal catalogo: come per [`Column`], un fatto per
/// condizione.
#[allow(clippy::struct_excessive_bools)]
struct Table {
    oid: u32,
    owner: bool,
    lock: bool,
    schema_usage: bool,
    /// `pg_class.relkind`.
    kind: char,
    /// `row_security_active`: RLS si applica al ruolo corrente.
    row_security: bool,
    /// Una policy permissiva di INSERT si applica al ruolo.
    insert_policy: bool,
    /// Un trigger `INSTEAD OF INSERT` per riga: `COPY` su una vista lo usa.
    instead_of_insert: bool,
    updatable: Updatable,
    emptying: Emptying,
    columns: BTreeMap<String, Column>,
}

impl Table {
    /// `COPY FROM` ammette una tabella, una tabella partizionata, una
    /// tabella esterna che accetta INSERT e una vista con un trigger
    /// `INSTEAD OF INSERT`.
    const fn copy_target(&self) -> bool {
        match self.kind {
            'r' | 'p' => true,
            'f' => self.updatable.insert,
            'v' => self.instead_of_insert,
            _ => false,
        }
    }

    /// `TRUNCATE` ammette tabelle, tabelle partizionate e tabelle esterne
    /// (queste ultime decise dal wrapper, che il catalogo non dice: si
    /// lasciano alla scrittura).
    const fn truncate_target(&self) -> bool {
        matches!(self.kind, 'r' | 'p' | 'f')
    }
}

#[allow(clippy::too_many_lines)]
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
                       THEN has_table_privilege(c.oid, 'MAINTAIN') ELSE false END, \
                    has_schema_privilege(n.oid, 'USAGE'), \
                    c.relkind::text, \
                    row_security_active(c.oid), \
                    EXISTS (SELECT 1 FROM pg_catalog.pg_policy p \
                            WHERE p.polrelid = c.oid AND p.polpermissive \
                              AND p.polcmd IN ('a', '*') \
                              AND (0::oid = ANY (p.polroles) \
                                   OR EXISTS (SELECT 1 FROM unnest(p.polroles) r \
                                              WHERE r <> 0 AND pg_has_role(r, 'USAGE')))), \
                    EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t \
                            WHERE t.tgrelid = c.oid AND NOT t.tgisinternal \
                              AND t.tgenabled <> 'D' \
                              AND (t.tgtype & 64) <> 0 AND (t.tgtype & 4) <> 0 \
                              AND (t.tgtype & 1) <> 0), \
                    pg_relation_is_updatable(c.oid, true) \
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
                    has_column_privilege(a.attrelid, a.attnum, 'SELECT'), \
                    a.attgenerated = 's', \
                    a.attidentity = 'a', \
                    a.attnotnull AND NOT a.atthasdef AND a.attidentity = '' \
                      AND a.attgenerated = '' \
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
                Column {
                    insert: row.get(1),
                    update: row.get(2),
                    select: row.get(3),
                    generated: row.get(4),
                    identity_always: row.get(5),
                    required: row.get(6),
                },
            )
        })
        .collect();
    let kind: String = row.get(6);
    let updatable: i32 = row.get(10);
    Ok(Some(Table {
        oid,
        owner: row.get(1),
        lock: row.get(4),
        schema_usage: row.get(5),
        kind: kind.chars().next().unwrap_or('?'),
        row_security: row.get(7),
        insert_policy: row.get(8),
        instead_of_insert: row.get(9),
        // I bit di `pg_relation_is_updatable`: `1 << CMD_UPDATE`,
        // `1 << CMD_INSERT`, `1 << CMD_DELETE`.
        updatable: Updatable {
            update: updatable & 4 != 0,
            insert: updatable & 8 != 0,
            delete: updatable & 16 != 0,
        },
        emptying: Emptying {
            delete: row.get(2),
            truncate: row.get(3),
        },
        columns,
    }))
}

/// L'indice che `ON CONFLICT (chiavi)` sceglierebbe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Arbiter {
    /// Unico, immediato, valido, senza predicato ne espressioni, con le
    /// colonne chiave esattamente quelle del piano.
    Immediate,
    /// Soltanto un vincolo differibile sulle chiavi: il server lo trova e lo
    /// rifiuta come arbitro (55000, «ON CONFLICT does not support deferrable
    /// unique constraints/exclusion constraints as arbiters»; misurato su
    /// PostgreSQL 16, scenario `upsert-unique-differibile` del runner).
    DeferrableOnly,
    /// Nessuno: 42P10.
    Missing,
}

async fn arbiter(client: &Client, oid: u32, keys: &[String]) -> Result<Arbiter> {
    let row = client
        .query_one(
            "SELECT bool_or(i.indimmediate), count(*) > 0 \
             FROM pg_catalog.pg_index i \
             WHERE i.indrelid = $1 AND i.indisunique AND i.indisvalid \
               AND i.indpred IS NULL AND i.indexprs IS NULL \
               AND (SELECT array_agg(DISTINCT a.attname::text ORDER BY a.attname::text) \
                    FROM unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, position) \
                    JOIN pg_catalog.pg_attribute a \
                      ON a.attrelid = i.indrelid AND a.attnum = k.attnum \
                    WHERE k.position <= i.indnkeyatts) \
                   = (SELECT array_agg(DISTINCT key ORDER BY key) FROM unnest($2::text[]) key)",
            &[&oid, &keys],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Prepare, &error))?;
    Ok(
        match (row.get::<_, Option<bool>>(0), row.get::<_, bool>(1)) {
            (Some(true), _) => Arbiter::Immediate,
            (_, true) => Arbiter::DeferrableOnly,
            _ => Arbiter::Missing,
        },
    )
}

/// Le colonne che lo statement della mode nomina, con il privilegio che
/// chiede su ciascuna, nell'ordine in cui lo statement le nomina.
fn statement_columns(operation: &WriteOperation, schema: &SchemaRef) -> Vec<(String, Need)> {
    let all = || schema.fields().iter().map(|field| field.name().clone());
    let keys = || operation.keys.iter().cloned();
    match operation.mode {
        WriteMode::Update => operation
            .update_columns
            .iter()
            .map(|name| (name.clone(), Need::Update))
            .chain(keys().map(|name| (name, Need::Select)))
            .collect(),
        WriteMode::DeleteByKeys => keys().map(|name| (name, Need::Select)).collect(),
        // `INSERT ... ON CONFLICT (chiavi) DO UPDATE SET c = EXCLUDED.c`:
        // INSERT su ogni colonna, SELECT sulle chiavi che arbitrano (anche
        // con `DO NOTHING`), UPDATE e SELECT su ogni colonna aggiornata, che
        // `EXCLUDED.c` legge (misurato su PostgreSQL 16: senza SELECT sulla
        // colonna aggiornata la prima riga e rifiutata con 42501).
        WriteMode::Upsert => {
            let updated = || all().filter(|name| !operation.keys.contains(name));
            all()
                .map(|name| (name, Need::Insert))
                .chain(keys().map(|name| (name, Need::Select)))
                .chain(updated().map(|name| (name, Need::Update)))
                .chain(updated().map(|name| (name, Need::Select)))
                .collect()
        }
        _ => all().map(|name| (name, Need::Insert)).collect(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Need {
    Insert,
    Update,
    Select,
}

/// La mode inserisce righe nel target esistente.
const fn inserts(mode: WriteMode) -> bool {
    matches!(
        mode,
        WriteMode::Append | WriteMode::Replace | WriteMode::TruncateInsert | WriteMode::Upsert
    )
}

/// Il verdetto del target per la scrittura di `operation` lungo `path`.
///
/// # Errors
///
/// Soltanto gli errori delle letture del catalogo; i rifiuti che la
/// scrittura darebbe stanno nel verdetto.
#[allow(clippy::too_many_lines)]
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
    // Senza USAGE sullo schema ogni statement sulla tabella e rifiutato, anche
    // il LOCK e il COMMENT della registrazione degli id.
    verdict.owner = table.owner && table.schema_usage;
    verdict.may_lock = table.lock && table.schema_usage;
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
    // proprietario), poi svuotamento del target. Il primo statement che
    // nomina la tabella incontra lo schema senza USAGE.
    let empties = matches!(
        operation.mode,
        WriteMode::Replace | WriteMode::TruncateInsert
    );
    verdict.before_input =
        if (!table.schema_usage && (evolves || empties)) || (evolves && !table.owner) {
            Some(denied(ErrorPhase::Write))
        } else {
            // La riscrittura del DELETE (vista non aggiornabile) precede il
            // controllo dei privilegi dell'esecutore; `TRUNCATE` controlla il
            // tipo della relazione prima dei privilegi.
            match operation.mode {
                WriteMode::Replace if !table.updatable.delete => {
                    Some(server("55000", ErrorPhase::Write))
                }
                WriteMode::Replace if !table.emptying.delete => Some(denied(ErrorPhase::Write)),
                WriteMode::TruncateInsert if !table.truncate_target() => {
                    Some(server("42809", ErrorPhase::Write))
                }
                WriteMode::TruncateInsert if !table.emptying.truncate => {
                    Some(denied(ErrorPhase::Write))
                }
                _ => None,
            }
        };
    let columns = statement_columns(operation, schema);
    // Lo statement si prepara (o `COPY` si apre) dopo l'ingresso letto: lo
    // schema senza USAGE e il primo rifiuto, alla risoluzione del nome.
    if !table.schema_usage {
        verdict.at_statement = Some(denied(ErrorPhase::Prepare));
        return Ok(verdict);
    }
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
    // Le colonne che non ricevono valori: `COPY` rifiuta le generate (non
    // `IDENTITY ALWAYS`, che scavalca); INSERT e UPDATE entrambe.
    let receives_no_value = columns.iter().any(|(name, need)| {
        table.columns.get(name).is_some_and(|column| match need {
            Need::Insert if path == WritePath::Copy => column.generated,
            Need::Insert | Need::Update => column.generated || column.identity_always,
            Need::Select => false,
        })
    });
    let lacks = columns.iter().any(|(name, need)| {
        table.columns.get(name).is_some_and(|column| match need {
            Need::Insert => !column.insert,
            Need::Update => !column.update,
            Need::Select => !column.select,
        })
    }) || (operation.mode == WriteMode::DeleteByKeys && !table.emptying.delete);
    // Il comando sul target: una vista che non si aggiorna da sola e senza
    // trigger `INSTEAD OF` lo rifiuta alla riscrittura dello statement.
    let command_refused = match operation.mode {
        WriteMode::Update => !table.updatable.update,
        WriteMode::DeleteByKeys => !table.updatable.delete,
        WriteMode::Upsert => !(table.updatable.insert && table.updatable.update),
        _ if path == WritePath::Copy => !table.copy_target(),
        _ => !table.updatable.insert,
    };
    // Una colonna obbligatoria che l'ingresso non porta: la prima riga.
    let names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let required_missing = inserts(operation.mode)
        && table
            .columns
            .iter()
            .any(|(name, column)| column.required && !names.contains(name.as_str()));
    // `ON CONFLICT` sceglie l'arbitro pianificando, prima che l'esecutore
    // controlli i privilegi: 42P10 senza arbitro (`invalid_plan`), 55000 se
    // c'e soltanto un vincolo differibile (`unsupported`).
    let arbiter_error = if operation.mode == WriteMode::Upsert && !operation.keys.is_empty() {
        match arbiter(client, table.oid, &operation.keys).await? {
            Arbiter::Immediate => None,
            Arbiter::DeferrableOnly => Some(server("55000", ErrorPhase::Write)),
            Arbiter::Missing => Some(server("42P10", ErrorPhase::Write)),
        }
    } else {
        None
    };
    // Row-level security: `COPY FROM` e rifiutato sempre; un INSERT senza
    // policy permissiva di INSERT rifiuta ogni riga. Il resto dipende dai
    // valori o dalle righe visibili, e si dichiara.
    let insert_only = matches!(
        operation.mode,
        WriteMode::Append | WriteMode::TruncateInsert
    ) || (operation.mode == WriteMode::Replace && path != WritePath::Parameters);
    let row_security_refuses = table.row_security && insert_only && !table.insert_policy;
    if table.row_security && !(path == WritePath::Copy || row_security_refuses) {
        verdict.row_security = RowSecurity::NotPerformed;
    }
    match path {
        // `COPY`: le colonne (assenti, generate) prima dei privilegi, poi
        // row-level security, poi il tipo della relazione (PostgreSQL 16,
        // `DoCopy` e `BeginCopyFrom`). La violazione NOT NULL arriva con i
        // dati.
        WritePath::Copy => {
            verdict.at_statement = if receives_no_value {
                Some(server("42P10", ErrorPhase::Prepare))
            } else if lacks {
                Some(denied(ErrorPhase::Prepare))
            } else if table.row_security {
                // «COPY FROM not supported with row-level security».
                Some(server("0A000", ErrorPhase::Prepare))
            } else {
                None
            };
            // «cannot copy to view» arriva dopo l'apertura, con i dati
            // (misurato su PostgreSQL 16: fase `write`).
            verdict.at_first_row = if command_refused {
                Some(FirstRow::Error(server("42809", ErrorPhase::Write)))
            } else {
                required_missing.then_some(FirstRow::NotNullViolation)
            };
        }
        // INSERT, UPDATE, DELETE preparati: la riscrittura rifiuta colonne
        // senza valori e viste non aggiornabili; la pianificazione l'arbitro,
        // l'esecutore i privilegi, la prima riga RLS e NOT NULL.
        WritePath::RowScoped | WritePath::Parameters => {
            verdict.at_statement = if command_refused {
                // «cannot insert into view», «cannot update view», ...
                Some(server("55000", ErrorPhase::Prepare))
            } else if receives_no_value {
                // «cannot insert a non-DEFAULT value into column».
                Some(server("428C9", ErrorPhase::Prepare))
            } else {
                None
            };
            // «new row violates row-level security policy»: 42501.
            verdict.at_first_row = arbiter_error
                .or_else(|| (lacks || row_security_refuses).then(|| denied(ErrorPhase::Write)))
                .map(FirstRow::Error)
                .or_else(|| required_missing.then_some(FirstRow::NotNullViolation));
        }
    }
    Ok(verdict)
}
