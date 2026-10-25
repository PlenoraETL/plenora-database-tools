//! La regola R su PostgreSQL: i fatti del catalogo che dicono se gli
//! statement di una mode (il `DELETE` e l'INSERT di Replace, l'INSERT di
//! Append e `TruncateInsert`, l'INSERT ... ON CONFLICT DO UPDATE di Upsert,
//! l'`UPDATE` di Update, il `DELETE` di `DeleteByKeys`) arrivano al target
//! cosi come sono e, se distruttivi, ne vedono tutte le righe.
//!
//! La decisione e di `plenora_database_core::row_visibility`; qui si leggono i
//! fatti, sulla chiusura del target: il target, le relazioni sotto una vista
//! (le dipendenze della sua regola `_RETURN` in `pg_depend`, ricorsivamente)
//! e i figli di ereditarieta e le partizioni (`pg_inherits`), su cui un
//! `DELETE` del padre scende e i cui trigger di riga scattano.
//!
//! - sicurezza di riga: `row_security_active` del target per il ruolo; per
//!   ogni altra relazione della chiusura basta `relrowsecurity`, perche chi ne
//!   e soggetto (il proprietario della vista, o l'invocante con
//!   `security_invoker`) non si decide qui;
//! - regole di riscrittura per uno degli eventi della mode
//!   (`pg_rewrite.ev_type` '3' INSERT, '2' UPDATE, '4' DELETE), abilitate,
//!   `DO INSTEAD` e `DO ALSO`: `COPY` non applica regole, quindi anche una
//!   `DO ALSO` farebbe della stessa scrittura due cose diverse secondo il
//!   profilo;
//! - trigger `INSTEAD OF` per uno degli eventi della mode (esistono soltanto
//!   sulle viste): il conteggio dello statement conta le righe per cui la
//!   funzione restituisce una riga, che le abbia scritte o no;
//! - trigger di riga `BEFORE` o `INSTEAD OF` per l'evento distruttivo, non
//!   interni e non disabilitati (anche quelli che scattano soltanto in
//!   modalita replica: la sessione puo cambiarla). Un `BEFORE INSERT` che
//!   restituisce `NULL` non conta: la riga non e scritta e non e contata;
//! - tabelle esterne nella chiusura: filtri e riscritture li decide il server
//!   remoto;
//! - il tipo del target: `Replace` su una vista si rifiuta sempre.
//!
//! Il target si risolve come lo risolve lo statement: per nome con lo schema
//! dichiarato (senza i controlli di `USAGE`, che restano dello statement), con
//! il `search_path` senza schema. Un target che non si risolve non ha fatti:
//! lo statement lo rifiutera da se.

use crate::error::classify_error;
use plenora_database_core::plan::{ObjectRef, ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::row_visibility::{
    destructive_event, mode_events, refuse_unfaithful_write, DestructiveEvent, RowFilters,
    WriteEvent,
};
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use tokio_postgres::GenericClient;

const FACTS_SQL: &str = "\
WITH RECURSIVE target AS (
    SELECT CASE WHEN $1::text IS NULL THEN to_regclass(quote_ident($2::text))::oid
           ELSE (SELECT c.oid FROM pg_catalog.pg_class c
                 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = $1::text AND c.relname = $2::text) END AS oid
), closure(oid, via_view) AS (
    SELECT oid, false FROM target WHERE oid IS NOT NULL
    UNION
    SELECT x.child, x.via_view FROM closure cl CROSS JOIN LATERAL (
        SELECT d.refobjid AS child, true AS via_view
          FROM pg_catalog.pg_rewrite r
          JOIN pg_catalog.pg_depend d
            ON d.classid = 'pg_catalog.pg_rewrite'::regclass AND d.objid = r.oid
           AND d.refclassid = 'pg_catalog.pg_class'::regclass AND d.refobjid <> cl.oid
         WHERE r.ev_class = cl.oid AND r.ev_type = '1'
        UNION ALL
        SELECT i.inhrelid, cl.via_view FROM pg_catalog.pg_inherits i WHERE i.inhparent = cl.oid
    ) x
)
SELECT
    EXISTS (SELECT 1 FROM target WHERE oid IS NOT NULL),
    coalesce((SELECT c.relkind::text FROM pg_catalog.pg_class c JOIN target t ON c.oid = t.oid), ''),
    coalesce((SELECT row_security_active(t.oid) FROM target t WHERE t.oid IS NOT NULL), false),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_class c ON c.oid = cl.oid
             WHERE cl.oid <> (SELECT oid FROM target) AND c.relrowsecurity),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_class c ON c.oid = cl.oid
             WHERE cl.via_view AND c.relrowsecurity),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_rewrite r ON r.ev_class = cl.oid
             WHERE r.ev_type::text = ANY ($3::text[]) AND r.ev_enabled <> 'D'),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_trigger g ON g.tgrelid = cl.oid
             WHERE NOT g.tgisinternal AND g.tgenabled <> 'D'
               AND (g.tgtype::int & $4::int) <> 0
               AND ((g.tgtype::int & 3) = 3 OR (g.tgtype::int & 64) <> 0)),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_trigger g ON g.tgrelid = cl.oid
             WHERE NOT g.tgisinternal AND g.tgenabled <> 'D'
               AND (g.tgtype::int & $5::int) <> 0 AND (g.tgtype::int & 64) <> 0),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_class c ON c.oid = cl.oid
             WHERE c.relkind = 'f')";

/// I fatti del catalogo sul target, per gli eventi di una mode.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default)]
pub struct TargetFacts {
    /// Il target si risolve.
    pub exists: bool,
    /// `pg_class.relkind` del target.
    pub kind: char,
    /// `row_security_active` del target per il ruolo corrente.
    pub target_row_security: bool,
    /// RLS abilitata su un'altra relazione della chiusura.
    pub related_row_security: bool,
    /// RLS abilitata su una relazione sotto una vista: un INSERT attraverso
    /// la vista passa dalle sue policy.
    pub view_base_row_security: bool,
    /// Una regola per uno degli eventi della mode.
    pub rewrite_rule: bool,
    /// Un trigger di riga `BEFORE` o `INSTEAD OF` per l'evento distruttivo.
    pub row_trigger: bool,
    /// Un trigger `INSTEAD OF` per uno degli eventi della mode.
    pub instead_trigger: bool,
    pub foreign_relation: bool,
}

impl TargetFacts {
    const fn filters(self) -> RowFilters {
        RowFilters {
            row_security: self.target_row_security || self.related_row_security,
            rewrite_rule: self.rewrite_rule,
            instead_trigger: self.instead_trigger,
            row_trigger: self.row_trigger,
            // `COPY` non salta vincoli ne trigger: scavalca soltanto le
            // regole, che la regola rifiuta gia per ogni profilo.
            path_bypass: false,
            view: self.kind == 'v',
            opaque_relation: self.foreign_relation,
            unreadable: false,
        }
    }
}

/// I bit di `pg_trigger.tgtype` per un evento (`TRIGGER_TYPE_INSERT`,
/// `_DELETE`, `_UPDATE`) e il valore di `pg_rewrite.ev_type`.
const fn event_codes(event: WriteEvent) -> (i32, &'static str) {
    match event {
        WriteEvent::Insert => (4, "3"),
        WriteEvent::Delete => (8, "4"),
        WriteEvent::Update => (16, "2"),
    }
}

/// I fatti del target per gli eventi di `mode`, letti con `client` (la
/// transazione della scrittura, o la sessione del dry run). Gli errori del
/// catalogo escono in `phase`.
pub async fn facts<C: GenericClient + Sync>(
    client: &C,
    target: &ObjectRef,
    mode: WriteMode,
    phase: ErrorPhase,
) -> Result<TargetFacts> {
    let events = mode_events(mode);
    let rules = events
        .iter()
        .map(|event| event_codes(*event).1)
        .collect::<Vec<_>>();
    let instead = events
        .iter()
        .fold(0_i32, |mask, event| mask | event_codes(*event).0);
    let destructive = match destructive_event(mode) {
        Some(DestructiveEvent::Delete) => event_codes(WriteEvent::Delete).0,
        Some(DestructiveEvent::Update) => event_codes(WriteEvent::Update).0,
        None => 0,
    };
    let row = client
        .query_one(
            FACTS_SQL,
            &[
                &target.schema,
                &target.object,
                &rules,
                &destructive,
                &instead,
            ],
        )
        .await
        .map_err(|error| classify_error(phase, &error))?;
    let kind: String = row.get(1);
    Ok(TargetFacts {
        exists: row.get(0),
        kind: kind.chars().next().unwrap_or('?'),
        target_row_security: row.get(2),
        related_row_security: row.get(3),
        view_base_row_security: row.get(4),
        rewrite_rule: row.get(5),
        row_trigger: row.get(6),
        instead_trigger: row.get(7),
        foreign_relation: row.get(8),
    })
}

/// Il rifiuto della regola R per `operation` sui fatti gia letti, nella
/// fase della scrittura.
#[must_use]
pub fn refusal(operation: &WriteOperation, facts: TargetFacts) -> Option<DatabaseError> {
    if !facts.exists {
        return None;
    }
    refuse_unfaithful_write(
        ProviderKind::Postgres,
        operation.mode,
        ErrorPhase::Write,
        facts.filters(),
    )
    .err()
}

/// La regola R nella transazione della scrittura: legge i fatti e rifiuta.
///
/// Si chiama prima del primo statement della mode, e di nuovo dopo che uno
/// statement ha preso il lock del target (`ROW EXCLUSIVE`, in conflitto con
/// `CREATE POLICY`, `CREATE RULE`, `CREATE TRIGGER`): la seconda lettura vede
/// i DDL concorrenti confermati prima del lock, che la prima non vedeva, e un
/// rifiuto a quel punto riporta indietro la transazione. Vale con
/// l'isolamento `READ COMMITTED` della scrittura: un
/// `default_transaction_isolation` piu alto del server legge il catalogo con
/// lo snapshot della transazione, e la seconda lettura resta quella della
/// prima (hazard del DDL concorrente, dichiarato in
/// `plenora_database_core::row_visibility`).
///
/// # Errors
///
/// Il rifiuto della regola, o l'errore della lettura del catalogo.
pub async fn enforce<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
) -> Result<()> {
    if mode_events(operation.mode).is_empty() {
        return Ok(());
    }
    let facts = facts(client, &operation.target, operation.mode, ErrorPhase::Write).await?;
    refusal(operation, facts).map_or(Ok(()), Err)
}
