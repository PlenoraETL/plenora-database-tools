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
//! - il tipo del target: `Replace` su una vista si rifiuta sempre;
//! - la regola W1 (fedelta del percorso): con `COPY`, una colonna `IDENTITY
//!   ALWAYS` che l'ingresso porta (l'elenco chiuso qui sotto).
//!
//! E la regola W2 (sessione fissata): [`pin_session`] riporta
//! `session_replication_role` a `origin` nella transazione della scrittura,
//! o la rifiuta, con il percorso gia chiuso dalla regola Q2
//! (`crate::search_path`).
//!
//! La regola K (`plenora_database_core::write_keys`): per Update e
//! `DeleteByKeys` le chiavi coincidono con un indice unico, non parziale e
//! immediato del target, letto con gli altri fatti; e la regola K2: quell'indice
//! confronta ogni chiave con la collation della colonna e con l'opclass btree
//! predefinita del suo tipo (`keys_unique`), e lo statement confronta le chiavi
//! con l'operatore di quell'opclass ([`key_equality`]). Upsert, che arbitra con
//! ogni indice unico sulle chiavi, si rifiuta se ha un arbitro e nessuno della
//! regola K2.
//!
//! # La regola W1 per `COPY`
//!
//! L'elenco **chiuso** delle garanzie che lo statement di riferimento
//! (l'INSERT preparato) applica e `COPY FROM` no, misurato su PostgreSQL 16
//! il 25/10.
//!
//! - le regole di riscrittura (`pg_rewrite`): rifiutate dalla regola R in
//!   ogni profilo, quindi qui non servono;
//! - `GENERATED ALWAYS AS IDENTITY` su una colonna che l'ingresso porta:
//!   l'INSERT rifiuta il valore (428C9), `COPY` lo scrive come con
//!   `OVERRIDING SYSTEM VALUE` e non avanza la sequenza. Fino al 25/10 le
//!   righe 10 e 11 finivano nella colonna.
//!
//! Il resto `COPY` lo applica come l'INSERT, provato in ogni profilo da
//! `write_semantics_live_tests`: vincoli `CHECK`, domini con vincoli (testo
//! e binario), unicita, foreign key (trigger di sistema), trigger di riga
//! `BEFORE` (anche quello che solleva o restituisce `NULL`) e `AFTER`,
//! trigger di statement, default e identita delle colonne assenti, colonne
//! generate (calcolate se assenti, rifiutate se nominate), partizioni. RLS
//! rifiuta `COPY FROM` con un errore (`target`).
//!
//! Il target si risolve come lo risolve lo statement: per nome con lo schema,
//! sempre dichiarato (regola T, `crate::target_schema`; senza i controlli di
//! `USAGE`, che restano dello statement). Un target che non si risolve non ha
//! fatti: lo statement lo rifiutera da se.

use super::target::WritePath;
use crate::error::classify_error;
use plenora_database_core::plan::{ProviderKind, WriteMode, WriteOperation};
use plenora_database_core::row_visibility::{
    destructive_event, mode_events, refuse_unfaithful_write, DestructiveEvent, RowFilters,
    WriteEvent, SESSION_WITHOUT_GUARANTEES,
};
use plenora_database_core::write_keys::keys_without_unique_index;
use plenora_database_core::{DatabaseError, ErrorPhase, Result};
use tokio_postgres::GenericClient;

/// L'indice unico sulle chiavi (il parametro `$keys`), per la forma (regola K): unico,
/// immediato, valido, pronto e vivo, senza predicato ne espressioni, con le
/// colonne chiave esattamente le chiavi, e senza figli di ereditarieta fuori
/// dall'indice (una tabella partizionata si: l'indice vale su ogni
/// partizione). Su `pg_catalog.pg_index i` e `pg_catalog.pg_class c` del
/// target; lo SQL gira con il percorso chiuso (regola Q2).
macro_rules! keys_index {
    ($keys:literal) => {
        concat!(
            "i.indisunique AND i.indimmediate AND i.indisvalid AND i.indisready AND i.indislive
               AND i.indpred IS NULL AND i.indexprs IS NULL
               AND (c.relkind = 'p'
                    OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_inherits h WHERE h.inhparent = c.oid))
               AND (SELECT pg_catalog.array_agg(DISTINCT a.attname::pg_catalog.text
                                                ORDER BY a.attname::pg_catalog.text)
                      FROM pg_catalog.unnest(i.indkey::pg_catalog.int2[]) WITH ORDINALITY AS k(attnum, position)
                      JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                     WHERE k.position <= i.indnkeyatts)
                   OPERATOR(pg_catalog.=) (SELECT pg_catalog.array_agg(DISTINCT key ORDER BY key)
                        FROM pg_catalog.unnest(",
            $keys,
            "::pg_catalog.text[]) key)"
        )
    };
}

/// La regola K2 sull'indice `i`: per ogni colonna chiave la collation
/// dell'indice e quella della colonna (`indcollation` = `attcollation`) e
/// l'opclass e quella btree predefinita per il tipo della colonna (per un
/// dominio, del suo tipo base; un dominio su un dominio non si risolve e non
/// prova). L'uguaglianza che rende unica la chiave e allora quella della
/// colonna, e quella con cui lo statement la confronta (`KEY_EQUALITY_SQL`).
macro_rules! key_semantics {
    () => {
        "NOT EXISTS (
            SELECT 1
              FROM ROWS FROM (pg_catalog.unnest(i.indkey::pg_catalog.int2[]),
                              pg_catalog.unnest(i.indcollation::pg_catalog.oid[]),
                              pg_catalog.unnest(i.indclass::pg_catalog.oid[]))
                   WITH ORDINALITY AS k(attnum, coll, opclass, position)
              JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
              JOIN pg_catalog.pg_type y ON y.oid = a.atttypid
              LEFT JOIN pg_catalog.pg_opclass oc ON oc.oid = k.opclass
             WHERE k.position <= i.indnkeyatts
               AND (k.coll IS DISTINCT FROM a.attcollation
                    OR oc.oid IS NULL OR NOT oc.opcdefault OR oc.opcmethod <> 403::pg_catalog.oid
                    OR (y.typtype = 'd' AND EXISTS (SELECT 1 FROM pg_catalog.pg_type b
                                                     WHERE b.oid = y.typbasetype AND b.typtype = 'd'))
                    OR EXISTS (SELECT 1 FROM pg_catalog.pg_opclass o
                                WHERE o.opcmethod = oc.opcmethod AND o.opcdefault AND o.oid <> oc.oid
                                  AND o.opcintype = CASE WHEN y.typtype = 'd' THEN y.typbasetype
                                                         ELSE y.oid END)))"
    };
}

/// Per ogni chiave, l'operatore di uguaglianza (strategia 3 della famiglia
/// btree) dell'opclass dell'indice che prova l'unicita, qualificato con il
/// suo schema, e il tipo, qualificato, a cui si portano i due lati del
/// confronto: quello dell'opclass, o il tipo base della colonna se l'opclass
/// e polimorfica (`anyarray`, `anyenum`, ...).
const KEY_EQUALITY_SQL: &str = concat!(
    "\
WITH target AS (
    SELECT c.oid FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text
)
SELECT DISTINCT ON (a.attname) a.attname::pg_catalog.text,
       pg_catalog.quote_ident(opn.nspname) || '.' || o.oprname::pg_catalog.text,
       pg_catalog.quote_ident(tn.nspname) || '.' || pg_catalog.quote_ident(t.typname)
  FROM target x
  JOIN pg_catalog.pg_class c ON c.oid = x.oid
  JOIN pg_catalog.pg_index i ON i.indrelid = x.oid
  CROSS JOIN LATERAL ROWS FROM (pg_catalog.unnest(i.indkey::pg_catalog.int2[]),
                                pg_catalog.unnest(i.indclass::pg_catalog.oid[]))
       WITH ORDINALITY AS k(attnum, opclass, position)
  JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
  JOIN pg_catalog.pg_type y ON y.oid = a.atttypid
  JOIN pg_catalog.pg_opclass oc ON oc.oid = k.opclass
  JOIN pg_catalog.pg_type it ON it.oid = oc.opcintype
  JOIN pg_catalog.pg_amop m ON m.amopfamily = oc.opcfamily AND m.amopmethod = oc.opcmethod
   AND m.amopstrategy = 3 AND m.amoplefttype = oc.opcintype AND m.amoprighttype = oc.opcintype
  JOIN pg_catalog.pg_operator o ON o.oid = m.amopopr
  JOIN pg_catalog.pg_namespace opn ON opn.oid = o.oprnamespace
  JOIN pg_catalog.pg_type t ON t.oid = CASE WHEN it.typtype = 'p'
                                            THEN CASE WHEN y.typtype = 'd' THEN y.typbasetype ELSE y.oid END
                                            ELSE oc.opcintype END
  JOIN pg_catalog.pg_namespace tn ON tn.oid = t.typnamespace
 WHERE k.position <= i.indnkeyatts AND ",
    keys_index!("$3"),
    " AND ",
    key_semantics!(),
    "
 ORDER BY a.attname, i.indexrelid"
);

const FACTS_SQL: &str = concat!(
    "\
WITH RECURSIVE target AS (
    SELECT (SELECT c.oid FROM pg_catalog.pg_class c
              JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text) AS oid
), closure(oid, via_view) AS (
    SELECT oid, false FROM target WHERE oid IS NOT NULL
    UNION
    SELECT x.child, x.via_view FROM closure cl CROSS JOIN LATERAL (
        SELECT d.refobjid AS child, true AS via_view
          FROM pg_catalog.pg_rewrite r
          JOIN pg_catalog.pg_depend d
            ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = r.oid
           AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid <> cl.oid
         WHERE r.ev_class = cl.oid AND r.ev_type = '1'
        UNION ALL
        SELECT i.inhrelid, cl.via_view FROM pg_catalog.pg_inherits i WHERE i.inhparent = cl.oid
    ) x
)
SELECT
    EXISTS (SELECT 1 FROM target WHERE oid IS NOT NULL),
    coalesce((SELECT c.relkind::pg_catalog.text FROM pg_catalog.pg_class c JOIN target t ON c.oid = t.oid), ''),
    coalesce((SELECT pg_catalog.row_security_active(t.oid) FROM target t WHERE t.oid IS NOT NULL), false),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_class c ON c.oid = cl.oid
             WHERE cl.oid <> (SELECT oid FROM target) AND c.relrowsecurity),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_class c ON c.oid = cl.oid
             WHERE cl.via_view AND c.relrowsecurity),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_rewrite r ON r.ev_class = cl.oid
             WHERE r.ev_type::pg_catalog.text = ANY ($3::pg_catalog.text[]) AND r.ev_enabled <> 'D'),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_trigger g ON g.tgrelid = cl.oid
             WHERE NOT g.tgisinternal AND g.tgenabled <> 'D'
               AND (g.tgtype::int & $4::int) <> 0
               AND ((g.tgtype::int & 3) = 3 OR (g.tgtype::int & 64) <> 0)),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_trigger g ON g.tgrelid = cl.oid
             WHERE NOT g.tgisinternal AND g.tgenabled <> 'D'
               AND (g.tgtype::int & $5::int) <> 0 AND (g.tgtype::int & 64) <> 0),
    EXISTS (SELECT 1 FROM closure cl JOIN pg_catalog.pg_class c ON c.oid = cl.oid
             WHERE c.relkind = 'f'),
    EXISTS (SELECT 1 FROM target t JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
             WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attidentity = 'a'
               AND a.attname::pg_catalog.text = ANY ($6::pg_catalog.text[])),
    EXISTS (SELECT 1 FROM target t
              JOIN pg_catalog.pg_class c ON c.oid = t.oid
              JOIN pg_catalog.pg_index i ON i.indrelid = t.oid
             WHERE ",
    keys_index!("$7"),
    "),
    EXISTS (SELECT 1 FROM target t
              JOIN pg_catalog.pg_class c ON c.oid = t.oid
              JOIN pg_catalog.pg_index i ON i.indrelid = t.oid
             WHERE ",
    keys_index!("$7"),
    " AND ",
    key_semantics!(),
    "),
    (SELECT pg_catalog.count(*)::pg_catalog.int8 FROM target t
       JOIN pg_catalog.pg_index i ON i.indrelid = t.oid
      WHERE i.indisunique
        AND (EXISTS (SELECT 1 FROM pg_catalog.unnest(i.indkey::pg_catalog.int2[]) AS k(attnum)
                       JOIN pg_catalog.pg_attribute a
                         ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                      WHERE a.attname::pg_catalog.text = ANY ($7::pg_catalog.text[]))
             OR EXISTS (SELECT 1 FROM pg_catalog.pg_depend d
                          JOIN pg_catalog.pg_attribute a
                            ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
                         WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                           AND d.objid = i.indexrelid
                           AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
                           AND d.refobjid = i.indrelid
                           AND a.attname::pg_catalog.text = ANY ($7::pg_catalog.text[]))))"
);

/// La guardia del `search_path` (regola Q): uno schema che precede
/// `pg_catalog` nel percorso effettivo (`current_schemas(true)`) e contiene
/// una funzione, un operatore o un tipo con il nome di uno di `pg_catalog`.
///
/// E scritta come ogni SQL della libreria (funzioni, relazioni e tipi
/// qualificati) e in piu con gli operatori qualificati. Copre gli operatori
/// non qualificati con operandi dello **stesso tipo esatto** di quelli di
/// `pg_catalog` (`oid = oid`, `name = text`): la corrispondenza esatta si
/// cerca lungo il percorso, e `pg_catalog` per primo la trova. Non copre un
/// operatore polimorfico (`anyarray = anyarray`) o raggiunto con un cast
/// implicito: un `public."="(text[], text[])` esatto vincerebbe anche dopo
/// `pg_catalog`. Quelli, nello SQL della libreria, sono qualificati con
/// `OPERATOR(pg_catalog.=)` o hanno gli operandi portati al tipo esatto
/// (seconda rilettura del 25/10), e nella scrittura girano con il percorso
/// chiuso (regola Q2, terza rilettura: `public.<>(oid, integer)` passava di
/// qui).
/// `11` e l'oid fisso di `pg_catalog` (`PG_CATALOG_NAMESPACE`).
const SEARCH_PATH_GUARD_SQL: &str = "\
SELECT EXISTS (
    SELECT 1
      FROM pg_catalog.unnest(pg_catalog.current_schemas(true)) WITH ORDINALITY AS s(name, position)
      JOIN pg_catalog.pg_namespace n ON n.nspname OPERATOR(pg_catalog.=) s.name
     WHERE n.oid OPERATOR(pg_catalog.<>) 11::pg_catalog.oid
       AND s.position OPERATOR(pg_catalog.<) (
           SELECT p.position
             FROM pg_catalog.unnest(pg_catalog.current_schemas(true)) WITH ORDINALITY AS p(name, position)
            WHERE p.name OPERATOR(pg_catalog.=) 'pg_catalog'::pg_catalog.name)
       AND (EXISTS (SELECT 1 FROM pg_catalog.pg_proc f
                     WHERE f.pronamespace OPERATOR(pg_catalog.=) n.oid
                       AND EXISTS (SELECT 1 FROM pg_catalog.pg_proc c
                                    WHERE c.pronamespace OPERATOR(pg_catalog.=) 11::pg_catalog.oid
                                      AND c.proname OPERATOR(pg_catalog.=) f.proname))
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_operator o
                        WHERE o.oprnamespace OPERATOR(pg_catalog.=) n.oid
                          AND EXISTS (SELECT 1 FROM pg_catalog.pg_operator c
                                       WHERE c.oprnamespace OPERATOR(pg_catalog.=) 11::pg_catalog.oid
                                         AND c.oprname OPERATOR(pg_catalog.=) o.oprname))
            OR EXISTS (SELECT 1 FROM pg_catalog.pg_type y
                        WHERE y.typnamespace OPERATOR(pg_catalog.=) n.oid
                          AND EXISTS (SELECT 1 FROM pg_catalog.pg_type c
                                       WHERE c.typnamespace OPERATOR(pg_catalog.=) 11::pg_catalog.oid
                                         AND c.typname OPERATOR(pg_catalog.=) y.typname))))";

/// Il motivo pubblico della guardia del `search_path` (regola Q).
pub const SEARCH_PATH_SHADOWS_CATALOG: &str = "il search_path della sessione mette prima di \
     pg_catalog uno schema con funzioni, operatori o tipi omonimi di quelli di pg_catalog";

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
    /// Il percorso e `COPY` e scrive una colonna `GENERATED ALWAYS AS
    /// IDENTITY` del target: `COPY` scrive il valore dell'ingresso, l'INSERT
    /// lo rifiuta.
    pub copy_identity_always: bool,
    /// Le chiavi coincidono con le colonne di un indice unico, immediato,
    /// valido, pronto, senza predicato ne espressioni, e il target non ha
    /// figli di ereditarieta fuori da quell'indice (una tabella partizionata
    /// si: l'indice unico vale su ogni partizione).
    pub keys_index: bool,
    /// La regola K2: uno di quegli indici confronta ogni chiave con la
    /// collation della colonna e con l'opclass btree predefinita del suo
    /// tipo (`key_semantics`).
    pub keys_unique: bool,
    /// Gli indici unici del target (anche parziali, con espressioni, non
    /// immediati o non validi) che coinvolgono una chiave: fra le colonne
    /// dell'indice o nelle sue espressioni e nel suo predicato (`pg_depend`).
    pub unique_on_keys: i64,
}

impl TargetFacts {
    const fn filters(self) -> RowFilters {
        RowFilters {
            row_security: self.target_row_security || self.related_row_security,
            rewrite_rule: self.rewrite_rule,
            instead_trigger: self.instead_trigger,
            row_trigger: self.row_trigger,
            // L'elenco chiuso di cio che `COPY` scavalca rispetto all'INSERT
            // (l'elenco chiuso del modulo): le regole, che la regola R rifiuta gia per
            // ogni profilo, e `IDENTITY ALWAYS` su una colonna scritta.
            path_bypass: self.copy_identity_always,
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
/// transazione della scrittura, o la sessione del dry run). `copied` sono le
/// colonne che `COPY` scrive, vuote se il percorso e uno statement. Gli
/// errori del catalogo escono in `phase`.
pub async fn facts<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    copied: &[String],
    phase: ErrorPhase,
) -> Result<TargetFacts> {
    let target = &operation.target;
    let mode = operation.mode;
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
                &crate::target_schema::schema_of(target)?,
                &target.object,
                &rules,
                &destructive,
                &instead,
                &copied,
                &operation.keys,
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
        copy_identity_always: row.get(9),
        keys_index: row.get(10),
        keys_unique: row.get(11),
        unique_on_keys: row.get(12),
    })
}

/// Il rifiuto della regola R, e poi della regola K
/// (`plenora_database_core::write_keys`), per `operation` sui fatti gia
/// letti, nella fase della scrittura.
///
/// La regola K vale per Update e `DeleteByKeys`: senza un indice unico sulle
/// chiavi una chiave tocca piu righe e un'altra nessuna, e i conteggi si
/// compensano (fino al 25/10, con `(1,a),(1,b),(2,c)` e un Update di
/// `(1,z),(99,q)`, `confirmed 2 skipped 0` con la 99 assente). Upsert ha gia
/// il suo arbitro: `ON CONFLICT` senza un indice unico immediato sulle chiavi
/// e rifiutato dal server (42P10, 55000). La regola R precede: una vista, che
/// un indice non ha, si rifiuta con il motivo della regola R quando ne ha uno.
///
/// Comportamento piu stretto, dichiarato: Update e `DeleteByKeys` su una
/// vista aggiornabile semplice, che la regola R ammetteva, ora si rifiutano
/// sempre (`schema`), perche il catalogo non prova l'unicita delle chiavi
/// attraverso la vista. Rientro: risolvere le colonne della vista fino alla
/// tabella di base e leggerne gli indici.
#[must_use]
pub fn refusal(operation: &WriteOperation, facts: TargetFacts) -> Option<DatabaseError> {
    if !facts.exists {
        return None;
    }
    if let Err(error) = refuse_unfaithful_write(
        ProviderKind::Postgres,
        operation.mode,
        ErrorPhase::Write,
        facts.filters(),
    ) {
        return Some(error);
    }
    // La condizione 4 della regola K (`write_keys::UPSERT_WITH_OTHER_UNIQUE_INDEX`):
    // l'arbitro dell'upsert e l'indice che prova le chiavi e nessun altro.
    // `ON CONFLICT (k)` sceglie ogni indice unico sulle stesse colonne,
    // qualunque collation abbia: fino al 25/10 (quarta rilettura, S6) con
    // `id text COLLATE "C" PRIMARY KEY` e `UNIQUE (id COLLATE ci)` l'upsert di
    // `('a', 'nuovo')` aggiornava la riga `A`. La regola chiusa e piu stretta
    // del server: un solo indice unico del target coinvolge le colonne
    // chiave. Comportamento piu stretto, dichiarato: si rifiutano anche
    // indici che non arbitrerebbero (`UNIQUE (id, altro)`, `UNIQUE
    // (lower(id))`, che il server rifiuterebbe con 23505).
    // Si decide prima delle altre condizioni: vale anche senza un indice
    // che provi le chiavi (il padre di un'ereditarieta).
    if operation.mode == WriteMode::Upsert && facts.unique_on_keys > 1 {
        return Some(
            plenora_database_core::write_keys::upsert_with_other_unique_index(
                ProviderKind::Postgres,
                ErrorPhase::Write,
            ),
        );
    }

    let unproven = match operation.mode {
        WriteMode::Update | WriteMode::DeleteByKeys => !facts.keys_unique,
        // `ON CONFLICT` arbitra con ogni indice unico sulle chiavi, qualunque
        // collation e opclass abbia: con un arbitro e nessun indice della
        // regola K2, `A` e `a` di una colonna con collation non
        // deterministica non entrano in conflitto. Senza arbitro decide il
        // server (42P10, 55000), come prima.
        WriteMode::Upsert => facts.keys_index && !facts.keys_unique,
        _ => false,
    };
    // Un indice unico che coinvolge le chiavi senza che nessuno le provi
    // (`keys_index` falso: parziale, con espressioni, o sul padre di
    // un'ereditarieta): `ON CONFLICT` puo arbitrare con quello, e nessun
    // arbitro e provato. Prima decideva il server, che con un padre di
    // ereditarieta lo accettava.
    let unproven = unproven
        || (operation.mode == WriteMode::Upsert && !facts.keys_index && facts.unique_on_keys > 0);
    unproven.then(|| keys_without_unique_index(ProviderKind::Postgres, ErrorPhase::Write))
}

/// L'uguaglianza con cui lo statement confronta una chiave: l'operatore,
/// qualificato con il suo schema (`"pg_catalog".=`), e il tipo qualificato a
/// cui si portano i due lati.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyEquality {
    pub operator: String,
    pub cast: String,
}

/// La condizione 2 della regola K sui `NULL`: un indice unico che prova le
/// chiavi (`keys_index`) e `NULLS NOT DISTINCT`. Dalla 15
/// (`pg_index.indnullsnotdistinct`); prima la colonna non c'e, e un indice
/// tratta sempre i `NULL` come distinti: la riga di `pg_index` letta come
/// `jsonb` la nomina soltanto dove esiste. Gira con il percorso chiuso
/// (regola Q2).
const NULL_KEYS_SQL: &str = concat!(
    "\
SELECT EXISTS (
    SELECT 1 FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
     WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text
       AND coalesce((pg_catalog.to_jsonb(i) ->> 'indnullsnotdistinct')::pg_catalog.bool, false)
       AND ",
    keys_index!("$3"),
    ")"
);

/// Come l'indice che prova le chiavi tratta i `NULL`
/// (`plenora_database_core::write_keys::NullKeys`): `Equal` se un indice
/// unico sulle chiavi e `NULLS NOT DISTINCT`, `Distinct` altrimenti. Fino al
/// 25/10 (quinta rilettura) due chiavi `NULL` non si confrontavano mai, e con
/// `UNIQUE NULLS NOT DISTINCT (k)` l'Upsert di `[(NULL,'uno'),(NULL,'due')]`
/// dava `confirmed 2` con una sola riga.
///
/// # Errors
///
/// L'errore classificato della lettura.
pub async fn null_keys<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
) -> Result<plenora_database_core::write_keys::NullKeys> {
    use plenora_database_core::write_keys::{keyed_mode, NullKeys};
    if !keyed_mode(operation.mode) || operation.keys.is_empty() {
        return Ok(NullKeys::Distinct);
    }
    let equal: bool = client
        .query_one(
            NULL_KEYS_SQL,
            &[
                &crate::target_schema::schema_of(&operation.target)?,
                &operation.target.object,
                &operation.keys,
            ],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Write, &error))?
        .get(0);
    Ok(if equal {
        NullKeys::Equal
    } else {
        NullKeys::Distinct
    })
}

/// Una chiave con una collation non deterministica: della colonna, o di un
/// indice unico che comprende una colonna chiave (`pg_index.indcollation`).
/// Gira con il percorso chiuso (regola Q2).
const NONDETERMINISTIC_KEYS_SQL: &str = "\
SELECT EXISTS (
    SELECT 1 FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
      JOIN pg_catalog.pg_collation k ON k.oid = a.attcollation
     WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text
       AND a.attnum > 0 AND NOT a.attisdropped
       AND a.attname::pg_catalog.text = ANY ($3::pg_catalog.text[])
       AND NOT k.collisdeterministic
) OR EXISTS (
    SELECT 1 FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_catalog.pg_index i ON i.indrelid = c.oid
      JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
       AND a.attnum = ANY (i.indkey::pg_catalog.int2[])
      CROSS JOIN LATERAL pg_catalog.unnest(i.indcollation::pg_catalog.oid[]) AS u(collation_oid)
      JOIN pg_catalog.pg_collation k ON k.oid = u.collation_oid
     WHERE n.nspname = $1::pg_catalog.text AND c.relname = $2::pg_catalog.text
       AND i.indisunique
       AND a.attname::pg_catalog.text = ANY ($3::pg_catalog.text[])
       AND NOT k.collisdeterministic
)";

/// Il motivo pubblico del rifiuto di una chiave con una collation non
/// deterministica.
pub const NONDETERMINISTIC_KEY: &str = "scrittura rifiutata: una chiave ha una collation non \
     deterministica, sotto cui il server confronta uguali valori diversi: l'unicita delle chiavi \
     dell'ingresso (regola K) non e provata";

/// La regola K sotto collation: le mode per chiavi rifiutano una chiave con
/// una collation non deterministica ([`NONDETERMINISTIC_KEYS_SQL`]).
///
/// La condizione 2 della regola K confronta i valori Arrow
/// (`plenora_database_core::write_keys`), con un'uguaglianza esatta o piu
/// stretta di quella del server sotto ogni collation deterministica (che
/// confronta per byte dopo l'ordinamento). Sotto una collation non
/// deterministica (ICU, `deterministic = false`) il server confronta uguali
/// valori diversi: fino al 25/10 l'Upsert `Strict` di `'a'` e `'A'` su una
/// chiave `COLLATE` `und-u-ks-level2` confermava due righe e ne lasciava una
/// sola, con il `LossReport` vuoto. Si rifiuta invece di indovinare
/// l'uguaglianza del server: decide la collation della colonna e di ogni
/// indice unico che ne comprende una (`ON CONFLICT` arbitra con quello).
///
/// # Errors
///
/// `unsupported` con [`NONDETERMINISTIC_KEY`] in `phase`; l'errore
/// classificato della lettura.
pub async fn refuse_nondeterministic_keys<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    phase: ErrorPhase,
) -> Result<()> {
    use plenora_database_core::write_keys::keyed_mode;
    if !keyed_mode(operation.mode) || operation.keys.is_empty() {
        return Ok(());
    }
    let nondeterministic: bool = client
        .query_one(
            NONDETERMINISTIC_KEYS_SQL,
            &[
                &crate::target_schema::schema_of(&operation.target)?,
                &operation.target.object,
                &operation.keys,
            ],
        )
        .await
        .map_err(|error| classify_error(phase, &error))?
        .get(0);
    if nondeterministic {
        return Err(crate::error::public_error(
            plenora_database_core::ErrorCategory::Unsupported,
            phase,
            false,
            NONDETERMINISTIC_KEY,
        ));
    }
    Ok(())
}

/// La regola K2 negli statement: per ogni chiave, l'uguaglianza dell'indice
/// che ne prova l'unicita ([`KEY_EQUALITY_SQL`]), per nome di colonna.
///
/// Fino al 25/10 lo statement confrontava `"k" = $1`, con l'operatore
/// risolto dal `search_path` e la collation della colonna, mentre l'unicita
/// la provava un indice qualsiasi sulle colonne: con una collation ICU non
/// deterministica sulla colonna e l'indice unico sotto `"C"`, Update di
/// `(A,N),(zzz,B)` dava `confirmed 2 updated 2` aggiornando `A` e `a`, con
/// `zzz` assente.
///
/// # Limite dichiarato
///
/// - **Regola**: l'indice che prova le chiavi confronta come la colonna
///   (collation della colonna, opclass btree predefinita del tipo), e lo
///   statement confronta con l'operatore di quell'opclass.
/// - **Ambito**: Update e `DeleteByKeys` (e Upsert con un arbitro), nella
///   transazione della scrittura e nel dry run con la DSN.
/// - **Hazard**: si rifiutano anche indici equivalenti che non sono quelli
///   predefiniti (`varchar_ops`, un alias di `text_ops` che non e
///   predefinito) e le chiavi su un dominio di un dominio, che la lettura
///   non risolve fino al tipo base.
/// - **Rientro**: un confronto per famiglia di operatori invece che per
///   opclass, e la risoluzione ricorsiva dei domini.
///
/// # Errors
///
/// L'errore classificato della lettura.
pub async fn key_equality<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
) -> Result<std::collections::BTreeMap<String, KeyEquality>> {
    let rows = client
        .query(
            KEY_EQUALITY_SQL,
            &[
                &crate::target_schema::schema_of(&operation.target)?,
                &operation.target.object,
                &operation.keys,
            ],
        )
        .await
        .map_err(|error| classify_error(ErrorPhase::Write, &error))?;
    Ok(rows
        .iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                KeyEquality {
                    operator: row.get(1),
                    cast: row.get(2),
                },
            )
        })
        .collect())
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
/// `copied` sono le colonne che `COPY` scrive (vuote sul percorso degli
/// statement): la regola W1 rifiuta il profilo se scavalca una garanzia del
/// target (l'elenco chiuso qui sopra).
///
/// # Errors
///
/// Il rifiuto della regola, o l'errore della lettura del catalogo.
pub async fn enforce<C: GenericClient + Sync>(
    client: &C,
    operation: &WriteOperation,
    copied: &[String],
) -> Result<()> {
    if mode_events(operation.mode).is_empty() {
        return Ok(());
    }
    let facts = facts(client, operation, copied, ErrorPhase::Write).await?;
    refusal(operation, facts).map_or(Ok(()), Err)
}

/// Le colonne che `COPY` scrive lungo `path`: quelle dell'ingresso se il
/// percorso e `COPY`, nessuna altrimenti.
#[must_use]
pub fn copied_columns(schema: &arrow_schema::SchemaRef, path: WritePath) -> Vec<String> {
    if path == WritePath::Copy {
        schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect()
    } else {
        Vec::new()
    }
}

/// La regola Q su una sessione della libreria: il `search_path` non mette
/// prima di `pg_catalog` uno schema che ne oscura funzioni, operatori o tipi
/// ([`SEARCH_PATH_GUARD_SQL`]).
///
/// Fino al 25/10 un ruolo con `search_path = <schema>, pg_catalog, public` e
/// una `<schema>.current_setting(text)` che rispondeva `origin` faceva
/// passare la regola W2 a una sessione `replica`: dry run `ok`, orfano
/// committato. Lo SQL della libreria ora qualifica funzioni, relazioni e
/// tipi di `pg_catalog`; gli operatori e gli statement sul target non si
/// possono qualificare senza cambiare la semantica dei tipi del target (un
/// `=` di `citext` non e quello di `pg_catalog`), e la guardia li copre.
///
/// # Limite dichiarato
///
/// - **Regola**: nessuno schema prima di `pg_catalog` nel percorso effettivo
///   contiene un nome di funzione, operatore o tipo di `pg_catalog`.
/// - **Ambito**: ogni sessione della libreria (`connect_session`: letture,
///   introspezione, preflight, dry run, transazioni applicative), quindi
///   anche ogni scrittura, che passa dal preflight; non `run_ddl`, che e SQL
///   del chiamante. Nella transazione della scrittura e nel dry run lo SQL
///   della libreria gira inoltre con il percorso chiuso della regola Q2
///   (`crate::search_path`), che copre anche gli operatori raggiunti con una
///   conversione implicita da uno schema dopo `pg_catalog`, che questa
///   guardia non vede.
/// - **Hazard**: il confronto e per nome, non per firma: si rifiuta anche chi
///   non oscura (uno schema con `PostGIS` messo prima di `pg_catalog`
///   definisce `text(geometry)` e `box(...)`). Una funzione o un operatore
///   creati da un'altra sessione dopo la guardia e prima di uno statement
///   della stessa transazione non si vedono (`READ COMMITTED`).
/// - **Rientro**: un confronto per firma e la guardia ripetuta dopo ogni lock
///   preso, se servisse un `search_path` con `pg_catalog` dopo altri schemi.
///
/// # Errors
///
/// `unsupported` con [`SEARCH_PATH_SHADOWS_CATALOG`] nella fase `phase`;
/// l'errore classificato della lettura.
pub async fn guard_search_path<C: GenericClient + Sync>(
    client: &C,
    phase: ErrorPhase,
) -> Result<()> {
    let shadowed: bool = client
        .query_one(SEARCH_PATH_GUARD_SQL, &[])
        .await
        .map_err(|error| classify_error(phase, &error))?
        .get(0);
    if shadowed {
        return Err(DatabaseError::unsupported(
            ProviderKind::Postgres,
            phase,
            format!("operazione rifiutata: {SEARCH_PATH_SHADOWS_CATALOG}"),
        ));
    }
    Ok(())
}

/// La regola W2 su PostgreSQL: la sessione della scrittura applica i trigger
/// e le foreign key del target.
///
/// `session_replication_role` diverso da `origin` (`replica` o `local`)
/// spegne i trigger ordinari e i trigger di sistema delle foreign key: fino
/// al 25/10 un ruolo con `ALTER ROLE ... SET session_replication_role =
/// replica` faceva committare ad Append un orfano e saltare un `BEFORE
/// INSERT` che solleva, con il dry run `ok`. Il valore viene dal ruolo, dal
/// database, dal server o da un `SET` precedente; `DISCARD ALL` lo riporta
/// al valore del ruolo, non a `origin`.
///
/// Si legge nella transazione della scrittura (o del dry run); se non e
/// `origin` si fissa con `set_config(..., true)`, locale alla transazione, e
/// si rilegge. Il parametro e riservato al superutente: un ruolo senza `SET`
/// sul parametro (`GRANT SET ON PARAMETER`, PostgreSQL 15) non lo puo
/// cambiare, e la scrittura si rifiuta prima di ogni effetto.
///
/// # Errors
///
/// `unsupported` con [`SESSION_WITHOUT_GUARANTEES`] se la sessione resta
/// fuori da `origin`; l'errore classificato di ogni altra lettura fallita. Il
/// rifiuto del `set_config` interrompe la transazione: il chiamante la
/// annulla.
pub async fn pin_session<C: GenericClient + Sync>(client: &C) -> Result<()> {
    let read = |error: tokio_postgres::Error| classify_error(ErrorPhase::Write, &error);
    let current: String = client
        .query_one(
            "SELECT pg_catalog.current_setting('session_replication_role')",
            &[],
        )
        .await
        .map_err(read)?
        .get(0);
    if current == "origin" {
        return Ok(());
    }
    let refused = || {
        DatabaseError::unsupported(
            ProviderKind::Postgres,
            ErrorPhase::Write,
            format!("scrittura rifiutata: session_replication_role: {SESSION_WITHOUT_GUARANTEES}"),
        )
    };
    match client
        .query_one(
            "SELECT pg_catalog.set_config('session_replication_role', 'origin', true)",
            &[],
        )
        .await
    {
        Ok(row) if row.get::<_, String>(0) == "origin" => Ok(()),
        Ok(_) => Err(refused()),
        Err(error)
            if error.code() == Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE) =>
        {
            Err(refused())
        }
        Err(error) => Err(read(error)),
    }
}
