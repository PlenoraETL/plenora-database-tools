//! La regola R: una scrittura pubblica soltanto conteggi di righe che sono
//! nel target, scritte con le garanzie del target; altrimenti non parte.
//!
//! Nata il 25/10 per le scritture distruttive e allargata lo stesso giorno a
//! ogni mode (la regola e una, il modulo conserva il nome).
//!
//! `Replace` svuota il target con un `DELETE` (PostgreSQL, `MySQL`, Oracle,
//! Db2) e promette che dopo il COMMIT il target contiene soltanto l'ingresso;
//! `DeleteByKeys` e `Update` pubblicano `skipped` per le chiavi che non trovano;
//! `Append`, `TruncateInsert`, `Upsert` pubblicano `inserted`/`updated` dal
//! conteggio dello statement. Ogni promessa vale soltanto se lo statement
//! della mode arriva al target cosi com'e e vede tutte le sue righe. Il
//! server risponde con successo, e la promessa e falsa, quando:
//!
//! - una policy di sicurezza di riga (RLS di PostgreSQL, VPD di Oracle, RCAC
//!   di Db2) restringe lo statement distruttivo: `Replace` lascia righe
//!   vecchie accanto alle nuove, `DeleteByKeys` pubblica assente una chiave
//!   che c'e ma non si vede;
//! - una regola di riscrittura per uno degli eventi della mode (`CREATE RULE
//!   ... ON INSERT DO INSTEAD INSERT INTO altrove`) manda le righe altrove o
//!   da nessuna parte; `COPY` le regole non le applica, quindi la stessa
//!   scrittura farebbe due cose diverse secondo il profilo;
//! - un trigger `INSTEAD OF` per uno degli eventi della mode (su una vista in
//!   PostgreSQL, Oracle, Db2; su tabella o vista in SQL Server) riceve le
//!   righe al posto del target: il conteggio dello statement (o l'`OUTPUT` di
//!   SQL Server) conta cio che il trigger ha ricevuto, non cio che ha scritto;
//! - un trigger di riga `BEFORE` sopprime righe dello statement distruttivo
//!   (`BEFORE DELETE ... RETURN NULL` in PostgreSQL) o le neutralizza (`BEFORE
//!   UPDATE ... SET NEW.c = OLD.c` in `MySQL`/`MariaDB`): la chiave c'e, lo
//!   statement la salta o la lascia com'era;
//! - il percorso di scrittura scelto non applica le garanzie del target (la
//!   regola W1, qui sotto);
//! - una vista (per `Replace`) o una relazione opaca (tabella esterna,
//!   nickname, sinonimo, vista non risolta) possono filtrare o riscrivere
//!   senza che il catalogo locale lo dica.
//!
//! Misurato il 25/10 su PostgreSQL 16 (riletture indipendenti): tabella
//! (1,a),(2,b),(3,a) con policy `USING (owner = 'a')` e Replace con le righe
//! 10 e 11 → `committed`, target 2,10,11; `ON DELETE DO INSTEAD NOTHING` →
//! `committed`, nulla sostituito; `ON INSERT DO INSTEAD INSERT INTO altrove` e
//! Append preparato → `committed`, `inserted 2`, righe in `altrove`, e Replace
//! svuotava il target; Append su una vista con `INSTEAD OF INSERT` che
//! restituisce `NEW` senza scrivere → `inserted 2`, nulla nella tabella. SQL
//! Server 2022: Append `TdsBulk` con `CHECK (id > 0)` e la riga -5 →
//! `committed`, vincolo non piu fidato; foreign key verso un padre assente →
//! orfano confermato; Upsert con `INSTEAD OF UPDATE` vuoto → `updated 1` e
//! riga invariata.
//!
//! # La regola W1: fedelta del percorso
//!
//! Un percorso di scrittura (`COPY` testo e binario o INSERT preparato di
//! PostgreSQL; bulk TDS, statement preparati o staging e scambio di Replace
//! su SQL Server; gli statement di `MySQL`, Oracle, Db2) e ammesso per un
//! target soltanto se applica ogni garanzia del target che applica lo
//! statement di riferimento della mode: vincoli di ogni tipo (anche le regole
//! legate), identita `ALWAYS`, colonne generate, domini, trigger, default e
//! semantica dei `NULL`. Ogni provider tiene l'elenco **chiuso** di cio che
//! ciascun suo percorso scavalca; se il target ne ha una, il percorso si
//! rifiuta (`path_bypass`, [`BYPASSED_BY_PATH`]) — mai un ripiego silenzioso
//! su un altro percorso — e il dry run concorda. Gli elenchi:
//!
//! - PostgreSQL `COPY` (`plenora-db-postgres`, `write::visibility`): le
//!   regole di riscrittura (gia rifiutate per ogni profilo) e `IDENTITY
//!   ALWAYS` su una colonna scritta. Misurato il 25/10: `COPY` scriveva 10 e
//!   11 nella colonna d'identita senza avanzare la sequenza, dove l'INSERT
//!   rifiuta (428C9);
//! - SQL Server bulk TDS (`write::lifecycle`): `CHECK`, foreign key, regole
//!   e default legati (`sp_bindrule`, `sp_bindefault`, sulla colonna o sul
//!   tipo alias), trigger `INSERT`, vincoli `DEFAULT`. Misurato il 25/10:
//!   la -5 passava contro una regola legata, un `NULL` diventava il default
//!   legato;
//! - SQL Server Replace (staging e scambio): la tabella nuova nasce dal
//!   piano, e il piano rifiuta cio che non rappresenta; dal 25/10 anche le
//!   regole e i default legati, che prima sparivano con la -5 nel target.
//!
//! # La regola W2: sessione fissata
//!
//! Ogni sessione del provider fissa, al bootstrap o nella transazione della
//! scrittura, le impostazioni che spegnerebbero garanzie del target, e le
//! verifica; se non le puo fissare la scrittura si rifiuta
//! ([`SESSION_WITHOUT_GUARANTEES`]) prima di ogni effetto, e il dry run
//! concorda dove c'e:
//!
//! - PostgreSQL: `session_replication_role = origin` (con `replica` i
//!   trigger ordinari e quelli delle foreign key non scattano; misurato il
//!   25/10, un orfano committato);
//! - `MySQL`/`MariaDB`: `foreign_key_checks = 1`, `unique_checks = 1`,
//!   `sql_mode` stretto, `sql_select_limit` al massimo (un limite globale
//!   tronca in silenzio ogni `SELECT`, letture e catalogo compresi),
//!   `CLIENT_FOUND_ROWS` per i conteggi di Update;
//!   `MariaDB` anche `check_constraint_checks = 1` (misurati il 25/10: con il
//!   valore globale spento un orfano, e su `MariaDB` 12.3 una riga oltre il
//!   `CHECK`, committati);
//! - SQL Server: `XACT_ABORT`, `ANSI_WARNINGS`, `ANSI_NULLS`, `ARITHABORT`,
//!   `ROWCOUNT 0` e gli altri del bootstrap, verificati su `@@OPTIONS`;
//! - Oracle e Db2: dalla documentazione dei prodotti (non misurato il 25/10:
//!   i fixture non erano accesi) nessun interruttore di sessione spegne in
//!   silenzio un vincolo o un trigger del target. I vincoli differiti di
//!   Oracle (`SET CONSTRAINTS ALL DEFERRED`, `ALTER SESSION SET
//!   CONSTRAINTS`) falliscono al COMMIT con un errore; trigger e vincoli di
//!   Db2 si spengono soltanto con DDL della tabella (`SET INTEGRITY`,
//!   `ALTER TRIGGER`), che il catalogo dice.
//!
//! La regola: quando il catalogo dice che uno di questi meccanismi agisce
//! sugli statement della mode, la scrittura si rifiuta **prima di ogni
//! effetto** con `unsupported`, e il dry run riporta lo stesso rifiuto. La
//! decisione e una sola ([`refuse_unfaithful_write`]); i provider leggono i
//! fatti ([`RowFilters`]) dal proprio catalogo, per gli eventi della mode
//! ([`mode_events`]), e non hanno un'altra strada.
//!
//! Restano ammessi, perche il conteggio resta vero:
//!
//! - un trigger `BEFORE INSERT` (o `BEFORE UPDATE` dentro un upsert) di riga
//!   che restituisce `NULL` in PostgreSQL: la riga non e scritta e non e
//!   contata (`confirmed` la esclude, `skipped` la conta). Misurato il 25/10:
//!   Append, `TruncateInsert`, Replace, Upsert, in ogni profilo;
//! - i vincoli, le identita, i default, le partizioni e i trigger di
//!   statement: non cambiano quali righe lo statement scrive;
//! - i trigger `AFTER` di riga. Lo statement ha scritto la riga, e la conta;
//!   un `AFTER INSERT` che poi la cancella, la sposta o la cambia e logica
//!   del target, che il target dichiara nel proprio catalogo e che ogni
//!   scrittura attraversa allo stesso modo: non e un'omissione silenziosa
//!   della scrittura, ed e ammessa (decisione del 25/10, verificata su SQL
//!   Server 2022: Append `inserted 1` e la riga tolta dal trigger). Il
//!   conteggio pubblicato dice le righe che lo statement ha scritto, non
//!   quelle che il target conserva dopo i propri trigger;
//! - in `MySQL`/`MariaDB` un `BEFORE INSERT` di riga: non puo sopprimere la
//!   riga senza un errore (`SIGNAL`), puo soltanto cambiarne i valori, e la
//!   riga scritta e contata. Il `BEFORE` sull'evento distruttivo invece si
//!   rifiuta, come sopra.
//!
//! # Limite dichiarato
//!
//! - **Regola.** Il rifiuto e conservativo. Ogni trigger di riga `BEFORE` o
//!   `INSTEAD OF` sull'evento distruttivo conta, anche uno che non sopprime
//!   mai righe (un `updated_at` aggiornato da un trigger rende `Update` non
//!   ammessa), perche il catalogo non dice che cosa restituisce la funzione.
//!   Ogni regola di riscrittura per un evento della mode conta, anche `DO
//!   ALSO`: la scrittura nel target avviene, ma gli effetti in piu non sono
//!   dichiarati nel documento e `COPY` non li produrrebbe. Una relazione
//!   opaca rifiuta ogni mode, anche quelle che inseriscono soltanto. In
//!   `MySQL`/`MariaDB` un `BEFORE DELETE` (che non puo neutralizzare la
//!   riga) rifiuta comunque Replace e `DeleteByKeys`, e i trigger si vedono
//!   soltanto con il privilegio `TRIGGER` concesso direttamente (un
//!   privilegio che arriva da un ruolo non e letto): senza, le mode
//!   distruttive si rifiutano (`unreadable`). Su PostgreSQL W2 rifiuta un
//!   ruolo che ha `session_replication_role` diverso da `origin` e non puo
//!   cambiarlo, anche se il target non ha trigger ne foreign key.
//! - **Ambito.** Ogni mode che scrive un target esistente (tutte tranne
//!   `Create`), su ogni provider che chiama la regola. Il `TRUNCATE` di
//!   `TruncateInsert` non passa da policy, regole o trigger di riga; il suo
//!   INSERT si.
//! - **Hazard.** Un DDL concorrente (una policy, una regola o un trigger
//!   creati fra la lettura del catalogo e lo statement) sfugge alla prima
//!   lettura: dove il provider lo puo fare la regola si rilegge con il lock
//!   dello statement preso (PostgreSQL, SQL Server), altrove resta la
//!   finestra. I privilegi che esentano da una policy (`EXEMPT ACCESS
//!   POLICY` di Oracle, `BYPASSRLS` di PostgreSQL a parte) e le etichette di
//!   Oracle Label Security non sono letti: la regola rifiuta comunque, o non
//!   vede (OLS). Un trigger `BEFORE INSERT` che scrive la riga altrove e
//!   restituisce `NULL` (partizionamento con trigger) la pubblica `skipped`
//!   anche se e in una tabella figlia. Un trigger `AFTER` che cancella o
//!   cambia la riga appena scritta lascia nel target meno di quanto il
//!   conteggio dice (ammesso, sopra). W2 non fissa la durabilita del COMMIT
//!   (`synchronous_commit = off` di PostgreSQL, `DELAYED_DURABILITY` di SQL
//!   Server, `innodb_flush_log_at_trx_commit` di `MySQL`): un COMMIT
//!   confermato puo perdersi in un crash del server se il server e
//!   configurato cosi; e una garanzia di durabilita, non del target, e resta
//!   del server.
//! - **Rientro.** Un conteggio delle righe del target prima e dopo lo
//!   statement, nella stessa transazione e con lo stesso snapshot, che provi
//!   che lo statement ha scritto nel target cio che pubblica; oppure una
//!   misura per meccanismo che ne restringa il rifiuto; per il bulk TDS, un
//!   driver che passi `CHECK_CONSTRAINTS, FIRE_TRIGGERS` (`tiberius-ng`
//!   0.13.1 non ha opzioni per `INSERT BULK`).

use crate::plan::{ProviderKind, WriteMode};
use crate::{DatabaseError, ErrorPhase, Result};

/// Un evento DML che lo statement di una mode produce sul target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteEvent {
    Insert,
    Update,
    Delete,
}

/// Gli eventi DML della mode sul target esistente, nell'ordine in cui la
/// scrittura li produce.
///
/// `Create` non ne ha: il target lo crea la scrittura, e non ha regole,
/// trigger o policy. `TruncateInsert` ha soltanto l'INSERT: `TRUNCATE` non e
/// un evento di riga. Upsert ha INSERT e UPDATE in ogni provider (`ON
/// CONFLICT DO UPDATE`, `MERGE`, `UPDATE` seguito da `INSERT`).
#[must_use]
pub const fn mode_events(mode: WriteMode) -> &'static [WriteEvent] {
    match mode {
        WriteMode::Create => &[],
        WriteMode::Append | WriteMode::TruncateInsert => &[WriteEvent::Insert],
        WriteMode::Replace => &[WriteEvent::Delete, WriteEvent::Insert],
        WriteMode::Update => &[WriteEvent::Update],
        WriteMode::Upsert => &[WriteEvent::Insert, WriteEvent::Update],
        WriteMode::DeleteByKeys => &[WriteEvent::Delete],
    }
}

/// L'evento DML le cui righe la mode deve vedere tutte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestructiveEvent {
    /// Il `DELETE` di `Replace` e di `DeleteByKeys`.
    Delete,
    /// L'`UPDATE` di `Update`.
    Update,
}

/// L'evento distruttivo della mode, se ne ha uno.
///
/// `Upsert` non c'e: una riga in conflitto nascosta da una policy fa fallire
/// lo statement (PostgreSQL non la salta in silenzio; SQL Server inserisce e
/// viola la chiave unica che il piano esige), e una riga che un trigger
/// `BEFORE UPDATE` sopprime non e contata. `TruncateInsert` non c'e: dove la
/// mode esiste e un `TRUNCATE`, che non passa da policy, regole o trigger di
/// riga.
#[must_use]
pub const fn destructive_event(mode: WriteMode) -> Option<DestructiveEvent> {
    match mode {
        WriteMode::Replace | WriteMode::DeleteByKeys => Some(DestructiveEvent::Delete),
        WriteMode::Update => Some(DestructiveEvent::Update),
        WriteMode::Create | WriteMode::Append | WriteMode::TruncateInsert | WriteMode::Upsert => {
            None
        }
    }
}

/// Cio che il catalogo dice del target, per gli eventi della mode.
///
/// Un fatto per meccanismo, cosi ogni provider riempie soltanto cio che il
/// proprio catalogo sa e lascia `false` cio che il suo prodotto non ha.
///
/// Un provider che non sa leggere un fatto che il suo prodotto ha lo dichiara
/// con `unreadable`: la regola rifiuta.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowFilters {
    /// Una policy di sicurezza di riga si applica allo statement distruttivo:
    /// RLS attiva per il ruolo sul target o su una relazione sotto di esso
    /// (PostgreSQL), una policy VPD per l'evento (Oracle), row permission
    /// attive (Db2), un predicato di filtro (SQL Server). Conta soltanto per
    /// le mode con un [`destructive_event`]: per un INSERT una policy rifiuta
    /// con un errore, non in silenzio.
    pub row_security: bool,
    /// Una regola di riscrittura per uno degli eventi della mode (PostgreSQL
    /// `pg_rewrite`), `DO INSTEAD` o `DO ALSO`.
    pub rewrite_rule: bool,
    /// Un trigger `INSTEAD OF` per uno degli eventi della mode, sul target o
    /// su una relazione sotto di esso.
    pub instead_trigger: bool,
    /// Un trigger di riga che puo sopprimere lo statement distruttivo:
    /// `BEFORE ... FOR EACH ROW` o `INSTEAD OF` per l'evento (PostgreSQL);
    /// ogni trigger abilitato per l'evento dove il catalogo non distingue
    /// (Oracle, Db2). Conta soltanto per le mode con un
    /// [`destructive_event`].
    pub row_trigger: bool,
    /// La regola W1: il percorso di scrittura scelto non applica una garanzia
    /// che il target ha e che lo statement di riferimento applica (il bulk
    /// TDS di SQL Server su un target con `CHECK`, foreign key, regole o
    /// default legati, trigger o default; `COPY` di PostgreSQL su una colonna
    /// `IDENTITY ALWAYS` scritta).
    pub path_bypass: bool,
    /// Il target e una vista. `Replace` la rifiuta sempre: la definizione
    /// della vista e un filtro, e il catalogo non prova che non lo sia.
    pub view: bool,
    /// Una relazione il cui comportamento il catalogo locale non dice: una
    /// tabella remota (foreign table, nickname), un sinonimo, una vista le
    /// cui relazioni di base il provider non risolve.
    pub opaque_relation: bool,
    /// Il catalogo che direbbe uno dei fatti non e leggibile dal ruolo.
    pub unreadable: bool,
}

/// I motivi, stabili, senza dati: uno per meccanismo.
pub const HIDDEN_BY_ROW_SECURITY: &str =
    "una policy di sicurezza di riga puo nascondere righe del target allo statement";
pub const HIDDEN_BY_RULE: &str = "una regola di riscrittura del target puo sostituire lo statement";
pub const HIDDEN_BY_INSTEAD_TRIGGER: &str =
    "un trigger INSTEAD OF del target sostituisce lo statement: i conteggi non direbbero righe del target";
pub const HIDDEN_BY_TRIGGER: &str =
    "un trigger di riga BEFORE o INSTEAD OF del target puo sopprimere righe";
pub const BYPASSED_BY_PATH: &str =
    "il profilo di scrittura non applica garanzie del target (vincoli, regole, identita, trigger o default)";
pub const HIDDEN_BY_VIEW: &str =
    "replace su una vista: la definizione della vista puo escludere righe";
pub const HIDDEN_BY_OPAQUE_RELATION: &str =
    "il target dipende da una relazione di cui il catalogo non dice filtri e riscritture";
pub const HIDDEN_UNREADABLE: &str =
    "il catalogo che dice se lo statement scrive tutte le righe nel target non e leggibile";
/// La regola W2: la sessione ha un'impostazione che spegne garanzie del
/// target (trigger, foreign key, vincoli, unicita) e la scrittura non la
/// puo riportare al valore che le applica.
pub const SESSION_WITHOUT_GUARANTEES: &str =
    "la sessione spegne garanzie del target e il ruolo non la puo riportare al valore che le applica";

const fn mode_name(mode: WriteMode) -> &'static str {
    match mode {
        WriteMode::Replace => "replace",
        WriteMode::DeleteByKeys => "delete_by_keys",
        WriteMode::Update => "update",
        WriteMode::Create => "create",
        WriteMode::Append => "append",
        WriteMode::TruncateInsert => "truncate_insert",
        WriteMode::Upsert => "upsert",
    }
}

/// Il motivo del rifiuto della regola R per `mode` sui fatti `filters`, o
/// `None` se la scrittura pubblica righe che sono nel target.
///
/// L'ordine dei meccanismi e dichiarato: catalogo illeggibile, sicurezza di
/// riga (mode distruttive), regola, trigger `INSTEAD OF`, trigger di riga
/// (mode distruttive), percorso che scavalca il target, relazione opaca,
/// vista (soltanto `Replace`). `Create` non ha eventi e passa sempre.
#[must_use]
pub const fn refusal_reason(mode: WriteMode, filters: RowFilters) -> Option<&'static str> {
    if mode_events(mode).is_empty() {
        return None;
    }
    let destructive = destructive_event(mode).is_some();
    if filters.unreadable {
        Some(HIDDEN_UNREADABLE)
    } else if destructive && filters.row_security {
        Some(HIDDEN_BY_ROW_SECURITY)
    } else if filters.rewrite_rule {
        Some(HIDDEN_BY_RULE)
    } else if filters.instead_trigger {
        Some(HIDDEN_BY_INSTEAD_TRIGGER)
    } else if destructive && filters.row_trigger {
        Some(HIDDEN_BY_TRIGGER)
    } else if filters.path_bypass {
        Some(BYPASSED_BY_PATH)
    } else if filters.opaque_relation {
        Some(HIDDEN_BY_OPAQUE_RELATION)
    } else if filters.view && matches!(mode, WriteMode::Replace) {
        Some(HIDDEN_BY_VIEW)
    } else {
        None
    }
}

/// La regola R: il rifiuto della scrittura `mode` sul target descritto da
/// `filters`, o `Ok` se gli statement della mode scrivono nel target cio che
/// la scrittura pubblichera.
///
/// # Errors
///
/// `unsupported` nella fase `phase`, con il motivo di [`refusal_reason`].
pub fn refuse_unfaithful_write(
    provider: ProviderKind,
    mode: WriteMode,
    phase: ErrorPhase,
    filters: RowFilters,
) -> Result<()> {
    refusal_reason(mode, filters).map_or(Ok(()), |reason| {
        Err(DatabaseError::unsupported(
            provider,
            phase,
            format!("{} rifiutata: {reason}", mode_name(mode)),
        ))
    })
}

#[cfg(test)]
#[path = "row_visibility_tests.rs"]
mod tests;
