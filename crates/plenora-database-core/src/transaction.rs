//! Transaction scope pubblico portabile.
//!
//! Il modulo definisce l'API dell'*application plane*: begin/commit/rollback,
//! savepoint annidati, gestione di commit ambigui (`OutcomeUnknown`). Non
//! contiene concetti di dominio (tenant, workorder, evidence, …): la libreria
//! espone primitive; la traduzione verso lo specifico provider spetta al
//! driver.

use crate::graph::{GraphRow, GraphStatement};
use crate::native_query_policy::NativeQueryPolicy;
use crate::outcome::{CertainPhase, Recovery};
use crate::provider::{ParameterValue, ProviderFuture};
use crate::row::Row;
use crate::session_context::SessionContext;
use crate::CancellationToken;
use serde::{Deserialize, Serialize};

/// Livello di isolamento richiesto per la transazione.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Modalità di accesso della transazione (read-only vs read-write).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    ReadWrite,
    ReadOnly,
}

/// Opzioni con cui viene aperta una transazione.
///
/// Tutte le opzioni sono facoltative: se non specificate il provider usa il
/// default della sessione (che a sua volta può essere influenzato dai timeout
/// di sessione già configurati).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionOptions {
    pub isolation: Option<IsolationLevel>,
    pub access_mode: Option<AccessMode>,
    /// Solo effettivo con `Serializable` + `ReadOnly`; ignorato altrove.
    pub deferrable: Option<bool>,
    /// Timeout in millisecondi applicato ai singoli statement della
    /// transazione; sopravvive per la durata della sessione ed è ripristinato
    /// al valore precedente in fase di commit/rollback.
    pub statement_timeout_ms: Option<u64>,
    /// Session context transaction-local: viene applicato dopo `BEGIN` e
    /// resettato automaticamente al commit/rollback. Impossibile far leakare
    /// tra riusi della stessa connessione.
    #[serde(default, skip_serializing_if = "SessionContext::is_empty")]
    pub context: SessionContext,
    /// Politica di ammissione degli statement passati al transaction scope.
    /// Default `Allow`. Il profilo PFM di produzione deve impostare `Deny`.
    #[serde(default)]
    pub native_query_policy: NativeQueryPolicy,
}

impl TransactionOptions {
    /// Opzioni PFM production-safe (CHG-003):
    /// - `native_query_policy: Deny` (solo SELECT/WITH/INSERT/UPDATE/
    ///   DELETE/VALUES/TABLE/MERGE, no DDL/session commands).
    /// - `access_mode: None` (auto — dipende dallo statement).
    /// - `isolation: None` (default provider).
    /// - `context: default vuoto`.
    ///
    /// I comandi PFM high-level (`doctor`, `execute-sql`, tx-test)
    /// devono partire da questo baseline. Consumer raw-SQL deve
    /// esplicitamente scegliere `TransactionOptions::default()` +
    /// `native_query_policy: Allow`.
    #[must_use]
    pub fn pfm_defaults() -> Self {
        Self {
            native_query_policy: NativeQueryPolicy::Deny,
            ..Self::default()
        }
    }
}

/// Statement portabile eseguibile dentro una transazione.
///
/// Il SQL è visto come opaco dal core: la validazione (allow-list, ban di
/// pattern DDL, ecc.) appartiene ai livelli superiori (facade + governance
/// `native_query` del PFM). I parametri sono legati positional con placeholder
/// `$1..$n`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    pub sql: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<ParameterValue>,
}

impl Statement {
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_params(mut self, params: Vec<ParameterValue>) -> Self {
        self.params = params;
        self
    }
}

/// Esito del `commit()` di una transazione.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CommitOutcome {
    /// Il commit è stato confermato dal server.
    Committed,
    /// Il commit è stato emesso ma l'esito remoto non è verificabile
    /// (disconnessione fra `COMMIT` e ACK, timeout, shutdown, ecc.). La
    /// sessione è compromessa e viene messa in quarantena.
    OutcomeUnknown { recovery: Recovery },
}

impl CommitOutcome {
    #[must_use]
    pub const fn is_committed(&self) -> bool {
        matches!(self, Self::Committed)
    }
}

/// Costruisce la `Recovery` canonica per un commit ambiguo.
#[must_use]
pub fn outcome_unknown_recovery() -> Recovery {
    Recovery {
        last_certain_phase: CertainPhase::CommitRequested,
        automatic_retry_allowed: false,
        idempotency_key: None,
        staging_object: None,
        verification_action: Some(
            "verificare fuori banda lo stato del target prima di ritentare".to_owned(),
        ),
    }
}

/// Stream di righe letto in modo server-side.
///
/// Il consumer chiama `next_batch()` finché non ottiene `None`, indicando
/// esaurimento del cursor sorgente. La dimensione dei batch è fissata alla
/// creazione dello stream (parametro `batch_size` di `query_stream`).
pub trait RowStream: Send {
    fn next_batch<'a>(
        &'a mut self,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Option<Vec<Row>>>;
}

/// Richiesta di update ottimistico condizionato.
///
/// `update` è lo statement `UPDATE ... WHERE key = ... AND version = ...` che
/// deve applicare esattamente `expected_affected_rows` righe (tipicamente 1).
///
/// `key_probe`, se presente, è un `SELECT 1 FROM ... WHERE key = ... LIMIT 1`
/// eseguito **solo** quando l'update non ha modificato la riga attesa: serve
/// a distinguere `ConcurrentModification` (chiave esiste ma versione diversa)
/// da `NotFound` (chiave assente). Se `key_probe` è `None`, il default
/// conservativo classifica il fallimento come `ConcurrentModification`.
#[derive(Debug, Clone, PartialEq)]
pub struct ConditionalUpdate<'a> {
    pub update: &'a Statement,
    pub key_probe: Option<&'a Statement>,
    pub expected_affected_rows: u64,
}

/// Costruisce l'errore canonico `ConcurrentModification`.
#[must_use]
pub fn concurrent_modification_error(message: impl Into<String>) -> crate::DatabaseError {
    crate::DatabaseError {
        category: crate::ErrorCategory::ConcurrentModification,
        phase: crate::ErrorPhase::Write,
        remote_effect: crate::RemoteEffect::RolledBack,
        retry: crate::RetryDisposition::Never,
        provider: None,
        execution_id: None,
        message: message.into(),
        diagnostics: None,
    }
}

/// Transazione applicativa: espone `execute` (multi-statement) + savepoint
/// annidati + commit/rollback. `commit`/`rollback` consumano l'oggetto.
///
/// Il `Drop` esplicito senza commit/rollback deve mettere la sessione in
/// quarantena: la transazione non chiusa esplicitamente non può essere
/// riutilizzata come sana.
pub trait TransactionScope: Send {
    /// Ritorna il provider al quale la transazione è collegata.
    /// Usato dalle facade portable per selezionare il compilatore SQL.
    fn provider_kind(&self) -> crate::plan::ProviderKind;

    fn execute<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, u64>;

    /// Esegue una query e restituisce le righe con schema colonne.
    /// Usa i decoder canonici: tipi non supportati producono `Unsupported`.
    fn query<'a>(
        &'a mut self,
        statement: &'a Statement,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<Row>>;

    /// Esegue una richiesta graph. Il default e deliberatamente fail-closed:
    /// soltanto un provider con un adapter qualificato puo aprire la
    /// superficie.
    fn execute_graph<'a>(
        &'a mut self,
        _statement: &'a GraphStatement,
        _cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Vec<GraphRow>> {
        let provider = self.provider_kind();
        Box::pin(async move {
            Err(crate::DatabaseError::unsupported(
                provider,
                crate::ErrorPhase::Prepare,
                "operazioni graph non supportate dal provider",
            ))
        })
    }

    /// Apre una lettura server-side: le righe vengono materializzate in
    /// batch di `batch_size` righe alla volta senza mai caricare l'intero
    /// result set in memoria. Il chiamante consuma lo stream chiamando
    /// `next_batch()` finché non ritorna `None`.
    ///
    /// La sessione (e con essa il cursor server-side) resta legata alla
    /// transazione: il commit/rollback chiude automaticamente il cursor.
    /// Lo stream borrow'a la transazione: finché è vivo, nessun altro
    /// `execute`/`query` sulla stessa transazione è consentito.
    fn query_stream<'a>(
        &'a mut self,
        statement: &'a Statement,
        batch_size: u32,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, Box<dyn RowStream + Send + 'a>>;

    fn savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()>;

    fn rollback_to_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()>;

    fn release_savepoint<'a>(
        &'a mut self,
        name: &'a str,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()>;

    /// Update ottimistico condizionato: ritorna `Ok(())` se il numero di
    /// righe modificate combacia con `expected_affected_rows`. Altrimenti:
    ///
    /// - `Err(NotFound)` se `key_probe` conferma che la chiave non esiste,
    /// - `Err(ConcurrentModification)` in tutti gli altri casi di mismatch
    ///   (chiave esiste ma con versione diversa, o probe assente).
    ///
    /// Il default (senza probe) è deliberatamente conservativo: preferisce
    /// segnalare un conflitto piuttosto che nasconderlo dietro un "no-op".
    fn execute_conditional_update<'a>(
        &'a mut self,
        request: ConditionalUpdate<'a>,
        cancellation: &'a CancellationToken,
    ) -> ProviderFuture<'a, ()>;

    fn commit(
        self: Box<Self>,
        cancellation: &CancellationToken,
    ) -> ProviderFuture<'_, CommitOutcome>;

    fn rollback(self: Box<Self>, cancellation: &CancellationToken) -> ProviderFuture<'_, ()>;
}

/// Valida che il nome di un savepoint sia un identificatore SQL semplice
/// `[A-Za-z_][A-Za-z0-9_]*` di lunghezza `1..=63` (limite PostgreSQL/MySQL).
///
/// I nomi di savepoint sono immessi dal chiamante applicativo: senza vincolo
/// esplicito il provider dovrebbe interpolare stringhe arbitrarie nella SQL,
/// esponendo una superficie d'attacco. Il core rifiuta a monte.
///
/// # Errors
///
/// Restituisce `InvalidPlan` per nomi vuoti, troppo lunghi o contenenti
/// caratteri non ammessi.
///
/// # Panics
///
/// Non panics: l'`expect()` interno è protetto dal controllo di non-vuoto
/// che lo precede.
pub fn validate_savepoint_name(name: &str) -> crate::Result<()> {
    if name.is_empty() {
        return Err(crate::DatabaseError::invalid_plan(
            "il nome di savepoint non può essere vuoto",
        ));
    }
    if name.len() > 63 {
        return Err(crate::DatabaseError::invalid_plan(
            "il nome di savepoint eccede 63 caratteri",
        ));
    }
    let mut chars = name.chars();
    let first = chars.next().expect("controllato non vuoto");
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(crate::DatabaseError::invalid_plan(
            "il nome di savepoint deve iniziare con lettera o underscore",
        ));
    }
    for c in chars {
        if !(c.is_ascii_alphanumeric() || c == '_') {
            return Err(crate::DatabaseError::invalid_plan(
                "il nome di savepoint può contenere solo lettere, cifre o underscore",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "transaction_tests.rs"]
mod tests;
