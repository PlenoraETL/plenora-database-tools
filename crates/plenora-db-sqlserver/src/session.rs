/// Batch invariabile eseguito e completamente drenato prima del primo uso.
///
/// La regola W2 (sessione fissata): ogni impostazione di sessione che
/// cambierebbe in silenzio cio che una scrittura fa al target ha qui il
/// valore che applica le garanzie, e [`options_pinned`] lo verifica
/// sull'`@@OPTIONS` della sessione appena aperta. `ANSI_WARNINGS OFF`
/// troncherebbe le stringhe oltre la colonna e renderebbe `NULL` un overflow
/// aritmetico (con `ARITHABORT OFF`); `XACT_ABORT OFF` lascerebbe la
/// transazione viva dopo un errore di statement; `SET ROWCOUNT n` fermerebbe
/// ogni `INSERT`/`UPDATE`/`DELETE` a `n` righe, pubblicando le altre
/// `skipped`. Il valore del server (`user options`) non conta: il bootstrap
/// li fissa tutti. Lo SQL del chiamante viaggia come RPC (`sp_executesql`) e
/// un suo `SET` dura fino alla fine della chiamata (misurato il 25/10): non
/// arriva alla scrittura successiva sulla stessa sessione.
pub const SESSION_BOOTSTRAP_SQL: &str = concat!(
    "SET XACT_ABORT ON; SET IMPLICIT_TRANSACTIONS OFF; SET NOCOUNT ON; ",
    "SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; ",
    "SET ARITHABORT ON; SET CONCAT_NULL_YIELDS_NULL ON; ",
    "SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF; SET ROWCOUNT 0;"
);

/// La lettura che verifica il bootstrap.
pub const SESSION_OPTIONS_SQL: &str = "SELECT CAST(@@OPTIONS AS int);";

/// I bit di `@@OPTIONS` che il bootstrap accende: `ANSI_WARNINGS` (8),
/// `ANSI_PADDING` (16), `ANSI_NULLS` (32), `ARITHABORT` (64),
/// `QUOTED_IDENTIFIER` (256), `NOCOUNT` (512), `CONCAT_NULL_YIELDS_NULL`
/// (4096), `XACT_ABORT` (16384).
const OPTIONS_ON: i32 = 8 | 16 | 32 | 64 | 256 | 512 | 4096 | 16384;

/// I bit che spegne: `IMPLICIT_TRANSACTIONS` (2), `NUMERIC_ROUNDABORT`
/// (8192).
const OPTIONS_OFF: i32 = 2 | 8192;

/// `@@OPTIONS` dice le impostazioni che il bootstrap fissa.
#[must_use]
pub const fn options_pinned(options: i32) -> bool {
    options & OPTIONS_ON == OPTIONS_ON && options & OPTIONS_OFF == 0
}

/// La lettura del livello di isolamento in vigore sulla sessione: la propria
/// riga di `sys.dm_exec_sessions` e visibile senza `VIEW SERVER STATE`.
pub const ISOLATION_LEVEL_SQL: &str = "SELECT CAST(transaction_isolation_level AS int) \
     FROM sys.dm_exec_sessions WHERE session_id = @@SPID;";

/// Il batch che fissa `level` sulla sessione e il valore di
/// `transaction_isolation_level` che deve seguirne (1 `READ UNCOMMITTED`, 2
/// `READ COMMITTED`, 3 `REPEATABLE READ`, 4 `SERIALIZABLE`; 5 sarebbe
/// `SNAPSHOT`, che il contratto non chiede).
///
/// Il batch viaggia **semplice**, non come RPC: un `SET` dentro
/// `sp_executesql` vale fino alla fine della chiamata, e fino al 25/10
/// `Serializable` chiesto lasciava `READ COMMITTED` in vigore senza errore.
/// Da semplice resta sulla sessione oltre la transazione: chi lo chiede
/// diverso da `READ COMMITTED` non rimette la sessione nel pool.
///
/// Limite dichiarato. **Regola**: il livello di una transazione applicativa
/// si fissa con un batch semplice prima del `BEGIN` e si verifica su
/// `sys.dm_exec_sessions`; senza livello si fissa `READ COMMITTED`, e la
/// scrittura lo fissa sempre. **Ambito**: `begin_transaction` e `write`.
/// **Hazard**: SQL Server non riporta indietro il livello al COMMIT, quindi
/// una transazione con un livello diverso da `READ COMMITTED` costa una
/// connessione nuova; `SNAPSHOT` non e chiedibile dal contratto.
/// **Rientro**: un reset di sessione verificato (`sp_reset_connection` o un
/// `SET` di ritorno riletto) che rimetta la sessione nel pool.
#[must_use]
pub const fn isolation_batch(
    level: plenora_database_core::transaction::IsolationLevel,
) -> (&'static str, i32) {
    use plenora_database_core::transaction::IsolationLevel;
    match level {
        IsolationLevel::ReadUncommitted => ("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;", 1),
        IsolationLevel::ReadCommitted => ("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;", 2),
        IsolationLevel::RepeatableRead => ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;", 3),
        IsolationLevel::Serializable => ("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;", 4),
    }
}

/// Stato locale conservativo di una sessione TDS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    Ready,
    Transaction,
    Uncommittable,
    Quarantined,
    Closed,
}

impl SessionState {
    #[must_use]
    pub const fn is_reusable(self) -> bool {
        matches!(self, Self::Ready)
    }
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
