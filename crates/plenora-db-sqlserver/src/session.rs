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
