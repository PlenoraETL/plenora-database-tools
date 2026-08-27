/// Batch invariabile eseguito e completamente drenato prima del primo uso.
pub const SESSION_BOOTSTRAP_SQL: &str = concat!(
    "SET XACT_ABORT ON; SET IMPLICIT_TRANSACTIONS OFF; SET NOCOUNT ON; ",
    "SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; ",
    "SET ARITHABORT ON; SET CONCAT_NULL_YIELDS_NULL ON; ",
    "SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF;"
);

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
