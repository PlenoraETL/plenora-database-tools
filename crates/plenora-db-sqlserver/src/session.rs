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
mod tests {
    use super::*;

    #[test]
    fn only_ready_session_is_reusable() {
        assert!(SessionState::Ready.is_reusable());
        for state in [
            SessionState::Transaction,
            SessionState::Uncommittable,
            SessionState::Quarantined,
            SessionState::Closed,
        ] {
            assert!(!state.is_reusable());
        }
    }

    #[test]
    fn bootstrap_fixes_transaction_and_rowcount_semantics() {
        assert!(SESSION_BOOTSTRAP_SQL.contains("XACT_ABORT ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("IMPLICIT_TRANSACTIONS OFF"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("NOCOUNT ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("ANSI_NULLS ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("ANSI_PADDING ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("ANSI_WARNINGS ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("ARITHABORT ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("CONCAT_NULL_YIELDS_NULL ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("QUOTED_IDENTIFIER ON"));
        assert!(SESSION_BOOTSTRAP_SQL.contains("NUMERIC_ROUNDABORT OFF"));
    }
}
