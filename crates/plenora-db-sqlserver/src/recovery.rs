use crate::SessionState;

/// Evento osservato nel lifecycle transazionale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionEvent {
    BeginSucceeded,
    StatementFailed,
    ServerReportsCommittable,
    ServerReportsUncommittable,
    CommitSucceeded,
    RollbackSucceeded,
    TransportLost,
    Cancelled,
}

/// Azione obbligatoria risultante dalla macchina a stati.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    None,
    Rollback,
    Quarantine,
    ReconcileCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryDecision {
    pub state: SessionState,
    pub action: RecoveryAction,
}

/// Macchina a stati pura usata anche dai test di fault injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionState {
    state: SessionState,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self {
            state: SessionState::Ready,
        }
    }
}

impl TransactionState {
    #[must_use]
    pub const fn state(self) -> SessionState {
        self.state
    }

    #[must_use]
    pub const fn apply(&mut self, event: TransactionEvent) -> RecoveryDecision {
        let decision = match (self.state, event) {
            (SessionState::Ready, TransactionEvent::BeginSucceeded) => RecoveryDecision {
                state: SessionState::Transaction,
                action: RecoveryAction::None,
            },
            (SessionState::Transaction, TransactionEvent::ServerReportsCommittable) => {
                RecoveryDecision {
                    state: SessionState::Transaction,
                    action: RecoveryAction::None,
                }
            }
            (
                SessionState::Transaction,
                TransactionEvent::StatementFailed | TransactionEvent::ServerReportsUncommittable,
            ) => RecoveryDecision {
                state: SessionState::Uncommittable,
                action: RecoveryAction::Rollback,
            },
            (
                SessionState::Transaction | SessionState::Uncommittable,
                TransactionEvent::RollbackSucceeded,
            )
            | (SessionState::Transaction, TransactionEvent::CommitSucceeded) => RecoveryDecision {
                state: SessionState::Ready,
                action: RecoveryAction::None,
            },
            (SessionState::Transaction, TransactionEvent::TransportLost) => RecoveryDecision {
                state: SessionState::Quarantined,
                action: RecoveryAction::ReconcileCommit,
            },
            (_, TransactionEvent::Cancelled | TransactionEvent::TransportLost) => {
                RecoveryDecision {
                    state: SessionState::Quarantined,
                    action: RecoveryAction::Quarantine,
                }
            }
            _ => RecoveryDecision {
                state: SessionState::Quarantined,
                action: RecoveryAction::Quarantine,
            },
        };
        self.state = decision.state;
        decision
    }
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;
