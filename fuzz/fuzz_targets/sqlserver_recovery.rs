#![no_main]

use libfuzzer_sys::fuzz_target;
use plenora_db_sqlserver::{RecoveryAction, SessionState, TransactionEvent, TransactionState};

const EVENTS: [TransactionEvent; 8] = [
    TransactionEvent::BeginSucceeded,
    TransactionEvent::StatementFailed,
    TransactionEvent::ServerReportsCommittable,
    TransactionEvent::ServerReportsUncommittable,
    TransactionEvent::CommitSucceeded,
    TransactionEvent::RollbackSucceeded,
    TransactionEvent::TransportLost,
    TransactionEvent::Cancelled,
];

fuzz_target!(|input: &[u8]| {
    let mut machine = TransactionState::default();
    assert_eq!(machine.state(), SessionState::Ready);

    // Ogni byte è una sequenza di eventi osservati sul wire, anche in ordine
    // arbitrario o impossibile: la macchina non deve mai concludere in modo
    // ottimistico.
    for byte in input {
        let before = machine.state();
        let event = EVENTS[usize::from(*byte) % EVENTS.len()];
        let decision = machine.apply(event);

        // La decisione descrive esattamente lo stato raggiunto.
        assert_eq!(decision.state, machine.state());

        // Lo stato di quarantena è assorbente: nessun evento può riportare la
        // sessione a riusabile.
        if before == SessionState::Quarantined {
            assert_eq!(machine.state(), SessionState::Quarantined);
        }

        // La macchina non produce lo stato Closed, che appartiene al
        // lifecycle della connessione e non a quello transazionale.
        assert_ne!(machine.state(), SessionState::Closed);

        // Solo uno stato non riusabile può richiedere un'azione di recupero.
        if decision.action != RecoveryAction::None {
            assert!(!machine.state().is_reusable());
        }

        // Un evento di perdita del trasporto o di cancellazione non può mai
        // lasciare la sessione riusabile.
        if matches!(
            event,
            TransactionEvent::TransportLost | TransactionEvent::Cancelled
        ) {
            assert_eq!(machine.state(), SessionState::Quarantined);
        }

        // La riconciliazione di un commit è richiesta solo uscendo da una
        // transazione aperta con trasporto perso.
        if decision.action == RecoveryAction::ReconcileCommit {
            assert_eq!(before, SessionState::Transaction);
            assert_eq!(event, TransactionEvent::TransportLost);
        }
    }
});
