//! Residui DDL di un'operazione abbandonata.
//!
//! Oracle e MySQL eseguono il `CREATE TABLE` di `mode='create'` con un
//! commit implicito: dopo il suo invio la tabella puo esistere qualunque cosa
//! accada al carico. Finche la scrittura arriva a un esito, il residuo si
//! dichiara sul valore restituito (`partial` o `unknown`). Una scrittura
//! **abbandonata** — la future lasciata cadere dal chiamante, un panic del
//! produttore dei batch — non arriva a nessun esito: senza altro, il residuo
//! resterebbe invisibile.
//!
//! [`ResidueLedger`] e il punto osservabile di quei casi. Una scrittura
//! `create` arma una [`ResidueGuard`] **prima** di inviare il DDL e la chiude
//! solo quando ha un esito da restituire; se la guardia cade armata (drop o
//! unwinding di un panic), il target finisce nel registro del provider, che
//! lo espone con [`ResidueLedger::snapshot`].
//!
//! # Limite dichiarato
//!
//! Il registro vive nella memoria del processo. **Regola**: un'operazione
//! `create` abbandonata e osservabile dal registro del provider che l'ha
//! eseguita. **Ambito**: il crash o l'uscita del processo (incluso un panic
//! con `panic = "abort"`) perdono il registro. **Hazard**: dopo un crash una
//! tabella creata puo restare senza che nessun esito lo dica. **Rientro**:
//! un registro durevole delle intenzioni (un journal su disco o una tabella
//! di servizio), che oggi la libreria non ha.

use crate::plan::ObjectRef;
use serde::Serialize;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

/// Quanti target recenti il registro conserva; il totale li conta tutti.
pub const RECENT_RESIDUES: usize = 32;

/// Il registro dei residui possibili di un provider.
#[derive(Debug, Default)]
pub struct ResidueLedger {
    inner: Mutex<LedgerState>,
}

#[derive(Debug, Default)]
struct LedgerState {
    total: u64,
    recent: VecDeque<ObjectRef>,
}

/// Cio che il registro ha visto: il numero di operazioni abbandonate dopo
/// l'invio di un DDL e gli ultimi target, dal piu vecchio al piu recente.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ResidueSnapshot {
    pub total: u64,
    pub recent: Vec<ObjectRef>,
}

impl ResidueLedger {
    /// Arma una guardia per `target`: se cade senza [`ResidueGuard::settle`],
    /// il target entra nel registro.
    #[must_use]
    pub fn arm(self: &Arc<Self>, target: ObjectRef) -> ResidueGuard {
        ResidueGuard {
            ledger: Arc::clone(self),
            target: Some(target),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ResidueSnapshot {
        let state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        ResidueSnapshot {
            total: state.total,
            recent: state.recent.iter().cloned().collect(),
        }
    }

    fn record(&self, target: ObjectRef) {
        // Durante l'unwinding di un panic il lock puo essere avvelenato: il
        // registro resta valido, e un secondo panic qui abortirebbe.
        let mut state = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        state.total = state.total.saturating_add(1);
        if state.recent.len() == RECENT_RESIDUES {
            state.recent.pop_front();
        }
        state.recent.push_back(target);
    }
}

/// La guardia di un residuo possibile: armata prima del DDL, chiusa
/// dall'esito.
#[derive(Debug)]
pub struct ResidueGuard {
    ledger: Arc<ResidueLedger>,
    target: Option<ObjectRef>,
}

impl ResidueGuard {
    /// L'operazione ha un esito da restituire: il residuo, se c'e, lo
    /// dichiara l'esito stesso.
    pub fn settle(mut self) {
        self.target = None;
    }
}

impl Drop for ResidueGuard {
    fn drop(&mut self) {
        if let Some(target) = self.target.take() {
            self.ledger.record(target);
        }
    }
}

/// Esegue `work` con una guardia armata su `target`: chiusa quando `work`
/// rende un esito, registrata se la future cade prima o se `work` va in
/// panic.
///
/// # Errors
///
/// Quelli di `work`, invariati.
pub async fn with_residue_guard<T, F>(
    ledger: &Arc<ResidueLedger>,
    target: ObjectRef,
    work: F,
) -> crate::Result<T>
where
    F: Future<Output = crate::Result<T>>,
{
    let guard = ledger.arm(target);
    let result = work.await;
    guard.settle();
    result
}

#[cfg(test)]
#[path = "ddl_residue_tests.rs"]
mod tests;
