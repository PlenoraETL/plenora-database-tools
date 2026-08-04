//! Seam di prova riutilizzabile per la diagnostica row-scoped.
//!
//! Il contratto `plenora-row-diagnostics-v1` promette che l'indice sorgente
//! pubblicato sia *provato*, non dedotto. La prova esiste solo se il percorso
//! diagnostico applica una riga per statement: questo modulo offre ai provider
//! un esecutore programmabile e un runtime minimo, così che entrambi i casi di
//! campagna siano verificabili senza database, container o credenziali.
//!
//! L'esecutore registra ogni indice applicato: un provider che raggruppa le
//! righe in un unico statement non può superare le asserzioni di questo seam.

use plenora_database_core::row_diagnostics::{
    RowApplication, RowRejection, RowScopedWriter, RowWriteFuture,
};
use plenora_database_core::{DatabaseError, Result, RollbackEvidence};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

/// Copione di una scrittura row-scoped simulata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowWriteScript {
    /// Indice sorgente assoluto che il server rifiuta.
    pub reject_at: u64,
    /// Causa di contratto associata al rifiuto.
    pub cause: String,
    /// Colonna dichiarata dal piano, mai dedotta da un messaggio vendor.
    pub column: Option<String>,
    /// Evidenza dell'annullamento raccolta dopo il rifiuto.
    pub rollback: RollbackEvidence,
}

impl RowWriteScript {
    /// Copione di un rifiuto per violazione di vincolo.
    #[must_use]
    pub fn constraint_violation(
        reject_at: u64,
        column: impl Into<String>,
        rollback: RollbackEvidence,
    ) -> Self {
        Self {
            reject_at,
            cause: plenora_database_core::row_diagnostics::CAUSE_CONSTRAINT_VIOLATION.to_owned(),
            column: Some(column.into()),
            rollback,
        }
    }
}

/// Esecutore programmabile del seam `RowScopedWriter`.
///
/// Applica ogni riga sorgente con uno statement proprio e rifiuta soltanto
/// l'indice del copione. Ogni indice applicato resta registrato, così che i
/// test possano dimostrare l'attribuzione per riga invece di assumerla.
#[derive(Debug, Clone)]
pub struct ScriptedRowWriter {
    script: RowWriteScript,
    statements: Vec<u64>,
    rollbacks: u32,
}

impl ScriptedRowWriter {
    #[must_use]
    pub const fn new(script: RowWriteScript) -> Self {
        Self {
            script,
            statements: Vec::new(),
            rollbacks: 0,
        }
    }

    /// Indici sorgente applicati, nell'ordine in cui sono stati eseguiti.
    #[must_use]
    pub fn statements(&self) -> &[u64] {
        &self.statements
    }

    /// Numero di annullamenti richiesti: senza rifiuto deve restare zero.
    #[must_use]
    pub const fn rollbacks(&self) -> u32 {
        self.rollbacks
    }

    /// Verifica che gli statement eseguiti siano uno per riga, dall'offset
    /// zero fino alla riga che precede il rifiuto.
    ///
    /// # Errors
    ///
    /// Restituisce `InvalidPlan` quando il percorso ha raggruppato le righe,
    /// saltato un indice o applicato la riga rifiutata.
    pub fn verify_one_statement_per_row(&self, expected: u64) -> Result<()> {
        let executed = u64::try_from(self.statements.len())
            .map_err(|_| DatabaseError::invalid_plan("statement non rappresentabili"))?;
        if executed != expected {
            return Err(DatabaseError::invalid_plan(
                "il percorso diagnostico non ha eseguito uno statement per riga",
            ));
        }
        if self
            .statements
            .iter()
            .enumerate()
            .any(|(position, index)| u64::try_from(position) != Ok(*index))
        {
            return Err(DatabaseError::invalid_plan(
                "gli statement non seguono l'offset sorgente assoluto",
            ));
        }
        Ok(())
    }
}

impl RowScopedWriter for ScriptedRowWriter {
    fn apply_row(&mut self, source_index: u64) -> RowWriteFuture<'_, Result<RowApplication>> {
        let rejected = source_index == self.script.reject_at;
        if rejected {
            let rejection = RowRejection {
                cause: self.script.cause.clone(),
                column: self.script.column.clone(),
            };
            return Box::pin(async move { Ok(RowApplication::Rejected(rejection)) });
        }
        self.statements.push(source_index);
        Box::pin(async move { Ok(RowApplication::Applied) })
    }

    fn finish_declared_input(&mut self) -> RowWriteFuture<'_, Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn rollback(&mut self) -> RowWriteFuture<'_, RollbackEvidence> {
        self.rollbacks = self.rollbacks.saturating_add(1);
        let evidence = self.script.rollback;
        Box::pin(async move { evidence })
    }
}

/// Esegue un futuro fino al termine senza runtime esterno.
///
/// Il seam è `async` perché lo è il percorso reale dei provider; i test
/// offline non devono però ereditarne il runtime. Il seam non sospende mai su
/// I/O reale, quindi un poll ripetuto con waker inerte è sufficiente.
pub fn block_on<F: Future>(future: F) -> F::Output {
    struct Idle;
    impl Wake for Idle {
        fn wake(self: Arc<Self>) {}
    }

    let mut future = std::pin::pin!(future);
    let waker = Waker::from(Arc::new(Idle));
    let mut context = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
}
