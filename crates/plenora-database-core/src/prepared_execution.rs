//! Confine unico fra il piano Arrow, il consenso e l'esito pubblicato.

use crate::loss::{LossReport, LossSeverity};
use crate::outcome::WriteOutcome;
use crate::provider::PreparedWrite;
use crate::{DatabaseError, ErrorCategory, ErrorPhase, Result};
use std::future::Future;

/// Verifica il rapporto prima di invocare il codice che puo scrivere.
///
/// # Errors
/// Rifiuta report invalidi e `DataLoss` senza consenso, prima dell'esecuzione.
pub async fn execute<F, Fut>(
    prepared: PreparedWrite,
    accept_loss: bool,
    run: F,
) -> Result<WriteOutcome>
where
    F: FnOnce(PreparedWrite) -> Fut,
    Fut: Future<Output = Result<WriteOutcome>>,
{
    let report = prepared.loss_report.clone();
    if report.policy != prepared.operation.mapping_policy {
        return Err(DatabaseError::invalid_plan(
            "policy del rapporto diversa dal piano",
        ));
    }
    authorize(&report, accept_loss)?;
    attach(report, run(prepared).await)
}

/// Ammissione condivisa anche dal percorso Rust SQL Server diretto.
///
/// # Errors
/// Rifiuta un rapporto invalido o una perdita non autorizzata.
pub fn authorize(report: &LossReport, accept_loss: bool) -> Result<()> {
    report.validate()?;
    if !report.permits_execution()
        || (!accept_loss
            && report
                .losses
                .iter()
                .any(|loss| loss.severity == LossSeverity::DataLoss))
    {
        return Err(DatabaseError::new(
            ErrorCategory::DataMapping,
            ErrorPhase::Prepare,
            None,
            "il piano richiede consenso esplicito alle perdite di dati",
        ));
    }
    Ok(())
}

/// Associa il rapporto al risultato anche in caso di commit parziale o incerto.
/// Gli errori senza outcome restano errori: non si inventa un rapporto vuoto.
///
/// # Errors
/// Preserva l'errore dell'esecutore senza modificarne gli assi.
pub fn attach(report: LossReport, result: Result<WriteOutcome>) -> Result<WriteOutcome> {
    result.map(|mut outcome| {
        outcome.schema_version = 3;
        outcome.loss_report = Some(report);
        outcome
    })
}
